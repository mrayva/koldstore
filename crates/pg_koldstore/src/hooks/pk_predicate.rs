//! Extracts a structured predicate -- a primary-key match plus any extra
//! ("residual") comparison conditions -- from a planned UPDATE/DELETE, for
//! the cold-DML write guard (`sql::cold_dml::guard`, upstream #122 Option
//! B).
//!
//! Deliberately conservative: any WHERE shape this cannot recognize with
//! full confidence returns `None`, which means "skip the guard for this
//! statement" (today's pre-#122-fix behavior), never "treat as unsafe."
//! A false *negative* (missing a case the guard could have caught) is
//! acceptable and exactly what upstream issue #122's Option B scoping
//! deferred to a follow-up (bulk/complex statement forms); a false
//! *positive* (rejecting or misjudging a statement this can't actually
//! analyze correctly) is not.
//!
//! Correctness note on why an extra ("residual") condition alongside the
//! PK match must be *evaluated*, not merely tolerated or rejected
//! outright: this extraction only matters when the native statement
//! already affected zero rows. If the true WHERE clause were `id = 5 AND
//! status = 'x'` and the real reason for zero rows was a hot row's
//! `status` mismatch (not cold-ness), treating `id = 5`'s mere existence
//! (hot or cold, via the merge-scan probe) as "unsafe" would wrongly
//! reject a statement that was already completely correct. The guard must
//! instead fetch the located row (see `sql::cold_dml::guard::locate_row`)
//! and re-check the residual condition against its *actual* values before
//! deciding: if the residual condition would also have matched, the zero
//! rows really is unexplained except by cold-ness, and rejecting is
//! correct; if not, the zero-row result was already legitimate and
//! unrelated to #122 at all.

use std::collections::HashMap;

use pgrx::pg_sys;

use crate::merge_scan::pg::{literals, qual};

/// One of the six ordinary scalar comparison operators this module
/// recognizes on a bare `Var`. Anything else (`LIKE`, `IS [NOT] NULL`,
/// function calls, ...) is out of scope -- the whole predicate extraction
/// fails rather than guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

impl ComparisonOp {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "=" => Some(Self::Eq),
            "<>" | "!=" => Some(Self::Ne),
            "<" => Some(Self::Lt),
            ">" => Some(Self::Gt),
            "<=" => Some(Self::Le),
            ">=" => Some(Self::Ge),
            _ => None,
        }
    }

    /// The operator's own SQL text -- safe to interpolate directly into
    /// generated SQL since it only ever comes from [`Self::from_name`]'s
    /// fixed, exhaustively-matched allowlist above, never from arbitrary
    /// input.
    pub(crate) fn sql_symbol(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "<>",
            Self::Lt => "<",
            Self::Gt => ">",
            Self::Le => "<=",
            Self::Ge => ">=",
        }
    }
}

/// One `Var OP <literal-or-list>` leaf from a WHERE clause, extracted
/// during the pointer-safe plan-tree walk but not yet classified as a
/// primary-key match or a residual condition -- that classification needs
/// a catalog lookup this phase must not perform (see the module doc
/// comment and `executor.rs`'s call site for why the walk and the
/// classification are two separate phases).
pub(crate) struct RawLeaf {
    attnum: i16,
    operator: ComparisonOp,
    /// More than one value only ever occurs for `Eq` via an `IN (...)`
    /// (`ScalarArrayOpExpr`) on a primary-key column.
    values: Vec<serde_json::Value>,
}

impl RawLeaf {
    /// Cost-estimate contribution for `executor.rs`'s `candidate_count`
    /// (an upper bound is fine even before phase 2's PK/residual split --
    /// see that call site's comment).
    pub(crate) fn value_count(&self) -> usize {
        self.values.len()
    }
}

/// A residual (non-primary-key) condition to re-check against a located
/// row's actual values, per [`resolve_predicate`]. `values` holds more
/// than one entry only for `Eq` (an `IN (...)` on this column); every
/// other operator always has exactly one.
pub(crate) struct ResidualLeaf {
    pub(crate) column: String,
    pub(crate) operator: ComparisonOp,
    pub(crate) values: Vec<serde_json::Value>,
}

/// Phase 1 (plan-tree walking only, **no SPI**): extracts every
/// `Var OP <literal-or-bound-param>` leaf this UPDATE/DELETE/MERGE's WHERE
/// clause is built from -- deliberately unaware of which columns are
/// actually the primary key, since resolving that requires a catalog/SPI
/// lookup this phase must not perform (see the call site in
/// `executor.rs`: the plan tree these pointers reference may not survive
/// past `standard_ExecutorEnd`, while SPI must not run *before* it -- this
/// phase runs first, while the pointers are still live, and hands its
/// pointer-free `Vec<RawLeaf>` result to [`resolve_predicate`] afterward).
///
/// Returns `None` when the WHERE clause is not cleanly a single leaf or an
/// `AND`-chain of them (any `OR`, unrecognized operator, non-literal
/// operand, or unrecognized plan shape).
#[must_use]
pub(crate) unsafe fn extract_raw_predicate_leaves(
    modify_table_plan: *mut pg_sys::Plan,
    params: pg_sys::ParamListInfo,
) -> Option<Vec<RawLeaf>> {
    unsafe {
        if modify_table_plan.is_null() {
            return None;
        }
        let subplan = (*modify_table_plan).lefttree;
        if subplan.is_null() {
            return None;
        }
        let (scanrelid, qual_lists) = scan_qual_sources(subplan)?;

        // Every qual source that applies to this scan must walk cleanly --
        // an IndexScan's index condition (`indexqualorig`) covers only
        // what the index itself can evaluate; anything else in the WHERE
        // clause that couldn't be pushed into the index lands in the
        // *residual* filter (`.plan.qual`) instead, a completely separate
        // list. Reading only one of the two would miss conditions there.
        let mut leaves: Vec<RawLeaf> = Vec::new();
        for qual_list in qual_lists {
            if qual_list.is_null() {
                continue;
            }
            if !walk_leaves(qual_list, scanrelid, params, &mut leaves) {
                return None;
            }
        }
        Some(leaves)
    }
}

/// Above this many candidate PK combinations, [`resolve_predicate`] gives
/// up rather than expanding the full cross-product -- each candidate
/// costs one `cold_pk_exists` merge-scan lookup, so an unbounded `IN`
/// list (or several on different composite-PK columns at once) could
/// otherwise turn one rejected statement into an unbounded amount of
/// guard work. Skipping (not rejecting) is always safe -- see the module
/// doc comment.
const MAX_CANDIDATE_COMBINATIONS: usize = 64;

/// Above this many values, a residual `IN (...)` (a non-PK column) is
/// left unguarded rather than re-checked -- see [`resolve_predicate`].
const MAX_RESIDUAL_IN_VALUES: usize = 64;

/// Phase 2 (SPI-safe, no plan-tree pointers involved): classifies
/// [`extract_raw_predicate_leaves`]'s output against the table's real
/// primary-key columns (`column_attnums` filtered to `pk_columns`) into
/// (a) the primary-key match -- every PK column must appear as an `Eq`
/// leaf (a non-`Eq` operator on a PK column, e.g. a range condition, is
/// out of scope and fails the whole extraction), expanded into the full
/// candidate-combination cross-product (see [`MAX_CANDIDATE_COMBINATIONS`]
/// and the `IN (...)` handling this mirrors), and (b) every other leaf as
/// a residual condition to re-check against each candidate's actual row
/// (see [`ResidualLeaf`] and the module doc comment on why) -- including
/// an `IN (...)` on a non-PK column (more than one value, always `Eq`),
/// re-checked as membership rather than equality; capped at
/// [`MAX_RESIDUAL_IN_VALUES`] for the same reason PK candidates are capped
/// at [`MAX_CANDIDATE_COMBINATIONS`] -- each residual `IN` value adds one
/// more coercion to the re-check query, so an unbounded list must not
/// turn into unbounded guard work.
#[must_use]
pub(crate) fn resolve_predicate(
    raw: &[RawLeaf],
    pk_columns: &[String],
    column_attnums: &HashMap<i16, String>,
) -> Option<(Vec<serde_json::Value>, Vec<ResidualLeaf>)> {
    if pk_columns.is_empty() {
        return None;
    }
    let pk_attnums: HashMap<i16, &str> = column_attnums
        .iter()
        .filter(|(_, name)| pk_columns.iter().any(|pk| pk == *name))
        .map(|(attnum, name)| (*attnum, name.as_str()))
        .collect();

    let mut pk_values: HashMap<i16, Vec<serde_json::Value>> = HashMap::new();
    let mut residual: Vec<ResidualLeaf> = Vec::new();
    for leaf in raw {
        if pk_attnums.contains_key(&leaf.attnum) {
            if leaf.operator != ComparisonOp::Eq {
                return None; // a range/inequality condition on a PK column -- deferred scope
            }
            pk_values.entry(leaf.attnum).or_default().extend(leaf.values.iter().cloned());
        } else {
            let name = column_attnums.get(&leaf.attnum)?;
            if leaf.values.is_empty() || leaf.values.len() > MAX_RESIDUAL_IN_VALUES {
                return None;
            }
            residual.push(ResidualLeaf {
                column: name.clone(),
                operator: leaf.operator,
                values: leaf.values.clone(),
            });
        }
    }
    if pk_attnums.len() != pk_columns.len() || pk_values.len() != pk_attnums.len() {
        return None;
    }

    // Cross-product of each PK column's candidate list, e.g.
    // [("id", [1,2,3])] -> [{"id":1}, {"id":2}, {"id":3}].
    let mut combinations: Vec<serde_json::Map<String, serde_json::Value>> = vec![serde_json::Map::new()];
    for (attnum, values) in &pk_values {
        let name = pk_attnums.get(attnum)?;
        if values.is_empty() {
            return None;
        }
        let expanded = combinations.len().checked_mul(values.len())?;
        if expanded > MAX_CANDIDATE_COMBINATIONS {
            return None;
        }
        let mut next = Vec::with_capacity(expanded);
        for base in &combinations {
            for value in values {
                let mut candidate = base.clone();
                candidate.insert((*name).to_string(), value.clone());
                next.push(candidate);
            }
        }
        combinations = next;
    }
    let candidates = combinations.into_iter().map(serde_json::Value::Object).collect();
    Some((candidates, residual))
}

/// Locates every qual list a simple point-lookup UPDATE/DELETE subplan's
/// WHERE clause could be split across, for the three physical scan shapes
/// a PK-equality (or PK `IN (...)`) lookup realistically produces.
/// Anything else (joins, partition-routed `ModifyTable`, ...) returns
/// `None` -- deferred scope.
unsafe fn scan_qual_sources(plan: *mut pg_sys::Plan) -> Option<(pg_sys::Index, Vec<*mut pg_sys::List>)> {
    unsafe {
        match (*plan).type_ {
            pg_sys::NodeTag::T_IndexScan => {
                let index_scan = plan.cast::<pg_sys::IndexScan>();
                // `indexqualorig` (what the index itself evaluates) and
                // `.scan.plan.qual` (the residual filter for whatever the
                // index couldn't cover) are separate lists -- see the call
                // site's comment for why both must be read.
                Some((
                    (*index_scan).scan.scanrelid,
                    vec![(*index_scan).indexqualorig, (*index_scan).scan.plan.qual],
                ))
            }
            pg_sys::NodeTag::T_SeqScan => {
                let scan = plan.cast::<pg_sys::Scan>();
                Some(((*scan).scanrelid, vec![(*plan).qual]))
            }
            pg_sys::NodeTag::T_BitmapHeapScan => {
                // `WHERE pk IN (...)` plans as BitmapHeapScan/BitmapIndexScan
                // -- confirmed live via EXPLAIN. `bitmapqualorig` mirrors
                // `indexqualorig`'s role (the original, pre-bitmap-probe
                // condition); `.scan.plan.qual` is its residual filter, same
                // reasoning as the IndexScan case above.
                let bitmap_scan = plan.cast::<pg_sys::BitmapHeapScan>();
                Some((
                    (*bitmap_scan).scan.scanrelid,
                    vec![(*bitmap_scan).bitmapqualorig, (*bitmap_scan).scan.plan.qual],
                ))
            }
            _ => None,
        }
    }
}

/// Walks `qual` (a `List` of ANDed quals) plus any nested `AND`-only
/// `BoolExpr`, requiring every leaf to be a `Var OP <literal>` comparison
/// or `Var = ANY(<literal array>)` (`IN (...)`) on this scan's own
/// relation. Returns `false` the moment anything else is seen (`OR`, an
/// unrecognized operator, a non-literal operand, ...) -- which leaves are
/// primary-key matches versus residual conditions is decided later, in
/// [`resolve_predicate`], once a catalog lookup can safely happen.
unsafe fn walk_leaves(
    qual: *mut pg_sys::List,
    scanrelid: pg_sys::Index,
    params: pg_sys::ParamListInfo,
    leaves: &mut Vec<RawLeaf>,
) -> bool {
    unsafe {
        literals::list_node_pointers(qual)
            .into_iter()
            .all(|node| walk_leaf_node(node.cast::<pg_sys::Expr>(), scanrelid, params, leaves))
    }
}

unsafe fn walk_leaf_node(
    expr: *mut pg_sys::Expr,
    scanrelid: pg_sys::Index,
    params: pg_sys::ParamListInfo,
    leaves: &mut Vec<RawLeaf>,
) -> bool {
    unsafe {
        if expr.is_null() {
            return false;
        }
        match (*expr).type_ {
            pg_sys::NodeTag::T_OpExpr => comparison_leaf(expr, scanrelid, params, leaves),
            pg_sys::NodeTag::T_ScalarArrayOpExpr => in_list_leaf(expr, scanrelid, params, leaves),
            pg_sys::NodeTag::T_BoolExpr => {
                let bool_expr = expr.cast::<pg_sys::BoolExpr>();
                if (*bool_expr).boolop != pg_sys::BoolExprType::AND_EXPR {
                    return false;
                }
                literals::list_node_pointers((*bool_expr).args)
                    .into_iter()
                    .all(|node| walk_leaf_node(node.cast::<pg_sys::Expr>(), scanrelid, params, leaves))
            }
            _ => false,
        }
    }
}

unsafe fn comparison_leaf(
    expr: *mut pg_sys::Expr,
    scanrelid: pg_sys::Index,
    params: pg_sys::ParamListInfo,
    leaves: &mut Vec<RawLeaf>,
) -> bool {
    unsafe {
        let op_expr = expr.cast::<pg_sys::OpExpr>();
        if !qual::operator_is_pg_catalog((*op_expr).opno) {
            return false;
        }
        let Some(operator) = comparison_operator((*op_expr).opno) else {
            return false;
        };
        let args = literals::list_node_pointers((*op_expr).args);
        if args.len() != 2 {
            return false;
        }
        let (left, right) = (args[0].cast::<pg_sys::Expr>(), args[1].cast::<pg_sys::Expr>());

        record_leaf(left, right, operator, scanrelid, params, leaves)
            || record_leaf(right, left, operator.flip(), scanrelid, params, leaves)
    }
}

/// `Var = ANY(<array>)` -- `IN (...)`'s planned form. `useOr = false`
/// (`<> ALL`, i.e. `NOT IN`) is rejected: it does not identify a bounded
/// candidate set the same way, and is out of scope here. Only a `Var` on
/// the left (the PostgreSQL-canonical form for `ScalarArrayOpExpr`) is
/// accepted -- an array on the left never occurs for `x IN (...)`.
unsafe fn in_list_leaf(
    expr: *mut pg_sys::Expr,
    scanrelid: pg_sys::Index,
    params: pg_sys::ParamListInfo,
    leaves: &mut Vec<RawLeaf>,
) -> bool {
    unsafe {
        let saop = expr.cast::<pg_sys::ScalarArrayOpExpr>();
        if !(*saop).useOr || !qual::operator_is_pg_catalog((*saop).opno) {
            return false;
        }
        let Some(ComparisonOp::Eq) = comparison_operator((*saop).opno) else {
            return false;
        };
        let args = literals::list_node_pointers((*saop).args);
        if args.len() != 2 {
            return false;
        }
        let Some(attnum) = var_attnum(args[0].cast::<pg_sys::Expr>(), scanrelid) else {
            return false;
        };
        let Some(values) = array_literal_json_values(args[1].cast::<pg_sys::Expr>(), params) else {
            return false;
        };
        if values.is_empty() {
            return false;
        }
        leaves.push(RawLeaf { attnum, operator: ComparisonOp::Eq, values });
        true
    }
}

/// If `column_side` is a bare `Var` on `scanrelid` and `value_side` is a
/// literal-or-bound-param, records the leaf and returns `true`. A partial
/// match (a `Var` paired with a non-literal, or a NULL literal) is treated
/// as a failure, not a skip -- see the module doc comment on why an
/// unrecognized shape must abort the whole extraction rather than being
/// silently dropped.
unsafe fn record_leaf(
    column_side: *mut pg_sys::Expr,
    value_side: *mut pg_sys::Expr,
    operator: ComparisonOp,
    scanrelid: pg_sys::Index,
    params: pg_sys::ParamListInfo,
    leaves: &mut Vec<RawLeaf>,
) -> bool {
    unsafe {
        let Some(attnum) = var_attnum(column_side, scanrelid) else {
            return false;
        };
        let Some((datum, isnull, type_oid)) = literals::const_or_param_datum(value_side, params) else {
            return false;
        };
        if isnull {
            return false;
        }
        let Some(value) = datum_to_json_text(datum, type_oid) else {
            return false;
        };
        leaves.push(RawLeaf { attnum, operator, values: vec![value] });
        true
    }
}

impl ComparisonOp {
    /// The operator to use when a leaf's `Var` and literal are swapped
    /// (`5 = id` instead of `id = 5`): only order-sensitive operators
    /// change (`<` becomes `>` and so on); `=`/`<>` are unaffected.
    const fn flip(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::Ne => Self::Ne,
            Self::Lt => Self::Gt,
            Self::Gt => Self::Lt,
            Self::Le => Self::Ge,
            Self::Ge => Self::Le,
        }
    }
}

unsafe fn var_attnum(expr: *mut pg_sys::Expr, scanrelid: pg_sys::Index) -> Option<i16> {
    unsafe {
        let expr = literals::unwrap_relabel(expr);
        if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Var {
            return None;
        }
        let var = expr.cast::<pg_sys::Var>();
        if (*var).varlevelsup != 0 || (*var).varno != scanrelid as std::ffi::c_int {
            return None;
        }
        ((*var).varattno > 0).then_some((*var).varattno)
    }
}

unsafe fn comparison_operator(operator: pg_sys::Oid) -> Option<ComparisonOp> {
    unsafe {
        let name_ptr = pg_sys::get_opname(operator);
        if name_ptr.is_null() {
            return None;
        }
        ComparisonOp::from_name(std::ffi::CStr::from_ptr(name_ptr).to_str().ok()?)
    }
}

unsafe fn datum_to_json_text(datum: pg_sys::Datum, type_oid: pg_sys::Oid) -> Option<serde_json::Value> {
    unsafe {
        let mut typoutput = pg_sys::InvalidOid;
        let mut typisvarlena = false;
        pg_sys::getTypeOutputInfo(type_oid, &mut typoutput, &mut typisvarlena);
        let out = pg_sys::OidOutputFunctionCall(typoutput, datum);
        let text = literals::cstr_owned_pfree(out)?;
        Some(serde_json::Value::String(text))
    }
}

/// Resolves `expr` (a `Const` or bound `PARAM_EXTERN`, per
/// `const_or_param_datum`) to an array-typed datum and decodes every
/// element to jsonb via the SAME generic output-function conversion
/// [`datum_to_json_text`] already uses for scalars. Any NULL element
/// fails the whole array (conservative -- `IN (1, NULL)` is not a clean
/// "these are the candidate keys" set to check).
unsafe fn array_literal_json_values(
    expr: *mut pg_sys::Expr,
    params: pg_sys::ParamListInfo,
) -> Option<Vec<serde_json::Value>> {
    unsafe {
        let (datum, isnull, array_type_oid) = literals::const_or_param_datum(expr, params)?;
        if isnull {
            return None;
        }
        let element_type_oid = pg_sys::get_element_type(array_type_oid);
        if element_type_oid == pg_sys::InvalidOid {
            return None; // not actually an array type
        }
        let mut typlen: i16 = 0;
        let mut typbyval = false;
        let mut typalign: std::ffi::c_char = 0;
        pg_sys::get_typlenbyvalalign(element_type_oid, &mut typlen, &mut typbyval, &mut typalign);

        let array = pg_sys::pg_detoast_datum(datum.cast_mut_ptr::<pg_sys::varlena>()).cast::<pg_sys::ArrayType>();
        let mut elems: *mut pg_sys::Datum = std::ptr::null_mut();
        let mut nulls: *mut bool = std::ptr::null_mut();
        let mut nelems: std::ffi::c_int = 0;
        pg_sys::deconstruct_array(
            array,
            element_type_oid,
            std::ffi::c_int::from(typlen),
            typbyval,
            typalign,
            &mut elems,
            &mut nulls,
            &mut nelems,
        );
        if nelems <= 0 {
            return None;
        }
        let mut values = Vec::with_capacity(nelems as usize);
        for index in 0..nelems as usize {
            if *nulls.add(index) {
                return None;
            }
            values.push(datum_to_json_text(*elems.add(index), element_type_oid)?);
        }
        Some(values)
    }
}
