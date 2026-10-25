//! Planner qual walking for safe prune predicates and post-merge filters.

use std::collections::BTreeSet;
use std::ffi::CStr;
use std::os::raw::c_char;

use koldstore_merge::scan::plan::SegmentPrunePredicate;
use pgrx::pg_sys;

use super::literals::{
    array_sort_key_values, list_node_pointers, literal_sort_key_value, typed_literal_sql, unwrap_relabel,
};

/// One base-relation attribute required by output projection or executor quals.
#[derive(Debug, Clone)]
pub(super) struct ScanProjectionColumn {
    pub(super) catalog: koldstore_migrate::order::CatalogColumn,
    /// Zero-based position in the base relation's scan tuple.
    pub(super) slot_index: usize,
}

/// Minimal base-relation projection used to build tuples for PostgreSQL `ExecScan`.
#[derive(Debug, Clone)]
pub(super) struct ScanProjection {
    pub(super) columns: Vec<ScanProjectionColumn>,
    pub(super) tuple_width: usize,
}

impl ScanProjection {
    pub(super) fn catalog_columns(&self) -> Vec<&koldstore_migrate::order::CatalogColumn> {
        self.columns.iter().map(|column| &column.catalog).collect()
    }
}

/// Canonical predicates eligible for primary-key source pruning.
#[derive(Debug, Default)]
pub(super) struct ResidualFilters {
    pub(super) hot_equality: Vec<super::hot::HotEqualityFilter>,
    pub(super) hot_range: Vec<super::hot::HotRangeFilter>,
}

#[derive(Debug, Clone, Copy)]
struct QualCatalog<'a> {
    scanrelid: pg_sys::Index,
    catalog: &'a koldstore_migrate::ExistingTableCatalog,
}

impl<'a> QualCatalog<'a> {
    fn column_by_attnum(self, attnum: i16) -> Option<&'a koldstore_migrate::order::CatalogColumn> {
        self.catalog.column_by_attnum(attnum)
    }
}

/// Full physical base-relation projection for `custom_scan_tlist` scans.
///
/// When PlanCustomPath sets `custom_scan_tlist`, setrefs rewrites plan
/// targetlist/qual Vars to `INDEX_VAR`. [`required_scan_projection`] then sees
/// no `scanrelid` Vars; materialize every user column so ExecScan can project.
pub(super) fn physical_scan_projection(
    catalog: &koldstore_migrate::ExistingTableCatalog,
    tuple_width: usize,
) -> Result<ScanProjection, String> {
    let mut projection = Vec::with_capacity(tuple_width);
    for slot_index in 0..tuple_width {
        let attnum =
            pg_sys::AttrNumber::try_from(slot_index + 1).map_err(|error| error.to_string())?;
        let Some(column) = catalog.column_by_attnum(attnum) else {
            // Dropped attributes stay NULL in the physical slot.
            continue;
        };
        projection.push(ScanProjectionColumn {
            catalog: column.clone(),
            slot_index,
        });
    }
    Ok(ScanProjection {
        columns: projection,
        tuple_width,
    })
}

/// Collects every base-table attribute referenced by the target list or quals.
///
/// PostgreSQL's generic Var walker covers arbitrary RLS expressions and planned
/// subqueries, so cold enforcement does not depend on KoldStore understanding a
/// policy's expression shape.
pub(super) unsafe fn required_scan_projection(
    scanrelid: pg_sys::Index,
    targetlist: *mut pg_sys::List,
    qual: *mut pg_sys::List,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    tuple_width: usize,
) -> Result<ScanProjection, String> {
    let mut attrs: *mut pg_sys::Bitmapset = std::ptr::null_mut();
    unsafe {
        pg_sys::pull_varattnos(targetlist.cast::<pg_sys::Node>(), scanrelid, &mut attrs);
        pg_sys::pull_varattnos(qual.cast::<pg_sys::Node>(), scanrelid, &mut attrs);
    }

    let whole_row_member = -pg_sys::FirstLowInvalidHeapAttributeNumber;
    let whole_row = unsafe { pg_sys::bms_is_member(whole_row_member, attrs) };
    let mut system_column = None;
    for attnum in (pg_sys::FirstLowInvalidHeapAttributeNumber + 1)..0 {
        if unsafe {
            pg_sys::bms_is_member(attnum - pg_sys::FirstLowInvalidHeapAttributeNumber, attrs)
        } {
            system_column = Some(attnum);
            break;
        }
    }

    let mut required = Vec::with_capacity(tuple_width);
    for attnum in 1..=tuple_width {
        required.push(
            whole_row
                || unsafe {
                    pg_sys::bms_is_member(
                        i32::try_from(attnum).map_err(|error| error.to_string())?
                            - pg_sys::FirstLowInvalidHeapAttributeNumber,
                        attrs,
                    )
                },
        );
    }
    unsafe {
        pg_sys::bms_free(attrs);
    }

    if let Some(attnum) = system_column {
        return Err(format!(
            "KoldMergeScan cannot materialize PostgreSQL system attribute {attnum} (ctid/xmin/... exist only on heap rows, not on cold rows; \
             remove it from the query, or use the primary key to identify rows)"
        ));
    }

    let mut projection = Vec::new();
    for (slot_index, is_required) in required.into_iter().enumerate() {
        if !is_required {
            continue;
        }
        let attnum =
            pg_sys::AttrNumber::try_from(slot_index + 1).map_err(|error| error.to_string())?;
        let Some(column) = catalog.column_by_attnum(attnum) else {
            if whole_row {
                // PostgreSQL represents a dropped attribute in a whole-row
                // value as NULL; it is intentionally absent from our catalog.
                continue;
            }
            return Err(format!(
                "required base-relation attribute {} is not present in the managed schema",
                slot_index + 1,
            ));
        };
        projection.push(ScanProjectionColumn {
            catalog: column.clone(),
            slot_index,
        });
    }

    Ok(ScanProjection {
        columns: projection,
        tuple_width,
    })
}

pub(super) unsafe fn residual_filters(
    scanrelid: pg_sys::Index,
    qual: *mut pg_sys::List,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    params: pg_sys::ParamListInfo,
) -> ResidualFilters {
    let catalog = QualCatalog { scanrelid, catalog };
    let mut filters = ResidualFilters::default();
    for node in list_node_pointers(qual) {
        collect_residual_filters(node.cast::<pg_sys::Expr>(), catalog, params, &mut filters);
    }
    filters
}

/// Returns whether equality clauses cover every primary-key attribute.
///
/// Only PostgreSQL catalog `=` operators with a base-relation [`pg_sys::Var`]
/// and a constant or external prepared-statement parameter qualify. Executor
/// parameters used by parameterized nested-loop paths remain on the
/// conservative merge path because their value may change across rescans.
pub(super) unsafe fn quals_cover_primary_key(
    scanrelid: pg_sys::Index,
    qual: *mut pg_sys::List,
    primary_key_attnums: &[i16],
) -> bool {
    if primary_key_attnums.is_empty() {
        return false;
    }
    let mut covered = BTreeSet::new();
    for node in list_node_pointers(qual) {
        collect_equality_attnums(node.cast::<pg_sys::Expr>(), scanrelid, &mut covered);
    }
    primary_key_attnums
        .iter()
        .all(|attnum| covered.contains(attnum))
}

unsafe fn collect_equality_attnums(
    expr: *mut pg_sys::Expr,
    scanrelid: pg_sys::Index,
    covered: &mut BTreeSet<i16>,
) {
    if expr.is_null() {
        return;
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_OpExpr => {
            let op_expr = expr.cast::<pg_sys::OpExpr>();
            if cstr_to_str(pg_sys::get_opname((*op_expr).opno)) != Some("=")
                || !operator_is_pg_catalog((*op_expr).opno)
            {
                return;
            }
            let args = list_node_pointers((*op_expr).args);
            if args.len() != 2 {
                return;
            }
            if let Some(attnum) = point_var_attnum(args[0], args[1], scanrelid)
                .or_else(|| point_var_attnum(args[1], args[0], scanrelid))
            {
                covered.insert(attnum);
            }
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let bool_expr = expr.cast::<pg_sys::BoolExpr>();
            if (*bool_expr).boolop != pg_sys::BoolExprType::AND_EXPR {
                return;
            }
            for arg in list_node_pointers((*bool_expr).args) {
                collect_equality_attnums(arg.cast::<pg_sys::Expr>(), scanrelid, covered);
            }
        }
        _ => {}
    }
}

unsafe fn point_var_attnum(
    variable_expr: *mut std::ffi::c_void,
    value_expr: *mut std::ffi::c_void,
    scanrelid: pg_sys::Index,
) -> Option<i16> {
    let variable_expr = unwrap_relabel(variable_expr.cast::<pg_sys::Expr>());
    if variable_expr.is_null() || (*variable_expr).type_ != pg_sys::NodeTag::T_Var {
        return None;
    }
    let var = variable_expr.cast::<pg_sys::Var>();
    let scanrelid = i32::try_from(scanrelid).ok()?;
    if (*var).varattno <= 0 || (*var).varlevelsup != 0 || (*var).varno != scanrelid {
        return None;
    }

    let value_expr = unwrap_relabel(value_expr.cast::<pg_sys::Expr>());
    if value_expr.is_null() {
        return None;
    }
    match (*value_expr).type_ {
        pg_sys::NodeTag::T_Const => Some((*var).varattno),
        pg_sys::NodeTag::T_Param => {
            let param = value_expr.cast::<pg_sys::Param>();
            ((*param).paramkind == pg_sys::ParamKind::PARAM_EXTERN).then_some((*var).varattno)
        }
        _ => None,
    }
}

/// The scope a user-scoped scan is restricted to by the session: a top-level conjunct
/// `scope_column = current_setting('koldstore.user_id'[, true])`, which is what the generated
/// row-level-security policy contributes. The value is read when the scan starts, so this is only
/// for executor-time use: a planner (or a cached generic plan) must not bake it in.
///
/// # Safety
///
/// `qual` must be a planner-owned expression list for `scanrelid`.
pub(super) unsafe fn session_scope_equality(
    scanrelid: pg_sys::Index,
    qual: *mut pg_sys::List,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    scope_column_id: koldstore_common::ColumnId,
) -> Option<String> {
    let catalog = QualCatalog { scanrelid, catalog };
    list_node_pointers(qual)
        .into_iter()
        .find_map(|node| session_scope_in_node(node.cast::<pg_sys::Expr>(), catalog, scope_column_id))
}

unsafe fn session_scope_in_node(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
    scope_column_id: koldstore_common::ColumnId,
) -> Option<String> {
    if expr.is_null() {
        return None;
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_BoolExpr => {
            let bool_expr = expr.cast::<pg_sys::BoolExpr>();
            if (*bool_expr).boolop != pg_sys::BoolExprType::AND_EXPR {
                return None;
            }
            list_node_pointers((*bool_expr).args)
                .into_iter()
                .find_map(|node| session_scope_in_node(node.cast::<pg_sys::Expr>(), catalog, scope_column_id))
        }
        pg_sys::NodeTag::T_OpExpr => {
            let op_expr = expr.cast::<pg_sys::OpExpr>();
            if cstr_to_str(pg_sys::get_opname((*op_expr).opno))? != "=" || !operator_is_pg_catalog((*op_expr).opno) {
                return None;
            }
            // Equal must mean byte-equal for a segment's scope to be compared by text.
            if !collation_is_deterministic((*op_expr).inputcollid) {
                return None;
            }
            let args = list_node_pointers((*op_expr).args);
            if args.len() != 2 {
                return None;
            }
            let (left, right) = (
                unwrap_relabel(args[0].cast::<pg_sys::Expr>()),
                unwrap_relabel(args[1].cast::<pg_sys::Expr>()),
            );
            let (var, func) = if !left.is_null() && (*left).type_ == pg_sys::NodeTag::T_Var {
                (left, right)
            } else {
                (right, left)
            };
            if var.is_null() || func.is_null() || (*var).type_ != pg_sys::NodeTag::T_Var {
                return None;
            }
            let column = var_column(var, catalog)?;
            if column.column_id != scope_column_id {
                return None;
            }
            current_setting_user_id(func)
        }
        _ => None,
    }
}

/// The current value of `koldstore.user_id` when `expr` is `current_setting('koldstore.user_id'[, true])`.
unsafe fn current_setting_user_id(expr: *mut pg_sys::Expr) -> Option<String> {
    if (*expr).type_ != pg_sys::NodeTag::T_FuncExpr {
        return None;
    }
    let func = expr.cast::<pg_sys::FuncExpr>();
    if cstr_to_str(pg_sys::get_func_name((*func).funcid))? != "current_setting" {
        return None;
    }
    let args = list_node_pointers((*func).args);
    let name_expr = unwrap_relabel(args.first()?.cast::<pg_sys::Expr>());
    if name_expr.is_null() || (*name_expr).type_ != pg_sys::NodeTag::T_Const {
        return None;
    }
    let constant = name_expr.cast::<pg_sys::Const>();
    if (*constant).constisnull {
        return None;
    }
    let name = <String as pgrx::FromDatum>::from_polymorphic_datum(
        (*constant).constvalue,
        false,
        (*constant).consttype,
    )?;
    if !name.eq_ignore_ascii_case("koldstore.user_id") {
        return None;
    }
    let name = std::ffi::CString::new(name).ok()?;
    let value = pg_sys::GetConfigOption(name.as_ptr(), true, false);
    if value.is_null() {
        return None;
    }
    let value = CStr::from_ptr(value).to_str().ok()?.to_string();
    (!value.is_empty()).then_some(value)
}

pub(super) unsafe fn segment_prune_predicates(
    scanrelid: pg_sys::Index,
    qual: *mut pg_sys::List,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    params: pg_sys::ParamListInfo,
) -> Vec<SegmentPrunePredicate> {
    let catalog = QualCatalog { scanrelid, catalog };
    list_node_pointers(qual)
        .into_iter()
        .flat_map(|node| {
            segment_prune_node_predicates(node.cast::<pg_sys::Expr>(), catalog, params)
        })
        .collect()
}

/// A containment predicate on a `text[]` / `jsonb` column, as clauses over the column's segment term
/// filter (see `koldstore_parquet::term_filter`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SegmentTermPredicate {
    pub(super) column_id: i16,
    pub(super) clauses: koldstore_parquet::TermClauses,
}

/// Collects `@>`, `&&`, `?`, `?|` and `?&` predicates on `text[]` / `jsonb` columns that compare against
/// a constant or bound parameter. Anything else yields nothing, which only means no pruning.
pub(super) unsafe fn segment_term_predicates(
    scanrelid: pg_sys::Index,
    qual: *mut pg_sys::List,
    catalog: &koldstore_migrate::ExistingTableCatalog,
    params: pg_sys::ParamListInfo,
) -> Vec<SegmentTermPredicate> {
    let catalog = QualCatalog { scanrelid, catalog };
    let mut out = Vec::new();
    for node in list_node_pointers(qual) {
        collect_term_predicates(node.cast::<pg_sys::Expr>(), catalog, params, &mut out);
    }
    out
}

unsafe fn collect_term_predicates(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
    out: &mut Vec<SegmentTermPredicate>,
) {
    if expr.is_null() {
        return;
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_OpExpr => out.extend(term_predicate_from_op(expr, catalog, params)),
        pg_sys::NodeTag::T_BoolExpr => {
            let bool_expr = expr.cast::<pg_sys::BoolExpr>();
            if (*bool_expr).boolop == pg_sys::BoolExprType::AND_EXPR {
                for node in list_node_pointers((*bool_expr).args) {
                    collect_term_predicates(node.cast::<pg_sys::Expr>(), catalog, params, out);
                }
            }
        }
        _ => {}
    }
}

unsafe fn term_predicate_from_op(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Option<SegmentTermPredicate> {
    let op_expr = expr.cast::<pg_sys::OpExpr>();
    let opname = cstr_to_str(pg_sys::get_opname((*op_expr).opno))?;
    if !matches!(opname, "@>" | "&&" | "?" | "?|" | "?&") || !operator_is_pg_catalog((*op_expr).opno) {
        return None;
    }
    let args = list_node_pointers((*op_expr).args);
    if args.len() != 2 {
        return None;
    }
    // `&&` is commutative; the others only mean what we need with the column on the left.
    let (column, literal) = match var_column(args[0].cast::<pg_sys::Expr>(), catalog) {
        Some(column) => (column, args[1]),
        None if opname == "&&" => (var_column(args[1].cast::<pg_sys::Expr>(), catalog)?, args[0]),
        None => return None,
    };
    let (datum, isnull, literal_type) = super::literals::const_or_param_datum(literal.cast::<pg_sys::Expr>(), params)?;
    if isnull {
        return None;
    }
    let clauses = match (column.pg_type, opname) {
        (koldstore_schema::PgType::TextArray, "@>" | "&&") => {
            // Element equality must be bytewise for a term (an exact string) to stand for it.
            if literal_type != pg_sys::TEXTARRAYOID || !collation_is_deterministic((*op_expr).inputcollid) {
                return None;
            }
            let elements = text_array_datum_elements(datum)?;
            if opname == "@>" {
                koldstore_parquet::array_contains_clauses(&elements)
            } else {
                koldstore_parquet::array_overlap_clauses(&elements)
            }
        }
        (koldstore_schema::PgType::Jsonb, "@>") => {
            if literal_type != pg_sys::JSONBOID {
                return None;
            }
            koldstore_parquet::jsonb_contains_clauses(&datum_output_text(datum, literal_type)?)
        }
        (koldstore_schema::PgType::Jsonb, "?") => {
            if literal_type != pg_sys::TEXTOID {
                return None;
            }
            koldstore_parquet::jsonb_exists_clauses(&[datum_output_text(datum, literal_type)?], false)
        }
        (koldstore_schema::PgType::Jsonb, "?|" | "?&") => {
            if literal_type != pg_sys::TEXTARRAYOID {
                return None;
            }
            let keys = text_array_datum_elements(datum)?;
            // A NULL key never exists; leave such a probe to the scan.
            let keys = keys.into_iter().collect::<Option<Vec<_>>>()?;
            koldstore_parquet::jsonb_exists_clauses(&keys, opname == "?&")
        }
        _ => return None,
    }?;
    Some(SegmentTermPredicate {
        column_id: column.column_id.get(),
        clauses,
    })
}

/// Renders a datum through its type's output function.
unsafe fn datum_output_text(datum: pg_sys::Datum, type_oid: pg_sys::Oid) -> Option<String> {
    let mut typoutput = pg_sys::InvalidOid;
    let mut typisvarlena = false;
    pg_sys::getTypeOutputInfo(type_oid, &mut typoutput, &mut typisvarlena);
    super::literals::cstr_owned_pfree(pg_sys::OidOutputFunctionCall(typoutput, datum))
}

/// The elements of a one-dimensional `text[]` datum; `None` for any other shape.
unsafe fn text_array_datum_elements(datum: pg_sys::Datum) -> Option<Vec<Option<String>>> {
    let array = pg_sys::pg_detoast_datum(datum.cast_mut_ptr()).cast::<pg_sys::ArrayType>();
    if array.is_null() || (*array).ndim > 1 {
        return None;
    }
    let mut typlen: i16 = 0;
    let mut typbyval = false;
    let mut typalign: std::os::raw::c_char = 0;
    pg_sys::get_typlenbyvalalign(pg_sys::TEXTOID, &mut typlen, &mut typbyval, &mut typalign);
    let mut elements: *mut pg_sys::Datum = std::ptr::null_mut();
    let mut nulls: *mut bool = std::ptr::null_mut();
    let mut count: std::os::raw::c_int = 0;
    pg_sys::deconstruct_array(
        array,
        pg_sys::TEXTOID,
        i32::from(typlen),
        typbyval,
        typalign,
        &mut elements,
        &mut nulls,
        &mut count,
    );
    let count = usize::try_from(count).ok()?;
    let mut values = Vec::with_capacity(count);
    for index in 0..count {
        if *nulls.add(index) {
            values.push(None);
        } else {
            values.push(Some(datum_output_text(*elements.add(index), pg_sys::TEXTOID)?));
        }
    }
    Some(values)
}

unsafe fn segment_prune_node_predicates(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Vec<SegmentPrunePredicate> {
    if expr.is_null() {
        return Vec::new();
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_OpExpr => segment_prune_op_expr(expr, catalog, params)
            .into_iter()
            .collect(),
        pg_sys::NodeTag::T_ScalarArrayOpExpr => segment_prune_array_expr(expr, catalog, params)
            .into_iter()
            .collect(),
        pg_sys::NodeTag::T_BoolExpr => {
            let bool_expr = expr.cast::<pg_sys::BoolExpr>();
            if (*bool_expr).boolop == pg_sys::BoolExprType::OR_EXPR {
                return segment_prune_or_equalities(bool_expr, catalog, params);
            }
            if (*bool_expr).boolop != pg_sys::BoolExprType::AND_EXPR {
                return Vec::new();
            }
            list_node_pointers((*bool_expr).args)
                .into_iter()
                .flat_map(|node| {
                    segment_prune_node_predicates(node.cast::<pg_sys::Expr>(), catalog, params)
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

/// `col = ANY(array)` / `col IN (...)` over a constant or bound array.
unsafe fn segment_prune_array_expr(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Option<SegmentPrunePredicate> {
    let saop = expr.cast::<pg_sys::ScalarArrayOpExpr>();
    if !(*saop).useOr {
        return None; // `= ALL(...)` is a conjunction, not a value list
    }
    let opname = cstr_to_str(pg_sys::get_opname((*saop).opno))?;
    if opname != "=" || !operator_is_pg_catalog((*saop).opno) {
        return None;
    }
    let args = list_node_pointers((*saop).args);
    if args.len() != 2 {
        return None;
    }
    let column = var_column(args[0].cast::<pg_sys::Expr>(), catalog)?;
    let sort_type = koldstore_sortkey::SortKeyType::from_type_oid_for_bounds(column.pg_type.type_oid())?;
    if !sort_type.is_order_preserving() && !collation_is_deterministic((*saop).inputcollid) {
        return None;
    }
    let values = array_sort_key_values(args[1].cast::<pg_sys::Expr>(), column, params)?;
    value_list_predicate(column, values)
}

/// `col = a OR col = b OR ...` on one column, and its composite-key form: a disjunction of
/// conjunctions of equalities, `(k1 = a AND k2 = b) OR (k1 = c AND k2 = d)`, which is also what a row
/// value `(k1, k2) IN ((a, b), (c, d))` is planned as.
///
/// Every disjunct has to pin a column for that column to get a value list: a row matching disjunct
/// *i* has `column = value_i`, so it has one of the listed values for each column all disjuncts
/// pin. The result is a per-column superset (it does not tie the values of different columns
/// together), which is all pruning needs. A disjunct that is neither an equality nor a conjunction
/// disables the whole list: it can match outside the listed values.
unsafe fn segment_prune_or_equalities(
    bool_expr: *mut pg_sys::BoolExpr,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Vec<SegmentPrunePredicate> {
    // column id -> one equality value per disjunct (None when a disjunct does not pin the column)
    let mut per_column: Vec<(i16, Vec<Option<koldstore_sortkey::SortKeyValue>>)> = Vec::new();
    let mut disjuncts = 0_usize;
    for node in list_node_pointers((*bool_expr).args) {
        let node = node.cast::<pg_sys::Expr>();
        if node.is_null() {
            return Vec::new();
        }
        let conjuncts: Vec<*mut pg_sys::Expr> = match (*node).type_ {
            pg_sys::NodeTag::T_OpExpr => vec![node],
            pg_sys::NodeTag::T_BoolExpr
                if (*node.cast::<pg_sys::BoolExpr>()).boolop == pg_sys::BoolExprType::AND_EXPR =>
            {
                list_node_pointers((*node.cast::<pg_sys::BoolExpr>()).args)
                    .into_iter()
                    .map(|arg| arg.cast::<pg_sys::Expr>())
                    .collect()
            }
            _ => return Vec::new(), // any other disjunct can match outside the listed values
        };
        let mut pinned: Vec<(i16, koldstore_sortkey::SortKeyValue)> = Vec::new();
        for conjunct in conjuncts {
            if conjunct.is_null() || (*conjunct).type_ != pg_sys::NodeTag::T_OpExpr {
                continue; // an unrecognised conjunct only narrows the disjunct further
            }
            if let Some(predicate) = segment_prune_op_expr(conjunct, catalog, params) {
                if predicate.is_equality() && !pinned.iter().any(|(id, _)| *id == predicate.column_id) {
                    if let Some(value) = predicate.min {
                        pinned.push((predicate.column_id, value));
                    }
                }
            }
        }
        if pinned.is_empty() {
            return Vec::new(); // this disjunct constrains no column: the whole OR is unconstrained
        }
        for (column_id, value) in pinned {
            match per_column.iter_mut().find(|(id, _)| *id == column_id) {
                Some((_, values)) => {
                    values.resize(disjuncts, None);
                    values.push(Some(value));
                }
                None => {
                    let mut values = vec![None; disjuncts];
                    values.push(Some(value));
                    per_column.push((column_id, values));
                }
            }
        }
        disjuncts += 1;
    }
    per_column
        .into_iter()
        .filter_map(|(column_id, mut values)| {
            values.resize(disjuncts, None);
            let values: Option<Vec<_>> = values.into_iter().collect();
            let column = catalog.column_by_attnum(column_id)?;
            value_list_predicate(column, values?)
        })
        .collect()
}

fn value_list_predicate(
    column: &koldstore_migrate::order::CatalogColumn,
    values: Vec<koldstore_sortkey::SortKeyValue>,
) -> Option<SegmentPrunePredicate> {
    let column_id = column.column_id.get();
    SegmentPrunePredicate::any_of(column_id, &column.name, values.clone()).or_else(|| {
        // zero or one distinct value: nothing to union; one value is a plain equality
        let mut distinct = values;
        distinct.dedup();
        (distinct.len() == 1).then(|| SegmentPrunePredicate::equality(column_id, &column.name, distinct.remove(0)))
    })
}

unsafe fn segment_prune_op_expr(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Option<SegmentPrunePredicate> {
    let op_expr = expr.cast::<pg_sys::OpExpr>();
    let opname = cstr_to_str(pg_sys::get_opname((*op_expr).opno))?;
    if !matches!(opname, "=" | "<" | "<=" | ">" | ">=") || !operator_is_pg_catalog((*op_expr).opno)
    {
        return None;
    }
    let args = list_node_pointers((*op_expr).args);
    if args.len() != 2 {
        return None;
    }

    if let Some((column, literal)) = var_and_sort_key_literal(args[0], args[1], catalog, params) {
        if !text_prune_allowed(&literal, opname, (*op_expr).inputcollid) {
            return None;
        }
        return prune_predicate_from_op(column, literal, opname, false);
    }
    if let Some((column, literal)) = var_and_sort_key_literal(args[1], args[0], catalog, params) {
        if !text_prune_allowed(&literal, opname, (*op_expr).inputcollid) {
            return None;
        }
        return prune_predicate_from_op(column, literal, opname, true);
    }
    None
}

/// A text key's segment bounds are UTF-8 byte order, which is not collation order: only equality can
/// prune, and only when equal means byte-equal (a deterministic collation). Every other type is
/// unaffected.
unsafe fn text_prune_allowed(
    literal: &koldstore_sortkey::SortKeyValue,
    opname: &str,
    collation: pg_sys::Oid,
) -> bool {
    literal.sort_key_type().is_order_preserving()
        || (opname == "=" && collation_is_deterministic(collation))
}

/// `true` for the default collation and any collation whose equality is bytewise.
unsafe fn collation_is_deterministic(collation: pg_sys::Oid) -> bool {
    collation == pg_sys::InvalidOid || pg_sys::get_collation_isdeterministic(collation)
}

fn prune_predicate_from_op(
    column: &koldstore_migrate::order::CatalogColumn,
    literal: koldstore_sortkey::SortKeyValue,
    opname: &str,
    reversed: bool,
) -> Option<SegmentPrunePredicate> {
    let column_id = column.column_id.get();
    match (opname, reversed) {
        ("=", _) => Some(SegmentPrunePredicate::equality(
            column_id,
            &column.name,
            literal,
        )),
        (">" | ">=", false) | ("<" | "<=", true) => Some(SegmentPrunePredicate::lower_bound(
            column_id,
            &column.name,
            literal,
        )),
        ("<" | "<=", false) | (">" | ">=", true) => Some(SegmentPrunePredicate::upper_bound(
            column_id,
            &column.name,
            literal,
        )),
        _ => None,
    }
}

unsafe fn var_and_sort_key_literal(
    column_expr: *mut std::ffi::c_void,
    literal_expr: *mut std::ffi::c_void,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Option<(
    &koldstore_migrate::order::CatalogColumn,
    koldstore_sortkey::SortKeyValue,
)> {
    let column = var_column(column_expr.cast::<pg_sys::Expr>(), catalog)?;
    let sort_type = koldstore_sortkey::SortKeyType::from_type_oid_for_bounds(column.pg_type.type_oid())?;
    let literal = literal_sort_key_value(literal_expr.cast::<pg_sys::Expr>(), column, params)?;
    if literal.sort_key_type() != sort_type {
        return None;
    }
    Some((column, literal))
}

unsafe fn collect_residual_filters(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
    filters: &mut ResidualFilters,
) {
    if expr.is_null() {
        return;
    }
    match (*expr).type_ {
        pg_sys::NodeTag::T_OpExpr => {
            if let Some((column, sql_literal)) = op_expr_equality_filter(expr, catalog, params) {
                filters.hot_equality.push(super::hot::HotEqualityFilter {
                    column,
                    sql_literal,
                });
            }
            if let Some(range) = op_expr_range_filter(expr, catalog, params) {
                filters.hot_range.push(range);
            }
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let bool_expr = expr.cast::<pg_sys::BoolExpr>();
            if (*bool_expr).boolop != pg_sys::BoolExprType::AND_EXPR {
                return;
            }
            for arg in list_node_pointers((*bool_expr).args) {
                collect_residual_filters(arg.cast::<pg_sys::Expr>(), catalog, params, filters);
            }
        }
        _ => {}
    }
}

unsafe fn op_expr_equality_filter(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Option<(String, String)> {
    let op_expr = expr.cast::<pg_sys::OpExpr>();
    let opname = cstr_to_str(pg_sys::get_opname((*op_expr).opno))?;
    if opname != "=" || !operator_is_pg_catalog((*op_expr).opno) {
        return None;
    }
    let args = list_node_pointers((*op_expr).args);
    if args.len() != 2 {
        return None;
    }
    let (column, literal) = hot_var_and_literal(args[0], args[1], catalog, params)?;
    Some((column.name.clone(), literal))
}

unsafe fn op_expr_range_filter(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Option<super::hot::HotRangeFilter> {
    let op_expr = expr.cast::<pg_sys::OpExpr>();
    let operator = match cstr_to_str(pg_sys::get_opname((*op_expr).opno))? {
        "<" => super::hot::HotRangeOperator::LessThan,
        "<=" => super::hot::HotRangeOperator::LessThanOrEqual,
        ">" => super::hot::HotRangeOperator::GreaterThan,
        ">=" => super::hot::HotRangeOperator::GreaterThanOrEqual,
        _ => return None,
    };
    if !operator_is_pg_catalog((*op_expr).opno) {
        return None;
    }
    let args = list_node_pointers((*op_expr).args);
    if args.len() != 2 {
        return None;
    }
    if let Some((column, sql_literal)) = hot_var_and_literal(args[0], args[1], catalog, params) {
        return Some(super::hot::HotRangeFilter {
            column: column.name.clone(),
            operator,
            sql_literal,
        });
    }
    let (column, sql_literal) = hot_var_and_literal(args[1], args[0], catalog, params)?;
    Some(super::hot::HotRangeFilter {
        column: column.name.clone(),
        operator: reverse_range_operator(operator),
        sql_literal,
    })
}

const fn reverse_range_operator(
    operator: super::hot::HotRangeOperator,
) -> super::hot::HotRangeOperator {
    match operator {
        super::hot::HotRangeOperator::LessThan => super::hot::HotRangeOperator::GreaterThan,
        super::hot::HotRangeOperator::LessThanOrEqual => {
            super::hot::HotRangeOperator::GreaterThanOrEqual
        }
        super::hot::HotRangeOperator::GreaterThan => super::hot::HotRangeOperator::LessThan,
        super::hot::HotRangeOperator::GreaterThanOrEqual => {
            super::hot::HotRangeOperator::LessThanOrEqual
        }
    }
}

unsafe fn hot_var_and_literal(
    left: *mut std::ffi::c_void,
    right: *mut std::ffi::c_void,
    catalog: QualCatalog<'_>,
    params: pg_sys::ParamListInfo,
) -> Option<(&koldstore_migrate::order::CatalogColumn, String)> {
    if let Some(column) = var_column(left.cast::<pg_sys::Expr>(), catalog) {
        if hot_filter_operand_types_compatible(
            column,
            pg_sys::exprType(right.cast::<pg_sys::Node>()),
        ) {
            if let Some(literal) = typed_literal_sql(right.cast::<pg_sys::Expr>(), column, params) {
                return Some((column, literal));
            }
        }
    }
    if let Some(column) = var_column(right.cast::<pg_sys::Expr>(), catalog) {
        if hot_filter_operand_types_compatible(
            column,
            pg_sys::exprType(left.cast::<pg_sys::Node>()),
        ) {
            if let Some(literal) = typed_literal_sql(left.cast::<pg_sys::Expr>(), column, params) {
                return Some((column, literal));
            }
        }
    }
    None
}

/// Returns true when a Const/Param OID can be reconstructed as a hot SPI
/// predicate against `column`.
///
/// Exact matches are always allowed. Integer width promotions (`int2`/`int4`
/// literal against a wider integer column) are allowed so untyped numeric
/// literals behave like an explicit cast. Float and other cross-types stay
/// rejected because Datum layouts are not interchangeable for SPI literals.
fn hot_filter_operand_types_compatible(
    column: &koldstore_migrate::order::CatalogColumn,
    literal_oid: pg_sys::Oid,
) -> bool {
    let literal_oid = u32::from(literal_oid);
    if literal_oid == column.pg_type.type_oid() {
        return true;
    }
    let Some(literal_ty) = koldstore_schema::PgType::from_integer_oid(literal_oid) else {
        return false;
    };
    column.pg_type.accepts_integer_equality_literal(literal_ty)
}

unsafe fn var_column<'a>(
    expr: *mut pg_sys::Expr,
    catalog: QualCatalog<'a>,
) -> Option<&'a koldstore_migrate::order::CatalogColumn> {
    let expr = unwrap_relabel(expr);
    if expr.is_null() || (*expr).type_ != pg_sys::NodeTag::T_Var {
        return None;
    }
    let var = expr.cast::<pg_sys::Var>();
    let attno = (*var).varattno;
    let scanrelid = i32::try_from(catalog.scanrelid).ok()?;
    if attno <= 0 || (*var).varlevelsup != 0 || (*var).varno != scanrelid {
        return None;
    }
    catalog.column_by_attnum(attno)
}

pub(crate) unsafe fn operator_is_pg_catalog(operator: pg_sys::Oid) -> bool {
    // Windows bindgen may already type this as i32; keep the cast for Linux/macOS.
    #[allow(clippy::unnecessary_cast)]
    let cache_id = pg_sys::SysCacheIdentifier::OPEROID as i32;
    let tuple = pg_sys::SearchSysCache1(cache_id, pg_sys::Datum::from(operator));
    if tuple.is_null() {
        return false;
    }
    let form = pg_sys::GETSTRUCT(tuple).cast::<pg_sys::FormData_pg_operator>();
    let is_pg_catalog = (*form).oprnamespace == pg_sys::PG_CATALOG_NAMESPACE.into();
    pg_sys::ReleaseSysCache(tuple);
    is_pg_catalog
}

fn cstr_to_str(value: *const c_char) -> Option<&'static str> {
    if value.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(value).to_str().ok() }
}

#[cfg(test)]
mod projection_tests {
    #[test]
    fn whole_row_bitmap_offset_matches_postgresql_contract() {
        assert_eq!(-pgrx::pg_sys::FirstLowInvalidHeapAttributeNumber, 7);
    }
}
