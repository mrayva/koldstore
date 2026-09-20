//! Extracts a "WHERE is exactly an equality match on every primary-key
//! column, nothing else" predicate from a planned UPDATE/DELETE, for the
//! cold-DML write guard (`sql::cold_dml::guard`, upstream #122 Option B).
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
//! Correctness note on why "exactly the PK columns, nothing else" is
//! required rather than "PK columns present, extra conditions allowed":
//! this extraction only matters when the native statement already
//! affected zero rows. If the true WHERE clause were `id = 5 AND status =
//! 'x'` and the real reason for zero rows was a hot row's `status`
//! mismatch (not cold-ness), a check that only looked at `id = 5` would
//! find that key via the hot-or-cold merge-scan existence probe and
//! wrongly report it as unsafe. Requiring an exact match on solely the PK
//! columns sidesteps this: if there is truly no other condition, "found
//! via merge-scan" can only mean the key exists and is cold (a hot match
//! would already have been the 0-row statement's result).

use std::collections::HashMap;

use pgrx::pg_sys;

use crate::merge_scan::pg::{literals, qual};

/// Phase 1 (plan-tree walking only, **no SPI**): extracts every
/// `attnum = <literal-or-bound-param>` equality this UPDATE/DELETE's WHERE
/// clause is built from, keyed by raw column `attnum` -- deliberately
/// unaware of which columns are actually the primary key, since resolving
/// that requires a catalog/SPI lookup this phase must not perform (see the
/// call site in `executor.rs`: the plan tree these pointers reference may
/// not survive past `standard_ExecutorEnd`, while SPI must not run
/// *before* it -- this phase runs first, while the pointers are still
/// live, and hands its pointer-free `HashMap<i16, Value>` result to
/// [`resolve_pk_predicate`] afterward).
///
/// Returns `None` when the WHERE clause is not cleanly a single
/// `Var = <literal>` or an `AND`-chain of them (any `OR`, non-equality
/// operator, non-literal operand, or unrecognized plan shape) -- see the
/// module doc comment for why this must be exact rather than best-effort.
#[must_use]
pub(crate) unsafe fn extract_raw_attnum_equality(
    modify_table_plan: *mut pg_sys::Plan,
    params: pg_sys::ParamListInfo,
) -> Option<HashMap<i16, serde_json::Value>> {
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
        // clause that couldn't be pushed into the index (e.g. a non-PK
        // column) lands in the *residual* filter (`.plan.qual`) instead,
        // a completely separate list. Reading only one of the two would
        // miss extra conditions there -- confirmed live: `WHERE id = 30
        // AND name = 'x'` on a genuinely *hot* row put `id = 30` in
        // `indexqualorig` and `name = 'x'` in the residual `qual`, so
        // reading `indexqualorig` alone made a legitimately-0-row
        // (name mismatch) UPDATE look identical to a cold-only key and
        // wrongly rejected it.
        let mut matched: HashMap<i16, serde_json::Value> = HashMap::new();
        for qual_list in qual_lists {
            if qual_list.is_null() {
                continue;
            }
            if !walk_and_equality(qual_list, scanrelid, params, &mut matched) {
                return None;
            }
        }
        Some(matched)
    }
}

/// Phase 2 (SPI-safe, no plan-tree pointers involved): validates that
/// `raw` -- [`extract_raw_attnum_equality`]'s output -- covers *exactly*
/// every primary-key column's `attnum` (from `column_attnums`, filtered to
/// `pk_columns`) and nothing else, and if so builds the `{column: value}`
/// jsonb object [`super::super::cold_pk_exists`] expects.
#[must_use]
pub(crate) fn resolve_pk_predicate(
    raw: &HashMap<i16, serde_json::Value>,
    pk_columns: &[String],
    column_attnums: &HashMap<i16, String>,
) -> Option<serde_json::Value> {
    if pk_columns.is_empty() {
        return None;
    }
    let pk_attnums: HashMap<i16, &str> = column_attnums
        .iter()
        .filter(|(_, name)| pk_columns.iter().any(|pk| pk == *name))
        .map(|(attnum, name)| (*attnum, name.as_str()))
        .collect();
    if pk_attnums.len() != pk_columns.len() || raw.len() != pk_attnums.len() {
        return None;
    }
    let mut object = serde_json::Map::with_capacity(raw.len());
    for (attnum, value) in raw {
        let name = pk_attnums.get(attnum)?;
        object.insert((*name).to_string(), value.clone());
    }
    Some(serde_json::Value::Object(object))
}

/// Locates every qual list a simple point-lookup UPDATE/DELETE subplan's
/// WHERE clause could be split across, for the two physical scan shapes a
/// PK-equality lookup realistically produces. Anything else
/// (`BitmapHeapScan`, joins, partition-routed `ModifyTable`, ...) returns
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
            _ => None,
        }
    }
}

/// Walks `qual` (a `List` of ANDed quals) plus any nested `AND`-only
/// `BoolExpr`, requiring every leaf to be a `Var = <literal>` equality on
/// this scan's own relation. Returns `false` the moment anything else is
/// seen (OR, a non-equality operator, a non-literal operand, ...) -- which
/// column each `Var` is belongs to a PK is validated later, in
/// [`resolve_pk_predicate`], once a catalog lookup can safely happen.
unsafe fn walk_and_equality(
    qual: *mut pg_sys::List,
    scanrelid: pg_sys::Index,
    params: pg_sys::ParamListInfo,
    matched: &mut HashMap<i16, serde_json::Value>,
) -> bool {
    unsafe {
        literals::list_node_pointers(qual)
            .into_iter()
            .all(|node| walk_and_equality_node(node.cast::<pg_sys::Expr>(), scanrelid, params, matched))
    }
}

unsafe fn walk_and_equality_node(
    expr: *mut pg_sys::Expr,
    scanrelid: pg_sys::Index,
    params: pg_sys::ParamListInfo,
    matched: &mut HashMap<i16, serde_json::Value>,
) -> bool {
    unsafe {
        if expr.is_null() {
            return false;
        }
        match (*expr).type_ {
            pg_sys::NodeTag::T_OpExpr => equality_predicate(expr, scanrelid, params, matched),
            pg_sys::NodeTag::T_BoolExpr => {
                let bool_expr = expr.cast::<pg_sys::BoolExpr>();
                if (*bool_expr).boolop != pg_sys::BoolExprType::AND_EXPR {
                    return false;
                }
                literals::list_node_pointers((*bool_expr).args)
                    .into_iter()
                    .all(|node| walk_and_equality_node(node.cast::<pg_sys::Expr>(), scanrelid, params, matched))
            }
            _ => false,
        }
    }
}

unsafe fn equality_predicate(
    expr: *mut pg_sys::Expr,
    scanrelid: pg_sys::Index,
    params: pg_sys::ParamListInfo,
    matched: &mut HashMap<i16, serde_json::Value>,
) -> bool {
    unsafe {
        let op_expr = expr.cast::<pg_sys::OpExpr>();
        if !qual::operator_is_pg_catalog((*op_expr).opno) || !is_equality_operator((*op_expr).opno) {
            return false;
        }
        let args = literals::list_node_pointers((*op_expr).args);
        if args.len() != 2 {
            return false;
        }
        let (left, right) = (args[0].cast::<pg_sys::Expr>(), args[1].cast::<pg_sys::Expr>());

        record_if_column_match(left, right, scanrelid, params, matched)
            || record_if_column_match(right, left, scanrelid, params, matched)
    }
}

/// If `column_side` is a bare `Var` on `scanrelid` and `value_side` is a
/// literal-or-bound-param, records `attnum -> value` into `matched` and
/// returns `true`. A partial match (a `Var` paired with a non-literal, or
/// a NULL literal) is treated as a failure, not a skip -- see the module
/// doc comment on why "no extra/ambiguous conditions" must hold exactly.
unsafe fn record_if_column_match(
    column_side: *mut pg_sys::Expr,
    value_side: *mut pg_sys::Expr,
    scanrelid: pg_sys::Index,
    params: pg_sys::ParamListInfo,
    matched: &mut HashMap<i16, serde_json::Value>,
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
        matched.insert(attnum, value).is_none()
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

unsafe fn is_equality_operator(operator: pg_sys::Oid) -> bool {
    unsafe {
        let name_ptr = pg_sys::get_opname(operator);
        if name_ptr.is_null() {
            return false;
        }
        std::ffi::CStr::from_ptr(name_ptr).to_str() == Ok("=")
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
