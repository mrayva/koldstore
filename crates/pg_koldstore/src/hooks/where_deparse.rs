//! Turns the WHERE clause of a planned UPDATE/DELETE back into SQL text, for
//! the generic cold-match probe of the cold-DML write guard (upstream #122).
//!
//! `pk_predicate` only understands a handful of shapes (an `AND`-chain of
//! `column OP literal`). Everything else -- `pk > 5`, `NOT IN`, an `OR` across
//! columns, `LIKE`, function calls, a missing WHERE -- used to be skipped and
//! could silently leave cold rows unmodified. Deparsing the scan's own quals
//! with PostgreSQL's ruleutils covers every shape at once; the caller counts
//! how many cold rows that text matches (see `hooks::executor`).
//!
//! Runs during `ExecutorEnd` phase 1 (plan-tree pointers live, no SPI) and
//! returns `None` for anything it cannot re-evaluate faithfully in a separate
//! query: joins, subqueries, volatile functions, `CURRENT OF`, parameters that
//! are not plain external parameters.

use std::ffi::{CStr, c_void};

use pgrx::pg_sys;

use crate::merge_scan::pg::literals;

struct Context {
    scanrelid: pg_sys::Index,
    params: pg_sys::ParamListInfo,
    ok: bool,
}

/// Replaces external parameters by constants and renumbers the scan
/// relation's `Var`s to range-table index 1 (the only relation the deparse
/// context knows); flags anything that cannot be re-run standalone.
unsafe extern "C-unwind" fn mutator(node: *mut pg_sys::Node, context: *mut c_void) -> *mut pg_sys::Node {
    unsafe {
        if node.is_null() {
            return std::ptr::null_mut();
        }
        let ctx = &mut *context.cast::<Context>();
        match (*node).type_ {
            pg_sys::NodeTag::T_Var => {
                let var = node.cast::<pg_sys::Var>();
                if (*var).varno as u32 != ctx.scanrelid || (*var).varlevelsup != 0 {
                    ctx.ok = false;
                    return node;
                }
                let copy = pg_sys::copyObjectImpl(node.cast()).cast::<pg_sys::Var>();
                (*copy).varno = 1;
                (*copy).varnosyn = 1;
                copy.cast()
            }
            pg_sys::NodeTag::T_Param => match extern_param_const(node.cast::<pg_sys::Param>(), ctx.params) {
                Some(constant) => constant,
                None => {
                    ctx.ok = false;
                    node
                }
            },
            pg_sys::NodeTag::T_SubPlan
            | pg_sys::NodeTag::T_AlternativeSubPlan
            | pg_sys::NodeTag::T_SubLink
            | pg_sys::NodeTag::T_CurrentOfExpr
            | pg_sys::NodeTag::T_PlaceHolderVar
            | pg_sys::NodeTag::T_Aggref
            | pg_sys::NodeTag::T_WindowFunc
            | pg_sys::NodeTag::T_GroupingFunc => {
                ctx.ok = false;
                node
            }
            _ => pg_sys::expression_tree_mutator_impl(node, Some(mutator), context),
        }
    }
}

unsafe fn extern_param_const(param: *mut pg_sys::Param, params: pg_sys::ParamListInfo) -> Option<*mut pg_sys::Node> {
    unsafe {
        if (*param).paramkind != pg_sys::ParamKind::PARAM_EXTERN || params.is_null() {
            return None;
        }
        let id = (*param).paramid;
        let mut workspace: pg_sys::ParamExternData = std::mem::zeroed();
        let prm = if let Some(fetch) = (*params).paramFetch {
            fetch(params, id, true, &mut workspace)
        } else {
            if id < 1 || id > (*params).numParams {
                return None;
            }
            (*params).params.as_mut_ptr().add((id - 1) as usize)
        };
        if prm.is_null() || (*prm).ptype == pg_sys::InvalidOid {
            return None;
        }
        let mut len: i16 = 0;
        let mut by_value = false;
        pg_sys::get_typlenbyval((*prm).ptype, &mut len, &mut by_value);
        Some(
            pg_sys::makeConst(
                (*prm).ptype,
                (*param).paramtypmod,
                (*param).paramcollid,
                i32::from(len),
                (*prm).value,
                (*prm).isnull,
                by_value,
            )
            .cast(),
        )
    }
}

/// Returns the WHERE clause of the UPDATE/DELETE whose `ModifyTable` plan is
/// `modify_table_plan`, as SQL text over the target table aliased `t`, or
/// `None` when it cannot be reproduced standalone. An unconditional statement
/// yields `"true"`.
#[must_use]
pub(crate) unsafe fn deparse_where(
    modify_table_plan: *mut pg_sys::Plan,
    params: pg_sys::ParamListInfo,
    table_oid: pg_sys::Oid,
) -> Option<String> {
    unsafe {
        if modify_table_plan.is_null() {
            return None;
        }
        let subplan = (*modify_table_plan).lefttree;
        if subplan.is_null() {
            return None;
        }
        let (scanrelid, qual_lists) = super::pk_predicate::scan_qual_sources(subplan)?;
        deparse_scan_quals(scanrelid, &qual_lists, params, table_oid)
    }
}

unsafe fn deparse_scan_quals(
    scanrelid: pg_sys::Index,
    qual_lists: &[*mut pg_sys::List],
    params: pg_sys::ParamListInfo,
    table_oid: pg_sys::Oid,
) -> Option<String> {
    unsafe {
        let dpcontext = pg_sys::deparse_context_for(c"t".as_ptr(), table_oid);
        let mut parts: Vec<String> = Vec::new();
        for qual_list in qual_lists {
            for node in literals::list_node_pointers(*qual_list) {
                let node = node.cast::<pg_sys::Node>();
                if node.is_null() || pg_sys::contain_volatile_functions(node) {
                    return None;
                }
                let mut ctx = Context {
                    scanrelid,
                    params,
                    ok: true,
                };
                let rewritten = mutator(node, std::ptr::from_mut(&mut ctx).cast());
                if !ctx.ok || rewritten.is_null() {
                    return None;
                }
                let text = pg_sys::deparse_expression(rewritten, dpcontext, true, true);
                if text.is_null() {
                    return None;
                }
                parts.push(format!("({})", CStr::from_ptr(text).to_string_lossy()));
            }
        }
        Some(if parts.is_empty() { "true".to_string() } else { parts.join(" AND ") })
    }
}

/// What a partitioned/inherited UPDATE/DELETE does to one specific leaf (ADR-008).
pub(crate) enum LeafWhere {
    /// The plan has no scan of this leaf at all: plan-time partition pruning proved the
    /// WHERE clause cannot match any of its rows, so the statement cannot touch it.
    Absent,
    /// The leaf's own scan filter, as SQL over the leaf aliased `t`.
    Sql(String),
    /// The plan's shape (a join, a sub-query, a volatile qual, ...) cannot be attributed to
    /// this leaf; the caller must fail closed.
    Unknown,
}

/// Collects every base-relation scan under `plan`, looking only through `Append` and
/// `MergeAppend`. Returns `false` on any other node type, so an unrecognised shape is never
/// mistaken for "this leaf is not scanned".
unsafe fn collect_leaf_scans(plan: *mut pg_sys::Plan, out: &mut Vec<*mut pg_sys::Plan>) -> bool {
    unsafe {
        if plan.is_null() {
            return false;
        }
        match (*plan).type_ {
            pg_sys::NodeTag::T_Append => {
                let append = plan.cast::<pg_sys::Append>();
                literals::list_node_pointers((*append).appendplans)
                    .into_iter()
                    .all(|child| collect_leaf_scans(child.cast::<pg_sys::Plan>(), out))
            }
            pg_sys::NodeTag::T_MergeAppend => {
                let merge = plan.cast::<pg_sys::MergeAppend>();
                literals::list_node_pointers((*merge).mergeplans)
                    .into_iter()
                    .all(|child| collect_leaf_scans(child.cast::<pg_sys::Plan>(), out))
            }
            pg_sys::NodeTag::T_SeqScan | pg_sys::NodeTag::T_IndexScan | pg_sys::NodeTag::T_BitmapHeapScan => {
                out.push(plan);
                true
            }
            _ => false,
        }
    }
}

/// The part of a partitioned/inherited UPDATE/DELETE's WHERE clause that applies to one
/// result relation `leaf_oid`. PostgreSQL gives `ModifyTable` a single child plan -- an
/// `Append` of one filtered scan per leaf -- and lists every leaf in `resultRelations`, so the
/// leaf's own scan is the only place its share of the filter lives. `rtable` maps each scan's
/// `scanrelid` back to the relation it reads.
#[must_use]
pub(crate) unsafe fn deparse_where_for_leaf(
    modify_table_plan: *mut pg_sys::Plan,
    rtable: *mut pg_sys::List,
    params: pg_sys::ParamListInfo,
    leaf_oid: pg_sys::Oid,
) -> LeafWhere {
    unsafe {
        if modify_table_plan.is_null() || rtable.is_null() {
            return LeafWhere::Unknown;
        }
        let mut scans = Vec::new();
        if !collect_leaf_scans((*modify_table_plan).lefttree, &mut scans) {
            return LeafWhere::Unknown;
        }
        let mut found = None;
        for scan in scans {
            let Some((scanrelid, qual_lists)) = super::pk_predicate::scan_qual_sources(scan) else {
                return LeafWhere::Unknown;
            };
            if scanrelid == 0 || scanrelid as i32 > (*rtable).length {
                return LeafWhere::Unknown;
            }
            let rte = (*(*rtable).elements.add(scanrelid as usize - 1))
                .ptr_value
                .cast::<pg_sys::RangeTblEntry>();
            if rte.is_null() || (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
                return LeafWhere::Unknown;
            }
            if (*rte).relid != leaf_oid {
                continue;
            }
            if found.is_some() {
                return LeafWhere::Unknown;
            }
            found = Some((scanrelid, qual_lists));
        }
        match found {
            None => LeafWhere::Absent,
            Some((scanrelid, qual_lists)) => match deparse_scan_quals(scanrelid, &qual_lists, params, leaf_oid) {
                Some(sql) => LeafWhere::Sql(sql),
                None => LeafWhere::Unknown,
            },
        }
    }
}
