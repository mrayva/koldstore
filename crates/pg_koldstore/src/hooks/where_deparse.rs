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
        let dpcontext = pg_sys::deparse_context_for(c"t".as_ptr(), table_oid);
        let mut parts: Vec<String> = Vec::new();
        for qual_list in qual_lists {
            for node in literals::list_node_pointers(qual_list) {
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
