//! Planner hook that prepares the cold-match probe for `UPDATE`/`DELETE`
//! statements a plain plan-qual deparse cannot reproduce: joins (`FROM`,
//! `USING`) and sub-queries (`IN (SELECT ...)`, `EXISTS`, ...), upstream #122.
//! `MERGE` always needs this probe (it is inherently a join of target and
//! source; there is no single-table shape for it to fall back to).
//!
//! A plain UPDATE/DELETE only sees the hot heap, so a cold-only row that the
//! statement's join would have matched is silently skipped. Detecting that needs
//! the statement's own conditions, which only the parse tree still has intact,
//! so this hook -- before planning consumes the tree -- turns the statement into
//! the equivalent `SELECT <target primary key> ...` over the same
//! `FROM`/`WHERE`, deparses it to SQL, and carries the text on the plan (a
//! marker constant in the `ModifyTable` node's `qual` list, which the executor
//! never evaluates) so it survives plan caching and reaches `ExecutorEnd`, where
//! `hooks::executor` counts the cold-only rows it matches.
//!
//! `MERGE`'s own parse tree is a special case worth spelling out, confirmed against
//! the PostgreSQL 18 source (`parse_merge.c`/`prepjointree.c`): `Query.mergeJoinCondition`
//! holds the `ON` clause as a *separate* field, not folded into `Query.jointree`'s quals
//! (which stay `NULL`), and `Query.jointree.fromlist` at this point holds only the
//! *source* relation -- the target is deliberately left out of the join at parse time
//! ("the join will be constructed fully by `transform_MERGE_to_join`", per that file's own
//! comment), and that transform runs later, inside `standard_planner` itself, which this
//! hook calls *after* building the probe. So the probe must add the target relation to a
//! cloned `jointree.fromlist` itself and use `mergeJoinCondition` as the new `WHERE`,
//! rather than just reusing the existing jointree the way the `UPDATE`/`DELETE` probe does.
//! A plain comma-join (`FROM target, source WHERE <mergeJoinCondition>`) is sufficient for
//! the probe's purpose -- finding target rows an action could touch -- even though the real
//! executed `MERGE` may need PostgreSQL's own more elaborate outer join to additionally
//! process `WHEN NOT MATCHED` cases, which the probe does not care about: hydrating a target
//! row the `ON` condition matches is correct regardless of which `WHEN` clause (if any) ends
//! up acting on it, and an insert-only `MERGE` (no `WHEN MATCHED`/`NOT MATCHED BY SOURCE`
//! action that changes an existing row) gets no probe at all, since it never touches one.
//!
//! Anything that cannot be re-run faithfully (volatile functions, CTEs,
//! `CURRENT OF`, no primary key) simply gets no probe: never a false rejection.

use std::ffi::{c_int, CStr, CString};

use pgrx::{pg_sys, PgBox};

/// First bytes of the marker constant carrying the probe SQL.
pub(crate) const PROBE_MARKER: &str = "/*koldstore:cold-match*/";

static mut PREVIOUS_PLANNER_HOOK: pg_sys::planner_hook_type = None;

pub(crate) fn register() {
    // SAFETY: called once from `_PG_init` while single-threaded.
    unsafe {
        PREVIOUS_PLANNER_HOOK = pg_sys::planner_hook;
        pg_sys::planner_hook = Some(planner);
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn planner(
    parse: *mut pg_sys::Query,
    query_string: *const std::ffi::c_char,
    cursor_options: c_int,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    unsafe {
        // Must run before planning: the planner rewrites the tree in place.
        let probe = if crate::guc::guard_scan_writes() || crate::guc::hydrate_on_write() {
            build_probe_sql(parse)
        } else {
            None
        };
        let planned = match PREVIOUS_PLANNER_HOOK {
            Some(previous) => previous(parse, query_string, cursor_options, bound_params),
            None => pg_sys::standard_planner(parse, query_string, cursor_options, bound_params),
        };
        if let Some(sql) = probe {
            attach_probe(planned, &sql);
        }
        planned
    }
}

/// The probe SQL for `parse`, when it is a join/sub-query `UPDATE`/`DELETE`, or any
/// row-changing `MERGE`, on a managed table whose statement can be re-run as a `SELECT`.
unsafe fn build_probe_sql(parse: *mut pg_sys::Query) -> Option<String> {
    unsafe {
        if parse.is_null() || !crate::catalog::cache::managed_catalog_ready() {
            return None;
        }
        match (*parse).commandType {
            pg_sys::CmdType::CMD_UPDATE | pg_sys::CmdType::CMD_DELETE => build_update_delete_probe_sql(parse),
            pg_sys::CmdType::CMD_MERGE => build_merge_probe_sql(parse),
            _ => None,
        }
    }
}

/// The probe SQL for a join/sub-query `UPDATE`/`DELETE` (see `build_probe_sql`).
unsafe fn build_update_delete_probe_sql(parse: *mut pg_sys::Query) -> Option<String> {
    unsafe {
        let query = &*parse;
        if query.resultRelation <= 0 || query.jointree.is_null() {
            return None;
        }
        // Only shapes the single-table deparse could not handle.
        let rtable_len = crate::merge_scan::pg::literals::list_node_pointers(query.rtable).len();
        let from_len = crate::merge_scan::pg::literals::list_node_pointers((*query.jointree).fromlist).len();
        if !query.hasSubLinks && from_len <= 1 && rtable_len <= 1 {
            return None;
        }
        if !query.cteList.is_null()
            || query.hasModifyingCTE
            || query.hasAggs
            || query.hasWindowFuncs
            || query.hasTargetSRFs
            || !query.onConflict.is_null()
        {
            return None;
        }
        let rte_index = usize::try_from(query.resultRelation).ok()?;
        let rte = crate::merge_scan::pg::literals::list_node_pointers(query.rtable)
            .get(rte_index - 1)
            .copied()?
            .cast::<pg_sys::RangeTblEntry>();
        if rte.is_null() || (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
            return None;
        }
        let table_oid = (*rte).relid;
        let snapshot = crate::merge_scan::pg::with_hook_disabled(|| {
            (crate::catalog::cache::is_managed_relation(table_oid))
                .then(|| crate::catalog::cache::managed_table_snapshot(table_oid).ok().flatten())
                .flatten()
        })?;
        let pk_attnums: Vec<i16> = snapshot.primary_key_columns.iter().map(|column| column.column_id.get()).collect();
        if pk_attnums.is_empty() {
            return None;
        }
        // Volatile functions and WHERE CURRENT OF cannot be re-evaluated.
        if pg_sys::contain_volatile_functions(query.jointree.cast()) {
            return None;
        }
        if query_mentions_current_of(query.jointree) {
            return None;
        }

        let select = pg_sys::copyObjectImpl(parse.cast()).cast::<pg_sys::Query>();
        let mut target_list: *mut pg_sys::List = std::ptr::null_mut();
        for (index, attnum) in pk_attnums.iter().enumerate() {
            let mut type_oid = pg_sys::InvalidOid;
            let mut typmod: i32 = -1;
            let mut collation = pg_sys::InvalidOid;
            pg_sys::get_atttypetypmodcoll(table_oid, *attnum, &mut type_oid, &mut typmod, &mut collation);
            if type_oid == pg_sys::InvalidOid {
                return None;
            }
            let var = pg_sys::makeVar(query.resultRelation, *attnum, type_oid, typmod, collation, 0);
            let resno = i16::try_from(index + 1).ok()?;
            target_list = pg_sys::lappend(
                target_list,
                pg_sys::makeTargetEntry(var.cast(), resno, std::ptr::null_mut(), false).cast(),
            );
        }
        // The DML target's range-table entry is not "in the FROM clause" for a
        // DML statement; the deparser skips such entries, so mark it.
        let select_rte = crate::merge_scan::pg::literals::list_node_pointers((*select).rtable)
            .get(rte_index - 1)
            .copied()?
            .cast::<pg_sys::RangeTblEntry>();
        (*select_rte).inFromCl = true;
        (*select).commandType = pg_sys::CmdType::CMD_SELECT;
        (*select).resultRelation = 0;
        (*select).targetList = target_list;
        (*select).returningList = std::ptr::null_mut();
        (*select).mergeActionList = std::ptr::null_mut();
        (*select).withCheckOptions = std::ptr::null_mut();
        (*select).rowMarks = std::ptr::null_mut();
        (*select).canSetTag = true;
        (*select).hasTargetSRFs = false;

        let text = pg_sys::pg_get_querydef(select, false);
        if text.is_null() {
            return None;
        }
        let inner = CStr::from_ptr(text).to_string_lossy().into_owned();
        Some(inner)
    }
}

/// The probe SQL for a `MERGE` whose target's own `ModifyTable` is on a managed table (see
/// `build_probe_sql` and the module doc comment for why this needs its own construction,
/// distinct from the `UPDATE`/`DELETE` path above).
unsafe fn build_merge_probe_sql(parse: *mut pg_sys::Query) -> Option<String> {
    unsafe {
        let query = &*parse;
        if query.resultRelation <= 0 || query.jointree.is_null() || query.mergeJoinCondition.is_null() {
            return None;
        }
        if !query.cteList.is_null() || query.hasModifyingCTE {
            return None;
        }
        // An insert-only MERGE (every action is WHEN NOT MATCHED THEN INSERT, or DO NOTHING)
        // never touches an existing target row, so there is nothing to hydrate -- and nothing
        // for this probe to usefully find either.
        let changes_rows = crate::merge_scan::pg::literals::list_node_pointers(query.mergeActionList)
            .into_iter()
            .any(|action| {
                let action = action.cast::<pg_sys::MergeAction>();
                !action.is_null()
                    && matches!((*action).commandType, pg_sys::CmdType::CMD_UPDATE | pg_sys::CmdType::CMD_DELETE)
            });
        if !changes_rows {
            return None;
        }
        let rte_index = usize::try_from(query.resultRelation).ok()?;
        let rte = crate::merge_scan::pg::literals::list_node_pointers(query.rtable)
            .get(rte_index - 1)
            .copied()?
            .cast::<pg_sys::RangeTblEntry>();
        if rte.is_null() || (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
            return None;
        }
        let table_oid = (*rte).relid;
        let snapshot = crate::merge_scan::pg::with_hook_disabled(|| {
            (crate::catalog::cache::is_managed_relation(table_oid))
                .then(|| crate::catalog::cache::managed_table_snapshot(table_oid).ok().flatten())
                .flatten()
        })?;
        let pk_attnums: Vec<i16> = snapshot.primary_key_columns.iter().map(|column| column.column_id.get()).collect();
        if pk_attnums.is_empty() {
            return None;
        }
        // Volatile functions cannot be re-evaluated; MERGE has no WHERE CURRENT OF to worry about.
        if pg_sys::contain_volatile_functions(query.mergeJoinCondition)
            || pg_sys::contain_volatile_functions(query.jointree.cast())
        {
            return None;
        }

        let select = pg_sys::copyObjectImpl(parse.cast()).cast::<pg_sys::Query>();
        let mut target_list: *mut pg_sys::List = std::ptr::null_mut();
        for (index, attnum) in pk_attnums.iter().enumerate() {
            let mut type_oid = pg_sys::InvalidOid;
            let mut typmod: i32 = -1;
            let mut collation = pg_sys::InvalidOid;
            pg_sys::get_atttypetypmodcoll(table_oid, *attnum, &mut type_oid, &mut typmod, &mut collation);
            if type_oid == pg_sys::InvalidOid {
                return None;
            }
            let var = pg_sys::makeVar(query.resultRelation, *attnum, type_oid, typmod, collation, 0);
            let resno = i16::try_from(index + 1).ok()?;
            target_list = pg_sys::lappend(
                target_list,
                pg_sys::makeTargetEntry(var.cast(), resno, std::ptr::null_mut(), false).cast(),
            );
        }

        // The target relation is deliberately absent from `jointree.fromlist` at this point
        // (see the module doc comment); add it, and reuse the `ON` clause as the new `WHERE` --
        // a plain comma-join is enough for the probe's purpose.
        let select_jointree = (*select).jointree;
        let mut target_ref = PgBox::<pg_sys::RangeTblRef>::alloc_node(pg_sys::NodeTag::T_RangeTblRef);
        target_ref.rtindex = query.resultRelation;
        (*select_jointree).fromlist = pg_sys::lappend((*select_jointree).fromlist, target_ref.into_pg().cast());
        (*select_jointree).quals = (*select).mergeJoinCondition;

        // The target's range-table entry is not "in the FROM clause" for a MERGE either; mark
        // it the same way the UPDATE/DELETE path does.
        let select_rte = crate::merge_scan::pg::literals::list_node_pointers((*select).rtable)
            .get(rte_index - 1)
            .copied()?
            .cast::<pg_sys::RangeTblEntry>();
        (*select_rte).inFromCl = true;
        (*select).commandType = pg_sys::CmdType::CMD_SELECT;
        (*select).resultRelation = 0;
        (*select).mergeTargetRelation = 0;
        (*select).mergeJoinCondition = std::ptr::null_mut();
        (*select).mergeActionList = std::ptr::null_mut();
        (*select).targetList = target_list;
        (*select).returningList = std::ptr::null_mut();
        (*select).withCheckOptions = std::ptr::null_mut();
        (*select).rowMarks = std::ptr::null_mut();
        (*select).canSetTag = true;
        (*select).hasTargetSRFs = false;

        let text = pg_sys::pg_get_querydef(select, false);
        if text.is_null() {
            return None;
        }
        let inner = CStr::from_ptr(text).to_string_lossy().into_owned();
        Some(inner)
    }
}

/// True when the tree contains a `WHERE CURRENT OF` (a cursor position, not a
/// condition that can be evaluated again).
unsafe fn query_mentions_current_of(node: *mut pg_sys::FromExpr) -> bool {
    unsafe extern "C-unwind" fn walker(node: *mut pg_sys::Node, found: *mut std::ffi::c_void) -> bool {
        unsafe {
            if node.is_null() {
                return false;
            }
            if (*node).type_ == pg_sys::NodeTag::T_CurrentOfExpr {
                *found.cast::<bool>() = true;
                return true;
            }
            pg_sys::expression_tree_walker_impl(node, Some(walker), found)
        }
    }
    let mut found = false;
    unsafe {
        walker(node.cast(), std::ptr::from_mut(&mut found).cast());
    }
    found
}

/// Appends the marker constant carrying `sql` to the `ModifyTable` node's
/// `qual` list (ignored by the executor, copied with the plan).
unsafe fn attach_probe(planned: *mut pg_sys::PlannedStmt, sql: &str) {
    unsafe {
        if planned.is_null() || (*planned).planTree.is_null() {
            return;
        }
        let plan = (*planned).planTree;
        if (*plan).type_ != pg_sys::NodeTag::T_ModifyTable {
            return;
        }
        let Ok(text) = CString::new(format!("{PROBE_MARKER}{sql}")) else {
            return;
        };
        let datum = pg_sys::Datum::from(pg_sys::cstring_to_text(text.as_ptr()));
        let constant = pg_sys::makeConst(
            pg_sys::TEXTOID,
            -1,
            pg_sys::DEFAULT_COLLATION_OID,
            -1,
            datum,
            false,
            false,
        );
        (*plan).qual = pg_sys::lappend((*plan).qual, constant.cast());
    }
}

/// The probe SQL carried by a planned `ModifyTable`, if any.
pub(crate) unsafe fn probe_sql_of(plan: *mut pg_sys::Plan) -> Option<String> {
    unsafe {
        if plan.is_null() || (*plan).type_ != pg_sys::NodeTag::T_ModifyTable {
            return None;
        }
        for node in crate::merge_scan::pg::literals::list_node_pointers((*plan).qual) {
            let node = node.cast::<pg_sys::Node>();
            if node.is_null() || (*node).type_ != pg_sys::NodeTag::T_Const {
                continue;
            }
            let constant = node.cast::<pg_sys::Const>();
            if (*constant).consttype != pg_sys::TEXTOID || (*constant).constisnull {
                continue;
            }
            let text = pg_sys::text_to_cstring((*constant).constvalue.cast_mut_ptr());
            let value = CStr::from_ptr(text).to_string_lossy().into_owned();
            if let Some(sql) = value.strip_prefix(PROBE_MARKER) {
                return Some(sql.to_string());
            }
        }
        None
    }
}

/// One bound parameter of the statement, copied so it outlives the executor.
pub(crate) struct ProbeParam {
    pub type_oid: pg_sys::Oid,
    pub value: pg_sys::Datum,
    pub is_null: bool,
}

/// Copies every external parameter of the statement, or `None` when one cannot
/// be resolved (the probe would then not run with the right values).
pub(crate) unsafe fn collect_params(params: pg_sys::ParamListInfo) -> Option<Vec<ProbeParam>> {
    unsafe {
        if params.is_null() {
            return Some(Vec::new());
        }
        let count = (*params).numParams;
        let mut collected = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
        for id in 1..=count {
            let mut workspace: pg_sys::ParamExternData = std::mem::zeroed();
            let prm = if let Some(fetch) = (*params).paramFetch {
                fetch(params, id, true, &mut workspace)
            } else {
                (*params).params.as_mut_ptr().add((id - 1) as usize)
            };
            if prm.is_null() || (*prm).ptype == pg_sys::InvalidOid {
                return None;
            }
            let mut len: i16 = 0;
            let mut by_value = false;
            pg_sys::get_typlenbyval((*prm).ptype, &mut len, &mut by_value);
            let value = if (*prm).isnull { pg_sys::Datum::from(0usize) } else { pg_sys::datumCopy((*prm).value, by_value, i32::from(len)) };
            collected.push(ProbeParam { type_oid: (*prm).ptype, value, is_null: (*prm).isnull });
        }
        Some(collected)
    }
}
