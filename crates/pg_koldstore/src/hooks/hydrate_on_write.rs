//! EXPERIMENTAL hydrate-on-write (ADR-007, option B): lets a plain single-table
//! `UPDATE`/`DELETE`, or a row-changing `MERGE`, change cold-only rows.
//!
//! An `ExecutorStart` hook, before the statement's scan begins, finds the cold-only
//! rows its `WHERE` clause matches (merged hot+cold view minus heap-only view),
//! inserts them into the heap with an ordinary `INSERT` (so capture, seq
//! assignment and flush behave as for any insert -- no second mirror writer), and
//! advances the statement snapshot's command id so the scan that follows sees them.
//! The native statement then runs unchanged, including triggers, foreign keys,
//! row-level security and `RETURNING`.
//!
//! Isolation levels: own-transaction visibility (`cmin`/`curcid`) is independent of
//! isolation level in PostgreSQL -- only the *other-transactions* snapshot (xmin/xmax)
//! is frozen for `REPEATABLE READ`/`SERIALIZABLE`, so forcing `curcid` forward on the
//! statement's own snapshot to reveal a just-hydrated row works the same way under all
//! three levels (confirmed live: same-statement visibility, cross-statement consistency
//! within one transaction, an already-declared cursor's own portal snapshot unaffected,
//! and normal first-committer-wins conflict detection on a concurrently hydrated row).
//! `SERIALIZABLE` has one additional wrinkle: the probe that decides which rows to
//! hydrate reads cold data without taking SSI predicate locks (same reason
//! `koldstore.reject_serializable_cold_reads` exists for ordinary reads, upstream #125).
//! So `SERIALIZABLE` only hydrates when that GUC is off -- the same accepted-weaker-
//! guarantee the GUC already grants ordinary reads, not a new risk; left on (the
//! default), a `SERIALIZABLE` statement here just falls through to the write guards,
//! same as before.
//!
//! Other limits of the prototype: single-table statements whose `WHERE` clause
//! `hooks::where_deparse` can reproduce (joins and sub-queries go through the planner
//! hook's probe, see `hooks::dml_planner`), and a transaction that has not already
//! written the table. Everything else keeps falling through to the write guards, which
//! reject it. The table job lock is not held by default (the hydrated row is
//! uncommitted, so a concurrent flush cannot see or prune it); `koldstore.hydrate_take_job_lock`
//! turns that on, with a bounded wait.
//!
//! A data-modifying CTE's own `UPDATE`/`DELETE` (`WITH d AS (DELETE ... RETURNING ...)
//! SELECT ...`) is hydrated too: the top-level statement's own `CmdType` is irrelevant --
//! `set_rel_pathlist` et al. already treat the CTE's `ModifyTable` node as its own
//! statement, buried in `PlannedStmt.subplans` rather than `planTree` -- and the SAME
//! statement-wide snapshot/command-id mechanics apply regardless of which node in the plan
//! tree does the hydrating insert, since there is exactly one snapshot for the whole
//! statement. Single-table only, same as the top level: a CTE whose own `WHERE` joins or
//! sub-queries never gets the planner hook's probe either (that hook only sees the
//! outermost statement, and declines outright on `hasModifyingCTE`/`cteList`), so it falls
//! through to the write guards unchanged, exactly like an unreproducible top-level clause
//! does.
//!
//! `MERGE` is hydrated too, through the SAME planner-hook probe mechanism (`hooks::dml_planner`
//! builds a MERGE-specific probe: a `SELECT` of the target's primary keys joined to the
//! `USING` source on the `ON` clause, since MERGE is inherently a join -- there is no
//! single-table shape for it to fall back to). Only when some `WHEN MATCHED`/`WHEN NOT
//! MATCHED BY SOURCE` action actually changes an existing row (`UPDATE`/`DELETE`, not
//! `INSERT`/`DO NOTHING`): an insert-only MERGE never touches an existing row, so there is
//! nothing to hydrate. A MERGE inside a CTE is not attempted -- the planner hook that builds
//! the probe only runs on the outermost statement, same limit the CTE case above already has
//! for a join/sub-query. The cold-DML write guard's own MERGE check
//! (`enforce_unverifiable_merge_guard`) now also uses this probe for a precise recount instead
//! of a blanket "the table has cold data somewhere" rejection, which is what makes hydration
//! compose correctly here: without it, a MERGE hydration already made safe to run would still
//! be wrongly rejected afterward.

use std::cell::RefCell;

use pgrx::pg_sys;

use crate::sql::job_lock::TableJobLockGuard;

thread_local! {
    /// Job locks taken by hydration, released when the statement (or transaction) ends.
    static HELD_LOCKS: RefCell<Vec<TableJobLockGuard>> = const { RefCell::new(Vec::new()) };
}

static mut PREVIOUS: pg_sys::ExecutorStart_hook_type = None;

pub(crate) fn register() {
    // SAFETY: called once from `_PG_init` while single-threaded.
    unsafe {
        PREVIOUS = pg_sys::ExecutorStart_hook;
        pg_sys::ExecutorStart_hook = Some(executor_start);
    }
}

/// Releases the job locks hydration took (statement end, commit or abort).
pub(crate) fn release_locks() {
    HELD_LOCKS.with(|locks| {
        if let Ok(mut locks) = locks.try_borrow_mut() {
            locks.clear();
        }
    });
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn executor_start(query_desc: *mut pg_sys::QueryDesc, eflags: std::ffi::c_int) {
    unsafe {
        if crate::thread_guard::is_foreign() {
            match PREVIOUS {
                Some(previous) => previous(query_desc, eflags),
                None => crate::thread_guard::standard::standard_ExecutorStart(query_desc, eflags),
            }
            return;
        }
        if crate::guc::hydrate_on_write() {
            hydrate_before_scan(query_desc, eflags);
        }
        match PREVIOUS {
            Some(previous) => previous(query_desc, eflags),
            None => pg_sys::standard_ExecutorStart(query_desc, eflags),
        }
    }
}

/// A single `UPDATE`/`DELETE` target hydrate-on-write found worth examining: the table and the
/// `WHERE` clause (as SQL text over `t`) its rows must match, plus any bound parameters the
/// clause's join/sub-query probe references (always empty for a data-modifying CTE's own
/// target, which never gets the probe attached -- see `hooks::dml_planner`).
struct HydrateTarget {
    table_oid: pg_sys::Oid,
    where_sql: String,
    params: Vec<crate::hooks::dml_planner::ProbeParam>,
}

unsafe fn hydrate_before_scan(query_desc: *mut pg_sys::QueryDesc, eflags: std::ffi::c_int) {
    unsafe {
        if query_desc.is_null() || (*query_desc).plannedstmt.is_null() || (*query_desc).snapshot.is_null() {
            return;
        }
        if eflags & (pg_sys::EXEC_FLAG_EXPLAIN_ONLY as std::ffi::c_int) != 0
            || crate::sql::cold_dml::guard::suspended()
            || isolation_blocks_hydration()
            || pg_sys::ParallelWorkerNumber >= 0
        {
            return;
        }
        let planned = (*query_desc).plannedstmt;
        let mut hydrated_any = false;

        if matches!(
            (*query_desc).operation,
            pg_sys::CmdType::CMD_UPDATE | pg_sys::CmdType::CMD_DELETE | pg_sys::CmdType::CMD_MERGE
        ) {
            if let Some(target) = top_level_target(query_desc, planned) {
                hydrated_any |= hydrate_target(&target);
            }
        }

        // A data-modifying CTE's own `UPDATE`/`DELETE` lives in the plan's sub-plans, not
        // `planTree` -- the top-level statement here is a `SELECT` (or an `INSERT`/`UPDATE`
        // referencing the CTE's output) -- same place the cold-DML write guard already looks
        // (`hooks::executor::cold_only_cte_candidates`). A statement can have both a top-level
        // target and one or more CTE targets at once (`WITH d AS (DELETE FROM a ...) UPDATE b
        // ... FROM d`), so this runs independently of the check above, not instead of it.
        if (*planned).hasModifyingCTE {
            for target in cte_targets(query_desc, planned) {
                hydrated_any |= hydrate_target(&target);
            }
        }

        if hydrated_any {
            // Make every hydrated row visible to whichever scan(s) are about to run: they carry
            // the current command id, which the statement's snapshot (taken earlier) does not
            // see. One bump covers every target examined above -- all part of the same statement,
            // hence the same snapshot.
            pg_sys::CommandCounterIncrement();
            (*(*query_desc).snapshot).curcid = pg_sys::GetCurrentCommandId(false);
        }
    }
}

/// True when the session's isolation level (and, for `SERIALIZABLE`, the cold-read policy)
/// rules hydration out for this statement entirely. See the module doc comment.
unsafe fn isolation_blocks_hydration() -> bool {
    unsafe {
        match pg_sys::XactIsoLevel {
            // SERIALIZABLE only when the probe's cold read is allowed to run without SSI
            // predicate locks, i.e. the same condition that already permits an ordinary
            // SERIALIZABLE cold read.
            level if level == pg_sys::XACT_SERIALIZABLE as std::ffi::c_int => {
                crate::guc::reject_serializable_cold_reads()
            }
            // READ COMMITTED and REPEATABLE READ always qualify (own-transaction visibility is
            // isolation-level independent).
            level if level == pg_sys::XACT_READ_COMMITTED as std::ffi::c_int
                || level == pg_sys::XACT_REPEATABLE_READ as std::ffi::c_int =>
            {
                false
            }
            _ => true,
        }
    }
}

/// The top-level statement's own target, when it is a plain `UPDATE`/`DELETE`/`MERGE` on a
/// single managed table (single-table `WHERE` for `UPDATE`/`DELETE`, or a join/sub-query --
/// always the case for `MERGE` -- via the planner hook's probe).
unsafe fn top_level_target(
    query_desc: *mut pg_sys::QueryDesc,
    planned: *mut pg_sys::PlannedStmt,
) -> Option<HydrateTarget> {
    unsafe {
        let result_relations = (*planned).resultRelations;
        let rtable = (*planned).rtable;
        // A partitioned/inherited target (ADR-008) lists every potential leaf here, not
        // just one -- confirmed live even for a literal, maximally selective WHERE clause
        // on the partition key, since PostgreSQL relies on runtime pruning rather than
        // eliminating siblings from this list at plan time. No probe mechanism exists for
        // that shape yet (same gap `hooks::executor`'s write guard has), so it is left
        // alone here too: the write guard now fails closed for it instead of silently
        // doing nothing, which is the safe outcome in the absence of hydration.
        if rtable.is_null() || result_relations.is_null() || (*result_relations).length != 1 {
            return None;
        }
        let range_table_index = (*(*result_relations).elements.add(0)).int_value;
        if range_table_index <= 0 || range_table_index > (*rtable).length {
            return None;
        }
        let rte = (*(*rtable).elements.add((range_table_index - 1) as usize))
            .ptr_value
            .cast::<pg_sys::RangeTblEntry>();
        if rte.is_null() || (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
            return None;
        }
        let table_oid = (*rte).relid;
        if !crate::catalog::cache::is_managed_relation(table_oid) {
            return None;
        }
        // Single-table statements: the plan's own WHERE. Joins, sub-queries, and MERGE (always
        // a join of target and source, never single-table): the planner hook's probe, a SELECT
        // of the target's primary keys over the statement's whole FROM/WHERE (or, for MERGE,
        // target/source/ON -- see `hooks::dml_planner`'s module doc comment), used as
        // `(pk) IN (probe)` so the same cold-only comparison applies.
        if let Some(probe) = crate::hooks::dml_planner::probe_sql_of((*planned).planTree) {
            let params = crate::hooks::dml_planner::collect_params((*query_desc).params)?;
            let pk_columns = crate::sql::cold_dml::primary_key_columns(table_oid).ok()?;
            let columns = pk_columns
                .iter()
                .map(|column| format!("t.{}", koldstore_common::sql::ident::quote_ident(column)))
                .collect::<Vec<_>>()
                .join(", ");
            Some(HydrateTarget { table_oid, where_sql: format!("({columns}) IN ({probe})"), params })
        } else if (*query_desc).operation == pg_sys::CmdType::CMD_MERGE {
            // No single-table fallback for MERGE: an insert-only MERGE (no probe, because
            // nothing it does ever touches an existing row) and a MERGE whose ON/source the
            // probe could not reproduce both fall through to the write guards unchanged.
            None
        } else {
            let where_sql =
                crate::hooks::where_deparse::deparse_where((*planned).planTree, (*query_desc).params, table_oid)?;
            Some(HydrateTarget { table_oid, where_sql, params: Vec::new() })
        }
    }
}

/// Every data-modifying CTE's own `UPDATE`/`DELETE` target on a managed table, single-table
/// only. A CTE whose own `WHERE` joins or sub-queries never gets the planner hook's probe
/// either -- that hook only runs on the outermost statement, and declines outright on
/// `hasModifyingCTE`/`cteList` (see `hooks::dml_planner::build_probe_sql`) -- so it is skipped
/// here too; the write guard still catches it unchanged, same fallback as an unreproducible
/// top-level clause already gets.
unsafe fn cte_targets(query_desc: *mut pg_sys::QueryDesc, planned: *mut pg_sys::PlannedStmt) -> Vec<HydrateTarget> {
    unsafe {
        let mut targets = Vec::new();
        let rtable = (*planned).rtable;
        if rtable.is_null() {
            return targets;
        }
        for subplan in crate::merge_scan::pg::literals::list_node_pointers((*planned).subplans) {
            let plan = subplan.cast::<pg_sys::Plan>();
            if plan.is_null() || (*plan).type_ != pg_sys::NodeTag::T_ModifyTable {
                continue;
            }
            let modify = plan.cast::<pg_sys::ModifyTable>();
            if !matches!((*modify).operation, pg_sys::CmdType::CMD_UPDATE | pg_sys::CmdType::CMD_DELETE) {
                continue;
            }
            if (*modify).resultRelations.is_null() || (*(*modify).resultRelations).length != 1 {
                continue;
            }
            let range_table_index = (*(*(*modify).resultRelations).elements.add(0)).int_value;
            if range_table_index <= 0 || range_table_index > (*rtable).length {
                continue;
            }
            let rte = (*(*rtable).elements.add((range_table_index - 1) as usize))
                .ptr_value
                .cast::<pg_sys::RangeTblEntry>();
            if rte.is_null() || (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
                continue;
            }
            let table_oid = (*rte).relid;
            if !crate::catalog::cache::is_managed_relation(table_oid) {
                continue;
            }
            let Some(where_sql) = crate::hooks::where_deparse::deparse_where(plan, (*query_desc).params, table_oid)
            else {
                continue;
            };
            targets.push(HydrateTarget { table_oid, where_sql, params: Vec::new() });
        }
        targets
    }
}

/// Hydrates `target`'s cold-only matching rows into the heap, if any. Returns whether anything
/// was inserted -- the caller advances the statement's command id once, after every target
/// (top-level and every data-modifying CTE) has been examined, not once per target.
fn hydrate_target(target: &HydrateTarget) -> bool {
    let table_oid = target.table_oid;
    let where_sql = &target.where_sql;
    let params = &target.params;
    let has_cold = matches!(
        crate::catalog::cache::cached_manifest_planner_hint(table_oid),
        Ok(Some((segments, _))) if segments > 0
    );
    // After an earlier write in this transaction the merged view cannot be trusted
    // (upstream #121); leave it to the write guards.
    if !has_cold || crate::txn_writes::was_written(table_oid) {
        return false;
    }

    // Keep flush away from the hydrated rows until the statement has scanned them.
    // Optional: the hydrated row is uncommitted and therefore invisible to (and safe from) a
    // concurrent flush, so the default is not to serialize behind flushes (see the GUC).
    if crate::guc::hydrate_take_job_lock() {
        let lock = TableJobLockGuard::lock_bounded(table_oid, crate::sql::job_lock::WRITE_LOCK_TIMEOUT)
            .unwrap_or_else(|error| pgrx::error!("koldstore: hydrate-on-write: {error}"));
        HELD_LOCKS.with(|locks| locks.borrow_mut().push(lock));
    }

    // Serialize per key, then look. A delete another transaction committed moments ago is not
    // masked from cold reads until the async mirror applies its tombstone, and two sessions
    // hydrating one key interleave badly (the second inserts its own copy once the first
    // commits a delete). So: fetch candidates, lock their keys (waiting out any other
    // transaction working on them), fence on the mirror, fetch again through a fresh
    // snapshot, and repeat until every candidate key is locked. What remains is exactly the
    // cold rows still alive after everyone who held those keys has finished.
    //
    // First a plain look, with no fence and no locks: a stale mirror can only make a row look
    // cold that is really deleted (an extra candidate), never hide a real one, so no candidates
    // here means nothing to hydrate, which is the common case (updating rows that are hot).
    let probe = crate::sql::cold_dml::guard::cold_only_matching_rows_with_params(table_oid, where_sql, params)
        .unwrap_or_else(|error| pgrx::error!("koldstore: hydrate-on-write failed: {error}"));
    if probe.is_empty() {
        return false;
    }
    let mut locked = std::collections::HashSet::<String>::new();
    let mut rows: Vec<(String, serde_json::Value)> = Vec::new();
    let mut settled = false;
    for _ in 0..6 {
        rows = crate::sql::cold_dml::key_lock::with_current_view(|| {
            crate::sql::cold_dml::guard::cold_only_matching_rows_with_params(table_oid, where_sql, params)
        })
        .unwrap_or_else(|error| pgrx::error!("koldstore: hydrate-on-write failed: {error}"));
        let new_keys: Vec<String> =
            rows.iter().map(|(key, _)| key.clone()).filter(|key| !locked.contains(key)).collect();
        if new_keys.is_empty() {
            settled = true;
            break;
        }
        if locked.len() + new_keys.len() > crate::guc::max_hydrate_rows() {
            break; // over the cap: reported below
        }
        crate::sql::cold_dml::key_lock::lock_keys_bounded(table_oid, &new_keys)
            .unwrap_or_else(|error| pgrx::error!("koldstore: hydrate-on-write: {error}"));
        locked.extend(new_keys);
    }
    if rows.is_empty() {
        return false;
    }
    if !settled && rows.len() <= crate::guc::max_hydrate_rows() {
        pgrx::error!(
            "koldstore: hydrate-on-write could not settle on a stable set of cold rows for {} (heavy \
             concurrent changes to the same keys); retry",
            crate::txn_writes::relation_display_name(table_oid)
        );
    }
    let cap = crate::guc::max_hydrate_rows();
    if rows.len() > cap {
        let name = crate::txn_writes::relation_display_name(table_oid);
        pgrx::error!(
            "koldstore: refusing this UPDATE/DELETE on managed table {name} -- it matches {} cold row(s), more \
             than koldstore.max_hydrate_rows ({cap}); narrow the statement or change the rows in batches with \
             koldstore.update_row()/delete_row() (upstream issue #122)",
            rows.len()
        );
    }
    let rows: Vec<serde_json::Value> = rows.into_iter().map(|(_, row)| row).collect();
    crate::sql::cold_dml::hydrate_rows(table_oid, &rows)
        .unwrap_or_else(|error| pgrx::error!("koldstore: hydrate-on-write failed: {error}"));
    crate::txn_writes::record_managed_write(table_oid);
    true
}
