//! EXPERIMENTAL hydrate-on-write (ADR-007, option B): lets a plain single-table
//! `UPDATE`/`DELETE` change cold-only rows.
//!
//! An `ExecutorStart` hook, before the statement's scan begins, finds the cold-only
//! rows its `WHERE` clause matches (merged hot+cold view minus heap-only view),
//! inserts them into the heap with an ordinary `INSERT` (so capture, seq
//! assignment and flush behave as for any insert -- no second mirror writer), and
//! advances the statement snapshot's command id so the scan that follows sees them.
//! The native statement then runs unchanged, including triggers, foreign keys,
//! row-level security and `RETURNING`.
//!
//! Limits of the prototype: `READ COMMITTED` only (the transaction snapshot of
//! `REPEATABLE READ`/`SERIALIZABLE` cannot be advanced), single-table statements
//! whose `WHERE` clause `hooks::where_deparse` can reproduce, and a transaction
//! that has not already written the table. Everything else keeps falling through
//! to the write guards, which reject it. The table job lock is held until the
//! statement ends so a flush cannot move the hydrated rows back to cold before the
//! scan reads them.

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
        if crate::guc::hydrate_on_write() {
            hydrate_before_scan(query_desc, eflags);
        }
        match PREVIOUS {
            Some(previous) => previous(query_desc, eflags),
            None => pg_sys::standard_ExecutorStart(query_desc, eflags),
        }
    }
}

unsafe fn hydrate_before_scan(query_desc: *mut pg_sys::QueryDesc, eflags: std::ffi::c_int) {
    unsafe {
        if query_desc.is_null() || (*query_desc).plannedstmt.is_null() || (*query_desc).snapshot.is_null() {
            return;
        }
        if !matches!(
            (*query_desc).operation,
            pg_sys::CmdType::CMD_UPDATE | pg_sys::CmdType::CMD_DELETE
        ) || eflags & (pg_sys::EXEC_FLAG_EXPLAIN_ONLY as std::ffi::c_int) != 0
            || crate::sql::cold_dml::guard::suspended()
            // The transaction snapshot of REPEATABLE READ / SERIALIZABLE cannot be advanced.
            || pg_sys::XactIsoLevel != pg_sys::XACT_READ_COMMITTED as std::ffi::c_int
            || pg_sys::ParallelWorkerNumber >= 0
        {
            return;
        }
        let planned = (*query_desc).plannedstmt;
        let result_relations = (*planned).resultRelations;
        let rtable = (*planned).rtable;
        if rtable.is_null() || result_relations.is_null() || (*result_relations).length != 1 {
            return;
        }
        let range_table_index = (*(*result_relations).elements.add(0)).int_value;
        if range_table_index <= 0 || range_table_index > (*rtable).length {
            return;
        }
        let rte = (*(*rtable).elements.add((range_table_index - 1) as usize))
            .ptr_value
            .cast::<pg_sys::RangeTblEntry>();
        if rte.is_null() || (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
            return;
        }
        let table_oid = (*rte).relid;
        if !crate::catalog::cache::is_managed_relation(table_oid) {
            return;
        }
        let Some(where_sql) =
            crate::hooks::where_deparse::deparse_where((*planned).planTree, (*query_desc).params, table_oid)
        else {
            return;
        };
        let has_cold = matches!(
            crate::catalog::cache::cached_manifest_planner_hint(table_oid),
            Ok(Some((segments, _))) if segments > 0
        );
        // After an earlier write in this transaction the merged view cannot be trusted
        // (upstream #121); leave it to the write guards.
        if !has_cold || crate::txn_writes::was_written(table_oid) {
            return;
        }

        // Keep flush away from the hydrated rows until the statement has scanned them.
        let lock = TableJobLockGuard::lock(table_oid).unwrap_or_else(|error| pgrx::error!("hydrate-on-write: {error}"));
        HELD_LOCKS.with(|locks| locks.borrow_mut().push(lock));

        let rows = crate::sql::cold_dml::guard::cold_only_matching_rows(table_oid, &where_sql)
            .unwrap_or_else(|error| pgrx::error!("koldstore: hydrate-on-write failed: {error}"));
        if rows.is_empty() {
            return;
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
        crate::sql::cold_dml::hydrate_rows(table_oid, &rows)
            .unwrap_or_else(|error| pgrx::error!("koldstore: hydrate-on-write failed: {error}"));
        crate::txn_writes::record_managed_write(table_oid);
        // Make the hydrated rows visible to the statement about to scan: they carry the
        // current command id, which the statement's snapshot (taken earlier) does not see.
        pg_sys::CommandCounterIncrement();
        (*(*query_desc).snapshot).curcid = pg_sys::GetCurrentCommandId(false);
    }
}
