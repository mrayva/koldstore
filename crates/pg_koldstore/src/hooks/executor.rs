//! DML hook and clean-schema mirror integration.

use koldstore_common::{scope, ScopeError, ScopeKey, TableKind};

pub use koldstore_merge::{
    extract_simple_pk_delete_predicate, plan_managed_delete_effect, plan_managed_insert_effect,
    plan_managed_update_effect, simple_pk_delete_supported, ManagedDmlEffect, SimplePkPredicate,
    HOT_DML_MANIFEST_SYNC_STATE,
};

/// DML operations observed by the managed hook shell.
#[must_use]
pub const fn managed_dml_hook_names() -> &'static [&'static str] {
    &["INSERT", "UPDATE", "DELETE", "MERGE", "COPY"]
}

/// Enforces user-scope checks before managed DML touches heap rows or cold metadata.
///
/// # Errors
///
/// Returns a scope error when user-scoped DML is missing an active session scope,
/// has no row scope, or targets a different scope.
pub fn enforce_dml_scope(
    table_kind: TableKind,
    session_user_id: Option<&str>,
    row_scope: Option<&ScopeKey>,
) -> Result<Option<ScopeKey>, ScopeError> {
    let active_scope = scope::active_scope_for_table(table_kind, session_user_id)?;
    if let Some(active_scope) = active_scope.as_ref() {
        scope::enforce_row_scope(active_scope, row_scope)?;
    }
    Ok(active_scope)
}

#[cfg(feature = "pg")]
mod live {
    use std::sync::atomic::{AtomicBool, Ordering};

    use pgrx::pg_sys;

    static REGISTERED: AtomicBool = AtomicBool::new(false);
    static mut PREVIOUS: pg_sys::ExecutorEnd_hook_type = None;

    pub(super) fn register() {
        if REGISTERED.swap(true, Ordering::AcqRel) {
            return;
        }
        unsafe {
            PREVIOUS = pg_sys::ExecutorEnd_hook;
            pg_sys::ExecutorEnd_hook = Some(executor_end);
        }
    }

    #[pgrx::pg_guard]
    unsafe extern "C-unwind" fn executor_end(query_desc: *mut pg_sys::QueryDesc) {
        unsafe {
            // Only managed result relations publish a WAL generation. Nested
            // trigger/cascade DML still fires ExecutorEnd with the managed
            // relation as the result target, so those writes are not missed.
            // Unmanaged DML in a database that happens to have a capture slot
            // must not wake maintenance or advance the logical slot.
            // Capture OIDs while QueryDesc is still live, but defer catalog/SPI
            // lookup until the previous ExecutorEnd has closed the executor.
            // Opening SPI before standard_ExecutorEnd can fail and used to turn
            // managed DML into a silent false negative.
            let changed_relation_oids = changed_relation_oids(query_desc);
            // Cold-DML write guard (upstream #122, Option B) phase 1: pure
            // plan-tree pointer walking, no SPI -- must happen here, before
            // `previous`/`standard_ExecutorEnd` may free the executor state
            // those pointers reference. See `pk_predicate`'s module doc
            // comment for why this can't be a single pass.
            let cold_guard_candidate = cold_only_update_delete_candidate(query_desc);
            if let Some(previous) = PREVIOUS {
                previous(query_desc);
            } else {
                pg_sys::standard_ExecutorEnd(query_desc);
            }
            if changed_relation_oids
                .into_iter()
                .any(crate::catalog::cache::is_managed_relation)
            {
                crate::worker::wake::mark_managed_dml_pending();
            }
            crate::memory::release_process_heap_if_pending();
            // Phase 2: SPI-safe now that standard_ExecutorEnd has run.
            if let Some((table_oid, raw_predicate)) = cold_guard_candidate {
                enforce_cold_only_update_delete_guard(table_oid, &raw_predicate);
            }
        }
    }

    /// Phase 1 of the cold-DML write guard for UPDATE/DELETE/MERGE (see
    /// the `executor_end` call site and `hooks::pk_predicate`'s module doc
    /// comment). Returns the target relation OID and the raw
    /// `attnum -> [value, ...]` equalities extracted from its WHERE/ON
    /// clause, but only when this statement is a plausible guard
    /// candidate at all: `CMD_UPDATE`/`CMD_DELETE`/`CMD_MERGE`, a single
    /// managed target relation, the write guard not currently suspended
    /// for this session (`sql::cold_dml::guard::suspended`), and -- for a
    /// plain equality-only predicate, where "zero rows affected" and "the
    /// one candidate wasn't hot" are the same fact -- the native statement
    /// already affected zero rows. A predicate with an `IN (...)` column
    /// is checked regardless of rows affected; see the comment at this
    /// function's `es_processed` check for why.
    ///
    /// `CMD_MERGE` reuses the exact same `extract_raw_attnum_equality`
    /// extraction as UPDATE/DELETE, not a MERGE-specific path -- confirmed
    /// live via a temporary plan-tree dump that a single-row `USING (...)`
    /// source (by far the common "upsert one row" shape, and the one that
    /// matters most: it is what silently created a duplicate before this
    /// fix) makes PostgreSQL's own planner collapse the join away entirely
    /// into a plain parameterized `IndexScan` on the target -- structurally
    /// identical to plain UPDATE/DELETE's shape, `Var = Const`. A genuine
    /// multi-row join source instead produces a `NestLoop` over the
    /// source with the target's `IndexScan` condition as a `PARAM_EXEC`
    /// (executor-internal, rebound per source row, no single fixed value
    /// to report at statement end) -- `const_or_param_datum` already only
    /// resolves `PARAM_EXTERN`, so this shape is safely skipped rather
    /// than mishandled, falling into the same deferred "bulk/complex
    /// statement" bucket as multi-row INSERT and compound-predicate
    /// UPDATE/DELETE already do.
    unsafe fn cold_only_update_delete_candidate(
        query_desc: *mut pg_sys::QueryDesc,
    ) -> Option<(pg_sys::Oid, std::collections::HashMap<i16, Vec<serde_json::Value>>)> {
        unsafe {
            if crate::sql::cold_dml::guard::suspended() {
                return None;
            }
            if query_desc.is_null() || (*query_desc).plannedstmt.is_null() || (*query_desc).estate.is_null() {
                return None;
            }
            if !matches!(
                (*query_desc).operation,
                pg_sys::CmdType::CMD_UPDATE | pg_sys::CmdType::CMD_DELETE | pg_sys::CmdType::CMD_MERGE
            ) {
                return None;
            }
            let estate = (*query_desc).estate;
            // `EXPLAIN` without `ANALYZE` calls ExecutorStart+ExecutorEnd
            // but never ExecutorRun, so `es_processed` stays at its
            // zeroed initial value by coincidence -- confirmed live, a
            // plain `EXPLAIN UPDATE ...` against a cold-PK row tripped
            // this guard even though no row modification was ever
            // attempted. `es_top_eflags` carries EXEC_FLAG_EXPLAIN_ONLY
            // for exactly this case.
            if (*estate).es_top_eflags & (pg_sys::EXEC_FLAG_EXPLAIN_ONLY as std::ffi::c_int) != 0 {
                return None;
            }
            let planned = (*query_desc).plannedstmt;
            let rtable = (*planned).rtable;
            let result_relations = (*planned).resultRelations;
            if rtable.is_null() || result_relations.is_null() || (*result_relations).length != 1 {
                // Multi-relation targets (partitioned tables) are out of
                // scope for this pass -- never a false positive, just
                // unguarded.
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
            let raw =
                crate::hooks::pk_predicate::extract_raw_attnum_equality((*planned).planTree, (*query_desc).params)?;
            // Every distinct PK candidate this predicate could name is at
            // most one row (PK columns are unique), so `es_processed`
            // (the statement's total affected-row count) can never exceed
            // the candidate count -- and it equals the candidate count
            // exactly iff the native statement found every single one.
            // `es_processed < candidate_count` is therefore precisely
            // "at least one candidate was not found natively" -- for a
            // plain equality predicate (candidate_count == 1) this is the
            // same `es_processed == 0` check as before (zero added cost on
            // the common hot-and-found path); for an `IN (...)` predicate
            // it correctly also catches a PARTIAL match. Confirmed live,
            // `DELETE ... WHERE id IN (1,2)` with id=1 hot and id=2
            // cold-only reported `es_processed = 1` (not 0) and silently
            // deleted only the hot row, leaving id=2's cold duplicate
            // untouched with no error -- exactly the silent-wrong-answer
            // #122 exists to prevent, just hiding behind a nonzero count.
            //
            // Checking every candidate whenever *any* mismatch exists
            // (rather than gating on "no candidate found at all") was
            // tried first and found unsafe: confirmed live, re-checking a
            // candidate that the native statement itself had *just*
            // deleted (as part of this same, not-yet-committed statement)
            // could still show as "exists" via a stale cold Parquet copy
            // predating a later `hydrate_pk` -- the async process that
            // normally suppresses that stale copy after a hot delete
            // hasn't run yet within the same uncommitted transaction,
            // producing a false rejection of an otherwise fully correct,
            // fully-matched IN-list DELETE. Comparing against
            // candidate_count avoids ever re-checking a candidate the
            // native statement already handled.
            let candidate_count: usize = raw.values().map(Vec::len).product();
            if (*estate).es_processed as usize >= candidate_count {
                return None;
            }
            Some((table_oid, raw))
        }
    }

    /// Phase 2 of the cold-DML write guard: resolves `raw_predicate`
    /// against the table's actual primary-key columns (a catalog lookup,
    /// safe now that `standard_ExecutorEnd` has already run) into every
    /// candidate PK combination (plain equality contributes one; an
    /// `IN (...)` contributes each element -- see `resolve_pk_predicate`),
    /// and raises the same error `koldstore.update_row`/`delete_row`'s own
    /// docs point callers at for the first candidate confirmed to exist
    /// hot-or-cold.
    unsafe fn enforce_cold_only_update_delete_guard(
        table_oid: pg_sys::Oid,
        raw_predicate: &std::collections::HashMap<i16, Vec<serde_json::Value>>,
    ) {
        let Ok(pk_columns) = crate::sql::cold_dml::primary_key_columns(table_oid) else {
            return;
        };
        let Ok(column_attnums) = crate::sql::cold_dml::guard::column_attnum_map(table_oid) else {
            return;
        };
        let Some(candidates) =
            crate::hooks::pk_predicate::resolve_pk_predicate(raw_predicate, &pk_columns, &column_attnums)
        else {
            return;
        };
        for pk_json in candidates {
            let Ok(exists) = crate::sql::cold_dml::guard::cold_pk_exists(table_oid, &pk_json) else {
                continue;
            };
            if exists {
                // `cold_pk_exists` reports hot-or-cold, not specifically
                // cold: for a single-candidate predicate this only ever
                // runs when the native statement already found nothing
                // (see the es_processed check at the call site), so
                // "exists" there can only mean cold. For a multi-candidate
                // `IN (...)` predicate that is not guaranteed -- some
                // candidates may be genuinely hot and already part of what
                // the statement affected. Confirmed live: an `IN` list
                // mixing one hot and one cold key reported the HOT key
                // here first (HashMap/Vec iteration order is unspecified),
                // which "exists in cold storage" would have misdescribed.
                // The wording below is deliberately accurate for both
                // cases rather than presuming cold.
                let table_name =
                    crate::catalog::resolve::qualified_relation_name(table_oid).unwrap_or_else(|_| "?".to_string());
                pgrx::error!(
                    "koldstore: refusing this UPDATE/DELETE/MERGE on managed table {table_name} -- primary key \
                     {pk_json} exists but this statement cannot reliably act on it (it may be cold-only, \
                     invisible to this statement's own scan); use koldstore.update_row()/delete_row() instead \
                     (upstream issue #122)"
                );
            }
        }
    }

    unsafe fn changed_relation_oids(query_desc: *mut pg_sys::QueryDesc) -> Vec<pg_sys::Oid> {
        unsafe {
            if query_desc.is_null()
                || (*query_desc).plannedstmt.is_null()
                || (*query_desc).estate.is_null()
            {
                return Vec::new();
            }
            if !matches!(
                (*query_desc).operation,
                pg_sys::CmdType::CMD_INSERT
                    | pg_sys::CmdType::CMD_UPDATE
                    | pg_sys::CmdType::CMD_DELETE
                    | pg_sys::CmdType::CMD_MERGE
            ) {
                return Vec::new();
            }

            let planned = (*query_desc).plannedstmt;
            let estate = (*query_desc).estate;
            let mut relation_oids = Vec::new();

            // PostgreSQL can leave PlannedStmt.resultRelations empty for some
            // ModifyTable shapes. EState's opened result array is the executed
            // source of truth and also covers routed partitions.
            if !(*estate).es_result_relations.is_null() {
                for index in 0..(*estate).es_range_table_size as usize {
                    let result_rel = *(*estate).es_result_relations.add(index);
                    if result_rel.is_null() || (*result_rel).ri_RelationDesc.is_null() {
                        continue;
                    }
                    let oid = (*(*result_rel).ri_RelationDesc).rd_id;
                    if !relation_oids.contains(&oid) {
                        relation_oids.push(oid);
                    }
                }
            }

            let result_relations = (*planned).resultRelations;
            let rtable = (*planned).rtable;
            if result_relations.is_null() || rtable.is_null() {
                return relation_oids;
            }
            relation_oids.reserve((*result_relations).length as usize);
            for index in 0..(*result_relations).length as usize {
                let range_table_index = (*(*result_relations).elements.add(index)).int_value;
                if range_table_index <= 0 || range_table_index > (*rtable).length {
                    continue;
                }
                let rte = (*(*rtable).elements.add((range_table_index - 1) as usize))
                    .ptr_value
                    .cast::<pg_sys::RangeTblEntry>();
                if !rte.is_null()
                    && (*rte).rtekind == pg_sys::RTEKind::RTE_RELATION
                    && !relation_oids.contains(&(*rte).relid)
                {
                    relation_oids.push((*rte).relid);
                }
            }
            relation_oids
        }
    }
}

/// Registers the lightweight managed-DML completion hook.
#[cfg(feature = "pg")]
pub(crate) fn register_executor_end_hook() {
    live::register();
}
