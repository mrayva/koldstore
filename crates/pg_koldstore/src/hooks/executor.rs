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

    /// Phase 1 of the cold-DML write guard for UPDATE/DELETE (see the
    /// `executor_end` call site and `hooks::pk_predicate`'s module doc
    /// comment). Returns the target relation OID and the raw
    /// `attnum -> value` equalities extracted from its WHERE clause, but
    /// only when this statement is a plausible guard candidate at all:
    /// `CMD_UPDATE`/`CMD_DELETE`, a single managed target relation, the
    /// native statement already affected zero rows (the only case the
    /// guard exists for), and the write guard is not currently suspended
    /// for this session (`sql::cold_dml::guard::suspended`).
    unsafe fn cold_only_update_delete_candidate(
        query_desc: *mut pg_sys::QueryDesc,
    ) -> Option<(pg_sys::Oid, std::collections::HashMap<i16, serde_json::Value>)> {
        unsafe {
            if crate::sql::cold_dml::guard::suspended() {
                return None;
            }
            if query_desc.is_null() || (*query_desc).plannedstmt.is_null() || (*query_desc).estate.is_null() {
                return None;
            }
            if !matches!(
                (*query_desc).operation,
                pg_sys::CmdType::CMD_UPDATE | pg_sys::CmdType::CMD_DELETE
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
            if (*estate).es_processed != 0 {
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
            Some((table_oid, raw))
        }
    }

    /// Phase 2 of the cold-DML write guard: resolves `raw_predicate`
    /// against the table's actual primary-key columns (a catalog lookup,
    /// safe now that `standard_ExecutorEnd` has already run) and, only if
    /// it resolves to a complete PK match that exists hot-or-cold, raises
    /// the same error `koldstore.update_row`/`delete_row`'s own docs point
    /// callers at.
    unsafe fn enforce_cold_only_update_delete_guard(
        table_oid: pg_sys::Oid,
        raw_predicate: &std::collections::HashMap<i16, serde_json::Value>,
    ) {
        let Ok(pk_columns) = crate::sql::cold_dml::primary_key_columns(table_oid) else {
            return;
        };
        let Ok(column_attnums) = crate::sql::cold_dml::guard::column_attnum_map(table_oid) else {
            return;
        };
        let Some(pk_json) =
            crate::hooks::pk_predicate::resolve_pk_predicate(raw_predicate, &pk_columns, &column_attnums)
        else {
            return;
        };
        let Ok(exists) = crate::sql::cold_dml::guard::cold_pk_exists(table_oid, &pk_json) else {
            return;
        };
        if exists {
            let table_name =
                crate::catalog::resolve::qualified_relation_name(table_oid).unwrap_or_else(|_| "?".to_string());
            pgrx::error!(
                "koldstore: refusing this UPDATE/DELETE on managed table {table_name} -- it affected zero rows, \
                 but primary key {pk_json} exists in cold storage; use koldstore.update_row()/delete_row() \
                 instead (upstream issue #122)"
            );
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
