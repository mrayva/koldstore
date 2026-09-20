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
            if let Some((table_oid, raw_predicate, es_processed)) = cold_guard_candidate {
                enforce_cold_only_update_delete_guard(table_oid, &raw_predicate, es_processed);
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
    ) -> Option<(pg_sys::Oid, Vec<crate::hooks::pk_predicate::RawLeaf>, u64)> {
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
                crate::hooks::pk_predicate::extract_raw_predicate_leaves((*planned).planTree, (*query_desc).params)?;
            // Every distinct PK candidate this predicate could name is at
            // most one row (PK columns are unique), so `es_processed`
            // (the statement's total affected-row count) can never exceed
            // the *true* PK candidate count -- and equals it exactly iff
            // the native statement found every single one. That is the
            // real skip condition (see `enforce_cold_only_update_delete_
            // guard`, where it is applied once phase 2's catalog lookup
            // can tell PK leaves apart from residual ones). Phase 1 can
            // only take the shortcut itself when EVERY leaf has exactly
            // one value, PK or residual alike -- then the true PK
            // candidate count is trivially 1 regardless of classification,
            // and "found >= 1" is unambiguous. Confirmed live this
            // distinction is required, not just tidy: computing a
            // candidate estimate from *all* leaves' value counts (PK and
            // residual together) inflates the count whenever a residual
            // `IN (...)` is present, e.g. `id = 1 AND status IN
            // ('a','b')` against a genuinely hot, genuinely matching row
            // produced `es_processed = 1` but an inflated estimate of 2,
            // so the "already fully handled" skip never fired and a
            // completely correct, already-successful UPDATE was wrongly
            // re-examined (and, per the residual-check reasoning, wrongly
            // rejected -- it re-found the same row via the cold-or-hot
            // probe and incorrectly treated that as unsafe). Residual
            // leaves must not count toward this early estimate at all.
            let single_valued = raw.iter().all(|leaf| leaf.value_count() == 1);
            let es_processed = (*estate).es_processed;
            if single_valued && es_processed != 0 {
                return None;
            }
            Some((table_oid, raw, es_processed))
        }
    }

    /// Phase 2 of the cold-DML write guard: resolves `raw` against the
    /// table's actual primary-key columns (a catalog lookup, safe now that
    /// `standard_ExecutorEnd` has already run) into every candidate PK
    /// combination (plain equality contributes one; an `IN (...)`
    /// contributes each element) plus any residual (non-PK) conditions --
    /// see `resolve_predicate` -- and raises the same error
    /// `koldstore.update_row`/`delete_row`'s own docs point callers at for
    /// the first candidate confirmed to (a) exist hot-or-cold and (b) still
    /// satisfy every residual condition against its actual located row.
    /// (b) is what makes an extra condition (`WHERE id = 5 AND status =
    /// 'x'`) safe to guard at all: a hot row whose `status` genuinely
    /// doesn't match `'x'` is a legitimate zero-row result unrelated to
    /// #122, not something to reject -- see the module doc comment on
    /// `hooks::pk_predicate` for the full reasoning.
    unsafe fn enforce_cold_only_update_delete_guard(
        table_oid: pg_sys::Oid,
        raw: &[crate::hooks::pk_predicate::RawLeaf],
        es_processed: u64,
    ) {
        let Ok(pk_columns) = crate::sql::cold_dml::primary_key_columns(table_oid) else {
            return;
        };
        let Ok(column_attnums) = crate::sql::cold_dml::guard::column_attnum_map(table_oid) else {
            return;
        };
        let Some((candidates, residual)) =
            crate::hooks::pk_predicate::resolve_predicate(raw, &pk_columns, &column_attnums)
        else {
            return;
        };
        // The real "already fully handled natively" skip -- see phase 1's
        // comment on why it can only be computed here, once PK leaves are
        // known apart from residual ones. `candidates.len()` is the true
        // count of distinct rows this predicate's PK portion alone could
        // name; a residual `IN (...)` never multiplies into it.
        if es_processed as usize >= candidates.len() {
            return;
        }
        for pk_json in candidates {
            let Ok(Some(row_json)) = crate::sql::cold_dml::guard::locate_row(table_oid, &pk_json) else {
                continue; // genuinely doesn't exist anywhere -- a legitimate zero-row result
            };
            let Ok(residual_matches) = crate::sql::cold_dml::guard::residual_conditions_match(
                table_oid,
                &row_json,
                &residual,
            ) else {
                continue;
            };
            if residual_matches {
                // `locate_row` reports hot-or-cold, not specifically cold:
                // for a single-candidate predicate with no residual
                // conditions this only ever runs when the native statement
                // already found nothing (see the es_processed check at the
                // call site), so "exists and matches" there can only mean
                // cold. For a multi-candidate `IN (...)` predicate that is
                // not guaranteed -- some candidates may be genuinely hot
                // and already part of what the statement affected.
                // Confirmed live: an `IN` list mixing one hot and one cold
                // key reported the HOT key here first (iteration order is
                // unspecified), which "exists in cold storage" would have
                // misdescribed. The wording below is deliberately accurate
                // for both cases rather than presuming cold.
                //
                // Known, accepted imprecision: when a PK `IN (...)` is
                // combined with a residual condition, and one candidate
                // was already hot-and-handled by the native statement
                // while a sibling candidate is genuinely cold, this loop
                // can name the ALREADY-HANDLED candidate in the error
                // instead of (or as well as) the truly offending one --
                // confirmed live, `DELETE ... WHERE id IN (1,3) AND
                // status='open'` with id=1 hot+matched+already-deleted-
                // this-transaction and id=3 genuinely cold+matching named
                // id=1. Root cause: `es_processed >= candidates.len()`
                // (the real skip check, above) can't be satisfied when
                // ANY candidate is missing, so every candidate is
                // re-checked including ones the native statement already
                // handled -- and an already-hydrated-then-deleted
                // candidate can still resolve via a stale cold copy
                // predating the hydrate, the same root cause documented
                // on the skip check for the PK-only IN-list case, just
                // not fully closed here since there is no cheap way to
                // know which specific candidates the native statement
                // already covered. The overall reject decision stays
                // correct either way (confirmed: id=3 alone reproduces
                // the same rejection on its own), so this is a diagnostic
                // imprecision only, never a false accept.
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
