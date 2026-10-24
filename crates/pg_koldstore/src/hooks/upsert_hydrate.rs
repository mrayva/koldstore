//! `INSERT ... ON CONFLICT` onto a primary key that lives in cold storage (ADR-007, option B).
//!
//! Native `ON CONFLICT` only inspects the hot index, and the per-table `BEFORE INSERT` guard
//! refuses a key that exists anywhere. With `koldstore.hydrate_on_write` on, an `ExecutorStart`
//! hook records top-level `INSERT ... ON CONFLICT DO NOTHING/UPDATE` statements whose arbiter is
//! the primary key; the guard then hydrates the conflicting key (an ordinary heap insert, like
//! `koldstore.hydrate_pk`) instead of refusing, and advances the statement snapshot's command id
//! so the native conflict handling sees the hot row. `DO NOTHING` and `DO UPDATE` then run
//! unchanged, including `BEFORE UPDATE` triggers and `RETURNING`.
//!
//! Limits: a top-level statement only (not inside a CTE), the arbiter must be the primary key
//! (or absent); a conflict on another unique constraint that only a cold row holds is still
//! refused by the unique guard.

use std::cell::RefCell;

use pgrx::pg_sys;

struct Upsert {
    table_oid: pg_sys::Oid,
    snapshot: usize,
    query_desc: usize,
    subxact: pg_sys::SubTransactionId,
}

thread_local! {
    static UPSERTS: RefCell<Vec<Upsert>> = const { RefCell::new(Vec::new()) };
}

/// Records `query_desc` when it is an `INSERT ... ON CONFLICT` on a managed table whose arbiter is
/// the primary key.
///
/// # Safety
///
/// `query_desc` must be the live `QueryDesc` `ExecutorStart` was called with.
pub(crate) unsafe fn register(query_desc: *mut pg_sys::QueryDesc) {
    unsafe {
        if query_desc.is_null()
            || (*query_desc).operation != pg_sys::CmdType::CMD_INSERT
            || (*query_desc).plannedstmt.is_null()
            || (*query_desc).snapshot.is_null()
        {
            return;
        }
        let planned = (*query_desc).plannedstmt;
        let plan = (*planned).planTree;
        if plan.is_null() || (*plan).type_ != pg_sys::NodeTag::T_ModifyTable {
            return;
        }
        let modify = plan.cast::<pg_sys::ModifyTable>();
        if !matches!(
            (*modify).onConflictAction,
            pg_sys::OnConflictAction::ONCONFLICT_NOTHING | pg_sys::OnConflictAction::ONCONFLICT_UPDATE
        ) {
            return;
        }
        let rtable = (*planned).rtable;
        let index = (*modify).nominalRelation as i32;
        if rtable.is_null() || index <= 0 || index > (*rtable).length {
            return;
        }
        let rte = (*(*rtable).elements.add((index - 1) as usize)).ptr_value.cast::<pg_sys::RangeTblEntry>();
        if rte.is_null() || (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
            return;
        }
        let table_oid = (*rte).relid;
        if !crate::catalog::cache::is_managed_relation(table_oid) {
            return;
        }
        let arbiters = (*modify).arbiterIndexes;
        if !arbiters.is_null() {
            let primary = (0..(*arbiters).length as usize).any(|i| {
                let index_oid = (*(*arbiters).elements.add(i)).oid_value;
                let args = [pgrx::datum::DatumWithOid::from(index_oid)];
                pgrx::Spi::get_one_with_args::<bool>("SELECT indisprimary FROM pg_catalog.pg_index WHERE indexrelid = $1", &args)
                    .ok()
                    .flatten()
                    .unwrap_or(false)
            });
            if !primary {
                return;
            }
        }
        UPSERTS.with(|upserts| {
            if let Ok(mut upserts) = upserts.try_borrow_mut() {
                upserts.push(Upsert {
                    table_oid,
                    snapshot: (*query_desc).snapshot as usize,
                    query_desc: query_desc as usize,
                    subxact: pg_sys::GetCurrentSubTransactionId(),
                });
            }
        });
    }
}

/// Forgets the statement `query_desc` (called from `ExecutorEnd`).
///
/// # Safety
///
/// `query_desc` must be valid or null.
pub(crate) unsafe fn release(query_desc: *mut pg_sys::QueryDesc) {
    unsafe {
        if query_desc.is_null() {
            return;
        }
        let snapshot = (*query_desc).snapshot as usize;
        UPSERTS.with(|upserts| {
            if let Ok(mut upserts) = upserts.try_borrow_mut() {
                upserts.retain(|entry| entry.snapshot != snapshot);
            }
        });
    }
}

/// Drops everything (transaction end).
pub(crate) fn clear() {
    UPSERTS.with(|upserts| {
        if let Ok(mut upserts) = upserts.try_borrow_mut() {
            upserts.clear();
        }
    });
}

/// A subtransaction aborted: statements started inside it are gone.
pub(crate) fn on_subxact_abort(my: pg_sys::SubTransactionId) {
    UPSERTS.with(|upserts| {
        if let Ok(mut upserts) = upserts.try_borrow_mut() {
            upserts.retain(|entry| entry.subxact != my);
        }
    });
}

/// If the running statement is a registered upsert on `table_oid`, hydrates the conflicting key
/// of `row` (a no-op when it is already hot) and makes it visible to the statement. Returns
/// whether the guard should let the row through to native conflict handling.
pub(crate) fn hydrate_conflicting(table_oid: pg_sys::Oid, row: &serde_json::Value) -> Result<bool, String> {
    let snapshot = UPSERTS.with(|upserts| {
        upserts
            .try_borrow()
            .ok()
            .and_then(|upserts| upserts.iter().rev().find(|entry| entry.table_oid == table_oid).map(|entry| (entry.snapshot, entry.query_desc)))
    });
    let Some((snapshot, query_desc)) = snapshot else { return Ok(false) };
    let mut pk = serde_json::Map::new();
    for column in crate::sql::cold_dml::primary_key_columns(table_oid)? {
        let value = row.get(&column).cloned().unwrap_or(serde_json::Value::Null);
        pk.insert(column, value);
    }
    let hydrated = crate::sql::cold_dml::hydrate_pk_locked(table_oid, &serde_json::Value::Object(pk))?;
    if hydrated > 0 {
        // SAFETY: `snapshot` belongs to the statement still executing (entries are removed at
        // ExecutorEnd and on abort); advancing curcid reveals the row inserted just now.
        unsafe {
            pg_sys::CommandCounterIncrement();
            (*(snapshot as *mut pg_sys::SnapshotData)).curcid = pg_sys::GetCurrentCommandId(false);
            // The hydrated tuple carries the statement's old output cid; native ON CONFLICT DO UPDATE
            // rejects tuples whose cmin >= es_output_cid as "affected a second time".
            (*(*(query_desc as *mut pg_sys::QueryDesc)).estate).es_output_cid = pg_sys::GetCurrentCommandId(true);
        }
    }
    Ok(true)
}
