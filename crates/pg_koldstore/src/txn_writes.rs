//! Same-transaction visibility guard for cold reads (upstream #121, the
//! "fail closed" option).
//!
//! Cold rows are masked by the async mirror, which is fed by logical decoding,
//! and logical decoding cannot see uncommitted work. So once a transaction has
//! modified a managed table, a read in that same transaction that has to consult
//! cold storage can return a cold row whose key the transaction already changed
//! or deleted: `koldstore.delete_row()` followed by a `SELECT` of that key in
//! the same transaction shows the row again. `koldstore.wait_for_async_mirror()`
//! cannot help; it only covers committed WAL.
//!
//! Rather than return a stale row, a KoldMergeScan that must consult cold data
//! fails with a clear error when the transaction (or an enclosing one) has
//! already written the scanned managed table. Writes are recorded per
//! subtransaction so a rolled-back savepoint clears its own entries, and the
//! whole set is dropped when the transaction ends. `koldstore.allow_same_txn_cold_reads`
//! accepts the risk.
//!
//! Extension-internal cold lookups (the write guard's existence probes, the
//! `hydrate_pk` / `update_row` / `delete_row` helpers) must keep working inside a
//! transaction that already wrote the table, so they run under
//! [`with_check_suppressed`] or with the write guard suspended.

use std::cell::{Cell, RefCell};

use pgrx::pg_sys;

thread_local! {
    /// `(managed relation, subtransaction that wrote it)`.
    static WRITTEN: RefCell<Vec<(pg_sys::Oid, pg_sys::SubTransactionId)>> =
        const { RefCell::new(Vec::new()) };
    static SUPPRESSED: Cell<u32> = const { Cell::new(0) };
}

/// Records that the current (sub)transaction wrote managed relation `relid`.
pub(crate) fn record_managed_write(relid: pg_sys::Oid) {
    // SAFETY: plain backend-local read of transaction state.
    let subid = unsafe { pg_sys::GetCurrentSubTransactionId() };
    WRITTEN.with(|written| {
        if let Ok(mut written) = written.try_borrow_mut() {
            if !written.iter().any(|&(oid, sub)| oid == relid && sub == subid) {
                written.push((relid, subid));
            }
        }
    });
}

/// Drops every recorded write; called when the top-level transaction ends.
pub(crate) fn clear() {
    WRITTEN.with(|written| {
        if let Ok(mut written) = written.try_borrow_mut() {
            written.clear();
        }
    });
}

/// A subtransaction committed: its writes now belong to its parent.
pub(crate) fn on_subxact_commit(my: pg_sys::SubTransactionId, parent: pg_sys::SubTransactionId) {
    WRITTEN.with(|written| {
        if let Ok(mut written) = written.try_borrow_mut() {
            for entry in written.iter_mut() {
                if entry.1 == my {
                    entry.1 = parent;
                }
            }
            written.sort_unstable_by_key(|&(oid, sub)| (u32::from(oid), sub));
            written.dedup();
        }
    });
}

/// A subtransaction aborted: its writes were rolled back with it.
pub(crate) fn on_subxact_abort(my: pg_sys::SubTransactionId) {
    WRITTEN.with(|written| {
        if let Ok(mut written) = written.try_borrow_mut() {
            written.retain(|&(_, sub)| sub != my);
        }
    });
}

/// True when this transaction (or an enclosing one) already wrote `relid`.
#[must_use]
pub(crate) fn was_written(relid: pg_sys::Oid) -> bool {
    WRITTEN.with(|written| {
        written
            .try_borrow()
            .map(|written| written.iter().any(|&(oid, _)| oid == relid))
            .unwrap_or(false)
    })
}

/// Runs `f` with the same-transaction check disabled (extension-internal cold
/// lookups). Safe to nest.
pub(crate) fn with_check_suppressed<T>(f: impl FnOnce() -> T) -> T {
    SUPPRESSED.with(|depth| depth.set(depth.get() + 1));
    let result = f();
    SUPPRESSED.with(|depth| depth.set(depth.get().saturating_sub(1)));
    result
}

/// Called right before a KoldMergeScan starts consulting cold data of `relid`.
///
/// Raises an error when the transaction already wrote the table, unless the
/// check is disabled by the GUC, suppressed for internal work, or the write
/// guard is suspended (the `hydrate_pk` / `update_row` / `delete_row` helpers).
pub(crate) fn enforce_before_cold_read(relid: pg_sys::Oid) {
    enforce_serializable_policy(relid);
    if crate::guc::allow_same_txn_cold_reads()
        || SUPPRESSED.with(Cell::get) > 0
        || crate::sql::cold_dml::guard::suspended()
        || !was_written(relid)
    {
        return;
    }
    let name = relation_display_name(relid);
    pgrx::ereport!(
        pgrx::PgLogLevel::ERROR,
        pgrx::PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
        format!(
            "koldstore: refusing to read cold data of managed table {name} -- this transaction has \
             already modified it, and a cold row for a key changed here could be returned stale \
             (logical decoding cannot see uncommitted work; upstream issue #121). COMMIT first (then \
             call koldstore.wait_for_async_mirror() if you need to see the effect immediately), or \
             set koldstore.allow_same_txn_cold_reads = on to accept the risk"
        )
    );
}

pub(crate) fn relation_display_name(relid: pg_sys::Oid) -> String {
    // SAFETY: syscache lookups on a valid relation OID; results are palloc'd
    // C strings owned by the current memory context.
    unsafe {
        let rel = pg_sys::get_rel_name(relid);
        if rel.is_null() {
            return format!("(oid {})", u32::from(relid));
        }
        let rel_name = std::ffi::CStr::from_ptr(rel).to_string_lossy().into_owned();
        let schema = pg_sys::get_namespace_name(pg_sys::get_rel_namespace(relid));
        if schema.is_null() {
            return rel_name;
        }
        let schema_name = std::ffi::CStr::from_ptr(schema).to_string_lossy().into_owned();
        format!("{schema_name}.{rel_name}")
    }
}

/// `koldstore.reject_serializable_cold_reads`: cold rows take no SSI predicate
/// locks, so a SERIALIZABLE transaction reading them does not get the usual
/// guarantee for those rows; when the policy is on, refuse instead of running.
fn enforce_serializable_policy(relid: pg_sys::Oid) {
    if !crate::guc::reject_serializable_cold_reads() || SUPPRESSED.with(Cell::get) > 0 {
        return;
    }
    // SAFETY: plain backend-local read of the session's isolation level.
    let serializable = unsafe { pg_sys::XactIsoLevel } == pg_sys::XACT_SERIALIZABLE as i32;
    if serializable {
        let name = relation_display_name(relid);
        pgrx::ereport!(
            pgrx::PgLogLevel::ERROR,
            pgrx::PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            format!(
                "koldstore: refusing to read cold data of managed table {name} under SERIALIZABLE isolation \
                 -- cold rows take no predicate locks, so serializability is not guaranteed for them \
                 (koldstore.reject_serializable_cold_reads is on; upstream issue #125). Use REPEATABLE READ, \
                 or turn the setting off to accept the weaker guarantee"
            )
        );
    }
}
