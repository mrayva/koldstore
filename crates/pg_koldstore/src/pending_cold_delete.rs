//! Defers cold-object-store deletion (DROP TABLE/SCHEMA cleanup, `unmanage_table`'s
//! `drop_cold`) to transaction commit -- PostgreSQL's own pending-delete pattern for
//! relation files (`smgrDoPendingDeletes`), applied to koldstore's object-store side.
//!
//! Before this module existed, both call sites deleted cold Parquet objects from
//! storage immediately, *while the DROP/unmanage DDL's own transaction was still
//! in-flight*. The catalog rows that say the table/cold-data no longer exists are
//! ordinary transactional writes and roll back cleanly on a later statement failure,
//! explicit `ROLLBACK`, or crash -- but the object-store delete already happened and
//! cannot be undone. That left a real, silent data-loss window: the catalog (after
//! rollback) says the cold data is still there, but the objects backing it are gone.
//! (#100)
//!
//! The fix mirrors what PostgreSQL itself does for file unlinks on DROP: stage the
//! keys to delete now (while `list()` naturally has them to hand, and while the
//! catalog/prefix context is still cheap to resolve), then only touch storage once
//! the transaction is known to have committed. On abort, the catalog rows are back,
//! so the objects must still exist too -- the staged list is simply discarded, never
//! physically deleted. Unlike PostgreSQL's own pending-deletes (which also delete
//! *new* files on abort), this path only ever stages deletions of objects that
//! predate the transaction, so "keep on abort" is always the correct action; there is
//! no symmetric "new object created by this xact" case to handle here.
//!
//! Deletion happens at `XACT_EVENT_COMMIT` rather than `XACT_EVENT_PRE_COMMIT`
//! precisely because the goal is "only once commit is certain" -- pre-commit still
//! runs before the commit WAL record is written. `StorageClient` calls are plain Rust
//! (object-store HTTP or filesystem I/O via `koldstore-storage`'s own async runtime),
//! not SPI, so running them post-commit is safe; compare
//! `row_counter_cache::flush_pending_deltas`, which genuinely does need SPI and is
//! therefore run at `XACT_EVENT_PRE_COMMIT` instead.

use std::cell::RefCell;

use koldstore_catalog::decode::FlushStorageContext;
use koldstore_storage::StorageClient;

/// One managed table's already-`list()`-ed cold objects, staged for deletion once the
/// transaction tearing down the table is known to have committed.
struct PendingDeletion {
    storage: FlushStorageContext,
    table_oid: u32,
    keys: Vec<String>,
}

std::thread_local! {
    static PENDING: RefCell<Vec<PendingDeletion>> = const { RefCell::new(Vec::new()) };
}

/// Stages `keys` (already listed under the table's cold-object prefix by the caller)
/// for deletion after this transaction commits. Call this in place of deleting
/// immediately from `DROP TABLE`/`DROP SCHEMA`/`unmanage_table`'s cleanup paths.
pub(crate) fn stage(storage: FlushStorageContext, table_oid: u32, keys: Vec<String>) {
    if keys.is_empty() {
        return;
    }
    PENDING.with(|pending| {
        pending.borrow_mut().push(PendingDeletion {
            storage,
            table_oid,
            keys,
        });
    });
}

/// Registers the permanent xact callback that performs (on commit) or discards (on
/// abort) staged deletions. Call once at extension init.
#[cfg(feature = "pg")]
pub fn register_xact_callback() {
    unsafe {
        pgrx::pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
    }
}

#[cfg(feature = "pg")]
#[pgrx::pg_guard]
unsafe extern "C-unwind" fn xact_callback(
    event: pgrx::pg_sys::XactEvent::Type,
    _arg: *mut std::ffi::c_void,
) {
    match event {
        pgrx::pg_sys::XactEvent::XACT_EVENT_COMMIT
        | pgrx::pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT => {
            delete_staged();
        }
        pgrx::pg_sys::XactEvent::XACT_EVENT_ABORT
        | pgrx::pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT => {
            // The catalog rows describing these objects rolled back too, so the
            // objects must still exist: discard the staged list, never delete.
            PENDING.with(|pending| pending.borrow_mut().clear());
        }
        _ => {}
    }
}

#[cfg(feature = "pg")]
fn delete_staged() {
    let staged = PENDING.with(|pending| std::mem::take(&mut *pending.borrow_mut()));
    for entry in staged {
        let client = match crate::object_store::open_managed_object_store_client(
            &entry.storage.storage_type,
            &entry.storage.base_path,
            &entry.storage.credentials,
            &entry.storage.config,
        ) {
            Ok(client) => client,
            Err(error) => {
                pgrx::warning!(
                    "koldstore: post-commit cold-object cleanup for table_oid={} failed to open storage, {} object(s) left orphaned: {error}",
                    entry.table_oid,
                    entry.keys.len()
                );
                continue;
            }
        };
        let mut deleted = 0_usize;
        for key in &entry.keys {
            if let Err(error) = client.delete(key) {
                pgrx::warning!(
                    "koldstore: post-commit cleanup: table_oid={} failed to delete cold object {key}: {error}",
                    entry.table_oid
                );
                continue;
            }
            deleted += 1;
        }
        pgrx::log!(
            "koldstore: post-commit cleanup: table_oid={} deleted_objects={}/{}",
            entry.table_oid,
            deleted,
            entry.keys.len()
        );
    }
}
