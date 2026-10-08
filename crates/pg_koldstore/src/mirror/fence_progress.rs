//! Commit-published progress of foreground read fences.
//!
//! A hydrate-on-write read fence ([`super::apply::fence_for_read`]) applies WAL into the mirror
//! inside the caller's own (sub)transaction and deliberately records and acknowledges nothing:
//! the transaction may still abort, and advancing the replication slot past WAL whose mirror
//! rows then roll back loses tombstones. The price was that nobody ever moved the slot while
//! hydrators held its lock back to back, so every fence re-decoded and re-applied an ever longer
//! backlog (about 12 row changes per fence with one client, 200 and growing with eight).
//!
//! Once the transaction *commits*, though, its mirror rows are durable, and everything up to the
//! fence it applied through is committed mirror state. Each fence's LSN is remembered with the
//! subtransaction level it ran at ([`koldstore_wal_mirror::wal::fence_pending::PendingFences`]),
//! dropped when that (sub)transaction aborts, and the maximum is published to the shared
//! `applied_through` watermark at top-level commit. The next slot-lock holder acknowledges the
//! slot up to that watermark (see `apply_bounded_locked`), exactly as it already does for the
//! applier's own committed progress.

use std::cell::RefCell;

use koldstore_wal_mirror::wal::fence_pending::PendingFences;
use pgrx::pg_sys;

thread_local! {
    static PENDING: RefCell<PendingFences> = const { RefCell::new(PendingFences::new()) };
}

/// Remembers that this (sub)transaction applied the mirror through `lsn`.
pub(crate) fn note_fence(lsn: u64) {
    let level = unsafe { pg_sys::GetCurrentTransactionNestLevel() }.max(1) as u32;
    PENDING.with(|pending| pending.borrow_mut().note(level, lsn));
}

/// Subtransaction commit folds its fences into the parent; abort drops them (the mirror rows
/// they wrote are rolled back with the subtransaction).
pub(crate) fn on_subxact(event: pg_sys::SubXactEvent::Type, nesting_level: u32) {
    PENDING.with(|pending| {
        let mut pending = pending.borrow_mut();
        match event {
            pg_sys::SubXactEvent::SUBXACT_EVENT_COMMIT_SUB => {
                pending.commit_subtransaction(nesting_level)
            }
            pg_sys::SubXactEvent::SUBXACT_EVENT_ABORT_SUB => {
                pending.abort_subtransaction(nesting_level)
            }
            _ => {}
        }
    });
}

/// Top-level commit: everything still pending is now committed mirror state.
pub(crate) fn on_commit() {
    if let Some(lsn) = PENDING.with(|pending| pending.borrow_mut().take_committed()) {
        let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
        crate::worker::wal::record_applied_through(database_oid, lsn);
    }
}

/// Top-level abort or prepare: nothing pending is committed.
pub(crate) fn on_abort() {
    PENDING.with(|pending| pending.borrow_mut().clear());
}
