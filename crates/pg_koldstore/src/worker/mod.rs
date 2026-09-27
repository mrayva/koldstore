//! PostgreSQL adapter for the [`koldstore_supervisor`] supervision tree.
//!
//! The static cluster supervisor owns dynamic worker registration. WAL apply is
//! a persistent per-database, latch-driven service; scheduled maintenance and
//! heavy flush executors remain ephemeral.

#[cfg(feature = "pg")]
mod control;
#[cfg(feature = "pg")]
mod flush_executor;
#[cfg(feature = "pg")]
mod flush_task;
#[cfg(feature = "pg")]
mod maintenance;
#[cfg(feature = "pg")]
mod proc_latch;
#[cfg(feature = "pg")]
pub(crate) mod spock_reconciler;
#[cfg(feature = "pg")]
mod supervisor;
#[cfg(feature = "pg")]
pub(crate) mod txn;
#[cfg(feature = "pg")]
pub(crate) mod wake;
#[cfg(feature = "pg")]
pub(crate) mod wal;

#[cfg(feature = "pg")]
pub(crate) use control::require_async_mirror_worker;
#[cfg(feature = "pg")]
pub(crate) use flush_executor::register_flush_executor_from_supervisor;
#[cfg(feature = "pg")]
pub use flush_task::run_flush_scheduler_tick_pg;
#[cfg(feature = "pg")]
pub(crate) use flush_task::schedule_policy_after_counter;
#[cfg(feature = "pg")]
pub(crate) use maintenance::register_maintenance_from_supervisor;
#[cfg(feature = "pg")]
pub(crate) use supervisor::register_if_shared_preload as register_supervisor_if_shared_preload;
#[cfg(feature = "pg")]
pub(crate) use wal::register_from_supervisor as register_wal_applier_from_supervisor;

/// `BackgroundWorker::wait_latch` followed by `CHECK_FOR_INTERRUPTS`.
///
/// pgrx's wait never processes interrupts, so a worker idling in it never absorbed a
/// `ProcSignalBarrier`: `DROP DATABASE ... WITH (FORCE)` (and anything else that emits a barrier)
/// waited forever for an idle worker. Same return value as the pgrx call.
pub(crate) fn wait_latch_interruptible(timeout: Option<std::time::Duration>) -> bool {
    let keep_running = pgrx::bgworkers::BackgroundWorker::wait_latch(timeout);
    pgrx::check_for_interrupts!();
    keep_running
}
