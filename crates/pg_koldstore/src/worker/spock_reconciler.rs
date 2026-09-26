//! Persistent worker that runs `koldstore.reconcile_spock_conflicts()` on an interval
//! (docs/multi-master.md).
//!
//! One worker is registered at startup for each database listed in
//! `koldstore.spock_reconcile_databases` when
//! `koldstore.spock_reconcile_interval_seconds` is positive. It is deliberately not part
//! of the supervisor's event-driven maintenance: it is a plain timer loop that connects
//! to its database, calls the SQL function in its own transaction, logs anything it did,
//! and sleeps. A failed run (extension or Spock not installed yet, a transient error) is
//! logged and retried on the next tick; the reconciler itself records permanent per-item
//! failures in `koldstore.spock_reconciled`.

use std::time::Duration;

use koldstore_supervisor::LIBRARY_NAME;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags};

const WORKER_FUNCTION: &str = "koldstore_spock_reconciler_main";
const WORKER_TYPE: &str = "koldstore spock reconciler";
/// Delay before the first run, so a restarting cluster settles first.
const FIRST_RUN_DELAY: Duration = Duration::from_secs(5);
/// Items handled per run; a backlog is drained over successive runs.
const ITEMS_PER_RUN: i32 = 200;

/// Registers the reconciler workers (only while preloading, only when enabled).
pub(crate) fn register_if_shared_preload() {
    if !unsafe { pgrx::pg_sys::process_shared_preload_libraries_in_progress } {
        return;
    }
    if crate::guc::spock_reconcile_interval_seconds() <= 0 {
        return;
    }
    let databases = crate::guc::spock_reconcile_databases();
    if databases.is_empty() {
        pgrx::warning!(
            "koldstore.spock_reconcile_interval_seconds is set but koldstore.spock_reconcile_databases is empty; \
             no Spock reconciler worker started"
        );
        return;
    }
    if !crate::guc::capture_replicated_changes() {
        pgrx::warning!(
            "koldstore Spock reconciler needs koldstore.capture_replicated_changes = on; no worker started"
        );
        return;
    }
    for (index, database) in databases.iter().enumerate() {
        BackgroundWorkerBuilder::new(&format!("{WORKER_TYPE} {database}"))
            .set_type(WORKER_TYPE)
            .set_library(LIBRARY_NAME)
            .set_function(WORKER_FUNCTION)
            .set_argument(Some(pgrx::pg_sys::Datum::from(index)))
            .enable_spi_access()
            .set_start_time(BgWorkerStartTime::RecoveryFinished)
            .set_restart_time(Some(Duration::from_secs(10)))
            .load();
    }
}

#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn koldstore_spock_reconciler_main(argument: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let Some(database) = crate::guc::spock_reconcile_databases().into_iter().nth(argument.value()) else {
        return;
    };
    BackgroundWorker::connect_worker_to_spi(Some(&database), None);
    let interval = Duration::from_secs(u64::try_from(crate::guc::spock_reconcile_interval_seconds()).unwrap_or(60).max(1));
    pgrx::log!("koldstore spock reconciler: database={database} interval={}s", interval.as_secs());

    let mut delay = FIRST_RUN_DELAY;
    while BackgroundWorker::wait_latch(Some(delay)) {
        delay = interval;
        match super::txn::run_recoverable("spock reconciler", reconcile_once) {
            Ok(Some(summary)) => {
                if summary_has_activity(&summary) {
                    pgrx::log!("koldstore spock reconciler: database={database} {summary}");
                }
            }
            Ok(None) => {}
            Err(error) => pgrx::warning!("koldstore spock reconciler: database={database} run failed: {error}"),
        }
    }
    pgrx::log!("koldstore spock reconciler: database={database} stopping");
}

/// One run; `Ok(None)` when the extension or Spock is not (yet) installed here.
fn reconcile_once() -> Result<Option<String>, String> {
    let sql = format!(
        "SELECT CASE WHEN to_regprocedure('koldstore.reconcile_spock_conflicts(integer)') IS NULL \
                       OR to_regclass('spock.exception_log') IS NULL THEN NULL \
                     ELSE koldstore.reconcile_spock_conflicts({ITEMS_PER_RUN})::text END"
    );
    pgrx::Spi::get_one::<String>(&sql).map_err(|error| error.to_string())
}

fn summary_has_activity(summary: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(summary).is_ok_and(|value| {
        ["transactions", "deletes", "failed"]
            .iter()
            .any(|key| value.get(key).and_then(serde_json::Value::as_i64).unwrap_or(0) > 0)
    })
}
