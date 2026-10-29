//! Persistent worker that keeps follower tables in step with their leader's cold tier
//! (docs/plans/multi-master-single-tiering-leader.md).
//!
//! One worker is registered at startup for each database listed in
//! `koldstore.follower_sync_databases` when `koldstore.follower_sync_interval_seconds` is
//! positive. On every tick it calls `koldstore.sync_from_leader()` for each table marked
//! with `koldstore.set_table_follower()`, one transaction per table so a table whose leader
//! has not published yet (or whose storage is unreachable) is logged and retried on the
//! next tick without holding up the others.

use std::time::Duration;

use koldstore_supervisor::LIBRARY_NAME;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags};

const WORKER_FUNCTION: &str = "koldstore_follower_sync_main";
const WORKER_TYPE: &str = "koldstore follower sync";
/// Delay before the first run, so a restarting cluster settles first.
const FIRST_RUN_DELAY: Duration = Duration::from_secs(5);

/// Registers the follower sync workers (only while preloading, only when enabled).
pub(crate) fn register_if_shared_preload() {
    if !unsafe { pgrx::pg_sys::process_shared_preload_libraries_in_progress } {
        return;
    }
    if crate::guc::follower_sync_interval_seconds() <= 0 {
        return;
    }
    let databases = crate::guc::follower_sync_databases();
    if databases.is_empty() {
        pgrx::warning!(
            "koldstore.follower_sync_interval_seconds is set but koldstore.follower_sync_databases is empty; \
             no follower sync worker started"
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
pub extern "C-unwind" fn koldstore_follower_sync_main(argument: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let Some(database) = crate::guc::follower_sync_databases().into_iter().nth(argument.value()) else {
        return;
    };
    BackgroundWorker::connect_worker_to_spi(Some(&database), None);
    let interval = Duration::from_secs(u64::try_from(crate::guc::follower_sync_interval_seconds()).unwrap_or(30).max(1));
    pgrx::log!("koldstore follower sync: database={database} interval={}s", interval.as_secs());

    let mut delay = FIRST_RUN_DELAY;
    while super::wait_latch_interruptible(Some(delay)) {
        delay = interval;
        let tables = match super::txn::run_recoverable("follower sync list", list_followers) {
            Ok(tables) => tables,
            Err(error) => {
                pgrx::warning!("koldstore follower sync: database={database} listing failed: {error}");
                continue;
            }
        };
        for table_oid in tables {
            match super::txn::run_recoverable("follower sync", || sync_one(table_oid)) {
                Ok(Some(summary)) => {
                    if summary_has_activity(&summary) {
                        pgrx::log!("koldstore follower sync: database={database} table oid {table_oid} {summary}");
                    }
                }
                Ok(None) => {}
                Err(error) => pgrx::warning!(
                    "koldstore follower sync: database={database} table oid {table_oid} skipped: {error}"
                ),
            }
        }
    }
    pgrx::log!("koldstore follower sync: database={database} stopping");
}

/// Oids of the active managed tables marked as followers; empty when the extension is absent.
fn list_followers() -> Result<Vec<u32>, String> {
    pgrx::Spi::connect(|client| {
        if client
            .select("SELECT to_regclass('koldstore.schemas') IS NOT NULL", None, &[])?
            .first()
            .get_one::<bool>()?
            != Some(true)
        {
            return Ok(Vec::new());
        }
        client
            .select(
                "SELECT table_oid::oid FROM koldstore.schemas \
                 WHERE active AND coalesce((options->>'follower')::boolean, false) ORDER BY table_oid",
                None,
                &[],
            )?
            .map(|row| Ok(row.get::<pgrx::pg_sys::Oid>(1)?.map_or(0, pgrx::pg_sys::Oid::to_u32)))
            .collect::<Result<Vec<_>, pgrx::spi::Error>>()
    })
    .map_err(|error| error.to_string())
}

/// Syncs one follower; `Ok(None)` when it was dropped since the listing.
fn sync_one(table_oid: u32) -> Result<Option<String>, String> {
    pgrx::Spi::get_one::<String>(&format!(
        "SELECT CASE WHEN to_regclass({table_oid}::oid::regclass::text) IS NULL THEN NULL \
                     ELSE koldstore.sync_from_leader({table_oid}::oid::regclass)::text END"
    ))
    .map_err(|error| error.to_string())
}

fn summary_has_activity(summary: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(summary).is_ok_and(|value| {
        ["added", "removed"]
            .iter()
            .any(|key| value.get(key).and_then(serde_json::Value::as_i64).unwrap_or(0) > 0)
    })
}
