//! Persistent worker that keeps the leader's published catalog and its followers in step
//! (docs/plans/multi-master-single-tiering-leader.md).
//!
//! One worker is registered at startup for each database listed in
//! `koldstore.catalog_sync_databases` when `koldstore.catalog_sync_interval_seconds` is
//! positive. On every tick it
//!
//! * calls `koldstore.publish_leader_catalog()` for each table marked with
//!   `koldstore.set_table_leader()` whose manifest generation moved since the last publish
//!   (publishing from this worker, not from the flush transaction, means followers only
//!   ever see committed state), and
//! * calls `koldstore.sync_from_leader()` for each table marked with
//!   `koldstore.set_table_follower()`.
//!
//! Each table runs in its own transaction, so a table whose leader has not published yet (or
//! whose storage is unreachable) is logged and retried on the next tick without holding up
//! the others.

use std::collections::HashMap;
use std::time::Duration;

use koldstore_supervisor::LIBRARY_NAME;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags};

const WORKER_FUNCTION: &str = "koldstore_catalog_sync_main";
const WORKER_TYPE: &str = "koldstore catalog sync";
/// Delay before the first run, so a restarting cluster settles first.
const FIRST_RUN_DELAY: Duration = Duration::from_secs(5);

/// Registers the catalog sync workers (only while preloading, only when enabled).
pub(crate) fn register_if_shared_preload() {
    if !unsafe { pgrx::pg_sys::process_shared_preload_libraries_in_progress } {
        return;
    }
    if crate::guc::catalog_sync_interval_seconds() <= 0 {
        return;
    }
    let databases = crate::guc::catalog_sync_databases();
    if databases.is_empty() {
        pgrx::warning!(
            "koldstore.catalog_sync_interval_seconds is set but koldstore.catalog_sync_databases is empty; \
             no catalog sync worker started"
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
pub extern "C-unwind" fn koldstore_catalog_sync_main(argument: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let Some(database) = crate::guc::catalog_sync_databases().into_iter().nth(argument.value()) else {
        return;
    };
    BackgroundWorker::connect_worker_to_spi(Some(&database), None);
    let interval = Duration::from_secs(u64::try_from(crate::guc::catalog_sync_interval_seconds()).unwrap_or(30).max(1));
    pgrx::log!("koldstore catalog sync: database={database} interval={}s", interval.as_secs());

    let mut delay = FIRST_RUN_DELAY;
    // Generation last published per leader table, so an idle table costs one cheap query.
    let mut published: HashMap<u32, String> = HashMap::new();
    // Etag of the catalog object right after our own publish: a different one later means a
    // deposed leader overwrote it (followers ignore that content), so it is published again.
    let mut etags: HashMap<u32, Option<String>> = HashMap::new();
    while super::wait_latch_interruptible(Some(delay)) {
        delay = interval;
        let tables = match super::txn::run_recoverable("catalog sync list", list_tables) {
            Ok(tables) => tables,
            Err(error) => {
                pgrx::warning!("koldstore catalog sync: database={database} listing failed: {error}");
                continue;
            }
        };
        published.retain(|oid, _| tables.iter().any(|(table, role)| table == oid && *role == Role::Leader));
        etags.retain(|oid, _| published.contains_key(oid));
        for (table_oid, role) in tables {
            let result = match role {
                Role::Follower => {
                    let synced = super::txn::run_recoverable("follower sync", || sync_one(table_oid));
                    if crate::guc::auto_elect() {
                        let advertised = super::txn::run_recoverable("candidacy", || {
                            crate::sql::leader_lease::publish_candidacy(pgrx::pg_sys::Oid::from(table_oid))
                        });
                        if let Err(error) = advertised {
                            pgrx::log!("koldstore catalog sync: database={database} table oid {table_oid} candidacy not published: {error}");
                        }
                        elect(&database, table_oid);
                    }
                    synced
                }
                Role::Leader => super::txn::run_recoverable("leader publish", || {
                    if crate::guc::auto_elect() {
                        if !maintain(&database, table_oid)? {
                            return Ok(None);
                        }
                    } else {
                        renew_lease(table_oid)?;
                    }
                    let clobbered = etags.get(&table_oid).is_some_and(|recorded| {
                        crate::sql::table_metadata::catalog_etag(pgrx::pg_sys::Oid::from(table_oid))
                            .is_ok_and(|current| &current != recorded)
                    });
                    if clobbered {
                        pgrx::warning!(
                            "koldstore catalog sync: database={database} table oid {table_oid} the published catalog changed behind this leader; publishing again"
                        );
                    }
                    publish_one(table_oid, if clobbered { None } else { published.get(&table_oid).map(String::as_str) })
                })
                .map(|outcome| {
                    outcome.map(|(fingerprint, summary)| {
                        published.insert(table_oid, fingerprint);
                        let etag = super::txn::run_recoverable("catalog etag", || {
                            crate::sql::table_metadata::catalog_etag(pgrx::pg_sys::Oid::from(table_oid))
                        });
                        etags.insert(table_oid, etag.ok().flatten());
                        summary
                    })
                }),
            };
            match result {
                Ok(Some(summary)) => {
                    if role == Role::Leader || summary_has_activity(&summary) {
                        pgrx::log!("koldstore catalog sync: database={database} table oid {table_oid} {summary}");
                    }
                }
                Ok(None) => {}
                Err(error) => pgrx::warning!(
                    "koldstore catalog sync: database={database} table oid {table_oid} skipped: {error}"
                ),
            }
        }
    }
    pgrx::log!("koldstore catalog sync: database={database} stopping");
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Leader,
    Follower,
}

/// Active managed tables marked as leader or follower; empty when the extension is absent.
fn list_tables() -> Result<Vec<(u32, Role)>, String> {
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
                "SELECT table_oid::oid, coalesce((options->>'follower')::boolean, false) \
                 FROM koldstore.schemas \
                 WHERE active AND (coalesce((options->>'follower')::boolean, false) \
                                   OR coalesce((options->>'leader')::boolean, false)) \
                 ORDER BY table_oid",
                None,
                &[],
            )?
            .map(|row| {
                let oid = row.get::<pgrx::pg_sys::Oid>(1)?.map_or(0, pgrx::pg_sys::Oid::to_u32);
                let follower = row.get::<bool>(2)?.unwrap_or(false);
                Ok((oid, if follower { Role::Follower } else { Role::Leader }))
            })
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

/// Publishes one leader's catalog when its generation moved; `Ok(None)` when nothing changed or
/// the table was dropped. Returns the published generation with the report.
fn publish_one(table_oid: u32, last: Option<&str>) -> Result<Option<(String, String)>, String> {
    let generation = pgrx::Spi::get_one::<i64>(&format!(
        "SELECT (SELECT COALESCE(max(generation), 0)::bigint FROM koldstore.manifest WHERE table_oid = {table_oid})"
    ))
    .map_err(|error| error.to_string())?
    .unwrap_or(0);
    // The unflushed delete markers ride in the catalog too, so a change in them republishes.
    let fingerprint = format!(
        "{generation}/{}",
        crate::sql::flush::peer_prune::tombstone_fingerprint(pgrx::pg_sys::Oid::from(table_oid)).unwrap_or_default()
    );
    if last == Some(fingerprint.as_str()) {
        return Ok(None);
    }
    let summary = pgrx::Spi::get_one::<String>(&format!(
        "SELECT CASE WHEN to_regclass({table_oid}::oid::regclass::text) IS NULL THEN NULL \
                     ELSE koldstore.publish_leader_catalog({table_oid}::oid::regclass)::text END"
    ))
    .map_err(|error| error.to_string())?;
    Ok(summary.map(|summary| (fingerprint, summary)))
}

/// Renews the leader lease of a table that holds one; a lost lease surfaces as the error that
/// skips this tick's publish (flush and compaction are already fenced by the same check).
fn renew_lease(table_oid: u32) -> Result<(), String> {
    pgrx::Spi::run(&format!(
        "SELECT CASE WHEN to_regclass({table_oid}::oid::regclass::text) IS NOT NULL \
                      AND coalesce((SELECT options ? 'lease_epoch' FROM koldstore.schemas \
                                    WHERE table_oid = {table_oid} AND active), false) \
                     THEN koldstore.renew_leader_lease({table_oid}::oid::regclass)::text END"
    ))
    .map_err(|error| error.to_string())
}

/// Claims leadership of a follower table whose leader's lease ran out. Any failure (lease still
/// alive, node behind, lost race) leaves the table a follower and is retried on the next tick.
fn elect(database: &str, table_oid: u32) {
    let result = super::txn::run_recoverable("election", || {
        if pgrx::Spi::get_one::<bool>(&format!("SELECT to_regclass({table_oid}::oid::regclass::text) IS NULL"))
            .map_err(|error| error.to_string())?
            == Some(true)
        {
            return Ok(None);
        }
        crate::sql::leader_lease::try_elect(pgrx::pg_sys::Oid::from(table_oid))
    });
    match result {
        Ok(Some(lease)) => pgrx::log!(
            "koldstore catalog sync: database={database} table oid {table_oid} elected leader, lease {lease}"
        ),
        Ok(None) => {}
        Err(error) => pgrx::log!("koldstore catalog sync: database={database} table oid {table_oid} not elected: {error}"),
    }
}

/// Renews a leader's lease, reclaiming an expired one or demoting the table when another node
/// holds it. Returns whether the table is still a leader (a demotion commits, so it is not an error).
fn maintain(database: &str, table_oid: u32) -> Result<bool, String> {
    use crate::sql::leader_lease::Maintained;

    let oid = pgrx::pg_sys::Oid::from(table_oid);
    if pgrx::Spi::get_one::<bool>(&format!(
        "SELECT to_regclass({table_oid}::oid::regclass::text) IS NULL \
             OR NOT coalesce((SELECT options ? 'lease_epoch' FROM koldstore.schemas WHERE table_oid = {table_oid} AND active), false)"
    ))
    .map_err(|error| error.to_string())?
        == Some(true)
    {
        return Ok(true);
    }
    match crate::sql::leader_lease::maintain_leader(oid)? {
        Maintained::Renewed => Ok(true),
        Maintained::Reclaimed(epoch) => {
            pgrx::log!("koldstore catalog sync: database={database} table oid {table_oid} reclaimed expired lease as epoch {epoch}");
            Ok(true)
        }
        Maintained::Demoted(reason) => {
            pgrx::warning!(
                "koldstore catalog sync: database={database} table oid {table_oid} demoted to follower: {reason}"
            );
            Ok(false)
        }
    }
}

fn summary_has_activity(summary: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(summary).is_ok_and(|value| {
        ["added", "removed"]
            .iter()
            .any(|key| value.get(key).and_then(serde_json::Value::as_i64).unwrap_or(0) > 0)
    })
}
