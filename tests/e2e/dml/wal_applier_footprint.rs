//! WAL applier process footprint: startup time and idle RSS.
//!
//! The launcher must stay a quiet PostgreSQL backend. Restart is supervisor-
//! driven (`BGW_NEVER_RESTART` + re-register), so the SLO includes the 1 s
//! child-lifecycle grace.

use std::time::Duration;

use anyhow::{bail, Result};
use koldstore_memory::format_bytes;

use crate::common;

async fn backend_pid(client: &tokio_postgres::Client) -> Result<i32> {
    Ok(client
        .query_one("SELECT pg_backend_pid()::int4", &[])
        .await?
        .get(0))
}

async fn idle_without_xact(client: &tokio_postgres::Client, pid: i32) -> Result<bool> {
    Ok(client
        .query_one(
            "SELECT xact_start IS NULL FROM pg_catalog.pg_stat_activity WHERE pid = $1",
            &[&pid],
        )
        .await?
        .get(0))
}

async fn manage_events(db: &common::TestDb, relation: &str) -> Result<()> {
    db.client
        .batch_execute(&format!(
            "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
        ))
        .await?;
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => 1000,
              auto_flush => false
            )
            "#,
            &[&relation, &db.storage_name],
        )
        .await?;
    common::wait_for_async_worker(&db.client).await?;
    common::fence_async_mirror(&db.client).await?;
    Ok(())
}

/// Idle WAL applier RSS must stay near a sibling client backend and under cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_wal_applier_rss_stays_near_a_client_backend() -> Result<()> {
    let _cluster = common::acquire_cluster_exclusive()?;
    common::require_pgrx_server().await?;
    let budget = common::memory::worker_footprint_budget_from_env();

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_footprint_rss").await?;
        let relation = db.relation(&format!("{}_events", db.schema));
        manage_events(&db, &relation).await?;

        let wal_pid = common::wal_applier_pid(&db.client).await?;
        tokio::time::sleep(Duration::from_millis(250)).await;
        anyhow::ensure!(
            idle_without_xact(&db.client, wal_pid).await?,
            "idle WAL applier pid={wal_pid} must not hold an open transaction"
        );

        let client_pid = backend_pid(&db.client).await?;
        let wal_rss = common::memory::pid_rss_bytes(wal_pid)?;
        let client_rss = common::memory::pid_rss_bytes(client_pid)?;
        let allowed = client_rss.saturating_add(budget.wal_idle_rss_slack_bytes);
        common::log_always(format!(
            "idle WAL applier pid={wal_pid} rss={} client_pid={client_pid} rss={} cap={} slack_cap={}",
            format_bytes(wal_rss),
            format_bytes(client_rss),
            format_bytes(budget.wal_idle_rss_max_bytes),
            format_bytes(allowed),
        ));
        if wal_rss > budget.wal_idle_rss_max_bytes {
            bail!(
                "idle WAL applier rss {} exceeded cap {}",
                format_bytes(wal_rss),
                format_bytes(budget.wal_idle_rss_max_bytes)
            );
        }
        if wal_rss > allowed {
            bail!(
                "idle WAL applier rss {} exceeded sibling backend {} by more than {}",
                format_bytes(wal_rss),
                format_bytes(client_rss),
                format_bytes(budget.wal_idle_rss_slack_bytes)
            );
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
        let wal_rss_later = common::memory::pid_rss_bytes(wal_pid)?;
        let idle_growth = wal_rss_later.saturating_sub(wal_rss);
        if idle_growth > 16 * 1024 * 1024 {
            bail!(
                "idle WAL applier rss grew {} while sleeping ({} → {})",
                format_bytes(idle_growth),
                format_bytes(wal_rss),
                format_bytes(wal_rss_later)
            );
        }

        db.client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
                &[&relation],
            )
            .await?;
        let _ = db
            .client
            .query_one("SELECT koldstore.disable_async_mirror()", &[])
            .await?;
    }
    Ok(())
}

/// Killing the resident applier must respawn within the startup SLO.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_applier_restart_stays_within_startup_budget() -> Result<()> {
    let _cluster = common::acquire_cluster_exclusive()?;
    common::require_pgrx_server().await?;
    let budget = common::memory::worker_footprint_budget_from_env();

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_footprint_start").await?;
        let relation = db.relation(&format!("{}_events", db.schema));
        manage_events(&db, &relation).await?;

        let original_pid = common::wal_applier_pid(&db.client).await?;
        anyhow::ensure!(
            common::terminate_async_worker(&db.client).await?,
            "expected to terminate the resident WAL applier"
        );
        let elapsed = common::wait_for_async_worker_auto_restart(&db.client, original_pid).await?;
        common::log_always(format!(
            "WAL applier restart {original_pid} -> new pid in {elapsed:?} (budget {:?})",
            budget.wal_startup
        ));
        if elapsed > budget.wal_startup {
            bail!(
                "WAL applier restart took {elapsed:?}, budget {:?}",
                budget.wal_startup
            );
        }

        let replacement = common::wal_applier_pid(&db.client).await?;
        let replacement_rss = common::memory::pid_rss_bytes(replacement)?;
        if replacement_rss > budget.wal_idle_rss_max_bytes {
            bail!(
                "restarted WAL applier rss {} exceeded cap {}",
                format_bytes(replacement_rss),
                format_bytes(budget.wal_idle_rss_max_bytes)
            );
        }

        db.client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
                &[&relation],
            )
            .await?;
        let _ = db
            .client
            .query_one("SELECT koldstore.disable_async_mirror()", &[])
            .await?;
    }
    Ok(())
}
