//! Flush executor process footprint: startup time, RSS, and concurrent latency.
//!
//! Queue-mode executors are one-shot backends. They must appear quickly, stay
//! O(file) in RSS at product defaults, and not stall hot PK work on other
//! sessions while Parquet encode runs.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use koldstore_memory::format_bytes;

use crate::common;

const SEED_ROWS: i64 = 8_000;
const HOT_ROW_LIMIT: i64 = 1_000;

async fn enable_queue_flush(db: &common::TestDb) -> Result<String> {
    let dbname: String = db
        .client
        .query_one("SELECT current_database()::text", &[])
        .await?
        .get(0);
    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" SET koldstore.flush_execution = 'queue'; \
             ALTER DATABASE \"{dbname}\" SET koldstore.max_parallel_flush_jobs = 1; \
             SET koldstore.flush_execution = 'queue'; \
             SET koldstore.max_parallel_flush_jobs = 1;"
        ))
        .await
        .context("enable queue flush_execution")?;
    Ok(dbname)
}

async fn reset_flush_execution(db: &common::TestDb, dbname: &str) -> Result<()> {
    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" RESET koldstore.flush_execution; \
             ALTER DATABASE \"{dbname}\" RESET koldstore.max_parallel_flush_jobs; \
             RESET koldstore.flush_execution; \
             RESET koldstore.max_parallel_flush_jobs;"
        ))
        .await
        .ok();
    Ok(())
}

async fn manage_for_queue_flush(db: &common::TestDb, relation: &str) -> Result<()> {
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => $3::bigint,
              min_flush_rows => 1,
              max_rows_per_file => 1000,
              migration_order_by => 'id',
              auto_flush => false
            )
            "#,
            &[&relation, &db.storage_name, &HOT_ROW_LIMIT],
        )
        .await?;
    common::wait_for_async_worker(&db.client).await?;
    common::fence_async_mirror(&db.client).await?;
    Ok(())
}

fn median_duration(samples: &mut [Duration]) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

/// Queue flush executor must start within budget and stay under the RSS cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_flush_executor_startup_and_rss_stay_within_budget() -> Result<()> {
    common::require_pgrx_server().await?;
    let budget = common::memory::worker_footprint_budget_from_env();

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_footprint_rss").await?;
        let dbname = enable_queue_flush(&db).await?;
        let table = db
            .create_indexed_items_table("flush_footprint_items", SEED_ROWS)
            .await?;
        manage_for_queue_flush(&db, &table.relation).await?;

        let stop = Arc::new(AtomicBool::new(false));
        let peak_rss = Arc::new(AtomicU64::new(0));
        let seen_pid = Arc::new(AtomicU64::new(0));
        let poll_client = common::connect_peer(&db).await?;
        let poll_stop = Arc::clone(&stop);
        let poll_peak = Arc::clone(&peak_rss);
        let poll_seen = Arc::clone(&seen_pid);
        let poller = tokio::spawn(async move {
            while !poll_stop.load(Ordering::Relaxed) {
                if let Ok(pids) = common::flush_executor_pids(&poll_client).await {
                    for pid in pids {
                        poll_seen.store(u64::try_from(pid).unwrap_or(0), Ordering::Relaxed);
                        if let Ok(rss) = common::memory::pid_rss_bytes(pid) {
                            poll_peak.fetch_max(rss, Ordering::Relaxed);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        });

        let started = Instant::now();
        let job_id = common::flush_table_job_id(&db.client, &table.relation, true)
            .await?
            .context("force flush must return a job id")?;
        let mut appeared = started.elapsed();
        let wait_deadline = budget
            .flush_startup
            .saturating_add(Duration::from_millis(250));
        let wait_until = Instant::now() + wait_deadline;
        while seen_pid.load(Ordering::Relaxed) == 0 && Instant::now() < wait_until {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if seen_pid.load(Ordering::Relaxed) != 0 {
            appeared = started.elapsed();
        }

        let flushed = common::wait_for_flush_job_terminal(&db.client, &job_id).await?;
        stop.store(true, Ordering::Relaxed);
        let _ = poller.await;
        common::wait_until_no_flush_executors(&db.client, Duration::from_secs(10)).await?;

        anyhow::ensure!(flushed > 0, "queue flush archived no rows");
        let peak = peak_rss.load(Ordering::Relaxed);
        common::log_always(format!(
            "flush executor startup={appeared:?} peak_rss={} job={job_id} rows={flushed}",
            format_bytes(peak)
        ));
        if appeared > budget.flush_startup {
            bail!(
                "flush executor startup {appeared:?} exceeded budget {:?}",
                budget.flush_startup
            );
        }
        if peak == 0 {
            bail!(
                "flush executor PID was never sampled (job finished in {appeared:?}); \
                 cannot bound process RSS"
            );
        }
        if peak > budget.flush_executor_rss_max_bytes {
            bail!(
                "flush executor peak rss {} exceeded cap {}",
                format_bytes(peak),
                format_bytes(budget.flush_executor_rss_max_bytes)
            );
        }

        reset_flush_execution(&db, &dbname).await?;
    }
    Ok(())
}

/// A concurrent session's hot PK reads and inserts must stay within latency SLO
/// while a queue flush executor encodes Parquet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_flush_does_not_stall_concurrent_hot_pk_work() -> Result<()> {
    common::require_pgrx_server().await?;
    let budget = common::memory::worker_footprint_budget_from_env();

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_footprint_lat").await?;
        let dbname = enable_queue_flush(&db).await?;
        let table = db
            .create_indexed_items_table("flush_lat_items", SEED_ROWS)
            .await?;
        manage_for_queue_flush(&db, &table.relation).await?;

        let wal_pid = common::wal_applier_pid(&db.client).await?;
        let hot_id = SEED_ROWS;
        let probe = common::connect_peer(&db).await?;
        let mut baseline = Vec::with_capacity(20);
        for _ in 0..20 {
            let started = Instant::now();
            let found: i64 = probe
                .query_one(
                    &format!("SELECT id FROM {} WHERE id = $1", table.relation),
                    &[&hot_id],
                )
                .await?
                .get(0);
            anyhow::ensure!(found == hot_id);
            baseline.push(started.elapsed());
        }
        let baseline_median = median_duration(&mut baseline);

        let job_id = common::flush_table_job_id(&db.client, &table.relation, true)
            .await?
            .context("force flush must return a job id")?;

        let stop = Arc::new(AtomicBool::new(false));
        let relation = table.relation.clone();
        let probe_stop = Arc::clone(&stop);
        let latency = tokio::spawn(async move {
            let mut selects = Vec::new();
            let mut inserts = Vec::new();
            let mut ping = Vec::new();
            let mut next_id = SEED_ROWS + 1;
            while !probe_stop.load(Ordering::Relaxed) {
                let started = Instant::now();
                let _: i32 = probe.query_one("SELECT 1::int4", &[]).await?.get(0);
                ping.push(started.elapsed());

                let started = Instant::now();
                let found: i64 = probe
                    .query_one(
                        &format!("SELECT id FROM {relation} WHERE id = $1"),
                        &[&hot_id],
                    )
                    .await?
                    .get(0);
                anyhow::ensure!(found == hot_id, "hot PK {hot_id} vanished during flush");
                selects.push(started.elapsed());

                // Pace DML so the WAL applier can drop the apply lock for
                // flush finalize. A tight insert loop is lock starvation,
                // not a "flush stalled the server" signal.
                let started = Instant::now();
                probe
                    .execute(
                        &format!(
                            "INSERT INTO {relation} (id, account_id, title, qty, category) \
                             VALUES ($1, 1, 'live', 1, 'hot')"
                        ),
                        &[&next_id],
                    )
                    .await?;
                inserts.push(started.elapsed());
                next_id += 1;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Ok::<_, anyhow::Error>((selects, inserts, ping))
        });

        let flushed = common::wait_for_flush_job_terminal(&db.client, &job_id).await?;
        stop.store(true, Ordering::Relaxed);
        let (mut selects, mut inserts, mut pings) = latency.await??;
        common::wait_until_no_flush_executors(&db.client, Duration::from_secs(10)).await?;
        anyhow::ensure!(flushed > 0, "queue flush archived no rows");
        anyhow::ensure!(
            selects.len() >= 3 && inserts.len() >= 3,
            "need overlapping samples during flush, got {} selects / {} inserts",
            selects.len(),
            inserts.len()
        );

        let select_median = median_duration(&mut selects);
        let insert_median = median_duration(&mut inserts);
        let ping_median = median_duration(&mut pings);
        let select_worst = *selects.iter().max().expect("non-empty");
        let insert_worst = *inserts.iter().max().expect("non-empty");
        let ping_worst = *pings.iter().max().expect("non-empty");
        common::log_always(format!(
            "concurrent during flush: select median={select_median:?} worst={select_worst:?} \
             (baseline median={baseline_median:?}); insert median={insert_median:?} \
             worst={insert_worst:?}; select 1 median={ping_median:?} worst={ping_worst:?}"
        ));

        if ping_worst > budget.concurrent_select_max {
            bail!(
                "SELECT 1 during flush took {ping_worst:?}, budget {:?}",
                budget.concurrent_select_max
            );
        }
        if select_worst > budget.concurrent_select_max {
            bail!(
                "hot PK SELECT during flush took {select_worst:?}, budget {:?}",
                budget.concurrent_select_max
            );
        }
        if insert_worst > budget.concurrent_insert_max {
            bail!(
                "INSERT during flush took {insert_worst:?}, budget {:?}",
                budget.concurrent_insert_max
            );
        }
        let select_allowed = baseline_median
            .saturating_mul(10)
            .saturating_add(Duration::from_millis(50));
        if select_median > select_allowed && select_median > Duration::from_millis(100) {
            bail!(
                "hot PK SELECT median during flush {select_median:?} exceeded 10× baseline {:?} + 50ms",
                baseline_median
            );
        }

        let after_pid = common::wal_applier_pid(&db.client).await?;
        anyhow::ensure!(
            after_pid == wal_pid,
            "flush must not replace the WAL applier ({wal_pid} -> {after_pid})"
        );

        reset_flush_execution(&db, &dbname).await?;
    }
    Ok(())
}
