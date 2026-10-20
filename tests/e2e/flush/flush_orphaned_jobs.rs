//! A pending flush job for a table that is gone must leave the queue.
//!
//! The queue executor used to treat an unclaimable candidate as "busy" and retry it every 200 ms
//! forever, logging "changed before claim" each time. A dropped (or unmanaged) table can never be
//! claimed, so the executor now cancels its pending flush jobs.

use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};

use crate::common;

#[tokio::test]
async fn pending_flush_job_of_a_missing_table_is_cancelled() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix()
        .into_iter()
        .next()
        .context("PostgreSQL target")?;
    let db = common::TestDb::start(target, "orphaned_flush_job").await?;
    let dbname: String = db
        .client
        .query_one("SELECT current_database()::text", &[])
        .await?
        .get(0);
    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" SET koldstore.flush_execution = 'queue'; \
             ALTER DATABASE \"{dbname}\" SET koldstore.max_parallel_flush_jobs = 1; \
             SET koldstore.flush_execution = 'queue'"
        ))
        .await?;

    let result = async {
        let table = db.create_indexed_items_table("orphan_probe", 8).await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass, storage => $2, hot_row_limit => 2,
                  min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;

        // Pending flush jobs for relations that do not exist (as left behind by a dropped table).
        db.client
            .batch_execute(
                "INSERT INTO koldstore.jobs (id, job_type, status, table_oid) \
                 SELECT gen_random_uuid(), 'flush', 'pending', oid \
                 FROM generate_series(4000000000::bigint, 4000000002::bigint) AS oid_series(oid)",
            )
            .await
            .context("insert orphaned jobs")?;
        // Wake the queue.
        db.client
            .execute(
                "SELECT koldstore.flush_table($1::text::regclass, true)",
                &[&table.relation],
            )
            .await?;

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let still_pending: i64 = db
                .client
                .query_one(
                    "SELECT count(*) FROM koldstore.jobs \
                     WHERE job_type = 'flush' AND status = 'pending' AND table_oid >= 4000000000",
                    &[],
                )
                .await?
                .get(0);
            if still_pending == 0 {
                break;
            }
            if Instant::now() > deadline {
                bail!("{still_pending} orphaned pending flush job(s) were never cancelled");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let cancelled: i64 = db
            .client
            .query_one(
                "SELECT count(*) FROM koldstore.jobs \
                 WHERE job_type = 'flush' AND status = 'cancelled' AND table_oid >= 4000000000",
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(cancelled == 3, "expected 3 cancelled orphan jobs, got {cancelled}");
        Ok(())
    }
    .await;

    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" RESET koldstore.flush_execution; \
             ALTER DATABASE \"{dbname}\" RESET koldstore.max_parallel_flush_jobs; \
             RESET koldstore.flush_execution"
        ))
        .await
        .ok();
    result
}
