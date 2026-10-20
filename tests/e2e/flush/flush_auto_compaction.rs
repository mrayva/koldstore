//! `koldstore.auto_compaction`: the maintenance worker compacts overlapping segments on its own,
//! then reaps the replaced objects, without changing what queries return.

use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context, Result};

use crate::common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintenance_worker_compacts_overlapping_segments() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix()
        .into_iter()
        .next()
        .context("PostgreSQL target")?;
    let db = common::TestDb::start(target, "auto_compaction").await?;
    let dbname: String = db
        .client
        .query_one("SELECT current_database()::text", &[])
        .await?
        .get(0);
    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" SET koldstore.flush_check_interval_seconds = 1; \
             ALTER DATABASE \"{dbname}\" SET koldstore.auto_compaction = on"
        ))
        .await?;

    let result = async {
        let table = db.create_indexed_items_table("auto_items", 6000).await?;
        let relation = table.relation.clone();
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass, storage => $2, hot_row_limit => 2,
                  min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;
        db.flush_table(&relation).await?;
        db.client.batch_execute("SET koldstore.hydrate_on_write = on").await?;
        db.client
            .batch_execute(&format!("UPDATE {relation} SET qty = qty + 1 WHERE id % 3 = 0"))
            .await?;
        db.client.batch_execute("RESET koldstore.hydrate_on_write").await?;
        common::fence_async_mirror(&db.client).await?;
        db.flush_table(&relation).await?;
        common::fence_async_mirror(&db.client).await?;

        let aggregate =
            format!("SELECT count(*)::bigint, sum(qty)::bigint FROM {relation}");
        let before = db.client.query_one(&aggregate, &[]).await?;
        let expected: (i64, i64) = (before.get(0), before.get(1));

        // The worker should retire the overlapping segments without anyone calling compact_table.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let retired: i64 = db
                .client
                .query_one(
                    "SELECT count(*)::bigint FROM koldstore.cold_segments \
                     WHERE table_oid = $1::text::regclass::oid \
                       AND status IN ('compacted')",
                    &[&relation],
                )
                .await?
                .get(0);
            if retired > 0 {
                break;
            }
            if Instant::now() > deadline {
                bail!("the maintenance worker never compacted the table");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let after = db.client.query_one(&aggregate, &[]).await?;
        ensure!(
            (after.get::<_, i64>(0), after.get::<_, i64>(1)) == expected,
            "content changed by automatic compaction"
        );
        // A second sweep finds nothing left to do.
        let again: serde_json::Value = db
            .client
            .query_one(
                "SELECT koldstore.compact_table($1::text::regclass, grace_seconds => 3600)::text",
                &[&relation],
            )
            .await
            .map(|row| serde_json::from_str(&row.get::<_, String>(0)))??;
        ensure!(again["compacted"] == false, "left work behind: {again}");
        Ok(())
    }
    .await;

    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" RESET koldstore.auto_compaction; \
             ALTER DATABASE \"{dbname}\" RESET koldstore.flush_check_interval_seconds"
        ))
        .await?;
    result
}
