//! `koldstore.compact_table` rewrites segments while readers keep querying.
//!
//! Updates and deletes of flushed rows, flushed again, leave several versions of a key across
//! segments. A reader looping over an aggregate during the compaction must always see the same
//! answer (the swap is one catalog statement under the manifest-generation CAS, and the replaced
//! objects outlive the grace period), and afterwards a full scan no longer retains cold keys.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{ensure, Context, Result};

use crate::common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_is_invisible_to_concurrent_readers() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix()
        .into_iter()
        .next()
        .context("PostgreSQL target")?;
    let db = common::TestDb::start(target, "compaction").await?;
    let table = db.create_indexed_items_table("compact_items", 12000).await?;
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

    // One transaction per statement: a second write in the same transaction does not hydrate cold
    // rows (the merged view cannot be trusted there), so each statement must stand alone.
    db.client
        .batch_execute("SET koldstore.hydrate_on_write = on")
        .await?;
    db.client
        .batch_execute(&format!("UPDATE {relation} SET qty = qty + 1 WHERE id % 5 = 0"))
        .await?;
    db.client
        .batch_execute(&format!("DELETE FROM {relation} WHERE id BETWEEN 7000 AND 7600"))
        .await?;
    db.client
        .batch_execute("RESET koldstore.hydrate_on_write")
        .await?;
    common::fence_async_mirror(&db.client).await?;
    db.flush_table(&relation).await?;
    common::fence_async_mirror(&db.client).await?;

    let aggregate = format!("SELECT count(*)::bigint, sum(id)::bigint, sum(qty)::bigint FROM {relation}");
    let expected: (i64, i64, i64) = {
        let row = db.client.query_one(&aggregate, &[]).await?;
        (row.get(0), row.get(1), row.get(2))
    };
    ensure!(expected.0 == 12000 - 601, "unexpected row count {expected:?}");

    let reader = common::connect_peer(&db).await?;
    let stop = Arc::new(AtomicBool::new(false));
    let reader_stop = Arc::clone(&stop);
    let reader_sql = aggregate.clone();
    let reads = tokio::spawn(async move {
        let mut seen = 0_u32;
        while !reader_stop.load(Ordering::Relaxed) {
            let row = reader.query_one(&reader_sql, &[]).await?;
            let got: (i64, i64, i64) = (row.get(0), row.get(1), row.get(2));
            ensure!(got == expected, "reader saw {got:?}, expected {expected:?}");
            seen += 1;
        }
        Ok::<u32, anyhow::Error>(seen)
    });

    let segments_before: i64 = db
        .client
        .query_one(
            "SELECT count(*)::bigint FROM koldstore.cold_segments \
             WHERE table_oid = $1::text::regclass::oid AND status = 'active'",
            &[&relation],
        )
        .await?
        .get(0);
    let report: serde_json::Value = db
        .client
        .query_one(
            "SELECT koldstore.compact_table($1::text::regclass, grace_seconds => 3600)::text",
            &[&relation],
        )
        .await
        .map(|row| serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(0)))??;
    stop.store(true, Ordering::Relaxed);
    let seen = reads.await??;
    ensure!(seen > 0, "the reader never ran");
    ensure!(report["compacted"] == true, "nothing was compacted: {report}");
    ensure!(
        report["rows_written"].as_i64() < report["rows_read"].as_i64(),
        "superseded versions were not dropped: {report}"
    );

    let after: (i64, i64, i64) = {
        let row = db.client.query_one(&aggregate, &[]).await?;
        (row.get(0), row.get(1), row.get(2))
    };
    ensure!(after == expected, "after compaction {after:?}, expected {expected:?}");

    let segments_after: i64 = db
        .client
        .query_one(
            "SELECT count(*)::bigint FROM koldstore.cold_segments \
             WHERE table_oid = $1::text::regclass::oid AND status = 'active'",
            &[&relation],
        )
        .await?
        .get(0);
    ensure!(
        segments_after <= segments_before,
        "segments grew: {segments_before} -> {segments_after}"
    );

    // Key ranges are disjoint again, so a full scan does not retain cold keys.
    db.client
        .batch_execute("SET koldstore.max_merge_seen_keys = 100")
        .await?;
    let counted: i64 = db
        .client
        .query_one(&format!("SELECT count(*)::bigint FROM {relation}"), &[])
        .await?
        .get(0);
    ensure!(counted == expected.0, "capped scan counted {counted}");
    db.client
        .batch_execute("RESET koldstore.max_merge_seen_keys")
        .await?;

    let retired: i64 = db
        .client
        .query_one(
            "SELECT koldstore.reap_compacted_segments($1::text::regclass, 0)::bigint",
            &[&relation],
        )
        .await?
        .get(0);
    ensure!(retired > 0, "no retired segments were reaped");
    let valid: String = db
        .client
        .query_one(
            "SELECT (koldstore.validate_cold_storage($1::text::regclass, true) ->> 'ok')",
            &[&relation],
        )
        .await?
        .get(0);
    ensure!(valid == "true", "cold storage invalid after reaping");
    let after_reap: (i64, i64, i64) = {
        let row = db.client.query_one(&aggregate, &[]).await?;
        (row.get(0), row.get(1), row.get(2))
    };
    ensure!(after_reap == expected, "after reaping {after_reap:?}, expected {expected:?}");
    Ok(())
}
