//! Compaction collapses a key's cold history, so `changes_since` must refuse a cursor that would
//! silently miss the dropped versions (and keep serving cursors taken after the compaction).

use anyhow::{ensure, Context, Result};

use crate::common;

#[tokio::test]
async fn changes_since_fails_closed_for_cursors_older_than_compaction() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix().into_iter().next().context("PostgreSQL target")?;
    let db = common::TestDb::start(target, "changes_compaction").await?;
    let table = db.create_indexed_items_table("cc_items", 2000).await?;
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

    let cursor_before = max_seq(&db, &relation).await?;
    ensure!(cursor_before > 0, "no changes recorded");

    // Two rewrites of the same keys leave two cold versions to collapse.
    db.client.batch_execute("SET koldstore.hydrate_on_write = on").await?;
    for _ in 0..2 {
        db.client.batch_execute(&format!("UPDATE {relation} SET qty = qty + 1 WHERE id % 4 = 0")).await?;
        common::fence_async_mirror(&db.client).await?;
        let flushed = db.flush_table_with_force(&relation, true).await?;
        ensure!(flushed > 0, "the rewritten rows were not flushed");
        common::fence_async_mirror(&db.client).await?;
    }
    db.client.batch_execute("RESET koldstore.hydrate_on_write").await?;

    let changes = |since: i64| format!("SELECT count(*)::bigint FROM koldstore.changes_since('{relation}'::regclass, {since}, 100000)");

    // Before compaction the old cursor still sees every version.
    let before: i64 = db.client.query_one(&changes(cursor_before), &[]).await?.get(0);
    ensure!(before > 0, "old cursor sees nothing before compaction");

    let report: serde_json::Value = db
        .client
        .query_one("SELECT koldstore.compact_table($1::text::regclass, grace_seconds => 3600)::text", &[&relation])
        .await
        .map(|row| serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(0)))??;
    ensure!(report["compacted"] == true, "nothing was compacted: {report}");

    // The old cursor now fails closed, with an actionable message.
    let err = db.client.query(&changes(cursor_before), &[]).await.err().context("stale cursor must be refused")?;
    let message = err.as_db_error().map_or_else(|| err.to_string(), |db| db.message().to_string());
    ensure!(message.contains("predates compaction"), "unexpected error: {message}");

    // A fresh read (since_seq 0) and a cursor taken after the compaction keep working.
    db.client.query_one(&changes(0), &[]).await?;
    let cursor_after = max_seq(&db, &relation).await?;
    db.client.query_one(&changes(cursor_after), &[]).await?;
    Ok(())
}

/// With `koldstore.cdc_retention_seconds` set, compaction moves the superseded versions into history
/// segments: a pre-compaction cursor keeps seeing every version, queries still see one row per key,
/// and turning the window off purges the history and fails old cursors closed again.
#[tokio::test]
async fn changes_since_survives_compaction_with_cdc_retention() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix().into_iter().next().context("PostgreSQL target")?;
    let db = common::TestDb::start(target, "changes_retention").await?;
    let table = db.create_indexed_items_table("cr_items", 2000).await?;
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
    let cursor_before = max_seq(&db, &relation).await?;
    ensure!(cursor_before > 0, "no changes recorded");

    db.client.batch_execute("SET koldstore.hydrate_on_write = on").await?;
    for _ in 0..2 {
        db.client.batch_execute(&format!("UPDATE {relation} SET qty = qty + 1 WHERE id % 4 = 0")).await?;
        common::fence_async_mirror(&db.client).await?;
        ensure!(db.flush_table_with_force(&relation, true).await? > 0, "the rewritten rows were not flushed");
        common::fence_async_mirror(&db.client).await?;
    }
    db.client.batch_execute("RESET koldstore.hydrate_on_write").await?;

    let changes = |since: i64| format!("SELECT count(*)::bigint FROM koldstore.changes_since('{relation}'::regclass, {since}, 100000)");
    let before: i64 = db.client.query_one(&changes(cursor_before), &[]).await?.get(0);
    let rows_before: i64 = db.client.query_one(&format!("SELECT count(*)::bigint FROM {relation}"), &[]).await?.get(0);
    let sum_before: i64 = db.client.query_one(&format!("SELECT sum(qty)::bigint FROM {relation}"), &[]).await?.get(0);

    db.client.batch_execute("SET koldstore.cdc_retention_seconds = 86400").await?;
    let compact = || async {
        let row = db
            .client
            .query_one("SELECT koldstore.compact_table($1::text::regclass, grace_seconds => 3600)::text", &[&relation])
            .await?;
        anyhow::Ok(serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(0))?)
    };
    let report = compact().await?;
    ensure!(report["compacted"] == true, "nothing was compacted: {report}");

    // History segments exist, queries see one version per key, and the old cursor sees every change.
    let history: i64 = db
        .client
        .query_one("SELECT count(*)::bigint FROM koldstore.cold_segments WHERE status = 'history'", &[])
        .await?
        .get(0);
    ensure!(history > 0, "compaction kept no history segments: {report}");
    let rows_after: i64 = db.client.query_one(&format!("SELECT count(*)::bigint FROM {relation}"), &[]).await?.get(0);
    let sum_after: i64 = db.client.query_one(&format!("SELECT sum(qty)::bigint FROM {relation}"), &[]).await?.get(0);
    ensure!(rows_after == rows_before && sum_after == sum_before, "history leaked into queries: {rows_before}/{sum_before} -> {rows_after}/{sum_after}");
    let after: i64 = db.client.query_one(&changes(cursor_before), &[]).await?.get(0);
    ensure!(after == before, "retained history lost versions: {before} changes before compaction, {after} after");
    let dup: i64 = db
        .client
        .query_one(
            &format!("SELECT count(*) - count(DISTINCT seq) FROM koldstore.changes_since('{relation}'::regclass, {cursor_before}, 100000)"),
            &[],
        )
        .await?
        .get(0);
    ensure!(dup == 0, "{dup} duplicate seqs in the feed");

    // A second pass must not duplicate or lose anything either.
    let again: i64 = db.client.query_one(&changes(cursor_before), &[]).await?.get(0);
    ensure!(again == before, "feed changed on re-read");

    // Window off: history is purged on the next reap and the old cursor fails closed again.
    db.client.batch_execute("RESET koldstore.cdc_retention_seconds").await?;
    db.client
        .execute("SELECT koldstore.reap_compacted_segments($1::text::regclass, 0)", &[&relation])
        .await?;
    let left: i64 = db
        .client
        .query_one("SELECT count(*)::bigint FROM koldstore.cold_segments WHERE status = 'history'", &[])
        .await?
        .get(0);
    ensure!(left == 0, "{left} history segments survived the purge");
    let err = db.client.query(&changes(cursor_before), &[]).await.err().context("stale cursor must be refused")?;
    let message = err.as_db_error().map_or_else(|| err.to_string(), |db| db.message().to_string());
    ensure!(message.contains("predates compaction"), "unexpected error: {message}");
    Ok(())
}

async fn max_seq(db: &common::TestDb, relation: &str) -> Result<i64> {
    let sql = format!("SELECT coalesce(max(seq), 0)::bigint FROM koldstore.changes_since('{relation}'::regclass, 0, 100000)");
    Ok(db.client.query_one(&sql, &[]).await?.get(0))
}
