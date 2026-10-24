use crate::common;

use anyhow::Result;

async fn total(db: &common::TestDb, relation: &str) -> Result<i64> {
    let row = db.client.query_one(&format!("SELECT count(*) FROM {relation}"), &[]).await?;
    Ok(row.get(0))
}

#[tokio::test]
async fn purge_where_deletes_cold_and_hot_rows_and_survives_flush() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "purge_where").await?;
        let table = db.create_indexed_items_table("purge_items", 20).await?;
        db.manage_shared(&table.relation, "id").await?;
        db.flush_table(&table.relation).await?;
        let relation = &table.relation;

        // A few hot rows next to the 20 cold ones.
        db.client
            .batch_execute(&format!(
                "INSERT INTO {relation} (id, account_id, title, qty, category) \
                 VALUES (1000, 7, 'hot-a', 7, 'hot'), (1001, 7, 'hot-b', 8, 'hot')"
            ))
            .await?;
        common::fence_async_mirror(&db.client).await?;

        assert_eq!(total(&db, relation).await?, 22);

        // Cold-only + hot match: ids 5..=9 are cold, 1000 is hot.
        let outcome = db
            .client
            .query_one(
                &format!("SELECT koldstore.purge_where('{relation}'::regclass, 't.id BETWEEN 5 AND 9 OR t.id = 1000')::text"),
                &[],
            )
            .await?;
        let outcome: serde_json::Value = serde_json::from_str(&outcome.get::<_, String>(0))?;
        assert_eq!(outcome["purged_cold"], 5, "{outcome}");
        assert_eq!(outcome["deleted_hot"], 1, "{outcome}");
        assert_eq!(total(&db, relation).await?, 16);

        // Masked in the merged view even though the cold segment still holds them.
        let gone: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {relation} WHERE id BETWEEN 5 AND 9 OR id = 1000"), &[])
            .await?
            .get(0);
        assert_eq!(gone, 0);

        // A flush must turn the tombstones into delete markers, and reads stay correct after.
        common::fence_async_mirror(&db.client).await?;
        db.flush_table(relation).await?;
        common::fence_async_mirror(&db.client).await?;
        assert_eq!(total(&db, relation).await?, 16);
        let gone: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {relation} WHERE id BETWEEN 5 AND 9 OR id = 1000"), &[])
            .await?
            .get(0);
        assert_eq!(gone, 0);

        // Purging nothing is a no-op; the key can be re-inserted afterwards.
        let none = db
            .client
            .query_one(&format!("SELECT koldstore.purge_where('{relation}'::regclass, 't.id = -1')::text"), &[])
            .await?;
        assert!(none.get::<_, String>(0).contains("\"purged_cold\": 0"));
        db.client
            .batch_execute(&format!(
                "INSERT INTO {relation} (id, account_id, title, qty, category) VALUES (5, 1, 'back', 1, 'x')"
            ))
            .await?;
        assert_eq!(total(&db, relation).await?, 17);

        // Not allowed inside a transaction that already wrote the table.
        db.client.batch_execute("BEGIN").await?;
        db.client
            .batch_execute(&format!("UPDATE {relation} SET qty = qty + 1 WHERE id = 5"))
            .await?;
        let refused = db
            .client
            .query_one(&format!("SELECT koldstore.purge_where('{relation}'::regclass, 't.id = 0')"), &[])
            .await;
        assert!(refused.is_err());
        db.client.batch_execute("ROLLBACK").await?;
    }
    Ok(())
}

/// On an ordered table the tombstone's `order_key` must be exactly what the applier would have
/// encoded for the row, or flush would place its delete marker in the wrong order position.
#[tokio::test]
async fn purge_where_encodes_the_same_order_key_as_the_applier() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "purge_where_order").await?;
        let relation = db.relation("purge_order_events");
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, event_time timestamptz NOT NULL, payload text NOT NULL)"
            ))
            .await?;
        db.client
            .execute(
                "SELECT koldstore.manage_table(table_name => $1::text::regclass, storage => $2, \
                 hot_row_limit => NULL, migration_order_by => 'id', segment_order_column => 'event_time')",
                &[&relation, &db.storage_name],
            )
            .await?;
        db.client
            .execute("SELECT koldstore.set_table_auto_flush($1::text::regclass, false)", &[&relation])
            .await?;
        db.client
            .batch_execute(&format!(
                "INSERT INTO {relation} VALUES (1, timestamptz '2024-06-01 00:00:00+00', 'a'), \
                 (2, timestamptz '2024-06-02 00:00:00+00', 'b'), (3, timestamptz '2024-06-03 00:00:00+00', 'c')"
            ))
            .await?;
        common::fence_async_mirror(&db.client).await?;
        let mirror = common::change_log_mirror_relation(&relation);
        let applier_key: Vec<u8> = db
            .client
            .query_one(&format!("SELECT order_key FROM {mirror} WHERE id = 1"), &[])
            .await?
            .get(0);

        db.flush_table(&relation).await?;
        common::fence_async_mirror(&db.client).await?;
        assert_eq!(total(&db, &relation).await?, 3);

        let outcome = db
            .client
            .query_one(&format!("SELECT koldstore.purge_where('{relation}'::regclass, 't.id = 1')::text"), &[])
            .await?;
        assert!(outcome.get::<_, String>(0).contains("\"purged_cold\": 1"));
        let purged: Vec<u8> = db
            .client
            .query_one(&format!("SELECT order_key FROM {mirror} WHERE id = 1 AND op = 3"), &[])
            .await?
            .get(0);
        assert_eq!(purged, applier_key);
        assert_eq!(total(&db, &relation).await?, 2);

        db.flush_table(&relation).await?;
        common::fence_async_mirror(&db.client).await?;
        assert_eq!(total(&db, &relation).await?, 2);
    }
    Ok(())
}
