//! Predicate move policy: `ALTER TABLE ... SET (koldstore_move_when = '...')`.

use crate::common;

use anyhow::Result;

async fn count(db: &common::TestDb, sql: &str) -> Result<i64> {
    Ok(db.client.query_one(sql, &[]).await?.get(0))
}

#[tokio::test]
async fn move_when_flushes_only_matching_rows() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "move_when").await?;
        let table = db.create_indexed_items_table("mw_items", 20).await?;
        db.manage_shared(&table.relation, "id").await?;
        let rel = &table.relation;
        db.client.batch_execute("SET koldstore.min_max_rows_per_file = 1").await?;

        // Invalid predicates are refused and leave the table without a policy.
        for bad in [
            "category = ''even'' OR qty = 1",
            "NOT category = ''even''",
            "created_at < now()",
            "no_such_column = 1",
            "qty = ''abc''",
            "category = (SELECT 1)",
        ] {
            let result = db
                .client
                .simple_query(&format!("ALTER TABLE {rel} SET (koldstore_move_when = '{bad}')"))
                .await;
            assert!(result.is_err(), "`{bad}` should be refused");
        }
        let combined = db
            .client
            .simple_query(&format!(
                "ALTER TABLE {rel} SET (koldstore_move_when = 'qty = 1', koldstore_hot_row_limit = 5)"
            ))
            .await;
        assert!(combined.is_err(), "move_when + hot_row_limit should be refused");
        let policy: String = db
            .client
            .query_one(
                &format!("SELECT coalesce(options->'flush_policy', 'null'::jsonb)::text FROM koldstore.schemas WHERE table_oid = '{rel}'::regclass AND active"),
                &[],
            )
            .await?
            .get(0);
        assert_eq!(policy, "null", "refused ALTERs must not change the policy");

        db.client
            .batch_execute(&format!(
                "ALTER TABLE {rel} SET (koldstore_move_when = 'category = ''even''', koldstore_min_flush_rows = 1, koldstore_max_rows_per_file = 1)"
            ))
            .await?;
        let policy: String = db
            .client
            .query_one(
                &format!("SELECT (options->'flush_policy')::text FROM koldstore.schemas WHERE table_oid = '{rel}'::regclass AND active"),
                &[],
            )
            .await?
            .get(0);
        let policy: serde_json::Value = serde_json::from_str(&policy)?;
        assert_eq!(policy["type"], "filter", "{policy}");
        assert_eq!(policy["predicate"]["terms"][0]["column"], "category", "{policy}");

        let oracle_sum = count(&db, &format!("SELECT sum(id)::bigint FROM {rel}")).await?;

        // Only the 10 even rows go cold; the odd rows and their mirror rows stay hot.
        let moved = db.flush_table(rel).await?;
        assert_eq!(moved, 10, "sparse selection moves exactly the matching rows");
        let status = common::table_status(&db.client, rel).await?;
        assert_eq!(status.cold_row_count, 10, "{status:?}");
        assert_eq!(status.hot_rows, 10, "{status:?}");
        assert_eq!(count(&db, &format!("SELECT count(*) FROM {rel}")).await?, 20);
        assert_eq!(count(&db, &format!("SELECT sum(id)::bigint FROM {rel}")).await?, oracle_sum);
        assert_eq!(count(&db, &format!("SELECT count(*) FROM {rel} WHERE category = 'even'")).await?, 10);

        // Nothing else matches: a second policy flush is a no-op.
        assert_eq!(db.flush_table(rel).await?, 0);

        // A hot row edited into the predicate becomes eligible on its own.
        db.client
            .batch_execute(&format!("UPDATE {rel} SET category = 'even' WHERE id = 3"))
            .await?;
        assert_eq!(db.flush_table(rel).await?, 1);
        assert_eq!(count(&db, &format!("SELECT count(*) FROM {rel}")).await?, 20);
        let status = common::table_status(&db.client, rel).await?;
        assert_eq!((status.cold_row_count, status.hot_rows), (11, 9), "{status:?}");

        // A cold row edited out of the predicate comes back hot and stays hot.
        db.client
            .batch_execute(&format!(
                "SET koldstore.hydrate_on_write = on; UPDATE {rel} SET category = 'odd' WHERE id = 2"
            ))
            .await?;
        assert_eq!(db.flush_table(rel).await?, 0, "edited-out row must not flush again");
        assert_eq!(
            db.client
                .query_one(&format!("SELECT category FROM {rel} WHERE id = 2"), &[])
                .await?
                .get::<_, String>(0),
            "odd"
        );
        assert_eq!(count(&db, &format!("SELECT count(*) FROM {rel}")).await?, 20);

        // Tombstones are not subject to the predicate.
        db.client
            .batch_execute(&format!("SET koldstore.hydrate_on_write = on; DELETE FROM {rel} WHERE id = 4"))
            .await?;
        db.flush_table(rel).await?;
        assert_eq!(count(&db, &format!("SELECT count(*) FROM {rel}")).await?, 19);
        assert_eq!(count(&db, &format!("SELECT count(*) FROM {rel} WHERE id = 4")).await?, 0);
    }
    Ok(())
}
