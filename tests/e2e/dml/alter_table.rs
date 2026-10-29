use crate::common;

use anyhow::Result;

async fn policy(db: &common::TestDb, relation: &str) -> Result<serde_json::Value> {
    let row = db
        .client
        .query_one(
            &format!("SELECT options::text FROM koldstore.schemas WHERE table_oid = '{relation}'::regclass AND active"),
            &[],
        )
        .await?;
    Ok(serde_json::from_str(&row.get::<_, String>(0))?)
}

#[tokio::test]
async fn alter_table_changes_flush_settings_in_place() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "alter_table").await?;
        let table = db.create_indexed_items_table("alter_items", 20).await?;
        db.manage_shared(&table.relation, "id").await?;
        db.flush_table(&table.relation).await?;
        let relation = &table.relation;
        db.client.batch_execute("SET koldstore.min_max_rows_per_file = 1").await?;

        // Managed without a flush policy: tuning limits alone is refused, naming hot_row_limit opts in.
        assert!(policy(&db, relation).await?.get("flush_policy").is_none());
        let refused = db
            .client
            .simple_query(&format!("SELECT koldstore.alter_table('{relation}'::regclass, min_flush_rows => 5)"))
            .await;
        assert!(refused.is_err());
        let created: String = db
            .client
            .query_one(&format!("SELECT koldstore.alter_table('{relation}'::regclass, hot_row_limit => 7)::text"), &[])
            .await
            .map_err(|e| anyhow::anyhow!("alter_table: {:?}", e.as_db_error()))?
            .get(0);
        let created: serde_json::Value = serde_json::from_str(&created)?;
        assert_eq!(created["flush_policy"]["hot_row_limit"], 7, "{created}");
        assert_eq!(created["flush_policy"]["type"], "row_limit", "{created}");
        assert_eq!(policy(&db, relation).await?, created);

        // Partial change: only the named key moves.
        let after: String = db
            .client
            .query_one(&format!("SELECT koldstore.alter_table('{relation}'::regclass, min_flush_rows => 4)::text"), &[])
            .await?
            .get(0);
        let after: serde_json::Value = serde_json::from_str(&after)?;
        let mut expected = created["flush_policy"].clone();
        expected["min_flush_rows"] = 4.into();
        assert_eq!(after["flush_policy"], expected, "{after}");

        // All three limits, then auto_flush off and back on.
        db.client
            .batch_execute(&format!(
                "SELECT koldstore.alter_table('{relation}'::regclass, 9, 3, 50)"
            ))
            .await?;
        let p = policy(&db, relation).await?;
        assert_eq!(
            (p["flush_policy"]["hot_row_limit"].as_i64(), p["flush_policy"]["min_flush_rows"].as_i64(), p["flush_policy"]["max_rows_per_file"].as_i64()),
            (Some(9), Some(3), Some(50))
        );
        db.client
            .batch_execute(&format!("SELECT koldstore.alter_table('{relation}'::regclass, auto_flush => false)"))
            .await?;
        assert_eq!(policy(&db, relation).await?["auto_flush"], false);
        db.client
            .batch_execute(&format!("SELECT koldstore.alter_table('{relation}'::regclass, auto_flush => true)"))
            .await?;
        assert!(policy(&db, relation).await?.get("auto_flush").is_none());

        // Invalid values are refused and change nothing.
        let snapshot = policy(&db, relation).await?;
        for args in ["hot_row_limit => 0", "min_flush_rows => -1", "max_rows_per_file => 0"] {
            let result = db.client.simple_query(&format!("SELECT koldstore.alter_table('{relation}'::regclass, {args})")).await;
            assert!(result.is_err(), "{args} should be refused");
        }
        assert_eq!(policy(&db, relation).await?, snapshot);

        // The data is untouched and a flush under the new limits still works.
        db.client
            .batch_execute(&format!("SELECT koldstore.alter_table('{relation}'::regclass, 2, 1, 50)"))
            .await?;
        db.flush_table(relation).await?;
        common::fence_async_mirror(&db.client).await?;
        let count: i64 = db.client.query_one(&format!("SELECT count(*) FROM {relation}"), &[]).await?.get(0);
        assert_eq!(count, 20);
    }
    Ok(())
}

#[tokio::test]
async fn alter_table_refuses_unmanaged_tables() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "alter_table_unmanaged").await?;
        db.client.batch_execute("CREATE TABLE plain_t (id bigint PRIMARY KEY)").await?;
        let result = db.client.simple_query("SELECT koldstore.alter_table('plain_t'::regclass, hot_row_limit => 5)").await;
        let error = result.expect_err("unmanaged table must be refused");
        let message = error.as_db_error().map(|db| db.message().to_owned()).unwrap_or_default();
        assert!(message.contains("not an active managed table"), "unexpected error: {message}");
    }
    Ok(())
}

#[tokio::test]
async fn alter_table_set_changes_compression_and_target_file_size() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "alter_compression").await?;
        let table = db.create_indexed_items_table("alter_comp", 20).await?;
        db.manage_shared(&table.relation, "id").await?;
        let relation = &table.relation;
        let before = policy(&db, relation).await?;

        for bad in ["koldstore_compression = 'lz77'", "koldstore_target_file_size_mb = 0", "koldstore_target_file_size_mb = 'x'"] {
            let refused = db.client.batch_execute(&format!("ALTER TABLE {relation} SET ({bad})")).await;
            assert!(refused.is_err(), "{bad} must be refused");
        }
        assert_eq!(policy(&db, relation).await?, before, "refused ALTER must not change options");

        db.client
            .batch_execute(&format!(
                "ALTER TABLE {relation} SET (koldstore_compression = 'snappy', koldstore_target_file_size_mb = 8)"
            ))
            .await?;
        let after = policy(&db, relation).await?;
        assert_eq!(after["compression"], "snappy", "{after}");
        assert_eq!(after["target_file_size_mb"], 8, "{after}");

        let expected: i64 = db.client.query_one(&format!("SELECT count(*) FROM {relation}"), &[]).await?.get(0);
        db.flush_table(relation).await?;
        let got: i64 = db.client.query_one(&format!("SELECT count(*) FROM {relation}"), &[]).await?.get(0);
        assert_eq!(got, expected, "reads must survive the codec change");
    }
    Ok(())
}
