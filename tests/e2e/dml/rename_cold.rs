use crate::common;

use anyhow::{ensure, Result};

async fn row_count(db: &common::TestDb, relation: &str) -> Result<i64> {
    Ok(db.client.query_one(&format!("SELECT count(*) FROM {relation}"), &[]).await?.get(0))
}

async fn validate(db: &common::TestDb, relation: &str) -> Result<()> {
    let ok: String = db
        .client
        .query_one("SELECT (koldstore.validate_cold_storage($1::text::regclass, true) ->> 'ok')", &[&relation])
        .await?
        .get(0);
    ensure!(ok == "true", "validate_cold_storage reported problems for {relation}");
    Ok(())
}

/// Renaming a managed table, moving it to another schema and renaming its schema keep reading
/// the cold rows and keep publishing new segments under the original prefix (upstream #123).
#[tokio::test]
async fn rename_and_schema_moves_keep_cold_rows_reachable() -> Result<()> {
    let target = common::scenario_pg_matrix().into_iter().next().expect("PostgreSQL target");
    let db = common::TestDb::start(target, "rename_cold").await?;
    let table = db.create_indexed_items_table("rc_items", 10).await?;
    let relation = table.relation.clone();
    let schema = relation.split('.').next().unwrap().to_string();
    db.manage_shared(&relation, "id").await?;
    db.flush_table(&relation).await?;
    common::fence_async_mirror(&db.client).await?;
    ensure!(row_count(&db, &relation).await? == 10);

    // Plain rename.
    let renamed = format!("{schema}.rc_renamed");
    db.client.batch_execute(&format!("ALTER TABLE {relation} RENAME TO rc_renamed")).await?;
    ensure!(row_count(&db, &renamed).await? == 10, "cold rows visible after rename");
    let pinned: i64 = db
        .client
        .query_one(
            "SELECT count(*) FROM koldstore.table_prefix_pins WHERE table_oid = $1::text::regclass::oid",
            &[&renamed],
        )
        .await?
        .get(0);
    ensure!(pinned == 1, "rename pins the object prefix");

    // New rows flush under the pinned prefix and merge with the old ones.
    db.client
        .batch_execute(&format!(
            "INSERT INTO {renamed} (id, account_id, title, qty, category) VALUES (900, 1, 'new', 1, 'x'), (901, 1, 'new', 1, 'x')"
        ))
        .await?;
    common::fence_async_mirror(&db.client).await?;
    db.flush_table(&renamed).await?;
    common::fence_async_mirror(&db.client).await?;
    ensure!(row_count(&db, &renamed).await? == 12);
    validate(&db, &renamed).await?;

    // A new table taking the old name cannot be managed while the pin holds the prefix.
    db.client
        .batch_execute(&format!("CREATE TABLE {relation} (id bigint PRIMARY KEY, title text)"))
        .await?;
    let err = db.manage_shared(&relation, "id").await.err().map(|e| format!("{e:#}")).unwrap_or_default();
    ensure!(err.contains("still pinned"), "expected a prefix collision error, got: {err}");
    db.client.batch_execute(&format!("DROP TABLE {relation}")).await?;

    // Move to another schema.
    let other = format!("{schema}_moved");
    db.client.batch_execute(&format!("CREATE SCHEMA {other}; ALTER TABLE {renamed} SET SCHEMA {other}")).await?;
    let moved = format!("{other}.rc_renamed");
    ensure!(row_count(&db, &moved).await? == 12, "cold rows visible after SET SCHEMA");

    // Rename the schema.
    let other2 = format!("{schema}_moved2");
    db.client.batch_execute(&format!("ALTER SCHEMA {other} RENAME TO {other2}")).await?;
    let final_name = format!("{other2}.rc_renamed");
    ensure!(row_count(&db, &final_name).await? == 12, "cold rows visible after schema rename");
    db.client
        .batch_execute(&format!("INSERT INTO {final_name} (id, account_id, title, qty, category) VALUES (950, 1, 'last', 1, 'x')"))
        .await?;
    common::fence_async_mirror(&db.client).await?;
    db.flush_table(&final_name).await?;
    common::fence_async_mirror(&db.client).await?;
    ensure!(row_count(&db, &final_name).await? == 13);
    validate(&db, &final_name).await?;

    // Compaction keeps working off the pinned prefix.
    db.client.batch_execute(&format!("SELECT koldstore.compact_table('{final_name}'::regclass, grace_seconds => 3600)")).await?;
    ensure!(row_count(&db, &final_name).await? == 13);
    validate(&db, &final_name).await?;

    // Dropping the table removes its pin.
    db.client.batch_execute(&format!("DROP TABLE {final_name}")).await?;
    let left: i64 = db.client.query_one("SELECT count(*) FROM koldstore.table_prefix_pins", &[]).await?.get(0);
    ensure!(left == 0, "pin removed when the table is dropped, {left} left");
    Ok(())
}
