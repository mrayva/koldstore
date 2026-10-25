use crate::common;

use anyhow::{ensure, Result};

/// A hot-only dump plus `export_table_metadata` / `import_table_metadata` reproduces a managed
/// table whose cold rows stay in object storage.
#[tokio::test]
async fn hot_only_dump_and_metadata_round_trip_restores_cold_rows() -> Result<()> {
    let target = common::scenario_pg_matrix().into_iter().next().expect("PostgreSQL target");
    let db = common::TestDb::start(target, "table_metadata").await?;
    let table = db.create_indexed_items_table("tm_items", 10).await?;
    let relation = &table.relation;
    db.manage_shared(relation, "id").await?;
    db.flush_table(relation).await?;
    common::fence_async_mirror(&db.client).await?;
    // One hot row on top of ten cold ones.
    db.client
        .batch_execute(&format!(
            "INSERT INTO {relation} (id, account_id, title, qty, category) VALUES (900, 1, 'hot', 1, 'x')"
        ))
        .await?;
    common::fence_async_mirror(&db.client).await?;

    // Unflushed hot state is fine, but a pending delete marker is not.
    db.client.batch_execute(&format!("SELECT koldstore.delete_row('{relation}'::regclass, '{{\"id\": 5}}'::jsonb)")).await?;
    common::fence_async_mirror(&db.client).await?;
    let err = db
        .client
        .query_one(&format!("SELECT koldstore.export_table_metadata('{relation}'::regclass)"), &[])
        .await
        .err()
        .map(|e| e.as_db_error().map_or_else(|| e.to_string(), |d| d.message().to_string()))
        .expect("export refused with an unflushed delete marker");
    ensure!(err.contains("delete marker"), "{err}");
    db.flush_table(relation).await?;
    common::fence_async_mirror(&db.client).await?;

    let expected: Vec<(i64, String)> = db
        .client
        .query(&format!("SELECT id, title FROM {relation} ORDER BY id"), &[])
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    ensure!(expected.len() == 10, "9 survivors + 1 hot insert, got {}", expected.len());

    // Hot-only dump: the cold rows stay out of the file.
    let hot_file = format!("{}/tm_hot.txt", db.storage_root.display());
    db.client
        .batch_execute(&format!("SET koldstore.dump_cold_as_metadata = on; COPY {relation} TO '{hot_file}'; RESET koldstore.dump_cold_as_metadata"))
        .await?;
    let hot_lines = std::fs::read_to_string(&hot_file)?.lines().count();
    ensure!(hot_lines < expected.len(), "hot-only dump exported {hot_lines} rows");

    let metadata: String = db
        .client
        .query_one(&format!("SELECT koldstore.export_table_metadata('{relation}'::regclass)::text"), &[])
        .await?
        .get(0);
    ensure!(!metadata.contains("credentials"), "export must not carry credentials");
    ensure!(metadata.contains("\"segments\": [{"), "{metadata}");

    // Drop the table (objects retained), recreate its DDL, load the hot rows, import the metadata.
    let shadow = format!("{relation}_shadow");
    let bare = relation.rsplit('.').next().unwrap();
    db.client
        .batch_execute(&format!(
            "CREATE TABLE {shadow} (LIKE {relation} INCLUDING ALL); \
             SET koldstore.cold_object_retention_seconds = 3600; \
             DROP TABLE {relation}; \
             ALTER TABLE {shadow} RENAME TO {bare}; \
             COPY {relation} FROM '{hot_file}'"
        ))
        .await?;
    let adopted: i64 = db
        .client
        .query_one(
            "SELECT koldstore.import_table_metadata($1::text::regclass, $2::text::jsonb)",
            &[relation, &metadata],
        )
        .await?
        .get(0);
    ensure!(adopted >= 1, "no segments adopted");

    let restored: Vec<(i64, String)> = db
        .client
        .query(&format!("SELECT id, title FROM {relation} ORDER BY id"), &[])
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    ensure!(restored == expected, "restored {restored:?} != original {expected:?}");
    let ok: String = db
        .client
        .query_one("SELECT (koldstore.validate_cold_storage($1::text::regclass, true) ->> 'ok')", &[relation])
        .await?
        .get(0);
    ensure!(ok == "true", "validate_cold_storage reported problems");

    // Importing again is refused: the table is managed now.
    ensure!(
        db.client
            .query_one("SELECT koldstore.import_table_metadata($1::text::regclass, $2::text::jsonb)", &[relation, &metadata])
            .await
            .is_err()
    );
    Ok(())
}
