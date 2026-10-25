use crate::common;

use anyhow::Result;

async fn fails_with(db: &common::TestDb, sql: &str) -> Result<Option<String>> {
    Ok(db.client.batch_execute(sql).await.err().map(|error| {
        error.as_db_error().map_or_else(|| error.to_string(), |db| db.message().to_string())
    }))
}

/// Non-key columns can be widened under cold data (old segments keep their narrow Arrow type);
/// changes the cold segments cannot be read under are refused and rolled back.
#[tokio::test]
async fn cold_data_survives_widening_and_refuses_unreadable_changes() -> Result<()> {
    let target = common::scenario_pg_matrix().into_iter().next().expect("PostgreSQL target");
    let db = common::TestDb::start(target, "cold_schema_evo").await?;
    let relation = db.relation("cse_items");
    db.client
        .batch_execute(&format!(
            "CREATE TABLE {relation} (id bigint PRIMARY KEY, account_id bigint NOT NULL, \
               small smallint, qty integer, ratio real, label text); \
             INSERT INTO {relation} SELECT g, g % 5, g::smallint, g * 1000, g / 4.0, 'row-' || g \
               FROM generate_series(1, 10) g"
        ))
        .await?;
    db.manage_shared(&relation, "id").await?;
    db.flush_table(&relation).await?;
    common::fence_async_mirror(&db.client).await?;

    let sum = |col: &'static str| {
        let (db, relation) = (&db, &relation);
        async move {
            let sql = format!("SELECT sum({col})::text FROM {relation}");
            anyhow::Ok(db.client.query_one(&sql, &[]).await?.get::<_, String>(0))
        }
    };

    // Widening a non-key column keeps the cold rows readable, and new rows may use the new range.
    db.client
        .batch_execute(&format!(
            "ALTER TABLE {relation} ALTER COLUMN qty TYPE bigint; \
             ALTER TABLE {relation} ALTER COLUMN small TYPE integer; \
             ALTER TABLE {relation} ALTER COLUMN ratio TYPE double precision; \
             ALTER TABLE {relation} ALTER COLUMN label TYPE varchar(40)"
        ))
        .await?;
    assert_eq!(sum("qty").await?, "55000");
    assert_eq!(sum("small").await?, "55");
    assert_eq!(sum("ratio").await?, "13.75");
    db.client
        .batch_execute(&format!("INSERT INTO {relation} VALUES (11, 1, 1, 5000000000, 0.5, 'new')"))
        .await?;
    common::fence_async_mirror(&db.client).await?;
    db.flush_table(&relation).await?;
    common::fence_async_mirror(&db.client).await?;
    assert_eq!(sum("qty").await?, "5000055000");

    // Widening to numeric is read back from the integer-typed segments too.
    db.client.batch_execute(&format!("ALTER TABLE {relation} ALTER COLUMN small TYPE numeric")).await?;
    assert_eq!(sum("small").await?, "56");

    // Changes the cold segments cannot be read under are refused, and the ALTER rolls back.
    let err = fails_with(&db, &format!("ALTER TABLE {relation} ALTER COLUMN qty TYPE integer")).await?
        .expect("narrowing refused");
    assert!(err.contains("changed type of managed"), "{err}");
    let err = fails_with(&db, &format!("ALTER TABLE {relation} ALTER COLUMN label TYPE int USING length(label)")).await?
        .expect("cross-type change refused");
    assert!(err.contains("changed type of managed"), "{err}");
    let err = fails_with(
        &db,
        &format!("ALTER TABLE {relation} DROP CONSTRAINT cse_items_pkey, ADD PRIMARY KEY (id, account_id)"),
    )
    .await?
    .expect("primary-key change refused");
    assert!(err.contains("changed the primary key"), "{err}");
    assert_eq!(sum("qty").await?, "5000055000");

    // Adding and dropping columns keeps working.
    db.client.batch_execute(&format!("ALTER TABLE {relation} ADD COLUMN extra text; ALTER TABLE {relation} DROP COLUMN ratio")).await?;
    assert_eq!(sum("qty").await?, "5000055000");
    Ok(())
}
