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

/// `rebuild_table` applies the changes `ALTER TABLE` refuses on cold data: it rehydrates, runs the
/// DDL and manages the table again with the same options; a failing DDL rolls everything back.
#[tokio::test]
async fn rebuild_table_applies_primary_key_and_narrowing_changes() -> Result<()> {
    let target = common::scenario_pg_matrix().into_iter().next().expect("PostgreSQL target");
    let db = common::TestDb::start(target, "cold_rebuild").await?;
    let relation = db.relation("rb_items");
    db.client
        .batch_execute(&format!(
            "CREATE TABLE {relation} (id bigint PRIMARY KEY, account_id bigint NOT NULL, qty bigint, label text); \
             INSERT INTO {relation} SELECT g, g % 3, g * 10, 'row-' || g FROM generate_series(1, 12) g"
        ))
        .await?;
    db.manage_shared(&relation, "id").await?;
    db.flush_table(&relation).await?;
    common::fence_async_mirror(&db.client).await?;
    let count = |sql: String| {
        let db = &db;
        async move { anyhow::Ok(db.client.query_one(&sql, &[]).await?.get::<_, i64>(0)) }
    };
    let cold_segments = || count(format!(
        "SELECT count(*) FROM koldstore.cold_segments s WHERE s.table_oid = '{relation}'::regclass::oid"
    ));
    assert!(cold_segments().await? > 0);

    // A DDL that fails (qty does not fit smallint) leaves the table exactly as it was.
    let err = fails_with(
        &db,
        &format!("SELECT koldstore.rebuild_table('{relation}'::regclass, 'ALTER TABLE {relation} ALTER COLUMN qty TYPE smallint USING qty * 1000')"),
    )
    .await?
    .expect("out-of-range narrowing fails");
    assert!(err.contains("out of range"), "{err}");
    assert!(cold_segments().await? > 0);
    assert_eq!(count(format!("SELECT count(*) FROM {relation}")).await?, 12);

    // Primary-key change plus narrowing, both refused by plain ALTER TABLE.
    db.client
        .batch_execute(&format!(
            "SELECT koldstore.rebuild_table('{relation}'::regclass, \
               'ALTER TABLE {relation} DROP CONSTRAINT rb_items_pkey, ADD PRIMARY KEY (id, account_id), \
                ALTER COLUMN qty TYPE integer')"
        ))
        .await?;
    assert_eq!(count(format!("SELECT sum(qty) FROM {relation}")).await?, 780);
    assert_eq!(count(format!("SELECT count(*) FROM {relation}")).await?, 12);
    db.client.batch_execute(&format!("INSERT INTO {relation} VALUES (1, 99, 5, 'same id, new key')")).await?;
    common::fence_async_mirror(&db.client).await?;
    db.flush_table(&relation).await?;
    common::fence_async_mirror(&db.client).await?;
    assert!(cold_segments().await? > 0);
    assert_eq!(count(format!("SELECT count(*) FROM {relation}")).await?, 13);
    assert_eq!(count(format!("SELECT count(*) FROM {relation} WHERE id = 1")).await?, 2);

    // The DDL runs with the caller's privileges, never the function owner's.
    db.client
        .batch_execute(&format!(
            "CREATE ROLE e2e_rb_low_app NOLOGIN; GRANT USAGE ON SCHEMA koldstore TO e2e_rb_low_app; ALTER TABLE {relation} OWNER TO e2e_rb_low_app"
        ))
        .await?;
    db.client.batch_execute("SET ROLE e2e_rb_low_app").await?;
    let err = fails_with(&db, &format!("SELECT koldstore.rebuild_table('{relation}'::regclass, 'CREATE ROLE rb_evil')"))
        .await?
        .expect("caller cannot create roles through rebuild_table");
    assert!(err.contains("permission denied"), "{err}");
    db.client.batch_execute("RESET ROLE").await?;
    assert_eq!(count(format!("SELECT count(*) FROM pg_roles WHERE rolname = 'rb_evil'")).await?, 0);
    assert_eq!(count(format!("SELECT count(*) FROM {relation}")).await?, 13);
    Ok(())
}
