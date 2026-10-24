use crate::common;

use anyhow::Result;

async fn fails_with(db: &common::TestDb, sql: &str) -> Result<Option<String>> {
    Ok(db.client.batch_execute(sql).await.err().map(|error| {
        error.as_db_error().map_or_else(|| error.to_string(), |db| format!("{}: {}", db.code().code(), db.message()))
    }))
}

#[tokio::test]
async fn outbound_fk_blocks_parent_changes_that_orphan_cold_children() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "fk_guard").await?;
        let table = db.create_indexed_items_table("fk_items", 10).await?;
        let relation = &table.relation;
        let schema = relation.split('.').next().unwrap();
        let parent = format!("{schema}.fk_parent");
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {parent} (id int PRIMARY KEY); \
                 INSERT INTO {parent} SELECT g FROM generate_series(1, 4) g; \
                 ALTER TABLE {relation} ADD COLUMN parent_id int CONSTRAINT fk_items_parent REFERENCES {parent}(id); \
                 UPDATE {relation} SET parent_id = 1 WHERE id <= 5; \
                 UPDATE {relation} SET parent_id = 2 WHERE id > 5"
            ))
            .await?;
        // An outbound FK to an unmanaged parent is accepted without allow_fk_hot_only.
        db.manage_shared(relation, "id").await?;
        db.flush_table(relation).await?;
        common::fence_async_mirror(&db.client).await?;

        // Parent 1 only has cold children: native RI sees none, the guard must refuse.
        let err = fails_with(&db, &format!("DELETE FROM {parent} WHERE id = 1")).await?.expect("must be refused");
        assert!(err.starts_with("23503"), "{err}");
        let err = fails_with(&db, &format!("UPDATE {parent} SET id = 10 WHERE id = 2")).await?.expect("must be refused");
        assert!(err.starts_with("23503"), "{err}");

        // Childless parents are free to go or change.
        db.client.batch_execute(&format!("DELETE FROM {parent} WHERE id = 4; UPDATE {parent} SET id = 30 WHERE id = 3")).await?;

        // Writing a child row still needs a real parent (native check on the hot heap).
        let err = fails_with(
            &db,
            &format!("INSERT INTO {relation} (id, account_id, title, qty, category, parent_id) VALUES (700, 1, 't', 1, 'x', 99)"),
        )
        .await?
        .expect("orphan child must be refused");
        assert!(err.starts_with("23503"), "{err}");

        // Dropping the FK removes the guard.
        db.client.batch_execute(&format!("ALTER TABLE {relation} DROP CONSTRAINT fk_items_parent")).await?;
        db.client.batch_execute(&format!("DELETE FROM {parent} WHERE id = 1")).await?;
    }
    Ok(())
}

/// `ON DELETE/UPDATE CASCADE` and `SET NULL` from a managed child act on its cold rows too: the
/// parent-side guard hydrates them so the native action fires.
#[tokio::test]
async fn outbound_fk_cascade_and_set_null_reach_cold_children() -> Result<()> {
    let target = common::scenario_pg_matrix().into_iter().next().expect("PostgreSQL target");
    let db = common::TestDb::start(target, "fk_cascade").await?;
    let table = db.create_indexed_items_table("fkc_items", 12).await?;
    let relation = &table.relation;
    let schema = relation.split('.').next().unwrap();
    let (pdel, pnull, pupd) = (format!("{schema}.fkc_del"), format!("{schema}.fkc_null"), format!("{schema}.fkc_upd"));
    db.client
        .batch_execute(&format!(
            "CREATE TABLE {pdel} (id int PRIMARY KEY); CREATE TABLE {pnull} (id int PRIMARY KEY); \
             CREATE TABLE {pupd} (id int PRIMARY KEY); \
             INSERT INTO {pdel} VALUES (1), (2); INSERT INTO {pnull} VALUES (1), (2); INSERT INTO {pupd} VALUES (1), (2); \
             ALTER TABLE {relation} ADD COLUMN d_id int REFERENCES {pdel}(id) ON DELETE CASCADE, \
                                    ADD COLUMN n_id int REFERENCES {pnull}(id) ON DELETE SET NULL, \
                                    ADD COLUMN u_id int REFERENCES {pupd}(id) ON UPDATE CASCADE; \
             UPDATE {relation} SET d_id = 1, n_id = 1, u_id = 1 WHERE id <= 6; \
             UPDATE {relation} SET d_id = 2, n_id = 2, u_id = 2 WHERE id > 6"
        ))
        .await?;
    // No allow_fk_hot_only needed.
    db.manage_shared(relation, "id").await?;
    db.flush_table(relation).await?;
    common::fence_async_mirror(&db.client).await?;
    let count = |sql: String| {
        let db = &db;
        async move { anyhow::Ok(db.client.query_one(&sql, &[]).await?.get::<_, i64>(0)) }
    };

    // Every child row is cold now; deleting the parent cascades to them.
    db.client.batch_execute(&format!("DELETE FROM {pdel} WHERE id = 1")).await?;
    common::fence_async_mirror(&db.client).await?;
    assert_eq!(count(format!("SELECT count(*) FROM {relation} WHERE d_id = 1")).await?, 0);
    assert_eq!(count(format!("SELECT count(*) FROM {relation}")).await?, 6);

    db.client.batch_execute(&format!("DELETE FROM {pnull} WHERE id = 2")).await?;
    common::fence_async_mirror(&db.client).await?;
    assert_eq!(count(format!("SELECT count(*) FROM {relation} WHERE n_id IS NULL AND id > 6")).await?, 6);
    assert_eq!(count(format!("SELECT count(*) FROM {relation} WHERE n_id = 1")).await?, count(format!("SELECT count(*) FROM {relation} WHERE n_id = 1 AND id <= 6")).await?);

    db.client.batch_execute(&format!("UPDATE {pupd} SET id = 20 WHERE id = 2")).await?;
    common::fence_async_mirror(&db.client).await?;
    assert_eq!(count(format!("SELECT count(*) FROM {relation} WHERE u_id = 20")).await?, 6);
    assert_eq!(count(format!("SELECT count(*) FROM {relation} WHERE u_id = 2")).await?, 0);

    // The hydrated-then-changed rows survive a flush with their new values.
    db.flush_table(relation).await?;
    common::fence_async_mirror(&db.client).await?;
    assert_eq!(count(format!("SELECT count(*) FROM {relation} WHERE u_id = 20")).await?, 6);
    assert_eq!(count(format!("SELECT count(*) FROM {relation}")).await?, 6);
    Ok(())
}
