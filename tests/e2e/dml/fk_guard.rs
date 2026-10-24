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
