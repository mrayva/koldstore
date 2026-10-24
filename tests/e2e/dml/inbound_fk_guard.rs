use crate::common;

use anyhow::Result;

async fn fails_with(db: &common::TestDb, sql: &str) -> Result<Option<String>> {
    Ok(db.client.batch_execute(sql).await.err().map(|error| {
        error.as_db_error().map_or_else(|| error.to_string(), |db| format!("{}: {}", db.code().code(), db.message()))
    }))
}

#[tokio::test]
async fn inbound_fk_sees_cold_parents_and_blocks_their_purge() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "inbound_fk_guard").await?;
        let table = db.create_indexed_items_table("ifk_items", 10).await?;
        let relation = &table.relation;
        let schema = relation.split('.').next().unwrap();
        let child_pre = format!("{schema}.ifk_child_pre");
        let child_post = format!("{schema}.ifk_child_post");
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {child_pre} (id int PRIMARY KEY, item_id int REFERENCES {relation}(id)); \
                 INSERT INTO {child_pre} VALUES (1, 1)"
            ))
            .await?;
        // An inbound FK from an unmanaged table is accepted without allow_fk_hot_only.
        db.manage_shared(relation, "id").await?;
        db.flush_table(relation).await?;
        common::fence_async_mirror(&db.client).await?;

        // A foreign key created after manage_table is guarded too (the DDL hook re-plans the parent).
        db.client
            .batch_execute(&format!("CREATE TABLE {child_post} (id int PRIMARY KEY, item_id int REFERENCES {relation}(id))"))
            .await?;

        // A child may reference a parent that only lives in cold storage.
        db.client.batch_execute(&format!("INSERT INTO {child_post} VALUES (1, 2); UPDATE {child_post} SET item_id = 4 WHERE id = 1")).await?;
        let err = fails_with(&db, &format!("INSERT INTO {child_post} VALUES (2, 99)")).await?.expect("orphan child refused");
        assert!(err.starts_with("23503"), "{err}");
        db.client.batch_execute(&format!("INSERT INTO {child_post} VALUES (3, NULL)")).await?;

        // Removing a referenced cold parent is refused, whichever path is used.
        let err = fails_with(&db, &format!("SELECT koldstore.delete_row('{relation}'::regclass, '{{\"id\": 1}}'::jsonb)"))
            .await?
            .expect("delete_row of a referenced parent refused");
        assert!(err.starts_with("23503"), "{err}");
        let err = fails_with(&db, &format!("SELECT koldstore.purge_where('{relation}'::regclass, 't.id <= 4')"))
            .await?
            .expect("purge_where of a referenced parent refused");
        assert!(err.starts_with("23503"), "{err}");

        // Unreferenced cold parents can be purged; so can referenced ones once the children are gone.
        db.client.batch_execute(&format!("SELECT koldstore.purge_where('{relation}'::regclass, 't.id = 7')")).await?;
        db.client.batch_execute(&format!("DELETE FROM {child_pre}")).await?;
        db.client.batch_execute(&format!("SELECT koldstore.purge_where('{relation}'::regclass, 't.id = 1')")).await?;

        // Hydrated parents flush back to cold storage while children still reference them.
        db.flush_table(relation).await?;
        common::fence_async_mirror(&db.client).await?;
        let err = fails_with(&db, &format!("SELECT koldstore.purge_where('{relation}'::regclass, 't.id = 4')"))
            .await?
            .expect("still referenced after re-flush");
        assert!(err.starts_with("23503"), "{err}");
    }
    Ok(())
}
