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

        // Guards follow the foreign keys: dropping a child table or its constraint removes them.
        let guards = format!(
            "SELECT count(*)::int FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = 'koldstore' AND p.prosrc LIKE '%_cold_fk_child_hydrate(' || '{relation}'::regclass::oid || '::oid,%'"
        );
        let guards = guards.as_str();
        assert_eq!(db.client.query_one(guards, &[]).await?.get::<_, i32>(0), 2);
        db.client.batch_execute(&format!("DROP TABLE {child_post}")).await?;
        assert_eq!(db.client.query_one(guards, &[]).await?.get::<_, i32>(0), 1);
        db.client.batch_execute(&format!("ALTER TABLE {child_pre} DROP CONSTRAINT ifk_child_pre_item_id_fkey")).await?;
        assert_eq!(db.client.query_one(guards, &[]).await?.get::<_, i32>(0), 0);
        db.client.batch_execute(&format!("SELECT koldstore.purge_where('{relation}'::regclass, 't.id = 4')")).await?;
    }
    Ok(())
}

/// `ON DELETE CASCADE` and `SET NULL` children of a managed parent are accepted by
/// `manage_table` and act on cold parents, through `delete_row` and `purge_where`.
#[tokio::test]
async fn inbound_fk_cascade_and_set_null_act_on_cold_parents() -> Result<()> {
    let target = common::scenario_pg_matrix().into_iter().next().expect("PostgreSQL target");
    let db = common::TestDb::start(target, "inbound_fk_cascade").await?;
    let table = db.create_indexed_items_table("ifc_items", 12).await?;
    let relation = &table.relation;
    let schema = relation.split('.').next().unwrap();
    let (casc, setnull) = (format!("{schema}.ifc_casc"), format!("{schema}.ifc_setnull"));
    db.client
        .batch_execute(&format!(
            "CREATE TABLE {casc} (id int PRIMARY KEY, item_id int REFERENCES {relation}(id) ON DELETE CASCADE); \
             CREATE TABLE {setnull} (id int PRIMARY KEY, item_id int REFERENCES {relation}(id) ON DELETE SET NULL); \
             INSERT INTO {casc} SELECT g, g FROM generate_series(1, 6) g; \
             INSERT INTO {setnull} SELECT g, g FROM generate_series(1, 6) g"
        ))
        .await?;
    // No allow_fk_hot_only needed: the actions are enforced globally.
    db.manage_shared(relation, "id").await?;
    db.flush_table(relation).await?;
    common::fence_async_mirror(&db.client).await?;
    let count = |sql: String| {
        let db = &db;
        async move { anyhow::Ok(db.client.query_one(&sql, &[]).await?.get::<_, i64>(0)) }
    };

    // delete_row on a cold parent hydrates it, so the native actions fire.
    db.client.batch_execute(&format!("SELECT koldstore.delete_row('{relation}'::regclass, '{{\"id\": 1}}'::jsonb)")).await?;
    assert_eq!(count(format!("SELECT count(*) FROM {casc} WHERE item_id = 1")).await?, 0);
    assert_eq!(count(format!("SELECT count(*) FROM {setnull} WHERE id = 1 AND item_id IS NULL")).await?, 1);

    // purge_where applies the action to the referencing rows of cold parents.
    db.flush_table(relation).await?;
    common::fence_async_mirror(&db.client).await?;
    db.client.batch_execute(&format!("SELECT koldstore.purge_where('{relation}'::regclass, 't.id IN (3, 4)')")).await?;
    assert_eq!(count(format!("SELECT count(*) FROM {casc} WHERE id IN (3, 4)")).await?, 0);
    assert_eq!(count(format!("SELECT count(*) FROM {setnull} WHERE id IN (3, 4) AND item_id IS NULL")).await?, 2);
    assert_eq!(count(format!("SELECT count(*) FROM {casc}")).await?, 3);
    assert_eq!(count(format!("SELECT count(*) FROM {relation} WHERE id IN (3, 4)")).await?, 0);

    // A child may still reference a cold parent.
    db.client.batch_execute(&format!("INSERT INTO {casc} VALUES (50, 5)")).await?;
    Ok(())
}
