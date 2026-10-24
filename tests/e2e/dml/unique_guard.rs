use crate::common;

use anyhow::Result;

async fn insert_err(db: &common::TestDb, sql: &str) -> Result<Option<String>> {
    Ok(db.client.batch_execute(sql).await.err().map(|error| {
        error.as_db_error().map_or_else(|| error.to_string(), |db| format!("{}: {}", db.code().code(), db.message()))
    }))
}

#[tokio::test]
async fn non_pk_unique_is_enforced_across_hot_and_cold() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "unique_guard").await?;
        let table = db.create_indexed_items_table("uniq_items", 10).await?;
        let relation = &table.relation;
        db.client
            .batch_execute(&format!(
                "ALTER TABLE {relation} ADD COLUMN code text; \
                 UPDATE {relation} SET code = 'c' || id WHERE id <= 5; \
                 ALTER TABLE {relation} ADD CONSTRAINT uniq_items_code_key UNIQUE (code)"
            ))
            .await?;
        // Managing a table with a non-PK UNIQUE constraint is accepted.
        db.manage_shared(relation, "id").await?;
        db.flush_table(relation).await?;
        common::fence_async_mirror(&db.client).await?;

        // INSERT of a value that only a cold row holds is refused (SQLSTATE 23505).
        let err = insert_err(
            &db,
            &format!("INSERT INTO {relation} (id, account_id, title, qty, category, code) VALUES (500, 1, 't', 1, 'x', 'c3')"),
        )
        .await?
        .expect("cold duplicate must be refused");
        assert!(err.starts_with("23505"), "{err}");

        // A fresh value and NULLs (any number of them) are fine.
        db.client
            .batch_execute(&format!(
                "INSERT INTO {relation} (id, account_id, title, qty, category, code) VALUES \
                 (501, 1, 't', 1, 'x', 'fresh'), (502, 1, 't', 1, 'x', NULL), (503, 1, 't', 1, 'x', NULL)"
            ))
            .await?;

        // UPDATE into a value held by a cold row is refused; the cold row's own PK may keep its value.
        let err = insert_err(&db, &format!("UPDATE {relation} SET code = 'c4' WHERE id = 501"))
            .await?
            .expect("update onto a cold value must be refused");
        assert!(err.starts_with("23505"), "{err}");
        db.client.batch_execute(&format!("UPDATE {relation} SET qty = qty + 1, code = 'fresh' WHERE id = 501")).await?;

        // After the holder is deleted, the value is free again.
        db.client.batch_execute(&format!("SELECT koldstore.delete_row('{relation}'::regclass, '{{\"id\": 3}}'::jsonb)")).await?;
        common::fence_async_mirror(&db.client).await?;
        db.client
            .batch_execute(&format!("INSERT INTO {relation} (id, account_id, title, qty, category, code) VALUES (504, 1, 't', 1, 'x', 'c3')"))
            .await?;

        // A constraint added after manage_table is enforced too (guard is re-planned by the DDL hook).
        db.client
            .batch_execute(&format!(
                "ALTER TABLE {relation} ADD COLUMN ext int; \
                 INSERT INTO {relation} (id, account_id, title, qty, category, ext) VALUES (600, 1, 't', 1, 'x', 6)"
            ))
            .await?;
        db.client.batch_execute(&format!("ALTER TABLE {relation} ADD CONSTRAINT uniq_items_ext_key UNIQUE (ext)")).await?;
        db.flush_table(relation).await?;
        common::fence_async_mirror(&db.client).await?;
        let err = insert_err(
            &db,
            &format!("INSERT INTO {relation} (id, account_id, title, qty, category, ext) VALUES (505, 1, 't', 1, 'x', 6)"),
        )
        .await?
        .expect("cold duplicate on a later-added constraint must be refused");
        assert!(err.starts_with("23505"), "{err}");
    }
    Ok(())
}
