use crate::common;

use anyhow::Result;

#[tokio::test]
async fn insert_on_conflict_hydrates_cold_primary_key() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "upsert_hydrate").await?;
        let table = db.create_indexed_items_table("upsert_items", 10).await?;
        let relation = &table.relation;
        db.manage_shared(relation, "id").await?;
        db.flush_table(relation).await?;
        common::fence_async_mirror(&db.client).await?;

        let insert = |tail: &str| {
            format!("INSERT INTO {relation} (id, account_id, title, qty, category) VALUES (3, 1, 'upserted', 777, 'x') {tail}")
        };

        // Without hydrate_on_write the upsert onto a cold key is still refused.
        let err = db.client.batch_execute(&insert("ON CONFLICT (id) DO NOTHING")).await.err();
        assert!(err.is_some(), "upsert onto a cold key must be refused when hydrate_on_write is off");

        db.client.batch_execute("SET koldstore.hydrate_on_write = on").await?;

        // DO NOTHING leaves the cold row alone and does not error.
        db.client.batch_execute(&insert("ON CONFLICT (id) DO NOTHING")).await?;
        let qty: i32 = db.client.query_one(&format!("SELECT qty FROM {relation} WHERE id = 3"), &[]).await?.get(0);
        assert_ne!(qty, 777);

        // DO UPDATE changes the cold row.
        db.client.batch_execute(&insert("ON CONFLICT (id) DO UPDATE SET qty = EXCLUDED.qty")).await?;
        let rows = db.client.query(&format!("SELECT qty FROM {relation} WHERE id = 3"), &[]).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i32>(0), 777);

        // A fresh key and a hot key still upsert normally.
        for qty in [2, 3] {
            db.client
                .batch_execute(&format!(
                    "INSERT INTO {relation} (id, account_id, title, qty, category) VALUES (900, 1, 't', 1, 'x') \
                     ON CONFLICT (id) DO UPDATE SET qty = {qty}"
                ))
                .await?;
        }
        let qty: i32 = db.client.query_one(&format!("SELECT qty FROM {relation} WHERE id = 900"), &[]).await?.get(0);
        assert_eq!(qty, 3);

        // Survives a flush: still exactly one row for the key.
        db.flush_table(relation).await?;
        common::fence_async_mirror(&db.client).await?;
        let rows = db.client.query(&format!("SELECT qty FROM {relation} WHERE id = 3"), &[]).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i32>(0), 777);

        // Aborted savepoint leaves no stale state.
        db.client
            .batch_execute(&format!(
                "BEGIN; SAVEPOINT s; {}; ROLLBACK TO s; COMMIT",
                insert("ON CONFLICT (id) DO UPDATE SET qty = 5")
            ))
            .await?;
        db.client.batch_execute("RESET koldstore.hydrate_on_write").await?;
    }
    Ok(())
}
