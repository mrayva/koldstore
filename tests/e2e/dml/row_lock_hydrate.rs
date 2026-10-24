use crate::common;

use anyhow::{ensure, Result};

#[tokio::test]
async fn select_for_update_by_primary_key_hydrates_cold_row() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "row_lock_hydrate").await?;
        let table = db.create_indexed_items_table("lock_items", 10).await?;
        let relation = &table.relation;
        db.manage_shared(relation, "id").await?;
        db.flush_table(relation).await?;
        common::fence_async_mirror(&db.client).await?;

        let lock = |clause: &str, id: i32| format!("SELECT id, qty FROM {relation} WHERE id = {id} FOR {clause}");

        // Off by default: a row lock that cold data could contribute to is refused.
        let err = db.client.query(&lock("UPDATE", 3), &[]).await.err().map(|e| e.to_string());
        ensure!(err.is_some(), "FOR UPDATE on a cold table must be refused without hydrate_on_write");

        db.client.batch_execute("SET koldstore.hydrate_on_write = on").await?;

        // Each strength locks the (hydrated) cold row and returns it.
        for (clause, id) in [("UPDATE", 3), ("NO KEY UPDATE", 4), ("SHARE", 5), ("KEY SHARE", 6)] {
            let rows = db.client.query(&lock(clause, id), &[]).await?;
            ensure!(rows.len() == 1 && rows[0].get::<_, i64>(0) == i64::from(id), "FOR {clause} on cold id {id}: {} rows", rows.len());
        }

        // Read-modify-write: lock, then update, in one transaction; the value sticks.
        db.client.batch_execute("BEGIN").await?;
        let qty: i32 = db.client.query_one(&lock("UPDATE", 7), &[]).await?.get(1);
        db.client.batch_execute(&format!("UPDATE {relation} SET qty = {} WHERE id = 7", qty + 1000)).await?;
        db.client.batch_execute("COMMIT").await?;
        let after: i32 = db.client.query_one(&format!("SELECT qty FROM {relation} WHERE id = 7"), &[]).await?.get(0);
        ensure!(after == qty + 1000, "update after lock lost: {after} vs {}", qty + 1000);
        let copies: i64 = db.client.query_one(&format!("SELECT count(*) FROM {relation} WHERE id = 7"), &[]).await?.get(0);
        ensure!(copies == 1, "expected one visible row for id 7, got {copies}");

        // The lock is a real heap tuple lock: a second session cannot take it while this one holds it.
        let peer = common::connect_peer(&db).await?;
        peer.batch_execute("SET koldstore.hydrate_on_write = on").await?;
        db.client.batch_execute("BEGIN").await?;
        db.client.query(&lock("UPDATE", 3), &[]).await?;
        let busy = peer.query(&format!("SELECT id FROM {relation} WHERE id = 3 FOR UPDATE NOWAIT"), &[]).await.err();
        db.client.batch_execute("COMMIT").await?;
        let code = busy.as_ref().and_then(|e| e.as_db_error()).map(|e| e.code().code().to_string());
        ensure!(code.as_deref() == Some("55P03"), "peer should hit lock_not_available, got {code:?}");

        // A missing key returns nothing, and a hot row still locks.
        ensure!(db.client.query(&lock("UPDATE", 9999), &[]).await?.is_empty());
        db.client
            .batch_execute(&format!("INSERT INTO {relation} (id, account_id, title, qty, category) VALUES (800, 1, 't', 1, 'x')"))
            .await?;
        ensure!(db.client.query(&lock("UPDATE", 800), &[]).await?.len() == 1);

        // Non-primary-key predicates hydrate every cold row they select, then lock them.
        let rows = db.client.query(&format!("SELECT id FROM {relation} WHERE id BETWEEN 1 AND 2 ORDER BY id FOR UPDATE"), &[]).await?;
        ensure!(rows.len() == 2 && rows[0].get::<_, i64>(0) == 1, "range lock returned {} rows", rows.len());
        db.client.batch_execute("BEGIN").await?;
        let n = db.client.query(&format!("SELECT id FROM {relation} WHERE id > 8 FOR SHARE"), &[]).await?.len();
        let busy = peer.query(&format!("SELECT id FROM {relation} WHERE id = 9 FOR UPDATE NOWAIT"), &[]).await.err();
        db.client.batch_execute("COMMIT").await?;
        ensure!(n >= 2, "expected the cold rows id > 8, got {n}");
        // The peer is refused either by the row lock or by the hydration retry guard (the key is mid-change).
        ensure!(busy.is_some(), "a peer must not be able to take a row the range lock holds");
        let limited = db.client.query(&format!("SELECT id FROM {relation} WHERE qty >= 0 ORDER BY id LIMIT 2 FOR UPDATE"), &[]).await?;
        ensure!(limited.len() == 2, "LIMIT lock returned {} rows", limited.len());

        // Joins and sub-queries: the locked table's cold rows are hydrated via the statement's own FROM/WHERE.
        let schema = relation.split('.').next().unwrap();
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {schema}.lock_dim (id bigint PRIMARY KEY, tag text); \
                 INSERT INTO {schema}.lock_dim VALUES (3, 'x'), (5, 'x'), (6, 'y')"
            ))
            .await?;
        // All three managed rows 3/5/6 were already hydrated above, so flush again to make them cold.
        db.flush_table_with_force(relation, true).await?;
        common::fence_async_mirror(&db.client).await?;
        let joined = db
            .client
            .query(
                &format!("SELECT t.id FROM {relation} t JOIN {schema}.lock_dim d ON d.id = t.id WHERE d.tag = 'x' ORDER BY t.id FOR UPDATE OF t"),
                &[],
            )
            .await?;
        let ids: Vec<i64> = joined.iter().map(|r| r.get(0)).collect();
        ensure!(ids == vec![3, 5], "join lock returned {ids:?}");
        db.client.batch_execute(&format!("SELECT 1 FROM {schema}.lock_dim")).await?;
        let sub = db
            .client
            .query(&format!("SELECT id FROM {relation} WHERE id IN (SELECT id FROM {schema}.lock_dim WHERE tag = 'y') FOR UPDATE"), &[])
            .await?;
        ensure!(sub.len() == 1 && sub[0].get::<_, i64>(0) == 6, "sub-query lock returned {} rows", sub.len());

        // SERIALIZABLE cannot hydrate: fail closed instead of skipping the cold row.
        db.client.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE").await?;
        let err = db.client.query(&lock("UPDATE", 8), &[]).await.err();
        db.client.batch_execute("ROLLBACK").await?;
        ensure!(err.is_some(), "SERIALIZABLE FOR UPDATE on a cold row must fail closed");
        db.client.batch_execute("RESET koldstore.hydrate_on_write").await?;
    }
    Ok(())
}
