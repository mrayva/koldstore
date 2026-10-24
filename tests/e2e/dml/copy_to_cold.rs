use crate::common;

use anyhow::{ensure, Result};

#[tokio::test]
async fn plain_copy_to_exports_hot_and_cold_rows() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "copy_to_cold").await?;
        let table = db.create_indexed_items_table("copy_items", 10).await?;
        let relation = &table.relation;
        db.manage_shared(relation, "id").await?;
        db.flush_table(relation).await?;
        common::fence_async_mirror(&db.client).await?;
        db.client
            .batch_execute(&format!("INSERT INTO {relation} (id, account_id, title, qty, category) VALUES (900, 1, 't', 1, 'x')"))
            .await?;

        let merged: i64 = db.client.query_one(&format!("SELECT count(*) FROM {relation}"), &[]).await?.get(0);
        ensure!(merged == 11, "expected 10 cold + 1 hot rows, got {merged}");
        let dir = db.storage_root.display();

        // The plain table form (what `pg_dump` issues) exports hot and cold rows alike.
        let all = format!("{dir}/copy_all.txt");
        db.client.batch_execute(&format!("COPY {relation} TO '{all}'")).await?;
        let lines = std::fs::read_to_string(&all)?.lines().count();
        ensure!(lines == 11, "plain COPY exported {lines} rows, expected 11");

        // An explicit column list and options are honoured.
        let cols = format!("{dir}/copy_cols.csv");
        db.client.batch_execute(&format!("COPY {relation} (id, qty) TO '{cols}' WITH (FORMAT csv, HEADER)")).await?;
        let text = std::fs::read_to_string(&cols)?;
        ensure!(text.lines().next() == Some("id,qty"), "header missing: {text:?}");
        ensure!(text.lines().count() == 12, "csv exported {} lines, expected header + 11", text.lines().count());
        ensure!(text.lines().any(|l| l.starts_with("900,")) && text.lines().any(|l| l.starts_with("3,")));

        // The query form is unchanged.
        let query = format!("{dir}/copy_query.txt");
        db.client.batch_execute(&format!("COPY (SELECT id FROM {relation}) TO '{query}'")).await?;
        ensure!(std::fs::read_to_string(&query)?.lines().count() == 11);
    }
    Ok(())
}
