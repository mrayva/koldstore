//! `koldstore.export_table` on a user-scoped table exports the scope named by `koldstore.user_id`.

use anyhow::Result;

use crate::common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn export_table_exports_one_scope() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix()
        .into_iter()
        .next()
        .expect("PostgreSQL target");
    let db = common::TestDb::start(target, "export_scope").await?;
    let table = db.create_user_notes_table("export_scope_notes").await?;
    let rel = table.relation.clone();
    db.manage_user_scoped(&rel, "user_id").await?;
    db.client
        .execute(
            "SELECT koldstore.set_table_auto_flush($1::text::regclass, false)",
            &[&rel],
        )
        .await?;
    db.client
        .batch_execute("SET koldstore.min_max_rows_per_file = 1")
        .await?;
    anyhow::ensure!(db.flush_table(&rel).await? > 0, "expected a cold flush");
    let storage = &db.storage_name;

    let unscoped = db
        .client
        .simple_query(&format!(
            "SELECT koldstore.export_table('{rel}'::regclass, '{storage}', 'scoped/none')"
        ))
        .await;
    anyhow::ensure!(unscoped.is_err(), "a scoped table needs koldstore.user_id");

    db.client
        .batch_execute("SET koldstore.user_id = 'user-a'")
        .await?;
    let summary: String = db
        .client
        .query_one(
            &format!("SELECT koldstore.export_table('{rel}'::regclass, '{storage}')::text"),
            &[],
        )
        .await
        .map_err(|e| anyhow::anyhow!("export_table: {:?}", e.as_db_error()))?
        .get(0);
    let summary: serde_json::Value = serde_json::from_str(&summary)?;
    anyhow::ensure!(
        summary["rows"] == 2 && summary["scope"] == "user-a",
        "{summary}"
    );
    let prefix = summary["prefix"].as_str().unwrap();
    anyhow::ensure!(prefix.contains("/scope-user-a/"), "{prefix}");

    let imp = format!("{rel}_imp");
    db.client
        .batch_execute(&format!(
            "CREATE TABLE {imp} (LIKE {rel} INCLUDING DEFAULTS)"
        ))
        .await?;
    db.client
        .query_one(
            &format!("SELECT koldstore.import_table('{imp}'::regclass, '{storage}', '{prefix}')"),
            &[],
        )
        .await?;
    let owners: Vec<String> = db
        .client
        .query(&format!("SELECT DISTINCT user_id FROM {imp}"), &[])
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    anyhow::ensure!(
        owners == ["user-a"],
        "only the exported scope's rows: {owners:?}"
    );
    Ok(())
}
