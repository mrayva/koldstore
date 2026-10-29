//! `koldstore.export_table`: a merged hot+cold snapshot as plain Parquet plus a manifest.

use crate::common;

use anyhow::Result;
use arrow_array::{cast::AsArray, Array, Int64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[tokio::test]
async fn export_table_writes_merged_snapshot() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "export_table").await?;
        let table = db.create_indexed_items_table("export_items", 50).await?;
        db.manage_shared(&table.relation, "id").await?;
        let rel = &table.relation;
        db.client
            .batch_execute("SET koldstore.min_max_rows_per_file = 1")
            .await?;
        db.flush_table_with_force(rel, true).await?;

        // Hot changes on top of cold: an update, deletes of cold rows, and new rows.
        db.client
            .batch_execute("SET koldstore.hydrate_on_write = on")
            .await?;
        for sql in [
            format!("UPDATE {rel} SET title = 'changed' WHERE id IN (10, 20, 30, 40, 50)"),
            format!("DELETE FROM {rel} WHERE id IN (7, 14, 21, 28, 35, 42, 49)"),
            format!(
                "INSERT INTO {rel} (id, account_id, title, qty, category)
                 SELECT g, 1, 'new-' || g, 5, 'odd' FROM generate_series(51, 60) g"
            ),
        ] {
            db.client.batch_execute(&sql).await?;
        }
        // Delete tombstones reach the merge scan through the async mirror.
        db.client
            .batch_execute("SELECT koldstore.wait_for_async_mirror()")
            .await?;

        let storage = &db.storage_name;
        let summary: String = db
            .client
            .query_one(
                &format!("SELECT koldstore.export_table('{rel}'::regclass, '{storage}', 'exp/run1', 20)::text"),
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("export_table: {:?}", e.as_db_error()))?
            .get(0);
        let summary: serde_json::Value = serde_json::from_str(&summary)?;
        let expected: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {rel}"), &[])
            .await?
            .get(0);
        assert_eq!(summary["rows"], expected, "{summary}");
        assert!(
            summary["files"].as_i64().unwrap() >= 2,
            "max_rows_per_file=20 must split: {summary}"
        );

        let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(
            db.storage_root.join("exp/run1/export-manifest.json"),
        )?)?;
        assert_eq!(manifest["format"], "koldstore-export-v1");
        assert_eq!(manifest["row_count"], expected);
        let names: Vec<&str> = manifest["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            ["id", "account_id", "title", "qty", "category", "created_at"]
        );

        // Read every file back: only the table's columns, ids ascending, matching the live table.
        let mut ids = Vec::new();
        let mut titles_changed = 0;
        for file in manifest["files"].as_array().unwrap() {
            let path = db
                .storage_root
                .join("exp/run1")
                .join(file["path"].as_str().unwrap());
            let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?;
            let fields: Vec<String> = builder
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            assert_eq!(
                fields, names,
                "archive files carry only the table's columns"
            );
            for batch in builder.build()? {
                let batch = batch?;
                let id = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                ids.extend((0..id.len()).map(|i| id.value(i)));
                let title = batch.column(2).as_string::<i32>();
                titles_changed += (0..title.len())
                    .filter(|i| title.value(*i) == "changed")
                    .count();
            }
        }
        let live: Vec<i64> = db
            .client
            .query(&format!("SELECT id FROM {rel} ORDER BY id"), &[])
            .await?
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(
            ids, live,
            "export must equal the merged table, in key order"
        );
        assert!(
            !ids.contains(&7) && ids.contains(&60),
            "deletes and inserts are reflected: {ids:?}"
        );
        assert_eq!(
            titles_changed, 5,
            "updated rows (ids 10, 20, 30, 40, 50) carry their new value"
        );

        // A prefix is never reused, and must stay inside the storage.
        for bad in ["exp/run1", "../escape", "/", "a//b"] {
            let refused = db
                .client
                .simple_query(&format!(
                    "SELECT koldstore.export_table('{rel}'::regclass, '{storage}', '{bad}')"
                ))
                .await;
            assert!(refused.is_err(), "prefix `{bad}` should be refused");
        }
        let missing = db
            .client
            .simple_query(&format!(
                "SELECT koldstore.export_table('{rel}'::regclass, 'no_such_storage')"
            ))
            .await;
        assert!(missing.is_err());

        // Default prefix: dated folder under exports/.
        let default: String = db
            .client
            .query_one(
                &format!(
                    "SELECT koldstore.export_table('{rel}'::regclass, '{storage}')->>'prefix'"
                ),
                &[],
            )
            .await?
            .get(0);
        assert!(default.starts_with("exports/"), "{default}");

        // Import the archive into a new plain table: identical rows, one transaction.
        let imp = format!("{rel}_imp");
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {imp} (LIKE {rel} INCLUDING DEFAULTS INCLUDING CONSTRAINTS)"
            ))
            .await?;
        let done: String = db
            .client
            .query_one(
                &format!("SELECT koldstore.import_table('{imp}'::regclass, '{storage}', 'exp/run1')::text"),
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("import_table: {:?}", e.as_db_error()))?
            .get(0);
        let done: serde_json::Value = serde_json::from_str(&done)?;
        assert_eq!(done["rows"], expected, "{done}");
        let diff: i64 = db
            .client
            .query_one(
                &format!(
                    "SELECT count(*) FROM ((SELECT * FROM {rel} EXCEPT SELECT * FROM {imp}) \
                     UNION ALL (SELECT * FROM {imp} EXCEPT SELECT * FROM {rel})) d"
                ),
                &[],
            )
            .await?
            .get(0);
        assert_eq!(diff, 0, "imported rows must equal the exported table");
        // Refusals: non-empty target, managed target, unknown prefix.
        for target in [imp.as_str(), rel.as_str()] {
            let again = db
                .client
                .simple_query(&format!(
                    "SELECT koldstore.import_table('{target}'::regclass, '{storage}', 'exp/run1')"
                ))
                .await;
            assert!(again.is_err(), "import into {target} must be refused");
        }
        db.client
            .batch_execute(&format!("CREATE TABLE {imp}2 (id bigint PRIMARY KEY)"))
            .await?;
        let narrow = db
            .client
            .simple_query(&format!(
                "SELECT koldstore.import_table('{imp}2'::regclass, '{storage}', 'exp/run1')"
            ))
            .await;
        assert!(narrow.is_err(), "a target missing archive columns must be refused");
    }
    Ok(())
}
