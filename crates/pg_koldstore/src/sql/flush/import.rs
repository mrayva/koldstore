//! `koldstore.import_table`: loads a `koldstore.export_table` archive into an empty plain table.
//!
//! The target is an ordinary, unmanaged table the caller created (for example from `pg_dump
//! --schema-only`); the archive's rows are inserted into its heap in one transaction, so a failure
//! leaves it empty. Management and flushing to cold storage are separate, later steps
//! (`manage_table`). Keeping the target new and empty is what avoids any ownership, conflict or
//! schema-merge rules.

use koldstore_parquet::{read_export_file_rows, PgColumn, PgType};
use pgrx::datum::DatumWithOid;
use serde_json::{json, Value};

use super::export::{load_storage_client, validate_prefix};
use koldstore_storage::StorageClient;

/// Rows per `INSERT` statement.
const INSERT_ROWS: usize = 1000;

fn manifest_columns(manifest: &Value) -> Result<Vec<PgColumn>, String> {
    manifest["columns"]
        .as_array()
        .ok_or("export manifest has no columns")?
        .iter()
        .map(|column| {
            let name = column["name"]
                .as_str()
                .ok_or("export manifest column has no name")?;
            let type_name = column["type"]
                .as_str()
                .ok_or("export manifest column has no type")?;
            let pg_type = PgType::from_postgres_name(type_name)
                .map_err(|error| format!("column `{name}`: {error}"))?;
            Ok(PgColumn::new(
                name.to_string(),
                pg_type,
                column["nullable"].as_bool().unwrap_or(true),
            ))
        })
        .collect()
}

/// Imports the archive under `prefix` of `storage_name` into `table_oid`.
pub(crate) fn import_table_value(
    table_oid: pgrx::pg_sys::Oid,
    storage_name: &str,
    prefix: &str,
) -> Result<Value, String> {
    if crate::catalog::cache::is_managed_relation(table_oid) {
        return Err(
            "the target is a managed table; import into a new plain table, then manage it"
                .to_string(),
        );
    }
    let args = [DatumWithOid::from(table_oid)];
    let relkind = pgrx::Spi::get_one_with_args::<String>(
        "SELECT relkind::text FROM pg_class WHERE oid = $1",
        &args,
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_default();
    if relkind != "r" {
        return Err("the target must be an ordinary (non-partitioned) table".to_string());
    }
    let relation = pgrx::Spi::get_one_with_args::<String>(
        "SELECT format('%I.%I', n.nspname, c.relname) FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = $1",
        &args,
    )
    .map_err(|error| error.to_string())?
    .ok_or("target table not found")?;
    // Held to commit: nothing else may write while the archive loads.
    pgrx::Spi::run(&format!("LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE"))
        .map_err(|error| error.to_string())?;
    let empty =
        pgrx::Spi::get_one::<bool>(&format!("SELECT NOT EXISTS (SELECT 1 FROM {relation})"))
            .map_err(|error| error.to_string())?
            .unwrap_or(false);
    if !empty {
        return Err("the target table must be empty".to_string());
    }

    let client = load_storage_client(storage_name)?;
    let prefix = validate_prefix(prefix)?;
    let manifest_key = format!("{prefix}/export-manifest.json");
    let manifest: Value = serde_json::from_slice(
        &client
            .get(&manifest_key)
            .map_err(|error| format!("reading `{manifest_key}` failed: {error}"))?,
    )
    .map_err(|error| format!("`{manifest_key}` is not valid JSON: {error}"))?;
    if manifest["format"] != "koldstore-export-v1" {
        return Err(format!("unsupported export format {}", manifest["format"]));
    }
    let columns = manifest_columns(&manifest)?;
    for column in &columns {
        let present = pgrx::Spi::get_one_with_args::<bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = $1 AND attname = $2 \
             AND attnum > 0 AND NOT attisdropped AND attgenerated = '')",
            &[
                DatumWithOid::from(table_oid),
                DatumWithOid::from(column.name.as_str()),
            ],
        )
        .map_err(|error| error.to_string())?
        .unwrap_or(false);
        if !present {
            return Err(format!(
                "the target table has no insertable column `{}`",
                column.name
            ));
        }
    }
    let column_list = columns
        .iter()
        .map(|column| koldstore_common::quote_ident(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_sql = format!(
        "INSERT INTO {relation} ({column_list}) OVERRIDING SYSTEM VALUE \
         SELECT {column_list} FROM jsonb_populate_recordset(NULL::{relation}, $1)"
    );

    let mut total = 0_u64;
    let files = manifest["files"]
        .as_array()
        .ok_or("export manifest has no file list")?;
    for file in files {
        let name = file["path"]
            .as_str()
            .ok_or("export manifest file has no path")?;
        let key = format!("{prefix}/{name}");
        let bytes = client
            .get(&key)
            .map_err(|error| format!("reading `{key}` failed: {error}"))?;
        // Archives written before checksums existed carry none; everything else must match.
        if let Some(want) = file["sha256"].as_str() {
            let have = koldstore_storage::content_checksum_sha256_hex(&bytes);
            if have != want {
                return Err(format!("`{key}` is corrupt: sha256 {have}, the manifest says {want}"));
            }
        }
        let expected = file["rows"].as_u64();
        let mut file_rows = 0_u64;
        read_export_file_rows(bytes, &columns, |rows| {
            for chunk in rows.chunks(INSERT_ROWS) {
                pgrx::Spi::run_with_args(
                    &insert_sql,
                    &[DatumWithOid::from(pgrx::JsonB(Value::Array(
                        chunk.to_vec(),
                    )))],
                )
                .map_err(|error| error.to_string())?;
                file_rows += chunk.len() as u64;
            }
            Ok(())
        })
        .map_err(|error| format!("`{key}`: {error}"))?;
        if expected.is_some_and(|rows| rows != file_rows) {
            return Err(format!(
                "`{key}` holds {file_rows} rows, the manifest says {expected:?}"
            ));
        }
        total += file_rows;
    }
    if manifest["row_count"]
        .as_u64()
        .is_some_and(|rows| rows != total)
    {
        return Err(format!(
            "imported {total} rows, the manifest says {}",
            manifest["row_count"]
        ));
    }
    Ok(json!({ "storage": storage_name, "prefix": prefix, "rows": total, "files": files.len() }))
}

/// Loads a `koldstore.export_table` archive into an empty, unmanaged table.
///
/// SQL contract: `koldstore.import_table(table_name regclass, storage text, prefix text) → jsonb`.
///
/// Superuser only, because it reads through a registered storage's credentials. The archive's
/// columns must all exist in the target; the load is one transaction.
#[pgrx::pg_extern(name = "import_table", schema = "koldstore", security_definer)]
pub fn import_table_pg(table_name: pgrx::PgRelation, storage: &str, prefix: &str) -> pgrx::JsonB {
    crate::security::require_superuser("import a table archive");
    import_table_value(table_name.oid(), storage, prefix)
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("import_table failed: {error}"))
}
