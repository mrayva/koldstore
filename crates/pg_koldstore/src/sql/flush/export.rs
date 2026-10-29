//! `koldstore.export_table`: a portable snapshot of one managed table.
//!
//! The export reads the table the way a query does (hot rows merged with cold segments, newest
//! version of each key, delete markers applied) through one cursor, so it sees a single snapshot.
//! It writes plain Parquet files holding only the table's own columns, in primary-key order, plus an
//! `export-manifest.json` that lists the files, row counts and column types. The destination is a
//! registered storage (`koldstore.register_storage`), under a prefix that never overwrites an
//! earlier export. The table's own cold objects are not touched, and the export does not depend on
//! them afterwards.
//!
//! The table-job lock is held for the call, so a flush or compaction cannot retire a cold segment
//! under the cursor. Hot DML continues; the cursor's snapshot is unaffected by it.

use koldstore_parquet::{
    CleanColdRecordBatchBuilder, PgColumn, StreamingParquetSegmentWriter, WriterOptions,
};
use koldstore_storage::{PutPrecondition, StorageClient};
use pgrx::datum::DatumWithOid;
use serde_json::{json, Value};

use super::execute::load_flush_prepared_context;
use super::mirror_fetch::read_column;

/// Archive format version written to `export-manifest.json`.
const FORMAT: &str = "koldstore-export-v1";
/// Rows fetched from the cursor per round trip.
const FETCH_ROWS: i64 = 4096;

/// A custom prefix must stay inside the storage: relative, no `..`, no empty parts.
pub(super) fn validate_prefix(prefix: &str) -> Result<String, String> {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        return Err("export prefix must not be empty".to_string());
    }
    if trimmed
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(format!(
            "export prefix `{prefix}` has an empty or relative part"
        ));
    }
    Ok(trimmed.to_string())
}

pub(super) fn load_storage_client(name: &str) -> Result<koldstore_storage::ObjectStoreClient, String> {
    let text = pgrx::Spi::get_one_with_args::<String>(
        "SELECT jsonb_build_object('storage_type', storage_type, 'base_path', base_path,
                                   'credentials', COALESCE(credentials, '{}'::jsonb),
                                   'config', COALESCE(config, '{}'::jsonb))::text
         FROM koldstore.storage WHERE name = $1",
        &[DatumWithOid::from(name)],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| format!("storage `{name}` does not exist"))?;
    let ctx: Value = serde_json::from_str(&text).map_err(|error| error.to_string())?;
    crate::object_store::open_managed_object_store_client(
        ctx["storage_type"].as_str().unwrap_or("filesystem"),
        ctx["base_path"].as_str().unwrap_or_default(),
        &ctx["credentials"],
        &ctx["config"],
    )
    .map_err(|error| error.to_string())
}

/// One Parquet file being filled.
struct OpenFile {
    writer: StreamingParquetSegmentWriter,
    rows: i64,
}

struct Exporter<'a> {
    client: &'a koldstore_storage::ObjectStoreClient,
    prefix: String,
    parquet_columns: Vec<PgColumn>,
    primary_key: Vec<String>,
    schema_version: u32,
    writer_options: WriterOptions,
    max_rows_per_file: i64,
    row_group_size: usize,
    builder: Option<CleanColdRecordBatchBuilder>,
    file: Option<OpenFile>,
    files: Vec<Value>,
    total_rows: i64,
}

impl Exporter<'_> {
    /// Rows to buffer before the next flush: a full row group, or what still fits in the open file.
    fn batch_limit(&self) -> usize {
        let used = self.file.as_ref().map_or(0, |file| file.rows);
        let room = usize::try_from((self.max_rows_per_file - used).max(1)).unwrap_or(usize::MAX);
        self.row_group_size.min(room)
    }

    /// Moves the rows buffered in the batch builder into the open file.
    fn flush_batch(&mut self) -> Result<(), String> {
        let Some(builder) = self.builder.take() else {
            return Ok(());
        };
        if builder.row_count() == 0 {
            self.builder = Some(builder);
            return Ok(());
        }
        let rows = i64::try_from(builder.row_count()).unwrap_or(i64::MAX);
        let full = builder.finish()?.batch;
        // The builder appends the cold-tier bookkeeping columns (seq, op, deleted, schema_version);
        // an archive carries only the table's own columns.
        let indices: Vec<usize> = (0..self.parquet_columns.len()).collect();
        let batch = full.project(&indices).map_err(|error| error.to_string())?;
        if self.file.is_none() {
            let writer =
                StreamingParquetSegmentWriter::try_new(batch.schema(), self.writer_options.clone())
                    .map_err(|error| error.to_string())?;
            self.file = Some(OpenFile { writer, rows: 0 });
        }
        let file = self.file.as_mut().expect("opened above");
        file.writer
            .write_batch(&batch)
            .map_err(|error| error.to_string())?;
        file.rows += rows;
        self.total_rows += rows;
        self.builder = Some(CleanColdRecordBatchBuilder::new(&self.parquet_columns)?);
        if file.rows >= self.max_rows_per_file {
            self.close_file()?;
        }
        Ok(())
    }

    fn close_file(&mut self) -> Result<(), String> {
        let Some(file) = self.file.take() else {
            return Ok(());
        };
        let bytes = file.writer.finish().map_err(|error| error.to_string())?;
        let name = format!("part-{:05}.parquet", self.files.len());
        let key = format!("{}/{name}", self.prefix);
        self.client
            .put(&key, &bytes, PutPrecondition::CreateIfAbsent)
            .map_err(|error| format!("writing `{key}` failed: {error}"))?;
        self.files
            .push(json!({
            "path": name,
            "rows": file.rows,
            "bytes": bytes.len(),
            "sha256": koldstore_storage::content_checksum_sha256_hex(&bytes),
        }));
        Ok(())
    }
}

/// Exports `table_oid` into `storage_name` under `prefix` and returns the archive summary.
pub(crate) fn export_table_value(
    table_oid: pgrx::pg_sys::Oid,
    storage_name: &str,
    prefix: Option<&str>,
    max_rows_per_file: Option<i64>,
) -> Result<Value, String> {
    let _lock = crate::sql::job_lock::TableJobLockGuard::try_lock(table_oid)?.ok_or(
        "a flush or compaction is running on this table; retry the export when it finishes",
    )?;
    let ctx =
        load_flush_prepared_context(table_oid, false, uuid::Uuid::new_v4(), uuid::Uuid::new_v4())?;
    // A user-scoped table reads one scope at a time (`koldstore.user_id`); the export is that
    // scope's rows, which is also what a per-tenant backup or data-subject export wants.
    let scope = if ctx.snapshot.scope_column_id.is_some() {
        let scope = pgrx::Spi::get_one::<String>("SELECT current_setting('koldstore.user_id', true)")
            .map_err(|error| error.to_string())?
            .unwrap_or_default();
        if scope.is_empty() {
            return Err(
                "this table is user-scoped: SET koldstore.user_id to the scope to export first".to_string(),
            );
        }
        Some(scope)
    } else {
        None
    };
    let client = load_storage_client(storage_name)?;
    let relation = format!(
        "{}.{}",
        koldstore_common::quote_ident(&ctx.relation.namespace),
        koldstore_common::quote_ident(&ctx.relation.name)
    );
    let prefix = match prefix {
        Some(custom) => validate_prefix(custom)?,
        None => {
            let stamp = pgrx::Spi::get_one::<String>(
                "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYYMMDD\"T\"HH24MISS\"Z\"')",
            )
            .map_err(|error| error.to_string())?
            .unwrap_or_default();
            let scope_part = scope.as_deref().map_or_else(String::new, |scope| {
                let safe: String = scope
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') { c } else { '_' })
                    .collect();
                format!("/scope-{safe}")
            });
            format!(
                "exports/{}.{}{scope_part}/{stamp}",
                ctx.relation.namespace, ctx.relation.name
            )
        }
    };
    let manifest_key = format!("{prefix}/export-manifest.json");
    // An existing manifest means an earlier export owns this prefix.
    if client.get(&manifest_key).is_ok() {
        return Err(format!("export prefix `{prefix}` already holds an export"));
    }

    let columns = &ctx.catalog.columns;
    let primary_key: Vec<String> = ctx
        .catalog
        .primary_key
        .columns
        .iter()
        .filter_map(|key| ctx.catalog.column_by_id(key.column_id))
        .map(|column| column.name.clone())
        .collect();
    if primary_key.is_empty() {
        return Err("export needs a primary key".to_string());
    }
    let parquet_columns: Vec<PgColumn> = columns
        .iter()
        .map(|column| PgColumn::new(column.name.clone(), column.pg_type, column.nullable))
        .collect();
    let select_list = columns
        .iter()
        .map(|column| koldstore_common::quote_ident(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let order_by = primary_key
        .iter()
        .map(|name| koldstore_common::quote_ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    // Scope isolation on a user-scoped table is row-level security, which a superuser bypasses, so
    // the scope is filtered explicitly rather than trusted to the GUC alone.
    let scope_filter = match (&scope, ctx.snapshot.scope_column_id) {
        (Some(_), Some(scope_id)) => {
            let column = ctx
                .catalog
                .columns
                .iter()
                .find(|column| column.column_id == scope_id)
                .ok_or("scope column is missing from the catalog")?;
            format!(" WHERE {}::text = $1", koldstore_common::quote_ident(&column.name))
        }
        _ => String::new(),
    };
    let query = format!("SELECT {select_list} FROM {relation}{scope_filter} ORDER BY {order_by}");
    let query_args: Vec<DatumWithOid> = scope.iter().map(|scope| DatumWithOid::from(scope.as_str())).collect();

    let row_group_size = ctx.parquet_row_group_size.max(1);
    let max_rows_per_file = max_rows_per_file
        .filter(|value| *value > 0)
        .unwrap_or_else(|| i64::try_from(ctx.max_rows_per_file).unwrap_or(i64::MAX));
    let mut exporter = Exporter {
        client: &client,
        prefix: prefix.clone(),
        parquet_columns: parquet_columns.clone(),
        primary_key,
        schema_version: u32::try_from(ctx.storage.schema_version).unwrap_or(1),
        writer_options: WriterOptions {
            compression: ctx.storage.compression.clone(),
            row_group_size,
            ..WriterOptions::default()
        },
        max_rows_per_file,
        row_group_size,
        builder: Some(CleanColdRecordBatchBuilder::new(&parquet_columns)?),
        file: None,
        files: Vec::new(),
        total_rows: 0,
    };

    pgrx::Spi::connect(|spi| -> Result<(), String> {
        let mut cursor = spi.open_cursor(&query, &query_args);
        loop {
            let tuples = cursor
                .fetch(FETCH_ROWS)
                .map_err(|error| error.to_string())?;
            if tuples.is_empty() {
                break;
            }
            for tuple in tuples {
                let mut values = Vec::with_capacity(columns.len());
                for (index, column) in columns.iter().enumerate() {
                    values.push(read_column(&tuple, column, index + 1).map_err(|e| e.to_string())?);
                }
                let builder = exporter
                    .builder
                    .as_mut()
                    .expect("builder is always present");
                builder.push_typed_row(
                    &values,
                    &exporter.primary_key,
                    0,
                    1,
                    exporter.schema_version,
                )?;
                let buffered = builder.row_count();
                if buffered >= exporter.batch_limit() {
                    exporter.flush_batch()?;
                }
            }
        }
        exporter.flush_batch()?;
        exporter.close_file()
    })?;

    let column_info: Vec<Value> = columns
        .iter()
        .map(|column| {
            json!({
                "name": column.name,
                "type": column.catalog_type_name,
                "nullable": column.nullable,
                "primary_key": column.is_primary_key,
            })
        })
        .collect();
    let manifest = json!({
        "format": FORMAT,
        "table": format!("{}.{}", ctx.relation.namespace, ctx.relation.name),
        "exported_at": pgrx::Spi::get_one::<String>("SELECT clock_timestamp()::text")
            .map_err(|error| error.to_string())?,
        "row_count": exporter.total_rows,
        "primary_key": exporter.primary_key,
        "columns": column_info,
        "ordered_by": "primary_key",
        "scope": scope,
        "compression": ctx.storage.compression,
        "files": exporter.files,
    });
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| error.to_string())?;
    client
        .put(
            &manifest_key,
            &manifest_bytes,
            PutPrecondition::CreateIfAbsent,
        )
        .map_err(|error| format!("writing `{manifest_key}` failed: {error}"))?;
    Ok(json!({
        "storage": storage_name,
        "prefix": prefix,
        "rows": exporter.total_rows,
        "files": exporter.files.len(),
        "manifest": manifest_key,
        "scope": scope,
    }))
}

/// Writes a consistent merged snapshot of a managed table to a registered storage.
///
/// SQL contract:
/// `koldstore.export_table(table_name regclass, storage text, prefix text default null,
/// max_rows_per_file bigint default null) → jsonb`.
///
/// Superuser only, because it writes through a registered storage's credentials. Refuses
/// user-scoped tables unless `koldstore.user_id` names the scope to export.
#[pgrx::pg_extern(name = "export_table", schema = "koldstore", security_definer)]
pub fn export_table_pg(
    table_name: pgrx::PgRelation,
    storage: &str,
    prefix: pgrx::default!(Option<&str>, "NULL"),
    max_rows_per_file: pgrx::default!(Option<i64>, "NULL"),
) -> pgrx::JsonB {
    crate::security::require_superuser("export a managed table");
    export_table_value(table_name.oid(), storage, prefix, max_rows_per_file)
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("export_table failed: {error}"))
}
