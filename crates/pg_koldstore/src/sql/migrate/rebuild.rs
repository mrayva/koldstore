//! `koldstore.rebuild_table`: unmanage, apply caller DDL, manage again -- in one transaction.
use koldstore_migrate::rehydrate::DemigrateOptions;
use pgrx::datum::DatumWithOid;

use super::partition::ManageArgs;

/// Options of the active schema version, in the shape `manage_table` takes them.
fn stored_manage_args(table_oid: pgrx::pg_sys::Oid) -> Result<ManageArgs, String> {
    // `client.select` keeps this read from assigning an XID (see `partition::is_partitioned_parent`).
    let stored = pgrx::Spi::connect(|client| -> Result<Option<String>, pgrx::spi::Error> {
        let rows = client.select(
            "SELECT jsonb_build_object('storage', st.name, 'table_type', s.table_type, \
               'scope_column', s.scope_column, 'options', s.options, \
               'order_column', (SELECT c->>'name' FROM jsonb_array_elements(s.columns) c \
                                 WHERE (c->>'column_id')::int = (s.options->>'segment_order_column_id')::int))::text \
             FROM koldstore.schemas s JOIN koldstore.storage st ON st.id = s.storage_id \
             WHERE s.table_oid = $1 AND s.active",
            Some(1),
            &[DatumWithOid::from(table_oid)],
        )?;
        match rows.into_iter().next() {
            Some(row) => row.get::<String>(1),
            None => Ok(None),
        }
    })
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "table is not managed by KoldStore".to_string())?;
    let stored: serde_json::Value = serde_json::from_str(&stored).map_err(|error| error.to_string())?;
    let options = &stored["options"];
    let (hot_row_limit, min_flush_rows, max_rows_per_file) = match options["flush_policy"]["type"].as_str() {
        None => (None, 1000, 1000),
        Some("row_limit") => {
            let policy = &options["flush_policy"];
            (policy["hot_row_limit"].as_i64(), policy["min_flush_rows"].as_i64().unwrap_or(1000), policy["max_rows_per_file"].as_i64().unwrap_or(1000))
        }
        Some(other) => return Err(format!("tables with a `{other}` flush policy cannot be rebuilt")),
    };
    let text_list = |key: &str| {
        options[key].as_array().map(|items| items.iter().filter_map(|item| item.as_str().map(ToString::to_string)).collect::<Vec<_>>())
    };
    Ok(ManageArgs {
        storage: stored["storage"].as_str().unwrap_or_default().to_string(),
        table_type: stored["table_type"].as_str().unwrap_or("shared").to_string(),
        scope_column: stored["scope_column"].as_str().map(ToString::to_string),
        migration_order_by: options["migration_order_by"].as_str().map(ToString::to_string),
        compression: options["compression"].as_str().map(ToString::to_string),
        target_file_size_mb: options["target_file_size_mb"].as_i64(),
        hot_row_limit,
        min_flush_rows,
        max_rows_per_file,
        auto_flush: options["auto_flush"].as_bool().unwrap_or(true),
        segment_order_column: stored["order_column"].as_str().map(ToString::to_string),
        pruning_columns: text_list("pruning_columns"),
        bloom_filter_columns: text_list("bloom_filter_columns"),
        parquet_row_group_size: options["parquet_row_group_size"].as_i64(),
        parquet_data_page_row_count_limit: options["parquet_data_page_row_count_limit"].as_i64(),
        parquet_bloom_filter_fpp: options["parquet_bloom_filter_fpp"].as_f64(),
        allow_fk_hot_only: options["allow_fk_hot_only"].as_bool().unwrap_or(false),
    })
}

pub(super) fn rebuild_table_pg_impl(table_oid: pgrx::pg_sys::Oid, ddl: &str) -> Result<pgrx::Uuid, String> {
    if super::partition::is_partitioned_parent(table_oid) {
        return Err("partitioned tables cannot be rebuilt; rebuild a partition instead".to_string());
    }
    // Slot provisioning must precede the first SPI call that assigns an XID.
    crate::mirror::lifecycle::prepare_capture()?;
    let args = stored_manage_args(table_oid)?;
    // The rehydrate rewrites every row. Take the table out of the capture publication first so that
    // internal write is never decoded: under the old key shape it would reach the async applier as
    // changes the re-managed table (possibly with a different primary key) cannot place.
    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let quoted = koldstore_common::QualifiedTableName::parse(&relation).map_err(|error| error.to_string())?.quoted();
    let published = pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_publication_rel pr JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid \
         WHERE p.pubname = $1 AND pr.prrelid = $2)",
        &[DatumWithOid::from(crate::mirror::lifecycle::PUBLICATION_NAME), DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    if published {
        pgrx::Spi::run(&format!(
            "ALTER PUBLICATION {} DROP TABLE {quoted}",
            crate::mirror::lifecycle::PUBLICATION_NAME
        ))
        .map_err(|error| error.to_string())?;
    }
    super::unmanage::unmanage_table_pg_impl(table_oid, DemigrateOptions { rehydrate: true, drop_cold: true })?;
    // The caller's DDL runs with the caller's privileges, not the function owner's.
    crate::catalog::owner::with_calling_role(|| pgrx::Spi::run(ddl).map_err(|error| error.to_string()))??;
    unsafe { pgrx::pg_sys::CommandCounterIncrement() };
    // Re-managing reads the freshly rehydrated heap. This transaction wrote it, but the table has
    // no cold data any more, so the same-transaction cold-read guard has nothing to protect.
    Ok(crate::txn_writes::with_check_suppressed(|| args.manage_leaf(table_oid)))
}
