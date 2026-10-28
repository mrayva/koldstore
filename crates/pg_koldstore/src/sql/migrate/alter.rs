//! `koldstore.alter_table`: change a managed table's flush settings in place.

use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::Oid;
use serde_json::Value;

/// New flush settings; `None` keeps the current value.
#[derive(Clone, Copy)]
pub(super) struct FlushChanges {
    pub hot_row_limit: Option<i64>,
    pub min_flush_rows: Option<i64>,
    pub max_rows_per_file: Option<i64>,
    pub auto_flush: Option<bool>,
}

/// Applies `changes` to a persisted options object. Only the touched keys change, so options this
/// build does not model survive. Returns the new object.
pub(super) fn apply_changes(options: &Value, changes: FlushChanges) -> Result<Value, String> {
    let mut options = if options.is_null() { Value::Object(Default::default()) } else { options.clone() };
    let object = options.as_object_mut().ok_or("table options are not a JSON object")?;
    if changes.hot_row_limit.is_some() || changes.min_flush_rows.is_some() || changes.max_rows_per_file.is_some() {
        if !object.contains_key("flush_policy") {
            // A table managed without a flush policy (manual flush only) can opt in by naming a
            // hot_row_limit; the other two limits take manage_table's defaults.
            if changes.hot_row_limit.is_none() {
                return Err("table has no flush policy; pass hot_row_limit to enable one".to_string());
            }
            let fresh = koldstore_common::FlushPolicy::new(1, 1000, 1000);
            object.insert("flush_policy".to_string(), serde_json::to_value(fresh).map_err(|e| e.to_string())?);
        }
        let policy = object
            .get_mut("flush_policy")
            .and_then(Value::as_object_mut)
            .ok_or("table options hold a malformed flush policy")?;
        if policy.get("type").and_then(Value::as_str) != Some("row_limit") {
            return Err("only row_limit flush policies can be altered".to_string());
        }
        for (key, value) in [
            ("hot_row_limit", changes.hot_row_limit),
            ("min_flush_rows", changes.min_flush_rows),
            ("max_rows_per_file", changes.max_rows_per_file),
        ] {
            if let Some(value) = value {
                policy.insert(key.to_string(), Value::from(value));
            }
        }
        let read = |key: &str| policy.get(key).and_then(Value::as_i64).unwrap_or(0);
        let floor = u64::try_from(crate::guc::min_max_rows_per_file())
            .unwrap_or(koldstore_common::DEFAULT_MIN_MAX_ROWS_PER_FILE);
        koldstore_migrate::manage_table::validate_manage_table_preflight(
            koldstore_migrate::manage_table::ManageTablePolicyInput {
                hot_row_limit: Some(read("hot_row_limit")),
                min_flush_rows: read("min_flush_rows"),
                max_rows_per_file: read("max_rows_per_file"),
                target_file_size_mb: None,
                min_max_rows_per_file: floor,
                auto_flush: true,
                parquet_row_group_size: None,
                parquet_data_page_row_count_limit: None,
                parquet_bloom_filter_fpp: None,
            },
            None,
        )
        .map_err(|error| error.to_string())?;
    }
    match changes.auto_flush {
        Some(true) => {
            object.remove("auto_flush");
        }
        Some(false) => {
            object.insert("auto_flush".to_string(), Value::Bool(false));
        }
        None => {}
    }
    Ok(options)
}

fn read_options(sql: &str, oid: Oid) -> Result<Option<Value>, String> {
    pgrx::Spi::get_one_with_args::<pgrx::JsonB>(sql, &[DatumWithOid::from(oid)])
        .map(|value| value.map(|json| json.0))
        .map_err(|error| error.to_string())
}

/// Alters one active managed table and returns its new options.
pub(super) fn alter_table_impl(table_oid: Oid, changes: FlushChanges) -> Result<Value, String> {
    let _table_lock = crate::sql::job_lock::TableJobLockGuard::lock(table_oid)?;
    let current = read_options(
        "SELECT COALESCE(options, '{}'::jsonb) FROM koldstore.schemas WHERE table_oid = $1 AND active",
        table_oid,
    )?
    .ok_or("table is not an active managed table")?;
    let updated = apply_changes(&current, changes)?;
    pgrx::Spi::run_with_args(
        "UPDATE koldstore.schemas SET options = $2, updated_at = now() WHERE table_oid = $1 AND active",
        &[DatumWithOid::from(table_oid), DatumWithOid::from(pgrx::JsonB(updated.clone()))],
    )
    .map_err(|error| error.to_string())?;
    crate::catalog::cache::invalidate_table_globally(table_oid);
    crate::worker::wake::mark_schedule_pending();
    Ok(updated)
}

/// Applies `changes` to the partition template of `parent`, so later partitions follow them.
pub(super) fn alter_template(parent: Oid, changes: FlushChanges) -> Result<(), String> {
    let Some(current) = read_options("SELECT options FROM koldstore.partition_templates WHERE parent_oid = $1", parent)?
    else {
        return Ok(());
    };
    let updated = apply_changes(&current, changes)?;
    pgrx::Spi::run_with_args(
        "UPDATE koldstore.partition_templates SET options = $2 WHERE parent_oid = $1",
        &[DatumWithOid::from(parent), DatumWithOid::from(pgrx::JsonB(updated))],
    )
    .map_err(|error| error.to_string())
}
