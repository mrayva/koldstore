//! Operational PostgreSQL SQL entrypoints.

#[cfg(feature = "pg")]
use koldstore_common::QualifiedTableName;

/// Operator status for one managed table, including database async-mirror health.
///
/// SQL contract: `koldstore.table_status(table_name regclass) → jsonb`.
///
/// Returns table storage / hot / mirror / cold / jobs fields plus `async_mirror`
/// (WAL tip vs applied LSN, slot, retention). This is the single public operator
/// status entrypoint.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "table_status", schema = "koldstore", security_definer)]
pub fn table_status_pg(table_name: pgrx::PgRelation) -> pgrx::JsonB {
    table_status_pg_impl(table_name.oid())
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("table status failed: {error}"))
}

#[cfg(feature = "pg")]
fn table_status_pg_impl(table_oid: pgrx::pg_sys::Oid) -> Result<serde_json::Value, String> {
    let mut value = if crate::sql::migrate::partition::is_partitioned_parent(table_oid) {
        crate::sql::migrate::partition::status_parent(table_oid)?
    } else {
        table_status_fields(table_oid)?
    };
    let async_mirror = crate::mirror::status::async_mirror_status_value()
        .unwrap_or_else(|error| serde_json::json!({ "error": error, "healthy": false }));
    if let Some(obj) = value.as_object_mut() {
        obj.insert("async_mirror".to_string(), async_mirror);
    }
    Ok(value)
}

/// Table-only status fields (no async mirror). Used by [`table_status_pg_impl`].
#[cfg(feature = "pg")]
pub(crate) fn table_status_fields(table_oid: pgrx::pg_sys::Oid) -> Result<serde_json::Value, String> {
    use pgrx::datum::DatumWithOid;

    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let table = QualifiedTableName::parse(&relation).map_err(|error| error.to_string())?;
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "managed schema has no change-log mirror".to_string())?;
    let mirror = QualifiedTableName::from_table_name(&snapshot.mirror_relation);
    let plan = koldstore_flush::ops::table_status_plan(&table, &mirror)
        .map_err(|error| error.to_string())?;
    let json = crate::merge_scan::pg::with_custom_scan_disabled(|| {
        crate::spi::select_one::<String>(&plan.statement, &[DatumWithOid::from(table_oid)])
    })
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "table status lookup returned no rows".to_string())?;
    serde_json::from_str(&json).map_err(|error| error.to_string())
}

/// Prometheus text exposition of per-table and async-mirror gauges.
///
/// SQL contract: `koldstore.metrics() → text`. Only tables the session user can `SELECT` appear.
/// Row counts come from the manifest counters (no heap scan), so scraping stays cheap.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "metrics", schema = "koldstore", security_definer)]
pub fn metrics_pg() -> String {
    metrics_text().unwrap_or_else(|error| pgrx::error!("metrics failed: {error}"))
}

#[cfg(feature = "pg")]
fn metrics_text() -> Result<String, String> {
    use std::fmt::Write as _;

    // (table, [value per TABLE_METRICS entry]); a NULL value is omitted from the output.
    type Row = (String, [Option<f64>; 7]);
    let rows: Vec<Row> = pgrx::Spi::connect(|client| {
        client
            .select(
                "SELECT s.table_oid::regclass::text,
                        m.hot_row_count::float8, m.mirror_row_count::float8, m.cold_row_count::float8,
                        m.segment_count::float8,
                        (SELECT count(*) FROM koldstore.jobs j
                          WHERE j.table_oid = s.table_oid AND j.status IN ('pending', 'running'))::float8,
                        (SELECT count(*) FROM koldstore.jobs j
                          WHERE j.table_oid = s.table_oid AND j.status = 'error')::float8,
                        (SELECT extract(epoch FROM max(j.finished_at)) FROM koldstore.jobs j
                          WHERE j.table_oid = s.table_oid AND j.job_type = 'flush'
                            AND j.status = 'completed')::float8
                 FROM koldstore.schemas s
                 LEFT JOIN koldstore.manifest m ON m.table_oid = s.table_oid AND m.scope_key = ''
                 WHERE s.active AND has_table_privilege(session_user, s.table_oid, 'SELECT')
                 ORDER BY 1",
                None,
                &[],
            )?
            .map(|row| {
                let mut values = [None; 7];
                for (slot, value) in values.iter_mut().enumerate() {
                    *value = row.get::<f64>(slot + 2)?;
                }
                Ok((row.get::<String>(1)?.unwrap_or_default(), values))
            })
            .collect::<Result<Vec<Row>, pgrx::spi::Error>>()
    })
    .map_err(|error| error.to_string())?;

    const TABLE_METRICS: [(&str, &str); 7] = [
        ("koldstore_hot_rows", "Rows in the managed heap (manifest counter)."),
        ("koldstore_mirror_rows", "Rows in the change-log mirror (manifest counter)."),
        ("koldstore_cold_rows", "Rows in active cold segments."),
        ("koldstore_cold_segments", "Active cold segments."),
        ("koldstore_pending_jobs", "Jobs pending or running."),
        ("koldstore_error_jobs", "Retained jobs in error state."),
        ("koldstore_last_flush_timestamp_seconds", "Unix time the last flush job completed."),
    ];
    let mut out = String::new();
    for (index, (name, help)) in TABLE_METRICS.iter().enumerate() {
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} gauge");
        for (table, values) in &rows {
            if let Some(value) = values[index] {
                let label = table.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n");
                let _ = writeln!(out, "{name}{{table=\"{label}\"}} {value}");
            }
        }
    }
    let mirror = crate::mirror::status::async_mirror_status_value().unwrap_or_else(|_| serde_json::json!({ "healthy": false }));
    let gauges = [
        ("koldstore_async_mirror_healthy", "1 when the async mirror reports healthy.", f64::from(u8::from(mirror["healthy"].as_bool().unwrap_or(false)))),
        ("koldstore_async_mirror_lag_bytes", "WAL bytes between the current and applied LSN.", mirror["wal"]["lag_bytes"].as_f64().unwrap_or(0.0)),
        ("koldstore_async_mirror_retained_bytes", "WAL bytes retained by the mirror slot.", mirror["retention"]["retained_bytes"].as_f64().unwrap_or(0.0)),
    ];
    for (name, help, value) in gauges {
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}");
    }
    Ok(out)
}
