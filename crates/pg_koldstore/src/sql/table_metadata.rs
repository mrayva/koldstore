//! Cold-tier metadata export/import for logical dumps.
//!
//! `pg_dump` sees only the hot heap (or, by default, the merged hot+cold rows -- see
//! `koldstore.dump_cold_as_metadata`). The catalog that describes the cold Parquet segments is keyed
//! by table OID and does not survive a dump/restore. These two functions carry it across:
//!
//! - [`export_table_metadata_pg`] captures a managed table's schema versions, options, manifest and
//!   segment/index rows as one `jsonb` document, without OIDs or storage credentials.
//! - [`import_table_metadata_pg`] re-manages a restored, unmanaged table (same columns) and adopts
//!   the exported segments under its new OID. The objects themselves are never copied; the named
//!   storage must still serve them.
//!
//! Export refuses while the hot heap alone would not reproduce the table: unflushed delete markers
//! shadowing cold rows, in-flight segments, or a manifest that is not in sync.

use pgrx::datum::DatumWithOid;
use serde_json::{Value, json};

use super::backup::{authorize, json_query};

const FORMAT: i64 = 1;

fn count(sql: &str, args: &[DatumWithOid<'_>]) -> Result<i64, String> {
    Ok(pgrx::Spi::get_one_with_args::<i64>(sql, args)
        .map_err(|error| error.to_string())?
        .unwrap_or(0))
}

fn export_value(table_oid: pgrx::pg_sys::Oid) -> Result<Value, String> {
    super::backup::managed_table_oids(Some(table_oid))?;
    crate::mirror::apply::fence_for_read()?;
    let oid = || DatumWithOid::from(table_oid);

    let pending = count(
        "SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = $1 AND status = 'pending'",
        &[oid()],
    )?;
    if pending > 0 {
        return Err(format!(
            "{pending} cold segment(s) are still pending; wait for the running flush, then retry"
        ));
    }
    let mirror = pgrx::Spi::get_one_with_args::<String>(
        "SELECT mirror_relation::text FROM koldstore.schemas WHERE table_oid = $1 AND active",
        &[oid()],
    )
    .map_err(|error| error.to_string())?
    .ok_or("table has no change-log mirror")?;
    let tombstones = count(&format!("SELECT count(*) FROM {mirror} WHERE op = 3"), &[])?;
    if tombstones > 0 {
        return Err(format!(
            "{tombstones} delete marker(s) have not been flushed yet; a hot-only dump would resurrect \
             the deleted cold rows. Run koldstore.flush_table() and retry"
        ));
    }

    let unsynced = count(
        "SELECT count(*) FROM koldstore.manifest WHERE table_oid = $1 AND sync_state <> 'in_sync'",
        &[oid()],
    )?;
    if unsynced > 0 {
        return Err("the cold manifest is not in sync; run koldstore.flush_table() and retry".into());
    }
    let target = super::backup::table_target(table_oid)?;
    let schemas = json_query(
        "SELECT COALESCE(jsonb_agg(to_jsonb(s) - 'id' - 'table_oid' - 'mirror_relation' - 'storage_id'
                                   - 'activation_lsn' ORDER BY s.version), '[]'::jsonb)::text
         FROM koldstore.schemas s WHERE s.table_oid = $1",
        &[oid()],
    )?;
    let manifests = json_query(
        "SELECT COALESCE(jsonb_agg(to_jsonb(m) - 'table_oid' ORDER BY m.scope_key), '[]'::jsonb)::text
         FROM koldstore.manifest m WHERE m.table_oid = $1",
        &[oid()],
    )?;
    let segments = json_query(
        "SELECT COALESCE(jsonb_agg(to_jsonb(c) - 'table_oid' ORDER BY c.scope_key, c.min_seq, c.segment_id),
                         '[]'::jsonb)::text
         FROM koldstore.cold_segments c WHERE c.table_oid = $1 AND c.status IN ('active', 'history')",
        &[oid()],
    )?;
    let index = json_query(
        "SELECT COALESCE(jsonb_agg(to_jsonb(i) - 'table_oid' ORDER BY i.segment_id, i.column_id), '[]'::jsonb)::text
         FROM koldstore.cold_segment_index i JOIN koldstore.cold_segments c USING (segment_id)
         WHERE c.table_oid = $1 AND c.status IN ('active', 'history')",
        &[oid()],
    )?;
    let order_index = json_query(
        "SELECT COALESCE(jsonb_agg(to_jsonb(i) - 'table_oid' ORDER BY i.segment_id, i.sort_order_id), '[]'::jsonb)::text
         FROM koldstore.cold_segment_order_index i JOIN koldstore.cold_segments c USING (segment_id)
         WHERE c.table_oid = $1 AND c.status IN ('active', 'history')",
        &[oid()],
    )?;
    let storage = json_query(
        "SELECT jsonb_build_object('name', st.name, 'storage_type', st.storage_type, 'base_path', st.base_path,
                                   'config', st.config, 'regular_path_tmpl', st.regular_path_tmpl,
                                   'scoped_path_tmpl', st.scoped_path_tmpl)::text
         FROM koldstore.schemas s JOIN koldstore.storage st ON st.id = s.storage_id
         WHERE s.table_oid = $1 AND s.active",
        &[oid()],
    )?;
    Ok(json!({
        "format": FORMAT,
        "table": format!("{}.{}", target.namespace, target.name),
        "prefix": target.prefix,
        "storage": storage,
        "schemas": schemas,
        "manifests": manifests,
        "segments": segments,
        "segment_index": index,
        "segment_order_index": order_index,
    }))
}

/// Captures a managed table's cold-tier catalog as `jsonb` for a logical dump.
///
/// SQL contract: `koldstore.export_table_metadata(table_name regclass) → jsonb`.
///
/// Pair it with a hot-only dump (`SET koldstore.dump_cold_as_metadata = on` for `pg_dump`/`COPY`),
/// and replay it with `koldstore.import_table_metadata()` after restoring. Contains no OIDs and no
/// storage credentials. Refuses while delete markers, pending segments or an out-of-sync manifest
/// would make the hot rows alone an incomplete picture.
#[pgrx::pg_extern(name = "export_table_metadata", schema = "koldstore", security_definer)]
pub fn export_table_metadata_pg(table_name: pgrx::PgRelation) -> pgrx::JsonB {
    let oid = table_name.oid();
    authorize(Some(oid), "export table metadata");
    export_value(oid)
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("export table metadata failed: {error}"))
}

fn names_and_ids(columns: &Value) -> Vec<(String, i64)> {
    let mut out: Vec<(String, i64)> = columns
        .as_array()
        .map(|array| {
            array
                .iter()
                .filter_map(|column| Some((column["name"].as_str()?.to_string(), column["column_id"].as_i64()?)))
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

fn import_value(table_oid: pgrx::pg_sys::Oid, metadata: &Value) -> Result<i64, String> {
    use koldstore_common::{FlushPolicy, ManageTableOptions};

    if metadata["format"].as_i64() != Some(FORMAT) {
        return Err("unsupported metadata format".into());
    }
    if super::backup::managed_table_oids(Some(table_oid)).is_ok() {
        return Err("table is already managed; import into a freshly restored, unmanaged table".into());
    }
    let schemas = metadata["schemas"].as_array().ok_or("metadata has no schemas")?;
    let active = schemas
        .iter()
        .find(|schema| schema["active"].as_bool() == Some(true))
        .ok_or("metadata has no active schema version")?;
    let storage_name = metadata["storage"]["name"].as_str().ok_or("metadata names no storage")?;
    let storage = pgrx::Spi::get_one_with_args::<String>(
        "SELECT storage_type || '|' || base_path FROM koldstore.storage WHERE name = $1",
        &[DatumWithOid::from(storage_name)],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| format!("storage \"{storage_name}\" is not registered; register_storage() it first"))?;
    let expected = format!(
        "{}|{}",
        metadata["storage"]["storage_type"].as_str().unwrap_or_default(),
        metadata["storage"]["base_path"].as_str().unwrap_or_default()
    );
    if storage != expected {
        pgrx::notice!("storage \"{storage_name}\" is {storage}, the export was taken against {expected}");
    }

    // The restored table must have the columns the segments were written with.
    let current = json_query(
        "SELECT COALESCE(jsonb_agg(jsonb_build_object('name', attname, 'column_id', attnum)), '[]'::jsonb)::text
         FROM pg_attribute WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped",
        &[DatumWithOid::from(table_oid)],
    )?;
    let current = names_and_ids(&current);
    let exported = names_and_ids(&active["columns"]);
    if let Some(missing) = exported.iter().find(|column| !current.contains(column)) {
        return Err(format!(
            "restored table does not have column \"{}\" at position {}; recreate it with the original column order",
            missing.0, missing.1
        ));
    }

    let options = ManageTableOptions::from_value(&active["options"]);
    let (hot_row_limit, min_flush_rows, max_rows_per_file) = match options.flush_policy {
        Some(FlushPolicy::RowLimit { hot_row_limit, min_flush_rows, max_rows_per_file, .. }) => {
            (i64::try_from(hot_row_limit).ok(), min_flush_rows as i64, max_rows_per_file as i64)
        }
        Some(FlushPolicy::OlderThan { min_flush_rows, max_rows_per_file, .. })
        | Some(FlushPolicy::Filter { min_flush_rows, max_rows_per_file, .. }) => {
            (None, min_flush_rows as i64, max_rows_per_file as i64)
        }
        _ => (None, 1000, 1000),
    };
    let scope_column = active["scope_column"].as_str();
    super::migrate::manage_table_pg_impl(
        table_oid,
        active["table_type"].as_str().unwrap_or("shared"),
        storage_name,
        scope_column,
        None,
        options.compression.as_ref().map(|c| c.as_str()),
        options.target_file_size_mb.map(|v| v as i64),
        hot_row_limit,
        min_flush_rows,
        max_rows_per_file,
        options.auto_flush.unwrap_or(true),
        None,
        options.pruning_columns.clone(),
        options.bloom_filter_columns.clone(),
        options.parquet_row_group_size.map(|v| v as i64),
        options.parquet_data_page_row_count_limit.map(|v| v as i64),
        options.parquet_bloom_filter_fpp.map(|v| v.get()),
        options.allow_fk_hot_only.unwrap_or(false),
    );

    let oid = || DatumWithOid::from(table_oid);
    let json = |key: &str| DatumWithOid::from(pgrx::JsonB(metadata[key].clone()));

    // Prior schema versions first (inactive), then bring the active row to the exported version.
    pgrx::Spi::run_with_args(
        "INSERT INTO koldstore.schemas
           (id, table_oid, version, active, table_type, columns, primary_key, scope_column, mirror_relation,
            primary_key_shape, initialization_state, indexed_columns, type_matrix, options, storage_id,
            last_flush_seq, last_flush_at, created_at, updated_at)
         SELECT gen_random_uuid(), $1, (e->>'version')::int, false, e->>'table_type', e->'columns', e->'primary_key',
                a.scope_column, a.mirror_relation, e->'primary_key_shape', a.initialization_state,
                e->'indexed_columns', e->'type_matrix', e->'options', a.storage_id,
                (e->>'last_flush_seq')::bigint, (e->>'last_flush_at')::timestamptz,
                (e->>'created_at')::timestamptz, (e->>'updated_at')::timestamptz
         FROM jsonb_array_elements($2) e, koldstore.schemas a
         WHERE a.table_oid = $1 AND a.active AND NOT (e->>'active')::boolean",
        &[oid(), json("schemas")],
    )
    .map_err(|error| error.to_string())?;
    pgrx::Spi::run_with_args(
        "UPDATE koldstore.schemas SET version = (SELECT max((e->>'version')::int) FROM jsonb_array_elements($2) e),
                options = $3 || jsonb_build_object('migration_status', options->'migration_status'),
                last_flush_seq = $4, updated_at = now()
         WHERE table_oid = $1 AND active",
        &[
            oid(),
            json("schemas"),
            DatumWithOid::from(pgrx::JsonB(active["options"].clone())),
            DatumWithOid::from(active["last_flush_seq"].as_i64().unwrap_or(0)),
        ],
    )
    .map_err(|error| error.to_string())?;

    pgrx::Spi::run_with_args(
        "INSERT INTO koldstore.cold_segments
         SELECT (jsonb_populate_record(NULL::koldstore.cold_segments, e || jsonb_build_object('table_oid', $1))).*
         FROM jsonb_array_elements($2) e",
        &[oid(), json("segments")],
    )
    .map_err(|error| error.to_string())?;
    pgrx::Spi::run_with_args(
        "INSERT INTO koldstore.cold_segment_index
         SELECT (jsonb_populate_record(NULL::koldstore.cold_segment_index, e || jsonb_build_object('table_oid', $1))).*
         FROM jsonb_array_elements($2) e",
        &[oid(), json("segment_index")],
    )
    .map_err(|error| error.to_string())?;
    pgrx::Spi::run_with_args(
        "INSERT INTO koldstore.cold_segment_order_index
         SELECT (jsonb_populate_record(NULL::koldstore.cold_segment_order_index,
                                       e || jsonb_build_object('table_oid', $1))).*
         FROM jsonb_array_elements($2) e",
        &[oid(), json("segment_order_index")],
    )
    .map_err(|error| error.to_string())?;
    pgrx::Spi::run_with_args(
        "INSERT INTO koldstore.manifest
         SELECT (jsonb_populate_record(NULL::koldstore.manifest,
                   e || jsonb_build_object('table_oid', $1, 'hot_row_count', 0, 'mirror_row_count', 0))).*
         FROM jsonb_array_elements($2) e
         ON CONFLICT (table_oid, scope_key) DO UPDATE SET
           etag = EXCLUDED.etag, generation = EXCLUDED.generation, sync_state = EXCLUDED.sync_state,
           segment_count = EXCLUDED.segment_count, max_seq = EXCLUDED.max_seq,
           cold_row_count = EXCLUDED.cold_row_count, history_floor_seq = EXCLUDED.history_floor_seq,
           last_error = NULL, updated_at = now()",
        &[oid(), json("manifests")],
    )
    .map_err(|error| error.to_string())?;
    // The exported prefix may differ from the restored table's name-derived one (the source was renamed
    // or moved, or the restore uses another name): pin it so the adopted segments stay reachable.
    if let Some(exported) = metadata["prefix"].as_str() {
        let relation = crate::catalog::resolve::relation_context(table_oid)?;
        let storage = crate::catalog::resolve::active_flush_storage_context(table_oid)?;
        let derived = crate::catalog::resolve::table_object_prefix(&storage, &relation.namespace, &relation.name)?;
        if derived != exported {
            pgrx::Spi::run_with_args(
                "INSERT INTO koldstore.table_prefix_pins (table_oid, prefix) VALUES ($1, $2)
                 ON CONFLICT (table_oid) DO UPDATE SET prefix = EXCLUDED.prefix",
                &[oid(), DatumWithOid::from(exported)],
            )
            .map_err(|error| error.to_string())?;
        }
    }
    crate::catalog::cache::invalidate_table_globally(table_oid);

    count(
        "SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = $1 AND status = 'active'",
        &[oid()],
    )
}

/// Re-binds exported cold-tier metadata to a restored, unmanaged table.
///
/// SQL contract: `koldstore.import_table_metadata(table_name regclass, metadata jsonb) → bigint`
/// (the number of active cold segments adopted).
///
/// The table must have been restored with its hot rows and original column order. It is managed
/// with the exported options, then the exported schema versions, manifest and segments are adopted
/// under its new OID. Run `koldstore.validate_cold_storage(table_name, deep => true)` afterwards.
#[pgrx::pg_extern(name = "import_table_metadata", schema = "koldstore", security_definer)]
pub fn import_table_metadata_pg(table_name: pgrx::PgRelation, metadata: pgrx::JsonB) -> i64 {
    let oid = table_name.oid();
    // Provision the async mirror slot before any SPI call: SPI assigns an XID, after which a
    // missing slot can no longer be created (the first managed table of a database).
    crate::mirror::lifecycle::prepare_capture()
        .unwrap_or_else(|error| pgrx::error!("import table metadata failed: {error}"));
    authorize(Some(oid), "import table metadata");
    import_value(oid, &metadata.0)
        .unwrap_or_else(|error| pgrx::error!("import table metadata failed: {error}"))
}
