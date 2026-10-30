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
//! - [`sync_table_metadata_pg`] re-applies a newer export to a table that already adopted one, so a
//!   follower sharing the leader's cold storage tracks the leader's segments incrementally.
//!
//! Export refuses while the hot heap alone would not reproduce the table: unflushed delete markers
//! shadowing cold rows, in-flight segments, or a manifest in a syncing, stale or error state (hot DML
//! since the last flush, `pending_write`, leaves the cold manifest valid and does not block it).

use pgrx::datum::DatumWithOid;
use serde_json::{Value, json};

use super::backup::{authorize, json_query};

const FORMAT: i64 = 1;

fn count(sql: &str, args: &[DatumWithOid<'_>]) -> Result<i64, String> {
    Ok(pgrx::Spi::get_one_with_args::<i64>(sql, args)
        .map_err(|error| error.to_string())?
        .unwrap_or(0))
}

/// `for_dump` additionally refuses unflushed delete markers (a hot-only dump would resurrect the
/// deleted cold rows); a leader catalog for followers has no such restriction.
fn export_value(table_oid: pgrx::pg_sys::Oid, for_dump: bool) -> Result<Value, String> {
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
    let tombstones = if for_dump { count(&format!("SELECT count(*) FROM {mirror} WHERE op = 3"), &[])? } else { 0 };
    if tombstones > 0 {
        return Err(format!(
            "{tombstones} delete marker(s) have not been flushed yet; a hot-only dump would resurrect \
             the deleted cold rows. Run koldstore.flush_table() and retry"
        ));
    }

    let unsynced = count(
        "SELECT count(*) FROM koldstore.manifest WHERE table_oid = $1 AND sync_state NOT IN ('in_sync', 'pending_write')",
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
    export_value(oid, true)
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
        "SELECT (SELECT storage_type || '|' || base_path FROM koldstore.storage WHERE name = $1)",
        &[DatumWithOid::from(storage_name)],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| format!("storage \"{storage_name}\" is not registered; register_storage() it first (a follower sharing the leader's directory needs \"check\" => false)"))?;
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
        active["options"]["migration_order_by"].as_str(),
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
                options = ($3 - 'leader' - 'follower' - 'lease_epoch' - 'seen_leader_epoch') || jsonb_build_object('migration_status', options->'migration_status'),
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

fn sync_value(table_oid: pgrx::pg_sys::Oid, metadata: &Value) -> Result<Value, String> {
    if metadata["format"].as_i64() != Some(FORMAT) {
        return Err("unsupported metadata format".into());
    }
    super::backup::managed_table_oids(Some(table_oid))
        .map_err(|_| "table is not managed; adopt the leader's metadata with import_table_metadata() first".to_string())?;
    let schemas = metadata["schemas"].as_array().ok_or("metadata has no schemas")?;
    let exported_version = schemas
        .iter()
        .find(|schema| schema["active"].as_bool() == Some(true))
        .and_then(|schema| schema["version"].as_i64())
        .ok_or("metadata has no active schema version")?;
    let local_version = pgrx::Spi::get_one_with_args::<i32>(
        "SELECT (SELECT version FROM koldstore.schemas WHERE table_oid = $1 AND active)",
        &[DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?
    .ok_or("table has no active schema version")?;
    if i64::from(local_version) != exported_version {
        return Err(format!(
            "schema version differs (local {local_version}, leader {exported_version}); apply the leader's DDL first"
        ));
    }

    let oid = || DatumWithOid::from(table_oid);
    let json = |key: &str| DatumWithOid::from(pgrx::JsonB(metadata[key].clone()));

    // Segments the leader no longer lists (compacted away) leave this node's catalog. The objects
    // belong to the leader and are never touched here; the index tables cascade.
    let removed = count(
        "WITH gone AS (
           DELETE FROM koldstore.cold_segments c
           WHERE c.table_oid = $1 AND c.status IN ('active', 'history')
             AND c.segment_id NOT IN (SELECT (e->>'segment_id')::uuid FROM jsonb_array_elements($2) e)
           RETURNING 1)
         SELECT count(*) FROM gone",
        &[oid(), json("segments")],
    )?;
    let added_ranges = pgrx::Spi::get_one_with_args::<pgrx::JsonB>(
        "WITH new AS (
           INSERT INTO koldstore.cold_segments
           SELECT (jsonb_populate_record(NULL::koldstore.cold_segments, e || jsonb_build_object('table_oid', $1))).*
           FROM jsonb_array_elements($2) e
           WHERE NOT EXISTS (SELECT 1 FROM koldstore.cold_segments c WHERE c.segment_id = (e->>'segment_id')::uuid)
           RETURNING min_seq, max_seq)
         SELECT coalesce(jsonb_agg(jsonb_build_array(min_seq, max_seq)), '[]'::jsonb) FROM new",
        &[oid(), json("segments")],
    )
    .map_err(|error| error.to_string())?
    .map(|value| value.0)
    .unwrap_or_else(|| json!([]));
    let added = added_ranges.as_array().map_or(0, Vec::len);
    // Index rows exist for exactly the segments that now have none.
    pgrx::Spi::run_with_args(
        "INSERT INTO koldstore.cold_segment_index
         SELECT (jsonb_populate_record(NULL::koldstore.cold_segment_index, e || jsonb_build_object('table_oid', $1))).*
         FROM jsonb_array_elements($2) e
         WHERE NOT EXISTS (SELECT 1 FROM koldstore.cold_segment_index i
                           WHERE i.segment_id = (e->>'segment_id')::uuid AND i.column_id = (e->>'column_id')::int)",
        &[oid(), json("segment_index")],
    )
    .map_err(|error| error.to_string())?;
    pgrx::Spi::run_with_args(
        "INSERT INTO koldstore.cold_segment_order_index
         SELECT (jsonb_populate_record(NULL::koldstore.cold_segment_order_index,
                                       e || jsonb_build_object('table_oid', $1))).*
         FROM jsonb_array_elements($2) e
         WHERE NOT EXISTS (SELECT 1 FROM koldstore.cold_segment_order_index i
                           WHERE i.segment_id = (e->>'segment_id')::uuid
                             AND i.sort_order_id = (e->>'sort_order_id')::int)",
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
    pgrx::Spi::run_with_args(
        "UPDATE koldstore.schemas SET last_flush_seq = $2, updated_at = now() WHERE table_oid = $1 AND active",
        &[
            oid(),
            DatumWithOid::from(
                schemas
                    .iter()
                    .find(|schema| schema["active"].as_bool() == Some(true))
                    .and_then(|schema| schema["last_flush_seq"].as_i64())
                    .unwrap_or(0),
            ),
        ],
    )
    .map_err(|error| error.to_string())?;
    crate::catalog::cache::invalidate_table_globally(table_oid);

    let segments = count(
        "SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = $1 AND status = 'active'",
        &[oid()],
    )?;
    let generation = count(
        "SELECT COALESCE(max(generation), 0) FROM koldstore.manifest WHERE table_oid = $1",
        &[oid()],
    )?;
    Ok(json!({
        "added": added, "removed": removed, "segments": segments, "generation": generation,
        "added_ranges": added_ranges,
    }))
}

/// Splits the `(min_seq, max_seq)` pairs of newly adopted segments out of a sync result.
fn take_added_ranges(result: &mut Value) -> Vec<(i64, i64)> {
    result
        .as_object_mut()
        .and_then(|map| map.remove("added_ranges"))
        .and_then(|ranges| {
            ranges.as_array().map(|pairs| {
                pairs
                    .iter()
                    .filter_map(|pair| Some((pair[0].as_i64()?, pair[1].as_i64()?)))
                    .collect()
            })
        })
        .unwrap_or_default()
}

/// Brings a table that already adopted a leader's cold tier up to date with a newer export.
///
/// SQL contract: `koldstore.sync_table_metadata(table_name regclass, metadata jsonb) → jsonb`
/// (`{added, removed, segments, generation}`).
///
/// For a follower node that shares the leader's cold storage: segments in the export that this node
/// lacks are adopted, segments the export no longer lists are dropped from this node's catalog
/// (never from storage), and the manifest takes the leader's generation. The schema version must
/// match. It is safe to call repeatedly; an unchanged export changes nothing.
#[pgrx::pg_extern(name = "sync_table_metadata", schema = "koldstore", security_definer)]
pub fn sync_table_metadata_pg(table_name: pgrx::PgRelation, metadata: pgrx::JsonB) -> pgrx::JsonB {
    let oid = table_name.oid();
    authorize(Some(oid), "sync table metadata");
    sync_value(oid, &metadata.0)
        .map(|mut result| {
            take_added_ranges(&mut result);
            pgrx::JsonB(result)
        })
        .unwrap_or_else(|error| pgrx::error!("sync table metadata failed: {error}"))
}

/// Where a table's leader catalog lives on its storage, outside every table prefix.
pub(crate) fn leader_catalog_key(namespace: &str, name: &str) -> String {
    format!("_leader_catalog/{namespace}/{name}.json")
}

pub(crate) fn max_generation(metadata: &Value) -> i64 {
    metadata["manifests"]
        .as_array()
        .map(|manifests| manifests.iter().filter_map(|m| m["generation"].as_i64()).max().unwrap_or(0))
        .unwrap_or(0)
}

fn publish_leader_catalog_value(table_oid: pgrx::pg_sys::Oid) -> Result<Value, String> {
    use koldstore_storage::StorageClient;

    super::leader_lease::require_lease(table_oid, "publish the leader catalog")?;
    let mut metadata = export_value(table_oid, false)?;
    let epoch = super::leader_lease::published_epoch(table_oid)?;
    if let Some(epoch) = epoch {
        metadata["lease_epoch"] = json!(epoch);
    }
    // Unflushed delete markers: followers mask these keys until the marker reaches a segment.
    // The first `koldstore.published_tombstone_limit` ride inline; the rest go to content-addressed
    // sidecar objects the catalog lists with their checksums.
    let target = super::backup::table_target(table_oid)?;
    let client = super::backup::storage_client(&target.storage.storage_id)?;
    let chunk_prefix = format!("_leader_tombstones/{}/{}/", target.namespace, target.name);
    let mut chunk_keys = Vec::new();
    let _ = super::flush::peer_prune::prune_reinserted(table_oid);
    if let Ok((mut keys, truncated)) = super::flush::peer_prune::leader_tombstones(table_oid) {
        let limit = crate::guc::published_tombstone_limit();
        let overflow = if keys.len() > limit { keys.split_off(limit) } else { Vec::new() };
        let mut chunks = Vec::new();
        for part in overflow.chunks(limit.max(1)) {
            let body = serde_json::to_vec(part).map_err(|error| error.to_string())?;
            let sha = koldstore_storage::content_checksum_sha256_hex(&body);
            let chunk_key = format!("{chunk_prefix}{sha}.json");
            client
                .put(&chunk_key, &body, koldstore_storage::PutPrecondition::Overwrite)
                .map_err(|error| format!("cannot write delete markers to {chunk_key}: {error}"))?;
            chunks.push(json!({ "key": chunk_key, "sha256": sha, "count": part.len() }));
            chunk_keys.push(chunk_key);
        }
        metadata["tombstones"] = Value::Array(keys);
        if !chunks.is_empty() {
            metadata["tombstone_chunks"] = Value::Array(chunks);
        }
        if truncated {
            metadata["tombstones_truncated"] = json!(true);
            pgrx::warning!(
                "koldstore: more unflushed delete markers than {} (koldstore.published_tombstone_limit x {}); \
                 followers see the rest only after the next flush",
                limit.saturating_mul(super::flush::peer_prune::TOMBSTONE_CHUNKS_MAX),
                super::flush::peer_prune::TOMBSTONE_CHUNKS_MAX
            );
        }
    }
    let key = leader_catalog_key(&target.namespace, &target.name);
    let generation = max_generation(&metadata);
    // Every publish gets the next publication number, across leaders (the successor reads the
    // catalog it follows), so a client can wait for a follower to reach the state it just wrote.
    let mut publication = 1_i64;
    // Fencing floor: a node whose catalog is behind what is already published is not the leader
    // any more (or has not caught up) and must not move the shared catalog backwards.
    if let Ok(existing) = client.get(&key) {
        if let Ok(existing) = serde_json::from_slice::<Value>(&existing) {
            publication = existing["publication"].as_i64().unwrap_or(0).saturating_add(1);
            if let (Some(mine), Some(theirs)) = (epoch, existing["lease_epoch"].as_i64()) {
                if theirs > mine {
                    return Err(format!(
                        "a catalog from a newer leader lease epoch is already published (epoch {theirs}, this node {mine})"
                    ));
                }
            }
            let published = max_generation(&existing);
            if published > generation {
                return Err(format!(
                    "a newer leader catalog is already published (generation {published}, this node {generation})"
                ));
            }
        }
    }
    metadata["publication"] = json!(publication);
    let bytes = serde_json::to_vec(&metadata).map_err(|error| error.to_string())?;
    // The export above can take a while; check again right before the write, and once more after
    // it. The write itself cannot be made conditional on the lease, so readers also verify it
    // against the checksum sealed by the successor's claim (verify_catalog_sealed).
    super::leader_lease::require_lease(table_oid, "publish the leader catalog")?;
    koldstore_storage::publish_mutable_object(&client, &key, &bytes).map_err(|error| error.to_string())?;
    super::leader_lease::require_lease(table_oid, "publish the leader catalog")
        .map_err(|error| format!("{error} (the catalog was written just before the lease was lost; followers ignore it)"))?;
    // Sidecars no longer listed by the catalog are garbage; a follower mid-read just retries.
    if let Ok(objects) = client.list(&chunk_prefix) {
        for object in objects.into_iter().filter(|object| !chunk_keys.contains(&object.key)) {
            let _ = client.delete(&object.key);
        }
    }
    Ok(json!({
        "key": key, "bytes": bytes.len(), "generation": generation, "publication": publication,
        "tombstones": metadata["tombstones"].as_array().map_or(0, Vec::len)
            + metadata["tombstone_chunks"].as_array().map_or(0, |c| c.iter().filter_map(|c| c["count"].as_u64()).sum::<u64>() as usize),
        "tombstones_truncated": metadata["tombstones_truncated"].as_bool().unwrap_or(false),
        "sidecars": chunk_keys.len(),
    }))
}

/// Whether a delete-marker sidecar the published catalog lists is gone from storage (cleaned up by
/// a concurrent or just-deposed publisher, or removed by hand). Followers refuse such a catalog
/// until it is republished, and an unchanged leader would not, so the worker checks.
pub(crate) fn sidecars_missing(table_oid: pgrx::pg_sys::Oid) -> Result<bool, String> {
    use koldstore_storage::StorageClient;

    let target = super::backup::table_target(table_oid)?;
    let client = super::backup::storage_client(&target.storage.storage_id)?;
    let bytes = client.get(&leader_catalog_key(&target.namespace, &target.name)).map_err(|error| error.to_string())?;
    let catalog: Value = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    let Some(chunks) = catalog["tombstone_chunks"].as_array().filter(|chunks| !chunks.is_empty()) else {
        return Ok(false);
    };
    let present = client
        .list(&format!("_leader_tombstones/{}/{}/", target.namespace, target.name))
        .map_err(|error| error.to_string())?;
    Ok(chunks.iter().filter_map(|chunk| chunk["key"].as_str()).any(|key| !present.iter().any(|object| object.key == key)))
}

/// Leader side of read-your-writes for a client that has just written: applies the async mirror up to
/// the current WAL position, then publishes, and returns the publication number to hand to
/// `wait_for_publication`.
///
/// SQL contract: `koldstore.publish_for_followers(table_name regclass) → jsonb` (the
/// `publish_leader_catalog` result).
///
/// `publish_leader_catalog` alone can miss a write: deletes reach the catalog through the async
/// mirror, which lags the commit. Inserts and updates still in the leader's hot tier are not covered:
/// a follower without Spock never receives them, and its own stale heap row shadows a flushed cold copy.
#[pgrx::pg_extern(name = "publish_for_followers", schema = "koldstore", security_definer)]
pub fn publish_for_followers_pg(table_name: pgrx::PgRelation) -> pgrx::JsonB {
    let oid = table_name.oid();
    authorize(Some(oid), "publish for followers");
    // The end of this session's last commit: a subscriber's origin position reaches exactly that once it
    // has applied the commit, whereas the current WAL position may never be reached on an idle table.
    // A session that has not committed yet falls back to the current position.
    let committed = unsafe { pgrx::pg_sys::XactLastCommitEnd };
    pgrx::Spi::run("SELECT koldstore.wait_for_async_mirror()")
        .map_err(|error| error.to_string())
        .and_then(|()| publish_leader_catalog_value(oid))
        .and_then(|mut result| {
            result["spock_node_id"] = json!(crate::spi::select_one_sql::<String>(
                "SELECT CASE WHEN to_regclass('spock.local_node') IS NOT NULL
                        THEN (SELECT node_id::text FROM spock.local_node) END",
                &[],
            )
            .map_err(|error| error.to_string())?);
            result["lsn"] = json!(if committed == 0 {
                crate::spi::select_one_sql::<String>("SELECT pg_current_wal_lsn()::text", &[]).map_err(|error| error.to_string())?
            } else {
                Some(koldstore_common::format_pg_lsn(committed))
            });
            Ok(result)
        })
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("publish for followers failed: {error}"))
}

/// Publishes this node's cold-tier catalog where followers can pull it.
///
/// SQL contract: `koldstore.publish_leader_catalog(table_name regclass) → jsonb`
/// (`{key, bytes, generation}`).
///
/// Run on the tiering leader after a flush or compaction. The object is written atomically beside
/// (not inside) the table prefix, and publishing is refused when a catalog with a higher manifest
/// generation is already there, so a stale former leader cannot roll the shared catalog back.
#[pgrx::pg_extern(name = "publish_leader_catalog", schema = "koldstore", security_definer)]
pub fn publish_leader_catalog_pg(table_name: pgrx::PgRelation) -> pgrx::JsonB {
    let oid = table_name.oid();
    authorize(Some(oid), "publish leader catalog");
    publish_leader_catalog_value(oid)
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("publish leader catalog failed: {error}"))
}

fn sync_from_leader_value(table_oid: pgrx::pg_sys::Oid) -> Result<Value, String> {
    sync_from_leader_inner(table_oid, false)
}

/// Applies the leader catalog the way a follower does (prune, delete markers) on a node that is
/// being promoted and so no longer carries the follower flag; no acknowledgement is recorded.
pub(crate) fn adopt_leader_catalog(table_oid: pgrx::pg_sys::Oid) -> Result<Value, String> {
    sync_from_leader_inner(table_oid, true)
}

fn sync_from_leader_inner(table_oid: pgrx::pg_sys::Oid, promoting: bool) -> Result<Value, String> {
    use koldstore_storage::StorageClient;

    let target = super::backup::table_target(table_oid)?;
    let client = super::backup::storage_client(&target.storage.storage_id)?;
    let key = leader_catalog_key(&target.namespace, &target.name);
    let bytes = client
        .get(&key)
        .map_err(|error| format!("no leader catalog at {key} ({error}); has the leader run publish_leader_catalog()?"))?;
    let metadata: Value = serde_json::from_slice(&bytes).map_err(|error| format!("leader catalog is not valid: {error}"))?;
    super::leader_lease::verify_catalog_sealed(table_oid, &bytes, metadata["lease_epoch"].as_i64())?;
    // Epoch fencing: never move back to a catalog from a deposed leader.
    if let Some(epoch) = metadata["lease_epoch"].as_i64() {
        let seen = count(
            "SELECT (SELECT coalesce((options->>'seen_leader_epoch')::bigint, 0) FROM koldstore.schemas WHERE table_oid = $1 AND active)",
            &[DatumWithOid::from(table_oid)],
        )?;
        if epoch < seen {
            return Err(format!("leader catalog is from lease epoch {epoch}, older than the epoch {seen} already followed"));
        }
        pgrx::Spi::run_with_args(
            "UPDATE koldstore.schemas SET options = jsonb_set(coalesce(options, '{}'::jsonb), '{seen_leader_epoch}', to_jsonb($2::bigint), true) \
             WHERE table_oid = $1 AND active",
            &[DatumWithOid::from(table_oid), DatumWithOid::from(epoch)],
        )
        .map_err(|error| error.to_string())?;
    }
    // Delete markers beyond the inline limit sit in checksummed sidecar objects.
    let mut tombstones = metadata["tombstones"].clone();
    if let (Some(chunks), Some(all)) = (metadata["tombstone_chunks"].as_array(), tombstones.as_array_mut()) {
        for chunk in chunks {
            let chunk_key = chunk["key"].as_str().ok_or("tombstone chunk has no key")?;
            let body = client
                .get(chunk_key)
                .map_err(|error| format!("cannot read delete markers at {chunk_key} ({error}); the leader may have republished, retrying"))?;
            if Some(koldstore_storage::content_checksum_sha256_hex(&body).as_str()) != chunk["sha256"].as_str() {
                return Err(format!("delete markers at {chunk_key} do not match the catalog's checksum"));
            }
            match serde_json::from_slice::<Value>(&body) {
                Ok(Value::Array(mut part)) => all.append(&mut part),
                _ => return Err(format!("delete markers at {chunk_key} are not valid")),
            }
        }
    }
    let mut result = sync_value(table_oid, &metadata)?;
    if let Some(publication) = metadata["publication"].as_i64() {
        pgrx::Spi::run_with_args(
            "UPDATE koldstore.schemas SET options = jsonb_set(coalesce(options, '{}'::jsonb), '{seen_publication}', to_jsonb($2::bigint), true) \
             WHERE table_oid = $1 AND active",
            &[DatumWithOid::from(table_oid), DatumWithOid::from(publication)],
        )
        .map_err(|error| error.to_string())?;
        result["publication"] = json!(publication);
    }
    let added = take_added_ranges(&mut result);
    // Same transaction as the adoption: a failed prune rolls the sync back and the next tick
    // retries, so a follower never holds segments whose hot copies were not considered.
    if (promoting || is_follower(table_oid)?) && !added.is_empty() {
        let pruned = super::flush::peer_prune::prune_covered(table_oid, Some(&added))?;
        result["hot_pruned"] = pruned["hot_pruned"].clone();
    }
    if (promoting || is_follower(table_oid)?) && metadata.get("tombstones").is_some() {
        let applied = super::flush::peer_prune::apply_leader_tombstones(table_oid, &tombstones)?;
        result["tombstones_added"] = applied["tombstones_added"].clone();
        result["tombstones_removed"] = applied["tombstones_removed"].clone();
        result["tombstones_truncated"] = json!(metadata["tombstones_truncated"].as_bool().unwrap_or(false));
    }
    if is_follower(table_oid)? {
        if let Some(generation) = result["generation"].as_i64() {
            // Lets the leader hold back reaping until this node has moved past the old segments.
            if let Err(error) = super::follower_ack::record(table_oid, generation) {
                pgrx::warning!("koldstore: could not record the follower acknowledgement: {error}");
            }
        }
    }
    Ok(result)
}

/// Deletes a follower's hot rows that exactly match rows in the leader's cold segments.
///
/// SQL contract: `koldstore.prune_follower_hot(table_name regclass) → jsonb`
/// (`{segments, rows_read, hot_pruned}`).
///
/// `sync_from_leader()` already prunes the segments it adopts; this covers every active segment,
/// for example those adopted by `import_table_metadata()` before follower mode was set. A hot row
/// that differs from the cold copy in any column is left alone. Deletes are neither replicated
/// nor mirrored. A hot row whose key the leader has since deleted is dropped when it equals an older
/// cold version of that key. Not supported on user-scoped tables.
#[pgrx::pg_extern(name = "prune_follower_hot", schema = "koldstore", security_definer)]
pub fn prune_follower_hot_pg(table_name: pgrx::PgRelation) -> pgrx::JsonB {
    let oid = table_name.oid();
    authorize(Some(oid), "prune follower hot rows");
    let result = is_follower(oid)
        .and_then(|follower| {
            if follower {
                super::flush::peer_prune::prune_covered(oid, None)
            } else {
                Err("the table is not a follower (koldstore.set_table_follower)".to_string())
            }
        })
        .unwrap_or_else(|error| pgrx::error!("prune follower hot failed: {error}"));
    pgrx::JsonB(result)
}

/// Pulls the leader's published catalog and applies it to this follower table.
///
/// SQL contract: `koldstore.sync_from_leader(table_name regclass) → jsonb`
/// (`{added, removed, segments, generation}`, as `sync_table_metadata`).
///
/// The table must already have adopted a leader export with `import_table_metadata()` and share
/// the leader's cold storage.
#[pgrx::pg_extern(name = "sync_from_leader", schema = "koldstore", security_definer)]
pub fn sync_from_leader_pg(table_name: pgrx::PgRelation) -> pgrx::JsonB {
    let oid = table_name.oid();
    authorize(Some(oid), "sync from leader");
    sync_from_leader_value(oid)
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("sync from leader failed: {error}"))
}

/// Blocks until this follower has applied the leader publication numbered `want` or later, then
/// returns the sync report (`{publication, waited_ms, synced, ...}`).
fn wait_for_publication_value(table_oid: pgrx::pg_sys::Oid, want: i64, timeout_ms: i32) -> Result<Value, String> {
    use koldstore_storage::StorageClient;

    let target = super::backup::table_target(table_oid)?;
    let client = super::backup::storage_client(&target.storage.storage_id)?;
    let key = leader_catalog_key(&target.namespace, &target.name);
    let started = std::time::Instant::now();
    let deadline = std::time::Duration::from_millis(u64::try_from(timeout_ms.max(0)).unwrap_or(0));
    loop {
        let seen = count(
            "SELECT (SELECT coalesce((options->>'seen_publication')::bigint, 0) FROM koldstore.schemas WHERE table_oid = $1 AND active)",
            &[DatumWithOid::from(table_oid)],
        )?;
        let waited = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
        if seen >= want {
            return Ok(json!({ "publication": seen, "waited_ms": waited, "synced": false }));
        }
        let published = client
            .get(&key)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .and_then(|catalog| catalog["publication"].as_i64())
            .unwrap_or(0);
        if published >= want {
            let mut result = sync_from_leader_value(table_oid)?;
            result["waited_ms"] = json!(i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX));
            result["synced"] = json!(true);
            return Ok(result);
        }
        if started.elapsed() >= deadline {
            return Err(format!(
                "timed out after {timeout_ms} ms waiting for leader publication {want}; the leader has published {published} \
                 and this follower has applied {seen}"
            ));
        }
        pgrx::check_for_interrupts!();
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

fn replication_reached(lsn: &str, spock_node_id: Option<&str>) -> Result<bool, String> {
    let to_error = |error: pgrx::spi::SpiError| error.to_string();
    if let Some(node_id) = spock_node_id {
        let has_progress = crate::spi::select_one_sql::<bool>("SELECT to_regclass('spock.progress') IS NOT NULL", &[])
            .map_err(to_error)?
            .unwrap_or(false);
        if has_progress {
            // The leader's own position in Spock's per-upstream progress, not the highest of all upstreams.
            return crate::spi::select_one_sql::<bool>(
                "SELECT coalesce((SELECT remote_commit_lsn FROM spock.progress WHERE remote_node_id = $1::oid), '0/0'::pg_lsn) >= $2::pg_lsn",
                &[DatumWithOid::from(node_id), DatumWithOid::from(lsn)],
            )
            .map(|reached| reached.unwrap_or(false))
            .map_err(to_error);
        }
    }
    crate::spi::select_one_sql::<bool>(
        "SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() >= $1::pg_lsn
                ELSE coalesce((SELECT max(remote_lsn) FROM pg_replication_origin_status), '0/0'::pg_lsn) >= $1::pg_lsn END",
        &[DatumWithOid::from(lsn)],
    )
    .map(|reached| reached.unwrap_or(false))
    .map_err(to_error)
}

/// Follower side of read-your-writes for the hot tier: waits until replication has applied the leader
/// position `lsn` returned by `publish_for_followers`.
///
/// SQL contract: `koldstore.wait_for_replication(table_name regclass, lsn text,
/// timeout_ms integer default 10000, spock_node_id text default null) → jsonb` (`{lsn, waited_ms}`).
///
/// Hot rows reach a follower through replication, not through the shared storage. On a physical
/// standby this waits for WAL replay to reach `lsn`; otherwise for the highest remote LSN of any
/// replication origin (native logical replication and Spock both advance one) to reach it. With several
/// upstreams the highest origin decides, so it can return early for a slower one. Pass the
/// `spock_node_id` that `publish_for_followers` returned (set when the leader runs Spock) to wait on that
/// upstream's own `spock.progress` entry instead.
#[pgrx::pg_extern(name = "wait_for_replication", schema = "koldstore", security_definer)]
pub fn wait_for_replication_pg(
    table_name: pgrx::PgRelation,
    lsn: &str,
    timeout_ms: pgrx::default!(i32, 10000),
    spock_node_id: pgrx::default!(Option<&str>, "NULL"),
) -> pgrx::JsonB {
    authorize(Some(table_name.oid()), "wait for replication");
    let started = std::time::Instant::now();
    let deadline = std::time::Duration::from_millis(u64::try_from(timeout_ms.max(0)).unwrap_or(0));
    loop {
        match replication_reached(lsn, spock_node_id) {
            Ok(true) => {
                let waited = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
                return pgrx::JsonB(json!({ "lsn": lsn, "waited_ms": waited }));
            }
            Ok(false) => {}
            Err(error) => pgrx::error!("wait for replication failed: {error}"),
        }
        if started.elapsed() >= deadline {
            pgrx::error!("timed out after {timeout_ms} ms waiting for replication to reach {lsn}");
        }
        pgrx::check_for_interrupts!();
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// Follower side of read-your-writes: waits until the leader publication numbered `publication`
/// has been applied here.
///
/// SQL contract: `koldstore.wait_for_publication(table_name regclass, publication bigint,
/// timeout_ms integer default 10000) → jsonb`.
///
/// After a write on the leader, `koldstore.publish_leader_catalog()` there returns the number of
/// the publication that contains it (`publication`). Calling this on a follower with that number
/// returns once the follower has synced that publication or a later one, so a read that follows
/// sees the write (a flushed row, a delete marker, a compaction). It polls the shared catalog and
/// syncs at once rather than waiting for the sync worker's next tick; it errors when the
/// timeout passes first.
#[pgrx::pg_extern(name = "wait_for_publication", schema = "koldstore", security_definer)]
pub fn wait_for_publication_pg(
    table_name: pgrx::PgRelation,
    publication: i64,
    timeout_ms: pgrx::default!(i32, 10000),
) -> pgrx::JsonB {
    let oid = table_name.oid();
    authorize(Some(oid), "wait for publication");
    wait_for_publication_value(oid, publication, timeout_ms)
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("wait for publication failed: {error}"))
}

/// Whether `table_oid` is marked as a follower of a leader's cold tier.
pub(crate) fn is_follower(table_oid: pgrx::pg_sys::Oid) -> Result<bool, String> {
    use pgrx::datum::DatumWithOid;

    pgrx::Spi::get_one_with_args::<bool>(
        "SELECT (SELECT coalesce((options->>'follower')::boolean, false) \
         FROM koldstore.schemas WHERE table_oid = $1 AND active)",
        &[DatumWithOid::from(table_oid)],
    )
    .map(|found| found.unwrap_or(false))
    .map_err(|error| error.to_string())
}

/// Errors when `table_oid` is a follower; `action` names the refused operation.
pub(crate) fn refuse_on_follower(table_oid: pgrx::pg_sys::Oid, action: &str) -> Result<(), String> {
    if is_follower(table_oid)? {
        return Err(format!(
            "cannot {action}: the table is a follower of a leader's cold tier \
             (koldstore.set_table_follower(table, false) turns follower mode off)"
        ));
    }
    Ok(())
}

pub(crate) fn set_follower_value(table_oid: pgrx::pg_sys::Oid, enabled: bool) -> Result<bool, String> {
    use pgrx::datum::DatumWithOid;

    let _lock = super::job_lock::TableJobLockGuard::lock(table_oid)?;
    if enabled && pgrx::Spi::get_one_with_args::<bool>(
        "SELECT (SELECT coalesce((options->>'leader')::boolean, false) \
         FROM koldstore.schemas WHERE table_oid = $1 AND active)",
        &[DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false)
    {
        return Err("the table is the leader; turn leader mode off first".to_string());
    }
    let updated = pgrx::Spi::get_one_with_args::<bool>(
        "WITH updated AS ( \
             UPDATE koldstore.schemas \
             SET options = CASE WHEN $2::boolean \
                     THEN jsonb_set(jsonb_set(coalesce(options, '{}'::jsonb), '{follower}', 'true'::jsonb, true), \
                                    '{auto_flush}', 'false'::jsonb, true) \
                     ELSE options - 'follower' END, \
                 updated_at = now() \
             WHERE table_oid = $1::oid AND active RETURNING 1) \
         SELECT EXISTS (SELECT 1 FROM updated)",
        &[DatumWithOid::from(table_oid), DatumWithOid::from(enabled)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    if !updated {
        return Err("table is not an active managed table".to_string());
    }
    if !enabled {
        super::flush::peer_prune::clear_synthetic_tombstones(table_oid)?;
        super::follower_ack::forget(table_oid);
    } else {
        // Announce this follower at the generation it holds, so the leader's reaper waits for it
        // before its first sync.
        let held = count(
            "SELECT COALESCE(max(generation), 0)::bigint FROM koldstore.manifest WHERE table_oid = $1",
            &[DatumWithOid::from(table_oid)],
        )?;
        if let Err(error) = super::follower_ack::record(table_oid, held) {
            pgrx::warning!("koldstore: could not record the follower acknowledgement: {error}");
        }
    }
    crate::catalog::cache::invalidate_table_globally(table_oid);
    Ok(true)
}

pub(crate) fn set_leader_value(table_oid: pgrx::pg_sys::Oid, enabled: bool) -> Result<bool, String> {
    use pgrx::datum::DatumWithOid;

    let _lock = super::job_lock::TableJobLockGuard::lock(table_oid)?;
    if enabled && is_follower(table_oid)? {
        return Err("the table is a follower; turn follower mode off first".to_string());
    }
    let updated = pgrx::Spi::get_one_with_args::<bool>(
        "WITH updated AS ( \
             UPDATE koldstore.schemas \
             SET options = CASE WHEN $2::boolean \
                     THEN jsonb_set(coalesce(options, '{}'::jsonb), '{leader}', 'true'::jsonb, true) \
                     ELSE options - 'leader' END, \
                 updated_at = now() \
             WHERE table_oid = $1::oid AND active RETURNING 1) \
         SELECT EXISTS (SELECT 1 FROM updated)",
        &[DatumWithOid::from(table_oid), DatumWithOid::from(enabled)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    if !updated {
        return Err("table is not an active managed table".to_string());
    }
    crate::catalog::cache::invalidate_table_globally(table_oid);
    Ok(true)
}

/// Marks (or unmarks) a table as the tiering leader whose catalog is published automatically.
///
/// SQL contract: `koldstore.set_table_leader(table_name regclass, enabled boolean) → boolean`.
///
/// With `koldstore.catalog_sync_interval_seconds` set, the catalog sync worker publishes the
/// leader catalog whenever the manifest generation changes (after a flush, compaction or
/// reap), so followers see committed state only. A table cannot be leader and follower.
#[pgrx::pg_extern(name = "set_table_leader", schema = "koldstore", security_definer)]
pub fn set_table_leader_pg(table_name: pgrx::PgRelation, enabled: bool) -> bool {
    let oid = table_name.oid();
    authorize(Some(oid), "change leader mode");
    set_leader_value(oid, enabled)
        .unwrap_or_else(|error| pgrx::error!("set_table_leader failed: {error}"))
}

/// Marks (or unmarks) a table as a follower of a leader's cold tier.
///
/// SQL contract: `koldstore.set_table_follower(table_name regclass, enabled boolean) → boolean`.
///
/// A follower refuses `flush_table`, `enqueue_flush_job`, `compact_table` and
/// `reap_compacted_segments`, and the maintenance worker skips it; enabling also turns
/// `auto_flush` off. Disabling leaves `auto_flush` off for the operator to re-enable.
#[pgrx::pg_extern(name = "set_table_follower", schema = "koldstore", security_definer)]
pub fn set_table_follower_pg(table_name: pgrx::PgRelation, enabled: bool) -> bool {
    let oid = table_name.oid();
    authorize(Some(oid), "change follower mode");
    set_follower_value(oid, enabled)
        .unwrap_or_else(|error| pgrx::error!("set_table_follower failed: {error}"))
}

/// Etag of the published leader catalog, `None` when there is none. The sync worker compares it
/// with the one it recorded after its own publish to notice a catalog overwritten by a deposed
/// leader.
pub(crate) fn catalog_etag(table_oid: pgrx::pg_sys::Oid) -> Result<Option<String>, String> {
    use koldstore_storage::StorageClient;

    let target = super::backup::table_target(table_oid)?;
    let client = super::backup::storage_client(&target.storage.storage_id)?;
    match client.head(&leader_catalog_key(&target.namespace, &target.name)) {
        Ok(object) => Ok(object.etag),
        Err(koldstore_storage::StorageClientError::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}
