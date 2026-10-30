//! Follower hot-heap prune against segments adopted from a leader.
//!
//! A follower shares the leader's cold tier but keeps its own heap (Spock still replicates hot
//! rows to it). Once the leader flushes a row, the follower's hot copy is redundant. A hot row is
//! deleted only when it equals the segment's copy exactly (primary key and whole-row JSON), so a
//! row edited after the leader's flush stays hot. Any doubt leaves the row in place.

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};

const BATCH_ROWS: usize = 500;

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Prunes hot rows covered by the table's active table-wide segments.
///
/// `only` restricts the work to segments whose `(min_seq, max_seq)` is listed (the segments a sync
/// just adopted); `None` covers every active segment. Returns `{segments, rows_read, hot_pruned}`.
pub(crate) fn prune_covered(
    table_oid: pgrx::pg_sys::Oid,
    only: Option<&[(i64, i64)]>,
) -> Result<Value, String> {
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or("table is not managed")?;
    if snapshot.scope_column.is_some() {
        return Err("peer prune does not support user-scoped tables".into());
    }
    let Some(manifest) = crate::catalog::cache::cached_manifest_scan_context(table_oid, &[])?
    else {
        return Ok(json!({ "segments": 0, "rows_read": 0, "hot_pruned": 0 }));
    };

    let table = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let mirror = koldstore_common::QualifiedTableName::from_table_name(&snapshot.mirror_relation)
        .quoted();
    let pk: Vec<String> = snapshot
        .primary_key_columns
        .iter()
        .map(|column| quote_ident(&column.name))
        .collect();
    let hot_match = pk
        .iter()
        .map(|c| format!("hot.{c} = cold.{c}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let mirror_match = pk
        .iter()
        .map(|c| format!("m.{c} = d.{c}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let returning = pk
        .iter()
        .map(|c| format!("hot.{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "WITH cold AS (SELECT * FROM jsonb_populate_recordset(NULL::{table}, $1)), \
         del AS (DELETE FROM ONLY {table} hot USING cold \
                 WHERE {hot_match} AND to_jsonb(hot) = to_jsonb(cold) RETURNING {returning}), \
         mir AS (DELETE FROM {mirror} m USING del d WHERE {mirror_match} RETURNING 1) \
         SELECT (SELECT count(*) FROM del)::bigint AS hot_pruned, \
                (SELECT count(*) FROM mir)::bigint AS mirror_pruned"
    );

    let run_batches = |images: &[Value]| -> Result<(i64, i64), String> {
        let (mut hot_pruned, mut mirror_pruned) = (0_i64, 0_i64);
        for batch in images.chunks(BATCH_ROWS) {
            let payload = pgrx::JsonB(Value::Array(batch.to_vec()));
            let (hot, mir) = super::spi::with_prune_session(|client| {
                let result = client
                    .update(&sql, None, &[pgrx::datum::DatumWithOid::from(payload)])
                    .map_err(|error| error.to_string())?;
                let row = result.first();
                let hot = row
                    .get_by_name::<i64, &str>("hot_pruned")
                    .map_err(|error| error.to_string())?
                    .unwrap_or(0);
                let mir = row
                    .get_by_name::<i64, &str>("mirror_pruned")
                    .map_err(|error| error.to_string())?
                    .unwrap_or(0);
                Ok((hot, mir))
            })?;
            hot_pruned += hot;
            mirror_pruned += mir;
        }
        Ok((hot_pruned, mirror_pruned))
    };
    let read_images = |segment: &koldstore_merge::scan::SegmentStatsHint| {
        crate::sql::events::read_cold_segment_page(table_oid, &snapshot, segment, 0, usize::MAX, None)
    };
    let to_image = |image: Value, pk: Value| -> Option<Value> {
        let Value::Object(mut image) = image else {
            return None;
        };
        if let Value::Object(pk) = pk {
            image.extend(pk);
        }
        Some(Value::Object(image))
    };

    let mut segments = 0_i64;
    let mut rows_read = 0_i64;
    let (mut hot_pruned, mut mirror_pruned) = (0_i64, 0_i64);
    let mut marker_keys: HashSet<String> = HashSet::new();
    for segment in &manifest.segments {
        if segment.scope_id.is_some() {
            continue;
        }
        if let Some(only) = only {
            let range = (segment.min_seq.get(), segment.max_seq.get());
            if !only.contains(&range) {
                continue;
            }
        }
        segments += 1;
        let changes = read_images(segment)?;
        let mut images: Vec<Value> = Vec::new();
        for change in changes {
            if change.deleted {
                marker_keys.insert(change.pk_json.to_string());
            } else if let Some(image) = change.row_image_json.and_then(|image| to_image(image, change.pk_json)) {
                images.push(image);
            }
        }
        rows_read += images.len() as i64;
        let (hot, mir) = run_batches(&images)?;
        hot_pruned += hot;
        mirror_pruned += mir;
    }

    // Keys the leader deleted: a follower hot copy is dropped when it equals an older cold version
    // of the key, provided the key's newest cold change (by the leader's seq) is the delete marker.
    // A re-insert after the delete, or a hot row that differs from every cold version, stays put.
    if !marker_keys.is_empty() {
        let mut newest: HashMap<String, (i64, bool)> = HashMap::new();
        let mut versions: HashMap<String, Vec<Value>> = HashMap::new();
        for segment in &manifest.segments {
            if segment.scope_id.is_some() {
                continue;
            }
            for change in read_images(segment)? {
                let key = change.pk_json.to_string();
                if !marker_keys.contains(&key) {
                    continue;
                }
                let seq = change.seq.get();
                let entry = newest.entry(key.clone()).or_insert((seq, change.deleted));
                if seq >= entry.0 {
                    *entry = (seq, change.deleted);
                }
                if let Some(image) = change.row_image_json.and_then(|image| to_image(image, change.pk_json)) {
                    versions.entry(key).or_default().push(image);
                }
            }
        }
        let stale: Vec<Value> = newest
            .iter()
            .filter(|(_, (_, deleted))| *deleted)
            .flat_map(|(key, _)| versions.remove(key).unwrap_or_default())
            .collect();
        let (hot, mir) = run_batches(&stale)?;
        hot_pruned += hot;
        mirror_pruned += mir;
    }
    if hot_pruned > 0 || mirror_pruned > 0 {
        super::execute::apply_flush_row_count_deltas(table_oid, mirror_pruned, hot_pruned, 0)?;
    }
    Ok(json!({ "segments": segments, "rows_read": rows_read, "hot_pruned": hot_pruned }))
}

struct MirrorParts {
    table: String,
    mirror: String,
    pk: Vec<String>,
    pk_names: Vec<String>,
}

fn mirror_parts(table_oid: pgrx::pg_sys::Oid) -> Result<MirrorParts, String> {
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or("table is not managed")?;
    if snapshot.scope_column.is_some() {
        return Err("tombstone sync does not support user-scoped tables".into());
    }
    Ok(MirrorParts {
        table: crate::catalog::resolve::qualified_relation_name(table_oid)?,
        mirror: koldstore_common::QualifiedTableName::from_table_name(&snapshot.mirror_relation).quoted(),
        pk: snapshot.primary_key_columns.iter().map(|c| quote_ident(&c.name)).collect(),
        pk_names: snapshot.primary_key_columns.iter().map(|c| c.name.clone()).collect(),
    })
}

/// Cheap change detector for the leader's unflushed delete markers (count and seq range).
pub(crate) fn tombstone_fingerprint(table_oid: pgrx::pg_sys::Oid) -> Result<String, String> {
    let parts = mirror_parts(table_oid)?;
    let deletes = pgrx::Spi::get_one::<String>(&format!(
        "SELECT count(*) || ':' || coalesce(min(\"seq\"), 0) || ':' || coalesce(max(\"seq\"), 0) \
         FROM {} WHERE \"op\" = 3 AND \"seq\" > 0",
        parts.mirror
    ))
    .map(Option::unwrap_or_default)
    .map_err(|error| error.to_string())?;
    let reinserts = pgrx::Spi::get_one_with_args::<String>(
        &format!(
            "SELECT count(*) || ':' || coalesce(max(m.\"seq\"), 0) FROM {}",
            reinserted_join(&parts)
        ),
        &[pgrx::datum::DatumWithOid::from(table_oid)],
    )
    .map(Option::unwrap_or_default)
    .map_err(|error| error.to_string())?;
    Ok(format!("{deletes}/{reinserts}"))
}

/// The recorded re-inserted keys that are still unflushed, joined to their mirror rows (`m`).
/// Takes the table oid as `$1`.
fn reinserted_join(parts: &MirrorParts) -> String {
    let same = parts.pk.iter().map(|c| format!("m.{c} = k.{c}")).collect::<Vec<_>>().join(" AND ");
    format!(
        "koldstore.reinserted_keys r \
         CROSS JOIN LATERAL jsonb_populate_record(NULL::{table}, r.key) k \
         JOIN {mirror} m ON {same} \
         WHERE r.table_oid = $1 AND m.\"op\" <> 3 AND m.\"seq\" > 0",
        table = parts.table,
        mirror = parts.mirror,
    )
}

/// Called by the insert guard on a leader: an INSERT of a key the mirror holds a delete marker for
/// is a re-insert of a deleted key, and the cold row it deleted is still in the segments. The
/// delete marker is about to be overwritten by the insert's mirror row, so the key is recorded
/// here and published as a marker until the leader flushes the new row; without it a follower that
/// does not replicate the leader's hot rows (no Spock) would return the old, deleted version.
pub(crate) fn record_reinsert(table_oid: pgrx::pg_sys::Oid, row: &Value) -> Result<(), String> {
    let parts = mirror_parts(table_oid)?;
    let object = parts
        .pk
        .iter()
        .zip(&parts.pk_names)
        .map(|(quoted, name)| format!("'{}', k.{quoted}", name.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    let same = parts.pk.iter().map(|c| format!("x.{c} = k.{c}")).collect::<Vec<_>>().join(" AND ");
    pgrx::Spi::run_with_args(
        &format!(
            "INSERT INTO koldstore.reinserted_keys (table_oid, key) \
             SELECT $1, jsonb_build_object({object}) FROM jsonb_populate_record(NULL::{table}, $2) k \
             WHERE coalesce((SELECT (options->>'leader')::boolean FROM koldstore.schemas \
                             WHERE table_oid = $1 AND active), false) \
               AND EXISTS (SELECT 1 FROM {mirror} x WHERE {same} AND x.\"op\" = 3) \
             ON CONFLICT DO NOTHING",
            table = parts.table,
            mirror = parts.mirror,
        ),
        &[pgrx::datum::DatumWithOid::from(table_oid), pgrx::datum::DatumWithOid::from(pgrx::JsonB(row.clone()))],
    )
    .map_err(|error| error.to_string())
}

/// Forgets recorded keys whose mirror row is gone (the leader flushed them): the new row is in a
/// segment followers get with the catalog, so the marker must stop masking it.
pub(crate) fn prune_reinserted(table_oid: pgrx::pg_sys::Oid) -> Result<(), String> {
    let parts = mirror_parts(table_oid)?;
    let same = parts.pk.iter().map(|c| format!("m.{c} = k.{c}")).collect::<Vec<_>>().join(" AND ");
    pgrx::Spi::run_with_args(
        &format!(
            "DELETE FROM koldstore.reinserted_keys r WHERE r.table_oid = $1 AND NOT EXISTS \
             (SELECT 1 FROM jsonb_populate_record(NULL::{table}, r.key) k \
              JOIN {mirror} m ON {same} WHERE m.\"seq\" > 0)",
            table = parts.table,
            mirror = parts.mirror,
        ),
        &[pgrx::datum::DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())
}

/// Most unflushed delete markers a leader publishes at all: the inline limit times this many
/// sidecar chunks. Beyond it the rest are hidden on followers only after the next flush.
pub(crate) const TOMBSTONE_CHUNKS_MAX: usize = 100;

/// The leader's unflushed delete markers as `[{pk column: value}]` (oldest first), capped at
/// `koldstore.published_tombstone_limit` times `TOMBSTONE_CHUNKS_MAX`, with a truncation flag.
pub(crate) fn leader_tombstones(table_oid: pgrx::pg_sys::Oid) -> Result<(Vec<Value>, bool), String> {
    let parts = mirror_parts(table_oid)?;
    let limit = crate::guc::published_tombstone_limit().saturating_mul(TOMBSTONE_CHUNKS_MAX);
    let object = parts
        .pk
        .iter()
        .zip(&parts.pk_names)
        .map(|(quoted, name)| format!("'{}', m.{quoted}", name.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    // Deletes first, then re-inserted keys (see `record_reinsert`), each oldest first.
    let pgrx::JsonB(Value::Array(mut keys)) = pgrx::Spi::get_one_with_args::<pgrx::JsonB>(
        &format!(
            "SELECT coalesce(jsonb_agg(j ORDER BY grp, ord), '[]'::jsonb) FROM \
             (SELECT j, grp, ord FROM \
               (SELECT jsonb_build_object({object}) AS j, 0 AS grp, m.\"seq\" AS ord FROM {mirror} m \
                 WHERE m.\"op\" = 3 AND m.\"seq\" > 0 \
                UNION ALL \
                SELECT r.key AS j, 1 AS grp, m.\"seq\" AS ord FROM {reinserted}) u \
              ORDER BY grp, ord LIMIT {limit1}) x",
            mirror = parts.mirror,
            reinserted = reinserted_join(&parts),
            limit1 = limit + 1,
        ),
        &[pgrx::datum::DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?
    .ok_or("no tombstone result")?
    else {
        return Err("unexpected tombstone result".into());
    };
    let truncated = keys.len() > limit;
    keys.truncate(limit);
    Ok((keys, truncated))
}

/// Replaces this follower's synthetic delete markers (mirror rows with `op = 3` and a negative
/// `seq`) with the leader's published unflushed ones. The merge scan treats them like any mirror
/// tombstone, so the keys stop returning their cold rows at once; a key that already has a mirror
/// row (the follower holds or deleted it itself) is left alone, and flush never reads them
/// because a follower does not flush.
pub(crate) fn apply_leader_tombstones(
    table_oid: pgrx::pg_sys::Oid,
    keys: &Value,
) -> Result<Value, String> {
    let parts = mirror_parts(table_oid)?;
    let empty = keys.as_array().map_or(true, Vec::is_empty);
    let existing = pgrx::Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM {} WHERE \"op\" = 3 AND \"seq\" < 0",
        parts.mirror
    ))
    .map_err(|error| error.to_string())?
    .unwrap_or(0);
    if empty && existing == 0 {
        return Ok(json!({ "tombstones_added": 0, "tombstones_removed": 0 }));
    }
    let cols = parts.pk.join(", ");
    let same = parts
        .pk
        .iter()
        .map(|c| format!("m.{c} = w.{c}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let payload = pgrx::JsonB(if empty { Value::Array(Vec::new()) } else { keys.clone() });
    let sql = format!(
        "WITH want AS (SELECT {cols} FROM jsonb_populate_recordset(NULL::{table}, $1)), \
         del AS (DELETE FROM {mirror} m WHERE m.\"op\" = 3 AND m.\"seq\" < 0 \
                 AND NOT EXISTS (SELECT 1 FROM want w WHERE {same}) RETURNING 1), \
         ins AS (INSERT INTO {mirror} ({cols}, \"seq\", \"op\") SELECT {cols}, -1, 3 FROM want \
                 ON CONFLICT DO NOTHING RETURNING 1) \
         SELECT (SELECT count(*) FROM ins)::bigint AS added, (SELECT count(*) FROM del)::bigint AS removed",
        table = parts.table,
        mirror = parts.mirror,
    );
    let (added, removed) = super::spi::with_prune_session(|client| {
        let result = client
            .update(&sql, None, &[pgrx::datum::DatumWithOid::from(payload)])
            .map_err(|error| error.to_string())?;
        let row = result.first();
        let get = |name: &str| {
            row.get_by_name::<i64, &str>(name)
                .map_err(|error| error.to_string())
                .map(|value| value.unwrap_or(0))
        };
        Ok((get("added")?, get("removed")?))
    })?;
    Ok(json!({ "tombstones_added": added, "tombstones_removed": removed }))
}

/// Drops every synthetic delete marker (the table stopped following a leader).
pub(crate) fn clear_synthetic_tombstones(table_oid: pgrx::pg_sys::Oid) -> Result<i64, String> {
    let parts = mirror_parts(table_oid)?;
    let sql = format!(
        "WITH d AS (DELETE FROM {} WHERE \"op\" = 3 AND \"seq\" < 0 RETURNING 1) SELECT count(*)::bigint AS n FROM d",
        parts.mirror
    );
    super::spi::with_prune_session(|client| {
        let result = client.update(&sql, None, &[]).map_err(|error| error.to_string())?;
        result
            .first()
            .get_by_name::<i64, &str>("n")
            .map_err(|error| error.to_string())
            .map(|value| value.unwrap_or(0))
    })
}
