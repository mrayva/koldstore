//! `koldstore.rebuild_segment_summaries`: gives segments written before value summaries existed the
//! summary flush would have written, so point lookups stop opening every one of them.
//!
//! A cold point lookup (`WHERE id = $1`) prunes by the catalog's min/max bounds and, for a primary key of
//! `smallint`/`integer`/`bigint`/`uuid`/`date`/`timestamp`/`timestamptz` columns (one or several), by a
//! per-segment membership bitmap (ADR-009).
//! Segments flushed before that column existed have a NULL summary, which always means "cannot prune":
//! correct, but every such segment whose bounds overlap the key is opened. This reads each of them
//! once, verifies it against the catalog, and stores the bitmap. It never changes cold data, only the
//! catalog's `cold_segment_index.value_summary`. The same call also gives segments the term filter or
//! vector sketch of any `text[]` / `jsonb` / `tsvector` / `vector` column that was listed in
//! `pruning_columns` after they were written (reported under `term_filters`).

use pgrx::datum::DatumWithOid;
use serde_json::{Value, json};

use koldstore_storage::StorageClient as _;

use super::backup::{authorize, json_query, managed_table_oids, table_target};

/// Segments handled by one call unless the caller says otherwise.
const DEFAULT_MAX_SEGMENTS: i32 = 100;

/// Why a segment could not be given a summary, for the report.
#[derive(Default)]
struct Outcome {
    rebuilt: i64,
    too_large: i64,
    failed: Vec<Value>,
}

/// The key of `table_oid` when a summary can exist for it: the stable id of its first column (whose
/// index row carries the summary) and every key column, in key order, with its Sort Key type.
fn summarizable_key(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Option<(i16, Vec<(String, koldstore_sortkey::SortKeyType)>)>, String> {
    let Some(snapshot) =
        crate::catalog::cache::managed_table_snapshot(table_oid).map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let Some(first) = snapshot.primary_key_columns.first() else {
        return Ok(None);
    };
    let mut columns = Vec::with_capacity(snapshot.primary_key_columns.len());
    for key in &snapshot.primary_key_columns {
        let type_oid = pgrx::Spi::get_one_with_args::<pgrx::pg_sys::Oid>(
            "SELECT atttypid FROM pg_catalog.pg_attribute \
             WHERE attrelid = $1 AND attnum = $2::smallint AND NOT attisdropped",
            &[
                DatumWithOid::from(table_oid),
                DatumWithOid::from(key.column_id.get()),
            ],
        )
        .map_err(|error| error.to_string())?;
        let Some(type_oid) = type_oid else {
            return Ok(None);
        };
        let Some(ty) = koldstore_sortkey::SortKeyType::from_type_oid(type_oid.to_u32())
            .filter(|ty| koldstore_parquet::supports_value_summary(*ty))
        else {
            return Ok(None);
        };
        columns.push((key.name.clone(), ty));
    }
    Ok(Some((first.column_id.get(), columns)))
}

fn missing_summary_segments(
    table_oid: pgrx::pg_sys::Oid,
    column_id: i16,
    limit: i32,
) -> Result<Vec<Value>, String> {
    let value = json_query(
        "SELECT COALESCE(jsonb_agg(s ORDER BY s_min_seq, s_id), '[]'::jsonb)::text FROM (
           SELECT jsonb_build_object('segment_id', cs.segment_id, 'path', cs.path,
                                     'row_count', cs.row_count, 'checksum', cs.checksum,
                                     'byte_size', cs.byte_size) AS s,
                  cs.min_seq AS s_min_seq, cs.segment_id AS s_id
           FROM koldstore.cold_segments cs
           JOIN koldstore.cold_segment_index csi ON csi.segment_id = cs.segment_id
           WHERE cs.table_oid = $1 AND cs.status = 'active'
             AND csi.column_id = $2::smallint AND csi.value_summary IS NULL
           ORDER BY cs.min_seq, cs.segment_id LIMIT $3::integer
         ) q",
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(column_id),
            DatumWithOid::from(limit),
        ],
    )?;
    Ok(value.as_array().cloned().unwrap_or_default())
}

fn count_missing(table_oid: pgrx::pg_sys::Oid, column_id: i16) -> Result<i64, String> {
    pgrx::Spi::get_one_with_args::<i64>(
        "SELECT count(*) FROM koldstore.cold_segments cs \
         JOIN koldstore.cold_segment_index csi ON csi.segment_id = cs.segment_id \
         WHERE cs.table_oid = $1 AND cs.status = 'active' \
           AND csi.column_id = $2::smallint AND csi.value_summary IS NULL",
        &[DatumWithOid::from(table_oid), DatumWithOid::from(column_id)],
    )
    .map_err(|error| error.to_string())
    .map(Option::unwrap_or_default)
}

fn rebuild_table(
    oid: pgrx::pg_sys::Oid,
    dry_run: bool,
    max_segments: i32,
) -> Result<Value, String> {
    let mut report = rebuild_summaries_table(oid, dry_run, max_segments)?;
    let term_filters = rebuild_term_filters_table(oid, dry_run, max_segments)?;
    let key_bounds = rebuild_key_bounds_table(oid, dry_run, max_segments)?;
    if !term_filters.is_null() {
        let rebuilt = report["rebuilt"].as_i64().unwrap_or(0) + term_filters["rebuilt"].as_i64().unwrap_or(0);
        report["rebuilt"] = json!(rebuilt);
        report["term_filters"] = term_filters;
    }
    if !key_bounds.is_null() {
        let rebuilt = report["rebuilt"].as_i64().unwrap_or(0) + key_bounds["rebuilt"].as_i64().unwrap_or(0);
        report["rebuilt"] = json!(rebuilt);
        report["key_bounds"] = key_bounds;
    }
    Ok(report)
}

/// What one stats column needs: a term filter, or a vector sketch.
enum FilterKind {
    Terms(koldstore_parquet::TermFilterKind),
    Sketch,
}

/// The `text[]` / `jsonb` / `tsvector` / `vector` columns of `table_oid` that get a term filter or
/// sketch at flush: `(column_id, name, type, kind)`.
fn filterable_columns(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Vec<(i16, String, koldstore_schema::PgType, FilterKind)>, String> {
    let value = json_query(
        "SELECT COALESCE(jsonb_agg(jsonb_build_object('column_id', c->>'column_id', 'name', c->>'name',
                                                      'type_name', c->>'type_name')), '[]'::jsonb)::text
         FROM koldstore.schemas s,
              jsonb_array_elements(s.options->'cold_metadata'->'stats_columns') sc,
              jsonb_array_elements(s.columns) c
         WHERE s.table_oid = $1 AND s.active AND sc->>'column_id' = c->>'column_id'",
        &[DatumWithOid::from(table_oid)],
    )?;
    let mut out = Vec::new();
    for entry in value.as_array().cloned().unwrap_or_default() {
        let (Some(id), Some(name), Some(type_name)) = (
            entry["column_id"].as_str().and_then(|id| id.parse::<i16>().ok()),
            entry["name"].as_str(),
            entry["type_name"].as_str(),
        ) else {
            continue;
        };
        let Ok(pg_type) = koldstore_schema::PgType::from_postgres_name(type_name) else {
            continue;
        };
        let kind = match pg_type {
            koldstore_schema::PgType::TextArray => FilterKind::Terms(koldstore_parquet::TermFilterKind::TextArray),
            koldstore_schema::PgType::Jsonb => FilterKind::Terms(koldstore_parquet::TermFilterKind::Jsonb),
            koldstore_schema::PgType::Tsvector => FilterKind::Terms(koldstore_parquet::TermFilterKind::Tsvector),
            koldstore_schema::PgType::Vector => FilterKind::Sketch,
            _ => continue,
        };
        out.push((id, name.to_string(), pg_type, kind));
    }
    Ok(out)
}

/// Active segments with no index row at all for `column_id` (flush only writes a term-filter or sketch
/// row when it built one), with the row-group count of their other index rows.
fn segments_without_filter(
    table_oid: pgrx::pg_sys::Oid,
    column_id: i16,
    limit: i32,
) -> Result<Vec<Value>, String> {
    let value = json_query(
        "SELECT COALESCE(jsonb_agg(s ORDER BY s_min_seq, s_id), '[]'::jsonb)::text FROM (
           SELECT jsonb_build_object('segment_id', cs.segment_id, 'path', cs.path,
                                     'row_count', cs.row_count, 'checksum', cs.checksum,
                                     'row_groups', COALESCE((SELECT cardinality(i.row_group_min_values)
                                                             FROM koldstore.cold_segment_index i
                                                             WHERE i.segment_id = cs.segment_id
                                                             ORDER BY i.column_id LIMIT 1), 0)) AS s,
                  cs.min_seq AS s_min_seq, cs.segment_id AS s_id
           FROM koldstore.cold_segments cs
           WHERE cs.table_oid = $1 AND cs.status = 'active'
             AND NOT EXISTS (SELECT 1 FROM koldstore.cold_segment_index t
                             WHERE t.segment_id = cs.segment_id AND t.column_id = $2::smallint)
           ORDER BY cs.min_seq, cs.segment_id LIMIT $3::integer
         ) q",
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(column_id),
            DatumWithOid::from(limit),
        ],
    )?;
    Ok(value.as_array().cloned().unwrap_or_default())
}

fn count_without_filter(table_oid: pgrx::pg_sys::Oid, column_id: i16) -> Result<i64, String> {
    pgrx::Spi::get_one_with_args::<i64>(
        "SELECT count(*) FROM koldstore.cold_segments cs \
         WHERE cs.table_oid = $1 AND cs.status = 'active' \
           AND NOT EXISTS (SELECT 1 FROM koldstore.cold_segment_index t \
                           WHERE t.segment_id = cs.segment_id AND t.column_id = $2::smallint)",
        &[DatumWithOid::from(table_oid), DatumWithOid::from(column_id)],
    )
    .map_err(|error| error.to_string())
    .map(Option::unwrap_or_default)
}

/// Gives segments flushed before a `text[]` / `jsonb` / `tsvector` / `vector` column was listed in
/// `pruning_columns` the term filter or vector sketch flush would have written. `Null` when the table
/// has no such column.
fn rebuild_term_filters_table(
    oid: pgrx::pg_sys::Oid,
    dry_run: bool,
    max_segments: i32,
) -> Result<Value, String> {
    let columns = filterable_columns(oid)?;
    if columns.is_empty() {
        return Ok(Value::Null);
    }
    let target = table_target(oid)?;
    let table = format!("{}.{}", target.namespace, target.name);
    let mut reports = Vec::new();
    let mut rebuilt_total = 0_i64;
    let client = if dry_run {
        None
    } else {
        Some(
            crate::object_store::open_managed_object_store_client(
                &target.storage.storage_type,
                &target.storage.base_path,
                &target.storage.credentials,
                &target.storage.config,
            )
            .map_err(|error| format!("{table}: open storage: {error}"))?,
        )
    };
    for (column_id, name, pg_type, kind) in columns {
        let missing_before = count_without_filter(oid, column_id)?;
        let Some(client) = client.as_ref() else {
            reports.push(json!({"column": name, "segments_without_filter": missing_before}));
            continue;
        };
        let (mut rebuilt, mut unbuildable) = (0_i64, 0_i64);
        let mut failed = Vec::new();
        for segment in segments_without_filter(oid, column_id, max_segments)? {
            let segment_id = segment["segment_id"].as_str().unwrap_or_default().to_string();
            let rows = segment["row_count"].as_i64().unwrap_or(0);
            let key = koldstore_common::join_object_key(
                &target.prefix,
                segment["path"].as_str().unwrap_or_default(),
            );
            let bytes = match client.get(&key) {
                Ok(bytes) => bytes,
                Err(error) => {
                    failed.push(json!({"segment_id": segment_id, "problem": format!("read {key}: {error}")}));
                    continue;
                }
            };
            let expected = segment["checksum"].as_str().unwrap_or_default();
            let actual = koldstore_storage::content_checksum_sha256_hex(&bytes);
            if !expected.eq_ignore_ascii_case(&actual) {
                failed.push(json!({
                    "segment_id": segment_id,
                    "problem": format!("checksum: catalog says {expected}, object hashes to {actual}"),
                }));
                continue;
            }
            let result = match &kind {
                FilterKind::Terms(kind) => {
                    koldstore_parquet::rebuild_term_filter_from_parquet(bytes, &name, *kind)
                }
                FilterKind::Sketch => koldstore_parquet::rebuild_vector_sketch_from_parquet(bytes, &name),
            };
            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    failed.push(json!({"segment_id": segment_id, "problem": error}));
                    continue;
                }
            };
            if i64::try_from(result.rows).ok() != Some(rows) {
                failed.push(json!({
                    "segment_id": segment_id,
                    "problem": format!("object has {} rows, catalog says {rows}", result.rows),
                }));
                continue;
            }
            let Some(summary) = result.summary else {
                unbuildable += 1;
                continue;
            };
            let row_groups = i32::try_from(segment["row_groups"].as_i64().unwrap_or(0)).unwrap_or(0);
            let codec = i16::try_from(koldstore_sortkey::CODEC_VERSION).unwrap_or(1);
            // `ON CONFLICT DO NOTHING` keeps this idempotent against a concurrent flush or rebuild: the
            // filter is a pure function of the (immutable) object.
            pgrx::Spi::run_with_args(
                "INSERT INTO koldstore.cold_segment_index
                   (segment_id, table_oid, scope_key, column_id, type_oid, codec_version, min_value, max_value,
                    row_group_min_values, row_group_max_values, row_group_null_counts, value_summary)
                 SELECT cs.segment_id, cs.table_oid, cs.scope_key, $2::smallint, $3::bigint::oid, $4::smallint, NULL, NULL,
                        array_fill(NULL::bytea, ARRAY[$5::integer]), array_fill(NULL::bytea, ARRAY[$5::integer]),
                        array_fill(NULL::bigint, ARRAY[$5::integer]), $6
                 FROM koldstore.cold_segments cs
                 WHERE cs.segment_id = $1::uuid AND cs.status = 'active'
                 ON CONFLICT (segment_id, column_id) DO NOTHING",
                &[
                    DatumWithOid::from(segment_id.as_str()),
                    DatumWithOid::from(column_id),
                    DatumWithOid::from(i64::from(pg_type.type_oid())),
                    DatumWithOid::from(codec),
                    DatumWithOid::from(row_groups),
                    DatumWithOid::from(summary),
                ],
            )
            .map_err(|error| format!("{table}: store filter for {segment_id}: {error}"))?;
            rebuilt += 1;
        }
        rebuilt_total += rebuilt;
        reports.push(json!({
            "column": name,
            "segments_without_filter_before": missing_before,
            "rebuilt": rebuilt,
            "cannot_carry_a_filter": unbuildable,
            "failed": failed,
            "remaining": count_without_filter(oid, column_id)?,
        }));
    }
    Ok(json!({"rebuilt": rebuilt_total, "columns": reports}))
}

fn rebuild_summaries_table(
    oid: pgrx::pg_sys::Oid,
    dry_run: bool,
    max_segments: i32,
) -> Result<Value, String> {
    let target = table_target(oid)?;
    let table = format!("{}.{}", target.namespace, target.name);
    let Some((column_id, key_columns)) = summarizable_key(oid)? else {
        return Ok(json!({
            "table": table, "eligible": false,
            "reason": "every primary-key column must be smallint, integer, bigint, uuid, date, \
                       timestamp or timestamptz",
        }));
    };
    let column = key_columns
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let missing_before = count_missing(oid, column_id)?;
    if dry_run {
        return Ok(json!({
            "table": table, "eligible": true, "column": column,
            "segments_without_summary": missing_before,
        }));
    }

    let client = crate::object_store::open_managed_object_store_client(
        &target.storage.storage_type,
        &target.storage.base_path,
        &target.storage.credentials,
        &target.storage.config,
    )
    .map_err(|error| format!("{table}: open storage: {error}"))?;

    let mut outcome = Outcome::default();
    for segment in missing_summary_segments(oid, column_id, max_segments)? {
        let segment_id = segment["segment_id"].as_str().unwrap_or_default().to_string();
        let fail = |outcome: &mut Outcome, why: String| {
            outcome
                .failed
                .push(json!({"segment_id": segment_id, "problem": why}));
        };
        let rows = segment["row_count"].as_i64().unwrap_or(0);
        if usize::try_from(rows).unwrap_or(usize::MAX) > koldstore_sortkey::SUMMARY_MAX_ROWS {
            outcome.too_large += 1;
            continue;
        }
        let key = koldstore_common::join_object_key(
            &target.prefix,
            segment["path"].as_str().unwrap_or_default(),
        );
        let bytes = match client.get(&key) {
            Ok(bytes) => bytes,
            Err(error) => {
                fail(&mut outcome, format!("read {key}: {error}"));
                continue;
            }
        };
        // Only trust an object that is exactly what the catalog recorded.
        let expected = segment["checksum"].as_str().unwrap_or_default();
        let actual = koldstore_storage::content_checksum_sha256_hex(&bytes);
        if !expected.eq_ignore_ascii_case(&actual) {
            fail(&mut outcome, format!("checksum: catalog says {expected}, object hashes to {actual}"));
            continue;
        }
        let key_refs: Vec<(&str, koldstore_sortkey::SortKeyType)> = key_columns
            .iter()
            .map(|(name, ty)| (name.as_str(), *ty))
            .collect();
        let rebuilt = match koldstore_parquet::rebuild_value_summary_from_parquet(bytes, &key_refs) {
            Ok(rebuilt) => rebuilt,
            Err(error) => {
                fail(&mut outcome, error);
                continue;
            }
        };
        if i64::try_from(rebuilt.rows).ok() != Some(rows) {
            fail(
                &mut outcome,
                format!("object has {} rows, catalog says {rows}", rebuilt.rows),
            );
            continue;
        }
        let Some(summary) = rebuilt.summary else {
            outcome.too_large += 1;
            continue;
        };
        // `value_summary IS NULL` keeps this idempotent and harmless against a concurrent flush or
        // another rebuild: the bitmap is a pure function of the (immutable) object.
        pgrx::Spi::run_with_args(
            "UPDATE koldstore.cold_segment_index SET value_summary = $1 \
             WHERE segment_id = $2::uuid AND column_id = $3::smallint AND value_summary IS NULL",
            &[
                DatumWithOid::from(summary),
                DatumWithOid::from(segment_id.as_str()),
                DatumWithOid::from(column_id),
            ],
        )
        .map_err(|error| format!("{table}: store summary for {segment_id}: {error}"))?;
        outcome.rebuilt += 1;
    }

    Ok(json!({
        "table": table, "eligible": true, "column": column,
        "segments_without_summary_before": missing_before,
        "rebuilt": outcome.rebuilt,
        "too_large_for_a_summary": outcome.too_large,
        "failed": outcome.failed,
        "remaining": count_missing(oid, column_id)?,
    }))
}

/// The composite key of `table_oid` when exact tuple bounds can exist for it: the stable id of its
/// first column (whose index row carries them) and every key column in key order with its Sort Key
/// type. `None` for a single-column key (its column bounds are already exact) or when a key column
/// has no bounds encoding.
fn tuple_bounds_key(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Option<(i16, Vec<(String, koldstore_sortkey::SortKeyType)>)>, String> {
    let Some(snapshot) =
        crate::catalog::cache::managed_table_snapshot(table_oid).map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    if snapshot.primary_key_columns.len() < 2 {
        return Ok(None);
    }
    let mut columns = Vec::with_capacity(snapshot.primary_key_columns.len());
    for key in &snapshot.primary_key_columns {
        let type_oid = pgrx::Spi::get_one_with_args::<pgrx::pg_sys::Oid>(
            "SELECT atttypid FROM pg_catalog.pg_attribute \
             WHERE attrelid = $1 AND attnum = $2::smallint AND NOT attisdropped",
            &[
                DatumWithOid::from(table_oid),
                DatumWithOid::from(key.column_id.get()),
            ],
        )
        .map_err(|error| error.to_string())?;
        let Some(type_oid) = type_oid else {
            return Ok(None);
        };
        let Some(ty) = koldstore_sortkey::SortKeyType::from_type_oid_for_bounds(type_oid.to_u32())
            .filter(|ty| koldstore_parquet::supports_key_bounds(*ty))
        else {
            return Ok(None);
        };
        columns.push((key.name.clone(), ty));
    }
    let first = snapshot.primary_key_columns[0].column_id.get();
    Ok(Some((first, columns)))
}

/// Gives segments of a composite-key table that were written before tuple bounds existed the
/// `key_min` / `key_max` a flush would have stored, so the scan's disjointness check can be exact.
/// `Value::Null` when the table has no composite key to bound.
fn rebuild_key_bounds_table(
    oid: pgrx::pg_sys::Oid,
    dry_run: bool,
    max_segments: i32,
) -> Result<Value, String> {
    let Some((column_id, key_columns)) = tuple_bounds_key(oid)? else {
        return Ok(Value::Null);
    };
    let target = table_target(oid)?;
    let table = format!("{}.{}", target.namespace, target.name);
    let count = |oid: pgrx::pg_sys::Oid| -> Result<i64, String> {
        pgrx::Spi::get_one_with_args::<i64>(
            "SELECT count(*) FROM koldstore.cold_segments cs \
             JOIN koldstore.cold_segment_index csi ON csi.segment_id = cs.segment_id \
             WHERE cs.table_oid = $1 AND cs.status = 'active' \
               AND csi.column_id = $2::smallint AND csi.key_min IS NULL",
            &[DatumWithOid::from(oid), DatumWithOid::from(column_id)],
        )
        .map_err(|error| error.to_string())
        .map(Option::unwrap_or_default)
    };
    let missing_before = count(oid)?;
    if dry_run {
        return Ok(json!({ "segments_without_key_bounds": missing_before }));
    }
    let segments = json_query(
        "SELECT COALESCE(jsonb_agg(s ORDER BY s_min_seq, s_id), '[]'::jsonb)::text FROM (
           SELECT jsonb_build_object('segment_id', cs.segment_id, 'path', cs.path,
                                     'row_count', cs.row_count, 'checksum', cs.checksum) AS s,
                  cs.min_seq AS s_min_seq, cs.segment_id AS s_id
           FROM koldstore.cold_segments cs
           JOIN koldstore.cold_segment_index csi ON csi.segment_id = cs.segment_id
           WHERE cs.table_oid = $1 AND cs.status = 'active'
             AND csi.column_id = $2::smallint AND csi.key_min IS NULL
           ORDER BY cs.min_seq, cs.segment_id LIMIT $3::integer
         ) q",
        &[
            DatumWithOid::from(oid),
            DatumWithOid::from(column_id),
            DatumWithOid::from(max_segments),
        ],
    )?;
    let client = crate::object_store::open_managed_object_store_client(
        &target.storage.storage_type,
        &target.storage.base_path,
        &target.storage.credentials,
        &target.storage.config,
    )
    .map_err(|error| format!("{table}: open storage: {error}"))?;
    let key_refs: Vec<(&str, koldstore_sortkey::SortKeyType)> = key_columns
        .iter()
        .map(|(name, ty)| (name.as_str(), *ty))
        .collect();
    let (mut rebuilt, mut failed) = (0_i64, Vec::new());
    for segment in segments.as_array().cloned().unwrap_or_default() {
        let segment_id = segment["segment_id"].as_str().unwrap_or_default().to_string();
        let key = koldstore_common::join_object_key(
            &target.prefix,
            segment["path"].as_str().unwrap_or_default(),
        );
        let bytes = match client.get(&key) {
            Ok(bytes) => bytes,
            Err(error) => {
                failed.push(json!({"segment_id": segment_id, "problem": format!("read {key}: {error}")}));
                continue;
            }
        };
        let expected = segment["checksum"].as_str().unwrap_or_default();
        let actual = koldstore_storage::content_checksum_sha256_hex(&bytes);
        if !expected.eq_ignore_ascii_case(&actual) {
            failed.push(json!({"segment_id": segment_id, "problem": format!("checksum: catalog says {expected}, object hashes to {actual}")}));
            continue;
        }
        let bounds = match koldstore_parquet::rebuild_key_bounds_from_parquet(bytes, &key_refs) {
            Ok(Some(bounds)) => bounds,
            Ok(None) => {
                failed.push(json!({"segment_id": segment_id, "problem": "no key bounds could be derived"}));
                continue;
            }
            Err(error) => {
                failed.push(json!({"segment_id": segment_id, "problem": error}));
                continue;
            }
        };
        pgrx::Spi::run_with_args(
            "UPDATE koldstore.cold_segment_index SET key_min = $1, key_max = $2 \
             WHERE segment_id = $3::uuid AND column_id = $4::smallint AND key_min IS NULL",
            &[
                DatumWithOid::from(bounds.0),
                DatumWithOid::from(bounds.1),
                DatumWithOid::from(segment_id.as_str()),
                DatumWithOid::from(column_id),
            ],
        )
        .map_err(|error| format!("{table}: store key bounds for {segment_id}: {error}"))?;
        rebuilt += 1;
    }
    Ok(json!({
        "segments_without_key_bounds_before": missing_before,
        "rebuilt": rebuilt,
        "failed": failed,
        "remaining": count(oid)?,
    }))
}

fn rebuild_value(
    only: Option<pgrx::pg_sys::Oid>,
    dry_run: bool,
    max_segments: i32,
) -> Result<Value, String> {
    let mut tables = Vec::new();
    let mut rebuilt = 0_i64;
    for oid in managed_table_oids(only)? {
        let report = rebuild_table(oid, dry_run, max_segments)?;
        rebuilt += report["rebuilt"].as_i64().unwrap_or(0);
        tables.push(report);
    }
    Ok(json!({
        "dry_run": dry_run,
        "max_segments_per_table": max_segments,
        "rebuilt": rebuilt,
        "tables": tables,
    }))
}

/// Gives segments flushed before value summaries existed their summary.
///
/// SQL contract: `koldstore.rebuild_segment_summaries(table_name regclass default null,
/// dry_run boolean default false, max_segments integer default 100) → jsonb` of
/// `{dry_run, max_segments_per_table, rebuilt, tables[]}`; each table reports the segments still
/// without a summary (`remaining`), those too large to carry one, and any it could not verify
/// (`failed[]`: a missing or modified object, a row count that disagrees with the catalog, a NULL
/// key). Call it repeatedly until `remaining` is 0 (or only too-large segments are left). It reads
/// each such object once, so run it off-peak on large tables; `dry_run` only counts. Cold data is
/// never modified.
#[pgrx::pg_extern(name = "rebuild_segment_summaries", schema = "koldstore", security_definer)]
pub fn rebuild_segment_summaries_pg(
    table_name: pgrx::default!(Option<pgrx::PgRelation>, "NULL"),
    dry_run: pgrx::default!(bool, false),
    max_segments: pgrx::default!(i32, 100),
) -> pgrx::JsonB {
    let table = table_name.as_ref().map(pgrx::PgRelation::oid);
    authorize(table, "rebuild segment summaries");
    let max_segments = if max_segments <= 0 { DEFAULT_MAX_SEGMENTS } else { max_segments };
    rebuild_value(table, dry_run, max_segments)
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("rebuild segment summaries failed: {error}"))
}
