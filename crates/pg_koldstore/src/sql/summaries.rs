//! `koldstore.rebuild_segment_summaries`: gives segments written before value summaries existed the
//! summary flush would have written, so point lookups stop opening every one of them.
//!
//! A cold point lookup (`WHERE id = $1`) prunes by the catalog's min/max bounds and, for a primary key of
//! `smallint`/`integer`/`bigint`/`uuid`/`date`/`timestamp`/`timestamptz` columns (one or several), by a
//! per-segment membership bitmap (ADR-009).
//! Segments flushed before that column existed have a NULL summary, which always means "cannot prune":
//! correct, but every such segment whose bounds overlap the key is opened. This reads each of them
//! once, verifies it against the catalog, and stores the bitmap. It never changes cold data, only the
//! catalog's `cold_segment_index.value_summary`.

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
