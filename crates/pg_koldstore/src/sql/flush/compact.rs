//! Segment compaction for `koldstore.compact_table`.
//!
//! Flushes append segments; updates and re-inserts leave older versions of a key in older segments,
//! deletes leave markers, and many small flushes leave many small objects. Compaction rewrites the
//! affected segments so each key has one version in one segment again, which lets scans skip
//! retaining cold identities (`cold_scan_disjoint_segments`) and keeps the object count bounded.
//!
//! A pass lists the active segments with their primary-key bounds, plans units
//! ([`koldstore_flush::plan_compaction_units`]) and rewrites each unit through the flush encoder:
//! newest version of every key wins (its original `seq` and operation code are kept), delete markers
//! are dropped, rows are sorted by key and re-split at `max_rows_per_file`. Publishing is one
//! catalog statement that activates the new segments and retires the old ones under the manifest
//! generation CAS, so readers see either the old segments or the new ones. Retired segments keep
//! their object for a grace period (readers planned on the previous generation may still be
//! reading them) and are reaped afterwards.
//!
//! The cold history of a key collapses to its newest version, so `changes_since` over cold data no
//! longer reports the overwritten versions. Scoped (per-user) tables and composite keys are not
//! handled yet.
//!
//! Output rows are sorted like a flush sorts them: by the segment-order column when the table has
//! one, then by key. When that column is not the key, rewritten segments do not become key-disjoint
//! (they are ordered by the other column), so a unit is only published when it removes superseded
//! rows or merges segments; otherwise every call would rewrite the same layout again.

use std::collections::{BTreeMap, HashMap};

use koldstore_common::SeqId;
use koldstore_flush::compaction::{plan_compaction_units, SegmentRange};
use koldstore_flush::plan_activate_compaction;
use koldstore_manifest::write_manifest_with_client;
use koldstore_merge::scan::plan::SegmentStatsHint;
use koldstore_parquet::{CellValue, FlushMirrorRow};
use koldstore_storage::{
    manifest_object_key, render_regular_table_prefix, PathTemplate, StorageClient,
};
use pgrx::datum::DatumWithOid;
use serde_json::{json, Value};

use super::execute::{
    apply_flush_row_count_deltas, build_stream_encode_input, load_flush_prepared_context,
    write_streamed_chunk, FlushCommitStyle, FlushPreparedContext, PendingFlushSegmentRef,
};
use super::spi::{
    manifest_from_publishable_cold_segments_excluding, manifest_generation,
    next_flush_batch_number,
};

struct ListedSegment {
    segment_id: uuid::Uuid,
    hint: SegmentStatsHint,
    row_count: i64,
    bounds: Option<(Vec<u8>, Vec<u8>)>,
}

/// Compacts `table_oid`'s cold segments; returns a JSON report.
pub(crate) fn compact_table_value(
    table_oid: pgrx::pg_sys::Oid,
    max_rows: i64,
    grace_seconds: i32,
) -> Result<Value, String> {
    if max_rows < 1 {
        return Err("max_rows must be at least 1".to_string());
    }
    let _lock = crate::sql::job_lock::TableJobLockGuard::try_lock(table_oid)?.ok_or_else(|| {
        "a flush or compaction is already running for this table; retry later".to_string()
    })?;
    let ctx = load_flush_prepared_context(
        table_oid,
        false,
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
    )?;
    let client = crate::object_store::open_managed_object_store_client(
        &ctx.storage.storage_type,
        &ctx.storage.base_path,
        &ctx.storage.credentials,
        &ctx.storage.config,
    )
    .map_err(|error| error.to_string())?;

    let reaped = reap_compacted_segments(table_oid, &client, grace_seconds)?;
    let catalog = &ctx.catalog;
    if catalog.primary_key.columns.len() != 1 {
        return Err("compaction supports single-column primary keys only".to_string());
    }
    let pk = &catalog.primary_key.columns[0];
    let pk_column = catalog
        .column_by_id(pk.column_id)
        .ok_or_else(|| "primary-key column is missing from the catalog".to_string())?;
    if koldstore_sortkey::SortKeyType::from_type_oid_for_bounds(pk_column.pg_type.type_oid())
        .is_none()
    {
        return Err(format!(
            "compaction needs Sort Key bounds for the primary key, which type `{}` has none",
            pk_column.catalog_type_name
        ));
    }

    let (segments, scoped_segments) = list_active_segments(table_oid, &ctx)?;
    if scoped_segments > 0 {
        return Err("compaction of user-scoped tables is not supported yet".to_string());
    }
    let segments_before = segments.len();
    // Delete markers may only be dropped when every segment's key range is known: a segment
    // without bounds could hold an older version of any key a marker still has to mask.
    let all_bounded = segments.iter().all(|segment| segment.bounds.is_some());
    if !all_bounded {
        return Ok(json!({
            "compacted": false,
            "reason": "some active segments have no primary-key bounds; \
                       run koldstore.rebuild_segment_summaries first",
            "segments": segments_before,
            "reaped_segments": reaped,
        }));
    }
    let ranges: Vec<SegmentRange> = segments
        .iter()
        .filter_map(|segment| {
            segment.bounds.as_ref().map(|(min, max)| SegmentRange {
                row_count: segment.row_count,
                min: min.clone(),
                max: max.clone(),
            })
        })
        .collect();
    let plan = plan_compaction_units(
        &ranges,
        i64::try_from(ctx.max_rows_per_file).unwrap_or(i64::MAX),
        max_rows,
    );

    let started = std::time::Instant::now();
    let mut budget = max_rows;
    let (mut units_done, mut units_left, mut units_without_gain) = (0_usize, 0_usize, 0_usize);
    let (mut segments_removed, mut segments_added) = (0_usize, 0_usize);
    let (mut rows_read, mut rows_written, mut dropped_versions) = (0_i64, 0_i64, 0_i64);
    let (mut bytes_removed, mut bytes_added) = (0_i64, 0_i64);
    for unit in &plan.units {
        if unit.rows > budget {
            units_left += 1;
            continue;
        }
        budget -= unit.rows;
        let members: Vec<&ListedSegment> =
            unit.members.iter().map(|index| &segments[*index]).collect();
        let Some(outcome) = compact_unit(table_oid, &ctx, &client, &members, started)? else {
            units_without_gain += 1;
            continue;
        };
        units_done += 1;
        segments_removed += members.len();
        segments_added += outcome.segments_written;
        rows_read += unit.rows;
        rows_written += outcome.rows_written;
        dropped_versions += unit.rows - outcome.rows_written;
        bytes_removed += members
            .iter()
            .map(|segment| {
                segment
                    .hint
                    .byte_size
                    .and_then(|size| i64::try_from(size).ok())
                    .unwrap_or(0)
            })
            .sum::<i64>();
        bytes_added += outcome.bytes_written;
    }
    Ok(json!({
        "compacted": units_done > 0,
        "units": units_done,
        "units_left_for_next_call": units_left,
        "units_over_max_rows": plan.skipped_over_cap,
        "units_without_gain": units_without_gain,
        "segments_before": segments_before,
        "segments_replaced": segments_removed,
        "segments_written": segments_added,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "versions_and_markers_dropped": dropped_versions,
        "bytes_replaced": bytes_removed,
        "bytes_written": bytes_added,
        "reaped_segments": reaped,
        "grace_seconds": grace_seconds,
    }))
}

struct UnitOutcome {
    segments_written: usize,
    rows_written: i64,
    bytes_written: i64,
}

fn compact_unit(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
    client: &koldstore_storage::ObjectStoreClient,
    members: &[&ListedSegment],
    started: std::time::Instant,
) -> Result<Option<UnitOutcome>, String> {
    let catalog = &ctx.catalog;
    let hints: Vec<SegmentStatsHint> = members.iter().map(|segment| segment.hint.clone()).collect();
    let rows = crate::merge_scan::pg::read_clean_rows_of_segments(
        client,
        &hints,
        catalog,
        ctx.storage.schema_version,
    )?;
    let rows_read: i64 = i64::try_from(rows.len()).unwrap_or(i64::MAX);

    // Newest version of each key wins; a winning delete marker removes the key altogether.
    let mut winners: HashMap<String, koldstore_parquet::CleanColdRow> =
        HashMap::with_capacity(rows.len());
    for row in rows {
        let key = row.pk_json.to_string();
        match winners.get(&key) {
            Some(existing) if existing.seq >= row.seq => {}
            _ => {
                winners.insert(key, row);
            }
        }
    }
    let pk_name = &catalog.primary_key.columns[0].name;
    let order_name: Option<&String> = ctx
        .snapshot
        .segment_order_column_id
        .and_then(|column_id| catalog.column_by_id(column_id))
        .map(|column| &column.name);
    let mut kept: Vec<koldstore_parquet::CleanColdRow> = winners
        .into_values()
        .filter(|row| !row.deleted)
        .collect();
    let null = CellValue::Null;
    let cell = |row: &koldstore_parquet::CleanColdRow, name: &String| -> CellValue {
        row.row_image.get(name).cloned().unwrap_or_else(|| null.clone())
    };
    kept.sort_by(|left, right| {
        let by_order = order_name.map_or(std::cmp::Ordering::Equal, |name| {
            cmp_cells(&cell(left, name), &cell(right, name))
        });
        by_order
            .then_with(|| cmp_cells(&cell(left, pk_name), &cell(right, pk_name)))
            .then(left.seq.cmp(&right.seq))
    });
    let key_ordered = order_name.is_none_or(|name| name == pk_name);
    if !key_ordered {
        let target = ctx.max_rows_per_file.max(1);
        let output_segments = kept.len().div_ceil(target);
        if kept.len() as i64 == rows_read && output_segments >= members.len() {
            return Ok(None);
        }
    }
    let sorted_by_key = ctx.snapshot.segment_order_column_id.is_some();
    let mirror_rows: Vec<FlushMirrorRow> = kept
        .into_iter()
        .map(|row| FlushMirrorRow {
            seq: row.seq,
            op: row.op,
            values: catalog
                .columns
                .iter()
                .map(|column| row.row_image.get(&column.name).cloned().unwrap_or(CellValue::Null))
                .collect(),
            // The encoder only checks the key is present (rows arrive sorted); paging below does
            // not use it.
            order_key: sorted_by_key.then(Vec::new),
        })
        .collect();
    let max_seq = mirror_rows.iter().map(|row| row.seq).max().unwrap_or(0);

    let table_prefix = render_regular_table_prefix(
        &PathTemplate::new(&ctx.storage.regular_path_tmpl),
        &ctx.relation.namespace,
        &ctx.relation.name,
    )?;
    let manifest_path = manifest_object_key(&table_prefix);
    let (encode_input, _) = build_stream_encode_input(ctx, max_seq, None)?;
    let fetch_batch_size = usize::try_from(encode_input.fetch_batch_size)
        .unwrap_or(1)
        .max(1);
    let mut batch_number = next_flush_batch_number(table_oid)?;
    let pass_id = uuid::Uuid::new_v4();
    let (mut total_rows, mut last_max_seq) = (0_i64, 0_i64);
    let mut pending: Vec<PendingFlushSegmentRef> = Vec::new();
    let mut next = 0_usize;
    koldstore_flush::stream_flush_chunks(
        &encode_input,
        |_statement, _max_seq, _cursor| {
            let end = (next + fetch_batch_size).min(mirror_rows.len());
            let page = mirror_rows[next..end].to_vec();
            next = end;
            Ok(page)
        },
        |chunk| {
            write_streamed_chunk(
                client,
                ctx,
                pass_id,
                &table_prefix,
                table_oid,
                FlushCommitStyle::Nested,
                &mut batch_number,
                &mut total_rows,
                &mut last_max_seq,
                &mut pending,
                chunk,
                started,
            )
        },
    )?;
    drop(mirror_rows);

    let source_ids: Vec<uuid::Uuid> = members.iter().map(|segment| segment.segment_id).collect();
    let new_ids: Vec<uuid::Uuid> = pending.iter().map(|segment| segment.segment_id).collect();
    let bytes_written = pending
        .iter()
        .map(|segment| segment.byte_size)
        .fold(0_i64, i64::saturating_add);

    // The manifest object describes the state activation produces; like flush, it is written
    // first and is rebuildable from the catalog if the transaction does not commit.
    let manifest = manifest_from_publishable_cold_segments_excluding(
        table_oid,
        &ctx.relation,
        &ctx.snapshot,
        ctx.storage.schema_version,
        &source_ids,
    )?;
    write_manifest_with_client(client, &manifest_path, &manifest)?;

    let expected_generation = manifest_generation(table_oid)?;
    let new_generation = expected_generation
        .checked_add(1)
        .ok_or_else(|| "manifest generation overflow".to_string())?;
    let to_pg = |ids: &[uuid::Uuid]| -> Vec<pgrx::Uuid> {
        ids.iter().copied().map(crate::spi::uuid_to_pgrx).collect()
    };
    let statement = plan_activate_compaction().map_err(|error| error.to_string())?;
    let activated = crate::spi::update_one::<i64>(
        &statement,
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(expected_generation),
            DatumWithOid::from(new_generation),
            DatumWithOid::from(i32::try_from(manifest.segments.len()).unwrap_or(i32::MAX)),
            DatumWithOid::from(manifest.max_seq),
            DatumWithOid::from(to_pg(&new_ids)),
            DatumWithOid::from(to_pg(&source_ids)),
        ],
    )
    .map_err(|error| error.to_string())?;
    if activated.is_none() {
        return Err(format!(
            "compaction lost the manifest generation race or the catalog changed under it \
             (expected generation {expected_generation}); nothing was published, retry"
        ));
    }
    let source_rows: i64 = members.iter().map(|segment| segment.row_count).sum();
    apply_flush_row_count_deltas(table_oid, 0, 0, total_rows - source_rows)?;
    crate::catalog::cache::invalidate_table_globally(table_oid);
    pgrx::log!(
        "koldstore compaction: replaced {} segments ({rows_read} rows) with {} segments ({total_rows} rows)",
        source_ids.len(),
        new_ids.len()
    );
    Ok(Some(UnitOutcome {
        segments_written: new_ids.len(),
        rows_written: total_rows,
        bytes_written,
    }))
}

/// Orders key cells the way their Sort Key V1 encodings order. Only the types that have a bounds
/// encoding reach this: integers, dates and timestamps (as integers), uuids and text (as strings).
fn cmp_cells(left: &CellValue, right: &CellValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left.as_i64(), right.as_i64()) {
        (Some(left), Some(right)) => return left.cmp(&right),
        (Some(_), None) => return Ordering::Less,
        (None, Some(_)) => return Ordering::Greater,
        (None, None) => {}
    }
    match (left, right) {
        (CellValue::Utf8(left), CellValue::Utf8(right)) => left.as_bytes().cmp(right.as_bytes()),
        (CellValue::Float64(left), CellValue::Float64(right)) => left.total_cmp(right),
        (CellValue::Float32(left), CellValue::Float32(right)) => left.total_cmp(right),
        _ => Ordering::Equal,
    }
}

/// Lists the active shared-scope segments (key order) and how many active scoped segments exist.
fn list_active_segments(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
) -> Result<(Vec<ListedSegment>, i64), String> {
    let pk = &ctx.catalog.primary_key.columns[0];
    let column = ctx
        .catalog
        .column_by_id(pk.column_id)
        .ok_or_else(|| "primary-key column is missing from the catalog".to_string())?;
    let statement = koldstore_catalog::queries::plan_compaction_segment_listing()
        .map_err(|error| error.to_string())?;
    let args = [
        DatumWithOid::from(table_oid),
        DatumWithOid::from(i32::from(pk.column_id.get())),
        DatumWithOid::from(pgrx::pg_sys::Oid::from(column.pg_type.type_oid())),
        DatumWithOid::from(i32::from(koldstore_sortkey::CODEC_VERSION)),
    ];
    let current_version = ctx.storage.schema_version;
    crate::catalog::owner::with_extension_owner(|| {
        crate::spi::execute_prepared(&statement, &args, |tuples| {
            let mut out = Vec::new();
            let mut scoped = 0_i64;
            for tuple in tuples {
                let object_path = tuple.get::<String>(1)?.unwrap_or_default();
                let byte_size = tuple
                    .get::<i64>(2)?
                    .and_then(|value| u64::try_from(value).ok());
                let schema_version = tuple.get::<i32>(3)?.unwrap_or(current_version);
                let min_seq = tuple.get::<i64>(4)?.unwrap_or(1);
                let max_seq = tuple.get::<i64>(5)?.unwrap_or(min_seq);
                let physical_names: BTreeMap<i16, String> = tuple
                    .get::<String>(6)?
                    .map(|json| serde_json::from_str(&json).unwrap_or_default())
                    .unwrap_or_default();
                let segment_id = tuple
                    .get::<pgrx::Uuid>(7)?
                    .map(crate::spi::uuid_from_pgrx)
                    .unwrap_or_default();
                let row_count = tuple.get::<i64>(8)?.unwrap_or(0);
                let min = tuple.get::<Vec<u8>>(9)?;
                let max = tuple.get::<Vec<u8>>(10)?;
                scoped = tuple.get::<i64>(11)?.unwrap_or(0);
                let to_seq = |raw: i64| {
                    SeqId::new(raw).map_err(|error| {
                        pgrx::spi::SpiError::DatumError(
                            pgrx::datum::TryFromDatumError::NoSuchAttributeName(format!(
                                "invalid seq {raw}: {error}"
                            )),
                        )
                    })
                };
                out.push(ListedSegment {
                    segment_id,
                    hint: SegmentStatsHint {
                        object_path,
                        schema_version,
                        physical_names,
                        byte_size,
                        min_seq: to_seq(min_seq)?,
                        max_seq: to_seq(max_seq)?,
                        selected_row_groups: None,
                    },
                    row_count,
                    bounds: min.zip(max),
                });
            }
            Ok((out, scoped))
        })
        .map_err(|error| error.to_string())
    })?
}

/// Deletes the objects of compacted segments retired at least `grace_seconds` ago (or the cold
/// object retention window, whichever is longer) and drops their catalog rows. Returns how many
/// segments were reaped.
pub(crate) fn reap_compacted_segments(
    table_oid: pgrx::pg_sys::Oid,
    client: &koldstore_storage::ObjectStoreClient,
    grace_seconds: i32,
) -> Result<i64, String> {
    // A retired object is still referenced by every backup taken before the compaction, so the
    // operator's retention window (`koldstore.cold_object_retention_seconds`, which exists to keep
    // such backups restorable) is a floor for the grace.
    let grace_seconds = grace_seconds.max(crate::guc::cold_object_retention_seconds());
    let statement = koldstore_catalog::queries::plan_compacted_segments_past_grace()
        .map_err(|error| error.to_string())?;
    let args = [
        DatumWithOid::from(table_oid),
        DatumWithOid::from(f64::from(grace_seconds.max(0))),
    ];
    let due: Vec<(pgrx::Uuid, String)> = crate::catalog::owner::with_extension_owner(|| {
        crate::spi::execute_prepared(&statement, &args, |tuples| {
            let mut out = Vec::new();
            for tuple in tuples {
                if let (Some(id), Some(key)) = (tuple.get::<pgrx::Uuid>(1)?, tuple.get::<String>(2)?)
                {
                    out.push((id, key));
                }
            }
            Ok(out)
        })
        .map_err(|error| error.to_string())
    })??;
    let mut done: Vec<pgrx::Uuid> = Vec::with_capacity(due.len());
    for (segment_id, key) in due {
        // Deletes are idempotent; a missing object is fine, any other failure leaves the row for
        // the next pass.
        match client.delete(&key) {
            Ok(()) => done.push(segment_id),
            Err(error) => pgrx::warning!("koldstore compaction: could not delete {key}: {error}"),
        }
    }
    if done.is_empty() {
        return Ok(0);
    }
    let reaped = i64::try_from(done.len()).unwrap_or(i64::MAX);
    pgrx::Spi::run_with_args(
        "DELETE FROM koldstore.cold_segments WHERE segment_id = ANY($1) AND status = 'compacted'",
        &[DatumWithOid::from(done)],
    )
    .map_err(|error| error.to_string())?;
    Ok(reaped)
}

/// `koldstore.reap_compacted_segments` / maintenance entry: opens the table's storage and reaps.
pub(crate) fn reap_table(
    table_oid: pgrx::pg_sys::Oid,
    grace_seconds: i32,
) -> Result<i64, String> {
    let storage = crate::catalog::resolve::active_flush_storage_context(table_oid)?;
    let client = crate::object_store::open_managed_object_store_client(
        &storage.storage_type,
        &storage.base_path,
        &storage.credentials,
        &storage.config,
    )
    .map_err(|error| error.to_string())?;
    reap_compacted_segments(table_oid, &client, grace_seconds)
}

/// Grace the maintenance worker applies before deleting a retired segment's object.
const MAINTENANCE_GRACE_SECONDS: i32 = 600;

/// One bounded maintenance pass: reaps compacted segments past [`MAINTENANCE_GRACE_SECONDS`] of a
/// few tables. `Ok(None)` when nothing is due.
pub(crate) fn reap_tick() -> Result<Option<Value>, String> {
    let grace = MAINTENANCE_GRACE_SECONDS.max(crate::guc::cold_object_retention_seconds());
    let tables: Vec<pgrx::pg_sys::Oid> = pgrx::Spi::connect(|client| {
        client
            .select(
                "SELECT DISTINCT table_oid FROM koldstore.cold_segments \
                 WHERE status = 'compacted' \
                   AND created_at <= now() - make_interval(secs => $1::double precision) \
                 LIMIT 5",
                None,
                &[DatumWithOid::from(f64::from(grace))],
            )?
            .map(|row| {
                Ok(row
                    .get::<pgrx::pg_sys::Oid>(1)?
                    .unwrap_or(pgrx::pg_sys::InvalidOid))
            })
            .collect::<Result<Vec<_>, pgrx::spi::Error>>()
    })
    .map_err(|error| error.to_string())?;
    if tables.is_empty() {
        return Ok(None);
    }
    let mut reaped = 0_i64;
    for table_oid in tables {
        reaped += reap_table(table_oid, grace)?;
    }
    Ok(Some(json!({ "reaped_segments": reaped })))
}
