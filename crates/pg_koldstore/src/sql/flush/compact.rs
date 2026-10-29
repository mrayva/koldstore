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
//! are dropped (kept while an older version of the key may live in a segment outside the unit's group), rows are sorted by key and re-split at `max_rows_per_file`. Publishing is one
//! catalog statement that activates the new segments and retires the old ones under the manifest
//! generation CAS, so readers see either the old segments or the new ones. Retired segments keep
//! their object for a grace period (readers planned on the previous generation may still be
//! reading them) and are reaped afterwards.
//!
//! The cold history of a key collapses to its newest version, so `changes_since` over cold data can no
//! longer replay the overwritten versions. The highest dropped `seq` is stored as the manifest's
//! `history_floor_seq` in the activation statement, and `changes_since` refuses older cursors. Scoped (per-user) tables compact per scope group (ADR-010).
//!
//! Output rows are sorted like a flush sorts them: by the segment-order column when the table has
//! one, then by the key columns in key order. Two segments can share a composite key only if their
//! per-column bounds intersect on every key column (`koldstore_flush::compaction`). A single-column
//! key ordered by itself ends up key-disjoint; otherwise (a composite key, or a table ordered by
//! another column) the rewritten segments' boxes may still touch, so a unit is only published when
//! it removes superseded rows or merges segments, and the maintenance worker leaves overlap units of
//! such tables to explicit `compact_table` calls (they would be reread unchanged every tick).

use std::collections::{BTreeMap, HashMap};

use koldstore_common::SeqId;
use koldstore_flush::compaction::{plan_compaction_units, CompactionUnit, SegmentRange};
use koldstore_flush::plan_activate_compaction;
use koldstore_manifest::write_manifest_with_client;
use koldstore_merge::scan::plan::SegmentStatsHint;
use koldstore_parquet::{CellValue, FlushMirrorRow};
use koldstore_storage::{
    manifest_object_key, StorageClient,
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
    /// `(min, max)` per key column, in key order; `None` when any column lacks usable bounds.
    bounds: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    /// Lexicographic `(min, max)` of the whole composite key tuple, when the segment has it.
    tuple_bounds: Option<(Vec<u8>, Vec<u8>)>,
    /// Scope the segment's rows belong to (user-scoped tables); `None` = table-wide.
    scope_id: Option<String>,
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
    crate::sql::table_metadata::refuse_on_follower(table_oid, "compact")?;
    crate::sql::leader_lease::require_lease(table_oid, "compact")?;
    // A flush that has just finished may still be releasing the lock; wait a few seconds for it
    // rather than failing a flush-then-compact sequence.
    let _lock = crate::sql::job_lock::TableJobLockGuard::lock_bounded(
        table_oid,
        std::time::Duration::from_secs(10),
    )
    .map_err(|error| format!("a flush or compaction is still running for this table: {error}"))?;
    compact_locked(table_oid, max_rows, grace_seconds, false)
}

/// One bounded pass for the maintenance worker. `Ok(None)` when the table is busy (a flush or
/// another compaction holds its lock) or has nothing the automatic mode will touch.
fn auto_compact_table(
    table_oid: pgrx::pg_sys::Oid,
    max_rows: i64,
) -> Result<Option<Value>, String> {
    let Some(_lock) = crate::sql::job_lock::TableJobLockGuard::try_lock(table_oid)? else {
        return Ok(None);
    };
    let report = compact_locked(table_oid, max_rows, 0, true)?;
    Ok(report["compacted"].as_bool().unwrap_or(false).then_some(report))
}

/// The pass itself, with the table's job lock held. `auto` makes unsupported tables a quiet
/// no-op instead of an error, skips reaping (the reap tick does it), and restricts overlap
/// components to tables whose rewrite is stable (ordered by the key): for others every pass
/// would reread and rewrite the same layout.
fn compact_locked(
    table_oid: pgrx::pg_sys::Oid,
    max_rows: i64,
    grace_seconds: i32,
    auto: bool,
) -> Result<Value, String> {
    let refuse = |message: String| -> Result<Value, String> {
        if auto {
            Ok(json!({ "compacted": false, "reason": message }))
        } else {
            Err(message)
        }
    };
    let ctx = load_flush_prepared_context(
        table_oid,
        false,
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
    )?;
    let open_client = || {
        crate::object_store::open_managed_object_store_client(
            &ctx.storage.storage_type,
            &ctx.storage.base_path,
            &ctx.storage.credentials,
            &ctx.storage.config,
        )
        .map_err(|error| error.to_string())
    };
    let mut client = None;
    let reaped = if auto {
        0
    } else {
        let opened = open_client()?;
        let reaped = reap_compacted_segments(table_oid, &opened, grace_seconds)?;
        client = Some(opened);
        reaped
    };
    let catalog = &ctx.catalog;
    if catalog.primary_key.columns.is_empty() {
        return refuse("compaction needs a primary key".to_string());
    }
    for key in &catalog.primary_key.columns {
        let column = catalog
            .column_by_id(key.column_id)
            .ok_or_else(|| "primary-key column is missing from the catalog".to_string())?;
        if koldstore_sortkey::SortKeyType::from_type_oid_for_bounds(column.pg_type.type_oid())
            .is_none()
        {
            return refuse(format!(
                "compaction needs Sort Key bounds for every primary-key column, which type `{}` of `{}` has none",
                column.catalog_type_name, column.name
            ));
        }
    }
    let lead_key = catalog.primary_key.columns[0].column_id;

    let (segments, scoped_segments) = list_active_segments(table_oid, &ctx)?;
    if scoped_segments > 0 {
        return refuse("compaction of user-scoped tables is not supported yet".to_string());
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
    // User-scoped tables keep one segment group per scope (ADR-010): a unit never crosses scopes, its
    // output stays in the scope's folder, and the table-wide group (delete markers, rows from before
    // scoping) is left alone once scoped segments exist -- a marker there may be masking an older
    // version in a scoped group, and the per-group rewrite below could not tell.
    let table_has_scoped = segments.iter().any(|segment| segment.scope_id.is_some());
    let mut groups: std::collections::BTreeMap<Option<String>, Vec<&ListedSegment>> =
        std::collections::BTreeMap::new();
    for segment in &segments {
        groups.entry(segment.scope_id.clone()).or_default().push(segment);
    }
    // A delete marker follows its row into the row's scope group (the mirror records the scope),
    // where compaction can drop it once it has met the older versions it masks. That is only safe
    // while nothing outside the scope group could still hold such a version: older versions never
    // live in another scope (a key has one scope), but the table-wide group (rows from before
    // scoping, NULL scopes, markers whose scope was unknown) can. While that group is not empty,
    // scope groups keep their winning markers; the table-wide group itself always keeps them
    // once scoped segments exist, since a marker there may mask a version in any scope group.
    let table_wide_present = segments.iter().any(|segment| segment.scope_id.is_none());
    let drop_markers_in = |scope: &Option<String>| {
        !table_has_scoped || (scope.is_some() && !table_wide_present)
    };
    // A composite key whose segments all have exact tuple bounds is planned with both tests (see
    // `SegmentRange::may_share_keys`): a sorted rewrite makes the output lexicographically disjoint,
    // so a repeat finds nothing to do and the scan sees disjoint segments afterwards.
    let composite = catalog.primary_key.columns.len() > 1;
    // The last flag marks an oversized overlap component that is rewritten in slices (streamed).
    let mut planned: Vec<(CompactionUnit, Vec<&ListedSegment>, Option<String>, bool, bool, bool)> =
        Vec::new();
    let mut skipped_over_cap = 0_usize;
    for (scope, group) in groups {
        let drop_markers = drop_markers_in(&scope);
        // A kept marker carries only its key, so it can be placed only when the segment order is
        // the key itself; for any other order column its group is left as it was.
        if !drop_markers
            && !ctx
                .snapshot
                .segment_order_column_id
                .is_none_or(|column_id| column_id == lead_key)
        {
            continue;
        }
        let exact_tuples = composite && group.iter().all(|segment| segment.tuple_bounds.is_some());
        let ranges: Vec<SegmentRange> = group
            .iter()
            .filter_map(|segment| {
                segment.bounds.as_ref().map(|bounds| SegmentRange {
                    row_count: segment.row_count,
                    bounds: bounds.clone(),
                    tuple: if exact_tuples { segment.tuple_bounds.clone() } else { None },
                })
            })
            .collect();
        let mut plan = plan_compaction_units(
            &ranges,
            i64::try_from(ctx.max_rows_per_file).unwrap_or(i64::MAX),
            max_rows,
        );
        skipped_over_cap += plan.skipped_over_cap;
        // An explicit call streams components over the cap when the rewrite is key-ordered (slices
        // come out in key order, so the result stays key-disjoint); the maintenance worker leaves
        // them, since one would read the whole component in a tick.
        let streamable = (!composite || exact_tuples)
            && ctx
                .snapshot
                .segment_order_column_id
                .is_none_or(|column_id| column_id == lead_key);
        if !auto && streamable {
            for unit in std::mem::take(&mut plan.oversized) {
                let members: Vec<&ListedSegment> = unit.members.iter().map(|index| group[*index]).collect();
                skipped_over_cap -= 1;
                planned.push((unit, members, scope.clone(), exact_tuples, true, drop_markers));
            }
        }
        if auto {
            // Only a single-column key ordered by itself is rewritten into key-disjoint segments;
            // for anything else an overlap component would be reread and rewritten unchanged every
            // tick.
            let stable_rewrite = (!composite || exact_tuples)
                && ctx
                    .snapshot
                    .segment_order_column_id
                    .is_none_or(|column_id| column_id == lead_key);
            if !stable_rewrite {
                plan.units.retain(|unit| !unit.overlapping);
            }
        }
        for unit in plan.units {
            let members: Vec<&ListedSegment> = unit.members.iter().map(|index| group[*index]).collect();
            planned.push((unit, members, scope.clone(), exact_tuples, false, drop_markers));
        }
    }
    if auto && planned.is_empty() {
        return Ok(json!({ "compacted": false }));
    }
    let client = match client {
        Some(client) => client,
        None => open_client()?,
    };

    let started = std::time::Instant::now();
    let mut budget = max_rows;
    let (mut units_done, mut units_left, mut units_without_gain) = (0_usize, 0_usize, 0_usize);
    let (mut segments_removed, mut segments_added) = (0_usize, 0_usize);
    let (mut rows_read, mut rows_written, mut dropped_versions) = (0_i64, 0_i64, 0_i64);
    let (mut bytes_removed, mut bytes_added) = (0_i64, 0_i64);
    for (unit, members, scope, exact_tuples, streamed, drop_markers) in &planned {
        // A streamed unit holds at most `max_rows` rows at a time, whatever its size, but it reads
        // all of it: it runs only as the first unit of a call, which then has no budget left.
        if *streamed {
            if budget < max_rows {
                units_left += 1;
                continue;
            }
            budget = 0;
        } else {
            if unit.rows > budget {
                units_left += 1;
                continue;
            }
            budget -= unit.rows;
        }
        let Some(outcome) = compact_unit(
            table_oid,
            &ctx,
            &client,
            members,
            started,
            *exact_tuples,
            scope.as_deref(),
            *drop_markers,
            streamed.then_some(max_rows),
        )?
        else {
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
        "units_over_max_rows": skipped_over_cap,
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
        "held_by_registered_backups": held_by_backups(table_oid)?,
        "grace_seconds": grace_seconds,
    }))
}

struct UnitOutcome {
    segments_written: usize,
    rows_written: i64,
    bytes_written: i64,
}

/// Versions of a unit's rows after resolution: the newest version of each key, the older versions
/// and delete markers that move to history segments, and the highest `seq` dropped for good.
struct ResolvedRows {
    kept: Vec<koldstore_parquet::CleanColdRow>,
    history: Vec<koldstore_parquet::CleanColdRow>,
    history_floor: i64,
}

/// Newest version of each key wins; a winning delete marker removes the key altogether. A dropped
/// version or marker newer than `retain_from` is moved to history
/// (`koldstore.cdc_retention_seconds`); `history_floor` is the highest seq of one dropped for good
/// (`changes_since` can no longer report it, so it refuses cursors below it). With `drop_markers`
/// false a winning delete marker is kept in the output instead (still
/// masking whatever older version of its key lives outside this unit, see `compact_locked`) and
/// only the older versions it beats inside the unit are dropped.
fn resolve_versions(
    rows: Vec<koldstore_parquet::CleanColdRow>,
    drop_markers: bool,
    retain_from: i64,
) -> ResolvedRows {
    let mut history_floor = 0_i64;
    let mut history: Vec<koldstore_parquet::CleanColdRow> = Vec::new();
    let mut drop_version = |row: koldstore_parquet::CleanColdRow, floor: &mut i64| {
        if row.seq >= retain_from {
            history.push(row);
        } else {
            *floor = (*floor).max(row.seq);
        }
    };
    let mut winners: HashMap<String, koldstore_parquet::CleanColdRow> =
        HashMap::with_capacity(rows.len());
    for row in rows {
        let key = row.pk_json.to_string();
        match winners.remove(&key) {
            Some(existing) if existing.seq >= row.seq => {
                winners.insert(key, existing);
                drop_version(row, &mut history_floor);
            }
            Some(existing) => {
                winners.insert(key, row);
                drop_version(existing, &mut history_floor);
            }
            None => {
                winners.insert(key, row);
            }
        }
    }
    if !drop_markers {
        return ResolvedRows { kept: winners.into_values().collect(), history, history_floor };
    }
    let marker_keys: Vec<String> = winners
        .iter()
        .filter(|(_, row)| row.deleted)
        .map(|(key, _)| key.clone())
        .collect();
    for key in marker_keys {
        if let Some(marker) = winners.remove(&key) {
            drop_version(marker, &mut history_floor);
        }
    }
    ResolvedRows { kept: winners.into_values().collect(), history, history_floor }
}

/// Peak-memory sample of lead-key values used to choose slice boundaries of a streamed unit.
const STREAM_SAMPLE_TARGET: i64 = 65_536;

fn compact_unit(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
    client: &koldstore_storage::ObjectStoreClient,
    members: &[&ListedSegment],
    started: std::time::Instant,
    exact_tuples: bool,
    scope: Option<&str>,
    drop_markers: bool,
    stream_slice_rows: Option<i64>,
) -> Result<Option<UnitOutcome>, String> {
    let catalog = &ctx.catalog;
    let hints: Vec<SegmentStatsHint> = members.iter().map(|segment| segment.hint.clone()).collect();
    let read_members = |hints: &[SegmentStatsHint]| {
        crate::merge_scan::pg::read_clean_rows_of_segments(
            client,
            hints,
            catalog,
            ctx.storage.schema_version,
        )
    };
    let retain_from = history_horizon();
    let key_names: Vec<&String> = catalog
        .primary_key
        .columns
        .iter()
        .filter_map(|key| catalog.column_by_id(key.column_id))
        .map(|column| &column.name)
        .collect();
    let order_name: Option<&String> = ctx
        .snapshot
        .segment_order_column_id
        .and_then(|column_id| catalog.column_by_id(column_id))
        .map(|column| &column.name);
    let null = CellValue::Null;
    let cell = |row: &koldstore_parquet::CleanColdRow, name: &String| -> CellValue {
        row.row_image
            .get(name)
            .or_else(|| row.key_image.get(name))
            .cloned()
            .unwrap_or_else(|| null.clone())
    };
    // Same order a flush produces: the segment-order column, then the key columns in key order.
    let sort_rows = |rows: &mut Vec<koldstore_parquet::CleanColdRow>| {
        rows.sort_by(|left, right| {
            let by_order = order_name.map_or(std::cmp::Ordering::Equal, |name| {
                cmp_cells(&cell(left, name), &cell(right, name))
            });
            key_names
                .iter()
                .fold(by_order, |ordering, name| {
                    ordering.then_with(|| cmp_cells(&cell(left, name), &cell(right, name)))
                })
                .then(left.seq.cmp(&right.seq))
        });
    };
    // A single-column key ordered by itself always ends up key-disjoint, which is worth publishing
    // even when no row is dropped. Otherwise (composite key, or ordered by another column) the
    // rewritten segments' key boxes may still touch, so publish only when it removes rows or merges
    // segments; otherwise every call would rewrite the same layout again.
    let stable_rewrite = (key_names.len() == 1 || exact_tuples)
        && order_name.is_none_or(|name| name == key_names[0]);
    let sorted_by_key = ctx.snapshot.segment_order_column_id.is_some();
    let to_mirror_rows = |rows: Vec<koldstore_parquet::CleanColdRow>| -> Vec<FlushMirrorRow> {
        rows.into_iter()
            .map(|row| FlushMirrorRow {
                seq: row.seq,
                op: row.op,
                values: catalog
                    .columns
                    .iter()
                    .map(|column| {
                        row.row_image
                            .get(&column.name)
                            .or_else(|| row.key_image.get(&column.name))
                            .cloned()
                            .unwrap_or(CellValue::Null)
                    })
                    .collect(),
                // The encoder only checks the key is present (rows arrive sorted); paging below
                // does not use it.
                order_key: sorted_by_key.then(Vec::new),
                scope: None,
            })
            .collect()
    };

    let table_prefix = crate::catalog::resolve::table_object_prefix(&ctx.storage, &ctx.relation.namespace, &ctx.relation.name)?;
    let manifest_path = manifest_object_key(&table_prefix);
    let mut batch_number = next_flush_batch_number(table_oid)?;
    let pass_id = uuid::Uuid::new_v4();
    // Streams `rows` out as pending segments; returns them with the row count written.
    let mut write_rows = |rows: Vec<FlushMirrorRow>,
                          rows_max_seq: i64|
     -> Result<(Vec<PendingFlushSegmentRef>, i64), String> {
        let (mut encode_input, _) = build_stream_encode_input(ctx, rows_max_seq, None)?;
        // Compaction keeps every row of a unit in the unit's scope (set per chunk below).
        encode_input.scope_column_index = None;
        let fetch_batch_size = usize::try_from(encode_input.fetch_batch_size)
            .unwrap_or(1)
            .max(1);
        let (mut total_rows, mut last_max_seq) = (0_i64, 0_i64);
        let mut pending: Vec<PendingFlushSegmentRef> = Vec::new();
        let mut next = 0_usize;
        koldstore_flush::stream_flush_chunks(
            &encode_input,
            |_statement, _max_seq, _cursor| {
                let end = (next + fetch_batch_size).min(rows.len());
                let page = rows[next..end].to_vec();
                next = end;
                Ok(page)
            },
            |mut chunk| {
                chunk.scope = scope.map(str::to_string);
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
        Ok((pending, total_rows))
    };

    let mut pending: Vec<PendingFlushSegmentRef> = Vec::new();
    let mut history_pending: Vec<PendingFlushSegmentRef> = Vec::new();
    let (mut total_rows, mut rows_read, mut history_floor) = (0_i64, 0_i64, 0_i64);
    // Resolves, sorts and writes one batch of the unit's rows; false when the unit must be left as it is.
    let mut rewrite_batch = |rows: Vec<koldstore_parquet::CleanColdRow>,
                             check_gain: bool|
     -> Result<bool, String> {
        let batch_rows = i64::try_from(rows.len()).unwrap_or(i64::MAX);
        let mut resolved = resolve_versions(rows, drop_markers, retain_from);
        sort_rows(&mut resolved.kept);
        sort_rows(&mut resolved.history);
        if check_gain && !stable_rewrite {
            let target = ctx.max_rows_per_file.max(1);
            let output_segments = resolved.kept.len().div_ceil(target);
            if resolved.kept.len() as i64 == batch_rows && output_segments >= members.len() {
                return Ok(false);
            }
        }
        rows_read += batch_rows;
        history_floor = history_floor.max(resolved.history_floor);
        let kept = to_mirror_rows(resolved.kept);
        let history = to_mirror_rows(resolved.history);
        let max_seq = kept.iter().map(|row| row.seq).max().unwrap_or(0);
        let (written, rows_written) = write_rows(kept, max_seq)?;
        pending.extend(written);
        total_rows += rows_written;
        if !history.is_empty() {
            let history_max_seq = history.iter().map(|row| row.seq).max().unwrap_or(0);
            history_pending.extend(write_rows(history, history_max_seq)?.0);
        }
        Ok(true)
    };

    let Some(slice_rows) = stream_slice_rows else {
        let rows = read_members(&hints)?;
        if !rewrite_batch(rows, true)? {
            return Ok(None);
        }
        return finish_unit(
            table_oid, ctx, client, members, &manifest_path, pending, history_pending, total_rows,
            rows_read, history_floor,
        );
    };

    // Streamed rewrite of a unit too large to hold in memory (key-ordered tables only, see
    // `compact_locked`). Every version of a key has the same lead-key value, so cutting the unit's
    // lead-key range into slices keeps each key's versions together: each slice is read (one
    // segment at a time, rows outside the slice dropped), resolved and written on its own, and the
    // slices come out in key order, so the output stays key-disjoint. One pass first samples the
    // lead keys to pick balanced boundaries; all slices are then published in one activation.
    let lead_name = key_names[0];
    let lead_type = catalog
        .column_by_id(catalog.primary_key.columns[0].column_id)
        .and_then(|column| {
            koldstore_sortkey::SortKeyType::from_type_oid_for_bounds(column.pg_type.type_oid())
        });
    let Some(lead_type) = lead_type else {
        return Ok(None);
    };
    let lead_key = |row: &koldstore_parquet::CleanColdRow| -> Option<Vec<u8>> {
        koldstore_sortkey::encode_sort_key_json(lead_type, row.pk_json.get(lead_name)?).ok()
    };
    let unit_rows: i64 = members.iter().map(|segment| segment.row_count).sum();
    let stride = usize::try_from((unit_rows / STREAM_SAMPLE_TARGET).max(1)).unwrap_or(1);
    let mut sample: Vec<Vec<u8>> = Vec::new();
    let mut seen = 0_usize;
    for hint in &hints {
        for row in read_members(std::slice::from_ref(hint))? {
            let Some(key) = lead_key(&row) else {
                return Ok(None);
            };
            if seen % stride == 0 {
                sample.push(key);
            }
            seen += 1;
        }
    }
    sample.sort_unstable();
    sample.dedup();
    // Aim at half the cap per slice: the sample is approximate and a slice must stay under the cap.
    let per_slice = (slice_rows / 2).max(1);
    let slice_count = usize::try_from((unit_rows + per_slice - 1) / per_slice).unwrap_or(1).max(1);
    let mut boundaries: Vec<Vec<u8>> = (1..slice_count)
        .filter_map(|i| sample.get(i * sample.len() / slice_count).cloned())
        .collect();
    boundaries.dedup();
    if boundaries.is_empty() {
        return Ok(None);
    }
    let slice_cap = usize::try_from(slice_rows).unwrap_or(usize::MAX);
    for slice in 0..=boundaries.len() {
        let lo = slice.checked_sub(1).map(|i| boundaries[i].as_slice());
        let hi = boundaries.get(slice).map(Vec::as_slice);
        let mut batch: Vec<koldstore_parquet::CleanColdRow> = Vec::new();
        for (hint, member) in hints.iter().zip(members) {
            // A member whose lead-key range lies outside the slice cannot contribute.
            if let Some((min, max)) = member.bounds.as_ref().and_then(|bounds| bounds.first()) {
                if hi.is_some_and(|hi| min.as_slice() >= hi) || lo.is_some_and(|lo| max.as_slice() < lo) {
                    continue;
                }
            }
            for row in read_members(std::slice::from_ref(hint))? {
                let key = lead_key(&row).ok_or_else(|| "compaction could not encode a primary key".to_string())?;
                if lo.is_some_and(|lo| key.as_slice() < lo) || hi.is_some_and(|hi| key.as_slice() >= hi) {
                    continue;
                }
                batch.push(row);
                if batch.len() > slice_cap {
                    return Err(format!(
                        "a key slice of the unit exceeds max_rows ({slice_rows}); the keys are too skewed to split, \
                         raise max_rows"
                    ));
                }
            }
        }
        if !batch.is_empty() && !rewrite_batch(batch, false)? {
            return Ok(None);
        }
    }
    finish_unit(
        table_oid, ctx, client, members, &manifest_path, pending, history_pending, total_rows,
        rows_read, history_floor,
    )
}

/// Publishes a rewritten unit: manifest first (rebuildable from the catalog if the transaction does
/// not commit), then one catalog statement that activates the new segments and retires the old.
fn finish_unit(
    table_oid: pgrx::pg_sys::Oid,
    ctx: &FlushPreparedContext,
    client: &koldstore_storage::ObjectStoreClient,
    members: &[&ListedSegment],
    manifest_path: &str,
    pending: Vec<PendingFlushSegmentRef>,
    history_pending: Vec<PendingFlushSegmentRef>,
    total_rows: i64,
    rows_read: i64,
    history_floor: i64,
) -> Result<Option<UnitOutcome>, String> {
    let source_ids: Vec<uuid::Uuid> = members.iter().map(|segment| segment.segment_id).collect();
    let history_ids: Vec<uuid::Uuid> =
        history_pending.iter().map(|segment| segment.segment_id).collect();
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
        &[source_ids.as_slice(), history_ids.as_slice()].concat(),
    )?;
    write_manifest_with_client(client, manifest_path, &manifest)?;

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
            DatumWithOid::from(history_floor),
            DatumWithOid::from(to_pg(&history_ids)),
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

/// Retained-history segments of `table_oid` (see [`history_horizon`]) as scan hints, oldest first.
/// Only `changes_since` reads them; no query plan includes them.
pub(crate) fn history_segment_hints(
    table_oid: pgrx::pg_sys::Oid,
    current_schema_version: i32,
) -> Result<Vec<SegmentStatsHint>, String> {
    let statement = koldstore_catalog::queries::plan_history_segment_listing()
        .map_err(|error| error.to_string())?;
    let args = [DatumWithOid::from(table_oid)];
    crate::catalog::owner::with_extension_owner(|| {
        crate::spi::execute_prepared(&statement, &args, |tuples| {
            let mut out = Vec::new();
            for tuple in tuples {
                let (Some(object_path), Some(min_seq), Some(max_seq)) = (
                    tuple.get::<String>(1)?,
                    tuple.get::<i64>(4)?,
                    tuple.get::<i64>(5)?,
                ) else {
                    continue;
                };
                let physical_names: BTreeMap<i16, String> = tuple
                    .get::<String>(6)?
                    .map(|json| serde_json::from_str(&json).unwrap_or_default())
                    .unwrap_or_default();
                let to_seq = |raw: i64| {
                    SeqId::new(raw).map_err(|error| {
                        pgrx::spi::SpiError::DatumError(
                            pgrx::datum::TryFromDatumError::NoSuchAttributeName(format!(
                                "invalid seq {raw}: {error}"
                            )),
                        )
                    })
                };
                out.push(SegmentStatsHint {
                    object_path,
                    schema_version: tuple.get::<i32>(3)?.unwrap_or(current_schema_version),
                    physical_names,
                    byte_size: tuple.get::<i64>(2)?.and_then(|size| u64::try_from(size).ok()),
                    min_seq: to_seq(min_seq)?,
                    max_seq: to_seq(max_seq)?,
                    selected_row_groups: None,
                    scope_id: None,
                });
            }
            Ok(out)
        })
        .map_err(|error| error.to_string())
    })?
}

/// First `seq` compaction keeps as history: versions at or after it are moved to history segments
/// instead of dropped. `i64::MAX` (keep nothing) when `koldstore.cdc_retention_seconds` is 0.
fn history_horizon() -> i64 {
    let retention = i64::from(crate::guc::cdc_retention_seconds());
    if retention == 0 {
        return i64::MAX;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX));
    koldstore_common::minimum_id_at_unix_millis(now.saturating_sub(retention * 1000)).unwrap_or(1)
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
    let key_ids: Vec<i16> = ctx
        .catalog
        .primary_key
        .columns
        .iter()
        .map(|key| key.column_id.get())
        .collect();
    let statement = koldstore_catalog::queries::plan_compaction_segment_listing()
        .map_err(|error| error.to_string())?;
    let args = [
        DatumWithOid::from(table_oid),
        DatumWithOid::from(key_ids.clone()),
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
                let to_vec = |array: Option<pgrx::Array<&[u8]>>| -> Vec<Option<Vec<u8>>> {
                    array
                        .map(|array| array.iter().map(|value| value.map(<[u8]>::to_vec)).collect())
                        .unwrap_or_default()
                };
                let mins = to_vec(tuple.get::<pgrx::Array<&[u8]>>(9)?);
                let maxs = to_vec(tuple.get::<pgrx::Array<&[u8]>>(10)?);
                scoped = tuple.get::<i64>(11)?.unwrap_or(0);
                let tuple_bounds = tuple.get::<Vec<u8>>(12)?.zip(tuple.get::<Vec<u8>>(13)?);
                let scope_id = tuple.get::<String>(14)?;
                let bounds = (mins.len() == key_ids.len() && maxs.len() == key_ids.len())
                    .then(|| {
                        mins.into_iter()
                            .zip(maxs)
                            .map(|(min, max)| min.zip(max))
                            .collect::<Option<Vec<_>>>()
                    })
                    .flatten();
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
                        scope_id: scope_id.clone(),
                    },
                    row_count,
                    bounds,
                    tuple_bounds,
                    scope_id,
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
    let purged_history = purge_history_segments(table_oid, client)?;
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
        return Ok(purged_history);
    }
    let reaped = purged_history + i64::try_from(done.len()).unwrap_or(i64::MAX);
    pgrx::Spi::run_with_args(
        "DELETE FROM koldstore.cold_segments WHERE segment_id = ANY($1) AND status = 'compacted'",
        &[DatumWithOid::from(done)],
    )
    .map_err(|error| error.to_string())?;
    Ok(reaped)
}

/// Deletes the retained-history segments (see [`history_horizon`]) whose newest row has fallen out
/// of `koldstore.cdc_retention_seconds` (all of them when it is 0), raising the manifest's
/// `history_floor_seq` first so `changes_since` refuses the cursors that can no longer be served.
/// Returns how many segments were purged.
fn purge_history_segments(
    table_oid: pgrx::pg_sys::Oid,
    client: &koldstore_storage::ObjectStoreClient,
) -> Result<i64, String> {
    let statement = koldstore_catalog::queries::plan_history_segment_listing()
        .map_err(|error| error.to_string())?;
    let args = [DatumWithOid::from(table_oid)];
    let horizon = history_horizon();
    let due: Vec<(pgrx::Uuid, String, i64)> = crate::catalog::owner::with_extension_owner(|| {
        crate::spi::execute_prepared(&statement, &args, |tuples| {
            let mut out = Vec::new();
            for tuple in tuples {
                let (key, max_seq, id) = (
                    tuple.get::<String>(1)?,
                    tuple.get::<i64>(5)?,
                    tuple.get::<pgrx::Uuid>(7)?,
                );
                if let (Some(key), Some(max_seq), Some(id)) = (key, max_seq, id) {
                    if max_seq < horizon {
                        out.push((id, key, max_seq));
                    }
                }
            }
            Ok(out)
        })
        .map_err(|error| error.to_string())
    })??;
    if due.is_empty() {
        return Ok(0);
    }
    let floor = due.iter().map(|(_, _, max_seq)| *max_seq).max().unwrap_or(0);
    pgrx::Spi::run_with_args(
        "UPDATE koldstore.manifest SET history_floor_seq = GREATEST(history_floor_seq, $2::bigint) \
         WHERE table_oid = $1::oid AND scope_key = ''",
        &[DatumWithOid::from(table_oid), DatumWithOid::from(floor)],
    )
    .map_err(|error| error.to_string())?;
    let mut done: Vec<pgrx::Uuid> = Vec::with_capacity(due.len());
    for (segment_id, key, _) in due {
        match client.delete(&key) {
            Ok(()) => done.push(segment_id),
            Err(error) => pgrx::warning!("koldstore history purge: could not delete {key}: {error}"),
        }
    }
    let purged = i64::try_from(done.len()).unwrap_or(i64::MAX);
    if purged > 0 {
        pgrx::Spi::run_with_args(
            "DELETE FROM koldstore.cold_segments WHERE segment_id = ANY($1) AND status = 'history'",
            &[DatumWithOid::from(done)],
        )
        .map_err(|error| error.to_string())?;
        crate::catalog::cache::invalidate_table_globally(table_oid);
    }
    Ok(purged)
}

/// `koldstore.reap_compacted_segments` / maintenance entry: opens the table's storage and reaps.
pub(crate) fn reap_table(
    table_oid: pgrx::pg_sys::Oid,
    grace_seconds: i32,
) -> Result<i64, String> {
    crate::sql::table_metadata::refuse_on_follower(table_oid, "reap compacted segments")?;
    crate::sql::leader_lease::require_lease(table_oid, "reap compacted segments")?;
    crate::sql::follower_ack::require_followers_caught_up(table_oid, "reap compacted segments")?;
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
                "SELECT DISTINCT table_oid FROM koldstore.cold_segments cs \
                 WHERE NOT EXISTS (SELECT 1 FROM koldstore.schemas s WHERE s.table_oid = cs.table_oid \
                       AND s.active AND coalesce((s.options->>'follower')::boolean, false)) \
                   AND ((status = 'compacted' \
                   AND created_at <= now() - make_interval(secs => $1::double precision) \
                   AND NOT EXISTS (SELECT 1 FROM koldstore.backups b WHERE b.taken_at < created_at)) \
                    OR (status = 'history' AND max_seq < $2::bigint)) \
                 LIMIT 5",
                None,
                &[DatumWithOid::from(f64::from(grace)), DatumWithOid::from(history_horizon())],
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
        // A follower still reading older segments defers the pass; the next tick retries.
        if crate::sql::follower_ack::require_followers_caught_up(table_oid, "reap compacted segments").is_err() {
            continue;
        }
        reaped += reap_table(table_oid, grace)?;
    }
    Ok(Some(json!({ "reaped_segments": reaped })))
}

/// Rows the maintenance worker may read per tick across all tables.
const AUTO_COMPACTION_TICK_ROWS: i64 = 200_000;

/// One maintenance pass of automatic compaction (`koldstore.auto_compaction`). `Ok(None)` when it
/// is off or nothing was compacted.
pub(crate) fn auto_compaction_tick() -> Result<Option<Value>, String> {
    if !crate::guc::auto_compaction() {
        return Ok(None);
    }
    let tables: Vec<pgrx::pg_sys::Oid> = pgrx::Spi::connect(|client| {
        client
            .select(
                "SELECT table_oid FROM koldstore.cold_segments cs \
                 WHERE status = 'active' AND scope_key = '' \
                   AND NOT EXISTS (SELECT 1 FROM koldstore.schemas s WHERE s.table_oid = cs.table_oid \
                       AND s.active AND coalesce((s.options->>'follower')::boolean, false)) \
                 GROUP BY table_oid HAVING count(*) >= 2 ORDER BY table_oid",
                None,
                &[],
            )?
            .map(|row| {
                Ok(row
                    .get::<pgrx::pg_sys::Oid>(1)?
                    .unwrap_or(pgrx::pg_sys::InvalidOid))
            })
            .collect::<Result<Vec<_>, pgrx::spi::Error>>()
    })
    .map_err(|error| error.to_string())?;
    let mut budget = AUTO_COMPACTION_TICK_ROWS;
    let (mut tables_compacted, mut rows_read) = (0_i64, 0_i64);
    for table_oid in tables {
        if budget <= 0 {
            break;
        }
        // One table's failure (storage hiccup, dropped mid-pass) must not starve the others.
        match auto_compact_table(table_oid, budget) {
            Ok(Some(report)) => {
                let read = report["rows_read"].as_i64().unwrap_or(0);
                budget -= read.max(1);
                rows_read += read;
                tables_compacted += 1;
            }
            Ok(None) => {}
            Err(error) => pgrx::warning!(
                "koldstore auto-compaction: table oid {} skipped: {error}",
                table_oid.to_u32()
            ),
        }
    }
    Ok((tables_compacted > 0)
        .then(|| json!({ "tables_compacted": tables_compacted, "rows_read": rows_read })))
}

/// Retired segments of `table_oid` that a registered backup (taken before their retirement) still
/// needs, so reaping keeps their objects.
fn held_by_backups(table_oid: pgrx::pg_sys::Oid) -> Result<i64, String> {
    pgrx::Spi::get_one_with_args::<i64>(
        "SELECT count(*)::bigint FROM koldstore.cold_segments cs \
         WHERE cs.table_oid = $1::oid AND cs.status = 'compacted' \
           AND EXISTS (SELECT 1 FROM koldstore.backups b WHERE b.taken_at < cs.created_at)",
        &[DatumWithOid::from(table_oid)],
    )
    .map(|count| count.unwrap_or(0))
    .map_err(|error| error.to_string())
}
