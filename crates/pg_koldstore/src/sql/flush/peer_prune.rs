//! Follower hot-heap prune against segments adopted from a leader.
//!
//! A follower shares the leader's cold tier but keeps its own heap (Spock still replicates hot
//! rows to it). Once the leader flushes a row, the follower's hot copy is redundant. A hot row is
//! deleted only when it equals the segment's copy exactly (primary key and whole-row JSON), so a
//! row edited after the leader's flush stays hot. Any doubt leaves the row in place.

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

    let mut segments = 0_i64;
    let mut rows_read = 0_i64;
    let (mut hot_pruned, mut mirror_pruned) = (0_i64, 0_i64);
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
        let changes = crate::sql::events::read_cold_segment_page(
            table_oid,
            &snapshot,
            segment,
            0,
            usize::MAX,
            None,
        )?;
        let images: Vec<Value> = changes
            .into_iter()
            .filter_map(|change| {
                let Value::Object(mut image) = change.row_image_json? else {
                    return None;
                };
                if let Value::Object(pk) = change.pk_json {
                    image.extend(pk);
                }
                Some(Value::Object(image))
            })
            .collect();
        rows_read += images.len() as i64;
        for batch in images.chunks(BATCH_ROWS) {
            let payload = pgrx::JsonB(Value::Array(batch.to_vec()));
            let (hot, mir) = super::spi::with_prune_session(|client| {
                let result = client
                    .update(
                        &sql,
                        None,
                        &[pgrx::datum::DatumWithOid::from(payload)],
                    )
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
    }
    if hot_pruned > 0 || mirror_pruned > 0 {
        super::execute::apply_flush_row_count_deltas(table_oid, mirror_pruned, hot_pruned, 0)?;
    }
    Ok(json!({ "segments": segments, "rows_read": rows_read, "hot_pruned": hot_pruned }))
}
