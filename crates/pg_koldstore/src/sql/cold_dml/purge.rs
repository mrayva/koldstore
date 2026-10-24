//! `koldstore.purge_where`: explicit, opt-in bulk delete that reaches cold rows (ADR-007 Option C).
//!
//! A plain `DELETE` cannot see cold-only rows, and turning it into a tombstone writer (Option A)
//! was rejected. This function is the deliberate bulk path: under a `SHARE ROW EXCLUSIVE` table
//! lock (no concurrent DML) and the table job lock (no concurrent flush), it finds the cold-only
//! rows a predicate matches, writes `op = 3` tombstones for them into the async mirror with
//! sequence numbers above everything the mirror and the durable watermark have seen, and then
//! deletes the matching hot rows natively. Reads mask the purged rows at once; the next flush
//! turns the tombstones into delete markers like any other delete.
//!
//! Privileges: the predicate is evaluated as the invoking role (the function is not
//! `SECURITY DEFINER`), and every value written to the mirror is computed in that same context.
//! Only the fixed mirror `INSERT`/watermark statements run as the extension owner, and they
//! consume nothing but `jsonb`.

use koldstore_common::sql::ident::quote_ident;
use pgrx::datum::DatumWithOid;

/// Mirror rows written per statement.
const PURGE_CHUNK_ROWS: usize = 5_000;

/// `READ COMMITTED` in `XactIsoLevel`.
const XACT_READ_COMMITTED: i32 = 1;

/// One cold-only row to tombstone: its lock key and the mirror-column object to insert.
struct PurgeRow {
    key: String,
    mirror: serde_json::Value,
}

struct PurgeTarget {
    mirror: String,
    primary_key: Vec<String>,
    order_column: Option<String>,
    scope_column: Option<String>,
}

fn load_target(table_oid: pgrx::pg_sys::Oid) -> Result<PurgeTarget, String> {
    let statement = koldstore_catalog::queries::plan_async_managed_relation_by_oid().map_err(|error| error.to_string())?;
    let json = crate::spi::select_one::<String>(&statement, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "table is not managed by koldstore".to_string())?;
    let value: serde_json::Value = serde_json::from_str(&json).map_err(|error| error.to_string())?;
    let meta = koldstore_catalog::async_managed_relation(&value)?;
    Ok(PurgeTarget {
        mirror: meta.mirror,
        primary_key: meta.primary_key,
        order_column: meta.order_column.map(|order| order.name),
        scope_column: meta.scope_column,
    })
}

/// The `jsonb` object (keyed by mirror column) a tombstone for row `t` is built from.
fn mirror_object_sql(target: &PurgeTarget) -> String {
    let mut pairs: Vec<String> = target
        .primary_key
        .iter()
        .map(|column| format!("{}, t.{}", quote_literal(column), quote_ident(column)))
        .collect();
    if let Some(order) = &target.order_column {
        pairs.push(format!(
            "'order_key', '\\x' || encode(koldstore.internal_encode_sort_key(t.{}), 'hex')",
            quote_ident(order)
        ));
    }
    if let Some(scope) = &target.scope_column {
        pairs.push(format!("'scope_id', t.{}::text", quote_ident(scope)));
    }
    format!("jsonb_build_object({})", pairs.join(", "))
}

fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// Rows of `relation` matching `predicate`, as `(lock key, mirror object)`.
fn fetch_matches(
    relation: &str,
    pk_columns: &[String],
    target: &PurgeTarget,
    predicate: &str,
) -> Result<Vec<PurgeRow>, String> {
    let sql = format!(
        "SELECT coalesce(jsonb_agg(jsonb_build_object('k', {}, 'm', {})), '[]'::jsonb)::text \
         FROM {relation} AS t WHERE ({predicate})",
        super::key_lock::key_sql(pk_columns, "t"),
        mirror_object_sql(target),
    );
    let text = pgrx::Spi::get_one::<String>(&sql)
        .map_err(|error| error.to_string())?
        .unwrap_or_else(|| "[]".to_string());
    let value: serde_json::Value = serde_json::from_str(&text).map_err(|error| error.to_string())?;
    Ok(value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    Some(PurgeRow {
                        key: item.get("k")?.as_str()?.to_string(),
                        mirror: item.get("m")?.clone(),
                    })
                })
                .collect()
        })
        .unwrap_or_default())
}

fn write_chunk_sql(target: &PurgeTarget) -> String {
    let mut columns: Vec<String> = target.primary_key.iter().map(|c| quote_ident(c)).collect();
    let mut values: Vec<String> = target.primary_key.iter().map(|c| format!("src.{}", quote_ident(c))).collect();
    if target.order_column.is_some() {
        columns.push("order_key".to_string());
        values.push("src.order_key".to_string());
    }
    columns.push("seq".to_string());
    values.push("$2::bigint + src.n".to_string());
    columns.push("op".to_string());
    values.push("3::smallint".to_string());
    let mut update = "seq = EXCLUDED.seq, op = 3".to_string();
    if target.scope_column.is_some() {
        columns.push("scope_id".to_string());
        values.push("src.scope_id".to_string());
        update.push_str(&format!(", scope_id = COALESCE(EXCLUDED.scope_id, {}.scope_id)", target.mirror));
    }
    let conflict = target.primary_key.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    format!(
        "WITH src AS (SELECT r.*, row_number() OVER () AS n FROM jsonb_populate_recordset(NULL::{mirror}, $1) AS r), \
         ins AS (INSERT INTO {mirror} ({columns}) SELECT {values} FROM src \
                 ON CONFLICT ({conflict}) DO UPDATE SET {update} RETURNING (xmax = 0) AS inserted) \
         SELECT count(*) FILTER (WHERE inserted)::bigint FROM ins",
        mirror = target.mirror,
        columns = columns.join(", "),
        values = values.join(", "),
    )
}

/// Writes the tombstones; returns `(rows written, newly inserted mirror rows)`.
fn write_tombstones(target: &PurgeTarget, rows: &[PurgeRow]) -> Result<(u64, i64), String> {
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId };
    let floor_sql = format!(
        "SELECT GREATEST(COALESCE((SELECT max(seq) FROM {}), 0), \
                COALESCE((SELECT seq_high_watermark FROM koldstore.async_mirror_state WHERE database_oid = $1), 0))",
        target.mirror
    );
    let chunk_sql = write_chunk_sql(target);
    crate::catalog::owner::with_extension_owner(|| -> Result<(u64, i64), String> {
        let floor = pgrx::Spi::get_one_with_args::<i64>(&floor_sql, &[DatumWithOid::from(database_oid)])
            .map_err(|error| error.to_string())?
            .unwrap_or(0);
        let mut offset = 0_i64;
        let mut inserted = 0_i64;
        for chunk in rows.chunks(PURGE_CHUNK_ROWS) {
            let payload = serde_json::Value::Array(chunk.iter().map(|row| row.mirror.clone()).collect());
            inserted += pgrx::Spi::get_one_with_args::<i64>(
                &chunk_sql,
                &[DatumWithOid::from(pgrx::JsonB(payload)), DatumWithOid::from(floor + offset)],
            )
            .map_err(|error| error.to_string())?
            .unwrap_or(0);
            offset += i64::try_from(chunk.len()).unwrap_or(i64::MAX);
        }
        pgrx::Spi::run_with_args(
            "UPDATE koldstore.async_mirror_state \
             SET seq_high_watermark = GREATEST(seq_high_watermark, $1) WHERE database_oid = $2",
            &[DatumWithOid::from(floor + offset), DatumWithOid::from(database_oid)],
        )
        .map_err(|error| error.to_string())?;
        Ok((u64::try_from(offset).unwrap_or(0), inserted))
    })?
}

fn purge_where_impl(table_oid: pgrx::pg_sys::Oid, predicate: &str) -> Result<serde_json::Value, String> {
    let relation = super::qualified_relation(table_oid)?;
    let quoted = relation.quoted();
    let pk_columns = super::primary_key_columns(table_oid)?;
    let target = load_target(table_oid)?;
    let cap = crate::guc::max_purge_rows();

    // Everything committed so far is mirrored (and the progress recorded in this transaction, so a
    // later applier replay cannot hand an old event a seq above our tombstones); then look through
    // a snapshot that includes it.
    crate::mirror::apply::wait_for_async_mirror();
    // SAFETY: plain command-counter bookkeeping; balanced snapshot push/pop (an error aborts the
    // transaction, which unwinds it).
    unsafe {
        pgrx::pg_sys::CommandCounterIncrement();
        pgrx::pg_sys::PushActiveSnapshot(pgrx::pg_sys::GetTransactionSnapshot());
    }
    let matches = (|| -> Result<Vec<PurgeRow>, String> {
        let merged = crate::txn_writes::with_check_suppressed(|| fetch_matches(&quoted, &pk_columns, &target, predicate))?;
        if merged.is_empty() {
            return Ok(merged);
        }
        super::inbound_fk::apply_purge_fk_actions(table_oid, predicate)?;
        let hot = {
            let _hot_only = crate::merge_scan::pg::HotOnlyRelation::new(table_oid);
            crate::txn_writes::with_check_suppressed(|| fetch_matches(&quoted, &pk_columns, &target, predicate))?
        };
        let hot_keys: std::collections::HashSet<String> = hot.into_iter().map(|row| row.key).collect();
        let cold: Vec<PurgeRow> = merged.into_iter().filter(|row| !hot_keys.contains(&row.key)).collect();
        if cold.len() > cap {
            return Err(format!(
                "predicate matches {} cold-only rows, more than koldstore.max_purge_rows ({cap}); \
                 narrow the predicate or purge in batches",
                cold.len()
            ));
        }
        Ok(cold)
    })();
    unsafe { pgrx::pg_sys::PopActiveSnapshot() };
    let cold = matches?;

    let (purged_cold, new_mirror_rows) = if cold.is_empty() { (0, 0) } else { write_tombstones(&target, &cold)? };
    crate::row_counter_cache::record_delta(table_oid, 0, new_mirror_rows);

    let delete_sql = format!("DELETE FROM {quoted} AS t WHERE ({predicate})");
    let deleted_hot = super::guard::with_guard_suspended(|| {
        pgrx::Spi::connect_mut(|client| client.update(&delete_sql, None, &[]).map(|table| table.len() as u64))
    })
    .map_err(|error| error.to_string())?;
    crate::txn_writes::record_managed_write(table_oid);

    Ok(serde_json::json!({ "purged_cold": purged_cold, "deleted_hot": deleted_hot }))
}

/// Deletes every row of a managed table matching `predicate`, cold rows included.
///
/// SQL contract: `koldstore.purge_where(table_name regclass, predicate text) → jsonb`.
/// `predicate` is a boolean expression over the table aliased `t` (for example
/// `t.created_at < now() - interval '2 years'`). Owner or superuser only; requires `READ COMMITTED`
/// and a transaction that has not yet written the table. Blocks concurrent writes and flushes of the
/// table until it commits. Returns `{ purged_cold, deleted_hot }`.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "purge_where", schema = "koldstore")]
pub fn purge_where_pg(table_name: pgrx::PgRelation, predicate: &str) -> pgrx::JsonB {
    let table_oid = table_name.oid();
    crate::security::require_relation_owner_or_superuser(table_oid, "purge rows from this table");
    drop(table_name);
    if predicate.trim().is_empty() {
        pgrx::error!("purge_where: predicate must not be empty");
    }
    if unsafe { pgrx::pg_sys::XactIsoLevel } != XACT_READ_COMMITTED {
        pgrx::error!("purge_where requires READ COMMITTED isolation");
    }
    if crate::txn_writes::was_written(table_oid) {
        pgrx::error!("purge_where: this transaction already wrote the table; run it in its own transaction");
    }
    let _job_lock =
        crate::sql::job_lock::TableJobLockGuard::lock_bounded(table_oid, crate::sql::job_lock::WRITE_LOCK_TIMEOUT)
            .unwrap_or_else(|error| pgrx::error!("purge_where failed to acquire table lock: {error}"));
    let relation = super::qualified_relation(table_oid).unwrap_or_else(|error| pgrx::error!("purge_where: {error}"));
    pgrx::Spi::run(&format!("LOCK TABLE ONLY {} IN SHARE ROW EXCLUSIVE MODE", relation.quoted()))
        .unwrap_or_else(|error| pgrx::error!("purge_where could not lock the table: {error}"));
    let result = purge_where_impl(table_oid, predicate).unwrap_or_else(|error| pgrx::error!("purge_where failed: {error}"));
    pgrx::JsonB(result)
}
