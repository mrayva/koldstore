//! Explicit cold-tier DML SQL entrypoints (`hydrate_pk`, `update_row`, `delete_row`).
//!
//! Standard SQL `INSERT`/`UPDATE`/`DELETE` only ever consults the hot heap
//! index -- a row pruned to cold Parquet storage has no heap presence for
//! PostgreSQL's own constraint machinery to see (upstream issue #122). These
//! functions give callers an explicit, opt-in way to materialize ("hydrate")
//! a specific cold-only row back into the heap, after which ordinary SQL on
//! that key works correctly again -- rather than pretending plain
//! `INSERT`/`UPDATE`/`DELETE` are safe once a table's data may have gone
//! cold (tracked: <https://github.com/kalamdb/koldstore/issues/55>, itself
//! one option for <https://github.com/kalamdb/koldstore/issues/122>).
//!
//! Deliberately reuses two already-correct primitives instead of adding new
//! low-level cold-storage code:
//! - the read side is the same `KoldMergeScan` every ordinary `SELECT`
//!   already goes through, so "does this PK exist, hot or cold" needs no new
//!   lookup machinery;
//! - the write side is ordinary heap `INSERT`/`UPDATE`/`DELETE`, so mirror /
//!   WAL-apply / change-log capture for a hydrated row is identical to any
//!   other managed-table DML -- no hand-written tombstone format to get
//!   subtly wrong.
//!
//! Concretely, hydration is two statements, reusing PostgreSQL's own JSON-to-
//! row type coercion instead of hand-tracking each PK column's type:
//!
//! ```sql
//! -- 1. locate the row (hot or cold) as jsonb
//! WITH pk AS (SELECT * FROM jsonb_populate_record(NULL::"schema"."table", $1))
//! SELECT to_jsonb(t) FROM "schema"."table" AS t, pk
//! WHERE t."id" = pk."id" -- one clause per primary-key column
//!
//! -- 2. materialize it into the heap
//! INSERT INTO "schema"."table"
//! SELECT * FROM jsonb_populate_record(NULL::"schema"."table", $1)
//! ON CONFLICT DO NOTHING
//! ```
//!
//! This MUST be two separate statements, not one `INSERT INTO t SELECT ...
//! FROM t`: confirmed live (`EXPLAIN ANALYZE`) that when a relation is
//! simultaneously the `INSERT` target and appears in the source `SELECT`'s
//! `FROM` clause, this crate's custom-scan planner hook does not replace the
//! source-side scan with `KoldMergeScan` -- it silently falls back to a
//! plain heap `Index Scan`, which only ever sees the hot tier. A single
//! combined statement's source-side read therefore finds nothing for a
//! cold-only key and the `INSERT` inserts zero rows, even though the
//! identical scan shape as a *standalone* `SELECT` (not also touching that
//! table as an `INSERT` target) correctly hits `KoldMergeScan` and returns
//! the row. Splitting into "locate the row" (step 1, a real standalone
//! `SELECT`, so `KoldMergeScan` fires) then "insert it" (step 2, whose sole
//! `FROM` source is `jsonb_populate_record`, so it never re-scans the
//! target table at all) sidesteps the hook's blind spot entirely. `ON
//! CONFLICT DO NOTHING` on step 2 absorbs the case where the row went hot
//! between steps 1 and 2 -- no duplicate, no error, matching this crate's
//! `manage_table` composite/typed primary key handling.

#[cfg(feature = "pg")]
use koldstore_common::QualifiedTableName;
#[cfg(feature = "pg")]
use pgrx::datum::DatumWithOid;

pub(crate) mod guard;

/// Resolves `table_oid` to a safely quotable schema-qualified name.
#[cfg(feature = "pg")]
fn qualified_relation(table_oid: pgrx::pg_sys::Oid) -> Result<QualifiedTableName, String> {
    let relation = crate::catalog::resolve::relation_context(table_oid)?;
    QualifiedTableName::parse(&format!("{}.{}", relation.namespace, relation.name))
        .map_err(|error| error.to_string())
}

/// Returns the current-schema primary-key column names for a managed table.
#[cfg(feature = "pg")]
pub(crate) fn primary_key_columns(table_oid: pgrx::pg_sys::Oid) -> Result<Vec<String>, String> {
    let snapshot = crate::catalog::cache::managed_table_snapshot(table_oid)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "relation is not managed by KoldStore".to_string())?;
    let columns: Vec<String> = snapshot.primary_key_names().map(str::to_string).collect();
    if columns.is_empty() {
        return Err("managed table has no primary-key columns".to_string());
    }
    Ok(columns)
}

/// Returns every live (non-dropped) column name for a relation, in
/// `attnum` order. Used to build the `SET (col_list) = (...)` target list
/// for [`update_row_impl`]'s generic jsonb-merge `UPDATE`.
#[cfg(feature = "pg")]
fn all_column_names(table_oid: pgrx::pg_sys::Oid) -> Result<Vec<String>, String> {
    let sql = "SELECT array_agg(attname::text ORDER BY attnum) FROM pg_catalog.pg_attribute \
               WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped";
    let args = [DatumWithOid::from(table_oid)];
    let columns = pgrx::Spi::get_one_with_args::<Vec<String>>(sql, &args)
        .map_err(|error| error.to_string())?
        .unwrap_or_default();
    if columns.is_empty() {
        return Err("relation has no columns".to_string());
    }
    Ok(columns)
}

/// Builds `t."col1" = pk."col1" AND t."col2" = pk."col2" ...` from PK names.
#[cfg(feature = "pg")]
fn pk_join_predicate(pk_columns: &[String]) -> String {
    pk_columns
        .iter()
        .map(|column| {
            let quoted = koldstore_common::sql::ident::quote_ident(column);
            format!("t.{quoted} = pk.{quoted}")
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Locates `pk` (hot or cold) as a full-row jsonb value via a *standalone*
/// `SELECT`, so `KoldMergeScan` actually fires on the source scan -- see the
/// module doc comment for why this must never be combined with a write
/// statement against the same table in one statement.
///
/// Shared by [`hydrate_pk_impl`] (its materialization source) and
/// [`guard::cold_pk_exists`] (the write-guard's existence probe) -- both
/// need exactly this "does this key exist, and if so what's the full row"
/// lookup.
#[cfg(feature = "pg")]
fn locate_row_json(
    table_oid: pgrx::pg_sys::Oid,
    pk_json: &serde_json::Value,
) -> Result<Option<pgrx::JsonB>, String> {
    let relation = qualified_relation(table_oid)?;
    let pk_columns = primary_key_columns(table_oid)?;
    let quoted = relation.quoted();
    let predicate = pk_join_predicate(&pk_columns);

    let locate_sql = format!(
        "WITH pk AS (SELECT * FROM jsonb_populate_record(NULL::{quoted}, $1)) \
         SELECT to_jsonb(t) FROM {quoted} AS t, pk \
         WHERE {predicate}"
    );
    let locate_args = [DatumWithOid::from(pgrx::JsonB(pk_json.clone()))];
    // `get_one_with_args` positions its cursor at row 0 unconditionally
    // (pgrx's `SpiTupleTable::first()` does this even when the result set
    // is empty), so a genuinely zero-row result -- the key does not exist
    // hot or cold -- surfaces as `Err(SpiError::InvalidPosition)` rather
    // than `Ok(None)`. Confirmed live: hydrate_pk on a nonexistent key
    // raised exactly this error before this match was added. Map it to
    // "not found" explicitly instead of propagating it as a real failure.
    match pgrx::Spi::get_one_with_args::<pgrx::JsonB>(&locate_sql, &locate_args) {
        Ok(row_json) => Ok(row_json),
        Err(pgrx::spi::Error::InvalidPosition) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

/// Materializes `pk` into the heap from cold storage if it is not already a
/// live heap row. Returns the number of rows inserted (0 or 1).
///
/// Two SPI round trips -- see the module doc comment for why a single
/// combined `INSERT INTO t SELECT ... FROM t` statement does not work here.
///
/// Reused by [`update_row_pg`]/[`delete_row_pg`] before falling through to an
/// ordinary `UPDATE`/`DELETE` once the row is guaranteed to have real heap
/// presence. Callers must hold the table job lock (see
/// `crate::sql::job_lock::TableJobLockGuard`) before calling this so a
/// concurrent flush cannot race the same key.
#[cfg(feature = "pg")]
fn hydrate_pk_impl(
    table_oid: pgrx::pg_sys::Oid,
    pk_json: &serde_json::Value,
) -> Result<u64, String> {
    let relation = qualified_relation(table_oid)?;
    let quoted = relation.quoted();

    // Step 1: locate the row (hot or cold).
    let Some(row_json) = locate_row_json(table_oid, pk_json)? else {
        return Ok(0);
    };

    // Step 2: insert it. The sole FROM source here is
    // jsonb_populate_record, never a scan on the target table, so this
    // step cannot hit the same planner blind spot as step 1 would if
    // combined. ON CONFLICT DO NOTHING absorbs a hot-race between steps.
    let insert_sql = format!(
        "INSERT INTO {quoted} \
         SELECT * FROM jsonb_populate_record(NULL::{quoted}, $1) \
         ON CONFLICT DO NOTHING"
    );
    let statement = koldstore_common::SqlStatement::write("koldstore hydrate_pk", &insert_sql)
        .map_err(|error| error.to_string())?;
    let insert_args = [DatumWithOid::from(row_json)];
    let rows = crate::spi::update(&statement, &insert_args).map_err(|error| error.to_string())?;
    Ok(rows.rows_affected)
}

/// Hydrates a cold-only primary key back into the heap.
///
/// SQL contract: `koldstore.hydrate_pk(table_name regclass, pk jsonb) → jsonb`.
/// `pk` names each primary-key column, e.g. `'{"id": 42}'` for a simple key
/// or `'{"tenant_id": 5, "id": 42}'` for a composite one.
///
/// Once hydrated, ordinary `UPDATE`/`DELETE`/`INSERT ... ON CONFLICT` on this
/// key work correctly again -- see `koldstore.update_row`/`koldstore.delete_row`
/// for a one-call version that hydrates only when needed.
///
/// Returns `{ affected_rows, hydrated }`. `hydrated = true` means the row was
/// cold-only and is now a live heap row; `hydrated = false` means it was
/// already hot (no-op) or the key does not exist anywhere.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "hydrate_pk", schema = "koldstore", security_definer)]
pub fn hydrate_pk_pg(table_name: pgrx::PgRelation, pk: pgrx::JsonB) -> pgrx::JsonB {
    let table_oid = table_name.oid();
    crate::security::require_relation_owner_or_superuser(table_oid, "hydrate a row for this table");
    drop(table_name);
    let _lock = crate::sql::job_lock::TableJobLockGuard::lock(table_oid)
        .unwrap_or_else(|error| pgrx::error!("hydrate_pk failed to acquire table lock: {error}"));
    // The materializing INSERT below would otherwise trip this table's own
    // BEFORE INSERT cold-DML write guard (see `guard::plan_insert_guard`) --
    // that guard exists to catch *callers* duplicating a cold PK via plain
    // SQL, not this function's own, deliberate, correctness-preserving
    // materialization of the same PK.
    let affected = guard::with_guard_suspended(|| {
        hydrate_pk_impl(table_oid, &pk.0).unwrap_or_else(|error| pgrx::error!("hydrate_pk failed: {error}"))
    });
    pgrx::JsonB(serde_json::json!({
        "affected_rows": affected,
        "hydrated": affected > 0,
    }))
}

/// Updates one row by primary key, hydrating it from cold storage first if
/// needed. Returns `(affected_rows, hydrated)`.
///
/// Fast path: an ordinary `UPDATE` against the heap. This is safe to try
/// unconditionally -- a hot row is a plain heap `Index Scan` match (no
/// `KoldMergeScan` involved), and a cold-only or nonexistent key just
/// affects 0 rows, which is exactly the signal needed to decide whether to
/// hydrate. When `lookup_cold` is true and the fast path affected 0 rows,
/// hydrates the key (see [`hydrate_pk_impl`]) and retries the identical
/// `UPDATE` once, now against a guaranteed-hot row.
///
/// The `UPDATE`'s `SET (col_list) = (...)` target list is built from every
/// live column, not just the ones named in `patch`: the correlated subquery
/// merges `to_jsonb(t.*) || patch` so unmentioned columns keep their
/// current value and only `patch`'s keys actually change -- a partial
/// patch, not a full-row replace.
#[cfg(feature = "pg")]
fn update_row_impl(
    table_oid: pgrx::pg_sys::Oid,
    pk_json: &serde_json::Value,
    patch_json: &serde_json::Value,
    lookup_cold: bool,
) -> Result<(u64, bool), String> {
    let relation = qualified_relation(table_oid)?;
    let pk_columns = primary_key_columns(table_oid)?;
    let all_columns = all_column_names(table_oid)?;
    let quoted = relation.quoted();
    let predicate = pk_join_predicate(&pk_columns);
    let col_list = all_columns
        .iter()
        .map(|column| koldstore_common::sql::ident::quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");

    let update_sql = format!(
        "WITH pk AS (SELECT * FROM jsonb_populate_record(NULL::{quoted}, $1)) \
         UPDATE {quoted} AS t \
         SET ({col_list}) = ( \
           SELECT {col_list} FROM jsonb_populate_record(NULL::{quoted}, to_jsonb(t.*) || $2::jsonb) \
         ) \
         FROM pk \
         WHERE {predicate}"
    );
    let statement = koldstore_common::SqlStatement::write("koldstore update_row", &update_sql)
        .map_err(|error| error.to_string())?;

    let try_update = |pk_json: &serde_json::Value, patch_json: &serde_json::Value| -> Result<u64, String> {
        let args = [
            DatumWithOid::from(pgrx::JsonB(pk_json.clone())),
            DatumWithOid::from(pgrx::JsonB(patch_json.clone())),
        ];
        let rows = crate::spi::update(&statement, &args).map_err(|error| error.to_string())?;
        Ok(rows.rows_affected)
    };

    let affected = try_update(pk_json, patch_json)?;
    if affected > 0 {
        return Ok((affected, false));
    }
    if !lookup_cold {
        return Ok((0, false));
    }
    let hydrated_rows = hydrate_pk_impl(table_oid, pk_json)?;
    if hydrated_rows == 0 {
        return Ok((0, false));
    }
    let affected_retry = try_update(pk_json, patch_json)?;
    Ok((affected_retry, true))
}

/// Updates a row by primary key, hydrating it from cold storage first if
/// its key currently only exists there.
///
/// SQL contract:
/// `koldstore.update_row(table_name regclass, pk jsonb, patch jsonb, lookup_cold default true) → jsonb`.
/// `patch` is a partial patch -- only the keys present in it are changed;
/// every other column keeps its current value. Pass `lookup_cold => false`
/// to restrict the update to hot rows only (no implicit hydration).
///
/// Returns `{ affected_rows, updated, hydrated }`. `updated = false` means
/// the key does not exist anywhere (hot, cold, or -- with
/// `lookup_cold => false` -- hot only).
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "update_row", schema = "koldstore", security_definer)]
pub fn update_row_pg(
    table_name: pgrx::PgRelation,
    pk: pgrx::JsonB,
    patch: pgrx::JsonB,
    lookup_cold: pgrx::default!(bool, true),
) -> pgrx::JsonB {
    let table_oid = table_name.oid();
    crate::security::require_relation_owner_or_superuser(table_oid, "update a row for this table");
    drop(table_name);
    let _lock = crate::sql::job_lock::TableJobLockGuard::lock(table_oid)
        .unwrap_or_else(|error| pgrx::error!("update_row failed to acquire table lock: {error}"));
    // Suspend the write guard for this call's own native statements --
    // see the identical comment on `hydrate_pk_pg`.
    let (affected, hydrated) = guard::with_guard_suspended(|| {
        update_row_impl(table_oid, &pk.0, &patch.0, lookup_cold)
            .unwrap_or_else(|error| pgrx::error!("update_row failed: {error}"))
    });
    pgrx::JsonB(serde_json::json!({
        "affected_rows": affected,
        "updated": affected > 0,
        "hydrated": hydrated,
    }))
}

/// Deletes one row by primary key, hydrating it from cold storage first if
/// needed. Returns `(affected_rows, hydrated)`.
///
/// Same fast-path/hydrate-and-retry shape as [`update_row_impl`]: an
/// ordinary `DELETE ... USING pk WHERE ...` is a plain heap `Index Scan`
/// match on the target, so trying it first is always safe, and a cold-only
/// or nonexistent key just affects 0 rows -- the signal to hydrate (if
/// `lookup_cold`) and retry once.
#[cfg(feature = "pg")]
fn delete_row_impl(
    table_oid: pgrx::pg_sys::Oid,
    pk_json: &serde_json::Value,
    lookup_cold: bool,
) -> Result<(u64, bool), String> {
    let relation = qualified_relation(table_oid)?;
    let pk_columns = primary_key_columns(table_oid)?;
    let quoted = relation.quoted();
    let predicate = pk_join_predicate(&pk_columns);

    let delete_sql = format!(
        "WITH pk AS (SELECT * FROM jsonb_populate_record(NULL::{quoted}, $1)) \
         DELETE FROM {quoted} AS t \
         USING pk \
         WHERE {predicate}"
    );
    let statement = koldstore_common::SqlStatement::write("koldstore delete_row", &delete_sql)
        .map_err(|error| error.to_string())?;

    let try_delete = |pk_json: &serde_json::Value| -> Result<u64, String> {
        let args = [DatumWithOid::from(pgrx::JsonB(pk_json.clone()))];
        let rows = crate::spi::update(&statement, &args).map_err(|error| error.to_string())?;
        Ok(rows.rows_affected)
    };

    let affected = try_delete(pk_json)?;
    if affected > 0 {
        return Ok((affected, false));
    }
    if !lookup_cold {
        return Ok((0, false));
    }
    let hydrated_rows = hydrate_pk_impl(table_oid, pk_json)?;
    if hydrated_rows == 0 {
        return Ok((0, false));
    }
    let affected_retry = try_delete(pk_json)?;
    Ok((affected_retry, true))
}

/// Deletes a row by primary key, hydrating it from cold storage first if
/// its key currently only exists there.
///
/// SQL contract:
/// `koldstore.delete_row(table_name regclass, pk jsonb, lookup_cold default true) → jsonb`.
/// Pass `lookup_cold => false` to restrict the delete to hot rows only (no
/// implicit hydration) -- a plain `DELETE ... WHERE <pk>` on a managed
/// table already does this; `lookup_cold => false` here mainly exists for
/// symmetry with [`update_row_pg`].
///
/// Returns `{ affected_rows, deleted, hydrated }`. `deleted = false` means
/// the key does not exist anywhere (hot, cold, or -- with
/// `lookup_cold => false` -- hot only).
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "delete_row", schema = "koldstore", security_definer)]
pub fn delete_row_pg(
    table_name: pgrx::PgRelation,
    pk: pgrx::JsonB,
    lookup_cold: pgrx::default!(bool, true),
) -> pgrx::JsonB {
    let table_oid = table_name.oid();
    crate::security::require_relation_owner_or_superuser(table_oid, "delete a row for this table");
    drop(table_name);
    let _lock = crate::sql::job_lock::TableJobLockGuard::lock(table_oid)
        .unwrap_or_else(|error| pgrx::error!("delete_row failed to acquire table lock: {error}"));
    // Suspend the write guard for this call's own native statements --
    // see the identical comment on `hydrate_pk_pg`.
    let (affected, hydrated) = guard::with_guard_suspended(|| {
        delete_row_impl(table_oid, &pk.0, lookup_cold)
            .unwrap_or_else(|error| pgrx::error!("delete_row failed: {error}"))
    });
    pgrx::JsonB(serde_json::json!({
        "affected_rows": affected,
        "deleted": affected > 0,
        "hydrated": hydrated,
    }))
}
