//! Per-key transaction-level advisory locks for cold-row hydration.
//!
//! Hydrating a cold key inserts it into the heap, and two sessions doing so for the same
//! key used to interleave badly: the second one's `INSERT` waits on the first's
//! uncommitted hydrated row, and when the first commits a delete the second simply inserts
//! its own copy and carries on, so a row deleted a moment ago could be acted on again
//! (an `UPDATE` succeeding after the `DELETE`, or the row coming back). The protocol here
//! serializes hydration per key: lock the key, and only then (re-)look at the mirror and
//! the cold data, so everything a previous holder committed is visible.
//!
//! Locks are polled with `pg_try_advisory_xact_lock` and a deadline. A blocking advisory
//! lock can be chosen by the deadlock detector, and the error it raises inside a
//! `DirectFunctionCall` boundary cannot unwind (it aborts the server); polling ends any
//! wait cycle with an ordinary, catchable timeout error instead. Locks are held to the end
//! of the (sub)transaction.

use std::time::{Duration, Instant};

use pgrx::datum::DatumWithOid;

/// Longest a hydrating statement waits for keys another transaction is changing.
pub(crate) const KEY_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// SQL text of `jsonb_build_array(<pk columns of alias>)::text`: one rendering of a key,
/// produced by PostgreSQL itself so every path (a fetched row, a caller-supplied primary
/// key object) yields the same string for the same key.
pub(crate) fn key_sql(pk_columns: &[String], alias: &str) -> String {
    let columns = pk_columns
        .iter()
        .map(|column| format!("{alias}.{}", koldstore_common::sql::ident::quote_ident(column)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("jsonb_build_array({columns})::text")
}

/// The lock key text for a caller-supplied primary-key object.
pub(crate) fn key_of_pk_json(table_oid: pgrx::pg_sys::Oid, pk_json: &serde_json::Value) -> Result<String, String> {
    let relation = super::qualified_relation(table_oid)?;
    let pk_columns = super::primary_key_columns(table_oid)?;
    let sql = format!(
        "SELECT {} FROM jsonb_populate_record(NULL::{}, $1) AS r",
        key_sql(&pk_columns, "r"),
        relation.quoted()
    );
    pgrx::Spi::get_one_with_args::<String>(&sql, &[DatumWithOid::from(pgrx::JsonB(pk_json.clone()))])
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "primary key produced no lock key".to_string())
}

/// Takes the xact-level lock of every key (table-scoped), waiting at most
/// [`KEY_LOCK_TIMEOUT`] in total.
pub(crate) fn lock_keys_bounded(table_oid: pgrx::pg_sys::Oid, keys: &[String]) -> Result<(), String> {
    if keys.is_empty() {
        return Ok(());
    }
    let namespace = i32::from_ne_bytes(table_oid.to_u32().to_ne_bytes());
    let deadline = Instant::now() + KEY_LOCK_TIMEOUT;
    let mut pending: Vec<String> = keys.to_vec();
    loop {
        // Returns the keys that could NOT be locked; the others are now held.
        let failed: Vec<String> = pgrx::Spi::connect(|client| -> Result<Vec<String>, pgrx::spi::Error> {
            client
                .select(
                    "SELECT k FROM unnest($1::text[]) AS k WHERE NOT pg_try_advisory_xact_lock($2::int4, hashtext(k))",
                    None,
                    &[DatumWithOid::from(pending.clone()), DatumWithOid::from(namespace)],
                )?
                .map(|row| Ok(row.get::<String>(1)?.unwrap_or_default()))
                .collect()
        })
        .map_err(|error| error.to_string())?;
        if failed.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{} key(s) of table oid {} are being changed by another transaction; retry",
                failed.len(),
                table_oid.to_u32()
            ));
        }
        pending = failed;
        // SAFETY: plain sleep, then honour cancel/terminate requests while waiting.
        unsafe { pgrx::pg_sys::pg_usleep(5_000) };
        pgrx::check_for_interrupts!();
    }
}

/// Runs `f` after making everything committed so far visible to cold reads: applies
/// committed WAL to the async mirror (when `koldstore.hydrate_fence_mirror` is on) and
/// looks through a fresh snapshot. Callers hold the key locks first.
pub(crate) fn with_current_view<T>(f: impl FnOnce() -> T) -> T {
    if crate::guc::hydrate_fence_mirror() {
        crate::mirror::apply::fence_for_read()
            .unwrap_or_else(|error| pgrx::error!("koldstore: mirror fence failed: {error}"));
        // SAFETY: plain command-counter bookkeeping.
        unsafe { pgrx::pg_sys::CommandCounterIncrement() };
    }
    // SAFETY: balanced push/pop; an error aborts the (sub)transaction, which unwinds it.
    unsafe { pgrx::pg_sys::PushActiveSnapshot(pgrx::pg_sys::GetTransactionSnapshot()) };
    let result = f();
    unsafe { pgrx::pg_sys::PopActiveSnapshot() };
    result
}
