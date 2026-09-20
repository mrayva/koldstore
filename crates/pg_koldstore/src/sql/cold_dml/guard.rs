//! Write-time guard against silent hot/cold correctness gaps (upstream #122,
//! Option B: reject SQL forms whose semantics can't be preserved instead of
//! silently doing the wrong thing).
//!
//! Two independent pieces, attached automatically by `manage_table`:
//!
//! - **INSERT**: a `BEFORE INSERT FOR EACH ROW` trigger, attached per
//!   managed table. Fires for every row of every INSERT shape (single-row,
//!   multi-row `VALUES`, `INSERT ... SELECT`) -- no statement-shape parsing
//!   needed, since a row trigger always sees the fully-resolved `NEW` row
//!   regardless of how the statement produced it. Rejects when the row's
//!   primary key already exists (hot or cold -- a hot match would also be
//!   caught by the table's own unique index, so checking both is harmless,
//!   just occasionally redundant with that constraint).
//!
//! - **UPDATE/DELETE**: no per-row trigger can catch this -- a row-level
//!   trigger only fires for rows the statement actually matched in the
//!   heap, and the whole failure mode is "the statement matched nothing in
//!   the heap because the target is cold-only." See
//!   `hooks::executor::guard_cold_only_update_delete` for the
//!   statement-level check this requires instead, restricted to the
//!   precise "WHERE is exactly an equality match on every primary-key
//!   column, nothing else" shape (upstream #122's Option B scoping
//!   deliberately deferred anything broader).
//!
//! Both pieces must not trip on `koldstore.hydrate_pk`/`update_row`/
//! `delete_row`'s own internal SPI statements -- [`suspended`] is a
//! session-local flag those functions hold for the duration of their own
//! native SQL calls.

use std::cell::Cell;

#[cfg(feature = "pg")]
use pgrx::datum::DatumWithOid;

thread_local! {
    static SUSPENDED: Cell<bool> = const { Cell::new(false) };
}

/// True while inside a `koldstore.hydrate_pk`/`update_row`/`delete_row`
/// call's own native SQL execution -- the cold-DML write guard must not
/// reject *those* statements, or the functions built to work around #122
/// would themselves become unusable the moment a table has cold data.
#[must_use]
pub(crate) fn suspended() -> bool {
    SUSPENDED.with(|flag| flag.get())
}

/// Runs `f` with the write guard suspended for this session, restoring the
/// previous state afterward (safe to nest).
pub(crate) fn with_guard_suspended<T>(f: impl FnOnce() -> T) -> T {
    let previous = SUSPENDED.with(|flag| flag.replace(true));
    let result = f();
    SUSPENDED.with(|flag| flag.set(previous));
    result
}

/// True when `pk_json` (a full row or a PK-only object -- extra keys are
/// ignored, see [`super::pk_join_predicate`]) currently exists anywhere
/// (hot or cold) for `table_oid`.
#[cfg(feature = "pg")]
pub(crate) fn cold_pk_exists(
    table_oid: pgrx::pg_sys::Oid,
    pk_json: &serde_json::Value,
) -> Result<bool, String> {
    Ok(super::locate_row_json(table_oid, pk_json)?.is_some())
}

/// Locates `pk_json` (hot or cold) and returns its full row as jsonb, for
/// [`residual_conditions_match`] to re-check extra WHERE conditions
/// against.
#[cfg(feature = "pg")]
pub(crate) fn locate_row(
    table_oid: pgrx::pg_sys::Oid,
    pk_json: &serde_json::Value,
) -> Result<Option<serde_json::Value>, String> {
    Ok(super::locate_row_json(table_oid, pk_json)?.map(|row| row.0))
}

/// Re-checks `residual` (every non-primary-key condition from the
/// original WHERE clause, see `hooks::pk_predicate::ResidualLeaf`) against
/// `row_json` (the row [`locate_row`] found for one PK candidate),
/// returning `true` only if *all* of them would also have matched.
///
/// Reuses `jsonb_populate_record`'s own type coercion instead of tracking
/// each residual column's real type: `row_json` is coerced through the
/// table's row type once (`loc`), and a second, small jsonb object built
/// purely from the residual leaves' own literal values is coerced the
/// same way (`extra`) -- comparing `loc.col OP extra.col` per leaf lets
/// PostgreSQL's own operators do a type-correct comparison (numeric,
/// text collation, ...) rather than a lossy textual one. `operator`'s SQL
/// text is safe to interpolate directly: it only ever comes from
/// `ComparisonOp::sql_symbol`'s fixed six-entry allowlist, never from
/// arbitrary input.
#[cfg(feature = "pg")]
pub(crate) fn residual_conditions_match(
    table_oid: pgrx::pg_sys::Oid,
    row_json: &serde_json::Value,
    residual: &[crate::hooks::pk_predicate::ResidualLeaf],
) -> Result<bool, String> {
    if residual.is_empty() {
        return Ok(true);
    }
    let relation = super::qualified_relation(table_oid)?;
    let quoted = relation.quoted();

    let mut extra = serde_json::Map::with_capacity(residual.len());
    let mut clauses = Vec::with_capacity(residual.len());
    for leaf in residual {
        let column = koldstore_common::sql::ident::quote_ident(&leaf.column);
        clauses.push(format!("(loc.{column} {} extra.{column})", leaf.operator.sql_symbol()));
        extra.insert(leaf.column.clone(), leaf.value.clone());
    }
    let sql = format!(
        "WITH loc AS (SELECT * FROM jsonb_populate_record(NULL::{quoted}, $1)), \
         extra AS (SELECT * FROM jsonb_populate_record(NULL::{quoted}, $2)) \
         SELECT ({}) FROM loc, extra",
        clauses.join(" AND ")
    );
    let args = [
        DatumWithOid::from(pgrx::JsonB(row_json.clone())),
        DatumWithOid::from(pgrx::JsonB(serde_json::Value::Object(extra))),
    ];
    match pgrx::Spi::get_one_with_args::<bool>(&sql, &args) {
        Ok(result) => Ok(result.unwrap_or(false)),
        Err(pgrx::spi::Error::InvalidPosition) => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

/// Maps every live column's `attnum` to its name, for
/// `hooks::pk_predicate::extract_pk_equality`'s `Var.varattno` lookups.
#[cfg(feature = "pg")]
pub(crate) fn column_attnum_map(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<std::collections::HashMap<i16, String>, String> {
    let sql = "SELECT attnum, attname::text FROM pg_catalog.pg_attribute \
               WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped";
    let args = [DatumWithOid::from(table_oid)];
    let mut map = std::collections::HashMap::new();
    pgrx::Spi::connect(|client| -> Result<(), String> {
        let mut cursor = client
            .select(sql, None, &args)
            .map_err(|error| error.to_string())?;
        while let Some(row) = cursor.next() {
            let attnum: i16 = row
                .get(1)
                .map_err(|error| error.to_string())?
                .ok_or("attnum column was unexpectedly NULL")?;
            let attname: String = row
                .get(2)
                .map_err(|error| error.to_string())?
                .ok_or("attname column was unexpectedly NULL")?;
            map.insert(attnum, attname);
        }
        Ok(())
    })?;
    Ok(map)
}

/// Backing function for the per-table `BEFORE INSERT` guard trigger (see
/// [`plan_insert_guard`]). Raises a SQL error when `row_json`'s primary key
/// already exists anywhere for `table_oid`; otherwise a harmless no-op.
///
/// Not itself `SECURITY DEFINER`-sensitive: purely a read-only existence
/// check, no different in sensitivity from an ordinary `SELECT` against the
/// table the caller must already have INSERT privilege on to reach this
/// trigger at all.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "_cold_insert_guard_check", schema = "koldstore")]
pub fn cold_insert_guard_check_pg(table_oid: pgrx::pg_sys::Oid, row: pgrx::JsonB, table_name: &str) {
    if suspended() {
        return;
    }
    let exists = cold_pk_exists(table_oid, &row.0)
        .unwrap_or_else(|error| pgrx::error!("cold-DML insert guard failed: {error}"));
    if exists {
        pgrx::error!(
            "koldstore: refusing INSERT on managed table {table_name} -- this primary key already \
             exists (hot or cold); use koldstore.update_row() to change it, or koldstore.hydrate_pk() \
             to materialize it before an ON CONFLICT upsert (upstream issue #122)"
        );
    }
}

/// Builds a truncation-safe `<= 63`-byte identifier from `prefix`+`suffix`,
/// truncating `prefix` (never `suffix`) when the combination would
/// otherwise exceed PostgreSQL's `NAMEDATALEN - 1` limit.
fn bounded_identifier(prefix: &str, suffix: &str) -> String {
    const MAX_LEN: usize = 63;
    let combined = format!("{prefix}{suffix}");
    if combined.len() <= MAX_LEN {
        return combined;
    }
    let keep = MAX_LEN.saturating_sub(suffix.len());
    let mut truncated = prefix.as_bytes();
    while truncated.len() > keep && !truncated.is_empty() {
        truncated = &truncated[..truncated.len() - 1];
    }
    // Never split a UTF-8 codepoint -- back off further if the byte cut
    // landed mid-character.
    while !truncated.is_empty() && std::str::from_utf8(truncated).is_err() {
        truncated = &truncated[..truncated.len() - 1];
    }
    format!("{}{suffix}", std::str::from_utf8(truncated).unwrap_or(""))
}

/// The guard function/trigger names for one managed table.
#[cfg(feature = "pg")]
pub(crate) struct InsertGuardNames {
    pub function: koldstore_common::QualifiedTableName,
    pub trigger: String,
}

#[cfg(feature = "pg")]
pub(crate) fn insert_guard_names(source: &koldstore_common::QualifiedTableName) -> InsertGuardNames {
    let base = source
        .schema
        .as_deref()
        .map_or_else(|| source.name.clone(), |schema| format!("{schema}_{}", source.name));
    InsertGuardNames {
        function: koldstore_common::QualifiedTableName {
            schema: Some("koldstore".to_string()),
            name: bounded_identifier(&base, "__cold_ins_guard"),
        },
        trigger: bounded_identifier(&base, "__cold_ins_guard_trg"),
    }
}

/// Plans (as raw SQL text, executed the same way the mirror PK-mutation
/// guard's statements already are -- see `sql/migrate/manage.rs`) the
/// `BEFORE INSERT FOR EACH ROW` guard trigger for one managed table.
///
/// The trigger body is deliberately plain PL/pgSQL calling
/// [`cold_insert_guard_check_pg`] rather than a Rust `#[pg_trigger]`: this
/// reuses `to_jsonb(NEW)`'s already-correct, generic typed-row-to-jsonb
/// conversion instead of hand-walking `NEW`'s columns from Rust, mirroring
/// how `koldstore-wal-mirror`'s own PK-mutation guard trigger
/// (`plan_mirror_pk_guard`) is written.
#[cfg(feature = "pg")]
#[must_use]
pub(crate) fn plan_insert_guard(source: &koldstore_common::QualifiedTableName) -> Vec<String> {
    let names = insert_guard_names(source);
    let function_name = names.function.quoted();
    let trigger_name = koldstore_common::sql::ident::quote_ident(&names.trigger);
    let source_quoted = source.quoted();
    let table_display = source.quoted();

    let function_sql = format!(
        r#"
CREATE OR REPLACE FUNCTION {function_name}()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, koldstore
AS $$
BEGIN
    PERFORM koldstore._cold_insert_guard_check(TG_RELID, to_jsonb(NEW), {table_literal});
    RETURN NEW;
END;
$$
"#,
        table_literal = quote_sql_literal(&table_display),
    );
    let trigger_sql = format!(
        "CREATE TRIGGER {trigger_name} BEFORE INSERT ON {source_quoted} \
         FOR EACH ROW EXECUTE FUNCTION {function_name}()"
    );
    let drop_trigger_sql = drop_trigger_if_present_sql(&names.trigger, &source_quoted);
    let drop_function_sql = format!("DROP FUNCTION IF EXISTS {function_name}()");

    vec![drop_trigger_sql, drop_function_sql, function_sql, trigger_sql]
}

/// Idempotently tears down the insert guard trigger/function for one
/// managed table (mirrors `plan_mirror_source_teardown`'s style).
#[cfg(feature = "pg")]
#[must_use]
pub(crate) fn plan_insert_guard_teardown(source: &koldstore_common::QualifiedTableName) -> Vec<String> {
    let names = insert_guard_names(source);
    let source_quoted = source.quoted();
    vec![
        drop_trigger_if_present_sql(&names.trigger, &source_quoted),
        format!("DROP FUNCTION IF EXISTS {}()", names.function.quoted()),
    ]
}

fn drop_trigger_if_present_sql(trigger_name: &str, source_table: &str) -> String {
    let quoted_trigger = koldstore_common::sql::ident::quote_ident(trigger_name);
    format!(
        r#"DO $koldstore_drop_trigger$
BEGIN
  BEGIN
    EXECUTE 'DROP TRIGGER {quoted_trigger} ON {source_table}';
  EXCEPTION WHEN undefined_object THEN
    NULL;
  END;
END
$koldstore_drop_trigger$;"#
    )
}

/// Single-quotes and escapes a value for embedding as a SQL string literal
/// inside generated DDL text (not a bind parameter -- this is building the
/// trigger function's *source code*, evaluated once at `CREATE FUNCTION`
/// time, not per-row).
fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
