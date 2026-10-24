//! Global enforcement of outbound foreign keys across hot and cold rows.
//!
//! For `T -> P` with `P` unmanaged and `NO ACTION`/`RESTRICT` actions, the native FK triggers
//! already check a hot child's parent exactly. What they miss is a parent `DELETE`, or a change
//! of its referenced key, that orphans a **cold** child row: the native check only scans `T`'s
//! heap. A guard trigger on `P` (planned alongside `T`'s write guard, see
//! `guard::plan_insert_guard`) closes that gap by looking for cold children in the merged view.
//!
//! With `CASCADE`/`SET NULL`/`SET DEFAULT` the guard instead hydrates the cold children (the
//! trigger is `BEFORE`, so this runs ahead of the native action): they become ordinary hot rows
//! and the native referential action then fires on them like on any other child. The parent may
//! itself be managed (the key may reference its primary key or any other unique key): the child-side guard of
//! `inbound_fk` hydrates a cold parent for a child write, and this guard covers the cold children.

use pgrx::datum::DatumWithOid;

use super::guard::{bounded_identifier, quote_sql_literal};

struct OutboundFk {
    conoid: u32,
    name: String,
    parent: String,
    fk_cols: Vec<String>,
    ref_cols: Vec<String>,
}

fn resolve_oid(source: &koldstore_common::QualifiedTableName) -> Option<pgrx::pg_sys::Oid> {
    pgrx::Spi::get_one::<pgrx::pg_sys::Oid>(&format!("SELECT {}::regclass::oid", quote_sql_literal(&source.quoted())))
        .ok()
        .flatten()
}

/// Statements removing every parent-side guard created for `child_oid` (found by the child oid
/// baked into the function body, so renames and dropped FKs are covered).
fn teardown_statements(child_oid: pgrx::pg_sys::Oid) -> Vec<String> {
    // Functions are found by the oid baked into their body (not through their triggers), so a
    // function whose table was dropped is cleaned up as well.
    let sql = "SELECT t.tgname::text, t.tgrelid::regclass::text, p.oid::regprocedure::text \
               FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
               LEFT JOIN pg_catalog.pg_trigger t ON t.tgfoid = p.oid AND NOT t.tgisinternal \
               WHERE n.nspname = 'koldstore' \
                 AND p.prosrc LIKE '%_cold_fk_parent_guard_check(' || $1::text || '::oid,%'";
    let args = [DatumWithOid::from(child_oid.to_u32() as i64)];
    let rows: Vec<(Option<String>, Option<String>, String)> = pgrx::Spi::connect(|client| {
        client
            .select(sql, None, &args)?
            .map(|row| Ok((row.get::<String>(1)?, row.get::<String>(2)?, row.get::<String>(3)?.unwrap_or_default())))
            .collect::<Result<Vec<_>, pgrx::spi::Error>>()
    })
    .unwrap_or_default();
    let mut statements = Vec::new();
    for (trigger, relation, _) in &rows {
        if let (Some(trigger), Some(relation)) = (trigger, relation) {
            statements.push(format!(
                "DROP TRIGGER IF EXISTS {} ON {relation}",
                koldstore_common::sql::ident::quote_ident(trigger)
            ));
        }
    }
    let mut functions: Vec<&String> = rows.iter().map(|(_, _, function)| function).collect();
    functions.sort();
    functions.dedup();
    for function in functions {
        statements.push(format!("DROP FUNCTION IF EXISTS {function}"));
    }
    statements
}

fn outbound_fks(child_oid: pgrx::pg_sys::Oid) -> Result<Vec<OutboundFk>, String> {
    let sql = "SELECT c.oid::int8, c.conname::text, c.confrelid::regclass::text, \
                      (SELECT jsonb_agg(a.attname::text ORDER BY k.ord) FROM unnest(c.conkey) WITH ORDINALITY k(attnum, ord) \
                        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum), \
                      (SELECT jsonb_agg(a.attname::text ORDER BY k.ord) FROM unnest(c.confkey) WITH ORDINALITY k(attnum, ord) \
                        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.attnum) \
               FROM pg_catalog.pg_constraint c \
               WHERE c.conrelid = $1 AND c.contype = 'f' \
               ORDER BY c.oid";
    let args = [DatumWithOid::from(child_oid)];
    pgrx::Spi::connect(|client| -> Result<Vec<OutboundFk>, String> {
        let mut found = Vec::new();
        for row in client.select(sql, None, &args).map_err(|e| e.to_string())? {
            let conoid: i64 = row.get(1).map_err(|e| e.to_string())?.unwrap_or_default();
            let cols = |index: usize| -> Result<Vec<String>, String> {
                let value: pgrx::JsonB = row.get(index).map_err(|e| e.to_string())?.ok_or("FK columns were NULL")?;
                serde_json::from_value(value.0).map_err(|e| e.to_string())
            };
            found.push(OutboundFk {
                conoid: u32::try_from(conoid).map_err(|e| e.to_string())?,
                name: row.get(2).map_err(|e| e.to_string())?.unwrap_or_default(),
                parent: row.get(3).map_err(|e| e.to_string())?.unwrap_or_default(),
                fk_cols: cols(4)?,
                ref_cols: cols(5)?,
            });
        }
        Ok(found)
    })
}

/// Statements (re)creating the parent-side guard for each globally enforced outbound FK of
/// `source`, after removing any earlier ones. Empty when the table cannot be resolved.
pub(crate) fn plan_fk_parent_guards(source: &koldstore_common::QualifiedTableName, install: bool) -> Vec<String> {
    let Some(child_oid) = resolve_oid(source) else { return Vec::new() };
    let mut statements = teardown_statements(child_oid);
    if !install {
        return statements;
    }
    let fks = outbound_fks(child_oid).unwrap_or_else(|error| pgrx::error!("koldstore: could not read foreign keys of {}: {error}", source.quoted()));
    let base = koldstore_common::derived_base_name(source.schema.as_deref(), &source.name);
    for fk in fks {
        let name = bounded_identifier(&base, &format!("__fk{}", fk.conoid));
        let function = format!("koldstore.{}", koldstore_common::sql::ident::quote_ident(&name));
        let trigger = koldstore_common::sql::ident::quote_ident(&name);
        let json = |cols: &[String]| quote_sql_literal(&serde_json::to_string(cols).unwrap_or_default());
        let update_of = fk.ref_cols.iter().map(|c| koldstore_common::sql::ident::quote_ident(c)).collect::<Vec<_>>().join(", ");
        let check = |new: &str| {
            format!(
                "PERFORM koldstore._cold_fk_parent_guard_check({}::oid, TG_RELID, {}::jsonb, {}::jsonb, to_jsonb(OLD), {new}, {});",
                child_oid.to_u32(),
                json(&fk.fk_cols),
                json(&fk.ref_cols),
                quote_sql_literal(&fk.name),
            )
        };
        statements.push(format!(
            "CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER \
             SET search_path = pg_catalog, koldstore AS $$ BEGIN \
             IF TG_OP = 'DELETE' THEN {} RETURN OLD; END IF; {} RETURN NEW; END; $$",
            check("NULL"),
            check("to_jsonb(NEW)")
        ));
        statements.push(format!(
            "CREATE TRIGGER {trigger} BEFORE DELETE OR UPDATE OF {update_of} ON {} \
             FOR EACH ROW EXECUTE FUNCTION {function}()",
            fk.parent
        ));
    }
    statements
}

fn cold_children(
    child_oid: pgrx::pg_sys::Oid,
    parent_oid: pgrx::pg_sys::Oid,
    fk_cols: &[String],
    ref_cols: &[String],
    parent_row: &serde_json::Value,
) -> Result<i64, String> {
    let child = super::qualified_relation(child_oid)?.quoted();
    let parent = super::qualified_relation(parent_oid)?.quoted();
    let quote = koldstore_common::sql::ident::quote_ident;
    let join = fk_cols
        .iter()
        .zip(ref_cols)
        .map(|(f, r)| format!("t.{} = p.{}", quote(f), quote(r)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!("SELECT count(*) FROM {child} AS t, jsonb_populate_record(NULL::{parent}, $1) AS p WHERE {join}");
    let args = [DatumWithOid::from(pgrx::JsonB(parent_row.clone()))];
    let run = || -> Result<i64, String> {
        crate::txn_writes::with_check_suppressed(|| pgrx::Spi::get_one_with_args::<i64>(&sql, &args))
            .map(|n| n.unwrap_or(0))
            .map_err(|e| e.to_string())
    };
    let merged = run()?;
    if merged == 0 {
        return Ok(0);
    }
    let hot = {
        let _hot_only = crate::merge_scan::pg::HotOnlyRelation::new(child_oid);
        run()?
    };
    Ok(merged - hot)
}

/// Backing function of the parent-side guard trigger. `new_row` is NULL for a DELETE.
#[pgrx::pg_extern(name = "_cold_fk_parent_guard_check", schema = "koldstore")]
pub fn cold_fk_parent_guard_check_pg(
    child_oid: pgrx::pg_sys::Oid,
    parent_oid: pgrx::pg_sys::Oid,
    fk_cols: pgrx::JsonB,
    ref_cols: pgrx::JsonB,
    old_row: pgrx::JsonB,
    new_row: Option<pgrx::JsonB>,
    constraint_name: &str,
) {
    // Deliberately not skipped while this table's write guard is suspended (`delete_row`,
    // `update_row`): those run ordinary DELETE/UPDATE statements whose cold children still count.
    let fk_cols: Vec<String> = serde_json::from_value(fk_cols.0).unwrap_or_else(|e| pgrx::error!("cold-DML FK guard failed: {e}"));
    let ref_cols: Vec<String> = serde_json::from_value(ref_cols.0).unwrap_or_else(|e| pgrx::error!("cold-DML FK guard failed: {e}"));
    if ref_cols.iter().any(|col| old_row.0.get(col).is_none_or(serde_json::Value::is_null)) {
        return;
    }
    if let Some(new) = &new_row {
        if ref_cols.iter().all(|col| new.0.get(col) == old_row.0.get(col)) {
            return;
        }
    }
    let written_earlier = crate::txn_writes::was_written(child_oid);
    let fail = |e: String| -> ! { pgrx::error!("cold-DML FK guard failed: {e}") };
    if referential_action_applies(child_oid, constraint_name, new_row.is_some()) {
        hydrate_cold_children(child_oid, parent_oid, &fk_cols, &ref_cols, &old_row.0).unwrap_or_else(|e| fail(e));
        return;
    }
    let mut count = cold_children(child_oid, parent_oid, &fk_cols, &ref_cols, &old_row.0).unwrap_or_else(|e| fail(e));
    if count > 0 && !written_earlier && crate::guc::insert_guard_fence() && crate::guc::hydrate_fence_mirror() {
        count = super::key_lock::with_current_view(|| cold_children(child_oid, parent_oid, &fk_cols, &ref_cols, &old_row.0))
            .unwrap_or_else(|e| fail(e));
    }
    if count > 0 {
        pgrx::ereport!(
            ERROR,
            pgrx::PgSqlErrorCode::ERRCODE_FOREIGN_KEY_VIOLATION,
            format!(
                "update or delete on table violates foreign key constraint \"{constraint_name}\": \
                 {count} archived (cold) row(s) of a koldstore-managed table still reference this key"
            )
        );
    }
}

/// True when the constraint's `ON DELETE` (or, for a key change, `ON UPDATE`) action is
/// `CASCADE`, `SET NULL` or `SET DEFAULT` rather than `NO ACTION`/`RESTRICT`.
fn referential_action_applies(child_oid: pgrx::pg_sys::Oid, constraint_name: &str, is_update: bool) -> bool {
    let column = if is_update { "confupdtype" } else { "confdeltype" };
    let sql = format!(
        "SELECT {column}::text IN ('c', 'n', 'd') FROM pg_catalog.pg_constraint WHERE conrelid = $1 AND conname = $2 AND contype = 'f'"
    );
    let args = [DatumWithOid::from(child_oid), DatumWithOid::from(constraint_name)];
    pgrx::Spi::get_one_with_args::<bool>(&sql, &args).ok().flatten().unwrap_or(false)
}

/// Moves the cold children of `parent_row` into the heap so the native referential action
/// (which fires after this `BEFORE` trigger) reaches them.
fn hydrate_cold_children(
    child_oid: pgrx::pg_sys::Oid,
    parent_oid: pgrx::pg_sys::Oid,
    fk_cols: &[String],
    ref_cols: &[String],
    parent_row: &serde_json::Value,
) -> Result<(), String> {
    let parent = super::qualified_relation(parent_oid)?.quoted();
    let quote = koldstore_common::sql::ident::quote_ident;
    let join = fk_cols
        .iter()
        .zip(ref_cols)
        .map(|(f, r)| format!("t.{} = p.{}", quote(f), quote(r)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let where_sql = format!(
        "EXISTS (SELECT 1 FROM jsonb_populate_record(NULL::{parent}, {}::jsonb) AS p WHERE {join})",
        quote_sql_literal(&parent_row.to_string())
    );
    hydrate_cold_matching(child_oid, &where_sql)
}

/// Hydrates the cold-only rows of managed `child_oid` matching `where_sql` (over alias `t`), under key
/// locks and a fresh mirror view, so that an ordinary statement (or the native referential
/// action) reaches them.
pub(crate) fn hydrate_cold_matching(child_oid: pgrx::pg_sys::Oid, where_sql: &str) -> Result<(), String> {
    hydrate_cold_matching_impl(child_oid, where_sql, true)
}

/// Like [`hydrate_cold_matching`] for a parent row a child write needs: it only materializes the
/// referenced row, so earlier writes of this transaction to the table do not matter (the same
/// leniency `hydrate_pk_locked` has) and the table is not counted as written afterwards.
pub(crate) fn hydrate_cold_parent_matching(parent_oid: pgrx::pg_sys::Oid, where_sql: &str) -> Result<(), String> {
    hydrate_cold_matching_impl(parent_oid, where_sql, false)
}

fn hydrate_cold_matching_impl(child_oid: pgrx::pg_sys::Oid, where_sql: &str, strict: bool) -> Result<(), String> {
    let fetch = || super::guard::cold_only_matching_rows_with_params(child_oid, where_sql, &[]);
    // A stale mirror can only over-report cold rows, so a plain look settles the common case.
    let probe = fetch()?;
    if probe.is_empty() {
        return Ok(());
    }
    if strict && crate::txn_writes::was_written(child_oid) {
        return Err(format!(
            "cannot apply the foreign key action to cold rows of {}: this transaction already modified the table, \
             so they cannot be hydrated safely; run the parent change in its own transaction",
            crate::txn_writes::relation_display_name(child_oid)
        ));
    }
    let keys: Vec<String> = probe.iter().map(|(key, _)| key.clone()).collect();
    super::key_lock::lock_keys_bounded(child_oid, &keys)?;
    let rows = super::key_lock::with_current_view(fetch)?;
    let cap = crate::guc::max_hydrate_rows();
    if rows.len() > cap {
        return Err(format!(
            "the foreign key action reaches {} cold row(s) of {}, more than koldstore.max_hydrate_rows ({cap})",
            rows.len(),
            crate::txn_writes::relation_display_name(child_oid)
        ));
    }
    let extra: Vec<String> = rows.iter().map(|(key, _)| key.clone()).filter(|key| !keys.contains(key)).collect();
    super::key_lock::lock_keys_bounded(child_oid, &extra)?;
    let rows: Vec<serde_json::Value> = rows.into_iter().map(|(_, row)| row).collect();
    super::hydrate_rows(child_oid, &rows)?;
    if strict {
        crate::txn_writes::record_managed_write(child_oid);
    }
    Ok(())
}
