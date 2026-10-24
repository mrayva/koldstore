//! Global enforcement of inbound foreign keys: an unmanaged table `C` referencing a managed
//! table `T`'s primary key with `NO ACTION`/`RESTRICT` actions.
//!
//! The native checks only see `T`'s heap, so two things are missing for cold parent rows:
//!
//! * **Child writes.** `INSERT`/`UPDATE` on `C` probes `T` for the key; a cold-only parent looks
//!   absent. A guard trigger on `C` therefore hydrates the parent row first (`hydrate_pk`
//!   semantics), after which the native check finds and locks a hot row. Flush later prunes hot
//!   rows with RI triggers off, so a hydrated parent returns to cold storage unharmed.
//! * **Cold deletes.** `purge_where` tombstones cold rows without a heap `DELETE`, so no native
//!   check fires; it calls [`ensure_no_children`] first. `delete_row`/`update_row` hydrate and
//!   then run an ordinary heap statement, which the native triggers already guard.
//!
//! Referencing a managed table through a non-primary-key unique constraint, a managed referencing
//! table, and `CASCADE`/`SET NULL`/`SET DEFAULT` actions stay unsupported.

use pgrx::datum::DatumWithOid;

use super::guard::{bounded_identifier, quote_sql_literal};

pub(crate) struct InboundFk {
    pub conoid: u32,
    pub name: String,
    pub child: String,
    pub fk_cols: Vec<String>,
    pub ref_cols: Vec<String>,
}

/// Inbound foreign keys of `parent_oid` the guard enforces.
pub(crate) fn inbound_fks(parent_oid: pgrx::pg_sys::Oid) -> Result<Vec<InboundFk>, String> {
    let sql = "SELECT c.oid::int8, c.conname::text, c.conrelid::regclass::text, \
                      (SELECT jsonb_agg(a.attname::text ORDER BY k.ord) FROM unnest(c.conkey) WITH ORDINALITY k(attnum, ord) \
                        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum), \
                      (SELECT jsonb_agg(a.attname::text ORDER BY k.ord) FROM unnest(c.confkey) WITH ORDINALITY k(attnum, ord) \
                        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.attnum) \
               FROM pg_catalog.pg_constraint c \
               WHERE c.confrelid = $1 AND c.contype = 'f' AND c.confdeltype IN ('a', 'r') AND c.confupdtype IN ('a', 'r') \
                 AND NOT EXISTS (SELECT 1 FROM koldstore.schemas ks WHERE ks.table_oid = c.conrelid AND ks.active) \
                 AND ARRAY(SELECT x FROM unnest(c.confkey) x ORDER BY x) = \
                     (SELECT ARRAY(SELECT x FROM unnest(i.indkey::int2[]) x ORDER BY x) FROM pg_catalog.pg_index i \
                       WHERE i.indrelid = c.confrelid AND i.indisprimary) \
               ORDER BY c.oid";
    let args = [DatumWithOid::from(parent_oid)];
    pgrx::Spi::connect(|client| -> Result<Vec<InboundFk>, String> {
        let mut found = Vec::new();
        for row in client.select(sql, None, &args).map_err(|e| e.to_string())? {
            let conoid: i64 = row.get(1).map_err(|e| e.to_string())?.unwrap_or_default();
            let cols = |index: usize| -> Result<Vec<String>, String> {
                let value: pgrx::JsonB = row.get(index).map_err(|e| e.to_string())?.ok_or("FK columns were NULL")?;
                serde_json::from_value(value.0).map_err(|e| e.to_string())
            };
            found.push(InboundFk {
                conoid: u32::try_from(conoid).map_err(|e| e.to_string())?,
                name: row.get(2).map_err(|e| e.to_string())?.unwrap_or_default(),
                child: row.get(3).map_err(|e| e.to_string())?.unwrap_or_default(),
                fk_cols: cols(4)?,
                ref_cols: cols(5)?,
            });
        }
        Ok(found)
    })
}

fn resolve_oid(source: &koldstore_common::QualifiedTableName) -> Option<pgrx::pg_sys::Oid> {
    pgrx::Spi::get_one::<pgrx::pg_sys::Oid>(&format!("SELECT {}::regclass::oid", quote_sql_literal(&source.quoted())))
        .ok()
        .flatten()
}

/// Statements removing every child-side guard created for `parent_oid` (found by the parent oid
/// baked into the function body).
fn teardown_statements(parent_oid: pgrx::pg_sys::Oid) -> Vec<String> {
    let sql = "SELECT t.tgname::text, t.tgrelid::regclass::text, p.oid::regprocedure::text \
               FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_proc p ON p.oid = t.tgfoid \
               JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
               WHERE n.nspname = 'koldstore' AND NOT t.tgisinternal \
                 AND p.prosrc LIKE '%_cold_fk_child_hydrate(' || $1::text || '::oid,%'";
    let args = [DatumWithOid::from(parent_oid.to_u32() as i64)];
    let rows: Vec<(String, String, String)> = pgrx::Spi::connect(|client| {
        client
            .select(sql, None, &args)?
            .map(|row| Ok((row.get::<String>(1)?.unwrap_or_default(), row.get::<String>(2)?.unwrap_or_default(), row.get::<String>(3)?.unwrap_or_default())))
            .collect::<Result<Vec<_>, pgrx::spi::Error>>()
    })
    .unwrap_or_default();
    let mut statements = Vec::new();
    for (trigger, relation, _) in &rows {
        statements.push(format!("DROP TRIGGER IF EXISTS {} ON {relation}", koldstore_common::sql::ident::quote_ident(trigger)));
    }
    for (_, _, function) in &rows {
        statements.push(format!("DROP FUNCTION IF EXISTS {function}"));
    }
    statements
}

/// Statements (re)creating the child-side hydrate trigger on each referencing table of `source`.
pub(crate) fn plan_fk_child_guards(source: &koldstore_common::QualifiedTableName, install: bool) -> Vec<String> {
    let Some(parent_oid) = resolve_oid(source) else { return Vec::new() };
    let mut statements = teardown_statements(parent_oid);
    if !install {
        return statements;
    }
    let fks = inbound_fks(parent_oid)
        .unwrap_or_else(|error| pgrx::error!("koldstore: could not read foreign keys referencing {}: {error}", source.quoted()));
    let base = koldstore_common::derived_base_name(source.schema.as_deref(), &source.name);
    let quote = koldstore_common::sql::ident::quote_ident;
    for fk in fks {
        let name = bounded_identifier(&base, &format!("__ifk{}", fk.conoid));
        let function = format!("koldstore.{}", quote(&name));
        let json = |cols: &[String]| quote_sql_literal(&serde_json::to_string(cols).unwrap_or_default());
        let update_of = fk.fk_cols.iter().map(|c| quote(c)).collect::<Vec<_>>().join(", ");
        statements.push(format!(
            "CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER \
             SET search_path = pg_catalog, koldstore AS $$ BEGIN \
             PERFORM koldstore._cold_fk_child_hydrate({}::oid, {}::jsonb, {}::jsonb, to_jsonb(NEW)); RETURN NEW; END; $$",
            parent_oid.to_u32(),
            json(&fk.fk_cols),
            json(&fk.ref_cols),
        ));
        statements.push(format!(
            "CREATE TRIGGER {} BEFORE INSERT OR UPDATE OF {update_of} ON {} FOR EACH ROW EXECUTE FUNCTION {function}()",
            quote(&name),
            fk.child
        ));
    }
    statements
}

/// Child-side guard: makes the parent row a hot row when it currently exists only in cold storage.
#[pgrx::pg_extern(name = "_cold_fk_child_hydrate", schema = "koldstore")]
pub fn cold_fk_child_hydrate_pg(parent_oid: pgrx::pg_sys::Oid, fk_cols: pgrx::JsonB, ref_cols: pgrx::JsonB, new_row: pgrx::JsonB) {
    if super::guard::suspended() {
        return;
    }
    let fk_cols: Vec<String> = serde_json::from_value(fk_cols.0).unwrap_or_else(|e| pgrx::error!("cold-DML FK guard failed: {e}"));
    let ref_cols: Vec<String> = serde_json::from_value(ref_cols.0).unwrap_or_else(|e| pgrx::error!("cold-DML FK guard failed: {e}"));
    let mut pk = serde_json::Map::new();
    for (fk, reference) in fk_cols.iter().zip(&ref_cols) {
        match new_row.0.get(fk).filter(|value| !value.is_null()) {
            // MATCH SIMPLE: any NULL column disables the check.
            None => return,
            Some(value) => {
                pk.insert(reference.clone(), value.clone());
            }
        }
    }
    let pk = serde_json::Value::Object(pk);
    let fail = |e: String| -> ! { pgrx::error!("cold-DML FK guard failed: {e}") };
    if hot_exists(parent_oid, &pk).unwrap_or_else(|e| fail(e)) {
        return;
    }
    super::hydrate_pk_locked(parent_oid, &pk).unwrap_or_else(|e| fail(e));
}

fn hot_exists(table_oid: pgrx::pg_sys::Oid, pk: &serde_json::Value) -> Result<bool, String> {
    let quoted = super::qualified_relation(table_oid)?.quoted();
    let predicate = super::pk_join_predicate(&super::primary_key_columns(table_oid)?);
    let sql = format!(
        "SELECT EXISTS (SELECT 1 FROM {quoted} AS t, jsonb_populate_record(NULL::{quoted}, $1) AS pk WHERE {predicate})"
    );
    let args = [DatumWithOid::from(pgrx::JsonB(pk.clone()))];
    let _hot_only = crate::merge_scan::pg::HotOnlyRelation::new(table_oid);
    crate::txn_writes::with_check_suppressed(|| pgrx::Spi::get_one_with_args::<bool>(&sql, &args))
        .map(|found| found.unwrap_or(false))
        .map_err(|e| e.to_string())
}

/// Refuses a `purge_where` that would orphan referencing rows. `predicate` is over `t`.
/// Must run where the merged view of the table is readable (after the mirror fence).
pub(crate) fn ensure_no_children(table_oid: pgrx::pg_sys::Oid, predicate: &str) -> Result<(), String> {
    let quoted = super::qualified_relation(table_oid)?.quoted();
    let quote = koldstore_common::sql::ident::quote_ident;
    for fk in inbound_fks(table_oid)? {
        let join = fk
            .fk_cols
            .iter()
            .zip(&fk.ref_cols)
            .map(|(f, r)| format!("c.{} = t.{}", quote(f), quote(r)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "SELECT EXISTS (SELECT 1 FROM {} AS c WHERE EXISTS (SELECT 1 FROM {quoted} AS t WHERE ({predicate}) AND {join}))",
            fk.child
        );
        let found = crate::txn_writes::with_check_suppressed(|| pgrx::Spi::get_one::<bool>(&sql))
            .map_err(|e| e.to_string())?
            .unwrap_or(false);
        if found {
            pgrx::ereport!(
                ERROR,
                pgrx::PgSqlErrorCode::ERRCODE_FOREIGN_KEY_VIOLATION,
                format!(
                    "purge_where violates foreign key constraint \"{}\" on table {}: \
                     rows matching the predicate are still referenced",
                    fk.name, fk.child
                )
            );
        }
    }
    Ok(())
}
