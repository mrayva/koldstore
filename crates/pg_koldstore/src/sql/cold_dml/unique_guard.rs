//! Global enforcement of non-primary-key UNIQUE constraints across hot and cold
//! storage (limitations: "Unique and Foreign Key Constraints").
//!
//! The table's own unique index only sees hot rows, so a value that already lives in a cold
//! segment could be inserted again. The per-table guard trigger (see
//! `guard::plan_insert_guard`) therefore also calls [`cold_unique_guard_check_pg`] for every
//! INSERT, and for every UPDATE that changes a constrained column. The check reads the merged
//! hot+cold view, so the cold side is pruned by the segment min/max statistics the merge scan
//! already applies to an equality predicate.
//!
//! Supported shapes are plain unique indexes/constraints over simple columns. Partial and
//! expression unique indexes are skipped with a warning when the guard is attached: their
//! predicate cannot be re-evaluated against cold rows here.

use pgrx::datum::DatumWithOid;

/// One enforced unique constraint, as embedded in the generated trigger function.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct UniqueSpec {
    pub cols: Vec<String>,
    /// `NULLS NOT DISTINCT`: NULL values conflict with each other.
    #[serde(default)]
    pub nnd: bool,
}

/// The unique constraints of `table_oid` the guard can enforce, plus the names of the ones it
/// cannot (partial or expression indexes).
pub(crate) fn unique_specs(table_oid: pgrx::pg_sys::Oid) -> Result<(Vec<UniqueSpec>, Vec<String>), String> {
    let sql = "SELECT c.relname::text, i.indpred IS NULL AND i.indexprs IS NULL AS supported, \
                      i.indnullsnotdistinct, \
                      coalesce((SELECT jsonb_agg(a.attname::text ORDER BY k.ord) \
                                FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) \
                                JOIN pg_catalog.pg_attribute a \
                                  ON a.attrelid = i.indrelid AND a.attnum = k.attnum), '[]'::jsonb) \
               FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid \
               WHERE i.indrelid = $1 AND i.indisunique AND NOT i.indisprimary AND i.indisvalid \
               ORDER BY c.relname";
    let args = [DatumWithOid::from(table_oid)];
    let mut specs = Vec::new();
    let mut skipped = Vec::new();
    pgrx::Spi::connect(|client| -> Result<(), String> {
        for row in client.select(sql, None, &args).map_err(|error| error.to_string())? {
            let name: String = row.get(1).map_err(|e| e.to_string())?.unwrap_or_default();
            let supported: bool = row.get(2).map_err(|e| e.to_string())?.unwrap_or(false);
            let nnd: bool = row.get(3).map_err(|e| e.to_string())?.unwrap_or(false);
            let cols: pgrx::JsonB = row.get(4).map_err(|e| e.to_string())?.ok_or("columns were NULL")?;
            if !supported {
                skipped.push(name);
                continue;
            }
            let cols: Vec<String> = serde_json::from_value(cols.0).map_err(|e| e.to_string())?;
            specs.push(UniqueSpec { cols, nnd });
        }
        Ok(())
    })?;
    Ok((specs, skipped))
}

/// Value of `column` in the row object, `None` for SQL NULL / absent.
fn column_value<'a>(row: &'a serde_json::Value, column: &str) -> Option<&'a serde_json::Value> {
    row.get(column).filter(|value| !value.is_null())
}

/// True when the new row could conflict on `spec`: it has all-non-NULL values (or the
/// constraint is NULLS NOT DISTINCT) and, for an UPDATE, at least one of them changed.
fn needs_check(spec: &UniqueSpec, new: &serde_json::Value, old: Option<&serde_json::Value>) -> bool {
    if !spec.nnd && spec.cols.iter().any(|col| column_value(new, col).is_none()) {
        return false;
    }
    match old {
        Some(old) => spec.cols.iter().any(|col| new.get(col) != old.get(col)),
        None => true,
    }
}

/// True when a row other than the one identified by `own_pk` already holds the new row's
/// values for `spec`, in the merged hot+cold view.
fn conflict_exists(
    table_oid: pgrx::pg_sys::Oid,
    spec: &UniqueSpec,
    new: &serde_json::Value,
    pk_columns: &[String],
) -> Result<bool, String> {
    let relation = super::qualified_relation(table_oid)?.quoted();
    let quote = koldstore_common::sql::ident::quote_ident;
    let same = spec
        .cols
        .iter()
        .map(|col| {
            let col = quote(col);
            if spec.nnd {
                format!("t.{col} IS NOT DISTINCT FROM n.{col}")
            } else {
                format!("t.{col} = n.{col}")
            }
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let other_row = pk_columns
        .iter()
        .map(|col| format!("t.{0} IS NOT DISTINCT FROM n.{0}", quote(col)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "SELECT EXISTS (SELECT 1 FROM {relation} AS t, \
         jsonb_populate_record(NULL::{relation}, $1) AS n WHERE ({same}) AND NOT ({other_row}))"
    );
    let args = [DatumWithOid::from(pgrx::JsonB(new.clone()))];
    crate::txn_writes::with_check_suppressed(|| pgrx::Spi::get_one_with_args::<bool>(&sql, &args))
        .map(|found| found.unwrap_or(false))
        .map_err(|error| error.to_string())
}

/// Backing function for the unique half of the per-table guard trigger. `old_row` is NULL for
/// an INSERT. Raises a unique-violation error when a conflicting row exists in hot or cold.
#[pgrx::pg_extern(name = "_cold_unique_guard_check", schema = "koldstore")]
pub fn cold_unique_guard_check_pg(
    table_oid: pgrx::pg_sys::Oid,
    new_row: pgrx::JsonB,
    old_row: Option<pgrx::JsonB>,
    specs: pgrx::JsonB,
    table_name: &str,
) {
    if super::guard::suspended() {
        return;
    }
    let specs: Vec<UniqueSpec> = serde_json::from_value(specs.0)
        .unwrap_or_else(|error| pgrx::error!("cold-DML unique guard failed: bad spec: {error}"));
    let pk_columns = super::primary_key_columns(table_oid)
        .unwrap_or_else(|error| pgrx::error!("cold-DML unique guard failed: {error}"));
    let written_earlier = crate::txn_writes::was_written(table_oid);
    let old = old_row.as_ref().map(|row| &row.0);
    for spec in &specs {
        if !needs_check(spec, &new_row.0, old) {
            continue;
        }
        // Serialise concurrent writers of the same value: the hot unique index cannot see a
        // value a concurrent transaction is about to write alongside a cold duplicate.
        let key = format!(
            "koldstore.uniq:{}:{}:{}",
            table_oid.to_u32(),
            spec.cols.join(","),
            spec.cols
                .iter()
                .map(|col| new_row.0.get(col).map_or_else(String::new, ToString::to_string))
                .collect::<Vec<_>>()
                .join("\u{1f}")
        );
        let lock_args = [DatumWithOid::from(key)];
        pgrx::Spi::run_with_args("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", &lock_args)
            .unwrap_or_else(|error| pgrx::error!("cold-DML unique guard failed: {error}"));

        let mut exists = conflict_exists(table_oid, spec, &new_row.0, &pk_columns)
            .unwrap_or_else(|error| pgrx::error!("cold-DML unique guard failed: {error}"));
        // Same mirror-lag fence as the primary-key guard: a delete committed moments ago may not be
        // masked from cold reads yet.
        if exists && !written_earlier && crate::guc::insert_guard_fence() && crate::guc::hydrate_fence_mirror() {
            exists = super::key_lock::with_current_view(|| conflict_exists(table_oid, spec, &new_row.0, &pk_columns))
                .unwrap_or_else(|error| pgrx::error!("cold-DML unique guard failed: {error}"));
        }
        if exists {
            pgrx::ereport!(
                ERROR,
                pgrx::PgSqlErrorCode::ERRCODE_UNIQUE_VIOLATION,
                format!(
                    "duplicate key value violates unique constraint on managed table {table_name} \
                     (columns: {}); a row with these values already exists (hot or cold)",
                    spec.cols.join(", ")
                )
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{needs_check, UniqueSpec};

    fn spec(nnd: bool) -> UniqueSpec {
        UniqueSpec { cols: vec!["email".to_string()], nnd }
    }

    #[test]
    fn null_values_do_not_conflict_unless_nulls_not_distinct() {
        let row = json!({"id": 1, "email": null});
        assert!(!needs_check(&spec(false), &row, None));
        assert!(needs_check(&spec(true), &row, None));
    }

    #[test]
    fn unchanged_columns_skip_the_check_on_update() {
        let old = json!({"id": 1, "email": "a"});
        assert!(!needs_check(&spec(false), &json!({"id": 1, "email": "a", "n": 2}), Some(&old)));
        assert!(needs_check(&spec(false), &json!({"id": 1, "email": "b"}), Some(&old)));
    }
}
