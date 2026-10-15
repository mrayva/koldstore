//! `koldstore.sync_sql_objects`: repairs SQL object drift (see [`super::drift`]).
//!
//! The extension version does not change when a build changes a function signature or adds a catalog
//! column, so `ALTER EXTENSION ... UPDATE` never runs and an older database keeps the old objects.
//! `validate_sql_objects()` detects that; this repairs it from definitions generated out of a freshly
//! created database (`manifest/repair_objects.json`, written by `scripts/check-sql-drift.sh
//! --update-expected`): missing functions are created, a function whose signature changed is replaced,
//! missing catalog columns are added, and a missing catalog table is recreated with its constraints and
//! indexes. Nothing is ever dropped except functions (never columns, never data), and only functions
//! that are members of the extension.
//!
//! Planning is a pure function ([`plan_repair`]); the SQL entry point only gathers the live state,
//! runs the plan in one transaction (so a failure leaves the database as it was) and re-checks.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

use super::drift::DriftReport;

/// Repair definitions generated from a fresh database, compiled into the library.
pub(crate) const REPAIR_OBJECTS: &str = include_str!("../../manifest/repair_objects.json");

/// How to recreate one function.
#[derive(Debug, Clone, Deserialize)]
pub struct FunctionRepair {
    /// Function name (without schema or arguments).
    pub name: String,
    /// Complete `CREATE OR REPLACE FUNCTION` statement.
    pub create: String,
    /// `schema.name(identity arguments)`, for `ALTER EXTENSION ... ADD FUNCTION` and `REVOKE`.
    pub add: String,
    /// Whether `PUBLIC` must not be able to execute it.
    pub revoke_public: bool,
}

/// How to add one catalog column.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnRepair {
    /// Table the column belongs to (in the `koldstore` schema).
    pub table: String,
    /// `ALTER TABLE ... ADD COLUMN ...` statement.
    pub add: String,
    /// `NOT NULL` without a default: cannot be added to a table that has rows.
    pub needs_default: bool,
    /// Identity or generated column: not added automatically.
    pub unsupported: bool,
}

/// How to recreate one catalog table.
#[derive(Debug, Clone, Deserialize)]
pub struct TableRepair {
    /// `CREATE TABLE` statement (columns, defaults, not-null).
    pub create: String,
    /// Primary key, unique and check constraints.
    pub constraints: Vec<String>,
    /// Foreign keys (applied after every missing table exists).
    pub foreign_keys: Vec<String>,
    /// Indexes that do not back a constraint.
    pub indexes: Vec<String>,
}

/// Everything needed to repair a database, keyed like `expected_objects.txt`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RepairDefs {
    /// By manifest line.
    pub functions: BTreeMap<String, FunctionRepair>,
    /// By manifest line.
    pub columns: BTreeMap<String, ColumnRepair>,
    /// By table name.
    pub tables: BTreeMap<String, TableRepair>,
}

/// A function that exists in the database but is not expected, with the statement that removes it.
#[derive(Debug, Clone)]
pub struct LiveFunction {
    /// Its manifest line.
    pub line: String,
    /// Statements removing it from the extension and the database.
    pub drop_sql: String,
}

/// One action of a repair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairStep {
    /// `drop_function`, `create_table`, `add_foreign_keys`, `add_column` or `create_function`.
    pub action: &'static str,
    /// What it concerns (a manifest line or a table name).
    pub item: String,
    /// The statements, run in order.
    pub statements: Vec<String>,
}

/// Something the repair deliberately leaves alone, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skipped {
    /// What it concerns.
    pub item: String,
    /// Why it was not repaired.
    pub reason: String,
}

/// The ordered steps of a repair plus what it skipped.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairPlan {
    /// Steps in execution order.
    pub steps: Vec<RepairStep>,
    /// Items left alone.
    pub skipped: Vec<Skipped>,
}

/// The function name of a manifest line (`function name(...)`), if it is one.
fn function_name(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("function ")?;
    rest.split_once('(').map(|(name, _)| name)
}

/// Plans the repair of a database whose objects differ from the library's.
///
/// * A function whose signature changed (an unexpected one with the same name as a missing one) is
///   dropped before the new one is created; an overload left behind would make calls ambiguous. Other
///   unexpected functions are dropped only with `drop_unexpected`.
/// * Unexpected columns are never dropped.
/// * A missing table is recreated whole (so its missing columns are not added one by one); missing
///   columns of existing tables are added unless they are `NOT NULL` without a default or generated.
#[must_use]
pub fn plan_repair(
    report: &DriftReport,
    defs: &RepairDefs,
    live_unexpected: &[LiveFunction],
    existing_tables: &BTreeSet<String>,
    drop_unexpected: bool,
) -> RepairPlan {
    let mut plan = RepairPlan::default();

    let replaced_names: BTreeSet<&str> = report
        .missing
        .iter()
        .filter_map(|line| function_name(line))
        .collect();

    for line in &report.unexpected {
        if function_name(line).is_some() {
            let Some(live) = live_unexpected.iter().find(|live| &live.line == line) else {
                plan.skipped.push(Skipped {
                    item: line.clone(),
                    reason: "unexpected function that is not a member of the extension".to_string(),
                });
                continue;
            };
            let replaced = function_name(line).is_some_and(|name| replaced_names.contains(name));
            if replaced || drop_unexpected {
                plan.steps.push(RepairStep {
                    action: "drop_function",
                    item: line.clone(),
                    statements: vec![live.drop_sql.clone()],
                });
            } else {
                plan.skipped.push(Skipped {
                    item: line.clone(),
                    reason: "unexpected function; pass drop_unexpected => true to remove it".to_string(),
                });
            }
        } else {
            plan.skipped.push(Skipped {
                item: line.clone(),
                reason: "unexpected column; columns are never dropped automatically".to_string(),
            });
        }
    }

    // Tables the database lacks entirely: recreated whole, once.
    let mut missing_tables = BTreeSet::new();
    for line in &report.missing {
        if let Some(column) = defs.columns.get(line) {
            if !existing_tables.contains(&column.table) {
                missing_tables.insert(column.table.clone());
            }
        }
    }
    for table in &missing_tables {
        let Some(def) = defs.tables.get(table) else {
            plan.skipped.push(Skipped {
                item: format!("table {table}"),
                reason: "no repair definition for this table".to_string(),
            });
            continue;
        };
        let mut statements = vec![def.create.clone()];
        statements.extend(def.constraints.iter().cloned());
        statements.extend(def.indexes.iter().cloned());
        statements.push(format!("ALTER EXTENSION koldstore ADD TABLE koldstore.{table}"));
        plan.steps.push(RepairStep {
            action: "create_table",
            item: format!("table {table}"),
            statements,
        });
    }
    // Foreign keys last, once every table they may reference exists.
    for table in &missing_tables {
        if let Some(def) = defs.tables.get(table) {
            if !def.foreign_keys.is_empty() {
                plan.steps.push(RepairStep {
                    action: "add_foreign_keys",
                    item: format!("table {table}"),
                    statements: def.foreign_keys.clone(),
                });
            }
        }
    }

    for line in &report.missing {
        if let Some(column) = defs.columns.get(line) {
            if missing_tables.contains(&column.table) {
                continue; // comes with its table
            }
            if column.unsupported {
                plan.skipped.push(Skipped {
                    item: line.clone(),
                    reason: "identity or generated column; add it by hand".to_string(),
                });
            } else if column.needs_default {
                plan.skipped.push(Skipped {
                    item: line.clone(),
                    reason: "NOT NULL column without a default cannot be added to a table that may have rows"
                        .to_string(),
                });
            } else {
                plan.steps.push(RepairStep {
                    action: "add_column",
                    item: line.clone(),
                    statements: vec![column.add.clone()],
                });
            }
        } else if let Some(function) = defs.functions.get(line) {
            let mut statements = vec![
                function.create.clone(),
                format!("ALTER EXTENSION koldstore ADD FUNCTION {}", function.add),
            ];
            if function.revoke_public {
                statements.push(format!("REVOKE ALL ON FUNCTION {} FROM PUBLIC", function.add));
            }
            plan.steps.push(RepairStep {
                action: "create_function",
                item: line.clone(),
                statements,
            });
        } else {
            plan.skipped.push(Skipped {
                item: line.clone(),
                reason: "no repair definition for this object (is repair_objects.json out of date?)"
                    .to_string(),
            });
        }
    }
    plan
}

#[cfg(feature = "pg")]
fn parse_repair_defs() -> Result<RepairDefs, String> {
    serde_json::from_str(REPAIR_OBJECTS).map_err(|error| format!("repair definitions: {error}"))
}

#[cfg(feature = "pg")]
fn existing_koldstore_tables() -> Result<BTreeSet<String>, String> {
    pgrx::Spi::connect(|client| {
        client
            .select(
                "SELECT c.relname::text FROM pg_catalog.pg_class c \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = 'koldstore' AND c.relkind IN ('r', 'p')",
                None,
                &[],
            )?
            .map(|row| Ok(row.get::<String>(1)?.unwrap_or_default()))
            .collect::<Result<BTreeSet<_>, pgrx::spi::Error>>()
    })
    .map_err(|error| error.to_string())
}

#[cfg(feature = "pg")]
fn live_unexpected_functions() -> Result<Vec<LiveFunction>, String> {
    let sql = format!(
        "SELECT line, drop_sql FROM ({}) AS m(line, kind, ref_oid, ref_num, drop_sql) WHERE kind = 'function'",
        super::drift::object_manifest_sql()
    );
    pgrx::Spi::connect(|client| {
        client
            .select(&sql, None, &[])?
            .map(|row| {
                Ok(LiveFunction {
                    line: row.get::<String>(1)?.unwrap_or_default(),
                    drop_sql: row.get::<String>(2)?.unwrap_or_default(),
                })
            })
            .collect::<Result<Vec<_>, pgrx::spi::Error>>()
    })
    .map_err(|error| error.to_string())
}

#[cfg(feature = "pg")]
fn sync_value(dry_run: bool, drop_unexpected: bool) -> Result<serde_json::Value, String> {
    use serde_json::json;

    let before = super::drift::current_report()?;
    let defs = parse_repair_defs()?;
    let plan = plan_repair(
        &before,
        &defs,
        &live_unexpected_functions()?,
        &existing_koldstore_tables()?,
        drop_unexpected,
    );
    if !dry_run {
        for step in &plan.steps {
            for statement in &step.statements {
                pgrx::Spi::run(statement)
                    .map_err(|error| format!("{} ({}): {error}; statement: {statement}", step.action, step.item))?;
            }
        }
    }
    let after = if dry_run { None } else { Some(super::drift::current_report()?) };
    let steps: Vec<_> = plan
        .steps
        .iter()
        .map(|step| json!({"action": step.action, "item": step.item, "statements": step.statements}))
        .collect();
    let skipped: Vec<_> = plan
        .skipped
        .iter()
        .map(|skip| json!({"item": skip.item, "reason": skip.reason}))
        .collect();
    Ok(json!({
        "dry_run": dry_run,
        "ok_before": before.is_clean(),
        "steps": steps,
        "applied": if dry_run { 0 } else { plan.steps.len() },
        "skipped": skipped,
        "ok_after": after.as_ref().map(DriftReport::is_clean),
        "still_missing": after.as_ref().map(|report| report.missing.clone()),
        "still_unexpected": after.as_ref().map(|report| report.unexpected.clone()),
    }))
}

/// Repairs this database's koldstore SQL objects so they match the loaded library.
///
/// SQL contract: `koldstore.sync_sql_objects(dry_run boolean default true, drop_unexpected boolean
/// default false) → jsonb` of `{dry_run, ok_before, steps[], applied, skipped[], ok_after,
/// still_missing[], still_unexpected[]}`. `dry_run` (the default) only returns the plan, with every
/// statement it would run. A real run executes the plan in this transaction, so any failure leaves the
/// database untouched, then re-checks. Functions with a changed signature are replaced;
/// `drop_unexpected => true` also removes functions the library does not know (for example leftovers of
/// a test build). Columns are never dropped, and a `NOT NULL` column without a default is reported, not
/// added. Superuser only. Run `koldstore.validate_sql_objects()` first to see what differs.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "sync_sql_objects", schema = "koldstore")]
pub fn sync_sql_objects_pg(
    dry_run: pgrx::default!(bool, true),
    drop_unexpected: pgrx::default!(bool, false),
) -> pgrx::JsonB {
    crate::security::require_superuser("repair koldstore SQL objects");
    sync_value(dry_run, drop_unexpected)
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("sync sql objects failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(missing: &[&str], unexpected: &[&str]) -> DriftReport {
        DriftReport {
            missing: missing.iter().map(ToString::to_string).collect(),
            unexpected: unexpected.iter().map(ToString::to_string).collect(),
        }
    }

    fn defs() -> RepairDefs {
        let mut defs = RepairDefs::default();
        defs.functions.insert(
            "function f(a integer, b integer) returns uuid [f_wrapper]".to_string(),
            FunctionRepair {
                name: "f".to_string(),
                create: "CREATE OR REPLACE FUNCTION koldstore.f(a integer, b integer) ...".to_string(),
                add: "koldstore.f(a integer, b integer)".to_string(),
                revoke_public: true,
            },
        );
        defs.columns.insert(
            "column t.extra bytea".to_string(),
            ColumnRepair {
                table: "t".to_string(),
                add: "ALTER TABLE koldstore.t ADD COLUMN extra bytea".to_string(),
                needs_default: false,
                unsupported: false,
            },
        );
        defs.columns.insert(
            "column t.must integer not null".to_string(),
            ColumnRepair {
                table: "t".to_string(),
                add: "ALTER TABLE koldstore.t ADD COLUMN must integer".to_string(),
                needs_default: true,
                unsupported: false,
            },
        );
        defs.columns.insert(
            "column gone.id bigint not null".to_string(),
            ColumnRepair {
                table: "gone".to_string(),
                add: "ALTER TABLE koldstore.gone ADD COLUMN id bigint".to_string(),
                needs_default: true,
                unsupported: false,
            },
        );
        defs.tables.insert(
            "gone".to_string(),
            TableRepair {
                create: "CREATE TABLE koldstore.gone (id bigint NOT NULL)".to_string(),
                constraints: vec!["ALTER TABLE koldstore.gone ADD CONSTRAINT gone_pkey PRIMARY KEY (id)".to_string()],
                foreign_keys: vec!["ALTER TABLE koldstore.gone ADD CONSTRAINT gone_fk FOREIGN KEY (id) REFERENCES koldstore.t(id)".to_string()],
                indexes: vec!["CREATE INDEX gone_idx ON koldstore.gone (id)".to_string()],
            },
        );
        defs
    }

    fn tables(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(ToString::to_string).collect()
    }

    fn live(line: &str) -> LiveFunction {
        LiveFunction {
            line: line.to_string(),
            drop_sql: format!("DROP /* {line} */"),
        }
    }

    #[test]
    fn a_changed_signature_drops_the_old_overload_before_creating_the_new_one() {
        let old = "function f(a integer) returns uuid [f_wrapper]";
        let plan = plan_repair(
            &report(&["function f(a integer, b integer) returns uuid [f_wrapper]"], &[old]),
            &defs(),
            &[live(old)],
            &tables(&["t"]),
            false,
        );
        let actions: Vec<_> = plan.steps.iter().map(|step| step.action).collect();
        assert_eq!(actions, vec!["drop_function", "create_function"]);
        let create = &plan.steps[1];
        assert_eq!(create.statements.len(), 3, "create, add to extension, revoke from public");
        assert!(create.statements[1].starts_with("ALTER EXTENSION koldstore ADD FUNCTION koldstore.f("));
        assert!(create.statements[2].starts_with("REVOKE ALL ON FUNCTION"));
        assert!(plan.skipped.is_empty());
    }

    #[test]
    fn an_unrelated_unexpected_function_is_kept_unless_asked() {
        let leftover = "function test_leftover() returns void [test_leftover_wrapper]";
        let kept = plan_repair(&report(&[], &[leftover]), &defs(), &[live(leftover)], &tables(&["t"]), false);
        assert!(kept.steps.is_empty());
        assert_eq!(kept.skipped.len(), 1);
        assert!(kept.skipped[0].reason.contains("drop_unexpected"));

        let dropped = plan_repair(&report(&[], &[leftover]), &defs(), &[live(leftover)], &tables(&["t"]), true);
        assert_eq!(dropped.steps.len(), 1);
        assert_eq!(dropped.steps[0].action, "drop_function");
    }

    #[test]
    fn columns_are_added_but_never_dropped() {
        let plan = plan_repair(
            &report(
                &["column t.extra bytea", "column t.must integer not null"],
                &["column t.old text"],
            ),
            &defs(),
            &[],
            &tables(&["t"]),
            true,
        );
        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.steps[0].action, "add_column");
        let reasons: Vec<_> = plan.skipped.iter().map(|skip| skip.reason.as_str()).collect();
        assert!(reasons.iter().any(|reason| reason.contains("NOT NULL column without a default")));
        assert!(reasons.iter().any(|reason| reason.contains("never dropped")));
    }

    #[test]
    fn a_missing_table_is_recreated_whole_with_foreign_keys_last() {
        let plan = plan_repair(
            &report(&["column gone.id bigint not null", "column t.extra bytea"], &[]),
            &defs(),
            &[],
            &tables(&["t"]),
            false,
        );
        let actions: Vec<_> = plan.steps.iter().map(|step| step.action).collect();
        assert_eq!(actions, vec!["create_table", "add_foreign_keys", "add_column"]);
        let create = &plan.steps[0].statements;
        assert!(create[0].starts_with("CREATE TABLE koldstore.gone"));
        assert!(create.last().unwrap().ends_with("ADD TABLE koldstore.gone"));
        assert!(plan.skipped.is_empty(), "its not-null column comes with the table: {:?}", plan.skipped);
    }

    #[test]
    fn an_object_without_a_definition_is_reported_not_guessed() {
        let plan = plan_repair(&report(&["function unknown() returns void [x]"], &[]), &defs(), &[], &tables(&["t"]), false);
        assert!(plan.steps.is_empty());
        assert!(plan.skipped[0].reason.contains("no repair definition"));
    }

    #[test]
    fn the_embedded_definitions_parse_and_cover_every_expected_object() {
        let defs: RepairDefs = serde_json::from_str(REPAIR_OBJECTS).expect("repair_objects.json parses");
        for line in include_str!("../../manifest/expected_objects.txt").lines().filter(|l| !l.is_empty()) {
            let covered = if line.starts_with("function ") {
                defs.functions.contains_key(line)
            } else {
                defs.columns.contains_key(line)
            };
            assert!(covered, "no repair definition for `{line}`; regenerate with scripts/check-sql-drift.sh --update-expected");
        }
    }
}
