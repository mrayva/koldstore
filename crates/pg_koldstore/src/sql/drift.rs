//! `koldstore.validate_sql_objects()`: detects a database whose koldstore SQL objects no longer match
//! the loaded library.
//!
//! The extension version stays the same while functions and catalog columns change between builds, so
//! `ALTER EXTENSION ... UPDATE` never runs and an older database keeps the previous SQL objects. The
//! library then fails at call time (`manage_table` with 17 arguments against a library that reads 18,
//! `unboxing allow_fk_hot_only_ argument failed`) or on a catalog column it expects
//! (`cold_segment_index.value_summary`). This compares what the database has with what the library was
//! built against and says exactly what differs.
//!
//! `manifest/expected_objects.txt` is generated from a freshly created database
//! (`scripts/check-sql-drift.sh --update-expected`); the `sql_drift` SQL case fails when it is stale.

use std::collections::BTreeSet;

const OBJECT_MANIFEST_SQL: &str = include_str!("../../manifest/object_manifest.sql");
const EXPECTED_OBJECTS: &str = include_str!("../../manifest/expected_objects.txt");
const LIBRARY_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Result of comparing a live object list with the expected one.
#[derive(Debug, PartialEq, Eq)]
pub struct DriftReport {
    /// Expected by the library but absent in the database.
    pub missing: Vec<String>,
    /// Present in the database but not expected by the library.
    pub unexpected: Vec<String>,
}

impl DriftReport {
    /// True when both lists match.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.missing.is_empty() && self.unexpected.is_empty()
    }
}

/// Compares `live` against `expected` (order-insensitive, blank lines ignored).
#[must_use]
pub fn compare_objects<'a>(
    expected: impl IntoIterator<Item = &'a str>,
    live: impl IntoIterator<Item = &'a str>,
) -> DriftReport {
    let expected: BTreeSet<&str> = expected.into_iter().filter(|line| !line.trim().is_empty()).collect();
    let live: BTreeSet<&str> = live.into_iter().filter(|line| !line.trim().is_empty()).collect();
    DriftReport {
        missing: expected.difference(&live).map(|line| (*line).to_string()).collect(),
        unexpected: live.difference(&expected).map(|line| (*line).to_string()).collect(),
    }
}

#[cfg(feature = "pg")]
fn live_objects() -> Result<Vec<String>, String> {
    let sql = format!(
        "SELECT coalesce(array_agg(line ORDER BY line), '{{}}'::text[]) FROM ({}) AS manifest(line)",
        OBJECT_MANIFEST_SQL.trim().trim_end_matches(';')
    );
    pgrx::Spi::get_one::<Vec<String>>(&sql)
        .map_err(|error| error.to_string())
        .map(Option::unwrap_or_default)
}

#[cfg(feature = "pg")]
fn installed_version() -> Result<Option<String>, String> {
    pgrx::Spi::get_one::<String>("SELECT extversion FROM pg_extension WHERE extname = 'koldstore'")
        .map_err(|error| error.to_string())
}

#[cfg(feature = "pg")]
fn report_value() -> Result<serde_json::Value, String> {
    let live = live_objects()?;
    let report = compare_objects(
        EXPECTED_OBJECTS.lines(),
        live.iter().map(String::as_str),
    );
    let installed = installed_version()?;
    let version_matches = installed.as_deref() == Some(LIBRARY_VERSION);
    let ok = report.is_clean() && version_matches;
    Ok(serde_json::json!({
        "ok": ok,
        "library_version": LIBRARY_VERSION,
        "extension_version": installed,
        "missing": report.missing,
        "unexpected": report.unexpected,
        "hint": if ok {
            serde_json::Value::Null
        } else {
            serde_json::Value::String(
                "this database's koldstore SQL objects differ from the loaded library: recreate the \
                 listed functions from the extension script, or apply the listed column changes, then \
                 re-check (docs/backup-and-operations.md, \"SQL object drift\")"
                    .to_string(),
            )
        },
    }))
}

/// Compares the database's koldstore SQL objects with the ones the loaded library was built for.
///
/// SQL contract: `koldstore.validate_sql_objects() → jsonb` of
/// `{ok, library_version, extension_version, missing[], unexpected[], hint}`. `missing` lists what the
/// library expects but the database lacks (for a function, a changed signature shows as one entry in
/// each list); `unexpected` lists what the database has that the library does not know. Read-only.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "validate_sql_objects", schema = "koldstore")]
pub fn validate_sql_objects_pg() -> pgrx::JsonB {
    report_value()
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("validate sql objects failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::compare_objects;

    #[test]
    fn identical_lists_are_clean_regardless_of_order() {
        let report = compare_objects(["a", "b", ""], ["b", "a"]);
        assert!(report.is_clean());
    }

    #[test]
    fn a_changed_signature_appears_on_both_sides() {
        let report = compare_objects(
            ["function f(a integer, b integer) returns uuid [f_wrapper]"],
            ["function f(a integer) returns uuid [f_wrapper]"],
        );
        assert_eq!(report.missing, vec!["function f(a integer, b integer) returns uuid [f_wrapper]"]);
        assert_eq!(report.unexpected, vec!["function f(a integer) returns uuid [f_wrapper]"]);
    }

    #[test]
    fn a_missing_column_is_reported() {
        let report = compare_objects(["column t.a bigint", "column t.b bytea"], ["column t.a bigint"]);
        assert_eq!(report.missing, vec!["column t.b bytea"]);
        assert!(report.unexpected.is_empty());
    }
}
