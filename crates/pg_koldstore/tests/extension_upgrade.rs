//! Extension packaging version contract tests.
//!
//! A supported upgrade path now exists (`koldstore--0.1.11-preview.0--0.1.12-preview.0.sql`,
//! introduced deliberately, not accidental sprawl -- see its own header comment). Catalog DDL
//! changes since then go into a new `koldstore--<from>--<to>.sql` upgrade edge alongside the
//! full snapshot, kept honest by `scripts/check-upgrade-path.sh`, not back into the bootstrap
//! file. `bootstrap_catalog_sql_exists` and `no_stale_upgrade_edges` below are what remains of
//! this contract check; the earlier "no upgrade edges at all" assertion (dropped 2026-09-27) is
//! obsolete now that this project ships one.

use std::fs;
use std::path::PathBuf;

fn sql_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sql")
}

fn control_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("koldstore.control")
}

#[test]
fn control_default_version_tracks_cargo_package_version() {
    let control = fs::read_to_string(control_path()).expect("read koldstore.control");
    assert!(
        control.contains("default_version = '@CARGO_VERSION@'"),
        "koldstore.control must use @CARGO_VERSION@ so packaged extversion matches Cargo; got:\n{control}"
    );
}

#[test]
fn bootstrap_catalog_sql_exists() {
    let path = sql_dir().join("koldstore--0.1.0.sql");
    assert!(
        path.is_file(),
        "missing bootstrap catalog fragment {}",
        path.display()
    );
    let body = fs::read_to_string(&path).expect("read bootstrap sql");
    assert!(
        !body.trim().is_empty(),
        "bootstrap catalog fragment must not be empty"
    );
}

/// Every packaged upgrade edge (`koldstore--<from>--<to>.sql`) names a `<to>` version that is
/// either the crate's current version (the live upgrade target) or another edge file's `<from>`
/// (a still-connected earlier hop) -- catching a dangling edge nobody's `CREATE EXTENSION
/// koldstore VERSION '<from>'` can reach `ALTER EXTENSION ... UPDATE` forward from.
#[test]
fn no_stale_upgrade_edges() {
    let crate_version = env!("CARGO_PKG_VERSION");
    let entries = fs::read_dir(sql_dir()).expect("read sql dir");
    let edges: Vec<(String, String, String)> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            let stem = name.strip_prefix("koldstore--")?.strip_suffix(".sql")?;
            let (from, to) = stem.split_once("--")?;
            let (from, to) = (from.to_string(), to.to_string());
            Some((name, from, to))
        })
        .collect();
    let live_targets: std::collections::HashSet<&str> =
        edges.iter().map(|(_, from, _)| from.as_str()).collect();
    for (name, _, to) in &edges {
        assert!(
            to == crate_version || live_targets.contains(to.as_str()),
            "upgrade edge {name} targets version {to}, which is neither the crate's current \
             version ({crate_version}) nor another edge's source -- dangling, nothing can reach it"
        );
    }
}
