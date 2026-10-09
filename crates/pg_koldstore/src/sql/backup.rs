//! Backup support: `koldstore.backup_manifest` and `koldstore.validate_cold_storage`.
//!
//! KoldStore state lives in two durability domains -- PostgreSQL (hot heap, catalog, async mirror,
//! replication slot) and object storage (cold Parquet segments). A physical backup plus WAL
//! archive captures the first coherently, including the slot, so a restore (or PITR) is sound
//! exactly when every cold object the *restored* catalog references still exists. These two
//! functions make that checkable:
//!
//! - [`backup_manifest_pg`] records, at backup time, what the catalog references (table, manifest
//!   generation, every active segment with size and SHA-256), where the WAL stood and how far the
//!   async mirror had applied. It never includes storage credentials.
//! - [`validate_cold_storage_pg`] checks the live catalog against the object store: missing
//!   objects, size mismatches and (with `deep`) checksum mismatches. Run it after a restore and
//!   before cutover.
//!
//! Cold objects are immutable and no compaction rewrites them, so the only things that delete
//! referenced objects are `DROP TABLE`, `unmanage_table(drop_cold)` and `recover_segments`;
//! retain the object prefix for as long as backups taken before such an operation may be restored.

use koldstore_storage::StorageClient;
use pgrx::datum::DatumWithOid;
use serde_json::{Value, json};

/// Active managed table OIDs, or just `only` when given (which must be managed).
fn managed_table_oids(only: Option<pgrx::pg_sys::Oid>) -> Result<Vec<pgrx::pg_sys::Oid>, String> {
    let all: Vec<pgrx::pg_sys::Oid> = pgrx::Spi::connect(|client| {
        client
            .select(
                "SELECT table_oid FROM koldstore.schemas WHERE active ORDER BY table_oid",
                None,
                &[],
            )?
            .map(|row| {
                Ok(row
                    .get::<pgrx::pg_sys::Oid>(1)?
                    .unwrap_or(pgrx::pg_sys::InvalidOid))
            })
            .collect::<Result<Vec<_>, pgrx::spi::Error>>()
    })
    .map_err(|error| error.to_string())?;
    match only {
        Some(oid) if all.contains(&oid) => Ok(vec![oid]),
        Some(_) => Err("table is not managed by koldstore".to_string()),
        None => Ok(all),
    }
}

fn authorize(table: Option<pgrx::pg_sys::Oid>, what: &str) {
    match table {
        Some(oid) => crate::security::require_relation_owner_or_superuser(oid, what),
        None => crate::security::require_superuser(what),
    }
}

/// One query returning a single `jsonb` value rendered as text, bound with `args`.
fn json_query(sql: &str, args: &[DatumWithOid<'_>]) -> Result<Value, String> {
    let text = pgrx::Spi::get_one_with_args::<String>(sql, args)
        .map_err(|error| error.to_string())?
        .unwrap_or_else(|| "null".to_string());
    serde_json::from_str(&text).map_err(|error| error.to_string())
}

/// Active segments of `table_oid` (all scopes), ordered for stable output.
fn active_segments(table_oid: pgrx::pg_sys::Oid) -> Result<Vec<Value>, String> {
    let value = json_query(
        "SELECT COALESCE(jsonb_agg(jsonb_build_object(
             'segment_id', segment_id, 'scope_key', scope_key, 'path', path,
             'row_count', row_count, 'byte_size', byte_size, 'checksum', checksum,
             'min_seq', min_seq, 'max_seq', max_seq, 'schema_version', schema_version)
             ORDER BY scope_key, min_seq, segment_id), '[]'::jsonb)::text
         FROM koldstore.cold_segments WHERE table_oid = $1 AND status = 'active'",
        &[DatumWithOid::from(table_oid)],
    )?;
    Ok(value.as_array().cloned().unwrap_or_default())
}

struct TableTarget {
    namespace: String,
    name: String,
    storage: koldstore_catalog::decode::FlushStorageContext,
    prefix: String,
}

fn table_target(table_oid: pgrx::pg_sys::Oid) -> Result<TableTarget, String> {
    use koldstore_storage::{PathTemplate, render_regular_table_prefix};

    let relation = crate::catalog::resolve::relation_context(table_oid)?;
    let storage = crate::catalog::resolve::active_flush_storage_context(table_oid)?;
    let prefix = render_regular_table_prefix(
        &PathTemplate::new(&storage.regular_path_tmpl),
        &relation.namespace,
        &relation.name,
    )?;
    Ok(TableTarget {
        namespace: relation.namespace,
        name: relation.name,
        storage,
        prefix,
    })
}

fn backup_manifest_value(only: Option<pgrx::pg_sys::Oid>) -> Result<Value, String> {
    let mut tables = Vec::new();
    for oid in managed_table_oids(only)? {
        let target = table_target(oid)?;
        let manifests = json_query(
            "SELECT COALESCE(jsonb_agg(jsonb_build_object(
                 'scope_key', scope_key, 'generation', generation, 'etag', etag,
                 'max_seq', max_seq, 'segment_count', segment_count,
                 'cold_row_count', cold_row_count, 'sync_state', sync_state)
                 ORDER BY scope_key), '[]'::jsonb)::text
             FROM koldstore.manifest WHERE table_oid = $1",
            &[DatumWithOid::from(oid)],
        )?;
        let pending = pgrx::Spi::get_one_with_args::<i64>(
            "SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = $1 AND status = 'pending'",
            &[DatumWithOid::from(oid)],
        )
        .map_err(|error| error.to_string())?
        .unwrap_or(0);
        let segments: Vec<Value> = active_segments(oid)?
            .into_iter()
            .map(|mut segment| {
                let key = segment["path"]
                    .as_str()
                    .map(|path| koldstore_common::join_object_key(&target.prefix, path));
                segment["key"] = json!(key);
                segment
            })
            .collect();
        tables.push(json!({
            "table_oid": oid.to_u32(),
            "table": format!("{}.{}", target.namespace, target.name),
            "storage": {
                "type": target.storage.storage_type,
                "base_path": target.storage.base_path,
                "prefix": target.prefix,
            },
            "manifests": manifests,
            "pending_segments": pending,
            "segments": segments,
        }));
    }

    let cluster = json_query(
        "SELECT jsonb_build_object(
             'database', current_database(),
             'system_identifier', (SELECT system_identifier::text FROM pg_control_system()),
             'in_recovery', pg_is_in_recovery(),
             'wal_lsn', (CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()
                              ELSE pg_current_wal_lsn() END)::text,
             'taken_at', now(),
             'server_version', current_setting('server_version'))::text",
        &[],
    )?;
    let async_mirror = crate::mirror::status::async_mirror_status_value()
        .unwrap_or_else(|error| json!({ "error": error, "healthy": false }));
    Ok(json!({
        "format": 1,
        "cluster": cluster,
        "async_mirror": async_mirror,
        "tables": tables,
    }))
}

/// Backup manifest of the catalog's cold-tier references.
///
/// SQL contract: `koldstore.backup_manifest(table_name regclass default null) → jsonb`.
///
/// Record this alongside a physical base backup. It lists, per managed table, the manifest
/// generation and every active cold segment with its object key, size and SHA-256, plus the WAL
/// position and async-mirror state at the time. Superuser for all tables; the table owner for one.
/// No storage credentials are included.
#[pgrx::pg_extern(name = "backup_manifest", schema = "koldstore", security_definer)]
pub fn backup_manifest_pg(
    table_name: pgrx::default!(Option<pgrx::PgRelation>, "NULL"),
) -> pgrx::JsonB {
    let table = table_name.as_ref().map(pgrx::PgRelation::oid);
    authorize(table, "produce a backup manifest");
    backup_manifest_value(table)
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("backup manifest failed: {error}"))
}

fn validate_cold_storage_value(
    only: Option<pgrx::pg_sys::Oid>,
    deep: bool,
) -> Result<Value, String> {
    let mut problems = Vec::new();
    let mut checked = 0_i64;
    let mut tables_checked = 0_i64;
    for oid in managed_table_oids(only)? {
        let target = table_target(oid)?;
        tables_checked += 1;
        let table = format!("{}.{}", target.namespace, target.name);
        let client = crate::object_store::open_managed_object_store_client(
            &target.storage.storage_type,
            &target.storage.base_path,
            &target.storage.credentials,
            &target.storage.config,
        )
        .map_err(|error| format!("{table}: open storage: {error}"))?;
        for segment in active_segments(oid)? {
            checked += 1;
            let path = segment["path"].as_str().unwrap_or_default();
            let key = koldstore_common::join_object_key(&target.prefix, path);
            let expected_size = segment["byte_size"].as_u64();
            let problem = |kind: &str, detail: String| {
                json!({
                    "table": table, "segment_id": segment["segment_id"], "key": key,
                    "problem": kind, "detail": detail,
                })
            };
            let head = match client.head(&key) {
                Ok(head) => head,
                Err(koldstore_storage::StorageClientError::NotFound { .. }) => {
                    problems.push(problem("missing", "object not found".to_string()));
                    continue;
                }
                Err(error) => {
                    problems.push(problem("storage_error", error.to_string()));
                    continue;
                }
            };
            if let (Some(expected), Some(actual)) = (expected_size, head.byte_size) {
                if expected != actual {
                    problems.push(problem(
                        "size_mismatch",
                        format!("catalog says {expected} bytes, object is {actual}"),
                    ));
                    continue;
                }
            }
            if deep {
                match client.get(&key) {
                    Ok(bytes) => {
                        let actual = koldstore_storage::content_checksum_sha256_hex(&bytes);
                        let expected = segment["checksum"].as_str().unwrap_or_default();
                        if !expected.eq_ignore_ascii_case(&actual) {
                            problems.push(problem(
                                "checksum_mismatch",
                                format!("catalog says {expected}, object hashes to {actual}"),
                            ));
                        }
                    }
                    Err(error) => problems.push(problem("storage_error", error.to_string())),
                }
            }
        }
    }
    Ok(json!({
        "ok": problems.is_empty(),
        "deep": deep,
        "tables_checked": tables_checked,
        "segments_checked": checked,
        "problems": problems,
    }))
}

/// Checks every active cold segment the catalog references against the object store.
///
/// SQL contract:
/// `koldstore.validate_cold_storage(table_name regclass default null, deep boolean default false)
/// → jsonb` of `{ok, deep, tables_checked, segments_checked, problems[]}`.
///
/// Each problem is `missing`, `size_mismatch`, `checksum_mismatch` (only with `deep`, which
/// downloads and hashes every object) or `storage_error`. Run after restoring a backup or PITR and
/// before cutover: `ok = true` means the restored catalog's cold references are all intact.
/// Unreferenced (orphan) objects are not reported here; use `recover_segments(..., dry_run => true)`.
#[pgrx::pg_extern(name = "validate_cold_storage", schema = "koldstore", security_definer)]
pub fn validate_cold_storage_pg(
    table_name: pgrx::default!(Option<pgrx::PgRelation>, "NULL"),
    deep: pgrx::default!(bool, false),
) -> pgrx::JsonB {
    let table = table_name.as_ref().map(pgrx::PgRelation::oid);
    authorize(table, "validate cold storage");
    validate_cold_storage_value(table, deep)
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("validate cold storage failed: {error}"))
}
