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
    // Whether dropped tables' cold objects were being retained when this backup was taken, and how
    // many were still waiting to be purged (those objects exist now; a later purge removes them).
    let retention = json_query(
        "SELECT jsonb_build_object(
             'cold_object_retention_seconds', current_setting('koldstore.cold_object_retention_seconds')::bigint,
             'deferred_objects', count(*),
             'oldest_staged_at', min(staged_at))::text
         FROM koldstore.deferred_cold_deletes",
        &[],
    )?;
    Ok(json!({
        "format": 1,
        "cluster": cluster,
        "async_mirror": async_mirror,
        "retention": retention,
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

/// Prefixes (with their storage) that currently-managed tables write to.
fn live_prefixes() -> Result<Vec<(String, String)>, String> {
    let mut prefixes = Vec::new();
    for oid in managed_table_oids(None)? {
        let target = table_target(oid)?;
        prefixes.push((target.storage.storage_id.clone(), target.prefix));
    }
    Ok(prefixes)
}

/// Opens a client for `koldstore.storage.id = storage_id`, or why it cannot be opened.
fn storage_client(storage_id: &str) -> Result<koldstore_storage::ObjectStoreClient, String> {
    let ctx = json_query(
        "SELECT jsonb_build_object('storage_type', storage_type, 'base_path', base_path,
                                   'credentials', credentials, 'config', config)::text
         FROM koldstore.storage WHERE id = $1",
        &[DatumWithOid::from(storage_id)],
    )?;
    if ctx.is_null() {
        return Err(format!("storage {storage_id} is no longer registered"));
    }
    crate::object_store::open_managed_object_store_client(
        ctx["storage_type"].as_str().unwrap_or("filesystem"),
        ctx["base_path"].as_str().unwrap_or_default(),
        &ctx["credentials"],
        &ctx["config"],
    )
    .map_err(|error| error.to_string())
}

fn purge_deferred_value(
    batch_limit: i64,
    older_than_seconds: Option<i64>,
    dry_run: bool,
) -> Result<Value, String> {
    let window = older_than_seconds
        .unwrap_or_else(|| i64::from(crate::guc::cold_object_retention_seconds()))
        .max(0);
    type Row = (i64, String, String);
    let rows: Vec<Row> = pgrx::Spi::connect(|client| {
        client
            .select(
                "SELECT id, storage_id, object_key FROM koldstore.deferred_cold_deletes
                 WHERE staged_at <= now() - make_interval(secs => $1::double precision)
                 ORDER BY id LIMIT $2",
                None,
                &[DatumWithOid::from(window), DatumWithOid::from(batch_limit)],
            )?
            .map(|row| {
                Ok((
                    row.get::<i64>(1)?.unwrap_or_default(),
                    row.get::<String>(2)?.unwrap_or_default(),
                    row.get::<String>(3)?.unwrap_or_default(),
                ))
            })
            .collect::<Result<Vec<Row>, pgrx::spi::Error>>()
    })
    .map_err(|error| error.to_string())?;

    // Two concurrent purges may both delete an object; deletes are idempotent, so no row lock.
    // A table dropped and recreated under the same name writes to the same prefix (including its
    // own manifest), so a key under a prefix a live table uses must never be deleted here.
    let live = live_prefixes()?;
    let mut clients = std::collections::HashMap::new();
    let (mut deleted, mut skipped_live, mut failed) = (0_i64, 0_i64, 0_i64);
    let mut errors = Vec::new();
    let mut done_ids = Vec::new();
    for (id, storage_id, key) in &rows {
        if live
            .iter()
            .any(|(sid, prefix)| sid == storage_id && key.starts_with(prefix.as_str()))
        {
            skipped_live += 1;
            done_ids.push(*id);
            continue;
        }
        if dry_run {
            deleted += 1;
            continue;
        }
        let client = clients
            .entry(storage_id.clone())
            .or_insert_with(|| storage_client(storage_id));
        match client
            .as_ref()
            .map_err(Clone::clone)
            .and_then(|client| client.delete(key).map_err(|error| error.to_string()))
        {
            Ok(()) => {
                deleted += 1;
                done_ids.push(*id);
            }
            Err(error) => {
                failed += 1;
                if errors.len() < 5 {
                    errors.push(json!({ "key": key, "error": error }));
                }
            }
        }
    }
    if !dry_run && !done_ids.is_empty() {
        pgrx::Spi::run_with_args(
            "DELETE FROM koldstore.deferred_cold_deletes WHERE id = ANY($1)",
            &[DatumWithOid::from(done_ids)],
        )
        .map_err(|error| error.to_string())?;
    }
    let remaining =
        pgrx::Spi::get_one::<i64>("SELECT count(*) FROM koldstore.deferred_cold_deletes")
            .map_err(|error| error.to_string())?
            .unwrap_or(0);
    Ok(json!({
        "dry_run": dry_run,
        "window_seconds": window,
        "considered": rows.len(),
        "deleted": deleted,
        "skipped_live_prefix": skipped_live,
        "failed": failed,
        "errors": errors,
        "remaining": remaining,
    }))
}

/// Deletes cold objects whose retention window has passed.
///
/// SQL contract: `koldstore.purge_deferred_cold_objects(batch_limit integer default 1000,
/// older_than_seconds integer default null, dry_run boolean default false) → jsonb`.
///
/// Objects of a table dropped (or unmanaged with `drop_cold`) while
/// `koldstore.cold_object_retention_seconds > 0` wait in `koldstore.deferred_cold_deletes`; this
/// removes those staged at least `older_than_seconds` ago (default: the setting) and returns
/// `{considered, deleted, skipped_live_prefix, failed, errors[], remaining}`. A key under a prefix a
/// currently managed table uses is dropped from the queue without deleting the object, because a
/// recreated table of the same name owns that prefix again. Run it periodically (for example from
/// pg_cron); safe to repeat. Superuser only.
#[pgrx::pg_extern(
    name = "purge_deferred_cold_objects",
    schema = "koldstore",
    security_definer
)]
pub fn purge_deferred_cold_objects_pg(
    batch_limit: pgrx::default!(i32, 1000),
    older_than_seconds: pgrx::default!(Option<i32>, "NULL"),
    dry_run: pgrx::default!(bool, false),
) -> pgrx::JsonB {
    crate::security::require_superuser("purge deferred cold objects");
    if batch_limit < 1 {
        pgrx::error!("batch_limit must be at least 1");
    }
    purge_deferred_value(
        i64::from(batch_limit),
        older_than_seconds.map(i64::from),
        dry_run,
    )
    .map(pgrx::JsonB)
    .unwrap_or_else(|error| pgrx::error!("purge deferred cold objects failed: {error}"))
}
