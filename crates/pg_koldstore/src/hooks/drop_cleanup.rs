//! DROP TABLE / DROP SCHEMA ProcessUtility cleanup for managed KoldStore tables.
//!
//! `DROP SCHEMA … CASCADE` does not emit per-table `DropStmt`s through
//! ProcessUtility, so schema drops must resolve managed heaps in the target
//! namespace here before PostgreSQL removes them.
//!
//! Order matters to avoid deadlocks with an in-flight flush:
//! 1. Resolve OIDs with `NoLock` (do not hold relation locks across waits)
//! 2. Signal cooperative cancel (`table_cancel_requests`)
//! 3. Wait for the table-job advisory lock (flush holds it for the statement)
//! 4. Catalog cleanup (transactional) + stage cold-object keys for deletion,
//!    then allow PostgreSQL DROP
//! 5. Drop the change-log mirror after the heap is gone
//!
//! The object-store objects themselves are not deleted here: they are staged via
//! `pending_cold_delete` and physically removed only after this transaction commits,
//! so a later statement failure or `ROLLBACK` in the same transaction (or a crash
//! before commit) leaves both the catalog rows and the cold objects intact (#100).

use std::ffi::{CStr, CString};

use koldstore_common::QualifiedTableName;
use koldstore_migrate::drop_table::{plan_drop_table_cleanup, DropTableCleanupPolicy};
use koldstore_storage::{render_regular_table_prefix, PathTemplate, StorageClient};
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys;

/// Cancels jobs and removes cold artifacts for managed tables about to drop.
///
/// # Errors
///
/// Returns an error when catalog SPI or object-store GC fails.
pub(super) fn cleanup_managed_tables_before_drop(
    table_oids: &[pg_sys::Oid],
) -> Result<Vec<QualifiedTableName>, String> {
    let mut mirrors = Vec::new();
    for &table_oid in table_oids {
        if !crate::catalog::cache::is_managed_relation(table_oid) {
            continue;
        }
        if let Some(mirror) = cleanup_one_managed_table_before_drop(table_oid)? {
            mirrors.push(mirror);
        }
    }
    Ok(mirrors)
}

/// Drops change-log mirrors captured before DROP (best-effort after heap gone).
pub(super) fn drop_captured_mirrors(mirrors: &[QualifiedTableName]) {
    use koldstore_wal_mirror::{plan_drop_mirror_table, MirrorRelation};

    for mirror in mirrors {
        let sql = match mirror.as_table_name() {
            Ok(table_name) => match plan_drop_mirror_table(&MirrorRelation::new(table_name)) {
                Ok(statement) => statement.sql,
                Err(error) => {
                    pgrx::warning!(
                        "koldstore drop: failed to plan drop for {}: {error}",
                        mirror.quoted()
                    );
                    continue;
                }
            },
            Err(error) => {
                pgrx::warning!(
                    "koldstore drop: invalid mirror {}: {error}",
                    mirror.quoted()
                );
                continue;
            }
        };
        if let Err(error) = pgrx::Spi::run(&sql) {
            pgrx::warning!(
                "koldstore drop: failed to drop mirror {}: {error}",
                mirror.quoted()
            );
        }
    }
}

fn cleanup_one_managed_table_before_drop(
    table_oid: pg_sys::Oid,
) -> Result<Option<QualifiedTableName>, String> {
    // Cancel first so a concurrent flush can stop at its next pass check, then
    // wait for the same advisory lock flush holds for the whole statement. That
    // serializes DROP cleanup after flush releases relation locks — no deadlock
    // between DROP AccessExclusive and flush AccessShare on heap/mirror.
    let cancelled = crate::sql::flush::jobs::cancel_jobs_for_drop(table_oid)?;
    pgrx::log!(
        "koldstore drop: oid={} cancelled_or_signalled_jobs={}",
        table_oid.to_u32(),
        cancelled
    );
    let _table_lock = crate::sql::job_lock::TableJobLockGuard::lock(table_oid)?;

    let relation = crate::catalog::resolve::relation_context(table_oid)?;
    let storage = crate::catalog::resolve::active_flush_storage_context(table_oid)?;
    let mirror = crate::catalog::resolve::mirror_relation_by_table_oid(table_oid)?;
    if let Some(mirror_relation) = &mirror {
        if crate::catalog::resolve::mirror_has_other_active_owner(table_oid, mirror_relation)? {
            return Err(format!(
                "refusing to drop managed table {}.{}: mirror {} is still referenced by another active managed table",
                relation.namespace,
                relation.name,
                mirror_relation.quoted()
            ));
        }
    }
    let prefix = render_regular_table_prefix(
        &PathTemplate::new(&storage.regular_path_tmpl),
        &relation.namespace,
        &relation.name,
    )?;
    let table = QualifiedTableName::new(Some(&relation.namespace), &relation.name)
        .map_err(|error| error.to_string())?;

    let client = crate::object_store::open_managed_object_store_client(
        &storage.storage_type,
        &storage.base_path,
        &storage.credentials,
        &storage.config,
    )
    .map_err(|error| error.to_string())?;

    // The insert-guard trigger and its function live outside the dropped table
    // (the function in the koldstore schema), so dropping the table alone would
    // leave the function behind. The trigger goes first: it depends on the function.
    for statement in crate::sql::cold_dml::guard::plan_insert_guard_teardown(&table) {
        pgrx::Spi::run(&statement).map_err(|error| error.to_string())?;
    }
    // Likewise the mirror's primary-key guard and capture functions (koldstore
    // schema), which only the mirror table's own DROP does not reach.
    if let Some(mirror_relation) = &mirror {
        let teardown = koldstore_wal_mirror::plan_mirror_source_teardown(&table, mirror_relation)
            .map_err(|error| error.to_string())?;
        for statement in teardown {
            pgrx::Spi::run(&statement.sql).map_err(|error| error.to_string())?;
        }
    }

    let plan = plan_drop_table_cleanup(
        table,
        koldstore_common::TableOid::from_raw(table_oid.to_u32()),
        DropTableCleanupPolicy::Delete,
    )
    .map_err(|error| error.to_string())?;
    for statement in &plan.statements {
        crate::spi::update(statement, &[DatumWithOid::from(table_oid)])
            .map_err(|error| error.to_string())?;
    }

    let objects = client.list(&prefix).map_err(|error| error.to_string())?;
    let staged = objects.len();
    let keys: Vec<String> = objects.into_iter().map(|object| object.key).collect();
    crate::pending_cold_delete::stage(storage, table_oid.to_u32(), keys);
    pgrx::log!(
        "koldstore drop: table_oid={} staged_for_post_commit_deletion={} prefix={}",
        table_oid.to_u32(),
        staged,
        prefix
    );

    if let Some(audit) = &plan.audit_job {
        crate::spi::update(audit, &[DatumWithOid::from(table_oid)])
            .map_err(|error| error.to_string())?;
    }

    crate::catalog::cache::invalidate_table_globally(table_oid);
    crate::spi::invalidate_all_prepared_plans();
    Ok(mirror)
}

/// Resolves managed table OIDs targeted by a `DROP TABLE` or `DROP SCHEMA`.
///
/// Uses `NoLock` so this hook does not hold relation locks while waiting for a
/// concurrent flush to finish after cancel.
///
/// # Safety
///
/// `stmt` must point at a live `DropStmt`.
pub(super) unsafe fn drop_table_oids(stmt: *mut pg_sys::DropStmt) -> Vec<pg_sys::Oid> {
    unsafe {
        if stmt.is_null() {
            return Vec::new();
        }
        match (*stmt).removeType {
            pg_sys::ObjectType::OBJECT_TABLE => drop_table_statement_oids(stmt),
            pg_sys::ObjectType::OBJECT_SCHEMA => drop_schema_managed_table_oids(stmt),
            _ => Vec::new(),
        }
    }
}

unsafe fn drop_table_statement_oids(stmt: *mut pg_sys::DropStmt) -> Vec<pg_sys::Oid> {
    unsafe {
        let objects = (*stmt).objects;
        if objects.is_null() {
            return Vec::new();
        }
        let mut oids = Vec::new();
        let count = (*objects).length as usize;
        // RVROption is a C enum: MSVC bindgen often types it as signed `c_int`,
        // while `RangeVarGetRelidExtended` takes `uint32` flags on every PG major.
        // On Unix the enum is already u32, so the cast is a no-op there.
        #[allow(clippy::unnecessary_cast)]
        let flags: u32 = if (*stmt).missing_ok {
            pg_sys::RVROption::RVR_MISSING_OK as u32
        } else {
            0
        };
        for index in 0..count {
            let names = (*(*objects).elements.add(index))
                .ptr_value
                .cast::<pg_sys::List>();
            if names.is_null() {
                continue;
            }
            let relation = pg_sys::makeRangeVarFromNameList(names);
            if relation.is_null() {
                continue;
            }
            let oid = pg_sys::RangeVarGetRelidExtended(
                relation,
                pg_sys::NoLock as pg_sys::LOCKMODE,
                flags,
                None,
                std::ptr::null_mut(),
            );
            if oid != pg_sys::InvalidOid {
                oids.push(oid);
            }
        }
        oids
    }
}

unsafe fn drop_schema_managed_table_oids(stmt: *mut pg_sys::DropStmt) -> Vec<pg_sys::Oid> {
    unsafe {
        let objects = (*stmt).objects;
        if objects.is_null() {
            return Vec::new();
        }
        let missing_ok = (*stmt).missing_ok;
        let mut oids = Vec::new();
        let count = (*objects).length as usize;
        for index in 0..count {
            // DROP SCHEMA lists its targets as bare `String` nodes (unlike DROP
            // TABLE, whose targets are name lists); reading them as lists made
            // every `DROP SCHEMA` without IF EXISTS fail with `schema "" does
            // not exist`, and skipped managed-table cleanup with IF EXISTS.
            let node = (*(*objects).elements.add(index)).ptr_value;
            if node.is_null() {
                continue;
            }
            let Some(schema_name) = schema_node_name(node.cast::<pg_sys::Node>()) else {
                continue;
            };
            oids.extend(active_managed_table_oids_in_schema(
                &schema_name,
                missing_ok,
            ));
        }
        oids
    }
}

/// The schema name of one `DROP SCHEMA` target: a `String` node, or a one-element
/// name list should a future PostgreSQL version wrap it.
unsafe fn schema_node_name(node: *mut pg_sys::Node) -> Option<String> {
    unsafe {
        match (*node).type_ {
            pg_sys::NodeTag::T_String => {
                let sval = (*node.cast::<pg_sys::String>()).sval;
                (!sval.is_null()).then(|| CStr::from_ptr(sval).to_string_lossy().into_owned())
            }
            pg_sys::NodeTag::T_List => name_list_to_string(node.cast::<pg_sys::List>()),
            _ => None,
        }
    }
}

unsafe fn name_list_to_string(names: *mut pg_sys::List) -> Option<String> {
    unsafe {
        let ptr = pg_sys::NameListToString(names);
        if ptr.is_null() {
            return None;
        }
        Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
    }
}

pub(crate) fn active_managed_table_oids_in_schema(schema_name: &str, missing_ok: bool) -> Vec<pg_sys::Oid> {
    let Ok(c_name) = CString::new(schema_name) else {
        return Vec::new();
    };
    let namespace = unsafe { pg_sys::get_namespace_oid(c_name.as_ptr(), missing_ok) };
    if namespace == pg_sys::InvalidOid {
        return Vec::new();
    }
    if !crate::catalog::cache::managed_catalog_ready() {
        return Vec::new();
    }
    pgrx::Spi::connect(|client| -> Result<Vec<pg_sys::Oid>, String> {
        let rows = client
            .select(
                "SELECT s.table_oid::oid \
                 FROM koldstore.schemas s \
                 JOIN pg_catalog.pg_class c ON c.oid = s.table_oid \
                 WHERE s.active AND c.relnamespace = $1::oid \
                 ORDER BY s.table_oid",
                None,
                &[DatumWithOid::from(namespace)],
            )
            .map_err(|error| error.to_string())?;
        let mut oids = Vec::new();
        for row in rows {
            if let Some(table_oid) = row
                .get::<pg_sys::Oid>(1)
                .map_err(|error| error.to_string())?
            {
                oids.push(table_oid);
            }
        }
        Ok(oids)
    })
    .unwrap_or_else(|error| {
        pgrx::warning!(
            "koldstore drop: failed to list managed tables in schema {schema_name}: {error}"
        );
        Vec::new()
    })
}
