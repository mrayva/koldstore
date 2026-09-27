//! Demigration and managed-table teardown execution.
#[cfg(feature = "pg")]
use koldstore_migrate::rehydrate::{ColdArtifactAction, DemigrateOptions};
#[cfg(feature = "pg")]
pub(super) fn unmanage_table_pg_impl(
    table_oid: pgrx::pg_sys::Oid,
    options: DemigrateOptions,
) -> Result<i64, String> {
    use koldstore_migrate::rehydrate::{demigration_context, plan_demigration};

    let table_oid_u32 = table_oid.to_u32();
    let relation = crate::catalog::resolve::qualified_relation_name(table_oid)?;
    let timer = koldstore_common::TimedOp::start(
        koldstore_common::log::component::UNMANAGE,
        format!("table={relation}"),
    );
    let table = koldstore_migrate::QualifiedTableName::parse(&relation)
        .map_err(|error| error.to_string())?;
    let mirror_table = crate::catalog::resolve::mirror_relation_by_table_oid(table_oid)?;
    if let Some(mirror_table) = &mirror_table {
        if crate::catalog::resolve::mirror_has_other_active_owner(table_oid, mirror_table)? {
            return Err(format!(
                "refusing to unmanage {relation}: mirror {} is still referenced by another active managed table",
                mirror_table.quoted()
            ));
        }
    }
    let context = demigration_context(
        table,
        koldstore_common::TableOid::from_raw(table_oid_u32),
        mirror_table,
    );
    let plan = plan_demigration(context, options).map_err(|error| error.to_string())?;
    // Resolve while the table is still active: catalog rows this reads are cleared by the
    // deactivation statements below (mirrors `hooks::drop_cleanup`'s DROP TABLE ordering, the only
    // other place koldstore deletes cold objects). `drop_cold` only deletes after `execute_
    // demigration_statements` succeeds, so a failed rehydrate never touches storage. The prefix is
    // recomputed from the storage registration's actual `regular_path_tmpl` here rather than trusting
    // `plan.cold_artifact_action`'s prefix, which `koldstore-migrate` (no catalog access) can only
    // guess at the *default* template -- a custom one would otherwise delete the wrong location, or
    // nothing at all.
    let storage_prefix = match &plan.cold_artifact_action {
        ColdArtifactAction::DeleteAfterRehydrate { .. } => {
            let storage = crate::catalog::resolve::active_flush_storage_context(table_oid)?;
            let relation = crate::catalog::resolve::relation_context(table_oid)?;
            let prefix = koldstore_storage::render_regular_table_prefix(
                &koldstore_storage::PathTemplate::new(&storage.regular_path_tmpl),
                &relation.namespace,
                &relation.name,
            )?;
            Some((storage, prefix))
        }
        ColdArtifactAction::Retain => None,
    };

    execute_demigration_locks(&plan)?;
    // Rehydrate's own final step re-INSERTs every row (hot or cold) back
    // into the real heap via `plan_rehydrate_heap` -- a legitimate internal
    // write that must not trip this table's own cold-DML insert guard (see
    // `guard::with_guard_suspended`'s doc comment; the same class of
    // exemption `AllowManagedTruncateGuard` a few lines below already
    // grants that step's TRUNCATE against koldstore's own ProcessUtility
    // guard).
    let deactivated =
        // The re-insert of every cold row is hydration too: run it with user triggers and
        // referential-integrity triggers off, like `hydrate_pk`, so unmanaging neither
        // fires an INSERT trigger per row nor fails on a cold child whose parent is gone.
        crate::sql::cold_dml::as_hydration(|| {
            crate::sql::cold_dml::guard::with_guard_suspended(|| execute_demigration_statements(&plan, table_oid))
        })?;

    let source = koldstore_common::QualifiedTableName::parse(&relation).map_err(|error| error.to_string())?;
    for statement in crate::sql::cold_dml::guard::plan_insert_guard_teardown(&source) {
        pgrx::Spi::run(&statement).map_err(|error| error.to_string())?;
    }

    if let Some((storage, prefix)) = &storage_prefix {
        delete_cold_artifacts(storage, prefix, table_oid_u32)?;
    }

    crate::catalog::cache::invalidate_table_globally(table_oid);
    crate::spi::invalidate_all_prepared_plans();

    timer.finish(format!(
        "unmanaged table={relation} schemas_deactivated={deactivated}"
    ));
    Ok(deactivated)
}

/// Deletes every object under `prefix` in `storage` (best-effort: logs and continues on a
/// single-object failure rather than leaving the table stuck half-torn-down after the heap has
/// already been rebuilt -- the same tradeoff `hooks::drop_cleanup` makes for DROP TABLE).
#[cfg(feature = "pg")]
fn delete_cold_artifacts(
    storage: &koldstore_catalog::decode::FlushStorageContext,
    prefix: &str,
    table_oid_u32: u32,
) -> Result<(), String> {
    use koldstore_storage::StorageClient;

    let client = crate::object_store::open_managed_object_store_client(
        &storage.storage_type,
        &storage.base_path,
        &storage.credentials,
        &storage.config,
    )
    .map_err(|error| error.to_string())?;
    let objects = client.list(prefix).map_err(|error| error.to_string())?;
    let mut deleted = 0_usize;
    for object in &objects {
        if let Err(error) = client.delete(&object.key) {
            pgrx::warning!(
                "koldstore unmanage: table_oid={table_oid_u32} failed to delete cold object {}: {error}",
                object.key
            );
            continue;
        }
        deleted += 1;
    }
    pgrx::log!(
        "koldstore unmanage: table_oid={table_oid_u32} deleted_objects={deleted}/{} prefix={prefix}",
        objects.len()
    );
    Ok(())
}

#[cfg(feature = "pg")]
fn execute_demigration_locks(
    plan: &koldstore_migrate::rehydrate::DemigrationPlan,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    for (index, statement) in plan.lock.statements.iter().enumerate() {
        if index == 0 {
            pgrx::Spi::run_with_args(
                &statement.sql,
                &[DatumWithOid::from(
                    plan.lock.lock_key.as_advisory_lock_key(),
                )],
            )
            .map_err(|error| error.to_string())?;
        } else {
            pgrx::Spi::run(&statement.sql).map_err(|error| error.to_string())?;
        }
    }

    Ok(())
}

#[cfg(feature = "pg")]
fn execute_demigration_statements(
    plan: &koldstore_migrate::rehydrate::DemigrationPlan,
    table_oid: pgrx::pg_sys::Oid,
) -> Result<i64, String> {
    use pgrx::datum::DatumWithOid;

    // Rehydrate issues `TRUNCATE TABLE ONLY <managed>` before catalog deactivation.
    // The ProcessUtility TRUNCATE guard must allow that internal path only.
    let _allow_truncate = crate::hooks::ddl::AllowManagedTruncateGuard::enter();

    let statement_count = plan.statements.len();
    let mut deactivated = 0_i64;

    for (index, statement) in plan.statements.iter().enumerate() {
        if index + 2 == statement_count {
            deactivated = pgrx::Spi::get_one_with_args::<i64>(
                &statement.sql,
                &[DatumWithOid::from(table_oid)],
            )
            .map_err(|error| error.to_string())?
            .unwrap_or(0);
        } else if index + 1 == statement_count {
            pgrx::Spi::run_with_args(&statement.sql, &[DatumWithOid::from(table_oid)])
                .map_err(|error| error.to_string())?;
        } else {
            pgrx::Spi::run(&statement.sql).map_err(|error| error.to_string())?;
        }
    }

    Ok(deactivated)
}
