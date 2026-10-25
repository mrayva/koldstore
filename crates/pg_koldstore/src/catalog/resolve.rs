//! PostgreSQL-backed catalog resolvers.

use koldstore_common::StorageId;
use koldstore_migrate::QualifiedTableName;

use crate::{
    catalog::{decode, queries},
    spi,
};

/// Resolves a fully qualified relation name by relation OID.
///
/// # Errors
///
/// Returns an error when SPI execution fails or the relation does not exist.
pub fn qualified_relation_name(table_oid: pgrx::pg_sys::Oid) -> Result<String, String> {
    let statement = queries::plan_qualified_relation_by_oid().map_err(|error| error.to_string())?;
    spi::select_one::<String>(&statement, &[pgrx::datum::DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("table oid {} does not exist", table_oid.to_u32()))
}

/// Resolves namespace and relation name by relation OID.
///
/// # Errors
///
/// Returns an error when SPI execution or JSON decoding fails.
pub fn relation_context(table_oid: pgrx::pg_sys::Oid) -> Result<decode::RelationContext, String> {
    let statement = queries::plan_relation_context_by_oid().map_err(|error| error.to_string())?;
    let value = spi::select_json_one(&statement, &[pgrx::datum::DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?;
    decode::relation_context(&value)
}

/// Resolves the active mirror relation for a managed table OID.
///
/// # Errors
///
/// Returns an error when SPI execution fails or the relation cannot be parsed.
pub fn mirror_relation_by_table_oid(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Option<QualifiedTableName>, String> {
    let statement =
        queries::plan_mirror_relation_by_table_oid().map_err(|error| error.to_string())?;
    let relation =
        spi::select_one::<String>(&statement, &[pgrx::datum::DatumWithOid::from(table_oid)])
            .map_err(|error| error.to_string())?;

    relation
        .map(|relation| QualifiedTableName::parse(&relation).map_err(|error| error.to_string()))
        .transpose()
}

/// Returns whether another active managed table references `mirror_relation`.
///
/// # Errors
///
/// Returns an error when PostgreSQL cannot inspect the managed-schema catalog.
pub fn mirror_has_other_active_owner(
    table_oid: pgrx::pg_sys::Oid,
    mirror_relation: &QualifiedTableName,
) -> Result<bool, String> {
    use pgrx::datum::DatumWithOid;

    let mirror_relation = mirror_relation.quoted();
    pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (\
           SELECT 1 \
           FROM koldstore.schemas \
           WHERE active \
             AND table_oid <> $1::oid \
             AND mirror_relation = pg_catalog.to_regclass($2)\
         )",
        &[
            DatumWithOid::from(table_oid),
            DatumWithOid::from(mirror_relation.as_str()),
        ],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "mirror ownership query returned no result".to_string())
}

/// Resolves a registered storage ID by name.
///
/// # Errors
///
/// Returns an error when SPI execution fails.
pub fn storage_id_by_name(name: &str) -> Result<Option<StorageId>, String> {
    let statement = queries::plan_storage_id_by_name().map_err(|error| error.to_string())?;
    let id = spi::select_one::<String>(&statement, &[pgrx::datum::DatumWithOid::from(name)])
        .map_err(|error| error.to_string())?;
    id.map(|value| StorageId::new(value).map_err(|error| error.to_string()))
        .transpose()
}

/// Resolves active schema/storage metadata required by flush.
///
/// # Errors
///
/// Returns an error when SPI execution or JSON decoding fails.
pub fn active_flush_storage_context(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<decode::FlushStorageContext, String> {
    let statement =
        queries::plan_active_flush_storage_context().map_err(|error| error.to_string())?;
    let value = spi::select_json_one(&statement, &[pgrx::datum::DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?;
    decode::flush_storage_context(&value)
}

/// Pins the table's current object prefix so a rename / schema move cannot relocate its published
/// cold objects (upstream #123). No-op for unmanaged tables and for tables already pinned.
///
/// # Errors
///
/// Returns an error when the prefix cannot be resolved or the pin cannot be written.
pub fn pin_table_prefix(table_oid: pgrx::pg_sys::Oid) -> Result<(), String> {
    if !super::cache::is_managed_relation(table_oid) {
        return Ok(());
    }
    let relation = relation_context(table_oid)?;
    let storage = active_flush_storage_context(table_oid)?;
    if storage.pinned_prefix.is_some() {
        return Ok(());
    }
    let prefix = table_object_prefix(&storage, &relation.namespace, &relation.name)?;
    pgrx::Spi::run_with_args(
        "INSERT INTO koldstore.table_prefix_pins (table_oid, prefix) VALUES ($1::oid, $2) \
         ON CONFLICT (table_oid) DO NOTHING",
        &[
            pgrx::datum::DatumWithOid::from(table_oid),
            pgrx::datum::DatumWithOid::from(prefix),
        ],
    )
    .map_err(|error| error.to_string())
}

/// Refuses to manage a table whose derived object prefix is already pinned by another (renamed) table,
/// which would interleave two tables' cold segments under one prefix.
///
/// # Errors
///
/// Returns an error describing the collision, or when the lookup fails.
pub fn check_prefix_pin_collision(
    table_oid: pgrx::pg_sys::Oid,
    storage_name: &str,
) -> Result<(), String> {
    let template = pgrx::Spi::connect(|client| -> Result<Option<String>, String> {
        let rows = client
            .select(
                "SELECT regular_path_tmpl FROM koldstore.storage WHERE name = $1",
                None,
                &[pgrx::datum::DatumWithOid::from(storage_name)],
            )
            .map_err(|error| error.to_string())?;
        for row in rows {
            return row.get::<String>(1).map_err(|error| error.to_string());
        }
        Ok(None)
    })?;
    let Some(template) = template else {
        return Ok(());
    };
    let relation = relation_context(table_oid)?;
    let prefix = koldstore_storage::render_regular_table_prefix(
        &koldstore_storage::PathTemplate::new(&template),
        &relation.namespace,
        &relation.name,
    )?;
    let owner = pgrx::Spi::connect(|client| -> Result<Option<u32>, String> {
        let rows = client
            .select(
                "SELECT table_oid::oid FROM koldstore.table_prefix_pins \
                 WHERE prefix = $1 AND table_oid <> $2::oid LIMIT 1",
                None,
                &[
                    pgrx::datum::DatumWithOid::from(prefix.clone()),
                    pgrx::datum::DatumWithOid::from(table_oid),
                ],
            )
            .map_err(|error| error.to_string())?;
        for row in rows {
            return Ok(row
                .get::<pgrx::pg_sys::Oid>(1)
                .map_err(|error| error.to_string())?
                .map(pgrx::pg_sys::Oid::to_u32));
        }
        Ok(None)
    })?;
    match owner {
        Some(owner) => Err(format!(
            "object prefix \"{prefix}\" is still pinned by another managed table (oid {owner}) that was renamed or moved away from this name; unmanage or drop that table, or rename this one",
        )),
        None => Ok(()),
    }
}

/// Object prefix of a managed table's cold objects: the one pinned by a rename / schema move, else
/// rendered from the storage template and the table's current names.
///
/// # Errors
///
/// Returns an error when the storage's path template is invalid.
pub fn table_object_prefix(
    storage: &decode::FlushStorageContext,
    namespace: &str,
    name: &str,
) -> Result<String, String> {
    if let Some(pinned) = &storage.pinned_prefix {
        return Ok(pinned.clone());
    }
    koldstore_storage::render_regular_table_prefix(
        &koldstore_storage::PathTemplate::new(&storage.regular_path_tmpl),
        namespace,
        name,
    )
}
