//! Partitioned-parent management (ADR-008, sixth slice).
//!
//! A partitioned table has no heap of its own, so koldstore never manages it directly; every *leaf*
//! partition is an ordinary managed table (earlier slices). This module is the convenience layer that
//! makes a partitioned table behave like one managed table:
//!
//! * `manage_table(parent, ...)` manages every current leaf with the given options and records them as
//!   a **template** in `koldstore.partition_templates`;
//! * `CREATE TABLE ... PARTITION OF` / `ALTER TABLE ... ATTACH PARTITION` under a parent (or any of
//!   its sub-partitioned descendants) that has a template manage the new leaf with the same options,
//!   in the same transaction as the DDL;
//! * `unmanage_table`, `flush_table`, `enqueue_flush_job`, `table_status` and `set_table_auto_flush`
//!   on the parent fan out to the managed leaves; `DROP TABLE parent` removes the template and cleans
//!   up every managed leaf it drops.
//!
//! A leaf stays an independent managed table: detaching or unmanaging one leaf does not touch its
//! siblings or the template.

#[cfg(feature = "pg")]
use pgrx::datum::DatumWithOid;
#[cfg(feature = "pg")]
use pgrx::pg_sys::Oid;

/// Every argument of `manage_table` that shapes a managed table, as stored in the template.
#[cfg(feature = "pg")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct ManageArgs {
    pub storage: String,
    pub table_type: String,
    pub scope_column: Option<String>,
    pub migration_order_by: Option<String>,
    pub compression: Option<String>,
    pub target_file_size_mb: Option<i64>,
    pub hot_row_limit: Option<i64>,
    pub min_flush_rows: i64,
    pub max_rows_per_file: i64,
    pub auto_flush: bool,
    pub segment_order_column: Option<String>,
    pub pruning_columns: Option<Vec<String>>,
    pub bloom_filter_columns: Option<Vec<String>>,
    pub parquet_row_group_size: Option<i64>,
    pub parquet_data_page_row_count_limit: Option<i64>,
    pub parquet_bloom_filter_fpp: Option<f64>,
    pub allow_fk_hot_only: bool,
}

#[cfg(feature = "pg")]
impl ManageArgs {
    pub(super) fn manage_leaf(&self, leaf: Oid) -> pgrx::Uuid {
        super::manage::manage_table_pg_impl(
            leaf,
            &self.table_type,
            &self.storage,
            self.scope_column.as_deref(),
            self.migration_order_by.as_deref(),
            self.compression.as_deref(),
            self.target_file_size_mb,
            self.hot_row_limit,
            self.min_flush_rows,
            self.max_rows_per_file,
            self.auto_flush,
            self.segment_order_column.as_deref(),
            self.pruning_columns.clone(),
            self.bloom_filter_columns.clone(),
            self.parquet_row_group_size,
            self.parquet_data_page_row_count_limit,
            self.parquet_bloom_filter_fpp,
            self.allow_fk_hot_only,
        )
    }
}

/// A recorded template: the parent that owns it, whether new partitions are managed automatically,
/// and the options they get.
#[cfg(feature = "pg")]
pub(crate) struct Template {
    pub parent: Oid,
    pub auto_manage: bool,
    pub args: ManageArgs,
}

#[cfg(feature = "pg")]
fn spi_err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(feature = "pg")]
fn templates_table_ready() -> bool {
    pgrx::Spi::get_one::<bool>("SELECT to_regclass('koldstore.partition_templates') IS NOT NULL")
        .ok()
        .flatten()
        .unwrap_or(false)
}

/// `true` when `oid` is a partitioned table (`relkind = 'p'`).
#[cfg(feature = "pg")]
pub(crate) fn is_partitioned_parent(oid: Oid) -> bool {
    // `client.select` (not `Spi::get_one_with_args`) keeps this read from assigning the transaction
    // an XID: `manage_table` provisions the logical slot afterwards, and that refuses a transaction
    // that has already written.
    pgrx::Spi::connect(|client| -> Result<bool, pgrx::spi::Error> {
        let table = client.select(
            "SELECT relkind::text FROM pg_catalog.pg_class WHERE oid = $1",
            Some(1),
            &[DatumWithOid::from(oid)],
        )?;
        Ok(match table.into_iter().next() {
            Some(row) => row.get::<String>(1)?.as_deref() == Some("p"),
            None => false,
        })
    })
    .unwrap_or(false)
}

/// Every leaf partition under `parent` (any depth), in OID order.
#[cfg(feature = "pg")]
fn leaf_oids(parent: Oid) -> Result<Vec<Oid>, String> {
    oid_list(
        "SELECT t.relid::oid FROM pg_catalog.pg_partition_tree($1::oid::regclass) t \
         WHERE t.isleaf ORDER BY t.relid::oid",
        parent,
    )
}

/// Leaf partitions under `parent` that are active managed tables.
#[cfg(feature = "pg")]
fn managed_leaf_oids(parent: Oid) -> Result<Vec<Oid>, String> {
    Ok(leaf_oids(parent)?
        .into_iter()
        .filter(|leaf| crate::catalog::cache::is_managed_relation(*leaf))
        .collect())
}

#[cfg(feature = "pg")]
fn oid_list(sql: &str, arg: Oid) -> Result<Vec<Oid>, String> {
    pgrx::Spi::connect(|client| -> Result<Vec<Oid>, pgrx::spi::Error> {
        let mut oids = Vec::new();
        for row in client.select(sql, None, &[DatumWithOid::from(arg)])? {
            if let Some(oid) = row.get::<Oid>(1)? {
                oids.push(oid);
            }
        }
        Ok(oids)
    })
    .map_err(spi_err)
}

#[cfg(feature = "pg")]
fn relation_name(oid: Oid) -> String {
    crate::catalog::resolve::qualified_relation_name(oid)
        .unwrap_or_else(|_| format!("(oid {})", oid.to_u32()))
}

#[cfg(feature = "pg")]
fn load_template(parent: Oid) -> Result<Option<Template>, String> {
    if !templates_table_ready() {
        return Ok(None);
    }
    let row = pgrx::Spi::connect(
        |client| -> Result<Option<(pgrx::JsonB, bool)>, pgrx::spi::Error> {
            let table = client.select(
                "SELECT options, auto_manage FROM koldstore.partition_templates WHERE parent_oid = $1",
                Some(1),
                &[DatumWithOid::from(parent)],
            )?;
            let Some(row) = table.into_iter().next() else {
                return Ok(None);
            };
            Ok(Some((
                row.get::<pgrx::JsonB>(1)?.unwrap_or(pgrx::JsonB(serde_json::Value::Null)),
                row.get::<bool>(2)?.unwrap_or(true),
            )))
        },
    )
    .map_err(spi_err)?;
    let Some((options, auto_manage)) = row else {
        return Ok(None);
    };
    let args = serde_json::from_value(options.0)
        .map_err(|error| format!("partition template for {} is unreadable: {error}", relation_name(parent)))?;
    Ok(Some(Template { parent, auto_manage, args }))
}

/// The template governing `partition`: the nearest ancestor that has one.
#[cfg(feature = "pg")]
fn template_for_partition(partition: Oid) -> Result<Option<Template>, String> {
    if !templates_table_ready() {
        return Ok(None);
    }
    let parent = pgrx::Spi::connect(|client| -> Result<Option<Oid>, pgrx::spi::Error> {
        let table = client.select(
            "SELECT t.parent_oid FROM koldstore.partition_templates t \
             JOIN pg_catalog.pg_partition_ancestors($1::oid::regclass) a ON a.relid::oid = t.parent_oid \
             WHERE a.relid::oid <> $1 \
             ORDER BY (SELECT count(*) FROM pg_catalog.pg_partition_ancestors(t.parent_oid::regclass)) DESC \
             LIMIT 1",
            Some(1),
            &[DatumWithOid::from(partition)],
        )?;
        match table.into_iter().next() {
            Some(row) => row.get::<Oid>(1),
            None => Ok(None),
        }
    })
    .map_err(spi_err)?;
    match parent {
        Some(parent) => load_template(parent),
        None => Ok(None),
    }
}

/// `manage_table(parent)`: manages every current leaf and records the template.
#[cfg(feature = "pg")]
pub(crate) fn manage_parent(parent: Oid, args: ManageArgs) -> pgrx::Uuid {
    // Provision logical capture first: it must run before this transaction takes an XID, and a
    // parent with no leaves yet still needs the infrastructure for the partitions it will get.
    crate::mirror::lifecycle::prepare_capture()
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let relation = relation_name(parent);
    if !templates_table_ready() {
        pgrx::error!(
            "migrate table failed: koldstore.partition_templates is missing -- run \
             ALTER EXTENSION koldstore UPDATE, or SELECT koldstore.sync_sql_objects(dry_run => false)"
        );
    }
    if load_template(parent)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"))
        .is_some()
    {
        pgrx::error!(
            "migrate table failed: {relation} is already managed as a partitioned table; \
             unmanage_table() it first to change its options"
        );
    }
    let leaves = leaf_oids(parent).unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let mut managed = 0_usize;
    for leaf in leaves {
        let name = relation_name(leaf);
        let relkind = pgrx::Spi::get_one_with_args::<String>(
            "SELECT relkind::text FROM pg_catalog.pg_class WHERE oid = $1",
            &[DatumWithOid::from(leaf)],
        )
        .ok()
        .flatten()
        .unwrap_or_default();
        if relkind != "r" {
            pgrx::notice!("koldstore: skipping partition {name}: it is not an ordinary table (relkind {relkind})");
            continue;
        }
        if crate::catalog::cache::is_managed_relation(leaf) {
            pgrx::notice!("koldstore: partition {name} is already managed; keeping its existing options");
            continue;
        }
        args.manage_leaf(leaf);
        managed += 1;
    }
    let options = serde_json::to_value(&args)
        .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"));
    let template_id = pgrx::Spi::get_one_with_args::<pgrx::Uuid>(
        "WITH stale AS (DELETE FROM koldstore.partition_templates t \
                        WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.oid = t.parent_oid)) \
         INSERT INTO koldstore.partition_templates (parent_oid, options) VALUES ($1, $2) \
         RETURNING template_id",
        &[DatumWithOid::from(parent), DatumWithOid::from(pgrx::JsonB(options))],
    )
    .unwrap_or_else(|error| pgrx::error!("migrate table failed: {error}"))
    .unwrap_or_else(|| pgrx::error!("migrate table failed: partition template was not recorded"));
    pgrx::notice!("koldstore: {relation} is managed as a partitioned table ({managed} partition(s) managed now; new partitions are managed automatically)");
    template_id
}

/// Manages a freshly created or attached partition when an ancestor has an auto-manage template.
///
/// Called from the utility hook right after `CREATE TABLE ... PARTITION OF` / `ATTACH PARTITION`.
#[cfg(feature = "pg")]
pub(crate) fn auto_manage_new_partition(partition: Oid) {
    let relkind = pgrx::Spi::get_one_with_args::<String>(
        "SELECT relkind::text FROM pg_catalog.pg_class WHERE oid = $1",
        &[DatumWithOid::from(partition)],
    )
    .ok()
    .flatten()
    .unwrap_or_default();
    // A sub-partitioned node has no heap; its future leaves are managed when they appear.
    if relkind != "r" || crate::catalog::cache::is_managed_relation(partition) {
        return;
    }
    let template = match template_for_partition(partition) {
        Ok(Some(template)) if template.auto_manage => template,
        Ok(_) => return,
        Err(error) => {
            // An unreadable template must not break DDL that has nothing to do with koldstore.
            pgrx::warning!("koldstore: partition template lookup failed; leaving the new partition unmanaged: {error}");
            return;
        }
    };
    let name = relation_name(partition);
    let parent = relation_name(template.parent);
    pgrx::notice!("koldstore: managing new partition {name} (template of {parent})");
    template.args.manage_leaf(partition);
}

/// `unmanage_table(parent)`: unmanages every managed leaf and forgets the template.
#[cfg(feature = "pg")]
pub(crate) fn unmanage_parent(
    parent: Oid,
    options: koldstore_migrate::rehydrate::DemigrateOptions,
) -> i64 {
    let mut total = 0_i64;
    for leaf in managed_leaf_oids(parent).unwrap_or_else(|error| pgrx::error!("unmanage table failed: {error}")) {
        total += super::unmanage::unmanage_table_pg_impl(leaf, options.clone())
            .unwrap_or_else(|error| pgrx::error!("unmanage table failed: {error}"));
    }
    forget_template(parent).unwrap_or_else(|error| pgrx::error!("unmanage table failed: {error}"));
    total
}

#[cfg(feature = "pg")]
fn forget_template(parent: Oid) -> Result<(), String> {
    if !templates_table_ready() {
        return Ok(());
    }
    pgrx::Spi::run_with_args(
        "DELETE FROM koldstore.partition_templates WHERE parent_oid = $1",
        &[DatumWithOid::from(parent)],
    )
    .map_err(spi_err)
}

/// `flush_table(parent)`: flushes every managed leaf.
#[cfg(feature = "pg")]
pub(crate) fn flush_parent(parent: Oid, force: bool) -> pgrx::JsonB {
    let mut results = Vec::new();
    let mut all_ok = true;
    for leaf in managed_leaf_oids(parent).unwrap_or_else(|error| pgrx::error!("flush table failed: {error}")) {
        let name = relation_name(leaf);
        let response = crate::sql::flush::execute::flush_table_pg_impl(leaf, force)
            .unwrap_or_else(|error| pgrx::error!("flush table failed for partition {name}: {error}"));
        all_ok &= response.ok;
        let mut value = response.to_jsonb().0;
        if let Some(object) = value.as_object_mut() {
            object.insert("table".to_string(), serde_json::Value::String(name));
        }
        results.push(value);
    }
    pgrx::JsonB(serde_json::json!({
        "partitioned_parent": true,
        "ok": all_ok,
        "partitions_flushed": results.len(),
        "partitions": results,
    }))
}

/// `enqueue_flush_job(parent)`: enqueues every managed leaf; returns the first job id, if any.
#[cfg(feature = "pg")]
pub(crate) fn enqueue_parent(parent: Oid, force: bool) -> Option<pgrx::Uuid> {
    let mut first = None;
    for leaf in managed_leaf_oids(parent).unwrap_or_else(|error| pgrx::error!("enqueue flush job failed: {error}")) {
        let job = crate::sql::flush::jobs::enqueue_flush_job_if_due(leaf, force)
            .unwrap_or_else(|error| pgrx::error!("enqueue flush job failed: {error}"));
        first = first.or(job);
    }
    first
}

/// `set_table_auto_flush(parent, enabled)`: applies to every managed leaf and to the template, so
/// partitions created later follow the same setting.
#[cfg(feature = "pg")]
pub(crate) fn set_auto_flush_parent(parent: Oid, enabled: bool) -> bool {
    for leaf in managed_leaf_oids(parent).unwrap_or_else(|error| pgrx::error!("set_table_auto_flush failed: {error}")) {
        super::manage::set_table_auto_flush_pg_impl(leaf, enabled)
            .unwrap_or_else(|error| pgrx::error!("set_table_auto_flush failed: {error}"));
    }
    if templates_table_ready() {
        pgrx::Spi::run_with_args(
            "UPDATE koldstore.partition_templates \
             SET options = jsonb_set(options, '{auto_flush}', to_jsonb($2::boolean)) WHERE parent_oid = $1",
            &[DatumWithOid::from(parent), DatumWithOid::from(enabled)],
        )
        .unwrap_or_else(|error| pgrx::error!("set_table_auto_flush failed: {error}"));
    }
    true
}

/// `set_partition_auto_manage(parent, enabled)`: pauses or resumes managing new partitions.
#[cfg(feature = "pg")]
pub(crate) fn set_auto_manage(parent: Oid, enabled: bool) -> bool {
    pgrx::Spi::get_one_with_args::<bool>(
        "WITH u AS (UPDATE koldstore.partition_templates SET auto_manage = $2 \
                    WHERE parent_oid = $1 RETURNING 1) SELECT count(*) > 0 FROM u",
        &[DatumWithOid::from(parent), DatumWithOid::from(enabled)],
    )
    .unwrap_or_else(|error| pgrx::error!("set_partition_auto_manage failed: {error}"))
    .unwrap_or(false)
}

/// `table_status(parent)`: a roll-up of every managed leaf plus a per-partition breakdown.
#[cfg(feature = "pg")]
pub(crate) fn status_parent(parent: Oid) -> Result<serde_json::Value, String> {
    let template = load_template(parent)?;
    let leaves = leaf_oids(parent)?;
    let mut partitions = Vec::new();
    let mut sums = serde_json::Map::new();
    let mut managed = 0_u64;
    for leaf in &leaves {
        let name = relation_name(*leaf);
        if !crate::catalog::cache::is_managed_relation(*leaf) {
            partitions.push(serde_json::json!({ "table": name, "managed": false }));
            continue;
        }
        managed += 1;
        let mut fields = crate::sql::ops::table_status_fields(*leaf)?;
        if let Some(object) = fields.as_object() {
            for key in [
                "hot_rows",
                "mirror_rows",
                "cold_row_count",
                "cold_segment_count",
                "pending_jobs",
                "heap_size_bytes",
                "index_size_bytes",
                "table_size_bytes",
            ] {
                if let Some(number) = object.get(key).and_then(serde_json::Value::as_i64) {
                    let entry = sums.entry(key.to_string()).or_insert(serde_json::json!(0));
                    *entry = serde_json::json!(entry.as_i64().unwrap_or(0) + number);
                }
            }
        }
        if let Some(object) = fields.as_object_mut() {
            object.remove("jobs");
            object.insert("table".to_string(), serde_json::Value::String(name));
            object.insert("managed".to_string(), serde_json::Value::Bool(true));
        }
        partitions.push(fields);
    }
    let mut value = serde_json::Map::new();
    value.insert("partitioned_parent".into(), true.into());
    value.insert("partitions_total".into(), leaves.len().into());
    value.insert("partitions_managed".into(), managed.into());
    value.insert(
        "auto_manage_new_partitions".into(),
        template.as_ref().is_some_and(|t| t.auto_manage).into(),
    );
    value.insert(
        "template".into(),
        template
            .as_ref()
            .and_then(|t| serde_json::to_value(&t.args).ok())
            .unwrap_or(serde_json::Value::Null),
    );
    value.extend(sums);
    value.insert("partitions".into(), serde_json::Value::Array(partitions));
    Ok(serde_json::Value::Object(value))
}

/// Widens a `DROP TABLE` target list for the cleanup that runs before the drop: dropping a
/// partitioned table drops its partitions too, so every managed leaf underneath needs the same
/// cold-object and catalog cleanup as a table dropped by name; templates of every partitioned node
/// dropped with it are forgotten.
#[cfg(feature = "pg")]
pub(crate) fn expand_drop_targets(targets: Vec<Oid>) -> Vec<Oid> {
    let mut expanded = Vec::with_capacity(targets.len());
    for oid in targets {
        expanded.push(oid);
        if !is_partitioned_parent(oid) {
            continue;
        }
        if let Ok(leaves) = leaf_oids(oid) {
            expanded.extend(leaves);
        }
        if templates_table_ready() {
            let _ = pgrx::Spi::run_with_args(
                "DELETE FROM koldstore.partition_templates \
                 WHERE parent_oid IN (SELECT t.relid::oid FROM pg_catalog.pg_partition_tree($1::oid::regclass) t)",
                &[DatumWithOid::from(oid)],
            );
        }
    }
    let mut seen = std::collections::HashSet::new();
    expanded.retain(|oid| seen.insert(oid.to_u32()));
    expanded
}
