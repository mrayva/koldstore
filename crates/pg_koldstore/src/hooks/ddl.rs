//! DDL and ProcessUtility integration for KoldStore table options.
//!
//! DROP cleanup planning lives in `koldstore-migrate`; this module
//! re-exports those plans for the extension shell and installs the live
//! ProcessUtility hook used for `ALTER TABLE … SET/RESET` KoldStore options
//! and managed `DROP TABLE` / `DROP SCHEMA … CASCADE` teardown. Managed-table
//! schema changes (including `RENAME COLUMN`) also refresh catalog metadata and
//! rename-sensitive runtime artifacts so DML keeps working without waiting for
//! the next flush.

pub use koldstore_migrate::drop_table::{
    plan_drop_table_cleanup, DropTableCleanupError, DropTableCleanupOutcome, DropTableCleanupPlan,
    DropTableCleanupPolicy,
};

#[cfg(feature = "pg")]
mod process_utility {
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::ffi::CStr;
    use std::sync::atomic::{AtomicBool, Ordering};

    use pgrx::pg_sys;

    use crate::hooks::drop_cleanup::{
        cleanup_managed_tables_before_drop, drop_captured_mirrors, drop_index_table_oids, drop_table_oids,
    };

    static REGISTERED: AtomicBool = AtomicBool::new(false);
    static mut PREVIOUS: pg_sys::ProcessUtility_hook_type = None;

    // Nesting counter so demigrate/unmanage can TRUNCATE the heap while rebuilding
    // it, without opening a hole for user-issued TRUNCATE on managed tables.
    thread_local! {
        static ALLOW_MANAGED_TRUNCATE: Cell<u32> = const { Cell::new(0) };
    }

    /// RAII guard that permits managed-table TRUNCATE for demigrate rehydrate.
    pub(crate) struct AllowManagedTruncateGuard;

    impl AllowManagedTruncateGuard {
        /// Enters a demigrate/unmanage scope that may issue internal TRUNCATE.
        #[must_use]
        pub(crate) fn enter() -> Self {
            ALLOW_MANAGED_TRUNCATE.with(|depth| depth.set(depth.get().saturating_add(1)));
            Self
        }
    }

    impl Drop for AllowManagedTruncateGuard {
        fn drop(&mut self) {
            ALLOW_MANAGED_TRUNCATE.with(|depth| depth.set(depth.get().saturating_sub(1)));
        }
    }

    fn managed_truncate_allowed() -> bool {
        ALLOW_MANAGED_TRUNCATE.with(|depth| depth.get() > 0)
    }

    pub(super) fn register() {
        if REGISTERED.swap(true, Ordering::AcqRel) {
            return;
        }
        unsafe {
            PREVIOUS = pg_sys::ProcessUtility_hook;
            pg_sys::ProcessUtility_hook = Some(hook);
        }
    }

    // Signature must match PostgreSQL ProcessUtility_hook_type.
    #[allow(clippy::too_many_arguments)]
    #[pgrx::pg_guard]
    unsafe extern "C-unwind" fn hook(
        pstmt: *mut pg_sys::PlannedStmt,
        query: *const core::ffi::c_char,
        read_only: bool,
        context: pg_sys::ProcessUtilityContext::Type,
        params: pg_sys::ParamListInfo,
        env: *mut pg_sys::QueryEnvironment,
        dest: *mut pg_sys::DestReceiver,
        qc: *mut pg_sys::QueryCompletion,
    ) {
        unsafe {
            if crate::thread_guard::is_foreign() {
                match PREVIOUS {
                    Some(previous) => previous(pstmt, query, read_only, context, params, env, dest, qc),
                    None => crate::thread_guard::standard::standard_ProcessUtility(
                        pstmt, query, read_only, context, params, env, dest, qc,
                    ),
                }
                return;
            }
            let copied = pg_sys::copyObjectImpl(pstmt.cast()).cast::<pg_sys::PlannedStmt>();
            let mut captured = None;
            let mut has_standard_actions = true;
            let mut drop_oids = Vec::new();
            let mut truncate_oids = Vec::new();
            let mut refresh_oid = None;
            let mut unique_guard_oid = None;
            // Table whose foreign keys may now point at a managed table.
            let mut fk_child_relation: Option<*mut pg_sys::RangeVar> = None;
            // Managed tables the statement's target referenced before it ran (their guards go stale
            // when a foreign key or the whole child disappears).
            let mut fk_parents_before: Vec<pg_sys::Oid> = Vec::new();
            // Tables whose unique indexes may be dropped by this statement.
            let mut dropped_index_tables: Vec<pg_sys::Oid> = Vec::new();
            let mut renamed_schema = None;
            let mut copy_from_oid = None;
            // Partitions created or attached by this statement; a partition template manages them.
            let mut new_partitions: Vec<*mut pg_sys::RangeVar> = Vec::new();
            if !copied.is_null() && !(*copied).utilityStmt.is_null() {
                match (*(*copied).utilityStmt).type_ {
                    pg_sys::NodeTag::T_CreateStmt => {
                        let stmt = (*copied).utilityStmt.cast::<pg_sys::CreateStmt>();
                        reject_create_in_managed_hierarchy(stmt);
                        fk_child_relation = Some((*stmt).relation);
                        if !(*stmt).partbound.is_null() {
                            new_partitions.push((*stmt).relation);
                        }
                    }
                    pg_sys::NodeTag::T_AlterTableStmt => {
                        let stmt = (*copied).utilityStmt.cast::<pg_sys::AlterTableStmt>();
                        reject_alter_hierarchy_of_managed(stmt);
                        collect_attached_partitions(stmt, &mut new_partitions);
                        captured = strip_options(stmt);
                        has_standard_actions = !(*stmt).cmds.is_null();
                        refresh_oid = relation_oid_from_range_var((*stmt).relation);
                        unique_guard_oid = refresh_oid;
                        fk_child_relation = Some((*stmt).relation);
                        if crate::catalog::cache::managed_catalog_ready() {
                            if let Some(child_oid) = refresh_oid {
                                fk_parents_before = crate::sql::cold_dml::guard::referenced_managed_parents(child_oid);
                            }
                        }
                    }
                    pg_sys::NodeTag::T_IndexStmt => {
                        let stmt = (*copied).utilityStmt.cast::<pg_sys::IndexStmt>();
                        if (*stmt).unique {
                            unique_guard_oid = relation_oid_from_range_var((*stmt).relation);
                        }
                    }
                    pg_sys::NodeTag::T_RenameStmt => {
                        // `ALTER TABLE … RENAME COLUMN` is RenameStmt, not AlterTableStmt.
                        let stmt = (*copied).utilityStmt.cast::<pg_sys::RenameStmt>();
                        pin_prefix_before_rename(stmt);
                        refresh_oid = rename_stmt_relation_oid(stmt);
                        renamed_schema = rename_stmt_schema_name(stmt);
                    }
                    pg_sys::NodeTag::T_AlterObjectSchemaStmt => {
                        // `ALTER TABLE … SET SCHEMA` is neither AlterTableStmt nor RenameStmt.
                        let stmt = (*copied)
                            .utilityStmt
                            .cast::<pg_sys::AlterObjectSchemaStmt>();
                        pin_prefix_before_set_schema(stmt);
                        refresh_oid = alter_object_schema_relation_oid(stmt);
                    }
                    pg_sys::NodeTag::T_DropStmt => {
                        let stmt = (*copied).utilityStmt.cast::<pg_sys::DropStmt>();
                        drop_oids = drop_table_oids(stmt);
                        if crate::catalog::cache::managed_catalog_ready() {
                            for oid in &drop_oids {
                                fk_parents_before
                                    .extend(crate::sql::cold_dml::guard::referenced_managed_parents(*oid));
                            }
                            dropped_index_tables = drop_index_table_oids(stmt);
                        }
                        if crate::catalog::cache::managed_catalog_ready() {
                            drop_oids = crate::sql::migrate::partition::expand_drop_targets(drop_oids);
                        }
                    }
                    pg_sys::NodeTag::T_CopyStmt => {
                        let stmt = (*copied).utilityStmt.cast::<pg_sys::CopyStmt>();
                        if !stmt.is_null() && (*stmt).is_from {
                            copy_from_oid = relation_oid_from_range_var((*stmt).relation);
                        } else {
                            rewrite_copy_table_to_with_cold_data(stmt);
                        }
                    }
                    pg_sys::NodeTag::T_TruncateStmt => {
                        let stmt = (*copied).utilityStmt.cast::<pg_sys::TruncateStmt>();
                        truncate_oids = truncate_table_oids(stmt);
                    }
                    _ => {}
                }
            }
            if !managed_truncate_allowed()
                && truncate_oids
                    .into_iter()
                    .any(crate::catalog::cache::is_managed_relation)
            {
                pgrx::error!(
                    "TRUNCATE is not supported for KoldStore-managed tables; use DELETE so WAL capture preserves hot/cold consistency"
                );
            }
            let mut mirrors = Vec::new();
            if !drop_oids.is_empty() {
                mirrors = cleanup_managed_tables_before_drop(&drop_oids)
                    .unwrap_or_else(|error| pgrx::error!("KoldStore DROP cleanup failed: {error}"));
            }
            if has_standard_actions {
                delegate(copied, query, read_only, context, params, env, dest, qc);
            } else if !qc.is_null() {
                (*qc).commandTag = pg_sys::CommandTag::CMDTAG_ALTER_TABLE;
                (*qc).nprocessed = 0;
            }
            if !mirrors.is_empty() {
                drop_captured_mirrors(&mirrors);
            }
            if let Some((relation, options)) = captured {
                apply_options(relation, options);
            }
            if !new_partitions.is_empty() && crate::catalog::cache::managed_catalog_ready() {
                // The partition must be visible to the SPI the template lookup and manage run.
                pg_sys::CommandCounterIncrement();
                for relation in new_partitions {
                    if let Some(oid) = relation_oid_from_range_var(relation) {
                        crate::sql::migrate::partition::auto_manage_new_partition(oid);
                    }
                }
            }
            if let Some(table_oid) = copy_from_oid {
                let copied_rows = qc.is_null() || (*qc).nprocessed > 0;
                if copied_rows && crate::catalog::cache::is_managed_relation(table_oid) {
                    crate::worker::wake::mark_managed_dml_pending();
                }
            }
            if let Some(table_oid) = refresh_oid {
                // shared_preload installs this hook before CREATE EXTENSION, and
                // initdb runs ALTER TABLE on system catalogs. Skip until
                // koldstore.schemas exists — SPI would FATAL the bootstrap.
                if crate::catalog::cache::managed_catalog_ready() {
                    // PortalRunUtility increments the command counter only after
                    // ProcessUtility returns. Refresh/SPI must see the just-applied
                    // ALTER (e.g. RENAME COLUMN) in this same transaction.
                    pg_sys::CommandCounterIncrement();
                    // Do not abort the user ALTER on refresh failure. Unsupported
                    // type additions must remain allowed at DDL time; flush refreshes
                    // again and records an error job without pruning hot rows.
                    if let Err(error) =
                        crate::sql::migrate::refresh_active_schema_if_changed(table_oid)
                    {
                        pgrx::warning!(
                            "KoldStore schema refresh after ALTER TABLE deferred: {error}"
                        );
                    }
                }
            }
            if let Some(table_oid) = unique_guard_oid {
                if crate::catalog::cache::managed_catalog_ready()
                    && crate::catalog::cache::is_managed_relation(table_oid)
                {
                    // New or dropped UNIQUE constraints change what the write guard enforces.
                    pg_sys::CommandCounterIncrement();
                    crate::sql::cold_dml::guard::refresh_insert_guard(table_oid);
                }
            }
            if crate::catalog::cache::managed_catalog_ready() {
                let mut stale_parents = std::mem::take(&mut fk_parents_before);
                if let Some(relation) = fk_child_relation {
                    if let Some(child_oid) = relation_oid_from_range_var(relation) {
                        pg_sys::CommandCounterIncrement();
                        stale_parents.extend(crate::sql::cold_dml::guard::referenced_managed_parents(child_oid));
                    }
                }
                if !stale_parents.is_empty() {
                    pg_sys::CommandCounterIncrement();
                    crate::sql::cold_dml::guard::refresh_guards_of(stale_parents);
                }
                if !dropped_index_tables.is_empty() {
                    // A dropped unique index changes what the write guard enforces.
                    pg_sys::CommandCounterIncrement();
                    crate::sql::cold_dml::guard::refresh_guards_of(dropped_index_tables);
                }
            }
            if let Some(schema_name) = renamed_schema {
                if crate::catalog::cache::managed_catalog_ready() {
                    pg_sys::CommandCounterIncrement();
                    crate::sql::migrate::sync_active_mirror_relation_names_in_schema(&schema_name)
                        .unwrap_or_else(|error| {
                            pgrx::error!(
                                "KoldStore schema rename could not rehome managed mirrors: {error}"
                            )
                        });
                }
            }
        }
    }

    // Forwards the fixed ProcessUtility hook arity to the previous hook / standard path.
    #[allow(clippy::too_many_arguments)]
    unsafe fn delegate(
        pstmt: *mut pg_sys::PlannedStmt,
        query: *const core::ffi::c_char,
        read_only: bool,
        context: pg_sys::ProcessUtilityContext::Type,
        params: pg_sys::ParamListInfo,
        env: *mut pg_sys::QueryEnvironment,
        dest: *mut pg_sys::DestReceiver,
        qc: *mut pg_sys::QueryCompletion,
    ) {
        unsafe {
            if let Some(previous) = PREVIOUS {
                previous(pstmt, query, read_only, context, params, env, dest, qc)
            } else {
                pg_sys::standard_ProcessUtility(
                    pstmt, query, read_only, context, params, env, dest, qc,
                )
            }
        }
    }

    unsafe fn strip_options(
        stmt: *mut pg_sys::AlterTableStmt,
    ) -> Option<(*mut pg_sys::RangeVar, HashMap<String, String>)> {
        unsafe {
            let commands = (*stmt).cmds;
            let command_count = if commands.is_null() {
                0
            } else {
                (*commands).length as usize
            };
            let mut kept: *mut pg_sys::List = std::ptr::null_mut();
            let mut found = HashMap::new();
            for index in 0..command_count {
                let cmd = (*(*commands).elements.add(index))
                    .ptr_value
                    .cast::<pg_sys::AlterTableCmd>();
                if (*cmd).subtype != pg_sys::AlterTableType::AT_SetRelOptions
                    && (*cmd).subtype != pg_sys::AlterTableType::AT_ResetRelOptions
                {
                    kept = pg_sys::lappend(kept, cmd.cast());
                    continue;
                }
                let reset = (*cmd).subtype == pg_sys::AlterTableType::AT_ResetRelOptions;
                let defs = (*cmd).def.cast::<pg_sys::List>();
                let def_count = if defs.is_null() {
                    0
                } else {
                    (*defs).length as usize
                };
                let mut standard: *mut pg_sys::List = std::ptr::null_mut();
                for d in 0..def_count {
                    let def = (*(*defs).elements.add(d))
                        .ptr_value
                        .cast::<pg_sys::DefElem>();
                    let name = CStr::from_ptr((*def).defname)
                        .to_string_lossy()
                        .into_owned();
                    if name.starts_with("koldstore_") {
                        if reset {
                            pgrx::error!("KoldStore RESET is not supported; set a replacement policy instead")
                        }
                        let value = CStr::from_ptr(pg_sys::defGetString(def))
                            .to_string_lossy()
                            .into_owned();
                        found.insert(name, value);
                    } else {
                        standard = pg_sys::lappend(standard, def.cast());
                    }
                }
                if !standard.is_null() {
                    (*cmd).def = standard.cast();
                    kept = pg_sys::lappend(kept, cmd.cast());
                }
            }
            (*stmt).cmds = kept;
            (!found.is_empty()).then_some(((*stmt).relation, found))
        }
    }

    unsafe fn apply_options(relation: *mut pg_sys::RangeVar, values: HashMap<String, String>) {
        // Logical-slot creation refuses to run after this backend has written.
        // Take AccessExclusiveLock / SPI only after the async slot exists —
        // locking the relation (and later catalog SPI) can assign an XID.
        let initial_enable = values.get("koldstore_enabled").is_some_and(|value| {
            matches!(value.to_ascii_lowercase().as_str(), "true" | "on" | "1")
        });
        if initial_enable {
            crate::mirror::lifecycle::prepare_capture()
                .unwrap_or_else(|error| pgrx::error!("KoldStore ALTER TABLE failed: {error}"));
        }
        let oid = unsafe {
            pg_sys::RangeVarGetRelidExtended(
                relation,
                pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
                0u32,
                None,
                std::ptr::null_mut(),
            )
        };
        let owns_relation = unsafe { relation_ownercheck(oid, pg_sys::GetUserId()) };
        if !owns_relation {
            pgrx::error!("must be owner of relation to configure KoldStore");
        }
        super::apply_management_options(oid, &values)
            .unwrap_or_else(|error| pgrx::error!("KoldStore ALTER TABLE failed: {error}"));
    }

    /// Errors when `relation` names a managed table (hierarchy changes such
    /// as `INHERIT`, `ATTACH PARTITION` or `INHERITS (...)` would put cold data
    /// under a parent scan or partition router it cannot take part in; upstream
    /// #125).
    unsafe fn reject_if_managed(relation: *mut pg_sys::RangeVar, action: &str) {
        unsafe {
            if !crate::catalog::cache::managed_catalog_ready() {
                return;
            }
            if let Some(oid) = relation_oid_from_range_var(relation) {
                if crate::catalog::cache::is_managed_relation(oid) {
                    let name = crate::catalog::resolve::qualified_relation_name(oid)
                        .unwrap_or_else(|_| format!("(oid {})", oid.to_u32()));
                    pgrx::error!(
                        "koldstore: {action} is not allowed: {name} is a managed table, and managed tables must \
                         stay outside any partition or inheritance hierarchy (upstream issue #125)"
                    );
                }
            }
        }
    }

    /// `CREATE TABLE ... INHERITS (managed)` / `PARTITION OF managed`.
    unsafe fn reject_create_in_managed_hierarchy(stmt: *mut pg_sys::CreateStmt) {
        unsafe {
            if stmt.is_null() {
                return;
            }
            for parent in crate::merge_scan::pg::literals::list_node_pointers((*stmt).inhRelations) {
                reject_if_managed(parent.cast::<pg_sys::RangeVar>(), "CREATE TABLE ... INHERITS / PARTITION OF");
            }
        }
    }

    /// `COPY <table> TO ...` (upstream #126): unlike `COPY (SELECT ...) TO`, the plain table form
    /// never goes through the planner, so it can never enter `KoldMergeScan` -- on its own it would
    /// export the hot heap only, silently omitting cold rows. For a table that actually has cold
    /// data the statement is rewritten to `COPY (SELECT <columns> FROM table) TO ...`, which plans
    /// normally and exports the merged hot+cold rows. This is also what `pg_dump` issues for every
    /// table's data, so a logical dump carries the cold rows too (as ordinary rows: restore them,
    /// then `manage_table` again). A `COPY (query) TO` (`relation` null, `query` set) is unaffected.
    unsafe fn rewrite_copy_table_to_with_cold_data(stmt: *mut pg_sys::CopyStmt) {
        unsafe {
            if stmt.is_null() || !(*stmt).query.is_null() || crate::guc::dump_cold_as_metadata() {
                return;
            }
            let Some(table_oid) = relation_oid_from_range_var((*stmt).relation) else {
                return;
            };
            if !managed_with_cold_data(table_oid) {
                return;
            }
            let name = match crate::catalog::resolve::qualified_relation_name(table_oid) {
                Ok(name) => name,
                Err(error) => pgrx::error!("koldstore: cannot export {table_oid:?} with COPY: {error}"),
            };
            let columns = crate::merge_scan::pg::literals::list_node_pointers((*stmt).attlist)
                .into_iter()
                .map(|node| {
                    let value = node.cast::<pg_sys::String>();
                    koldstore_common::sql::ident::quote_ident(
                        &std::ffi::CStr::from_ptr((*value).sval).to_string_lossy(),
                    )
                })
                .collect::<Vec<_>>();
            let columns = if columns.is_empty() { "*".to_string() } else { columns.join(", ") };
            let sql = std::ffi::CString::new(format!("SELECT {columns} FROM {name}"))
                .unwrap_or_else(|_| pgrx::error!("koldstore: invalid relation name"));
            let parsed = pg_sys::raw_parser(sql.as_ptr(), pg_sys::RawParseMode::RAW_PARSE_DEFAULT);
            let raw = crate::merge_scan::pg::literals::list_node_pointers(parsed);
            let Some(raw) = raw.first().copied().map(|node| node.cast::<pg_sys::RawStmt>()) else {
                pgrx::error!("koldstore: could not build the merged-view query for COPY");
            };
            (*stmt).query = (*raw).stmt;
            (*stmt).relation = std::ptr::null_mut();
            (*stmt).attlist = std::ptr::null_mut();
        }
    }

    /// True when `table_oid` is managed and has published cold segments (upstream #123: object
    /// paths are derived from the current name, so renaming a table or its schema after cold
    /// publication would orphan the segments already written under the old name -- the merge scan
    /// would look for cold data under the new path and silently find nothing there).
    fn managed_with_cold_data(table_oid: pg_sys::Oid) -> bool {
        crate::catalog::cache::is_managed_relation(table_oid)
            && matches!(
                crate::catalog::cache::cached_manifest_planner_hint(table_oid),
                Ok(Some((segments, _))) if segments > 0
            )
    }

    /// `ALTER TABLE ... RENAME TO` and `ALTER SCHEMA ... RENAME TO` on a managed table/schema
    /// (upstream #123). Object-store paths derive from the table and schema names, so the current
    /// prefix is pinned first and the published cold objects stay where they are. `RENAME COLUMN`
    /// is unaffected -- column names do not feed the path template.
    unsafe fn pin_prefix_before_rename(stmt: *mut pg_sys::RenameStmt) {
        unsafe {
            if stmt.is_null() {
                return;
            }
            match (*stmt).renameType {
                pg_sys::ObjectType::OBJECT_TABLE => {
                    if let Some(table_oid) = relation_oid_from_range_var((*stmt).relation) {
                        pin_or_error(table_oid);
                    }
                }
                pg_sys::ObjectType::OBJECT_SCHEMA => {
                    let Some(old_name) = rename_stmt_old_schema_name(stmt) else {
                        return;
                    };
                    for table_oid in
                        crate::hooks::drop_cleanup::active_managed_table_oids_in_schema(&old_name, true)
                    {
                        pin_or_error(table_oid);
                    }
                }
                _ => {}
            }
        }
    }

    /// `ALTER TABLE ... SET SCHEMA` on a managed table: the schema name feeds the same template as a
    /// plain rename, so the current prefix is pinned first.
    unsafe fn pin_prefix_before_set_schema(stmt: *mut pg_sys::AlterObjectSchemaStmt) {
        unsafe {
            if stmt.is_null() || (*stmt).objectType != pg_sys::ObjectType::OBJECT_TABLE {
                return;
            }
            if let Some(table_oid) = relation_oid_from_range_var((*stmt).relation) {
                pin_or_error(table_oid);
            }
        }
    }

    fn pin_or_error(table_oid: pg_sys::Oid) {
        if let Err(error) = crate::catalog::resolve::pin_table_prefix(table_oid) {
            let name = crate::txn_writes::relation_display_name(table_oid);
            pgrx::error!("koldstore: could not pin the object prefix of managed table {name} before the rename: {error}");
        }
    }

    /// The schema being renamed by `ALTER SCHEMA <name> RENAME TO ...` (the *old* name -- the
    /// `subname`/`object` field this statement type uses for its single string target).
    unsafe fn rename_stmt_old_schema_name(stmt: *mut pg_sys::RenameStmt) -> Option<String> {
        unsafe {
            if stmt.is_null() || (*stmt).renameType != pg_sys::ObjectType::OBJECT_SCHEMA {
                return None;
            }
            let subname = (*stmt).subname;
            (!subname.is_null()).then(|| CStr::from_ptr(subname).to_string_lossy().into_owned())
        }
    }

    /// `ALTER TABLE ... INHERIT parent` and `ATTACH PARTITION` where the
    /// relation taking the **parent** role is managed (ADR-008 option A: a
    /// managed table cannot itself be an inheritance/partition parent, since
    /// it has no storage of its own to aggregate over). The relation taking
    /// the **child**/leaf role is unaffected by either side's management
    /// status -- a managed leaf's own hot/cold storage does not change by
    /// gaining a parent, and an unmanaged leaf can still be `manage_table`'d
    /// afterward, same as any standalone table.
    unsafe fn reject_alter_hierarchy_of_managed(stmt: *mut pg_sys::AlterTableStmt) {
        unsafe {
            if stmt.is_null() || (*stmt).cmds.is_null() {
                return;
            }
            for cmd in crate::merge_scan::pg::literals::list_node_pointers((*stmt).cmds) {
                let cmd = cmd.cast::<pg_sys::AlterTableCmd>();
                if cmd.is_null() {
                    continue;
                }
                match (*cmd).subtype {
                    pg_sys::AlterTableType::AT_AddInherit => {
                        // `stmt.relation` is the child becoming an inheritance
                        // child (leaf) -- allowed. `cmd.def` is the parent --
                        // still refused when managed.
                        reject_if_managed((*cmd).def.cast::<pg_sys::RangeVar>(), "ALTER TABLE ... INHERIT");
                    }
                    pg_sys::AlterTableType::AT_AttachPartition => {
                        // `stmt.relation` is the partitioned parent -- still
                        // refused when managed. The partition named in
                        // `cmd.def` is the child/leaf becoming its partition
                        // -- allowed.
                        reject_if_managed((*stmt).relation, "ALTER TABLE ... ATTACH PARTITION");
                    }
                    _ => {}
                }
            }
        }
    }

    /// The partitions named by `ALTER TABLE ... ATTACH PARTITION` commands of `stmt`.
    unsafe fn collect_attached_partitions(
        stmt: *mut pg_sys::AlterTableStmt,
        out: &mut Vec<*mut pg_sys::RangeVar>,
    ) {
        unsafe {
            if stmt.is_null() || (*stmt).cmds.is_null() {
                return;
            }
            for cmd in crate::merge_scan::pg::literals::list_node_pointers((*stmt).cmds) {
                let cmd = cmd.cast::<pg_sys::AlterTableCmd>();
                if cmd.is_null() || (*cmd).subtype != pg_sys::AlterTableType::AT_AttachPartition {
                    continue;
                }
                let partition = (*cmd).def.cast::<pg_sys::PartitionCmd>();
                if !partition.is_null() && !(*partition).name.is_null() {
                    out.push((*partition).name);
                }
            }
        }
    }

    /// Resolves a relation OID from a `RangeVar`, tolerating missing relations.
    unsafe fn relation_oid_from_range_var(relation: *mut pg_sys::RangeVar) -> Option<pg_sys::Oid> {
        unsafe {
            if relation.is_null() {
                return None;
            }
            #[allow(clippy::unnecessary_cast)]
            let flags: u32 = pg_sys::RVROption::RVR_MISSING_OK as u32;
            let oid = pg_sys::RangeVarGetRelidExtended(
                relation,
                pg_sys::NoLock as pg_sys::LOCKMODE,
                flags,
                None,
                std::ptr::null_mut(),
            );
            (oid != pg_sys::InvalidOid).then_some(oid)
        }
    }

    /// Resolves every explicitly targeted relation before TRUNCATE executes.
    unsafe fn truncate_table_oids(stmt: *mut pg_sys::TruncateStmt) -> Vec<pg_sys::Oid> {
        unsafe {
            if stmt.is_null() || (*stmt).relations.is_null() {
                return Vec::new();
            }
            let relations = (*stmt).relations;
            let mut oids = Vec::with_capacity((*relations).length as usize);
            for index in 0..(*relations).length as usize {
                let relation = (*(*relations).elements.add(index))
                    .ptr_value
                    .cast::<pg_sys::RangeVar>();
                if let Some(oid) = relation_oid_from_range_var(relation) {
                    oids.push(oid);
                }
            }
            oids
        }
    }

    /// Resolves the table OID for column/table renames that need schema sync.
    unsafe fn rename_stmt_relation_oid(stmt: *mut pg_sys::RenameStmt) -> Option<pg_sys::Oid> {
        unsafe {
            if stmt.is_null() {
                return None;
            }
            match (*stmt).renameType {
                pg_sys::ObjectType::OBJECT_COLUMN | pg_sys::ObjectType::OBJECT_TABLE => {
                    relation_oid_from_range_var((*stmt).relation)
                }
                _ => None,
            }
        }
    }

    /// Resolves the table OID for `ALTER TABLE … SET SCHEMA`.
    unsafe fn alter_object_schema_relation_oid(
        stmt: *mut pg_sys::AlterObjectSchemaStmt,
    ) -> Option<pg_sys::Oid> {
        unsafe {
            if stmt.is_null() || (*stmt).objectType != pg_sys::ObjectType::OBJECT_TABLE {
                return None;
            }
            relation_oid_from_range_var((*stmt).relation)
        }
    }

    /// Returns the new schema name for a schema rename statement.
    unsafe fn rename_stmt_schema_name(stmt: *mut pg_sys::RenameStmt) -> Option<String> {
        unsafe {
            if stmt.is_null() || (*stmt).renameType != pg_sys::ObjectType::OBJECT_SCHEMA {
                return None;
            }
            let new_name = (*stmt).newname;
            (!new_name.is_null()).then(|| CStr::from_ptr(new_name).to_string_lossy().into_owned())
        }
    }

    /// True when `role` owns relation `oid` (PG15 vs PG16+ ACL helper names differ).
    unsafe fn relation_ownercheck(oid: pg_sys::Oid, role: pg_sys::Oid) -> bool {
        #[cfg(feature = "pg15")]
        unsafe {
            pg_sys::pg_class_ownercheck(oid, role)
        }
        #[cfg(not(feature = "pg15"))]
        unsafe {
            pg_sys::object_ownercheck(pg_sys::RelationRelationId, oid, role)
        }
    }
}

#[cfg(feature = "pg")]
pub(crate) fn register_process_utility_hook() {
    process_utility::register();
}

/// Allows demigrate/unmanage SPI to TRUNCATE a still-managed heap safely.
#[cfg(feature = "pg")]
pub(crate) use process_utility::AllowManagedTruncateGuard;

#[cfg(feature = "pg")]
fn option_value<'a>(
    values: &'a std::collections::HashMap<String, String>,
    name: &str,
) -> Option<&'a str> {
    values.get(name).map(String::as_str)
}

#[cfg(feature = "pg")]
fn validate_management_option_conflicts(
    values: &std::collections::HashMap<String, String>,
) -> Result<(), String> {
    if option_value(values, "koldstore_move_when").is_some() {
        return Err("filter policy is not supported yet".into());
    }
    if option_value(values, "koldstore_hot_row_limit").is_some()
        && option_value(values, "koldstore_move_after").is_some()
    {
        return Err("hot_row_limit and move_after cannot be set together".into());
    }
    if option_value(values, "koldstore_enabled")
        .is_some_and(|v| !matches!(v.to_ascii_lowercase().as_str(), "true" | "on" | "1"))
    {
        return Err(
            "koldstore_enabled=false is not supported; use koldstore.unmanage_table(...)".into(),
        );
    }
    Ok(())
}

#[cfg(feature = "pg")]
fn ensure_initial_management(
    table_oid: pgrx::pg_sys::Oid,
    values: &std::collections::HashMap<String, String>,
) -> Result<(), String> {
    let enabled = option_value(values, "koldstore_enabled").map(|value| value.to_ascii_lowercase());
    if !matches!(enabled.as_deref(), Some("true" | "on" | "1")) {
        return Err("initial management requires koldstore_enabled = true".into());
    }
    let storage = option_value(values, "koldstore_storage")
        .ok_or("initial management requires koldstore_storage")?;
    let hot = option_value(values, "koldstore_hot_row_limit")
        .map(str::parse::<i64>)
        .transpose()
        .map_err(|_| "hot_row_limit must be a positive integer")?;
    if hot.is_none() && option_value(values, "koldstore_move_after").is_none() {
        return Err("initial management requires hot_row_limit or move_after".into());
    }
    let min = option_value(values, "koldstore_min_flush_rows")
        .unwrap_or("1000")
        .parse()
        .map_err(|_| "min_flush_rows must be a positive integer")?;
    let file = option_value(values, "koldstore_max_rows_per_file")
        .unwrap_or("1000")
        .parse()
        .map_err(|_| "max_rows_per_file must be a positive integer")?;
    // Same default as `koldstore.manage_table`'s own `allow_fk_hot_only` argument: refused unless
    // explicitly accepted, since flushing can silently move a foreign-keyed row out of PostgreSQL's
    // own FK triggers' reach (upstream #122's FK gap).
    let allow_fk_hot_only = option_value(values, "koldstore_allow_fk_hot_only")
        .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "true" | "on" | "1"));
    crate::sql::migrate::manage_table_pg_impl(
        table_oid,
        "shared",
        storage,
        None,
        None,
        None,
        None,
        hot.or(Some(1)),
        min,
        file,
        true,
        None,
        None,
        None,
        None,
        None,
        None,
        allow_fk_hot_only,
    );
    Ok(())
}

#[cfg(feature = "pg")]
fn resolve_flush_batching(
    values: &std::collections::HashMap<String, String>,
    old: Option<&koldstore_common::FlushPolicy>,
) -> Result<(u64, u64, u64), String> {
    let min = option_value(values, "koldstore_min_flush_rows")
        .map(str::parse)
        .transpose()
        .map_err(|_| "min_flush_rows must be positive")?
        .unwrap_or_else(|| {
            old.map(koldstore_common::FlushPolicy::min_flush_rows)
                .unwrap_or(1_000)
        });
    let file = option_value(values, "koldstore_max_rows_per_file")
        .map(str::parse)
        .transpose()
        .map_err(|_| "max_rows_per_file must be positive")?
        .unwrap_or_else(|| {
            old.map(koldstore_common::FlushPolicy::max_rows_per_file)
                .unwrap_or(koldstore_common::DEFAULT_MIN_MAX_ROWS_PER_FILE)
        });
    let max = option_value(values, "koldstore_max_rows_per_flush")
        .map(str::parse)
        .transpose()
        .map_err(|_| "max_rows_per_flush must be positive")?
        .unwrap_or_else(|| {
            old.map(koldstore_common::FlushPolicy::max_rows_per_flush)
                .unwrap_or(koldstore_common::DEFAULT_MAX_ROWS_PER_FLUSH)
        });
    if min == 0 || file == 0 || max == 0 {
        return Err("flush batching settings must be greater than zero".into());
    }
    koldstore_common::validate_max_rows_per_file(
        file,
        u64::try_from(crate::guc::min_max_rows_per_file())
            .unwrap_or(koldstore_common::DEFAULT_MIN_MAX_ROWS_PER_FILE),
        None,
    )?;
    Ok((min, file, max))
}

#[cfg(feature = "pg")]
fn apply_flush_policy_updates(
    options: &mut koldstore_common::ManageTableOptions,
    values: &std::collections::HashMap<String, String>,
    min: u64,
    file: u64,
    max: u64,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    let old = options.flush_policy();
    let changes_batching = option_value(values, "koldstore_min_flush_rows").is_some()
        || option_value(values, "koldstore_max_rows_per_file").is_some()
        || option_value(values, "koldstore_max_rows_per_flush").is_some();
    if changes_batching
        && option_value(values, "koldstore_hot_row_limit").is_none()
        && option_value(values, "koldstore_move_after").is_none()
    {
        options.flush_policy = old.map(|policy| policy.with_batching(min, file, max));
    }
    if let Some(value) = option_value(values, "koldstore_hot_row_limit") {
        let hot_row_limit = value
            .parse()
            .map_err(|_| "hot_row_limit must be positive")?;
        if hot_row_limit == 0 {
            return Err("hot_row_limit must be greater than zero".into());
        }
        options.flush_policy = Some(koldstore_common::FlushPolicy::RowLimit {
            hot_row_limit,
            min_flush_rows: min,
            max_rows_per_file: file,
            max_rows_per_flush: max,
        });
    }
    if let Some(value) = option_value(values, "koldstore_move_after") {
        let interval = pgrx::Spi::get_one_with_args::<pgrx::datetime::Interval>(
            "SELECT $1::text::interval",
            &[DatumWithOid::from(value)],
        )
        .map_err(|e| e.to_string())?
        .ok_or("invalid move_after interval")?;
        let age = koldstore_common::MoveAfter::new(
            interval.months(),
            interval.days(),
            interval.micros(),
        )?;
        options.flush_policy = Some(koldstore_common::FlushPolicy::OlderThan {
            age,
            min_flush_rows: min,
            max_rows_per_file: file,
            max_rows_per_flush: max,
        });
    }
    Ok(())
}

#[cfg(feature = "pg")]
fn apply_parquet_layout_updates(
    options: &mut koldstore_common::ManageTableOptions,
    values: &std::collections::HashMap<String, String>,
) -> Result<(), String> {
    if let Some(value) = option_value(values, "koldstore_parquet_row_group_size") {
        let row_count = value
            .parse::<u64>()
            .map_err(|_| "parquet_row_group_size must be a positive integer")?;
        if row_count == 0 {
            return Err("parquet_row_group_size must be greater than zero".into());
        }
        *options = options.clone().with_parquet_row_group_size(row_count);
    }
    if let Some(value) = option_value(values, "koldstore_parquet_data_page_row_count_limit") {
        let row_count = value
            .parse::<u64>()
            .map_err(|_| "parquet_data_page_row_count_limit must be a positive integer")?;
        if row_count == 0 {
            return Err("parquet_data_page_row_count_limit must be greater than zero".into());
        }
        *options = options
            .clone()
            .with_parquet_data_page_row_count_limit(row_count);
    }
    if let Some(value) = option_value(values, "koldstore_parquet_bloom_filter_fpp") {
        let fpp = value
            .parse::<f64>()
            .map_err(|_| "parquet_bloom_filter_fpp must be greater than 0 and less than 1")?;
        let fpp = koldstore_common::ParquetBloomFilterFpp::new(fpp)?;
        *options = options.clone().with_parquet_bloom_filter_fpp(fpp);
    }
    Ok(())
}

#[cfg(feature = "pg")]
fn apply_management_options(
    table_oid: pgrx::pg_sys::Oid,
    values: &std::collections::HashMap<String, String>,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    validate_management_option_conflicts(values)?;
    // Initial ALTER TABLE … SET (koldstore_enabled=true, …) looks up the
    // management catalog via SPI before calling manage_table. SPI assigns an
    // XID, and logical-slot creation then deadlocks with this backend.
    // `apply_options` provisions the slot before AccessExclusiveLock; keep this
    // call as a safety net for other entry points (idempotent when ready).
    let initial_enable = option_value(values, "koldstore_enabled")
        .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "true" | "on" | "1"));
    if initial_enable {
        crate::mirror::lifecycle::prepare_capture()?;
    }
    let catalog_lookup = koldstore_catalog::queries::plan_management_options_lookup()
        .map_err(|error| error.to_string())?;
    let row = pgrx::Spi::get_one_with_args::<pgrx::JsonB>(
        &catalog_lookup.sql,
        &[DatumWithOid::from(table_oid)],
    )
    .map_err(|e| e.to_string())?;
    if row.is_none() {
        ensure_initial_management(table_oid, values)?;
    }
    let current = pgrx::Spi::get_one_with_args::<pgrx::JsonB>(
        &catalog_lookup.sql,
        &[DatumWithOid::from(table_oid)],
    )
    .map_err(|e| e.to_string())?
    .ok_or("table management catalog row is missing")?;
    if let Some(requested) = option_value(values, "koldstore_storage") {
        if current.0["storage"].as_str() != Some(requested) {
            return Err("storage cannot be changed after a table is managed".into());
        }
    }
    let mut options = koldstore_common::ManageTableOptions::from_value(&current.0["options"]);
    let (min, file, max) = resolve_flush_batching(values, options.flush_policy().as_ref())?;
    apply_flush_policy_updates(&mut options, values, min, file, max)?;
    apply_parquet_layout_updates(&mut options, values)?;
    let json = pgrx::JsonB(options.to_value());
    let update = koldstore_migrate::register::plan_update_schema_options()
        .map_err(|error| error.to_string())?;
    pgrx::Spi::run_with_args(
        &update.sql,
        &[DatumWithOid::from(table_oid), DatumWithOid::from(json)],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}
