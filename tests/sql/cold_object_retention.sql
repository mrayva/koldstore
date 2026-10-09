-- Cold-object retention: DROP TABLE / unmanage_table(drop_cold) must not delete a table's cold
-- objects while koldstore.cold_object_retention_seconds > 0, so a backup taken before the DROP stays
-- restorable. The keys are recorded in koldstore.deferred_cold_deletes inside the dropping
-- transaction and removed by koldstore.purge_deferred_cold_objects() after the window.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE FUNCTION sqlreg.count_files_recursive(root text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
  total bigint := 0;
  entry text;
  full_path text;
BEGIN
  FOR entry IN SELECT * FROM pg_ls_dir(root, true, false) LOOP
    full_path := root || '/' || entry;
    IF (pg_stat_file(full_path, true)).isdir THEN
      total := total + sqlreg.count_files_recursive(full_path);
    ELSE
      total := total + 1;
    END IF;
  END LOOP;
  RETURN total;
END
$$;

-- the setting is superuser-only, so a table owner cannot switch the guard off before a DROP
CREATE ROLE sqlreg_ret_other NOLOGIN;
SET ROLE sqlreg_ret_other;
SET koldstore.cold_object_retention_seconds = 0;
SELECT koldstore.purge_deferred_cold_objects();
RESET ROLE;

-- ------------------------------------------------ retention off: delete at commit (default)
CREATE TABLE sqlreg.ret0 (id bigint PRIMARY KEY, v text NOT NULL);
INSERT INTO sqlreg.ret0 SELECT g, 'v' || g FROM generate_series(1, 5) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.ret0'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 1, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS ret0_managed;
SELECT sqlreg.flush_table('sqlreg.ret0'::regclass) IS NOT NULL AS ret0_flushed;
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/ret0') > 0 AS objects_before;
DROP TABLE sqlreg.ret0;
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/ret0') AS files_after_plain_drop;
SELECT count(*) AS queued_when_retention_off FROM koldstore.deferred_cold_deletes;

-- ------------------------------------------------ retention on: DROP keeps the objects
SET koldstore.cold_object_retention_seconds = 3600;
CREATE TABLE sqlreg.ret1 (id bigint PRIMARY KEY, v text NOT NULL);
INSERT INTO sqlreg.ret1 SELECT g, 'v' || g FROM generate_series(1, 5) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.ret1'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 1, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS ret1_managed;
SELECT sqlreg.flush_table('sqlreg.ret1'::regclass) IS NOT NULL AS ret1_flushed;
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/ret1') AS files_before_drop \gset
DROP TABLE sqlreg.ret1;
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/ret1') = :files_before_drop AS objects_kept_after_drop;
SELECT count(*) = :files_before_drop AS every_object_queued,
       count(DISTINCT storage_id) AS storages,
       bool_and(object_key LIKE 'sqlreg/ret1/%') AS keys_under_table_prefix
FROM koldstore.deferred_cold_deletes;

-- not due yet: the window has not passed
SELECT (koldstore.purge_deferred_cold_objects() ->> 'considered')::int AS considered_inside_window,
       (koldstore.purge_deferred_cold_objects() ->> 'deleted')::int AS deleted_inside_window;

-- dry run reports what would go and changes nothing
SELECT (r ->> 'dry_run')::boolean AS dry_run, (r ->> 'deleted')::int = :files_before_drop AS would_delete_all
FROM (SELECT koldstore.purge_deferred_cold_objects(older_than_seconds => 0, dry_run => true) AS r) s;
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/ret1') = :files_before_drop AS still_there_after_dry_run;

-- the manifest records that deletions are pending
SELECT (koldstore.backup_manifest() -> 'retention' ->> 'cold_object_retention_seconds')::int AS manifest_retention,
       (koldstore.backup_manifest() -> 'retention' ->> 'deferred_objects')::int = :files_before_drop AS manifest_counts_deferred;

-- once the window has passed the objects are removed and the queue empties
SELECT (r ->> 'deleted')::int = :files_before_drop AS deleted_all, (r ->> 'failed')::int AS failed,
       (r ->> 'remaining')::int AS remaining
FROM (SELECT koldstore.purge_deferred_cold_objects(older_than_seconds => 0) AS r) s;
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/ret1') AS files_after_purge;

-- ------------------------------------------------ a rolled-back DROP records nothing
CREATE TABLE sqlreg.ret2 (id bigint PRIMARY KEY, v text NOT NULL);
INSERT INTO sqlreg.ret2 SELECT g, 'v' || g FROM generate_series(1, 5) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.ret2'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 1, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS ret2_managed;
SELECT sqlreg.flush_table('sqlreg.ret2'::regclass) IS NOT NULL AS ret2_flushed;
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/ret2') AS files_ret2 \gset
BEGIN;
DROP TABLE sqlreg.ret2;
ROLLBACK;
SELECT count(*) AS queued_after_rollback FROM koldstore.deferred_cold_deletes;
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/ret2') = :files_ret2 AS objects_untouched;

-- ------------------------------------------------ unmanage_table(drop_cold) is deferred too
CREATE TABLE sqlreg.ret4 (id bigint PRIMARY KEY, v text NOT NULL);
INSERT INTO sqlreg.ret4 SELECT g, 'v' || g FROM generate_series(1, 5) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.ret4'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 1, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS ret4_managed;
SELECT sqlreg.flush_table('sqlreg.ret4'::regclass) IS NOT NULL AS ret4_flushed;
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/ret4') AS files_ret4 \gset
SELECT koldstore.unmanage_table('sqlreg.ret4'::regclass, drop_cold => true) IS NOT NULL AS unmanaged;
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/ret4') = :files_ret4 AS objects_kept_after_unmanage;
SELECT count(*) = :files_ret4 AS ret4_queued FROM koldstore.deferred_cold_deletes WHERE object_key LIKE 'sqlreg/ret4/%';
SELECT (koldstore.purge_deferred_cold_objects(older_than_seconds => 0) ->> 'deleted')::int = :files_ret4 AS ret4_purged;
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/ret4') AS files_ret4_after_purge;

-- ------------------------------------------------ DROP then recreate under the same name
-- The new table owns the same prefix (including its manifest), so the purge must not delete under it.
CREATE TABLE sqlreg.ret3 (id bigint PRIMARY KEY, v text NOT NULL);
INSERT INTO sqlreg.ret3 SELECT g, 'v' || g FROM generate_series(1, 5) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.ret3'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 1, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS ret3_managed;
SELECT sqlreg.flush_table('sqlreg.ret3'::regclass) IS NOT NULL AS ret3_flushed;
DROP TABLE sqlreg.ret3;
CREATE TABLE sqlreg.ret3 (id bigint PRIMARY KEY, v text NOT NULL);
INSERT INTO sqlreg.ret3 SELECT g, 'v' || g FROM generate_series(1, 5) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.ret3'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 1, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS ret3_managed;
SELECT sqlreg.flush_table('sqlreg.ret3'::regclass) IS NOT NULL AS ret3_flushed;
SELECT (r ->> 'skipped_live_prefix')::int > 0 AS skipped_live_prefix, (r ->> 'deleted')::int AS deleted,
       (r ->> 'remaining')::int AS remaining
FROM (SELECT koldstore.purge_deferred_cold_objects(older_than_seconds => 0) AS r) s;
SELECT (v ->> 'ok')::boolean AS recreated_table_cold_tier_intact
FROM (SELECT koldstore.validate_cold_storage('sqlreg.ret3'::regclass, true) AS v) s;
SELECT count(*) AS recreated_table_rows FROM sqlreg.ret3;

-- ------------------------------------------------ privileges
SET ROLE sqlreg_ret_other;
SELECT koldstore.purge_deferred_cold_objects();
RESET ROLE;
DROP ROLE sqlreg_ret_other;
RESET koldstore.cold_object_retention_seconds;
