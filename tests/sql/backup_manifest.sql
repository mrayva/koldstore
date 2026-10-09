-- Backup support: koldstore.backup_manifest and koldstore.validate_cold_storage.
--
-- backup_manifest records what the catalog references in cold storage (never credentials);
-- validate_cold_storage checks those references against the object store, so a restored or
-- PITR-recovered cluster can prove its cold tier is intact before cutover. The faults here are
-- injected into the catalog inside rolled-back transactions; the real-object damage cases (truncated,
-- corrupted, deleted objects) and the end-to-end restore live in scripts/backup-restore-drill.sh.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE TABLE sqlreg.bk (id bigint PRIMARY KEY, v text NOT NULL);
INSERT INTO sqlreg.bk SELECT g, 'v' || g FROM generate_series(1, 5) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.bk'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 1, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS bk_managed;
SELECT sqlreg.flush_table('sqlreg.bk'::regclass) IS NOT NULL AS flushed;

-- an unflushed unmanaged table is rejected
CREATE TABLE sqlreg.plain_t (id int);
SELECT koldstore.backup_manifest('sqlreg.plain_t'::regclass);
SELECT koldstore.validate_cold_storage('sqlreg.plain_t'::regclass);

-- ------------------------------------------------------------ manifest shape
SELECT m ->> 'format' AS format,
       jsonb_array_length(m -> 'tables') AS tables,
       m -> 'tables' -> 0 ->> 'table' AS table_name,
       m -> 'tables' -> 0 -> 'storage' ->> 'type' AS storage_type,
       m -> 'tables' -> 0 -> 'storage' ->> 'prefix' AS prefix,
       m -> 'cluster' ->> 'database' = current_database() AS database_recorded,
       (m -> 'cluster' ->> 'system_identifier') ~ '^[0-9]+$' AS system_identifier_recorded,
       (m -> 'cluster' ->> 'wal_lsn') ~ '^[0-9A-F]+/[0-9A-F]+$' AS wal_lsn_recorded,
       m ? 'async_mirror' AS has_async_mirror
FROM (SELECT koldstore.backup_manifest('sqlreg.bk'::regclass) AS m) s;

-- every active segment is listed with a key, size and SHA-256, and matches the catalog
WITH mf AS (
  SELECT jsonb_array_elements(koldstore.backup_manifest('sqlreg.bk'::regclass) -> 'tables' -> 0 -> 'segments') AS s
)
SELECT count(*) = (SELECT count(*) FROM koldstore.cold_segments
                   WHERE table_oid = 'sqlreg.bk'::regclass AND status = 'active') AS lists_every_active_segment,
       bool_and((s ->> 'key') = 'sqlreg/bk/' || (s ->> 'path')) AS keys_are_prefix_plus_path,
       bool_and((s ->> 'checksum') ~ '^[0-9a-f]{64}$') AS checksums_are_sha256,
       bool_and((s ->> 'byte_size')::bigint > 0) AS sizes_positive
FROM mf;

-- storage credentials never appear in a manifest
SELECT koldstore.backup_manifest()::text ~* 'credentials|secret' AS leaks_credentials;

-- the all-tables form includes this table
SELECT jsonb_path_exists(koldstore.backup_manifest(), '$.tables[*] ? (@.table == "sqlreg.bk")') AS all_tables_includes_bk;

-- ------------------------------------------------------- validation: healthy
SELECT (v ->> 'ok')::boolean AS ok,
       (v ->> 'segments_checked')::int = (SELECT count(*) FROM koldstore.cold_segments
                                           WHERE table_oid = 'sqlreg.bk'::regclass AND status = 'active') AS checked_all,
       jsonb_array_length(v -> 'problems') AS problems
FROM (SELECT koldstore.validate_cold_storage('sqlreg.bk'::regclass) AS v) s;
SELECT (v ->> 'ok')::boolean AS deep_ok, (v ->> 'deep')::boolean AS deep
FROM (SELECT koldstore.validate_cold_storage('sqlreg.bk'::regclass, true) AS v) s;

-- ------------------------------------------- validation: injected catalog faults
-- wrong size: caught by the cheap check
BEGIN;
UPDATE koldstore.cold_segments SET byte_size = byte_size + 1
WHERE segment_id = (SELECT min(segment_id::text)::uuid FROM koldstore.cold_segments
                    WHERE table_oid = 'sqlreg.bk'::regclass AND status = 'active');
SELECT p ->> 'problem' AS problem
FROM jsonb_array_elements(koldstore.validate_cold_storage('sqlreg.bk'::regclass) -> 'problems') p;
ROLLBACK;

-- referenced object absent
BEGIN;
UPDATE koldstore.cold_segments SET path = 'no/such/object.parquet'
WHERE segment_id = (SELECT min(segment_id::text)::uuid FROM koldstore.cold_segments
                    WHERE table_oid = 'sqlreg.bk'::regclass AND status = 'active');
SELECT p ->> 'problem' AS problem
FROM jsonb_array_elements(koldstore.validate_cold_storage('sqlreg.bk'::regclass) -> 'problems') p;
ROLLBACK;

-- wrong checksum: invisible to the shallow check, caught by deep
BEGIN;
UPDATE koldstore.cold_segments SET checksum = repeat('0', 64)
WHERE segment_id = (SELECT min(segment_id::text)::uuid FROM koldstore.cold_segments
                    WHERE table_oid = 'sqlreg.bk'::regclass AND status = 'active');
SELECT (koldstore.validate_cold_storage('sqlreg.bk'::regclass) ->> 'ok')::boolean AS shallow_ok;
SELECT p ->> 'problem' AS problem
FROM jsonb_array_elements(koldstore.validate_cold_storage('sqlreg.bk'::regclass, true) -> 'problems') p;
ROLLBACK;

-- the injected faults left nothing behind
SELECT (koldstore.validate_cold_storage('sqlreg.bk'::regclass, true) ->> 'ok')::boolean AS ok_after_rollback;

-- ------------------------------------------------------------- privileges
CREATE ROLE sqlreg_bk_other NOLOGIN;
GRANT USAGE ON SCHEMA sqlreg TO sqlreg_bk_other;
SET ROLE sqlreg_bk_other;
SELECT koldstore.backup_manifest('sqlreg.bk'::regclass);
SELECT koldstore.validate_cold_storage('sqlreg.bk'::regclass);
SELECT koldstore.backup_manifest();
SELECT koldstore.validate_cold_storage();
RESET ROLE;
REVOKE USAGE ON SCHEMA sqlreg FROM sqlreg_bk_other;
DROP ROLE sqlreg_bk_other;
