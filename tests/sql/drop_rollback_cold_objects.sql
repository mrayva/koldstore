-- #100: cold-object-store deletion must be deferred to transaction commit.
--
-- Before this fix, DROP TABLE / unmanage_table's drop_cold deleted cold
-- Parquet objects from storage immediately, inside the still-open DDL
-- transaction. A rolled-back DROP restored the catalog rows (ordinary
-- transactional writes) but left the objects gone -- silent, irreversible
-- data loss on what looked like a safely aborted statement. Deletion is now
-- staged and only physically performed once the transaction is known to have
-- committed (an XACT_EVENT_COMMIT callback); on abort the staged list is
-- discarded and the objects are left exactly as the rolled-back catalog rows
-- say they should be.

\set VERBOSITY terse
\set ON_ERROR_STOP off

-- Object-store deletes remove files, not the (possibly nested, generation-
-- keyed) directories that held them, so a plain one-level pg_ls_dir can see
-- leftover empty subdirectories after a real deletion. Count actual files.
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

-- ---------------------------------------------------------------- fixture
CREATE TABLE sqlreg.cold100 (id bigint PRIMARY KEY, v text NOT NULL);
INSERT INTO sqlreg.cold100 SELECT g, 'v' || g FROM generate_series(1, 5) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.cold100'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 1, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS cold100_managed;
SELECT sqlreg.flush_table('sqlreg.cold100'::regclass) IS NOT NULL AS flushed;
SELECT (koldstore.table_status('sqlreg.cold100'::regclass) ->> 'cold_row_count')::int > 0 AS has_cold_rows_before;

-- cold objects are really on disk before any drop is attempted
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/cold100') > 0 AS objects_exist_before_drop;

-- a rolled-back DROP TABLE must not touch the staged objects
BEGIN;
DROP TABLE sqlreg.cold100;
ROLLBACK;
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/cold100') > 0 AS objects_still_exist_after_rollback;
-- and the table, cold data included, is fully usable again -- not just the
-- catalog row: prove the rows are actually still readable through KoldMergeScan.
SELECT count(*) FROM sqlreg.cold100;
SELECT (koldstore.table_status('sqlreg.cold100'::regclass) ->> 'cold_row_count')::int > 0 AS still_has_cold_rows_after_rollback;

-- a DROP that actually commits does eventually remove the staged objects
DROP TABLE sqlreg.cold100;
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/cold100') AS objects_after_committed_drop;

-- same story for unmanage_table's drop_cold, which stages through the same path
CREATE TABLE sqlreg.cold100b (id bigint PRIMARY KEY, v text NOT NULL);
INSERT INTO sqlreg.cold100b SELECT g, 'v' || g FROM generate_series(1, 5) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.cold100b'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 1, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS cold100b_managed;
SELECT sqlreg.flush_table('sqlreg.cold100b'::regclass) IS NOT NULL AS flushed_b;
SELECT (koldstore.table_status('sqlreg.cold100b'::regclass) ->> 'cold_row_count')::int > 0 AS has_cold_rows_before_b;

BEGIN;
SELECT koldstore.unmanage_table(table_name => 'sqlreg.cold100b'::regclass, rehydrate => true, drop_cold => true);
ROLLBACK;
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/cold100b') > 0 AS objects_still_exist_after_unmanage_rollback;
-- rehydrate's TRUNCATE also rolled back with it, so the table is still managed
SELECT (koldstore.table_status('sqlreg.cold100b'::regclass) ->> 'cold_row_count')::int > 0 AS still_managed_after_unmanage_rollback;

SELECT koldstore.unmanage_table(table_name => 'sqlreg.cold100b'::regclass, rehydrate => true, drop_cold => true);
SELECT sqlreg.count_files_recursive(:'STORAGE_ROOT' || '/sqlreg/cold100b') AS objects_after_committed_unmanage;
