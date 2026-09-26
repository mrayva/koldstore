-- Dropping a managed table (or its schema) must remove the per-table helper
-- objects that live outside it: the insert-guard trigger function in the
-- koldstore schema used to be left behind, one per dropped table.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE FUNCTION sqlreg.guard_functions(pattern text) RETURNS bigint LANGUAGE sql AS $$
  SELECT count(*) FROM pg_proc
  WHERE pronamespace = 'koldstore'::regnamespace AND proname LIKE pattern
$$;

CREATE TABLE sqlreg.d1 (id bigint PRIMARY KEY, v text);
CREATE TABLE sqlreg.d2 (id bigint PRIMARY KEY, v text);
SELECT koldstore.manage_table(table_name => 'sqlreg.d1'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, auto_flush => false) IS NOT NULL AS d1_managed;
SELECT koldstore.manage_table(table_name => 'sqlreg.d2'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, auto_flush => false) IS NOT NULL AS d2_managed;
SELECT sqlreg.guard_functions('sqlreg\_d1\_%') AS d1_guard_before,
       sqlreg.guard_functions('sqlreg\_d2\_%') AS d2_guard_before;

-- a rolled-back DROP leaves everything in place
BEGIN;
DROP TABLE sqlreg.d1;
ROLLBACK;
SELECT sqlreg.guard_functions('sqlreg\_d1\_%') AS d1_guard_after_rollback;
SELECT count(*) AS d1_triggers_after_rollback FROM pg_trigger WHERE tgrelid = 'sqlreg.d1'::regclass AND NOT tgisinternal;

DROP TABLE sqlreg.d1;
SELECT sqlreg.guard_functions('sqlreg\_d1\_%') AS d1_guard_after_drop;
SELECT sqlreg.guard_functions('sqlreg\_d2\_%') AS d2_guard_untouched;

-- dropping the whole schema cleans up every table in it
CREATE SCHEMA sqlreg_dropme;
CREATE TABLE sqlreg_dropme.a (id bigint PRIMARY KEY);
CREATE TABLE sqlreg_dropme.b (id bigint PRIMARY KEY);
SELECT koldstore.manage_table(table_name => 'sqlreg_dropme.a'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, auto_flush => false) IS NOT NULL AS a_managed;
SELECT koldstore.manage_table(table_name => 'sqlreg_dropme.b'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, auto_flush => false) IS NOT NULL AS b_managed;
SELECT sqlreg.guard_functions('sqlreg\_dropme\_%') AS schema_guards_before;
DROP SCHEMA sqlreg_dropme CASCADE;
SELECT sqlreg.guard_functions('sqlreg\_dropme\_%') AS schema_guards_after;

-- DROP SCHEMA without IF EXISTS used to fail for every schema ("schema "" does
-- not exist"), managed or not, and with IF EXISTS skipped the cleanup above.
CREATE SCHEMA sqlreg_empty;
DROP SCHEMA sqlreg_empty;
SELECT to_regnamespace('sqlreg_empty') IS NULL AS empty_schema_dropped;
DROP SCHEMA IF EXISTS sqlreg_never_existed;
DROP SCHEMA sqlreg_never_existed;

CREATE SCHEMA sqlreg_dropme2;
CREATE TABLE sqlreg_dropme2.a (id bigint PRIMARY KEY);
SELECT koldstore.manage_table(table_name => 'sqlreg_dropme2.a'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, auto_flush => false) IS NOT NULL AS a2_managed;
SELECT count(*) AS mirrors_before FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'koldstore' AND c.relname LIKE 'sqlreg\_dropme2%';
DROP SCHEMA IF EXISTS sqlreg_dropme2 CASCADE;
SELECT sqlreg.guard_functions('sqlreg\_dropme2\_%') AS if_exists_guards_after;
SELECT count(*) AS mirrors_after FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'koldstore' AND c.relname LIKE 'sqlreg\_dropme2%';
SELECT count(*) AS active_schema_rows_after FROM koldstore.schemas s WHERE s.active AND NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = s.table_oid);
