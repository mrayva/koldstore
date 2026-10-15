-- koldstore.sync_sql_objects(): repairs a database whose koldstore SQL objects differ from the loaded
-- library. The extension version does not change when a build changes a function signature or adds a
-- catalog column, so an older database keeps the old objects; this recreates them from definitions
-- generated out of a fresh database.
--
-- Each kind of drift is induced inside a transaction, repaired, checked, and rolled back, so the
-- shared regression database is never left drifted.

\set VERBOSITY terse
\set ON_ERROR_STOP off

-- ------------------------------------------------------------------ a clean database needs nothing
SELECT (r ->> 'ok_before')::boolean AS clean_before, jsonb_array_length(r -> 'steps') AS steps,
       jsonb_array_length(r -> 'skipped') AS skipped, (r ->> 'dry_run')::boolean AS dry_run
FROM (SELECT koldstore.sync_sql_objects() AS r) s;

-- ------------------------------------------------------------------ the 17- vs 18-argument case
BEGIN;
ALTER EXTENSION koldstore DROP FUNCTION koldstore.manage_table(regclass, text, bigint, bigint, bigint, text, text, text, text, bigint, boolean, text, text[], text[], bigint, bigint, double precision, boolean);
DROP FUNCTION koldstore.manage_table(regclass, text, bigint, bigint, bigint, text, text, text, text, bigint, boolean, text, text[], text[], bigint, bigint, double precision, boolean);
CREATE FUNCTION koldstore.manage_table(table_name regclass, storage text, hot_row_limit bigint, min_flush_rows bigint DEFAULT 1000)
RETURNS uuid SECURITY DEFINER LANGUAGE c AS '$libdir/koldstore', 'manage_table_pg_wrapper';
ALTER EXTENSION koldstore ADD FUNCTION koldstore.manage_table(regclass, text, bigint, bigint);
SELECT (koldstore.validate_sql_objects() ->> 'ok')::boolean AS drifted_is_detected;
-- the plan (dry run) replaces the old overload before creating the new one, and changes nothing
SELECT (r ->> 'dry_run')::boolean AS dry_run,
       (SELECT array_agg(x ->> 'action' ORDER BY ord) FROM jsonb_array_elements(r -> 'steps') WITH ORDINALITY a(x, ord)) AS actions
FROM (SELECT koldstore.sync_sql_objects() AS r) s;
SELECT count(*) AS manage_table_overloads_after_dry_run FROM pg_proc WHERE proname = 'manage_table' AND pronamespace = 'koldstore'::regnamespace;
SELECT (r ->> 'ok_after')::boolean AS repaired, (r ->> 'applied')::int AS applied
FROM (SELECT koldstore.sync_sql_objects(dry_run => false) AS r) s;
SELECT count(*) AS manage_table_overloads, max(pronargs) AS arguments
FROM pg_proc WHERE proname = 'manage_table' AND pronamespace = 'koldstore'::regnamespace;
SELECT (koldstore.validate_sql_objects() ->> 'ok')::boolean AS ok_after;
ROLLBACK;

-- ------------------------------------------------------------------ a missing column
BEGIN;
ALTER TABLE koldstore.cold_segment_index DROP COLUMN value_summary;
SELECT (r -> 'steps' -> 0 ->> 'action') AS action, r -> 'steps' -> 0 -> 'statements' ->> 0 AS statement
FROM (SELECT koldstore.sync_sql_objects() AS r) s;
SELECT (r ->> 'ok_after')::boolean AS repaired FROM (SELECT koldstore.sync_sql_objects(dry_run => false) AS r) s;
SELECT count(*) AS value_summary_columns FROM pg_attribute
WHERE attrelid = 'koldstore.cold_segment_index'::regclass AND attname = 'value_summary' AND NOT attisdropped;
ROLLBACK;

-- ------------------------------------------------------------------ a missing table with a foreign key
BEGIN;
ALTER EXTENSION koldstore DROP TABLE koldstore.cold_segment_index;
DROP TABLE koldstore.cold_segment_index;
SELECT (SELECT array_agg(x ->> 'action' ORDER BY ord) FROM jsonb_array_elements(r -> 'steps') WITH ORDINALITY a(x, ord)) AS actions
FROM (SELECT koldstore.sync_sql_objects() AS r) s;
SELECT (r ->> 'ok_after')::boolean AS repaired FROM (SELECT koldstore.sync_sql_objects(dry_run => false) AS r) s;
SELECT (SELECT count(*) FROM pg_constraint WHERE conrelid = 'koldstore.cold_segment_index'::regclass AND contype = 'p') AS primary_keys,
       (SELECT count(*) FROM pg_constraint WHERE conrelid = 'koldstore.cold_segment_index'::regclass AND contype = 'f') AS foreign_keys,
       (SELECT count(*) FROM pg_constraint WHERE conrelid = 'koldstore.cold_segment_index'::regclass AND contype = 'c') AS checks,
       (SELECT count(*) FROM pg_indexes WHERE schemaname = 'koldstore' AND tablename = 'cold_segment_index') AS indexes,
       (SELECT count(*) FROM pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = 'koldstore.cold_segment_index'::regclass
          AND d.refclassid = 'pg_extension'::regclass AND d.deptype = 'e') AS extension_member;
ROLLBACK;

-- ------------------------------------------------------------------ an unexpected function is kept unless asked
BEGIN;
CREATE FUNCTION koldstore.zz_leftover() RETURNS int LANGUAGE sql AS 'SELECT 1';
ALTER EXTENSION koldstore ADD FUNCTION koldstore.zz_leftover();
SELECT jsonb_array_length(r -> 'steps') AS steps, r -> 'skipped' -> 0 ->> 'reason' AS reason
FROM (SELECT koldstore.sync_sql_objects() AS r) s;
SELECT (r ->> 'applied')::int AS applied, (r ->> 'ok_after')::boolean AS still_ok
FROM (SELECT koldstore.sync_sql_objects(dry_run => false) AS r) s;
SELECT count(*) AS leftover_kept FROM pg_proc WHERE proname = 'zz_leftover';
SELECT (r ->> 'applied')::int AS applied, (r ->> 'ok_after')::boolean AS repaired
FROM (SELECT koldstore.sync_sql_objects(dry_run => false, drop_unexpected => true) AS r) s;
SELECT count(*) AS leftover_after_drop FROM pg_proc WHERE proname = 'zz_leftover';
ROLLBACK;

-- ------------------------------------------------------------------ a function that is not an extension member is never touched
BEGIN;
CREATE FUNCTION koldstore.zz_user_function() RETURNS int LANGUAGE sql AS 'SELECT 2';
SELECT (r ->> 'ok_before')::boolean AS not_listed_so_clean FROM (SELECT koldstore.sync_sql_objects() AS r) s;
ROLLBACK;

-- ------------------------------------------------------------------ privileges
CREATE ROLE sqlreg_sync_other NOLOGIN;
SET ROLE sqlreg_sync_other;
SELECT koldstore.sync_sql_objects();
RESET ROLE;
DROP ROLE sqlreg_sync_other;

-- nothing above may have left the database drifted
SELECT (koldstore.validate_sql_objects() ->> 'ok')::boolean AS clean_at_the_end;
