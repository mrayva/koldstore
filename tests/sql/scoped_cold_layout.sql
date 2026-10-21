-- Scoped cold storage, slice 1 (ADR-010): a user-scoped table flushes one segment group per scope. Each
-- segment holds a single scope's rows, lives in that scope's folder (`{scopeId}/{folder}/segment-….parquet`,
-- percent-encoded) and records it in cold_segments.scope_id; delete markers carry no row values, so they
-- stay table-wide (scope_id NULL). Reads still consider every segment, so content must match an unmanaged
-- control, and compaction runs per scope without ever mixing scopes.

\set VERBOSITY terse
\set ON_ERROR_STOP off

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sqlreg_scoped2') THEN
    DROP OWNED BY sqlreg_scoped2;
    DROP ROLE sqlreg_scoped2;
  END IF;
END $$;
CREATE ROLE sqlreg_scoped2 NOINHERIT LOGIN;
GRANT USAGE ON SCHEMA sqlreg TO sqlreg_scoped2;

CREATE FUNCTION sqlreg.same_content(managed regclass, control regclass, key text) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE a text; b text;
BEGIN
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', key, managed) INTO a;
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', key, control) INTO b;
  RETURN a = b;
END
$$;

-- four scopes, interleaved in load order; one name needs path encoding
CREATE TABLE sqlreg.sl (id bigint PRIMARY KEY, user_id text NOT NULL, n int NOT NULL, v text);
INSERT INTO sqlreg.sl
SELECT g, (ARRAY['user-a', 'user-b', 'user-c', 'ann/lee'])[1 + g % 4], g, 'v' || g FROM generate_series(1, 240) g;
CREATE TABLE sqlreg.sl_ctl AS SELECT * FROM sqlreg.sl;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.sl'::regclass, storage => 'sqlreg_fs', table_type => 'user', scope_column => 'user_id',
  hot_row_limit => 2, min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS sl_managed;
ALTER TABLE sqlreg.sl FORCE ROW LEVEL SECURITY;
GRANT SELECT, INSERT, UPDATE, DELETE ON sqlreg.sl TO sqlreg_scoped2;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS sl_caught_up;
SELECT sqlreg.flush_table('sqlreg.sl'::regclass) IS NOT NULL AS sl_flushed;

-- one group of segments per scope, none table-wide yet
SELECT coalesce(scope_id, '(table-wide)') AS scope, count(*) AS segments, sum(row_count) AS rows
FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.sl'::regclass::oid AND status = 'active'
GROUP BY scope_id ORDER BY scope_id;
-- every scoped segment lives in its scope's (encoded) folder
SELECT count(*) AS sl_misplaced FROM koldstore.cold_segments
WHERE table_oid = 'sqlreg.sl'::regclass::oid AND status = 'active'
  AND split_part(path, '/', 1) <> replace(replace(scope_id, '/', '%2F'), ' ', '%20');
SELECT sqlreg.same_content('sqlreg.sl', 'sqlreg.sl_ctl', 'id') AS sl_content_equal;

SET ROLE sqlreg_scoped2;
SET koldstore.user_id = 'user-b';
SELECT count(*) AS sl_user_b_rows FROM sqlreg.sl;
SET koldstore.user_id = 'ann/lee';
SELECT count(*) AS sl_ann_rows FROM sqlreg.sl;
RESET koldstore.user_id;
RESET ROLE;

-- updates stay in their scope; deletes leave a table-wide marker segment
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.sl SET v = 'edited' WHERE n % 5 = 0;
UPDATE sqlreg.sl_ctl SET v = 'edited' WHERE n % 5 = 0;
DELETE FROM sqlreg.sl WHERE id BETWEEN 100 AND 111;
DELETE FROM sqlreg.sl_ctl WHERE id BETWEEN 100 AND 111;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS sl_fenced;
-- a flush writes the delete markers in a pass of their own; the second one takes the updated rows
SELECT sqlreg.flush_table('sqlreg.sl'::regclass) IS NOT NULL AS sl_reflushed;
SELECT sqlreg.flush_table('sqlreg.sl'::regclass) IS NOT NULL AS sl_reflushed_2;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS sl_fenced_2;
SELECT count(*) FILTER (WHERE scope_id IS NULL) > 0 AS sl_marker_segment_is_table_wide,
       count(DISTINCT scope_id) AS sl_scopes
FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.sl'::regclass::oid AND status = 'active';
SELECT count(*) AS sl_misplaced_2 FROM koldstore.cold_segments
WHERE table_oid = 'sqlreg.sl'::regclass::oid AND status = 'active' AND scope_id IS NOT NULL
  AND split_part(path, '/', 1) <> replace(replace(scope_id, '/', '%2F'), ' ', '%20');
SELECT sqlreg.same_content('sqlreg.sl', 'sqlreg.sl_ctl', 'id') AS sl_content_equal_after_dml;

-- compaction works per scope and never mixes them
CREATE TEMP TABLE sl_before AS
SELECT scope_id, count(*) AS segs FROM koldstore.cold_segments
WHERE table_oid = 'sqlreg.sl'::regclass::oid AND status = 'active' GROUP BY scope_id;
SELECT (koldstore.compact_table('sqlreg.sl'::regclass, grace_seconds => 3600) ->> 'compacted')::boolean AS sl_compacted;
SELECT sqlreg.same_content('sqlreg.sl', 'sqlreg.sl_ctl', 'id') AS sl_content_equal_after_compaction;
SELECT count(*) AS sl_misplaced_3 FROM koldstore.cold_segments
WHERE table_oid = 'sqlreg.sl'::regclass::oid AND status = 'active' AND scope_id IS NOT NULL
  AND split_part(path, '/', 1) <> replace(replace(scope_id, '/', '%2F'), ' ', '%20');
-- the table-wide group (the delete markers) is left alone while scoped segments exist
SELECT (SELECT segs FROM sl_before WHERE scope_id IS NULL) =
       (SELECT count(*) FROM koldstore.cold_segments
         WHERE table_oid = 'sqlreg.sl'::regclass::oid AND status = 'active' AND scope_id IS NULL) AS sl_marker_group_untouched;
SELECT count(*) FILTER (WHERE scope_id IS NOT NULL) <= (SELECT sum(segs) FROM sl_before WHERE scope_id IS NOT NULL) AS sl_scoped_segments_not_more
FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.sl'::regclass::oid AND status = 'active';
SELECT (koldstore.compact_table('sqlreg.sl'::regclass, grace_seconds => 3600) ->> 'compacted')::boolean AS sl_noop_second;

SELECT koldstore.reap_compacted_segments('sqlreg.sl'::regclass, 0) > 0 AS sl_reaped;
SELECT koldstore.recover_segments('sqlreg.sl'::regclass, true) AS sl_orphans;
SELECT koldstore.validate_cold_storage('sqlreg.sl'::regclass) ->> 'ok' AS sl_valid;
SELECT sqlreg.same_content('sqlreg.sl', 'sqlreg.sl_ctl', 'id') AS sl_content_final;
