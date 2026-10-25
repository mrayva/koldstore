-- Compaction of a scoped table that still has a table-wide group (ADR-010). The table-wide group is merged too,
-- in a "keep markers" mode, and scope groups keep their winning delete markers while the table-wide group is
-- non-empty (it could hold an older version of the key). Content must match an unmanaged control throughout.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE FUNCTION sqlreg.same_content(managed regclass, control regclass, key text) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE a text; b text;
BEGIN
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', key, managed) INTO a;
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', key, control) INTO b;
  RETURN a = b;
END
$$;

CREATE FUNCTION sqlreg.flush_after_catchup(rel regclass) RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM koldstore.wait_for_async_mirror();
  RETURN sqlreg.flush_table(rel) IS NOT NULL;
END
$$;

-- a first flush across more scopes than the limit is written table-wide
SET koldstore.max_scopes_per_flush = 1;
CREATE TABLE sqlreg.cm (id bigint PRIMARY KEY, user_id text NOT NULL, n int NOT NULL, v text);
CREATE TABLE sqlreg.cm_ctl (id bigint PRIMARY KEY, user_id text NOT NULL, n int NOT NULL, v text);
INSERT INTO sqlreg.cm SELECT g, 'user-' || (g % 3), g, 'v' || g FROM generate_series(1, 15) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.cm'::regclass, storage => 'sqlreg_fs', table_type => 'user', scope_column => 'user_id',
  hot_row_limit => 2, min_flush_rows => 1, max_rows_per_file => 100, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS cm_managed;
-- six small flushes leave six small table-wide segments
SELECT sqlreg.flush_after_catchup('sqlreg.cm'::regclass) AS cm_flush_1;
INSERT INTO sqlreg.cm SELECT g, 'user-' || (g % 3), g, 'v' || g FROM generate_series(16, 30) g;
SELECT sqlreg.flush_after_catchup('sqlreg.cm'::regclass) AS cm_flush_2;
INSERT INTO sqlreg.cm SELECT g, 'user-' || (g % 3), g, 'v' || g FROM generate_series(31, 45) g;
SELECT sqlreg.flush_after_catchup('sqlreg.cm'::regclass) AS cm_flush_3;
INSERT INTO sqlreg.cm SELECT g, 'user-' || (g % 3), g, 'v' || g FROM generate_series(46, 60) g;
SELECT sqlreg.flush_after_catchup('sqlreg.cm'::regclass) AS cm_flush_4;
INSERT INTO sqlreg.cm SELECT g, 'user-' || (g % 3), g, 'v' || g FROM generate_series(61, 75) g;
SELECT sqlreg.flush_after_catchup('sqlreg.cm'::regclass) AS cm_flush_5;
INSERT INTO sqlreg.cm SELECT g, 'user-' || (g % 3), g, 'v' || g FROM generate_series(76, 90) g;
SELECT sqlreg.flush_after_catchup('sqlreg.cm'::regclass) AS cm_flush_6;
INSERT INTO sqlreg.cm_ctl SELECT g, 'user-' || (g % 3), g, 'v' || g FROM generate_series(1, 90) g;

-- later flushes are per scope again
SET koldstore.max_scopes_per_flush = 100;
INSERT INTO sqlreg.cm SELECT g, 'user-' || (g % 3), g, 'v' || g FROM generate_series(91, 150) g;
INSERT INTO sqlreg.cm_ctl SELECT g, 'user-' || (g % 3), g, 'v' || g FROM generate_series(91, 150) g;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cm_fenced;
SELECT sqlreg.flush_table('sqlreg.cm'::regclass) IS NOT NULL AS cm_flushed_scoped;

-- deletes of rows living in the table-wide group and in scope groups leave markers in their scope groups
SET koldstore.hydrate_on_write = on;
DELETE FROM sqlreg.cm WHERE id % 7 = 0;
DELETE FROM sqlreg.cm_ctl WHERE id % 7 = 0;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cm_fenced_2;
SELECT sqlreg.flush_table('sqlreg.cm'::regclass) IS NOT NULL AS cm_flushed_deletes;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cm_fenced_3;

SELECT count(*) FILTER (WHERE scope_id IS NULL) > 1 AS cm_table_wide_fragmented,
       count(*) FILTER (WHERE scope_id IS NOT NULL) > 0 AS cm_has_scoped
FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.cm'::regclass::oid AND status = 'active';
SELECT sqlreg.same_content('sqlreg.cm', 'sqlreg.cm_ctl', 'id') AS cm_content_equal;
SELECT count(*) AS cm_rows FROM sqlreg.cm;

-- the table-wide group is merged as well, and the content is unchanged
SELECT count(*) FILTER (WHERE scope_id IS NULL) AS cm_table_wide_before
FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.cm'::regclass::oid AND status = 'active';
SELECT (koldstore.compact_table('sqlreg.cm'::regclass, grace_seconds => 3600) ->> 'compacted')::boolean AS cm_compacted;
SELECT count(*) FILTER (WHERE scope_id IS NULL) AS cm_table_wide_after
FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.cm'::regclass::oid AND status = 'active';
SELECT sqlreg.same_content('sqlreg.cm', 'sqlreg.cm_ctl', 'id') AS cm_content_equal_after_compaction;
SELECT count(*) AS cm_rows_after FROM sqlreg.cm;
SELECT count(*) AS cm_deleted_still_gone FROM sqlreg.cm WHERE id % 7 = 0;

-- a repeat finds nothing to do
SELECT (koldstore.compact_table('sqlreg.cm'::regclass, grace_seconds => 3600) ->> 'compacted')::boolean AS cm_noop_second;
SELECT koldstore.reap_compacted_segments('sqlreg.cm'::regclass, 0) > 0 AS cm_reaped;
SELECT sqlreg.same_content('sqlreg.cm', 'sqlreg.cm_ctl', 'id') AS cm_content_equal_after_reap;
SELECT koldstore.recover_segments('sqlreg.cm'::regclass, true) AS cm_orphans;
SELECT koldstore.validate_cold_storage('sqlreg.cm'::regclass) ->> 'ok' AS cm_valid;

RESET koldstore.max_scopes_per_flush;
