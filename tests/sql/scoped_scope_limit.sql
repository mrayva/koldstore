-- koldstore.max_scopes_per_flush (ADR-010): a flush pass that touches more distinct scopes than the limit is
-- written as ordinary table-wide segments (scope_id NULL, original order) instead of one segment per scope,
-- so a flush across very many scopes does not scatter into very many tiny objects. Readers always include
-- table-wide segments, so content is unchanged and later flushes below the limit go back to scoped segments.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE FUNCTION sqlreg.explain_counter(query text, label text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
  line text;
  value bigint;
BEGIN
  FOR line IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) ' || query LOOP
    IF line ~ ('^\s*' || label || ': [0-9]+') THEN
      value := substring(line FROM label || ': ([0-9]+)')::bigint;
      EXIT;
    END IF;
  END LOOP;
  RETURN value;
END
$$;

CREATE FUNCTION sqlreg.same_content(managed regclass, control regclass, key text) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE a text; b text;
BEGIN
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', key, managed) INTO a;
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', key, control) INTO b;
  RETURN a = b;
END
$$;

-- flushes run inline in this session, so a session setting applies
SET koldstore.max_scopes_per_flush = 3;

-- 6 scopes x 20 rows, interleaved
CREATE TABLE sqlreg.sc (id bigint PRIMARY KEY, user_id text NOT NULL, n int NOT NULL, v text);
INSERT INTO sqlreg.sc SELECT g, 'user-' || (g % 6), g, 'v' || g FROM generate_series(1, 120) g;
CREATE TABLE sqlreg.sc_ctl AS SELECT * FROM sqlreg.sc;
SHOW koldstore.max_scopes_per_flush;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.sc'::regclass, storage => 'sqlreg_fs', table_type => 'user', scope_column => 'user_id',
  hot_row_limit => 2, min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS sc_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS sc_caught_up;
SELECT sqlreg.flush_table('sqlreg.sc'::regclass) IS NOT NULL AS sc_flushed;

-- 6 scopes > 3: table-wide segments only
SELECT count(*) FILTER (WHERE scope_id IS NULL) > 0 AS sc_table_wide, count(*) FILTER (WHERE scope_id IS NOT NULL) AS sc_scoped
FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.sc'::regclass::oid AND status = 'active';
SELECT sqlreg.same_content('sqlreg.sc', 'sqlreg.sc_ctl', 'id') AS sc_content_equal;
SELECT count(*) AS sc_user_3_rows FROM sqlreg.sc WHERE user_id = 'user-3';
SELECT count(*) AS sc_ctl_user_3_rows FROM sqlreg.sc_ctl WHERE user_id = 'user-3';

-- raise the limit: a pass touching few scopes is written per scope again
SET koldstore.max_scopes_per_flush = 100;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.sc SET v = 'edited' WHERE user_id IN ('user-1', 'user-4') AND n % 2 = 0;
UPDATE sqlreg.sc_ctl SET v = 'edited' WHERE user_id IN ('user-1', 'user-4') AND n % 2 = 0;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS sc_fenced;
SELECT sqlreg.flush_table('sqlreg.sc'::regclass) IS NOT NULL AS sc_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS sc_fenced_2;
SELECT scope_id, count(*) AS segments FROM koldstore.cold_segments
WHERE table_oid = 'sqlreg.sc'::regclass::oid AND status = 'active' AND scope_id IS NOT NULL GROUP BY scope_id ORDER BY scope_id;
SELECT sqlreg.same_content('sqlreg.sc', 'sqlreg.sc_ctl', 'id') AS sc_content_equal_after_edit;

-- a scan restricted to a scope reads its scoped segments plus the table-wide ones, and answers correctly
SELECT count(*) AS sc_user_1_rows FROM sqlreg.sc WHERE user_id = 'user-1';
SELECT count(*) AS sc_ctl_user_1_rows FROM sqlreg.sc_ctl WHERE user_id = 'user-1';
SELECT v AS sc_edited_value FROM sqlreg.sc WHERE user_id = 'user-4' AND id = 4;

-- compaction handles the mix: scoped groups compact, the table-wide group is merged keeping its markers
SELECT (koldstore.compact_table('sqlreg.sc'::regclass, grace_seconds => 3600) ->> 'compacted')::boolean AS sc_compacted;
SELECT sqlreg.same_content('sqlreg.sc', 'sqlreg.sc_ctl', 'id') AS sc_content_equal_after_compaction;
SELECT koldstore.recover_segments('sqlreg.sc'::regclass, true) AS sc_orphans;
SELECT koldstore.validate_cold_storage('sqlreg.sc'::regclass) ->> 'ok' AS sc_valid;

RESET koldstore.max_scopes_per_flush;
