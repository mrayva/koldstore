-- Cold segments were pruned only by columns whose values never change across versions of a row (key, scope,
-- order column): pruning by a mutable column could hide the newer version of a row and resurrect an older one.
-- When no key is stored in more than one active segment (their key ranges are disjoint), every key has exactly one
-- cold version, so any column that has segment statistics -- one with a regular index, or listed in
-- `pruning_columns` -- can prune. The moment a re-flushed update puts a key into two segments the table falls
-- back to the exact behaviour.
--
-- Pruning is read from EXPLAIN ANALYZE ("Parquet Segments Opened"); results are always compared with an unmanaged copy,
-- including the case this rule exists for: a row whose indexed column changed.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE FUNCTION sqlreg.same_rows(managed regclass, control regclass, key text, preds text[]) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE p text; a text; b text; bad int := 0;
BEGIN
  FOREACH p IN ARRAY preds LOOP
    EXECUTE format('SELECT coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''') FROM %s t WHERE %s', key, managed, p) INTO a;
    EXECUTE format('SELECT coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''') FROM %s t WHERE %s', key, control, p) INTO b;
    IF a IS DISTINCT FROM b THEN
      bad := bad + 1;
      RAISE NOTICE 'mismatch: %', p;
    END IF;
  END LOOP;
  RETURN bad;
END
$$;

-- number of cold segments a query actually opened
CREATE FUNCTION sqlreg.candidate_segments(q text) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE line text; found int;
BEGIN
  FOR line IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) ' || q LOOP
    IF line ~ 'Parquet Segments Opened: [0-9]+' THEN
      found := substring(line from 'Parquet Segments Opened: ([0-9]+)')::int;
    END IF;
  END LOOP;
  RETURN found;
END
$$;

CREATE TABLE sqlreg.nk_ev (
  id bigint PRIMARY KEY, ts timestamptz NOT NULL, tenant int NOT NULL, amount int NOT NULL, note text
);
INSERT INTO sqlreg.nk_ev
SELECT g, timestamptz '2026-01-01 00:00:00+00' + g * interval '1 hour', g % 13, g * 10, 'n' || g
FROM generate_series(1, 400) g;
CREATE INDEX nk_ev_ts_idx ON sqlreg.nk_ev (ts);
CREATE TABLE sqlreg.nk_ev_ctl AS SELECT * FROM sqlreg.nk_ev;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.nk_ev'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS ev_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ev_caught_up;
SELECT sqlreg.flush_table('sqlreg.nk_ev'::regclass) IS NOT NULL AS ev_flushed;
SELECT (SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.nk_ev'::regclass::oid AND status = 'active') >= 15 AS ev_many_segments;

-- ------------------------------------------------ disjoint key ranges: the indexed column prunes
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.nk_ev WHERE ts >= timestamptz '2026-01-10 00:00:00+00' AND ts < timestamptz '2026-01-10 12:00:00+00'$$) <= 2 AS ts_range_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.nk_ev WHERE ts = timestamptz '2026-01-04 05:00:00+00'$$) <= 1 AS ts_equality_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.nk_ev WHERE ts > timestamptz '2026-01-16 05:00:00+00'$$) <= 5 AS ts_open_range_prunes;
-- a column without statistics reads every segment
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.nk_ev WHERE tenant = 7$$) >= 15 AS tenant_reads_all;
SELECT sqlreg.same_rows('sqlreg.nk_ev', 'sqlreg.nk_ev_ctl', 'id', ARRAY[
  $$ts >= timestamptz '2026-01-10 00:00:00+00' AND ts < timestamptz '2026-01-10 12:00:00+00'$$,
  $$ts = timestamptz '2026-01-04 05:00:00+00'$$,
  $$ts > timestamptz '2026-01-16 05:00:00+00'$$, $$ts < timestamptz '2026-01-01 00:00:00+00'$$,
  $$tenant = 7$$, $$tenant = 7 AND ts > timestamptz '2026-01-12 00:00:00+00'$$
]) AS ev_mismatches_before;

-- ------------------------------------------------ the case the rule exists for: an indexed column changes
SET koldstore.hydrate_on_write = on;
-- row 77 moves from 2026-01-04 06:00 to 2026-02-20 06:00; row 5 leaves the range, row 300 enters it
UPDATE sqlreg.nk_ev SET ts = timestamptz '2026-02-20 06:00:00+00' WHERE id = 77;
UPDATE sqlreg.nk_ev_ctl SET ts = timestamptz '2026-02-20 06:00:00+00' WHERE id = 77;
UPDATE sqlreg.nk_ev SET ts = timestamptz '2026-01-10 03:30:00+00' WHERE id = 300;
UPDATE sqlreg.nk_ev_ctl SET ts = timestamptz '2026-01-10 03:30:00+00' WHERE id = 300;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ev_fenced;
-- before the re-flush the old versions are cold, the new ones hot
SELECT sqlreg.same_rows('sqlreg.nk_ev', 'sqlreg.nk_ev_ctl', 'id', ARRAY[
  $$ts >= timestamptz '2026-01-04 05:00:00+00' AND ts <= timestamptz '2026-01-04 07:00:00+00'$$,
  $$ts >= timestamptz '2026-01-10 00:00:00+00' AND ts < timestamptz '2026-01-10 12:00:00+00'$$,
  $$ts >= timestamptz '2026-02-20 00:00:00+00'$$, $$id IN (77, 300)$$
]) AS ev_mismatches_hot_update;
-- re-flush: the old and new versions of keys 77 and 300 are now in two segments
SELECT sqlreg.flush_table('sqlreg.nk_ev'::regclass) IS NOT NULL AS ev_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ev_fenced_2;
SELECT sqlreg.same_rows('sqlreg.nk_ev', 'sqlreg.nk_ev_ctl', 'id', ARRAY[
  $$ts >= timestamptz '2026-01-04 05:00:00+00' AND ts <= timestamptz '2026-01-04 07:00:00+00'$$,
  $$ts >= timestamptz '2026-01-10 00:00:00+00' AND ts < timestamptz '2026-01-10 12:00:00+00'$$,
  $$ts >= timestamptz '2026-02-20 00:00:00+00'$$, $$id IN (77, 300)$$,
  $$ts > timestamptz '2026-01-16 05:00:00+00'$$
]) AS ev_mismatches_after_reflush;
-- overlapping key ranges: pruning by the mutable column is off again, the exact path is used
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.nk_ev WHERE ts >= timestamptz '2026-01-10 00:00:00+00' AND ts < timestamptz '2026-01-10 12:00:00+00'$$) >= 15 AS overlapping_reads_all;
SET koldstore.max_merge_seen_keys = 0;
SELECT id, ts FROM sqlreg.nk_ev WHERE id IN (77, 300) ORDER BY id;
SELECT count(*) AS exact_total, count(DISTINCT id) AS exact_distinct FROM sqlreg.nk_ev;
RESET koldstore.max_merge_seen_keys;
