-- `WHERE key IN (...)`, `key = ANY(array)` and `key = a OR key = b` on a primary key used to read every cold
-- segment (only a single value was pruned). Each listed value now selects its own candidate segments and
-- the union is read; a list over 64 values falls back to its [min, max] range.
--
-- (A table whose segment key ranges are disjoint scans without retaining keys, so the cap would never trip on
-- it; each table below first re-flushes one updated key, which puts that key into two segments and makes the
-- exact, key-retaining path -- and therefore the cap -- apply.)
-- Pruning is proved with the identity cap: koldstore.max_merge_seen_keys is set well below the number of
-- cold rows, so a scan of every segment fails; only a pruned read can succeed. Results are compared with
-- an unmanaged copy of the table.

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

-- ------------------------------------------------ bigint key
CREATE TABLE sqlreg.vl_big (id bigint PRIMARY KEY, n int NOT NULL, v text);
INSERT INTO sqlreg.vl_big SELECT g, g, 'v' || g FROM generate_series(1, 400) g;
CREATE TABLE sqlreg.vl_big_ctl AS SELECT * FROM sqlreg.vl_big;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.vl_big'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS big_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS big_caught_up;
SELECT sqlreg.flush_table('sqlreg.vl_big'::regclass) IS NOT NULL AS big_flushed;
SELECT (SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.vl_big'::regclass::oid AND status = 'active') >= 15 AS big_many_segments;

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.vl_big SET v = 'overlap' WHERE id = 3;
UPDATE sqlreg.vl_big_ctl SET v = 'overlap' WHERE id = 3;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS big_fenced_0;
SELECT sqlreg.flush_table('sqlreg.vl_big'::regclass) IS NOT NULL AS big_reflushed_0;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS big_fenced_00;
SET koldstore.max_merge_seen_keys = 70;
-- a full cold scan trips the cap
SELECT count(*) FROM sqlreg.vl_big;
-- value lists across a few segments
SELECT id FROM sqlreg.vl_big WHERE id IN (7, 150) ORDER BY id;
SELECT id FROM sqlreg.vl_big WHERE id = ANY (ARRAY[7, 150, 300]::bigint[]) ORDER BY id;
SELECT id FROM sqlreg.vl_big WHERE id = 7 OR id = 150 OR id = 301 ORDER BY id;
SELECT id FROM sqlreg.vl_big WHERE id IN (7, 7, 150) ORDER BY id;
-- integer literals of another width, a key that does not exist, and NULL in the list
SELECT id FROM sqlreg.vl_big WHERE id IN (7::int, 150::smallint) ORDER BY id;
SELECT id FROM sqlreg.vl_big WHERE id IN (7, 100000) ORDER BY id;
SELECT id FROM sqlreg.vl_big WHERE id IN (7, NULL, 150) ORDER BY id;
SELECT id FROM sqlreg.vl_big WHERE id IN (-5, 100000) ORDER BY id;
-- a list combined with another key predicate
SELECT id FROM sqlreg.vl_big WHERE id IN (7, 150, 300) AND id > 100 ORDER BY id;
-- prepared with a bound array
PREPARE vl_any(bigint[]) AS SELECT id FROM sqlreg.vl_big WHERE id = ANY ($1) ORDER BY id;
EXECUTE vl_any(ARRAY[8, 151]);
EXECUTE vl_any(ARRAY[8, 151, 299]);
DEALLOCATE vl_any;
RESET koldstore.max_merge_seen_keys;

SELECT sqlreg.same_rows('sqlreg.vl_big', 'sqlreg.vl_big_ctl', 'id', ARRAY[
  $$id IN (7, 150)$$, $$id = ANY (ARRAY[7, 150, 300]::bigint[])$$, $$id = 7 OR id = 150 OR id = 301$$,
  $$id IN (-5, 100000)$$, $$id IN (7, NULL, 150)$$, $$id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)$$,
  $$id IN (7, 150, 300) AND id > 100$$, $$id NOT IN (7, 150)$$, $$id <> ALL (ARRAY[7, 150])$$,
  $$id IN (7, 150) OR n > 395$$
]) AS big_list_mismatches;

-- a list longer than the per-value limit still returns exactly the right rows
SELECT sqlreg.same_rows('sqlreg.vl_big', 'sqlreg.vl_big_ctl', 'id', ARRAY[
  'id IN (' || (SELECT string_agg((g * 5)::text, ',') FROM generate_series(1, 80) g) || ')'
]) AS long_list_mismatches;

-- ------------------------------------------------ list queries still see hot rows and cold deletes
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.vl_big SET v = 'changed' WHERE id IN (7, 150);
UPDATE sqlreg.vl_big_ctl SET v = 'changed' WHERE id IN (7, 150);
DELETE FROM sqlreg.vl_big WHERE id = 300;
DELETE FROM sqlreg.vl_big_ctl WHERE id = 300;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS big_fenced;
SELECT sqlreg.same_rows('sqlreg.vl_big', 'sqlreg.vl_big_ctl', 'id', ARRAY[
  $$id IN (7, 150, 300)$$, $$id = ANY (ARRAY[7, 8, 150, 300]::bigint[])$$
]) AS big_after_dml_mismatches;

-- ------------------------------------------------ uuid and timestamptz keys
CREATE TABLE sqlreg.vl_uuid (u uuid PRIMARY KEY, n int NOT NULL, v text);
INSERT INTO sqlreg.vl_uuid SELECT ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid, g, 'v' FROM generate_series(1, 200) g;
CREATE TABLE sqlreg.vl_uuid_ctl AS SELECT * FROM sqlreg.vl_uuid;
CREATE TABLE sqlreg.vl_ts (tm timestamptz PRIMARY KEY, n int NOT NULL, v text);
INSERT INTO sqlreg.vl_ts SELECT timestamptz '2026-01-01 00:00:00+00' + g * interval '1 hour', g, 'v' FROM generate_series(1, 200) g;
CREATE TABLE sqlreg.vl_ts_ctl AS SELECT * FROM sqlreg.vl_ts;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.vl_uuid'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS uuid_managed,
koldstore.manage_table(
  table_name => 'sqlreg.vl_ts'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS ts_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS others_caught_up;
SELECT sqlreg.flush_table('sqlreg.vl_uuid'::regclass) IS NOT NULL AS uuid_flushed,
       sqlreg.flush_table('sqlreg.vl_ts'::regclass) IS NOT NULL AS ts_flushed;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.vl_uuid SET v = 'overlap' WHERE n = 3;
UPDATE sqlreg.vl_uuid_ctl SET v = 'overlap' WHERE n = 3;
UPDATE sqlreg.vl_ts SET v = 'overlap' WHERE n = 3;
UPDATE sqlreg.vl_ts_ctl SET v = 'overlap' WHERE n = 3;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS others_fenced_0;
SELECT sqlreg.flush_table('sqlreg.vl_uuid'::regclass) IS NOT NULL AS uuid_reflushed,
       sqlreg.flush_table('sqlreg.vl_ts'::regclass) IS NOT NULL AS ts_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS others_fenced_00;
SET koldstore.max_merge_seen_keys = 50;
SELECT n FROM sqlreg.vl_uuid WHERE u IN ('00000000-0000-0000-0000-000000000007', '00000000-0000-0000-0000-000000000150') ORDER BY n;
SELECT n FROM sqlreg.vl_ts WHERE tm IN (timestamptz '2026-01-01 07:00:00+00', timestamptz '2026-01-07 06:00:00+00') ORDER BY n;
RESET koldstore.max_merge_seen_keys;
SELECT sqlreg.same_rows('sqlreg.vl_uuid', 'sqlreg.vl_uuid_ctl', 'n', ARRAY[
  $$u IN ('00000000-0000-0000-0000-000000000007', '00000000-0000-0000-0000-000000000150', '00000000-0000-0000-0000-00000000ffff')$$
]) AS uuid_list_mismatches;
SELECT sqlreg.same_rows('sqlreg.vl_ts', 'sqlreg.vl_ts_ctl', 'n', ARRAY[
  $$tm IN (timestamptz '2026-01-01 07:00:00+00', timestamptz '2026-01-07 06:00:00+00', timestamptz '2030-01-01 00:00:00+00')$$
]) AS ts_list_mismatches;
