-- A numeric primary key now has segment min/max bounds, so equality lookups, IN lists, ranges and the INSERT
-- guard read only the segments that can hold the key. numeric is stored as text in Parquet, whose
-- statistics sort digits as strings, so the bounds are computed from the values at flush time using an
-- order-preserving decimal encoding in which 1.5, 1.50 and 1.500 are the same key.
--
-- Pruning is proved with koldstore.max_merge_seen_keys set below the table size (a full cold scan trips
-- it); results are compared with an unmanaged copy.

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

-- ------------------------------------------------ numeric(12,2): negatives, zero, many segments
CREATE TABLE sqlreg.nk (x numeric(12, 2) PRIMARY KEY, n int NOT NULL, v text);
INSERT INTO sqlreg.nk SELECT (g - 150) * 1.25, g, 'v' || g FROM generate_series(1, 300) g;
INSERT INTO sqlreg.nk VALUES (-99999999.99, 301, 'min'), (99999999.99, 302, 'max'), (0.01, 303, 'tiny');
CREATE TABLE sqlreg.nk_ctl AS SELECT * FROM sqlreg.nk;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.nk'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS nk_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS nk_caught_up;
SELECT sqlreg.flush_table('sqlreg.nk'::regclass) IS NOT NULL AS nk_flushed;
SELECT (SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.nk'::regclass::oid AND status = 'active') >= 14 AS nk_many_segments;
SELECT count(*) AS nk_bounded_segments FROM koldstore.cold_segment_index
WHERE table_oid = 'sqlreg.nk'::regclass::oid AND type_oid = 1700 AND min_value IS NOT NULL AND max_value IS NOT NULL;

SET koldstore.max_merge_seen_keys = 70;
SELECT count(*) FROM sqlreg.nk;
SELECT x FROM sqlreg.nk WHERE x = 1.25;
SELECT x FROM sqlreg.nk WHERE x = 1.250;
SELECT x FROM sqlreg.nk WHERE x = 1.25::numeric(5,1) + 0.0;
SELECT x FROM sqlreg.nk WHERE x = -187.5;
SELECT x FROM sqlreg.nk WHERE x = 0;
SELECT x FROM sqlreg.nk WHERE x = 99999999.99;
SELECT x FROM sqlreg.nk WHERE x = 5;
SELECT x FROM sqlreg.nk WHERE x IN (1.25, 100, 37.5) ORDER BY x;
SELECT x FROM sqlreg.nk WHERE x BETWEEN 10 AND 20 ORDER BY x;
SELECT x FROM sqlreg.nk WHERE x > 180 AND x < 190 ORDER BY x;
INSERT INTO sqlreg.nk VALUES (12345.67, 400, 'fresh');
INSERT INTO sqlreg.nk_ctl VALUES (12345.67, 400, 'fresh');
INSERT INTO sqlreg.nk VALUES (1.25, 401, 'dup');
INSERT INTO sqlreg.nk VALUES (1.250, 402, 'dup, other scale');
RESET koldstore.max_merge_seen_keys;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS nk_fenced;
SELECT sqlreg.same_rows('sqlreg.nk', 'sqlreg.nk_ctl', 'x', ARRAY[
  $$x = 1.25$$, $$x = 1.250$$, $$x = -187.5$$, $$x = 0$$, $$x = 0.01$$, $$x IN (1.25, 100, 37.5, 5)$$,
  $$x BETWEEN 10 AND 20$$, $$x > 180$$, $$x < -180$$, $$x >= 99999999.99$$, $$x <= -99999999.99$$,
  $$x > 0 AND x < 0.02$$, $$x = 1.25 OR x = 3.75$$, $$x <> 1.25$$, $$x = 12345.67$$
]) AS nk_mismatches;

-- ------------------------------------------------ unconstrained numeric: scales, NaN and infinities
CREATE TABLE sqlreg.nu (x numeric PRIMARY KEY, n int NOT NULL);
INSERT INTO sqlreg.nu SELECT (g - 60)::numeric / 8, g FROM generate_series(1, 120) g;
INSERT INTO sqlreg.nu VALUES ('NaN', 200), ('Infinity', 201), ('-Infinity', 202),
  (123456789012345678901234567890.1234567890, 204), (0.000000000000000000001, 205);
CREATE TABLE sqlreg.nu_ctl AS SELECT * FROM sqlreg.nu;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.nu'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS nu_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS nu_caught_up;
SELECT sqlreg.flush_table('sqlreg.nu'::regclass) IS NOT NULL AS nu_flushed;
SELECT sqlreg.same_rows('sqlreg.nu', 'sqlreg.nu_ctl', 'n', ARRAY[
  $$x = 0.125$$, $$x = 0.1250$$, $$x = -0.25$$, $$x = 'NaN'$$, $$x = 'Infinity'$$, $$x = '-Infinity'$$, $$x = 0$$,
  $$x = 123456789012345678901234567890.123456789$$, $$x = 123456789012345678901234567890.1234567890$$,
  $$x = 0.000000000000000000001$$, $$x > 1$$, $$x < -1$$, $$x > 'NaN'$$, $$x < 'Infinity'$$,
  $$x BETWEEN -2 AND 2$$, $$x IN (0.125, -0.25, 'NaN', 'Infinity', 7)$$
]) AS nu_mismatches;
SET koldstore.max_merge_seen_keys = 40;
SELECT x FROM sqlreg.nu WHERE x = 0.125;
SELECT x FROM sqlreg.nu WHERE x = 0.1250;
SELECT x FROM sqlreg.nu WHERE x = 'NaN';
SET koldstore.max_merge_seen_keys = 70;
SELECT x FROM sqlreg.nu WHERE x IN (0.125, -0.25) ORDER BY x;
SELECT x FROM sqlreg.nu WHERE x BETWEEN 1 AND 1.5 ORDER BY x;
RESET koldstore.max_merge_seen_keys;

-- ------------------------------------------------ segments without a numeric index row stay candidates
DELETE FROM koldstore.cold_segment_index
WHERE table_oid = 'sqlreg.nk'::regclass::oid AND type_oid = 1700
  AND segment_id IN (SELECT segment_id FROM koldstore.cold_segments
                      WHERE table_oid = 'sqlreg.nk'::regclass::oid AND status = 'active'
                      ORDER BY batch_number LIMIT 8);
SELECT sqlreg.same_rows('sqlreg.nk', 'sqlreg.nk_ctl', 'x', ARRAY[
  $$x = 1.25$$, $$x = -187.5$$, $$x IN (1.25, 100, 37.5)$$, $$x BETWEEN 10 AND 20$$, $$x = 5$$
]) AS nk_unindexed_mismatches;
