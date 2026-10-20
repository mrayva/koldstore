-- real / double precision primary keys. They could not be managed (double precision was written into
-- the change-log mirror DDL as a quoted identifier) and an ordered flush of a real key failed because the
-- flush keyset had no float bind. A float key is compared between heap, change-log mirror and cold rows
-- through its to_jsonb text, so NaN, the infinities, tiny and huge magnitudes, and real values (which
-- must not be widened to double) all have to render exactly as PostgreSQL does.
--
-- Oracle: an unmanaged copy of every table. The order column repeats (several rows per value), so the
-- ordered flush has to break ties on the float key across page boundaries.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE FUNCTION sqlreg.fingerprint(rel regclass, ord text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE result text;
BEGIN
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', ord, rel) INTO result;
  RETURN result;
END
$$;

CREATE FUNCTION sqlreg.predicate_mismatches(managed regclass, control regclass, ord text, preds text[]) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE p text; a text; b text; bad int := 0;
BEGIN
  FOREACH p IN ARRAY preds LOOP
    EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t WHERE %s', ord, managed, p) INTO a;
    EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t WHERE %s', ord, control, p) INTO b;
    IF a IS DISTINCT FROM b THEN
      bad := bad + 1;
      RAISE NOTICE 'mismatch: %', p;
    END IF;
  END LOOP;
  RETURN bad;
END
$$;

-- ------------------------------------------------ double precision
CREATE TABLE sqlreg.fk8 (k double precision PRIMARY KEY, n int NOT NULL, note text NOT NULL DEFAULT 'x');
INSERT INTO sqlreg.fk8 (k, n)
SELECT g * 0.5 + 0.1, g / 10 FROM generate_series(1, 90) g;
INSERT INTO sqlreg.fk8 (k, n) VALUES
  ('NaN', 3), ('Infinity', 3), ('-Infinity', 3), (0, 3), (1e20, 4), (1e-7, 4), (-1.5e-7, 4),
  (123456789.123456789, 5), (-0.1, 5), (5e-324, 5), (1.7976931348623157e308, 6);
CREATE TABLE sqlreg.fk8_ctl AS SELECT * FROM sqlreg.fk8;
SELECT sqlreg.fingerprint('sqlreg.fk8', 'k') AS fk8_before \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.fk8'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS fk8_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS fk8_caught_up;
SELECT sqlreg.flush_table('sqlreg.fk8'::regclass) IS NOT NULL AS fk8_flushed;
SELECT (koldstore.table_status('sqlreg.fk8'::regclass) ->> 'cold_row_count')::int >= 90 AS fk8_mostly_cold;
SELECT (SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.fk8'::regclass::oid AND status = 'active') >= 4 AS fk8_several_segments;

SELECT sqlreg.fingerprint('sqlreg.fk8', 'k') = :'fk8_before' AS fk8_rows_identical_after_flush;
SELECT sqlreg.predicate_mismatches('sqlreg.fk8', 'sqlreg.fk8_ctl', 'k', ARRAY[
  $$k = 0.6$$, $$k = 0.1 + 0.5$$, $$k > 20$$, $$k < 0$$, $$k BETWEEN 1 AND 10$$, $$k IN (0.6, 1.1, 1e20)$$,
  $$k = 'NaN'$$, $$k = 'Infinity'$$, $$k = '-Infinity'$$, $$k > 'NaN'$$, $$k < 'Infinity'$$, $$k = 0$$,
  $$k = 1e20$$, $$k = 1e-7$$, $$k = -1.5e-7$$, $$k = 5e-324$$, $$k = 1.7976931348623157e308$$,
  $$k = 123456789.123456789$$, $$k IS NOT NULL AND n = 3$$, $$k <> 'NaN' AND k > 30$$
]) AS fk8_predicate_mismatches;

-- cold rows change through hydrate-on-write (incl. the special values) and re-flush intact.
-- (A key deleted from cold storage cannot be re-inserted before the next flush, for any key type, so
-- the inserts below use new keys.)
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.fk8 SET note = 'changed' WHERE k IN (0.6, 'NaN', 'Infinity', -0.1, 1e20, 20.1);
UPDATE sqlreg.fk8_ctl SET note = 'changed' WHERE k IN (0.6, 'NaN', 'Infinity', -0.1, 1e20, 20.1);
DELETE FROM sqlreg.fk8 WHERE k IN ('-Infinity', 1e-7, 10.1);
DELETE FROM sqlreg.fk8_ctl WHERE k IN ('-Infinity', 1e-7, 10.1);
INSERT INTO sqlreg.fk8 (k, n) VALUES (2e-7, 9), (99.5, 9);
INSERT INTO sqlreg.fk8_ctl (k, n, note) VALUES (2e-7, 9, 'x'), (99.5, 9, 'x');
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS fk8_fenced;
SELECT sqlreg.fingerprint('sqlreg.fk8', 'k') = sqlreg.fingerprint('sqlreg.fk8_ctl', 'k') AS fk8_identical_after_dml;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS fk8_caught_up_2;
SELECT sqlreg.flush_table('sqlreg.fk8'::regclass) IS NOT NULL AS fk8_flushed_2;
SELECT sqlreg.fingerprint('sqlreg.fk8', 'k') = sqlreg.fingerprint('sqlreg.fk8_ctl', 'k') AS fk8_identical_after_reflush;

-- ------------------------------------------------ real
CREATE TABLE sqlreg.fk4 (k real PRIMARY KEY, n int NOT NULL);
INSERT INTO sqlreg.fk4 SELECT g * 0.1, g / 10 FROM generate_series(1, 60) g;
INSERT INTO sqlreg.fk4 VALUES ('NaN', 2), ('Infinity', 2), (1e10, 3), (1e-5, 3);
CREATE TABLE sqlreg.fk4_ctl AS SELECT * FROM sqlreg.fk4;
SELECT sqlreg.fingerprint('sqlreg.fk4', 'k') AS fk4_before \gset
SELECT koldstore.manage_table(
  table_name => 'sqlreg.fk4'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS fk4_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS fk4_caught_up;
SELECT sqlreg.flush_table('sqlreg.fk4'::regclass) IS NOT NULL AS fk4_flushed;
SELECT (koldstore.table_status('sqlreg.fk4'::regclass) ->> 'cold_row_count')::int >= 60 AS fk4_mostly_cold;
SELECT sqlreg.fingerprint('sqlreg.fk4', 'k') = :'fk4_before' AS fk4_rows_identical_after_flush;
SELECT sqlreg.predicate_mismatches('sqlreg.fk4', 'sqlreg.fk4_ctl', 'k', ARRAY[
  $$k = 0.3::real$$, $$k = 0.1::real$$, $$k > 3$$, $$k BETWEEN 1 AND 2$$, $$k = 'NaN'$$, $$k = 'Infinity'$$,
  $$k = 1e10::real$$, $$k = 1e-5::real$$, $$k IN (0.2::real, 0.7::real)$$
]) AS fk4_predicate_mismatches;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.fk4 SET n = n WHERE k IN (0.3::real, 'NaN', 1e10::real);
DELETE FROM sqlreg.fk4 WHERE k IN (0.1::real, 'Infinity');
DELETE FROM sqlreg.fk4_ctl WHERE k IN (0.1::real, 'Infinity');
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS fk4_fenced;
SELECT sqlreg.fingerprint('sqlreg.fk4', 'k') = sqlreg.fingerprint('sqlreg.fk4_ctl', 'k') AS fk4_identical_after_dml;
SELECT sqlreg.flush_table('sqlreg.fk4'::regclass) IS NOT NULL AS fk4_flushed_2;
SELECT sqlreg.fingerprint('sqlreg.fk4', 'k') = sqlreg.fingerprint('sqlreg.fk4_ctl', 'k') AS fk4_identical_after_reflush;
