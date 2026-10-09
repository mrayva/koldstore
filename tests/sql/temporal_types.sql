-- date, timestamp (without time zone) and timestamptz columns, including as primary keys. These used
-- to be unmanageable (date/timestamp were not in the type matrix at all). PostgreSQL's epoch is
-- 2000-01-01 while Parquet/Arrow use 1970-01-01, and `infinity` / `-infinity` are the extreme integer
-- values, so every conversion has to shift the epoch without shifting the infinities.
--
-- Oracle: an unmanaged copy of every table. After a flush the managed table must return exactly the
-- same rows, and every predicate must select exactly the same rows as on the copy.

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

-- number of predicates (WHERE clause texts) on which the two tables select different rows
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

-- ------------------------------------------------ temporal non-key columns
CREATE TABLE sqlreg.tev (
  id bigint PRIMARY KEY, d date, ts timestamp, tz timestamptz, note text NOT NULL DEFAULT 'x'
);
INSERT INTO sqlreg.tev (id, d, ts, tz)
SELECT g, date '2019-12-20' + g, timestamp '2019-12-20 10:00:00.123456' + g * interval '7 hours',
       timestamptz '2019-12-20 10:00:00+00' + g * interval '7 hours'
FROM generate_series(1, 60) g;
INSERT INTO sqlreg.tev (id, d, ts, tz) VALUES
  (101, 'infinity', 'infinity', 'infinity'),
  (102, '-infinity', '-infinity', '-infinity'),
  (103, '1969-07-20', '1969-07-20 20:17:40.5', '1969-07-20 20:17:40+00'),
  (104, '0044-03-15 BC', '0044-03-15 12:00:00 BC', '0044-03-15 12:00:00+00 BC'),
  (105, '2000-01-01', '2000-01-01 00:00:00', '2000-01-01 00:00:00+00'),
  (106, NULL, NULL, NULL);
CREATE TABLE sqlreg.tev_ctl AS SELECT * FROM sqlreg.tev;
SELECT sqlreg.fingerprint('sqlreg.tev', 'id') AS before_flush \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.tev'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS tev_managed;
SELECT sqlreg.flush_table('sqlreg.tev'::regclass) IS NOT NULL AS tev_flushed;
SELECT (koldstore.table_status('sqlreg.tev'::regclass) ->> 'cold_row_count')::int >= 50 AS mostly_cold;

SELECT sqlreg.fingerprint('sqlreg.tev', 'id') = :'before_flush' AS rows_identical_after_flush;
SELECT sqlreg.predicate_mismatches('sqlreg.tev', 'sqlreg.tev_ctl', 'id', ARRAY[
  $$d = date '2020-01-05'$$, $$d > date '2020-01-05'$$, $$d <= date '2000-01-01'$$,
  $$d BETWEEN date '2019-12-25' AND date '2020-01-10'$$, $$d IN (date '2019-12-21', date '1969-07-20')$$,
  $$d = 'infinity'$$, $$d = '-infinity'$$, $$d < 'infinity'$$, $$d > '-infinity'$$, $$d IS NULL$$,
  $$d < date '0100-01-01'$$, $$d = '0044-03-15 BC'$$,
  $$ts = timestamp '2019-12-21 17:00:00.123456'$$, $$ts > timestamp '2020-01-01'$$,
  $$ts < timestamp '1970-01-01'$$, $$ts BETWEEN timestamp '2019-12-25' AND timestamp '2020-01-03 12:00'$$,
  $$ts = 'infinity'$$, $$ts = '-infinity'$$, $$ts <= timestamp '2000-01-01 00:00:00'$$,
  $$ts = timestamp '1969-07-20 20:17:40.5'$$, $$ts < 'infinity' AND ts > '-infinity'$$,
  $$tz = timestamptz '2019-12-21 17:00:00+00'$$, $$tz > timestamptz '2020-01-01 00:00:00+00'$$,
  $$tz = 'infinity'$$, $$tz = '-infinity'$$, $$tz < timestamptz '1970-01-01 00:00:00+00'$$,
  $$d > date '2020-01-01' AND ts < timestamp '2020-01-05' AND note = 'x'$$
]) AS predicate_mismatches;
SELECT count(*) AS infinities FROM sqlreg.tev WHERE d IN ('infinity', '-infinity');

-- ordering by the temporal columns
SELECT (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.tev ORDER BY ts, id LIMIT 8) a)
     = (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.tev_ctl ORDER BY ts, id LIMIT 8) b) AS order_by_ts_matches;
SELECT (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.tev ORDER BY d DESC, id LIMIT 8) a)
     = (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.tev_ctl ORDER BY d DESC, id LIMIT 8) b) AS order_by_d_desc_matches;

-- cold rows change through hydrate-on-write, including their temporal columns, and re-flush intact
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.tev SET note = 'changed' WHERE id IN (3, 40, 102);
UPDATE sqlreg.tev_ctl SET note = 'changed' WHERE id IN (3, 40, 102);
UPDATE sqlreg.tev SET d = d + 1, ts = ts + interval '1 day', tz = tz + interval '1 day' WHERE id = 7;
UPDATE sqlreg.tev_ctl SET d = d + 1, ts = ts + interval '1 day', tz = tz + interval '1 day' WHERE id = 7;
DELETE FROM sqlreg.tev WHERE id = 104;
DELETE FROM sqlreg.tev_ctl WHERE id = 104;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.tev', 'id') = sqlreg.fingerprint('sqlreg.tev_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.tev'::regclass, true) IS NOT NULL AS tev_reflushed;
SELECT sqlreg.fingerprint('sqlreg.tev', 'id') = sqlreg.fingerprint('sqlreg.tev_ctl', 'id') AS rows_match_after_reflush;
SELECT count(*) AS rows_after FROM sqlreg.tev;

-- ------------------------------------------------ temporal primary keys
-- Both with a separate order column and with the key itself as the order column. Equality by a
-- literal takes the point-lookup path (catalog bounds, then Parquet statistics/bloom filter), which
-- compares against the Arrow (Unix-epoch) values; the IN and subquery forms take other paths.
CREATE TABLE sqlreg.pk_date (id date PRIMARY KEY, n int NOT NULL, v text NOT NULL DEFAULT 'x');
INSERT INTO sqlreg.pk_date SELECT date '2019-12-25' + g, g, 'x' FROM generate_series(1, 45) g;
INSERT INTO sqlreg.pk_date VALUES ('infinity', 100, 'x'), ('-infinity', 101, 'x'), ('1969-07-20', 102, 'x');
CREATE TABLE sqlreg.pk_ts (id timestamp PRIMARY KEY, n int NOT NULL, v text NOT NULL DEFAULT 'x');
INSERT INTO sqlreg.pk_ts SELECT timestamp '2019-12-25 00:00:00.5' + g * interval '5 hours', g, 'x' FROM generate_series(1, 45) g;
INSERT INTO sqlreg.pk_ts VALUES ('infinity', 100, 'x'), ('-infinity', 101, 'x'), ('1969-07-20 20:17:40.5', 102, 'x');
CREATE TABLE sqlreg.pk_tz (id timestamptz PRIMARY KEY, n int NOT NULL, v text NOT NULL DEFAULT 'x');
INSERT INTO sqlreg.pk_tz SELECT timestamptz '2019-12-25 00:00:00+00' + g * interval '5 hours', g, 'x' FROM generate_series(1, 45) g;
INSERT INTO sqlreg.pk_tz VALUES ('infinity', 100, 'x'), ('-infinity', 101, 'x'), ('1969-07-20 20:17:40+00', 102, 'x');
CREATE TABLE sqlreg.pk_datek (id date PRIMARY KEY, v text NOT NULL DEFAULT 'x');
INSERT INTO sqlreg.pk_datek SELECT date '2019-12-25' + g, 'x' FROM generate_series(1, 45) g;
CREATE TABLE sqlreg.pk_tsk (id timestamp PRIMARY KEY, v text NOT NULL DEFAULT 'x');
INSERT INTO sqlreg.pk_tsk SELECT timestamp '2019-12-25 00:00:00.5' + g * interval '5 hours', 'x' FROM generate_series(1, 45) g;

CREATE TABLE sqlreg.pk_date_ctl AS SELECT * FROM sqlreg.pk_date;
CREATE TABLE sqlreg.pk_ts_ctl AS SELECT * FROM sqlreg.pk_ts;
CREATE TABLE sqlreg.pk_tz_ctl AS SELECT * FROM sqlreg.pk_tz;
CREATE TABLE sqlreg.pk_datek_ctl AS SELECT * FROM sqlreg.pk_datek;
CREATE TABLE sqlreg.pk_tsk_ctl AS SELECT * FROM sqlreg.pk_tsk;

SELECT koldstore.manage_table(table_name => t::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
         min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'n', auto_flush => false) IS NOT NULL AS managed
FROM unnest(ARRAY['sqlreg.pk_date', 'sqlreg.pk_ts', 'sqlreg.pk_tz']) t;
SELECT koldstore.manage_table(table_name => t::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
         min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false) IS NOT NULL AS managed_key_ordered
FROM unnest(ARRAY['sqlreg.pk_datek', 'sqlreg.pk_tsk']) t;
SELECT t, sqlreg.flush_table(t::regclass) IS NOT NULL AS flushed
FROM unnest(ARRAY['sqlreg.pk_date', 'sqlreg.pk_ts', 'sqlreg.pk_tz', 'sqlreg.pk_datek', 'sqlreg.pk_tsk']) t;

SELECT sqlreg.fingerprint('sqlreg.pk_date', 'id') = sqlreg.fingerprint('sqlreg.pk_date_ctl', 'id') AS date_pk_rows,
       sqlreg.fingerprint('sqlreg.pk_ts', 'id') = sqlreg.fingerprint('sqlreg.pk_ts_ctl', 'id') AS ts_pk_rows,
       sqlreg.fingerprint('sqlreg.pk_tz', 'id') = sqlreg.fingerprint('sqlreg.pk_tz_ctl', 'id') AS tz_pk_rows,
       sqlreg.fingerprint('sqlreg.pk_datek', 'id') = sqlreg.fingerprint('sqlreg.pk_datek_ctl', 'id') AS date_key_ordered_rows,
       sqlreg.fingerprint('sqlreg.pk_tsk', 'id') = sqlreg.fingerprint('sqlreg.pk_tsk_ctl', 'id') AS ts_key_ordered_rows;

-- literal equality for every key (cold and hot), and for keys that do not exist
SELECT sqlreg.predicate_mismatches('sqlreg.pk_date', 'sqlreg.pk_date_ctl', 'id',
         ARRAY(SELECT format('id = %L::date', id) FROM sqlreg.pk_date_ctl WHERE id NOT IN ('infinity', '-infinity')
               UNION ALL SELECT $$id = 'infinity'$$ UNION ALL SELECT $$id = '-infinity'$$
               UNION ALL SELECT $$id = date '1999-01-01'$$ UNION ALL SELECT $$id >= date '2020-01-10' AND id < date '2020-01-20'$$
               UNION ALL SELECT $$id IN (date '2020-01-03', date '1969-07-20', date '2030-01-01')$$)) AS date_pk_mismatches;
SELECT sqlreg.predicate_mismatches('sqlreg.pk_ts', 'sqlreg.pk_ts_ctl', 'id',
         ARRAY(SELECT format('id = %L::timestamp', id) FROM sqlreg.pk_ts_ctl WHERE id NOT IN ('infinity', '-infinity')
               UNION ALL SELECT $$id = 'infinity'$$ UNION ALL SELECT $$id = '-infinity'$$
               UNION ALL SELECT $$id = timestamp '1999-01-01'$$ UNION ALL SELECT $$id > timestamp '2020-01-02' AND id < timestamp '2020-01-05'$$)) AS ts_pk_mismatches;
SELECT sqlreg.predicate_mismatches('sqlreg.pk_tz', 'sqlreg.pk_tz_ctl', 'id',
         ARRAY(SELECT format('id = %L::timestamptz', id) FROM sqlreg.pk_tz_ctl WHERE id NOT IN ('infinity', '-infinity')
               UNION ALL SELECT $$id = 'infinity'$$ UNION ALL SELECT $$id = '-infinity'$$
               UNION ALL SELECT $$id = timestamptz '1999-01-01 00:00:00+00'$$)) AS tz_pk_mismatches;
SELECT sqlreg.predicate_mismatches('sqlreg.pk_datek', 'sqlreg.pk_datek_ctl', 'id',
         ARRAY(SELECT format('id = %L::date', id) FROM sqlreg.pk_datek_ctl)) AS date_key_ordered_mismatches;
SELECT sqlreg.predicate_mismatches('sqlreg.pk_tsk', 'sqlreg.pk_tsk_ctl', 'id',
         ARRAY(SELECT format('id = %L::timestamp', id) FROM sqlreg.pk_tsk_ctl)) AS ts_key_ordered_mismatches;
SELECT count(*) AS date_pk_found FROM sqlreg.pk_date WHERE id = date '2020-01-03';
SELECT count(*) AS date_pk_absent FROM sqlreg.pk_date WHERE id = date '1999-01-01';

-- a cold-only key is hydrated by a plain UPDATE/DELETE with the key as a literal
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.pk_date SET v = 'u' WHERE id = date '2020-01-03';
UPDATE sqlreg.pk_date_ctl SET v = 'u' WHERE id = date '2020-01-03';
UPDATE sqlreg.pk_ts SET v = 'u' WHERE id = timestamp '2019-12-25 10:00:00.5';
UPDATE sqlreg.pk_ts_ctl SET v = 'u' WHERE id = timestamp '2019-12-25 10:00:00.5';
DELETE FROM sqlreg.pk_date WHERE id = date '2020-01-05';
DELETE FROM sqlreg.pk_date_ctl WHERE id = date '2020-01-05';
DELETE FROM sqlreg.pk_ts WHERE id = timestamp '2019-12-25 15:00:00.5';
DELETE FROM sqlreg.pk_ts_ctl WHERE id = timestamp '2019-12-25 15:00:00.5';
-- a timestamptz key renders differently in every session zone; the delete must still mask the cold copy
SET TimeZone = 'America/New_York';
DELETE FROM sqlreg.pk_tz WHERE id = timestamptz '2019-12-25 20:00:00+00';
DELETE FROM sqlreg.pk_tz_ctl WHERE id = timestamptz '2019-12-25 20:00:00+00';
RESET TimeZone;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.pk_date', 'id') = sqlreg.fingerprint('sqlreg.pk_date_ctl', 'id') AS date_pk_rows_after_dml,
       sqlreg.fingerprint('sqlreg.pk_ts', 'id') = sqlreg.fingerprint('sqlreg.pk_ts_ctl', 'id') AS ts_pk_rows_after_dml,
       sqlreg.fingerprint('sqlreg.pk_tz', 'id') = sqlreg.fingerprint('sqlreg.pk_tz_ctl', 'id') AS tz_pk_rows_after_dml;
SELECT (SELECT count(*) FROM sqlreg.pk_date WHERE id = date '2020-01-05') AS deleted_date_gone,
       (SELECT count(*) FROM sqlreg.pk_ts WHERE id = timestamp '2019-12-25 15:00:00.5') AS deleted_ts_gone,
       (SELECT count(*) FROM sqlreg.pk_tz WHERE id = timestamptz '2019-12-25 20:00:00+00') AS deleted_tz_gone;
-- and the deletes survive a flush
SELECT sqlreg.flush_table(t::regclass, true) IS NOT NULL AS reflushed
FROM unnest(ARRAY['sqlreg.pk_date', 'sqlreg.pk_ts', 'sqlreg.pk_tz']) t;
SELECT sqlreg.fingerprint('sqlreg.pk_date', 'id') = sqlreg.fingerprint('sqlreg.pk_date_ctl', 'id') AS date_pk_rows_after_reflush,
       sqlreg.fingerprint('sqlreg.pk_ts', 'id') = sqlreg.fingerprint('sqlreg.pk_ts_ctl', 'id') AS ts_pk_rows_after_reflush,
       sqlreg.fingerprint('sqlreg.pk_tz', 'id') = sqlreg.fingerprint('sqlreg.pk_tz_ctl', 'id') AS tz_pk_rows_after_reflush;

-- ------------------------------------------------ boolean primary key
-- (a boolean key has two values, so keep one hot row to leave something to flush)
CREATE TABLE sqlreg.pk_bool (id boolean PRIMARY KEY, n int NOT NULL, v text NOT NULL);
INSERT INTO sqlreg.pk_bool VALUES (true, 1, 'yes'), (false, 2, 'no');
SELECT koldstore.manage_table(table_name => 'sqlreg.pk_bool'::regclass, storage => 'sqlreg_fs', hot_row_limit => 1,
         min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'n', auto_flush => false) IS NOT NULL AS bool_managed;
SELECT sqlreg.flush_table('sqlreg.pk_bool'::regclass) IS NOT NULL AS bool_flushed;
SELECT id, v FROM sqlreg.pk_bool WHERE id ORDER BY id;
SELECT id, v FROM sqlreg.pk_bool WHERE NOT id;
SELECT id, v FROM sqlreg.pk_bool WHERE id = false;
SELECT count(*) AS bool_rows FROM sqlreg.pk_bool;
