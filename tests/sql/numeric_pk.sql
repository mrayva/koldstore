-- numeric primary keys (plain, with a precision/scale modifier) and a varchar(n) key. Ordered flush
-- compared the numeric key with a text bind ("operator does not exist: numeric > text"), the datum
-- could not be read back as text, the change-log mirror's DDL mis-rendered `numeric(12,2)`, and the
-- ordered merge claimed to deliver numeric order although numeric has no Sort Key V1 encoding.
--
-- Oracle: an unmanaged copy of each table.

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

CREATE TABLE sqlreg.np (id numeric PRIMARY KEY, n int NOT NULL, v text NOT NULL DEFAULT 'x');
INSERT INTO sqlreg.np SELECT g * 1.25, g % 50, 'x' FROM generate_series(1, 2500) g;
INSERT INTO sqlreg.np VALUES (-7.5, 3, 'x'), (0, 4, 'x'), (123456789012345.678, 5, 'x');
CREATE TABLE sqlreg.np2 (id numeric(12,2) PRIMARY KEY, n int NOT NULL, v text NOT NULL DEFAULT 'x');
INSERT INTO sqlreg.np2 SELECT g * 1.25, g % 50, 'x' FROM generate_series(1, 2500) g;
CREATE TABLE sqlreg.np3 (id varchar(20) PRIMARY KEY, n int NOT NULL, v text NOT NULL DEFAULT 'x');
INSERT INTO sqlreg.np3 SELECT 'k' || lpad(g::text, 6, '0'), g % 50, 'x' FROM generate_series(1, 2500) g;
CREATE TABLE sqlreg.np_ctl AS SELECT * FROM sqlreg.np;
CREATE TABLE sqlreg.np2_ctl AS SELECT * FROM sqlreg.np2;
CREATE TABLE sqlreg.np3_ctl AS SELECT * FROM sqlreg.np3;

SELECT t, koldstore.manage_table(table_name => t::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
         min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'n', auto_flush => false) IS NOT NULL AS managed
FROM unnest(ARRAY['sqlreg.np', 'sqlreg.np2', 'sqlreg.np3']) t;
SELECT t, sqlreg.flush_table(t::regclass) IS NOT NULL AS flushed
FROM unnest(ARRAY['sqlreg.np', 'sqlreg.np2', 'sqlreg.np3']) t;
SELECT t, (koldstore.table_status(t::regclass) ->> 'cold_row_count')::int >= 2400 AS mostly_cold
FROM unnest(ARRAY['sqlreg.np', 'sqlreg.np2', 'sqlreg.np3']) t;

SELECT sqlreg.fingerprint('sqlreg.np', 'id') = sqlreg.fingerprint('sqlreg.np_ctl', 'id') AS np_rows,
       sqlreg.fingerprint('sqlreg.np2', 'id') = sqlreg.fingerprint('sqlreg.np2_ctl', 'id') AS np2_rows,
       sqlreg.fingerprint('sqlreg.np3', 'id') = sqlreg.fingerprint('sqlreg.np3_ctl', 'id') AS np3_rows;

-- ORDER BY on the key, both directions
SELECT (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.np ORDER BY id LIMIT 4) a) AS np_first,
       (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.np ORDER BY id DESC LIMIT 3) a) AS np_last;
SELECT (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.np2 ORDER BY id DESC LIMIT 3) a)
     = (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.np2_ctl ORDER BY id DESC LIMIT 3) b) AS np2_desc_matches;

-- literal lookups: equal values with different scale find the same row; absent keys find nothing
SELECT (SELECT count(*) FROM sqlreg.np WHERE id = 1.25) AS hit,
       (SELECT count(*) FROM sqlreg.np WHERE id = 1.250) AS hit_other_scale,
       (SELECT count(*) FROM sqlreg.np WHERE id = -7.5) AS negative,
       (SELECT count(*) FROM sqlreg.np WHERE id = 123456789012345.678) AS large,
       (SELECT count(*) FROM sqlreg.np WHERE id = 99999) AS absent,
       (SELECT count(*) FROM sqlreg.np2 WHERE id = 3125.00) AS fixed_scale,
       (SELECT count(*) FROM sqlreg.np3 WHERE id = 'k000100') AS varchar_key;
SELECT count(*) AS range_rows FROM sqlreg.np WHERE id > 100 AND id <= 200;
SELECT count(*) AS range_rows_ctl FROM sqlreg.np_ctl WHERE id > 100 AND id <= 200;

-- cold rows change through hydrate-on-write and survive a re-flush
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.np SET v = 'u' WHERE id = 62.5;
UPDATE sqlreg.np_ctl SET v = 'u' WHERE id = 62.5;
DELETE FROM sqlreg.np WHERE id = 125;
DELETE FROM sqlreg.np_ctl WHERE id = 125;
UPDATE sqlreg.np2 SET v = 'u' WHERE id = 62.50;
UPDATE sqlreg.np2_ctl SET v = 'u' WHERE id = 62.50;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
-- An unconstrained numeric does not keep its display scale when a cold row is hydrated (62.50 comes
-- back as 62.5: the row travels as JSON), so compare np by value; np2's numeric(12,2) restores it.
SELECT (SELECT count(*) FROM (SELECT id, n, v FROM sqlreg.np EXCEPT SELECT id, n, v FROM sqlreg.np_ctl) a)
     + (SELECT count(*) FROM (SELECT id, n, v FROM sqlreg.np_ctl EXCEPT SELECT id, n, v FROM sqlreg.np) b) AS np_differing_rows_after_dml,
       sqlreg.fingerprint('sqlreg.np2', 'id') = sqlreg.fingerprint('sqlreg.np2_ctl', 'id') AS np2_rows_after_dml;
SELECT sqlreg.flush_table(t::regclass, true) IS NOT NULL AS reflushed FROM unnest(ARRAY['sqlreg.np', 'sqlreg.np2']) t;
SELECT (SELECT count(*) FROM (SELECT id, n, v FROM sqlreg.np EXCEPT SELECT id, n, v FROM sqlreg.np_ctl) a)
     + (SELECT count(*) FROM (SELECT id, n, v FROM sqlreg.np_ctl EXCEPT SELECT id, n, v FROM sqlreg.np) b) AS np_differing_rows_after_reflush,
       sqlreg.fingerprint('sqlreg.np2', 'id') = sqlreg.fingerprint('sqlreg.np2_ctl', 'id') AS np2_rows_after_reflush,
       (SELECT count(*) FROM sqlreg.np) AS np_rows, (SELECT count(DISTINCT id) FROM sqlreg.np) AS np_distinct_ids;
SELECT count(*) AS deleted_key_gone FROM sqlreg.np WHERE id = 125;
