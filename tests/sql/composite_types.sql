-- User-defined composite types are stored as their record text and read back through record_in.

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

SET TimeZone = 'UTC';
CREATE TYPE sqlreg.inner_t AS (n numeric, tags text[]);
CREATE TYPE sqlreg.outer_t AS (
  i int, t text, f float8, ts timestamptz, d date, iv interval, b bytea, j jsonb, inner_v sqlreg.inner_t);
CREATE DOMAIN sqlreg.outer_d AS sqlreg.outer_t;
CREATE TABLE sqlreg.cmp (id bigint PRIMARY KEY, c sqlreg.outer_t, dm sqlreg.outer_d, tag text);
INSERT INTO sqlreg.cmp VALUES
  (1, ROW(1, 'plain', 0.1, '2024-01-01 10:00+00', '2024-01-02', '1 day 02:03:04', '\xdeadbeef', '{"a":[1,2]}', ROW(1.50, ARRAY['x','y'])::sqlreg.inner_t),
     ROW(7, 'dom', 1, '2024-02-02 00:00+00', '2024-02-02', '1 mon', '\x00', '{}', NULL)::sqlreg.outer_t, 'a'),
  (2, ROW(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)::sqlreg.outer_t, NULL, 'allnull'),
  (3, NULL, NULL, 'null'),
  (4, ROW(-5, 'quote " comma , paren ( ) backslash \ end', 0.30000000000000004, 'infinity', '-infinity', '-1 year -2 days', '\x', '"s"',
          ROW(NULL, ARRAY[NULL, 'a b', ''])::sqlreg.inner_t),
     NULL, 'awkward'),
  (5, ROW(0, '', 'NaN', '1999-12-31 23:59:59.999999+00', '0001-01-01', '0', '\x5c', '{"k": null}', ROW(0, '{}')::sqlreg.inner_t), NULL, 'empty');
INSERT INTO sqlreg.cmp SELECT g,
  ROW(g, 'row ' || g, g / 3.0, timestamptz '2024-01-01 00:00+00' + g * interval '1 hour', date '2024-01-01' + g,
      g * interval '1 minute 30 seconds', decode(lpad(to_hex(g), 4, '0'), 'hex'), jsonb_build_object('g', g),
      ROW(g * 1.25, ARRAY[g::text])::sqlreg.inner_t)::sqlreg.outer_t, NULL, 'bulk'
FROM generate_series(6, 60) g;
CREATE TABLE sqlreg.cmp_ctl AS SELECT * FROM sqlreg.cmp;
SELECT sqlreg.fingerprint('sqlreg.cmp', 'id') AS before_flush \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.cmp'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS cmp_managed;
-- flush under unusual output settings: the stored text must not depend on them
SET extra_float_digits = -3;
SET DateStyle = 'German, DMY';
SET IntervalStyle = 'postgres_verbose';
SET bytea_output = 'escape';
SET TimeZone = 'Asia/Tokyo';
SELECT sqlreg.flush_table('sqlreg.cmp'::regclass) IS NOT NULL AS cmp_flushed;
RESET extra_float_digits;
RESET DateStyle;
RESET IntervalStyle;
RESET bytea_output;
SET TimeZone = 'UTC';
SELECT sqlreg.fingerprint('sqlreg.cmp', 'id') = :'before_flush' AS same_after_flush;
SELECT a.id FROM sqlreg.cmp a JOIN sqlreg.cmp_ctl b USING (id)
WHERE a.c::text IS DISTINCT FROM b.c::text OR a.dm::text IS DISTINCT FROM b.dm::text ORDER BY a.id;
SELECT id, c, dm FROM sqlreg.cmp WHERE id <= 5 ORDER BY id;
SELECT id, c IS NULL AS c_null, (c).i, (c).t, (c).inner_v, (c).iv FROM sqlreg.cmp WHERE id <= 5 ORDER BY id;

-- field access and predicates run on the cold rows
SELECT count(*) AS big_i FROM sqlreg.cmp WHERE (c).i > 30;
SELECT count(*) AS big_i_ctl FROM sqlreg.cmp_ctl WHERE (c).i > 30;
SELECT count(*) AS null_composites FROM sqlreg.cmp WHERE c IS NULL;
SELECT count(*) AS null_composites_ctl FROM sqlreg.cmp_ctl WHERE c IS NULL;
SELECT count(*) AS inner_match FROM sqlreg.cmp WHERE ((c).inner_v).tags[1] = '7';
SELECT count(*) AS eq_match FROM sqlreg.cmp WHERE c = (SELECT c FROM sqlreg.cmp_ctl WHERE id = 20);

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.cmp SET tag = 'changed' WHERE id IN (1, 2, 3, 4, 5);
UPDATE sqlreg.cmp_ctl SET tag = 'changed' WHERE id IN (1, 2, 3, 4, 5);
UPDATE sqlreg.cmp SET c = ROW(99, 'new', 1, now(), current_date, '1 s', '\x01', '1', NULL)::sqlreg.outer_t WHERE id = 3;
UPDATE sqlreg.cmp_ctl SET c = (SELECT c FROM sqlreg.cmp WHERE id = 3) WHERE id = 3;
DELETE FROM sqlreg.cmp WHERE id = 6;
DELETE FROM sqlreg.cmp_ctl WHERE id = 6;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.cmp', 'id') = sqlreg.fingerprint('sqlreg.cmp_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.cmp'::regclass, true) IS NOT NULL AS cmp_reflushed;
SELECT sqlreg.fingerprint('sqlreg.cmp', 'id') = sqlreg.fingerprint('sqlreg.cmp_ctl', 'id') AS rows_match_after_reflush;
SELECT koldstore.unmanage_table('sqlreg.cmp'::regclass, true, true) IS NOT NULL AS cmp_unmanaged;
SELECT sqlreg.fingerprint('sqlreg.cmp', 'id') = sqlreg.fingerprint('sqlreg.cmp_ctl', 'id') AS rows_match_after_unmanage;

-- a composite key and composite arrays are refused
CREATE TABLE sqlreg.cmp_pk (c sqlreg.inner_t PRIMARY KEY, v int NOT NULL);
SELECT koldstore.manage_table(table_name => 'sqlreg.cmp_pk'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'v', auto_flush => false) IS NOT NULL AS pk_managed;
CREATE TABLE sqlreg.cmp_arr (id bigint PRIMARY KEY, c sqlreg.inner_t[]);
SELECT koldstore.manage_table(table_name => 'sqlreg.cmp_arr'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false) IS NOT NULL AS arr_managed;
