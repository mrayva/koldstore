-- The built-in range types are stored as their canonical text and read back exactly.

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
CREATE TABLE sqlreg.rg (id bigint PRIMARY KEY, i4 int4range, i8 int8range, nr numrange, ts tsrange, tz tstzrange, dr daterange, tag text);
INSERT INTO sqlreg.rg VALUES
  (1, '[1,10)', '[1,9000000000000)', '[1.5,2.25]', '[2024-01-01 10:00,2024-01-02)', '[2024-01-01 00:00+00,2024-06-01 12:30+05)', '[2024-01-01,2024-02-01)', 'a'),
  (2, 'empty', 'empty', 'empty', 'empty', 'empty', 'empty', 'empty'),
  (3, '(,5]', '[7,)', '(,)', '[2024-05-05 00:00,)', '(,2030-01-01 00:00+00)', '(,)', 'infinite'),
  (4, NULL, NULL, NULL, NULL, NULL, NULL, 'nulls'),
  (5, '[-100,-50]', '(0,1)', '[0.000,0.000]', '(-infinity,infinity)', '[-infinity,infinity]', '[-infinity,infinity]', 'odd');
INSERT INTO sqlreg.rg SELECT g, int4range(g, g + 5), int8range(g, g::bigint * 1000000000), numrange(g / 4.0, g / 2.0),
  tsrange(timestamp '2024-01-01' + g * interval '1 hour', timestamp '2024-01-01' + (g + 3) * interval '1 hour'),
  tstzrange(timestamptz '2024-01-01 00:00+00' + g * interval '1 day', timestamptz '2024-01-01 00:00+00' + (g + 2) * interval '1 day'),
  daterange(date '2024-01-01' + g, date '2024-01-01' + g + 10), 'bulk' FROM generate_series(6, 60) g;
CREATE TABLE sqlreg.rg_ctl AS SELECT * FROM sqlreg.rg;
SELECT sqlreg.fingerprint('sqlreg.rg', 'id') AS before_flush \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.rg'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS rg_managed;
SELECT sqlreg.flush_table('sqlreg.rg'::regclass) IS NOT NULL AS rg_flushed;
SELECT sqlreg.fingerprint('sqlreg.rg', 'id') = :'before_flush' AS same_after_flush;
SELECT a.id FROM sqlreg.rg a JOIN sqlreg.rg_ctl b USING (id)
WHERE a.i4 IS DISTINCT FROM b.i4 OR a.i8 IS DISTINCT FROM b.i8 OR a.nr IS DISTINCT FROM b.nr
   OR a.ts IS DISTINCT FROM b.ts OR a.tz IS DISTINCT FROM b.tz OR a.dr IS DISTINCT FROM b.dr ORDER BY a.id;
SELECT id, i4, i8, nr, dr FROM sqlreg.rg WHERE id <= 5 ORDER BY id;
SELECT id, ts, tz FROM sqlreg.rg WHERE id <= 5 ORDER BY id;

-- range operators run on the cold rows
SELECT count(*) AS overlap_i4 FROM sqlreg.rg WHERE i4 && int4range(20, 25);
SELECT count(*) AS overlap_i4_ctl FROM sqlreg.rg_ctl WHERE i4 && int4range(20, 25);
SELECT count(*) AS contains_date FROM sqlreg.rg WHERE dr @> date '2024-03-01';
SELECT count(*) AS contains_date_ctl FROM sqlreg.rg_ctl WHERE dr @> date '2024-03-01';
SELECT count(*) AS empties FROM sqlreg.rg WHERE isempty(i4);
SELECT id, lower(i4), upper(i4), upper_inf(i4) FROM sqlreg.rg WHERE id IN (1, 3);

-- reading under another TimeZone and DateStyle gives the same instants and dates
SET TimeZone = 'Asia/Tokyo';
SET DateStyle = 'German, DMY';
SELECT id, lower(tz) AS tz_lo, lower(ts) AS ts_lo, lower(dr) AS dr_lo FROM sqlreg.rg WHERE id IN (1, 6) ORDER BY id;
SELECT id, lower(tz) AS tz_lo_ctl, lower(ts) AS ts_lo_ctl, lower(dr) AS dr_lo_ctl FROM sqlreg.rg_ctl WHERE id IN (1, 6) ORDER BY id;
RESET DateStyle;
SET TimeZone = 'UTC';

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.rg SET tag = 'changed' WHERE id IN (1, 2, 3, 5);
UPDATE sqlreg.rg_ctl SET tag = 'changed' WHERE id IN (1, 2, 3, 5);
UPDATE sqlreg.rg SET i4 = int4range(1, 100), dr = 'empty' WHERE id = 4;
UPDATE sqlreg.rg_ctl SET i4 = int4range(1, 100), dr = 'empty' WHERE id = 4;
DELETE FROM sqlreg.rg WHERE id = 6;
DELETE FROM sqlreg.rg_ctl WHERE id = 6;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.rg', 'id') = sqlreg.fingerprint('sqlreg.rg_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.rg'::regclass, true) IS NOT NULL AS rg_reflushed;
SELECT sqlreg.fingerprint('sqlreg.rg', 'id') = sqlreg.fingerprint('sqlreg.rg_ctl', 'id') AS rows_match_after_reflush;
SELECT koldstore.unmanage_table('sqlreg.rg'::regclass, true, true) IS NOT NULL AS rg_unmanaged;
SELECT sqlreg.fingerprint('sqlreg.rg', 'id') = sqlreg.fingerprint('sqlreg.rg_ctl', 'id') AS rows_match_after_unmanage;

-- user-defined range types, multiranges and range arrays are still refused
CREATE TYPE sqlreg.floatrange AS RANGE (subtype = float8);
CREATE TABLE sqlreg.rg_udt (id bigint PRIMARY KEY, r sqlreg.floatrange);
SELECT koldstore.manage_table(table_name => 'sqlreg.rg_udt'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false) IS NOT NULL AS udt_managed;
CREATE TABLE sqlreg.rg_multi (id bigint PRIMARY KEY, r int4multirange);
SELECT koldstore.manage_table(table_name => 'sqlreg.rg_multi'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false) IS NOT NULL AS multi_managed;
