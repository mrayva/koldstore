-- date[], timestamp[], timestamptz[] and interval[] are stored as ISO text, so values are exact whatever
-- DateStyle, TimeZone and IntervalStyle the flushing session used.

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

SET timezone = 'UTC';
SET datestyle = 'ISO, MDY';
CREATE TABLE sqlreg.ta (id bigint PRIMARY KEY, d date[], ts timestamp[], tz timestamptz[], iv interval[], tag text);
INSERT INTO sqlreg.ta VALUES
  (1, '{2026-01-31,2026-02-01}', '{"2026-01-31 23:59:59.999999","2026-02-01 00:00:00"}',
      '{"2026-01-31 23:59:59.999999+00","2026-06-01 12:00:00+05:30"}', '{"1 day","24 hours","30 days"}', 'a'),
  (2, '{}', '{}', '{}', '{}', 'empty'),
  (3, NULL, NULL, NULL, NULL, 'null'),
  (4, '{2026-03-01,NULL,2026-03-03}', '{NULL,"2026-03-03 03:03:03"}', '{"2026-03-03 03:03:03-08",NULL}', '{NULL,"1 mon"}', 'nulls'),
  (5, '{{2026-01-01,2026-01-02},{2026-01-03,2026-01-04}}', '{{"2026-01-01 00:00:00"},{"2026-01-02 00:00:00"}}',
      '{{"2026-01-01 00:00:00+00"},{"2026-01-02 00:00:00+00"}}', '{{"1 day"},{"2 days"}}', 'multi'),
  (6, '[0:1]={2026-05-05,2026-05-06}', '[3:3]={"2026-05-05 05:05:05"}', '[-1:0]={"2026-05-05 05:05:05+00","2026-05-06 05:05:05+00"}', '[2:2]={"3 days"}', 'bounds'),
  (7, '{infinity,-infinity,0044-03-15 BC}', '{infinity,-infinity,"0044-03-15 00:00:00 BC"}', '{infinity,-infinity,"0044-03-15 00:00:00+00 BC"}',
      '{infinity,-infinity,"-1 year -2 mons -3 days -04:05:06.789"}', 'special'),
  (8, '{2020-02-29}', '{"2020-02-29 12:00:00"}', '{"2020-02-29 12:00:00+00","2020-02-29 12:00:00-12"}', '{"0"}', 'leap');
INSERT INTO sqlreg.ta SELECT g, ARRAY[date '2026-01-01' + g], ARRAY[timestamp '2026-01-01 00:00' + g * interval '1 hour'],
  ARRAY[timestamptz '2026-01-01 00:00+00' + g * interval '1 hour'], ARRAY[g * interval '1 minute'], 'bulk' FROM generate_series(9, 60) g;
CREATE TABLE sqlreg.ta_ctl AS SELECT * FROM sqlreg.ta;
SELECT sqlreg.fingerprint('sqlreg.ta', 'id') AS before_flush \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.ta'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS ta_managed;
-- flush under unusual settings, read back under the originals
SET datestyle = 'German, DMY';
SET timezone = 'Asia/Kolkata';
SET intervalstyle = 'sql_standard';
SELECT sqlreg.flush_table('sqlreg.ta'::regclass) IS NOT NULL AS ta_flushed;
RESET intervalstyle;
SET timezone = 'UTC';
SET datestyle = 'ISO, MDY';
SELECT sqlreg.fingerprint('sqlreg.ta', 'id') = :'before_flush' AS same_after_flush;
SELECT a.id FROM sqlreg.ta a JOIN sqlreg.ta_ctl b USING (id)
WHERE a.d::text IS DISTINCT FROM b.d::text OR a.ts::text IS DISTINCT FROM b.ts::text
   OR a.tz::text IS DISTINCT FROM b.tz::text OR a.iv::text IS DISTINCT FROM b.iv::text
ORDER BY a.id;
SELECT id, array_dims(d) AS d_dims, array_dims(tz) AS tz_dims FROM sqlreg.ta WHERE id IN (5, 6) ORDER BY id;

SELECT count(*) AS d_contains FROM sqlreg.ta WHERE d @> ARRAY[date '2026-02-01'];
SELECT count(*) AS d_contains_ctl FROM sqlreg.ta_ctl WHERE d @> ARRAY[date '2026-02-01'];
SELECT count(*) AS tz_any FROM sqlreg.ta WHERE timestamptz '2026-06-01 06:30:00+00' = ANY (tz);
SELECT count(*) AS tz_any_ctl FROM sqlreg.ta_ctl WHERE timestamptz '2026-06-01 06:30:00+00' = ANY (tz);
SELECT count(*) AS cardinality_eq FROM sqlreg.ta WHERE cardinality(iv) = 3;

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.ta SET tag = 'changed' WHERE id IN (1, 4, 5, 6, 7, 8);
UPDATE sqlreg.ta_ctl SET tag = 'changed' WHERE id IN (1, 4, 5, 6, 7, 8);
DELETE FROM sqlreg.ta WHERE id = 2;
DELETE FROM sqlreg.ta_ctl WHERE id = 2;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.ta', 'id') = sqlreg.fingerprint('sqlreg.ta_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.ta'::regclass, true) IS NOT NULL AS ta_reflushed;
SELECT sqlreg.fingerprint('sqlreg.ta', 'id') = sqlreg.fingerprint('sqlreg.ta_ctl', 'id') AS rows_match_after_reflush;
SELECT koldstore.unmanage_table('sqlreg.ta'::regclass, true, true) IS NOT NULL AS ta_unmanaged;
SELECT sqlreg.fingerprint('sqlreg.ta', 'id') = sqlreg.fingerprint('sqlreg.ta_ctl', 'id') AS rows_match_after_unmanage;
RESET timezone;
RESET datestyle;
