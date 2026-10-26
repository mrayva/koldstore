-- interval is stored as ISO 8601 text, so values are exact and independent of IntervalStyle at flush time.

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

CREATE TABLE sqlreg.iv (id bigint PRIMARY KEY, span interval, ms interval(3), ym interval year to month, tag text);
INSERT INTO sqlreg.iv VALUES
  (1, '1 day', '1.2345 seconds', '1 year 2 months', 'day'),
  (2, '24 hours', '00:00:01', '14 months', 'hours'),
  (3, '30 days', NULL, '0 months', 'thirty'),
  (4, '1 mon', '-0.001 seconds', '-1 years', 'mon'),
  (5, '-1 day +02:00:00', '1 hour', '2 years 6 months', 'mixed'),
  (6, '1 year 2 mons 3 days 04:05:06.789012', '10 minutes', NULL, 'full'),
  (7, '0', '0', '0', 'zero'),
  (8, NULL, NULL, NULL, 'nulls'),
  (9, 'infinity', NULL, NULL, 'inf'),
  (10, '-infinity', NULL, NULL, 'ninf'),
  (11, '-7 mons -8 days -09:10:11.121314', '2 days', '5 years', 'neg');
INSERT INTO sqlreg.iv SELECT g, g * interval '1 hour 7 minutes', NULL, NULL, 'bulk' FROM generate_series(12, 60) g;
CREATE TABLE sqlreg.iv_ctl AS SELECT * FROM sqlreg.iv;
SELECT sqlreg.fingerprint('sqlreg.iv', 'id') AS before_flush \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.iv'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS iv_managed;
-- flushed under a different IntervalStyle than the one it is read back with
SET intervalstyle = 'sql_standard';
SELECT sqlreg.flush_table('sqlreg.iv'::regclass) IS NOT NULL AS iv_flushed;
RESET intervalstyle;
SELECT sqlreg.fingerprint('sqlreg.iv', 'id') = :'before_flush' AS same_after_flush;
SELECT a.id, a.span::text, b.span::text AS expected FROM sqlreg.iv a JOIN sqlreg.iv_ctl b USING (id)
WHERE a.span::text IS DISTINCT FROM b.span::text OR a.ms::text IS DISTINCT FROM b.ms::text OR a.ym::text IS DISTINCT FROM b.ym::text
ORDER BY a.id;
-- the same cold rows under another style
SET intervalstyle = 'iso_8601';
SELECT sqlreg.fingerprint('sqlreg.iv', 'id') = sqlreg.fingerprint('sqlreg.iv_ctl', 'id') AS same_under_iso;
RESET intervalstyle;

-- predicates evaluate over the merged rows
SELECT count(*) AS eq_day FROM sqlreg.iv WHERE span = interval '1 day';
SELECT count(*) AS eq_day_ctl FROM sqlreg.iv_ctl WHERE span = interval '1 day';
SELECT count(*) AS lt_mon FROM sqlreg.iv WHERE span < interval '1 mon';
SELECT count(*) AS lt_mon_ctl FROM sqlreg.iv_ctl WHERE span < interval '1 mon';
SELECT count(*) AS null_span FROM sqlreg.iv WHERE span IS NULL;

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.iv SET tag = 'changed' WHERE id IN (1, 2, 5, 9, 10, 11);
UPDATE sqlreg.iv_ctl SET tag = 'changed' WHERE id IN (1, 2, 5, 9, 10, 11);
DELETE FROM sqlreg.iv WHERE id = 6;
DELETE FROM sqlreg.iv_ctl WHERE id = 6;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.iv', 'id') = sqlreg.fingerprint('sqlreg.iv_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.iv'::regclass, true) IS NOT NULL AS iv_reflushed;
SELECT sqlreg.fingerprint('sqlreg.iv', 'id') = sqlreg.fingerprint('sqlreg.iv_ctl', 'id') AS rows_match_after_reflush;

SELECT koldstore.unmanage_table('sqlreg.iv'::regclass, true, true) IS NOT NULL AS iv_unmanaged;
SELECT sqlreg.fingerprint('sqlreg.iv', 'id') = sqlreg.fingerprint('sqlreg.iv_ctl', 'id') AS rows_match_after_unmanage;

-- an interval cannot be the migration-order column
CREATE TABLE sqlreg.iv_ord (id bigint PRIMARY KEY, span interval NOT NULL);
SELECT koldstore.manage_table(
  table_name => 'sqlreg.iv_ord'::regclass, storage => 'sqlreg_fs', hot_row_limit => 1,
  min_flush_rows => 1, migration_order_by => 'span', auto_flush => false
) IS NOT NULL AS interval_order_managed;
