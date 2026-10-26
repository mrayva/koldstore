-- money is stored as its raw int64, so cold rows do not depend on lc_monetary.

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

CREATE TABLE sqlreg.mo (id bigint PRIMARY KEY, amt money, tag text);
INSERT INTO sqlreg.mo VALUES
  (1, '0.00', 'zero'), (2, '12345.67', 'a'), (3, '-0.01', 'neg'), (4, NULL, 'null'),
  (5, '92233720368547758.07', 'max'), (6, '-92233720368547758.08', 'min'), (7, '1000000.00', 'm');
INSERT INTO sqlreg.mo SELECT g, (g * 37 % 1000 + 0.25)::numeric::money, 'bulk' FROM generate_series(8, 60) g;
CREATE TABLE sqlreg.mo_ctl AS SELECT * FROM sqlreg.mo;
SELECT sqlreg.fingerprint('sqlreg.mo', 'id') AS before_flush \gset
SELECT sum(amt::numeric) AS total_before FROM sqlreg.mo WHERE amt < '92233720368547758.07'::money AND amt > '-92233720368547758.08'::money \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.mo'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS mo_managed;
SELECT sqlreg.flush_table('sqlreg.mo'::regclass) IS NOT NULL AS mo_flushed;
SELECT sqlreg.fingerprint('sqlreg.mo', 'id') = :'before_flush' AS same_after_flush;
SELECT a.id FROM sqlreg.mo a JOIN sqlreg.mo_ctl b USING (id) WHERE a.amt IS DISTINCT FROM b.amt ORDER BY a.id;
SELECT id, amt::numeric AS amt FROM sqlreg.mo WHERE id <= 7 ORDER BY id;
SELECT count(*) AS big FROM sqlreg.mo WHERE amt > '500.00'::money;
SELECT count(*) AS big_ctl FROM sqlreg.mo_ctl WHERE amt > '500.00'::money;
SELECT sum(amt::numeric) = :total_before AS sum_matches FROM sqlreg.mo WHERE amt < '92233720368547758.07'::money AND amt > '-92233720368547758.08'::money;

-- reading under a different lc_monetary gives the same values
SET lc_monetary = 'C';
SELECT sum(amt::numeric) = :total_before AS sum_matches_under_c FROM sqlreg.mo WHERE amt < '92233720368547758.07'::money AND amt > '-92233720368547758.08'::money;
SELECT amt::numeric AS two FROM sqlreg.mo WHERE id = 2;
RESET lc_monetary;

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.mo SET tag = 'changed' WHERE id IN (1, 2, 3, 5, 6);
UPDATE sqlreg.mo_ctl SET tag = 'changed' WHERE id IN (1, 2, 3, 5, 6);
UPDATE sqlreg.mo SET amt = amt + '1.00'::money WHERE id = 7;
UPDATE sqlreg.mo_ctl SET amt = amt + '1.00'::money WHERE id = 7;
DELETE FROM sqlreg.mo WHERE id = 8;
DELETE FROM sqlreg.mo_ctl WHERE id = 8;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.mo', 'id') = sqlreg.fingerprint('sqlreg.mo_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.mo'::regclass, true) IS NOT NULL AS mo_reflushed;
SELECT sqlreg.fingerprint('sqlreg.mo', 'id') = sqlreg.fingerprint('sqlreg.mo_ctl', 'id') AS rows_match_after_reflush;
SELECT koldstore.unmanage_table('sqlreg.mo'::regclass, true, true) IS NOT NULL AS mo_unmanaged;
SELECT sqlreg.fingerprint('sqlreg.mo', 'id') = sqlreg.fingerprint('sqlreg.mo_ctl', 'id') AS rows_match_after_unmanage;
