-- bit(n), bit varying, macaddr8, name and oid are stored in text form and read back exactly.

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

CREATE TABLE sqlreg.bno (id bigint PRIMARY KEY, b bit(8), vb bit varying(70), m8 macaddr8, nm name, o oid, tag text);
INSERT INTO sqlreg.bno VALUES
  (1, B'00000001', B'1', '08:00:2b:01:02:03:04:05', 'alpha', 0, 'a'),
  (2, B'11111111', B'', 'ff:ff:ff:ff:ff:ff:ff:ff', 'beta', 4294967295, 'b'),
  (3, B'10101010', B'0000', '00:00:00:00:00:00:00:00', repeat('n', 63), 16384, 'c'),
  (4, NULL, NULL, NULL, NULL, NULL, 'nulls'),
  (5, B'00000000', repeat('10', 35)::bit varying, '0a:1b:2c:3d:4e:5f:60:71', 'with space "q"', 1, 'wide');
INSERT INTO sqlreg.bno SELECT g, (g % 256)::bit(8), (g::bit(16))::varbit, ('00:00:00:00:00:00:00:' || lpad(to_hex(g % 256), 2, '0'))::macaddr8,
  'row' || g, g, 'bulk' FROM generate_series(6, 60) g;
CREATE TABLE sqlreg.bno_ctl AS SELECT * FROM sqlreg.bno;
SELECT sqlreg.fingerprint('sqlreg.bno', 'id') AS before_flush \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.bno'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS bno_managed;
SELECT sqlreg.flush_table('sqlreg.bno'::regclass) IS NOT NULL AS bno_flushed;
SELECT sqlreg.fingerprint('sqlreg.bno', 'id') = :'before_flush' AS same_after_flush;
SELECT a.id FROM sqlreg.bno a JOIN sqlreg.bno_ctl b USING (id)
WHERE a.b::text IS DISTINCT FROM b.b::text OR a.vb::text IS DISTINCT FROM b.vb::text OR a.m8::text IS DISTINCT FROM b.m8::text
   OR a.nm::text IS DISTINCT FROM b.nm::text OR a.o::text IS DISTINCT FROM b.o::text
ORDER BY a.id;
SELECT id, length(vb) AS vb_len, bit_length(b) AS b_len, length(nm) AS nm_len FROM sqlreg.bno WHERE id IN (1, 2, 3, 5) ORDER BY id;

SELECT count(*) AS b_eq FROM sqlreg.bno WHERE b = B'10101010';
SELECT count(*) AS b_eq_ctl FROM sqlreg.bno_ctl WHERE b = B'10101010';
SELECT count(*) AS o_big FROM sqlreg.bno WHERE o >= 4294967295;
SELECT count(*) AS o_big_ctl FROM sqlreg.bno_ctl WHERE o >= 4294967295;
SELECT id FROM sqlreg.bno WHERE nm = 'beta';
SELECT count(*) AS m8_eq FROM sqlreg.bno WHERE m8 = '08:00:2b:01:02:03:04:05';

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.bno SET tag = 'changed' WHERE id IN (1, 2, 3, 5);
UPDATE sqlreg.bno_ctl SET tag = 'changed' WHERE id IN (1, 2, 3, 5);
DELETE FROM sqlreg.bno WHERE id = 4;
DELETE FROM sqlreg.bno_ctl WHERE id = 4;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.bno', 'id') = sqlreg.fingerprint('sqlreg.bno_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.bno'::regclass, true) IS NOT NULL AS bno_reflushed;
SELECT sqlreg.fingerprint('sqlreg.bno', 'id') = sqlreg.fingerprint('sqlreg.bno_ctl', 'id') AS rows_match_after_reflush;
SELECT koldstore.unmanage_table('sqlreg.bno'::regclass, true, true) IS NOT NULL AS bno_unmanaged;
SELECT sqlreg.fingerprint('sqlreg.bno', 'id') = sqlreg.fingerprint('sqlreg.bno_ctl', 'id') AS rows_match_after_unmanage;
