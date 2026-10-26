-- smallint/int/bigint/real/double/numeric/uuid/boolean arrays are carried as their text form: they flush to
-- cold, read back identically (NULL elements, empty and multi-dimensional arrays, custom lower bounds), and
-- hydrate-on-write and unmanage. Every answer is compared with an unmanaged copy.

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

CREATE TABLE sqlreg.ar (
  id bigint PRIMARY KEY,
  a2 smallint[], a4 integer[], a8 bigint[], r4 real[], r8 double precision[], n numeric(10,2)[], u uuid[], b boolean[],
  note text
);
INSERT INTO sqlreg.ar
SELECT g,
       ARRAY[g % 100, NULL, 7]::smallint[],
       ARRAY[g, g * 2, g * 3],
       ARRAY[g::bigint * 10000000000, -g],
       ARRAY[g / 8.0, 0.1]::real[],
       ARRAY[g / 3.0, 1e300, -0.0]::double precision[],
       ARRAY[g / 7.0, 0.5]::numeric(10,2)[],
       ARRAY[md5(g::text)::uuid, '00000000-0000-0000-0000-000000000000'],
       ARRAY[g % 2 = 0, NULL, true],
       'n' || g
FROM generate_series(1, 60) g;
INSERT INTO sqlreg.ar (id, a2, a4, a8, r4, r8, n, u, b, note) VALUES
  (101, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', 'empty'),
  (102, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'nulls'),
  (103, '{{1,2},{3,4}}', '{{1,2,3},{4,5,6}}', '{{{1}},{{2}}}', '{{1.5}}', '{{2.5,3.5}}', '{{1.25}}', NULL, '{{t,f}}', 'multidim'),
  (104, '[0:2]={1,2,3}', '[-1:1]={4,5,6}', NULL, NULL, '[5:5]={9.5}', NULL, NULL, NULL, 'bounds'),
  (105, NULL, NULL, NULL, '{NaN,Infinity,-Infinity}', '{NaN,Infinity,-Infinity}', '{NaN}', NULL, NULL, 'special');
CREATE TABLE sqlreg.ar_ctl AS SELECT * FROM sqlreg.ar;
SELECT sqlreg.fingerprint('sqlreg.ar', 'id') AS before_flush \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.ar'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS ar_managed;
SELECT sqlreg.flush_table('sqlreg.ar'::regclass) IS NOT NULL AS ar_flushed;
SELECT (koldstore.table_status('sqlreg.ar'::regclass) ->> 'cold_row_count')::int >= 50 AS mostly_cold;
SELECT sqlreg.fingerprint('sqlreg.ar', 'id') = :'before_flush' AS rows_identical_after_flush;
SELECT pg_typeof(a2)::text, pg_typeof(a4)::text, pg_typeof(a8)::text, pg_typeof(r4)::text, pg_typeof(r8)::text,
       pg_typeof(n)::text, pg_typeof(u)::text, pg_typeof(b)::text FROM sqlreg.ar WHERE id = 2;
SELECT id, a2, a4, r8, b FROM sqlreg.ar WHERE id IN (101, 102, 103, 104, 105) ORDER BY id;

SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.ar WHERE 7 = ANY (a4))
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.ar_ctl WHERE 7 = ANY (a4)) AS any_match;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.ar WHERE a4 @> ARRAY[20])
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.ar_ctl WHERE a4 @> ARRAY[20]) AS contains;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.ar WHERE a8 && ARRAY[-5::bigint])
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.ar_ctl WHERE a8 && ARRAY[-5::bigint]) AS overlaps;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.ar WHERE array_length(a4, 1) = 3 AND b[1])
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.ar_ctl WHERE array_length(a4, 1) = 3 AND b[1]) AS length_and_elem;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.ar WHERE cardinality(a2) = 0 OR a2 IS NULL)
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.ar_ctl WHERE cardinality(a2) = 0 OR a2 IS NULL) AS empty_or_null;

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.ar SET note = 'changed' WHERE id IN (3, 40, 101, 102, 103, 104, 105);
UPDATE sqlreg.ar_ctl SET note = 'changed' WHERE id IN (3, 40, 101, 102, 103, 104, 105);
UPDATE sqlreg.ar SET a4 = ARRAY[9, 9], n = ARRAY[1.5, NULL]::numeric(10,2)[] WHERE id IN (7, 41);
UPDATE sqlreg.ar_ctl SET a4 = ARRAY[9, 9], n = ARRAY[1.5, NULL]::numeric(10,2)[] WHERE id IN (7, 41);
DELETE FROM sqlreg.ar WHERE id = 9;
DELETE FROM sqlreg.ar_ctl WHERE id = 9;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.ar', 'id') = sqlreg.fingerprint('sqlreg.ar_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.ar'::regclass, true) IS NOT NULL AS ar_reflushed;
SELECT sqlreg.fingerprint('sqlreg.ar', 'id') = sqlreg.fingerprint('sqlreg.ar_ctl', 'id') AS rows_match_after_reflush;

SELECT koldstore.unmanage_table('sqlreg.ar'::regclass, true, true) IS NOT NULL AS ar_unmanaged;
SELECT sqlreg.fingerprint('sqlreg.ar', 'id') = sqlreg.fingerprint('sqlreg.ar_ctl', 'id') AS rows_match_after_unmanage;

-- element types whose text form depends on a session setting stay unsupported
CREATE TABLE sqlreg.ar_ts (id bigint PRIMARY KEY, t timestamptz[]);
SELECT koldstore.manage_table(table_name => 'sqlreg.ar_ts'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, migration_order_by => 'id', auto_flush => false);
