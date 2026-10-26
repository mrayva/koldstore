-- The built-in geometric types are stored as their text form and read back bit-exactly.

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

CREATE TABLE sqlreg.geo (id bigint PRIMARY KEY, pt point, ls lseg, pa path, bx box, pg polygon, ln line, ci circle, tag text);
INSERT INTO sqlreg.geo VALUES
  (1, '(1,2)', '[(0,0),(3,4)]', '[(0,0),(1,1),(2,0)]', '((0,0),(2,2))', '((0,0),(4,0),(4,3))', '{1,-1,0}', '<(1,1),2.5>', 'a'),
  (2, '(0.1,0.2)', '[(0.1,0.2),(0.30000000000000004,1e-9)]', '((0,0),(1,1),(2,0))', '((-1.5,-1.5),(1e10,1e10))', '((0.1,0.1),(0.2,0.7),(0.9,0.3))', '{0.1,0.2,0.3}', '<(0.1,0.2),0.30000000000000004>', 'floats'),
  (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'nulls'),
  (4, '(-0,Infinity)', '[(-Infinity,0),(NaN,1)]', '[(1,1)]', '((0,0),(0,0))', '((0,0))', '{0,1,1}', '<(0,0),0>', 'odd');
INSERT INTO sqlreg.geo SELECT g, point(g / 3.0, g * 1.1), lseg(point(g, g), point(g / 7.0, 2)),
  path(polygon(box(point(g, g), point(g + 1.5, g + 2.5)))), box(point(g, g), point(g / 3.0, g / 9.0)),
  polygon(circle(point(g, g / 3.0), 1 + g / 10.0)), line(point(g, 0), point(0, g + 1)), circle(point(g / 3.0, g), g / 7.0), 'bulk'
FROM generate_series(5, 60) g;
CREATE TABLE sqlreg.geo_ctl AS SELECT * FROM sqlreg.geo;
SELECT sqlreg.fingerprint('sqlreg.geo', 'id') AS before_flush \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.geo'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS geo_managed;
-- flush under a low extra_float_digits: the stored text must not lose digits
SET extra_float_digits = -3;
SELECT sqlreg.flush_table('sqlreg.geo'::regclass) IS NOT NULL AS geo_flushed;
RESET extra_float_digits;
SELECT sqlreg.fingerprint('sqlreg.geo', 'id') = :'before_flush' AS same_after_flush;
SELECT a.id FROM sqlreg.geo a JOIN sqlreg.geo_ctl b USING (id)
WHERE a.pt::text IS DISTINCT FROM b.pt::text OR a.ls::text IS DISTINCT FROM b.ls::text
   OR a.pa::text IS DISTINCT FROM b.pa::text OR a.bx::text IS DISTINCT FROM b.bx::text
   OR a.pg::text IS DISTINCT FROM b.pg::text OR a.ln::text IS DISTINCT FROM b.ln::text
   OR a.ci::text IS DISTINCT FROM b.ci::text ORDER BY a.id;
SELECT id, pt, ls, pa, bx FROM sqlreg.geo WHERE id <= 4 ORDER BY id;
SELECT id, pg, ln, ci FROM sqlreg.geo WHERE id <= 4 ORDER BY id;

-- geometric operators run on the cold rows
SELECT count(*) AS inside_box FROM sqlreg.geo WHERE pt <@ box '((0,0),(10,10))';
SELECT count(*) AS inside_box_ctl FROM sqlreg.geo_ctl WHERE pt <@ box '((0,0),(10,10))';
SELECT count(*) AS circle_overlap FROM sqlreg.geo WHERE ci && circle '<(5,5),3>';
SELECT count(*) AS circle_overlap_ctl FROM sqlreg.geo_ctl WHERE ci && circle '<(5,5),3>';

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.geo SET tag = 'changed' WHERE id IN (1, 2, 3, 4);
UPDATE sqlreg.geo_ctl SET tag = 'changed' WHERE id IN (1, 2, 3, 4);
UPDATE sqlreg.geo SET pt = '(9,9)', bx = '((1,1),(2,2))' WHERE id = 3;
UPDATE sqlreg.geo_ctl SET pt = '(9,9)', bx = '((1,1),(2,2))' WHERE id = 3;
DELETE FROM sqlreg.geo WHERE id = 6;
DELETE FROM sqlreg.geo_ctl WHERE id = 6;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.geo', 'id') = sqlreg.fingerprint('sqlreg.geo_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.geo'::regclass, true) IS NOT NULL AS geo_reflushed;
SELECT sqlreg.fingerprint('sqlreg.geo', 'id') = sqlreg.fingerprint('sqlreg.geo_ctl', 'id') AS rows_match_after_reflush;
SELECT koldstore.unmanage_table('sqlreg.geo'::regclass, true, true) IS NOT NULL AS geo_unmanaged;
SELECT sqlreg.fingerprint('sqlreg.geo', 'id') = sqlreg.fingerprint('sqlreg.geo_ctl', 'id') AS rows_match_after_unmanage;

-- composite types and geometric arrays are still refused
CREATE TYPE sqlreg.pair AS (a int, b int);
CREATE TABLE sqlreg.geo_comp (id bigint PRIMARY KEY, c sqlreg.pair);
SELECT koldstore.manage_table(table_name => 'sqlreg.geo_comp'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false) IS NOT NULL AS comp_managed;
CREATE TABLE sqlreg.geo_arr (id bigint PRIMARY KEY, p point[]);
SELECT koldstore.manage_table(table_name => 'sqlreg.geo_arr'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false) IS NOT NULL AS arr_managed;
