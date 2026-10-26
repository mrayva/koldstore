-- xml is stored as its text and read back exactly, content fragments and documents alike.

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

CREATE TABLE sqlreg.xm (id bigint PRIMARY KEY, doc xml, tag text);
INSERT INTO sqlreg.xm VALUES
  (1, '<a x="1">hi</a>', 'a'),
  (2, '<?xml version="1.0"?><root><k>v &amp; w</k></root>', 'b'),
  (3, 'text <b/> more <c>d</c>', 'fragment'),
  (4, NULL, 'null'),
  (5, '<e>é ü 日本</e>', 'unicode'),
  (6, '<!-- c --><p>  spaced  </p>', 'comment');
INSERT INTO sqlreg.xm SELECT g, xmlelement(name row, xmlattributes(g AS n), 'v' || g), 'bulk' FROM generate_series(7, 60) g;
CREATE TABLE sqlreg.xm_ctl AS SELECT * FROM sqlreg.xm;
SELECT sqlreg.fingerprint('sqlreg.xm', 'id') AS before_flush \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.xm'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS xm_managed;
SELECT sqlreg.flush_table('sqlreg.xm'::regclass) IS NOT NULL AS xm_flushed;
SELECT sqlreg.fingerprint('sqlreg.xm', 'id') = :'before_flush' AS same_after_flush;
SELECT a.id FROM sqlreg.xm a JOIN sqlreg.xm_ctl b USING (id) WHERE a.doc::text IS DISTINCT FROM b.doc::text ORDER BY a.id;
SELECT id, doc::text FROM sqlreg.xm WHERE id <= 6 ORDER BY id;
SELECT id, (xpath('/a/@x', doc))[1]::text AS x FROM sqlreg.xm WHERE id = 1;
SELECT count(*) AS with_doc FROM sqlreg.xm WHERE doc IS NOT NULL;

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.xm SET tag = 'changed' WHERE id IN (1, 2, 3, 5, 6);
UPDATE sqlreg.xm_ctl SET tag = 'changed' WHERE id IN (1, 2, 3, 5, 6);
UPDATE sqlreg.xm SET doc = '<new/>' WHERE id = 4;
UPDATE sqlreg.xm_ctl SET doc = '<new/>' WHERE id = 4;
DELETE FROM sqlreg.xm WHERE id = 7;
DELETE FROM sqlreg.xm_ctl WHERE id = 7;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.xm', 'id') = sqlreg.fingerprint('sqlreg.xm_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.xm'::regclass, true) IS NOT NULL AS xm_reflushed;
SELECT sqlreg.fingerprint('sqlreg.xm', 'id') = sqlreg.fingerprint('sqlreg.xm_ctl', 'id') AS rows_match_after_reflush;
SELECT koldstore.unmanage_table('sqlreg.xm'::regclass, true, true) IS NOT NULL AS xm_unmanaged;
SELECT sqlreg.fingerprint('sqlreg.xm', 'id') = sqlreg.fingerprint('sqlreg.xm_ctl', 'id') AS rows_match_after_unmanage;
