-- STORED generated columns: flush to cold, read back, hydrate-on-write, full-text over a generated
-- tsvector, and VIRTUAL generated columns still rejected.

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

CREATE TABLE sqlreg.gdoc (
  id bigint PRIMARY KEY,
  body text,
  n int NOT NULL DEFAULT 1,
  tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', coalesce(body, ''))) STORED,
  n2 int GENERATED ALWAYS AS (n * 2) STORED
);
INSERT INTO sqlreg.gdoc (id, body, n)
SELECT g, format('alpha%s beta%s fox %s', g % 7, g % 5, CASE WHEN g % 9 = 0 THEN 'jumps' ELSE 'sleeps' END), g
FROM generate_series(1, 60) g;
INSERT INTO sqlreg.gdoc (id, body, n) VALUES (101, NULL, 5);
CREATE TABLE sqlreg.gdoc_ctl (LIKE sqlreg.gdoc INCLUDING GENERATED INCLUDING DEFAULTS);
INSERT INTO sqlreg.gdoc_ctl (id, body, n) SELECT id, body, n FROM sqlreg.gdoc;
SELECT sqlreg.fingerprint('sqlreg.gdoc', 'id') = sqlreg.fingerprint('sqlreg.gdoc_ctl', 'id') AS same_before;

SELECT koldstore.manage_table(
  table_name => 'sqlreg.gdoc'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS gdoc_managed;
SELECT sqlreg.flush_table('sqlreg.gdoc'::regclass) IS NOT NULL AS gdoc_flushed;
SELECT (koldstore.table_status('sqlreg.gdoc'::regclass) ->> 'cold_row_count')::int >= 50 AS mostly_cold;
SELECT sqlreg.fingerprint('sqlreg.gdoc', 'id') = sqlreg.fingerprint('sqlreg.gdoc_ctl', 'id') AS same_after_flush;

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.gdoc SET body = 'replaced text', n = 77 WHERE id IN (3, 40);
UPDATE sqlreg.gdoc_ctl SET body = 'replaced text', n = 77 WHERE id IN (3, 40);
UPDATE sqlreg.gdoc SET n = n + 1 WHERE id = 7;
UPDATE sqlreg.gdoc_ctl SET n = n + 1 WHERE id = 7;
DELETE FROM sqlreg.gdoc WHERE id = 9;
DELETE FROM sqlreg.gdoc_ctl WHERE id = 9;
INSERT INTO sqlreg.gdoc (id, body, n) VALUES (200, 'fresh row', 3);
INSERT INTO sqlreg.gdoc_ctl (id, body, n) VALUES (200, 'fresh row', 3);
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.gdoc', 'id') = sqlreg.fingerprint('sqlreg.gdoc_ctl', 'id') AS same_after_dml;
SELECT sqlreg.flush_table('sqlreg.gdoc'::regclass, true) IS NOT NULL AS gdoc_reflushed;
SELECT sqlreg.fingerprint('sqlreg.gdoc', 'id') = sqlreg.fingerprint('sqlreg.gdoc_ctl', 'id') AS same_after_reflush;

SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.gdoc WHERE tsv @@ to_tsquery('simple', 'fox & jumps'))
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.gdoc_ctl WHERE tsv @@ to_tsquery('simple', 'fox & jumps')) AS fts_matches;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.gdoc WHERE n2 > 100)
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.gdoc_ctl WHERE n2 > 100) AS int_matches;

-- a direct write to a generated column stays an error
UPDATE sqlreg.gdoc SET n2 = 1 WHERE id = 1;

-- VIRTUAL generated columns have no storage and stay unsupported
CREATE TABLE sqlreg.gvirt (id bigint PRIMARY KEY, a int, b int GENERATED ALWAYS AS (a + 1) VIRTUAL);
SELECT koldstore.manage_table(table_name => 'sqlreg.gvirt'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, migration_order_by => 'id', auto_flush => false);

-- unmanaging restores every row (cold ones included) with generated values recomputed
SELECT koldstore.unmanage_table('sqlreg.gdoc'::regclass, true, true) IS NOT NULL AS gdoc_unmanaged;
SELECT sqlreg.fingerprint('sqlreg.gdoc', 'id') = sqlreg.fingerprint('sqlreg.gdoc_ctl', 'id') AS same_after_unmanage;
