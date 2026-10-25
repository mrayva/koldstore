-- pgvector `vector(n)` columns: flush to cold, read back, hydrate-on-write, delete, re-flush, and
-- nearest-neighbour ordering over hot + cold rows against an unmanaged copy.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE EXTENSION IF NOT EXISTS vector;

CREATE FUNCTION sqlreg.fingerprint(rel regclass, ord text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE result text;
BEGIN
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', ord, rel) INTO result;
  RETURN result;
END
$$;

CREATE TABLE sqlreg.vec (id bigint PRIMARY KEY, emb vector(3), note text NOT NULL DEFAULT 'x');
INSERT INTO sqlreg.vec (id, emb)
SELECT g, ARRAY[g % 7, (g * 3) % 11, (g * 5) % 13 + 0.5]::real[]::vector FROM generate_series(1, 60) g;
INSERT INTO sqlreg.vec (id, emb) VALUES (101, NULL), (102, '[0,0,0]'), (103, '[-1.5,2.25,1e3]');
CREATE TABLE sqlreg.vec_ctl AS SELECT * FROM sqlreg.vec;
SELECT sqlreg.fingerprint('sqlreg.vec', 'id') AS before_flush \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.vec'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS vec_managed;
SELECT sqlreg.flush_table('sqlreg.vec'::regclass) IS NOT NULL AS vec_flushed;
SELECT (koldstore.table_status('sqlreg.vec'::regclass) ->> 'cold_row_count')::int >= 50 AS mostly_cold;
SELECT sqlreg.fingerprint('sqlreg.vec', 'id') = :'before_flush' AS rows_identical_after_flush;

SELECT id, emb FROM sqlreg.vec WHERE id IN (2, 102, 103) ORDER BY id;

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.vec SET emb = '[9,9,9]' WHERE id IN (3, 40);
UPDATE sqlreg.vec_ctl SET emb = '[9,9,9]' WHERE id IN (3, 40);
UPDATE sqlreg.vec SET note = 'changed' WHERE id = 7;
UPDATE sqlreg.vec_ctl SET note = 'changed' WHERE id = 7;
DELETE FROM sqlreg.vec WHERE id = 102;
DELETE FROM sqlreg.vec_ctl WHERE id = 102;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.vec', 'id') = sqlreg.fingerprint('sqlreg.vec_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.vec'::regclass, true) IS NOT NULL AS vec_reflushed;
SELECT sqlreg.fingerprint('sqlreg.vec', 'id') = sqlreg.fingerprint('sqlreg.vec_ctl', 'id') AS rows_match_after_reflush;
SELECT count(*) AS rows_after FROM sqlreg.vec;

-- vector predicates and distance ordering see cold rows
SELECT (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.vec ORDER BY emb <-> '[1,2,3]', id LIMIT 8) a)
     = (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.vec_ctl ORDER BY emb <-> '[1,2,3]', id LIMIT 8) b) AS knn_l2_matches;
SELECT (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.vec WHERE emb IS NOT NULL ORDER BY emb <=> '[1,2,3]', id LIMIT 8) a)
     = (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.vec_ctl WHERE emb IS NOT NULL ORDER BY emb <=> '[1,2,3]', id LIMIT 8) b) AS knn_cosine_matches;
SELECT count(*) AS equal_matches FROM sqlreg.vec WHERE emb = '[9,9,9]';
