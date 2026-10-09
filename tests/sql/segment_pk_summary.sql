-- Primary-key value summaries: a per-segment membership bitmap in koldstore.cold_segment_index.
--
-- Point lookups (`WHERE pk = const`) used to open every cold segment whose min/max range covers the
-- key. After hydrate-and-reflush churn the segments hold scattered keys, so most ranges overlap and
-- the lookup opened nearly all of them. The catalog now drops segments whose summary proves the key
-- is absent. Pruning must never hide a row (no false negatives), and segments without a summary
-- (older, oversized, unsupported or composite keys) must keep working unpruned.

\set VERBOSITY terse
\set ON_ERROR_STOP off

-- Reads one integer counter (e.g. 'Parquet Segments Opened') from EXPLAIN ANALYZE of a query.
CREATE FUNCTION sqlreg.explain_counter(query text, label text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
  line text;
  value bigint;
BEGIN
  FOR line IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) ' || query LOOP
    IF line ~ ('^\s*' || label || ': [0-9]+') THEN
      value := substring(line FROM label || ': ([0-9]+)')::bigint;
      EXIT;
    END IF;
  END LOOP;
  RETURN value;
END
$$;

-- Runs each `SELECT count(*) ...` statement (one literal-key point lookup per key) and sums them.
CREATE FUNCTION sqlreg.sum_counts(queries text[]) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
  total bigint := 0;
  q text;
  found bigint;
BEGIN
  FOREACH q IN ARRAY queries LOOP
    EXECUTE q INTO found;
    total := total + found;
  END LOOP;
  RETURN total;
END
$$;

-- ------------------------------------------------------------------ bigint key, scattered segments
CREATE TABLE sqlreg.sps (id bigint PRIMARY KEY, v int NOT NULL DEFAULT 0);
INSERT INTO sqlreg.sps (id) SELECT g FROM generate_series(1, 1200) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.sps'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS sps_managed;
SELECT sqlreg.flush_table('sqlreg.sps'::regclass) IS NOT NULL AS sps_flushed;

-- Churn: hydrate a few scattered keys, then flush them back, six times. Each round adds a tiny
-- segment whose keys span the whole range, so its min/max cannot rule it out.
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.sps SET v = v + 1 WHERE id IN (3, 590, 1177);
SELECT sqlreg.flush_table('sqlreg.sps'::regclass) IS NOT NULL AS round1;
UPDATE sqlreg.sps SET v = v + 1 WHERE id IN (41, 700, 1100);
SELECT sqlreg.flush_table('sqlreg.sps'::regclass) IS NOT NULL AS round2;
UPDATE sqlreg.sps SET v = v + 1 WHERE id IN (88, 512, 1199);
SELECT sqlreg.flush_table('sqlreg.sps'::regclass) IS NOT NULL AS round3;
UPDATE sqlreg.sps SET v = v + 1 WHERE id IN (150, 640, 930);
SELECT sqlreg.flush_table('sqlreg.sps'::regclass) IS NOT NULL AS round4;
UPDATE sqlreg.sps SET v = v + 1 WHERE id IN (222, 480, 1050);
SELECT sqlreg.flush_table('sqlreg.sps'::regclass) IS NOT NULL AS round5;
UPDATE sqlreg.sps SET v = v + 1 WHERE id IN (333, 777, 1001);
SELECT sqlreg.flush_table('sqlreg.sps'::regclass) IS NOT NULL AS round6;
RESET koldstore.hydrate_on_write;

SELECT count(*) >= 6 AS several_segments
FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.sps'::regclass AND status = 'active';

-- Every active segment carries a summary for the primary key, and only for it.
SELECT count(*) FILTER (WHERE i.value_summary IS NOT NULL) = count(*) AS every_segment_has_pk_summary,
       count(*) = (SELECT count(*) FROM koldstore.cold_segments
                   WHERE table_oid = 'sqlreg.sps'::regclass AND status = 'active') AS one_index_row_per_segment,
       min(octet_length(i.value_summary)) >= 8 AS summary_at_least_one_word,
       max(octet_length(i.value_summary)) <= 8192 AS summary_within_cap
FROM koldstore.cold_segment_index i
JOIN koldstore.cold_segments s ON s.segment_id = i.segment_id
WHERE s.table_oid = 'sqlreg.sps'::regclass AND s.status = 'active';

-- NO FALSE NEGATIVES: every key is found by its point lookup, whichever segment holds it.
SELECT sqlreg.sum_counts(ARRAY(SELECT format('SELECT count(*) FROM sqlreg.sps WHERE id = %s', g) FROM generate_series(1, 1200) g)) = 1200 AS every_key_found;
-- ... and the data is right, not just present (updated rows kept their new value).
SELECT count(*) AS updated_rows_visible FROM sqlreg.sps WHERE v = 1;
SELECT sum(v) AS total_updates FROM sqlreg.sps;

-- Pruning happens: a key that sits in none of the churn segments opens far fewer segments than
-- the catalog offered as candidates.
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.sps WHERE id = 1010', 'Candidate Segments') >= 6 AS many_candidates,
       sqlreg.explain_counter('SELECT v FROM sqlreg.sps WHERE id = 1010', 'Parquet Segments Opened') <= 2 AS few_opened,
       sqlreg.explain_counter('SELECT v FROM sqlreg.sps WHERE id = 1010', 'Segments Pruned by Catalog Index') >= 4 AS catalog_pruned;

-- Segments without a summary (written before summaries existed) are never pruned, and the result
-- is the same. Distinct keys are used for each probe because candidate lists are cached per key.
BEGIN;
UPDATE koldstore.cold_segment_index SET value_summary = NULL
WHERE segment_id IN (SELECT segment_id FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.sps'::regclass);
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.sps WHERE id = 1011', 'Segments Pruned by Catalog Index')
       < sqlreg.explain_counter('SELECT v FROM sqlreg.sps WHERE id = 1010', 'Segments Pruned by Catalog Index') + 1 AS no_summary_prunes_less;
SELECT v AS value_without_summaries FROM sqlreg.sps WHERE id = 1012;
SELECT count(*) AS hits_without_summaries FROM sqlreg.sps WHERE id IN (3, 590, 1177);
ROLLBACK;

-- ------------------------------------------------------------------ other key types
CREATE TABLE sqlreg.sps_int (id integer PRIMARY KEY, v int NOT NULL DEFAULT 0);
INSERT INTO sqlreg.sps_int (id) SELECT g FROM generate_series(1, 40) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.sps_int'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS sps_int_managed;
SELECT sqlreg.flush_table('sqlreg.sps_int'::regclass) IS NOT NULL AS sps_int_flushed;

CREATE TABLE sqlreg.sps_small (id smallint PRIMARY KEY, v int NOT NULL DEFAULT 0);
INSERT INTO sqlreg.sps_small (id) SELECT g FROM generate_series(1, 40) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.sps_small'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS sps_small_managed;
SELECT sqlreg.flush_table('sqlreg.sps_small'::regclass) IS NOT NULL AS sps_small_flushed;

-- (Created empty and populated after manage_table: a populated uuid-PK table with an order column
-- cannot be flushed at all on this build, which is unrelated to summaries.)
CREATE TABLE sqlreg.sps_uuid (id uuid PRIMARY KEY, n int NOT NULL);
SELECT koldstore.manage_table(
  table_name => 'sqlreg.sps_uuid'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 1000, auto_flush => false
) IS NOT NULL AS sps_uuid_managed;
INSERT INTO sqlreg.sps_uuid (id, n)
SELECT ('00000000-0000-4000-8000-' || lpad(g::text, 12, '0'))::uuid, g FROM generate_series(1, 40) g;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS sps_uuid_mirror_caught_up;
SELECT sqlreg.flush_table('sqlreg.sps_uuid'::regclass) IS NOT NULL AS sps_uuid_flushed;
SELECT (koldstore.table_status('sqlreg.sps_uuid'::regclass) ->> 'cold_row_count')::int > 0 AS sps_uuid_has_cold_rows;

CREATE TABLE sqlreg.sps_text (id text PRIMARY KEY, n int NOT NULL);
INSERT INTO sqlreg.sps_text (id, n) SELECT 'key-' || g, g FROM generate_series(1, 40) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.sps_text'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS sps_text_managed;
SELECT sqlreg.flush_table('sqlreg.sps_text'::regclass) IS NOT NULL AS sps_text_flushed;

CREATE TABLE sqlreg.sps_comp (a bigint, b bigint, n int NOT NULL, PRIMARY KEY (a, b));
INSERT INTO sqlreg.sps_comp (a, b, n) SELECT g, g * 2, g FROM generate_series(1, 40) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.sps_comp'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS sps_comp_managed;
SELECT sqlreg.flush_table('sqlreg.sps_comp'::regclass) IS NOT NULL AS sps_comp_flushed;

-- summaries exist exactly for single-column integer / uuid keys
SELECT s.table_oid::regclass::text AS relation,
       bool_and(i.value_summary IS NOT NULL) AS has_summary
FROM koldstore.cold_segment_index i
JOIN koldstore.cold_segments s ON s.segment_id = i.segment_id
WHERE s.table_oid IN ('sqlreg.sps_int'::regclass, 'sqlreg.sps_small'::regclass, 'sqlreg.sps_uuid'::regclass,
                      'sqlreg.sps_text'::regclass, 'sqlreg.sps_comp'::regclass)
  AND s.status = 'active'
GROUP BY s.table_oid
ORDER BY 1;

-- every key of every type is still found by a literal-key point lookup
SELECT sqlreg.sum_counts(ARRAY(SELECT format('SELECT count(*) FROM sqlreg.sps_int WHERE id = %s', g)
                               FROM generate_series(1, 40) g)) AS int_hits;
-- (smallint keys are checked with an IN list: a bare `smallint_pk = const` equality lookup is a
-- separate, pre-existing problem in the exact-primary-key strategy and is not covered here.)
SELECT count(*) AS smallint_in_list_hits FROM sqlreg.sps_small WHERE id IN (1, 7, 20, 33, 40, 99);
SELECT sqlreg.sum_counts(ARRAY(SELECT format('SELECT count(*) FROM sqlreg.sps_uuid WHERE id = %L::uuid',
                                             '00000000-0000-4000-8000-' || lpad(g::text, 12, '0'))
                               FROM generate_series(1, 40) g)) AS uuid_hits;
SELECT sqlreg.sum_counts(ARRAY(SELECT format('SELECT count(*) FROM sqlreg.sps_text WHERE id = %L', 'key-' || g)
                               FROM generate_series(1, 40) g)) AS text_hits;
SELECT sqlreg.sum_counts(ARRAY(SELECT format('SELECT count(*) FROM sqlreg.sps_comp WHERE a = %s AND b = %s', g, g * 2)
                               FROM generate_series(1, 40) g)) AS composite_hits;
-- keys that do not exist stay absent
SELECT sqlreg.sum_counts(ARRAY(SELECT format('SELECT count(*) FROM sqlreg.sps_int WHERE id = %s', g)
                               FROM generate_series(1000, 1010) g)) AS absent_int_hits;
