-- koldstore.rebuild_segment_summaries(): gives segments written before value summaries existed the
-- summary flush would have written. A segment without one is never pruned by the catalog, so a point
-- lookup opens every segment whose min/max range covers the key.
--
-- Legacy segments are simulated by nulling the summaries of a table flushed on this build. The
-- rebuilt bytes must equal what flush stored, pruning must come back, no key may be lost, and
-- anything it cannot verify must be left alone and reported.

\set VERBOSITY terse
\set ON_ERROR_STOP off

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

-- ------------------------------------------------------------------ a table with scattered segments
CREATE TABLE sqlreg.rss (id bigint PRIMARY KEY, v int NOT NULL DEFAULT 0);
INSERT INTO sqlreg.rss (id) SELECT g FROM generate_series(1, 1200) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.rss'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS rss_managed;
SELECT sqlreg.flush_table('sqlreg.rss'::regclass) IS NOT NULL AS rss_flushed;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.rss SET v = v + 1 WHERE id IN (3, 590, 1177);
SELECT sqlreg.flush_table('sqlreg.rss'::regclass) IS NOT NULL AS round1;
UPDATE sqlreg.rss SET v = v + 1 WHERE id IN (41, 700, 1100);
SELECT sqlreg.flush_table('sqlreg.rss'::regclass) IS NOT NULL AS round2;
UPDATE sqlreg.rss SET v = v + 1 WHERE id IN (88, 512, 1199);
SELECT sqlreg.flush_table('sqlreg.rss'::regclass) IS NOT NULL AS round3;
UPDATE sqlreg.rss SET v = v + 1 WHERE id IN (150, 640, 930);
SELECT sqlreg.flush_table('sqlreg.rss'::regclass) IS NOT NULL AS round4;
RESET koldstore.hydrate_on_write;
SELECT count(*) >= 5 AS several_segments
FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.rss'::regclass AND status = 'active';

-- what flush stored, to compare against later
CREATE TABLE sqlreg.rss_before AS
SELECT i.segment_id, i.value_summary
FROM koldstore.cold_segment_index i JOIN koldstore.cold_segments s ON s.segment_id = i.segment_id
WHERE s.table_oid = 'sqlreg.rss'::regclass AND s.status = 'active';
SELECT count(*) FILTER (WHERE value_summary IS NOT NULL) = count(*) AS flush_wrote_every_summary FROM sqlreg.rss_before;

-- pruning with summaries (a key none of the churn segments hold)
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.rss WHERE id = 1010', 'Segments Pruned by Catalog Index') >= 3 AS pruned_with_summaries;

-- ------------------------------------------------------------------ simulate legacy segments
UPDATE koldstore.cold_segment_index SET value_summary = NULL
WHERE segment_id IN (SELECT segment_id FROM sqlreg.rss_before);
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.rss WHERE id = 1011', 'Segments Pruned by Catalog Index') AS pruned_without_summaries;

-- a dry run only counts
SELECT (t ->> 'eligible')::boolean AS eligible, (t ->> 'segments_without_summary')::int = (SELECT count(*) FROM sqlreg.rss_before) AS counts_every_segment,
       (r ->> 'dry_run')::boolean AS dry_run, (r ->> 'rebuilt')::int AS rebuilt
FROM (SELECT koldstore.rebuild_segment_summaries('sqlreg.rss'::regclass, dry_run => true) AS r) s,
     LATERAL jsonb_array_elements(r -> 'tables') t;
SELECT count(*) AS still_without_summary
FROM koldstore.cold_segment_index WHERE value_summary IS NULL AND segment_id IN (SELECT segment_id FROM sqlreg.rss_before);

-- rebuild in batches: max_segments bounds one call
SELECT (r ->> 'rebuilt')::int AS rebuilt_first_batch, (t ->> 'remaining')::int = (SELECT count(*) FROM sqlreg.rss_before) - 2 AS two_fewer_remaining,
       jsonb_array_length(t -> 'failed') AS failed
FROM (SELECT koldstore.rebuild_segment_summaries('sqlreg.rss'::regclass, max_segments => 2) AS r) s,
     LATERAL jsonb_array_elements(r -> 'tables') t;
SELECT (r ->> 'rebuilt')::int = (SELECT count(*) FROM sqlreg.rss_before) - 2 AS rebuilt_the_rest, (t ->> 'remaining')::int AS remaining,
       jsonb_array_length(t -> 'failed') AS failed
FROM (SELECT koldstore.rebuild_segment_summaries('sqlreg.rss'::regclass) AS r) s,
     LATERAL jsonb_array_elements(r -> 'tables') t;

-- the rebuilt bitmaps are exactly what flush wrote
SELECT count(*) AS bytes_differ_from_flush
FROM sqlreg.rss_before b JOIN koldstore.cold_segment_index i ON i.segment_id = b.segment_id
WHERE i.value_summary IS DISTINCT FROM b.value_summary;

-- pruning is back, and nothing is lost
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.rss WHERE id = 1012', 'Segments Pruned by Catalog Index') >= 3 AS pruned_again;
SELECT sqlreg.sum_counts(ARRAY(SELECT format('SELECT count(*) FROM sqlreg.rss WHERE id = %s', g) FROM generate_series(1, 1200) g)) = 1200 AS every_key_found;
SELECT count(*) AS updated_rows_visible, sum(v) AS total_updates FROM sqlreg.rss WHERE v > 0;

-- idempotent: nothing left to do
SELECT (r ->> 'rebuilt')::int AS rebuilt_again FROM (SELECT koldstore.rebuild_segment_summaries('sqlreg.rss'::regclass) AS r) s;

-- ------------------------------------------------------------------ never trust what it cannot verify
UPDATE koldstore.cold_segment_index SET value_summary = NULL
WHERE segment_id = (SELECT segment_id FROM sqlreg.rss_before ORDER BY segment_id LIMIT 1);
UPDATE koldstore.cold_segments SET checksum = repeat('0', 64)
WHERE segment_id = (SELECT segment_id FROM sqlreg.rss_before ORDER BY segment_id LIMIT 1);
SELECT (r ->> 'rebuilt')::int AS rebuilt_with_bad_checksum, jsonb_array_length(t -> 'failed') AS reported_failed,
       t -> 'failed' -> 0 ->> 'problem' LIKE 'checksum:%' AS says_checksum, (t ->> 'remaining')::int AS remaining
FROM (SELECT koldstore.rebuild_segment_summaries('sqlreg.rss'::regclass) AS r) s,
     LATERAL jsonb_array_elements(r -> 'tables') t;
SELECT count(*) AS summaries_left_null
FROM koldstore.cold_segment_index WHERE value_summary IS NULL AND segment_id IN (SELECT segment_id FROM sqlreg.rss_before);

-- ------------------------------------------------------------------ keys without a summary are reported, not guessed
CREATE TABLE sqlreg.rss_text (id text PRIMARY KEY, n int NOT NULL);
INSERT INTO sqlreg.rss_text (id, n) SELECT 'k' || g, g FROM generate_series(1, 20) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.rss_text'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS rss_text_managed;
CREATE TABLE sqlreg.rss_comp (a int, b int, v int NOT NULL DEFAULT 0, PRIMARY KEY (a, b));
INSERT INTO sqlreg.rss_comp (a, b) SELECT g, g FROM generate_series(1, 20) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.rss_comp'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'a', auto_flush => false
) IS NOT NULL AS rss_comp_managed;
SELECT t ->> 'table' AS tbl, (t ->> 'eligible')::boolean AS eligible
FROM (SELECT koldstore.rebuild_segment_summaries(dry_run => true) AS r) s,
     LATERAL jsonb_array_elements(r -> 'tables') t
WHERE t ->> 'table' IN ('sqlreg.rss_text', 'sqlreg.rss_comp') ORDER BY 1;

-- ------------------------------------------------------------------ privileges
CREATE ROLE sqlreg_rss_other NOLOGIN;
SET ROLE sqlreg_rss_other;
SELECT koldstore.rebuild_segment_summaries('sqlreg.rss'::regclass);
SELECT koldstore.rebuild_segment_summaries();
RESET ROLE;
DROP ROLE sqlreg_rss_other;
