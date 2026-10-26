-- koldstore.rebuild_segment_summaries() also gives segments written before a `text[]` / `jsonb` / `tsvector` /
-- `vector` column was listed in `pruning_columns` their term filter or vector sketch. Legacy segments are
-- simulated by deleting the filter rows; the rebuilt bitmaps must be byte-identical to the ones flush wrote.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE EXTENSION IF NOT EXISTS vector;

CREATE FUNCTION sqlreg.candidate_segments(q text) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE line text; found int;
BEGIN
  FOR line IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) ' || q LOOP
    IF line ~ 'Parquet Segments Opened: [0-9]+' THEN
      found := substring(line from 'Parquet Segments Opened: ([0-9]+)')::int;
    END IF;
  END LOOP;
  RETURN found;
END
$$;

CREATE TABLE sqlreg.rtf (id bigint PRIMARY KEY, tags text[], doc jsonb, tsv tsvector, emb vector(3), note text);
INSERT INTO sqlreg.rtf
SELECT g, ARRAY['blk' || (g / 20), 'common'],
       CASE WHEN g % 17 = 0 THEN NULL ELSE jsonb_build_object('grp', 'g' || (g / 20)) END,
       to_tsvector('simple', 'blk' || (g / 20) || ' common'),
       ARRAY[(g / 20) * 100 + (g % 5), (g % 7) * 0.5, (g % 3) * 0.25]::real[]::vector,
       'n' || g
FROM generate_series(1, 400) g;
INSERT INTO sqlreg.rtf VALUES (1001, NULL, NULL, NULL, NULL, 'all null');
CREATE TABLE sqlreg.rtf_ctl AS SELECT * FROM sqlreg.rtf;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.rtf'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false,
  pruning_columns => ARRAY['tags', 'doc', 'tsv', 'emb']
) IS NOT NULL AS rtf_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS rtf_caught_up;
SELECT sqlreg.flush_table('sqlreg.rtf'::regclass) IS NOT NULL AS rtf_flushed;

-- the filters flush wrote, kept for comparison
CREATE TABLE sqlreg.rtf_orig AS
SELECT i.segment_id, i.column_id, i.value_summary, cardinality(i.row_group_min_values) AS rgs
FROM koldstore.cold_segment_index i
WHERE i.table_oid = 'sqlreg.rtf'::regclass::oid AND i.min_value IS NULL AND i.value_summary IS NOT NULL;
SELECT count(DISTINCT column_id) AS filtered_columns, count(*) >= 60 AS filters_written FROM sqlreg.rtf_orig;

SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.rtf WHERE tags @> ARRAY['blk7']$$) <= 8 AS tags_prune_before;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.rtf WHERE tsv @@ 'blk7'::tsquery$$) <= 8 AS tsv_prune_before;

-- ------------------------------------------------ legacy segments: no filters, so nothing prunes
DELETE FROM koldstore.cold_segment_index i USING sqlreg.rtf_orig o
WHERE i.segment_id = o.segment_id AND i.column_id = o.column_id;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.rtf WHERE tags @> ARRAY['blk7']$$) >= 15 AS tags_read_all;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.rtf WHERE tsv @@ 'blk7'::tsquery$$) >= 15 AS tsv_read_all;

-- ------------------------------------------------ a dry run only counts
SELECT (c->>'column') AS col, (c->>'segments_without_filter')::int >= 15 AS missing
FROM (SELECT koldstore.rebuild_segment_summaries('sqlreg.rtf'::regclass, dry_run => true) AS r) s,
     jsonb_array_elements(s.r->'tables'->0->'term_filters'->'columns') c
ORDER BY 1;

-- ------------------------------------------------ rebuild in two bounded passes
SELECT (r->'tables'->0->'term_filters'->>'rebuilt')::int AS first_pass_rebuilt
FROM (SELECT koldstore.rebuild_segment_summaries('sqlreg.rtf'::regclass, max_segments => 3) AS r) s;
SELECT (r->'tables'->0->'term_filters'->>'rebuilt')::int > 0 AS second_pass_rebuilt,
       (SELECT bool_and(jsonb_array_length(c->'failed') = 0) FROM jsonb_array_elements(r->'tables'->0->'term_filters'->'columns') c) AS no_failures
FROM (SELECT koldstore.rebuild_segment_summaries('sqlreg.rtf'::regclass, max_segments => 1000) AS r) s;
SELECT (r->'tables'->0->'term_filters'->>'rebuilt')::int AS idempotent_rebuilt
FROM (SELECT koldstore.rebuild_segment_summaries('sqlreg.rtf'::regclass) AS r) s;

-- ------------------------------------------------ byte-identical to what flush wrote
SELECT count(*) AS differing FROM sqlreg.rtf_orig o
LEFT JOIN koldstore.cold_segment_index i ON i.segment_id = o.segment_id AND i.column_id = o.column_id
WHERE i.value_summary IS DISTINCT FROM o.value_summary OR cardinality(i.row_group_min_values) IS DISTINCT FROM o.rgs
   OR i.min_value IS NOT NULL;
SELECT count(*) AS extra_rows FROM koldstore.cold_segment_index i
WHERE i.table_oid = 'sqlreg.rtf'::regclass::oid AND i.min_value IS NULL AND i.value_summary IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM sqlreg.rtf_orig o WHERE o.segment_id = i.segment_id AND o.column_id = i.column_id);

-- ------------------------------------------------ pruning is back, answers unchanged
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.rtf WHERE tags @> ARRAY['blk7']$$) <= 8 AS tags_prune_after;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.rtf WHERE doc @> '{"grp": "g5"}'$$) <= 8 AS doc_prune_after;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.rtf WHERE tsv @@ 'blk7'::tsquery$$) <= 8 AS tsv_prune_after;
SELECT (SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '') FROM sqlreg.rtf WHERE tags @> ARRAY['blk7'])
     = (SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '') FROM sqlreg.rtf_ctl WHERE tags @> ARRAY['blk7']) AS tags_rows_match;
SELECT (SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '') FROM sqlreg.rtf WHERE tsv @@ 'blk7'::tsquery)
     = (SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '') FROM sqlreg.rtf_ctl WHERE tsv @@ 'blk7'::tsquery) AS tsv_rows_match;
SELECT (SELECT string_agg(id::text, ',' ORDER BY n) FROM (SELECT id, row_number() OVER () n FROM (SELECT id FROM sqlreg.rtf ORDER BY emb <-> '[700,0,0]' LIMIT 5) s) r)
     = (SELECT string_agg(id::text, ',' ORDER BY n) FROM (SELECT id, row_number() OVER () n FROM (SELECT id FROM sqlreg.rtf_ctl ORDER BY emb <-> '[700,0,0]' LIMIT 5) s) r) AS knn_rows_match;
