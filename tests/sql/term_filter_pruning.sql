-- Term filters: a per-segment membership filter over the elements of `text[]` columns and the keys / values of
-- `jsonb` columns listed in `pruning_columns`, so `@>`, `&&`, `?`, `?|` and `?&` skip cold segments that cannot
-- match. As with any mutable column, pruning is only allowed while no key is stored in two active segments.
-- Every answer is compared with an unmanaged copy, including values that need escaping.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE FUNCTION sqlreg.same_rows(managed regclass, control regclass, key text, preds text[]) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE p text; a text; b text; bad int := 0;
BEGIN
  FOREACH p IN ARRAY preds LOOP
    EXECUTE format('SELECT coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''') FROM %s t WHERE %s', key, managed, p) INTO a;
    EXECUTE format('SELECT coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''') FROM %s t WHERE %s', key, control, p) INTO b;
    IF a IS DISTINCT FROM b THEN
      bad := bad + 1;
      RAISE NOTICE 'mismatch: %', p;
    END IF;
  END LOOP;
  RETURN bad;
END
$$;

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

-- The filters are one-hash bitmaps, so a probe may open a few extra segments (false positives) but never fewer.
-- Each block of 20 ids carries its own tag / key vocabulary, so a probe matches a single segment.
CREATE TABLE sqlreg.tf_doc (
  id bigint PRIMARY KEY, tags text[] NOT NULL, doc jsonb, note text
);
INSERT INTO sqlreg.tf_doc
SELECT g,
       ARRAY['blk' || (g / 20), 'common', CASE WHEN g % 20 = 0 THEN 'q"uo\te' ELSE 'plain' END],
       CASE WHEN g % 17 = 0 THEN NULL
            ELSE jsonb_build_object('grp', 'g' || (g / 20), 'k' || (g / 20), g % 3, 'nested', jsonb_build_object('deep', 'd' || (g / 20))) END,
       'n' || g
FROM generate_series(1, 400) g;
CREATE TABLE sqlreg.tf_doc_ctl AS SELECT * FROM sqlreg.tf_doc;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.tf_doc'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false,
  pruning_columns => ARRAY['tags', 'doc']
) IS NOT NULL AS tf_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS tf_caught_up;
SELECT sqlreg.flush_table('sqlreg.tf_doc'::regclass) IS NOT NULL AS tf_flushed;
SELECT (SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.tf_doc'::regclass::oid AND status = 'active') >= 15 AS tf_many_segments;
SELECT count(*) > 0 AS tf_has_term_filters FROM koldstore.cold_segment_index
WHERE table_oid = 'sqlreg.tf_doc'::regclass::oid AND value_summary IS NOT NULL AND min_value IS NULL;

-- ------------------------------------------------ disjoint keys: the term filters prune
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tf_doc WHERE tags @> ARRAY['blk7']$$) <= 3 AS contains_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tf_doc WHERE tags && ARRAY['blk3', 'blk11']$$) <= 5 AS overlap_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tf_doc WHERE doc @> '{"grp": "g5"}'$$) <= 8 AS jsonb_contains_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tf_doc WHERE doc ? 'k9'$$) <= 8 AS jsonb_key_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tf_doc WHERE doc ?| ARRAY['k2', 'k12']$$) <= 10 AS jsonb_any_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tf_doc WHERE doc ?& ARRAY['grp', 'k4']$$) <= 8 AS jsonb_all_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tf_doc WHERE doc @> '{"nested": {"deep": "d6"}}'$$) <= 8 AS jsonb_nested_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tf_doc WHERE tags @> ARRAY['nosuchtag']$$) = 0 AS absent_opens_nothing;
-- predicates that cannot prune read every segment
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tf_doc WHERE tags @> ARRAY['common']$$) >= 15 AS common_reads_all;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tf_doc WHERE tags @> ARRAY[NULL]::text[]$$) >= 15 AS null_probe_reads_all;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tf_doc WHERE doc @> '"g5"'$$) >= 15 AS scalar_probe_reads_all;
SELECT sqlreg.same_rows('sqlreg.tf_doc', 'sqlreg.tf_doc_ctl', 'id', ARRAY[
  $$tags @> ARRAY['blk7']$$, $$tags && ARRAY['blk3', 'blk11']$$, $$tags @> ARRAY['q"uo\te']$$,
  $$tags @> ARRAY['plain', 'blk2']$$, $$tags && ARRAY['nosuchtag']$$, $$tags @> ARRAY['common']$$,
  $$doc @> '{"grp": "g5"}'$$, $$doc @> '{"k9": 0}'$$, $$doc @> '{"nested": {"deep": "d6"}}'$$,
  $$doc ? 'k9'$$, $$doc ? 'grp'$$, $$doc ? 'g5'$$, $$doc ?| ARRAY['k2', 'k12']$$, $$doc ?& ARRAY['grp', 'k4']$$,
  $$doc @> '"g5"'$$, $$doc @> '{}'$$, $$doc IS NULL$$
]) AS tf_mismatches_before;

-- ------------------------------------------------ a re-flushed update puts a key in two segments
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.tf_doc SET tags = ARRAY['moved'], doc = '{"moved": true}' WHERE id IN (45, 190);
UPDATE sqlreg.tf_doc_ctl SET tags = ARRAY['moved'], doc = '{"moved": true}' WHERE id IN (45, 190);
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS tf_fenced;
SELECT sqlreg.flush_table('sqlreg.tf_doc'::regclass) IS NOT NULL AS tf_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS tf_fenced_2;
-- the stale cold versions must stay hidden and the new ones visible
SELECT sqlreg.same_rows('sqlreg.tf_doc', 'sqlreg.tf_doc_ctl', 'id', ARRAY[
  $$tags @> ARRAY['blk2']$$, $$tags @> ARRAY['blk9']$$, $$tags @> ARRAY['moved']$$, $$tags && ARRAY['moved', 'blk2']$$,
  $$doc ? 'moved'$$, $$doc @> '{"moved": true}'$$, $$doc @> '{"grp": "g2"}'$$, $$doc ? 'k9'$$
]) AS tf_mismatches_after_reflush;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tf_doc WHERE tags @> ARRAY['blk7']$$) >= 15 AS overlapping_reads_all;
SELECT id, tags FROM sqlreg.tf_doc WHERE id IN (45, 190) ORDER BY id;
SELECT count(*) AS tf_total, count(DISTINCT id) AS tf_distinct FROM sqlreg.tf_doc;

-- ------------------------------------------------ compaction rewrites the segments with their term filters
SELECT (koldstore.compact_table('sqlreg.tf_doc'::regclass, grace_seconds => 3600) ->> 'compacted')::boolean AS tf_compacted;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS tf_fenced_3;
SELECT count(DISTINCT cs.segment_id) = count(DISTINCT t.segment_id) AS compacted_segments_have_term_filters
FROM koldstore.cold_segments cs
LEFT JOIN koldstore.cold_segment_index t
  ON t.segment_id = cs.segment_id AND t.column_id = 2 AND t.value_summary IS NOT NULL
WHERE cs.table_oid = 'sqlreg.tf_doc'::regclass::oid AND cs.status = 'active';
SELECT sqlreg.same_rows('sqlreg.tf_doc', 'sqlreg.tf_doc_ctl', 'id', ARRAY[
  $$tags @> ARRAY['blk2']$$, $$tags @> ARRAY['moved']$$, $$doc ? 'moved'$$, $$doc @> '{"grp": "g5"}'$$
]) AS tf_mismatches_after_compaction;
