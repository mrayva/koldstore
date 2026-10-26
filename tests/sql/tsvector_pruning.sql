-- tsvector term filter: a per-segment lexeme membership filter, so `tsv @@ <tsquery>` skips cold segments
-- that cannot match. Every answer is compared with an unmanaged copy.

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

-- Each block of 20 ids carries its own vocabulary, so a probe matches a single segment.
CREATE TABLE sqlreg.tsv_doc (id bigint PRIMARY KEY, tsv tsvector, note text);
INSERT INTO sqlreg.tsv_doc
SELECT g,
       to_tsvector('simple', 'blk' || (g / 20) || ' common word' || (g % 5) || ' extra' || (g / 20)),
       'n' || g
FROM generate_series(1, 400) g;
INSERT INTO sqlreg.tsv_doc VALUES (1001, NULL, 'null'), (1002, ''::tsvector, 'empty'),
  (1003, $$'it''s':1 'back\\slash':2 'quoted lexeme':3 'blk7':4$$::tsvector, 'escapes');
CREATE TABLE sqlreg.tsv_doc_ctl AS SELECT * FROM sqlreg.tsv_doc;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.tsv_doc'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false,
  pruning_columns => ARRAY['tsv']
) IS NOT NULL AS tsv_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS tsv_caught_up;
SELECT sqlreg.flush_table('sqlreg.tsv_doc'::regclass) IS NOT NULL AS tsv_flushed;
SELECT (SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.tsv_doc'::regclass::oid AND status = 'active') >= 15 AS tsv_many_segments;
SELECT count(*) > 0 AS tsv_has_term_filters FROM koldstore.cold_segment_index
WHERE table_oid = 'sqlreg.tsv_doc'::regclass::oid AND value_summary IS NOT NULL AND min_value IS NULL;

-- ------------------------------------------------ the filters prune
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tsv_doc WHERE tsv @@ 'blk7'::tsquery$$) <= 8 AS lexeme_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tsv_doc WHERE tsv @@ to_tsquery('simple', 'blk3 & extra3')$$) <= 4 AS and_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tsv_doc WHERE tsv @@ to_tsquery('simple', 'blk3 | blk11')$$) <= 6 AS or_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tsv_doc WHERE to_tsquery('simple', 'blk9') @@ tsv$$) <= 4 AS commuted_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tsv_doc WHERE tsv @@ plainto_tsquery('simple', 'blk5 extra5')$$) <= 4 AS plain_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tsv_doc WHERE tsv @@ phraseto_tsquery('simple', 'blk5 common')$$) <= 4 AS phrase_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tsv_doc WHERE tsv @@ websearch_to_tsquery('simple', 'blk6 extra6')$$) <= 4 AS websearch_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tsv_doc WHERE tsv @@ 'nosuchword'::tsquery$$) = 0 AS absent_opens_nothing;
-- shapes that prove nothing read every segment
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tsv_doc WHERE tsv @@ 'common'::tsquery$$) >= 15 AS common_reads_all;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tsv_doc WHERE tsv @@ '!blk7'::tsquery$$) >= 15 AS negation_reads_all;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.tsv_doc WHERE tsv @@ 'blk:*'::tsquery$$) >= 15 AS prefix_reads_all;
SELECT sqlreg.same_rows('sqlreg.tsv_doc', 'sqlreg.tsv_doc_ctl', 'id', ARRAY[
  $$tsv @@ 'blk7'::tsquery$$, $$tsv @@ to_tsquery('simple', 'blk3 & extra3')$$,
  $$tsv @@ to_tsquery('simple', 'blk3 | blk11')$$, $$to_tsquery('simple', 'blk9') @@ tsv$$,
  $$tsv @@ plainto_tsquery('simple', 'blk5 extra5')$$, $$tsv @@ phraseto_tsquery('simple', 'blk5 common')$$,
  $$tsv @@ websearch_to_tsquery('simple', 'blk6 extra6')$$, $$tsv @@ 'nosuchword'::tsquery$$,
  $$tsv @@ 'common'::tsquery$$, $$tsv @@ '!blk7'::tsquery$$, $$tsv @@ 'blk:*'::tsquery$$,
  $$tsv @@ 'blk7:A'::tsquery$$, $$tsv @@ $q$'it''s' & 'back\\slash'$q$::tsquery$$,
  $$tsv @@ $q$'quoted lexeme'$q$::tsquery$$, $$tsv @@ 'blk7 <-> word2'::tsquery$$,
  $$tsv @@ '(blk1 | blk2) & !extra2'::tsquery$$, $$tsv IS NULL$$
]) AS tsv_mismatches_before;

-- ------------------------------------------------ a bound parameter prunes too
PREPARE tsv_q(tsquery) AS SELECT count(*) FROM sqlreg.tsv_doc WHERE tsv @@ $1;
SET plan_cache_mode = force_generic_plan;
EXECUTE tsv_q('blk7'::tsquery);
RESET plan_cache_mode;
SELECT (SELECT count(*) FROM sqlreg.tsv_doc WHERE tsv @@ 'blk7'::tsquery) = (SELECT count(*) FROM sqlreg.tsv_doc_ctl WHERE tsv @@ 'blk7'::tsquery) AS param_matches;

-- ------------------------------------------------ a re-flushed update puts a key in two segments
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.tsv_doc SET tsv = to_tsvector('simple', 'moved') WHERE id IN (45, 190);
UPDATE sqlreg.tsv_doc_ctl SET tsv = to_tsvector('simple', 'moved') WHERE id IN (45, 190);
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS tsv_fenced;
SELECT sqlreg.flush_table('sqlreg.tsv_doc'::regclass) IS NOT NULL AS tsv_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS tsv_fenced_2;
SELECT sqlreg.same_rows('sqlreg.tsv_doc', 'sqlreg.tsv_doc_ctl', 'id', ARRAY[
  $$tsv @@ 'moved'::tsquery$$, $$tsv @@ 'blk2'::tsquery$$, $$tsv @@ 'blk9'::tsquery$$, $$tsv @@ 'word0'::tsquery$$
]) AS tsv_mismatches_after;
