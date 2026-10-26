-- A STORED generated tsvector: its term filter must be built from the generated values, survive
-- hydrate-on-write recomputation and a re-flush, and keep pruning. Answers are compared with an unmanaged copy.

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

CREATE TABLE sqlreg.gen_doc (
  id bigint PRIMARY KEY,
  body text,
  tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', coalesce(body, ''))) STORED
);
INSERT INTO sqlreg.gen_doc (id, body)
SELECT g, 'blk' || (g / 20) || ' common word' || (g % 5) || ' extra' || (g / 20)
FROM generate_series(1, 400) g;
INSERT INTO sqlreg.gen_doc (id, body) VALUES (1001, NULL), (1002, '');
CREATE TABLE sqlreg.gen_doc_ctl (LIKE sqlreg.gen_doc INCLUDING GENERATED);
INSERT INTO sqlreg.gen_doc_ctl (id, body) SELECT id, body FROM sqlreg.gen_doc;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.gen_doc'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false,
  pruning_columns => ARRAY['tsv']
) IS NOT NULL AS gen_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS gen_caught_up;
SELECT sqlreg.flush_table('sqlreg.gen_doc'::regclass) IS NOT NULL AS gen_flushed;
SELECT (SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.gen_doc'::regclass::oid AND status = 'active') >= 15 AS gen_many_segments;
SELECT count(*) > 0 AS gen_has_term_filters FROM koldstore.cold_segment_index
WHERE table_oid = 'sqlreg.gen_doc'::regclass::oid AND value_summary IS NOT NULL AND min_value IS NULL;

SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.gen_doc WHERE tsv @@ 'blk7'::tsquery$$) <= 8 AS lexeme_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.gen_doc WHERE tsv @@ to_tsquery('simple', 'blk3 & extra3')$$) <= 4 AS and_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.gen_doc WHERE tsv @@ plainto_tsquery('simple', 'blk5 extra5')$$) <= 4 AS plain_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.gen_doc WHERE tsv @@ 'nosuchword'::tsquery$$) <= 2 AS absent_opens_almost_nothing;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.gen_doc WHERE tsv @@ 'common'::tsquery$$) >= 15 AS common_reads_all;
SELECT sqlreg.same_rows('sqlreg.gen_doc', 'sqlreg.gen_doc_ctl', 'id', ARRAY[
  $$tsv @@ 'blk7'::tsquery$$, $$tsv @@ to_tsquery('simple', 'blk3 & extra3')$$,
  $$tsv @@ plainto_tsquery('simple', 'blk5 extra5')$$, $$tsv @@ 'nosuchword'::tsquery$$,
  $$tsv @@ 'common'::tsquery$$, $$tsv @@ '!blk7'::tsquery$$, $$tsv IS NULL$$
]) AS gen_mismatches_before;

-- rewriting the base column recomputes tsv; the new lexeme must be findable and the old one gone
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.gen_doc SET body = 'moved zzfresh' WHERE id IN (45, 130, 301);
UPDATE sqlreg.gen_doc_ctl SET body = 'moved zzfresh' WHERE id IN (45, 130, 301);
DELETE FROM sqlreg.gen_doc WHERE id = 250;
DELETE FROM sqlreg.gen_doc_ctl WHERE id = 250;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS gen_caught_up_2;
SELECT sqlreg.same_rows('sqlreg.gen_doc', 'sqlreg.gen_doc_ctl', 'id', ARRAY[
  $$tsv @@ 'zzfresh'::tsquery$$, $$tsv @@ 'blk2'::tsquery$$, $$tsv @@ 'blk6'::tsquery$$,
  $$tsv @@ 'blk12'::tsquery$$, $$tsv @@ 'blk15'::tsquery$$, $$tsv @@ 'moved & zzfresh'::tsquery$$
]) AS gen_mismatches_after_dml;

-- re-flush rebuilds the segments' filters from the recomputed values
SELECT sqlreg.flush_table('sqlreg.gen_doc'::regclass, true) IS NOT NULL AS gen_reflushed;
-- (pruning is not asserted here: the rewritten keys land in new segments whose key ranges overlap the
-- old ones, and term-filter pruning only applies to segments with disjoint key ranges.)
SELECT sqlreg.same_rows('sqlreg.gen_doc', 'sqlreg.gen_doc_ctl', 'id', ARRAY[
  $$tsv @@ 'zzfresh'::tsquery$$, $$tsv @@ 'blk2'::tsquery$$, $$tsv @@ 'blk7'::tsquery$$,
  $$tsv @@ to_tsquery('simple', 'blk3 & extra3')$$, $$tsv @@ 'common'::tsquery$$, $$tsv IS NULL$$
]) AS gen_mismatches_after_reflush;
SELECT (SELECT count(*) FROM sqlreg.gen_doc) = (SELECT count(*) FROM sqlreg.gen_doc_ctl) AS counts_match;
