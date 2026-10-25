-- Nearest-neighbour queries (`ORDER BY emb <-> const LIMIT k`) skip cold segments whose vector ball cannot hold a
-- top-k row. Each block of 20 ids sits in its own cluster. Answers are always compared with an unmanaged copy,
-- including hot updates / deletes that mask cold rows and layouts or query shapes that must not prune.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE EXTENSION IF NOT EXISTS vector;

CREATE FUNCTION sqlreg.same_knn(managed regclass, control regclass, queries text[]) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE q text; a text; b text; bad int := 0;
BEGIN
  FOREACH q IN ARRAY queries LOOP
    EXECUTE format('SELECT coalesce(string_agg(id::text, '','' ORDER BY n), '''') FROM (SELECT id, row_number() OVER () n FROM (%s) s) r', replace(q, '$T', managed::text)) INTO a;
    EXECUTE format('SELECT coalesce(string_agg(id::text, '','' ORDER BY n), '''') FROM (SELECT id, row_number() OVER () n FROM (%s) s) r', replace(q, '$T', control::text)) INTO b;
    IF a IS DISTINCT FROM b THEN
      bad := bad + 1;
      RAISE NOTICE 'mismatch: %', q;
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

CREATE TABLE sqlreg.kn (id bigint PRIMARY KEY, emb vector(3), note text);
INSERT INTO sqlreg.kn
SELECT g, ARRAY[(g / 20) * 100 + (g % 5), (g % 7) * 0.5, (g % 3) * 0.25]::real[]::vector, 'n' || g
FROM generate_series(1, 400) g;
INSERT INTO sqlreg.kn VALUES (1001, NULL, 'null vec'), (1002, '[0,0,0]', 'zero');
CREATE TABLE sqlreg.kn_ctl AS SELECT * FROM sqlreg.kn;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.kn'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false,
  pruning_columns => ARRAY['emb']
) IS NOT NULL AS kn_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS kn_caught_up;
SELECT sqlreg.flush_table('sqlreg.kn'::regclass) IS NOT NULL AS kn_flushed;
SELECT (SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.kn'::regclass::oid AND status = 'active') >= 15 AS kn_many_segments;
SELECT count(*) >= 15 AS kn_has_sketches FROM koldstore.cold_segment_index
WHERE table_oid = 'sqlreg.kn'::regclass::oid AND value_summary IS NOT NULL AND min_value IS NULL;

-- ------------------------------------------------ disjoint keys: sketches prune
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.kn ORDER BY emb <-> '[700,1,0]' LIMIT 5$$) <= 5 AS l2_prunes;
SELECT sqlreg.candidate_segments($$SELECT id FROM sqlreg.kn ORDER BY emb <-> '[700,1,0]' LIMIT 5 OFFSET 2$$) <= 5 AS offset_prunes;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.kn ORDER BY emb <=> '[0,1,1]' LIMIT 3$$) >= 1 AS cosine_runs;
-- a larger LIMIT needs more segments but never fewer than the rows demand
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.kn ORDER BY emb <-> '[700,1,0]' LIMIT 100$$) >= 5 AS big_limit_reads_enough;
-- shapes that must not prune
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.kn WHERE note <> 'x' ORDER BY emb <-> '[700,1,0]' LIMIT 5$$) >= 15 AS where_reads_all;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.kn ORDER BY emb <-> '[700,1,0]'$$) >= 15 AS no_limit_reads_all;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.kn ORDER BY emb <-> '[700,1,0]' DESC LIMIT 5$$) >= 15 AS desc_reads_all;
SELECT sqlreg.candidate_segments($$SELECT * FROM sqlreg.kn ORDER BY emb <-> '[700,1,0]' LIMIT 5 OFFSET 0$$) >= 1 AS offset_zero_runs;
SELECT sqlreg.same_knn('sqlreg.kn', 'sqlreg.kn_ctl', ARRAY[
  $$SELECT id FROM $T ORDER BY emb <-> '[700,1,0]', id LIMIT 5$$,
  $$SELECT id FROM $T ORDER BY emb <-> '[700,1,0]', id LIMIT 5 OFFSET 7$$,
  $$SELECT id FROM $T ORDER BY emb <-> '[0,0,0]', id LIMIT 30$$,
  $$SELECT id FROM $T ORDER BY emb <-> '[1999,9,9]', id LIMIT 3$$,
  $$SELECT id FROM $T ORDER BY emb <=> '[0,1,1]', id LIMIT 10$$,
  $$SELECT id FROM $T ORDER BY emb <=> '[1,0,0]', id LIMIT 50$$,
  $$SELECT id FROM $T ORDER BY emb <-> '[300,0,0]', id LIMIT 1000$$,
  $$SELECT id FROM $T WHERE note <> 'n5' ORDER BY emb <-> '[700,1,0]', id LIMIT 5$$,
  $$SELECT id FROM $T ORDER BY emb <-> '[700,1,0]' DESC, id LIMIT 5$$
]) AS kn_mismatches_before;

-- ------------------------------------------------ hot updates / deletes mask cold rows
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.kn SET emb = '[5000,5000,5000]' WHERE id IN (141, 142, 143, 144, 145, 146);
UPDATE sqlreg.kn_ctl SET emb = '[5000,5000,5000]' WHERE id IN (141, 142, 143, 144, 145, 146);
DELETE FROM sqlreg.kn WHERE id IN (147, 148, 149);
DELETE FROM sqlreg.kn_ctl WHERE id IN (147, 148, 149);
UPDATE sqlreg.kn SET emb = '[700,1,0]' WHERE id IN (3, 250);
UPDATE sqlreg.kn_ctl SET emb = '[700,1,0]' WHERE id IN (3, 250);
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS kn_fenced;
SELECT sqlreg.same_knn('sqlreg.kn', 'sqlreg.kn_ctl', ARRAY[
  $$SELECT id FROM $T ORDER BY emb <-> '[700,1,0]', id LIMIT 5$$,
  $$SELECT id FROM $T ORDER BY emb <-> '[700,1,0]', id LIMIT 12$$,
  $$SELECT id FROM $T ORDER BY emb <-> '[700,1,0]', id LIMIT 3 OFFSET 4$$,
  $$SELECT id FROM $T ORDER BY emb <-> '[700,1,0]', id LIMIT 50$$,
  $$SELECT id FROM $T ORDER BY emb <-> '[140,0,0]', id LIMIT 6$$,
  $$SELECT id FROM $T ORDER BY emb <-> '[140,0,0]', id LIMIT 20$$,
  $$SELECT id FROM $T ORDER BY emb <=> '[1,0,0]', id LIMIT 8$$
]) AS kn_mismatches_masked;

-- ------------------------------------------------ re-flush overlaps keys across segments: no pruning, still right
SELECT sqlreg.flush_table('sqlreg.kn'::regclass) IS NOT NULL AS kn_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS kn_fenced_2;
SELECT sqlreg.same_knn('sqlreg.kn', 'sqlreg.kn_ctl', ARRAY[
  $$SELECT id FROM $T ORDER BY emb <-> '[700,1,0]', id LIMIT 5$$,
  $$SELECT id FROM $T ORDER BY emb <-> '[140,0,0]', id LIMIT 20$$,
  $$SELECT id FROM $T ORDER BY emb <=> '[1,0,0]', id LIMIT 8$$
]) AS kn_mismatches_reflush;
SELECT count(*) AS kn_total, count(DISTINCT id) AS kn_distinct FROM sqlreg.kn;

-- ------------------------------------------------ a prepared statement with a parameter never prunes
PREPARE kn_p(vector) AS SELECT id FROM sqlreg.kn ORDER BY emb <-> $1, id LIMIT 4;
PREPARE kn_c(vector) AS SELECT id FROM sqlreg.kn_ctl ORDER BY emb <-> $1, id LIMIT 4;
CREATE TEMP TABLE kn_pa AS EXECUTE kn_p('[300,0,0]');
CREATE TEMP TABLE kn_pb AS EXECUTE kn_c('[300,0,0]');
SELECT (SELECT array_agg(id ORDER BY id) FROM kn_pa) = (SELECT array_agg(id ORDER BY id) FROM kn_pb) AS kn_param_matches;
