-- A text primary key now has segment min/max bounds (the UTF-8 bytes of Parquet's string statistics), so an
-- equality lookup, an IN list and the INSERT guard read only the segments that can hold the key.
--
-- Safety rules this case pins down:
--   * bytes are not collation order, so a text RANGE predicate must never prune;
--   * a non-deterministic collation ('ABC' = 'abc') makes byte bounds wrong, so equality there must not prune;
--   * Parquet truncates long string statistics (still valid bounds) -- keys over 64 bytes must be found;
--   * a segment with no index row for the key column (flushed before text keys had bounds) must stay a candidate.
--
-- Pruning is proved with koldstore.max_merge_seen_keys set below the table size: a full cold scan trips it.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE FUNCTION sqlreg.same_rows(managed regclass, control regclass, key text, preds text[]) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE p text; a text; b text; bad int := 0;
BEGIN
  FOREACH p IN ARRAY preds LOOP
    EXECUTE format('SELECT coalesce(string_agg(t::text, ''|'' ORDER BY %s COLLATE "C"), '''') FROM %s t WHERE %s', key, managed, p) INTO a;
    EXECUTE format('SELECT coalesce(string_agg(t::text, ''|'' ORDER BY %s COLLATE "C"), '''') FROM %s t WHERE %s', key, control, p) INTO b;
    IF a IS DISTINCT FROM b THEN
      bad := bad + 1;
      RAISE NOTICE 'mismatch: %', p;
    END IF;
  END LOOP;
  RETURN bad;
END
$$;

CREATE TABLE sqlreg.tk (k text PRIMARY KEY, n int NOT NULL, v text);
-- ordered keys (disjoint segment ranges), multibyte keys, a quote, the empty string, and keys past 64 bytes
INSERT INTO sqlreg.tk SELECT 'key-' || lpad(g::text, 6, '0'), g, 'v' || g FROM generate_series(1, 300) g;
INSERT INTO sqlreg.tk VALUES
  ('', 301, 'empty'), ('key-it''s', 302, 'quote'), ('ключ-ёж', 303, 'cyrillic'), ('鍵-漢字', 304, 'cjk'),
  ('key-' || repeat('x', 100) || '-a', 305, 'long a'), ('key-' || repeat('x', 100) || '-b', 306, 'long b');
CREATE TABLE sqlreg.tk_ctl AS SELECT * FROM sqlreg.tk;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.tk'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS tk_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS tk_caught_up;
SELECT sqlreg.flush_table('sqlreg.tk'::regclass) IS NOT NULL AS tk_flushed;
SELECT (SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.tk'::regclass::oid AND status = 'active') >= 14 AS tk_many_segments;
-- the key column has an index row in every segment, with text bounds
SELECT count(*) AS tk_index_rows FROM koldstore.cold_segment_index
WHERE table_oid = 'sqlreg.tk'::regclass::oid AND type_oid = 25 AND min_value IS NOT NULL;

SET koldstore.max_merge_seen_keys = 70;
-- the cap is in force: a full scan trips it
SELECT count(*) FROM sqlreg.tk;
-- equality and value lists prune
SELECT k FROM sqlreg.tk WHERE k = 'key-000007';
SELECT k FROM sqlreg.tk WHERE k = 'key-' || repeat('x', 100) || '-a';
SELECT k FROM sqlreg.tk WHERE k = 'ключ-ёж';
SELECT k FROM sqlreg.tk WHERE k = '鍵-漢字';
SELECT k FROM sqlreg.tk WHERE k = '';
SELECT k FROM sqlreg.tk WHERE k = 'key-it''s';
SELECT k FROM sqlreg.tk WHERE k = 'no such key';
SELECT k FROM sqlreg.tk WHERE k IN ('key-000007', 'key-000150') ORDER BY k;
SELECT k FROM sqlreg.tk WHERE k = ANY (ARRAY['key-000007', 'key-000250', 'absent']) ORDER BY k;
-- the INSERT guard prunes too
INSERT INTO sqlreg.tk VALUES ('key-brand-new', 400, 'fresh');
INSERT INTO sqlreg.tk_ctl VALUES ('key-brand-new', 400, 'fresh');
INSERT INTO sqlreg.tk VALUES ('key-000042', 401, 'dup');
RESET koldstore.max_merge_seen_keys;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS tk_fenced;

-- ranges are never pruned by bytes: results must still be exactly the control's (default cap)
SELECT sqlreg.same_rows('sqlreg.tk', 'sqlreg.tk_ctl', 'k', ARRAY[
  $$k > 'key-000290'$$, $$k < 'key-000010'$$, $$k BETWEEN 'key-000100' AND 'key-000110'$$,
  $$k >= '鍵'$$, $$k <= ''$$, $$k LIKE 'key-0001%'$$, $$k = 'key-000007'$$,
  $$k IN ('key-000007', 'key-000150', 'ключ-ёж')$$, $$k = 'key-000007' OR k = 'key-000300' OR k = 'zzz'$$,
  $$k NOT IN ('key-000007', 'key-000150')$$
]) AS tk_mismatches;

-- ------------------------------------------------ non-deterministic collation
-- Byte bounds would be wrong under a collation where 'KEY-1' = 'key-1'. A primary key can never have one:
-- manage_table refuses it, so a text key's equality is always byte equality.
CREATE COLLATION sqlreg.ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
CREATE TABLE sqlreg.tk_ci (k text COLLATE sqlreg.ci PRIMARY KEY, n int NOT NULL);
INSERT INTO sqlreg.tk_ci SELECT 'key-' || lpad(g::text, 6, '0'), g FROM generate_series(1, 50) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.tk_ci'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS ci_managed;

-- ------------------------------------------------ segments without an index row for the key stay candidates
DELETE FROM koldstore.cold_segment_index
WHERE table_oid = 'sqlreg.tk'::regclass::oid AND type_oid = 25
  AND segment_id IN (SELECT segment_id FROM koldstore.cold_segments
                      WHERE table_oid = 'sqlreg.tk'::regclass::oid AND status = 'active'
                      ORDER BY batch_number LIMIT 8);
SELECT sqlreg.same_rows('sqlreg.tk', 'sqlreg.tk_ctl', 'k', ARRAY[
  $$k = 'key-000007'$$, $$k = 'key-000150'$$, $$k IN ('key-000001', 'key-000070', 'key-000299')$$, $$k = 'nope'$$
]) AS unindexed_mismatches;
