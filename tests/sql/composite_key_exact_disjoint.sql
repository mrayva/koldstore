-- Exact disjointness for composite primary keys. Per-column bounds only give a box test, which stays
-- pessimistic when a leading value straddles two segments: here tenant 1 is split across segments with
-- overlapping id ranges, so the boxes intersect although no (tenant, id) is in two segments. The
-- segment's lexicographic key-tuple min/max (cold_segment_index.key_min / key_max) is exact: sorted by
-- minimum, each maximum is below the next minimum.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE FUNCTION sqlreg.same_content(managed regclass, control regclass, key text) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE a text; b text;
BEGIN
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', key, managed) INTO a;
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', key, control) INTO b;
  RETURN a = b;
END
$$;

-- tenant-major, 130 rows per tenant, segments of 100 rows in that order: tenants straddle segments
CREATE TABLE sqlreg.ex (tenant int NOT NULL, id bigint NOT NULL, n int NOT NULL, v text, PRIMARY KEY (tenant, id));
INSERT INTO sqlreg.ex SELECT (g - 1) / 130, ((g - 1) % 130) + 1, g, 'v' || g FROM generate_series(1, 650) g;
CREATE TABLE sqlreg.ex_ctl AS SELECT * FROM sqlreg.ex;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.ex'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 100, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS ex_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ex_caught_up;
SELECT sqlreg.flush_table('sqlreg.ex'::regclass) IS NOT NULL AS ex_flushed;
SELECT (SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.ex'::regclass::oid AND status = 'active') >= 6 AS ex_many_segments;

-- the first key column's index row carries the tuple bounds; the others do not
SELECT count(*) FILTER (WHERE key_min IS NOT NULL AND key_max IS NOT NULL) = count(*) FILTER (WHERE column_id = 1) AS ex_bounds_on_first_column_only
FROM koldstore.cold_segment_index WHERE table_oid = 'sqlreg.ex'::regclass::oid;

-- exact bounds: no key can be in two segments, so a full scan retains nothing
SET koldstore.max_merge_seen_keys = 50;
SELECT count(*) AS ex_count_under_cap FROM sqlreg.ex;
RESET koldstore.max_merge_seen_keys;
SELECT sqlreg.same_content('sqlreg.ex', 'sqlreg.ex_ctl', 'tenant, id') AS ex_content_equal;

-- without tuple bounds (segments written before they existed) the box test applies: it cannot tell
UPDATE koldstore.cold_segment_index SET key_min = NULL, key_max = NULL WHERE table_oid = 'sqlreg.ex'::regclass::oid;
\c
SET koldstore.max_merge_seen_keys = 50;
SELECT count(*) AS ex_count_without_tuple_bounds FROM sqlreg.ex;
RESET koldstore.max_merge_seen_keys;

-- rebuild_segment_summaries restores them from the objects
SELECT (koldstore.rebuild_segment_summaries('sqlreg.ex'::regclass, dry_run => true) -> 'tables' -> 0 -> 'key_bounds' ->> 'segments_without_key_bounds')::int > 0 AS ex_dry_run_sees_missing;
SELECT (koldstore.rebuild_segment_summaries('sqlreg.ex'::regclass) -> 'tables' -> 0 -> 'key_bounds' ->> 'rebuilt')::int > 0 AS ex_rebuilt;
SELECT (koldstore.rebuild_segment_summaries('sqlreg.ex'::regclass) -> 'tables' -> 0 -> 'key_bounds' ->> 'remaining')::int AS ex_remaining;
\c
SET koldstore.max_merge_seen_keys = 50;
SELECT count(*) AS ex_count_after_rebuild FROM sqlreg.ex;
RESET koldstore.max_merge_seen_keys;

-- a re-flush of edited rows makes two segments hold the same key: the exact check says overlapping
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.ex SET v = 'edited' WHERE n % 4 = 0;
UPDATE sqlreg.ex_ctl SET v = 'edited' WHERE n % 4 = 0;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ex_fenced;
SELECT sqlreg.flush_table('sqlreg.ex'::regclass) IS NOT NULL AS ex_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ex_fenced_2;
SET koldstore.max_merge_seen_keys = 50;
SELECT count(*) AS ex_count_overlapping_under_cap FROM sqlreg.ex;
SET koldstore.max_merge_seen_keys = 0;
SELECT count(*) AS ex_count_exact FROM sqlreg.ex;
RESET koldstore.max_merge_seen_keys;
SELECT sqlreg.same_content('sqlreg.ex', 'sqlreg.ex_ctl', 'tenant, id') AS ex_content_equal_after_reflush;

-- compaction publishes lexicographically disjoint segments, found disjoint by the exact check, and a
-- repeat has nothing to do
SELECT (koldstore.compact_table('sqlreg.ex'::regclass, grace_seconds => 3600) ->> 'compacted')::boolean AS ex_compacted;
SELECT sqlreg.same_content('sqlreg.ex', 'sqlreg.ex_ctl', 'tenant, id') AS ex_content_after_compaction;
\c
SET koldstore.max_merge_seen_keys = 50;
SELECT count(*) AS ex_count_after_compaction_under_cap FROM sqlreg.ex;
RESET koldstore.max_merge_seen_keys;
SELECT (koldstore.compact_table('sqlreg.ex'::regclass, grace_seconds => 3600) ->> 'compacted')::boolean AS ex_noop_second;
