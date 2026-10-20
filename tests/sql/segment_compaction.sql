-- Compaction rewrites overlapping cold segments (older versions of re-flushed keys, delete markers) so each
-- key has one version in one segment, packs runs of small segments, and keeps the replaced objects until a
-- grace period has passed. Content must match an unmanaged control at every step.

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

CREATE TABLE sqlreg.cp (id bigint PRIMARY KEY, grp int NOT NULL, n int NOT NULL, v text);
INSERT INTO sqlreg.cp SELECT g, g % 7, g, 'v' || g FROM generate_series(1, 400) g;
CREATE TABLE sqlreg.cp_ctl AS SELECT * FROM sqlreg.cp;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.cp'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS cp_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cp_caught_up;
SELECT sqlreg.flush_table('sqlreg.cp'::regclass) IS NOT NULL AS cp_flushed;

-- nothing overlaps and every segment is full: compaction has nothing to do
SELECT (koldstore.compact_table('sqlreg.cp'::regclass, grace_seconds => 3600) ->> 'compacted')::boolean AS cp_noop_first;

-- updates and deletes of flushed rows, flushed again: the same keys now live in two segments
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.cp SET v = 'edited' WHERE id IN (3, 77, 205, 399);
UPDATE sqlreg.cp_ctl SET v = 'edited' WHERE id IN (3, 77, 205, 399);
DELETE FROM sqlreg.cp WHERE id BETWEEN 100 AND 104;
DELETE FROM sqlreg.cp_ctl WHERE id BETWEEN 100 AND 104;
INSERT INTO sqlreg.cp VALUES (1000, 1, 1000, 'fresh');
INSERT INTO sqlreg.cp_ctl VALUES (1000, 1, 1000, 'fresh');
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cp_fenced;
SELECT sqlreg.flush_table('sqlreg.cp'::regclass) IS NOT NULL AS cp_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cp_fenced_2;
SELECT sqlreg.same_content('sqlreg.cp', 'sqlreg.cp_ctl', 'id') AS cp_content_before;
CREATE TEMP TABLE cp_before AS
SELECT count(*) AS segs, sum(row_count) AS rows
FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.cp'::regclass::oid AND status = 'active';

-- overlapping segments make the merge retain keys, so a tiny cap trips on a full scan
SET koldstore.max_merge_seen_keys = 50;
SELECT count(*) AS cp_count_overlapping_under_cap FROM sqlreg.cp;
RESET koldstore.max_merge_seen_keys;

-- ------------------------------------------------ compact
CREATE TEMP TABLE cp_report AS SELECT koldstore.compact_table('sqlreg.cp'::regclass, grace_seconds => 3600) AS r;
SELECT (r ->> 'compacted')::boolean AS cp_compacted,
       (r ->> 'units')::int >= 1 AS cp_has_units,
       (r ->> 'rows_written')::int < (r ->> 'rows_read')::int AS cp_dropped_old_versions,
       (r ->> 'segments_written')::int <= (r ->> 'segments_replaced')::int AS cp_fewer_segments
FROM cp_report;
SELECT sqlreg.same_content('sqlreg.cp', 'sqlreg.cp_ctl', 'id') AS cp_content_after;
SELECT (SELECT sum(row_count) FROM koldstore.cold_segments
         WHERE table_oid = 'sqlreg.cp'::regclass::oid AND status = 'active') < (SELECT rows FROM cp_before) AS cp_rows_shrank;
SELECT (SELECT count(*) FROM koldstore.cold_segments
         WHERE table_oid = 'sqlreg.cp'::regclass::oid AND status = 'compacted') > 0 AS cp_retired_rows_kept;

-- the ranges are disjoint again: a full scan no longer retains keys, so the tiny cap does not trip
SET koldstore.max_merge_seen_keys = 50;
SELECT count(*) AS cp_count_after_under_cap FROM sqlreg.cp;
RESET koldstore.max_merge_seen_keys;
SELECT v AS cp_edited_value FROM sqlreg.cp WHERE id = 77;
SELECT count(*) AS cp_deleted_rows_still_gone FROM sqlreg.cp WHERE id BETWEEN 100 AND 104;

SELECT koldstore.validate_cold_storage('sqlreg.cp'::regclass) ->> 'ok' AS cp_validate_inside_grace;

-- a second call finds nothing; the retired segments are inside their grace period and stay
SELECT (koldstore.compact_table('sqlreg.cp'::regclass, grace_seconds => 3600) ->> 'compacted')::boolean AS cp_noop_second;
SELECT koldstore.reap_compacted_segments('sqlreg.cp'::regclass, 3600) AS cp_reaped_inside_grace;
SELECT (SELECT count(*) FROM koldstore.cold_segments
         WHERE table_oid = 'sqlreg.cp'::regclass::oid AND status = 'compacted') > 0 AS cp_still_retired;

-- past the grace period the objects and rows go; nothing is left orphaned
SELECT koldstore.reap_compacted_segments('sqlreg.cp'::regclass, 0) > 0 AS cp_reaped;
SELECT count(*) AS cp_retired_left FROM koldstore.cold_segments
 WHERE table_oid = 'sqlreg.cp'::regclass::oid AND status = 'compacted';
SELECT koldstore.recover_segments('sqlreg.cp'::regclass, true) AS cp_orphans;
SELECT koldstore.validate_cold_storage('sqlreg.cp'::regclass) ->> 'ok' AS cp_validate_after_reap;
SELECT sqlreg.same_content('sqlreg.cp', 'sqlreg.cp_ctl', 'id') AS cp_content_after_reap;

-- writes and flushes carry on over the compacted layout
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.cp SET v = 'again' WHERE id IN (5, 6, 300);
UPDATE sqlreg.cp_ctl SET v = 'again' WHERE id IN (5, 6, 300);
INSERT INTO sqlreg.cp VALUES (1001, 2, 1001, 'late');
INSERT INTO sqlreg.cp_ctl VALUES (1001, 2, 1001, 'late');
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cp_fenced_3;
SELECT sqlreg.flush_table('sqlreg.cp'::regclass) IS NOT NULL AS cp_reflushed_2;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cp_fenced_4;
SELECT sqlreg.same_content('sqlreg.cp', 'sqlreg.cp_ctl', 'id') AS cp_content_final;
SELECT (koldstore.compact_table('sqlreg.cp'::regclass, grace_seconds => 0) ->> 'compacted')::boolean AS cp_compacted_again;
SELECT sqlreg.same_content('sqlreg.cp', 'sqlreg.cp_ctl', 'id') AS cp_content_after_second_compaction;

-- ------------------------------------------------ refusals
CREATE TABLE sqlreg.cp_comp (a int, b int, v text, PRIMARY KEY (a, b));
SELECT koldstore.manage_table(table_name => 'sqlreg.cp_comp'::regclass, storage => 'sqlreg_fs',
  hot_row_limit => 5, min_flush_rows => 1, auto_flush => false) IS NOT NULL AS comp_managed;
SELECT koldstore.compact_table('sqlreg.cp_comp'::regclass);
SELECT koldstore.compact_table('sqlreg.cp'::regclass, max_rows => 0);
-- ordered by a column other than the key: still deduplicates, sorted by that column
CREATE TABLE sqlreg.cp_ord (id bigint PRIMARY KEY, n int NOT NULL, v text);
INSERT INTO sqlreg.cp_ord SELECT g, 1000 - g, 'v' || g FROM generate_series(1, 120) g;
CREATE TABLE sqlreg.cp_ord_ctl AS SELECT * FROM sqlreg.cp_ord;
SELECT koldstore.manage_table(table_name => 'sqlreg.cp_ord'::regclass, storage => 'sqlreg_fs',
  hot_row_limit => 5, min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n',
  auto_flush => false) IS NOT NULL AS ord_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ord_caught_up;
SELECT sqlreg.flush_table('sqlreg.cp_ord'::regclass) IS NOT NULL AS ord_flushed;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.cp_ord SET v = 'edited' WHERE id IN (2, 50, 118);
UPDATE sqlreg.cp_ord_ctl SET v = 'edited' WHERE id IN (2, 50, 118);
DELETE FROM sqlreg.cp_ord WHERE id IN (60, 61);
DELETE FROM sqlreg.cp_ord_ctl WHERE id IN (60, 61);
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ord_fenced;
SELECT sqlreg.flush_table('sqlreg.cp_ord'::regclass) IS NOT NULL AS ord_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ord_fenced_2;
SELECT (koldstore.compact_table('sqlreg.cp_ord'::regclass, grace_seconds => 0) ->> 'compacted')::boolean AS ord_compacted;
SELECT sqlreg.same_content('sqlreg.cp_ord', 'sqlreg.cp_ord_ctl', 'id') AS ord_content_after;
-- a repeat finds no more gain instead of rewriting the same layout
SELECT (koldstore.compact_table('sqlreg.cp_ord'::regclass, grace_seconds => 0) ->> 'compacted')::boolean AS ord_second_noop;
SELECT id FROM sqlreg.cp_ord ORDER BY n LIMIT 3;
SELECT koldstore.recover_segments('sqlreg.cp_ord'::regclass, true) AS ord_orphans;

-- ------------------------------------------------ a text key: overlapping segments merge, content survives
CREATE TABLE sqlreg.cp_txt (k text PRIMARY KEY, n int NOT NULL, v int NOT NULL);
INSERT INTO sqlreg.cp_txt SELECT 'key' || lpad(g::text, 4, '0'), g, g FROM generate_series(1, 200) g;
CREATE TABLE sqlreg.cp_txt_ctl AS SELECT * FROM sqlreg.cp_txt;
SELECT koldstore.manage_table(table_name => 'sqlreg.cp_txt'::regclass, storage => 'sqlreg_fs',
  hot_row_limit => 5, min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n',
  auto_flush => false) IS NOT NULL AS txt_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS txt_caught_up;
SELECT sqlreg.flush_table('sqlreg.cp_txt'::regclass) IS NOT NULL AS txt_flushed;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.cp_txt SET v = v + 1000 WHERE k IN ('key0003', 'key0090', 'key0150');
UPDATE sqlreg.cp_txt_ctl SET v = v + 1000 WHERE k IN ('key0003', 'key0090', 'key0150');
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS txt_fenced;
SELECT sqlreg.flush_table('sqlreg.cp_txt'::regclass) IS NOT NULL AS txt_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS txt_fenced_2;
SELECT (koldstore.compact_table('sqlreg.cp_txt'::regclass, grace_seconds => 0) ->> 'compacted')::boolean AS txt_compacted;
SELECT sqlreg.same_content('sqlreg.cp_txt', 'sqlreg.cp_txt_ctl', 'k') AS txt_content_after;
SET koldstore.max_merge_seen_keys = 20;
SELECT count(*) AS txt_count_under_cap FROM sqlreg.cp_txt;
RESET koldstore.max_merge_seen_keys;
