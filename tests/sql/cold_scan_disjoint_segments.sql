-- A merged read keeps every cold key it has seen so an older version in another segment can be masked; that
-- costs ~86 bytes per key and trips koldstore.max_merge_seen_keys on large tables. When the single-column
-- primary key's min/max ranges of the active segments do not overlap, no key can be in two segments and the
-- cold keys are not retained at all (they are still checked against hot rows and tombstones).
--
-- With the cap set far below the number of cold rows: an append-only table must scan fully; once re-flushed
-- updates put the same key into two segments the exact (retaining) path must take over -- the scan trips the
-- cap again, and with the cap raised returns each key once, with its newest version.

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

CREATE TABLE sqlreg.ds (id bigint PRIMARY KEY, grp int NOT NULL, n int NOT NULL, v text);
INSERT INTO sqlreg.ds SELECT g, g % 7, g, 'v' || g FROM generate_series(1, 400) g;
CREATE TABLE sqlreg.ds_ctl AS SELECT * FROM sqlreg.ds;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.ds'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS ds_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ds_caught_up;
SELECT sqlreg.flush_table('sqlreg.ds'::regclass) IS NOT NULL AS ds_flushed;
SELECT (SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.ds'::regclass::oid AND status = 'active') >= 15 AS ds_many_segments;

-- ------------------------------------------------ append-only: segment key ranges are disjoint
SET koldstore.max_merge_seen_keys = 50;
SELECT count(*) AS ds_count_under_cap FROM sqlreg.ds;
SELECT count(*) AS ds_filtered_count_under_cap FROM sqlreg.ds WHERE grp = 3;
SELECT sum(n) AS ds_sum_under_cap FROM sqlreg.ds;
SELECT count(DISTINCT id) AS ds_distinct_under_cap FROM sqlreg.ds;
RESET koldstore.max_merge_seen_keys;
SELECT sqlreg.same_content('sqlreg.ds', 'sqlreg.ds_ctl', 'id') AS ds_content_equal;

-- hot rows and deletes still mask cold ones in the disjoint path
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.ds SET v = 'edited' WHERE id IN (3, 77, 205);
UPDATE sqlreg.ds_ctl SET v = 'edited' WHERE id IN (3, 77, 205);
DELETE FROM sqlreg.ds WHERE id BETWEEN 100 AND 104;
DELETE FROM sqlreg.ds_ctl WHERE id BETWEEN 100 AND 104;
INSERT INTO sqlreg.ds VALUES (1000, 1, 1000, 'fresh');
INSERT INTO sqlreg.ds_ctl VALUES (1000, 1, 1000, 'fresh');
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ds_fenced;
SET koldstore.max_merge_seen_keys = 50;
SELECT count(*) AS ds_count_after_dml_under_cap FROM sqlreg.ds;
RESET koldstore.max_merge_seen_keys;
SELECT sqlreg.same_content('sqlreg.ds', 'sqlreg.ds_ctl', 'id') AS ds_content_equal_after_dml;

-- ------------------------------------------------ re-flush the edited rows: keys now live in two segments
SELECT sqlreg.flush_table('sqlreg.ds'::regclass) IS NOT NULL AS ds_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ds_fenced_2;
-- the exact path is back: the cap trips on a full scan ...
SET koldstore.max_merge_seen_keys = 50;
SELECT count(*) AS ds_count_overlapping_under_cap FROM sqlreg.ds;
-- ... and with the cap raised each key is returned once, newest version first
SET koldstore.max_merge_seen_keys = 0;
SELECT count(*) AS ds_count_exact, count(DISTINCT id) AS ds_distinct_exact FROM sqlreg.ds;
SELECT v AS edited_value FROM sqlreg.ds WHERE id = 77;
RESET koldstore.max_merge_seen_keys;
SELECT sqlreg.same_content('sqlreg.ds', 'sqlreg.ds_ctl', 'id') AS ds_content_equal_after_reflush;
