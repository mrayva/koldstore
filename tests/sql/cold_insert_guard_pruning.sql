-- The INSERT guard asks "does this key already exist, hot or cold?" once per inserted row. That lookup
-- used to join the managed table against a jsonb_populate_record CTE, which hides the key from the
-- planner, so every INSERT read the whole cold tier (seconds per row at millions of cold rows, and an
-- error once the scan retained more than koldstore.max_merge_seen_keys identities). It is now a typed
-- constant equality, so cold segments are pruned by their key bounds, bloom filters and value summaries.
--
-- (A table whose segment key ranges are disjoint scans without retaining keys, so the cap would never trip on
-- it; each table below first re-flushes one updated key, which puts that key into two segments and makes the
-- exact, key-retaining path -- and therefore the cap -- apply.)
--
-- Test: with the identity cap set below the number of cold rows (but above one segment), a full cold
-- scan trips it, so an INSERT only works if the guard pruned. That holds for the key types the merge
-- scan can prune on (integers, uuid, date/timestamp). text and numeric keys have no segment bounds yet and
-- composite keys prune only when every part does, so for those the case checks correctness only: new keys
-- insert and an existing cold key is refused, under the default cap.

\set VERBOSITY terse
\set ON_ERROR_STOP off

-- ------------------------------------------------ bigint
CREATE TABLE sqlreg.cig_big (id bigint PRIMARY KEY, n int NOT NULL, v text);
INSERT INTO sqlreg.cig_big SELECT g, g, 'v' || g FROM generate_series(1, 200) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.cig_big'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS big_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS big_caught_up;
SELECT sqlreg.flush_table('sqlreg.cig_big'::regclass) IS NOT NULL AS big_flushed;
SELECT (koldstore.table_status('sqlreg.cig_big'::regclass) ->> 'cold_row_count')::int >= 150 AS big_mostly_cold;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.cig_big SET v = 'edited' WHERE id = 7;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS big_fenced;
SELECT sqlreg.flush_table('sqlreg.cig_big'::regclass) IS NOT NULL AS big_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS big_fenced_2;
SET koldstore.max_merge_seen_keys = 30;
-- the cap is really in force: a full cold scan trips it
SELECT count(*) FROM sqlreg.cig_big;
INSERT INTO sqlreg.cig_big VALUES (201, 201, 'new');
INSERT INTO sqlreg.cig_big VALUES (-1, 0, 'below every segment');
INSERT INTO sqlreg.cig_big VALUES (100000, 0, 'above every segment');
INSERT INTO sqlreg.cig_big SELECT g, g, 'batch' FROM generate_series(300, 340) g;
-- existing cold keys are still refused
INSERT INTO sqlreg.cig_big VALUES (7, 7, 'dup');
INSERT INTO sqlreg.cig_big VALUES (150, 150, 'dup');
RESET koldstore.max_merge_seen_keys;
SELECT count(*) AS big_total FROM sqlreg.cig_big;

-- ------------------------------------------------ text key with quotes, composite key, uuid, timestamptz, numeric
CREATE TABLE sqlreg.cig_txt (k text PRIMARY KEY, n int NOT NULL);
INSERT INTO sqlreg.cig_txt SELECT 'key ''' || g || '"\', g FROM generate_series(1, 80) g;
CREATE TABLE sqlreg.cig_cmp (a int, b text, n int NOT NULL, PRIMARY KEY (a, b));
INSERT INTO sqlreg.cig_cmp SELECT g % 8, 'b' || g, g FROM generate_series(1, 80) g;
CREATE TABLE sqlreg.cig_uuid (u uuid PRIMARY KEY, n int NOT NULL, v text);
INSERT INTO sqlreg.cig_uuid SELECT ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid, g, 'v' FROM generate_series(1, 80) g;
CREATE TABLE sqlreg.cig_ts (tm timestamptz PRIMARY KEY, n int NOT NULL, v text);
INSERT INTO sqlreg.cig_ts SELECT timestamptz '2026-01-01 00:00:00+00' + g * interval '1 hour 7 minutes 3.5 seconds', g, 'v' FROM generate_series(1, 80) g;
CREATE TABLE sqlreg.cig_num (x numeric(12, 2) PRIMARY KEY, n int NOT NULL);
INSERT INTO sqlreg.cig_num SELECT g * 1.25, g FROM generate_series(1, 80) g;
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['cig_txt', 'cig_cmp', 'cig_uuid', 'cig_ts', 'cig_num'] LOOP
    PERFORM koldstore.manage_table(
      table_name => ('sqlreg.' || t)::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
      min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false);
  END LOOP;
END $$;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS others_caught_up;
SELECT sqlreg.flush_table('sqlreg.cig_txt'::regclass) IS NOT NULL AS txt_flushed,
       sqlreg.flush_table('sqlreg.cig_cmp'::regclass) IS NOT NULL AS cmp_flushed,
       sqlreg.flush_table('sqlreg.cig_uuid'::regclass) IS NOT NULL AS uuid_flushed,
       sqlreg.flush_table('sqlreg.cig_ts'::regclass) IS NOT NULL AS ts_flushed,
       sqlreg.flush_table('sqlreg.cig_num'::regclass) IS NOT NULL AS num_flushed;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.cig_uuid SET v = 'edited' WHERE u = '00000000-0000-0000-0000-000000000003';
UPDATE sqlreg.cig_ts SET v = 'edited' WHERE n = 3;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS others_fenced;
SELECT sqlreg.flush_table('sqlreg.cig_uuid'::regclass) IS NOT NULL AS uuid_reflushed,
       sqlreg.flush_table('sqlreg.cig_ts'::regclass) IS NOT NULL AS ts_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS others_fenced_2;
-- correctness of the typed lookup for keys that cannot prune yet (default cap)
INSERT INTO sqlreg.cig_txt VALUES ('brand new', 1000);
INSERT INTO sqlreg.cig_txt VALUES ('key ''7"\', 1001);
INSERT INTO sqlreg.cig_cmp VALUES (3, 'fresh', 1000);
INSERT INTO sqlreg.cig_cmp VALUES (3, 'b11', 1001);
INSERT INTO sqlreg.cig_num VALUES (9999.99, 1000);
INSERT INTO sqlreg.cig_num VALUES (12.50, 1001);
-- keys that prune: a tiny cap would trip on a full cold scan of the 80 rows
SET koldstore.max_merge_seen_keys = 30;
INSERT INTO sqlreg.cig_uuid VALUES ('00000000-0000-0000-0000-0000000000ff', 1000, 'x');
INSERT INTO sqlreg.cig_uuid VALUES ('00000000-0000-0000-0000-000000000009', 1001, 'x');
INSERT INTO sqlreg.cig_ts VALUES ('2030-01-01 00:00:00+00', 1000, 'x');
INSERT INTO sqlreg.cig_ts SELECT tm, 1001, 'x' FROM (SELECT timestamptz '2026-01-01 00:00:00+00' + 5 * interval '1 hour 7 minutes 3.5 seconds' AS tm) s;
RESET koldstore.max_merge_seen_keys;
SELECT (SELECT count(*) FROM sqlreg.cig_txt) AS txt_total, (SELECT count(*) FROM sqlreg.cig_cmp) AS cmp_total,
       (SELECT count(*) FROM sqlreg.cig_uuid) AS uuid_total, (SELECT count(*) FROM sqlreg.cig_ts) AS ts_total,
       (SELECT count(*) FROM sqlreg.cig_num) AS num_total;
