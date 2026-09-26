-- Generic cold-match guard (upstream #122): an UPDATE/DELETE whose WHERE clause
-- also matches rows that exist only in cold storage is rejected, whatever the
-- shape of the clause (range, NOT IN, OR across columns, non-key column,
-- function, no WHERE at all). The exact-primary-key guard covers equality
-- shapes; this one is the fallback for everything else.
--
-- try() reports "REJECTED scan matches=<n>" for a rejection by this guard.

\set VERBOSITY terse

-- try(stmt): "ok: <rows>" | "REJECTED insert" | "REJECTED write pk=<key>" | "ERROR: ..."
-- try(stmt, true): same, but a koldstore rejection keeps its complete message.
CREATE FUNCTION sqlreg.try(stmt text, keep_message boolean DEFAULT false) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  n bigint;
  msg text;
BEGIN
  EXECUTE stmt;
  IF stmt ~* '^\s*explain' THEN
    RETURN 'ok (explain)';
  END IF;
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN 'ok: ' || n;
EXCEPTION WHEN OTHERS THEN
  msg := split_part(SQLERRM, E'\n', 1);
  IF keep_message THEN
    RETURN 'ERROR: ' || msg;
  ELSIF msg LIKE 'koldstore: refusing INSERT%' THEN
    RETURN 'REJECTED insert';
  ELSIF msg LIKE 'koldstore: refusing this UPDATE/DELETE on managed table%' THEN
    RETURN 'REJECTED scan matches=' || substring(msg from 'also matches ([0-9]+) cold');
  ELSIF msg LIKE 'koldstore: refusing this MERGE on managed table%' THEN
    RETURN 'REJECTED merge';
  ELSIF msg LIKE 'koldstore: refusing this UPDATE/DELETE/MERGE%' THEN
    RETURN 'REJECTED write pk=' || substring(msg from 'primary key (\{[^}]*\})');
  END IF;
  RETURN 'ERROR: ' || msg;
END
$$;

-- Committed mirror work applied, so reads below see completed masking.
CREATE FUNCTION sqlreg.settle() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM koldstore.wait_for_async_mirror();
END
$$;

-- ---------------------------------------------------------------- fixture
CREATE TABLE sqlreg.s1 (id bigint PRIMARY KEY, val text NOT NULL, grp int NOT NULL);
INSERT INTO sqlreg.s1 SELECT g, 'v' || g, g % 2 FROM generate_series(1, 10) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.s1'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS managed;
SELECT sqlreg.flush_table('sqlreg.s1'::regclass) IS NOT NULL AS flushed;
SELECT sqlreg.settle();
-- ids 1..10 are cold-only; 11 and 12 are hot
INSERT INTO sqlreg.s1 VALUES (11, 'h11', 1), (12, 'h12', 0);
SELECT sqlreg.settle();

-- ------------------------------------------------ primary-key range shapes
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 WHERE id BETWEEN 1 AND 2$$) AS delete_between;
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET val = 'x' WHERE id < 3$$) AS update_less_than;
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET val = 'x' WHERE id > 8$$) AS update_range_spanning_cold_and_hot;
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET val = 'x' WHERE id > 100$$) AS update_range_matching_nothing;
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 WHERE id <> 3$$) AS delete_not_equal;

-- --------------------------------------------------------- NOT IN / OR / NOT
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 WHERE id NOT IN (5, 6)$$) AS delete_not_in;
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 WHERE id = 1 OR val = 'v2'$$) AS delete_or_across_columns;
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 WHERE NOT (id = 4)$$) AS delete_not_equal_negated;

-- ------------------------------------------------- non-key columns / functions
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET grp = 9 WHERE val = 'v3'$$) AS update_by_value_cold;
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET grp = 9 WHERE val = 'nothing'$$) AS update_by_value_nothing;
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET grp = 9 WHERE lower(val) = 'v3'$$) AS update_by_function;
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET grp = 9 WHERE val LIKE 'v%'$$) AS update_like_cold;
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET grp = 9 WHERE grp = 1$$) AS update_by_low_cardinality_column;
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET grp = 9$$) AS update_without_where;
SELECT sqlreg.try($$DELETE FROM sqlreg.s1$$) AS delete_without_where;
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET grp = 9 WHERE true$$) AS update_where_true;

CREATE TABLE sqlreg.s_merge_src (id bigint PRIMARY KEY, val text NOT NULL);
INSERT INTO sqlreg.s_merge_src VALUES (1, 'a'), (2, 'b');
CREATE TABLE sqlreg.s2 (id bigint PRIMARY KEY, val text NOT NULL);
SELECT koldstore.manage_table(
  table_name => 'sqlreg.s2'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS managed_hot_only;
INSERT INTO sqlreg.s2 SELECT g, 'h' || g FROM generate_series(1, 5) g;

-- ------------------------------------------------------- bound parameters
PREPARE upd_by_val(text) AS UPDATE sqlreg.s1 SET grp = 9 WHERE val = $1;
SELECT sqlreg.try($$EXECUTE upd_by_val('v4')$$) AS prepared_cold_value;
SELECT sqlreg.try($$EXECUTE upd_by_val('nothing')$$) AS prepared_no_match;
PREPARE del_range(bigint) AS DELETE FROM sqlreg.s1 WHERE id >= $1;
SELECT sqlreg.try($$EXECUTE del_range(5)$$) AS prepared_range_cold;

-- Statements that touch only hot rows keep working and change exactly those.
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET val = 'h11b' WHERE id >= 11 AND val LIKE 'h%'$$) AS update_hot_only;
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET grp = 7 WHERE val LIKE 'h%'$$) AS update_hot_by_prefix;
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 WHERE id NOT IN (1,2,3,4,5,6,7,8,9,10)$$) AS delete_all_hot_via_not_in;
SELECT id, val, grp FROM sqlreg.s1 ORDER BY id;

-- ------------------------------------------------------------------- MERGE
-- A MERGE that changes target rows through a multi-row join can only match hot
-- rows, and the source keys are gone once it ends, so it is refused when the
-- table has cold data.
SELECT sqlreg.try($$MERGE INTO sqlreg.s1 t USING (VALUES (1, 'a'), (2, 'b')) v(id, val) ON t.id = v.id
  WHEN MATCHED THEN UPDATE SET val = v.val$$) AS merge_update_multirow_source;
SELECT sqlreg.try($$MERGE INTO sqlreg.s1 t USING (VALUES (1, 'a'), (2, 'b')) v(id, val) ON t.id = v.id
  WHEN MATCHED THEN DELETE$$) AS merge_delete_multirow_source;
SELECT sqlreg.try($$MERGE INTO sqlreg.s1 t USING (VALUES (1, 'a'), (2, 'b')) v(id, val) ON t.id = v.id
  WHEN MATCHED THEN UPDATE SET val = v.val WHEN NOT MATCHED THEN INSERT VALUES (v.id, v.val, 0)$$) AS merge_upsert_multirow_source;
SELECT sqlreg.try($$MERGE INTO sqlreg.s1 t USING sqlreg.s_merge_src v ON t.id = v.id
  WHEN MATCHED THEN UPDATE SET val = v.val$$) AS merge_update_from_table;
-- inserting keys that exist nowhere, or doing nothing when matched, is fine
SELECT sqlreg.try($$MERGE INTO sqlreg.s1 t USING (VALUES (50, 'n1'), (51, 'n2')) v(id, val) ON t.id = v.id
  WHEN NOT MATCHED THEN INSERT VALUES (v.id, v.val, 0)$$) AS merge_insert_only_new_keys;
SELECT sqlreg.try($$MERGE INTO sqlreg.s1 t USING (VALUES (1, 'a'), (52, 'n3')) v(id, val) ON t.id = v.id
  WHEN NOT MATCHED THEN INSERT VALUES (v.id, v.val, 0)$$) AS merge_insert_only_cold_key;
SELECT sqlreg.try($$MERGE INTO sqlreg.s1 t USING (VALUES (60, 'n1'), (61, 'n2')) v(id, val) ON t.id = v.id
  WHEN MATCHED THEN DO NOTHING WHEN NOT MATCHED THEN INSERT VALUES (v.id, v.val, 0)$$) AS merge_do_nothing_and_insert;
-- a single-row source still goes through the exact primary-key guard
SELECT sqlreg.try($$MERGE INTO sqlreg.s1 t USING (VALUES (3, 'z')) v(id, val) ON t.id = v.id
  WHEN MATCHED THEN UPDATE SET val = v.val$$) AS merge_update_single_row_cold_key;
-- the hot rows written above are ordinary hot rows: matched merges on them work
-- for a table without cold data
SELECT sqlreg.try($$MERGE INTO sqlreg.s2 t USING (VALUES (1, 'a'), (2, 'b')) v(id, val) ON t.id = v.id
  WHEN MATCHED THEN UPDATE SET val = v.val$$) AS merge_hot_only_table;

-- --------------------------------------------------------------- the switch
SET koldstore.guard_scan_writes = off;
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 WHERE id BETWEEN 1 AND 2$$) AS delete_between_guard_off;
RESET koldstore.guard_scan_writes;
SELECT count(*) FILTER (WHERE id <= 10) AS cold_rows_still_there FROM sqlreg.s1;

-- ------------------------------------------- deliberately unguarded (skipped)
-- After a write to the table in the same transaction the probe cannot be
-- trusted (the async mirror has not seen the transaction's own work), so it is
-- skipped rather than risking a false rejection.
BEGIN;
INSERT INTO sqlreg.s1 VALUES (13, 'h13', 0);
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 WHERE id BETWEEN 1 AND 2$$) AS delete_between_after_write_in_txn;
ROLLBACK;

-- ------------------------------------------------- joins and sub-queries
-- The statement's own FROM/USING/sub-query conditions are turned into a probe
-- SELECT at plan time, so cold-only target rows a join would have matched are
-- detected the same way as for a single-table WHERE.
CREATE TABLE sqlreg.s_other (id bigint PRIMARY KEY);
INSERT INTO sqlreg.s_other VALUES (1), (2);
CREATE TABLE sqlreg.s_hot (id bigint PRIMARY KEY);
INSERT INTO sqlreg.s_hot VALUES (50), (51);
CREATE TABLE sqlreg.s_none (id bigint PRIMARY KEY);

SELECT sqlreg.try($$DELETE FROM sqlreg.s1 USING sqlreg.s_other o WHERE s1.id = o.id$$) AS delete_using_cold;
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET val = 'x' FROM sqlreg.s_other o WHERE s1.id = o.id$$) AS update_from_cold;
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 WHERE id IN (SELECT id FROM sqlreg.s_other)$$) AS delete_in_subquery_cold;
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 WHERE id NOT IN (SELECT id FROM sqlreg.s_other)$$) AS delete_not_in_subquery;
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET val = 'y' WHERE EXISTS (SELECT 1 FROM sqlreg.s_other o WHERE o.id = s1.id)$$) AS update_exists_cold;
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 a USING sqlreg.s1 b WHERE a.id = b.id AND b.id < 3$$) AS delete_self_join_cold;
-- joins that only reach hot rows, or nothing, keep working
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET val = 'hot-joined' FROM sqlreg.s_hot h WHERE s1.id = h.id$$) AS update_from_hot_only;
SELECT sqlreg.try($$UPDATE sqlreg.s1 SET val = 'hot-in' WHERE id IN (SELECT id FROM sqlreg.s_hot)$$) AS update_in_subquery_hot_only;
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 USING sqlreg.s_none n WHERE s1.id = n.id$$) AS delete_using_empty_source;
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 WHERE id IN (SELECT id FROM sqlreg.s_none)$$) AS delete_in_empty_subquery;

-- the probe survives plan caching, bound parameters and reuse
SET plan_cache_mode = force_generic_plan;
PREPARE del_in_sub(bigint) AS DELETE FROM sqlreg.s1 WHERE id IN (SELECT id FROM sqlreg.s_other WHERE id >= $1);
SELECT sqlreg.try($$EXECUTE del_in_sub(1)$$) AS prepared_join_cold_first;
SELECT sqlreg.try($$EXECUTE del_in_sub(1)$$) AS prepared_join_cold_second;
SELECT sqlreg.try($$EXECUTE del_in_sub(100)$$) AS prepared_join_no_match;
SELECT sqlreg.try($$EXECUTE del_in_sub(2)$$) AS prepared_join_cold_other_param;
RESET plan_cache_mode;

-- EXPLAIN builds the plan (carrying the probe) without changing anything and
-- prints nothing extra; EXPLAIN ANALYZE runs the statement, so it is guarded too
SELECT sqlreg.try($$EXPLAIN (COSTS OFF) DELETE FROM sqlreg.s1 USING sqlreg.s_other o WHERE s1.id = o.id$$) AS explain_delete_using;
CREATE FUNCTION sqlreg.explain_text(stmt text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE line text; acc text := '';
BEGIN
  FOR line IN EXECUTE 'EXPLAIN (VERBOSE, COSTS OFF) ' || stmt LOOP acc := acc || line || E'\n'; END LOOP;
  RETURN acc;
END $$;
SELECT position('koldstore' IN sqlreg.explain_text($$DELETE FROM sqlreg.s1 USING sqlreg.s_other o WHERE s1.id = o.id$$)) AS explain_mentions_probe_at;
BEGIN;
SELECT sqlreg.try($$EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) DELETE FROM sqlreg.s1 USING sqlreg.s_other o WHERE s1.id = o.id$$) AS explain_analyze_delete_using;
ROLLBACK;

-- a second managed table with cold data as the source: read hot + cold too
CREATE TABLE sqlreg.s4 (id bigint PRIMARY KEY);
INSERT INTO sqlreg.s4 SELECT g FROM generate_series(1, 4) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.s4'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS s4_managed;
SELECT sqlreg.flush_table('sqlreg.s4'::regclass) IS NOT NULL AS s4_flushed;
SELECT sqlreg.settle();
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 USING sqlreg.s4 x WHERE s1.id = x.id$$) AS delete_using_cold_source_table;
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 WHERE id IN (SELECT id FROM sqlreg.s4 WHERE id > 100)$$) AS delete_in_cold_source_no_match;

-- the switch and the same-transaction rule apply here too
SET koldstore.guard_scan_writes = off;
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 USING sqlreg.s_other o WHERE s1.id = o.id$$) AS delete_using_guard_off;
RESET koldstore.guard_scan_writes;
BEGIN;
INSERT INTO sqlreg.s1 VALUES (70, 'h70', 0);
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 USING sqlreg.s_other o WHERE s1.id = o.id$$) AS delete_using_after_write_in_txn;
ROLLBACK;

-- still unguarded: statements whose conditions cannot be re-run faithfully
SELECT sqlreg.try($$DELETE FROM sqlreg.s1 WHERE id IN (SELECT id FROM sqlreg.s_other WHERE random() < 2)$$) AS gap_volatile_subquery;
SELECT sqlreg.try($$WITH c AS (SELECT id FROM sqlreg.s_other) DELETE FROM sqlreg.s1 USING c WHERE s1.id = c.id$$) AS gap_cte_source;

-- UPDATE/DELETE inside a data-modifying CTE: the top-level statement is a SELECT,
-- so the guard inspects the sub-plan's ModifyTable nodes
SELECT sqlreg.try($$WITH d AS (DELETE FROM sqlreg.s1 WHERE id BETWEEN 1 AND 2 RETURNING id) SELECT count(*) FROM d$$) AS cte_delete_cold_range;
SELECT sqlreg.try($$WITH u AS (UPDATE sqlreg.s1 SET val = 'cte' WHERE val = 'v3' RETURNING id) SELECT count(*) FROM u$$) AS cte_update_cold_value;
SELECT sqlreg.try($$WITH d AS (DELETE FROM sqlreg.s1 WHERE id > 1000 RETURNING id) SELECT count(*) FROM d$$) AS cte_delete_matching_nothing;
SELECT sqlreg.try($$WITH d AS (DELETE FROM sqlreg.s1 WHERE id IN (50, 51) RETURNING id) SELECT count(*) FROM d$$) AS cte_delete_hot_rows;

-- ------------------------------------------------ tables without cold data
SELECT sqlreg.try($$UPDATE sqlreg.s2 SET val = 'z' WHERE id < 4$$) AS hot_only_range;
SELECT sqlreg.try($$DELETE FROM sqlreg.s2$$) AS hot_only_delete_all;

-- ------------------------------------------------------- composite primary key
CREATE TABLE sqlreg.s3 (a int, b int, val text, PRIMARY KEY (a, b));
INSERT INTO sqlreg.s3 SELECT g, g * 10, 'w' || g FROM generate_series(1, 4) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.s3'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'a', auto_flush => false
) IS NOT NULL AS managed_composite;
SELECT sqlreg.flush_table('sqlreg.s3'::regclass) IS NOT NULL AS flushed_composite;
SELECT sqlreg.settle();
SELECT sqlreg.try($$UPDATE sqlreg.s3 SET val = 'x' WHERE a > 1$$) AS composite_leading_range;
SELECT sqlreg.try($$UPDATE sqlreg.s3 SET val = 'x' WHERE b >= 30$$) AS composite_second_column_range;
SELECT sqlreg.try($$DELETE FROM sqlreg.s3 WHERE a = 2$$) AS composite_partial_key;
SELECT sqlreg.try($$DELETE FROM sqlreg.s3 WHERE a = 99$$) AS composite_partial_key_no_match;
SELECT sqlreg.try($$UPDATE sqlreg.s3 SET val = 'x' FROM sqlreg.s_other o WHERE s3.a = o.id$$) AS composite_update_from_cold;
SELECT sqlreg.try($$DELETE FROM sqlreg.s3 WHERE a IN (SELECT id FROM sqlreg.s_none)$$) AS composite_delete_empty_subquery;
