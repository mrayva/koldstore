-- ADR-008 option A: a partition/inheritance LEAF may be managed on its own,
-- exactly like a plain table. The partitioned/inheritance PARENT stays
-- permanently unmanageable (see manage_relation_kinds.sql for that gate).
-- This file proves the central claim of ADR-008: a plain SELECT/INSERT
-- against the PARENT sees a managed leaf's cold data with zero planner-level
-- change, because PostgreSQL's own Append/MergeAppend construction already
-- calls the per-relation pathlist hook once per leaf.

CREATE TABLE sqlreg.p_sales (
  id bigint,
  region text NOT NULL,
  amt bigint NOT NULL,
  PRIMARY KEY (id, region)
) PARTITION BY LIST (region);

CREATE TABLE sqlreg.p_sales_east PARTITION OF sqlreg.p_sales FOR VALUES IN ('east');
CREATE TABLE sqlreg.p_sales_west PARTITION OF sqlreg.p_sales FOR VALUES IN ('west');

-- Only the east leaf is managed; west stays an ordinary, unmanaged partition.
SELECT koldstore.manage_table(
  table_name => 'sqlreg.p_sales_east'::regclass,
  storage => 'sqlreg_fs',
  hot_row_limit => 2,
  min_flush_rows => 1,
  max_rows_per_file => 10,
  migration_order_by => 'id',
  auto_flush => false
);

INSERT INTO sqlreg.p_sales (id, region, amt)
SELECT gs, 'east', gs * 10 FROM generate_series(1, 6) AS gs;
INSERT INTO sqlreg.p_sales (id, region, amt)
SELECT gs, 'west', gs * 10 FROM generate_series(101, 103) AS gs;

-- Wait for the async WAL mirror to apply these commits before flushing --
-- otherwise flush_table sees a stale (empty) mirror and flushes nothing.
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;

-- Push every east row cold; west has no koldstore involvement at all.
SELECT sqlreg.flush_table('sqlreg.p_sales_east'::regclass) IS NOT NULL AS flushed_east;

-- Querying the managed leaf directly sees hot+cold.
SELECT count(*)::bigint AS east_leaf_count FROM sqlreg.p_sales_east;

-- Querying the PARENT must see the SAME east rows (now cold-only) plus the
-- untouched west rows -- the free read-path win ADR-008 predicted.
SELECT count(*)::bigint AS parent_total_count FROM sqlreg.p_sales;
SELECT region, count(*)::bigint AS region_count, sum(amt)::bigint AS region_sum
FROM sqlreg.p_sales GROUP BY region ORDER BY region;
SELECT id, region, amt FROM sqlreg.p_sales WHERE id = 3 AND region = 'east';
SELECT id, region, amt FROM sqlreg.p_sales ORDER BY region, id;

-- Partition pruning still applies: a region = 'west' query should not even
-- touch the managed east leaf (unaffected by koldstore either way, but worth
-- confirming the parent-level query still plans normally).
SELECT count(*)::bigint AS west_only_count FROM sqlreg.p_sales WHERE region = 'west';

-- An INSERT through the parent routes to the right leaf, hot side, no change
-- in behavior from an ordinary partitioned table.
INSERT INTO sqlreg.p_sales (id, region, amt) VALUES (999, 'east', 9990);
SELECT amt FROM sqlreg.p_sales WHERE id = 999 AND region = 'east';
SELECT count(*)::bigint AS parent_total_after_insert FROM sqlreg.p_sales;

-- Plan-shape check (avoid dumping unstable cold-query EXPLAIN text): the
-- parent's Append must contain a KoldMergeScan for the east leaf.
DO $$
DECLARE
  r record;
  plan text := '';
BEGIN
  FOR r IN
    EXECUTE $q$
      EXPLAIN (COSTS OFF)
      SELECT * FROM sqlreg.p_sales
    $q$
  LOOP
    plan := plan || r."QUERY PLAN" || E'\n';
  END LOOP;
  IF position('Custom Scan (KoldMergeScan)' IN plan) = 0 THEN
    RAISE EXCEPTION 'expected KoldMergeScan under the parent''s Append:%', E'\n' || plan;
  END IF;
  IF position('Append' IN plan) = 0 THEN
    RAISE EXCEPTION 'expected an Append node in plan:%', E'\n' || plan;
  END IF;
END $$;

-- Regression: a semi-join (IN) whose sub-query joins the PARENT to a VALUES list used to crash the
-- whole server. KoldMergeScan replaced the managed leaf's path list with wrappers around only its
-- cheapest native path; under that join the cheapest was an index scan parameterized by the VALUES
-- rows, so the leaf was left with no unparameterized path and PostgreSQL's append planning
-- dereferenced a NULL path. A managed relation must always keep an unparameterized path.
SELECT count(*)::bigint AS parent_in_parent_join_values
FROM sqlreg.p_sales t
WHERE (t.id, t.region) IN (
  SELECT s.id, s.region FROM sqlreg.p_sales s, (VALUES (3::bigint), (998)) v(i) WHERE s.id = v.i);
SELECT count(*)::bigint AS leaf_in_parent_join_values
FROM sqlreg.p_sales_east t
WHERE (t.id, t.region) IN (
  SELECT s.id, s.region FROM sqlreg.p_sales s, (VALUES (3::bigint), (4), (998)) v(i) WHERE s.id = v.i);

-- ------------------------- write guard covers the parent, per leaf
-- (docs/limitations.md, ADR-008). PostgreSQL lists every partition in
-- PlannedStmt.resultRelations for a parent-routed statement but gives
-- ModifyTable a single child plan: an Append of one filtered scan per leaf.
-- The guard recovers each managed leaf's own filter from its scan node and
-- runs the same cold-match recount as for a plain table; a leaf with no scan
-- (pruned at plan time) cannot be touched and is skipped. Shapes that cannot
-- be attributed to a leaf (a join, a sub-query, MERGE) use the planner hook's
-- probe, built against the parent, which the recount reads once merged and
-- once hot-only per managed leaf.
CREATE FUNCTION sqlreg.try(stmt text) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE stmt;
  RETURN 'ok';
EXCEPTION WHEN OTHERS THEN
  RETURN 'ERROR: ' || split_part(SQLERRM, E'\n', 1);
END
$$;

SELECT sqlreg.try($$UPDATE sqlreg.p_sales SET amt = amt + 1 WHERE id = 3 AND region = 'east'$$)
  AS parent_update_of_cold_row_refused;
SELECT amt FROM sqlreg.p_sales WHERE id = 3 AND region = 'east';

-- The identical statement issued directly against the managed leaf is
-- unaffected by any of this -- the single-table write guard covers it.
SELECT sqlreg.try($$UPDATE sqlreg.p_sales_east SET amt = amt + 1 WHERE id = 4$$)
  AS leaf_update_of_cold_row_refused;
SELECT amt FROM sqlreg.p_sales_east WHERE id = 4;

-- A parent-routed UPDATE that only touches a HOT row (id=999, inserted earlier)
-- now succeeds even though the east leaf has cold data elsewhere.
SELECT sqlreg.try($$UPDATE sqlreg.p_sales SET amt = amt + 1 WHERE id = 999 AND region = 'east'$$)
  AS parent_update_of_hot_row_succeeds;
SELECT amt FROM sqlreg.p_sales WHERE id = 999 AND region = 'east';

-- Without the partition key in the WHERE clause both leaves are scanned; the
-- east scan's own filter (id = 999, a hot row) matches no cold row. A cold id
-- (every originally inserted row was flushed) is refused the same way.
SELECT sqlreg.try($$UPDATE sqlreg.p_sales SET amt = amt + 1 WHERE id = 999$$)
  AS parent_update_hot_row_without_partition_key;
SELECT amt FROM sqlreg.p_sales WHERE id = 999;
SELECT sqlreg.try($$UPDATE sqlreg.p_sales SET amt = amt + 1 WHERE id = 5$$)
  AS parent_update_cold_row_without_partition_key;
SELECT amt FROM sqlreg.p_sales WHERE id = 5;

-- A range / unconditional statement that does reach cold rows is refused.
SELECT sqlreg.try($$DELETE FROM sqlreg.p_sales WHERE id < 3$$)
  AS parent_range_delete_matching_cold_refused;
SELECT sqlreg.try($$UPDATE sqlreg.p_sales SET amt = amt + 1$$)
  AS parent_unconditional_update_refused;
SELECT count(*)::bigint AS parent_total_after_refusals FROM sqlreg.p_sales;

-- Pruning the managed leaf at plan time means the statement cannot touch it,
-- cold data or not.
SELECT sqlreg.try($$UPDATE sqlreg.p_sales SET amt = amt + 1 WHERE region = 'west'$$)
  AS parent_update_pruned_to_unmanaged_leaf;
SELECT amt FROM sqlreg.p_sales WHERE id = 101 AND region = 'west';

-- External parameters in a generic plan are resolved the same way.
SET plan_cache_mode = force_generic_plan;
PREPARE sqlreg_bump(bigint) AS UPDATE sqlreg.p_sales SET amt = amt + 1 WHERE id = $1;
SELECT sqlreg.try($$EXECUTE sqlreg_bump(2)$$) AS generic_plan_cold_param_refused;
SELECT sqlreg.try($$EXECUTE sqlreg_bump(999)$$) AS generic_plan_hot_param_succeeds;
DEALLOCATE sqlreg_bump;
RESET plan_cache_mode;

-- A real join over several leaves cannot be attributed to one leaf, so the probe stands in.
-- A join that only reaches hot rows succeeds...
SELECT sqlreg.try($$UPDATE sqlreg.p_sales s SET amt = amt + 1
                    FROM (VALUES (999::bigint), (998)) v(i) WHERE s.id = v.i$$)
  AS parent_update_from_join_hot_ids_succeeds;
-- ...and one that reaches a cold row is refused with the precise join message.
SELECT sqlreg.try($$UPDATE sqlreg.p_sales s SET amt = amt + 1
                    FROM (VALUES (5::bigint), (998)) v(i) WHERE s.id = v.i$$)
  AS parent_update_from_join_cold_id_refused;
SELECT sqlreg.try($$DELETE FROM sqlreg.p_sales WHERE id IN (SELECT 5::bigint)$$)
  AS parent_delete_subquery_cold_id_refused;

-- MERGE through the parent takes the same path. A WHEN MATCHED action that reaches a cold row
-- is refused...
SELECT sqlreg.try($$MERGE INTO sqlreg.p_sales t
                    USING (VALUES (5::bigint, 'east'::text)) v(i, r) ON t.id = v.i AND t.region = v.r
                    WHEN MATCHED THEN UPDATE SET amt = t.amt + 1$$)
  AS parent_merge_matched_cold_row_refused;
-- ...one that only matches a hot row succeeds...
SELECT sqlreg.try($$MERGE INTO sqlreg.p_sales t
                    USING (VALUES (999::bigint, 'east'::text)) v(i, r) ON t.id = v.i AND t.region = v.r
                    WHEN MATCHED THEN UPDATE SET amt = t.amt + 1$$)
  AS parent_merge_matched_hot_row_succeeds;
-- ...and an insert-only MERGE of a new key is not held up by the leaf's unrelated cold data.
SELECT sqlreg.try($$MERGE INTO sqlreg.p_sales t
                    USING (VALUES (7000::bigint, 'east'::text, 70::bigint)) v(i, r, a)
                    ON t.id = v.i AND t.region = v.r
                    WHEN NOT MATCHED THEN INSERT (id, region, amt) VALUES (v.i, v.r, v.a)$$)
  AS parent_merge_insert_only_succeeds;
SELECT amt AS id7000_amt FROM sqlreg.p_sales WHERE id = 7000 AND region = 'east';
SELECT sqlreg.try($$DELETE FROM sqlreg.p_sales WHERE id = 7000 AND region = 'east'$$) AS id7000_cleanup;

-- ------------------------- hydrate-on-write through the parent, per leaf
-- With koldstore.hydrate_on_write on, a parent-routed UPDATE/DELETE changes cold-only rows
-- instead of being refused: each managed leaf's own scan filter selects the cold rows to insert
-- into that leaf's heap first, then the native statement runs. A join across partitions, a
-- sub-query and MERGE cannot be attributed to one leaf, so they use the planner hook's probe over
-- the parent: a leaf hydrates the cold-only rows whose primary key the probe returns.
SET koldstore.hydrate_on_write = on;

SELECT sqlreg.try($$UPDATE sqlreg.p_sales SET amt = amt + 1000 WHERE id = 3 AND region = 'east'$$)
  AS hydrate_parent_update_cold_row;
SELECT amt AS id3_amt_via_parent FROM sqlreg.p_sales WHERE id = 3 AND region = 'east';
SELECT amt AS id3_amt_via_leaf FROM sqlreg.p_sales_east WHERE id = 3;

-- No partition key in the predicate: both leaves are scanned, only the east filter matches cold rows.
SELECT sqlreg.try($$DELETE FROM sqlreg.p_sales WHERE id = 4$$) AS hydrate_parent_delete_without_partition_key;
-- The cold copy stays visible until the async mirror applies the delete's tombstone.
SELECT koldstore.wait_for_async_mirror() >= 0 AS mirror_settled;
SELECT count(*)::bigint AS id4_rows_left FROM sqlreg.p_sales WHERE id = 4;

SELECT sqlreg.try($$UPDATE sqlreg.p_sales SET amt = amt + 1 WHERE id < 3$$) AS hydrate_parent_range_update;
SELECT id, amt FROM sqlreg.p_sales WHERE id < 3 ORDER BY id;

-- A partition-key-changing UPDATE is refused by koldstore's ordinary primary-key rule, hydration
-- or not: a partitioned table's primary key must include the partition key, so moving a row
-- across leaves is always a primary-key update. Nothing is hydrated or moved.
SELECT sqlreg.try($$UPDATE sqlreg.p_sales SET region = 'west' WHERE id = 5 AND region = 'east'$$)
  AS hydrate_cross_leaf_move_refused;
SELECT id, region, amt FROM sqlreg.p_sales_east WHERE id = 5;
SELECT count(*)::bigint AS id5_rows_in_west FROM sqlreg.p_sales_west WHERE id = 5;

-- A statement that only touches the unmanaged sibling leaf is unaffected.
SELECT sqlreg.try($$UPDATE sqlreg.p_sales SET amt = amt + 1 WHERE region = 'west' AND id = 102$$)
  AS hydrate_unmanaged_leaf_only;

-- A join across partitions hydrates through the probe.
SELECT sqlreg.try($$UPDATE sqlreg.p_sales s SET amt = amt + 1
                    FROM (VALUES (6::bigint), (998)) v(i) WHERE s.id = v.i$$)
  AS hydrate_join_update;
SELECT amt AS id6_amt_after_join_update FROM sqlreg.p_sales WHERE id = 6 AND region = 'east';

-- MERGE through the parent hydrates through the same probe: an UPDATE action...
SELECT sqlreg.try($$MERGE INTO sqlreg.p_sales t
                    USING (VALUES (6::bigint, 'east'::text)) v(i, r) ON t.id = v.i AND t.region = v.r
                    WHEN MATCHED THEN UPDATE SET amt = t.amt + 1$$)
  AS hydrate_merge_update;
SELECT amt AS id6_amt_after_merge_update FROM sqlreg.p_sales WHERE id = 6 AND region = 'east';
-- ...and a DELETE action.
SELECT sqlreg.try($$MERGE INTO sqlreg.p_sales t
                    USING (VALUES (6::bigint, 'east'::text)) v(i, r) ON t.id = v.i AND t.region = v.r
                    WHEN MATCHED THEN DELETE$$)
  AS hydrate_merge_delete;
SELECT koldstore.wait_for_async_mirror() >= 0 AS mirror_settled_merge;
SELECT count(*)::bigint AS id6_rows_left FROM sqlreg.p_sales WHERE id = 6;

RESET koldstore.hydrate_on_write;

-- Detaching the managed leaf is an ordinary partition-maintenance operation,
-- unaffected by its management status (ADR-008: a managed leaf's own
-- storage does not change by losing a parent either).
ALTER TABLE sqlreg.p_sales DETACH PARTITION sqlreg.p_sales_east;
SELECT count(*)::bigint AS east_leaf_count_after_detach FROM sqlreg.p_sales_east;
SELECT count(*)::bigint AS parent_total_after_detach FROM sqlreg.p_sales;

-- ------------------------- hydrate-on-write with TWO managed leaves in one statement
CREATE TABLE sqlreg.p2 (
  id bigint,
  region text NOT NULL,
  amt bigint NOT NULL,
  PRIMARY KEY (id, region)
) PARTITION BY LIST (region);
CREATE TABLE sqlreg.p2_a PARTITION OF sqlreg.p2 FOR VALUES IN ('a');
CREATE TABLE sqlreg.p2_b PARTITION OF sqlreg.p2 FOR VALUES IN ('b');

SELECT koldstore.manage_table(table_name => 'sqlreg.p2_a'::regclass, storage => 'sqlreg_fs',
  hot_row_limit => 2, min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id',
  auto_flush => false) IS NOT NULL AS managed_a;
SELECT koldstore.manage_table(table_name => 'sqlreg.p2_b'::regclass, storage => 'sqlreg_fs',
  hot_row_limit => 2, min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id',
  auto_flush => false) IS NOT NULL AS managed_b;

INSERT INTO sqlreg.p2 SELECT gs, 'a', gs * 10 FROM generate_series(1, 4) AS gs;
INSERT INTO sqlreg.p2 SELECT gs, 'b', gs * 100 FROM generate_series(1, 4) AS gs;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up_p2;
SELECT sqlreg.flush_table('sqlreg.p2_a'::regclass) IS NOT NULL AS flushed_a;
SELECT sqlreg.flush_table('sqlreg.p2_b'::regclass) IS NOT NULL AS flushed_b;

-- Without hydration both leaves' cold rows make the statement refuse.
SELECT sqlreg.try($$UPDATE sqlreg.p2 SET amt = amt + 1 WHERE id IN (2, 3)$$) AS p2_refused_without_hydration;

SET koldstore.hydrate_on_write = on;
-- One statement, cold rows in BOTH managed leaves: each leaf hydrates its own matches.
SELECT sqlreg.try($$UPDATE sqlreg.p2 SET amt = amt + 1 WHERE id IN (2, 3)$$) AS p2_update_hits_both_leaves;
SELECT id, region, amt FROM sqlreg.p2 ORDER BY region, id;
-- A MERGE whose source matches cold rows in BOTH managed leaves, and also inserts a new key.
SELECT sqlreg.try($$MERGE INTO sqlreg.p2 t
                    USING (VALUES (1::bigint, 'a'::text), (1, 'b'), (9, 'a')) v(i, r)
                    ON t.id = v.i AND t.region = v.r
                    WHEN MATCHED THEN UPDATE SET amt = t.amt + 5
                    WHEN NOT MATCHED THEN INSERT (id, region, amt) VALUES (v.i, v.r, 1)$$)
  AS p2_merge_hits_both_leaves;
SELECT id, region, amt FROM sqlreg.p2 WHERE id IN (1, 9) ORDER BY region, id;
SELECT sqlreg.try($$DELETE FROM sqlreg.p2$$) AS p2_unconditional_delete_hits_both_leaves;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_settled_p2;
SELECT count(*)::bigint AS p2_rows_left FROM sqlreg.p2;
RESET koldstore.hydrate_on_write;
