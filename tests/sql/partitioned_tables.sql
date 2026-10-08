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

-- ------------------------- write guard covers the parent, per leaf
-- (docs/limitations.md, ADR-008). PostgreSQL lists every partition in
-- PlannedStmt.resultRelations for a parent-routed statement but gives
-- ModifyTable a single child plan: an Append of one filtered scan per leaf.
-- The guard recovers each managed leaf's own filter from its scan node and
-- runs the same cold-match recount as for a plain table; a leaf with no scan
-- (pruned at plan time) cannot be touched and is skipped. Shapes that cannot
-- be attributed to a leaf (a join, MERGE) still fail closed.
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

-- A real join over several leaves cannot be attributed to a leaf: fails closed
-- (coarsely) even for hot rows, the documented remaining imprecision. (A join
-- the planner folds into a single-leaf index scan, e.g. a one-row VALUES with a
-- literal partition key, is just an ordinary precise statement.)
SELECT sqlreg.try($$UPDATE sqlreg.p_sales s SET amt = amt + 1
                    FROM (VALUES (999::bigint), (998)) v(i) WHERE s.id = v.i$$)
  AS parent_update_from_join_refused_coarsely;

-- Detaching the managed leaf is an ordinary partition-maintenance operation,
-- unaffected by its management status (ADR-008: a managed leaf's own
-- storage does not change by losing a parent either).
ALTER TABLE sqlreg.p_sales DETACH PARTITION sqlreg.p_sales_east;
SELECT count(*)::bigint AS east_leaf_count_after_detach FROM sqlreg.p_sales_east;
SELECT count(*)::bigint AS parent_total_after_detach FROM sqlreg.p_sales;
