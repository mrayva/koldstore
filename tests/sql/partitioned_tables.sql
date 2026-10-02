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

-- ------------------------- known, tracked gap: UPDATE/DELETE/MERGE through
-- the parent (docs/limitations.md, ADR-008). The write guard and
-- hydrate-on-write both assume a single result relation; a parent-routed
-- statement keeps the nested-ModifyTable shape even when PostgreSQL's own
-- plan-time partition pruning leaves only one leaf reachable, so neither
-- mechanism recognizes it. Confirmed red on purpose: this must start
-- refusing (or hydrating and succeeding) once a later slice extends the
-- write guard/hydrate-on-write to multi-relation targets -- that is the
-- signal this comment and this case need updating, not a surprise failure.
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
  AS parent_update_of_cold_row_KNOWN_GAP_silently_succeeds;
SELECT amt FROM sqlreg.p_sales WHERE id = 3 AND region = 'east';

-- The identical statement issued directly against the managed leaf (not
-- through the parent) is unaffected by the gap above -- today's
-- single-table write guard already covers it correctly.
SELECT sqlreg.try($$UPDATE sqlreg.p_sales_east SET amt = amt + 1 WHERE id = 4$$)
  AS leaf_update_of_cold_row_correctly_refused;
SELECT amt FROM sqlreg.p_sales_east WHERE id = 4;

-- Detaching the managed leaf is an ordinary partition-maintenance operation,
-- unaffected by its management status (ADR-008: a managed leaf's own
-- storage does not change by losing a parent either).
ALTER TABLE sqlreg.p_sales DETACH PARTITION sqlreg.p_sales_east;
SELECT count(*)::bigint AS east_leaf_count_after_detach FROM sqlreg.p_sales_east;
SELECT count(*)::bigint AS parent_total_after_detach FROM sqlreg.p_sales;
