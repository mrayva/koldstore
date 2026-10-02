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

-- ------------------------- write guard now covers the parent, coarsely
-- (docs/limitations.md, ADR-008). Confirmed live that PostgreSQL always
-- lists every partition in PlannedStmt.resultRelations for a parent-routed
-- statement -- even one with a literal, maximally selective WHERE clause on
-- the partition key -- so there is no "N happens to be 1" shortcut to lean
-- on the way the single-table/CTE cases do. Rather than attempt a precise
-- per-leaf recount (needs its own investigation into how multiple result
-- relations share one ModifyTable child plan in modern PostgreSQL, deferred
-- as ADR-008's next step), a managed leaf among the result relations gets a
-- deliberately unverifiable candidate, which falls through to the same
-- fail-closed guards already used for other hard-to-verify shapes (a CTE
-- join source, a volatile function): refuse whenever the leaf has cold data
-- ANYWHERE, not just in the rows this specific statement would touch. This
-- replaces the old silent "UPDATE 0, no error" with a loud rejection, at
-- the cost of also refusing a parent-routed statement that would only have
-- touched hot rows, as long as the leaf has cold data somewhere else too --
-- confirmed and documented below, not an oversight.
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
  AS parent_update_of_cold_row_now_refused;
SELECT amt FROM sqlreg.p_sales WHERE id = 3 AND region = 'east';

-- The identical statement issued directly against the managed leaf (not
-- through the parent) is unaffected -- today's single-table write guard
-- already covers it correctly, unchanged by this round.
SELECT sqlreg.try($$UPDATE sqlreg.p_sales_east SET amt = amt + 1 WHERE id = 4$$)
  AS leaf_update_of_cold_row_correctly_refused;
SELECT amt FROM sqlreg.p_sales_east WHERE id = 4;

-- The coarser edge, confirmed on purpose: a parent-routed UPDATE that would
-- only ever touch a HOT row (id=999, inserted earlier in this file) is also
-- refused, because the east leaf has cold data elsewhere -- this guard
-- cannot yet tell "this leaf has cold data" apart from "this specific row
-- is cold", the same imprecision level the codebase already accepts for
-- other unverifiable shapes (see enforce_unverifiable_scan_guard's doc
-- comment).
SELECT sqlreg.try($$UPDATE sqlreg.p_sales SET amt = amt + 1 WHERE id = 999 AND region = 'east'$$)
  AS parent_update_of_hot_row_also_refused_coarsely;
SELECT amt FROM sqlreg.p_sales WHERE id = 999 AND region = 'east';

-- A parent-routed statement touching ONLY the unmanaged west leaf is
-- unaffected -- west was never a candidate at all (not managed).
SELECT sqlreg.try($$UPDATE sqlreg.p_sales SET amt = amt + 1 WHERE id = 101 AND region = 'west'$$)
  AS parent_update_of_unmanaged_leaf_unaffected;
SELECT amt FROM sqlreg.p_sales WHERE id = 101 AND region = 'west';

-- Detaching the managed leaf is an ordinary partition-maintenance operation,
-- unaffected by its management status (ADR-008: a managed leaf's own
-- storage does not change by losing a parent either).
ALTER TABLE sqlreg.p_sales DETACH PARTITION sqlreg.p_sales_east;
SELECT count(*)::bigint AS east_leaf_count_after_detach FROM sqlreg.p_sales_east;
SELECT count(*)::bigint AS parent_total_after_detach FROM sqlreg.p_sales;
