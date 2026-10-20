-- ADR-008 sixth slice: a partitioned parent can be managed. manage_table(parent) manages every leaf and
-- records a template; partitions created or attached later are managed with the same options;
-- flush/status/auto-flush/unmanage fan out to the leaves; DROP TABLE parent cleans every managed leaf up.
--
-- Oracle: an unmanaged, unpartitioned copy of the data.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE FUNCTION sqlreg.fingerprint(rel regclass, ord text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE result text;
BEGIN
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', ord, rel) INTO result;
  RETURN result;
END
$$;

-- managed leaves of a parent, by name
CREATE FUNCTION sqlreg.managed_leaves(parent regclass) RETURNS text[]
LANGUAGE sql AS $$
  SELECT coalesce(array_agg(t.relid::text ORDER BY t.relid::text), '{}')
  FROM pg_partition_tree(parent) t
  JOIN koldstore.schemas s ON s.table_oid = t.relid::oid AND s.active
  WHERE t.isleaf
$$;

CREATE TABLE sqlreg.pp (id bigint PRIMARY KEY, body text NOT NULL) PARTITION BY RANGE (id);
CREATE TABLE sqlreg.pp_1 PARTITION OF sqlreg.pp FOR VALUES FROM (0) TO (100);
CREATE TABLE sqlreg.pp_2 PARTITION OF sqlreg.pp FOR VALUES FROM (100) TO (200);
INSERT INTO sqlreg.pp SELECT g, 'row ' || g FROM generate_series(1, 150) g;
CREATE TABLE sqlreg.pp_ctl AS SELECT * FROM sqlreg.pp;

-- ------------------------------------------------ manage the parent
SELECT koldstore.manage_table(
  table_name => 'sqlreg.pp'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS parent_managed;
SELECT sqlreg.managed_leaves('sqlreg.pp') AS leaves_after_manage;
SELECT (SELECT count(*) FROM koldstore.partition_templates WHERE parent_oid = 'sqlreg.pp'::regclass::oid) AS templates;
-- the parent itself never gets a schema row
SELECT count(*) AS parent_schema_rows FROM koldstore.schemas WHERE table_oid = 'sqlreg.pp'::regclass::oid;
-- managing it twice is refused
SELECT koldstore.manage_table(table_name => 'sqlreg.pp'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false);

-- flush through the parent
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS caught_up;
SELECT (koldstore.flush_table('sqlreg.pp'::regclass, true) ->> 'ok')::boolean AS flushed,
       koldstore.flush_table('sqlreg.pp'::regclass, true) ->> 'partitioned_parent' AS via_parent;
SELECT (koldstore.table_status('sqlreg.pp'::regclass) ->> 'partitions_managed')::int AS status_managed,
       (koldstore.table_status('sqlreg.pp'::regclass) ->> 'partitions_total')::int AS status_total,
       (koldstore.table_status('sqlreg.pp'::regclass) ->> 'cold_row_count')::int > 100 AS mostly_cold,
       koldstore.table_status('sqlreg.pp'::regclass) ? 'async_mirror' AS has_async_mirror;
SELECT sqlreg.fingerprint('sqlreg.pp', 'id') = sqlreg.fingerprint('sqlreg.pp_ctl', 'id') AS rows_identical_after_flush;

-- ------------------------------------------------ new partitions are managed automatically
CREATE TABLE sqlreg.pp_3 PARTITION OF sqlreg.pp FOR VALUES FROM (200) TO (300);
SELECT sqlreg.managed_leaves('sqlreg.pp') AS leaves_after_create;

-- attaching an empty table
CREATE TABLE sqlreg.pp_4 (LIKE sqlreg.pp INCLUDING ALL);
ALTER TABLE sqlreg.pp ATTACH PARTITION sqlreg.pp_4 FOR VALUES FROM (300) TO (400);
SELECT sqlreg.managed_leaves('sqlreg.pp') AS leaves_after_attach;

-- attaching a populated table
CREATE TABLE sqlreg.pp_5 (LIKE sqlreg.pp INCLUDING ALL);
INSERT INTO sqlreg.pp_5 SELECT g, 'attached ' || g FROM generate_series(400, 430) g;
ALTER TABLE sqlreg.pp ATTACH PARTITION sqlreg.pp_5 FOR VALUES FROM (400) TO (500);
SELECT sqlreg.managed_leaves('sqlreg.pp') AS leaves_after_populated_attach;

-- a sub-partitioned node has no heap; its leaves inherit the ancestor's template
CREATE TABLE sqlreg.pp_sub PARTITION OF sqlreg.pp FOR VALUES FROM (500) TO (700) PARTITION BY RANGE (id);
CREATE TABLE sqlreg.pp_sub_a PARTITION OF sqlreg.pp_sub FOR VALUES FROM (500) TO (600);
SELECT sqlreg.managed_leaves('sqlreg.pp') AS leaves_after_subpartition;

-- the template's options reached the new partitions
SELECT s.options -> 'flush_policy' ->> 'hot_row_limit' AS pp_3_hot_row_limit
FROM koldstore.schemas s WHERE s.table_oid = 'sqlreg.pp_3'::regclass::oid AND s.active;

INSERT INTO sqlreg.pp SELECT g, 'new ' || g FROM generate_series(200, 230) g;
INSERT INTO sqlreg.pp_ctl SELECT g, 'new ' || g FROM generate_series(200, 230) g;
INSERT INTO sqlreg.pp_ctl SELECT g, 'attached ' || g FROM generate_series(400, 430) g;
INSERT INTO sqlreg.pp SELECT g, 'sub ' || g FROM generate_series(500, 520) g;
INSERT INTO sqlreg.pp_ctl SELECT g, 'sub ' || g FROM generate_series(500, 520) g;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS caught_up_2;
SELECT (koldstore.flush_table('sqlreg.pp'::regclass, true) ->> 'ok')::boolean AS flushed_2;
SELECT sqlreg.fingerprint('sqlreg.pp', 'id') = sqlreg.fingerprint('sqlreg.pp_ctl', 'id') AS rows_identical_after_growth;

-- ------------------------------------------------ pausing auto-management
SELECT koldstore.set_partition_auto_manage('sqlreg.pp'::regclass, false) AS paused;
CREATE TABLE sqlreg.pp_6 PARTITION OF sqlreg.pp FOR VALUES FROM (700) TO (800);
SELECT sqlreg.managed_leaves('sqlreg.pp') AS leaves_while_paused;
SELECT koldstore.set_partition_auto_manage('sqlreg.pp'::regclass, true) AS resumed;
CREATE TABLE sqlreg.pp_7 PARTITION OF sqlreg.pp FOR VALUES FROM (800) TO (900);
SELECT sqlreg.managed_leaves('sqlreg.pp') AS leaves_after_resume;

-- auto-flush follows the parent
SELECT koldstore.set_table_auto_flush('sqlreg.pp'::regclass, true) AS autoflush_on;
SELECT (SELECT bool_and(coalesce((s.options ->> 'auto_flush')::boolean, true))
        FROM koldstore.schemas s JOIN pg_partition_tree('sqlreg.pp') t ON t.relid::oid = s.table_oid
        WHERE s.active AND t.isleaf) AS all_leaves_autoflush;
SELECT koldstore.set_table_auto_flush('sqlreg.pp'::regclass, false) AS autoflush_off;
SELECT (SELECT bool_or(coalesce((s.options ->> 'auto_flush')::boolean, true))
        FROM koldstore.schemas s JOIN pg_partition_tree('sqlreg.pp') t ON t.relid::oid = s.table_oid
        WHERE s.active AND t.isleaf) AS any_leaf_autoflush;
-- a partition created now follows the template's new setting
CREATE TABLE sqlreg.pp_8 PARTITION OF sqlreg.pp FOR VALUES FROM (900) TO (1000);
SELECT coalesce((s.options ->> 'auto_flush')::boolean, true) AS new_leaf_autoflush
FROM koldstore.schemas s WHERE s.table_oid = 'sqlreg.pp_8'::regclass::oid AND s.active;

-- ------------------------------------------------ a detached leaf stays managed on its own
ALTER TABLE sqlreg.pp DETACH PARTITION sqlreg.pp_7;
SELECT (SELECT count(*) FROM koldstore.schemas WHERE table_oid = 'sqlreg.pp_7'::regclass::oid AND active) AS detached_still_managed;
SELECT sqlreg.managed_leaves('sqlreg.pp') AS leaves_after_detach;
DROP TABLE sqlreg.pp_7;

-- ------------------------------------------------ unmanage through the parent
SELECT koldstore.unmanage_table('sqlreg.pp'::regclass) IS NOT NULL AS parent_unmanaged;
SELECT sqlreg.managed_leaves('sqlreg.pp') AS leaves_after_unmanage;
SELECT count(*) AS templates_after_unmanage FROM koldstore.partition_templates WHERE parent_oid = 'sqlreg.pp'::regclass::oid;
SELECT sqlreg.fingerprint('sqlreg.pp', 'id') = sqlreg.fingerprint('sqlreg.pp_ctl', 'id') AS rows_identical_after_unmanage;

-- ------------------------------------------------ DROP TABLE parent cleans every managed leaf
CREATE TABLE sqlreg.dp (id bigint PRIMARY KEY, body text NOT NULL) PARTITION BY RANGE (id);
CREATE TABLE sqlreg.dp_1 PARTITION OF sqlreg.dp FOR VALUES FROM (0) TO (100);
INSERT INTO sqlreg.dp SELECT g, 'x' FROM generate_series(1, 20) g;
SELECT koldstore.manage_table(table_name => 'sqlreg.dp'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false) IS NOT NULL AS dp_managed;
CREATE TABLE sqlreg.dp_2 PARTITION OF sqlreg.dp FOR VALUES FROM (100) TO (200);
SELECT sqlreg.managed_leaves('sqlreg.dp') AS dp_leaves;
SELECT 'sqlreg.dp'::regclass::oid AS dp_oid \gset
SELECT 'sqlreg.dp_1'::regclass::oid AS dp1_oid \gset
SELECT 'sqlreg.dp_2'::regclass::oid AS dp2_oid \gset
DROP TABLE sqlreg.dp;
SELECT (SELECT count(*) FROM koldstore.schemas WHERE table_oid IN (:dp1_oid, :dp2_oid) AND active) AS leaf_schema_rows_left,
       (SELECT count(*) FROM koldstore.partition_templates WHERE parent_oid = :dp_oid) AS template_rows_left;

-- ------------------------------------------------ an empty partitioned table can be managed first
CREATE TABLE sqlreg.ep (id bigint PRIMARY KEY, body text NOT NULL) PARTITION BY RANGE (id);
SELECT koldstore.manage_table(table_name => 'sqlreg.ep'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false) IS NOT NULL AS ep_managed;
CREATE TABLE sqlreg.ep_1 PARTITION OF sqlreg.ep FOR VALUES FROM (0) TO (100);
SELECT sqlreg.managed_leaves('sqlreg.ep') AS ep_leaves;
