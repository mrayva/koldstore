-- manage_table() accepts only ordinary, permanent heap tables (upstream #125):
-- every other relation kind is refused up front, before any catalog row,
-- mirror table, trigger or slot is created.

\set ON_ERROR_STOP off
\set VERBOSITY terse

-- Runs manage_table on `rel` and reports the outcome and whether it left any
-- catalog state behind.
CREATE FUNCTION sqlreg.try_manage(rel regclass) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  msg text;
  leaked int;
BEGIN
  PERFORM koldstore.manage_table(
    table_name => rel, storage => 'sqlreg_fs', hot_row_limit => 10,
    min_flush_rows => 1, max_rows_per_file => 10, auto_flush => false);
  RETURN 'managed';
EXCEPTION WHEN OTHERS THEN
  msg := regexp_replace(split_part(SQLERRM, E'\n', 1), 'pg_temp_[0-9]+', 'pg_temp');
  SELECT count(*) INTO leaked FROM koldstore.schemas WHERE table_oid = rel::oid;
  RETURN 'ERROR: ' || msg || CASE WHEN leaked > 0 THEN ' [LEAKED CATALOG ROW]' ELSE '' END;
END
$$;

-- ordinary table: the supported case
CREATE TABLE sqlreg.k_plain (id bigint PRIMARY KEY, v text);
SELECT sqlreg.try_manage('sqlreg.k_plain') AS plain_table;

-- partitioned table: the parent stays unmanageable (ADR-008: no storage of
-- its own to flush or scan), but a partition leaf may be managed on its own,
-- exactly like a plain table -- its own local PK-backed index is enough.
CREATE TABLE sqlreg.k_part (id bigint, v text, PRIMARY KEY (id)) PARTITION BY RANGE (id);
CREATE TABLE sqlreg.k_part_1 PARTITION OF sqlreg.k_part FOR VALUES FROM (0) TO (100);
SELECT sqlreg.try_manage('sqlreg.k_part') AS partitioned_parent;
SELECT sqlreg.try_manage('sqlreg.k_part_1') AS partition_child;

-- plain inheritance: the parent stays unmanageable for the same reason. A
-- plain-inheritance child is a different story from a partition child,
-- though -- traditional inheritance does NOT propagate a PRIMARY KEY to the
-- child table, so k_inh_child fails on the (separate, pre-existing)
-- no-primary-key check, not the hierarchy check.
CREATE TABLE sqlreg.k_inh_parent (id bigint PRIMARY KEY, v text);
CREATE TABLE sqlreg.k_inh_child (extra int) INHERITS (sqlreg.k_inh_parent);
SELECT sqlreg.try_manage('sqlreg.k_inh_parent') AS inheritance_parent;
SELECT sqlreg.try_manage('sqlreg.k_inh_child') AS inheritance_child;
-- give the plain-inheritance child its own PK to isolate the hierarchy gate
-- from the no-primary-key gate.
CREATE TABLE sqlreg.k_inh_child_pk (id bigint PRIMARY KEY, extra int) INHERITS (sqlreg.k_inh_parent);
SELECT sqlreg.try_manage('sqlreg.k_inh_child_pk') AS inheritance_child_with_own_pk;

-- temporary and unlogged
CREATE TEMP TABLE k_temp (id bigint PRIMARY KEY, v text);
SELECT sqlreg.try_manage('k_temp') AS temporary_table;
CREATE UNLOGGED TABLE sqlreg.k_unlogged (id bigint PRIMARY KEY, v text);
SELECT sqlreg.try_manage('sqlreg.k_unlogged') AS unlogged_table;

-- foreign table (a dummy wrapper is enough: nothing is ever read)
CREATE FOREIGN DATA WRAPPER k_fdw;
CREATE SERVER k_server FOREIGN DATA WRAPPER k_fdw;
CREATE FOREIGN TABLE sqlreg.k_foreign (id bigint, v text) SERVER k_server;
SELECT sqlreg.try_manage('sqlreg.k_foreign') AS foreign_table;

-- views, materialized views, sequences, indexes
CREATE VIEW sqlreg.k_view AS SELECT * FROM sqlreg.k_plain;
SELECT sqlreg.try_manage('sqlreg.k_view') AS plain_view;
CREATE MATERIALIZED VIEW sqlreg.k_matview AS SELECT * FROM sqlreg.k_plain;
SELECT sqlreg.try_manage('sqlreg.k_matview') AS materialized_view;
CREATE SEQUENCE sqlreg.k_seq;
SELECT sqlreg.try_manage('sqlreg.k_seq') AS sequence;
SELECT sqlreg.try_manage('sqlreg.k_plain_pkey') AS index_relation;

-- no primary key
CREATE TABLE sqlreg.k_nopk (id bigint, v text);
SELECT sqlreg.try_manage('sqlreg.k_nopk') AS no_primary_key;

-- a permanent table that is the target of inheritance is caught above; one that
-- merely has a partitioned/inherited relative elsewhere is unaffected
CREATE TABLE sqlreg.k_other (id bigint PRIMARY KEY, v text);
SELECT sqlreg.try_manage('sqlreg.k_other') AS unrelated_table;

-- ------------------------------------- hierarchy changes after management
-- A managed table cannot become a partitioned table or an inheritance parent
-- (ADR-008: no storage of its own to aggregate over). Becoming a child/leaf
-- is unaffected either way -- a managed leaf's own storage does not change by
-- gaining a parent.
CREATE FUNCTION sqlreg.try_ddl(stmt text) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE stmt;
  RETURN 'ok';
EXCEPTION WHEN OTHERS THEN
  RETURN 'ERROR: ' || split_part(SQLERRM, E'\n', 1);
END
$$;

SELECT sqlreg.try_ddl('CREATE TABLE sqlreg.k_late_child () INHERITS (sqlreg.k_other)') AS create_child_of_managed;
SELECT sqlreg.try_ddl('CREATE TABLE sqlreg.k_late_part PARTITION OF sqlreg.k_other FOR VALUES IN (1)') AS create_partition_of_managed;
-- k_other is the PARENT role in both of these -- still refused.
CREATE TABLE sqlreg.k_loose (id bigint PRIMARY KEY, v text);
SELECT sqlreg.try_ddl('ALTER TABLE sqlreg.k_loose INHERIT sqlreg.k_other') AS inherit_from_managed;
-- k_plain is the CHILD role here -- allowed (ADR-008 option A).
SELECT sqlreg.try_ddl('ALTER TABLE sqlreg.k_plain INHERIT sqlreg.k_loose') AS managed_inherits_from_other;
CREATE TABLE sqlreg.k_att_parent (id bigint, v text, PRIMARY KEY (id)) PARTITION BY RANGE (id);
-- k_other is the CHILD/leaf role here (being attached as a partition) --
-- allowed (ADR-008 option A).
SELECT sqlreg.try_ddl('ALTER TABLE sqlreg.k_att_parent ATTACH PARTITION sqlreg.k_other FOR VALUES FROM (0) TO (10)') AS attach_managed_as_partition;
-- unmanaged tables are unaffected
CREATE TABLE sqlreg.k_free (id bigint PRIMARY KEY, v text);
SELECT sqlreg.try_ddl('ALTER TABLE sqlreg.k_loose INHERIT sqlreg.k_free') AS inherit_unmanaged;
