-- Value lists on composite primary keys. A prefix or a range of the key already prunes segments by the
-- columns' bounds; what a composite key lacked was a LIST: `(tenant, id) IN (...)`, an OR of full-key
-- equalities, and `tenant IN (...)` fell back to the [min, max] range of the list (every segment between
-- the smallest and largest value). Each listed value now selects its own segments, and an OR of
-- conjunctions gives every key column the list of values its disjuncts pin. Pruning must never hide a row.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE FUNCTION sqlreg.explain_counter(query text, label text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
  line text;
  value bigint;
BEGIN
  FOR line IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) ' || query LOOP
    IF line ~ ('^\s*' || label || ': [0-9]+') THEN
      value := substring(line FROM label || ': ([0-9]+)')::bigint;
      EXIT;
    END IF;
  END LOOP;
  RETURN value;
END
$$;

CREATE FUNCTION sqlreg.same_content(managed regclass, control regclass, key text) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE a text; b text;
BEGIN
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', key, managed) INTO a;
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', key, control) INTO b;
  RETURN a = b;
END
$$;

-- tenant-major layout: 20 tenants, 100 rows each, one segment per tenant
CREATE TABLE sqlreg.kl (tenant int NOT NULL, id bigint NOT NULL, n int NOT NULL, v text, PRIMARY KEY (tenant, id));
INSERT INTO sqlreg.kl SELECT (g - 1) / 100, ((g - 1) % 100) + 1, g, 'v' || g FROM generate_series(1, 2000) g;
CREATE TABLE sqlreg.kl_ctl AS SELECT * FROM sqlreg.kl;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.kl'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 100, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS kl_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS kl_caught_up;
SELECT sqlreg.flush_table('sqlreg.kl'::regclass) IS NOT NULL AS kl_flushed;
SELECT (SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.kl'::regclass::oid AND status = 'active') >= 18 AS kl_many_segments;

-- a list on the leading column opens only the listed tenants' segments (was: everything in [min, max])
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.kl WHERE tenant IN (3, 12)', 'Parquet Segments Opened') AS kl_tenant_in_opened;
-- an OR of full-key equalities
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.kl WHERE (tenant = 3 AND id = 4) OR (tenant = 12 AND id = 9)', 'Parquet Segments Opened') AS kl_or_opened;
-- the row-value form is planned as the same disjunction
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.kl WHERE (tenant, id) IN ((3, 4), (12, 9))', 'Parquet Segments Opened') AS kl_row_in_opened;
-- three keys, one of them absent
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.kl WHERE (tenant, id) IN ((3, 4), (12, 9), (7, 500))', 'Parquet Segments Opened') <= 3 AS kl_three_keys_opened;
-- disjuncts on different columns say nothing about either: no list, nothing pruned
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.kl WHERE tenant = 3 OR id = 4', 'Segments Pruned by Catalog Index') AS kl_mixed_or_pruned;

-- the answers are right
SELECT sqlreg.same_content('sqlreg.kl', 'sqlreg.kl_ctl', 'tenant, id') AS kl_content_equal;
SELECT count(*) AS kl_tenant_in_rows FROM sqlreg.kl WHERE tenant IN (3, 12);
SELECT count(*) AS kl_ctl_tenant_in_rows FROM sqlreg.kl_ctl WHERE tenant IN (3, 12);
SELECT tenant, id FROM sqlreg.kl WHERE (tenant = 3 AND id = 4) OR (tenant = 12 AND id = 9) ORDER BY tenant, id;
SELECT tenant, id FROM sqlreg.kl WHERE (tenant, id) IN ((3, 4), (12, 9), (7, 500)) ORDER BY tenant, id;
SELECT count(*) AS kl_mixed_or_rows FROM sqlreg.kl WHERE tenant = 3 OR id = 4;
SELECT count(*) AS kl_ctl_mixed_or_rows FROM sqlreg.kl_ctl WHERE tenant = 3 OR id = 4;
-- a conjunct that is not an equality only narrows a disjunct
SELECT count(*) AS kl_range_in_or FROM sqlreg.kl WHERE (tenant = 3 AND id > 90) OR (tenant = 12 AND id < 5);
SELECT count(*) AS kl_ctl_range_in_or FROM sqlreg.kl_ctl WHERE (tenant = 3 AND id > 90) OR (tenant = 12 AND id < 5);

-- hot edits and deletes still win over the cold copy
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.kl SET v = 'edited' WHERE (tenant, id) IN ((3, 4), (12, 9));
UPDATE sqlreg.kl_ctl SET v = 'edited' WHERE (tenant, id) IN ((3, 4), (12, 9));
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS kl_fenced;
SELECT tenant, id, v FROM sqlreg.kl WHERE (tenant, id) IN ((3, 4), (12, 9), (3, 5)) ORDER BY tenant, id;
SELECT sqlreg.same_content('sqlreg.kl', 'sqlreg.kl_ctl', 'tenant, id') AS kl_content_equal_after_edit;
