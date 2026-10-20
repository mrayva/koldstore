-- Composite primary keys and the retention-free cold scan. A merged read normally keeps every cold key it
-- has seen (~86 bytes each, capped by koldstore.max_merge_seen_keys) so an older version in another segment
-- can be masked. When no two active segments can share a key, nothing needs retaining. For a composite key
-- two segments can only share a key if their bounds intersect on EVERY key column, so segments that
-- overlap on the leading column but are separated by another column still count as disjoint.

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

-- every segment spans all tenants, ids ascend with the load order: separated by the second column only
CREATE TABLE sqlreg.ck (tenant int NOT NULL, id bigint NOT NULL, n int NOT NULL, v text, PRIMARY KEY (tenant, id));
INSERT INTO sqlreg.ck SELECT g % 4, g, g, 'v' || g FROM generate_series(1, 400) g;
CREATE TABLE sqlreg.ck_ctl AS SELECT * FROM sqlreg.ck;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.ck'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS ck_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ck_caught_up;
SELECT sqlreg.flush_table('sqlreg.ck'::regclass) IS NOT NULL AS ck_flushed;
SELECT (SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.ck'::regclass::oid AND status = 'active') >= 15 AS ck_many_segments;

-- no two segments can share a key: a full scan retains nothing, so a tiny cap does not trip
SET koldstore.max_merge_seen_keys = 50;
SELECT count(*) AS ck_count_under_cap FROM sqlreg.ck;
SELECT count(*) AS ck_filtered_count_under_cap FROM sqlreg.ck WHERE tenant = 2;
SELECT count(DISTINCT (tenant, id)) AS ck_distinct_under_cap FROM sqlreg.ck;
RESET koldstore.max_merge_seen_keys;
SELECT sqlreg.same_content('sqlreg.ck', 'sqlreg.ck_ctl', 'tenant, id') AS ck_content_equal;

-- hot rows and deletes still mask cold ones in the disjoint path
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.ck SET v = 'edited' WHERE id IN (3, 77, 205);
UPDATE sqlreg.ck_ctl SET v = 'edited' WHERE id IN (3, 77, 205);
DELETE FROM sqlreg.ck WHERE id BETWEEN 100 AND 104;
DELETE FROM sqlreg.ck_ctl WHERE id BETWEEN 100 AND 104;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ck_fenced;
SET koldstore.max_merge_seen_keys = 50;
SELECT count(*) AS ck_count_after_dml_under_cap FROM sqlreg.ck;
RESET koldstore.max_merge_seen_keys;
SELECT sqlreg.same_content('sqlreg.ck', 'sqlreg.ck_ctl', 'tenant, id') AS ck_content_equal_after_dml;

-- re-flushing the edited rows puts the same keys in two segments whose boxes intersect: exact path again
SELECT sqlreg.flush_table('sqlreg.ck'::regclass) IS NOT NULL AS ck_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ck_fenced_2;
SET koldstore.max_merge_seen_keys = 50;
SELECT count(*) AS ck_count_overlapping_under_cap FROM sqlreg.ck;
SET koldstore.max_merge_seen_keys = 0;
SELECT count(*) AS ck_count_exact, count(DISTINCT (tenant, id)) AS ck_distinct_exact FROM sqlreg.ck;
SELECT v AS ck_edited_value FROM sqlreg.ck WHERE tenant = 1 AND id = 77;
RESET koldstore.max_merge_seen_keys;
SELECT sqlreg.same_content('sqlreg.ck', 'sqlreg.ck_ctl', 'tenant, id') AS ck_content_equal_after_reflush;

-- boxes that intersect on the leading column only are NOT enough to be called overlapping, but a key
-- that really lives in two segments (same tenant, same id) must still be resolved to its newest version
SELECT count(*) AS ck_tenant2_rows FROM sqlreg.ck WHERE tenant = 2;
SELECT count(*) AS ck_ctl_tenant2_rows FROM sqlreg.ck_ctl WHERE tenant = 2;
