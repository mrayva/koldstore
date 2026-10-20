-- compact_table on a composite primary key: overlapping segments (older versions of re-flushed keys,
-- delete markers) are merged to the newest version of each (tenant, id), content stays identical to an
-- unmanaged control, and a repeat finds nothing more to do.

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

-- every segment spans all tenants; ids ascend with the load order
CREATE TABLE sqlreg.cc (tenant int NOT NULL, id bigint NOT NULL, n int NOT NULL, v text, PRIMARY KEY (tenant, id));
INSERT INTO sqlreg.cc SELECT g % 4, g, g, 'v' || g FROM generate_series(1, 400) g;
CREATE TABLE sqlreg.cc_ctl AS SELECT * FROM sqlreg.cc;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.cc'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS cc_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cc_caught_up;
SELECT sqlreg.flush_table('sqlreg.cc'::regclass) IS NOT NULL AS cc_flushed;

-- full segments with separated boxes: nothing to do
SELECT (koldstore.compact_table('sqlreg.cc'::regclass, grace_seconds => 3600) ->> 'compacted')::boolean AS cc_noop_first;

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.cc SET v = 'edited' WHERE (tenant, id) IN ((3, 3), (1, 77), (1, 205), (3, 399));
UPDATE sqlreg.cc_ctl SET v = 'edited' WHERE (tenant, id) IN ((3, 3), (1, 77), (1, 205), (3, 399));
DELETE FROM sqlreg.cc WHERE id BETWEEN 100 AND 104;
DELETE FROM sqlreg.cc_ctl WHERE id BETWEEN 100 AND 104;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cc_fenced;
SELECT sqlreg.flush_table('sqlreg.cc'::regclass) IS NOT NULL AS cc_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cc_fenced_2;
SELECT sqlreg.same_content('sqlreg.cc', 'sqlreg.cc_ctl', 'tenant, id') AS cc_content_before;
CREATE TEMP TABLE cc_before AS
SELECT count(*) AS segs, sum(row_count) AS rows
FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.cc'::regclass::oid AND status = 'active';

CREATE TEMP TABLE cc_report AS SELECT koldstore.compact_table('sqlreg.cc'::regclass, grace_seconds => 3600) AS r;
SELECT (r ->> 'compacted')::boolean AS cc_compacted,
       (r ->> 'rows_written')::int < (r ->> 'rows_read')::int AS cc_dropped_old_versions
FROM cc_report;
SELECT sqlreg.same_content('sqlreg.cc', 'sqlreg.cc_ctl', 'tenant, id') AS cc_content_after;
SELECT (SELECT sum(row_count) FROM koldstore.cold_segments
         WHERE table_oid = 'sqlreg.cc'::regclass::oid AND status = 'active') < (SELECT rows FROM cc_before) AS cc_rows_shrank;
SELECT v AS cc_edited_value FROM sqlreg.cc WHERE tenant = 1 AND id = 77;
SELECT count(*) AS cc_deleted_rows_gone FROM sqlreg.cc WHERE id BETWEEN 100 AND 104;

-- a repeat finds no gain instead of rewriting the same layout
SELECT (koldstore.compact_table('sqlreg.cc'::regclass, grace_seconds => 3600) ->> 'compacted')::boolean AS cc_noop_second;
SELECT koldstore.reap_compacted_segments('sqlreg.cc'::regclass, 0) > 0 AS cc_reaped;
SELECT koldstore.recover_segments('sqlreg.cc'::regclass, true) AS cc_orphans;
SELECT koldstore.validate_cold_storage('sqlreg.cc'::regclass) ->> 'ok' AS cc_valid;

-- writes and flushes carry on over the compacted layout
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.cc SET v = 'again' WHERE (tenant, id) IN ((1, 5), (2, 6), (0, 300));
UPDATE sqlreg.cc_ctl SET v = 'again' WHERE (tenant, id) IN ((1, 5), (2, 6), (0, 300));
INSERT INTO sqlreg.cc VALUES (2, 1001, 1001, 'late');
INSERT INTO sqlreg.cc_ctl VALUES (2, 1001, 1001, 'late');
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cc_fenced_3;
SELECT sqlreg.flush_table('sqlreg.cc'::regclass) IS NOT NULL AS cc_reflushed_2;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cc_fenced_4;
SELECT sqlreg.same_content('sqlreg.cc', 'sqlreg.cc_ctl', 'tenant, id') AS cc_content_final;
SELECT (koldstore.compact_table('sqlreg.cc'::regclass, grace_seconds => 0) ->> 'compacted')::boolean AS cc_compacted_again;
SELECT sqlreg.same_content('sqlreg.cc', 'sqlreg.cc_ctl', 'tenant, id') AS cc_content_after_second;

-- a composite key with a text column
CREATE TABLE sqlreg.cc_txt (org text NOT NULL, sq int NOT NULL, n int NOT NULL, v int NOT NULL, PRIMARY KEY (org, sq));
INSERT INTO sqlreg.cc_txt SELECT 'org' || (g % 3), g, g, g FROM generate_series(1, 200) g;
CREATE TABLE sqlreg.cc_txt_ctl AS SELECT * FROM sqlreg.cc_txt;
SELECT koldstore.manage_table(table_name => 'sqlreg.cc_txt'::regclass, storage => 'sqlreg_fs',
  hot_row_limit => 5, min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n',
  auto_flush => false) IS NOT NULL AS cct_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cct_caught_up;
SELECT sqlreg.flush_table('sqlreg.cc_txt'::regclass) IS NOT NULL AS cct_flushed;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.cc_txt SET v = v + 1000 WHERE (org, sq) IN (('org1', 4), ('org0', 90), ('org2', 152));
UPDATE sqlreg.cc_txt_ctl SET v = v + 1000 WHERE (org, sq) IN (('org1', 4), ('org0', 90), ('org2', 152));
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cct_fenced;
SELECT sqlreg.flush_table('sqlreg.cc_txt'::regclass) IS NOT NULL AS cct_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS cct_fenced_2;
SELECT (koldstore.compact_table('sqlreg.cc_txt'::regclass, grace_seconds => 0) ->> 'compacted')::boolean AS cct_compacted;
SELECT sqlreg.same_content('sqlreg.cc_txt', 'sqlreg.cc_txt_ctl', 'org, sq') AS cct_content_after;
