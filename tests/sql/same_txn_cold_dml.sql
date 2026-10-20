-- An UPDATE/DELETE that matches cold rows after an earlier write to the table in the same transaction
-- cannot hydrate them (the merged view is stale for keys changed here). It used to skip them silently, so
-- a DELETE of 601 rows deleted only the already-hot ones. It is now refused unless nothing cold matches.

\set ON_ERROR_STOP off
\set VERBOSITY terse

CREATE TABLE sqlreg.sd (id bigint PRIMARY KEY, n int NOT NULL, v text);
INSERT INTO sqlreg.sd SELECT g, g, 'v' || g FROM generate_series(1, 40) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.sd'::regclass, storage => 'sqlreg_fs', hot_row_limit => 2,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS sd_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS sd_caught_up;
SELECT sqlreg.flush_table('sqlreg.sd'::regclass) IS NOT NULL AS sd_flushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS sd_fenced;
SET koldstore.hydrate_on_write = on;

-- first statement hydrates 5, 10 ...; a second one over other cold rows is refused, not skipped
BEGIN;
UPDATE sqlreg.sd SET v = 'a' WHERE id IN (5, 10);
DELETE FROM sqlreg.sd WHERE id BETWEEN 20 AND 25;
ROLLBACK;

-- re-editing rows this transaction already hydrated is fine
BEGIN;
UPDATE sqlreg.sd SET v = 'a' WHERE id IN (5, 10);
UPDATE sqlreg.sd SET v = 'b' WHERE id IN (5, 10);
DELETE FROM sqlreg.sd WHERE id = 5;
COMMIT;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS sd_fenced_2;
SELECT id, v FROM sqlreg.sd WHERE id IN (5, 10) ORDER BY id;

-- a later statement matching nothing cold is fine too
BEGIN;
INSERT INTO sqlreg.sd VALUES (100, 1, 'new');
UPDATE sqlreg.sd SET v = 'x' WHERE id = 100;
COMMIT;

-- the same statements in their own transactions apply to every cold row
DELETE FROM sqlreg.sd WHERE id BETWEEN 20 AND 25;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS sd_fenced_3;
SELECT count(*) AS sd_between_20_25 FROM sqlreg.sd WHERE id BETWEEN 20 AND 25;

-- the documented escape hatch skips the cold rows (old behaviour)
BEGIN;
SET LOCAL koldstore.allow_same_txn_cold_reads = on;
UPDATE sqlreg.sd SET v = 'a' WHERE id = 11;
DELETE FROM sqlreg.sd WHERE id BETWEEN 30 AND 32;
COMMIT;
RESET koldstore.hydrate_on_write;
