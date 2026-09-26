-- Same-transaction visibility (upstream #121, "fail closed"): once a transaction
-- has modified a managed table, a read that must consult its cold storage is
-- refused, because logical decoding cannot see uncommitted work and the cold row
-- for a key changed in this transaction would come back stale.
--
-- Errors are expected here and the transactions are driven by hand, so psql
-- keeps going after an error and each aborted transaction is rolled back
-- explicitly.

\set ON_ERROR_STOP off
\set VERBOSITY terse

CREATE FUNCTION sqlreg.settle() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM koldstore.wait_for_async_mirror();
END
$$;

-- ---------------------------------------------------------------- fixture
CREATE TABLE sqlreg.v1 (id bigint PRIMARY KEY, val text NOT NULL);
INSERT INTO sqlreg.v1 SELECT g, 'v' || g FROM generate_series(1, 4) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.v1'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS managed_with_cold;
SELECT sqlreg.flush_table('sqlreg.v1'::regclass) IS NOT NULL AS flushed;
SELECT sqlreg.settle();

-- A managed table that never gets flushed: no cold data, nothing to go stale.
CREATE TABLE sqlreg.v2 (id bigint PRIMARY KEY, val text NOT NULL);
SELECT koldstore.manage_table(
  table_name => 'sqlreg.v2'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS managed_hot_only;

SHOW koldstore.allow_same_txn_cold_reads;

-- ------------------------------------------- reads that must keep working
-- a transaction that has not written the table reads cold data normally
BEGIN;
SELECT count(*) AS read_only_txn FROM sqlreg.v1;
COMMIT;

-- a table with no cold data is unaffected, even after a write in the same txn
BEGIN;
INSERT INTO sqlreg.v2 VALUES (1, 'x');
SELECT count(*) AS hot_only_table_after_write FROM sqlreg.v2;
COMMIT;

-- writing one managed table does not restrict reads of another
BEGIN;
INSERT INTO sqlreg.v2 VALUES (2, 'y');
SELECT count(*) AS other_table_after_write FROM sqlreg.v1;
COMMIT;

-- ------------------------------------------- the #121 scenario, now refused
-- delete_row() then read the same key in the same transaction: the cold copy
-- would resurface until the async mirror masks it
BEGIN;
SELECT (koldstore.delete_row('sqlreg.v1'::regclass, '{"id": 1}'::jsonb, true) ->> 'deleted') AS deleted;
SELECT id FROM sqlreg.v1 WHERE id = 1;
ROLLBACK;
SELECT count(*) AS rolled_back_delete_still_visible FROM sqlreg.v1 WHERE id = 1;

-- a full scan after any write is refused too (fail closed, not just exact-PK)
BEGIN;
SELECT (koldstore.update_row('sqlreg.v1'::regclass, '{"id": 2}'::jsonb, '{"val": "u2"}'::jsonb, true) ->> 'updated') AS updated;
SELECT count(*) FROM sqlreg.v1;
ROLLBACK;

-- a brand-new key is refused as well: the check cannot know the key is new
BEGIN;
INSERT INTO sqlreg.v1 VALUES (10, 'new');
SELECT count(*) FROM sqlreg.v1;
ROLLBACK;

-- COPY reaches the same rule (it never goes through ExecutorEnd)
BEGIN;
COPY sqlreg.v1 (id, val) FROM STDIN WITH (FORMAT csv);
11,copied
\.
SELECT count(*) FROM sqlreg.v1;
ROLLBACK;

-- plain EXPLAIN builds the plan without reading, so it is fine after a write
BEGIN;
INSERT INTO sqlreg.v1 VALUES (12, 'e');
EXPLAIN (COSTS OFF) SELECT * FROM sqlreg.v1 WHERE id = 12;
ROLLBACK;

-- ---------------------------------------------------------- the escape hatch
BEGIN;
SET LOCAL koldstore.allow_same_txn_cold_reads = on;
SELECT (koldstore.delete_row('sqlreg.v1'::regclass, '{"id": 1}'::jsonb, true) ->> 'deleted') AS deleted;
-- the accepted risk: the deleted key's stale cold copy is what the read returns
SELECT id FROM sqlreg.v1 WHERE id = 1;
ROLLBACK;

-- ------------------------------------------------------------- savepoints
BEGIN;
SAVEPOINT s1;
SELECT (koldstore.delete_row('sqlreg.v1'::regclass, '{"id": 3}'::jsonb, true) ->> 'deleted') AS deleted;
ROLLBACK TO s1;
-- the write was rolled back with the savepoint: reads work again
SELECT count(*) AS after_rollback_to_savepoint FROM sqlreg.v1;
SAVEPOINT s2;
SELECT (koldstore.delete_row('sqlreg.v1'::regclass, '{"id": 3}'::jsonb, true) ->> 'deleted') AS deleted;
RELEASE SAVEPOINT s2;
-- a released savepoint hands its write to the parent: still refused
SELECT count(*) FROM sqlreg.v1;
ROLLBACK;

-- ---------------------------------- internal cold lookups keep working
-- several write helpers / INSERTs in one transaction all probe cold storage
BEGIN;
SELECT (koldstore.update_row('sqlreg.v1'::regclass, '{"id": 3}'::jsonb, '{"val": "u3"}'::jsonb, true) ->> 'updated') AS updated_3;
SELECT (koldstore.update_row('sqlreg.v1'::regclass, '{"id": 4}'::jsonb, '{"val": "u4"}'::jsonb, true) ->> 'updated') AS updated_4;
INSERT INTO sqlreg.v1 VALUES (20, 'a');
INSERT INTO sqlreg.v1 VALUES (21, 'b');
COMMIT;

-- after COMMIT (and the fence for the async mirror) everything is readable again
SELECT sqlreg.settle();
SELECT id, val FROM sqlreg.v1 ORDER BY id;
