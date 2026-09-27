-- Compatibility matrix for constructs that cannot be honoured over hot + cold
-- data (upstream #125): each is either refused with a clear error before any
-- rows or side effects, or pinned here as supported.
--
-- Regression: TABLESAMPLE quietly sampled only the hot heap (cold rows were all
-- returned), and row-level locks failed late with an "system attribute -1" error.

\set ON_ERROR_STOP off
\set VERBOSITY terse

CREATE TABLE sqlreg.u1 (id bigint PRIMARY KEY, val text NOT NULL);
INSERT INTO sqlreg.u1 SELECT g, 'c' || g FROM generate_series(1, 6) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.u1'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS managed;
SELECT sqlreg.flush_table('sqlreg.u1'::regclass) IS NOT NULL AS flushed;
INSERT INTO sqlreg.u1 VALUES (100, 'hot');
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS settled;

-- A managed table that never got flushed: no cold data, so nothing is refused.
CREATE TABLE sqlreg.u2 (id bigint PRIMARY KEY, val text NOT NULL);
SELECT koldstore.manage_table(
  table_name => 'sqlreg.u2'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS managed_hot_only;
INSERT INTO sqlreg.u2 SELECT g, 'h' || g FROM generate_series(1, 5) g;

SELECT count(*) AS baseline FROM sqlreg.u1;

-- ------------------------------------------------------------- TABLESAMPLE
SELECT count(*) FROM sqlreg.u1 TABLESAMPLE BERNOULLI (0);
SELECT count(*) FROM sqlreg.u1 TABLESAMPLE SYSTEM (100);
SELECT count(*) AS hot_only_table_tablesample FROM sqlreg.u2 TABLESAMPLE BERNOULLI (100);

-- ------------------------------------------------------ row-level locking
SELECT id FROM sqlreg.u1 WHERE id = 2 FOR UPDATE;
SELECT id FROM sqlreg.u1 FOR SHARE;
SELECT id FROM sqlreg.u1 FOR NO KEY UPDATE;
SELECT id FROM sqlreg.u1 FOR KEY SHARE;
SELECT a.id FROM sqlreg.u2 a JOIN sqlreg.u1 b ON b.id = a.id FOR UPDATE OF a;
SELECT id FROM sqlreg.u2 WHERE id = 3 FOR UPDATE;
-- an exact-key lock of a hot row is served without touching cold data
SELECT id FROM sqlreg.u1 WHERE id = 100 FOR UPDATE;

-- ------------------------------------------------- system columns / ctid
SELECT ctid FROM sqlreg.u1;
SELECT count(*) AS hot_only_table_ctid FROM (SELECT ctid FROM sqlreg.u2) s;

-- ------------------------------------------------------ isolation levels
-- REPEATABLE READ is unaffected: only SERIALIZABLE's guarantee is at risk.
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) AS repeatable_read FROM sqlreg.u1;
COMMIT;
-- Default (2026-09-27, matching upstream #121's same-transaction guard): cold rows take no SSI
-- predicate locks, so a SERIALIZABLE transaction reading them refuses rather than silently running
-- with a weaker guarantee (upstream #125). Hot-only reads, and an exact hot-key lookup, are unaffected.
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT count(*) AS serializable_default_rejected FROM sqlreg.u1;
ROLLBACK;
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT count(*) AS serializable_default_hot_only FROM sqlreg.u2;
SELECT count(*) AS serializable_default_exact_hot_key FROM sqlreg.u1 WHERE id = 100;
COMMIT;
-- SET ... = off accepts the weaker guarantee explicitly, the pre-2026-09-27 behavior.
SET koldstore.reject_serializable_cold_reads = off;
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT count(*) AS serializable_policy_off FROM sqlreg.u1;
COMMIT;
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) AS repeatable_read_policy_off FROM sqlreg.u1;
COMMIT;
RESET koldstore.reject_serializable_cold_reads;

-- ------------------------------------------------------------- TRUNCATE
CREATE TABLE sqlreg.u_child (id int PRIMARY KEY, p bigint REFERENCES sqlreg.u1 (id));
TRUNCATE sqlreg.u1;
TRUNCATE sqlreg.u1 CASCADE;
-- refused before any effect: nothing was truncated, including the referencing table
SELECT count(*) AS rows_after_truncate FROM sqlreg.u1;

-- ------------------------------------------------------------- RENAME / SET SCHEMA
-- Object-store paths are derived from the table/schema name (upstream #123): renaming
-- either after cold publication would orphan the segments already written under the
-- old name. u2 has no cold data, so it is unaffected; RENAME COLUMN never touches a
-- storage path either way. u1 stays as sqlreg.u1 throughout (every rename/move of it
-- below is refused), so later sections of this file can keep referring to it.
ALTER TABLE sqlreg.u1 RENAME TO u1_renamed;
ALTER TABLE sqlreg.u2 RENAME TO u2_renamed;
CREATE SCHEMA sqlreg2;
ALTER TABLE sqlreg.u1 SET SCHEMA sqlreg2;
ALTER TABLE sqlreg.u2_renamed SET SCHEMA sqlreg2;
ALTER TABLE sqlreg.u1 RENAME COLUMN val TO val2;
ALTER SCHEMA sqlreg RENAME TO sqlreg_renamed;
SELECT count(*) AS u2_rows_after_move FROM sqlreg2.u2_renamed;
DROP SCHEMA sqlreg2 CASCADE;
SELECT to_regclass('sqlreg.u_child') IS NOT NULL AS child_still_there;
