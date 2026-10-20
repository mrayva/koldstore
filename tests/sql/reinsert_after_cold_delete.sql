-- A key deleted from cold storage could not be re-inserted for the few milliseconds until the background applier
-- had put the delete's tombstone into the mirror: the INSERT guard still found the key in the cold segment and
-- refused. When the key looks present and the applier is behind, the guard now waits (bounded, no lock) for the
-- applier to catch up and looks again. A genuine duplicate is still refused, and so is a delete+insert inside one
-- transaction (the delete is not committed yet, so nothing can be waited for).
--
-- Also: hydrate-on-write on a table with a column named `t` (the hydrate query aliases the table as `t`, and
-- `to_jsonb(t)` used to resolve to that column instead of the whole row).
--
-- Oracle: an unmanaged copy of the table.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE TABLE sqlreg.ri (id bigint PRIMARY KEY, n int NOT NULL, v text NOT NULL, t int NOT NULL);
INSERT INTO sqlreg.ri SELECT g, g, 'v' || g, g * 2 FROM generate_series(1, 100) g;
CREATE TABLE sqlreg.ri_ctl AS SELECT * FROM sqlreg.ri;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.ri'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS ri_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ri_caught_up;
SELECT sqlreg.flush_table('sqlreg.ri'::regclass) IS NOT NULL AS ri_flushed;
SELECT (koldstore.table_status('sqlreg.ri'::regclass) ->> 'cold_row_count')::int >= 90 AS ri_mostly_cold;

SET koldstore.hydrate_on_write = on;
-- a column named t
UPDATE sqlreg.ri SET v = 'edited' WHERE id = 7;
UPDATE sqlreg.ri_ctl SET v = 'edited' WHERE id = 7;
SELECT id, v, t FROM sqlreg.ri WHERE id = 7;

-- delete a cold row, then re-insert the same key at once (no fence in between)
DELETE FROM sqlreg.ri WHERE id = 8;
DELETE FROM sqlreg.ri_ctl WHERE id = 8;
INSERT INTO sqlreg.ri VALUES (8, 8, 'back', 16);
INSERT INTO sqlreg.ri_ctl VALUES (8, 8, 'back', 16);
SELECT id, v FROM sqlreg.ri WHERE id = 8;

-- and again, several keys back to back
DELETE FROM sqlreg.ri WHERE id IN (21, 22, 23);
DELETE FROM sqlreg.ri_ctl WHERE id IN (21, 22, 23);
INSERT INTO sqlreg.ri VALUES (21, 21, 'again21', 42);
INSERT INTO sqlreg.ri VALUES (22, 22, 'again22', 44);
INSERT INTO sqlreg.ri_ctl VALUES (21, 21, 'again21', 42);
INSERT INTO sqlreg.ri_ctl VALUES (22, 22, 'again22', 44);
RESET koldstore.hydrate_on_write;

-- a genuine duplicate of a cold key is still refused
INSERT INTO sqlreg.ri VALUES (9, 9, 'dup', 18);
-- a key deleted and re-inserted in ONE transaction is refused (the delete is not committed yet)
SET koldstore.hydrate_on_write = on;
BEGIN;
DELETE FROM sqlreg.ri WHERE id = 30;
INSERT INTO sqlreg.ri VALUES (30, 30, 'same txn', 60);
ROLLBACK;
RESET koldstore.hydrate_on_write;

-- with the fence disabled the old behaviour is back for an immediate re-insert
SET koldstore.hydrate_on_write = on;
SET koldstore.insert_guard_fence = off;
DELETE FROM sqlreg.ri WHERE id = 40;
INSERT INTO sqlreg.ri VALUES (40, 40, 'immediate', 80);
RESET koldstore.insert_guard_fence;
RESET koldstore.hydrate_on_write;

SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ri_fenced;
-- id 30 rolled back, 40 deleted by the last statement (its insert was refused)
DELETE FROM sqlreg.ri_ctl WHERE id = 40;
SELECT (SELECT md5(string_agg(r::text, '|' ORDER BY id)) FROM sqlreg.ri r)
     = (SELECT md5(string_agg(c::text, '|' ORDER BY id)) FROM sqlreg.ri_ctl c) AS ri_content_equal;
SELECT count(*) AS ri_total FROM sqlreg.ri;

-- the re-inserted keys survive a flush
SELECT sqlreg.flush_table('sqlreg.ri'::regclass) IS NOT NULL AS ri_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ri_fenced_2;
SET koldstore.max_merge_seen_keys = 0;
SELECT (SELECT md5(string_agg(r::text, '|' ORDER BY id)) FROM sqlreg.ri r)
     = (SELECT md5(string_agg(c::text, '|' ORDER BY id)) FROM sqlreg.ri_ctl c) AS ri_content_equal_after_flush;
SELECT id, v FROM sqlreg.ri WHERE id IN (8, 21, 22) ORDER BY id;
RESET koldstore.max_merge_seen_keys;
