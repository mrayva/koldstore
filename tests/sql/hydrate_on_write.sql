-- EXPERIMENTAL hydrate-on-write (ADR-007 option B): with koldstore.hydrate_on_write
-- on, a plain single-table UPDATE/DELETE changes cold-only rows instead of being
-- rejected, by inserting the cold-only rows its WHERE clause matches into the heap
-- first. Default off: the write guards reject as before.

\set VERBOSITY terse
\set ON_ERROR_STOP off

-- try(stmt): "ok: <rows>" | "REJECTED insert" | "REJECTED write pk=<key>" | "ERROR: ..."
-- try(stmt, true): same, but a koldstore rejection keeps its complete message.
CREATE FUNCTION sqlreg.try(stmt text, keep_message boolean DEFAULT false) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  n bigint;
  msg text;
BEGIN
  EXECUTE stmt;
  IF stmt ~* '^\s*explain' THEN
    RETURN 'ok (explain)';
  END IF;
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN 'ok: ' || n;
EXCEPTION WHEN OTHERS THEN
  msg := split_part(SQLERRM, E'\n', 1);
  IF keep_message THEN
    RETURN 'ERROR: ' || msg;
  ELSIF msg LIKE 'koldstore: refusing INSERT%' THEN
    RETURN 'REJECTED insert';
  ELSIF msg LIKE 'koldstore: refusing this UPDATE/DELETE on managed table%' THEN
    RETURN 'REJECTED scan matches=' || substring(msg from 'also matches ([0-9]+) cold');
  ELSIF msg LIKE 'koldstore: refusing this MERGE on managed table%' THEN
    RETURN 'REJECTED merge';
  ELSIF msg LIKE 'koldstore: refusing this UPDATE/DELETE/MERGE%' THEN
    RETURN 'REJECTED write pk=' || substring(msg from 'primary key (\{[^}]*\})');
  END IF;
  RETURN 'ERROR: ' || msg;
END
$$;

-- Committed mirror work applied, so reads below see completed masking.
CREATE FUNCTION sqlreg.settle() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM koldstore.wait_for_async_mirror();
END
$$;

-- val(query): first column of the first row as text
CREATE FUNCTION sqlreg.val(stmt text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE result text;
BEGIN
  EXECUTE stmt INTO result;
  RETURN coalesce(result, '(null)');
EXCEPTION WHEN OTHERS THEN
  RETURN 'ERROR: ' || split_part(SQLERRM, E'\n', 1);
END
$$;

CREATE TABLE sqlreg.h1 (id bigint PRIMARY KEY, val text NOT NULL, grp int NOT NULL);
INSERT INTO sqlreg.h1 SELECT g, 'v' || g, g % 3 FROM generate_series(1, 20) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.h1'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS managed;
SELECT sqlreg.flush_table('sqlreg.h1'::regclass) IS NOT NULL AS flushed;
SELECT sqlreg.settle();
INSERT INTO sqlreg.h1 VALUES (21, 'v21', 0), (22, 'v22', 1);
SELECT sqlreg.settle();
-- ids 1..20 are cold-only; 21 and 22 are hot
SELECT count(*) AS total_rows FROM sqlreg.h1;

-- default off: rejected as before
SELECT sqlreg.try($$DELETE FROM sqlreg.h1 WHERE id < 3$$) AS delete_default_off;

SET koldstore.hydrate_on_write = on;

-- DELETE of cold rows by range, then the effect once the mirror has applied it
SELECT sqlreg.try($$DELETE FROM sqlreg.h1 WHERE id BETWEEN 1 AND 3$$) AS delete_cold_range;
SELECT sqlreg.settle();
SELECT sqlreg.val($$SELECT string_agg(id::text, ',' ORDER BY id) FROM sqlreg.h1 WHERE id <= 6$$) AS remaining_low_ids;
SELECT sqlreg.val($$SELECT count(*) FROM sqlreg.h1$$) AS total_after_delete;

-- UPDATE of cold rows by a non-key predicate (mixed cold and hot matches)
SELECT sqlreg.try($$UPDATE sqlreg.h1 SET val = 'upd' WHERE grp = 1$$) AS update_cold_and_hot_by_group;
SELECT sqlreg.settle();
SELECT sqlreg.val($$SELECT string_agg(id::text, ',' ORDER BY id) FROM sqlreg.h1 WHERE val = 'upd'$$) AS updated_ids;
SELECT sqlreg.val($$SELECT count(*) FROM sqlreg.h1$$) AS total_after_update;

-- a statement matching nothing hydrates nothing
SELECT sqlreg.try($$UPDATE sqlreg.h1 SET val = 'none' WHERE id > 1000$$) AS update_matching_nothing;

-- RETURNING sees the hydrated rows
DELETE FROM sqlreg.h1 WHERE id IN (5, 6) RETURNING id, val;
SELECT sqlreg.settle();
SELECT sqlreg.val($$SELECT count(*) FROM sqlreg.h1 WHERE id IN (5, 6)$$) AS rows_5_6_after;

-- a data-modifying CTE is not seen by the ExecutorStart hook (its top-level
-- statement is a SELECT) and, separately, bypasses the write guards
CREATE TABLE sqlreg.h_cte_note (x int);
SELECT sqlreg.val($$WITH d AS (DELETE FROM sqlreg.h1 WHERE id = 14 RETURNING id) SELECT count(*) FROM d$$) AS cte_delete_matched;
SELECT sqlreg.settle();
SELECT sqlreg.val($$SELECT count(*) FROM sqlreg.h1 WHERE id = 14$$) AS row_14_after_cte_delete;

-- rollback leaves the cold rows exactly as they were (id 8 is cold-only)
SELECT sqlreg.val($$SELECT count(*) FROM sqlreg.h1$$) AS total_before_rollback;
BEGIN;
DELETE FROM sqlreg.h1 WHERE id = 8;
ROLLBACK;
SELECT sqlreg.settle();
SELECT sqlreg.val($$SELECT count(*) FROM sqlreg.h1 WHERE id = 8$$) AS row_8_after_rollback;
SELECT sqlreg.val($$SELECT count(*) FROM sqlreg.h1$$) AS total_after_rollback;

-- the read after a write in the same transaction is refused (upstream #121)
BEGIN;
DELETE FROM sqlreg.h1 WHERE id = 9;
SELECT sqlreg.val($$SELECT count(*) FROM sqlreg.h1$$) AS read_after_hydrating_write;
ROLLBACK;

-- cap
SET koldstore.max_hydrate_rows = 2;
SELECT sqlreg.try($$DELETE FROM sqlreg.h1 WHERE id BETWEEN 15 AND 20$$, true) AS delete_over_cap;
RESET koldstore.max_hydrate_rows;
SELECT sqlreg.val($$SELECT count(*) FROM sqlreg.h1 WHERE id BETWEEN 15 AND 20$$) AS rows_untouched_after_cap;

-- prepared statements, custom and generic plans (verify the effect: ROW_COUNT is
-- always 0 for EXECUTE run through PL/pgSQL, with or without this feature)
PREPARE del_one(bigint) AS DELETE FROM sqlreg.h1 WHERE id = $1;
SELECT sqlreg.try($$EXECUTE del_one(11)$$) AS prepared_custom_plan;
SET plan_cache_mode = force_generic_plan;
SELECT sqlreg.try($$EXECUTE del_one(12)$$) AS prepared_generic_plan;
SELECT sqlreg.try($$EXECUTE del_one(15)$$) AS prepared_generic_plan_again;
RESET plan_cache_mode;
SELECT sqlreg.settle();
SELECT sqlreg.val($$SELECT count(*) FROM sqlreg.h1 WHERE id IN (11, 12, 15)$$) AS rows_11_12_15_after;

-- other isolation levels fall back to the guards (the snapshot cannot be advanced)
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT sqlreg.try($$DELETE FROM sqlreg.h1 WHERE id = 18$$) AS repeatable_read_delete;
ROLLBACK;

-- joins and sub-queries hydrate through the planner hook's probe: the cold rows the join
-- matches (and only those) are pulled into the heap first
CREATE TABLE sqlreg.h_src (id bigint PRIMARY KEY, tag text);
INSERT INTO sqlreg.h_src VALUES (22, 'a'), (19, 'b'), (21, 'c'), (9999, 'no-such-row');
SELECT sqlreg.val($$SELECT count(*) FROM sqlreg.h1 WHERE id IN (22, 19, 21, 17)$$) AS join_targets_before;
SELECT sqlreg.try($$DELETE FROM sqlreg.h1 USING sqlreg.h_src s WHERE h1.id = s.id AND s.tag = 'a'$$) AS join_delete;
SELECT sqlreg.try($$UPDATE sqlreg.h1 SET val = 'joined' FROM sqlreg.h_src s WHERE h1.id = s.id AND s.tag = 'b'$$) AS join_update;
SELECT sqlreg.try($$UPDATE sqlreg.h1 SET val = 'sub' WHERE id IN (SELECT id FROM sqlreg.h_src WHERE tag = 'c')$$) AS subquery_update;
SELECT sqlreg.try($$DELETE FROM sqlreg.h1 WHERE EXISTS (SELECT 1 FROM sqlreg.h_src s WHERE s.id = h1.id AND s.tag = 'nope')$$) AS exists_matching_nothing;
SELECT sqlreg.settle();
SELECT sqlreg.val($$SELECT string_agg(id || '=' || val, ',' ORDER BY id) FROM sqlreg.h1 WHERE id IN (17, 19, 21, 22)$$) AS join_results;
-- with a bound parameter (generic and custom plans)
PREPARE h_join(text) AS UPDATE sqlreg.h1 SET val = 'prep' FROM sqlreg.h_src s WHERE h1.id = s.id AND s.tag = $1;
INSERT INTO sqlreg.h_src VALUES (17, 'd');
SELECT sqlreg.try($$EXECUTE h_join('d')$$) AS prepared_join_update;
SELECT sqlreg.settle();
SELECT sqlreg.val($$SELECT val FROM sqlreg.h1 WHERE id = 17$$) AS prepared_join_result;
DEALLOCATE h_join;

-- User triggers: the hydration INSERT must not fire ordinary triggers (the row already
-- exists logically); the user's own DELETE does. A trigger marked ENABLE ALWAYS still
-- fires for hydration and can tell it apart through koldstore.hydrating.
CREATE TABLE sqlreg.h_log (op text, id bigint, hydrating text);
CREATE FUNCTION sqlreg.h_trg() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO sqlreg.h_log VALUES (TG_OP, coalesce(NEW.id, OLD.id), current_setting('koldstore.hydrating', true));
  RETURN coalesce(NEW, OLD);
END $$;
CREATE TRIGGER h_after AFTER INSERT OR DELETE ON sqlreg.h1 FOR EACH ROW EXECUTE FUNCTION sqlreg.h_trg();
SELECT sqlreg.try($$DELETE FROM sqlreg.h1 WHERE id = 20$$) AS delete_with_triggers;
SELECT op, id, hydrating FROM sqlreg.h_log ORDER BY op, id;
-- an ALWAYS trigger sees the hydration, flagged
CREATE TRIGGER h_always AFTER INSERT ON sqlreg.h1 FOR EACH ROW EXECUTE FUNCTION sqlreg.h_trg();
ALTER TABLE sqlreg.h1 ENABLE ALWAYS TRIGGER h_always;
TRUNCATE sqlreg.h_log;
SELECT sqlreg.try($$DELETE FROM sqlreg.h1 WHERE id = 18$$) AS delete_with_always_trigger;
SELECT op, id, hydrating FROM sqlreg.h_log ORDER BY op, id;
SELECT current_setting('koldstore.hydrating') AS hydrating_after_statement;
SELECT current_setting('session_replication_role') AS role_after_statement;
DROP TRIGGER h_after ON sqlreg.h1;
DROP TRIGGER h_always ON sqlreg.h1;

-- explicit cold-row functions share the same hydration: update_row fires the UPDATE only
CREATE TRIGGER h_after AFTER INSERT OR UPDATE OR DELETE ON sqlreg.h1 FOR EACH ROW EXECUTE FUNCTION sqlreg.h_trg();
TRUNCATE sqlreg.h_log;
SELECT koldstore.update_row('sqlreg.h1'::regclass, '{"id": 17}', '{"val": "explicit"}') ->> 'updated' AS update_row_updated;
SELECT op, id FROM sqlreg.h_log ORDER BY op, id;
DROP TRIGGER h_after ON sqlreg.h1;

-- foreign keys. A flush-enabled manage_table() refuses a table with an existing foreign key by
-- default (upstream #122: koldstore enforces FKs on hot rows only, and a flush can silently move
-- either side to cold storage, out of PostgreSQL's own FK triggers' reach); allow_fk_hot_only
-- accepts that risk explicitly.
CREATE TABLE sqlreg.h_fk_p (id bigint PRIMARY KEY);
CREATE TABLE sqlreg.h_fk_c (id bigint PRIMARY KEY, pid bigint REFERENCES sqlreg.h_fk_p (id));
SELECT sqlreg.try($$SELECT koldstore.manage_table(table_name => 'sqlreg.h_fk_c'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10, min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id')::text$$, true) AS fk_refused_by_default;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.h_fk_c'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', allow_fk_hot_only => true
) IS NOT NULL AS fk_allowed_opt_in;

-- A cold child whose parent is gone: the hydration insert must not trip the FK
-- (referential-integrity triggers are off under hydration), while the FK itself still refuses an
-- ordinary insert. This fixture instead adds the FK after management (the child heap is empty
-- then), the other way to reach the same cold-child-with-FK state without the opt-in.
CREATE TABLE sqlreg.h_parent (id bigint PRIMARY KEY);
INSERT INTO sqlreg.h_parent VALUES (1), (2);
CREATE TABLE sqlreg.h_child (id bigint PRIMARY KEY, pid bigint NOT NULL, v text);
INSERT INTO sqlreg.h_child SELECT g, 1, 'c' || g FROM generate_series(1, 30) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.h_child'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS child_managed;
SELECT sqlreg.flush_table('sqlreg.h_child'::regclass) IS NOT NULL AS child_flushed;
SELECT sqlreg.settle();
ALTER TABLE sqlreg.h_child ADD CONSTRAINT h_child_pid_fk FOREIGN KEY (pid) REFERENCES sqlreg.h_parent (id);
-- deleting the parent runs an RI scan of the child with FOR KEY SHARE, which cannot see cold rows
SELECT sqlreg.try($$DELETE FROM sqlreg.h_parent WHERE id = 1$$, true) AS delete_parent_with_cold_children;
-- remove it the way a replicated delete would (RI triggers off), leaving cold children dangling
BEGIN;
SET LOCAL session_replication_role = replica;
DELETE FROM sqlreg.h_parent WHERE id = 1;
COMMIT;
SELECT sqlreg.try($$INSERT INTO sqlreg.h_child VALUES (100, 1, 'new')$$, true) AS ordinary_insert_refused_by_fk;
SELECT koldstore.delete_row('sqlreg.h_child'::regclass, '{"id": 3}') ->> 'deleted' AS delete_row_dangling_child;
DELETE FROM sqlreg.h_child WHERE id IN (4, 5) RETURNING id;
SELECT sqlreg.try($$UPDATE sqlreg.h_child SET v = 'dangling' WHERE id = 6$$, true) AS update_dangling_child_fk_recheck;
SELECT sqlreg.settle();
SELECT sqlreg.val($$SELECT count(*) FROM sqlreg.h_child WHERE id IN (3, 4, 5)$$) AS deleted_children_remaining;
SELECT sqlreg.val($$SELECT v FROM sqlreg.h_child WHERE id = 6$$) AS child_6_unchanged;
-- unmanaging with the cold rows pulled back is hydration too: no FK failure, no trigger per row
CREATE TRIGGER h_child_trg AFTER INSERT ON sqlreg.h_child FOR EACH ROW EXECUTE FUNCTION sqlreg.h_trg();
TRUNCATE sqlreg.h_log;
SELECT koldstore.unmanage_table('sqlreg.h_child'::regclass, true) IS NOT NULL AS child_unmanaged;
SELECT count(*) AS child_rows_after_unmanage FROM sqlreg.h_child;
SELECT count(*) AS insert_triggers_fired_by_unmanage FROM sqlreg.h_log;

-- A statement that fails after its mirror fence must not lose an earlier committed delete: the
-- fence's applied progress is never recorded or acknowledged, so the rolled-back work is
-- decoded again instead of being skipped by an advanced slot.
CREATE TABLE sqlreg.h_ab (id bigint PRIMARY KEY, v text);
INSERT INTO sqlreg.h_ab SELECT g, 'r' || g FROM generate_series(1, 6) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.h_ab'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS ab_managed;
SELECT sqlreg.flush_table('sqlreg.h_ab'::regclass) IS NOT NULL AS ab_flushed;
SELECT sqlreg.settle();
SET koldstore.hydrate_on_write = on;
DELETE FROM sqlreg.h_ab WHERE id = 1;
BEGIN;
SAVEPOINT s;
SET LOCAL koldstore.max_hydrate_rows = 1;
DELETE FROM sqlreg.h_ab WHERE id BETWEEN 2 AND 5;
ROLLBACK TO s;
DELETE FROM sqlreg.h_ab WHERE id = 6;
COMMIT;
RESET koldstore.hydrate_on_write;
SELECT sqlreg.settle();
SELECT sqlreg.settle();
SELECT id FROM sqlreg.h_ab ORDER BY id;

-- the job lock is released after the statements: a flush still runs
SELECT sqlreg.flush_table('sqlreg.h1'::regclass) IS NOT NULL AS flush_after_hydration;
