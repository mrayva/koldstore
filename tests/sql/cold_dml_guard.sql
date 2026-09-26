-- Cold-tier write guard (upstream #122, Option B): INSERT / UPDATE / DELETE /
-- MERGE against rows that exist ONLY in cold storage must be rejected instead
-- of silently missing them.
--
-- Every statement runs through sqlreg.try(), which executes it in a
-- subtransaction and reports "ok: <rows affected>" or "ERROR: <first line>",
-- so one rejected statement never aborts the case and the result of each
-- statement is visible in the expected output.
--
-- Shapes the exact-primary-key analysis cannot name row by row (ranges, NOT IN,
-- OR across columns, ...) fall through to the generic cold-match guard, which
-- has its own case: cold_dml_scan_guard. The statements below that used to be
-- pinned as unguarded gaps run against a table earlier cases have already
-- changed, so they only assert that the newer guard does not misfire here.

\set VERBOSITY terse

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

-- ---------------------------------------------------------------- fixture
CREATE TABLE sqlreg.g1 (id bigint PRIMARY KEY, val text NOT NULL);
INSERT INTO sqlreg.g1 SELECT g, 'v' || g FROM generate_series(1, 6) g;

SELECT koldstore.manage_table(
  table_name => 'sqlreg.g1'::regclass,
  storage => 'sqlreg_fs',
  hot_row_limit => 10,
  min_flush_rows => 1,
  max_rows_per_file => 10,
  migration_order_by => 'id',
  auto_flush => false
) IS NOT NULL AS managed;

SELECT sqlreg.flush_table('sqlreg.g1'::regclass) IS NOT NULL AS flushed;
SELECT sqlreg.settle();

-- ids 1..6 are cold-only; ids 7, 8 below are hot.
SELECT (koldstore.table_status('sqlreg.g1'::regclass) ->> 'hot_rows')::int AS hot_rows,
       (koldstore.table_status('sqlreg.g1'::regclass) ->> 'cold_row_count')::int AS cold_rows;

SELECT sqlreg.try($$INSERT INTO sqlreg.g1 VALUES (7, 'v7'), (8, 'v8')$$) AS insert_new_hot_keys;
SELECT sqlreg.settle();

-- -------------------------------------------------------------------- INSERT
-- full wording of both rejection messages, once
SELECT sqlreg.try($$INSERT INTO sqlreg.g1 VALUES (3, 'dup')$$, true) AS insert_dup_cold_pk_full_message;
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE id = 2$$, true) AS delete_cold_eq_full_message;
SELECT sqlreg.try($$INSERT INTO sqlreg.g1 VALUES (3, 'dup')$$) AS insert_dup_cold_pk;
SELECT sqlreg.try($$INSERT INTO sqlreg.g1 VALUES (7, 'dup')$$) AS insert_dup_hot_pk;
SELECT sqlreg.try($$INSERT INTO sqlreg.g1 VALUES (9, 'v9')$$) AS insert_new_key;
SELECT sqlreg.try($$INSERT INTO sqlreg.g1 VALUES (10, 'ok'), (4, 'dup')$$) AS insert_multirow_one_dup_cold;
SELECT sqlreg.try($$INSERT INTO sqlreg.g1 SELECT 2, 'dup'$$) AS insert_select_dup_cold;

-- ---------------------------------------------------- UPDATE / DELETE, equality
SELECT sqlreg.try($$UPDATE sqlreg.g1 SET val = 'x' WHERE id = 2$$) AS update_cold_eq;
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE id = 2$$) AS delete_cold_eq;
SELECT sqlreg.try($$UPDATE sqlreg.g1 SET val = 'h' WHERE id = 7$$) AS update_hot_eq;
SELECT sqlreg.try($$UPDATE sqlreg.g1 SET val = 'x' WHERE id = 999$$) AS update_nonexistent;
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE id = 999$$) AS delete_nonexistent;

-- ------------------------------------------------------------------ IN lists
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE id IN (1, 2)$$) AS delete_in_all_cold;
SELECT sqlreg.try($$UPDATE sqlreg.g1 SET val = 'mixed' WHERE id IN (7, 1)$$) AS update_in_hot_and_cold;
-- atomic: the hot row that WAS matched natively is rolled back with the reject
SELECT id, val FROM sqlreg.g1 WHERE id IN (1, 7) ORDER BY id;
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE id IN (997, 998, 999)$$) AS delete_in_nonexistent;

-- -------------------------------------------- OR chains (same column = IN list)
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE id = 1 OR id = 2$$) AS delete_or_all_cold;
SELECT sqlreg.try($$UPDATE sqlreg.g1 SET val = 'x' WHERE id = 7 OR id = 3$$) AS update_or_hot_and_cold;
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE id = 1 OR id IN (2, 3)$$) AS delete_or_mixed_in;
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE id = 997 OR id = 998$$) AS delete_or_nonexistent;
-- an OR across DIFFERENT columns is caught by the generic cold-match guard
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE id = 1 OR val = 'v2'$$) AS or_across_columns;

-- --------------------------------- extra (residual) non-PK conditions
SELECT sqlreg.try($$UPDATE sqlreg.g1 SET val = 'x' WHERE id = 3 AND val = 'v3'$$) AS residual_matches_cold;
SELECT sqlreg.try($$UPDATE sqlreg.g1 SET val = 'x' WHERE id = 3 AND val = 'nope'$$) AS residual_no_match_cold;
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE id = 3 AND val IN ('nope', 'v3')$$) AS residual_in_matches_cold;
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE id = 3 AND val IN ('nope', 'never')$$) AS residual_in_no_match_cold;
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE (id = 3 OR id = 4) AND val = 'v4'$$) AS or_plus_residual_matches;
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE (id = 3 OR id = 4) AND val = 'nope'$$) AS or_plus_residual_no_match;

-- ------------------------------------------- shapes for the generic guard
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE id BETWEEN 1 AND 2$$) AS pk_range;
SELECT sqlreg.try($$UPDATE sqlreg.g1 SET val = 'x' WHERE id < 3$$) AS pk_less_than;
SELECT sqlreg.try($$DELETE FROM sqlreg.g1 WHERE id NOT IN (5, 6, 7, 8, 9, 10)$$) AS not_in;
-- ... and the cold rows those statements were meant to hit are all still there
SELECT sqlreg.settle();
SELECT id, val FROM sqlreg.g1 ORDER BY id;

-- ----------------------------------------------------------------------- MERGE
SELECT sqlreg.try($$MERGE INTO sqlreg.g1 t USING (VALUES (2, 'm')) AS s(id, val) ON t.id = s.id
                    WHEN MATCHED THEN DELETE$$) AS merge_delete_cold;
SELECT sqlreg.try($$MERGE INTO sqlreg.g1 t USING (VALUES (2, 'm')) AS s(id, val) ON t.id = s.id
                    WHEN MATCHED THEN UPDATE SET val = s.val$$) AS merge_update_cold;
SELECT sqlreg.try($$MERGE INTO sqlreg.g1 t USING (VALUES (11, 'm')) AS s(id, val) ON t.id = s.id
                    WHEN MATCHED THEN UPDATE SET val = s.val
                    WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val)$$) AS merge_new_key_inserts;
SELECT sqlreg.try($$MERGE INTO sqlreg.g1 t USING (VALUES (7, 'mh')) AS s(id, val) ON t.id = s.id
                    WHEN MATCHED THEN UPDATE SET val = s.val$$) AS merge_update_hot;

-- ------------------------------------------- EXPLAIN never trips the guard
SELECT sqlreg.try($$EXPLAIN (COSTS OFF) UPDATE sqlreg.g1 SET val = 'x' WHERE id = 2$$) AS explain_update_cold;
SELECT sqlreg.try($$EXPLAIN (COSTS OFF) DELETE FROM sqlreg.g1 WHERE id IN (1, 2)$$) AS explain_delete_in_cold;

-- ----------------------------------------- the supported escape hatches work
SELECT koldstore.update_row('sqlreg.g1'::regclass, '{"id": 2}'::jsonb, '{"val": "patched"}'::jsonb, true) AS update_row_cold;
SELECT koldstore.delete_row('sqlreg.g1'::regclass, '{"id": 1}'::jsonb, true) AS delete_row_cold;
SELECT koldstore.delete_row('sqlreg.g1'::regclass, '{"id": 999}'::jsonb, true) AS delete_row_missing;
SELECT sqlreg.settle();
SELECT id, val FROM sqlreg.g1 ORDER BY id;

-- ------------------------------------------------------ composite primary key
CREATE TABLE sqlreg.g2 (a int, b int, val text, PRIMARY KEY (a, b));
INSERT INTO sqlreg.g2 SELECT g, g * 10, 'w' || g FROM generate_series(1, 4) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.g2'::regclass,
  storage => 'sqlreg_fs',
  hot_row_limit => 10,
  min_flush_rows => 1,
  max_rows_per_file => 10,
  migration_order_by => 'a',
  auto_flush => false
) IS NOT NULL AS managed_composite;
SELECT sqlreg.flush_table('sqlreg.g2'::regclass) IS NOT NULL AS flushed_composite;
SELECT sqlreg.settle();

SELECT sqlreg.try($$INSERT INTO sqlreg.g2 VALUES (2, 20, 'dup')$$) AS comp_insert_dup_cold;
SELECT sqlreg.try($$INSERT INTO sqlreg.g2 VALUES (2, 21, 'ok')$$) AS comp_insert_new_second_col;
SELECT sqlreg.try($$UPDATE sqlreg.g2 SET val = 'x' WHERE a = 2 AND b = 20$$) AS comp_update_cold;
SELECT sqlreg.try($$DELETE FROM sqlreg.g2 WHERE a = 3 AND b = 30$$) AS comp_delete_cold;
SELECT sqlreg.try($$DELETE FROM sqlreg.g2 WHERE a = 3 AND b = 31$$) AS comp_delete_nonexistent;
SELECT sqlreg.try($$DELETE FROM sqlreg.g2 WHERE a IN (1, 2) AND b IN (10, 20)$$) AS comp_delete_in_lists;
-- OR across the two PK columns is not a single-column chain: generic guard
SELECT sqlreg.try($$DELETE FROM sqlreg.g2 WHERE a = 1 OR b = 20$$) AS comp_or_across_columns;
SELECT sqlreg.settle();
SELECT a, b, val FROM sqlreg.g2 ORDER BY val;

-- -------------------------------------------- guard is torn down with the table
SELECT koldstore.unmanage_table('sqlreg.g1'::regclass) IS NOT NULL AS unmanaged;
SELECT sqlreg.try($$INSERT INTO sqlreg.g1 VALUES (3, 'dup-after-unmanage')$$) AS insert_after_unmanage;
