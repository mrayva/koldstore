-- Spock reconciliation helpers (docs/multi-master.md). Spock itself is not installed
-- here, so this covers everything that does not read spock.exception_log: the tuple
-- conversion, the primary-key extraction, replaying INSERT/UPDATE/DELETE on a managed table with
-- cold rows and on a plain table, and the guard rails of reconcile_spock_conflicts().

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

CREATE TABLE sqlreg.sr1 (id bigint PRIMARY KEY, val text NOT NULL, n int);
INSERT INTO sqlreg.sr1 SELECT g, 'v' || g, g FROM generate_series(1, 15) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.sr1'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS managed;
SELECT sqlreg.flush_table('sqlreg.sr1'::regclass) IS NOT NULL AS flushed;
SELECT sqlreg.settle();
CREATE TABLE sqlreg.sr_plain (id bigint PRIMARY KEY, val text, n int);
INSERT INTO sqlreg.sr_plain VALUES (1, 'p1', 1), (2, 'p2', 2);

-- Spock's tuple format -> object; primary-key extraction
SELECT koldstore.spock_tuple_object('[{"value": 7, "attname": "id", "atttype": "int8"}, {"value": "x", "attname": "val", "atttype": "text"}]') AS tuple_object;
SELECT koldstore.spock_tuple_object(NULL) IS NULL AS null_tuple;
SELECT koldstore.spock_pk_object('sqlreg.sr1'::regclass, '{"id": 3, "val": "q", "n": 9}') AS pk_object;

-- replaying on a managed table whose rows are cold
SELECT sqlreg.try($$SELECT koldstore.spock_replay_operation('sqlreg.sr1'::regclass, 'UPDATE', '{"id": 2, "val": "replayed", "n": 22}', NULL)$$, true) AS replay_update_cold;
SELECT sqlreg.try($$SELECT koldstore.spock_replay_operation('sqlreg.sr1'::regclass, 'DELETE', NULL, '{"id": 3}')$$, true) AS replay_delete_cold;
SELECT sqlreg.try($$SELECT koldstore.spock_replay_operation('sqlreg.sr1'::regclass, 'INSERT', '{"id": 100, "val": "new", "n": 1}', NULL)$$, true) AS replay_insert_new;
SELECT sqlreg.try($$SELECT koldstore.spock_replay_operation('sqlreg.sr1'::regclass, 'INSERT', '{"id": 4, "val": "dup", "n": 1}', NULL)$$, true) AS replay_insert_existing_cold_key;
SELECT sqlreg.try($$SELECT koldstore.spock_replay_operation('sqlreg.sr1'::regclass, 'UPDATE', '{"id": 999, "val": "ghost"}', NULL)$$, true) AS replay_update_missing_key;
SELECT sqlreg.settle();
SELECT id, val, n FROM sqlreg.sr1 WHERE id IN (2, 3, 4, 100, 999) ORDER BY id;
SELECT count(*) AS total_rows FROM sqlreg.sr1;

-- and on a plain table
SELECT koldstore.spock_replay_operation('sqlreg.sr_plain'::regclass, 'UPDATE', '{"id": 1, "val": "p1-new", "n": 11}', NULL) AS plain_update;
SELECT koldstore.spock_replay_operation('sqlreg.sr_plain'::regclass, 'DELETE', NULL, '{"id": 2}') AS plain_delete;
SELECT koldstore.spock_replay_operation('sqlreg.sr_plain'::regclass, 'INSERT', '{"id": 3, "val": "p3", "n": 3}', NULL) AS plain_insert;
SELECT koldstore.spock_replay_operation('sqlreg.sr_plain'::regclass, 'INSERT', '{"id": 3, "val": "p3", "n": 3}', NULL) AS plain_insert_again;
SELECT id, val, n FROM sqlreg.sr_plain ORDER BY id;

-- guard rails
SELECT sqlreg.try($$SELECT koldstore.reconcile_spock_conflicts()$$, true) AS reconcile_without_spock;
SELECT has_function_privilege('public', 'koldstore.reconcile_spock_conflicts(integer)', 'execute') AS public_can_reconcile;
