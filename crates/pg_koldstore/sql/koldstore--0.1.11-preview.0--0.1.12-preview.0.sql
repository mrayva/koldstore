-- koldstore 0.1.11-preview.0 -> 0.1.12-preview.0
--
-- pgrx only generates full install snapshots (koldstore--<version>.sql); this
-- upgrade script is hand-written and installed alongside them by `cargo pgrx
-- install`. It was derived by diffing the pg_catalog contents of a real 0.1.11
-- install against a real 0.1.12 one (functions, tables, columns, constraints,
-- indexes, triggers, ACLs): the catalog tables are unchanged, four functions
-- were added, and manage_table() gained three optional trailing parameters.
--
-- Keep the CREATE FUNCTION statements below identical to what pgrx generates
-- for the same functions in the 0.1.12 snapshot.

-- Cold-row write helpers (upstream issue #122).
CREATE FUNCTION koldstore."hydrate_pk"(
	"table_name" regclass,
	"pk" jsonb
) RETURNS jsonb
STRICT SECURITY DEFINER
LANGUAGE c
AS 'MODULE_PATHNAME', 'hydrate_pk_pg_wrapper';

CREATE FUNCTION koldstore."update_row"(
	"table_name" regclass,
	"pk" jsonb,
	"patch" jsonb,
	"lookup_cold" bool DEFAULT true
) RETURNS jsonb
STRICT SECURITY DEFINER
LANGUAGE c
AS 'MODULE_PATHNAME', 'update_row_pg_wrapper';

CREATE FUNCTION koldstore."delete_row"(
	"table_name" regclass,
	"pk" jsonb,
	"lookup_cold" bool DEFAULT true
) RETURNS jsonb
STRICT SECURITY DEFINER
LANGUAGE c
AS 'MODULE_PATHNAME', 'delete_row_pg_wrapper';

-- Backing function of the per-table BEFORE INSERT guard trigger.
CREATE FUNCTION koldstore."_cold_insert_guard_check"(
	"table_oid" oid,
	"row" jsonb,
	"table_name" TEXT
) RETURNS void
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'cold_insert_guard_check_pg_wrapper';

-- Installs the BEFORE INSERT guard trigger on tables managed under 0.1.11.
CREATE FUNCTION koldstore."internal_attach_insert_guards"() RETURNS bigint
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'internal_attach_insert_guards_wrapper';

-- manage_table() gained the parquet_* tuning parameters, and later (same
-- 0.1.12 cycle) allow_fk_hot_only. The argument list changed, so it cannot be
-- replaced in place.
DROP FUNCTION koldstore."manage_table"(regclass, text, bigint, bigint, bigint, text, text, text, text, bigint, boolean, text, text[], text[]);
CREATE FUNCTION koldstore."manage_table"(
	"table_name" regclass,
	"storage" TEXT,
	"hot_row_limit" bigint,
	"min_flush_rows" bigint DEFAULT 1000,
	"max_rows_per_file" bigint DEFAULT 1000,
	"table_type" TEXT DEFAULT 'shared',
	"scope_column" TEXT DEFAULT NULL,
	"migration_order_by" TEXT DEFAULT NULL,
	"compression" TEXT DEFAULT NULL,
	"target_file_size_mb" bigint DEFAULT NULL,
	"auto_flush" bool DEFAULT true,
	"segment_order_column" TEXT DEFAULT NULL,
	"pruning_columns" TEXT[] DEFAULT NULL,
	"bloom_filter_columns" TEXT[] DEFAULT NULL,
	"parquet_row_group_size" bigint DEFAULT NULL,
	"parquet_data_page_row_count_limit" bigint DEFAULT NULL,
	"parquet_bloom_filter_fpp" double precision DEFAULT NULL,
	"allow_fk_hot_only" bool DEFAULT false
) RETURNS uuid
SECURITY DEFINER
LANGUAGE c
AS 'MODULE_PATHNAME', 'manage_table_pg_wrapper';

SELECT koldstore."internal_attach_insert_guards"();

-- Objects created while an extension script runs become members of the
-- extension, but the per-table guard trigger functions belong to their managed
-- table (manage_table creates them outside any script, so unmanage_table can
-- drop them). Detach the ones just created so they behave the same way.
DO $koldstore_detach$
DECLARE
  guard regprocedure;
BEGIN
  FOR guard IN
    SELECT p.oid::regprocedure
    FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_depend d ON d.classid = 'pg_catalog.pg_proc'::regclass AND d.objid = p.oid
      AND d.deptype = 'e'
    JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid AND e.extname = 'koldstore'
    WHERE p.pronamespace = 'koldstore'::regnamespace
      AND p.proname LIKE '%\_\_cold\_ins\_guard'
  LOOP
    EXECUTE format('ALTER EXTENSION koldstore DROP FUNCTION %s', guard);
  END LOOP;
END
$koldstore_detach$;

-- Releases before this one left a table's guard functions (insert guard and
-- mirror primary-key guard) behind when the table or its schema was dropped
-- (only unmanage_table removed them). Sweep the ones no trigger uses.
DO $koldstore_sweep$
DECLARE
  orphan regprocedure;
BEGIN
  FOR orphan IN
    SELECT p.oid::regprocedure
    FROM pg_catalog.pg_proc p
    WHERE p.pronamespace = 'koldstore'::regnamespace
      AND (p.proname LIKE '%\_\_cold\_ins\_guard' OR p.proname LIKE '%\_\_cl\_pk\_guard')
      AND p.prorettype = 'pg_catalog.trigger'::regtype
      AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgfoid = p.oid)
  LOOP
    EXECUTE format('DROP FUNCTION %s', orphan);
  END LOOP;
END
$koldstore_sweep$;

-- Spock reconciliation (see koldstore-spock-reconcile.sql).
-- Spock reconciliation for tiered (hot/cold) tables (docs/multi-master.md).
--
-- Spock applies a replicated UPDATE/DELETE straight to the heap. When the target row
-- was already flushed to cold storage on this node the heap has no such row, so
--   * DELETE  -> spock.resolutions gets a `delete_missing` row (change skipped);
--   * UPDATE  -> the whole remote transaction is discarded (spock.exception_behaviour =
--     transdiscard) and every operation of it is logged in spock.exception_log.
-- The nodes then disagree. koldstore.reconcile_spock_conflicts() replays those changes
-- through the cold-row-aware update_row()/delete_row(), once, under its own replication
-- origin so the replay is not forwarded back to nodes that already applied the change.
-- Requires koldstore.capture_replicated_changes = on (otherwise the mirror would not see
-- the replay).

CREATE TABLE IF NOT EXISTS koldstore.spock_reconciled (
  item_key text PRIMARY KEY,           -- txn:<origin>:<xid> or res:<node>:<id>
  status text NOT NULL CHECK (status IN ('replayed', 'skipped', 'failed')),
  detail text,
  reconciled_at timestamptz NOT NULL DEFAULT now()
);

-- Keeps only the primary-key columns of a row object.
CREATE OR REPLACE FUNCTION koldstore.spock_pk_object(rel regclass, obj jsonb) RETURNS jsonb
LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
  SELECT coalesce(jsonb_object_agg(a.attname, obj -> a.attname::text), '{}'::jsonb)
  FROM pg_index i
  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
  WHERE i.indrelid = rel AND i.indisprimary
$$;

-- Spock logs tuples as [{"attname": ..., "value": ..., "atttype": ...}, ...].
CREATE OR REPLACE FUNCTION koldstore.spock_tuple_object(tup jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $$
  SELECT CASE WHEN tup IS NULL THEN NULL ELSE
    (SELECT coalesce(jsonb_object_agg(e ->> 'attname', e -> 'value'), '{}'::jsonb)
     FROM jsonb_array_elements(tup) e) END
$$;

-- Applies one replicated operation to `rel`; returns 'applied' or 'skipped'.
CREATE OR REPLACE FUNCTION koldstore.spock_replay_operation(rel regclass, op text, new_obj jsonb, old_obj jsonb)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog, koldstore AS $$
DECLARE
  managed boolean := EXISTS (SELECT 1 FROM koldstore.schemas s WHERE s.table_oid = rel::oid AND s.active);
  key_obj jsonb := koldstore.spock_pk_object(rel, coalesce(old_obj, new_obj));
  patch jsonb;
  n bigint;
  outcome jsonb;
BEGIN
  IF op = 'INSERT' THEN
    EXECUTE format('INSERT INTO %s SELECT * FROM jsonb_populate_record(NULL::%s, $1) ON CONFLICT DO NOTHING', rel, rel)
      USING new_obj;
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN CASE WHEN n > 0 THEN 'applied' ELSE 'skipped' END;
  ELSIF op = 'UPDATE' THEN
    patch := new_obj - ARRAY(SELECT jsonb_object_keys(koldstore.spock_pk_object(rel, new_obj)));
    IF patch = '{}'::jsonb THEN RETURN 'skipped'; END IF;
    IF managed THEN
      outcome := koldstore.update_row(rel, key_obj, patch);
      RETURN CASE WHEN (outcome ->> 'updated')::boolean THEN 'applied' ELSE 'skipped' END;
    END IF;
    EXECUTE format('UPDATE %s t SET %s FROM jsonb_populate_record(NULL::%s, $1) r WHERE %s', rel,
      (SELECT string_agg(format('%I = r.%I', k, k), ', ') FROM jsonb_object_keys(patch) k), rel,
      (SELECT string_agg(format('t.%I = r.%I', k, k), ' AND ') FROM jsonb_object_keys(key_obj) k))
      USING new_obj;
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN CASE WHEN n > 0 THEN 'applied' ELSE 'skipped' END;
  ELSIF op = 'DELETE' THEN
    IF managed THEN
      outcome := koldstore.delete_row(rel, key_obj);
      RETURN CASE WHEN (outcome ->> 'deleted')::boolean THEN 'applied' ELSE 'skipped' END;
    END IF;
    EXECUTE format('DELETE FROM %s t USING jsonb_populate_record(NULL::%s, $1) r WHERE %s', rel, rel,
      (SELECT string_agg(format('t.%I = r.%I', k, k), ' AND ') FROM jsonb_object_keys(key_obj) k))
      USING key_obj;
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN CASE WHEN n > 0 THEN 'applied' ELSE 'skipped' END;
  END IF;
  RETURN 'skipped';
END
$$;

-- Replays Spock changes that could not be applied because the row was cold on this node.
-- Returns {"transactions": n, "deletes": n, "failed": n, ...}. Safe to run repeatedly;
-- every item is handled once (see koldstore.spock_reconciled; delete a row there to retry).
CREATE OR REPLACE FUNCTION koldstore.reconcile_spock_conflicts(max_items integer DEFAULT 200)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, koldstore
  SET session_replication_role = replica AS $$
DECLARE
  t record;
  o record;
  r record;
  rel regclass;
  origin_name constant text := 'koldstore_reconcile';
  txns int := 0; deletes int := 0; failed int := 0; skipped int := 0; applied int := 0;
  outcome text;
BEGIN
  IF to_regclass('spock.exception_log') IS NULL OR to_regclass('spock.resolutions') IS NULL THEN
    RAISE EXCEPTION 'koldstore.reconcile_spock_conflicts: Spock is not installed in this database';
  END IF;
  IF current_setting('koldstore.capture_replicated_changes', true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION 'koldstore.reconcile_spock_conflicts needs koldstore.capture_replicated_changes = on (restart), otherwise the mirror would not see the replayed changes';
  END IF;
  IF NOT pg_try_advisory_lock(hashtext('koldstore.reconcile_spock_conflicts')) THEN
    RETURN jsonb_build_object('busy', true);
  END IF;
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_replication_origin WHERE roname = origin_name) THEN
      PERFORM pg_replication_origin_create(origin_name);
    END IF;
    PERFORM pg_replication_origin_session_setup(origin_name);

    -- 1. discarded transactions: replay every logged operation of each, in order
    FOR t IN
      SELECT e.remote_origin, e.remote_xid, min(e.remote_commit_ts) AS ts
      FROM spock.exception_log e
      WHERE e.error_message LIKE 'logical replication did not find row to be %'
        AND EXISTS (SELECT 1 FROM koldstore.schemas s
                    WHERE s.table_oid = to_regclass(format('%I.%I', e.table_schema, e.table_name))::oid AND s.active)
        AND NOT EXISTS (SELECT 1 FROM koldstore.spock_reconciled x
                        WHERE x.item_key = 'txn:' || e.remote_origin || ':' || e.remote_xid)
      GROUP BY e.remote_origin, e.remote_xid
      ORDER BY min(e.remote_commit_ts)
      LIMIT max_items
    LOOP
      BEGIN
        FOR o IN
          SELECT DISTINCT ON (command_counter) *
          FROM spock.exception_log
          WHERE remote_origin = t.remote_origin AND remote_xid = t.remote_xid
            AND operation IN ('INSERT', 'UPDATE', 'DELETE')
          ORDER BY command_counter, retry_errored_at DESC
        LOOP
          rel := to_regclass(format('%I.%I', o.table_schema, o.table_name));
          IF rel IS NULL THEN
            RAISE EXCEPTION 'table %.% does not exist on this node', o.table_schema, o.table_name;
          END IF;
          outcome := koldstore.spock_replay_operation(rel, o.operation,
            koldstore.spock_tuple_object(o.remote_new_tup), koldstore.spock_tuple_object(o.remote_old_tup));
          IF outcome = 'applied' THEN applied := applied + 1; ELSE skipped := skipped + 1; END IF;
        END LOOP;
        INSERT INTO koldstore.spock_reconciled (item_key, status)
          VALUES ('txn:' || t.remote_origin || ':' || t.remote_xid, 'replayed');
        txns := txns + 1;
      EXCEPTION WHEN OTHERS THEN
        INSERT INTO koldstore.spock_reconciled (item_key, status, detail)
          VALUES ('txn:' || t.remote_origin || ':' || t.remote_xid, 'failed', SQLERRM)
          ON CONFLICT (item_key) DO NOTHING;
        failed := failed + 1;
      END;
    END LOOP;

    -- 2. skipped deletes of rows that are cold here
    FOR r IN
      SELECT c.id, c.node_name, c.relname, c.remote_tuple
      FROM spock.resolutions c
      WHERE c.conflict_type = 'delete_missing'
        AND EXISTS (SELECT 1 FROM koldstore.schemas s WHERE s.table_oid = to_regclass(c.relname)::oid AND s.active)
        AND NOT EXISTS (SELECT 1 FROM koldstore.spock_reconciled x WHERE x.item_key = 'res:' || c.node_name || ':' || c.id)
      ORDER BY c.log_time
      LIMIT max_items
    LOOP
      BEGIN
        outcome := koldstore.spock_replay_operation(to_regclass(r.relname), 'DELETE', NULL, r.remote_tuple::jsonb);
        INSERT INTO koldstore.spock_reconciled (item_key, status)
          VALUES ('res:' || r.node_name || ':' || r.id, CASE WHEN outcome = 'applied' THEN 'replayed' ELSE 'skipped' END);
        deletes := deletes + 1;
      EXCEPTION WHEN OTHERS THEN
        INSERT INTO koldstore.spock_reconciled (item_key, status, detail)
          VALUES ('res:' || r.node_name || ':' || r.id, 'failed', SQLERRM)
          ON CONFLICT (item_key) DO NOTHING;
        failed := failed + 1;
      END;
    END LOOP;

    PERFORM pg_replication_origin_session_reset();
  EXCEPTION WHEN OTHERS THEN
    PERFORM pg_replication_origin_session_reset();
    PERFORM pg_advisory_unlock(hashtext('koldstore.reconcile_spock_conflicts'));
    RAISE;
  END;
  PERFORM pg_advisory_unlock(hashtext('koldstore.reconcile_spock_conflicts'));
  RETURN jsonb_build_object('transactions', txns, 'deletes', deletes, 'operations_applied', applied,
                            'operations_skipped', skipped, 'failed', failed);
END
$$;

REVOKE ALL ON FUNCTION koldstore.reconcile_spock_conflicts(integer) FROM PUBLIC;
