-- A row changed since the last flush has its current state in the hot heap (or is deleted); the older copy in a
-- cold segment must never be returned. The native hot child scan carries the query's own filter, so a hot row
-- whose updated value no longer satisfies it is never read -- and the stale cold copy (which matches) used to
-- reappear:  UPDATE t SET tenant = 99 WHERE id = 10;  SELECT ... WHERE tenant = 10  returned id 10.
-- The merge now also masks cold rows by every key the change-log mirror lists as changed since the last flush
-- when the query filters on a non-key column.
--
-- Oracle: an unmanaged copy of the table.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE FUNCTION sqlreg.same_rows(managed regclass, control regclass, key text, preds text[]) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE p text; a text; b text; bad int := 0;
BEGIN
  FOREACH p IN ARRAY preds LOOP
    EXECUTE format('SELECT coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''') FROM %s t WHERE %s', key, managed, p) INTO a;
    EXECUTE format('SELECT coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''') FROM %s t WHERE %s', key, control, p) INTO b;
    IF a IS DISTINCT FROM b THEN
      bad := bad + 1;
      RAISE NOTICE 'mismatch: %', p;
    END IF;
  END LOOP;
  RETURN bad;
END
$$;

CREATE TABLE sqlreg.sv (id bigint PRIMARY KEY, tenant int NOT NULL, label text NOT NULL, ts timestamptz NOT NULL);
INSERT INTO sqlreg.sv SELECT g, g % 13, 'l' || (g % 5), timestamptz '2026-01-01 00:00:00+00' + g * interval '1 hour' FROM generate_series(1, 300) g;
CREATE INDEX sv_ts_idx ON sqlreg.sv (ts);
CREATE TABLE sqlreg.sv_ctl AS SELECT * FROM sqlreg.sv;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.sv'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS sv_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS sv_caught_up;
SELECT sqlreg.flush_table('sqlreg.sv'::regclass) IS NOT NULL AS sv_flushed;

SET koldstore.hydrate_on_write = on;
-- rows 10, 23, 36 had tenant 10; move them to 99. Row 77 changes its label; row 150 changes its timestamp.
UPDATE sqlreg.sv SET tenant = 99 WHERE id IN (10, 23, 36);
UPDATE sqlreg.sv_ctl SET tenant = 99 WHERE id IN (10, 23, 36);
UPDATE sqlreg.sv SET label = 'changed' WHERE id = 77;
UPDATE sqlreg.sv_ctl SET label = 'changed' WHERE id = 77;
UPDATE sqlreg.sv SET ts = timestamptz '2027-01-01 00:00:00+00' WHERE id = 150;
UPDATE sqlreg.sv_ctl SET ts = timestamptz '2027-01-01 00:00:00+00' WHERE id = 150;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS sv_fenced;

-- the old value must not come back, with and without ORDER BY / LIMIT
SELECT id FROM sqlreg.sv WHERE tenant = 10 ORDER BY id LIMIT 4;
SELECT id FROM sqlreg.sv WHERE tenant = 10 AND id BETWEEN 5 AND 40 ORDER BY id;
SELECT count(*) AS tenant_10 FROM sqlreg.sv WHERE tenant = 10;
SELECT count(*) AS tenant_99 FROM sqlreg.sv WHERE tenant = 99;
SELECT id, label FROM sqlreg.sv WHERE id BETWEEN 76 AND 78 AND label <> 'changed' ORDER BY id;
SELECT count(*) AS ts_near_150 FROM sqlreg.sv WHERE ts >= timestamptz '2026-01-07 00:00:00+00' AND ts < timestamptz '2026-01-07 12:00:00+00';
SELECT sqlreg.same_rows('sqlreg.sv', 'sqlreg.sv_ctl', 'id', ARRAY[
  $$tenant = 10$$, $$tenant = 99$$, $$tenant IN (10, 99)$$, $$tenant <> 99 AND id < 40$$, $$label = 'changed'$$,
  $$label <> 'changed' AND id BETWEEN 70 AND 80$$, $$ts >= timestamptz '2026-01-07 00:00:00+00' AND ts < timestamptz '2026-01-07 12:00:00+00'$$,
  $$ts >= timestamptz '2027-01-01 00:00:00+00'$$, $$id IN (10, 23, 36, 77, 150)$$, $$tenant = 10 OR tenant = 99$$,
  $$tenant > 5 AND tenant < 12$$, $$id = 10 AND tenant = 10$$
]) AS sv_mismatches_hot_update;
-- the same through an aggregate and an ordered scan with LIMIT
SELECT count(*) = (SELECT count(*) FROM sqlreg.sv_ctl WHERE tenant = 10) AS agg_matches_control FROM sqlreg.sv WHERE tenant = 10;
SELECT (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.sv WHERE tenant = 10 ORDER BY id DESC LIMIT 5) a)
     = (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.sv_ctl WHERE tenant = 10 ORDER BY id DESC LIMIT 5) b) AS desc_limit_matches_control;

-- after a flush the updated versions are cold and the stale ones are replaced; nothing changes for the reader
SELECT sqlreg.flush_table('sqlreg.sv'::regclass) IS NOT NULL AS sv_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS sv_fenced_2;
SET koldstore.max_merge_seen_keys = 0;
SELECT sqlreg.same_rows('sqlreg.sv', 'sqlreg.sv_ctl', 'id', ARRAY[
  $$tenant = 10$$, $$tenant = 99$$, $$label = 'changed'$$, $$id IN (10, 23, 36, 77, 150)$$,
  $$ts >= timestamptz '2027-01-01 00:00:00+00'$$
]) AS sv_mismatches_after_reflush;
RESET koldstore.max_merge_seen_keys;
