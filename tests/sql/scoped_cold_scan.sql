-- Scoped cold storage, slice 2 (ADR-010): a user-scoped scan reads only its scope's segments (plus the
-- table-wide ones), and judges disjointness over that set alone. The scope comes from a predicate on the
-- scope column (equality, IN, OR) or from the session's koldstore.user_id under row-level security.
-- Pruning must never hide a row: results are compared with an unmanaged control.

\set VERBOSITY terse
\set ON_ERROR_STOP off

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sqlreg_scoped3') THEN
    DROP OWNED BY sqlreg_scoped3;
    DROP ROLE sqlreg_scoped3;
  END IF;
END $$;
CREATE ROLE sqlreg_scoped3 NOINHERIT LOGIN;
GRANT USAGE ON SCHEMA sqlreg TO sqlreg_scoped3;

CREATE FUNCTION sqlreg.explain_counter(query text, label text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
  line text;
  value bigint;
BEGIN
  FOR line IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) ' || query LOOP
    IF line ~ ('^\s*' || label || ': [0-9]+') THEN
      value := substring(line FROM label || ': ([0-9]+)')::bigint;
      EXIT;
    END IF;
  END LOOP;
  RETURN value;
END
$$;

CREATE FUNCTION sqlreg.same_content(managed regclass, control regclass, key text) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE a text; b text;
BEGIN
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', key, managed) INTO a;
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', key, control) INTO b;
  RETURN a = b;
END
$$;

-- 4 scopes x 60 rows, interleaved in load order; segments of 20 rows -> 3 per scope
CREATE TABLE sqlreg.ss (id bigint PRIMARY KEY, user_id text NOT NULL, n int NOT NULL, v text);
INSERT INTO sqlreg.ss
SELECT g, (ARRAY['user-a', 'user-b', 'user-c', 'user-d'])[1 + g % 4], g, 'v' || g FROM generate_series(1, 240) g;
CREATE TABLE sqlreg.ss_ctl AS SELECT * FROM sqlreg.ss;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.ss'::regclass, storage => 'sqlreg_fs', table_type => 'user', scope_column => 'user_id',
  hot_row_limit => 2, min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS ss_managed;
ALTER TABLE sqlreg.ss FORCE ROW LEVEL SECURITY;
GRANT SELECT, INSERT, UPDATE, DELETE ON sqlreg.ss TO sqlreg_scoped3;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ss_caught_up;
SELECT sqlreg.flush_table('sqlreg.ss'::regclass) IS NOT NULL AS ss_flushed;
SELECT count(*) AS ss_segments FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.ss'::regclass::oid AND status = 'active';

-- a predicate on the scope column: only that scope's 3 segments are opened (of 12)
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.ss WHERE user_id = ''user-b''', 'Parquet Segments Opened') AS ss_eq_opened;
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.ss WHERE user_id IN (''user-a'', ''user-d'')', 'Parquet Segments Opened') AS ss_in_opened;
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.ss WHERE user_id = ''user-a'' OR user_id = ''user-c''', 'Parquet Segments Opened') AS ss_or_opened;
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.ss WHERE user_id = ''nobody''', 'Parquet Segments Opened') AS ss_unknown_scope_opened;
-- combined with a key predicate the narrower of the two wins
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.ss WHERE user_id = ''user-b'' AND id = 5', 'Parquet Segments Opened') AS ss_scope_and_key_opened;
-- no scope predicate (superuser, no filter): every segment, as before
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.ss WHERE n < 100000', 'Parquet Segments Opened') AS ss_unscoped_opened;
SELECT count(*) AS ss_b_rows FROM sqlreg.ss WHERE user_id = 'user-b';
SELECT count(*) AS ss_ctl_b_rows FROM sqlreg.ss_ctl WHERE user_id = 'user-b';
SELECT count(*) AS ss_ad_rows FROM sqlreg.ss WHERE user_id IN ('user-a', 'user-d');
SELECT count(*) AS ss_ctl_ad_rows FROM sqlreg.ss_ctl WHERE user_id IN ('user-a', 'user-d');

-- a row-level-security session: the policy's current_setting(...) qual restricts the scan
SET ROLE sqlreg_scoped3;
SET koldstore.user_id = 'user-c';
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.ss', 'Parquet Segments Opened') AS ss_rls_opened;
SELECT count(*) AS ss_rls_rows FROM sqlreg.ss;
SET koldstore.user_id = 'user-d';
SELECT count(*) AS ss_rls_other_rows FROM sqlreg.ss;
-- a prepared statement reused under another user_id must not keep the first user's segments
PREPARE ss_q AS SELECT count(*) FROM sqlreg.ss;
EXECUTE ss_q;
SET koldstore.user_id = 'user-a';
EXECUTE ss_q;
SET koldstore.user_id = 'user-b';
EXECUTE ss_q;
RESET koldstore.user_id;
RESET ROLE;
-- a superuser has RLS bypassed: user_id does not restrict it
SET koldstore.user_id = 'user-a';
SELECT count(*) AS ss_super_rows FROM sqlreg.ss;
RESET koldstore.user_id;

-- only user-a's rows are edited: its segments now overlap, the other scopes' stay disjoint
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.ss SET v = 'edited' WHERE user_id = 'user-a' AND n % 3 = 0;
UPDATE sqlreg.ss_ctl SET v = 'edited' WHERE user_id = 'user-a' AND n % 3 = 0;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ss_fenced_0;
SELECT sqlreg.flush_table('sqlreg.ss'::regclass) IS NOT NULL AS ss_edit_flushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ss_fenced_0b;
SELECT count(DISTINCT scope_id) FILTER (WHERE scope_id IS NOT NULL) AS ss_scopes_with_segments,
       count(*) FILTER (WHERE scope_id IS NULL) AS ss_table_wide_segments
FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.ss'::regclass::oid AND status = 'active';
-- disjointness is judged per scope: a scan of an untouched scope retains no cold keys, the edited one does
SET koldstore.max_merge_seen_keys = 15;
SELECT count(*) AS ss_untouched_scope_under_cap FROM sqlreg.ss WHERE user_id = 'user-d';
SELECT count(*) AS ss_overlapping_scope_under_cap FROM sqlreg.ss WHERE user_id = 'user-a';
SET koldstore.max_merge_seen_keys = 0;
SELECT count(*) AS ss_overlapping_scope_exact FROM sqlreg.ss WHERE user_id = 'user-a';
RESET koldstore.max_merge_seen_keys;
SELECT sqlreg.same_content('sqlreg.ss', 'sqlreg.ss_ctl', 'id') AS ss_content_equal_after_edit;

-- deletes: markers are table-wide, join every scope's candidates and must still mask their scope's rows
SET koldstore.hydrate_on_write = on;
DELETE FROM sqlreg.ss WHERE id BETWEEN 100 AND 111;
DELETE FROM sqlreg.ss_ctl WHERE id BETWEEN 100 AND 111;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ss_fenced;
SELECT sqlreg.flush_table('sqlreg.ss'::regclass) IS NOT NULL AS ss_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS ss_fenced_2;
SELECT sqlreg.same_content('sqlreg.ss', 'sqlreg.ss_ctl', 'id') AS ss_content_equal;
SELECT user_id, count(*) AS rows FROM sqlreg.ss WHERE user_id IN ('user-a', 'user-c') GROUP BY user_id ORDER BY user_id;
SELECT user_id, count(*) AS rows FROM sqlreg.ss_ctl WHERE user_id IN ('user-a', 'user-c') GROUP BY user_id ORDER BY user_id;
SELECT count(*) AS ss_deleted_visible FROM sqlreg.ss WHERE id BETWEEN 100 AND 111;
SELECT count(*) AS ss_b_rows_after FROM sqlreg.ss WHERE user_id = 'user-b';
SELECT count(*) AS ss_ctl_b_rows_after FROM sqlreg.ss_ctl WHERE user_id = 'user-b';
-- an RLS session sees the same through the policy
SET ROLE sqlreg_scoped3;
SET koldstore.user_id = 'user-c';
SELECT count(*) AS ss_rls_c_after FROM sqlreg.ss;
RESET koldstore.user_id;
RESET ROLE;
