#!/usr/bin/env bash
# Stress test for koldstore.hydrate_on_write against a concurrent flush.
#
# Many sessions hydrate-update random cold keys (each success is recorded in a side table
# in the same transaction) while a flusher loops flush_table so hydrated rows are pushed
# back to cold constantly. After the run, with the async mirror settled, it checks:
#   1. for every key, version == number of recorded bumps (no lost or doubled update)
#   2. the merged row count is unchanged and every key appears exactly once
#   3. (delete phase) exactly the keys whose delete was recorded are gone
# Usage: scripts/stress-hydrate-on-write.sh
#   env: PGHOST PGPORT PGUSER PSQL PGBENCH  ROWS(20000) CLIENTS(8) DURATION(30) FLUSHERS(1)
#        KEYS (updaters draw keys from 1..KEYS; small = many collisions, default ROWS)
set -euo pipefail
PSQL="${PSQL:-psql}"; PGBENCH="${PGBENCH:-pgbench}"
ROWS="${ROWS:-20000}"; KEYS="${KEYS:-${ROWS:-20000}}"; KEYS3="${KEYS3:-400}"; CLIENTS="${CLIENTS:-8}"; SECONDS_RUN="${DURATION:-30}"; FLUSHERS="${FLUSHERS:-1}"
DB="${STRESS_DB:-koldstore_stress}"
STORE="$(mktemp -d "${TMPDIR:-/tmp}/koldstore-stress.XXXXXX")"
p() { "$PSQL" -X -q -Atv ON_ERROR_STOP=1 "$@"; }
work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT

p -d postgres -c "DROP DATABASE IF EXISTS $DB WITH (FORCE)" -c "CREATE DATABASE $DB" >/dev/null 2>&1
p -d "$DB" >/dev/null 2>&1 <<EOSQL
CREATE EXTENSION koldstore;
SET koldstore.min_max_rows_per_file = 1;
SELECT koldstore.register_storage('stress_fs','filesystem','$STORE','{}'::jsonb,'{}'::jsonb) IS NOT NULL;
CREATE SCHEMA st;
CREATE TABLE st.t (id bigint PRIMARY KEY, val text NOT NULL, ver int NOT NULL DEFAULT 0);
INSERT INTO st.t SELECT g, 'v'||g, 0 FROM generate_series(1, $ROWS) g;
CREATE TABLE st.applied (id bigint NOT NULL);
CREATE TABLE st.deleted (id bigint NOT NULL);
CREATE TABLE st.errors (op text, msg text);
CREATE TABLE st.oplog (id bigint NOT NULL, op text NOT NULL, xid xid8 NOT NULL DEFAULT pg_current_xact_id());
SELECT koldstore.manage_table(table_name=>'st.t'::regclass, storage=>'stress_fs', hot_row_limit=>10,
  min_flush_rows=>1, max_rows_per_file=>5000, migration_order_by=>'id', auto_flush=>false) IS NOT NULL;
CREATE FUNCTION st.bump(k bigint) RETURNS boolean LANGUAGE plpgsql AS \$\$
DECLARE n bigint;
BEGIN
  BEGIN
    UPDATE st.t SET ver = ver + 1 WHERE id = k;
    GET DIAGNOSTICS n = ROW_COUNT;
    IF n = 1 THEN INSERT INTO st.applied VALUES (k); INSERT INTO st.oplog (id, op) VALUES (k, 'bump'); END IF;
    RETURN n = 1;
  EXCEPTION WHEN OTHERS THEN
    INSERT INTO st.errors VALUES ('bump', left(regexp_replace(SQLERRM, '[0-9]+', 'N', 'g'), 90));
    RETURN false;
  END;
END \$\$;
CREATE FUNCTION st.drop_key(k bigint) RETURNS boolean LANGUAGE plpgsql AS \$\$
DECLARE n bigint;
BEGIN
  BEGIN
    DELETE FROM st.t WHERE id = k;
    GET DIAGNOSTICS n = ROW_COUNT;
    IF n = 1 THEN INSERT INTO st.deleted VALUES (k); INSERT INTO st.oplog (id, op) VALUES (k, 'drop'); END IF;
    RETURN n = 1;
  EXCEPTION WHEN OTHERS THEN
    INSERT INTO st.errors VALUES ('drop', left(regexp_replace(SQLERRM, '[0-9]+', 'N', 'g'), 90));
    RETURN false;
  END;
END \$\$;
CREATE FUNCTION st.flush_once() RETURNS boolean LANGUAGE plpgsql AS \$\$
BEGIN
  BEGIN
    PERFORM koldstore.flush_table('st.t'::regclass, true);
    RETURN true;
  EXCEPTION WHEN OTHERS THEN
    INSERT INTO st.errors VALUES ('flush', left(regexp_replace(SQLERRM, '[0-9]+', 'N', 'g'), 90));
    RETURN false;
  END;
END \$\$;
EOSQL
p -d "$DB" -c "SET koldstore.flush_execution='inline'; SET koldstore.min_max_rows_per_file=1; SELECT koldstore.flush_table('st.t'::regclass, true) IS NOT NULL" >/dev/null 2>&1 || true
# wait until the rows are cold and the mirror has settled
for _ in $(seq 1 60); do
  cold="$(p -d "$DB" -c "SELECT koldstore.table_status('st.t'::regclass)->>'cold_row_count'" 2>/dev/null || echo 0)"
  [ "${cold:-0}" -ge "$ROWS" ] && break
  p -d "$DB" -c "SET koldstore.flush_execution='inline'; SET koldstore.min_max_rows_per_file=1; SELECT koldstore.flush_table('st.t'::regclass, true) IS NOT NULL" >/dev/null 2>&1 || true
  sleep 1
done
p -d "$DB" -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null
echo "fixture: $(p -d "$DB" -c "SELECT count(*) FROM st.t") rows, cold=$(p -d "$DB" -c "SELECT koldstore.table_status('st.t'::regclass)->>'cold_row_count'"), hot=$(p -d "$DB" -c "SELECT koldstore.table_status('st.t'::regclass)->>'hot_rows'")"

cat > "$work/bump.sql" <<EOSQL
\set k random(1, $KEYS)
SELECT st.bump(:k);
EOSQL
cat > "$work/flush.sql" <<'EOSQL'
SELECT st.flush_once();
SELECT pg_sleep(0.05);
EOSQL
export PGOPTIONS="-c koldstore.hydrate_on_write=on -c koldstore.min_max_rows_per_file=1 -c koldstore.flush_execution=inline"
echo "== phase 1: $CLIENTS updaters + $FLUSHERS flusher(s), ${SECONDS_RUN}s"
"$PGBENCH" -n -c "$CLIENTS" -j "$CLIENTS" -T "$SECONDS_RUN" -f "$work/bump.sql" -d "$DB" 2>&1 | grep -E "transactions actually|failed transactions|latency average|tps|aborted" &
PB=$!
if [ "$FLUSHERS" -gt 0 ]; then "$PGBENCH" -n -c "$FLUSHERS" -j 1 -T "$SECONDS_RUN" -f "$work/flush.sql" -d "$DB" 2>&1 | grep -E "transactions actually|failed transactions|aborted" | sed 's/^/flusher: /' & fi
wait
unset PGOPTIONS
p -d "$DB" -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null
sleep 2; p -d "$DB" -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null

echo "== checks after phase 1"
p -d "$DB" <<'EOSQL'
SELECT 'total bumps recorded', count(*) FROM st.applied;
SELECT 'merged rows (expect all rows)', count(*), count(DISTINCT id) FROM st.t;
SELECT 'keys where ver <> recorded bumps', count(*) FROM st.t t
  LEFT JOIN (SELECT id, count(*) c FROM st.applied GROUP BY id) a USING (id) WHERE t.ver <> coalesce(a.c, 0);
SELECT 'sum(ver) vs bumps', sum(ver), (SELECT count(*) FROM st.applied) FROM st.t;
SELECT 'errors: ' || op || ' | ' || msg, count(*) FROM st.errors GROUP BY op, msg ORDER BY 2 DESC;
EOSQL
echo "storage dir: $STORE (remove when done); database: $DB"

echo "== phase 2: $CLIENTS deleters + $FLUSHERS flusher(s), ${SECONDS_RUN}s (each success recorded in st.deleted)"
cat > "$work/drop.sql" <<EOSQL
\set k random(1, $ROWS)
SELECT st.drop_key(:k);
EOSQL
export PGOPTIONS="-c koldstore.hydrate_on_write=on -c koldstore.min_max_rows_per_file=1 -c koldstore.flush_execution=inline ${EXTRA_PGOPTIONS:-}"
"$PGBENCH" -n -c "$CLIENTS" -j "$CLIENTS" -T "$SECONDS_RUN" -f "$work/drop.sql" -d "$DB" 2>&1 | grep -E "transactions actually|tps" &
if [ "$FLUSHERS" -gt 0 ]; then "$PGBENCH" -n -c "$FLUSHERS" -j 1 -T "$SECONDS_RUN" -f "$work/flush.sql" -d "$DB" 2>&1 | grep -E "transactions actually" | sed 's/^/flusher: /' & fi
wait
unset PGOPTIONS
p -d "$DB" -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null; sleep 2
p -d "$DB" -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null
echo "== checks after phase 2"
p -d "$DB" -v rows="$ROWS" <<'EOSQL'
SELECT 'distinct keys deleted', count(DISTINCT id), 'delete records', count(*) FROM st.deleted;
SELECT 'merged rows', count(*), 'expected', :rows - (SELECT count(DISTINCT id) FROM st.deleted) FROM st.t;
SELECT 'deleted keys still visible (expect 0)', count(*) FROM st.t WHERE id IN (SELECT id FROM st.deleted);
SELECT 'live keys wrongly missing (expect 0)', count(*) FROM generate_series(1, :rows) g
  WHERE g NOT IN (SELECT id FROM st.deleted) AND g NOT IN (SELECT id FROM st.t);
SELECT 'duplicate keys (expect 0)', count(*) FROM (SELECT id FROM st.t GROUP BY id HAVING count(*) > 1) d;
SELECT 'errors: ' || op || ' | ' || msg, count(*) FROM st.errors GROUP BY op, msg ORDER BY 2 DESC;
EOSQL

# ---- phase 3: updaters and deleters race on the SAME keys; needs commit timestamps
if [ "$(p -d "$DB" -c "SHOW track_commit_timestamp")" = "on" ]; then
  echo "== phase 3: $CLIENTS mixed clients on $KEYS3 shared keys + $FLUSHERS flusher(s), ${SECONDS_RUN}s"
  p -d "$DB" -c "TRUNCATE st.oplog, st.applied, st.deleted, st.errors" >/dev/null
  p -d "$DB" -c "INSERT INTO st.t SELECT g, 'm'||g, 0 FROM generate_series(200001, 200000 + $KEYS3) g" >/dev/null
  p -d "$DB" -c "SET koldstore.flush_execution='inline'; SET koldstore.min_max_rows_per_file=1; SELECT koldstore.flush_table('st.t'::regclass, true) IS NOT NULL" >/dev/null 2>&1 || true
  sleep 3; p -d "$DB" -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null
  cat > "$work/mixed.sql" <<EOSQL
\set k random(200001, 200000 + $KEYS3)
\set r random(1, 100)
SELECT CASE WHEN :r <= 30 THEN st.drop_key(:k) ELSE st.bump(:k) END;
EOSQL
  export PGOPTIONS="-c koldstore.hydrate_on_write=on -c koldstore.min_max_rows_per_file=1 -c koldstore.flush_execution=inline ${EXTRA_PGOPTIONS:-}"
  "$PGBENCH" -n -c "$CLIENTS" -j "$CLIENTS" -T "$SECONDS_RUN" -f "$work/mixed.sql" -d "$DB" 2>&1 | grep -E "transactions actually|tps" &
  if [ "$FLUSHERS" -gt 0 ]; then "$PGBENCH" -n -c "$FLUSHERS" -j 1 -T "$SECONDS_RUN" -f "$work/flush.sql" -d "$DB" 2>&1 | grep -E "transactions actually" | sed 's/^/flusher: /' & fi
  wait
  unset PGOPTIONS
  p -d "$DB" -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null; sleep 2
  p -d "$DB" -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null
  echo "== checks after phase 3 (native PostgreSQL would give 0 for every violation line)"
  p -d "$DB" <<'EOSQL'
WITH first_drop AS (
  SELECT id, min(pg_xact_commit_timestamp(xid::text::xid)) AS drop_ts FROM st.oplog WHERE op = 'drop' GROUP BY id)
SELECT 'updates that succeeded AFTER the key was already deleted', count(*)
  FROM st.oplog b JOIN first_drop d USING (id)
  WHERE b.op = 'bump' AND pg_xact_commit_timestamp(b.xid::text::xid) > d.drop_ts;
SELECT 'keys deleted successfully more than once', count(*) FROM (SELECT id FROM st.oplog WHERE op = 'drop' GROUP BY id HAVING count(*) > 1) x;
SELECT 'keys visible although a delete succeeded', count(*) FROM st.t WHERE id IN (SELECT id FROM st.oplog WHERE op = 'drop');
SELECT 'delete successes / bump successes', (SELECT count(*) FROM st.oplog WHERE op='drop'), (SELECT count(*) FROM st.oplog WHERE op='bump');
SELECT 'errors: ' || op || ' | ' || msg, count(*) FROM st.errors GROUP BY op, msg ORDER BY 2 DESC;
EOSQL
else
  echo "== phase 3 skipped (needs track_commit_timestamp = on)"
fi
