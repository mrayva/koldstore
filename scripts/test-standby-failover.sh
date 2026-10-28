#!/usr/bin/env bash
# Primary + physical hot standby sharing one S3 bucket: standby cold reads, replicated DML and
# compaction, the reap-vs-replay-lag race, and failover with a synchronized failover slot.
# Exits non-zero on the first mismatch. See docs/standby-and-failover.md.
#
# Needs an S3-compatible store (e.g. `weed server -s3`), a package built with the `s3` feature,
# and free ports. Never touches an existing cluster.
#   KOLDSTORE_PKG        pgrx package root (contains usr/lib/postgresql/18/lib/koldstore.so)
#   S3_ENDPOINT/S3_ACCESS_KEY/S3_SECRET_KEY/KOLDSTORE_S3_BUCKET   (default 127.0.0.1:18333, x/y, spike-bucket)
#   KOLDSTORE_FAILOVER_WORK   scratch directory (default: mktemp)
set -uo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_BIN="${PG_BIN:-/usr/lib/postgresql/18/bin}"
PKG="${KOLDSTORE_PKG:?set KOLDSTORE_PKG to a pgrx package root built with the s3 feature}"
PA="${KOLDSTORE_FAILOVER_PORT_A:-28911}"; PB="${KOLDSTORE_FAILOVER_PORT_B:-28912}"
W="${KOLDSTORE_FAILOVER_WORK:-$(mktemp -d)}"
SOCK="$(mktemp -d /tmp/ks-fo-sock.XXXXXX)"   # unix socket paths must stay under 107 bytes
export S3_ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:18333}" S3_ACCESS_KEY="${S3_ACCESS_KEY:-x}" S3_SECRET_KEY="${S3_SECRET_KEY:-y}"
BUCKET="${KOLDSTORE_S3_BUCKET:-spike-bucket}"; PREFIX="failover-$(date +%s)"
PG_OPTS="-c dynamic_library_path='$PKG/usr/lib/postgresql/18/lib:\$libdir' -c extension_control_path='$PKG/usr/share/postgresql/18:\$system'"
s3() { python3 -I "$ROOT_DIR/scripts/lib/s3_tool.py" "$@"; }
q() { local port=$1 db=$2; shift 2; "$PG_BIN/psql" -X -q -At -h "$SOCK" -p "$port" -d "$db" "$@" 2>&1; }
snap() { q "$1" fo -c "SELECT count(*) || ':' || md5(string_agg(id||'='||v, ',' ORDER BY id)) FROM $2"; }
fail=0
check() { if [[ "$2" == "$3" ]]; then echo "ok: $1"; else echo "FAIL: $1 (got '$2', want '$3')"; fail=1; fi; }
cleanup() { "$PG_BIN/pg_ctl" -D "$W/a" -m immediate stop >/dev/null 2>&1; "$PG_BIN/pg_ctl" -D "$W/b" -m immediate stop >/dev/null 2>&1; rm -rf "$SOCK"; }
trap cleanup EXIT
mkdir -p "$W"; s3 mb "$BUCKET" >/dev/null

"$PG_BIN/initdb" -D "$W/a" -A trust >/dev/null
cat >> "$W/a/postgresql.conf" <<CONF
shared_preload_libraries = 'koldstore'
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
max_worker_processes = 40
hot_standby_feedback = on
CONF
echo "host replication all 127.0.0.1/32 trust" >> "$W/a/pg_hba.conf"
"$PG_BIN/pg_ctl" -D "$W/a" -l "$W/a.log" -w -o "-p $PA -k $SOCK $PG_OPTS" start >/dev/null || { tail "$W/a.log"; exit 1; }
q "$PA" postgres -c "CREATE DATABASE fo" >/dev/null
q "$PA" fo >/dev/null <<SQL
CREATE EXTENSION koldstore;
SELECT koldstore.register_storage('s3s','s3','s3://$BUCKET/$PREFIX/',
  '{"access_key_id":"$S3_ACCESS_KEY","secret_access_key":"$S3_SECRET_KEY"}'::jsonb,
  '{"endpoint":"$S3_ENDPOINT","region":"us-east-1","path_style":true,"allow_http":true}'::jsonb) IS NOT NULL;
CREATE TABLE t (id bigint PRIMARY KEY, v text NOT NULL);
CREATE TABLE c (id bigint PRIMARY KEY, v text NOT NULL);   -- unmanaged oracle
INSERT INTO t SELECT g, 'v' || g FROM generate_series(1, 6000) g;
INSERT INTO c SELECT g, 'v' || g FROM generate_series(1, 6000) g;
SELECT koldstore.manage_table('t'::regclass, storage=>'s3s', hot_row_limit=>10, min_flush_rows=>1,
  max_rows_per_file=>1000, migration_order_by=>'id', auto_flush=>false) IS NOT NULL;
SQL
flush() { q "$1" fo -c "SET koldstore.flush_execution='inline'; SET koldstore.min_max_rows_per_file=1; SELECT koldstore.flush_table('t'::regclass, true)->>'status'" >/dev/null; }
dml() { for tbl in t c; do q "$1" fo >/dev/null <<SQL
SET koldstore.hydrate_on_write = on;
UPDATE $tbl SET v = 'upd' || id WHERE id % 7 = 0;
DELETE FROM $tbl WHERE id % 11 = 0;
INSERT INTO $tbl SELECT g, 'new' || g FROM generate_series(6001, 6500) g;
SQL
done; }
flush "$PA"; q "$PA" fo -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null

# Standby with slot synchronization (failover slot copied via sync_replication_slots).
q "$PA" postgres -c "SELECT pg_create_physical_replication_slot('phys_b')" >/dev/null
"$PG_BIN/pg_basebackup" -h 127.0.0.1 -p "$PA" -D "$W/b" -R -X stream -S phys_b >/dev/null 2>&1 || { echo "basebackup failed"; exit 1; }
cat >> "$W/b/postgresql.auto.conf" <<CONF
primary_conninfo = 'host=127.0.0.1 port=$PA dbname=fo'
sync_replication_slots = on
hot_standby_feedback = on
CONF
"$PG_BIN/pg_ctl" -D "$W/b" -l "$W/b.log" -w -o "-p $PB -k $SOCK $PG_OPTS" start >/dev/null || { tail "$W/b.log"; exit 1; }
q "$PA" postgres -c "ALTER SYSTEM SET synchronized_standby_slots='phys_b'" -c "SELECT pg_reload_conf()" >/dev/null
sleep 6

check "standby cold snapshot equals oracle" "$(snap "$PB" t)" "$(snap "$PA" c)"
check "standby point lookup" "$(q "$PB" fo -c 'SELECT v FROM t WHERE id = 3')" "v3"
check "standby join with oracle" "$(q "$PB" fo -c 'SELECT count(*) FROM t JOIN c USING (id) WHERE t.v = c.v')" "6000"
check "mirror slot synchronized to standby" "$(q "$PB" fo -c "SELECT synced AND failover FROM pg_replication_slots WHERE slot_name LIKE 'koldstore_async_%'")" "t"

dml "$PA"; q "$PA" fo -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null
flush "$PA"; q "$PA" fo -c "SELECT koldstore.compact_table('t'::regclass)" >/dev/null
sleep 2
check "primary equals oracle after DML+flush+compact" "$(snap "$PA" t)" "$(snap "$PA" c)"
check "standby equals primary after replay" "$(snap "$PB" t)" "$(snap "$PA" t)"

# Reaping with grace 0 while the standby lags must break the standby's stale-catalog read.
q "$PB" fo -c "SELECT pg_wal_replay_pause()" >/dev/null
q "$PA" fo -c "SELECT koldstore.reap_compacted_segments('t'::regclass, 0)" >/dev/null
q "$PA" fo >/dev/null <<SQL
SET koldstore.hydrate_on_write = on;
UPDATE t SET v = 'late' || id WHERE id % 13 = 0;
UPDATE c SET v = 'late' || id WHERE id % 13 = 0;
SQL
q "$PA" fo -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null
flush "$PA"; q "$PA" fo -c "SELECT koldstore.compact_table('t'::regclass)" >/dev/null
q "$PA" fo -c "SELECT koldstore.reap_compacted_segments('t'::regclass, 0)" >/dev/null
lagged="$(snap "$PB" t)"
[[ "$lagged" == *NoSuchKey* || "$lagged" == *"not found"* ]] && echo "ok: lagging standby fails when grace < replay lag (expected)" || { echo "FAIL: expected a missing-object error, got '$lagged'"; fail=1; }
q "$PB" fo -c "SELECT pg_wal_replay_resume()" >/dev/null; sleep 3
check "standby recovers after replay resumes" "$(snap "$PB" t)" "$(snap "$PA" c)"

# Failover.
expect="$(snap "$PA" c)"
"$PG_BIN/pg_ctl" -D "$W/a" -m immediate stop >/dev/null
"$PG_BIN/pg_ctl" -D "$W/b" -w promote >/dev/null; sleep 2
check "promoted node has the pre-failover data" "$(snap "$PB" t)" "$expect"
check "promoted node has the mirror slot" "$(q "$PB" fo -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'koldstore_async_%'")" "1"
dml "$PB"; q "$PB" fo -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null
check "post-promotion DML equals oracle" "$(snap "$PB" t)" "$(snap "$PB" c)"
flush "$PB"; q "$PB" fo -c "SELECT koldstore.compact_table('t'::regclass)" >/dev/null
check "post-promotion flush+compact equals oracle" "$(snap "$PB" t)" "$(snap "$PB" c)"
check "validate_cold_storage ok" "$(q "$PB" fo -c "SELECT (koldstore.validate_cold_storage('t'::regclass, true)->>'ok')")" "true"
exit "$fail"
