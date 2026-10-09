#!/usr/bin/env bash
# End-to-end backup / PITR drill for a managed (hot+cold) table.
#
# Proves the recovery contract in docs/backup-and-operations.md on throwaway clusters:
#   * a physical base backup + WAL archive restores the hot heap, the koldstore catalog, the async
#     mirror and the replication slot coherently;
#   * restoring to an EARLIER restore point still reads exactly the data that existed then, even
#     though later flushes added cold segments (extra objects are harmless);
#   * koldstore.validate_cold_storage() reports ok after each restore, and DOES report the problem
#     when the retained object prefix is damaged or when a DROP TABLE after the backup deleted the
#     objects (which is why the object prefix must be retained as long as old backups may be restored);
#   * koldstore.cold_object_retention_seconds keeps a dropped table's objects so an older backup stays
#     valid, and only koldstore.purge_deferred_cold_objects() removes them.
#
# Usage: scripts/backup-restore-drill.sh
#   env: STORAGE      fs (default) or s3. s3 needs a build with the `s3` cargo feature and an
#                     S3-compatible server, e.g.
#                       docker run -d --name ks-drill-minio -p 127.0.0.1:19090:9000 \
#                         -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
#                         minio/minio server /data
#        S3_ENDPOINT  (http://127.0.0.1:19090)  S3_BUCKET (koldstore-drill, created if absent)
#        S3_ACCESS_KEY / S3_SECRET_KEY (minioadmin)   -- objects go under a unique per-run prefix
#        PG_BIN       PostgreSQL bin dir               (default /usr/lib/postgresql/18/bin)
#        PG_OPTS      extra server options, e.g. "-c dynamic_library_path='/tmp/kl-stage/lib:\$libdir'
#                     -c extension_control_path='/tmp/kl-stage/share:\$system'" to test a staged build
#        PORT_A PORT_B  ports for the source and restored clusters (28901 / 28902)
#        KEEP=1       keep the work directory
# Uses only its own clusters in a temp directory; touches nothing else.
set -euo pipefail

PG_BIN="${PG_BIN:-/usr/lib/postgresql/18/bin}"
PORT_A="${PORT_A:-28901}"; PORT_B="${PORT_B:-28902}"
PG_OPTS="${PG_OPTS:-}"
W="$(mktemp -d "${TMPDIR:-/tmp}/koldstore-drill.XXXXXX")"
SOCK="$W/sock"; STORE="$W/cold"; ARCH="$W/archive"
mkdir -p "$SOCK" "$STORE" "$ARCH"; chmod 777 "$STORE"
fail=0

STORAGE="${STORAGE:-fs}"
HERE="$(cd "$(dirname "$0")" && pwd)"
S3_ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:19090}"; S3_BUCKET="${S3_BUCKET:-koldstore-drill}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-minioadmin}"; S3_SECRET_KEY="${S3_SECRET_KEY:-minioadmin}"
S3_PREFIX="drill-$(basename "$W")"
export S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY
s3() { python3 -I "$HERE/lib/s3_tool.py" "$@"; }

# Object-store operations on a "locator": a file path (fs) or an object key (s3).
if [ "$STORAGE" = s3 ]; then
  list_segments() { s3 ls "$S3_BUCKET" "$S3_PREFIX/" | cut -f1 | grep 'segment-.*\.parquet$' | sort; }
  obj_get() { s3 get "$S3_BUCKET" "$1" "$2"; }
  obj_put() { s3 put "$S3_BUCKET" "$1" "$2"; }
  obj_rm()  { s3 rm "$S3_BUCKET" "$1"; }
  storage_sql() { echo "SELECT koldstore.register_storage('drill_fs','s3','s3://$S3_BUCKET/$S3_PREFIX/',
    '{\"access_key_id\":\"$S3_ACCESS_KEY\",\"secret_access_key\":\"$S3_SECRET_KEY\"}'::jsonb,
    '{\"endpoint\":\"$S3_ENDPOINT\",\"region\":\"us-east-1\",\"path_style\":true,\"allow_http\":true}'::jsonb) IS NOT NULL;"; }
  s3 mb "$S3_BUCKET"
else
  list_segments() { find "$STORE" -name 'segment-*.parquet' | sort; }
  obj_get() { cp "$1" "$2"; }
  obj_put() { cp "$2" "$1"; }
  obj_rm()  { rm -f -- "${1:?}"; }
  storage_sql() { echo "SELECT koldstore.register_storage('drill_fs','filesystem','$STORE','{}'::jsonb,'{}'::jsonb) IS NOT NULL;"; }
fi

stop_all() {
  "$PG_BIN/pg_ctl" -D "${W:?}/a" -m immediate stop >/dev/null 2>&1 || true
  "$PG_BIN/pg_ctl" -D "${W:?}/b" -m immediate stop >/dev/null 2>&1 || true
}
cleanup() {
  stop_all
  if [ "$STORAGE" = s3 ] && [ "${KEEP:-0}" != 1 ]; then
    s3 ls "$S3_BUCKET" "$S3_PREFIX/" 2>/dev/null | cut -f1 | while read -r key; do s3 rm "$S3_BUCKET" "$key" || true; done
  fi
  [ "${KEEP:-0}" = 1 ] && echo "kept: $W" || rm -rf "${W:?}"
}
trap cleanup EXIT

say() { echo; echo "== $*"; }
check() { # check "description" actual expected
  if [ "$2" = "$3" ]; then echo "  ok   $1"; else echo "  FAIL $1: got [$2] expected [$3]"; fail=1; fi
}
q() { local port="$1" db="$2"; shift 2; "$PG_BIN/psql" -X -q -At -h "$SOCK" -p "$port" -d "$db" -v ON_ERROR_STOP=1 "$@"; }
snapshot() { # snapshot port  -> "count:md5" of the merged (hot+cold) table
  q "$1" drill -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null
  q "$1" drill -c "SELECT count(*) || ':' || md5(string_agg(id || '=' || v, ',' ORDER BY id)) FROM t"
}
wait_ready() { local port="$1"; for _ in $(seq 1 120); do
    "$PG_BIN/pg_isready" -h "$SOCK" -p "$port" >/dev/null 2>&1 && \
      [ "$(q "$port" postgres -c 'SELECT pg_is_in_recovery()' 2>/dev/null || echo t)" = f ] && return 0
    sleep 1; done; echo "cluster on $port did not become ready" >&2; return 1; }
start() { # start dir port [extra -o options]
  local dir="$1" port="$2"; shift 2
  "$PG_BIN/pg_ctl" -D "$dir" -l "$dir.log" -w -o "-p $port -k $SOCK $PG_OPTS $*" start >/dev/null
}

say "source cluster A (wal_level=logical, WAL archiving on)"
"$PG_BIN/initdb" -D "$W/a" -A trust >/dev/null
cat >> "$W/a/postgresql.conf" <<EOF
shared_preload_libraries = 'koldstore'
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
max_worker_processes = 40
archive_mode = on
archive_command = 'cp %p $ARCH/%f'
EOF
start "$W/a" "$PORT_A"
q "$PORT_A" postgres -c "CREATE DATABASE drill"
q "$PORT_A" drill >/dev/null <<EOF
CREATE EXTENSION koldstore;
$(storage_sql)
CREATE TABLE t (id bigint PRIMARY KEY, v text NOT NULL);
INSERT INTO t SELECT g, 'v' || g FROM generate_series(1, 6000) g;
SELECT koldstore.manage_table('t'::regclass, storage=>'drill_fs', hot_row_limit=>10, min_flush_rows=>1,
  max_rows_per_file=>1000, migration_order_by=>'id', auto_flush=>false) IS NOT NULL;
EOF
flush() { q "$PORT_A" drill -c "SET koldstore.flush_execution='inline'; SET koldstore.min_max_rows_per_file=1; SELECT koldstore.flush_table('t'::regclass, true)->>'status'" >/dev/null; }
flush
# Changes after the flush: update + delete cold rows (hydrate-on-write), insert fresh rows.
q "$PORT_A" drill >/dev/null <<EOF
SET koldstore.hydrate_on_write = on;
UPDATE t SET v = 'updated' WHERE id = 17;
DELETE FROM t WHERE id = 42;
EOF
q "$PORT_A" drill -c "INSERT INTO t SELECT g, 'new' || g FROM generate_series(7001, 7100) g"
S1="$(snapshot "$PORT_A")"
M1="$(q "$PORT_A" drill -c "SELECT jsonb_array_length(koldstore.backup_manifest('t'::regclass)->'tables'->0->'segments')")"
echo "  state S1 = $S1 ; manifest lists $M1 segments"
check "backup_manifest reports credentials-free storage" \
  "$(q "$PORT_A" drill -c "SELECT koldstore.backup_manifest()::text LIKE '%credentials%'")" f

say "physical base backup, then restore point s1"
"$PG_BIN/pg_basebackup" -h "$SOCK" -p "$PORT_A" -D "$W/base" -X stream -c fast >/dev/null
q "$PORT_A" drill -c "SELECT pg_create_restore_point('s1')" >/dev/null

say "keep working on A after the backup: more rows, another flush (new cold segments), restore point s2"
q "$PORT_A" drill -c "INSERT INTO t SELECT g, 'later' || g FROM generate_series(8001, 9500) g"
flush
q "$PORT_A" drill >/dev/null <<EOF
SET koldstore.hydrate_on_write = on;
DELETE FROM t WHERE id BETWEEN 100 AND 109;
EOF
S2="$(snapshot "$PORT_A")"
SEG2="$(q "$PORT_A" drill -c "SELECT count(*) FROM koldstore.cold_segments WHERE status='active'")"
echo "  state S2 = $S2 ; $SEG2 active segments now"
check "the $STORAGE object store holds exactly the $SEG2 active segments" "$(list_segments | wc -l)" "$SEG2"
check "S2 differs from S1" "$([ "$S1" != "$S2" ] && echo yes || echo no)" yes
q "$PORT_A" drill -c "SELECT pg_create_restore_point('s2')" >/dev/null
q "$PORT_A" postgres -c "SELECT pg_switch_wal()" >/dev/null
for _ in $(seq 1 60); do [ -n "$(ls "$ARCH" 2>/dev/null)" ] && \
  [ "$(q "$PORT_A" postgres -c "SELECT last_archived_wal >= pg_walfile_name(pg_current_wal_lsn() - 1) FROM pg_stat_archiver" 2>/dev/null || echo f)" != x ] && break; sleep 1; done
sleep 3

restore_to() { # restore_to name target_restore_point
  local name="$1" target="$2"
  rm -rf "${W:?}/$name"; cp -a "$W/base" "$W/$name"; chmod 700 "$W/$name"
  : > "$W/$name/recovery.signal"
  cat >> "$W/$name/postgresql.auto.conf" <<EOF
restore_command = 'cp $ARCH/%f %p'
recovery_target_name = '$target'
recovery_target_action = 'promote'
archive_mode = off
EOF
  start "$W/$name" "$PORT_B"
  wait_ready "$PORT_B"
}

say "restore #1 to restore point s1 (cold storage now holds MORE segments than the backup knew)"
stop_all_b() { "$PG_BIN/pg_ctl" -D "${W:?}/$1" -m fast -w stop >/dev/null 2>&1 || true; }
restore_to b1 s1
check "merged table at s1 equals state S1" "$(snapshot "$PORT_B")" "$S1"
check "validate_cold_storage (deep) ok at s1" "$(q "$PORT_B" drill -c "SELECT koldstore.validate_cold_storage('t'::regclass, true)->>'ok'")" true
check "restored catalog lists the same $M1 segments" \
  "$(q "$PORT_B" drill -c "SELECT jsonb_array_length(koldstore.backup_manifest('t'::regclass)->'tables'->0->'segments')")" "$M1"
stop_all_b b1

say "restore #2 to restore point s2"
restore_to b2 s2
check "merged table at s2 equals state S2" "$(snapshot "$PORT_B")" "$S2"
check "validate_cold_storage (deep) ok at s2" "$(q "$PORT_B" drill -c "SELECT koldstore.validate_cold_storage('t'::regclass, true)->>'ok'")" true
stop_all_b b2

say "damage the retained object prefix ($STORAGE): truncated, deleted and same-size-corrupted objects must be noticed"
mapfile -t SEGS < <(list_segments | head -3)
[ "${#SEGS[@]}" = 3 ] || { echo "expected at least 3 segments, found ${#SEGS[@]}" >&2; exit 1; }
SA="${SEGS[0]}"; SB="${SEGS[1]}"; SC="${SEGS[2]}"
obj_get "$SA" "$W/a.keep"; obj_get "$SB" "$W/b.keep"; obj_get "$SC" "$W/c.keep"
head -c "$(( $(stat -c %s "$W/a.keep") - 10 ))" "$W/a.keep" > "$W/a.bad"
python3 -I - "$W/c.keep" "$W/c.bad" <<'PY'
import sys
b = bytearray(open(sys.argv[1], "rb").read()); b[100] ^= 0xFF; open(sys.argv[2], "wb").write(b)
PY
obj_put "$SA" "$W/a.bad"      # truncated
obj_rm  "$SB"                 # deleted
obj_put "$SC" "$W/c.bad"      # same size, one byte flipped
restore_to b3 s2
problems() { q "$PORT_B" drill -c "SELECT COALESCE(string_agg(p->>'problem', ',' ORDER BY p->>'problem'), 'none')
  FROM jsonb_array_elements(koldstore.validate_cold_storage('t'::regclass, $1)->'problems') p"; }
check "shallow validation reports the deleted and the truncated object" "$(problems false)" "missing,size_mismatch"
check "deep validation also reports the same-size corruption" "$(problems true)" "checksum_mismatch,missing,size_mismatch"
stop_all_b b3
obj_put "$SA" "$W/a.keep"; obj_put "$SB" "$W/b.keep"; obj_put "$SC" "$W/c.keep"
restore_to b3 s2
check "after repairing the objects validation is clean again" \
  "$(q "$PORT_B" drill -c "SELECT koldstore.validate_cold_storage('t'::regclass, true)->>'ok'")" true
stop_all_b b3

say "DROP TABLE with retention off deletes the cold objects: an older restore must NOT validate"
# (a separate table keeps this destructive case away from the retention case below)
q "$PORT_A" drill >/dev/null <<EOF
CREATE TABLE gone (id bigint PRIMARY KEY, v text NOT NULL);
INSERT INTO gone SELECT g, 'g' || g FROM generate_series(1, 3) g;
EOF
q "$PORT_A" drill -c "SELECT koldstore.manage_table('gone'::regclass, storage=>'drill_fs', hot_row_limit=>10, min_flush_rows=>1,
  max_rows_per_file=>1000, migration_order_by=>'id', auto_flush=>false) IS NOT NULL" >/dev/null
q "$PORT_A" drill -c "SET koldstore.flush_execution='inline'; SET koldstore.min_max_rows_per_file=1; SELECT koldstore.flush_table('gone'::regclass, true)->>'status'" >/dev/null
q "$PORT_A" postgres -c "SELECT pg_switch_wal()" >/dev/null
q "$PORT_A" drill -c "SELECT pg_create_restore_point('s3')" >/dev/null
q "$PORT_A" postgres -c "SELECT pg_switch_wal()" >/dev/null
sleep 3
q "$PORT_A" drill -c "DROP TABLE gone" >/dev/null
sleep 2
restore_to b4 s3
check "restore after an unprotected DROP TABLE reports missing objects" \
  "$(q "$PORT_B" drill -c "SELECT (koldstore.validate_cold_storage('gone'::regclass)->>'ok')")" false
stop_all_b b4

say "DROP TABLE with koldstore.cold_object_retention_seconds > 0 keeps the objects: the older restore stays valid"
q "$PORT_A" drill -c "SET koldstore.cold_object_retention_seconds = 3600; DROP TABLE t" >/dev/null
check "dropped table's objects are queued, not deleted" \
  "$(q "$PORT_A" drill -c "SELECT count(*) > 0 FROM koldstore.deferred_cold_deletes")" t
restore_to b5 s1
check "merged table at s1 still equals S1 after the DROP" "$(snapshot "$PORT_B")" "$S1"
check "validate_cold_storage (deep) still ok after the DROP" \
  "$(q "$PORT_B" drill -c "SELECT koldstore.validate_cold_storage('t'::regclass, true)->>'ok'")" true
stop_all_b b5

say "after the window the objects are purged; only then is the older restore no longer valid"
check "purge reports the objects as deleted" \
  "$(q "$PORT_A" drill -c "SELECT (koldstore.purge_deferred_cold_objects(older_than_seconds => 0)->>'deleted')::int > 0")" t
restore_to b6 s1
check "restore after the purge reports missing objects" \
  "$(q "$PORT_B" drill -c "SELECT (koldstore.validate_cold_storage('t'::regclass)->>'ok')")" false
stop_all_b b6

echo
if [ "$fail" = 0 ]; then echo "DRILL PASSED"; else echo "DRILL FAILED"; exit 1; fi
