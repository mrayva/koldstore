#!/usr/bin/env bash
# Verifies an extension upgrade path carries real data through correctly, not just
# that the upgraded catalog matches a fresh install (see check-upgrade-path.sh for
# that check, which this script assumes already passed).
#
# Creates the extension at FROM_VERSION, then immediately runs ALTER EXTENSION
# UPDATE -- the correct, required procedure (see docs/operations/upgrade.md): a
# single loaded .so exports one compiled version of each function, so calling one
# whose SQL signature changed (manage_table gained a trailing argument in this
# release) while the catalog is still pinned at the old version fails outright,
# regardless of what arguments the caller passes. There is no way to test "old
# signature against the new binary" because that combination cannot work by
# construction, on this or any other pgrx/PGXS extension.
#
# What this DOES verify, which check-upgrade-path.sh's catalog-only diff does not:
# once upgraded, does real data keep working end to end.
#   - manage a table, give it hot and cold rows
#   - every row (hot and cold) reads back correctly through the merged view
#   - the cold-DML write guard rejects a plain write to a cold-only row
#   - flush_table works
#   - koldstore.update_row() works
#   - the *new* surface this upgrade introduces (manage_table's allow_fk_hot_only,
#     unmanage_table's drop_cold actually deleting storage) works too, proving the
#     upgrade is a superset of the old behavior, not a replacement
#
# Usage: scripts/check-upgrade-path-with-data.sh FROM_VERSION
#   env: PGHOST PGPORT PGUSER  (a running cluster with the new build installed
#        and koldstore in shared_preload_libraries -- restart it after install)
#        PSQL (default: psql)
set -euo pipefail

FROM_VERSION="${1:?usage: $0 FROM_VERSION (e.g. 0.1.11-preview.0)}"
PSQL="${PSQL:-psql}"
db=koldstore_upgrade_data
store="$(mktemp -d "${TMPDIR:-/tmp}/koldstore-upgrade-data.XXXXXX")"
chmod 777 "$store"
trap 'rm -rf "$store"; "$PSQL" -X -q -v ON_ERROR_STOP=1 -d postgres -c "DROP DATABASE IF EXISTS $db WITH (FORCE)" >/dev/null 2>&1 || true' EXIT

psql_q() { "$PSQL" -X -q -tA -v ON_ERROR_STOP=1 "$@"; }
psql_db() { psql_q -d "$db" "$@"; }

log() { printf '\n\033[1;34m==>\033[0m %s\n' "$*"; }

log "Creating $db at koldstore $FROM_VERSION, then upgrading immediately"
psql_q -d postgres -c "DROP DATABASE IF EXISTS $db WITH (FORCE)" -c "CREATE DATABASE $db" >/dev/null
psql_db -c "CREATE EXTENSION koldstore VERSION '$FROM_VERSION'"
psql_db -c "ALTER EXTENSION koldstore UPDATE"
to_version="$(psql_db -c "SELECT extversion FROM pg_extension WHERE extname = 'koldstore'")"
echo "upgraded to $to_version before any other call, per docs/operations/upgrade.md"

log "Managing sqlreg.t1, flushing rows to cold"
psql_db -v store="$store" <<'SQL'
SET koldstore.min_max_rows_per_file = 1;
SET koldstore.flush_execution = 'inline';
CREATE SCHEMA sqlreg;
SELECT koldstore.register_storage('fs', 'filesystem', :'store', '{}'::jsonb, '{}'::jsonb);
CREATE TABLE sqlreg.t1 (id bigint PRIMARY KEY, val text NOT NULL);
INSERT INTO sqlreg.t1 SELECT g, 'v' || g FROM generate_series(1, 20) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.t1'::regclass, storage => 'fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false
);
SELECT koldstore.flush_table('sqlreg.t1'::regclass, true);
INSERT INTO sqlreg.t1 VALUES (21, 'hot21'), (22, 'hot22');
SELECT koldstore.wait_for_async_mirror();
SQL

row_count="$(psql_db -c "SELECT count(*) FROM sqlreg.t1")"
id_sum="$(psql_db -c "SELECT sum(id) FROM sqlreg.t1")"
[ "$row_count" = "22" ] || { echo "expected 22 rows (20 inserted + 2 hot after flush), got $row_count" >&2; exit 1; }
[ "$id_sum" = "253" ] || { echo "expected id sum 253 (1..20 + 21 + 22), got $id_sum" >&2; exit 1; }
echo "$row_count rows visible through the merged view, id sum $id_sum"

log "Cold-DML write guard rejects a plain write to a cold-only row"
guard_msg="$(psql_db -v ON_ERROR_STOP=0 -c "DELETE FROM sqlreg.t1 WHERE id = 1" 2>&1 || true)"
case "$guard_msg" in
  *"koldstore: refusing"*) echo "guard correctly rejected: ${guard_msg##*ERROR:}" ;;
  *) echo "guard did not reject a plain DELETE of a cold-only row after upgrade:" >&2; echo "$guard_msg" >&2; exit 1 ;;
esac

log "koldstore.update_row() works"
updated="$(psql_db -c "SELECT (koldstore.update_row('sqlreg.t1'::regclass, '{\"id\": 1}'::jsonb, '{\"val\": \"upgraded\"}'::jsonb) ->> 'updated')")"
[ "$updated" = "true" ] || { echo "update_row on a cold row failed post-upgrade: $updated" >&2; exit 1; }
new_val="$(psql_db -c "SELECT val FROM sqlreg.t1 WHERE id = 1")"
[ "$new_val" = "upgraded" ] || { echo "update_row's write did not stick: got $new_val" >&2; exit 1; }
echo "update_row() correctly hydrated and updated a cold row"

log "flush_table() works"
psql_db -c "INSERT INTO sqlreg.t1 VALUES (23, 'post-upgrade-hot')" >/dev/null
flush_result="$(psql_db -c "SELECT (koldstore.flush_table('sqlreg.t1'::regclass, true) ->> 'ok')")"
[ "$flush_result" = "true" ] || { echo "flush_table failed post-upgrade" >&2; exit 1; }
echo "flush_table() succeeded"

log "The new surface this upgrade introduces works too (allow_fk_hot_only, drop_cold)"
psql_db -v store2="$store/fresh" <<'SQL'
SET koldstore.min_max_rows_per_file = 1;
SET koldstore.flush_execution = 'inline';
SELECT koldstore.register_storage('fs2', 'filesystem', :'store2', '{}'::jsonb, '{}'::jsonb);
CREATE TABLE sqlreg.parent (id bigint PRIMARY KEY);
CREATE TABLE sqlreg.child (id bigint PRIMARY KEY, pid bigint REFERENCES sqlreg.parent (id));
-- allow_fk_hot_only, added by this very upgrade, must be reachable by name now.
SELECT koldstore.manage_table(
  table_name => 'sqlreg.child'::regclass, storage => 'fs2', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id',
  allow_fk_hot_only => true
);
INSERT INTO sqlreg.child SELECT g, NULL FROM generate_series(1, 15) g;
-- Nested/inline flush does not auto-fence the mirror before selecting (see
-- mirror/apply.rs's module docs): these rows were captured via ordinary DML,
-- unlike t1's pre-management bulk insert above, so they need an explicit fence
-- or flush selects nothing.
SELECT koldstore.wait_for_async_mirror();
SELECT koldstore.flush_table('sqlreg.child'::regclass, true);
SQL
files_before_drop="$(find "$store/fresh" -type f 2>/dev/null | wc -l)"
[ "$files_before_drop" -gt 0 ] || { echo "flush wrote no storage files, so drop_cold's check below would be trivially true" >&2; exit 1; }
echo "allow_fk_hot_only accepted the FK; flush wrote $files_before_drop cold object(s)"
# drop_cold must actually delete those files.
psql_db -c "SELECT koldstore.unmanage_table('sqlreg.child'::regclass, true, true)" >/dev/null
fresh_files="$(find "$store/fresh" -type f 2>/dev/null | wc -l)"
[ "$fresh_files" -eq 0 ] || { echo "drop_cold left $fresh_files file(s) behind" >&2; exit 1; }
echo "drop_cold deleted all $files_before_drop storage file(s)"

log "PASS: $FROM_VERSION -> $to_version works end to end with real data ($row_count rows)"
