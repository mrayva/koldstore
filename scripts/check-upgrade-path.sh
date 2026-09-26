#!/usr/bin/env bash
# Verifies an extension upgrade path: creates the extension at FROM_VERSION in a
# scratch database, runs ALTER EXTENSION ... UPDATE, and diffs its catalog
# against a database created directly at the current default version.
#
# pgrx only generates full snapshots (koldstore--<version>.sql); each
# koldstore--<from>--<to>.sql lives in crates/pg_koldstore/sql/ and is written by
# hand, so this check is what keeps them honest.
#
# Usage: scripts/check-upgrade-path.sh FROM_VERSION
#   env: PGHOST PGPORT PGUSER  (a running cluster with the new build installed
#        and koldstore in shared_preload_libraries -- restart it after install)
#        PSQL (default: psql)
set -euo pipefail

FROM_VERSION="${1:?usage: $0 FROM_VERSION (e.g. 0.1.11-preview.0)}"
PSQL="${PSQL:-psql}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
DUMP="$HERE/tests/upgrade/catalog_dump.sql"
psql_q() { "$PSQL" -X -q -tA -v ON_ERROR_STOP=1 "$@"; }

up_old=koldstore_upgrade_old
up_new=koldstore_upgrade_new
work="$(mktemp -d)"
trap 'rm -rf "$work"; psql_q -d postgres -c "DROP DATABASE IF EXISTS $up_old" -c "DROP DATABASE IF EXISTS $up_new" >/dev/null 2>&1 || true' EXIT

psql_q -d postgres -c "DROP DATABASE IF EXISTS $up_old" -c "DROP DATABASE IF EXISTS $up_new" \
  -c "CREATE DATABASE $up_old" -c "CREATE DATABASE $up_new" >/dev/null 2>&1
psql_q -d "$up_old" -c "CREATE EXTENSION koldstore VERSION '$FROM_VERSION'"
psql_q -d "$up_new" -c "CREATE EXTENSION koldstore"
psql_q -d "$up_old" -c "ALTER EXTENSION koldstore UPDATE"

to_version="$(psql_q -d "$up_new" -c "SELECT extversion FROM pg_extension WHERE extname = 'koldstore'")"
got_version="$(psql_q -d "$up_old" -c "SELECT extversion FROM pg_extension WHERE extname = 'koldstore'")"
[ "$got_version" = "$to_version" ] || { echo "upgraded to $got_version, expected $to_version" >&2; exit 1; }

psql_q -d "$up_old" -f "$DUMP" > "$work/old.txt"
psql_q -d "$up_new" -f "$DUMP" > "$work/new.txt"
if diff -u "$work/old.txt" "$work/new.txt"; then
  echo "upgrade $FROM_VERSION -> $to_version: catalog identical to a fresh install"
else
  echo "upgrade $FROM_VERSION -> $to_version: catalog DIFFERS from a fresh install (see diff above)" >&2
  exit 1
fi
