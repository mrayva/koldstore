#!/usr/bin/env bash
# Compares a database's koldstore SQL objects (functions and catalog columns) with a freshly created
# database built from the installed extension script, and prints what differs.
#
# The extension version stays the same while SQL objects change between builds, so a database created
# earlier keeps the old functions and the new library fails at call time (for example
# `manage_table` with 17 arguments against a library that reads 18). Works on databases too old to have
# koldstore.validate_sql_objects().
#
#   scripts/check-sql-drift.sh DBNAME             # compare DBNAME with a fresh database
#   scripts/check-sql-drift.sh --update-expected  # regenerate crates/pg_koldstore/manifest/expected_objects.txt
#
# Connection: the usual PGHOST/PGPORT/PGUSER environment. A scratch database named
# koldstore_drift_ref_<pid> is created and dropped; it needs the same server and a user allowed to
# CREATE DATABASE and CREATE EXTENSION koldstore.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST_SQL="${ROOT_DIR}/crates/pg_koldstore/manifest/object_manifest.sql"
EXPECTED="${ROOT_DIR}/crates/pg_koldstore/manifest/expected_objects.txt"

usage() { sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; }

target=""
update=0
case "${1:-}" in
  -h|--help|"") usage; [[ -z "${1:-}" ]] && exit 2 || exit 0 ;;
  --update-expected) update=1 ;;
  *) target="$1" ;;
esac

scratch="koldstore_drift_ref_$$"
psql_admin() { psql -X -q -v ON_ERROR_STOP=1 -d postgres "$@"; }
cleanup() { psql_admin -c "DROP DATABASE IF EXISTS ${scratch}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

manifest() { psql -X -q -At -v ON_ERROR_STOP=1 -d "$1" -f "$MANIFEST_SQL"; }

psql_admin -c "CREATE DATABASE ${scratch}"
psql -X -q -v ON_ERROR_STOP=1 -d "$scratch" -c "CREATE EXTENSION koldstore" >/dev/null
fresh="$(mktemp)"
manifest "$scratch" | LC_ALL=C sort >"$fresh"

if [[ "$update" == 1 ]]; then
  cp "$fresh" "$EXPECTED"
  echo "wrote ${EXPECTED} ($(wc -l <"$EXPECTED") objects)"
  rm -f "$fresh"
  exit 0
fi

live="$(mktemp)"
manifest "$target" | LC_ALL=C sort >"$live"
status=0
if diff_out="$(diff <(cat "$fresh") <(cat "$live"))"; then
  echo "ok: ${target} matches a fresh database ($(wc -l <"$live") objects)"
else
  status=1
  echo "DRIFT in ${target}: '<' is expected by the library, '>' is what the database has"
  echo "$diff_out" | grep '^[<>]' || true
  echo
  echo "Fix: recreate each changed function from the extension script (ALTER EXTENSION koldstore DROP FUNCTION ...;"
  echo "DROP FUNCTION ...; CREATE FUNCTION ... AS '\$libdir/koldstore', '<symbol>'; ALTER EXTENSION koldstore ADD FUNCTION ...)"
  echo "and apply listed column changes with ALTER TABLE, then re-run this check."
fi
rm -f "$fresh" "$live"
exit "$status"
