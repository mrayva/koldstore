# Extension install and upgrade

KoldStore packages as a normal PostgreSQL extension named `koldstore`.

## Versioning

- Cargo / binary version: `[workspace.package].version` in the repo root
  `Cargo.toml` (also returned by `koldstore.koldstore_version()`).
- Packaged SQL `default_version`: `crates/pg_koldstore/koldstore.control` uses
  `@CARGO_VERSION@`, which `cargo pgrx install` / `package` substitutes from
  Cargo. Fresh installs therefore get `extversion` equal to the Cargo version
  (for example `0.1.8-beta.0`).
- Bootstrap catalog fragment: `crates/pg_koldstore/sql/koldstore--0.1.0.sql` is
  embedded into the generated install script; it is not the versioned install
  file name on disk after packaging.
- **Development, before a version is released:** edit `koldstore--0.1.0.sql`
  directly for catalog DDL; no upgrade edge is needed yet. Local iterative
  installs reinstall / resync extension SQL when the bootstrap fragment
  changes (see e2e cluster harness).
- **Development, after a version has shipped an upgrade edge** (the project's
  first is `koldstore--0.1.11-preview.0--0.1.12-preview.0.sql`): further
  catalog DDL goes into a *new* `koldstore--<from>--<to>.sql` edge alongside
  the full snapshot, not back into the bootstrap file. Keep its hand-written
  `CREATE FUNCTION` statements byte-identical to what pgrx generates for the
  same functions in the newer full snapshot (`scripts/check-upgrade-path.sh`
  diffs an upgraded catalog against a fresh install and will catch drift --
  run it after every change that touches a function's SQL signature; a stale
  edge that dropped a trailing argument shipped undetected for one release
  cycle here because this check was not re-run after the argument was added).

## Install

`koldstore` **must** be in `shared_preload_libraries` before
`CREATE EXTENSION` (and before `manage_table`). Reload is not enough — restart
PostgreSQL after changing the preload list. `session_preload_libraries` is not
sufficient.

```bash
# Example: Ubuntu / Debian
echo "shared_preload_libraries = 'koldstore'" | \
  sudo tee /etc/postgresql/16/main/conf.d/koldstore.conf
sudo systemctl restart postgresql@16-main
```

```sql
CREATE EXTENSION koldstore;
SELECT koldstore.preload_status();  -- loaded_via_shared_preload must be true
```

Requires the shared library and control/SQL files from `cargo pgrx install` or
a release package to be present on the server.

## Upgrade

```sql
ALTER EXTENSION koldstore UPDATE;
SELECT extversion FROM pg_extension WHERE extname = 'koldstore';
```

**Install the new shared library, restart PostgreSQL, then run `ALTER
EXTENSION UPDATE` immediately -- before any other session calls a koldstore
function whose SQL signature changed in the new version** (`manage_table`
gained `allow_fk_hot_only` in 0.1.12, for example). A single `.so` exports one
compiled version of each function; PostgreSQL fills in defaults and calls it
with however many arguments the *currently active* `pg_proc` row declares.
While the catalog is still pinned at the old version but the newer `.so` is
already loaded (the window between restart and `ALTER EXTENSION UPDATE`), a
call to a changed function fails with an internal argument-unboxing error
(confirmed live) rather than running with old semantics -- there is no way
to serve two different argument counts from one loaded library. Treat that
window as a maintenance window with no managed-table DDL traffic, exactly
like restarting for any other reason.

Existing data and catalog state are unaffected by an upgrade whose SQL
changes are additive (new optional trailing arguments, new functions): the
underlying catalog tables and Parquet layout are untouched, only
`pg_proc`/`pg_type` entries change. Verified with real data through
`0.1.11-preview.0 -> 0.1.12-preview.0`
(`scripts/check-upgrade-path-with-data.sh`): a managed table's rows, the
cold-DML write guard, `flush_table()` and `update_row()` all keep working
unchanged after the upgrade, and the new surface it introduces
(`allow_fk_hot_only`, `unmanage_table`'s `drop_cold` actually deleting
storage) works on a table managed after upgrading.

`pg_upgrade` across a PostgreSQL major version is a separate ops runbook
item, not covered by `ALTER EXTENSION UPDATE`.

## Production GUC baseline (async)

Prefer `ALTER DATABASE` / `ALTER SYSTEM` for background-worker GUCs (session
`SET` does not affect the worker):

| GUC | Production baseline | Notes |
|-----|---------------------|--------|
| `shared_preload_libraries` | include `koldstore` (**required**) | Merge-scan hooks + workers; removing preload after manage is unsupported |
| `wal_level` | `logical` | Required for async mirror |
| `koldstore.async_mirror_max_retained_bytes` | `1073741824` (default) | Retained-WAL health threshold; exceeding it alerts but never stops apply. Use PostgreSQL disk/slot safeguards independently; `0` disables this threshold. |
| `koldstore.flush_check_interval_seconds` | `30` (default) or tuned | Built-in auto-flush enqueue cadence |
| `koldstore.flush_execution` | `queue` (default) | Production enqueue-and-return; `inline` is SPI tests only |
| `koldstore.max_parallel_flush_jobs` | `2` (default) or tuned | Concurrent one-shot flush executors per database |
| `koldstore.async_apply_watchdog_interval_ms` | `30000` (default) | Registered; the applier's idle `WaitLatch` timeout is currently hardcoded 30 s, not this GUC |

`koldstore.async_apply_poll_interval_ms` was removed. Managed commits wake the
worker directly. Drop any leftover `async_apply_poll_interval_ms` lines from
`postgresql.conf` / `ALTER DATABASE`. The idle `WaitLatch` timeout is 30 s in
the applier; `koldstore.async_apply_watchdog_interval_ms` is registered at that
default but is not read by the loop.

Also alert on `koldstore.async_mirror_status()` (`healthy`, retained bytes,
`updated_at` age). See [scheduling.md](scheduling.md) and
[architecture/mirror-capture.md](../architecture/mirror-capture.md).
