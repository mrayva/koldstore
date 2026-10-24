# Backup and Operations

KoldStore has two durability domains: PostgreSQL owns the hot heap and local
catalog state, while cold row images live in filesystem or object-store
artifacts. A backup is sound when **both** are recoverable to the same point.

## Supported recovery: physical backup + WAL archive + retained cold objects

A physical base backup (`pg_basebackup`) plus a WAL archive restores the hot heap, the KoldStore
catalog (`koldstore.*` tables), the async mirror and the replication slot together, as of any
recovery target, exactly like any other PostgreSQL data. The cold tier then needs only one thing:
**every cold object the restored catalog references must still exist, unchanged.** Cold objects are
immutable (they are never modified in place), so this holds as long as nothing deleted them. The only
operations that delete referenced cold objects are:

- `DROP TABLE` / `DROP SCHEMA` of a managed table,
- `compact_table` / the maintenance worker, once a replaced segment's grace period has passed (never
  shorter than `koldstore.cold_object_retention_seconds`, and never while a backup taken before the
  compaction is registered with `koldstore.register_backup`; see the SQL API),
- `unmanage_table(..., drop_cold => true)`,
- `recover_segments(...)` on objects the *current* catalog no longer references (orphans relative to
  now, but possibly referenced by an older backup).

Backups taken before such an operation stay restorable only while those objects survive. Two ways to
guarantee that:

- **Retention guard (recommended).** Set `koldstore.cold_object_retention_seconds` to at least your
  backup retention (superuser-only; in `postgresql.conf` so every session inherits it). `DROP TABLE` and
  `unmanage_table(drop_cold)` then record the objects in `koldstore.deferred_cold_deletes`
  (inside the same transaction, so an aborted DROP records nothing) and leave them in place. Schedule
  `koldstore.purge_deferred_cold_objects()` to remove them once the window has passed, or leave
  `koldstore.cold_object_auto_purge` on (the default) and each database's maintenance worker removes them
  automatically, in bounded batches, as their window passes. If the keys cannot be recorded the DDL fails rather than deleting. A table dropped and recreated
  under the same name owns the same prefix again, so the purge never deletes a key under a live table's
  prefix. The default is `0`, the historical delete-at-commit behavior.
- Or retain the object prefix yourself (object-store versioning or a lifecycle rule) and avoid the
  operations above while old backups may be restored.

`recover_segments` copies an orphan to a `.quarantine.` key before removing it, and only objects the
current catalog never referenced become orphans, so it does not threaten a restorable backup.

Objects flushed after a backup are harmless to an earlier restore: they are simply unreferenced by the
older catalog.

### Procedure

1. Record the cold references with the backup:
   `SELECT koldstore.backup_manifest();` (store the JSON next to the base backup; it contains no
   credentials, and lists each segment's key, size and SHA-256).
2. Take the base backup and keep archiving WAL as usual.
3. To restore or recover to a point in time, restore the base backup, configure `restore_command` and
   a recovery target, and start the server.
4. **Before cutover**, run `SELECT koldstore.validate_cold_storage(deep => true);` on the restored
   cluster. `ok = true` means every cold segment the restored catalog references exists with the
   catalogued size and checksum. Any `missing`, `size_mismatch` or `checksum_mismatch` problem names the
   object; restore it from the object store's own backup/versioning before using the table.
5. Allow the async mirror to catch up (`koldstore.wait_for_async_mirror()`) before comparing results;
   the restored slot re-decodes retained WAL exactly as after a crash.

`scripts/backup-restore-drill.sh` runs this end to end on throwaway clusters (base backup, further
writes and flushes, restore to several restore points, then damage, unprotected `DROP TABLE`, retained
`DROP TABLE` and purge cases) and asserts that the merged hot+cold table equals what it was at each
restore point. It passes against a filesystem
store and against an S3-compatible server (`STORAGE=s3`, verified with MinIO: the object operations are
done through a small SigV4 helper, `scripts/lib/s3_tool.py`, so no AWS CLI is needed). S3 needs a build with the
`s3` cargo feature (`--features "pg18 s3"`); a build without it rejects `register_storage(..., 's3', ...)`
with `unsupported storage_type`. Use `PG_OPTS` to point the drill at a staged build.

## Not covered

- A **logical** dump (`pg_dump` of the database) is a *data* export, not a backup of the managed
  setup. The plain-table `COPY ... TO` that `pg_dump` issues is rewritten to
  `COPY (SELECT ... FROM table) TO ...` for a table with cold data, so the dump carries hot **and**
  cold rows (verified end to end: dump, restore into a fresh database, `manage_table` again). The
  KoldStore catalog tables are not registered with `pg_extension_config_dump`, so the dump carries no
  cold-tier metadata: the restored table is an ordinary table with every row in the heap (plus the
  per-table guard triggers and change-log table, which are harmless), and is not managed until you
  register a storage and run `manage_table` on it again (it reuses the leftover change-log table).
  Use the physical procedure above to keep the cold tier and its history.
- `koldstore.validate_cold_storage` does not report *unreferenced* objects (use
  `recover_segments(..., dry_run => true)`). The retention guard defers deletion by a fixed window; it
  does not know which backups still exist, so size the window to your backup retention. It does not
  survive `DROP EXTENSION koldstore`, which drops the queue table, and it is off by default.
- The deferred queue is never purged automatically; schedule `purge_deferred_cold_objects()`.
- Packaged export/import (`EXPORT TABLE` / `IMPORT TABLE`) is not shipped.

## `pg_dump` and `COPY`

These commands do not all enter the planner in the same way:

- `pg_dump` and `COPY managed_table TO ...` (the plain table form) are rewritten to
  `COPY (SELECT ... FROM managed_table) TO ...` when the table has cold data, so they export the merged
  hot+cold rows instead of only the heap (see "Not covered" for what a dump does not carry).
- `COPY (SELECT ... FROM managed_table) TO ...` plans the query and can use
  `KoldMergeScan`, so it includes cold rows for query shapes within the
  supported scan contract.
- `COPY FROM` writes the heap. It does not provide global hot+cold uniqueness or
  conflict checking.

Do not present a plain dump or direct table `COPY` as a logical backup of a
managed relation. A KoldStore-aware backup/export workflow and explicit
failure/diagnostics for unsafe dump paths are tracked separately from the
planned operator APIs in
[#103](https://github.com/kalamdb/koldstore/issues/103).
The end-to-end backup, restore, PITR, and unsafe-dump contract is tracked in
[#126](https://github.com/kalamdb/koldstore/issues/126).

## Object lifecycle

`DROP TABLE`/`DROP SCHEMA` cleanup and `unmanage_table`'s `drop_cold` option
both stage the table's cold objects for deletion, then physically delete them
only after the enclosing PostgreSQL transaction commits (a background xact
callback, matching PostgreSQL's own pending-delete pattern for relation
files). With `koldstore.cold_object_retention_seconds > 0` the objects are not deleted at commit at all;
they are queued in `koldstore.deferred_cold_deletes` and removed later by
`purge_deferred_cold_objects()` (see above). If that transaction later aborts, the staged deletion is discarded and
the objects are left in place, alongside the catalog rows PostgreSQL itself
rolled back -- closing the [#100](https://github.com/kalamdb/koldstore/issues/100)
gap where an aborted DROP could leave catalog state pointing at objects that
were already gone. Object-store deletion is still not itself transactional
(a crash between commit and the post-commit callback running can leave
orphaned objects, and a delete that fails partway through a prefix is logged
and skipped rather than retried), so this closes the silent-data-loss-on-
rollback case, not every durability edge around cold GC.

The `drop_cold` argument to `unmanage_table` deletes the table's cold objects
(staged the same way) after a successful rehydrate; it is refused with
`rehydrate => false`, since that combination would destroy the only copy of
rows never brought back into the heap.

## Available diagnostics

`koldstore.table_status` reports current table, manifest, segment, job, and
async-mirror information. It is operational telemetry, not a backup manifest.

`koldstore.backup_manifest` and `koldstore.validate_cold_storage` are the backup tools described
above; `koldstore.validate_sql_objects` checks the database's SQL objects against the library (below). Packaged export/import is planned but not shipped.

`koldstore.recover_segments` is a maintenance surface for orphan/pending
objects; it does not create a coordinated backup or reconstruct arbitrary
missing cold data.

## Older segments and lookup speed

Segments flushed before primary-key value summaries existed (ADR-009) carry no summary, so a point lookup
opens every one of them whose min/max range covers the key; after hydrate-and-reflush churn that is nearly
all of them (summaries cover single- and multi-column keys of integer, uuid, date and timestamp types). After
upgrading, give them summaries once:
`SELECT koldstore.rebuild_segment_summaries(dry_run => true);` to see how many, then call it without
`dry_run` (repeatedly, until `remaining` is 0). It reads each object once and never changes cold data; see
the [SQL reference](sql-api.md#koldstorerebuild_segment_summaries).

## SQL object drift

The extension version does not change when a build changes a function signature or adds a catalog
column, so `ALTER EXTENSION koldstore UPDATE` never runs and a database created earlier keeps the old SQL
objects while the loaded library expects the new ones. The symptom is a call failing at run time
(`manage_table` with 17 arguments against a library that reads 18: `unboxing allow_fk_hot_only_ argument
failed`) or an error on a catalog column the library expects.

- `koldstore.validate_sql_objects()` compares the database's koldstore functions and catalog columns with
  what the loaded library was built against and returns `{ok, missing[], unexpected[], hint}`. A changed
  function signature shows up once in each list.
- Each database's persistent WAL applier runs the same comparison once when it starts and logs one `WARNING`
  (`this database's SQL objects do not match the loaded library (missing: ...; unexpected: ...)`) if anything
  differs, so drift after installing a new library shows up in the server log without anyone asking. It never
  affects the worker; a database with no managed tables has no worker and so no warning.
- `scripts/check-sql-drift.sh DBNAME` does the same against a freshly created database and works on a
  database too old to have that function (it reports the function itself as missing). Run it after
  installing a new library, for every database that has koldstore.

To fix drift, let koldstore do it: `SELECT koldstore.sync_sql_objects();` shows the repair (a dry run, with every
statement it would run) and `SELECT koldstore.sync_sql_objects(dry_run => false);` applies it. It is superuser-only and
runs in one transaction, so a failure leaves the database as it was, and it re-checks afterwards (`ok_after`). It
replaces a function whose signature changed (dropping the old overload first, so calls are not ambiguous), creates
missing functions, adds missing catalog columns, and recreates a missing catalog table with its constraints and
indexes. It never drops a column or data, and only drops functions that are members of the extension: functions the
library does not know (for example leftovers of a test build) are kept unless you pass `drop_unexpected => true`. A
`NOT NULL` column without a default cannot be added to a table that may have rows, so it is reported in `skipped`.

A database that predates `sync_sql_objects` itself does not have it yet; create it once, then use it:

```sql
CREATE FUNCTION koldstore.sync_sql_objects(dry_run boolean DEFAULT true, drop_unexpected boolean DEFAULT false)
RETURNS jsonb LANGUAGE c AS '$libdir/koldstore', 'sync_sql_objects_pg_wrapper';
ALTER EXTENSION koldstore ADD FUNCTION koldstore.sync_sql_objects(boolean, boolean);
```

The definitions it repairs from are `crates/pg_koldstore/manifest/repair_objects.json`, generated from a freshly
created database together with the expected list (`crates/pg_koldstore/manifest/expected_objects.txt`) by
`scripts/check-sql-drift.sh --update-expected` after changing SQL objects. The `sql_drift` and `sync_sql_objects` SQL
cases and a unit test fail while they are stale.

Logical replication captures source-heap changes, not a portable snapshot of
the cold object set. Downstream consumers must not infer that subscribing to
the source publication reproduces a managed table's existing cold history.
