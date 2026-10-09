# Backup and Operations

KoldStore has two durability domains: PostgreSQL owns the hot heap and local
catalog state, while cold row images live in filesystem or object-store
artifacts. A backup is sound when **both** are recoverable to the same point.

## Supported recovery: physical backup + WAL archive + retained cold objects

A physical base backup (`pg_basebackup`) plus a WAL archive restores the hot heap, the KoldStore
catalog (`koldstore.*` tables), the async mirror and the replication slot together, as of any
recovery target, exactly like any other PostgreSQL data. The cold tier then needs only one thing:
**every cold object the restored catalog references must still exist, unchanged.** Cold objects are
immutable (no compaction rewrites them), so this holds as long as nothing deleted them. The only
operations that delete referenced cold objects are:

- `DROP TABLE` / `DROP SCHEMA` of a managed table,
- `unmanage_table(..., drop_cold => true)`,
- `recover_segments(...)` on objects the *current* catalog no longer references (orphans relative to
  now, but possibly referenced by an older backup).

Retain the cold object prefix (object-store versioning, a lifecycle rule, or simply not running the
operations above) for as long as any backup taken before them may be restored. Objects flushed after a
backup are harmless to an earlier restore: they are simply unreferenced by the older catalog.

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
writes and flushes, restore to two restore points, then damage and `DROP TABLE` cases) and asserts that
the merged hot+cold table equals what it was at each restore point. It passes against a filesystem
store; object stores take the same code path through the storage client, but the drill does not
exercise one. Use `PG_OPTS` to point it at a staged build.

## Not covered

- A **logical** dump (`pg_dump` of the database) is not a backup of a managed table. For a managed
  table that has cold data, `pg_dump` fails outright: the plain-table `COPY ... TO` it issues is
  refused by the [#126](https://github.com/kalamdb/koldstore/issues/126) guard (verified: `pg_dump:
  error: query failed: ERROR: koldstore: refusing COPY public.t TO ...`). Even where a dump succeeds
  (a schema-only dump, or a table with no cold data yet), the KoldStore catalog tables are not
  registered with `pg_extension_config_dump`, so the dump carries no cold-tier metadata and the
  restored database does not know the table is managed. Use the physical procedure above.
- `koldstore.validate_cold_storage` does not report *unreferenced* objects (use
  `recover_segments(..., dry_run => true)`), and does not protect objects from deletion; retention is
  the operator's responsibility today.
- Packaged export/import (`EXPORT TABLE` / `IMPORT TABLE`) is not shipped.

## `pg_dump` and `COPY`

These commands do not all enter the planner in the same way:

- `pg_dump --data-only -t managed_table` reads the physical heap and therefore
  omits rows that exist only in cold storage.
- `COPY managed_table TO ...` likewise exports the heap relation and can omit
  cold-only rows.
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
files). If that transaction later aborts, the staged deletion is discarded and
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
above. Packaged export/import is planned but not shipped.

`koldstore.recover_segments` is a maintenance surface for orphan/pending
objects; it does not create a coordinated backup or reconstruct arbitrary
missing cold data.

Logical replication captures source-heap changes, not a portable snapshot of
the cold object set. Downstream consumers must not infer that subscribing to
the source publication reproduces a managed table's existing cold history.
