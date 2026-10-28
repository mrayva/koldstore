# koldstore on a physical hot standby (read replicas and failover)

Tested 2026-10-28 on PostgreSQL 18.6 with a primary plus a `pg_basebackup -R` hot standby, both
using one S3 bucket (SeaweedFS) for the cold tier. `scripts/test-standby-failover.sh` reproduces it.
Nothing here is multi-master: one node writes, standbys only read, and a promoted standby takes over.

koldstore's catalog (segments, tombstones, commit sequence) lives in ordinary tables, so physical
replication copies it. The Parquet segments are objects in storage the standby can also reach, so
**the cold tier must be shared object storage (S3), not a local directory.**

## What works

* Reads of managed tables on the standby: full scans, point and range lookups, aggregates and joins,
  hot and cold rows. Writes are refused as read-only, as on any standby.
* Read-only SQL functions on the standby: `table_status`, `async_mirror_status`, `backup_manifest`,
  `validate_cold_storage`, `validate_sql_objects`, `sync_sql_objects(dry_run => true)`,
  `verify_table_integrity`, `changes_since`, `list_jobs`, `preload_status`.
* Primary-only (they write, or apply the mirror, so a standby refuses them): `hydrate_pk`,
  `export_table_metadata`, `wait_for_async_mirror`, flush, compaction, reap, and all manage,
  unmanage and DDL paths.
* Primary DML, flush and compaction replay to the standby and stay consistent.
* Failover: the promoted node has every committed change, resumes the async mirror from the last
  confirmed LSN, and accepts DML, flush and compaction.

## Required configuration

| Where | Setting | Why |
|-------|---------|-----|
| primary and standby | S3 storage (`register_storage(..., 's3', ...)`) and an extension built with the `s3` feature | Both nodes read the same objects |
| standby | `sync_replication_slots = on`, `hot_standby_feedback = on`, a `primary_slot_name`, and `dbname=` in `primary_conninfo` | Copies the mirror slot to the standby so it survives promotion |
| primary | `synchronized_standby_slots = '<standby physical slot>'` | Keeps the logical slot from running ahead of the standby's copy |

The mirror slot `koldstore_async_<dboid>` is created with `failover = true`. A slot created by an older
version is not a failover slot and will not be synchronized (not tested: how to convert an existing mirror).

## Constraints

* **Reap grace must exceed the worst standby replay lag.** A compacted segment is deleted once its
  grace passes, and a standby still replaying an older catalog then fails with `Object at location ...
  not found` until it catches up (reproduced with replay paused and `grace_seconds => 0`). The default
  grace is 600 s and is never below `koldstore.cold_object_retention_seconds`; raise that setting if a
  standby can lag by more.
* **`synchronized_standby_slots` can stall the mirror.** While the named physical slot is missing or its
  standby is down, logical decoding waits, and so do `flush_table` and `wait_for_async_mirror`.
* **Administrative functions need the primary.** koldstore functions that write (such as
  `wait_for_async_mirror`) fail on a standby with `cannot assign TransactionIds during recovery`.
* **Without a synchronized slot a promoted node refuses cold DML.** If the mirror slot did not reach the
  standby, hydrate-on-write and the metadata export fail with `async mirror slot ... is missing` rather
  than silently losing cold DELETEs and UPDATEs, which an earlier version did. Recreating the slot on the
  promoted node would lose changes the old primary had not yet mirrored; fix the standby configuration
  instead.
