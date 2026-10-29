# SQL API

This page documents the SQL functions and GUCs exposed by the installed extension
today. Signatures below match the generated `koldstore--<version>.sql` output from
pgrx.

## Call style

PostgreSQL accepts both positional and named arguments for the same function.
These docs use **named arguments** (`arg => value`) so call sites stay readable
and resilient to optional trailing parameters. Positional calls remain valid;
they are not a second API.

```sql
-- Preferred (named)
SELECT koldstore.register_storage(
  name         => 'local-dev',
  storage_type => 'filesystem',
  base_path    => '/tmp/koldstore-demo',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb
);

-- Also valid (positional)
SELECT koldstore.register_storage(
  'local-dev',
  'filesystem',
  '/tmp/koldstore-demo',
  '{}'::jsonb,
  '{}'::jsonb
);
```

## Session

```sql
SELECT snowflake_id();
SELECT koldstore_version();
SELECT koldstore_user_id();
SELECT koldstore.preload_status();
```

| Function | Returns | Meaning |
|----------|---------|---------|
| `snowflake_id()` | `bigint` | Monotonic Snowflake-like id |
| `koldstore_version()` | `text` | Extension version |
| `koldstore_user_id()` | `text` | Active `koldstore.user_id` GUC value, or `NULL` when unset |
| `koldstore.preload_status()` | `jsonb` | Whether `shared_preload_libraries` lists `koldstore`, whether this process loaded via shared preload, and `enable_merge_scan` |

`shared_preload_libraries = 'koldstore'` is **mandatory** for correct managed-table
reads. Without it, `_PG_init` / `CREATE EXTENSION` / `LOAD` fail closed, and
`manage_table` errors. `session_preload_libraries` is not sufficient.

## Configuration

Runtime settings use the `koldstore.` GUC prefix:

```sql
SET koldstore.user_id = 'tenant-a';
SET koldstore.cold_reads = 'auto';
SET koldstore.enable_merge_scan = on;
SET koldstore.max_open_parquet_readers = 32;
SET koldstore.log_level = 'info';
SET koldstore.min_max_rows_per_file = 1000;
```

Use PostgreSQL-native persistence for durable configuration, for example
`ALTER SYSTEM SET`, `ALTER DATABASE ... SET`, or `ALTER ROLE ... SET`, followed
by the normal PostgreSQL reload rules for the chosen scope.

### Public GUCs

| GUC | Type | Default | Meaning |
|-----|------|---------|---------|
| `koldstore.user_id` | string | empty | User-set application scope for user-scoped managed tables. Required for scoped reads and writes; not an authentication credential. |
| `koldstore.cold_reads` | string | `auto` | `auto`: cold eligible by catalog/cost; `on`: cold eligible without forcing unnecessary object reads; `off`: hot-only and ERROR when correctness requires cold segments. |
| `koldstore.enable_merge_scan` | bool | `on` | Required for managed-table SELECT. When `off`, `KoldMergeScan` errors at execution instead of allowing an incorrect heap-only read. |
| `koldstore.explain_pipeline` | bool | `off` | When `on`, `EXPLAIN (FORMAT JSON)` includes the nested `KoldStore Pipeline` diagnostic tree. `EXPLAIN … VERBOSE` also enables it for JSON. Default keeps concise Custom Scan properties plus real `Plans` children. |
| `koldstore.allow_same_txn_cold_reads` | bool | `off` | When `off`, a read that must consult cold data of a managed table fails if the current transaction already modified that table (a cold row for a changed key could otherwise come back stale, because the async mirror only sees committed WAL; upstream #121). `on` accepts that risk. |
| `koldstore.guard_scan_writes` | bool | `on` | After an `UPDATE`/`DELETE` on a managed table with cold data whose `WHERE` clause is not an exact primary key, count the cold-only rows the clause matches and reject the statement if there are any (a plain statement cannot reach them; upstream #122). Also rejects a `MERGE` that changes target rows through a multi-row join. Costs a cold read for such statements; `off` accepts silently partial writes. |
| `koldstore.hydrating` | bool | `off` | Set to `on` by KoldStore around the hydration `INSERT` (`hydrate_pk`, `update_row`, `delete_row`, hydrate-on-write), together with `session_replication_role = replica` so ordinary user triggers do not fire for it. A trigger marked `ENABLE ALWAYS` can test `current_setting('koldstore.hydrating', true) = 'on'`. Not meant to be set by applications. |
| `koldstore.capture_replicated_changes` | bool | `off` | Postmaster-level. Mirror changes that carry a replication origin (Spock, subscriptions) and stamp flush prunes with a named origin instead. Required on every node that receives replicated writes (docs/multi-master.md). Restart, and run `wait_for_async_mirror()` first. |
| `koldstore.spock_reconcile_interval_seconds` | int | `0` | Postmaster-level. When positive, background workers run `koldstore.reconcile_spock_conflicts()` on this interval; needs `capture_replicated_changes`. |
| `koldstore.spock_reconcile_databases` | string | empty | Postmaster-level. Comma-separated databases that get a reconciler worker. |
| `koldstore.hydrate_on_write` | bool | `off` | EXPERIMENTAL. Before a single-table `UPDATE`/`DELETE` on a managed table scans, insert the cold-only rows its `WHERE` clause matches into the heap so the native statement can change them (ADR-007). `READ COMMITTED` only; other shapes keep being rejected by the write guards. User `INSERT` triggers fire for the hydrated rows. |
| `koldstore.insert_guard_fence` | bool | `on` | When an `INSERT` finds its key in cold storage, apply committed WAL to the mirror (the hydrate-on-write fence) and look again before refusing, so a key can be re-inserted straight after its cold row was deleted. Skipped when the transaction already wrote the table (its own delete is not committed yet) and when `koldstore.hydrate_fence_mirror` is off. |
| `koldstore.max_hydrate_rows` | int | `10000` | Most cold rows one hydrate-on-write statement may hydrate; above it the statement is rejected with nothing changed. |
| `koldstore.reject_serializable_cold_reads` | bool | `off` | Cold rows take no SSI predicate locks. When `on`, a read that consults cold data under `SERIALIZABLE` isolation fails instead of running with the weaker guarantee (upstream #125). |
| `koldstore.max_open_parquet_readers` | int | `32` | Per-backend open Parquet reader cap for cold scans (fail-fast when exceeded). Clamped to `1..=1024`. |
| `koldstore.max_merge_seen_keys` | int | `1000000` | Per-scan cap on exact PK identities retained by `KoldMergeScan` (fail-closed when exceeded). Protects backends from accidental full-table scans. `0` disables the cap. Clamped to `0..=100000000`. |
| `koldstore.log_level` | string | `info` | Extension log verbosity: `error`, `warn`, `info`, `debug`, or `trace`. |
| `koldstore.min_max_rows_per_file` | int | `1000` | Minimum allowed `max_rows_per_file` for `manage_table` and flush. Lower temporarily for tests, for example `SET koldstore.min_max_rows_per_file = 100`. Clamped to `1..=1000000`. |
| `koldstore.flush_check_interval_seconds` | int | `30` | How often ephemeral maintenance evaluates `auto_flush` tables and enqueues at most one due flush job. The supervisor may then register a one-shot flush executor. Clamped to `1..=86400`. Independent of PostgreSQL autovacuum. Does not by itself fork a flush executor. |
| `koldstore.max_parallel_flush_jobs` | int | `2` | Max concurrent one-shot flush executor workers per database. Clamped to `1..=16`. Cluster cap is 8. Use `1` on small-memory hosts so encode spikes do not overlap. |
| `koldstore.flush_job_max_runtime_seconds` | int | `1800` | Wall-clock budget for one flush job attempt. Checked between passes and between streamed batches within a pass (so one oversized force-flush pass cannot outrun the budget); exceeded attempts fail with an error so a stuck worker cannot run forever. `0` disables. Clamped to `0..=86400`. |
| `koldstore.flush_execution` | string | `queue` | `queue`: `flush_table` enqueues a durable job and returns its UUID; a one-shot executor runs the work. `inline`: enqueue then run in the calling backend (SPI / `#[pg_test]` only). |
| `koldstore.job_retention_days` | int | `30` | Days to retain terminal jobs before purge; `0` disables. Jobs still referenced by pending cold segments are never deleted. |
| `koldstore.cold_object_retention_seconds` | int | `0` | Seconds to keep the cold objects of a dropped table (or `unmanage_table(drop_cold)`) before `purge_deferred_cold_objects()` may delete them; `0` deletes at commit. Superuser-only, so a table owner cannot disable it. See [Backup and Operations](backup-and-operations.md). |
| `koldstore.max_scopes_per_flush` | int | `256` | A user-scoped table flushes one segment group per scope (ADR-010). When one flush pass touches more distinct scopes than this, it is written as ordinary table-wide segments instead (original order, `scope_id` NULL), so a flush across very many scopes does not scatter into very many tiny objects; readers always include table-wide segments. `0` disables the limit. A pass is held in memory to group it (see `koldstore.max_rows_per_flush`). Superuser-only. |
| `koldstore.auto_compaction` | bool | `off` | Lets each database's maintenance worker run bounded `compact_table` passes (up to 200000 rows read per tick across tables) over tables whose cold segments overlap or are small. It skips a table with a flush or compaction running, tables with composite keys or user scopes, and, for tables ordered by a column other than the key, overlap components (rewriting those cannot make segments key-disjoint, so they would be redone every tick). Replaced objects are reaped by the same worker after `max(600 s, koldstore.cold_object_retention_seconds)`. Enable it only once backups tolerate that (see `koldstore.compact_table`). Superuser-only. |
| `koldstore.dump_cold_as_metadata` | bool | `off` | `on` makes `COPY table TO` / `pg_dump` export only the heap rows of managed tables (the cold rows travel as metadata via `export_table_metadata`). `off` exports merged hot+cold rows. |
| `koldstore.cold_object_auto_purge` | bool | `on` | While `koldstore.cold_object_retention_seconds > 0`, each database's maintenance worker deletes retained cold objects whose window has passed (bounded batches, same live-prefix protection as `purge_deferred_cold_objects()`), so `koldstore.deferred_cold_deletes` does not grow without limit. `off` leaves purging to your own calls. No effect while retention is `0`. Superuser-only. |
| `koldstore.async_apply_watchdog_interval_ms` | int | `30000` | Idle wake interval for the persistent WAL applier (clamped `1000..=300000`). Managed commits `SetLatch` it immediately regardless; this interval only matters while it is otherwise idle. Two roles: a missed-wake safety net, and bounding how far this database's slot can fall behind *unrelated* WAL other databases on the same PostgreSQL instance write while idle (every idle wake also runs a normal, cheap-when-caught-up drain pass). `SET`/`ALTER SYSTEM` + reload; the worker picks up changes on SIGHUP. |
| `koldstore.hydrate_slot_lock_poll_ms` | int | `0` | Hydrate-on-write only. `0` (default): queue on the async-mirror slot lock, so the deadlock detector resolves lock cycles, but many concurrent hydrating transactions can starve the WAL applier and flush. A positive value polls instead (about 1.5-3x the throughput at 8 clients), but fails the statement with a retryable `serialization_failure` if still busy at the deadline. |
| `koldstore.async_apply_max_rows_per_tick` | int | `0` | Max source row changes per apply tick (`0` = unlimited / drain available WAL). Cap this on small machines (for example `8192`) via `ALTER DATABASE` so background workers see it. |
| `koldstore.async_apply_max_ms_per_tick` | int | `0` | Max wall-clock ms per apply tick (`0` = unlimited). When exhausted, commit `applied_lsn` and continue next wake. Cap alongside row budget on small hosts. |
| `koldstore.flush_prelock_max_passes` | int | `3` | Max phase-5.5 pre-lock async apply passes during flush before failing closed. |
| `koldstore.flush_prelock_max_ms` | int | `5000` | Combined wall-clock budget (ms) for flush phase-5.5 pre-lock catch-up. |
| `koldstore.async_mirror_max_retained_bytes` | int | `1073741824` (1 GiB) | Health threshold for slot-retained WAL bytes. Exceeding it marks `async_mirror_status().retention.ok` false but never blocks the applier from draining WAL. `0` disables this health threshold. Configure PostgreSQL retention/disk safeguards independently. |

### Internal GUCs

These are reserved for extension maintenance paths. Application roles cannot set
them.

| GUC | Type | Default | Meaning |
|-----|------|---------|---------|
| `koldstore.internal_system_write` | bool | `off` | Allows internal KoldStore system writes. |
| `koldstore.internal_flush_cleanup` | bool | `off` | Allows pruning flushed hot and mirror rows during flush cleanup. |

## Exposed functions

Every SQL-callable function the extension installs today:

| Function | Returns | Value |
|----------|---------|-------|
| `snowflake_id()` | `bigint` | Generated Snowflake-like id |
| `koldstore_version()` | `text` | Extension version string |
| `koldstore_user_id()` | `text` | Active `koldstore.user_id`, or `NULL` |
| `koldstore.register_storage(...)` | `uuid` | Storage backend id (`koldstore.storage.id`) |
| `koldstore.alter_storage_credentials(...)` | `void` | No value |
| `koldstore.alter_storage_location(...)` | `uuid` | Storage backend id |
| `koldstore.manage_table(...)` | `uuid` | Migration job id (`koldstore.jobs.id`); for a partitioned table, the partition template id |
| `koldstore.set_partition_auto_manage(...)` | `boolean` | `true` when the table has a partition template |
| `koldstore.set_table_auto_flush(...)` | `boolean` | `true` when an active managed table was updated |
| `koldstore.unmanage_table(...)` | `bigint` | Count of deactivated `koldstore.schemas` rows |
| `koldstore.rebuild_table(table, ddl)` | `uuid` | Job id of the re-management. Rehydrates the table, runs `ddl` as the caller, and manages it again with its stored options, in one transaction; use it for primary-key and narrowing type changes that `ALTER TABLE` refuses on tables with cold data |
| `koldstore.wait_for_async_mirror()` | `bigint` | Async source row changes applied by this fence |
| `koldstore.async_mirror_status()` | `jsonb` | DB-scoped slot lag, WAL watermarks, apply rates, health |
| `koldstore.disable_async_mirror()` | `boolean` | Whether async publication or slot infrastructure was removed |
| `koldstore.enqueue_flush_job(...)` | `uuid` | Flush job id, or `NULL` when nothing is due |
| `koldstore.flush_table(...)` | `jsonb` | Flush result object (`job_id`, `status`, `error`, …); see below |
| `koldstore.table_status(...)` | `jsonb` | Operator view: table hot/mirror/cold/jobs, effective `settings` (flush policy, `auto_flush`, compression, target file size, Parquet layout) + `async_mirror` |
| `koldstore.metrics()` | `text` | Prometheus text exposition: per-table hot / mirror / cold rows, cold segments, pending and error jobs, last flush time, plus async-mirror health, lag and retained WAL. Counters come from the manifest (no heap scan). Lists only tables the session user can `SELECT` |
| `koldstore.recover_segments(...)` | `bigint` | Number of orphan recovery actions planned |
| `koldstore.backup_manifest(...)` | `jsonb` | Catalog's cold-tier references at backup time (no credentials) |
| `koldstore.export_table_metadata(table)` | `jsonb` | One table's cold-tier catalog (schemas, manifest, segments, indexes) for a logical dump; no OIDs or credentials |
| `koldstore.import_table_metadata(table, metadata)` | `bigint` | Manages a restored, unmanaged table and adopts exported segments; returns active segments adopted |
| `koldstore.validate_cold_storage(...)` | `jsonb` | Catalog vs object store: `{ok, problems[]}` |
| `koldstore.purge_deferred_cold_objects(...)` | `jsonb` | Deletes retained cold objects whose window has passed |
| `koldstore.compact_table(...)` | `jsonb` | Rewrites overlapping / small cold segments; report of what changed |
| `koldstore.reap_compacted_segments(...)` | `bigint` | Deletes replaced segments' objects past their grace period |
| `koldstore.register_backup(...)` / `koldstore.unregister_backup(...)` | `timestamptz` / `boolean` | Registers a backup whose cold objects must be kept (see below) |

## Storage and Migration

### `koldstore.register_storage`

Two overloads are available. Both accept named arguments.

```sql
-- Default path templates: {namespace}/{tableName}/ and
-- {namespace}/{tableName}/{scopeId}/
SELECT koldstore.register_storage(
  name         => 'local-dev',
  storage_type => 'filesystem',
  base_path    => '/tmp/koldstore-demo',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb
);

-- Custom path templates
SELECT koldstore.register_storage(
  name               => 'local-dev',
  storage_type       => 'filesystem',
  base_path          => '/tmp/koldstore-demo',
  credentials        => '{}'::jsonb,
  config             => '{}'::jsonb,
  regular_path_tmpl  => '{namespace}/{tableName}/',
  scoped_path_tmpl   => '{namespace}/{tableName}/{scopeId}/'
);
```

`storage_type` must be one of `filesystem`, `s3`, `gcs`, or `azure` (cloud
kinds require the matching extension cargo feature: `s3`, `gcs`, `azure`, or
`cloud`).

Optional `check` (default `true`): opens the configured backend (filesystem,
S3, GCS, or Azure) and performs a put/delete probe object
(`.koldstore-write-probe`) so registration fails fast on bad credentials,
unreachable endpoints, or unwritable paths. Filesystem backends also create
`base_path` when needed and **require the directory to be empty** so existing
files are not mixed with cold objects. Pass `check => false` to skip both the
emptiness requirement and the writability probe (for example when you
intentionally reuse a non-empty directory, or credentials/mounts will exist
later).

```sql
SELECT koldstore.register_storage(
  name         => 'local-dev',
  storage_type => 'filesystem',
  base_path    => '/koldstore-data/cold/',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb,
  check        => false   -- skip emptiness + writability probe
);
```

**Returns:** `uuid` — the storage backend id (`koldstore.storage.id`). Fails with
`storage \`<name>\` already exists` when the name is taken; use
`alter_storage_credentials` / `alter_storage_location` to change an existing
backend.

### `koldstore.alter_storage_credentials`

```sql
SELECT koldstore.alter_storage_credentials(
  name        => 'local-dev',
  credentials => '{"access_key_id":"...","secret_access_key":"..."}'::jsonb
);
```

Rotates credentials without rewriting existing cold object paths.

**Returns:** `void` — no value. Errors if the storage name does not exist.

### `koldstore.alter_storage_location`

```sql
SELECT koldstore.alter_storage_location(
  name      => 'local-dev',
  base_path => '/var/lib/koldstore',
  config    => '{}'::jsonb
);
```

Updates storage location/configuration without direct catalog DML. Optional
`check` (default `true`) probes the new location the same way as
`register_storage` (filesystem roots must be empty; put/delete probe for all
backends). Pass `check => false` to skip.

**Returns:** `uuid` — the storage backend id. Errors if the storage name does
not exist.

### Default table management with `ALTER TABLE`

```sql
ALTER TABLE messages SET (
  koldstore_enabled = true,
  koldstore_storage = 'cold_s3',
  koldstore_move_after = '90 days',
  koldstore_min_flush_rows = 1000,
  koldstore_max_rows_per_file = 10000,
  koldstore_max_rows_per_flush = 10000
);
```

Use `koldstore_hot_row_limit` instead of `koldstore_move_after` to retain a
fixed hot-row count. The selectors are mutually exclusive and replace one
another atomically; omitted batching settings are preserved. Native PostgreSQL
interval input such as `'90 days'`, `'3 months'`, and `'P90D'` is accepted.
Age is measured from the latest captured mirror mutation encoded in `seq`.

`koldstore_move_when` is reserved and fails closed. Use
`koldstore.unmanage_table(...)` rather than `koldstore_enabled = false`.
Storage cannot be changed after management.

### Advanced and compatibility management: `koldstore.manage_table`

```sql
SELECT koldstore.manage_table(
  table_name        => 'chat.messages',
  storage           => 's3_archive',
  hot_row_limit     => 10000,
  min_flush_rows    => 1000,
  max_rows_per_file => 1000,
  target_file_size_mb => 256,
  migration_order_by  => 'created_at',
  parquet_row_group_size => 256,
  parquet_data_page_row_count_limit => 64,
  parquet_bloom_filter_fpp => 0.01
);
```

Registers a heap table for KoldStore management with structured flush settings.
`hot_row_limit` is required in the call (pass `NULL` for hot-only tables).
`table_type` defaults to `shared`; optional `scope_column`,
`migration_order_by`, `compression`, and `target_file_size_mb` arguments are
also available.

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `table_name` | required | Table to manage (`regclass`) |
| `storage` | required | Registered storage backend name |
| `hot_row_limit` | required (`NULL` allowed) | Maximum mirror rows to keep hot; `NULL` for hot-only tables |
| `min_flush_rows` | `1000` | Minimum excess rows required before a flush moves data cold |
| `max_rows_per_file` | `1000` | Maximum rows written into one Parquet segment per flush batch (minimum `1000` unless lowered via `koldstore.min_max_rows_per_file`). Dominates flush peak RSS — keep small on low-RAM hosts; see [Memory and small machines](performance.md#memory-and-small-machines) |
| `table_type` | `'shared'` | `shared` or `user` |
| `scope_column` | `NULL` | Required when `table_type => 'user'` |
| `migration_order_by` | `NULL` | Optional oldest-to-newest column used for populated-table migration |
| `compression` | `NULL` | Optional Parquet compression name |
| `target_file_size_mb` | `NULL` | Optional target Parquet segment size in MiB; stored for future size-aware flushing |
| `parquet_row_group_size` | `NULL` | Rows per Parquet row group on future flushes; omitted keeps the writer default |
| `parquet_data_page_row_count_limit` | `NULL` | Rows per Parquet data page on future flushes; smaller values enable finer page-index pruning |
| `parquet_bloom_filter_fpp` | `NULL` | Bloom filter false-positive probability for future flushes; must be strictly between `0` and `1` |
| `auto_flush` | `true` | When `true`, ephemeral maintenance may enqueue flush jobs for this table; set `false` to reserve flushes for cron / manual `flush_table` |

Existing tables can change the same settings for future segments with
`ALTER TABLE ... SET (koldstore_parquet_row_group_size = 256,
koldstore_parquet_data_page_row_count_limit = 64,
koldstore_parquet_bloom_filter_fpp = 0.01)`. `koldstore_compression = 'snappy'|'zstd'|'uncompressed'`
and `koldstore_target_file_size_mb = 128` work the same way. Existing segments are unchanged.

**Returns:** `uuid` — the migration job id written to `koldstore.jobs` (empty
tables get a completed migrate job; populated tables run mirror initialization
inline and return that job id).

Flush selection keeps the newest rows hot by mirror `seq` and always flushes the
oldest eligible excess first. Example with `hot_row_limit = 10000` and
`min_flush_rows = 1000`:

**Constraint note:** when `hot_row_limit` is set, `manage_table` rejects tables
with non-primary-key `UNIQUE` constraints or foreign keys. Koldstore enforces
those constraints on hot rows only after management; cold Parquet is not checked
on normal DML. See [Limitations](limitations.md#unique-and-foreign-key-constraints).

| Mirror rows | Flush result |
|-------------|--------------|
| 10,505 | No flush (`505` excess is below `min_flush_rows`) |
| 11,000 | Flush `1,000` rows into `1` file (`max_rows_per_file = 1000`) |
| 11,250 | Flush `1,000` rows, keep `10,250` hot |
| 11,500 | Flush `1,500` rows into `2` files |

#### Mirror capture

`manage_table` always configures committed-WAL capture. It requires
`wal_level=logical`; KoldStore manages the empty `koldstore_async_mirror`
publication and one deterministic logical slot per database. Applications must
tolerate the normal short lag or call `koldstore.wait_for_async_mirror()` at a
required consistency boundary. `flush_table` invokes the same catch-up path
automatically.

Authoritative mirror `seq` values are allocated only by the serialized WAL
applier and are the exclusive `changes_since` cursor (`seq > last_seq`). See
[Mirror capture](architecture/mirror-capture.md).

### `koldstore.set_table_auto_flush`

```sql
SELECT koldstore.set_table_auto_flush(
  table_name => 'chat.messages',
  enabled    => false
);
```

Updates `koldstore.schemas.options.auto_flush` for an active managed table.
Manual `flush_table` / `enqueue_flush_job` ignore this flag. See
[Scheduling](operations/scheduling.md).

**Returns:** `boolean` — `true` when an active managed row was updated.

### Partitioned tables

`manage_table`, `unmanage_table`, `flush_table`, `enqueue_flush_job`, `set_table_auto_flush` and
`table_status` accept a partitioned table. `manage_table` manages every leaf partition with the given
options and remembers them (`koldstore.partition_templates`), so a partition created or attached later
is managed the same way in the same transaction; the other functions fan out to the managed leaves.
`table_status` returns `partitioned_parent`, `partitions_total`, `partitions_managed`,
`auto_manage_new_partitions`, the `template`, summed `hot_rows` / `mirror_rows` / `cold_row_count` /
`cold_segment_count` / `pending_jobs` / size fields, and a `partitions` array with each partition's own
status. `enqueue_flush_job(parent)` enqueues every leaf and returns the first job id.

```sql
SELECT koldstore.set_partition_auto_manage('events', false);  -- stop managing new partitions
SELECT koldstore.set_partition_auto_manage('events', true);   -- resume
```

See [limitations](limitations.md) and [ADR-008](decisions/008-partitioned-inherited-table-support.md).

### `koldstore.unmanage_table`

```sql
SELECT koldstore.unmanage_table(
  table_name => 'chat.messages'
);
```

Disables management after rehydration or archive-detach mode. Optional
`rehydrate` and `drop_cold` arguments default to `NULL` and can be passed by
name when needed:

```sql
SELECT koldstore.unmanage_table(
  table_name => 'chat.messages',
  rehydrate  => true,
  drop_cold  => false
);
```

`rehydrate` controls whether cold rows are restored before detaching.
`drop_cold` deletes the table's cold objects from storage after a successful
rehydrate (refused with `rehydrate => false`: that combination would destroy
the only copy of rows never brought back into the heap). Like `DROP TABLE`'s
own cold-object deletion (see the `DROP TABLE` section below), the objects are
staged now and only physically deleted once the surrounding PostgreSQL
transaction commits, so a rollback of this statement leaves the objects in
place alongside the catalog rows PostgreSQL itself restores
([#100](https://github.com/kalamdb/koldstore/issues/100)). A single object's
delete failure is logged and skipped rather than retried, so a stubborn
object can leave a partial prefix behind; re-running
`unmanage_table(..., drop_cold => true)` on an already-unmanaged table is not
supported (there is no active managed table left to unmanage), so a
leftover object from a failed delete needs the storage backend's own
tooling to remove.

**Returns:** `bigint` — number of `koldstore.schemas` rows deactivated for the
table (normally `1` when the table was actively managed, `0` if none were
active).

## Async Mirror Operations

These functions operate on database-scoped infrastructure used by managed
tables (WAL-only committed-WAL mirror capture).

### `koldstore.wait_for_async_mirror`

```sql
SELECT koldstore.wait_for_async_mirror();
```

Applies committed source changes available at the fence boundary and returns
when the mirror has reached that boundary. The fence LSN is captured at call
time (and forced durable), so concurrent writers after that point do not extend
the wait. It cannot decode the caller's uncommitted changes, and a fixed
`REPEATABLE READ` or `SERIALIZABLE` snapshot acquired before the call cannot see
state applied afterward. Invoke the fence before acquiring the snapshot that
must include the committed boundary. This is an **optional committed-visibility
API** for reads/benchmarks —
`flush_table` and auto-flush do **not** call it (they enqueue/spawn and return).
The background worker normally keeps the mirror caught up without an explicit call.

**Returns:** `bigint` — the number of source row-change messages applied by
this invocation. A return value of `0` can mean the worker had already caught
up; it does not mean async capture is disabled.

### `koldstore.async_mirror_status`

```sql
SELECT koldstore.async_mirror_status();
```

Database-scoped async mirror health (no managed table required). Prefer
`table_status(table)` when you already have a table — it embeds the same object
under `async_mirror`.

Includes:

| Field | Meaning |
|-------|---------|
| `slot_name` | Deterministic logical-slot name for this database |
| `wal.current_lsn` | Current WAL tip (`pg_current_wal_lsn`) — latest commit frontier |
| `wal.applied_lsn` | Durable mirror apply watermark (`async_mirror_state`) — latest read/apply |
| `wal.confirmed_flush_lsn` | Slot ack / retention frontier |
| `wal.lag_bytes` | Bytes between current WAL and confirmed flush |
| `slot` / `state` / `apply` / `retention` / `healthy` | Slot presence, durable state, process-local rates, retention threshold |

### `koldstore.disable_async_mirror`

```sql
SELECT koldstore.disable_async_mirror();
```

Drops the current database's async logical slot and publication and clears its
apply checkpoint. It refuses cleanup while any actively managed table uses
async capture. Unmanage those tables first. Calling it repeatedly is safe; a
later async `manage_table` recreates compatible infrastructure automatically.

**Returns:** `boolean` — `true` when a slot or publication existed and was
removed, otherwise `false`.

## Flush and Cold Data

### `koldstore.enqueue_flush_job`

```sql
SELECT koldstore.enqueue_flush_job(
  table_name => 'chat.messages'
);
```

Inserts a pending flush job when none is already active for the table, or
returns the existing active job UUID. Returns `NULL` when no flush work is due
(same eligibility rules as `flush_table`). Does **not** spawn flush executors —
use `flush_table` when you want work to start. Flush jobs are table-wide;
user-scope partitioning uses the managed table's `scope_column` and session
`koldstore.user_id`, not an enqueue argument.

**Returns:** `uuid` — the flush job id (`koldstore.jobs.id`), or `NULL` when
nothing is due.

### `koldstore.flush_table`

```sql
SELECT koldstore.flush_table(
  table_name => 'chat.messages'
);
-- → jsonb, for example:
-- {"ok": true, "job_id": "...", "status": "queued", "execution": "queue", ...}
-- {"ok": true, "job_id": null, "status": "not_due", "reason": "..."}
-- {"ok": false, "job_id": "...", "status": "error", "error": "Permission denied ..."}
```

Ensures a durable flush job exists and starts queue execution (production
default `koldstore.flush_execution = queue`):

1. Inserts a pending job or returns the existing active job UUID inside JSON
2. Spawns a one-shot flush executor when capacity allows
3. Returns immediately with `status = queued` — progress is visible in
   `koldstore.jobs` / `list_jobs` / `table_status`

`status = not_due` (with `job_id` null) when policy selection is empty —
including when mirror excess is positive but below `max_rows_per_file` — so
undersized Parquet segments are never queued.

Queue-mode storage failures happen in the executor after return; they set
`koldstore.jobs.error_trace` and emit a PostgreSQL **WARNING**
(`koldstore flush: FAILED ...`) so `docker logs` shows them. Inline mode
includes `error` / `rows_flushed` in the same JSON response.

With `flush_execution = inline` (SPI tests only), the calling backend also runs
the flush before returning (`status = completed` or `error`).

If another backend already holds this table's flush lock while no active job
row is visible yet, the call fails with an ERROR such as
`flush already in progress`. When an active job already exists,
`status = already_running` and `job_id` is that job.

**Returns:** `jsonb` — see fields above.

### `koldstore.list_jobs`

```sql
SELECT koldstore.list_jobs();
SELECT koldstore.list_jobs(
  statuses  => '["running","pending"]'::jsonb,
  job_types => '["flush"]'::jsonb,
  table_name => 'chat.messages'::regclass
);
```

Returns a JSON array of job objects (status, phase, progress fields, payload).
Filters are optional. Progress updates from an in-progress flush are visible to
other sessions when that flush statement ends (mid-statement live commits are
not used).

### `koldstore.cancel_job` / `koldstore.cancel_table_jobs`

```sql
SELECT koldstore.cancel_job(job_id => '…'::uuid);
SELECT koldstore.cancel_table_jobs(table_name => 'chat.messages'::regclass);
```

Cooperative cancel:

- **pending** jobs are marked `cancelled` immediately when unlocked
- **running** jobs are signalled via `koldstore.table_cancel_requests` (avoids
  blocking on the flush statement's jobs-row lock) and stop at the next wave
  boundary (before activate when possible)

If cancel is observed after cold publish already committed, the job finishes as
`completed` with `payload.cancel_requested_after_publish = true` (data is not
unpublished).

`DROP TABLE` on a managed relation cancels active jobs, waits for any in-flight
flush/migrate advisory lock to release, deactivates catalog metadata, stages
cold objects under the table prefix for deletion, drops the change-log mirror,
and records a completed `drop_table_cleanup` job before PostgreSQL removes the
heap.

Staged cold objects are only physically deleted after the surrounding
PostgreSQL DDL transaction commits; an aborted `DROP TABLE` discards the staged
deletion instead, so the objects stay in place alongside the catalog rows
PostgreSQL restores ([#100](https://github.com/kalamdb/koldstore/issues/100)).
`unmanage_table`'s `drop_cold` option (above) has the same property. Object-
store deletion itself is still not transactional with PostgreSQL's commit --
a crash between commit and the post-commit deletion running can leave objects
orphaned -- so this closes the rollback case, not every cold-GC durability
edge.

### `koldstore.table_status`

```sql
SELECT jsonb_pretty(koldstore.table_status(
  table_name => 'chat.messages'
));
```

Preferred single operator API. Returns table hot / mirror / cold / jobs fields
**plus** `async_mirror` (same object as `async_mirror_status()`), so one call
shows table state and WAL tip vs applied LSN / slot health.

`async_mirror` is database-scoped (one slot per DB), not per-table — it is
embedded here for convenience when you already know the table.

**Returns:** `jsonb` — managed-table storage, mirror, cold-segment, size,
manifest, recent jobs, and `async_mirror`. Counters are table-wide across
scopes. Errors if the table is not actively managed.

Sample result after a small flush:

```json
{
  "jobs": [
    {
      "id": "e30eb374-a9db-4ff1-97d3-72f8511dfc60",
      "phase": "finished",
      "status": "completed",
      "job_type": "flush",
      "updated_at": "2026-07-07T16:56:10.123456+03:00",
      "duration_ms": 842,
      "rows_flushed": 12,
      "checkpoint_seq": 332882280212668416,
      "rows_processed": 12,
    },
    {
      "id": "2c2bcf44-d6ea-4b3e-b62c-cfaf18ad5225",
      "phase": "finished",
      "status": "completed",
      "job_type": "migrate_backfill",
      "updated_at": "2026-07-07T16:56:09.987654+03:00",
      "duration_ms": 1205,
      "rows_flushed": 0,
      "checkpoint_seq": 0,
      "rows_processed": 1012,
    }
  ],
  "hot_rows": 1000,
  "mirror_rows": 1000,
  "cold_row_count": 12,
  "cold_segment_count": 1,
  "heap_size_bytes": 442368,
  "table_size_bytes": 606208,
  "index_size_bytes": 16384,
  "manifest_state": "in_sync",
  "manifest_max_seq": 332882280212668416,
  "pending_jobs": 0,
  "storage_binding": "4a3b2ab3-5ea8-4761-9e37-1a2f98b128e4",
  "last_error": null
}
```

Top-level fields:

| Field | Type | Meaning |
| ----- | ---- | ------- |
| `hot_rows` | `bigint` | Rows still present in the PostgreSQL heap |
| `mirror_rows` | `bigint` | Primary keys tracked in the `__cl` mirror |
| `cold_row_count` | `bigint` | Rows already copied to active cold segments |
| `cold_segment_count` | `bigint` | Active Parquet segment count |
| `heap_size_bytes` | `bigint` | `pg_relation_size(table)` — main heap fork only |
| `table_size_bytes` | `bigint` | `pg_table_size(table)` — heap + TOAST + FSM/VM, **excluding indexes** |
| `index_size_bytes` | `bigint` | `pg_indexes_size(table)` — all indexes on the table |
| `manifest_state` | `text` | Catalog/manifest sync state; `in_sync` means they agree |
| `manifest_max_seq` | `bigint` | Highest mirror `seq` represented in cold data |
| `pending_jobs` | `bigint` | Jobs for this table with status `pending` or `running` |
| `jobs` | `jsonb` | Up to 20 recent jobs, newest first (see below) |
| `storage_binding` | `text` | Bound storage backend id as text |
| `last_error` | `text` | Last manifest or storage error, or `null` |
| `async_mirror` | `jsonb` | Same payload as `async_mirror_status()` |

Each element of `jobs`:

| Field | Type | Meaning |
| ----- | ---- | ------- |
| `id` | `text` | Job uuid |
| `job_type` | `text` | e.g. `flush`, `migrate_backfill` |
| `status` | `text` | Job status (`pending`, `running`, `completed`, …) |
| `phase` | `text` | Current or final phase |
| `rows_processed` | `bigint` | Rows processed by the job |
| `rows_flushed` | `bigint` | Rows written cold by the job |
| `checkpoint_seq` | `bigint` | Mirror `seq` checkpoint |
| `checkpoint_seq` | `bigint` | Seq checkpoint |
| `duration_ms` | `bigint` | Wall time from job start (flush run start when available) to last update; live for in-progress jobs |
| `updated_at` | `timestamptz` | Last job update time |

Size notes:

- `heap_size_bytes` + `index_size_bytes` is **not** the same as
  `pg_total_relation_size(table)` (that also includes TOAST).
- `table_size_bytes` excludes indexes; use
  `table_size_bytes + index_size_bytes` for a closer total, or call
  `pg_total_relation_size` directly when you need PostgreSQL’s total.
- Percent-saved figures require a caller-held baseline; `table_status` does
  not store pre-flush sizes.

For job-level progress, inspect `koldstore.jobs`:

```sql
SELECT
  job_type,
  status,
  phase,
  rows_processed,
  rows_flushed,
  (payload->>'duration_ms')::bigint AS duration_ms,
  error_trace
FROM koldstore.jobs
WHERE table_oid = 'chat.messages'::regclass
ORDER BY created_at DESC
LIMIT 5;
```

### `koldstore.recover_segments`

```sql
SELECT koldstore.recover_segments(
  table_name => 'chat.messages'
);

SELECT koldstore.recover_segments(
  table_name => 'chat.messages',
  dry_run    => true
);
```

Discovers orphan cold objects under the table prefix that are not referenced by
the current object-store manifest **or** by active `koldstore.cold_segments`
rows, plans recovery actions for them, and applies the plan unless
`dry_run => true`. `dry_run` defaults to `false`. Catalog-referenced segments
are preserved so crash-before-manifest-publish recovery does not delete Parquet
that merge scan still needs.

**Returns:** `bigint` — number of recovery actions planned (orphan objects
found). With `dry_run => true`, the count is still returned and no objects are
changed.

### `koldstore.backup_manifest`

```sql
SELECT koldstore.backup_manifest();                       -- every managed table (superuser)
SELECT koldstore.backup_manifest('chat.messages');        -- one table (owner or superuser)
```

Records what the catalog references in cold storage so a backup can be paired with the object
prefix and checked after a restore. **Returns:** `jsonb` with `format`, `cluster` (database, system
identifier, WAL position, server version, timestamp), `async_mirror` (the same health object as
`async_mirror_status`) and `tables[]`: per table the storage type, base path and prefix, per-scope
manifest generation / etag / row counts, the number of `pending` segments, and every `active`
segment with its object `key`, `byte_size`, SHA-256 `checksum` and sequence range. Storage
credentials are never included. Take it together with the physical base backup; see
[Backup and Operations](backup-and-operations.md).

### `koldstore.purge_deferred_cold_objects`

```sql
SELECT koldstore.purge_deferred_cold_objects();                          -- objects past the window
SELECT koldstore.purge_deferred_cold_objects(older_than_seconds => 0, dry_run => true);
```

With `koldstore.cold_object_retention_seconds > 0`, `DROP TABLE` and `unmanage_table(..., drop_cold =>
true)` leave the table's cold objects in place and record their keys in
`koldstore.deferred_cold_deletes` inside the same transaction (a rolled-back DROP records nothing).
This deletes the ones staged at least `older_than_seconds` ago (default: the setting), in batches of
`batch_limit` (default 1000), and returns `{dry_run, window_seconds, considered, deleted,
skipped_live_prefix, failed, errors[], remaining}`. A key under a prefix a currently managed table uses
(a table dropped and recreated under the same name) is removed from the queue without deleting the
object. Safe to repeat; run it periodically, for example from pg_cron. Superuser only.

### `koldstore.compact_table`

```sql
SELECT koldstore.compact_table('app.events');                                   -- up to 500000 rows per call
SELECT koldstore.compact_table('app.events', max_rows => 2000000, grace_seconds => 3600);
SELECT koldstore.reap_compacted_segments('app.events', 3600);
```

Flushes only append. An update or re-insert of an already flushed key leaves its older version in an older
segment, a delete leaves a marker, and many small flushes leave many small objects. `compact_table` rewrites
the affected segments:

- segments whose primary-key ranges overlap (they may hold several versions of a key) are merged: the newest
  version of every key wins (keeping its original `seq`), delete markers are dropped, rows are sorted like a
  flush sorts them and split at `max_rows_per_file`;
- runs of adjacent, already disjoint segments that are at most half full are packed into fuller ones;
- when the table has a single-column primary key and is ordered by it (or has no order column) the rewritten
  segments are key-disjoint, which lets scans skip retaining cold keys (`koldstore.max_merge_seen_keys` no longer limits a
  full scan). A composite key, or a table ordered by another column, is deduplicated and packed (rows sorted by the
  order column, then the key columns, like a flush), but the rewritten segments' key boxes may still touch, so a
  unit is only rewritten when it removes rows or merges segments, and automatic compaction leaves such tables'
  overlap units to explicit calls.

Each unit is published atomically: one catalog statement activates the new segments and retires the old ones
under the manifest-generation CAS (the same one flush uses), so a reader sees either layout, never a mix.
`max_rows` bounds the rows one call reads (and so its memory); a unit that does not fit is left for the next
call. An overlapping unit larger than `max_rows` alone is streamed when the rewrite is key-ordered (a
single-column key ordered by itself, or a composite key with exact tuple bounds): its lead-key range is cut
into slices, each read (one segment at a time), resolved and written on its own, so at most about `max_rows` rows
are in memory whatever the unit's size, the output stays key-disjoint, and all slices are published in one
activation. It reads the whole unit, so it runs alone as the first unit of an explicit call (run the call again
for the rest) and never in automatic compaction. A unit that cannot be streamed (ordered by another column, a lead key
without a Sort Key encoding, keys too skewed to split into slices that fit `max_rows`) is reported in
`units_over_max_rows` (or fails with a message to raise `max_rows`). The call holds the table's
flush lock, so it fails fast when a flush is running and flushes skip the table meanwhile. It returns a report
(`compacted`, `units`, `segments_replaced`, `segments_written`, `rows_read`, `rows_written`,
`versions_and_markers_dropped`, `bytes_replaced`, `bytes_written`, `reaped_segments`, ...).

Compaction collapses a key's cold history, so by default `koldstore.changes_since` refuses a cursor older than
what was dropped ("predates compaction"). Set `koldstore.cdc_retention_seconds` (superuser, default `0`) to keep
that history for a window instead: compaction then writes the superseded versions and delete markers newer than
the window into separate `history` segments, which queries never read and `changes_since` merges into its feed, so
a cursor within the window keeps working across compaction. History segments are purged (and
`history_floor_seq` raised first) by the next `compact_table` / `reap_compacted_segments` call or the maintenance
worker once their newest row is older than the window; setting the window back to `0` purges all of it. History
segments are not part of backups, and user-scoped tables are not compacted.

Replaced segments are marked `compacted` and keep their objects for `grace_seconds` (default 600) so queries
planned on the previous layout finish; the next `compact_table` call, `koldstore.reap_compacted_segments`, or
the maintenance worker deletes them afterwards. The grace is never shorter than
`koldstore.cold_object_retention_seconds`, and objects are kept while a backup taken before the compaction is
registered with `koldstore.register_backup`. A backup that is neither covered by the window nor registered can
no longer restore the cold tier of a compacted table once the grace has passed.

With `koldstore.auto_compaction = on` the maintenance worker runs the same pass on its own (bounded per tick, off by default). Not handled yet: user-scoped tables (refused with an error). Compaction collapses
the cold history of a key to its newest version, so the overwritten versions (and dropped delete markers) can no longer be replayed. The highest `seq` it dropped is recorded per table (`koldstore.manifest.history_floor_seq`), and `changes_since` with `0 < since_seq < history_floor_seq` fails with an error instead of silently skipping them; resync with `since_seq => 0` and resume from the last returned `seq`. Cursors at or past the floor, and `since_seq => 0`, are unaffected.
Segments without primary-key bounds (written by a very old release) make the call decline until
`koldstore.rebuild_segment_summaries` has run, since delete markers can only be dropped when every segment's
key range is known. Owner or superuser.

### `koldstore.register_backup` / `koldstore.unregister_backup`

```sql
SELECT koldstore.register_backup('nightly');                         -- taken_at defaults to now()
SELECT koldstore.register_backup('nightly', '2026-10-20 02:00+00');  -- or the time the base backup started
SELECT koldstore.unregister_backup('nightly');                       -- once it can no longer be restored
```

A physical backup restores the catalog, which references cold objects. Anything that deletes an object the
backup references makes it unrestorable, so the two operations that delete objects of live data honor a
registry (`koldstore.backups`, superuser-only):

- a segment retired by `compact_table` / automatic compaction keeps its object while any registered backup
  has `taken_at` earlier than the retirement (reported as `held_by_registered_backups`);
- while any backup is registered, `DROP TABLE` and `unmanage_table(drop_cold => true)` queue the table's
  objects (`koldstore.deferred_cold_deletes`) instead of deleting them at commit, and
  `purge_deferred_cold_objects` (also run by the maintenance worker) skips objects queued after the oldest
  registered backup.

`unregister_backup` also purges the dropped-table objects the backup was holding (the retention window, if one is set, still applies). Register at the moment the base backup *starts* (the `now()` default is right for that) and unregister when the
backup expires. A forgotten registration keeps objects forever (growing storage), by design; list them with
`SELECT * FROM koldstore.backups`. Registration is a promise from the backup tooling to KoldStore: it does not
take or verify a backup. Superuser only.

### `koldstore.validate_cold_storage`

```sql
SELECT koldstore.validate_cold_storage();                              -- all tables, sizes only
SELECT koldstore.validate_cold_storage('chat.messages', deep => true); -- one table, also checksums
```

Checks every active cold segment in the catalog against the object store. **Returns:** `jsonb`
`{ok, deep, tables_checked, segments_checked, problems[]}`; each problem names the table, segment,
object key and a `problem` of `missing`, `size_mismatch`, `checksum_mismatch` (only with
`deep => true`, which downloads and hashes every object) or `storage_error`. `ok = true` means every
cold reference the catalog holds is intact. Run it after restoring a backup or recovering to a point
in time, before cutover. It does not report unreferenced objects; use
`recover_segments(..., dry_run => true)` for those.

### `koldstore.validate_sql_objects`

```sql
SELECT koldstore.validate_sql_objects();
```

Compares this database's koldstore functions and catalog columns with the ones the loaded library was
built against. **Returns:** `jsonb` `{ok, library_version, extension_version, missing[], unexpected[], hint}`.
`missing` is what the library expects but the database lacks; `unexpected` is what the database has that
the library does not know (a changed function signature appears once in each). `ok = true` means no
difference and matching versions. Read-only. See "SQL object drift" in `backup-and-operations.md`.

### `koldstore.rebuild_segment_summaries`

```sql
SELECT koldstore.rebuild_segment_summaries(dry_run => true);                    -- count only
SELECT koldstore.rebuild_segment_summaries('chat.messages', max_segments => 50); -- one table, bounded
```

Gives segments written before primary-key value summaries existed (ADR-009) the summary flush would have
written, so a point lookup (`WHERE id = $1`) stops opening every such segment whose min/max range covers
the key. **Returns:** `jsonb` `{dry_run, max_segments_per_table, rebuilt, tables[]}`; each table reports
`rebuilt`, `remaining` (segments still without a summary), `too_large_for_a_summary` and `failed[]`. Each
segment's object is read once and verified against the catalog (checksum and row count) before its summary
is stored; one that does not verify is reported and left alone. Cold data is never modified. Only tables
whose key columns are all `smallint`/`integer`/`bigint`/`uuid`/`date`/`timestamp`/`timestamptz` (one column or
several) are eligible (others are reported as ineligible). Call it repeatedly until `remaining` is 0 (segments over 32,768 rows are too large to carry a
summary and stay unsummarized). Owner or superuser. Sessions that already planned a lookup keep using the
unpruned candidate list until the next manifest change, which is correct, only slower.

### `koldstore.sync_sql_objects`

```sql
SELECT koldstore.sync_sql_objects();                                -- the repair plan, nothing changed
SELECT koldstore.sync_sql_objects(dry_run => false);                -- apply it
SELECT koldstore.sync_sql_objects(false, drop_unexpected => true);  -- also remove functions the library does not know
```

Repairs a database whose koldstore SQL objects differ from the loaded library (see `validate_sql_objects`).
**Returns:** `jsonb` `{dry_run, ok_before, steps[], applied, skipped[], ok_after, still_missing[], still_unexpected[]}`;
each step names its action (`drop_function`, `create_table`, `add_foreign_keys`, `add_column`, `create_function`) and
the statements it runs. A real run executes the plan in one transaction and re-checks. Columns and data are never
dropped. Superuser only. See "SQL object drift" in [Backup and Operations](backup-and-operations.md).

## DML Boundaries

- Normal hot `INSERT`, `UPDATE`, and `DELETE` operate on the heap and mark local
  manifest state pending.
- Standard SQL cold-only `UPDATE` affects zero rows in the MVP.

Explicit cold-row DML is available through `koldstore.hydrate_pk(table, pk)`,
`koldstore.update_row(table, pk, patch, lookup_cold => true)` and
`koldstore.delete_row(table, pk, lookup_cold => true)`. A `BEFORE INSERT` guard
trigger on each managed table rejects an `INSERT` whose primary key already
exists hot or cold, and `UPDATE`/`DELETE`/`MERGE` statements that would need to
change a cold-only key are rejected rather than silently matching nothing
(upstream #122). Rows changed in a transaction are visible to later reads only
after `COMMIT`; see `koldstore.allow_same_txn_cold_reads` above.

To change a managed table's flush settings without unmanaging it, use
`koldstore.alter_table(table, hot_row_limit, min_flush_rows, max_rows_per_file, auto_flush)`; a NULL
argument keeps the current value, and the result is the table's new options. The values are checked
like `manage_table`'s (positive, `max_rows_per_file` at or above `koldstore.min_max_rows_per_file`).
A table managed without a flush policy can opt in by passing `hot_row_limit` (the other two limits
then default to 1000). Existing segments are untouched; the new limits apply from the next flush.
On a partitioned parent it changes every managed leaf and the template for partitions created later.
Owner or superuser only. Storage and scope are not alterable. Compression, target file size and the Parquet layout options
change with `ALTER TABLE ... SET (koldstore_...)` (see `manage_table`).

To delete many rows at once, cold ones included, use
`koldstore.purge_where(table, predicate) → {purged_cold, deleted_hot}`. The predicate is a boolean
expression over the table aliased `t`, e.g. `t.created_at < now() - interval '2 years'`. Owner or
superuser only; requires `READ COMMITTED` and a transaction that has not yet written the table. It
blocks concurrent writes and flushes of that table until it commits, and refuses (without changing
anything) a predicate matching more than `koldstore.max_purge_rows` (default 1,000,000) cold-only
rows. Reads stop returning the purged rows immediately; the next flush records the deletes in cold
storage. Plain `DELETE` is unchanged.

## Changes and Operations

### `koldstore.changes_since`

```sql
-- Resume from an exclusive last-seen cursor (KalamDB `from` / `from_seq_id`).
SELECT seq, op, pk, deleted, row_image, source
FROM koldstore.changes_since(
  table_name => 'messages'::regclass,
  since_seq  => 0,
  limit_rows => 1000
);

-- Newest-N rewind (KalamDB `last_rows`); delivered oldest→newest.
SELECT seq, op, pk, deleted, source
FROM koldstore.changes_since(
  table_name => 'messages'::regclass,
  since_seq  => 0,
  limit_rows => 1000,
  last_rows  => 100
);
```

Returns changes ordered by exclusive `seq`, catalog-routing to the oldest
applicable cold Parquet segment (streamed until `limit_rows`) or the hot `__cl`
mirror. The same primary key may appear again on a later page with a higher
`seq` (no in-page latest-state collapse). Modes match KalamDB live subscribe
options:

| Mode | Args | Behavior |
|------|------|----------|
| Resume | `since_seq > 0` | exclusive `seq > since_seq`, ASC, capped by `limit_rows`; **`last_rows` ignored** |
| Rewind | `since_seq = 0` + `last_rows` | newest N retained changes, ASC after rewind; no older pages |
| From start | `since_seq = 0`, no `last_rows` | from start of retained history, ASC, `limit_rows` |

`last_rows` must be `<= limit_rows`. A positive `since_seq` older than the
retained floor raises a retention-gap error. `source` is `hot` or `cold`.

The following operator surface is planned but not yet exposed by the extension
(tracked: https://github.com/kalamdb/koldstore/issues/56); `backup_manifest` and
`validate_cold_storage` are shipped (see above):

- `koldstore_exec('EXPORT TABLE ...')` — `IMPORT TABLE` remains rejected until
  ownership and conflict rules are complete

## Security

User-scoped tables require `koldstore.user_id` and fail closed when it is
missing. The GUC is user-settable and must be bound by a trusted connection
layer; it is not authentication. The generated policy is permissive, so another
permissive policy on the same table can broaden the combined RLS expression.
RLS/security qualifiers must be enforceable on cold rows or planning must fail
closed. Until extension-function grants, ownership checks, and definer
`search_path` are hardened, do not expose the management SQL API to untrusted
database roles.

## Upgrade note

Existing flat `hot_row_limit` catalog JSON remains readable; new policy writes
use tagged `flush_policy` JSON and require no eager rewrite. The `ALTER TABLE`
hook becomes available after installing the new shared library and restarting
PostgreSQL with KoldStore in `shared_preload_libraries`.

### Extension upgrades

`ALTER EXTENSION koldstore UPDATE` needs a `koldstore--<from>--<to>.sql` file.
pgrx only generates full install snapshots, so these are written by hand in
`crates/pg_koldstore/sql/` and shipped by `cargo pgrx install`. The
`0.1.11-preview.0` -> `0.1.12-preview.0` script adds the cold-row functions,
replaces `manage_table` (new optional parquet tuning parameters), and installs
the insert guard trigger on tables managed under 0.1.11.

After adding or changing an upgrade script, run
`scripts/check-upgrade-path.sh <from-version>` against a cluster that has the new
build installed and loaded. It upgrades a scratch database and diffs its
catalog (functions, tables, columns, constraints, indexes, triggers, ACLs)
against a freshly created one. Restart PostgreSQL after installing a new shared
library; a backend that loaded the old library cannot call new functions.
