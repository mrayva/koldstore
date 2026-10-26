# Limitations

pg-koldstore keeps PostgreSQL authoritative for hot rows and stores flushed rows
as Parquet plus manifest metadata. That boundary is important: cold row values
can be preserved in cold files, but PostgreSQL-owned indexes remain attached to
rows that still live inside PostgreSQL.

## Shared preload is mandatory

`shared_preload_libraries` must include `koldstore`. Planner hooks that inject
`KoldMergeScan` are registered only when the library loads at postmaster start.
Without preload, fresh sessions can silently run ordinary heap `Seq Scan` on
managed tables and return **hot-only** rows after flush.

- Install order: package files → set preload → **restart** → `CREATE EXTENSION`
- `session_preload_libraries` is not sufficient
- Removing preload after `manage_table` is **unsupported** (no extension code
  runs to intercept heap reads; restore preload and restart)

Check with `SELECT koldstore.preload_status();`.

## PostgreSQL semantic compatibility

The original relation remains a PostgreSQL heap, but a cold row returned from
Parquet is not a heap tuple. The preview therefore does not claim that every
PostgreSQL operation keeps its normal semantics across both tiers.

- Mirror capture is asynchronous and sees committed WAL only, so it cannot
  provide read-your-own-uncommitted-writes for a key with an older cold version
  ([#121](https://github.com/kalamdb/koldstore/issues/121)). Rather than return
  a stale cold row, a read that has to consult cold data **fails** once the same
  transaction (or an enclosing one) has written that table: `COMMIT`, call
  `wait_for_async_mirror()`, then read. Reads of other tables, of tables with no
  cold data, and plain `EXPLAIN` are unaffected, and a rolled-back savepoint
  forgets its writes. `SET koldstore.allow_same_txn_cold_reads = on` accepts the
  stale-read risk instead.
- `wait_for_async_mirror()` fences commits up to a captured WAL boundary. Call
  it before acquiring a fixed `REPEATABLE READ` or `SERIALIZABLE` snapshot; it
  cannot advance an existing snapshot or decode the caller's uncommitted work.
- A plain `UPDATE`, `DELETE` or `MERGE` only sees the hot heap, so it can never
  change a row that exists only in cold storage
  ([#122](https://github.com/kalamdb/koldstore/issues/122)). Instead of silently
  skipping such rows, KoldStore rejects the statement:
  - `INSERT` (including `INSERT ... SELECT`, `COPY` and the insert branch of a
    `MERGE`) of a primary key that already exists hot or cold is rejected by a
    per-table `BEFORE INSERT` trigger. Native `ON CONFLICT` inspects only the hot
    index, so hydrate the key first with `koldstore.hydrate_pk()`. With the
    experimental `koldstore.hydrate_on_write = on`, a top-level
    `INSERT ... ON CONFLICT (pk) DO NOTHING / DO UPDATE` onto a managed table
    hydrates the conflicting cold key itself and then lets native conflict
    handling run. Not covered: inserts inside CTEs, partitioned parents, and
    arbiters other than the primary key (a conflict on a non-PK unique key held
    only by a cold row is still refused).
  - `UPDATE`/`DELETE`/`MERGE` on a primary key (equality, `IN`, an `OR` chain on
    one column, combined with further conditions) that exists but was not reached
    is rejected, naming the key.
  - For every other `WHERE` shape (`id > 5`, `NOT IN`, `OR` across columns, a
    non-key column, a function, no `WHERE` at all) the statement is rejected if
    the clause also matches any cold-only row; the check counts cold matches
    with a second read of cold storage, so it costs a cold scan for such
    statements on a table that has cold data (`koldstore.guard_scan_writes`,
    default on, turns it off). It is skipped, not rejected, when the
    transaction already wrote the table (uncommitted work is invisible to the
    async mirror, so a stale cold copy could be miscounted).
    Joins, `USING` and sub-queries are handled too: the statement is turned into
    an equivalent `SELECT <primary key>` at plan time and counted the
    same way, with the target table read hot-only and every other managed table
    read hot + cold. A clause that cannot be reproduced at all -- a volatile
    function, `WHERE CURRENT OF`, a CTE used as a join source, or any other
    shape the plan-time probe declines -- fails closed instead of silently
    skipping: rejected whenever the table has cold data at all, regardless of
    whether the statement would actually have matched a cold row.
  - A `MERGE` that updates or deletes target rows through a multi-row join is
    rejected when the table has cold data, because the join only matches hot
    rows and the source keys are gone once it ends. `INSERT`-only and
    `DO NOTHING` merges, and single-row merges by primary key, are unaffected.

  Change a cold row with `koldstore.update_row()` / `delete_row()`, which act on
  one primary key at a time, or `koldstore.purge_where()` to delete many rows (hot and cold)
  matching a predicate.
- Cold rows have no heap `ctid`, `xmin`, tuple lock, or SSI predicate lock.
  When cold data can contribute, `SELECT ... FOR UPDATE/NO KEY UPDATE/SHARE/KEY
  SHARE`, `TABLESAMPLE` and system-column projections (`ctid`, `xmin`, ...) are
  refused with an error naming the table and the construct; a managed table with
  no cold data keeps the ordinary PostgreSQL behavior. `TRUNCATE` (including
  `CASCADE`) is refused before anything is changed. `SERIALIZABLE` reading cold
  data is not a PostgreSQL-equivalent guarantee (no predicate locks on cold
  rows), so it is refused by default (2026-09-27, matching #121's
  same-transaction guard); `koldstore.reject_serializable_cold_reads = off`
  accepts the weaker guarantee instead.
  With the experimental `koldstore.hydrate_on_write = on`, a top-level
  `SELECT ... FOR UPDATE/NO KEY UPDATE/SHARE/KEY SHARE` hydrates the cold rows it selects first and
  then takes the native tuple locks: any single-table `WHERE` (primary key or not), and joins /
  sub-queries (the statement's own FROM/WHERE is re-run to find the locked table's cold rows). A
  `LIMIT` / `OFFSET` bounds the hydration: only the rows through the end of the window (`LIMIT + OFFSET`
  of the statement's own ordering) are copied into the heap; under `NOWAIT` / `SKIP LOCKED` every
  matching cold row is hydrated instead, since a skipped row shifts the window. A partitioned or inherited parent works too: each managed leaf
  hydrates the rows its own scan filter selects (pruned leaves are skipped); a join/sub-query across
  partitions whose filter cannot be reproduced per leaf errors. A lock inside a CTE body or sub-select, and a locking statement that joins a (non-recursive,
  non-data-modifying) CTE, are hydrated the same way; a join *inside* a CTE body that locks, or a correlated
  sub-select lock, errors. Not covered: recursive / data-modifying CTEs, set operations, and a
  `WHERE` with volatile functions or `CURRENT OF` (these error). Needs READ COMMITTED or REPEATABLE READ (or
  `reject_serializable_cold_reads = off`); otherwise, and when this transaction already wrote the
  table, it errors rather than skipping the cold row. A second session locking a key the first is
  still hydrating gets a retry error. A prepared plan made while the GUC was on keeps its hot-only
  shape if the GUC is later turned off.
- Foreign keys that reference a managed table with cold data: deleting or re-keying a
  parent row is refused (the referential-integrity scan of the child needs row locks,
  which cold rows do not support, #125), so parent rows cannot be removed while such a
  child has cold rows. Foreign keys are enforced on hot rows only. (Superseded for FKs from
  unmanaged tables onto the primary key: see "Inbound foreign keys".)
- **Partitioned tables.** `manage_table(partitioned_parent, ...)` manages every
  current leaf partition with the given options and records them as a
  *template* (`koldstore.partition_templates`); a partition created
  (`CREATE TABLE ... PARTITION OF`) or attached (`ATTACH PARTITION`) later --
  at any depth of sub-partitioning -- is managed with the same options in the
  same transaction, and a failure to manage it fails that DDL
  (`koldstore.set_partition_auto_manage(parent, false)` pauses this). On the
  parent, `unmanage_table`, `flush_table`, `enqueue_flush_job`,
  `set_table_auto_flush` and `table_status` fan out to the managed leaves
  (`table_status` adds roll-up totals and a per-partition breakdown), and
  `DROP TABLE parent` cleans up every managed leaf it drops. Foreign-table
  partitions are skipped with a notice. The parent itself never gets a catalog
  row -- it has no storage of its own to flush or scan -- and detaching a leaf
  leaves it managed on its own. An inheritance *parent* still cannot be managed
  ([#125](https://github.com/kalamdb/koldstore/issues/125),
  [ADR-008](decisions/008-partitioned-inherited-table-support.md)). A
  partition or inheritance **child/leaf** may also be managed on its own, exactly
  like a plain table, and `ATTACH PARTITION`/`INHERIT`/`DETACH PARTITION` work
  normally in either direction as long as the relation taking the *parent*
  role is not itself managed. A plain `SELECT` through the parent sees a
  managed leaf's hot and cold data correctly (PostgreSQL's own
  `Append`/`MergeAppend` construction calls the same per-relation scan hook
  used for a standalone managed table) and a plain `INSERT` through the
  parent routes to the leaf's hot heap exactly as it would for an unmanaged
  partition -- both verified live. **`UPDATE`/`DELETE` through the
  parent get the same precise cold-match check as a plain table, per managed
  leaf.** PostgreSQL gives `ModifyTable` a single child plan -- an `Append` of
  one filtered scan per leaf -- so the guard recovers each managed leaf's own
  filter from its scan; a leaf the plan never scans (plan-time pruning) cannot
  be touched and is skipped. A statement whose predicate reaches cold rows is
  refused with the usual "matches N cold row(s)" message, and one that only
  touches hot rows, or only an unmanaged sibling leaf, runs normally.
  **`MERGE`, and an `UPDATE`/`DELETE` that joins across partitions or uses a
  sub-query, get the same precision** through the planner hook's probe, built
  against the parent: for each managed leaf the guard counts the cold-only rows
  the probe matches (read once through the merged view and once hot-only), so a
  `MERGE` that only matches hot rows, or only inserts new keys, is not held up
  by the leaf's unrelated cold data, and one that reaches a cold row is refused
  with the usual "matches N cold row(s)" message. Only a shape with no probe
  either (a volatile function) is refused whenever the managed leaf has cold
  data *anywhere*. With `koldstore.hydrate_on_write` on, all of these hydrate
  the cold rows they match in each managed leaf before running, exactly as on
  a plain table (one statement can hydrate several managed leaves, and a
  `MERGE` can update, delete and insert across them). A partition-key-changing
  `UPDATE` is not a gap: a partitioned table's primary key must include the
  partition key, so moving a row across leaves is a primary-key update, which
  koldstore refuses on a managed table in any case. `manage_table` still
  refuses foreign, temporary and unlogged tables, views, materialized views and
  sequences as before.
- Schema, table and column names may be any valid PostgreSQL identifier:
  mixed case, reserved words, spaces, embedded quotes and dots, slashes,
  non-ASCII letters, a leading digit, and leading or trailing blanks. Names are
  taken exactly (never trimmed or case-folded) and always double-quoted in
  generated SQL. Helper objects derive plain ASCII names from the source name
  (a hash keeps two different names apart), and schema and table names are
  percent-encoded in object-store prefixes, so a name like `../x` cannot leave
  its own prefix. Names that were plain ASCII keep exactly the names and paths
  they had before. PostgreSQL's own 63-byte identifier limit still applies.

- `ALTER TABLE ... RENAME TO`, `ALTER TABLE ... SET SCHEMA` and
  `ALTER SCHEMA ... RENAME TO` work on managed tables. Object-store paths are
  derived from the table and schema name, so the first such change pins the
  table's current prefix in `koldstore.table_prefix_pins`: published cold
  objects stay where they are, and later flushes and compactions keep writing
  under the pinned prefix ([#123](https://github.com/kalamdb/koldstore/issues/123)).
  A table created under a name whose prefix is still pinned by a renamed table
  cannot be managed until that table is unmanaged or dropped. Exported table
  metadata carries the prefix, and `import_table_metadata` re-pins it when the
  restored table has a different name. Schema evolution on a table with cold data:
  adding, dropping and renaming columns, and `varchar(n)` / numeric-precision
  changes work. Widening a non-key, non-indexed column (`smallint` -> `integer`
  -> `bigint`, `real` -> `double precision`, integer -> `numeric`) works too:
  older segments keep their narrow type and are widened on read. Narrowing,
  cross-type or `USING` changes, any type change of a primary-key or indexed
  column (their cold bounds and blooms are type-encoded), and primary-key
  changes are refused and the `ALTER TABLE` is rolled back; apply one with
  `koldstore.rebuild_table(table, 'ALTER TABLE ...')`, which rehydrates the cold
  rows, runs the DDL as the calling role and manages the table again with its
  stored options, all in one transaction (a failing DDL, e.g. a new key the data
  violates, rolls everything back; rows return to cold storage on the next
  flush; it holds the table locked and rewrites it, so it suits tables that fit
  back in the heap). Defaults and constraints are not
  retroactively enforced on older Parquet rows.
- `COPY <table> TO ...` (the plain table form, including what `pg_dump` issues for table data)
  never goes through the planner, so on its own it would export the hot heap only
  ([#126](https://github.com/kalamdb/koldstore/issues/126)). When the table has cold data it is
  rewritten to `COPY (SELECT <columns> FROM table) TO ...`, which plans normally and exports hot and
  cold rows. A logical `pg_dump` therefore carries all rows, but not the cold-tier metadata: the
  restored table is an ordinary table until `manage_table` is run on it again; alternatively set
  `koldstore.dump_cold_as_metadata = on` for a hot-only dump and carry the cold tier with
  `koldstore.export_table_metadata()` / `import_table_metadata()`. Backup and PITR that keep
  the cold tier are supported through a physical base backup plus WAL archive and a retained cold
  object prefix, checked with `koldstore.backup_manifest` / `koldstore.validate_cold_storage` (see
  [Backup and Operations](backup-and-operations.md)).

### Compatibility matrix

Each row is pinned by a case in `tests/sql/`; "refused" means an error before
any row or object is changed.

| Construct | Behavior | Case |
|-----------|----------|------|
| `SELECT` hot + cold, `ORDER BY` (incl. composite primary key) | supported | `merge_order_composite_pk`, `query_semantics` |
| Read of a table written in the same transaction | refused when cold data is consulted | `txn_local_visibility` |
| User-scoped tables: cold segments per scope | one segment group and folder per scope (`{scopeId}/…`); delete markers follow their row into its scope; a read with a resolvable scope (RLS role or equality / `IN` on the scope column) opens only that scope's segments plus table-wide ones (ADR-010) | `scoped_cold_layout` |
| `UPDATE` changing a scoped table's scope column | refused (SQLSTATE `0A000`); a key has one scope for life | `scope` |
| `UPDATE`/`DELETE` matching cold rows after an earlier write to the table in the same transaction | refused (it cannot hydrate them safely; it used to skip them silently) | `same_txn_cold_dml` |
| `SELECT ... FOR UPDATE / SHARE` | refused when cold data can contribute | `unsupported_constructs` |
| `TABLESAMPLE` | refused when cold data can contribute | `unsupported_constructs` |
| `ctid` / system columns | refused when cold data can contribute | `unsupported_constructs` |
| `SERIALIZABLE` cold reads | refused by default; runs with a weaker guarantee if `koldstore.reject_serializable_cold_reads = off` | `unsupported_constructs` |
| `TRUNCATE`, `TRUNCATE ... CASCADE` | refused | `unsupported_constructs` |
| `INSERT` of an existing hot or cold key | refused | `cold_dml_guard` |
| `UPDATE`/`DELETE` by primary key reaching a cold-only row | refused | `cold_dml_guard` |
| `UPDATE`/`DELETE` by range, `NOT IN`, `OR`, non-key column, function, no `WHERE` | refused if it also matches cold-only rows | `cold_dml_scan_guard` |
| `UPDATE`/`DELETE` with a join, `USING` or sub-query (`IN`, `EXISTS`, `NOT IN`, self-join, another managed table as source) | refused if it also matches cold-only target rows | `cold_dml_scan_guard` |
| `UPDATE`/`DELETE` inside a data-modifying CTE (`WITH d AS (DELETE ... RETURNING ...)`), with `koldstore.hydrate_on_write` off (the default), or with a join/sub-query source | refused if it also matches cold-only rows | `cold_dml_scan_guard` |
| A statement whose WHERE clause cannot be reproduced at all (a volatile function, `WHERE CURRENT OF`, a CTE used as a join source) | refused whenever the table has cold data | `cold_dml_scan_guard` |
| Plain `UPDATE`/`DELETE` on cold-only rows, single table -- the top-level statement or a data-modifying CTE's own target -- or a row-changing `MERGE` (experimental, `koldstore.hydrate_on_write = on`) | supported by hydrating the rows first, up to `koldstore.max_hydrate_rows`; `READ COMMITTED`/`REPEATABLE READ` always, `SERIALIZABLE` only when `koldstore.reject_serializable_cold_reads` is off; user insert triggers fire for the hydrated rows; a `MERGE` inside a CTE is not attempted | `hydrate_on_write` |
| `MERGE` changing target rows through a join the planner hook's probe cannot reproduce (a CTE source, a volatile function in the `ON` clause), or with `koldstore.hydrate_on_write` off (the default) | refused if the `ON` clause also matches cold-only target rows (a precise recount, not a blanket "the table has cold data" rejection) | `cold_dml_scan_guard` |
| Inheritance parents, foreign, temporary, unlogged tables, views | refused by `manage_table` (a partitioned table is managed through its leaves) | `manage_relation_kinds`, `partitioned_parent` |
| Adding a managed table to a hierarchy | refused | `manage_relation_kinds` |
| Renaming/moving-schema a managed table (or its schema) with cold data | refused | `unsupported_constructs` |
| `COPY <table> TO ...` (plain table form) on a managed table with cold data | exports hot+cold rows (rewritten to a merged-view query) | `unsupported_constructs` |
| Any valid identifier (spaces, quotes, dots, slashes, non-ASCII, leading digit, blanks, long names) in schema, table or column | supported | `odd_identifiers` |
| `DROP TABLE` / `DROP SCHEMA` of managed tables | supported; helper objects removed | `drop_cleanup_objects` |

The generated user-scope policy is application-context filtering, not an
authentication boundary. `koldstore.user_id` is a user-settable GUC, and the
generated policy is permissive, so another permissive policy can broaden the
combined RLS expression. Use a trusted connection layer and dedicated roles;
do not advertise this surface as database-enforced tenant isolation. Management
API privilege hardening is tracked in
[#120](https://github.com/kalamdb/koldstore/issues/120).

Planner cardinality and cost estimates are also a preview limitation, mostly
addressed. `KoldMergeScan`'s own row estimate adds the active cold row total
to the hot child's estimate for broad scans (the strategies other than an
exact primary-key equality lookup, which correctly stays a ~1-row point
estimate regardless of how much cold data exists) -- `EXPLAIN`, anything
costed directly on top of the scan (a `LIMIT`, a non-join aggregate, a
`Sort`), **and join-order sizing** (a managed table's `RelOptInfo.rows`,
which `calc_joinrel_size_estimate` reads when it takes part in a join) now
all see a realistic total instead of hot-only. Not yet addressed: the
addition is not reduced by `WHERE`-clause selectivity (koldstore has no
cold-side column statistics to estimate that with, only row totals), so a
highly selective filter over a broad-scan strategy still estimates as if it
might match everything cold. Tracked in
[#124](https://github.com/kalamdb/koldstore/issues/124).

## Extensions that run Postgres on helper threads

Some extensions (`pg_duckdb`, `pg_ducklake`) run Postgres work, such as SPI or subtransactions
inside `CREATE EXTENSION`, on a DuckDB helper thread while the backend thread waits. Postgres
then invokes KoldStore's hooks and transaction callbacks on that helper thread. pgrx aborts the
whole process on any Postgres call from a second thread, and KoldStore's per-backend state is
thread-local, so every hook and callback is a pass-through off the backend's own thread
(`thread_guard`). Before this, `CREATE EXTENSION pg_ducklake` with KoldStore preloaded crashed
the entire cluster ("postgres FFI may not be called from multiple threads").

The consequence: Postgres work done on such a helper thread is invisible to KoldStore. A write to
a managed table made there is not tracked (no maintenance wake-up, no same-transaction cold-read
tracking), the cold-DML write guard and hydrate-on-write do not run for it, and a plan built there
does not get `KoldMergeScan`, so it would read only the hot heap. Run such statements on the
backend's own thread (plain SQL in the session) when they touch managed tables.

## Unique and Foreign Key Constraints

Non-primary-key `UNIQUE` constraints and **foreign keys** (outbound to an unmanaged or a
managed parent, inbound from an unmanaged or a managed child; onto the primary key or any
other unique key) are enforced **globally across hot and cold rows**. (Flush removes the
flushed rows' index and constraint entries from the hot table, so PostgreSQL alone would see
the hot heap only.)

### Global `UNIQUE` enforcement

`koldstore.manage_table` accepts tables with non-PK `UNIQUE` constraints and unique
indexes. The per-table write-guard trigger (the same one that guards primary keys)
then fires on `INSERT`, and on `UPDATE` of a constrained column, and looks the new
values up in the merged hot+cold view. A conflicting row anywhere is refused with
SQLSTATE `23505`. The cold side of the lookup is pruned by the per-segment column
min/max statistics and reads only the surviving row groups.

- Multi-column constraints are supported. `NULL` values do not conflict (unless
  the constraint is `NULLS NOT DISTINCT`).
- Concurrent writers of the same value are serialised with a transaction-scoped
  advisory lock, so a hot insert and a cold-duplicate insert cannot both succeed.
- A delete committed moments ago is honoured (the same mirror-lag fence the primary
  key guard uses).
- Constraints added or dropped through `ALTER TABLE … ADD/DROP CONSTRAINT`,
  `CREATE UNIQUE INDEX` or `DROP INDEX` after `manage_table` re-plan the guard automatically.
- **Partial and expression unique indexes are enforced on hot rows only**
  (a warning names them when the guard is attached).
- The check costs one merged-view lookup per constrained insert, so bulk loads
  into a table with a cold-heavy history and a non-selective unique column pay for
  it; the lookup is cheapest when the unique column correlates with segment
  ranges (for example time-ordered or sequential identifiers).

### Runtime behavior

| Constraint | Hot rows | Cold rows | Normal DML checks cold? |
|------------|----------|-----------|-------------------------|
| Primary key | Yes | Logical winner via merge | Insert guard (`INSERT` only) |
| `UNIQUE` (non-PK) | Yes | Yes | Yes, via the write guard |
| Outbound FK to an unmanaged parent (any action) | Yes | Yes | Yes, via the parent-side guard |
| Inbound FK from an unmanaged child onto the PK (any `ON DELETE` action) | Yes | Yes | Yes, via the child-side guard and hydration |
| FK between two managed tables onto the parent's PK (any action) | Yes | Yes | Yes, via both guards |
| Other foreign keys | Yes | No | No |

### Outbound foreign keys

For `child -> parent` with a managed child and an unmanaged parent, the native FK trigger
still checks every hot child write exactly. `manage_table` additionally installs a guard
trigger on the **parent** that refuses a `DELETE`, or an `UPDATE` of the referenced key,
while *cold* child rows still reference it (SQLSTATE `23503`). Notes:

- `NO ACTION`/`RESTRICT` refuse while cold children exist. `CASCADE`, `SET NULL` and
  `SET DEFAULT` (on delete, and on update of the referenced key) are applied to cold children
  too: the guard first hydrates them into the heap (at most `koldstore.max_hydrate_rows`; the
  statement is refused beyond that, or when this transaction already wrote the child table),
  and the native action then fires on them. Hydrated children are flushed back to cold storage
  by the next flush.
- The parent's owner must be the user running `manage_table` (a trigger is created on it).
  The guard is re-planned on `ALTER TABLE` of the child and removed by `unmanage_table`.
- PostgreSQL's own parent-side check probes the child with `SELECT … FOR KEY SHARE`, which
  koldstore refuses on a table with cold data; that exact probe is planned hot-only and the
  parent guard covers the cold side.
- A foreign key to a *managed* parent's primary key is enforced globally as well; see
  "Inbound foreign keys" (the parent is hydrated for child writes, and this guard covers its
  cold children). The referenced key may also be a non-primary-key unique constraint: the
  parent is then looked up by those columns, and changing the referenced value is covered
  like a delete (refused, or applied to cold children, per the `ON UPDATE` action).

### Inbound foreign keys

For `child -> managed` with an **unmanaged** child that references the managed table's
**primary key or another unique key** (any `ON DELETE` action; the child may itself be managed):

- A guard trigger on the child hydrates a cold-only parent row (as `koldstore.hydrate_pk`
  does) before a write, so the native check finds and locks a hot parent. Flush prunes hot
  rows with RI triggers off, so the parent returns to cold storage while children keep
  referencing it. A missing parent still fails with `23503`.
- `delete_row`/`update_row` hydrate the row and run an ordinary statement, so native RI
  refuses to orphan children (`NO ACTION`/`RESTRICT`) or applies the action
  (`CASCADE`/`SET NULL`/`SET DEFAULT`). `koldstore.purge_where` does the same for the
  referencing rows of the purged cold rows: it applies `CASCADE`/`SET NULL`/`SET DEFAULT`, and
  with `NO ACTION`/`RESTRICT` it refuses (`23503`) when any matching row is referenced.
  Managed tables do not support primary-key updates, so `ON UPDATE` actions only matter for
  a key onto a non-primary-key unique constraint, whose value can change.
- The guard is created by `manage_table`, re-planned when a foreign key is added by
  `CREATE TABLE`/`ALTER TABLE` on the child, and removed by `unmanage_table`.  Dropping the child table or the constraint removes the guard.
- For a managed child, `purge_where` on the parent hydrates the matching cold children before
  applying `CASCADE`/`SET NULL`/`SET DEFAULT`, and `delete_row` on the parent runs the
  parent-side guard (refusing, or hydrating cold children for the native action).
- Each child write against a cold parent pays one hydration (a heap insert of the row).

### `manage_table` validation

Every foreign key shape is now enforced globally, so `koldstore.manage_table` no longer
needs `options.allow_fk_hot_only = true` for any of them; the option is still accepted.

Hot-only management (`hot_row_limit` omitted) keeps native PostgreSQL constraint
semantics because flushed rows are not expected to leave the hot heap.

## Custom and Extension Indexes

PostgreSQL indexes do not move to cold storage. When a flush writes eligible
rows to cold files and removes those rows from the hot table, PostgreSQL removes
their index entries too.

This applies to built-in indexes, custom indexes, and extension-owned indexes.
Kalam does not automatically translate those indexes into object-storage
indexes over Parquet files.

Queries stay correct, though: a predicate that a hot-only index would serve (GIN on `jsonb`
or arrays, BRIN, expression, partial and `text_pattern_ops` indexes, expression full-text
indexes) is answered over the merged hot and cold rows, whichever scan types the planner may
use, and stale cold versions of updated rows never match. The cold side of such a query is a
filtered scan of the segments that min/max statistics and Bloom filters cannot rule out; list
the columns you filter on in `pruning_columns` / `bloom_filter_columns` to narrow it. The
`merge/cold_index_predicates` e2e test pins this against a plain-table reference. One-dimensional `text[]` and
`smallint[]`, `int[]`, `bigint[]`, `real[]`, `double precision[]`, `numeric[]`, `uuid[]`, `bool[]`, `date[]`, `timestamp[]`, `timestamptz[]` and `interval[]` are supported, including
NULL elements, `NaN`/`Infinity`, explicit lower bounds (`[0:2]={x,y,z}`) and multi-dimensional values (scalar
arrays: `tests/sql/scalar_array_types.sql`; the temporal ones are stored as ISO text, so values are exact whatever `DateStyle`, `TimeZone` and `IntervalStyle` the flush ran under: `tests/sql/temporal_array_types.sql`); other array types are rejected at `manage_table`, and a
multi-dimensional `text[]` value fails the flush. Cold reads keep every `jsonb` value, bare JSON strings included
(`cold_array_jsonb_fidelity`). Hydrate-on-write goes through row JSON, with array columns carried as their text
literal so bounds survive; a JSON `null` in a `jsonb` column still becomes SQL `NULL`.

`json`, `inet`, `cidr`, `macaddr`, `char(n)`, `time`, `timetz` and `interval` are managed the same way, as their text
form, so a cold read returns exactly what was stored (`json` keeps its whitespace and key order; `char(n)` keeps its
padding; an `interval` is stored as ISO 8601, so `1 day`, `24 hours` and `30 days` stay distinct whatever `IntervalStyle` the flush ran under (`tests/sql/interval_type.sql`; the others: `tests/sql/text_form_types.sql`). They carry no min/max statistics and cannot be a migration-order column.
A user-defined enum column is stored as its label and read back as the column's own enum type, so comparisons and `ORDER BY` follow the declaration order; labels added later with `ALTER TYPE ... ADD VALUE` work on new and old rows (`tests/sql/enum_types.sql`). Enum predicates are evaluated after the cold read rather than pushed into it, and an enum cannot be the primary key or the migration-order column; enum arrays are refused.
`bit(n)`, `bit varying(n)`, `macaddr8`, `name` and `oid` are managed in text form too (`tests/sql/bit_name_oid_types.sql`).
A domain over a supported type (nested domains included) is stored as its base type, as a column, a key or the
migration-order column; the domain's constraints keep applying to the heap, hydrate-on-write included
(`tests/sql/domain_types.sql`). A domain over an unsupported type is refused like the type itself.
Still unsupported, and refused by `manage_table`: `money` (its text form depends on `lc_monetary`),
composite and range types, geometric types, `xml`, and arrays of other element types (`money[]`, enum arrays, …).

`tsvector` columns are managed as their text form (lexemes, positions and weights round-trip;
`tests/sql/tsvector_type.sql`). `STORED` generated columns (including a generated `tsvector`) are supported: their values are flushed to cold
storage like any column, and hydrate-on-write and `unmanage_table` skip them on insert so PostgreSQL recomputes
them from the restored base columns (`tests/sql/generated_columns.sql`). `VIRTUAL` generated columns have no
storage and are still rejected. Identity columns (`ALWAYS` and `BY DEFAULT`) round-trip: hydration and
`unmanage_table` insert with `OVERRIDING SYSTEM VALUE`, and the sequence keeps counting from where it was
(`tests/sql/identity_columns.sql`).

Listing a `text[]`, `jsonb` or `tsvector` column in `pruning_columns` also gives every new segment a *term
filter* (a one-hash membership bitmap in `cold_segment_index.value_summary`, built from the
array elements, or the keys, scalar values and key/value pairs of the JSON at any depth). A
constant or parameter `@>`, `&&`, `?`, `?|` or `?&` predicate then skips the segments whose
filter rules it out. It prunes only while no key is stored in two active segments (otherwise a
newer version could be hidden and a stale one resurrected), never for a NULL or empty probe,
a bare scalar `jsonb @>` probe or a non-deterministic collation, and never for segments
written before the column was listed (they carry no filter and are always read until
`koldstore.rebuild_segment_summaries()` gives them one; it rebuilds term filters and `vector`
sketches from the objects, byte-identical to what flush writes, `tests/sql/rebuild_term_filters.sql`). The filter
can only over-read, never under-read; `tests/sql/term_filter_pruning.sql` pins both.

For a `tsvector` the filter holds the lexemes, and `tsv @@ q` (either order) with a constant or
parameter `tsquery`, or `to_tsquery` / `plainto_tsquery` / `phraseto_tsquery` /
`websearch_to_tsquery` over constants or parameters, is reduced to the lexemes a match must contain:
`&` and phrase operators require both sides, `|` requires either, weights are ignored. Negation and
prefix (`:*`) operands prove nothing, so a query made only of them reads every segment
(`tests/sql/tsvector_pruning.sql`).

## pgvector

pgvector indexes such as HNSW and IVFFlat speed vector similarity search over
rows in a PostgreSQL table. IVFFlat splits vectors into lists and searches
nearby lists; HNSW builds a graph for approximate nearest-neighbor search. Both
index entries point to rows that are still resident in PostgreSQL.

When Kalam flushes old rows to cold storage:

```text
PostgreSQL hot table: row removed
pgvector index: row removed from index
Cold Parquet: row values retained in cold storage
```

The result is intentionally strict:

- Hot rows remain searchable through pgvector.
- Cold rows are not part of pgvector HNSW or IVFFlat indexes after they are
  flushed.
- `vector(n)` columns are in the type matrix: they flush to Parquet (stored as
  the vector's text form), hydrate back on write and re-flush losslessly
  (`tests/sql/vector_type.sql`). The `vector` type must be installed
  (`CREATE EXTENSION vector`) before a table with such a column is flushed or
  read back from cold storage.
- A distance query that the planner serves by a sequential scan or sort reads
  cold rows too (exact results, no index); one served by an HNSW/IVFFlat index
  scan sees hot rows only.
- When a `vector` column is listed in `pruning_columns`, each cold segment
  stores a small ball sketch (centre and radius, for L2 and for cosine). A
  query of the form `SELECT ... FROM t ORDER BY emb <-> '[...]' LIMIT k`
  (also `<=>`, with optional `OFFSET`) then skips segments that provably hold
  none of the `k` nearest rows. It is exact, not approximate, and applies
  only to a single-table query with no `WHERE`, ascending order, a constant
  query vector and a constant `LIMIT`, over keys that do not overlap between
  segments (`tests/sql/vector_knn_pruning.sql`). Any other shape reads every
  segment, as before. Segments written before the column was indexed have no
  sketch and are never skipped.

For v1 behavior, vector search should be treated as hot-only unless a
Kalam-managed cold-vector mode is explicitly enabled.

## ParadeDB and BM25

ParadeDB and BM25-style indexes follow the same boundary. They index data that
is resident in PostgreSQL. They do not automatically index Kalam's external
Parquet cold files.

Kalam's current product promise is ordinary PostgreSQL app tables that can
retain history cheaply, not that every PostgreSQL extension index follows rows
into object storage.

## Supported Search Modes

### Hot-only search

This is the default and safest v1 behavior for pgvector queries:

```sql
SELECT *
FROM documents
ORDER BY embedding <-> $query_embedding
LIMIT 20;
```

That query searches only rows still hot in PostgreSQL. It is a good fit for
recent messages, recent memories, active user documents, and fresh
recommendations. It is not a complete search over archived cold history.

### Cold exact scan

For narrow filters with a small amount of cold data, Kalam can later support
exact vector scans by reading candidate Parquet segments, computing distances in
Rust, and merging cold top-k results with hot pgvector results.

This can work for user-scoped queries such as:

```sql
WHERE user_id = 'u_123'
  AND created_at > now() - interval '1 year'
```

It is not appropriate for global semantic search over all users or millions of
cold vectors.

### Cold vector side index

The future path is a Kalam-managed cold-vector engine. On flush, Kalam can write
a segment-level sidecar vector index next to each Parquet segment:

```text
s3://bucket/kalam/documents/user_id=u1/segment-001.parquet
s3://bucket/kalam/documents/user_id=u1/segment-001.usearch
s3://bucket/kalam/documents/user_id=u1/segment-001.manifest.json
```

Cold vector search would then:

1. Use the manifest to choose candidate cold segments.
2. Search the segment-level sidecar index.
3. Fetch matching rows from Parquet.
4. Merge cold results with hot pgvector results.

USearch is the current preferred candidate for this file-backed custom vector
index. Other embedded index implementations may be evaluated later, but the
important design rule is that cold vector indexes are Kalam-owned files, not
pgvector indexes moved out of PostgreSQL.

## Design Rule

Cold vector search should start segment-based, not as one giant global cold
index. A practical layout is per table, per user or tenant, and per time
segment:

```text
documents/user_id=123/year=2026/month=01/segment-0001.parquet
documents/user_id=123/year=2026/month=01/segment-0001.usearch
```

That keeps rebuilds, compaction, deletes, and tenant-scoped search manageable.

## Memory on small machines

Flushing millions of rows is streaming: peak extension memory tracks
**`max_rows_per_file`**, not total flush volume. Large demo/bench file sizes
(hundreds of thousands to 1M rows per Parquet segment) intentionally raise RSS;
product defaults (`max_rows_per_file = 1000`) keep the spike small. Idle
container RSS near ~200 MiB after work is usually PostgreSQL `shared_buffers`
(default 128 MB), which does not shrink while the postmaster is up.

See [Memory and small machines](performance.md#memory-and-small-machines) for
knobs (`max_parallel_flush_jobs`, async apply tick budgets, merge seen-key cap)
and a small-host checklist.

## Behavior Summary

| Feature | Hot rows | Cold rows |
|---------|----------|-----------|
| Supported `SELECT` shapes | Yes | Yes, through `KoldMergeScan` |
| Primary key enforcement | Yes | No global enforcement; winner resolution only |
| `UNIQUE` (non-PK) | Yes | Yes (write guard; partial/expression indexes hot-only) |
| Foreign keys | Yes | Yes (outbound, inbound, managed or not; onto the PK or a non-PK unique key) |
| PostgreSQL custom indexes | Yes | No |
| pgvector index search | Yes | No |
| ParadeDB/BM25 index search | Yes | No, unless separately indexed |
| Vector column value | Yes | Planned with explicit type support |
| Exact vector scan | Yes | Possible for narrow scans, slower |
| Approximate vector search | Yes, through pgvector | Future Kalam sidecar index |
