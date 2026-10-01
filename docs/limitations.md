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
    index, so hydrate the key first with `koldstore.hydrate_pk()`.
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
  one primary key at a time.
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
- Foreign keys that reference a managed table with cold data: deleting or re-keying a
  parent row is refused (the referential-integrity scan of the child needs row locks,
  which cold rows do not support, #125), so parent rows cannot be removed while such a
  child has cold rows. Foreign keys are enforced on hot rows only.
- Only ordinary, permanent tables that take no part in a partition or
  inheritance hierarchy can be managed: `manage_table` refuses partitioned
  tables, partitions, inheritance parents and children, foreign, temporary and
  unlogged tables, views, materialized views and sequences, and later `INHERIT`,
  `ATTACH PARTITION`, `INHERITS (managed)` or `PARTITION OF managed` on a managed
  table is refused
  ([#125](https://github.com/kalamdb/koldstore/issues/125)).
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
  `ALTER SCHEMA ... RENAME TO` are refused for a managed table (or a schema
  containing one) that has published cold data, because object-store paths
  are derived from the table and schema name and renaming would orphan the
  segments already written under the old name
  ([#123](https://github.com/kalamdb/koldstore/issues/123)). A managed table
  with no cold data yet, and `RENAME COLUMN` on any managed table, are
  unaffected. Other schema evolution can still apply in PostgreSQL before
  KoldStore discovers it is unsupported; defaults and constraints are not
  retroactively enforced on older Parquet rows.
- `COPY <table> TO ...` (the plain table form, including what `pg_dump --data-only -t table`
  issues under the hood) never goes through the planner, so it can never enter `KoldMergeScan`
  and would silently export the hot heap only. Refused when the table has cold data
  ([#126](https://github.com/kalamdb/koldstore/issues/126)); use `COPY (SELECT * FROM table) TO ...`
  instead, which plans normally and sees cold data too. Coordinated backup/PITR across hot and cold
  storage is still not shipped.

### Compatibility matrix

Each row is pinned by a case in `tests/sql/`; "refused" means an error before
any row or object is changed.

| Construct | Behavior | Case |
|-----------|----------|------|
| `SELECT` hot + cold, `ORDER BY` (incl. composite primary key) | supported | `merge_order_composite_pk`, `query_semantics` |
| Read of a table written in the same transaction | refused when cold data is consulted | `txn_local_visibility` |
| `SELECT ... FOR UPDATE / SHARE` | refused when cold data can contribute | `unsupported_constructs` |
| `TABLESAMPLE` | refused when cold data can contribute | `unsupported_constructs` |
| `ctid` / system columns | refused when cold data can contribute | `unsupported_constructs` |
| `SERIALIZABLE` cold reads | refused by default; runs with a weaker guarantee if `koldstore.reject_serializable_cold_reads = off` | `unsupported_constructs` |
| `TRUNCATE`, `TRUNCATE ... CASCADE` | refused | `unsupported_constructs` |
| `INSERT` of an existing hot or cold key | refused | `cold_dml_guard` |
| `UPDATE`/`DELETE` by primary key reaching a cold-only row | refused | `cold_dml_guard` |
| `UPDATE`/`DELETE` by range, `NOT IN`, `OR`, non-key column, function, no `WHERE` | refused if it also matches cold-only rows | `cold_dml_scan_guard` |
| `UPDATE`/`DELETE` with a join, `USING` or sub-query (`IN`, `EXISTS`, `NOT IN`, self-join, another managed table as source) | refused if it also matches cold-only target rows | `cold_dml_scan_guard` |
| `UPDATE`/`DELETE` inside a data-modifying CTE (`WITH d AS (DELETE ... RETURNING ...)`) | refused if it also matches cold-only rows | `cold_dml_scan_guard` |
| A statement whose WHERE clause cannot be reproduced at all (a volatile function, `WHERE CURRENT OF`, a CTE used as a join source) | refused whenever the table has cold data | `cold_dml_scan_guard` |
| Plain `UPDATE`/`DELETE` on cold-only rows, single table (experimental, `koldstore.hydrate_on_write = on`) | supported by hydrating the rows first, up to `koldstore.max_hydrate_rows`; `READ COMMITTED` only; user insert triggers fire for the hydrated rows | `hydrate_on_write` |
| `MERGE` changing target rows through a multi-row source | refused when the table has cold data | `cold_dml_scan_guard` |
| Partitioned, inherited, foreign, temporary, unlogged tables, views | refused by `manage_table` | `manage_relation_kinds` |
| Adding a managed table to a hierarchy | refused | `manage_relation_kinds` |
| Renaming/moving-schema a managed table (or its schema) with cold data | refused | `unsupported_constructs` |
| `COPY <table> TO ...` (plain table form) on a managed table with cold data | refused | `unsupported_constructs` |
| Any valid identifier (spaces, quotes, dots, slashes, non-ASCII, leading digit, blanks, long names) in schema, table or column | supported | `odd_identifiers` |
| `DROP TABLE` / `DROP SCHEMA` of managed tables | supported; helper objects removed | `drop_cleanup_objects` |

The generated user-scope policy is application-context filtering, not an
authentication boundary. `koldstore.user_id` is a user-settable GUC, and the
generated policy is permissive, so another permissive policy can broaden the
combined RLS expression. Use a trusted connection layer and dedicated roles;
do not advertise this surface as database-enforced tenant isolation. Management
API privilege hardening is tracked in
[#120](https://github.com/kalamdb/koldstore/issues/120).

Planner cardinality and cost estimates are also a preview limitation, partially
addressed. `KoldMergeScan`'s own row estimate now adds the active cold row
total to the hot child's estimate for broad scans (the strategies other than
an exact primary-key equality lookup, which correctly stays a ~1-row point
estimate regardless of how much cold data exists) -- `EXPLAIN` and anything
costed directly on top of the scan (a `LIMIT`, a non-join aggregate, a `Sort`)
now see a realistic total instead of hot-only. Not yet addressed: the
addition is not reduced by `WHERE`-clause selectivity (koldstore has no
cold-side column statistics to estimate that with, only row totals), so a
highly selective filter over a broad-scan strategy still estimates as if it
might match everything cold; and **join-order sizing still only sees the hot
child**, because `RelOptInfo.rows` (what `calc_joinrel_size_estimate` reads)
is set earlier in planning, before `KoldMergeScan`'s own path is even built --
fixing that needs a `get_relation_info_hook` adjusting `rel->tuples` up front,
not attempted yet. Tracked in
[#124](https://github.com/kalamdb/koldstore/issues/124).

## Unique and Foreign Key Constraints

PostgreSQL `UNIQUE` and foreign-key constraints on managed tables are enforced on
the **hot heap only**. After flush, cold row values are preserved in Parquet,
but PostgreSQL removes the corresponding index and constraint entries from the
hot table.

Koldstore does **not** currently implement a global hot+cold constraint layer.
Manifest metadata and segment statistics are used for pruning and operator
accounting, not for proving that a unique value is absent from cold storage on
`INSERT` or `UPDATE`.

### Runtime behavior

| Constraint | Hot rows | Cold rows | Normal DML checks cold? |
|------------|----------|-----------|-------------------------|
| Primary key | Yes | Logical winner via merge | No; the native check is hot-only |
| `UNIQUE` (non-PK) | Yes | No | No |
| Foreign keys | Yes | No | No |

Example after flush:

```text
cold row:  id=1, email='a@x.com'
hot heap:  (no row with email='a@x.com')
INSERT INTO users (id=2, email='a@x.com')  → succeeds on hot path
merge scan: can return two rows with the same email
```

The same boundary applies to foreign keys: a child row can reference a parent
that exists only in cold storage, or miss a parent that was flushed, because
native FK checks inspect the hot heap only.

### `manage_table` validation

When `hot_row_limit` is set (flush enabled), `koldstore.manage_table` fails fast
before creating mirrors or migration jobs if the table has:

- non-primary-key `UNIQUE` constraints or unique indexes
- inbound or outbound foreign keys

The error lists the constraint names and columns involved.

Hot-only management (`hot_row_limit` omitted) keeps native PostgreSQL constraint
semantics because flushed rows are not expected to leave the hot heap.

### What manifest metadata cannot do today

Segment `row_count`, manifest generation state, and per-column min/max stats can
help pruning and `koldstore.table_status`, but they cannot reliably answer
“does this unique value already exist in cold storage?” on the insert path.
Min/max stats can only prove absence when a value falls outside every cold
segment range; values inside the observed range still require a future dedicated
cold constraint index or a Parquet read.

Until that layer exists, treat global uniqueness and referential integrity
across hot and cold as an explicit non-goal.

## Custom and Extension Indexes

PostgreSQL indexes do not move to cold storage. When a flush writes eligible
rows to cold files and removes those rows from the hot table, PostgreSQL removes
their index entries too.

This applies to built-in indexes, custom indexes, and extension-owned indexes.
Kalam does not automatically translate those indexes into object-storage
indexes over Parquet files.

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
- Vector columns require explicit Kalam type support before they can be flushed
  safely; v0.1 does not yet include pgvector's `vector` type in the supported
  type matrix.

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
| `UNIQUE` (non-PK) | Yes | No |
| Foreign keys | Yes | No |
| PostgreSQL custom indexes | Yes | No |
| pgvector index search | Yes | No |
| ParadeDB/BM25 index search | Yes | No, unless separately indexed |
| Vector column value | Yes | Planned with explicit type support |
| Exact vector scan | Yes | Possible for narrow scans, slower |
| Approximate vector search | Yes, through pgvector | Future Kalam sidecar index |
