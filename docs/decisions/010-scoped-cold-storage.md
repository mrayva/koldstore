# ADR-010: Scoped cold storage (user-scoped tables)

Status: accepted; slice 1 built (2026-10-21). Related: [ADR-004](004-segment-publication-protocol.md) (publication),
[ADR-009](009-segment-pk-value-summaries.md) (pruning and disjointness).

## Context

A `table_type => 'user'` table has a `scope_column` (for example `user_id`). Hot rows are isolated by a generated RLS
policy keyed on `koldstore.user_id`; cold segments are not: flush writes table-wide segments (`scope_key = ''`) in
`{namespace}/{table}/{folder}/`, each mixing every scope's rows. A scoped read can only prune them by the scope column's
per-segment min/max, which is useless when scopes interleave in time (the usual case): every segment is a candidate, so a
single user's query pays a footer read per segment of the whole table.

## Assumptions

* Clean sheet: no existing scoped data has to be migrated (old `scope_key = ''` segments still read correctly, see below,
  but nothing rewrites them).
* Limited multitenancy: tens to a few hundred scopes per table, not millions. A scope usually has enough rows to fill a
  segment over time.
* A scope value never changes for a row (it backs the RLS policy), and a primary key is unique table-wide, so a key lives in
  exactly one scope.
* Priority is read latency and scan cost; write amplification and object count are secondary but bounded.

## Decision

### Layout

Segments never mix scopes. Object path: `{namespace}/{table}/{scopeId}/{folder:03}/segment-….parquet` (the registered
`scoped_path_tmpl`, `scopeId` percent-encoded like every path segment). The catalog already keys segments, index rows,
order-index rows and manifest rows by `(table_oid, scope_key)`; `scope_key` becomes the scope's canonical text (`''` stays
"table-wide" for unscoped tables and for legacy/fallback segments). Unscoped tables are untouched.

* **Flush** orders the selected mirror rows by `(scope, <existing order>)` and cuts a segment whenever the scope changes (the
  streaming encoder gains a "segment key" column next to `max_rows_per_file`). The keyset cursor gains the scope as its
  leading element; text scopes are compared `COLLATE "C"` so the SQL ordering and the cursor agree. Row selection stays a
  `seq` prefix, so the hot/mirror prune is unchanged. A scope with few rows produces a small segment; compaction packs them.
* **Publication.** One flush still activates all its segments in one statement. The compare-and-set generation stays a
  *table-level* counter (the `scope_key = ''` manifest row), which is also what caches key on; per-scope
  `{scopeId}/manifest.json` objects are derived and rewritten only for scopes the flush touched.
* **Bounded cardinality.** If one flush touches more than `koldstore.max_scopes_per_flush` scopes (default 256) it writes
  table-wide segments sorted by `(scope, order)` instead (`scope_key = ''`): bounded object count, still clustered by scope so
  the scope column's bounds prune well, just not separable by prefix. Readers already must accept both kinds.

### Reads

The scan resolves a scope when it can: the session's `koldstore.user_id` for an RLS-forced role, or a constant equality / `IN`
on the scope column. Candidate segments are then `scope_key IN (<scopes>, '')`; with no resolvable scope (superuser, no filter)
all segments, exactly as today. Because a key lives in one scope, winner resolution is unchanged, and the retention-free scan
generalises: disjointness is checked **within each `scope_key` group** (cross-scope pairs cannot share a key); any `''`
segment in the mix is conservatively treated as overlapping everything. So a per-user full scan of an append-mostly table
retains no cold keys and opens only that user's segments.

### Compaction

Units never cross scopes: the planner (ADR-009 box/tuple tests, small-run packing) runs once per `scope_key` group, rewrites
stay in the scope's folder and keep `scope_key`, and retirement/reaping are unchanged. `''` fallback segments form their own
group. Automatic compaction iterates scopes with the same per-tick row budget, so a busy tenant cannot starve the others.
This is where tiny per-scope flush segments are merged, so the object count converges to roughly (rows / `max_rows_per_file`)
+ (number of scopes).

### Other effects

* `DROP TABLE` / `unmanage_table(drop_cold)` already delete by listing the table prefix, which covers scope folders.
* Per-scope export, backup and deletion become prefix operations; a `koldstore.drop_scope()` can follow but is not required.
* `recover_segments`, `validate_cold_storage`, `backup_manifest` iterate `(table, scope)` instead of `(table)`.
* Hot-side RLS and its caveats (application-context filtering, not an authentication boundary) are unchanged.

## Slices

1. Flush writes scoped segments and per-scope manifests (reads still consider every segment, so correctness comes first).
2. Scope resolution in the scan, scope-aware candidate query, per-scope disjointness.
3. Per-scope compaction and automatic compaction; `max_scopes_per_flush` fallback; docs and limits.

## Expected effect

A scoped lookup or range scan moves from "every segment of the table is a candidate" to "this scope's segments", the same
proportion as the number of scopes; a full scan of one scope no longer retains cold keys.

## Open questions

* Default and behaviour of `koldstore.max_scopes_per_flush` (fallback vs error).
* Whether a scope below some row count should skip its own segment and ride in the next flush instead (needs per-scope
  hot/prune cuts; deliberately not proposed).
* NULL scope values (the policy already excludes them; `manage_table` should require `NOT NULL`).

## Slice 1 as built (2026-10-21)

What shipped, and where it differs from the sketch above:

* **`cold_segments.scope_id text`** (NULL = table-wide) records a segment's scope. The existing `scope_key` column is *not*
  used yet: every read-side catalog query filters `scope_key = ''`, so keeping scoped segments in that group means every
  existing scan, count, manifest and recovery query still sees them with no change. Slice 2 introduces the scope-aware
  candidate query on `scope_id` (and may then retire `scope_key`).
* **Flush** (`StreamEncodeInput::scope_column_index`): a user-scoped table's pass is read in full, stably grouped by scope
  (`group_rows_by_scope`), and the encoder closes a segment when the scope changes, so no segment mixes scopes. The pass is held
  in memory (bounded by `max_rows_per_flush`; a pass over 2,000,000 rows is refused). The object path is
  `{scopeId}/{folder}/segment-….parquet` relative to the table prefix (`flush_segment_relative_path`), `scopeId` percent-encoded.
  The manifest shard-folder parser accepts the extra leading component.
* **Delete markers stay table-wide.** A marker has the primary key only; the row is gone, so its scope is unknowable at flush
  time (the mirror stores no scope column). Markers (and rows with a NULL or empty scope) go to `scope_id IS NULL` segments in the
  table-wide folder. They must mask older versions in any scoped group, so readers will always include the table-wide group.
* **No per-scope manifest objects yet.** One table manifest lists every segment, scoped ones with their scope-prefixed relative
  paths. Per-scope manifests are only worth having once something reads them per scope.
* **Compaction is already per scope** (slice 3's core): units never cross scopes, output keeps the unit's scope and folder, and the
  table-wide group is skipped once scoped segments exist (a marker there may be masking an older version in a scoped group; rewriting
  it needs a "keep markers" mode). Markers therefore accumulate in small table-wide segments for now.
* Reads are unchanged: all segments are candidates. Tests: `scoped_cold_layout` (SQL), encoder and manifest-path unit tests.

Still open for slices 2 and 3: scope resolution in the scan and per-scope disjointness; the `max_scopes_per_flush` fallback; a
"keep delete markers" compaction mode so the table-wide group can be merged too.

## Slice 2 as built (2026-10-21)

A user-scoped scan now reads only its scope's segments plus the table-wide ones.

* **Resolving the scope** (executor time, in `plan_cold_segments`): the scope column's value list from the scan's own prune
  predicates (equality, `IN`, `= ANY`, an OR of equalities -- all already extracted for bounds pruning), intersected, plus the
  session's `koldstore.user_id` when the quals contain `scope_column = current_setting('koldstore.user_id'[, true])`, which is
  what the generated RLS policy contributes (`qual::session_scope_equality`). The GUC is read when the scan starts, never at plan
  time, so a cached generic plan reused under another `user_id` selects that user's segments. It is the *qual* that restricts the
  scan, not the role: a superuser (RLS bypassed) with `user_id` set is not restricted and reads everything, and a query whose
  quals do not pin the scope reads everything as before. A restriction to scopes the table does not have leaves only the
  table-wide segments.
* **Filtering**: segment hints carry `scope_id` (catalog candidate query, scan-context JSON); segments of other scopes are
  dropped after the catalog lookup, before any Parquet footer is read, and counted in "Segments Pruned by Catalog Index".
* **Per-scope disjointness**: `cold_pk_ranges_disjoint` takes the scope set (cache key gains its hash) and the key-box query
  lists only those scopes' segments plus table-wide ones, so a scope whose rows were never rewritten scans without retaining
  cold keys even when another scope's segments overlap.
* **Catalog driver**: when a query pins the scope and another indexed column, the other column drives the catalog lookup (the
  scope is already handled by the filter).
* **Known limitation**: delete markers are table-wide and their key range can overlap any scope's segments, so while a table has
  markers every scope's scan treats the layout as possibly overlapping (correct, but retention returns). Fixing it needs the
  scope at delete time: store it in the mirror (a captured column carried forward from the row's last insert/update) so markers
  can be written to their scope's folder. Slice 3 candidate, with the `max_scopes_per_flush` fallback and a keep-markers
  compaction mode.

Tests: `scoped_cold_scan` (segments opened per predicate shape, RLS session, prepared statement reuse, superuser, edits and
deletes against an unmanaged control, per-scope retention-free scans).
