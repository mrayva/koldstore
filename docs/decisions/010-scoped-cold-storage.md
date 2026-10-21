# ADR-010: Scoped cold storage (user-scoped tables)

Status: proposed (2026-10-20). Related: [ADR-004](004-segment-publication-protocol.md) (publication),
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
