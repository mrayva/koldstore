# ADR-011: Global non-PK UNIQUE enforcement across hot and cold

Status: accepted (slice 1; foreign keys follow separately)

## Context

A flushed row leaves the hot heap and its unique-index entries go with it, so the native
`UNIQUE` check could not see cold duplicates. `manage_table` used to refuse such tables.

## Decision

Reuse the per-table write-guard trigger (ADR-007) instead of a new constraint index:

- the trigger becomes `BEFORE INSERT OR UPDATE` only when the table has enforceable
  non-PK unique indexes; the constraint list is baked into the generated function and
  re-planned by the DDL hook on `ALTER TABLE` / `CREATE UNIQUE INDEX`;
- `koldstore._cold_unique_guard_check` takes a per-value advisory transaction lock, then
  looks for a different row (by primary key) with equal values in the merged view, with the
  same mirror-lag fence as the primary-key guard;
- cold pruning relies on the existing segment min/max statistics plus a Parquet read of the
  surviving row groups. No new storage format or bloom/value-summary index.

## Consequences

- Cost: one merged-view lookup per constrained write; cheap only when the column correlates
  with segment ranges. A value-summary index (as ADR-009 does for primary keys) is the
  upgrade path if this proves too slow.
- Partial and expression unique indexes remain hot-only (warned about at attach time).
- `DROP INDEX` is hooked (the owning table is resolved before the drop and its guard re-planned after).
- Not covered here: foreign keys (outbound, then inbound), `MERGE`/`ON CONFLICT` upserts onto
  cold rows (still governed by ADR-007's refusal and `hydrate_pk`).

## Addendum: outbound foreign keys

`child -> parent` (parent unmanaged, `NO ACTION`/`RESTRICT`) uses the same machinery: a
`BEFORE DELETE OR UPDATE OF <key>` trigger on the parent calls
`koldstore._cold_fk_parent_guard_check`, which counts cold-only child rows (merged view minus
heap-only view) and raises `23503`. It is planned by `guard::plan_insert_guard` (so attach,
upgrade, DDL refresh and teardown share one path); the parent-side objects are found for
teardown by the child oid baked into the function body. The merge-scan planner hook plans the
RI trigger's `SELECT 1 FROM ONLY child x … FOR KEY SHARE OF x` probe hot-only instead of
refusing it (#125). A foreign key to a managed parent needs cold-row locking or a child-side
check against the parent's merged view and stays refused.

## Addendum: inbound foreign keys

`child -> managed` (child unmanaged, FK onto the managed table's primary key, `NO ACTION`/
`RESTRICT`) is handled by *hydrating on child write* rather than teaching RI to read cold
rows: a `BEFORE INSERT OR UPDATE OF <fk>` trigger on the child calls
`koldstore._cold_fk_child_hydrate`, which hydrates the parent key (job lock + key lock, same
path as `hydrate_pk`) if it has no hot row, so the native probe finds and locks a heap row.
Because that probe (`SELECT 1 FROM ONLY parent x … FOR KEY SHARE`) is now planned hot-only,
this hydration is what keeps cold parents visible. Cold deletes that bypass the heap
(`purge_where`) check the children explicitly; `delete_row`/`update_row` hydrate first, so
native RI covers them. Flush already prunes with `session_replication_role = replica`, so
hydrated parents are not pinned in the heap. Rejected: stripping row marks so the probe runs
unlocked on the merged view (fragile with the junk ctid column) and `NOT ENFORCED` FKs (too
invasive). Child-side guards are found for teardown by the parent oid in the function body and
re-planned from the DDL hook when a child gains a foreign key.
