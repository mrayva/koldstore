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
- `DROP INDEX` of a unique index leaves the guard over-enforcing until it is re-planned.
- Not covered here: foreign keys (outbound, then inbound), `MERGE`/`ON CONFLICT` upserts onto
  cold rows (still governed by ADR-007's refusal and `hydrate_pk`).
