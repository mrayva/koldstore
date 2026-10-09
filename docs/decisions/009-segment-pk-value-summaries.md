# ADR-009: Segment primary-key value summaries

Status: accepted (2026-10-09). Related: [ADR-007](007-cold-row-writes-tombstone-vs-hydrate.md) (where the
cost was measured), [ADR-004](004-segment-publication-protocol.md).

## Problem

A cold point lookup (`WHERE pk = const`, strategy *Exact Primary Key*) asks the catalog for segments whose
`[min_value, max_value]` covers the key, then opens every one of them. That is fine while segments hold
disjoint key ranges. It is not once keys are scattered: every hydrate-on-write update followed by a flush
writes the touched rows into a new small segment whose keys span the whole range, so its bounds can never
rule it out. Measured (ADR-007): 2.0 ms per lookup with 4 segments, 16.9 ms with 14 (11 opened), because each
candidate costs a footer read (~1.5 ms). Parquet Bloom filters do not help: they prune row groups *inside* an
opened file, after the cost of opening it has been paid.

## Decision

`koldstore.cold_segment_index` gets a nullable `value_summary bytea`: a one-hash membership bitmap over the
column's values in that segment. The point-lookup candidate query tests one bit per candidate segment and
returns only the segments that may contain the key.

* **Which columns:** single-column primary keys of type `smallint`, `integer`, `bigint` or `uuid` (the
  types whose Arrow layout is mapped exactly like the min/max statistics). Composite, text, date, timestamp
  and boolean keys get `NULL`.
* **`NULL` never prunes.** Segments written before this change, oversized segments, unsupported types and any
  segment whose summary could not be completed are simply treated as candidates, exactly as before. There is no
  backfill and none is required for correctness.
* **Input is the Sort Key V1 encoding** of the value, the same bytes that back `min_value`/`max_value`, so the
  writer and the planner agree by construction. The writer hashes every value of the key column as batches are
  encoded (`koldstore_flush::encode::PkSummaryState`); the planner hashes the probe bytes it already derives.
* **Frozen format** (`koldstore-sortkey::summary`): `summary_hash` = FNV-1a 64 + splitmix64 finalizer, shifted
  to a non-negative 63-bit value so PostgreSQL carries it as `bigint`. Bit index = `hash % (8 * length)`, stored
  so that `get_bit(bytea, n)` reads it (bit `n` is `1 << (n % 8)` of byte `n / 8`). The hash has golden-value
  unit tests: changing it would turn every stored summary into a source of false negatives and needs a new codec
  version, not an edit.
* **Size:** 16 bits per row, rounded to a byte, minimum 64 bits, capped at 8 KiB. That is a ~6% false-positive
  rate for the default 1,000-row segments and ~7% at the cap; segments over 32,768 rows (load above 50%) get no
  summary. The catalog therefore stays `O(segments)` with a hard 8 KiB per segment bound, which is why this is a
  bitmap and not the exact per-key hints the segment writer deliberately does not store.
* **Safety net:** the number of summarized values must equal the segment's row count, otherwise the summary is
  dropped (a missed row would be a false negative). Any error while summarizing disables it for that segment; it
  never fails the flush.
* **Query:** `plan_cold_segment_candidates_point` adds
  `value_summary IS NULL OR get_bit(value_summary, $8 % (octet_length(value_summary) * 8)) = 1` to both index
  arms. It is used only when `lower == upper` on a supported column. The reported lookup shape stays
  `bounded_range`; the saving appears in the existing *Segments Pruned by Catalog Index* counter.

## Consequences

* Point lookups on a table with scattered segments open ~1-2 files instead of all overlapping ones. Measured on
  the 14-segment case: 16.9 ms -> 3.1 ms (59 -> 322 lookups/s), 12 of 14 segments pruned by the catalog. A fresh
  4-segment table is unchanged (2.0 ms).
* The ADR-007 stress with a looping inline flusher, which had lost 35-50% of hydrator throughput to this effect,
  now loses ~0% at a flush every 0.5 s (39.2 tps, equal to no flusher) and ~19% at a flush every 50 ms
  (31.7 tps against 16.5), even with more flushes completing.
* Existing segments keep `NULL` until rewritten; a future `rebuild`/compaction pass can fill them.
* Range and `IN`-list predicates are unchanged (they use min/max only); `IN` lists and the join/probe shapes are a
  natural extension (probe each element) but are not done here.
* Storage grows by up to 8 KiB per segment per summarized column, against segment files that are far larger.

## Known, separate problems found while testing (not caused by this change)

* A `smallint` primary key's equality lookup (`WHERE id = 7`) returns no rows on the *Exact Primary Key* path,
  while `IN`, `BETWEEN` and `id + 0 = 7` return them.
* A populated table with a `uuid` primary key and a `migration_order_by` column cannot be flushed
  (`invalid uuid keyset value: invalid length: found 0`); `uuid` itself is rejected as the order column.
