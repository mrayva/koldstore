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

## Separate problems found while testing

* **Fixed:** a `smallint` primary key's equality lookup (`WHERE id = 7`, also `IN (7)`) returned no rows on the
  *Exact Primary Key* path while `IN` lists of several values, `BETWEEN` and `id + 0 = 7` worked. Cause: the
  row-level recheck of the PK probe (`koldstore-parquet::reader::decode::arrow_cell_matches_pk_values`) knew
  `Int64`, `Int32` and string columns but not `Int16`, so it rejected every row of the (correctly selected) row
  group. It now also matches 16-bit and boolean columns, and an Arrow type it cannot compare keeps the row instead
  of dropping it, because the planner re-applies the equality qual to every returned row anyway. Covered by a
  Parquet-level test, matcher unit tests and the `segment_pk_summary` SQL case.
* **Fixed:** a populated table with a `uuid` primary key and a `migration_order_by` column could not be flushed
  (`invalid uuid keyset value: invalid length: found 0`). The ordered flush pages through the mirror with a keyset
  cursor; its first-page parameters are placeholders that the `$2::boolean OR ...` guard never compares, but they
  still have to parse as the column type, and the uuid placeholder was an empty string. It is now the nil UUID.
  Covered by the `uuid_pk_ordered_flush` SQL case (2,500 rows, 50 distinct order keys, three segments, so the
  uuid tiebreak across page boundaries is exercised); verified red without the fix. `uuid` itself is still rejected
  as the *order column* (a separate validation).
* **Fixed:** `date` and `timestamp` (without time zone) columns could not be managed at all (they were not in the
  type matrix), and `timestamptz` keys never worked as primary keys. All three, as ordinary columns and as primary
  keys (with a separate `migration_order_by` column or with the key itself as the order column), now manage, flush,
  hydrate and delete correctly; covered by the `temporal_types` SQL case against an unmanaged control copy.
  Three things had to line up:
  * **Epochs.** PostgreSQL counts from 2000-01-01, Arrow/Parquet from 1970-01-01. `CellValue` holds PostgreSQL-epoch
    values; the Parquet codec shifts at the boundary. `infinity` / `-infinity` are the extreme integers and are never
    shifted.
  * **Key identity.** A primary key is compared as JSON across the heap, the change-log mirror (`to_jsonb`) and cold
    rows. Temporal cells therefore render as PostgreSQL's own `to_jsonb` text (`2020-01-05`,
    `2019-12-25T15:00:00.5`, `... BC`, `infinity`), and `timestamptz` in UTC; the heap and mirror reads that feed
    the merge run with `TimeZone = UTC` so the session zone cannot change a key. (Numbers would sort correctly but
    never matched the mirror's tombstones, so deleted cold rows reappeared.)
  * **Point lookups.** `WHERE id = <date literal>` also feeds Parquet statistics and bloom filters, which hold the
    Unix-epoch value; the probe is shifted accordingly (it used to return no rows for a cold key).
  `boolean` keys manage and flush too, but `boolean` is still rejected as the *order column* itself.
* **Fixed:** `numeric` primary keys (plain and with a modifier such as `numeric(12,2)`, plus `varchar(n)`) could not
  be flushed with an order column. Four separate causes, each found by the `numeric_pk` SQL case against an
  unmanaged copy: the ordered-flush keyset compared `numeric > text` (a `numeric` bind type that casts in SQL, with a
  parseable first-page placeholder); the mirror table DDL quoted `numeric(12,2)` as a type *name*; a `numeric` datum
  could not be read as text (it is read as a number and kept as its exact text); and the ordered merge was offered
  for any key type although only types with a Sort Key V1 encoding can be merged in order, so a numeric `ORDER BY`
  came back unsorted (it now falls back to a normal sort). Key identity for `numeric` is the JSON number `jsonb`
  prints, with whole numbers normalized (`125`, `125.0` and `125.00` are one key), so a delete's tombstone masks the
  cold copy.
* **Open (limits, not bugs):** an unconstrained `numeric` column does not keep its display scale when a cold row is
  hydrated (`62.50` comes back as `62.5`; the value is equal, and `numeric(p,s)` columns restore the scale) because
  the row travels as JSON. `date`, `timestamp`, `timestamptz`, `boolean` and `numeric` keys have no value summary, so
  point lookups on them rely on min/max bounds and Parquet statistics alone. `float4`/`float8` keys are still
  rejected for ordered flush.
