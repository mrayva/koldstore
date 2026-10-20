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
* Existing segments keep `NULL` until rewritten; `koldstore.rebuild_segment_summaries()` fills them without
  rewriting anything: it reads each object once, verifies its checksum and row count against the catalog, and
  stores the bitmap flush would have written (byte-identical, covered by the `rebuild_segment_summaries` SQL
  case). Segments over `SUMMARY_MAX_ROWS` stay unsummarized by design.
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
  uuid tiebreak across page boundaries is exercised); verified red without the fix. `uuid` and `boolean` are accepted as the *order column* too (see below).
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
  `boolean` keys manage and flush too.
* **Fixed:** `numeric` primary keys (plain and with a modifier such as `numeric(12,2)`, plus `varchar(n)`) could not
  be flushed with an order column. Four separate causes, each found by the `numeric_pk` SQL case against an
  unmanaged copy: the ordered-flush keyset compared `numeric > text` (a `numeric` bind type that casts in SQL, with a
  parseable first-page placeholder); the mirror table DDL quoted `numeric(12,2)` as a type *name*; a `numeric` datum
  could not be read as text (it is read as a number and kept as its exact text); and the ordered merge was offered
  for any key type although only types with a Sort Key V1 encoding can be merged in order, so a numeric `ORDER BY`
  came back unsorted (it now falls back to a normal sort). Key identity for `numeric` is the JSON number `jsonb`
  prints, with whole numbers normalized (`125`, `125.0` and `125.00` are one key), so a delete's tombstone masks the
  cold copy.
* **Open (limits, not bugs):** `boolean`, `numeric`, `text`, `real` and `double precision` keys have no value summary,
  so point lookups on them rely on min/max bounds (and Parquet statistics where they exist) alone. A float cannot be
  the order column, nor can `text` or `numeric` (see "Bounds-only key types" below).
* **Fixed (2026-10-20): numeric digits through hydration.** Hydrated rows travel as `jsonb` read into `serde_json`,
  which parsed a JSON number as `f64`: `62.50` came back as `62.5` and a 30-digit value was rounded. `serde_json`'s
  `arbitrary_precision` feature keeps the digits verbatim (`numeric_fidelity` SQL case).
* **Fixed (2026-10-20): `real` / `double precision` primary keys.** They could not be managed (`double precision`
  was written into the change-log mirror DDL as a quoted identifier; it is now `float8`) and an ordered flush failed
  ("keyset does not support primary-key type") because the flush keyset had no float bind. A float key is compared
  between heap, mirror and cold rows through its `to_jsonb` text, which `CellValue::to_json` now reproduces exactly:
  the shortest round-trip digits as a plain decimal (`real` 0.1 is `0.1`, not the widened `0.10000000149011612`),
  `-0` as `0`, and `"NaN"` / `"Infinity"` / `"-Infinity"` as strings. The native hot cursor reads float datums by
  value (`float_primary_keys` SQL case: multi-segment ordered flush with ties on the order column, NaN and the
  infinities, tiny/huge magnitudes, hydrate-on-write updates and deletes, re-flush).
* **Fixed:** `uuid` and `boolean` were rejected as the order column (`migration_order_by`) by the migration
  validation although both have a Sort Key V1 encoding and everything downstream (mirror `order_key`, ordered flush,
  the cold order index, the ordered merge) already handled them. Both are accepted now: a time-ordered (v7) uuid is a
  natural order column, and a uuid primary key can be its own order column; a boolean sorts false before true, then by
  key. Covered by the `uuid_bool_order_column` SQL case (flush, ordering, range predicates, hydrate-on-write DML,
  re-flush against an unmanaged copy).
* **Extended (2026-10-15): composite keys and temporal keys.** A summary now covers any primary key made of
  `smallint`/`integer`/`bigint`/`uuid`/`date`/`timestamp`/`timestamptz` columns, one or several. A row's key is its key
  columns' Sort Key V1 bytes back to back in key order (every supported encoding is fixed-width, so this is
  unambiguous, and for one column it is exactly the column's own encoding, so existing summaries stay valid). The
  summary lives on the *first* key column's `cold_segment_index` row. The planner probes it only when every key column
  has an equality predicate (`composite_key_probe`); for any other shape it must not, because the stored bitmap hashes the
  whole key. This is a correctness rule, not an optimisation: testing a partial key (`WHERE a = 2` on key `(a, b)`) or one
  column of a composite key against it dropped segments that held the row, and was caught by the existing
  `cold_dml_scan_guard` and `merge_order_composite_pk` cases. The single-value path therefore applies only to a column that
  is the entire key. Covered by the `composite_pk_summary` SQL case (a key declared in non-column order, half-matching and
  partial lookups, infinity and pre-epoch dates, a `timestamptz` key under a non-UTC session zone, rebuild byte-identical
  to flush). **Downgrade caution:** a library older than this change treats any summary on a column as that column's
  own, so running it against segments written after this change loses rows on composite-key lookups. Before reverting
  to an older build, clear them: `UPDATE koldstore.cold_segment_index SET value_summary = NULL;` (they can be rebuilt with
  `rebuild_segment_summaries()` afterwards).

## Scale follow-ups (2026-10-20)

A 3M-row S3 run (`docs/operations/s3-scale-test.md`) exposed four scale limits, all fixed:

* **INSERT guard.** The existence probe joined the managed table against a `jsonb_populate_record` CTE, hiding the key
  from the planner: every INSERT read the whole cold tier. It is now `t.col = ($1 ->> 'col')::type`, which folds to a
  constant when bound; `date` / `timestamp` / `timestamptz` text input is `STABLE` and does not fold, so those keys are
  parsed by PostgreSQL in a first query and bound as typed parameters.
* **Value lists.** `IN`, `= ANY(array)` and an `OR` chain of equalities on one key column prune per value
  (`SegmentPrunePredicate::any_of`: the list plus its `[min, max]`, so range-only consumers stay correct); the candidate
  segments are the union of the per-value lookups for a single-column key up to 64 values.
* **Disjoint segments.** When the single-column key's per-segment ranges are pairwise disjoint (checked once per manifest
  generation) every key has one cold version: cold identities are not retained (`NewestFirstWinnerResolver::
  without_cold_identity_retention`), and any column with segment statistics may prune (see
  `docs/architecture/scanning-table.md`).
* **Bounds-only key types.** `text` (raw UTF-8, the same bytes as Parquet string statistics; truncated long-value
  statistics are valid bounds) and `numeric` (a one-way decimal encoding, see `koldstore_sortkey::encode_numeric_key`)
  are `SortKeyType` variants that `SortKeyType::from_type_oid` does *not* return: that function is the order-capable
  set that order-column validation, ordered merges and mirror order keys use, and byte order is not collation order for
  text. `from_type_oid_for_bounds` includes them for pruning only; a text predicate prunes on equality only (never a
  range) and only under a deterministic collation (`manage_table` already refuses a nondeterministic key collation).
  Numeric bounds cannot come from Parquet statistics (numeric is stored as text), so flush computes them
  (`NumericBoundsState`). The candidate query gained a third arm that keeps every active segment that has no index
  row for the column, so segments flushed before a type had bounds are never excluded.

Three existing bugs surfaced and were fixed along the way:

* a stale cold copy returned after a hydrate-on-write update when the query's filter excluded the new value (above);
* a narrow projection (`SELECT id ...`) through the hot-only / delegated path returned a tuple of the wrong width
  (`unexpected field count in "D" message`, `unsupported format code`): the check that a native child's layout matches
  the scan compared descriptor pointers and, for a scan child without a result slot, read a null slot;
* hydrate-on-write aliased the table as `t` in `to_jsonb(t)`, which resolves to a *column* named `t` instead of the
  whole row, so a managed table with a column called `t` could not hydrate (fixed afterwards with `to_jsonb(t.*)`).

## Composite keys (2026-10-20, slice 1)

The disjoint-segment bypass above now covers composite primary keys. Per-column bounds say nothing about which tuples a
segment holds, so two segments *may share a key* only if their `[min, max]` intervals intersect on **every** key column
(a box test, `koldstore_flush::compaction::SegmentRange::may_share_keys`); a single column reduces to the interval
overlap used before. `keys_disjoint` sweeps the segments by their first column's minimum and tests only pairs whose
first-column intervals intersect, giving up (treating the layout as overlapping) past two million pairs. A leading column
that splits segments well keeps it near-linear; segments that all span the leading column (a tenant) are still separated by
another column (an ascending id) and so count as disjoint. Every key column needs a bounds encoding and an index row in
every segment, otherwise the answer is "not disjoint". Covered by the `composite_key_disjoint_scan` SQL case.

Compaction (`koldstore.compact_table`) still refuses composite keys; its planner already groups segments by the same box
overlap, so the remaining work is ordering and deduplicating on the full key (slice 3). Slice 2 is pruning by a key prefix.

Slice 2 (pruning): a prefix or range of a composite key already pruned by the key columns' bounds, and a full-key equality
by the summary. A list did not: `(tenant, id) IN (...)`, an OR of full-key equalities, or `tenant IN (...)` fell back to the
list's `[min, max]`. Now an OR whose disjuncts are equalities or conjunctions of equalities (which is also how PostgreSQL plans
a row-value `IN`) gives every key column that every disjunct pins a value list (a per-column superset: it does not tie
different columns' values together), and the per-value candidate union is no longer limited to single-column keys. A lookup
of one column of a composite key never probes the value summary (it hashes the whole key); the column's row-group bounds and
the other predicates still narrow each value. Covered by `composite_key_value_lists`.
