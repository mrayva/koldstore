# S3 at scale: results of a 3M-row run (2026-10-20)

A single-node run of the deployed S3-enabled build against a local SeaweedFS (`weed server -s3`, path-style
HTTP on loopback, data on the local disk). It measures the cost of tiering and reading a table whose rows are
all cold, what happens when storage goes away mid-flush, and whether cleanup leaves anything behind. It is
not a throughput benchmark of S3 itself: loopback latency is near zero, so request-count effects are
understated and per-request cost is the thing to carry over, not the absolute times.

**Setup.** PostgreSQL 18, `shared_buffers = 512MB`, `koldstore.flush_execution = queue`,
`max_parallel_flush_jobs = 2`. Table `big`: `id bigint PK, tenant int, ts timestamptz, amount numeric(12,2),
payload text` (128 hex characters), 3,000,000 rows (636 MB in the heap). `hot_row_limit = 1000`,
`max_rows_per_file = 100000` (a flush batch is capped at `max_rows_per_flush = 10000`, so segments hold about
10,000 rows), `migration_order_by = 'id'`. An unmanaged copy of every table was the oracle.

## Results

| Measurement | Result |
|---|---|
| Backfill + mirror catch-up of 3M existing rows | 17 s |
| Flush of 3M rows to S3 (4 jobs of ~640k rows) | 232 s total, ~13k rows/s; 300 segments, 257 MB in the bucket |
| Executor private memory while flushing | under 22 MB (RSS looks like 650 MB only because it counts the touched shared buffers) |
| 4 tables x 300k rows flushed concurrently | 39 s, ~31k rows/s aggregate, tens of MB private memory in total |
| Point lookup by PK (cold) | 4-19 ms |
| `id BETWEEN` a 10k-row range | 49 ms |
| UPDATE / DELETE of 100 cold rows (hydrate-on-write) | 37 ms / 25 ms |
| INSERT of 100 new keys / one duplicate cold key | 24 ms / 16 ms (refused, as designed) |
| Full read of all 3M cold rows with the key cap disabled | 12-55 s depending on the aggregate; 258 MB private memory (~86 bytes per key) |
| Result after update + delete + insert vs the oracle | identical row count and an identical content hash over 3M rows |
| Storage killed mid-flush, down 25 s, then restarted | flush failed cleanly (job back to `pending`, attempts incremented), nothing published, no cold read of a partial segment; the next retry after restart completed; final content identical to the oracle, 0 orphaned objects (`recover_segments` dry run), `validate_cold_storage(deep)` ok over 40 segments |
| `DROP TABLE` of five tables | all their objects removed from the bucket (351 MB to 257 MB, the rest is the surviving tables) |

While storage is down, any query that has to read a cold segment fails with the S3 error after the client's
retries; hot data and writes are unaffected.

## Defects found and what became of them

All five limits below were fixed afterwards (see ADR-009, "Scale follow-ups", and
`docs/architecture/scanning-table.md`); the numbers in the table above are from before the fixes.

1. **INSERT into a table with many cold rows scanned the entire cold tier.** The write guard asks "does this key
   already exist, hot or cold?" once per inserted row. It joined the managed table against a `jsonb_populate_record`
   CTE, which hides the key from the planner, so no cold segment could be pruned: a single-row INSERT read all 300
   segments (6 s, then an error at the default `koldstore.max_merge_seen_keys`; 100 rows ran for over ten minutes
   before I cancelled it). The lookup is now a constant equality, so it prunes: 9-20 ms per statement, 100 rows in
   24 ms (SQL case `cold_insert_guard_pruning`).
2. **A multi-value key lookup was a full cold scan.** `id IN (a, b)`, `= ANY(array[...])` and `id = a OR id = b` all
   took ~12 s; each listed value now selects its own segments and the union is read (`pk_value_list_pruning`).
3. **Large scans hit the identity cap or needed unbounded memory.** At the default 1,000,000, a plain `count(*)`
   over 3M cold rows errored; with the cap off it needed ~86 bytes per key (258 MB for 3M, ~8.6 GB for 100M). When
   the segments' key ranges are disjoint, cold keys are no longer retained at all (`cold_scan_disjoint_segments`).
4. **text and numeric primary keys could not prune cold segments** (no Sort Key encoding), so every lookup and INSERT
   guard on them read all segments. Both now have segment bounds (`text_primary_key_pruning`,
   `numeric_primary_key_pruning`).
5. **Non-key predicates read everything** (`WHERE tenant = 7`, a `ts` range), because pruning by a column that can
   change could resurrect an older version. When every key has one cold version (disjoint segments), any column with
   segment statistics -- a regular index or `pruning_columns` -- prunes; a `ts` range over 3M rows then opens 2 of 15
   segments in the small case (`non_key_segment_pruning`).

Three further bugs found while verifying were fixed too: a stale cold copy reappearing after a hydrate-on-write
update when the filter excluded the new value (`cold_stale_version_masking`); a narrow projection through the
hot-only path returning a tuple of the wrong width; and an INSERT guard / hydrate quirk noted in ADR-009.

## Reproducing

```sh
weed server -dir=$DATA -s3 -s3.config=s3.json -s3.port=18333 -s3.port.iceberg=0 -s3.port.lance=0 \
  -master.port=19333 -volume.port=18080 -filer.port=18888 -volume.max=100 -ip=127.0.0.1
echo "s3.bucket.create -name koldstore-scale" | weed shell -master=127.0.0.1:19333 -filer=127.0.0.1:18888
```

```sql
SELECT koldstore.register_storage('s3w', 's3', 's3://koldstore-scale/ks/',
  '{"access_key_id":"admin","secret_access_key":"adminsecret"}',
  '{"endpoint":"http://127.0.0.1:18333","region":"us-east-1"}');
```

Then load a table, `manage_table(..., migration_order_by => 'id', max_rows_per_file => 100000)`, call
`flush_table(t, true)` until `table_status(t)->>'hot_rows'` is at most the hot limit, and compare with an
unmanaged copy. Measure executor memory with `RssAnon` in `/proc/<pid>/status` (not RSS, which includes shared
buffers). Do not use `pgrep -f` with a pattern that also appears in your own command line.
