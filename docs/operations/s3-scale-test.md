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

## Defects found

1. **INSERT into a table with many cold rows scanned the entire cold tier (fixed).** The write guard asks "does
   this key already exist, hot or cold?" once per inserted row. It joined the managed table against a
   `jsonb_populate_record` CTE, which hides the key from the planner, so no cold segment could be pruned: a
   single-row INSERT read all 300 segments (6 s, then an error at the default
   `koldstore.max_merge_seen_keys`; 100 rows ran for over ten minutes before I cancelled it). The lookup is now a
   constant equality (`t.id = ($1 ->> 'id')::bigint`; date/timestamp/timestamptz keys are parsed by PostgreSQL in a
   first query and bound as typed parameters, because their text input is `STABLE` and is not folded). Result: 9-20 ms
   per statement. SQL case `cold_insert_guard_pruning`.
2. **A multi-value key lookup is a full cold scan.** `id IN (a, b)`, `id = ANY(array[...])` and `id = a OR id = b`
   all took ~12 s (every segment read); a single value or `IN (a)` takes ~10 ms. Batched key lookups are the common
   ORM shape, so this is the next thing to fix: prune each segment against the union of the listed values.
3. **Large scans hit the identity cap or need unbounded memory.** A merged read keeps every key it has seen so an
   older version in another segment can be masked. At the default `koldstore.max_merge_seen_keys = 1000000`, a plain
   `count(*)` over 3M cold rows, a filter on a non-key column, or an `IN` list errors out (the message says how to
   raise it); with the cap off it needs ~86 bytes per key (258 MB for 3M, ~8.6 GB for 100M). When the cold segments'
   key ranges do not overlap each other (an append-style table), the keys need not be retained at all.
4. **text and numeric primary keys cannot prune cold segments.** The catalog keeps segment bounds and value
   summaries only for integer, uuid, date and timestamp keys; a text or numeric key has no Sort Key encoding, and
   composite keys prune only when every part does. For those key types every lookup, and every INSERT guard, reads
   all cold segments, so they do not scale the way integer keys do. (Correctness is unaffected; see
   `cold_insert_guard_pruning`.)
5. **Non-key predicates read everything.** `WHERE tenant = 7` or a `ts` range scanned all 3M cold rows
   (~12-14 s here) because only key columns carry segment bounds by default; `pruning_columns` adds bounds for others.
   Cold decode ran at about 20 MB/s of object data per scan.

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
