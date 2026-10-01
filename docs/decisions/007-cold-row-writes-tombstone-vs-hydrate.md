# ADR-007: Writing cold-only rows: synchronous tombstone vs hydrate-on-write

## Status

Proposed. Option B has a working prototype behind `koldstore.hydrate_on_write` (default off, experimental); see "Prototype findings". Option A is not built. Today a plain `UPDATE`,
`DELETE` or `MERGE` that would have to change a cold-only row is *rejected*
(upstream [#122](https://github.com/kalamdb/koldstore/issues/122)); the explicit
`koldstore.update_row()` / `delete_row()` / `hydrate_pk()` functions are the only
way to change one.

## Date

2026-09-26

## Context

A managed table is a PostgreSQL heap (hot) plus immutable Parquet segments
(cold). A native `UPDATE`/`DELETE` plans against the heap only, so a cold-only
row is invisible to it and is silently skipped. The write guards now detect this
for every statement shape except a few that cannot be re-run faithfully (see
`docs/limitations.md`) and reject the statement. The open question is whether
plain SQL should instead *succeed* on cold rows, and how.

Facts about the current design that constrain any answer (all verified in code):

- **Reads mask cold rows through the mirror.** `merge_scan/pg/mirror.rs` loads
  mirror rows with `op = 3` (tombstones) and `koldstore_merge::MirrorOverlay`
  drops cold rows whose primary key is masked. A hot row for the same key
  shadows the cold copy through the scan's seen-keys set.
- **The mirror has exactly one writer.** The async WAL applier decodes committed
  changes in commit order and assigns each change a strictly increasing `seq`
  from `koldstore.async_mirror_state.seq_high_watermark`. Its four upsert
  builders (`plan_upsert_mirror_row`, `plan_async_mirror_batch_upsert`,
  `_update`, `_delete_existing` in `koldstore-wal-mirror/src/mirror/shared/write.rs`)
  are unconditional last-write-wins: `ON CONFLICT ... SET seq = EXCLUDED.seq,
  op = EXCLUDED.op` with no `WHERE EXCLUDED.seq > mirror.seq`. That is safe only
  because there is one writer applying in commit order.
- **Flush moves the mirror into cold.** A flush selects mirror rows by `seq`
  range, writes them (tombstones become `deleted` markers in a newer segment),
  and then prunes the mirror with `DELETE ... WHERE seq <= floor`
  (`koldstore-flush/src/cleanup.rs`). Newer segments win over older ones
  (`NewestFirstWinnerResolver`).
- **Uncommitted work is invisible to decoding** (upstream
  [#121](https://github.com/kalamdb/koldstore/issues/121)), which is why reads
  after a write in the same transaction are refused.
- There is no per-key catalog of cold presence by design
  (`cold_segment_index` comment): cold existence is found through PK min/max
  bounds and Parquet bloom filters.
- Compaction (#89) and cold GC (#100) do not exist yet, so a cold row that is
  "deleted" is only ever masked, never physically removed.

## Option A: synchronous tombstone

The `DELETE` (inside the user's transaction) writes an `op = 3` mirror row for
each cold-only key it matched. Cheap per row, no data movement.

Why it is harder than it looks:

1. **It is a second writer to a table whose safety argument assumes one.** Two
   properties must be added to the shared upsert SQL, on the exact path the live
   applier depends on:
   - a seq guard in all four builders (`... DO UPDATE SET ... WHERE
     EXCLUDED.seq > mirror.seq`), so an older write can never overwrite a newer
     one regardless of arrival order;
   - a seq for the synchronous tombstone that is *greater than anything the
     applier will later assign for an earlier-committed change to the same key*.
     The only floor available (`seq_high_watermark`) reflects applied WAL, not
     committed-but-undecoded WAL, so the allocation has to happen under the same
     lock the applier holds (`lock_slot` / the apply lock), which puts a new
     lock acquisition on every cold-touching `DELETE`.
2. **The flush window.** A tombstone whose `seq` falls at or below a flush's
   already-chosen prune floor is deleted from the mirror without ever reaching a
   segment, and the cold row it masked comes back. Allocation therefore must be
   above any in-progress flush's floor, again requiring the shared lock (flush
   holds it across its publish window).
3. **Same-key race, concrete.** `DELETE` sees `k` as cold-only and writes a
   tombstone; a concurrent transaction commits `hydrate_pk(k)` / `update_row(k)`
   between the statement snapshot and commit. Both orders must converge to a
   state a native PostgreSQL run could have produced. With a seq guard and a
   single allocator they do; without either, whichever write lands last wins.
4. **`UPDATE` still needs a hydrate.** An update produces a new version, which
   must live in the heap; the old cold copy is then masked (by the hot row).
   So option A alone does not remove the need for hydration; it only covers
   `DELETE`.
5. **Unbounded growth.** With no compaction or cold GC, tombstones flush into
   segments as delete markers and the physical cold rows they mask stay forever.
6. **Semantics that native SQL provides for free are lost:** `BEFORE/AFTER
   DELETE` triggers, `ON DELETE CASCADE`/foreign-key actions, row-level
   security, `RETURNING`, and `DELETE ... RETURNING` row images would all have
   to be re-implemented for rows the heap never held.

Verdict: correct only with a shared-write-path change (seq guards + a
lock-protected allocator) that is larger and riskier than every guard shipped so
far, and it still leaves `UPDATE` unsolved.

## Option B: hydrate on write (recommended)

Before the native statement scans, materialize the cold-only rows it would
match into the heap, using ordinary heap `INSERT`, then let the native statement
run unchanged.

- **No new mirror writer.** Hydration is a normal heap `INSERT`; capture, seq
  assignment, ordering and flush behave exactly as for any insert. The native
  `DELETE`/`UPDATE` then produces its own tombstone/new version through the
  existing WAL path. Everything the one-writer argument needs stays true.
- **Uniform for `UPDATE`, `DELETE` and `MERGE`,** including triggers, foreign
  keys, RLS and `RETURNING`, because after hydration the rows are real heap rows.
- **Machinery already exists.** The guard already computes, per statement, the
  set of cold-only rows the predicate matches (merged-view rows minus heap-only
  rows, for single-table WHERE shapes and, since the join/sub-query probe, for
  joins). `hydrate_pk` already does "locate in merged view, then `INSERT ... ON
  CONFLICT DO NOTHING` from `jsonb_populate_record`" as two statements because a
  single statement whose target is also its source does not get a
  `KoldMergeScan`.

Design sketch:

1. Trigger point: `ExecutorStart` for `UPDATE`/`DELETE` on a managed table with
   cold segments, when the guard would otherwise reject (same eligibility rules
   as the guard, including the skipped shapes).
2. Compute the cold-only match set with the existing probe (a `SELECT` returning
   full rows instead of a count), capped (`koldstore.max_hydrate_rows`, default
   e.g. 10 000; over the cap the statement is rejected with today's message).
3. Insert the rows with `jsonb_populate_recordset` + `ON CONFLICT DO NOTHING`
   under `guard::with_guard_suspended`.
4. `CommandCounterIncrement()` and advance the statement's snapshot command id
   (`estate->es_snapshot->curcid`) so the scan that follows sees the hydrated
   rows. This is the delicate step and needs its own review against PostgreSQL's
   snapshot rules; the alternative is to hydrate in a first statement via the
   planner hook rewriting to a `WITH` and is worse.
5. Record the write for the same-transaction read check (#121); reads of the
   table later in the transaction are refused exactly as after any other write.

Costs and honest limits:

- **Data movement and bloat.** Deleting N cold rows first inserts N heap rows,
  then deletes them: 2N heap tuples and 2N WAL changes. Hence the cap; bulk cold
  deletes remain an operator job (`update_row`/`delete_row` in batches, or a
  future purge-by-predicate).
- **Triggers fire twice in spirit.** User `AFTER INSERT` triggers run for the
  hydrated rows. `hydrate_pk` already behaves this way; the hydration insert
  should run with user triggers off where the caller may (`session_replication_role
  = replica` needs privileges), otherwise document it.
- **Concurrency.** Two transactions hydrating the same key converge through
  `ON CONFLICT DO NOTHING`; a concurrent native update of a row that only one
  side has hydrated is a normal write conflict.
- **Unchanged limits:** statements the probe cannot reproduce (volatile
  functions, CTEs, `CURRENT OF`, no primary key) stay rejected.

## Option C: both (later, only if needed)

Option B covers correctness. If very large cold deletes matter, add a *bulk
purge* as a separate, explicitly-named operation that runs under the apply lock
with the seq-guarded upsert, rather than making every plain `DELETE` a second
mirror writer. That keeps the risky change opt-in and isolated.

## Decision (proposed)

Adopt Option B; do not build Option A. The prerequisites for A (seq guards in
the shared upsert SQL and a lock-protected seq allocator) are only worth taking
if a measured workload cannot live with the B cap, and then as the isolated bulk
purge of Option C.

## Work breakdown for Option B

1. Refactor the probe to return matching rows (single-table `where_sql` and the
   join/sub-query probe) with a row cap.
2. `ExecutorStart` hook + hydration insert + snapshot advance; regression cases
   for `DELETE`/`UPDATE`/`MERGE`, triggers, foreign keys, RLS, `RETURNING`,
   prepared statements, savepoints, concurrent hydration of one key.
3. Interaction tests with flush running concurrently (hydration during the
   flush publish window) and with `koldstore.allow_same_txn_cold_reads`.
4. Documentation: limitations matrix row moves from "rejected" to "supported up
   to `max_hydrate_rows`".

## Consequences

- Plain SQL becomes usable on tiered tables without new shared-write-path risk.
- Cold storage grows only by delete/version markers already produced by the
  existing capture path; nothing new needs compaction to stay correct, though
  #89/#100 remain needed for space reclamation.
- The snapshot-advance step in `ExecutorStart` is the one piece of new
  low-level risk and should be prototyped and reviewed first.

## Prototype findings (2026-09-26)

Implemented in `hooks/hydrate_on_write.rs` (an `ExecutorStart` hook), covered by
`tests/sql/hydrate_on_write.sql`. Enabled per session with
`SET koldstore.hydrate_on_write = on`; `koldstore.max_hydrate_rows` (default
10 000) is the cap.

What worked:

- **The snapshot advance is sound in `READ COMMITTED`.** After the hydration
  `INSERT`, `CommandCounterIncrement()` plus setting the query descriptor
  snapshot's `curcid` to `GetCurrentCommandId(false)` *before*
  `standard_ExecutorStart` makes the native scan see the hydrated rows, while the
  statement's own new tuples (command id = `es_output_cid`) stay invisible to it,
  so there is no Halloween problem. Verified for `DELETE` and `UPDATE`, plain and
  prepared statements (custom and forced generic plans), `RETURNING`, cold and hot
  rows mixed in one statement, and statements matching nothing.
- **Rollback is clean** (the hydrated rows roll back with the statement), the cap
  rejects with nothing changed, and a flush still runs afterwards (the table job
  lock is held to statement end, then released).
- **Reads after the write in the same transaction are refused** by the existing
  #121 check, exactly as after any other write.

Known limits of the prototype (all fail closed or are documented, none silent):

- `REPEATABLE READ` / `SERIALIZABLE`: the transaction snapshot cannot be advanced,
  so the hook stays out and the write guards reject as before.
- Single-table statements whose `WHERE` clause `where_deparse` can reproduce, plus joins
  (`UPDATE ... FROM`, `DELETE ... USING`) and sub-queries (`IN`, `EXISTS`, ...) through the
  planner hook's probe (2026-09-26): the probe is a `SELECT` of the target's primary key over
  the statement's whole `FROM`/`WHERE`, and hydration looks for cold-only rows matching
  `(pk) IN (probe)` with the statement's own parameters bound (prepared statements work).
  Data-modifying CTEs and `MERGE` still fall through to the guards (which reject), as do
  statements the probe cannot reproduce (volatile functions, `CURRENT OF`).
- **User triggers (resolved 2026-09-26): the hydration `INSERT` no longer fires them.**
  The prototype originally fired user `AFTER INSERT` triggers for the hydrated row,
  so deleting one cold row logged `INSERT` then `DELETE`. Hydration now runs under
  `session_replication_role = replica` (scoped to a GUC nest level) with
  `koldstore.hydrating = on`, for `hydrate_on_write`, `hydrate_pk`, `update_row` and
  `delete_row`. Ordinary triggers see only the user's real operation; a trigger marked
  `ENABLE ALWAYS` still fires for hydration and can test the marker. The setting
  changes trigger firing only, so the insert is still logged, decoded and replicated
  (verified on the Spock mesh). Referential-integrity triggers are skipped too, which
  should stop a cold child whose parent is gone from blocking its own delete; that part
  is now tested (2026-09-26, scratch instance and the shared cluster): the FK is added
  after management, because `manage_table` refuses foreign keys on flush-enabled
  tables and `allow_fk_hot_only` is not reachable through its arguments. With the
  parent removed, `delete_row` and a hydrate-on-write `DELETE` of a cold child succeed,
  while an ordinary `INSERT` of a child still violates the FK. Two consequences to know:
  an `UPDATE` of a hydrated dangling child fails the FK recheck (PostgreSQL rechecks a
  row inserted by the current transaction) and changes nothing; and deleting a parent
  row is refused while the child has cold data, because the RI scan uses
  `FOR KEY SHARE`, which cold rows cannot support (#125, previously an obscure
  system-attribute error). `unmanage_table(..., true)`, which re-inserts every cold row,
  now runs as hydration too: it no longer fails on dangling children and no longer fires
  an `INSERT` trigger per row. `CHECK` and unique constraints still apply.
- **Concurrency on one cold key** (rewritten 2026-09-26 after stress testing; the original
  "serialize on the primary-key conflict" behavior was not enough, see below). Protocol per
  hydrating statement: a cheap first look with no fence and no locks (a stale mirror can only
  add candidates, never hide one; no candidates is the common case of updating hot rows);
  then, only with candidates, loop: fetch candidates through a fence, take per-key advisory
  locks (`pg_try_advisory_xact_lock`, polled with a deadline: a blocking lock error cannot be
  unwound from a `DirectFunctionCall` and aborted the server), fence and fetch again through a
  fresh snapshot until every candidate key is locked. What is left is the set of cold rows still
  alive once everyone who held those keys has finished. The explicit `update_row`/`delete_row`
  functions take the same per-key locks.
  The mirror fence is needed because a committed delete is not masked from cold reads until the
  async mirror applies its tombstone: without it a second session resurrected the row
  (`updates that succeeded AFTER the key was already deleted`, `keys deleted successfully more
  than once`). The fence **must not record or acknowledge applied progress**
  (`mirror::apply::fence_for_read`, like flush's prune fence): the first version reused
  `wait_for_async_mirror()`, whose applied-LSN record lives in the calling transaction, so a
  second fence in the same statement treated that uncommitted record as durable and advanced
  the replication slot past WAL whose mirror rows a later abort rolled back. The delete's
  tombstone was then lost forever and the row came back (regression case `h_child`, and
  `h_ab` now covers a failed statement after the fence). Knobs: `koldstore.hydrate_fence_mirror`
  (default on), `koldstore.hydrate_take_job_lock` (default off: taking the table job lock
  serialized every hydration behind flush, ~10 tps, and is unnecessary because hydrated rows are
  uncommitted and invisible to flush).
- Data movement: N cold rows cost N heap inserts plus N deletes/updates.

Side finding (fixed): `UPDATE`/`DELETE` inside a **data-modifying CTE** bypassed the
write guards entirely, because the top-level statement is a `SELECT`. The guard now
also inspects `ModifyTable` nodes in the plan's sub-plans and applies the generic
cold-match check (`cold_dml_scan_guard`, `cte_*` cases).

Test-harness lesson: `FROM ONLY t` is **not** a heap-only scan on a managed table
(the merge-scan hook still applies). Use `pageinspect`, or the guard's own
hot-only probe, to look at the heap.

Throughput and the slot lock (2026-09-27). Hydrating statements run ~10-25 tps at 8 clients, and
the cost is the mirror fence, not the cold reads (a hydrating statement is ~7 ms without it and ~20 ms
with it when idle). Findings: (1) the fence's slot lock (`pg_advisory_xact_lock`, held until commit)
serializes hydrating transactions, and because they queue on it the WAL applier and flush finalize
(which only try-lock) are starved; nothing acknowledges the slot, `restart_lsn` fell 21 MB behind and
each fence re-decoded it (80-95 ms per statement). (2) `koldstore.hydrate_slot_lock_poll_ms` (default 0
= queue) polls instead, which fixed the starvation (gap 29 kB, ~2x throughput in a pgbench run) but
loses deadlock detection. (3) The deadlocks seen on the shared cluster are a lock-order inversion, not
hydrator vs hydrator: flush finalize takes the slot lock and then `SHARE ROW EXCLUSIVE` on the table,
while a hydrating DELETE/UPDATE already holds `ROW EXCLUSIVE` on the table (taken at parse time, before
any hook) and then wants the slot lock. PostgreSQL resolves it as `40P01` in ~1 s and the hydrator was
the victim every time. With polling the cycle only ends at the poll deadline, so the statement fails with
a retryable `40001` (2 s: ~3% of statements while a flusher loops; 10 s: worse; poll-then-queue: no
throughput gain, so not kept). The inversion is fixed (2026-09-27) by taking the table lock before the slot lock in flush
finalize: the pre-lock catch-up now runs as committed passes (slot lock per pass, progress recorded like
the applier), the manifest object is written with no lock held, then `SHARE ROW EXCLUSIVE`, then the slot
lock with a 2 s bounded wait, then catalog activation, fence and prune. Catalog activation had to move
after the locks too: it writes rows hydrating writers update, and running it first made a second cycle
(130+ deadlocks per stress run). Result on the stress script: no deadlocks and no slot-lock timeouts, with
or without polling. The `after_manifest_publish` failpoint now sits right after the manifest write,
before any lock, because the prune-race tests run writers while the flush is parked there. Tried and dropped: an "already caught
up" skip (never fires under write load). Also fixed: idle background workers never absorbed a `ProcSignalBarrier`
(pgrx's `wait_latch` does not `CHECK_FOR_INTERRUPTS`), which made `DROP DATABASE ... WITH (FORCE)` hang
indefinitely; worker latch waits now go through `wait_latch_interruptible`.

Throughput, continued: `restart_lsn` (2026-09-27, later). "Logging running-transactions records more
often" was tried once above and dropped as "no gain," but that measurement predates the lock-order fix
and was confounded by starvation -- re-measured after, on its own: PostgreSQL only writes a
running-transactions WAL record on its own roughly every 15s (checkpoint-driven), and a logical slot's
`restart_lsn` can only advance up to the most recent one, so every read fence opens its decode cursor
at a `restart_lsn` that falls tens of MB behind `confirmed_flush_lsn` under continuous write load
(confirmed live: 13-28 MB gap at 8 pgbench clients) and re-decodes that whole stale range. The WAL
applier worker now calls `pg_sys::LogStandbySnapshot()` itself, rate-limited to once per 200ms
(`worker::wal::log_running_xacts_if_due`), closing the gap to single-digit KB when the applier can
actually run. Measured on a 20k-row table, hot_row_limit 10, random cold-key updates,
`koldstore.hydrate_on_write = on`:

| clients | before this fix | after (queue, default) | after + `hydrate_slot_lock_poll_ms=2000` |
|---|---|---|---|
| 1 | 18 tps, 17 MB gap | 44 tps, ~0 gap | -- |
| 8 | 10.5 tps, 24 MB gap | 13 tps, 24 MB gap | 32 tps, 1.3 MB gap |

Single-client throughput roughly doubles on its own. Eight concurrent hydrators barely move under the
default queueing mode: they (and the applier) all contend for the same slot lock, and with 8 client
statements re-queueing far more often than the applier's one drain pass per wake, the applier rarely
gets a turn to log the snapshot at all. Combined with polling (already available, previously the only
throughput lever), the two fixes compound to ~3x the original 8-client baseline. The two remaining
candidate directions from the original throughput TODO (batching one fence across concurrent hydrators;
having the applier itself do more of the acknowledging) are not done -- this closes the `restart_lsn`
half of the gap, not the per-statement decode-and-lock cost itself.

Throughput, continued: shared applied-through watermark (2026-09-27, later still). Picked up the two
remaining candidate directions together, since they turn out to be one mechanism: the WAL applier
already commits its own apply passes (`acknowledge_durable_checkpoint: true`), so by the time
`drain_wal_through_fixed_fence` returns from a pass, whatever LSN it reached is durably visible --
safe to publish to a small shared-memory watermark (`WalApplierRegistry::record_applied_through`,
monotonic, per database) that any *foreground* `fence_for_read` can check for free before doing any
work of its own. A read fence must never publish to it itself (its apply runs inside the caller's own,
possibly-aborting transaction -- the same class of bug the non-acknowledging fence exists to avoid),
so this only ever flows one way: applier writes, foreground reads. `fence_for_read` now checks the
watermark first and returns immediately, no lock and no decode, whenever it already covers the fence.

Result on the same benchmark: no clear win over the running-xacts fix alone, and no regression --
1-client and 8-client-default numbers were statistically indistinguishable from before, and 8-client
polling landed 17-39 tps across repeated runs, the same noisy band already seen without this change.
The reason is visible in the mechanism's own design: under 8 concurrent hydrators the applier is
competing for the *same* slot lock as all of them, so it rarely gets far enough ahead for the watermark
to already cover a fence by the time one is checked -- the exact contention this and the prior fix both
still have to fight. Kept anyway: it is strictly cheap when it doesn't help (one atomic load plus a
fence-LSN capture already needed either way), safe by construction, and should pay off in workloads
this narrow single-table pgbench benchmark doesn't create -- many concurrent hydrators against
different tables, or a system where *other* sessions' commits give the applier free room to run ahead
of any one hydrator's own request pace. Full correctness re-verified (suite x2, stress script x3 each
mode) with no violations; the throughput ceiling itself is not meaningfully moved by this change alone.

Stress test (`scripts/stress-hydrate-on-write.sh`, pgbench, 8 clients + concurrent flushers):
phase 1 hydrating updates against flush, phase 2 hydrating deletes, phase 3 mixed
update/delete on 400 shared keys, then checks that every violation counter is 0
(versions equal successful bumps, no deleted key visible, no key deleted twice, no update
after a delete, impossible commit orderings). Final protocol: 0 violations in 5 repeated mixed
runs and again after the fence fix (3 + 1 runs). Throughput is ~12 tps for phase 3 on the
scratch instance; the fence and per-key locks cost only when a statement has cold candidates.
The stress database accumulates many tiny segments, which inflates cold probe cost.

Throughput, tried and reverted: defer-to-applier fence batching (2026-09-30). The natural next
step after the shared watermark's "no clear win" result seemed to be making it actually pay off:
instead of a hydrator checking the watermark once and, if it isn't there yet, immediately
competing with the applier for the slot lock, have it wake the applier and poll the watermark
(cheap, no lock) for a short bound first, so any number of concurrently-waiting hydrators collapse
onto whichever fence the applier's next pass reaches -- batching the *requests*, not just the
*progress* (`koldstore.hydrate_wait_for_applier_ms`, default 20, wake via `worker::wal::wake`,
fall back to the unchanged direct-apply path on timeout or no live applier).

Measured on a clean 20k-row fixture (hot_row_limit 10, single-table random-key updates, the same
shape as the `restart_lsn` table above), lever off vs on, back to back on the same instance:

| clients | off (baseline) | on (20ms) |
|---|---|---|
| 1 | 10.3-11.1 tps | 7.4-8.3 tps |
| 8 | 8.9 tps | 6.5 tps |

A consistent regression, not noise -- reproduced at wait bounds of 2ms, 5ms, and 20ms, and in both
the 1-client (no contention at all, so no possible benefit, pure added latency) and 8-client cases
(where contention exists and a win should have been possible). Root cause: a direct timing check
(`UPDATE ...; SELECT koldstore.wait_for_async_mirror();`) showed a single apply pass costs 40-80ms
end to end even for a tiny amount of new WAL -- the cost is dominated by fixed per-invocation
overhead (slot peek setup, SPI, decode initialization), not by how much there is to decode. That
fixed cost is larger than any poll window worth paying, so the applier essentially never finishes
a fresh pass inside the wait; every hydrator pays the full poll cost and then still falls back to
doing the same direct-apply work it would have done anyway. The "redundant decode work" this
theory assumed concurrent hydrators duplicate turns out to be small next to this fixed cost, so
there is little to actually batch away. Reverted in full (guc.rs, mirror/apply.rs) rather than
shipped disabled-by-default -- a mechanism measured as a pure loss in every configuration tested
is not worth keeping as a dead opt-in knob. The slot-lock contention itself (not just restart_lsn
lag) remains unaddressed; a future attempt should look at reducing the applier's own fixed
per-pass cost first, since that is what makes deferring to it a bad trade today.

Correction to the above (2026-09-30, later): the "40-80ms fixed per-invocation overhead" diagnosis
was itself wrong, found by actually instrumenting `apply_bounded_locked` with per-phase timers
(`Instant::now()` + `pgrx::log!`) instead of inferring cost from `psql`-level wall-clock
measurements. With the measured calls kept to a single warm backend and the real persistent
applier paused (`kill -STOP`) so it could not race the same replication slot, every fixed-overhead
sub-step -- slot-exists check, `wait_until_slot_inactive`, the durable/seq reads,
`acknowledge_committed_apply`, `open_decode_cursor` -- logged sub-millisecond, and a full pass
applying one real row change totaled 2.8-4.6ms. There is no large per-call
`ReplicationSlotAcquire`/`CreateDecodingContext` tax to eliminate with a persistent-decoding-context
rewrite; that idea would not have helped. What the same log DID catch: two early passes on an
otherwise-idle database logged `fetch_decode_loop applied=0` at 122ms and 1.93s. Root cause:
PostgreSQL WAL is one physical stream shared by every database in the cluster, so a
koldstore-managed database that sits idle must still decode-and-filter whatever *unrelated* WAL
other databases produce once it finally gets a fence to apply -- and nothing before this pointed
the slot forward in the meantime, since `wal_due` only reflects this database's own generation.
The original 40-80ms `psql`-level numbers were this same effect at smaller scale (unrelated
regression/stress runs on the same shared pgrx test cluster in between calls), not a fixed
per-invocation cost -- so the earlier A/B against `hydrate_wait_for_applier_ms` above was measured
honestly but its *explanation* was wrong; the regression it found was real regardless (added
latency with nothing to batch), just not for the reason given.

Fixed: the persistent WAL applier's idle wait (`worker::wal::run_wal_applier`) now runs its normal
`drain_wal_through_fixed_fence` drain pass on every idle wake too, not only when this database's
own `wal_due`/`recovery_apply_due` are set -- bounding backlog-from-other-databases to one
watchdog interval's worth instead of an unbounded amount, at near-zero added cost when truly idle
(the same sub-millisecond-dominated pass measured above). `koldstore.async_apply_watchdog_interval_ms`
(existed already, default 30s, documented as a "safety watchdog") was wired into this wait for the
first time -- the worker had been using a hardcoded 30s constant instead, same default so no
behavior change from that part alone.

A first attempt at this fix was wrong and caught by the regression suite, not shipped: calling
`acknowledge_slot_lsn` (a raw `pg_replication_slot_advance`) directly on every idle wake, skipping
straight to the current WAL position without decoding it first. That silently discarded any
genuinely new committed change that happened to land in the skipped range -- the same "lost
tombstone" bug class fixed earlier this session for the read-fence path, reintroduced here for the
idle-wake path. Caught immediately: every `odd_identifiers.sql` sub-case and `hydrate_on_write.sql`
showed exactly one row surviving a `DELETE` that should have removed it, 100% reproducible. Fixed
by reusing `drain_wal_through_fixed_fence` itself (the existing, already-correct decode-then-apply
pass) instead of a shortcut -- it is safe to call unconditionally because it only ever advances
past WAL it has actually decoded and found irrelevant, never past WAL it has not looked at.
Verified: full 18-case SQL suite x2, full 95-test `#[pg_test]` suite, and the hydrate-on-write
correctness stress script (0 violations), all green after the fix.

Open items before this could be defaulted on: the double-delete row count (a statement racing a
delete reports the same count native PostgreSQL would only sometimes), data-modifying CTE
hydration, `REPEATABLE READ`, and a partitioned-table story.
