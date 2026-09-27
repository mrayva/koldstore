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

Stress test (`scripts/stress-hydrate-on-write.sh`, pgbench, 8 clients + concurrent flushers):
phase 1 hydrating updates against flush, phase 2 hydrating deletes, phase 3 mixed
update/delete on 400 shared keys, then checks that every violation counter is 0
(versions equal successful bumps, no deleted key visible, no key deleted twice, no update
after a delete, impossible commit orderings). Final protocol: 0 violations in 5 repeated mixed
runs and again after the fence fix (3 + 1 runs). Throughput is ~12 tps for phase 3 on the
scratch instance; the fence and per-key locks cost only when a statement has cold candidates.
The stress database accumulates many tiny segments, which inflates cold probe cost.

Open items before this could be defaulted on: the double-delete row count (a statement racing a
delete reports the same count native PostgreSQL would only sometimes), data-modifying CTE
hydration, `REPEATABLE READ`, and a partitioned-table story.
