# Spock mesh with a single tiering leader (design spike)

Status: spike done; stages 0 to 6 are implemented and verified (see "Progress"). Date: 2026-10-29.

## Progress

- Stage 0 done: `migration_order_by` import fix, clear unregistered-storage error naming the `"check" => false` follower recipe.
- Stage 1 done: `koldstore.sync_table_metadata()`, verified on two clusters over shared storage through new, compacted-away and unchanged segments; schema-version mismatch refused.
- Stage 2 done: `koldstore.publish_leader_catalog()` (atomic write, generation fencing floor) and `koldstore.sync_from_leader()` work. `koldstore.set_table_follower()` marks a follower (manual, job and background flush, compaction and reaping are refused or skipped), and a background worker (`koldstore.catalog_sync_interval_seconds` / `catalog_sync_databases`) syncs every follower from the leader's published catalog. `koldstore.set_table_leader()` marks the leader: the same worker publishes its catalog whenever the manifest generation changes (from the worker, so followers only see committed state; latency is one interval). 
- Stage 3 done: peer prune. `koldstore.sync_from_leader()` (so the sync worker) deletes, in the same transaction as the adoption, each follower hot row that exactly equals a row in a newly adopted segment; `koldstore.prune_follower_hot()` does the same for every active segment. The comparison is the primary key plus whole-row JSON equality read back from the segment, so no segment row hash is needed and a row edited after the leader's flush stays hot. Verified on two clusters: 300 hot rows became 1 (the edited one), and 201 became 2 after a worker sync. Limits: user-scoped tables are refused (deletes: see stage 6).
- Stage 4 done: leader lease with epoch fencing (`acquire_leader_lease`, `renew_leader_lease`, `release_leader_lease`, `leader_lease_status`). Storage has only create-if-absent and overwrite, so the lease is a series of epoch objects under `_leader_lease/<schema>/<table>/`; claiming epoch N+1 is a create-if-absent put, the holder renews by overwriting its own object, and the highest epoch wins. Once a node has acquired a lease, flush, enqueue, compaction, reaping and publishing require it valid, published catalogs carry the epoch, and followers refuse an older epoch. The sync worker renews for leader tables. Verified on two clusters: second claimer refused, takeover after ttl, the old leader fenced on all five operations and on renew, release then immediate re-acquire, worker keeps the lease alive, a stale-epoch catalog is refused. Not done: automatic election (who calls acquire is left to an external coordinator such as the write-router or etcd), and expiry relies on roughly synchronized clocks. Follow-up (queued jobs): the lease is also checked when an executor claims a queued flush job and just before a running flush writes its manifest; a node that lost the lease cancels its pending flush jobs (`scripts/test-lease-queued-job.sh`).
- Stage 5 done: end-to-end run on a scratch 3-node Spock mesh (`scripts/test-mesh-single-leader.sh`, now a pass/fail script that exits non-zero on any failed invariant, vanilla PG 18.6 with Spock's core patches, shared filesystem cold storage, `capture_replicated_changes`, `hydrate_on_write`, reconcile worker at 4 s). n1 leader, n2/n3 followers. Results:
  - Concurrent inserts and updates from all three nodes converge (count + md5 equal on all nodes). Updates of rows that are cold on the leader are skipped by Spock on arrival (`did not find row to be updated`), and the reconciler replays them within one interval (2 replayed, then 3 for an update + delete of rows cold on followers).
  - Leader flush, then follower sync and peer prune: segment counts equal on all three nodes, results equal.
  - Leader kill (immediate stop): a follower's `acquire_leader_lease` is refused until the ttl passes, then claims epoch 2; the sequence is `set_table_follower(false)`, `acquire_leader_lease`, `set_table_leader(true)`. The new leader flushed with n1 down, and n3 followed.
  - The restarted old leader is fenced on flush and renew ("shared lease is epoch 2"). It must be demoted (`set_table_leader(false)`, `set_table_follower(true)`) to catch up; left as a leader it keeps a stale catalog and fails reads once the new leader reaps.
  - Compaction and reap on the new leader: followers keep reading. With an offline follower and reap grace 0 the follower's read fails closed (`segment ... not found`) until its next sync, then all nodes converge, so the reap grace must exceed the slowest follower's lag.
  - `validate_cold_storage` is clean on every node at the end.
  - Operational requirements the run confirmed: without the reconciler, one skipped UPDATE discards the whole Spock transaction (`transdiscard`), so the other inserts in it are lost on the leader; run the reconcile worker (or the reconciler) on every node of a mesh.
  - Failure tests (`scripts/test-lease-partition.sh`): with storage unreachable from the leader, new sessions fail closed (lease status and flush error out, local writes keep working). With the whole leader cluster frozen (SIGSTOP) while a follower takes over and then resumed, the old leader is refused on renew, flush and publish, the new leader's published epoch is never overwritten, and after demotion the old leader catches up on the new leader's segments and rows. Caveat: the freeze is a process-level pause, not a network partition; and the lease check immediately before the manifest write is not atomic with the write, so a pause inside that window can still publish one stale manifest object (it is not activated in any catalog by a fenced node, and followers refuse older-epoch catalogs).
  - Not covered: a coordinator that automates election, real clock skew, a true network partition (the freeze above only approximates it), and S3 storage.

- Stage 6 done: tombstone sync (`scripts/test-tombstone-sync.sh`, two clusters).
  - Flushed deletes already reached followers through the delete markers in adopted segments; what was missing was (a) a follower hot copy of a deleted key survived the adoption, and (b) the window between the leader's DELETE and its flush.
  - (a) Peer prune now also handles delete markers: a hot row is dropped when the key's newest cold change (leader seq) is a marker and the hot row equals an older cold version of the key. An edited hot row, or a key the leader re-inserted afterwards, stays.
  - (b) The published leader catalog carries the leader's unflushed delete markers (`tombstones`, capped at 10 000, `tombstones_truncated` beyond that; the sync worker republishes when their count or seq range changes). A follower writes them into its own mirror as synthetic markers (`op = 3`, `seq = -1`), so the merge scan masks the cold row within one worker tick instead of waiting for Spock plus the reconciler. Each sync replaces the set; a key with any mirror row of its own is left alone; `set_table_follower(false)` removes them; flush never reads them because a follower does not flush.
  - Found on the way: a leader with any hot DML since its last flush (`manifest.sync_state = 'pending_write'`) could not publish at all (export refused "manifest is not in sync"); that state leaves the cold manifest valid and no longer blocks export.
  - Limit: a key re-inserted on the leader between two worker ticks is masked on the follower until the next tick.

- Hydrate-on-write check (scratch 3-node mesh, row cold on all nodes, UPDATE on follower n2 with `hydrate_on_write`): the update replicates, the reconciler replays it on the leader, and each node ends with its own mirror row (`op=2`, node-local seq; n1 and n2 differ). That is harmless: only the leader flushes, and its hot copy carries the leader's seq. After the leader's flush and the followers' peer prune the hot copies and mirror rows are gone on all nodes, count + md5 are equal everywhere, and compaction keeps them equal. A follower seq would matter only if that follower were promoted, and then its hot rows are simply flushed as its own. No code change needed. (`ONLY t` is a hybrid read on a managed table, so heap checks use pageinspect.)

## Goal

Today every node in a multi-master mesh tiers independently (`docs/multi-master.md`): each node
flushes its own hot rows to its own cold tier, and a row that is hot on one node and cold on a
peer diverges on plain UPDATE/DELETE (Finding 3, mitigated eventually by
`reconcile_spock_conflicts()`). The goal here is one **leader** that alone flushes and compacts,
with every other node reading the leader's cold tier, so there is one cold history and the
hot/cold split stops being per-node.

## What the spike measured

Two scratch PG18 clusters (A leader, B follower) over one shared filesystem storage
(`/tmp/ks-mm-share`), script `mm_adopt.sh` in the session scratchpad.

| Question | Result |
|---|---|
| Can a peer adopt the leader's segments from an export? | **Yes.** `export_table_metadata` on A (about 8 KB for 3 segments) plus `import_table_metadata` on B adopted 3 segments. B's snapshot (count + md5 over all rows) equalled A's, point and range reads worked. |
| Is the export deterministic and free of node-local ids? | Yes: no OIDs, no credentials. `min_seq`/`max_seq` are still leader-local snowflake ids. |
| Can a follower follow incrementally (A flushes again)? | **No.** `import_table_metadata` refuses an already managed table. A new incremental "sync from leader" function is required. |
| Does adopt reclaim the follower's hot copies? | **No.** B keeps all of its heap rows. A prune step is missing. |

Import problems found on the way (only the first is fixed in this branch):

1. `import_table_metadata` ignored the exported `migration_order_by`, so tables without an identity
   key could not be adopted ("requires an auto-increment primary key or explicit order column").
2. An unregistered storage name surfaced as `SpiTupleTable positioned before the start or after the
   end` instead of the intended message. The cause on the follower was that `register_storage`
   refuses a non-empty directory (the leader's files), so a follower must register with
   `"check" => false`. Worth a clearer error and a documented follower recipe.
3. `max_rows_per_file` below the floor needs `koldstore.min_max_rows_per_file` on the follower too.

## Architecture options

**A. Pull-based adopt (recommended first step).** The leader publishes its catalog metadata next to
the segments (a `_catalog/<table>.json` object written at each manifest generation, CAS on
generation). Followers poll it and apply a diff: insert new `cold_segments` (+ index rows), mark
removed ones superseded, bump the local manifest. No change to Spock, works with filesystem/S3
storage, reuses the existing export format. Cost: the follower needs an incremental
`sync_from_leader(table)` and a worker to call it.

**B. Replicate the catalog through Spock.** Add `koldstore.cold_segments` and friends to a
replication set. Appealing but wrong: rows carry node-local OIDs (`table_oid`), the manifest is a
generation-CAS counter, and conflicting writers on the same catalog row are exactly what a single
leader is meant to avoid. Rejected.

**C. Leader-only flush without pull.** Followers never tier and keep everything hot. Simplest, but
defeats the purpose on followers (their storage grows unbounded).

## Remaining hard parts

1. **Peer prune.** After adopting a segment a follower must delete the hot rows it covers, but only
   those whose version is no newer than the cold copy. Seqs are node-local (snowflake, time-ordered,
   roughly comparable across nodes), so a plain `seq <= segment.max_seq` is unsafe: a row edited on
   the follower after the leader's flush has a newer local seq and must stay. Proposal: prune by
   `(pk, row hash)` match against the segment, falling back to leaving the row hot when the hash
   differs. This needs a per-row hash in the segment or a read-back compare.
2. **Finding 3 does not disappear.** UPDATE of a row the leader just flushed and a follower has not
   yet pruned still races. The window shrinks (only between a leader flush and a follower sync) but
   the reconciler stays.
3. **Leader election and fencing.** Two leaders flushing the same rows corrupt the cold history.
   Needs a lease with fencing (generation CAS on the shared catalog object already gives a floor),
   ideally backed by the quorum layer in Spock PR #604 (etcd/pgraft) or the ColdFront bakery
   protocol. The write-router prototype (`lease_guard.py`) solves the same problem and can be reused.
4. **Compaction and reaping.** Only the leader compacts; followers must not read a reaped segment.
   Reuse the standby rule: reap grace must exceed the slowest follower's sync lag.
5. **Hydrate-on-write.** Analysed and found not to need a fix (see "Hydrate-on-write check" in Progress): a follower's hydrated hot copy is an ordinary edit that Spock replicates; the leader holds its own copy with its own seq, so no seq has to cross nodes.

## Staged plan and cost

| Stage | Work | Estimate |
|---|---|---|
| 0 | `migration_order_by` import fix (done), clearer unregistered-storage error, follower registration recipe | 0.5 day |
| 1 | `koldstore.sync_from_leader(table)`: incremental adopt (diff by `segment_id`, superseded handling, generation CAS) plus a unit/e2e test on two clusters | 3 days |
| 2 | Leader publishes `_catalog` object per generation; follower worker polls | 2 days |
| 3 | Peer prune with row-hash match and tests for the "edited after flush" case | 4 to 5 days |
| 4 | Lease and fencing (start with a table-local lease row on shared storage, then plug in pgraft/etcd) | 3 to 5 days |
| 5 | Multi-master e2e on the 3-node Spock mesh: concurrent writes, leader kill, follower lag, reap races (done) | 4 days |

Roughly 3 to 4 weeks of focused work end to end. Stages 0 to 2 are a safe, independently useful
milestone (read-only followers over shared cold storage) and could ship before the prune and
election work.

## Recommendation

Do stages 0 to 2 only if the goal is "followers read the leader's cold tier". Do not start stage 3
before deciding the per-row version comparison, since it determines whether segments need a row
hash column. Until then the existing per-node tiering plus the reconciler remains the supported
multi-master mode.
