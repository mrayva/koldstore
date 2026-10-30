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
  - Failure tests (`scripts/test-lease-partition.sh`): with storage unreachable from the leader, new sessions fail closed (lease status and flush error out, local writes keep working). With the whole leader cluster frozen (SIGSTOP) while a follower takes over and then resumed, the old leader is refused on renew, flush and publish, the new leader's published epoch is never overwritten, and after demotion the old leader catches up on the new leader's segments and rows. Caveat: the freeze is a process-level pause, not a network partition; and the lease check immediately before the manifest write is not atomic with the write (see below).
  - Non-atomic lease check (`scripts/test-catalog-fencing.sh`): storage only offers create-if-absent and overwrite, so "still the leader" and "write the catalog" cannot be one step. Instead the write is fenced on the reader side. Claiming epoch N+1 seals the SHA-256 of the then-current leader catalog into the new lease object (`sealed_sha`). A follower accepts a catalog stamped with epoch N only if its hash equals that seal, so anything a deposed leader writes after the claim is ignored. The new leader's worker compares the catalog etag after each publish and republishes if it changed behind its back. The leader also re-checks the lease before and after the publish write, and about once a second inside the reap and history-purge delete loops. The window between the claimant's catch-up check and its claim write is closed (`scripts/test-claim-window.sh`, section below). Stale segment writes only leave orphans, and a stale leader's local manifest changes stay local.
  - Best election candidate (`scripts/test-election-priority.sh`): `koldstore.election_priority` (0 opts out). Followers publish `_leader_candidates/<schema>/<table>/<node>.json` (priority, generation, epoch, heartbeat) on each worker tick. At election time a node ranks itself among the live candidates that hold the leader's latest generation and epoch (liveness = heartbeat seen changing on the local clock within 3 ttl), by priority, then generation, then cluster id. Rank 0 claims at once; rank N waits N staggers (ttl/5, at least 2 s) after the lease became electable, so a dead preferred node costs a stagger, not an outage. Ordering only: the lease claim still arbitrates, so a wrong ranking can never produce two leaders. Not covered: a candidate that has not published yet is invisible, so right after startup a lower-priority node can win; a promoted node leaves its candidate record behind until it goes stale; no load or region awareness, only the static priority.
  - S3: the same gate passes unchanged on an S3-compatible store (`MESH_STORAGE=s3 scripts/test-mesh-single-leader.sh`, SeaweedFS `weed server -s3`, package built with the `s3` feature, 24 checks). The bucket ends up with both lease epochs (create-if-absent claims worked), the three follower acknowledgements, the published catalog and the data objects.
  - True partition (`scripts/test-lease-netns-partition.sh`, needs sudo, socat, S3 store): the leader runs in a network namespace and every packet to the S3 endpoint is dropped (not refused) while it keeps running. Lease status and flush fail after the 30 s object-store timeout (bounded, no hang), local writes keep working, B takes epoch 2 after the ttl, flushes and publishes. After healing, A is fenced (renew, flush and publish all refuse with the epoch mismatch), B's published catalog is untouched, and after demotion A converges with B and validates.
  - Automatic election (`koldstore.auto_elect`, `koldstore.leader_lease_ttl_seconds`; `scripts/test-auto-election.sh`, three clusters over a shared store): the catalog sync worker of a follower claims the next lease epoch once the shared lease has expired, provided the node holds the leader's last published generation and epoch (a lagging node refuses). Several followers racing is safe because the claim is create-if-absent; the loser rolls its role change back. No lease object means no election (a leader that never enforced a lease cannot be taken over). The leader's worker renews each tick, takes the next epoch if its own lease expired unclaimed, and demotes the table to follower when another node holds the lease, so a restarted old leader fences and demotes itself and catches up. Verified: nothing elects while the lease is alive (2 x ttl), kill the leader and exactly one follower becomes epoch 2 and flushes, the other follows, the restarted old leader demotes and converges, kill the new leader and the survivor takes epoch 3.
  - Clock skew (`scripts/test-clock-skew.sh`, the election test with node wall clocks at +0 s, -30 s and +30 s via an LD_PRELOAD shim, ttl 8 s): the first version compared the holder's heartbeat with the reader's wall clock, and the test reproduced both failures: the +30 s node took over a live leader within seconds, and a slow-clock node delayed the election after a kill to 40 s instead of about 8 s. Takeover no longer compares clocks: a node watches the lease object and requires the holder's heartbeat to stay unchanged for a full ttl on its own monotonic clock (released leases are free at once). The holder still judges its own lease by its own clock. Now nothing elects over a live leader, and elections take 8 s. Cost: a node needs one ttl of observation after (re)start before it can take over, and a manual `acquire_leader_lease` on a lease that looks expired waits (up to ttl + 10 s) for that observation instead of failing. Only clock rate drift matters now.
  - Not covered: choosing the best candidate (any caught-up follower may win).

- Stage 6 done: tombstone sync (`scripts/test-tombstone-sync.sh`, two clusters).
  - Flushed deletes already reached followers through the delete markers in adopted segments; what was missing was (a) a follower hot copy of a deleted key survived the adoption, and (b) the window between the leader's DELETE and its flush.
  - (a) Peer prune now also handles delete markers: a hot row is dropped when the key's newest cold change (leader seq) is a marker and the hot row equals an older cold version of the key. An edited hot row, or a key the leader re-inserted afterwards, stays.
  - (b) The published leader catalog carries the leader's unflushed delete markers (`tombstones`, capped at 10 000, `tombstones_truncated` beyond that; the sync worker republishes when their count or seq range changes). A follower writes them into its own mirror as synthetic markers (`op = 3`, `seq = -1`), so the merge scan masks the cold row within one worker tick instead of waiting for Spock plus the reconciler. Each sync replaces the set; a key with any mirror row of its own is left alone; `set_table_follower(false)` removes them; flush never reads them because a follower does not flush.
  - Found on the way: a leader with any hot DML since its last flush (`manifest.sync_state = 'pending_write'`) could not publish at all (export refused "manifest is not in sync"); that state leaves the cold manifest valid and no longer blocks export.
  - Limits (`scripts/test-tombstone-limits.sh`): the first `koldstore.published_tombstone_limit` markers (default 10 000, oldest first) ride inline in the catalog. The rest go to content-addressed sidecar objects `_leader_tombstones/<schema>/<table>/<sha256>.json` (limit-sized chunks), listed in the catalog as `tombstone_chunks` with their checksums. A follower fetches and verifies them in the same sync, and a mismatch or missing chunk fails the whole sync (nothing is applied, the next tick retries). The leader deletes sidecars the new catalog no longer lists, after the publish. So a burst of deletes is masked on followers within one tick no matter its size. Only past limit x 100 markers (1 000 000 by default) is the remainder cut: a warning is logged, `tombstones_truncated` is reported by `publish_leader_catalog` and `sync_from_leader`, and the rest is hidden on followers only after the leader's next flush. A key the follower writes itself is never re-masked: its own mirror row replaces the synthetic marker, and later syncs leave it alone. A key re-inserted on the leader is not replicated to a non-Spock follower before the leader flushes; it stays hidden there until then (`koldstore.reinserted_keys` records it at insert time and the leader publishes it as a marker; it used to resurface the old, deleted cold version, `scripts/test-reinsert-mask.sh`). Not covered: a delete and re-insert in one transaction, or before the applier has put the delete into the mirror.

- Hydrate-on-write check (scratch 3-node mesh, row cold on all nodes, UPDATE on follower n2 with `hydrate_on_write`): the update replicates, the reconciler replays it on the leader, and each node ends with its own mirror row (`op=2`, node-local seq; n1 and n2 differ). That is harmless: only the leader flushes, and its hot copy carries the leader's seq. After the leader's flush and the followers' peer prune the hot copies and mirror rows are gone on all nodes, count + md5 are equal everywhere, and compaction keeps them equal. A follower seq would matter only if that follower were promoted, and then its hot rows are simply flushed as its own. No code change needed. (`ONLY t` is a hybrid read on a managed table, so heap checks use pageinspect.)

- Reap guard (`scripts/test-reap-follower-guard.sh`, two clusters; also the offline-follower step of the mesh gate): the reap grace was a number the operator had to get right. Now each follower writes an acknowledgement (`_follower_acks/<schema>/<table>/<node>.json`: the leader generation it holds and when) after every sync, rewritten at most every 30 s while unchanged. Before reaping, the leader lists them and defers (manual call: error naming the lagging nodes; maintenance tick: skips the table) while an acknowledgement younger than `koldstore.follower_ack_max_age_seconds` (default 3600, 0 = off) is below the leader's current generation. Verified: refused while a follower is behind, proceeds once it syncs, proceeds when the acknowledgement ages out (the silent follower then fails closed and heals on its next sync), and proceeds with the check off. Edges closed: `set_table_follower(true)` records an acknowledgement at the generation the node holds, so a follower is waited for before its first sync; the acknowledgement is written after the follower's transaction commits (a rolled-back sync never claims a generation, verified); `set_table_follower(false)` (promotion, decommissioning) removes it, so a node that left does not hold reaping back for up to the max age. Limits that remain: the comparison uses the leader's current generation, so flushing faster than followers sync keeps deferring reaping while any live follower lags (segments do not record the generation that retired them; fixing that needs a catalog column); the age compares the follower's wall clock to the leader's, so skew matters only on the scale of `koldstore.follower_ack_max_age_seconds` (an hour by default); a follower that was never registered and never synced (set up before this change and not yet synced) is still unknown; the acknowledgement write after commit is best effort and logged on failure, and the next sync rewrites it.

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

## Read-your-writes (implemented)

Every `publish_leader_catalog` stamps the catalog with a `publication` number, one higher than the
catalog it replaces, so it keeps increasing across leaders. The call returns it. A client that wrote
on the leader passes that number to `koldstore.wait_for_publication(table, n)` on a follower, which
polls the shared catalog, syncs as soon as it reaches `n`, and records `seen_publication` in
`koldstore.schemas.options`. A follower that has already applied `n` returns without a sync; a
publication that never arrives is an error after `timeout_ms`. This covers cold-tier visibility
(flushed rows, delete markers, compaction); a write still in the leader's hot tier is not on the
follower until it flushes. Test: `scripts/test-read-your-writes.sh` (12 checks).

## Reap no longer starves under fast flushing (implemented)

The reap guard used to compare every live follower with the table's *current* generation, so a
leader that flushed faster than followers synced always had someone "behind" and never reaped.
Compaction now records the manifest generation that stopped listing each segment
(`cold_segments.retired_generation`). A segment is deletable once every live follower has
acknowledged that generation, however far the leader has moved since; the lowest live
acknowledgement is the floor. Segments retired before the column existed count as retired at the
current generation (the old, stricter behaviour). A manual reap that can delete nothing because of
a lagging follower still errors and names it; one that can delete some does so. Compaction's own
reap pass is now guarded too. Test: `scripts/test-reap-follower-guard.sh` section 7.

## Follower liveness without trusting its clock (implemented)

The reap guard used to age a follower's acknowledgement by `now - synced_ms`, where `synced_ms` is
the follower's wall clock. A follower running behind looked dead (its segments could be reaped from
under it); one running ahead never expired. The leader now records the first time it saw each
distinct acknowledgement in `_follower_seen/<ns>/<name>/<node>.json` (its own clock only, on the
shared storage so every backend and a restart share it) and ages from there. An observer that has
not seen the acknowledgement before starts at zero, so a new leader waits one full
`follower_ack_max_age_seconds` before writing off a follower. Test:
`scripts/test-reap-follower-guard.sh` section 8 (clock decades behind and far ahead).

## Closing the claim window (implemented)

An election checked that the node had caught up with the leader catalog, then (later) read the
catalog again to seal it and wrote the claim. A deposed leader's write that landed in between was
either sealed in unseen or rejected by every follower, while the new leader, which never saw it,
could not publish over it (the generation floor refuses) and so was stuck, with that write's
segments missing from its view. Now the claim seals the exact catalog the node verified, and right
after the claim `settle_seal` re-reads the catalog: if it moved, the node re-seals the new content
(the holder may rewrite its own epoch object), adopts it as a promoting follower (hot-row prune and
delete markers included, no acknowledgement) and checks again, up to five times. Once the sealed
catalog is the current one, any later stale write mismatches the seal and is ignored. A claim that
cannot settle errors and the election retries after the lease lapses. Test hook:
`koldstore.election_claim_delay_ms` pauses an election between check and claim so a test can place
the late write exactly there; `scripts/test-claim-window.sh` fails without the settle step (B stays
on the old generation, loses the late rows and cannot publish) and passes with it.

## Candidacy records and sidecar loss

- A leader withdraws its own `_leader_candidates/<schema>/<table>/<node>.json` (once per process per leader table), so a node that no longer follows is not ranked, and its out-of-date generation and epoch are not mistaken for a live candidate for three ttls. A demoted node publishes again on its next follower tick. Every candidate, the preferred one too, now waits one `catalog_sync_interval_seconds` after first seeing the lease electable before claiming, so a peer that has not yet published a record (just started, failed write) is not skipped. `scripts/test-auto-election.sh` checks the withdrawal and re-advertising.
- Delete-marker sidecars (`scripts/test-sidecar-race.sh`): a sidecar that disappears between a follower's catalog read and its fetch already fails the whole sync closed and recovers on the next tick. A sidecar the *current* catalog lists that is gone (a deposed publisher's cleanup in the lease-check window, or removed by hand) was not repaired: the unchanged leader never republished, so followers failed until some other change. The leader's worker now checks, each tick, that the sidecars its last publish listed still exist (`sidecars_missing`, one LIST) and republishes if not. `publish_leader_catalog` reports `sidecars`.

