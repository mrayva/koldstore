# koldstore under multi-master logical replication (Spock)

Tested 2026-09-26 on the local three-node Spock mesh (`~/pg-spock`, PostgreSQL 18.6,
Spock with `spock_output`), koldstore 0.1.12-preview.0, table `public.kt` in the
default replication set on all nodes, each node with its **own** storage and its
**own** cold tier. Nothing here was run against production data.

## What has to be configured

| Setting | Why |
|---------|-----|
| `output_plugin_libraries` includes `pgoutput` | The mesh's PostgreSQL restricts logical-decoding plugins; koldstore's mirror slot uses `pgoutput` and otherwise fails with "async mirror slot ... incompatible ... output plugin". |
| `wal_level = logical`, enough `max_worker_processes` / `max_replication_slots` | koldstore adds a supervisor, a WAL applier and flush executors per database, plus one slot. |
| `koldstore.capture_replicated_changes = on` (restart) | **Required on every node that receives replicated writes.** See below. |

## Finding 1: without `koldstore.capture_replicated_changes`, replicated writes are invisible to koldstore

The async mirror reads its slot with pgoutput `origin = 'none'`, which drops every
change that carries a replication origin. A Spock apply worker stamps its
origin on everything it writes, so on a peer the mirror never sees replicated
inserts, updates or deletes (the peer's mirror was empty even for a plain replicated
`INSERT`). Consequences: a replicated delete of a cold row never produces a
tombstone, and replicated rows never reach the mirror that flush selects from.

`koldstore.capture_replicated_changes = on` (postmaster-level, default off) switches
to the mode PG15 always used: flush prunes are stamped with a **named** origin
`koldstore_flush_<dboid>`, the slot is read without the origin filter, and only that
named origin is skipped. Rules for turning it on: restart the node, and first make
the mirror fully caught up (`koldstore.wait_for_async_mirror()`), because prune
deletes still in the slot from the old mode would otherwise be mirrored as
tombstones and hide cold rows.

Verified with it on: flush prunes are not mirrored (0 tombstones after flushing all
three nodes), replicated inserts appear in every peer's mirror, and prune deletes
do not replicate to peers through Spock (each node flushes independently).

## Finding 2: hydrate-on-write converges across the mesh

With the setting on and `koldstore.hydrate_on_write = on` on the originating node:

- `DELETE` of three cold-only keys on node 1: the hydration `INSERT` and the `DELETE`
  replicate, each peer applies both, and each peer's own mirror records the
  tombstones. All three nodes ended at the same row count with the keys gone.
- `UPDATE` of a cold-only key on node 1: all three nodes show the new value.

This is why the hydration insert must replicate (see ADR-007): the peer gets a hot
copy for the following change to act on. Hydration no longer fires user triggers on the
originating node either, so a trigger-maintained derived table matches native semantics
(one `DELETE` audit row, not `INSERT` + `DELETE`).

## Finding 3 (open, real): a change to a row that is cold on a peer diverges

Say a row is hot on node 1 and node 3 but has already been flushed on node 2 (each
node flushes on its own schedule). A plain `DELETE` or `UPDATE` on node 1 replicates
to node 2, whose heap has no such row. Spock records `delete_missing / skip` (an
update to a missing row is likewise not applied) and node 2 keeps the old row:

    DELETE id 610 on node1:  node1 (absent)   node2 c610   node3 (absent)
    UPDATE id 620 on node1:  node1 upd-n1     node2 c620   node3 upd-n1

Spock's apply does not go through the executor, so no koldstore hook runs on the
peer, and a trigger cannot fire for a row that is not in the heap.

Spock records the two cases differently, which matters for the fix:

- **`DELETE`** of a missing row: a `delete_missing` row in `spock.resolutions` (the key),
  and the change is skipped.
- **`UPDATE`** of a missing row is an *error* inside the apply worker. With
  `spock.exception_behaviour = transdiscard` (the mesh default) Spock discards the
  **entire remote transaction**, not just that statement, and logs every operation of it
  (with the full new tuple, in `command_counter` order) in `spock.exception_log`.
  Verified: an unrelated `INSERT` in the same transaction was lost on the peer too.

## The reconciler (implemented): `koldstore.reconcile_spock_conflicts()`

Reads those two logs and replays what was lost, through the cold-row-aware
`update_row()` / `delete_row()` for managed tables and plain SQL for other tables:

    SELECT koldstore.reconcile_spock_conflicts();   -- {"transactions": 2, "deletes": 1, ...}

- **Whole transactions are replayed**, in order, atomically per transaction (a
  sub-transaction each), so a discarded mixed transaction is restored completely.
- **Once only.** Each handled item is recorded in `koldstore.spock_reconciled`
  (`txn:<origin>:<xid>` or `res:<node>:<id>`, status `replayed` / `skipped` /
  `failed`). Delete a row there to retry. A transaction that fails (for example an
  `INSERT` of a key that already exists cold, rejected by the insert guard) is marked
  `failed` with the error and left for an operator.
- **Not forwarded.** The replay runs under its own replication origin
  (`koldstore_reconcile`), which Spock does not forward, so nodes that already applied
  the change do not receive it again. Verified: the other nodes' mirrors did not move.
- **User triggers do not fire during the replay** (the function runs with
  `session_replication_role = replica`, as Spock's own apply workers do): the change
  already fired its triggers once at the node where it originated. Verified with a
  trigger maintaining a replicated audit table: before this, the peer's replay wrote its
  own extra audit rows that were not forwarded (the derived table diverged), and a
  trigger relying on client-session state (`inet_server_port()` is NULL in a background
  worker) made the replay fail outright; after it the audit tables were identical on all
  three nodes.
- **Requires** `koldstore.capture_replicated_changes = on` (checked), so the mirror
  records the replay's tombstones and new versions; executable by superusers only.
- **Background worker.** Instead of scheduling it yourself, let koldstore run it. Set
  (restart required):

      koldstore.capture_replicated_changes = on
      koldstore.spock_reconcile_interval_seconds = 15      # 0 (default) = off
      koldstore.spock_reconcile_databases = 'spockdb'      # comma-separated

  One persistent worker per listed database wakes on that interval (first run 5 s after
  startup), calls the function in its own transaction, and logs a line only when it did
  something. A failed run (extension or Spock not installed yet) is logged and retried
  on the next tick. Manual calls remain fine: the function takes an advisory lock, so
  overlapping runs are harmless.

Tested on the mesh (manually, then with the background worker at a 5 s interval: a divergence
existed 3 s after the writes and had healed by 15 s with no manual call): updates and deletes of rows cold on node 2, originating on node 1
and on node 3, a discarded mixed transaction (update + insert + delete), and an
idempotent rerun. All nodes converged and the mirrors on the originating nodes were
untouched. Spock-free coverage is in `tests/sql/spock_reconcile_helpers.sql`.

Limits (deliberate for now):

- **Eventual, not immediate:** the peer diverges until the reconciler runs.
- **No timestamp arbitration.** If the peer changed the same cold key locally between
  the conflict and the reconcile, the replayed change overwrites it (last reconcile
  wins). Spock's own `last_update_wins` does not apply because the row was not in the
  heap when the conflict happened.
- **Only the missing-row cases** are handled; other conflict types are Spock's.
- **A Spock-side hook** on the missing-row paths (letting an extension hydrate and retry
  inside the apply worker) would remove the delay and the ordering gap. That remains the
  cleaner long-term design and needs a change in Spock.
- **Age-based flush with a shared margin** on every node still helps: fewer rows are cold
  on one node and hot on another.

## Not covered

Spock DDL replication with koldstore (turned off in this mesh), sequences and
snowflake ids across nodes, three-way concurrent changes to one cold key, and the
delete-vs-delete row-count quirk from ADR-007 across nodes.
