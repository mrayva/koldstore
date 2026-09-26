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
copy for the following change to act on.

## Finding 3 (open, real): a change to a row that is cold on a peer diverges

Say a row is hot on node 1 and node 3 but has already been flushed on node 2 (each
node flushes on its own schedule). A plain `DELETE` or `UPDATE` on node 1 replicates
to node 2, whose heap has no such row. Spock records `delete_missing / skip` (an
update to a missing row is likewise not applied) and node 2 keeps the old row:

    DELETE id 610 on node1:  node1 (absent)   node2 c610   node3 (absent)
    UPDATE id 620 on node1:  node1 upd-n1     node2 c620   node3 upd-n1

Spock's apply does not go through the executor, so no koldstore hook runs on the
peer, and a trigger cannot fire for a row that is not in the heap.

Options, none implemented:

1. **Age-based flush with a shared, generous margin on every node**, so a row is only
   cold when it is old enough that nobody modifies it in place. Changes to cold rows
   then go through hydrate-on-write on the originating node (Finding 2), which
   replicates correctly. This narrows the window but does not close it (clock skew,
   replication lag).
2. **A reconciler** driven by `spock.resolutions` (`save_resolutions = on` is already
   set): for each `delete_missing` / `update_missing` on a managed table, apply the
   change to the peer's cold row with `delete_row()` / `update_row()` (the conflict
   log carries the remote tuple). Eventually consistent, and it needs an ordering
   story.
3. **A Spock-side hook** on the missing-row conflict paths that lets an extension
   hydrate before the operation is retried. Cleanest, but a Spock change.

## Not covered

Spock DDL replication with koldstore (turned off in this mesh), sequences and
snowflake ids across nodes, three-way concurrent changes to one cold key, and the
delete-vs-delete row-count quirk from ADR-007 across nodes.
