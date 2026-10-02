# ADR-008: Partitioned/inherited table support (upstream #125)

## Status

Option A, two slices shipped and live-verified (2026-10-02): a partition or
inheritance leaf can be `manage_table`'d like a plain table; the parent stays
permanently unmanageable; `ATTACH PARTITION`/`INHERIT`/`DETACH PARTITION`
work in either direction as long as the parent role isn't managed. Confirmed
live: a plain `SELECT`/`INSERT` through the parent sees a managed leaf's hot
and cold data correctly, with zero planner-level code added, exactly as this
ADR predicted. `UPDATE`/`DELETE`/`MERGE` through the parent are now refused
whenever a managed leaf has cold data anywhere (a coarse, table-wide check,
not a precise per-row recount) instead of silently doing nothing -- see
"What shipped" below and `docs/limitations.md`. A precise per-leaf recount
and the partition-key-changing-`UPDATE` hydration case remain open, tracked
in "Next step".

## Date

2026-10-02

## Context

`manage_table` refuses every relation that takes any part in a partition or
inheritance hierarchy (`reject_unsupported_relation_kind`,
`crates/pg_koldstore/src/sql/migrate/manage.rs:19-74`): a partitioned parent
(`relkind = 'p'`), a partition (`relispartition`), an inheritance parent
(`has_children`) or child (`is_child`). DDL that would *create* such a
hierarchy touching an already-managed table is separately refused
(`reject_create_in_managed_hierarchy`, `reject_alter_hierarchy_of_managed`,
`hooks/ddl.rs:315-497`: `CREATE TABLE ... INHERITS/PARTITION OF`, `ALTER TABLE
... INHERIT`, `ALTER TABLE ... ATTACH PARTITION`, on either side). This is the
largest remaining item in the gap list ([[koldstore_remaining_gaps]]) and the
one explicitly flagged there as needing its own design decision rather than a
quick fix.

Three independent subsystems currently assume "one managed table = one
relation, one OID, one set of options" and would need to look at partitioning
one way or another:

1. **Read path.** `set_rel_pathlist_hook` (`merge_scan/pg.rs:409-470`) already
   fires once per base relation in the finished range table — confirmed by
   reading the hook itself: it takes `(root, rel, rti, rte)` and reads
   `rte->relid` directly, with no assumption baked in that the relation is a
   plain top-level target. PostgreSQL's own partition planning
   (`set_append_rel_pathlist`) calls this same hook once per *leaf* partition
   when building the `Append`/`MergeAppend` over them, before rolling the
   per-leaf paths up. So a leaf partition that was independently managed would
   already get `KoldMergeScan` injected for it today, with no planner-level
   change — this was the single most useful fact found in this pass.
2. **Write guard / hydrate-on-write.** Both assume a single result relation
   today, and say so explicitly: `executor.rs:264-269`
   (`cold_only_update_delete_candidate`) bails out whenever
   `PlannedStmt.resultRelations.length != 1`, with a comment calling
   multi-relation targets (i.e. partitioned `UPDATE`/`DELETE`/`MERGE`, which
   PostgreSQL plans as one `ModifyTable` with N result relations, one per
   affected leaf) "out of scope for this pass — never a false positive, just
   unguarded." `dml_planner.rs`'s probe builder has the same single-relation
   assumption. Partitioned DML would need this generalized to loop per result
   relation.
3. **Mirror / logical replication capture.** `ALTER PUBLICATION ... ADD TABLE
   <table> (<columns>)` (`mirror/lifecycle.rs:222,248`) already targets one
   relation by name, and PostgreSQL allows adding an individual partition to a
   publication directly (independent of `publish_via_partition_root`). So a
   managed leaf partition needs no mirror-subsystem change either — it is
   just another independently-captured table as far as the mirror is
   concerned.

All three facts point the same way: nothing fundamental blocks a design where
a **partition leaf is managed individually, like any other plain table**, and
the partitioned parent itself stays a thin, un-managed router that PostgreSQL
already knows how to plan over.

## Option A: leaf-level management (recommended direction)

`manage_table` is allowed to target a partition (`relispartition = true`,
`relkind = 'r'`), with no other change to what "managed" means. The
*partitioned parent* (`relkind = 'p'`) stays permanently unmanageable — it has
no storage of its own, so there is nothing to flush or scan. Plain inheritance
(non-partition `INHERITS`) is treated the same as a partition leaf for
managing a child, but a managed table still cannot itself *be* an inheritance
parent (`has_children`) or a partitioned parent, since both require
query-time inheritance expansion over a relation that is itself managed,
which is a materially different, larger problem (see "Deferred" below).

What this gets for free, per the subsystem findings above:
- `SELECT ... FROM parent` already plans correctly — PostgreSQL's own
  partition pruning and `Append` construction call the existing pathlist
  hook per leaf; no Append-level koldstore code needed.
- Logical replication capture of a managed leaf needs no mirror changes.
- Per-leaf cold storage, compression, retention and `scope_column` policy —
  genuinely useful, since partitioning by (e.g.) a date range and tiering
  older partitions to cold storage sooner is a natural, common pairing, not
  just an edge case to tolerate.

What this requires building:
- **DDL guard rework.** `reject_if_managed` currently refuses `ATTACH
  PARTITION`/`INHERIT`/`PARTITION OF` unconditionally when either side is
  managed. Under Option A this becomes conditional: attaching an
  **unmanaged** table under a parent stays allowed (today's existing
  behavior for ordinary tables, unchanged); the user then calls
  `manage_table` on the leaf directly, same as any standalone table.
  Attaching an **already-managed** leaf also needs a decision: allow it
  (its own hot/cold storage is unaffected either way) or keep refusing it
  until proven safe — leaning toward allow, since nothing about the leaf's
  own storage changes by gaining a parent.
- **Write-guard / hydrate-on-write generalization.** Both need to loop over
  `PlannedStmt.resultRelations` instead of assuming exactly one. Turned out
  less mechanical than it looked from here: PostgreSQL lists *every*
  partition in that list regardless of how selective the WHERE clause is
  (runtime, not plan-time, pruning), and modern PostgreSQL's `ModifyTable`
  has a single child plan rather than one subplan per result relation, so
  there is no straightforward per-leaf WHERE clause to recover yet. Shipped
  as a coarse, fail-closed "has cold data anywhere" check instead of a
  precise recount -- see "What shipped" below.
- **Cross-partition tuple routing.** PostgreSQL implements a partition-key
  `UPDATE` that moves a row to a different leaf as a `DELETE` on the old leaf
  plus an `INSERT` on the new one. If the row being moved is cold-only on the
  old leaf, hydrate-on-write would need to hydrate it there first (same
  mechanism as today's single-table hydrate-before-delete, just needs to
  resolve which leaf currently holds the row before acting) before the
  native delete half can see it. This is the single trickiest new case
  found in this pass and needs its own design note once Option A is
  scoped for real implementation — not solved here.
- **A cascading convenience, not a new catalog concept.** Rather than
  inventing parent-level configuration, a `manage_table(parent, ..., cascade
  => true)` helper could just walk `pg_inherits` and call the existing
  per-table `manage_table` on each current leaf with the same options —
  pure sugar over Option A, addable later without blocking the core work.
- **Global PK/FK uniqueness** stays exactly the existing, already-documented
  non-goal (hot+cold uniqueness is only ever enforced within one relation's
  own hot heap today) — partitioning does not make this worse, since
  PostgreSQL itself already requires the partition key be part of any
  unique constraint on a partitioned table, so "the PK" was never
  table-wide in the cross-leaf sense anyway.

## Option B: whole-table virtual management (considered, not recommended first)

Let `manage_table` target the partitioned *parent* directly, with koldstore
owning a single unified config and presenting one logical hot/cold view
across all current and future leaves — cascading storage/compression/
retention automatically to new partitions as they're attached, rolling up
manifest hints and row counters across leaves, and handling `DETACH
PARTITION`/`ATTACH PARTITION`/partition-maintenance DDL as *koldstore*
lifecycle events rather than ordinary DDL.

This is a materially larger build: new catalog modeling for a
parent-with-children managed entity (today's catalog is entirely
one-row-per-table), Append-aware manifest/row-counter rollups, and DDL hooks
that must intercept and react to partition-maintenance statements rather than
just reject them. It also doesn't obviously buy correctness or performance
over Option A plus a cascading `manage_table` helper — the per-leaf planning
win (point 1 above) is identical either way, since PostgreSQL's own Append
construction is what does the rollup at query time regardless of which option
manages the catalog metadata.

Recommendation: **do not start here.** Revisit only if Option A's per-leaf
config turns out to be a real operational burden in practice (e.g. users
routinely re-partitioning with many leaves, config drift becoming a problem),
which is better learned after Option A ships than guessed now.

## Deferred, explicitly out of scope for both options

- A managed table being itself an inheritance **parent** (`has_children`) or
  a **partitioned parent** (`relkind = 'p'`) — i.e., cold data living
  *above* a hierarchy rather than at a leaf. This needs inheritance-aware
  scan planning at the managed table's own level (not just "the hook already
  fires per leaf," since here the managed relation *is* the thing being
  expanded), which is a different and larger problem than Option A. Not
  scoped here.
- Sub-partitioning (a managed leaf that is itself further partitioned) — not
  examined; likely composes fine under Option A's "a partition is just a
  table" framing but not verified.
- The partition-key-changing `UPDATE` hydrate-before-move case flagged above
  under Option A — identified, not designed.

## What shipped (first slice, 2026-10-02)

- `reject_unsupported_relation_kind` (`sql/migrate/manage.rs`) no longer
  refuses a relation merely for being a partition or a plain-inheritance
  child; it still refuses `relkind = 'p'` (partitioned parent) and
  `has_children` (plain-inheritance parent). A plain-inheritance child does
  **not** automatically inherit its parent's `PRIMARY KEY` (traditional
  PostgreSQL inheritance never propagated unique/PK constraints, unlike
  declarative partitioning) -- it needs its own local PK to be manageable,
  same as any table; this surfaced as the pre-existing, unrelated
  "managed tables require a primary key" error when tested, not a new
  hierarchy-specific one.
- `reject_alter_hierarchy_of_managed` (`hooks/ddl.rs`) now only refuses the
  relation taking the **parent** role in `ALTER TABLE ... INHERIT`/`ATTACH
  PARTITION`; the relation taking the **child/leaf** role is unaffected by
  its own management status in either direction.
- Live-verified end-to-end on an isolated pgrx cluster (`tests/sql/
  partitioned_tables.sql`): a managed leaf's `EXPLAIN` under the parent's
  `Append` shows `Custom Scan (KoldMergeScan)`, a plain `SELECT`/`INSERT`
  through the parent returns correct hot+cold results and routes inserts to
  the right leaf, and `DETACH PARTITION` of a managed leaf is unaffected.
  Full 19-case SQL regression suite green (run twice for determinism),
  clippy clean.
- **Confirmed, not just predicted, the open risk**: `UPDATE sqlreg.p_sales
  SET ... WHERE id = 3 AND region = 'east'` (a cold row, reached through the
  parent) returned `UPDATE 0` with no error, leaving the cold row unchanged
  -- the exact silent-skip behavior #122 closed for a plain single-table
  statement, now reachable again through a managed leaf's parent. The
  identical statement issued directly against the managed leaf (bypassing
  the parent) was unaffected -- today's write guard already covers it
  correctly. Fixed in the second slice below.

## What shipped (second slice, 2026-10-02, same day)

Investigated the precise shape before writing any fix, via a temporary debug
probe rather than assumption: **every partition is always listed in
`PlannedStmt.resultRelations` for a parent-routed statement, even one with a
literal, maximally selective WHERE clause on the partition key** --
confirmed live (`UPDATE p_sales SET ... WHERE id = 3 AND region = 'east'`
against a 2-partition table produced `resultRelations.length = 2`, not 1,
even though `EXPLAIN` only displayed one reachable branch). PostgreSQL relies
on *runtime* partition pruning to skip touching the others, not plan-time
elimination from this list, so the "N happens to be 1" shortcut the first
slice's "Next step" assumed does not actually occur in practice. Modern
PostgreSQL's `ModifyTable` also has a single child plan (no more
`plans`/one-subplan-per-result-relation list), so there is no
straightforward way to recover a precise per-leaf WHERE clause from the plan
tree the way the single-table path does -- that would need its own
investigation into the single child plan's shape (an `Append` over per-leaf
scans tagged by a hidden `tableoid` junk column, at a guess), not attempted
this round.

Given that, `cold_only_update_delete_candidate` (`hooks/executor.rs`) now
returns one candidate per *managed* result relation instead of bailing
outright when `resultRelations.length != 1`. Each multi-relation candidate
is built with `raw`/`where_sql`/`join_probe` all deliberately `None` --
unverifiable by construction -- so it falls through to
`enforce_unverifiable_scan_guard`/`enforce_unverifiable_merge_guard`, the
same fail-closed checks already used for other hard-to-verify shapes (a CTE
join source, a volatile function): refuse whenever the leaf has cold data
*anywhere*. Both guards' messages now name "a partitioned/inherited target"
specifically instead of their generic wording. `hydrate-on-write`'s
`top_level_target` (`hooks/hydrate_on_write.rs`) is unaffected -- it already
bailed on `resultRelations.length != 1`, which is now the *safe* outcome
(falls through to the corrected write guard) rather than the dangerous one
(falls through to the old, silently-permissive guard gap).

This is a real precision trade-off, confirmed and accepted rather than
hidden: a parent-routed statement that would only touch a **hot** row is
also refused, as long as that leaf has cold data elsewhere, because this
guard cannot yet tell "this leaf has cold data" apart from "this specific
row is cold" for a multi-relation target. New regression cases in
`tests/sql/partitioned_tables.sql` cover all three outcomes: a cold-row
parent-update now refused, a hot-row parent-update *also* refused (the
coarse edge, confirmed on purpose), and a statement touching only an
unmanaged sibling leaf correctly unaffected. Full SQL regression suite green
(confirmed clean multiple times; a few runs hit the pre-existing, unrelated
"flush finalize could not acquire slot lock before deadline" flakiness on
random *other* files, not on `partitioned_tables`), clippy clean.

## Next step (not started)

A **precise per-leaf recount** for a partitioned/inherited UPDATE/DELETE/
MERGE, replacing the current coarse "has cold data anywhere" refusal --
needs investigating modern PostgreSQL's single-child-plan shape for a
multi-result-relation `ModifyTable` first (see "What shipped" above).
`koldstore.hydrate_on_write` support for a partitioned target depends on the
same missing mechanism. The partition-key-changing-`UPDATE` hydration case
(moving a cold-only row across leaves via PostgreSQL's native DELETE+INSERT)
is a separate, harder follow-on, not designed yet.

See [[koldstore_remaining_gaps]] for where this sits in the overall gap list.
