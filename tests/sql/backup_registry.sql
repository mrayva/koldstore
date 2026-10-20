-- Registered backups keep the cold objects they reference: segments replaced by compaction, and the
-- objects of a dropped table, are not deleted while a backup taken before the change is registered.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE TABLE sqlreg.br (id bigint PRIMARY KEY, n int NOT NULL, v text);
INSERT INTO sqlreg.br SELECT g, g, 'v' || g FROM generate_series(1, 200) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.br'::regclass, storage => 'sqlreg_fs', hot_row_limit => 2,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS br_managed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS br_caught_up;
SELECT sqlreg.flush_table('sqlreg.br'::regclass) IS NOT NULL AS br_flushed;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.br SET v = 'edited' WHERE id IN (3, 77, 150);
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS br_fenced;
SELECT sqlreg.flush_table('sqlreg.br'::regclass) IS NOT NULL AS br_reflushed;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS br_fenced_2;

-- a backup starts here
SELECT koldstore.register_backup('nightly') IS NOT NULL AS br_registered;
SELECT count(*) AS br_backups FROM koldstore.backups;

SELECT (r ->> 'compacted')::boolean AS br_compacted,
       (r ->> 'held_by_registered_backups')::int > 0 AS br_held_in_report
FROM (SELECT koldstore.compact_table('sqlreg.br'::regclass, grace_seconds => 0) AS r) c;
-- grace is 0, yet the backup holds every retired object
SELECT koldstore.reap_compacted_segments('sqlreg.br'::regclass, 0) AS br_reaped_while_registered;
SELECT (SELECT count(*) FROM koldstore.cold_segments
         WHERE table_oid = 'sqlreg.br'::regclass::oid AND status = 'compacted') > 0 AS br_retired_kept;

SELECT koldstore.unregister_backup('nightly') AS br_unregistered;
SELECT koldstore.unregister_backup('nightly') AS br_unregistered_again;
SELECT koldstore.reap_compacted_segments('sqlreg.br'::regclass, 0) > 0 AS br_reaped_after;

-- a backup taken after a compaction does not hold what that compaction retired
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.br SET v = 'again' WHERE id IN (4, 78);
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS br_fenced_3;
SELECT sqlreg.flush_table('sqlreg.br'::regclass) IS NOT NULL AS br_reflushed_2;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS br_fenced_4;
SELECT (koldstore.compact_table('sqlreg.br'::regclass, grace_seconds => 0) ->> 'compacted')::boolean AS br_compacted_2;
SELECT koldstore.register_backup('later') IS NOT NULL AS br_registered_later;
SELECT koldstore.reap_compacted_segments('sqlreg.br'::regclass, 0) > 0 AS br_reaped_not_held;
SELECT koldstore.unregister_backup('later') AS br_unregistered_later;

-- a dropped table's objects are queued, not deleted, while a backup older than the drop is registered
SELECT koldstore.register_backup('predrop') IS NOT NULL AS br_registered_predrop;
DROP TABLE sqlreg.br;
SELECT count(*) > 0 AS br_drop_deferred FROM koldstore.deferred_cold_deletes;
SELECT (koldstore.purge_deferred_cold_objects(older_than_seconds => 0) ->> 'deleted')::int AS br_purged_while_registered;
-- unregistering releases and purges them (retention is off here, so no window applies)
SELECT koldstore.unregister_backup('predrop') AS br_unregistered_predrop;
SELECT count(*) AS br_queue_left FROM koldstore.deferred_cold_deletes;
