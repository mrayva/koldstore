-- Value summaries for composite primary keys and for date / timestamp / timestamptz keys (ADR-009).
--
-- A summary hashes the whole key (the key columns' encodings back to back, in key order) and is stored
-- on the first key column's index row. A lookup that names every key column probes it; a partial key
-- or a single column of a composite key must NOT, or segments holding the row would be dropped.
-- Pruning must never hide a row.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE FUNCTION sqlreg.explain_counter(query text, label text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
  line text;
  value bigint;
BEGIN
  FOR line IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) ' || query LOOP
    IF line ~ ('^\s*' || label || ': [0-9]+') THEN
      value := substring(line FROM label || ': ([0-9]+)')::bigint;
      EXIT;
    END IF;
  END LOOP;
  RETURN value;
END
$$;

CREATE FUNCTION sqlreg.sum_counts(queries text[]) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
  total bigint := 0;
  q text;
  found bigint;
BEGIN
  FOREACH q IN ARRAY queries LOOP
    EXECUTE q INTO found;
    total := total + found;
  END LOOP;
  RETURN total;
END
$$;

-- ------------------------------------------------------------------ composite key, scattered segments
-- Declared in an order that differs from the column order: (tenant, id), columns (id, tenant, v).
CREATE TABLE sqlreg.cps (id bigint, tenant int, v int NOT NULL DEFAULT 0, PRIMARY KEY (tenant, id));
INSERT INTO sqlreg.cps (id, tenant) SELECT g, 1 + (g % 3) FROM generate_series(1, 1200) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.cps'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS cps_managed;
SELECT sqlreg.flush_table('sqlreg.cps'::regclass) IS NOT NULL AS cps_flushed;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.cps SET v = v + 1 WHERE id IN (3, 590, 1177);
SELECT sqlreg.flush_table('sqlreg.cps'::regclass) IS NOT NULL AS round1;
UPDATE sqlreg.cps SET v = v + 1 WHERE id IN (41, 700, 1100);
SELECT sqlreg.flush_table('sqlreg.cps'::regclass) IS NOT NULL AS round2;
UPDATE sqlreg.cps SET v = v + 1 WHERE id IN (88, 512, 1199);
SELECT sqlreg.flush_table('sqlreg.cps'::regclass) IS NOT NULL AS round3;
UPDATE sqlreg.cps SET v = v + 1 WHERE id IN (150, 640, 930);
SELECT sqlreg.flush_table('sqlreg.cps'::regclass) IS NOT NULL AS round4;
RESET koldstore.hydrate_on_write;
SELECT count(*) >= 5 AS several_segments
FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.cps'::regclass AND status = 'active';

-- Every segment carries a summary, on the FIRST key column's index row (tenant) and only there.
SELECT count(*) FILTER (WHERE i.value_summary IS NOT NULL) AS summaries,
       count(*) FILTER (WHERE i.value_summary IS NOT NULL AND i.column_id = (
         SELECT attnum FROM pg_attribute WHERE attrelid = 'sqlreg.cps'::regclass AND attname = 'tenant')) AS on_first_key_column,
       (SELECT count(*) FROM koldstore.cold_segments WHERE table_oid = 'sqlreg.cps'::regclass AND status = 'active') AS segments
FROM koldstore.cold_segment_index i JOIN koldstore.cold_segments s ON s.segment_id = i.segment_id
WHERE s.table_oid = 'sqlreg.cps'::regclass AND s.status = 'active';

-- NO FALSE NEGATIVES: every key is found by a full-key lookup, a partial-key lookup and a
-- single-column lookup, whichever segment holds it.
SELECT sqlreg.sum_counts(ARRAY(SELECT format('SELECT count(*) FROM sqlreg.cps WHERE tenant = %s AND id = %s', 1 + (g % 3), g)
                               FROM generate_series(1, 1200) g)) = 1200 AS every_full_key_found;
SELECT sqlreg.sum_counts(ARRAY(SELECT format('SELECT count(*) FROM sqlreg.cps WHERE id = %s', g)
                               FROM generate_series(1, 1200) g)) = 1200 AS every_id_found;
SELECT count(*) AS rows_for_tenant_2 FROM sqlreg.cps WHERE tenant = 2;
SELECT count(*) AS updated_rows_visible, sum(v) AS total_updates FROM sqlreg.cps WHERE v > 0;

-- Pruning: a full key that exists nowhere, and a key whose parts each exist but not together (the
-- half-matching case a per-column check could never prune).
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.cps WHERE tenant = 1 AND id = 1010', 'Candidate Segments') >= 5 AS many_candidates,
       sqlreg.explain_counter('SELECT v FROM sqlreg.cps WHERE tenant = 1 AND id = 1010', 'Segments Pruned by Catalog Index') >= 3 AS absent_key_pruned,
       sqlreg.explain_counter('SELECT v FROM sqlreg.cps WHERE tenant = 2 AND id = 1011', 'Segments Pruned by Catalog Index') >= 3 AS half_matching_key_pruned;
SELECT count(*) AS absent_found FROM sqlreg.cps WHERE tenant = 1 AND id = 1010;

-- Partial keys and other shapes must not use the composite summary (they would lose rows).
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.cps WHERE tenant = 2', 'Segments Pruned by Catalog Index') = 0 AS partial_key_unpruned;
SELECT count(*) AS partial_key_rows FROM sqlreg.cps WHERE tenant = 3 AND id > 1100;

-- Legacy segments (no summary) are never pruned; rebuild gives back exactly what flush wrote.
CREATE TABLE sqlreg.cps_before AS
SELECT i.segment_id, i.column_id, i.value_summary
FROM koldstore.cold_segment_index i JOIN koldstore.cold_segments s ON s.segment_id = i.segment_id
WHERE s.table_oid = 'sqlreg.cps'::regclass AND s.status = 'active' AND i.value_summary IS NOT NULL;
UPDATE koldstore.cold_segment_index i SET value_summary = NULL
FROM sqlreg.cps_before b WHERE i.segment_id = b.segment_id AND i.column_id = b.column_id;
SELECT (t ->> 'eligible')::boolean AS eligible, t ->> 'column' AS key_columns,
       (t ->> 'segments_without_summary')::int = (SELECT count(*) FROM sqlreg.cps_before) AS counts_every_segment
FROM (SELECT koldstore.rebuild_segment_summaries('sqlreg.cps'::regclass, dry_run => true) AS r) s,
     LATERAL jsonb_array_elements(r -> 'tables') t;
SELECT (t ->> 'remaining')::int AS remaining, jsonb_array_length(t -> 'failed') AS failed
FROM (SELECT koldstore.rebuild_segment_summaries('sqlreg.cps'::regclass) AS r) s,
     LATERAL jsonb_array_elements(r -> 'tables') t;
SELECT count(*) AS bytes_differ_from_flush
FROM sqlreg.cps_before b
JOIN koldstore.cold_segment_index i ON i.segment_id = b.segment_id AND i.column_id = b.column_id
WHERE i.value_summary IS DISTINCT FROM b.value_summary;
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.cps WHERE tenant = 3 AND id = 1013', 'Segments Pruned by Catalog Index') >= 3 AS pruned_again;
SELECT sqlreg.sum_counts(ARRAY(SELECT format('SELECT count(*) FROM sqlreg.cps WHERE tenant = %s AND id = %s', 1 + (g % 3), g)
                               FROM generate_series(1, 1200) g)) = 1200 AS every_full_key_found_after_rebuild;

-- ------------------------------------------------------------------ date key (with infinity)
CREATE TABLE sqlreg.cps_date (d date PRIMARY KEY, n int NOT NULL, v int NOT NULL DEFAULT 0);
INSERT INTO sqlreg.cps_date (d, n) SELECT date '2000-01-01' + g, g FROM generate_series(1, 1100) g;
INSERT INTO sqlreg.cps_date (d, n) VALUES ('infinity', 5000), ('-infinity', 5001), ('1969-07-20', 5002);
SELECT koldstore.manage_table(
  table_name => 'sqlreg.cps_date'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS cps_date_managed;
SELECT sqlreg.flush_table('sqlreg.cps_date'::regclass) IS NOT NULL AS cps_date_flushed;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.cps_date SET v = v + 1 WHERE d IN (date '2000-01-10', date '2001-03-01', 'infinity');
SELECT sqlreg.flush_table('sqlreg.cps_date'::regclass) IS NOT NULL AS date_round1;
UPDATE sqlreg.cps_date SET v = v + 1 WHERE d IN (date '2000-06-01', date '2001-12-01', '-infinity');
SELECT sqlreg.flush_table('sqlreg.cps_date'::regclass) IS NOT NULL AS date_round2;
UPDATE sqlreg.cps_date SET v = v + 1 WHERE d IN (date '2000-08-01', date '2002-01-01', date '1969-07-20');
SELECT sqlreg.flush_table('sqlreg.cps_date'::regclass) IS NOT NULL AS date_round3;
RESET koldstore.hydrate_on_write;
SELECT count(*) FILTER (WHERE i.value_summary IS NOT NULL) = count(*) AS every_segment_has_date_summary
FROM koldstore.cold_segment_index i JOIN koldstore.cold_segments s ON s.segment_id = i.segment_id
WHERE s.table_oid = 'sqlreg.cps_date'::regclass AND s.status = 'active'
  AND i.column_id = (SELECT attnum FROM pg_attribute WHERE attrelid = 'sqlreg.cps_date'::regclass AND attname = 'd');
SELECT sqlreg.sum_counts(ARRAY(SELECT format('SELECT count(*) FROM sqlreg.cps_date WHERE d = %L::date', date '2000-01-01' + g)
                               FROM generate_series(1, 1100) g)) = 1100 AS every_date_found;
SELECT (SELECT count(*) FROM sqlreg.cps_date WHERE d = 'infinity') AS infinity_found,
       (SELECT count(*) FROM sqlreg.cps_date WHERE d = '-infinity') AS neg_infinity_found,
       (SELECT count(*) FROM sqlreg.cps_date WHERE d = date '1969-07-20') AS pre_epoch_found,
       (SELECT count(*) FROM sqlreg.cps_date WHERE d = date '2030-01-01') AS absent_found;
SELECT sum(v) AS total_updates FROM sqlreg.cps_date;
SELECT sqlreg.explain_counter('SELECT v FROM sqlreg.cps_date WHERE d = date ''2030-01-01''', 'Segments Pruned by Catalog Index') >= 1 AS date_absent_pruned;

-- ------------------------------------------------------------------ timestamptz inside a composite key
CREATE TABLE sqlreg.cps_ts (tenant int, ts timestamptz, n int NOT NULL, PRIMARY KEY (tenant, ts));
INSERT INTO sqlreg.cps_ts SELECT 1 + (g % 2), timestamptz '2020-01-01 00:00:00+00' + g * interval '1 hour', g FROM generate_series(1, 1100) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.cps_ts'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS cps_ts_managed;
SELECT sqlreg.flush_table('sqlreg.cps_ts'::regclass) IS NOT NULL AS cps_ts_flushed;
SET TimeZone = 'America/New_York';
SELECT sqlreg.sum_counts(ARRAY(SELECT format('SELECT count(*) FROM sqlreg.cps_ts WHERE tenant = %s AND ts = %L', 1 + (g % 2),
                                              timestamptz '2020-01-01 00:00:00+00' + g * interval '1 hour')
                               FROM generate_series(1, 1100) g)) = 1100 AS every_tenant_ts_found;
RESET TimeZone;
SELECT count(*) FILTER (WHERE value_summary IS NOT NULL) AS ts_summaries
FROM koldstore.cold_segment_index i JOIN koldstore.cold_segments s ON s.segment_id = i.segment_id
WHERE s.table_oid = 'sqlreg.cps_ts'::regclass AND s.status = 'active';
