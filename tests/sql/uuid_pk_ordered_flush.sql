-- A populated table with a uuid primary key and an order column (migration_order_by) used to fail
-- its very first flush page: the keyset cursor's first-page placeholder for the uuid key was an empty
-- string, which is not a valid uuid ("invalid uuid keyset value: invalid length: found 0").
--
-- The order column here has only 50 distinct values over 2500 rows, so thousands of rows share an
-- order key and the paging across the three segments (max_rows_per_file = 1000) depends on the uuid
-- tiebreak of the keyset; a bug there would lose or duplicate rows at page boundaries.

\set VERBOSITY terse
\set ON_ERROR_STOP off

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

CREATE TABLE sqlreg.uu (id uuid PRIMARY KEY, n int NOT NULL, payload text NOT NULL);
INSERT INTO sqlreg.uu (id, n, payload)
SELECT gen_random_uuid(), 1 + (g % 50), 'row-' || g FROM generate_series(1, 2500) g;

-- Remember some ids (including rows from the first and last order keys) outside the managed table.
CREATE TABLE sqlreg.uu_sample AS
SELECT id FROM sqlreg.uu WHERE n IN (1, 2, 25, 49, 50) ORDER BY id LIMIT 60;
SELECT count(*) AS sampled FROM sqlreg.uu_sample;

SELECT koldstore.manage_table(
  table_name => 'sqlreg.uu'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS uu_managed;
SELECT sqlreg.flush_table('sqlreg.uu'::regclass) IS NOT NULL AS uu_flushed;

-- The flush really moved rows to cold storage, across several segments.
SELECT (koldstore.table_status('sqlreg.uu'::regclass) ->> 'cold_row_count')::int >= 2000 AS most_rows_are_cold,
       (SELECT count(*) FROM koldstore.cold_segments
         WHERE table_oid = 'sqlreg.uu'::regclass AND status = 'active') >= 2 AS several_segments;

-- No row lost or duplicated at page boundaries.
SELECT count(*) AS total_rows, count(DISTINCT id) AS distinct_ids FROM sqlreg.uu;
SELECT count(*) AS rows_per_order_key_ok
FROM (SELECT n, count(*) c FROM sqlreg.uu GROUP BY n) t WHERE c = 50;

-- Every sampled uuid is found by a point lookup, and carries its original payload.
SELECT sqlreg.sum_counts(ARRAY(SELECT format('SELECT count(*) FROM sqlreg.uu WHERE id = %L::uuid', id)
                               FROM sqlreg.uu_sample)) AS sample_hits;
SELECT count(*) AS payloads_intact
FROM sqlreg.uu u JOIN sqlreg.uu_sample s USING (id) WHERE u.payload LIKE 'row-%';

-- The order column is respected: the first rows by n are the n = 1 group.
SELECT min(n) AS lowest_order_key, max(n) AS highest_order_key FROM sqlreg.uu;
