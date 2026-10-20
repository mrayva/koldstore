-- numeric values must survive a hydrate-on-write round trip digit for digit. Rows hydrate through a
-- jsonb value read into serde_json; without serde_json's arbitrary_precision a numeric such as 1.50
-- came back as 1.5 and a 30-digit value was silently rounded through a float64.
--
-- Oracle: an unmanaged copy of the table.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE TABLE sqlreg.numfid (id bigint PRIMARY KEY, u numeric, c numeric(10,3), n int NOT NULL);
INSERT INTO sqlreg.numfid VALUES
  (1, 1.50, 1.500, 1), (2, 2.00, 2.000, 2), (3, 1e3, 10, 3), (4, 0.10, 0.100, 4),
  (5, 'NaN', 0, 5), (6, -0.0, -0.000, 6),
  (7, 123456789012345678901234567890.1200, 1, 7), (8, 1e-20, 1, 8), (9, -12.30, 5, 9),
  (10, NULL, NULL, 10);
CREATE TABLE sqlreg.numfid_ctl AS SELECT * FROM sqlreg.numfid;

SELECT koldstore.manage_table(
  table_name => 'sqlreg.numfid'::regclass, storage => 'sqlreg_fs', hot_row_limit => 1,
  min_flush_rows => 1, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS numfid_managed;
SELECT sqlreg.flush_table('sqlreg.numfid'::regclass) IS NOT NULL AS numfid_flushed;
SELECT (koldstore.table_status('sqlreg.numfid'::regclass) ->> 'cold_row_count')::int = 10 AS all_cold;

-- cold read
SELECT count(*) AS cold_read_mismatches
FROM sqlreg.numfid a JOIN sqlreg.numfid_ctl b USING (id)
WHERE a.u::text IS DISTINCT FROM b.u::text OR a.c::text IS DISTINCT FROM b.c::text;

-- hydrate-on-write of every cold row
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.numfid SET c = c WHERE id <> 6;
RESET koldstore.hydrate_on_write;
SELECT count(*) AS hydrated_mismatches
FROM sqlreg.numfid a JOIN sqlreg.numfid_ctl b USING (id)
WHERE a.u::text IS DISTINCT FROM b.u::text OR a.c::text IS DISTINCT FROM b.c::text;
SELECT id, u::text AS u, c::text AS c FROM sqlreg.numfid WHERE id IN (1, 2, 7, 8, 9) ORDER BY id;

-- explicit hydrate_pk
SELECT koldstore.hydrate_pk('sqlreg.numfid'::regclass, '{"id": 6}'::jsonb) ->> 'hydrated' AS hydrated_6;
SELECT u::text AS u6, c::text AS c6 FROM sqlreg.numfid WHERE id = 6;
