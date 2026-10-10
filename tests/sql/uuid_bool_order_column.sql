-- uuid and boolean were rejected as the order column (migration_order_by) although both have a Sort Key
-- V1 encoding. A time-ordered uuid (v7) is a natural order column; a boolean is allowed too (rows sort
-- false before true, then by primary key).
--
-- Oracle: an unmanaged copy of each table.

\set VERBOSITY terse
\set ON_ERROR_STOP off

CREATE FUNCTION sqlreg.fingerprint(rel regclass, ord text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE result text;
BEGIN
  EXECUTE format('SELECT md5(coalesce(string_agg(t::text, ''|'' ORDER BY %s), '''')) FROM %s t', ord, rel) INTO result;
  RETURN result;
END
$$;

CREATE TABLE sqlreg.uo (id bigint PRIMARY KEY, u uuid NOT NULL, v text NOT NULL DEFAULT 'x');
INSERT INTO sqlreg.uo SELECT g, ('00000000-0000-7000-8000-' || lpad(to_hex(g), 12, '0'))::uuid, 'x' FROM generate_series(1, 2500) g;
CREATE TABLE sqlreg.bo (id bigint PRIMARY KEY, flag boolean NOT NULL, v text NOT NULL DEFAULT 'x');
INSERT INTO sqlreg.bo SELECT g, g % 3 = 0, 'x' FROM generate_series(1, 2500) g;
-- the uuid primary key is its own order column
CREATE TABLE sqlreg.uk (id uuid PRIMARY KEY, v text NOT NULL DEFAULT 'x');
INSERT INTO sqlreg.uk SELECT ('00000000-0000-7000-8000-' || lpad(to_hex(g), 12, '0'))::uuid, 'x' FROM generate_series(1, 2500) g;
CREATE TABLE sqlreg.uo_ctl AS SELECT * FROM sqlreg.uo;
CREATE TABLE sqlreg.bo_ctl AS SELECT * FROM sqlreg.bo;
CREATE TABLE sqlreg.uk_ctl AS SELECT * FROM sqlreg.uk;

SELECT koldstore.manage_table(table_name => 'sqlreg.uo'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
         min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'u', auto_flush => false) IS NOT NULL AS uo_managed;
SELECT koldstore.manage_table(table_name => 'sqlreg.bo'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
         min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'flag', auto_flush => false) IS NOT NULL AS bo_managed;
SELECT koldstore.manage_table(table_name => 'sqlreg.uk'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
         min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'id', auto_flush => false) IS NOT NULL AS uk_managed;
SELECT t, sqlreg.flush_table(t::regclass) IS NOT NULL AS flushed FROM unnest(ARRAY['sqlreg.uo', 'sqlreg.bo', 'sqlreg.uk']) t;
SELECT t, (koldstore.table_status(t::regclass) ->> 'cold_row_count')::int >= 2400 AS mostly_cold
FROM unnest(ARRAY['sqlreg.uo', 'sqlreg.bo', 'sqlreg.uk']) t;

SELECT sqlreg.fingerprint('sqlreg.uo', 'id') = sqlreg.fingerprint('sqlreg.uo_ctl', 'id') AS uo_rows,
       sqlreg.fingerprint('sqlreg.bo', 'id') = sqlreg.fingerprint('sqlreg.bo_ctl', 'id') AS bo_rows,
       sqlreg.fingerprint('sqlreg.uk', 'id') = sqlreg.fingerprint('sqlreg.uk_ctl', 'id') AS uk_rows;

-- ordering and range predicates on the order column
SELECT (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.uo ORDER BY u LIMIT 5) a) = (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.uo_ctl ORDER BY u LIMIT 5) b) AS uo_asc,
       (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.uo ORDER BY u DESC LIMIT 5) a) = (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.uo_ctl ORDER BY u DESC LIMIT 5) b) AS uo_desc,
       (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.uk ORDER BY id LIMIT 5) a) = (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.uk_ctl ORDER BY id LIMIT 5) b) AS uk_asc,
       (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.uk ORDER BY id DESC LIMIT 5) a) = (SELECT array_agg(id) FROM (SELECT id FROM sqlreg.uk_ctl ORDER BY id DESC LIMIT 5) b) AS uk_desc;
SELECT (SELECT count(*) FROM sqlreg.uo WHERE u = '00000000-0000-7000-8000-0000000003e8') AS uo_hit,
       (SELECT count(*) FROM sqlreg.uo WHERE u = '00000000-0000-7000-8000-ffffffffffff') AS uo_absent,
       (SELECT count(*) FROM sqlreg.uo WHERE u > '00000000-0000-7000-8000-0000000007d0') AS uo_range,
       (SELECT count(*) FROM sqlreg.uo_ctl WHERE u > '00000000-0000-7000-8000-0000000007d0') AS uo_range_ctl;
SELECT (SELECT count(*) FROM sqlreg.bo WHERE flag) AS flagged, (SELECT count(*) FROM sqlreg.bo_ctl WHERE flag) AS flagged_ctl,
       (SELECT count(*) FROM sqlreg.bo WHERE NOT flag) AS unflagged, (SELECT count(*) FROM sqlreg.bo WHERE id = 1000) AS by_id,
       (SELECT count(*) FROM sqlreg.bo WHERE flag AND id > 2000) AS combined, (SELECT count(*) FROM sqlreg.bo_ctl WHERE flag AND id > 2000) AS combined_ctl;

-- cold rows change through hydrate-on-write and survive a re-flush
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.uo SET v = 'u' WHERE id = 1000;
UPDATE sqlreg.uo_ctl SET v = 'u' WHERE id = 1000;
DELETE FROM sqlreg.uo WHERE id = 1500;
DELETE FROM sqlreg.uo_ctl WHERE id = 1500;
UPDATE sqlreg.bo SET v = 'u' WHERE id = 900;
UPDATE sqlreg.bo_ctl SET v = 'u' WHERE id = 900;
DELETE FROM sqlreg.bo WHERE id = 901;
DELETE FROM sqlreg.bo_ctl WHERE id = 901;
UPDATE sqlreg.uk SET v = 'u' WHERE id = '00000000-0000-7000-8000-0000000003e8';
UPDATE sqlreg.uk_ctl SET v = 'u' WHERE id = '00000000-0000-7000-8000-0000000003e8';
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.uo', 'id') = sqlreg.fingerprint('sqlreg.uo_ctl', 'id') AS uo_rows_after_dml,
       sqlreg.fingerprint('sqlreg.bo', 'id') = sqlreg.fingerprint('sqlreg.bo_ctl', 'id') AS bo_rows_after_dml,
       sqlreg.fingerprint('sqlreg.uk', 'id') = sqlreg.fingerprint('sqlreg.uk_ctl', 'id') AS uk_rows_after_dml;
SELECT sqlreg.flush_table(t::regclass, true) IS NOT NULL AS reflushed FROM unnest(ARRAY['sqlreg.uo', 'sqlreg.bo', 'sqlreg.uk']) t;
SELECT sqlreg.fingerprint('sqlreg.uo', 'id') = sqlreg.fingerprint('sqlreg.uo_ctl', 'id') AS uo_rows_after_reflush,
       sqlreg.fingerprint('sqlreg.bo', 'id') = sqlreg.fingerprint('sqlreg.bo_ctl', 'id') AS bo_rows_after_reflush,
       sqlreg.fingerprint('sqlreg.uk', 'id') = sqlreg.fingerprint('sqlreg.uk_ctl', 'id') AS uk_rows_after_reflush;
