-- User-defined enums are stored as their label and read back as the column's own enum type.

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

CREATE TYPE sqlreg.mood AS ENUM ('sad', 'ok', 'happy', 'it''s "odd"');
CREATE TYPE sqlreg.level AS ENUM ('low', 'high');
CREATE TABLE sqlreg.en (id bigint PRIMARY KEY, m sqlreg.mood, l sqlreg.level NOT NULL DEFAULT 'low', tag text);
INSERT INTO sqlreg.en VALUES
  (1, 'sad', 'low', 'a'), (2, 'ok', 'high', 'b'), (3, 'happy', 'low', 'c'), (4, 'it''s "odd"', 'high', 'd'), (5, NULL, 'low', 'nulls');
INSERT INTO sqlreg.en SELECT g, (enum_range(NULL::sqlreg.mood))[1 + g % 4], (enum_range(NULL::sqlreg.level))[1 + g % 2], 'bulk' FROM generate_series(6, 60) g;
CREATE TABLE sqlreg.en_ctl AS SELECT * FROM sqlreg.en;
SELECT sqlreg.fingerprint('sqlreg.en', 'id') AS before_flush \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.en'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS en_managed;
SELECT sqlreg.flush_table('sqlreg.en'::regclass) IS NOT NULL AS en_flushed;
SELECT sqlreg.fingerprint('sqlreg.en', 'id') = :'before_flush' AS same_after_flush;
SELECT pg_typeof(m)::text AS m_type, pg_typeof(l)::text AS l_type FROM sqlreg.en WHERE id = 1;

-- enum ordering follows the declaration order, not the label text
SELECT count(*) AS gt_ok FROM sqlreg.en WHERE m > 'ok';
SELECT count(*) AS gt_ok_ctl FROM sqlreg.en_ctl WHERE m > 'ok';
SELECT count(*) AS eq_happy FROM sqlreg.en WHERE m = 'happy';
SELECT count(*) AS eq_happy_ctl FROM sqlreg.en_ctl WHERE m = 'happy';
SELECT count(*) AS in_two FROM sqlreg.en WHERE m IN ('sad', 'it''s "odd"');
SELECT count(*) AS in_two_ctl FROM sqlreg.en_ctl WHERE m IN ('sad', 'it''s "odd"');
SELECT id FROM sqlreg.en WHERE m IS NULL;
SELECT m, count(*) FROM sqlreg.en GROUP BY m ORDER BY m;
SELECT max(m) AS max_m, min(m) AS min_m FROM sqlreg.en;

-- a label added after the flush is usable on new rows and old rows are unchanged
ALTER TYPE sqlreg.mood ADD VALUE 'ecstatic' AFTER 'happy';
ALTER TYPE sqlreg.mood ADD VALUE 'gloomy' BEFORE 'sad';
INSERT INTO sqlreg.en VALUES (100, 'ecstatic', 'high', 'new'), (101, 'gloomy', 'low', 'new');
INSERT INTO sqlreg.en_ctl VALUES (100, 'ecstatic', 'high', 'new'), (101, 'gloomy', 'low', 'new');
SELECT id, m FROM sqlreg.en WHERE m > 'happy' ORDER BY id LIMIT 3;

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.en SET m = 'happy', tag = 'changed' WHERE id IN (1, 2, 5);
UPDATE sqlreg.en_ctl SET m = 'happy', tag = 'changed' WHERE id IN (1, 2, 5);
UPDATE sqlreg.en SET tag = 'touched' WHERE id IN (3, 4);
UPDATE sqlreg.en_ctl SET tag = 'touched' WHERE id IN (3, 4);
DELETE FROM sqlreg.en WHERE id = 6;
DELETE FROM sqlreg.en_ctl WHERE id = 6;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.en', 'id') = sqlreg.fingerprint('sqlreg.en_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.en'::regclass, true) IS NOT NULL AS en_reflushed;
SELECT sqlreg.fingerprint('sqlreg.en', 'id') = sqlreg.fingerprint('sqlreg.en_ctl', 'id') AS rows_match_after_reflush;
SELECT koldstore.unmanage_table('sqlreg.en'::regclass, true, true) IS NOT NULL AS en_unmanaged;
SELECT sqlreg.fingerprint('sqlreg.en', 'id') = sqlreg.fingerprint('sqlreg.en_ctl', 'id') AS rows_match_after_unmanage;

-- an enum primary key is refused (the flush keyset cannot page it); the enum column itself is fine
CREATE TABLE sqlreg.en_pk (m sqlreg.mood PRIMARY KEY, v int NOT NULL);
SELECT koldstore.manage_table(
  table_name => 'sqlreg.en_pk'::regclass, storage => 'sqlreg_fs', hot_row_limit => 2, min_flush_rows => 1,
  max_rows_per_file => 20, migration_order_by => 'v', auto_flush => false
) IS NOT NULL AS pk_managed;

-- enum arrays are still refused
CREATE TABLE sqlreg.en_arr (id bigint PRIMARY KEY, ms sqlreg.level[]);
SELECT koldstore.manage_table(
  table_name => 'sqlreg.en_arr'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5, min_flush_rows => 1,
  max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS arr_managed;
