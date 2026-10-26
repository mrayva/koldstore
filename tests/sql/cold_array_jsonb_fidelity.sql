-- Array bounds/dimensions and jsonb operators must behave identically on cold rows and on an unmanaged
-- copy of the table. Audit prompted by pgEdge/coldfront#121 (array lower bounds renumbered from 1, numeric
-- NaN inside arrays) and #124 (jsonb operators failing on cold data, column read back as json).
--
-- Oracle: an unmanaged copy of each table.

\set VERBOSITY terse
\set ON_ERROR_STOP off

-- ------------------------------------------------ arrays
-- Only one-dimensional text[] is supported; every other array type is refused up front.
CREATE TABLE sqlreg.arr_int (id bigint PRIMARY KEY, a int[], n int NOT NULL);
SELECT koldstore.manage_table(
  table_name => 'sqlreg.arr_int'::regclass, storage => 'sqlreg_fs', hot_row_limit => 1,
  min_flush_rows => 1, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS int_array_managed;
CREATE TABLE sqlreg.arr_num (id bigint PRIMARY KEY, a numeric[], n int NOT NULL);
SELECT koldstore.manage_table(
  table_name => 'sqlreg.arr_num'::regclass, storage => 'sqlreg_fs', hot_row_limit => 1,
  min_flush_rows => 1, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS numeric_array_managed;

-- text[] keeps its values, NULL elements, and its lower bound (coldfront#121: renumbered from 1)
CREATE TABLE sqlreg.arrfid (id bigint PRIMARY KEY, ta text[], n int NOT NULL);
INSERT INTO sqlreg.arrfid VALUES
  (1, '[0:1]={7,8}', 1),
  (2, '{}', 2),
  (3, '{a,NULL,c}', 3),
  (4, '[-3:-2]={x,y}', 4),
  (5, '{"a b","c,d","NULL","",NULL,"\"q\"","\\"}', 5),
  (6, '[0:2]={x,y,z}', 6),
  (7, '[5:5]={only}', 7),
  (8, NULL, 8);
CREATE TABLE sqlreg.arrfid_ctl AS SELECT * FROM sqlreg.arrfid;

SELECT koldstore.manage_table(
  table_name => 'sqlreg.arrfid'::regclass, storage => 'sqlreg_fs', hot_row_limit => 1,
  min_flush_rows => 1, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS arrfid_managed;
SELECT sqlreg.flush_table('sqlreg.arrfid'::regclass) IS NOT NULL AS arrfid_flushed;

SELECT a.id, a.ta::text AS cold, b.ta::text AS expected, array_dims(a.ta) AS cold_dims, array_dims(b.ta) AS expected_dims
FROM sqlreg.arrfid a JOIN sqlreg.arrfid_ctl b USING (id)
WHERE a.ta::text IS DISTINCT FROM b.ta::text OR array_dims(a.ta) IS DISTINCT FROM array_dims(b.ta)
ORDER BY a.id;
SELECT count(*) AS arrfid_rows FROM sqlreg.arrfid;

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.arrfid SET n = n;
RESET koldstore.hydrate_on_write;
-- Hydration goes through to_jsonb(row) -> jsonb_populate_recordset, and a JSON array has no lower bound:
-- elements, NULL elements and escaping survive, explicit bounds are renumbered from 1 (documented limitation).
SELECT a.id, a.ta::text AS hydrated, b.ta::text AS expected
FROM sqlreg.arrfid a JOIN sqlreg.arrfid_ctl b USING (id)
WHERE (SELECT array_agg(e) FROM unnest(a.ta) e) IS DISTINCT FROM (SELECT array_agg(e) FROM unnest(b.ta) e)
   OR cardinality(a.ta) IS DISTINCT FROM cardinality(b.ta)
ORDER BY a.id;
SELECT id, array_dims(ta) AS hydrated_dims FROM sqlreg.arrfid WHERE id IN (1, 6) ORDER BY id;

-- a multi-dimensional text[] must not be silently flattened
CREATE TABLE sqlreg.arr_md (id bigint PRIMARY KEY, ta text[], n int NOT NULL);
SELECT koldstore.manage_table(
  table_name => 'sqlreg.arr_md'::regclass, storage => 'sqlreg_fs', hot_row_limit => 1,
  min_flush_rows => 1, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS md_managed;
INSERT INTO sqlreg.arr_md VALUES (1, '{{a,b},{c,d}}', 1), (2, '{x}', 2);
SELECT sqlreg.flush_table('sqlreg.arr_md'::regclass) IS NOT NULL AS md_flush_returned;
SELECT id, ta::text AS md_value, array_dims(ta) AS md_dims FROM sqlreg.arr_md ORDER BY id;
SELECT (koldstore.table_status('sqlreg.arr_md'::regclass) ->> 'cold_row_count')::int AS md_cold_rows;

-- ------------------------------------------------ jsonb
CREATE TABLE sqlreg.jsfid (id bigint PRIMARY KEY, data jsonb, n int NOT NULL);
INSERT INTO sqlreg.jsfid VALUES
  (1, '{"k":1,"a":[1,2]}', 1),
  (2, '{"k":"v","a":[{"b":2}],"z":null}', 2),
  (3, '[1,2,{"k":3}]', 3),
  (4, '"scalar"', 4),
  (5, 'null', 5),
  (6, '{"num":1.50,"big":123456789012345678901234567890.12,"e":1e3,"neg":-0.0}', 6),
  (7, '{"u":"héllo é 😀","esc":"a\"b\\c\nd"}', 7),
  (8, '{}', 8),
  (9, NULL, 9);
CREATE TABLE sqlreg.jsfid_ctl AS SELECT * FROM sqlreg.jsfid;

SELECT koldstore.manage_table(
  table_name => 'sqlreg.jsfid'::regclass, storage => 'sqlreg_fs', hot_row_limit => 1,
  min_flush_rows => 1, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS jsfid_managed;
SELECT sqlreg.flush_table('sqlreg.jsfid'::regclass) IS NOT NULL AS jsfid_flushed;
SELECT (koldstore.table_status('sqlreg.jsfid'::regclass) ->> 'cold_row_count')::int = 9 AS jsfid_all_cold;

SELECT pg_typeof(data)::text AS cold_column_type FROM sqlreg.jsfid LIMIT 1;

-- every operator, one expression at a time so an error is compared too
CREATE FUNCTION sqlreg.js_probe(t regclass) RETURNS TABLE (r text) LANGUAGE plpgsql AS $f$
DECLARE
  ex text; rid bigint; v text;
  exprs text[] := ARRAY[
    'data::text', 'jsonb_typeof(data)', 'data->''k''', 'data->>''k''', 'data->0', 'data->>0',
    'data#>''{a,0}''', 'data#>>''{a,0}''', 'data#>''{a,0,b}''', 'data ? ''k''', 'data ?| array[''k'',''z'']',
    'data ?& array[''k'',''a'']', 'data @> ''{"k":1}''::jsonb', 'data <@ ''{"k":1,"a":[1,2],"x":1}''::jsonb',
    'data @> ''[1]''::jsonb', 'data @? ''$.a[*] ? (@ > 1)''', 'data @@ ''$.k == 1''',
    'jsonb_path_query_array(data, ''$.**'')', 'data || ''{"x":1}''::jsonb', 'data - ''k''::text', 'data #- ''{a,0}''',
    'jsonb_array_length(data)', '(SELECT string_agg(key, '','' ORDER BY key) FROM jsonb_object_keys(data) key)',
    'data = ''{"k":1,"a":[1,2]}''::jsonb', 'data IS NULL', 'to_jsonb(data)::text', 'data::json::text',
    'jsonb_pretty(data)', '(data->''num'')::numeric', '(data->>''big'')::numeric', '(data->''e'')::int'];
BEGIN
  FOR rid IN EXECUTE format('SELECT id FROM %s ORDER BY id', t) LOOP
    FOREACH ex IN ARRAY exprs LOOP
      BEGIN
        EXECUTE format('SELECT (%s)::text FROM %s WHERE id = %s', ex, t, rid) INTO v;
        r := rid || ' | ' || ex || ' | ' || coalesce(v, '<null>');
      EXCEPTION WHEN OTHERS THEN
        r := rid || ' | ' || ex || ' | ERR ' || SQLSTATE;
      END;
      RETURN NEXT;
    END LOOP;
  END LOOP;
END $f$;
SELECT count(*) AS probe_rows FROM sqlreg.js_probe('sqlreg.jsfid');
SELECT r AS probe_mismatch FROM (
  SELECT r FROM sqlreg.js_probe('sqlreg.jsfid') EXCEPT SELECT r FROM sqlreg.js_probe('sqlreg.jsfid_ctl')) d ORDER BY 1;
SELECT r AS control_only FROM (
  SELECT r FROM sqlreg.js_probe('sqlreg.jsfid_ctl') EXCEPT SELECT r FROM sqlreg.js_probe('sqlreg.jsfid')) d ORDER BY 1;

-- operators as WHERE predicates, and an index-free ordering by a jsonb expression
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.jsfid WHERE data ? 'k')
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.jsfid_ctl WHERE data ? 'k') AS where_exists_ok,
       (SELECT array_agg(id ORDER BY id) FROM sqlreg.jsfid WHERE data @> '{"k":1}')
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.jsfid_ctl WHERE data @> '{"k":1}') AS where_contains_ok,
       (SELECT array_agg(id ORDER BY id) FROM sqlreg.jsfid WHERE (data::jsonb) ? 'k')
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.jsfid_ctl WHERE (data::jsonb) ? 'k') AS where_cast_ok,
       (SELECT array_agg(id ORDER BY data->>'k', id) FROM sqlreg.jsfid)
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY data->>'k', id) FROM sqlreg.jsfid_ctl) AS order_by_expr_ok;

-- hydrate-on-write round trip
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.jsfid SET n = n;
RESET koldstore.hydrate_on_write;
-- A JSON null in a jsonb column hydrates as SQL NULL (row JSON cannot tell the two apart); every other
-- value, including bare JSON strings, must be unchanged.
SELECT r AS hydrated_probe_mismatch FROM (
  SELECT r FROM sqlreg.js_probe('sqlreg.jsfid') WHERE r NOT LIKE '5 | %'
  EXCEPT SELECT r FROM sqlreg.js_probe('sqlreg.jsfid_ctl')) d ORDER BY 1;
SELECT id, data IS NULL AS sql_null, jsonb_typeof(data) AS json_type FROM sqlreg.jsfid WHERE id = 5;
