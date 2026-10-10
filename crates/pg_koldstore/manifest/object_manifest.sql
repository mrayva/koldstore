-- One text line per SQL object the koldstore extension owns in the current database: its functions
-- (signature, result, security, C symbol) and the columns of its catalog tables. Objects created for a
-- managed table (mirror tables, trigger functions) are not extension members and are not listed.
--
-- Shared by koldstore.validate_sql_objects() (compared with expected_objects.txt, compiled into the
-- library) and scripts/check-sql-drift.sh (compared with a freshly created database).
WITH ext AS (
    SELECT oid FROM pg_extension WHERE extname = 'koldstore'
), members AS (
    SELECT d.classid, d.objid
    FROM pg_depend d JOIN ext ON d.refclassid = 'pg_extension'::regclass AND d.refobjid = ext.oid
    WHERE d.deptype = 'e'
)
SELECT 'function ' || p.proname || '(' || pg_get_function_arguments(p.oid) || ') returns '
       || pg_get_function_result(p.oid)
       || CASE WHEN p.prosecdef THEN ' security definer' ELSE '' END
       || ' [' || p.prosrc || ']' AS line
FROM pg_proc p JOIN members m ON m.classid = 'pg_proc'::regclass AND m.objid = p.oid
WHERE p.prolang = (SELECT oid FROM pg_language WHERE lanname = 'c')
UNION ALL
SELECT 'column ' || c.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod)
       || CASE WHEN a.attnotnull THEN ' not null' ELSE '' END
FROM pg_class c
JOIN members m ON m.classid = 'pg_class'::regclass AND m.objid = c.oid
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
WHERE c.relkind IN ('r', 'p')
ORDER BY 1
