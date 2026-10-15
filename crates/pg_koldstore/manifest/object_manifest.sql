-- One row per SQL object the koldstore extension owns in the current database: every function
-- (signature, result, security, and either the C symbol or a checksum of the body) and the columns of
-- its catalog tables. Objects created for a managed table (mirror tables, trigger functions) are not
-- extension members and are not listed.
--
-- Columns: line (the text compared with expected_objects.txt), kind ('function' | 'column'), ref_oid
-- (the function's, or the column's table's, oid), ref_num (the column's attnum, else 0) and drop_sql
-- (statements that remove a function from its extension and database; NULL for columns).
--
-- Shared by koldstore.validate_sql_objects() / koldstore.sync_sql_objects() (compiled into the library)
-- and scripts/check-sql-drift.sh (which also generates expected_objects.txt and repair_objects.json
-- from a freshly created database).
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
       || ' [' || CASE WHEN l.lanname = 'c' THEN p.prosrc
                       ELSE l.lanname || ':' || left(md5(p.prosrc), 12) END || ']' AS line,
       'function'::text AS kind,
       p.oid AS ref_oid,
       0 AS ref_num,
       format('ALTER EXTENSION koldstore DROP FUNCTION %I.%I(%s); DROP FUNCTION %I.%I(%s)',
              n.nspname, p.proname, pg_get_function_identity_arguments(p.oid),
              n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)) AS drop_sql
FROM pg_proc p
JOIN members m ON m.classid = 'pg_proc'::regclass AND m.objid = p.oid
JOIN pg_namespace n ON n.oid = p.pronamespace
JOIN pg_language l ON l.oid = p.prolang
WHERE p.prokind = 'f'
UNION ALL
SELECT 'column ' || c.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod)
       || CASE WHEN a.attnotnull THEN ' not null' ELSE '' END,
       'column', c.oid, a.attnum::int, NULL
FROM pg_class c
JOIN members m ON m.classid = 'pg_class'::regclass AND m.objid = c.oid
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
WHERE c.relkind IN ('r', 'p')
ORDER BY 1
