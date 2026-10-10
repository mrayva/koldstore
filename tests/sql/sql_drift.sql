-- koldstore.validate_sql_objects(): a database created from this build matches the manifest compiled
-- into the library, and drift in a function signature or a catalog column is reported (and fixed by
-- restoring the object). A stale crates/pg_koldstore/manifest/expected_objects.txt fails the first
-- check; regenerate it with scripts/check-sql-drift.sh --update-expected.

\set VERBOSITY terse
\set ON_ERROR_STOP off

SELECT (r ->> 'ok')::boolean AS clean, jsonb_array_length(r -> 'missing') AS missing, jsonb_array_length(r -> 'unexpected') AS unexpected,
       r ->> 'library_version' = r ->> 'extension_version' AS versions_match
FROM (SELECT koldstore.validate_sql_objects() AS r) s;

-- a catalog column the library expects is gone
BEGIN;
ALTER TABLE koldstore.cold_segment_index RENAME COLUMN value_summary TO value_summary_old;
SELECT (r ->> 'ok')::boolean AS clean_with_renamed_column,
       r -> 'missing' ->> 0 AS missing_entry, r -> 'unexpected' ->> 0 AS unexpected_entry,
       r ->> 'hint' IS NOT NULL AS has_hint
FROM (SELECT koldstore.validate_sql_objects() AS r) s;
ROLLBACK;

-- a function whose signature differs from the one the library reads (the 17- vs 18-argument case)
BEGIN;
ALTER FUNCTION koldstore.unmanage_table(regclass, boolean, boolean) RENAME TO unmanage_table_old;
SELECT (r ->> 'ok')::boolean AS clean_with_renamed_function,
       jsonb_array_length(r -> 'missing') AS missing, jsonb_array_length(r -> 'unexpected') AS unexpected
FROM (SELECT koldstore.validate_sql_objects() AS r) s;
ROLLBACK;

SELECT (koldstore.validate_sql_objects() ->> 'ok')::boolean AS clean_again;
