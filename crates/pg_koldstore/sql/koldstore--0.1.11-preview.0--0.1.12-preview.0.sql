-- koldstore 0.1.11-preview.0 -> 0.1.12-preview.0
--
-- pgrx only generates full install snapshots (koldstore--<version>.sql); this
-- upgrade script is hand-written and installed alongside them by `cargo pgrx
-- install`. It was derived by diffing the pg_catalog contents of a real 0.1.11
-- install against a real 0.1.12 one (functions, tables, columns, constraints,
-- indexes, triggers, ACLs): the catalog tables are unchanged, four functions
-- were added, and manage_table() gained three optional trailing parameters.
--
-- Keep the CREATE FUNCTION statements below identical to what pgrx generates
-- for the same functions in the 0.1.12 snapshot.

-- Cold-row write helpers (upstream issue #122).
CREATE FUNCTION koldstore."hydrate_pk"(
	"table_name" regclass,
	"pk" jsonb
) RETURNS jsonb
STRICT SECURITY DEFINER
LANGUAGE c
AS 'MODULE_PATHNAME', 'hydrate_pk_pg_wrapper';

CREATE FUNCTION koldstore."update_row"(
	"table_name" regclass,
	"pk" jsonb,
	"patch" jsonb,
	"lookup_cold" bool DEFAULT true
) RETURNS jsonb
STRICT SECURITY DEFINER
LANGUAGE c
AS 'MODULE_PATHNAME', 'update_row_pg_wrapper';

CREATE FUNCTION koldstore."delete_row"(
	"table_name" regclass,
	"pk" jsonb,
	"lookup_cold" bool DEFAULT true
) RETURNS jsonb
STRICT SECURITY DEFINER
LANGUAGE c
AS 'MODULE_PATHNAME', 'delete_row_pg_wrapper';

-- Backing function of the per-table BEFORE INSERT guard trigger.
CREATE FUNCTION koldstore."_cold_insert_guard_check"(
	"table_oid" oid,
	"row" jsonb,
	"table_name" TEXT
) RETURNS void
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'cold_insert_guard_check_pg_wrapper';

-- Installs the BEFORE INSERT guard trigger on tables managed under 0.1.11.
CREATE FUNCTION koldstore."internal_attach_insert_guards"() RETURNS bigint
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'internal_attach_insert_guards_wrapper';

-- manage_table() gained the parquet_* tuning parameters. The argument list
-- changed, so it cannot be replaced in place.
DROP FUNCTION koldstore."manage_table"(regclass, text, bigint, bigint, bigint, text, text, text, text, bigint, boolean, text, text[], text[]);
CREATE FUNCTION koldstore."manage_table"(
	"table_name" regclass,
	"storage" TEXT,
	"hot_row_limit" bigint,
	"min_flush_rows" bigint DEFAULT 1000,
	"max_rows_per_file" bigint DEFAULT 1000,
	"table_type" TEXT DEFAULT 'shared',
	"scope_column" TEXT DEFAULT NULL,
	"migration_order_by" TEXT DEFAULT NULL,
	"compression" TEXT DEFAULT NULL,
	"target_file_size_mb" bigint DEFAULT NULL,
	"auto_flush" bool DEFAULT true,
	"segment_order_column" TEXT DEFAULT NULL,
	"pruning_columns" TEXT[] DEFAULT NULL,
	"bloom_filter_columns" TEXT[] DEFAULT NULL,
	"parquet_row_group_size" bigint DEFAULT NULL,
	"parquet_data_page_row_count_limit" bigint DEFAULT NULL,
	"parquet_bloom_filter_fpp" double precision DEFAULT NULL
) RETURNS uuid
SECURITY DEFINER
LANGUAGE c
AS 'MODULE_PATHNAME', 'manage_table_pg_wrapper';

SELECT koldstore."internal_attach_insert_guards"();

-- Objects created while an extension script runs become members of the
-- extension, but the per-table guard trigger functions belong to their managed
-- table (manage_table creates them outside any script, so unmanage_table can
-- drop them). Detach the ones just created so they behave the same way.
DO $koldstore_detach$
DECLARE
  guard regprocedure;
BEGIN
  FOR guard IN
    SELECT p.oid::regprocedure
    FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_depend d ON d.classid = 'pg_catalog.pg_proc'::regclass AND d.objid = p.oid
      AND d.deptype = 'e'
    JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid AND e.extname = 'koldstore'
    WHERE p.pronamespace = 'koldstore'::regnamespace
      AND p.proname LIKE '%\_\_cold\_ins\_guard'
  LOOP
    EXECUTE format('ALTER EXTENSION koldstore DROP FUNCTION %s', guard);
  END LOOP;
END
$koldstore_detach$;

-- Releases before this one left a table's guard functions (insert guard and
-- mirror primary-key guard) behind when the table or its schema was dropped
-- (only unmanage_table removed them). Sweep the ones no trigger uses.
DO $koldstore_sweep$
DECLARE
  orphan regprocedure;
BEGIN
  FOR orphan IN
    SELECT p.oid::regprocedure
    FROM pg_catalog.pg_proc p
    WHERE p.pronamespace = 'koldstore'::regnamespace
      AND (p.proname LIKE '%\_\_cold\_ins\_guard' OR p.proname LIKE '%\_\_cl\_pk\_guard')
      AND p.prorettype = 'pg_catalog.trigger'::regtype
      AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgfoid = p.oid)
  LOOP
    EXECUTE format('DROP FUNCTION %s', orphan);
  END LOOP;
END
$koldstore_sweep$;
