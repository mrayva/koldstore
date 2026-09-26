-- Arbitrary identifiers: upper/mixed case, reserved words, spaces, embedded
-- quotes and dots, slashes, non-ASCII letters, a leading digit, leading and
-- trailing blanks, and very long names, in schema, table and column names.
--
-- Each variant is managed, flushed to cold storage, read back through the merge
-- scan (plain and ordered), written through every write guard and the explicit
-- cold-row functions, evolved with ALTER TABLE, then unmanaged, one statement
-- (= one transaction) at a time.

\set VERBOSITY terse
\set ON_ERROR_STOP off

-- try(stmt): "ok: <rows>" | "REJECTED insert" | "REJECTED write pk=<key>" | "ERROR: ..."
-- try(stmt, true): same, but a koldstore rejection keeps its complete message.
CREATE FUNCTION sqlreg.try(stmt text, keep_message boolean DEFAULT false) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  n bigint;
  msg text;
BEGIN
  EXECUTE stmt;
  IF stmt ~* '^\s*explain' THEN
    RETURN 'ok (explain)';
  END IF;
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN 'ok: ' || n;
EXCEPTION WHEN OTHERS THEN
  msg := split_part(SQLERRM, E'\n', 1);
  IF keep_message THEN
    RETURN 'ERROR: ' || msg;
  ELSIF msg LIKE 'koldstore: refusing INSERT%' THEN
    RETURN 'REJECTED insert';
  ELSIF msg LIKE 'koldstore: refusing this UPDATE/DELETE on managed table%' THEN
    RETURN 'REJECTED scan matches=' || substring(msg from 'also matches ([0-9]+) cold');
  ELSIF msg LIKE 'koldstore: refusing this MERGE on managed table%' THEN
    RETURN 'REJECTED merge';
  ELSIF msg LIKE 'koldstore: refusing this UPDATE/DELETE/MERGE%' THEN
    RETURN 'REJECTED write pk=' || substring(msg from 'primary key (\{[^}]*\})');
  END IF;
  RETURN 'ERROR: ' || msg;
END
$$;

-- Committed mirror work applied, so reads below see completed masking.
CREATE FUNCTION sqlreg.settle() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM koldstore.wait_for_async_mirror();
END
$$;

-- val(query): the first column of the first row as text, or "ERROR: ..."
CREATE FUNCTION sqlreg.val(stmt text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE result text;
BEGIN
  EXECUTE stmt INTO result;
  RETURN coalesce(result, '(null)');
EXCEPTION WHEN OTHERS THEN
  RETURN 'ERROR: ' || split_part(SQLERRM, E'\n', 1);
END
$$;

CREATE FUNCTION sqlreg.try_manage(rel regclass, order_col text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE leaked int;
BEGIN
  PERFORM koldstore.manage_table(table_name => rel, storage => 'sqlreg_fs', hot_row_limit => 10,
    min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => order_col, auto_flush => false);
  RETURN 'managed';
EXCEPTION WHEN OTHERS THEN
  SELECT count(*) INTO leaked FROM koldstore.schemas WHERE table_oid = rel::oid;
  RETURN 'ERROR: ' || split_part(SQLERRM, E'\n', 1) || CASE WHEN leaked > 0 THEN ' [LEAKED CATALOG ROW]' ELSE '' END;
END
$$;

CREATE SCHEMA "we.ird";
CREATE SCHEMA "Odd Schema";

-- mixed_case
CREATE TABLE sqlreg."MixedCase" ("OrderId" bigint PRIMARY KEY, "SomeValue" text);
INSERT INTO sqlreg."MixedCase" SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg."MixedCase"$q$::regclass, $q$OrderId$q$) AS mixed_case_manage;
SELECT sqlreg.flush_table($q$sqlreg."MixedCase"$q$::regclass) IS NOT NULL AS mixed_case_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."MixedCase"$q$) AS mixed_case_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT "OrderId" AS v FROM sqlreg."MixedCase" ORDER BY "OrderId" DESC LIMIT 2) s$q$) AS mixed_case_ordered_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg."MixedCase" ("OrderId", "SomeValue") VALUES (1, 'dup')$q$) AS mixed_case_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg."MixedCase" SET "SomeValue" = 'x' WHERE "OrderId" = 2$q$) AS mixed_case_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg."MixedCase" WHERE "OrderId" > 0$q$) AS mixed_case_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$sqlreg."MixedCase"$q2$::regclass, jsonb_build_object($q3$OrderId$q3$, 2), jsonb_build_object($q3$SomeValue$q3$, 'via_fn'))$q$) AS mixed_case_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."MixedCase" WHERE "SomeValue" = 'via_fn'$q$) AS mixed_case_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$sqlreg."MixedCase"$q2$::regclass, jsonb_build_object($q3$OrderId$q3$, 3))$q$) AS mixed_case_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."MixedCase"$q$) AS mixed_case_after_delete_row;
SELECT koldstore.unmanage_table($q$sqlreg."MixedCase"$q$::regclass, true) IS NOT NULL AS mixed_case_unmanaged;
SELECT count(*) AS mixed_case_rows_after_unmanage FROM sqlreg."MixedCase";

-- reserved
CREATE TABLE sqlreg."select" ("user" bigint PRIMARY KEY, "order" text);
INSERT INTO sqlreg."select" SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg."select"$q$::regclass, $q$user$q$) AS reserved_manage;
SELECT sqlreg.flush_table($q$sqlreg."select"$q$::regclass) IS NOT NULL AS reserved_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."select"$q$) AS reserved_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT "user" AS v FROM sqlreg."select" ORDER BY "user" DESC LIMIT 2) s$q$) AS reserved_ordered_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg."select" ("user", "order") VALUES (1, 'dup')$q$) AS reserved_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg."select" SET "order" = 'x' WHERE "user" = 2$q$) AS reserved_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg."select" WHERE "user" > 0$q$) AS reserved_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$sqlreg."select"$q2$::regclass, jsonb_build_object($q3$user$q3$, 2), jsonb_build_object($q3$order$q3$, 'via_fn'))$q$) AS reserved_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."select" WHERE "order" = 'via_fn'$q$) AS reserved_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$sqlreg."select"$q2$::regclass, jsonb_build_object($q3$user$q3$, 3))$q$) AS reserved_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."select"$q$) AS reserved_after_delete_row;
SELECT koldstore.unmanage_table($q$sqlreg."select"$q$::regclass, true) IS NOT NULL AS reserved_unmanaged;
SELECT count(*) AS reserved_rows_after_unmanage FROM sqlreg."select";

-- spaces
CREATE TABLE sqlreg."Odd Name" ("Order Id" bigint PRIMARY KEY, "Some Value" text);
INSERT INTO sqlreg."Odd Name" SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg."Odd Name"$q$::regclass, $q$Order Id$q$) AS spaces_manage;
SELECT sqlreg.flush_table($q$sqlreg."Odd Name"$q$::regclass) IS NOT NULL AS spaces_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."Odd Name"$q$) AS spaces_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT "Order Id" AS v FROM sqlreg."Odd Name" ORDER BY "Order Id" DESC LIMIT 2) s$q$) AS spaces_ordered_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg."Odd Name" ("Order Id", "Some Value") VALUES (1, 'dup')$q$) AS spaces_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg."Odd Name" SET "Some Value" = 'x' WHERE "Order Id" = 2$q$) AS spaces_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg."Odd Name" WHERE "Order Id" > 0$q$) AS spaces_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$sqlreg."Odd Name"$q2$::regclass, jsonb_build_object($q3$Order Id$q3$, 2), jsonb_build_object($q3$Some Value$q3$, 'via_fn'))$q$) AS spaces_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."Odd Name" WHERE "Some Value" = 'via_fn'$q$) AS spaces_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$sqlreg."Odd Name"$q2$::regclass, jsonb_build_object($q3$Order Id$q3$, 3))$q$) AS spaces_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."Odd Name"$q$) AS spaces_after_delete_row;
SELECT koldstore.unmanage_table($q$sqlreg."Odd Name"$q$::regclass, true) IS NOT NULL AS spaces_unmanaged;
SELECT count(*) AS spaces_rows_after_unmanage FROM sqlreg."Odd Name";

-- quotes
CREATE TABLE sqlreg."Quo""te" ("i""d" bigint PRIMARY KEY, "va""l" text);
INSERT INTO sqlreg."Quo""te" SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg."Quo""te"$q$::regclass, $q$i"d$q$) AS quotes_manage;
SELECT sqlreg.flush_table($q$sqlreg."Quo""te"$q$::regclass) IS NOT NULL AS quotes_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."Quo""te"$q$) AS quotes_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT "i""d" AS v FROM sqlreg."Quo""te" ORDER BY "i""d" DESC LIMIT 2) s$q$) AS quotes_ordered_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg."Quo""te" ("i""d", "va""l") VALUES (1, 'dup')$q$) AS quotes_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg."Quo""te" SET "va""l" = 'x' WHERE "i""d" = 2$q$) AS quotes_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg."Quo""te" WHERE "i""d" > 0$q$) AS quotes_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$sqlreg."Quo""te"$q2$::regclass, jsonb_build_object($q3$i"d$q3$, 2), jsonb_build_object($q3$va"l$q3$, 'via_fn'))$q$) AS quotes_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."Quo""te" WHERE "va""l" = 'via_fn'$q$) AS quotes_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$sqlreg."Quo""te"$q2$::regclass, jsonb_build_object($q3$i"d$q3$, 3))$q$) AS quotes_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."Quo""te"$q$) AS quotes_after_delete_row;
SELECT koldstore.unmanage_table($q$sqlreg."Quo""te"$q$::regclass, true) IS NOT NULL AS quotes_unmanaged;
SELECT count(*) AS quotes_rows_after_unmanage FROM sqlreg."Quo""te";

-- unicode
CREATE TABLE sqlreg."Café" ("clé" bigint PRIMARY KEY, "valeur é" text);
INSERT INTO sqlreg."Café" SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg."Café"$q$::regclass, $q$clé$q$) AS unicode_manage;
SELECT sqlreg.flush_table($q$sqlreg."Café"$q$::regclass) IS NOT NULL AS unicode_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."Café"$q$) AS unicode_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT "clé" AS v FROM sqlreg."Café" ORDER BY "clé" DESC LIMIT 2) s$q$) AS unicode_ordered_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg."Café" ("clé", "valeur é") VALUES (1, 'dup')$q$) AS unicode_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg."Café" SET "valeur é" = 'x' WHERE "clé" = 2$q$) AS unicode_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg."Café" WHERE "clé" > 0$q$) AS unicode_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$sqlreg."Café"$q2$::regclass, jsonb_build_object($q3$clé$q3$, 2), jsonb_build_object($q3$valeur é$q3$, 'via_fn'))$q$) AS unicode_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."Café" WHERE "valeur é" = 'via_fn'$q$) AS unicode_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$sqlreg."Café"$q2$::regclass, jsonb_build_object($q3$clé$q3$, 3))$q$) AS unicode_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."Café"$q$) AS unicode_after_delete_row;
SELECT koldstore.unmanage_table($q$sqlreg."Café"$q$::regclass, true) IS NOT NULL AS unicode_unmanaged;
SELECT count(*) AS unicode_rows_after_unmanage FROM sqlreg."Café";

-- digit_first
CREATE TABLE sqlreg."1table" ("2nd" bigint PRIMARY KEY, "3rd" text);
INSERT INTO sqlreg."1table" SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg."1table"$q$::regclass, $q$2nd$q$) AS digit_first_manage;
SELECT sqlreg.flush_table($q$sqlreg."1table"$q$::regclass) IS NOT NULL AS digit_first_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."1table"$q$) AS digit_first_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT "2nd" AS v FROM sqlreg."1table" ORDER BY "2nd" DESC LIMIT 2) s$q$) AS digit_first_ordered_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg."1table" ("2nd", "3rd") VALUES (1, 'dup')$q$) AS digit_first_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg."1table" SET "3rd" = 'x' WHERE "2nd" = 2$q$) AS digit_first_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg."1table" WHERE "2nd" > 0$q$) AS digit_first_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$sqlreg."1table"$q2$::regclass, jsonb_build_object($q3$2nd$q3$, 2), jsonb_build_object($q3$3rd$q3$, 'via_fn'))$q$) AS digit_first_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."1table" WHERE "3rd" = 'via_fn'$q$) AS digit_first_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$sqlreg."1table"$q2$::regclass, jsonb_build_object($q3$2nd$q3$, 3))$q$) AS digit_first_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."1table"$q$) AS digit_first_after_delete_row;
SELECT koldstore.unmanage_table($q$sqlreg."1table"$q$::regclass, true) IS NOT NULL AS digit_first_unmanaged;
SELECT count(*) AS digit_first_rows_after_unmanage FROM sqlreg."1table";

-- dots_slashes
CREATE TABLE "we.ird"."a/b..c" ("a.b" bigint PRIMARY KEY, "c/d" text);
INSERT INTO "we.ird"."a/b..c" SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$"we.ird"."a/b..c"$q$::regclass, $q$a.b$q$) AS dots_slashes_manage;
SELECT sqlreg.flush_table($q$"we.ird"."a/b..c"$q$::regclass) IS NOT NULL AS dots_slashes_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM "we.ird"."a/b..c"$q$) AS dots_slashes_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT "a.b" AS v FROM "we.ird"."a/b..c" ORDER BY "a.b" DESC LIMIT 2) s$q$) AS dots_slashes_ordered_read;
SELECT sqlreg.try($q$INSERT INTO "we.ird"."a/b..c" ("a.b", "c/d") VALUES (1, 'dup')$q$) AS dots_slashes_insert_cold_key;
SELECT sqlreg.try($q$UPDATE "we.ird"."a/b..c" SET "c/d" = 'x' WHERE "a.b" = 2$q$) AS dots_slashes_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM "we.ird"."a/b..c" WHERE "a.b" > 0$q$) AS dots_slashes_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$"we.ird"."a/b..c"$q2$::regclass, jsonb_build_object($q3$a.b$q3$, 2), jsonb_build_object($q3$c/d$q3$, 'via_fn'))$q$) AS dots_slashes_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM "we.ird"."a/b..c" WHERE "c/d" = 'via_fn'$q$) AS dots_slashes_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$"we.ird"."a/b..c"$q2$::regclass, jsonb_build_object($q3$a.b$q3$, 3))$q$) AS dots_slashes_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM "we.ird"."a/b..c"$q$) AS dots_slashes_after_delete_row;
SELECT koldstore.unmanage_table($q$"we.ird"."a/b..c"$q$::regclass, true) IS NOT NULL AS dots_slashes_unmanaged;
SELECT count(*) AS dots_slashes_rows_after_unmanage FROM "we.ird"."a/b..c";

-- odd_schema
CREATE TABLE "Odd Schema"."t 1" ("id" bigint PRIMARY KEY, "val" text);
INSERT INTO "Odd Schema"."t 1" SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$"Odd Schema"."t 1"$q$::regclass, $q$id$q$) AS odd_schema_manage;
SELECT sqlreg.flush_table($q$"Odd Schema"."t 1"$q$::regclass) IS NOT NULL AS odd_schema_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM "Odd Schema"."t 1"$q$) AS odd_schema_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT "id" AS v FROM "Odd Schema"."t 1" ORDER BY "id" DESC LIMIT 2) s$q$) AS odd_schema_ordered_read;
SELECT sqlreg.try($q$INSERT INTO "Odd Schema"."t 1" ("id", "val") VALUES (1, 'dup')$q$) AS odd_schema_insert_cold_key;
SELECT sqlreg.try($q$UPDATE "Odd Schema"."t 1" SET "val" = 'x' WHERE "id" = 2$q$) AS odd_schema_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM "Odd Schema"."t 1" WHERE "id" > 0$q$) AS odd_schema_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$"Odd Schema"."t 1"$q2$::regclass, jsonb_build_object($q3$id$q3$, 2), jsonb_build_object($q3$val$q3$, 'via_fn'))$q$) AS odd_schema_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM "Odd Schema"."t 1" WHERE "val" = 'via_fn'$q$) AS odd_schema_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$"Odd Schema"."t 1"$q2$::regclass, jsonb_build_object($q3$id$q3$, 3))$q$) AS odd_schema_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM "Odd Schema"."t 1"$q$) AS odd_schema_after_delete_row;
SELECT koldstore.unmanage_table($q$"Odd Schema"."t 1"$q$::regclass, true) IS NOT NULL AS odd_schema_unmanaged;
SELECT count(*) AS odd_schema_rows_after_unmanage FROM "Odd Schema"."t 1";

-- padded
CREATE TABLE sqlreg." padded " (" pk " bigint PRIMARY KEY, " val " text);
INSERT INTO sqlreg." padded " SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg." padded "$q$::regclass, $q$ pk $q$) AS padded_manage;
SELECT sqlreg.flush_table($q$sqlreg." padded "$q$::regclass) IS NOT NULL AS padded_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg." padded "$q$) AS padded_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT " pk " AS v FROM sqlreg." padded " ORDER BY " pk " DESC LIMIT 2) s$q$) AS padded_ordered_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg." padded " (" pk ", " val ") VALUES (1, 'dup')$q$) AS padded_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg." padded " SET " val " = 'x' WHERE " pk " = 2$q$) AS padded_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg." padded " WHERE " pk " > 0$q$) AS padded_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$sqlreg." padded "$q2$::regclass, jsonb_build_object($q3$ pk $q3$, 2), jsonb_build_object($q3$ val $q3$, 'via_fn'))$q$) AS padded_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg." padded " WHERE " val " = 'via_fn'$q$) AS padded_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$sqlreg." padded "$q2$::regclass, jsonb_build_object($q3$ pk $q3$, 3))$q$) AS padded_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg." padded "$q$) AS padded_after_delete_row;
SELECT koldstore.unmanage_table($q$sqlreg." padded "$q$::regclass, true) IS NOT NULL AS padded_unmanaged;
SELECT count(*) AS padded_rows_after_unmanage FROM sqlreg." padded ";

-- long_unicode
CREATE TABLE sqlreg."éééééééééééééééééééééééééééééé" ("id" bigint PRIMARY KEY, "val" text);
INSERT INTO sqlreg."éééééééééééééééééééééééééééééé" SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg."éééééééééééééééééééééééééééééé"$q$::regclass, $q$id$q$) AS long_unicode_manage;
SELECT sqlreg.flush_table($q$sqlreg."éééééééééééééééééééééééééééééé"$q$::regclass) IS NOT NULL AS long_unicode_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."éééééééééééééééééééééééééééééé"$q$) AS long_unicode_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT "id" AS v FROM sqlreg."éééééééééééééééééééééééééééééé" ORDER BY "id" DESC LIMIT 2) s$q$) AS long_unicode_ordered_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg."éééééééééééééééééééééééééééééé" ("id", "val") VALUES (1, 'dup')$q$) AS long_unicode_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg."éééééééééééééééééééééééééééééé" SET "val" = 'x' WHERE "id" = 2$q$) AS long_unicode_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg."éééééééééééééééééééééééééééééé" WHERE "id" > 0$q$) AS long_unicode_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$sqlreg."éééééééééééééééééééééééééééééé"$q2$::regclass, jsonb_build_object($q3$id$q3$, 2), jsonb_build_object($q3$val$q3$, 'via_fn'))$q$) AS long_unicode_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."éééééééééééééééééééééééééééééé" WHERE "val" = 'via_fn'$q$) AS long_unicode_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$sqlreg."éééééééééééééééééééééééééééééé"$q2$::regclass, jsonb_build_object($q3$id$q3$, 3))$q$) AS long_unicode_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."éééééééééééééééééééééééééééééé"$q$) AS long_unicode_after_delete_row;
SELECT koldstore.unmanage_table($q$sqlreg."éééééééééééééééééééééééééééééé"$q$::regclass, true) IS NOT NULL AS long_unicode_unmanaged;
SELECT count(*) AS long_unicode_rows_after_unmanage FROM sqlreg."éééééééééééééééééééééééééééééé";

-- long_plain
CREATE TABLE sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b ("id" bigint PRIMARY KEY, "val" text);
INSERT INTO sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b$q$::regclass, $q$id$q$) AS long_plain_manage;
SELECT sqlreg.flush_table($q$sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b$q$::regclass) IS NOT NULL AS long_plain_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b$q$) AS long_plain_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT "id" AS v FROM sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b ORDER BY "id" DESC LIMIT 2) s$q$) AS long_plain_ordered_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b ("id", "val") VALUES (1, 'dup')$q$) AS long_plain_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b SET "val" = 'x' WHERE "id" = 2$q$) AS long_plain_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b WHERE "id" > 0$q$) AS long_plain_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b$q2$::regclass, jsonb_build_object($q3$id$q3$, 2), jsonb_build_object($q3$val$q3$, 'via_fn'))$q$) AS long_plain_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b WHERE "val" = 'via_fn'$q$) AS long_plain_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b$q2$::regclass, jsonb_build_object($q3$id$q3$, 3))$q$) AS long_plain_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b$q$) AS long_plain_after_delete_row;
SELECT koldstore.unmanage_table($q$sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b$q$::regclass, true) IS NOT NULL AS long_plain_unmanaged;
SELECT count(*) AS long_plain_rows_after_unmanage FROM sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b;

-- collide_1
CREATE TABLE sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1 ("id" bigint PRIMARY KEY, "val" text);
INSERT INTO sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1 SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1$q$::regclass, $q$id$q$) AS collide_1_manage;
SELECT sqlreg.flush_table($q$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1$q$::regclass) IS NOT NULL AS collide_1_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1$q$) AS collide_1_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT "id" AS v FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1 ORDER BY "id" DESC LIMIT 2) s$q$) AS collide_1_ordered_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1 ("id", "val") VALUES (1, 'dup')$q$) AS collide_1_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1 SET "val" = 'x' WHERE "id" = 2$q$) AS collide_1_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1 WHERE "id" > 0$q$) AS collide_1_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1$q2$::regclass, jsonb_build_object($q3$id$q3$, 2), jsonb_build_object($q3$val$q3$, 'via_fn'))$q$) AS collide_1_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1 WHERE "val" = 'via_fn'$q$) AS collide_1_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1$q2$::regclass, jsonb_build_object($q3$id$q3$, 3))$q$) AS collide_1_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1$q$) AS collide_1_after_delete_row;
SELECT koldstore.unmanage_table($q$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1$q$::regclass, true) IS NOT NULL AS collide_1_unmanaged;
SELECT count(*) AS collide_1_rows_after_unmanage FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1;

-- collide_2
CREATE TABLE sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2 ("id" bigint PRIMARY KEY, "val" text);
INSERT INTO sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2 SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2$q$::regclass, $q$id$q$) AS collide_2_manage;
SELECT sqlreg.flush_table($q$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2$q$::regclass) IS NOT NULL AS collide_2_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2$q$) AS collide_2_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT "id" AS v FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2 ORDER BY "id" DESC LIMIT 2) s$q$) AS collide_2_ordered_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2 ("id", "val") VALUES (1, 'dup')$q$) AS collide_2_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2 SET "val" = 'x' WHERE "id" = 2$q$) AS collide_2_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2 WHERE "id" > 0$q$) AS collide_2_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2$q2$::regclass, jsonb_build_object($q3$id$q3$, 2), jsonb_build_object($q3$val$q3$, 'via_fn'))$q$) AS collide_2_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2 WHERE "val" = 'via_fn'$q$) AS collide_2_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2$q2$::regclass, jsonb_build_object($q3$id$q3$, 3))$q$) AS collide_2_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2$q$) AS collide_2_after_delete_row;
SELECT koldstore.unmanage_table($q$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2$q$::regclass, true) IS NOT NULL AS collide_2_unmanaged;
SELECT count(*) AS collide_2_rows_after_unmanage FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2;

-- twin_a
CREATE TABLE sqlreg."twin a" ("id" bigint PRIMARY KEY, "val" text);
INSERT INTO sqlreg."twin a" SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg."twin a"$q$::regclass, $q$id$q$) AS twin_a_manage;
SELECT sqlreg.flush_table($q$sqlreg."twin a"$q$::regclass) IS NOT NULL AS twin_a_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."twin a"$q$) AS twin_a_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT "id" AS v FROM sqlreg."twin a" ORDER BY "id" DESC LIMIT 2) s$q$) AS twin_a_ordered_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg."twin a" ("id", "val") VALUES (1, 'dup')$q$) AS twin_a_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg."twin a" SET "val" = 'x' WHERE "id" = 2$q$) AS twin_a_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg."twin a" WHERE "id" > 0$q$) AS twin_a_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$sqlreg."twin a"$q2$::regclass, jsonb_build_object($q3$id$q3$, 2), jsonb_build_object($q3$val$q3$, 'via_fn'))$q$) AS twin_a_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."twin a" WHERE "val" = 'via_fn'$q$) AS twin_a_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$sqlreg."twin a"$q2$::regclass, jsonb_build_object($q3$id$q3$, 3))$q$) AS twin_a_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."twin a"$q$) AS twin_a_after_delete_row;
SELECT koldstore.unmanage_table($q$sqlreg."twin a"$q$::regclass, true) IS NOT NULL AS twin_a_unmanaged;
SELECT count(*) AS twin_a_rows_after_unmanage FROM sqlreg."twin a";

-- twin_b
CREATE TABLE sqlreg."twin_a" ("id" bigint PRIMARY KEY, "val" text);
INSERT INTO sqlreg."twin_a" SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg."twin_a"$q$::regclass, $q$id$q$) AS twin_b_manage;
SELECT sqlreg.flush_table($q$sqlreg."twin_a"$q$::regclass) IS NOT NULL AS twin_b_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."twin_a"$q$) AS twin_b_read;
SELECT sqlreg.val($q$SELECT string_agg(v::text, ',' ORDER BY v DESC) FROM (SELECT "id" AS v FROM sqlreg."twin_a" ORDER BY "id" DESC LIMIT 2) s$q$) AS twin_b_ordered_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg."twin_a" ("id", "val") VALUES (1, 'dup')$q$) AS twin_b_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg."twin_a" SET "val" = 'x' WHERE "id" = 2$q$) AS twin_b_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg."twin_a" WHERE "id" > 0$q$) AS twin_b_range_delete;
SELECT sqlreg.try($q$SELECT koldstore.update_row($q2$sqlreg."twin_a"$q2$::regclass, jsonb_build_object($q3$id$q3$, 2), jsonb_build_object($q3$val$q3$, 'via_fn'))$q$) AS twin_b_update_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."twin_a" WHERE "val" = 'via_fn'$q$) AS twin_b_updated_visible;
SELECT sqlreg.try($q$SELECT koldstore.delete_row($q2$sqlreg."twin_a"$q2$::regclass, jsonb_build_object($q3$id$q3$, 3))$q$) AS twin_b_delete_row;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."twin_a"$q$) AS twin_b_after_delete_row;
SELECT koldstore.unmanage_table($q$sqlreg."twin_a"$q$::regclass, true) IS NOT NULL AS twin_b_unmanaged;
SELECT count(*) AS twin_b_rows_after_unmanage FROM sqlreg."twin_a";

-- helper objects (guard/capture functions, mirrors) of every unmanaged table are gone
SELECT count(*) AS helper_functions_left FROM pg_proc WHERE pronamespace = 'koldstore'::regnamespace
  AND (proname LIKE '%\_\_cold\_ins\_guard' OR proname LIKE '%\_\_cl\_pk\_guard');
SELECT count(*) AS mirrors_left FROM pg_class WHERE relnamespace = 'koldstore'::regnamespace AND relname LIKE '%\_\_cl' AND relkind = 'r';

-- ------------------------------------------- schema evolution with odd names
CREATE TABLE sqlreg."Evolve Me" ("Row Id" bigint PRIMARY KEY, "First Col" text);
INSERT INTO sqlreg."Evolve Me" SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage('sqlreg."Evolve Me"'::regclass, 'Row Id') AS evolve_manage;
SELECT sqlreg.flush_table('sqlreg."Evolve Me"'::regclass) IS NOT NULL AS evolve_flush_1;
SELECT sqlreg.settle();
ALTER TABLE sqlreg."Evolve Me" ADD COLUMN "Added ""Col""" int DEFAULT 7;
INSERT INTO sqlreg."Evolve Me" VALUES (10, 'hot', 42);
ALTER TABLE sqlreg."Evolve Me" RENAME COLUMN "First Col" TO "Renamed é";
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT concat_ws('|', "Row Id", "Renamed é", "Added ""Col""") FROM sqlreg."Evolve Me" ORDER BY "Row Id" DESC LIMIT 1$q$) AS evolve_read_hot;
SELECT sqlreg.flush_table('sqlreg."Evolve Me"'::regclass, true) IS NOT NULL AS evolve_flush_2;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."Evolve Me"$q$) AS evolve_read_all;
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."Evolve Me" WHERE "Added ""Col""" = 42$q$) AS evolve_filter_new_col;
SELECT koldstore.unmanage_table('sqlreg."Evolve Me"'::regclass, true) IS NOT NULL AS evolve_unmanaged;

-- ----------------------------------------- odd order, scope, pruning columns
CREATE TABLE sqlreg."Scoped Notes" ("Note Id" bigint PRIMARY KEY, "Tenant Id" text NOT NULL, "Body Text" text, "Created At" timestamptz DEFAULT now());
INSERT INTO sqlreg."Scoped Notes" SELECT g, 't1', 'n' || g FROM generate_series(1, 3) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg."Scoped Notes"'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, table_type => 'user', scope_column => 'Tenant Id',
  migration_order_by => 'Note Id', auto_flush => false,
  pruning_columns => ARRAY['Created At'], bloom_filter_columns => ARRAY['Body Text']) IS NOT NULL AS scoped_managed;
SET koldstore.user_id = 't1';
SELECT sqlreg.flush_table('sqlreg."Scoped Notes"'::regclass) IS NOT NULL AS scoped_flush;
SELECT sqlreg.settle();
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."Scoped Notes"$q$) AS scoped_read;
SELECT sqlreg.val($q$SELECT count(*) FROM sqlreg."Scoped Notes" WHERE "Body Text" = 'n2'$q$) AS scoped_bloom_filter_read;
RESET koldstore.user_id;
DROP TABLE sqlreg."Scoped Notes";
DROP SCHEMA "we.ird" CASCADE;
DROP SCHEMA "Odd Schema" CASCADE;
SELECT count(*) AS helper_functions_after_drops FROM pg_proc WHERE pronamespace = 'koldstore'::regnamespace
  AND (proname LIKE '%\_\_cold\_ins\_guard' OR proname LIKE '%\_\_cl\_pk\_guard');
