-- Identifiers that are legal in PostgreSQL but not plain lower-case words.
--
-- Supported: upper/mixed case, reserved words, and very long names (including
-- two long names sharing a prefix). Each is managed, flushed to cold storage,
-- read back through the merge scan and written through the write guards, then
-- unmanaged again, one statement (= one transaction) at a time.
--
-- Not supported yet: spaces, embedded quotes, non-ASCII letters or a leading
-- digit in a schema, table or column name. manage_table refuses them up front,
-- before creating anything (they used to be accepted and then fail every flush).

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


-- mixed_case
CREATE TABLE sqlreg."MixedCase" ("OrderId" bigint PRIMARY KEY, "SomeValue" text);
INSERT INTO sqlreg."MixedCase" SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg."MixedCase"$q$::regclass, $q$OrderId$q$) AS mixed_case_manage;
SELECT sqlreg.flush_table($q$sqlreg."MixedCase"$q$::regclass) IS NOT NULL AS mixed_case_flush;
SELECT sqlreg.settle();
SELECT sqlreg.try($q$SELECT count(*) FROM sqlreg."MixedCase"$q$) AS mixed_case_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg."MixedCase" ("OrderId", "SomeValue") VALUES (1, 'dup')$q$) AS mixed_case_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg."MixedCase" SET "SomeValue" = 'x' WHERE "OrderId" = 2$q$) AS mixed_case_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg."MixedCase" WHERE "OrderId" > 0$q$) AS mixed_case_range_delete;
SELECT koldstore.unmanage_table($q$sqlreg."MixedCase"$q$::regclass, true) IS NOT NULL AS mixed_case_unmanaged;
SELECT count(*) AS mixed_case_rows_after_unmanage FROM sqlreg."MixedCase";
SELECT count(*) AS mixed_case_helper_objects_left FROM pg_proc WHERE pronamespace = 'koldstore'::regnamespace AND proname LIKE 'sqlreg\_MixedCase%';

-- reserved
CREATE TABLE sqlreg."select" ("user" bigint PRIMARY KEY, "order" text);
INSERT INTO sqlreg."select" SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg."select"$q$::regclass, $q$user$q$) AS reserved_manage;
SELECT sqlreg.flush_table($q$sqlreg."select"$q$::regclass) IS NOT NULL AS reserved_flush;
SELECT sqlreg.settle();
SELECT sqlreg.try($q$SELECT count(*) FROM sqlreg."select"$q$) AS reserved_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg."select" ("user", "order") VALUES (1, 'dup')$q$) AS reserved_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg."select" SET "order" = 'x' WHERE "user" = 2$q$) AS reserved_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg."select" WHERE "user" > 0$q$) AS reserved_range_delete;
SELECT koldstore.unmanage_table($q$sqlreg."select"$q$::regclass, true) IS NOT NULL AS reserved_unmanaged;
SELECT count(*) AS reserved_rows_after_unmanage FROM sqlreg."select";
SELECT count(*) AS reserved_helper_objects_left FROM pg_proc WHERE pronamespace = 'koldstore'::regnamespace AND proname LIKE 'sqlreg\_select%';

-- tbl_long
CREATE TABLE sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b (id bigint PRIMARY KEY, val text);
INSERT INTO sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b$q$::regclass, $q$id$q$) AS tbl_long_manage;
SELECT sqlreg.flush_table($q$sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b$q$::regclass) IS NOT NULL AS tbl_long_flush;
SELECT sqlreg.settle();
SELECT sqlreg.try($q$SELECT count(*) FROM sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b$q$) AS tbl_long_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b (id, val) VALUES (1, 'dup')$q$) AS tbl_long_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b SET val = 'x' WHERE id = 2$q$) AS tbl_long_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b WHERE id > 0$q$) AS tbl_long_range_delete;
SELECT koldstore.unmanage_table($q$sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b$q$::regclass, true) IS NOT NULL AS tbl_long_unmanaged;
SELECT count(*) AS tbl_long_rows_after_unmanage FROM sqlreg.a_table_with_a_very_long_name_that_uses_nearly_all_sixty_three_b;
SELECT count(*) AS tbl_long_helper_objects_left FROM pg_proc WHERE pronamespace = 'koldstore'::regnamespace AND proname LIKE 'sqlreg\_a\_table%';

-- collide_1
CREATE TABLE sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1 (id bigint PRIMARY KEY, val text);
INSERT INTO sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1 SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1$q$::regclass, $q$id$q$) AS collide_1_manage;
SELECT sqlreg.flush_table($q$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1$q$::regclass) IS NOT NULL AS collide_1_flush;
SELECT sqlreg.settle();
SELECT sqlreg.try($q$SELECT count(*) FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1$q$) AS collide_1_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1 (id, val) VALUES (1, 'dup')$q$) AS collide_1_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1 SET val = 'x' WHERE id = 2$q$) AS collide_1_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1 WHERE id > 0$q$) AS collide_1_range_delete;
SELECT koldstore.unmanage_table($q$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1$q$::regclass, true) IS NOT NULL AS collide_1_unmanaged;
SELECT count(*) AS collide_1_rows_after_unmanage FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1;
SELECT count(*) AS collide_1_helper_objects_left FROM pg_proc WHERE pronamespace = 'koldstore'::regnamespace AND proname LIKE 'sqlreg\_collide\_%';

-- collide_2
CREATE TABLE sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2 (id bigint PRIMARY KEY, val text);
INSERT INTO sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2 SELECT g, 'v' || g FROM generate_series(1, 3) g;
SELECT sqlreg.try_manage($q$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2$q$::regclass, $q$id$q$) AS collide_2_manage;
SELECT sqlreg.flush_table($q$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2$q$::regclass) IS NOT NULL AS collide_2_flush;
SELECT sqlreg.settle();
SELECT sqlreg.try($q$SELECT count(*) FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2$q$) AS collide_2_read;
SELECT sqlreg.try($q$INSERT INTO sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2 (id, val) VALUES (1, 'dup')$q$) AS collide_2_insert_cold_key;
SELECT sqlreg.try($q$UPDATE sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2 SET val = 'x' WHERE id = 2$q$) AS collide_2_update_cold_key;
SELECT sqlreg.try($q$DELETE FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2 WHERE id > 0$q$) AS collide_2_range_delete;
SELECT koldstore.unmanage_table($q$sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2$q$::regclass, true) IS NOT NULL AS collide_2_unmanaged;
SELECT count(*) AS collide_2_rows_after_unmanage FROM sqlreg.collide_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2;
SELECT count(*) AS collide_2_helper_objects_left FROM pg_proc WHERE pronamespace = 'koldstore'::regnamespace AND proname LIKE 'sqlreg\_collide\_%';

-- ------------------------------------------------ refused up front
CREATE TABLE sqlreg.odd_col_space (id bigint PRIMARY KEY, "Some Value" text);
SELECT sqlreg.try_manage($q$sqlreg.odd_col_space$q$::regclass, $q$id$q$) AS col_space_manage;
CREATE TABLE sqlreg.odd_col_quote (id bigint PRIMARY KEY, "va""l" text);
SELECT sqlreg.try_manage($q$sqlreg.odd_col_quote$q$::regclass, $q$id$q$) AS col_quote_manage;
CREATE TABLE sqlreg.odd_col_unicode (id bigint PRIMARY KEY, "valeur_é" text);
SELECT sqlreg.try_manage($q$sqlreg.odd_col_unicode$q$::regclass, $q$id$q$) AS col_unicode_manage;
CREATE TABLE sqlreg.odd_col_digit (id bigint PRIMARY KEY, "2nd" text);
SELECT sqlreg.try_manage($q$sqlreg.odd_col_digit$q$::regclass, $q$id$q$) AS col_digit_first_manage;
CREATE TABLE sqlreg.odd_pk_space ("Order Id" bigint PRIMARY KEY, val text);
SELECT sqlreg.try_manage($q$sqlreg.odd_pk_space$q$::regclass, $q$Order Id$q$) AS pk_space_manage;
CREATE TABLE sqlreg."Odd Name" (id bigint PRIMARY KEY, val text);
SELECT sqlreg.try_manage($q$sqlreg."Odd Name"$q$::regclass, $q$id$q$) AS tbl_space_manage;
CREATE TABLE sqlreg."Quo""te" (id bigint PRIMARY KEY, val text);
SELECT sqlreg.try_manage($q$sqlreg."Quo""te"$q$::regclass, $q$id$q$) AS tbl_quote_manage;
CREATE TABLE sqlreg."Café" (id bigint PRIMARY KEY, val text);
SELECT sqlreg.try_manage($q$sqlreg."Café"$q$::regclass, $q$id$q$) AS tbl_unicode_manage;
CREATE TABLE sqlreg."1table" (id bigint PRIMARY KEY, val text);
SELECT sqlreg.try_manage($q$sqlreg."1table"$q$::regclass, $q$id$q$) AS tbl_digit_first_manage;
