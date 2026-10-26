-- Domains over supported types are stored as their base type; domain constraints keep applying to the heap,
-- including on hydrate-on-write. A domain over an unsupported type is still refused.

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

CREATE DOMAIN sqlreg.email AS text CHECK (VALUE LIKE '%@%');
CREATE DOMAIN sqlreg.posint AS int CHECK (VALUE > 0);
CREATE DOMAIN sqlreg.posint_small AS sqlreg.posint CHECK (VALUE < 1000000);
CREATE DOMAIN sqlreg.money2 AS numeric(10,2);
CREATE DOMAIN sqlreg.doc AS jsonb;
CREATE DOMAIN sqlreg.tags AS text[];
CREATE DOMAIN sqlreg.keyid AS bigint;

CREATE TABLE sqlreg.dom (
  id sqlreg.keyid PRIMARY KEY,
  mail sqlreg.email NOT NULL,
  qty sqlreg.posint_small,
  price sqlreg.money2,
  body sqlreg.doc,
  tg sqlreg.tags
);
INSERT INTO sqlreg.dom
SELECT g, 'u' || g || '@example.com', g, g * 1.25, jsonb_build_object('n', g), ARRAY['t' || g, 'x']
FROM generate_series(1, 60) g;
INSERT INTO sqlreg.dom VALUES (61, 'nul@example.com', NULL, NULL, NULL, NULL);
CREATE TABLE sqlreg.dom_ctl AS SELECT * FROM sqlreg.dom;
SELECT sqlreg.fingerprint('sqlreg.dom', 'id') AS before_flush \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.dom'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS dom_managed;
SELECT sqlreg.flush_table('sqlreg.dom'::regclass) IS NOT NULL AS dom_flushed;
SELECT sqlreg.fingerprint('sqlreg.dom', 'id') = :'before_flush' AS same_after_flush;
SELECT count(*) AS cold_pred_rows FROM sqlreg.dom WHERE mail = 'u7@example.com' AND qty = 7 AND price = 8.75;
SELECT count(*) AS cold_tag_rows FROM sqlreg.dom WHERE tg @> ARRAY['t9'];

-- hydrate-on-write; domain constraints still enforced
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.dom SET mail = 'new@example.com', qty = qty + 1 WHERE id IN (3, 40);
UPDATE sqlreg.dom_ctl SET mail = 'new@example.com', qty = qty + 1 WHERE id IN (3, 40);
UPDATE sqlreg.dom SET mail = 'not-an-email' WHERE id = 5;
UPDATE sqlreg.dom SET qty = 0 WHERE id = 6;
DELETE FROM sqlreg.dom WHERE id = 9;
DELETE FROM sqlreg.dom_ctl WHERE id = 9;
RESET koldstore.hydrate_on_write;
INSERT INTO sqlreg.dom (id, mail) VALUES (100, 'bad');
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.dom', 'id') = sqlreg.fingerprint('sqlreg.dom_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.dom'::regclass, true) IS NOT NULL AS dom_reflushed;
SELECT sqlreg.fingerprint('sqlreg.dom', 'id') = sqlreg.fingerprint('sqlreg.dom_ctl', 'id') AS rows_match_after_reflush;

SELECT koldstore.unmanage_table('sqlreg.dom'::regclass, true, true) IS NOT NULL AS dom_unmanaged;
SELECT sqlreg.fingerprint('sqlreg.dom', 'id') = sqlreg.fingerprint('sqlreg.dom_ctl', 'id') AS rows_match_after_unmanage;

-- a domain as the migration-order column and as the key (sort-key, bounds and summary paths)
CREATE DOMAIN sqlreg.stamp AS timestamptz;
CREATE DOMAIN sqlreg.uid AS uuid;
CREATE TABLE sqlreg.dom_ord (id sqlreg.uid PRIMARY KEY, at sqlreg.stamp NOT NULL, body text);
INSERT INTO sqlreg.dom_ord
SELECT ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid,
       timestamptz '2026-01-01 00:00:00+00' + g * interval '1 hour', 'r' || g
FROM generate_series(1, 60) g;
CREATE TABLE sqlreg.dom_ord_ctl AS SELECT * FROM sqlreg.dom_ord;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.dom_ord'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'at', auto_flush => false
) IS NOT NULL AS dom_ord_managed;
SELECT sqlreg.flush_table('sqlreg.dom_ord'::regclass) IS NOT NULL AS dom_ord_flushed;
SELECT count(*) AS range_rows FROM sqlreg.dom_ord WHERE at < timestamptz '2026-01-01 10:30:00+00';
SELECT count(*) AS range_rows_ctl FROM sqlreg.dom_ord_ctl WHERE at < timestamptz '2026-01-01 10:30:00+00';
SELECT body AS key_lookup FROM sqlreg.dom_ord WHERE id = '00000000-0000-0000-0000-000000000007';
SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.dom_ord SET body = 'changed' WHERE id = '00000000-0000-0000-0000-000000000007';
UPDATE sqlreg.dom_ord_ctl SET body = 'changed' WHERE id = '00000000-0000-0000-0000-000000000007';
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS dom_ord_mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.dom_ord', 'at') = sqlreg.fingerprint('sqlreg.dom_ord_ctl', 'at') AS dom_ord_match;

-- a domain over an unsupported type is still refused
CREATE DOMAIN sqlreg.span AS interval;
CREATE TABLE sqlreg.dom_bad (id bigint PRIMARY KEY, d sqlreg.span, n int NOT NULL);
SELECT koldstore.manage_table(
  table_name => 'sqlreg.dom_bad'::regclass, storage => 'sqlreg_fs', hot_row_limit => 1,
  min_flush_rows => 1, migration_order_by => 'n', auto_flush => false
) IS NOT NULL AS bad_domain_managed;
