-- json, inet, cidr, macaddr, char(n), time and timetz are carried as their text form: they flush to cold,
-- read back identically, hydrate-on-write, and unmanage. Every answer is compared with an unmanaged copy.

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

CREATE TABLE sqlreg.tf (
  id bigint PRIMARY KEY,
  j json, ip inet, net cidr, mac macaddr, c char(6), t time, tz timetz, t3 time(3), note text
);
INSERT INTO sqlreg.tf
SELECT g,
       CASE WHEN g % 11 = 0 THEN NULL ELSE format('{"k": %s,  "arr": [1, 2,3], "s": "v%s"}', g, g % 4)::json END,
       CASE WHEN g % 13 = 0 THEN NULL ELSE ('10.0.' || (g % 250) || '.' || (g % 7) || '/' || (24 + g % 8))::inet END,
       ('192.168.' || (g % 200) || '.0/24')::cidr,
       ('08:00:2b:01:' || lpad(to_hex(g % 256), 2, '0') || ':02')::macaddr,
       'ab' || (g % 10),
       make_time(g % 24, g % 60, (g % 60) + 0.25),
       (make_time(g % 24, g % 60, 0)::text || '+0' || (g % 9) || ':30')::timetz,
       make_time(g % 24, g % 60, 1.123),
       'n' || g
FROM generate_series(1, 60) g;
INSERT INTO sqlreg.tf (id, j, ip, c, t) VALUES
  (101, '{"a":null}', '::1', '', '23:59:59.999999'),
  (102, '[]', '2001:db8::/32', 'x', '00:00:00'),
  (103, 'null', '10.0.0.1', 'abcdef', '24:00:00');
CREATE TABLE sqlreg.tf_ctl AS SELECT * FROM sqlreg.tf;
SELECT sqlreg.fingerprint('sqlreg.tf', 'id') AS before_flush \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.tf'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS tf_managed;
SELECT sqlreg.flush_table('sqlreg.tf'::regclass) IS NOT NULL AS tf_flushed;
SELECT (koldstore.table_status('sqlreg.tf'::regclass) ->> 'cold_row_count')::int >= 50 AS mostly_cold;
SELECT sqlreg.fingerprint('sqlreg.tf', 'id') = :'before_flush' AS rows_identical_after_flush;
SELECT pg_typeof(j)::text, pg_typeof(ip)::text, pg_typeof(net)::text, pg_typeof(mac)::text, pg_typeof(c)::text,
       pg_typeof(t)::text, pg_typeof(tz)::text FROM sqlreg.tf WHERE id = 2;
SELECT id, j, ip, c, t FROM sqlreg.tf WHERE id IN (101, 102, 103) ORDER BY id;

-- predicates and functions evaluate over cold rows
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.tf WHERE ip << '10.0.0.0/16'::inet)
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.tf_ctl WHERE ip << '10.0.0.0/16'::inet) AS inet_contained;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.tf WHERE net = '192.168.7.0/24')
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.tf_ctl WHERE net = '192.168.7.0/24') AS cidr_eq;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.tf WHERE t BETWEEN '05:00' AND '09:00')
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.tf_ctl WHERE t BETWEEN '05:00' AND '09:00') AS time_range;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.tf WHERE c = 'ab3')
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.tf_ctl WHERE c = 'ab3') AS bpchar_eq;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.tf WHERE j ->> 's' = 'v2')
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.tf_ctl WHERE j ->> 's' = 'v2') AS json_extract;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.tf WHERE mac = '08:00:2b:01:0a:02')
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.tf_ctl WHERE mac = '08:00:2b:01:0a:02') AS macaddr_eq;

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.tf SET note = 'changed' WHERE id IN (3, 40, 101, 102);
UPDATE sqlreg.tf_ctl SET note = 'changed' WHERE id IN (3, 40, 101, 102);
UPDATE sqlreg.tf SET j = '{"new": true}', ip = '172.16.0.9', t = '12:34:56' WHERE id IN (7, 41);
UPDATE sqlreg.tf_ctl SET j = '{"new": true}', ip = '172.16.0.9', t = '12:34:56' WHERE id IN (7, 41);
DELETE FROM sqlreg.tf WHERE id = 9;
DELETE FROM sqlreg.tf_ctl WHERE id = 9;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.tf', 'id') = sqlreg.fingerprint('sqlreg.tf_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.tf'::regclass, true) IS NOT NULL AS tf_reflushed;
SELECT sqlreg.fingerprint('sqlreg.tf', 'id') = sqlreg.fingerprint('sqlreg.tf_ctl', 'id') AS rows_match_after_reflush;

SELECT koldstore.unmanage_table('sqlreg.tf'::regclass, true, true) IS NOT NULL AS tf_unmanaged;
SELECT sqlreg.fingerprint('sqlreg.tf', 'id') = sqlreg.fingerprint('sqlreg.tf_ctl', 'id') AS rows_match_after_unmanage;

-- money is still unsupported: its text form depends on lc_monetary
CREATE TABLE sqlreg.tf_money (id bigint PRIMARY KEY, i money);
SELECT koldstore.manage_table(table_name => 'sqlreg.tf_money'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, migration_order_by => 'id', auto_flush => false);
