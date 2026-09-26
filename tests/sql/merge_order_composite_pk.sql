-- ORDER BY over a managed table with a composite primary key, hot and cold rows
-- sharing the leading key.
--
-- Regression: the ordered merge scan advertised the hot index's whole pathkey
-- list (a, b) although it only guarantees order on the leading key `a`, so
-- PostgreSQL skipped the Sort that ORDER BY a, b needs and rows tying on `a`
-- came back hot-first instead of ordered by `b`.

CREATE TABLE sqlreg.o1 (a int, b int, val text, PRIMARY KEY (a, b));
INSERT INTO sqlreg.o1 SELECT g, g * 10, 'w' || g FROM generate_series(1, 4) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.o1'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'a', auto_flush => false
) IS NOT NULL AS managed;
SELECT sqlreg.flush_table('sqlreg.o1'::regclass) IS NOT NULL AS flushed;

-- Cold now: (1,10) (2,20) (3,30) (4,40). Hot rows tie on a=2 and a=4 with cold ones.
INSERT INTO sqlreg.o1 VALUES (2, 21, 'hot21'), (2, 5, 'hot5'), (4, 41, 'hot41');
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_settled;

SELECT a, b, val FROM sqlreg.o1 ORDER BY a, b;
SELECT a, b, val FROM sqlreg.o1 ORDER BY a DESC, b DESC;
SELECT a, b, val FROM sqlreg.o1 ORDER BY a, b DESC;
SELECT a, b, val FROM sqlreg.o1 ORDER BY b;
-- a LIMIT over the tie must return the smallest (a, b) rows, not the first hot ones
SELECT a, b FROM sqlreg.o1 ORDER BY a, b LIMIT 3;
SELECT a, b FROM sqlreg.o1 WHERE a = 2 ORDER BY b;

-- single-column primary key: the ordered path still needs no extra keys
CREATE TABLE sqlreg.o2 (id int PRIMARY KEY, val text);
INSERT INTO sqlreg.o2 SELECT g, 'c' || g FROM generate_series(1, 3) g;
SELECT koldstore.manage_table(
  table_name => 'sqlreg.o2'::regclass, storage => 'sqlreg_fs', hot_row_limit => 10,
  min_flush_rows => 1, max_rows_per_file => 10, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS managed_single;
SELECT sqlreg.flush_table('sqlreg.o2'::regclass) IS NOT NULL AS flushed_single;
INSERT INTO sqlreg.o2 VALUES (0, 'h0'), (4, 'h4');
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_settled_single;
SELECT id, val FROM sqlreg.o2 ORDER BY id;
SELECT id, val FROM sqlreg.o2 ORDER BY id DESC;
