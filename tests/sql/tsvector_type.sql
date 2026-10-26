-- `tsvector` columns: flush to cold, read back, hydrate-on-write, delete, re-flush, and full-text
-- predicates over hot + cold rows against an unmanaged copy.

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

CREATE TABLE sqlreg.doc (id bigint PRIMARY KEY, body text, tsv tsvector, note text NOT NULL DEFAULT 'x');
INSERT INTO sqlreg.doc (id, body, tsv)
SELECT g,
       format('alpha%s beta%s the quick brown fox %s', g % 7, g % 5, CASE WHEN g % 9 = 0 THEN 'jumps' ELSE 'sleeps' END),
       to_tsvector('english', format('alpha%s beta%s the quick brown fox %s', g % 7, g % 5, CASE WHEN g % 9 = 0 THEN 'jumps' ELSE 'sleeps' END))
FROM generate_series(1, 60) g;
INSERT INTO sqlreg.doc (id, body, tsv) VALUES
  (101, NULL, NULL),
  (102, '', ''::tsvector),
  (103, 'weights', 'a:1A,2B b:3 ''quoted lexeme'':4 ''it''''s'':5 ''back\\slash'':6'),
  (104, 'unicode', to_tsvector('simple', 'Ünïcode naïve café 日本語'));
CREATE TABLE sqlreg.doc_ctl AS SELECT * FROM sqlreg.doc;
SELECT sqlreg.fingerprint('sqlreg.doc', 'id') AS before_flush \gset

SELECT koldstore.manage_table(
  table_name => 'sqlreg.doc'::regclass, storage => 'sqlreg_fs', hot_row_limit => 5,
  min_flush_rows => 1, max_rows_per_file => 20, migration_order_by => 'id', auto_flush => false
) IS NOT NULL AS doc_managed;
SELECT sqlreg.flush_table('sqlreg.doc'::regclass) IS NOT NULL AS doc_flushed;
SELECT (koldstore.table_status('sqlreg.doc'::regclass) ->> 'cold_row_count')::int >= 50 AS mostly_cold;
SELECT sqlreg.fingerprint('sqlreg.doc', 'id') = :'before_flush' AS rows_identical_after_flush;
SELECT pg_typeof(tsv)::text AS cold_type FROM sqlreg.doc WHERE id = 2;
SELECT id, tsv FROM sqlreg.doc WHERE id IN (103, 104) ORDER BY id;

SET koldstore.hydrate_on_write = on;
UPDATE sqlreg.doc SET tsv = to_tsvector('english', 'replaced text') WHERE id IN (3, 40);
UPDATE sqlreg.doc_ctl SET tsv = to_tsvector('english', 'replaced text') WHERE id IN (3, 40);
UPDATE sqlreg.doc SET note = 'changed' WHERE id IN (7, 103, 104);
UPDATE sqlreg.doc_ctl SET note = 'changed' WHERE id IN (7, 103, 104);
DELETE FROM sqlreg.doc WHERE id = 102;
DELETE FROM sqlreg.doc_ctl WHERE id = 102;
RESET koldstore.hydrate_on_write;
SELECT koldstore.wait_for_async_mirror() IS NOT NULL AS mirror_caught_up;
SELECT sqlreg.fingerprint('sqlreg.doc', 'id') = sqlreg.fingerprint('sqlreg.doc_ctl', 'id') AS rows_match_after_dml;
SELECT sqlreg.flush_table('sqlreg.doc'::regclass, true) IS NOT NULL AS doc_reflushed;
SELECT sqlreg.fingerprint('sqlreg.doc', 'id') = sqlreg.fingerprint('sqlreg.doc_ctl', 'id') AS rows_match_after_reflush;

-- full-text predicates, ranking and tsvector functions see cold rows
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.doc WHERE tsv @@ to_tsquery('english', 'fox & jump'))
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.doc_ctl WHERE tsv @@ to_tsquery('english', 'fox & jump')) AS and_matches;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.doc WHERE tsv @@ to_tsquery('english', 'alpha3 | beta4'))
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.doc_ctl WHERE tsv @@ to_tsquery('english', 'alpha3 | beta4')) AS or_matches;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.doc WHERE tsv @@ to_tsquery('english', 'fox & !jump'))
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.doc_ctl WHERE tsv @@ to_tsquery('english', 'fox & !jump')) AS not_matches;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.doc WHERE tsv @@ to_tsquery('english', 'alph:*'))
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.doc_ctl WHERE tsv @@ to_tsquery('english', 'alph:*')) AS prefix_matches;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.doc WHERE tsv @@ phraseto_tsquery('english', 'quick brown fox'))
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.doc_ctl WHERE tsv @@ phraseto_tsquery('english', 'quick brown fox')) AS phrase_matches;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.doc WHERE tsv @@ 'a:A & b'::tsquery)
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.doc_ctl WHERE tsv @@ 'a:A & b'::tsquery) AS weight_matches;
SELECT (SELECT array_agg(id ORDER BY id) FROM sqlreg.doc WHERE 'it''s'::tsquery @@ tsv)
     IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM sqlreg.doc_ctl WHERE 'it''s'::tsquery @@ tsv) AS commuted_matches;
SELECT (SELECT array_agg(id ORDER BY ts_rank(tsv, to_tsquery('english', 'fox')) DESC, id) FROM sqlreg.doc WHERE tsv @@ to_tsquery('english', 'fox'))
     = (SELECT array_agg(id ORDER BY ts_rank(tsv, to_tsquery('english', 'fox')) DESC, id) FROM sqlreg.doc_ctl WHERE tsv @@ to_tsquery('english', 'fox')) AS ranking_matches;
SELECT (SELECT sum(length(tsv))::int FROM sqlreg.doc) = (SELECT sum(length(tsv))::int FROM sqlreg.doc_ctl) AS lexeme_count_matches;
