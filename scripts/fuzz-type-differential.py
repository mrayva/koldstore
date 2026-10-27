#!/usr/bin/env python3
"""Seeded differential fuzz of a managed table against an unmanaged control.

One table carries a column of every type family koldstore stores in text form (ranges, geometric,
composite, enum, money, xml, bit strings, arrays incl. multi-dimensional, jsonb, tsvector, inet, ...).
The same random operations (insert / re-insert / duplicate insert / update / range update / delete /
flush / compact) run on the managed table `fz.t` and the plain control `fz.t_ctl`; after every batch
the two are compared row by row, by predicate counts and by ordered LIMIT queries, under randomised
DateStyle / TimeZone / IntervalStyle / extra_float_digits / bytea_output (flushes run under random
settings too, so stored text must not depend on them). Any divergence prints the seed and the
differing rows and exits 1; the run is reproducible from its seed.

Usage: scripts/fuzz-type-differential.py [--host H] [--port P] [--user U] [--db NAME]
                                         [--seed N] [--ops N] [--batch N] [--rows N]
Needs psycopg2 and a server with koldstore in shared_preload_libraries.
"""
import argparse
import os
import random
import sys
import tempfile

import psycopg2

COLUMNS = [
    # (name, type, expression over a,b,c,d : ints, and id)
    ("tag", "int", "a % 1000"),
    ("i4", "int4range", "CASE a % 5 WHEN 0 THEN 'empty'::int4range WHEN 1 THEN '(,)'::int4range "
          "ELSE int4range(b - 50, b - 50 + c % 40, (ARRAY['[)','[]','(]','()'])[1 + d % 4]) END"),
    ("nr", "numrange", "numrange(b / 7.0, b / 7.0 + c / 3.0, (ARRAY['[)','[]','(]','()'])[1 + d % 4])"),
    ("dr", "daterange", "CASE a % 6 WHEN 0 THEN daterange(NULL, date '2024-01-01' + b) "
          "WHEN 1 THEN daterange(date '2024-01-01' + b, NULL) "
          "ELSE daterange(date '2024-01-01' + b, date '2024-01-01' + b + c % 90) END"),
    ("tz", "tstzrange", "tstzrange(timestamptz '2024-01-01 00:00+00' + b * interval '1 hour', "
          "timestamptz '2024-01-01 00:00+00' + (b + c) * interval '1 hour', (ARRAY['[)','[]','(]','()'])[1 + d % 4])"),
    ("pt", "point", "point(a / 3.0, b * 1.1)"),
    ("pg", "polygon", "polygon(circle(point(a / 3.0, b / 7.0), 1 + c / 10.0))"),
    ("ci", "circle", "circle(point(b / 3.0, a / 9.0), 0.1 + c / 7.0)"),
    ("bx", "box", "box(point(a % 100, b % 100), point(a % 100 + c % 20, b % 100 + d % 20))"),
    ("rec", "fz.rec", "ROW(a - 500, (ARRAY['plain','with space','quo\"te','back\\slash','comma,here',"
          "'ünï €','','paren (x)','{brace}'])[1 + b % 9], c / 3.0, "
          "timestamptz '2024-01-01 00:00+00' + d * interval '1 minute', ARRAY[a, b, NULL])::fz.rec"),
    ("mood", "fz.mood", "(ARRAY['sad','ok','happy'])[1 + a % 3]::fz.mood"),
    ("amt", "money", "(a * 1.37)::numeric::money"),
    ("doc", "xml", "xmlelement(name item, xmlattributes(a AS n), 'txt & <' || b)"),
    ("bv", "varbit", "substr(a::bit(24)::text, 1 + b % 20)::varbit"),
    ("mac8", "macaddr8", "('08:00:2b:01:' || lpad(to_hex(a % 256), 2, '0') || ':' || lpad(to_hex(b % 256), 2, '0') "
          "|| ':' || lpad(to_hex(c % 256), 2, '0') || ':' || lpad(to_hex(d % 256), 2, '0'))::macaddr8"),
    ("nm", "name", "('n' || a)::name"),
    ("oi", "oid", "a::oid"),
    ("iv", "interval", "make_interval(years => a % 3, months => b % 12, days => c % 400 - 200, "
          "hours => d % 24, secs => (a % 600) / 10.0)"),
    ("ia", "interval[]", "ARRAY[make_interval(days => a % 30), make_interval(hours => b % 48), NULL]"),
    ("ta", "text[]", "CASE a % 4 WHEN 0 THEN ARRAY['a b', 'q\"uote', NULL, '', 'x,y', a::text] "
          "WHEN 1 THEN ARRAY[ARRAY['p', 'q' || b], ARRAY['r', NULL]] "
          "WHEN 2 THEN ARRAY[ARRAY[ARRAY['1','2'],ARRAY['3','4']],ARRAY[ARRAY['5','6'],ARRAY['7', c::text]]] "
          "ELSE '{}'::text[] END"),
    ("da", "date[]", "ARRAY[date '2024-01-01' + a, NULL, date '2020-02-29' + b]"),
    ("na", "numeric[]", "ARRAY[a / 7.0, 'NaN'::numeric, b * 1e10, NULL]"),
    ("fa", "float8[]", "ARRAY[a / 3.0, 'Infinity'::float8, -0.0::float8, 0.30000000000000004::float8]"),
    ("js", "jsonb", "jsonb_build_object('k', a, 'a', jsonb_build_array(b, 'str', jsonb_build_object('z', NULL)), "
          "'s', 'q\"uote \\ é', 'n', c / 3.0)"),
    ("tsv", "tsvector", "to_tsvector('simple', 'w' || a || ' w' || b || ' common')"),
    ("ip", "inet", "('10.' || a % 256 || '.' || b % 256 || '.' || c % 256 || '/' || (8 + d % 25))::inet"),
    ("tm", "timetz", "(make_time(a % 24, b % 60, c % 60)::text || (ARRAY['+02','-05','+00','+05:30'])[1 + d % 4])::timetz"),
    ("dom", "fz.pos_int", "a + 1"),
    ("txt", "text", "(ARRAY['plain','with space','quo\"te','back\\slash','comma,here','ünï €','',"
          "E'new\\nline','{braces}','(paren)'])[1 + a % 10]"),
]
NULLABLE = [name for name, _, _ in COLUMNS]

READ_SETTINGS = {
    "TimeZone": ["UTC", "Asia/Tokyo", "America/New_York", "Europe/Berlin"],
    "DateStyle": ["ISO, MDY", "German, DMY", "SQL, DMY", "Postgres, MDY"],
    "IntervalStyle": ["postgres", "iso_8601", "sql_standard", "postgres_verbose"],
    "extra_float_digits": ["-3", "0", "1", "3"],
    "bytea_output": ["hex", "escape"],
}


def ddl():
    cols = ",\n  ".join(f"{n} {t}" for n, t, _ in COLUMNS)
    assigns = []
    for i, (name, _, expr) in enumerate(COLUMNS):
        assigns.append(
            f"  a := fz.r(i, s, {10 * i + 1}, 1000); b := fz.r(i, s, {10 * i + 2}, 1000); "
            f"c := fz.r(i, s, {10 * i + 3}, 1000); d := fz.r(i, s, {10 * i + 4}, 1000);\n"
            f"  r.{name} := CASE WHEN fz.r(i, s, {10 * i + 5}, 9) = 0 THEN NULL ELSE {expr} END;")
    body = "\n".join(assigns)
    return f"""
CREATE SCHEMA fz;
CREATE TYPE fz.rec AS (i int, t text, f float8, ts timestamptz, ia int[]);
CREATE TYPE fz.mood AS ENUM ('sad', 'ok', 'happy');
CREATE DOMAIN fz.pos_int AS int CHECK (VALUE > 0);
CREATE TABLE fz.t (id bigint PRIMARY KEY,
  {cols});
CREATE FUNCTION fz.r(i bigint, s int, n int, m int) RETURNS int LANGUAGE sql IMMUTABLE AS
  $$ SELECT (abs(hashtextextended(i || ':' || s || ':' || n, 7) % m))::int $$;
CREATE FUNCTION fz.row_for(i bigint, s int) RETURNS fz.t LANGUAGE plpgsql AS $$
DECLARE r fz.t; a int; b int; c int; d int;
BEGIN
  r.id := i;
{body}
  RETURN r;
END $$;
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("PGHOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "28818")))
    ap.add_argument("--user", default=os.environ.get("PGUSER", ""))
    ap.add_argument("--db", default="koldstore_fuzz")
    ap.add_argument("--seed", type=int, default=random.SystemRandom().randrange(1 << 30))
    ap.add_argument("--ops", type=int, default=400)
    ap.add_argument("--batch", type=int, default=25)
    ap.add_argument("--rows", type=int, default=120)
    ap.add_argument("--stop-on-refusal", action="store_true", help="exit 3 at the first refused statement, leaving the database")
    args = ap.parse_args()
    rnd = random.Random(args.seed)
    print(f"seed={args.seed} ops={args.ops} batch={args.batch} rows={args.rows}", flush=True)

    def connect(db):
        kw = dict(host=args.host, port=args.port, dbname=db)
        if args.user:
            kw["user"] = args.user
        conn = psycopg2.connect(**kw)
        conn.autocommit = True
        return conn

    admin = connect("postgres")
    cur = admin.cursor()
    import time
    for attempt in range(30):  # the previous run's slot may still be releasing
        try:
            cur.execute(f"DROP DATABASE IF EXISTS {args.db} WITH (FORCE)")
            break
        except psycopg2.errors.ObjectInUse:
            cur.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active")
            time.sleep(1)
    else:
        cur.execute(f"DROP DATABASE IF EXISTS {args.db} WITH (FORCE)")
    cur.execute(f"CREATE DATABASE {args.db}")
    admin.close()
    store = tempfile.mkdtemp(prefix="koldstore-fuzz.")
    os.chmod(store, 0o777)
    conn = connect(args.db)
    cur = conn.cursor()
    cur.execute("SET client_min_messages TO WARNING")
    cur.execute("CREATE EXTENSION koldstore")
    cur.execute("SET koldstore.min_max_rows_per_file = 1")
    cur.execute("SET koldstore.flush_execution = 'inline'")
    cur.execute("SELECT koldstore.register_storage('fuzz_fs','filesystem',%s,'{}'::jsonb,'{}'::jsonb) IS NOT NULL", (store,))
    cur.execute(ddl())
    cur.execute("CREATE TABLE fz.t_ctl (LIKE fz.t INCLUDING ALL)")
    cur.execute("""SELECT koldstore.manage_table(table_name => 'fz.t'::regclass, storage => 'fuzz_fs',
        hot_row_limit => 5, min_flush_rows => 1, max_rows_per_file => 40, migration_order_by => 'id',
        auto_flush => false) IS NOT NULL""")
    cur.execute("SET koldstore.hydrate_on_write = on")

    names = [n for n, _, _ in COLUMNS]
    set_list = ", ".join(f"{n} = r.{n}" for n in names)
    stats = dict(ins=0, dup=0, upd=0, rng_upd=0, dele=0, flush=0, compact=0, checks=0, refused=0)
    live = set()
    next_id = [1]
    salt = [0]

    def run_both(sql, params=(), expect_same_error=False):
        """Runs on managed then control. Returns False when the managed table refused (control skipped)."""
        try:
            cur.execute(sql.replace("{T}", "fz.t"), params)
            managed_err = None
        except psycopg2.Error as e:
            managed_err = e
        if managed_err is not None:
            msg = str(managed_err)
            if expect_same_error:
                try:
                    cur.execute(sql.replace("{T}", "fz.t_ctl"), params)
                except psycopg2.Error:
                    return False
                fail(f"managed raised but control accepted: {msg}\n  sql={sql} params={params}")
            if any(k in msg for k in ("does not support", "cold rows", "refus", "not supported")):
                stats["refused"] += 1
                if args.stop_on_refusal:
                    print(f"STOP at refusal: {msg}\n  sql={sql} params={params}", flush=True)
                    sys.exit(3)
                if stats["refused"] <= 3:
                    print(f"  refused (skipped on both): {msg.splitlines()[0][:160]}\n    sql={sql[:90]} params={params}", flush=True)
                return False
            fail(f"unexpected managed error: {msg}\n  sql={sql} params={params}")
        try:
            cur.execute(sql.replace("{T}", "fz.t_ctl"), params)
        except psycopg2.Error as e:
            fail(f"control raised but managed accepted: {e}\n  sql={sql} params={params}")
        return True

    def fail(msg):
        print(f"FAIL seed={args.seed}: {msg}", flush=True)
        sys.exit(1)

    def random_settings():
        for k, vals in READ_SETTINGS.items():
            cur.execute(f"SET {k} = %s", (rnd.choice(vals),))

    def reset_settings():
        for k in READ_SETTINGS:
            cur.execute(f"RESET {k}")

    def flush(force=True):
        random_settings()
        for attempt in range(40):
            try:
                cur.execute("SELECT koldstore.flush_table('fz.t'::regclass, %s)", (force,))
                break
            except psycopg2.Error as e:
                m = str(e)
                if attempt < 39 and any(k in m for k in ("apply lock", "retry shortly", "flush unavailable", "flush already in progress")):
                    cur.execute("SELECT pg_sleep(0.05)")
                    continue
                fail(f"flush error: {m}")
        reset_settings()
        stats["flush"] += 1

    def compact():
        for attempt in range(40):
            try:
                cur.execute("SELECT koldstore.compact_table('fz.t'::regclass)")
                cur.execute("SELECT koldstore.reap_compacted_segments('fz.t'::regclass, 0)")
                break
            except psycopg2.Error as e:
                m = str(e)
                if attempt < 39 and any(k in m for k in ("apply lock", "retry shortly", "already in progress", "unavailable")):
                    cur.execute("SELECT pg_sleep(0.05)")
                    continue
                fail(f"compact error: {m}")
        stats["compact"] += 1

    def insert_new(i):
        salt[0] += 1
        ok = run_both("INSERT INTO {T} SELECT (fz.row_for(%s, %s)).*", (i, salt[0]))
        if ok:
            live.add(i)
            stats["ins"] += 1

    for i in range(1, args.rows + 1):
        insert_new(i)
    next_id[0] = args.rows + 1
    flush()

    def check(tag):
        stats["checks"] += 1
        cur.execute("SELECT koldstore.wait_for_async_mirror() IS NOT NULL")
        random_settings()
        problems = []
        cur.execute("SELECT count(*) FROM fz.t")
        n1 = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM fz.t_ctl")
        n2 = cur.fetchone()[0]
        if n1 != n2:
            problems.append(f"row count {n1} != control {n2}")
        cur.execute("SELECT md5(string_agg(t::text, '|' ORDER BY id)) FROM fz.t t")
        h1 = cur.fetchone()[0]
        cur.execute("SELECT md5(string_agg(t::text, '|' ORDER BY id)) FROM fz.t_ctl t")
        h2 = cur.fetchone()[0]
        if h1 != h2:
            diffs = " OR ".join(f"a.{n}::text IS DISTINCT FROM b.{n}::text" for n in names)
            cols = ", ".join(f"CASE WHEN a.{n}::text IS DISTINCT FROM b.{n}::text THEN '{n}' END" for n in names)
            cur.execute(f"""SELECT coalesce(a.id, b.id), array_remove(ARRAY[{cols}], NULL)
                            FROM fz.t a FULL JOIN fz.t_ctl b ON a.id = b.id WHERE {diffs} OR a.id IS NULL OR b.id IS NULL
                            ORDER BY 1 LIMIT 8""")
            for row in cur.fetchall():
                problems.append(f"row id={row[0]} differs in {row[1]}")
        # predicates and ordered queries must agree too
        lo = rnd.randrange(0, 200)
        preds = [
            f"i4 && int4range({lo}, {lo + 30})", f"tz && tstzrange(timestamptz '2024-01-01+00' + {lo} * interval '1 hour', timestamptz '2024-01-02+00' + {lo} * interval '1 hour')",
            f"dr @> date '2024-01-01' + {lo}", f"pt <@ box '((0,0),({lo},{lo}))'", f"ci && circle '<({lo % 50},{lo % 40}),20>'",
            f"(rec).i > {lo}", "mood = 'ok'", "ta @> ARRAY['q\"uote']", f"js @> '{{\"k\": {lo}}}'", "tsv @@ to_tsquery('simple', 'common')",
            "ip << inet '10.128.0.0/9'", f"tag BETWEEN {lo} AND {lo + 100}", f"id BETWEEN {lo} AND {lo + 40}", f"amt > {lo * 1.37}::numeric::money",
            "ia IS NULL", "txt = 'comma,here'", "txt LIKE 'q%'", "doc IS NOT NULL", f"dom > {lo}", "mood IN ('sad', 'happy') AND tag % 2 = 0",
        ]
        for p in preds:
            cur.execute(f"SELECT count(*) FROM fz.t WHERE {p}")
            x = cur.fetchone()[0]
            cur.execute(f"SELECT count(*) FROM fz.t_ctl WHERE {p}")
            y = cur.fetchone()[0]
            if x != y:
                problems.append(f"predicate [{p}] counts {x} != control {y}")
        for q in ("SELECT id FROM fz.{T} ORDER BY tag, id LIMIT 9",
                  "SELECT id FROM fz.{T} WHERE mood <> 'ok' ORDER BY txt, id DESC LIMIT 9",
                  "SELECT mood, count(*), sum(tag) FROM fz.{T} GROUP BY mood ORDER BY mood"):
            cur.execute(q.replace("fz.{T}", "fz.t"))
            r1 = cur.fetchall()
            cur.execute(q.replace("fz.{T}", "fz.t_ctl"))
            r2 = cur.fetchall()
            if r1 != r2:
                problems.append(f"query [{q}] differs: {r1} vs {r2}")
        reset_settings()
        if problems:
            fail(f"after {tag}:\n  " + "\n  ".join(problems))

    check("initial load + flush")
    for op_no in range(1, args.ops + 1):
        r = rnd.random()
        if r < 0.20:
            if live and rnd.random() < 0.15:
                i = rnd.choice(sorted(live))  # duplicate key: both must reject
                salt[0] += 1
                run_both("INSERT INTO {T} SELECT (fz.row_for(%s, %s)).*", (i, salt[0]), expect_same_error=True)
                stats["dup"] += 1
            else:
                i = next_id[0]
                next_id[0] += 1
                insert_new(i)
        elif r < 0.50 and live:
            i = rnd.choice(sorted(live))
            salt[0] += 1
            if rnd.random() < 0.5:
                sql = f"UPDATE {{T}} x SET {set_list} FROM (SELECT (fz.row_for(%s, %s)).*) r WHERE x.id = r.id"
                run_both(sql, (i, salt[0]))
            else:
                run_both("UPDATE {T} SET tag = tag + 1, txt = 'upd ' || id WHERE id = %s", (i,))
            stats["upd"] += 1
        elif r < 0.58 and live:
            lo = rnd.choice(sorted(live))
            run_both("UPDATE {T} SET tag = (tag + 7) %% 1000 WHERE id BETWEEN %s AND %s", (lo, lo + rnd.randrange(1, 15)))
            stats["rng_upd"] += 1
        elif r < 0.72 and live:
            if rnd.random() < 0.6:
                i = rnd.choice(sorted(live))
                if run_both("DELETE FROM {T} WHERE id = %s", (i,)):
                    live.discard(i)
            else:
                lo = rnd.choice(sorted(live))
                hi = lo + rnd.randrange(1, 10)
                if run_both("DELETE FROM {T} WHERE id BETWEEN %s AND %s", (lo, hi)):
                    live -= set(range(lo, hi + 1))
            stats["dele"] += 1
        elif r < 0.90:
            flush(force=rnd.random() < 0.7)
        else:
            compact()
        if op_no % args.batch == 0:
            check(f"op {op_no}")
    flush()
    compact()
    check("final flush + compact")
    cur.execute("SELECT koldstore.unmanage_table('fz.t'::regclass, true, true) IS NOT NULL")
    check("unmanage")
    print("OK", stats, flush=True)


if __name__ == "__main__":
    main()
