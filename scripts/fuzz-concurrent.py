#!/usr/bin/env python3
"""Concurrent differential fuzz for koldstore.

N writer sessions each own a disjoint key set (id % N == worker) and keep an exact model of it in
Python while they insert / update / delete / read it, with hydrate_on_write on. Flusher and compactor
sessions run concurrently against the same managed table, so writes, hydration, flush and compaction
interleave. Every point read, owner-filtered range read and the final whole-table comparison must
match the models. Transient errors (deadlock, lock timeout, serialization, flush busy) are retried;
a koldstore refusal is counted and the operation is treated as not applied.

Run with `python3 -I`. Usage: fuzz-concurrent.py [--workers 4] [--seconds 40] [--seed N]
"""
import argparse
import os
import random
import sys
import tempfile
import threading
import time

import psycopg2

TRANSIENT = ("deadlock", "could not serialize", "lock timeout", "canceling statement due to lock",
             "apply lock", "retry shortly", "flush unavailable", "flush already in progress",
             "already in progress", "unavailable", "could not obtain lock")
REFUSAL = ("does not support", "cold rows", "refus", "not supported")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("PGHOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "28818")))
    ap.add_argument("--user", default=os.environ.get("PGUSER", ""))
    ap.add_argument("--db", default="koldstore_fuzz_conc")
    ap.add_argument("--seed", type=int, default=random.SystemRandom().randrange(1 << 30))
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--seconds", type=float, default=40)
    ap.add_argument("--no-hydrate", action="store_true")
    args = ap.parse_args()
    print(f"seed={args.seed} workers={args.workers} seconds={args.seconds}", flush=True)

    def connect(db):
        kw = dict(host=args.host, port=args.port, dbname=db)
        if args.user:
            kw["user"] = args.user
        c = psycopg2.connect(**kw)
        c.autocommit = True
        return c

    for _ in range(60):  # the server may still be in crash recovery from a previous run
        try:
            admin = connect("postgres")
            break
        except psycopg2.OperationalError:
            time.sleep(1)
    else:
        sys.exit("server not accepting connections")
    a = admin.cursor()
    for _ in range(30):
        try:
            a.execute(f"DROP DATABASE IF EXISTS {args.db} WITH (FORCE)")
            break
        except psycopg2.errors.ObjectInUse:
            a.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active")
            time.sleep(1)
    a.execute(f"CREATE DATABASE {args.db}")
    admin.close()
    store = tempfile.mkdtemp(prefix="koldstore-fuzzc.")
    os.chmod(store, 0o777)

    def session():
        c = connect(args.db)
        cur = c.cursor()
        cur.execute("SET koldstore.min_max_rows_per_file = 1")
        cur.execute("SET koldstore.flush_execution = 'inline'")
        cur.execute(f"SET koldstore.hydrate_on_write = {'off' if args.no_hydrate else 'on'}")
        cur.execute("SET lock_timeout = '5s'")
        return c, cur

    setup, s = session()
    s.execute("SET client_min_messages TO WARNING")
    s.execute("CREATE EXTENSION koldstore")
    s.execute("SELECT koldstore.register_storage('fuzzc_fs','filesystem',%s,'{}'::jsonb,'{}'::jsonb) IS NOT NULL", (store,))
    s.execute("""CREATE TABLE public.ct (id bigint PRIMARY KEY, owner int NOT NULL, ver int NOT NULL,
                 payload text, arr int[], doc jsonb, ts timestamptz)""")
    s.execute("""SELECT koldstore.manage_table(table_name => 'public.ct'::regclass, storage => 'fuzzc_fs',
        hot_row_limit => 10, min_flush_rows => 1, max_rows_per_file => 30, migration_order_by => 'id',
        auto_flush => false) IS NOT NULL""")

    stop = threading.Event()
    failures = []
    lock = threading.Lock()
    stats = {"ins": 0, "upd": 0, "del": 0, "read": 0, "range": 0, "flush": 0, "compact": 0, "retry": 0, "refused": 0}
    history = {}  # id -> [(t, op)]
    models = [dict() for _ in range(args.workers)]  # id -> (ver, payload)

    def log(i, op):
        history.setdefault(i, []).append((round(time.time() - t0, 3), op))

    t0 = time.time()

    def bump(k, n=1):
        with lock:
            stats[k] += n

    def fail(msg):
        with lock:
            failures.append(msg)
        stop.set()

    def exec_retry(cur, sql, params=(), tries=30):
        """Returns (ok, rowcount_or_rows). ok=False means refused (not applied)."""
        for attempt in range(tries):
            try:
                cur.execute(sql, params)
                return True, cur
            except psycopg2.Error as e:
                m = str(e).lower()
                if any(k in m for k in TRANSIENT):
                    bump("retry")
                    time.sleep(0.02 * (attempt + 1))
                    continue
                if any(k in m for k in REFUSAL):
                    bump("refused")
                    return False, None
                raise
        fail(f"gave up retrying: {sql[:80]}")
        return False, None

    def val(i, ver):
        return (f"p{i}v{ver}", [i % 7, ver], {"i": i, "v": ver}, f"2026-01-{1 + i % 28:02d} 00:00:{ver % 60:02d}+00")

    def writer(w):
        rnd = random.Random(args.seed * 1000 + w)
        c, cur = session()
        model = models[w]
        next_id = w + args.workers  # ids congruent to w mod workers, starting past 0
        try:
            while not stop.is_set():
                r = rnd.random()
                if r < 0.30 or not model:
                    i = next_id
                    next_id += args.workers
                    p, arr, doc, ts = val(i, 0)
                    ok, _ = exec_retry(cur, "INSERT INTO public.ct VALUES (%s,%s,0,%s,%s,%s::jsonb,%s)",
                                       (i, w, p, arr, psycopg2.extras.Json(doc) if False else __import__("json").dumps(doc), ts))
                    if ok:
                        model[i] = 0
                        log(i, 'insert')
                        bump("ins")
                elif r < 0.60:
                    i = rnd.choice(list(model))
                    nv = model[i] + 1
                    p, arr, doc, ts = val(i, nv)
                    ok, _ = exec_retry(cur, "UPDATE public.ct SET ver=%s, payload=%s, arr=%s, doc=%s::jsonb, ts=%s WHERE id=%s",
                                       (nv, p, arr, __import__("json").dumps(doc), ts, i))
                    if ok:
                        if cur.rowcount != 1:
                            fail(f"worker {w}: UPDATE id={i} touched {cur.rowcount} rows (expected 1)")
                        model[i] = nv
                        log(i, f'update->{nv}')
                        bump("upd")
                elif r < 0.68:
                    # owner-filtered range update: only own rows change, other workers' rows are untouched
                    lo = rnd.choice(list(model))
                    hi = lo + rnd.randrange(1, 40)
                    ok, _ = exec_retry(cur, "UPDATE public.ct SET ver = ver + 1, payload = 'p' || id || 'v' || (ver + 1) "
                                       "WHERE owner = %s AND id BETWEEN %s AND %s", (w, lo, hi))
                    if ok:
                        expect = [i for i in model if lo <= i <= hi]
                        if cur.rowcount != len(expect):
                            fail(f"worker {w}: range UPDATE {lo}..{hi} touched {cur.rowcount}, model has {len(expect)}")
                        for i in expect:
                            model[i] += 1
                            log(i, f'rangeupd->{model[i]}')
                            # payload must keep matching: val() uses the same 'p<id>v<ver>' scheme
                        bump("upd")
                elif r < 0.78:
                    i = rnd.choice(list(model))
                    ok, _ = exec_retry(cur, "DELETE FROM public.ct WHERE id = %s", (i,))
                    if ok:
                        if cur.rowcount != 1:
                            fail(f"worker {w}: DELETE id={i} touched {cur.rowcount} rows (expected 1)")
                        del model[i]
                        log(i, 'delete')
                        bump("del")
                elif r < 0.90:
                    i = rnd.choice(list(model))
                    ok, _ = exec_retry(cur, "SELECT ver, payload, arr, doc->>'v' FROM public.ct WHERE id = %s", (i,))
                    if ok:
                        row = cur.fetchall()
                        v = model[i]
                        want = [(v, f"p{i}v{v}", [i % 7, v] if True else None, str(v))]
                        # range updates rewrite only ver/payload, so arr/doc keep their last full-row value
                        if len(row) != 1 or row[0][0] != v or row[0][1] != f"p{i}v{v}":
                            fail(f"worker {w}: point read id={i} got {row}, model ver={v}")
                        bump("read")
                else:
                    # A committed DELETE of a formerly-cold key is masked once the async mirror applies
                    # its tombstone (documented lag), so fence before comparing with the exact model.
                    exec_retry(cur, "SELECT koldstore.wait_for_async_mirror()")
                    ok, _ = exec_retry(cur, "SELECT id, ver FROM public.ct WHERE owner = %s ORDER BY id", (w,))
                    if ok:
                        rows = cur.fetchall()
                        want = sorted(model.items())
                        if [tuple(x) for x in rows] != want:
                            have, exp = dict(rows), dict(want)
                            diff = [(k, have.get(k), exp.get(k)) for k in sorted(set(have) | set(exp)) if have.get(k) != exp.get(k)][:5]
                            fail(f"worker {w}: owner scan differs ({len(rows)} vs {len(want)} rows); first diffs {diff}; history {[history.get(d[0]) for d in diff[:2]]} observed at t={round(time.time()-t0,3)}")
                        bump("range")
        except Exception as e:  # noqa: BLE001
            fail(f"worker {w}: {type(e).__name__}: {e}")

    def maintenance(kind):
        rnd = random.Random(args.seed + hash(kind) % 1000)
        c, cur = session()
        try:
            while not stop.is_set():
                if kind == "flush":
                    ok, _ = exec_retry(cur, "SELECT koldstore.flush_table('public.ct'::regclass, %s)", (rnd.random() < 0.7,))
                else:
                    ok, _ = exec_retry(cur, "SELECT koldstore.compact_table('public.ct'::regclass)")
                    if ok:
                        exec_retry(cur, "SELECT koldstore.reap_compacted_segments('public.ct'::regclass, 5)")
                if ok:
                    bump(kind)
                time.sleep(rnd.uniform(0.05, 0.4))
        except Exception as e:  # noqa: BLE001
            fail(f"{kind}: {type(e).__name__}: {e}")

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(args.workers)]
    threads += [threading.Thread(target=maintenance, args=("flush",)), threading.Thread(target=maintenance, args=("flush",)),
                threading.Thread(target=maintenance, args=("compact",))]
    for t in threads:
        t.start()
    deadline = time.time() + args.seconds
    while time.time() < deadline and not stop.is_set():
        time.sleep(0.5)
    stop.set()
    for t in threads:
        t.join()

    if failures:
        print(f"FAIL seed={args.seed}: {failures[0]}", flush=True)
        for f in failures[1:5]:
            print("  also:", f)
        sys.exit(1)

    # Quiesce, then compare the whole table with the union of the models, before and after a full
    # flush + compaction and after unmanaging.
    def final_check(label):
        s.execute("SELECT koldstore.wait_for_async_mirror() IS NOT NULL")
        s.execute("SELECT id, owner, ver, payload FROM public.ct ORDER BY id")
        have = {r[0]: r for r in s.fetchall()}
        want = {}
        for w, m in enumerate(models):
            for i, v in m.items():
                want[i] = (i, w, v, f"p{i}v{v}")
        if set(have) != set(want):
            miss = sorted(set(want) - set(have))[:5]
            extra = sorted(set(have) - set(want))[:5]
            print(f"FAIL seed={args.seed} {label}: row set differs: missing {miss} extra {extra} ({len(have)} vs {len(want)})")
            sys.exit(1)
        bad = [(i, have[i], want[i]) for i in want if tuple(have[i]) != want[i]][:5]
        if bad:
            print(f"FAIL seed={args.seed} {label}: content differs: {bad}")
            sys.exit(1)
        print(f"  {label}: {len(have)} rows match", flush=True)

    final_check("after concurrent phase")
    s.execute("SET lock_timeout = 0")
    s.execute("SELECT koldstore.flush_table('public.ct'::regclass, true)")
    s.execute("SELECT koldstore.compact_table('public.ct'::regclass)")
    s.execute("SELECT koldstore.reap_compacted_segments('public.ct'::regclass, 0)")
    final_check("after flush + compact")
    s.execute("SELECT koldstore.unmanage_table('public.ct'::regclass, true, true) IS NOT NULL")
    final_check("after unmanage")
    print("OK", stats, flush=True)


if __name__ == "__main__":
    import psycopg2.extras  # noqa: F401
    main()
