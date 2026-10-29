#!/usr/bin/env bash
# Stage 5 of docs/plans/multi-master-single-tiering-leader.md: a scratch 3-node Spock mesh
# with ONE koldstore tiering leader (n1) and two followers (n2, n3) over shared filesystem
# cold storage. Exercises: concurrent writes, leader flush + follower sync/prune, writes to
# rows that are cold on a peer (reconciler heals), leader kill + lease takeover, stale-leader
# fencing, demotion + catch-up, compaction + reap, and a reap-vs-stale-follower race.
#
# Needs a PostgreSQL built with Spock's core patches plus the spock extension, and a built
# koldstore package. Uses its own data dirs and ports; never touches another cluster.
#
#   SPOCK_PREFIX=~/pg-spock/install KOLDSTORE_PKG=/tmp/kl-stage/pkg-fol scripts/test-mesh-single-leader.sh
#
# Output is a transcript to read, not a pass/fail gate: compare the per-node "count:md5"
# lines, which must match after every "heal" step.
set -uo pipefail
SP=${SPOCK_PREFIX:?set SPOCK_PREFIX to the Spock-patched PostgreSQL install prefix}
PKG=${KOLDSTORE_PKG:?set KOLDSTORE_PKG to a cargo pgrx package output dir}
PG_BIN=$SP/bin
PG_OPTS="-c dynamic_library_path='$PKG/usr/lib/postgresql/18/lib:\$libdir' -c extension_control_path='$PKG/usr/share/postgresql/18:\$system'"
P1=28941; P2=28942; P3=28943
W=${MESH_DIR:-/tmp/ks-mesh}/data
SOCK=${MESH_DIR:-/tmp/ks-mesh}/sock; SHARE=${MESH_DIR:-/tmp/ks-mesh}/share
q() { local port=$1 db=$2; shift 2; $PG_BIN/psql -X -q -At -h $SOCK -p $port -d $db "$@" 2>&1 | grep -v pg_circuit; }
say() { echo; echo "== $*"; }
start() { $PG_BIN/pg_ctl -D $W/n$1 -l $W/n$1.log -w -o "-p $2 -k $SOCK $PG_OPTS" start >/dev/null || { tail -20 $W/n$1.log; exit 1; }; }
for n in 1 2 3; do $PG_BIN/pg_ctl -D $W/n$n -m immediate stop >/dev/null 2>&1; done
rm -rf "${MESH_DIR:-/tmp/ks-mesh}"; mkdir -p $W $SOCK $SHARE
i=0
for P in $P1 $P2 $P3; do
  i=$((i+1))
  $PG_BIN/initdb -D $W/n$i -A trust >/dev/null
  cat >> $W/n$i/postgresql.conf <<EOC
shared_preload_libraries = 'spock,koldstore'
wal_level = logical
max_replication_slots = 20
max_wal_senders = 20
max_worker_processes = 60
track_commit_timestamp = on
output_plugin_libraries = 'spock_output,pgoutput'
koldstore.capture_replicated_changes = on
koldstore.hydrate_on_write = on
koldstore.spock_reconcile_interval_seconds = 4
koldstore.spock_reconcile_databases = 'spike'
koldstore.catalog_sync_interval_seconds = 2
koldstore.catalog_sync_databases = 'spike'
EOC
  start $i $P
  q $P postgres -c "CREATE DATABASE spike" >/dev/null
  $PG_BIN/pg_ctl -D $W/n$i -m fast -w restart -o "-p $P -k $SOCK $PG_OPTS" >/dev/null
done
ports=($P1 $P2 $P3)
say "mesh"
for i in 1 2 3; do P=${ports[$((i-1))]}
  q $P spike -c "CREATE EXTENSION spock; CREATE EXTENSION koldstore; SELECT spock.node_create('n$i','host=$SOCK port=$P dbname=spike') IS NOT NULL" | tail -1
done
for i in 1 2 3; do for j in 1 2 3; do [ $i = $j ] && continue
  q ${ports[$((i-1))]} spike -c "SELECT spock.sub_create('s${i}_$j','host=$SOCK port=${ports[$((j-1))]} dbname=spike') IS NOT NULL" | tail -1 >/dev/null
done; done
sleep 4
for i in 1 2 3; do echo "n$i subs: $(q ${ports[$((i-1))]} spike -c "SELECT string_agg(status,',') FROM spock.sub_show_status()")"; done
REG="SELECT koldstore.register_storage('sh','filesystem','$SHARE','{}'::jsonb,'{}'::jsonb) IS NOT NULL;"
REGB="SELECT koldstore.register_storage('sh','filesystem','$SHARE','{}'::jsonb,'{}'::jsonb, \"check\"=>false) IS NOT NULL;"
for P in ${ports[@]}; do q $P spike -c "CREATE TABLE t (id bigint PRIMARY KEY, v text NOT NULL); SELECT spock.repset_add_table('default','t')" >/dev/null; done
snap() { q $1 spike -c "SELECT count(*) || ':' || md5(string_agg(id||'='||v, ',' ORDER BY id)) FROM t"; }
heapn() { q $1 spike -c "CREATE EXTENSION IF NOT EXISTS pageinspect; SELECT count(*) FROM generate_series(0, GREATEST(pg_relation_size('t')/8192-1,0)) p, heap_page_items(get_raw_page('t',p::int)) i WHERE i.lp_flags=1 AND (i.t_xmax=0 OR (i.t_infomask & 128)<>0 OR (i.t_infomask & 2048)<>0)"; }
allsnap() { for i in 1 2 3; do echo "  n$i: $(snap ${ports[$((i-1))]})"; done; }
settle() { sleep 4; }
say "seed 300 rows on n1 (Spock replicates)"
q $P1 spike -c "INSERT INTO t SELECT g,'v'||g FROM generate_series(1,300) g" >/dev/null; settle
allsnap
say "n1 becomes leader: manage, flush, lease, publish"
q $P1 spike <<EOC
$REG
SET koldstore.min_max_rows_per_file=1;
SELECT koldstore.manage_table('t'::regclass, storage=>'sh', hot_row_limit=>20, min_flush_rows=>1, max_rows_per_file=>100, migration_order_by=>'id', auto_flush=>false) IS NOT NULL;
EOC
flush1() { q $1 spike -c "SET koldstore.flush_execution='inline'; SET koldstore.min_max_rows_per_file=1; SELECT koldstore.flush_table('t'::regclass, true)->>'status'" | tail -1; }
echo "flush: $(flush1 $P1)"
q $P1 spike -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null
echo "lease: $(q $P1 spike -c "SELECT koldstore.acquire_leader_lease('t'::regclass, 8)::text" | tail -1 | cut -c1-120)"
echo "leader: $(q $P1 spike -c "SELECT koldstore.set_table_leader('t'::regclass, true)")"
q $P1 spike -c "SELECT koldstore.export_table_metadata('t'::regclass)::text" > $W/meta.json
META=$(cat $W/meta.json)
for i in 2 3; do P=${ports[$((i-1))]}
  q $P spike >/dev/null <<EOC
$REGB
SET koldstore.min_max_rows_per_file=1;
SELECT koldstore.import_table_metadata('t'::regclass, \$m\$$META\$m\$::jsonb) IS NOT NULL;
SELECT koldstore.set_table_follower('t'::regclass, true);
EOC
  echo "n$i follower: $(q $P spike -c "SELECT options->>'follower' FROM koldstore.schemas WHERE active AND table_oid='t'::regclass")"
done
settle
echo "heap live: n1=$(heapn $P1) n2=$(heapn $P2) n3=$(heapn $P3)"; allsnap
say "concurrent writes on all three nodes (new ids per node + updates of hot rows)"
for i in 1 2 3; do P=${ports[$((i-1))]}
  ( q $P spike -c "INSERT INTO t SELECT g,'n$i-'||g FROM generate_series(1000*$i,1000*$i+199) g"; q $P spike -c "UPDATE t SET v='upd-n$i' WHERE id = $((280+i))" ) &
done; wait; settle
allsnap
rec() { for i in 1 2 3; do echo "  n$i reconciled: $(q ${ports[$((i-1))]} spike -c "SELECT coalesce(string_agg(status||'='||c,','),'-') FROM (SELECT status,count(*) c FROM koldstore.spock_reconciled GROUP BY 1) x")"; done; }
heal() { echo "-- waiting for the reconcile worker"; sleep 11; allsnap; rec; }
exc() { for i in 1 2 3; do echo "  n$i spock exceptions: $(q ${ports[$((i-1))]} spike -c "SELECT count(*) FROM spock.exception_log")"; done; }
exc
heal
say "leader flush #2, followers sync + prune"
q $P1 spike -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null
echo "flush: $(flush1 $P1)"
segs() { q $1 spike -c "SELECT count(*) FROM koldstore.cold_segments WHERE status='active'"; }
for k in $(seq 1 10); do [ "$(segs $P2)" = "$(segs $P1)" ] && [ "$(segs $P3)" = "$(segs $P1)" ] && break; sleep 1; done
echo "segments n1=$(segs $P1) n2=$(segs $P2) n3=$(segs $P3)"
echo "heap live: n1=$(heapn $P1) n2=$(heapn $P2) n3=$(heapn $P3)"; allsnap
say "writes after prune, then DELETE/UPDATE of a row cold on followers (Finding 3 territory)"
q $P2 spike -c "UPDATE t SET v='post-prune-n2' WHERE id=10; DELETE FROM t WHERE id=11" >/dev/null; settle
echo "id10: n1=$(q $P1 spike -c 'SELECT v FROM t WHERE id=10') n2=$(q $P2 spike -c 'SELECT v FROM t WHERE id=10') n3=$(q $P3 spike -c 'SELECT v FROM t WHERE id=10')"
echo "id11 present: n1=$(q $P1 spike -c 'SELECT count(*) FROM t WHERE id=11') n2=$(q $P2 spike -c 'SELECT count(*) FROM t WHERE id=11') n3=$(q $P3 spike -c 'SELECT count(*) FROM t WHERE id=11')"
allsnap
heal
say "kill leader n1 (immediate)"
$PG_BIN/pg_ctl -D $W/n1 -m immediate stop >/dev/null
q $P2 spike -c "SELECT koldstore.set_table_follower('t'::regclass, false)" >/dev/null
echo "n2 takeover before ttl: $(q $P2 spike -c "SELECT koldstore.acquire_leader_lease('t'::regclass, 8)::text" | head -1 | cut -c1-140)"
sleep 10
echo "n2 takeover after ttl: $(q $P2 spike -c "SELECT koldstore.acquire_leader_lease('t'::regclass, 8)::text" | tail -1 | cut -c1-140)"
echo "n2 -> leader: $(q $P2 spike -c "SELECT koldstore.set_table_leader('t'::regclass, true)" | tr '\n' ' ')"
echo "n2 lease: $(q $P2 spike -c "SELECT koldstore.leader_lease_status('t'::regclass)::text" | tail -1 | cut -c1-200)"
say "writes with n1 down, n2 flushes as new leader, n3 follows"
q $P3 spike -c "INSERT INTO t SELECT g,'n3b-'||g FROM generate_series(5000,5099) g" >/dev/null
q $P2 spike -c "INSERT INTO t SELECT g,'n2b-'||g FROM generate_series(6000,6099) g" >/dev/null; settle
q $P2 spike -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null
echo "n2 flush: $(flush1 $P2)"
for k in $(seq 1 10); do [ "$(segs $P3)" = "$(segs $P2)" ] && break; sleep 1; done
echo "segments n2=$(segs $P2) n3=$(segs $P3)"
echo "heap live: n2=$(heapn $P2) n3=$(heapn $P3)"
echo "n3 snap=$(snap $P3) n2 snap=$(snap $P2)"
say "n1 restarts: stale leader must be fenced"
start 1 $P1; sleep 3
echo "n1 flush: $(q $P1 spike -c "SET koldstore.flush_execution='inline'; SELECT koldstore.flush_table('t'::regclass, true)" | head -1 | cut -c1-160)"
echo "n1 renew: $(q $P1 spike -c "SELECT koldstore.renew_leader_lease('t'::regclass)::text" | head -1 | cut -c1-160)"
echo "n1 demoted: $(q $P1 spike -c "SELECT koldstore.set_table_leader('t'::regclass, false); SELECT koldstore.set_table_follower('t'::regclass, true)" | tr '\n' ' ')"
sleep 8
echo "n1 snap after catch-up: $(snap $P1)"; allsnap
say "reap with all followers caught up"
echo "n2 compact: $(q $P2 spike -c "SELECT koldstore.compact_table('t'::regclass)::text" | tail -1 | cut -c1-120)"
echo "n2 reap g=0: $(q $P2 spike -c "SELECT koldstore.reap_compacted_segments('t'::regclass, 0)::text" | tail -1 | cut -c1-120)"
for k in $(seq 1 8); do [ "$(segs $P3)" = "$(segs $P2)" ] && break; sleep 1; done
echo "n3 read after reap: $(snap $P3)   n2: $(snap $P2)"
for i in 2 3; do echo "validate n$i: $(q ${ports[$((i-1))]} spike -c "SELECT koldstore.validate_cold_storage('t'::regclass, true)->>'ok'" 2>&1 | tail -1 | cut -c1-120)"; done
say "adversarial reap: n3 offline, leader rewrites + reaps with grace 0, n3 reads before its next sync"
$PG_BIN/pg_ctl -D $W/n3 -m immediate stop >/dev/null
for r in 1 2 3; do
  q $P2 spike -c "INSERT INTO t SELECT g,'late-'||g FROM generate_series(9000+300*$r,9299+300*$r) g" >/dev/null
  q $P2 spike -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null
  flush1 $P2 >/dev/null
done
q $P2 spike -c "UPDATE t SET v='re-'||id WHERE id BETWEEN 2000 AND 2199 OR id BETWEEN 9300 AND 9500" >/dev/null
q $P2 spike -c "SELECT koldstore.wait_for_async_mirror() IS NOT NULL" >/dev/null
flush1 $P2 >/dev/null
echo "n2 segments: $(segs $P2)"
echo "n2 compact: $(q $P2 spike -c "SELECT koldstore.compact_table('t'::regclass)::text" | tail -1 | cut -c1-80)"
echo "n2 reap g=0: $(q $P2 spike -c "SELECT koldstore.reap_compacted_segments('t'::regclass, 0)::text" | tail -1 | cut -c1-80)"
start 3 $P3
echo "n3 read right after restart (stale catalog, segments gone): $(snap $P3 | head -1 | cut -c1-160)"
for k in $(seq 1 15); do [ "$(segs $P3)" = "$(segs $P2)" ] && break; sleep 1; done
sleep 3
echo "n3 after sync (waited ~${k}s): $(snap $P3)   n2: $(snap $P2)"
echo "validate n3: $(q $P3 spike -c "SELECT koldstore.validate_cold_storage('t'::regclass, true)->>'ok'" | tail -1)"
say "final"
allsnap
for n in 1 2 3; do $PG_BIN/pg_ctl -D $W/n$n -m immediate stop >/dev/null 2>&1; done
echo "== done"
