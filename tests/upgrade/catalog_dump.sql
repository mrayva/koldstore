-- Dumps everything an extension upgrade must reproduce: member functions (identity,
-- result, symbol, flags, defaults), tables, columns, constraints, indexes, types,
-- triggers and ACLs. Diffed between an upgraded and a freshly created database by
-- scripts/check-upgrade-path.sh.
\echo ---FUNCS
SELECT p.oid::regprocedure::text||' -> '||pg_get_function_result(p.oid)||' | '||l.lanname::text||' | '||coalesce(p.probin,'')||':'||coalesce(p.prosrc,'')||' | sd='||p.prosecdef||' strict='||p.proisstrict::text||' vol='||p.provolatile::text||' par='||p.proparallel::text||' | defaults='||coalesce(pg_get_expr(p.proargdefaults,0),'')
FROM pg_proc p JOIN pg_depend d ON d.objid=p.oid AND d.deptype='e' JOIN pg_extension e ON e.oid=d.refobjid AND e.extname='koldstore' JOIN pg_language l ON l.oid=p.prolang ORDER BY 1;
\echo ---RELS
SELECT c.relkind::text||' '||c.oid::regclass FROM pg_class c JOIN pg_depend d ON d.objid=c.oid AND d.classid='pg_class'::regclass AND d.deptype='e' JOIN pg_extension e ON e.oid=d.refobjid AND e.extname='koldstore' ORDER BY 1;
\echo ---COLS
SELECT attrelid::regclass::text||'.'||attname||' '||format_type(atttypid,atttypmod)||' nn='||attnotnull::text||' def='||coalesce(pg_get_expr(adbin,adrelid),'') FROM pg_attribute a LEFT JOIN pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum WHERE attrelid IN (SELECT oid FROM pg_class WHERE relnamespace='koldstore'::regnamespace AND relkind IN ('r','p')) AND attnum>0 AND NOT attisdropped ORDER BY 1;
\echo ---CONS
SELECT conrelid::regclass::text||' '||conname||' '||pg_get_constraintdef(oid) FROM pg_constraint WHERE connamespace='koldstore'::regnamespace ORDER BY 1;
\echo ---IDX
SELECT indexdef FROM pg_indexes WHERE schemaname='koldstore' ORDER BY 1;
\echo ---TYPES
SELECT t.oid::regtype::text||' '||t.typtype::text FROM pg_type t WHERE typnamespace='koldstore'::regnamespace AND typtype<>'c' AND typname NOT LIKE '\_%' ORDER BY 1;
\echo ---TRIG
SELECT tgrelid::regclass::text||' '||pg_get_triggerdef(oid) FROM pg_trigger WHERE NOT tgisinternal AND tgrelid IN (SELECT oid FROM pg_class WHERE relnamespace='koldstore'::regnamespace);
\echo ---EVT
SELECT evtname||' '||evtevent::text||' '||evtfoid::regproc FROM pg_event_trigger;
\echo ---SEQ/ACL
SELECT c.oid::regclass::text||' '||coalesce(c.relacl::text,'') FROM pg_class c WHERE relnamespace='koldstore'::regnamespace AND relkind IN ('r','S') ORDER BY 1;
\echo ---FUNCACL
SELECT p.oid::regprocedure::text||' '||coalesce(p.proacl::text,'') FROM pg_proc p WHERE pronamespace='koldstore'::regnamespace AND proacl IS NOT NULL ORDER BY 1;
