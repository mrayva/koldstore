//! PostgreSQL GUC registration.

use crate::settings;

#[cfg(feature = "pg")]
use std::ffi::CString;

#[cfg(feature = "pg")]
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

#[cfg(feature = "pg")]
static COLD_READS: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(Some(c"auto"));
#[cfg(feature = "pg")]
static USER_ID: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(Some(c""));
#[cfg(feature = "pg")]
static MAX_OPEN_PARQUET_READERS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_MAX_OPEN_PARQUET_READERS);
#[cfg(feature = "pg")]
static MAX_MERGE_SEEN_KEYS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_MAX_MERGE_SEEN_KEYS);
#[cfg(feature = "pg")]
static OBJECT_STORE_TIMEOUT_MS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_OBJECT_STORE_TIMEOUT_MS);
#[cfg(feature = "pg")]
static LOG_LEVEL: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(Some(c"info"));
#[cfg(feature = "pg")]
static ENABLE_MERGE_SCAN: GucSetting<bool> = GucSetting::<bool>::new(true);
#[cfg(feature = "pg")]
static ALLOW_SAME_TXN_COLD_READS: GucSetting<bool> = GucSetting::<bool>::new(false);
#[cfg(feature = "pg")]
static GUARD_SCAN_WRITES: GucSetting<bool> = GucSetting::<bool>::new(true);
#[cfg(feature = "pg")]
static HYDRATE_ON_WRITE: GucSetting<bool> = GucSetting::<bool>::new(false);
#[cfg(feature = "pg")]
static HYDRATING: GucSetting<bool> = GucSetting::<bool>::new(false);
#[cfg(feature = "pg")]
static HYDRATE_TAKE_JOB_LOCK: GucSetting<bool> = GucSetting::<bool>::new(false);
#[cfg(feature = "pg")]
static HYDRATE_FENCE_MIRROR: GucSetting<bool> = GucSetting::<bool>::new(true);
#[cfg(feature = "pg")]
static CAPTURE_REPLICATED_CHANGES: GucSetting<bool> = GucSetting::<bool>::new(false);
#[cfg(feature = "pg")]
static SPOCK_RECONCILE_INTERVAL: GucSetting<i32> = GucSetting::<i32>::new(0);
#[cfg(feature = "pg")]
static SPOCK_RECONCILE_DATABASES: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
#[cfg(feature = "pg")]
static MAX_HYDRATE_ROWS: GucSetting<i32> = GucSetting::<i32>::new(10_000);
#[cfg(feature = "pg")]
static HYDRATE_SLOT_LOCK_POLL_MS: GucSetting<i32> = GucSetting::<i32>::new(0);
#[cfg(feature = "pg")]
static REJECT_SERIALIZABLE_COLD_READS: GucSetting<bool> = GucSetting::<bool>::new(true);
#[cfg(feature = "pg")]
static INTERNAL_SYSTEM_WRITE: GucSetting<bool> = GucSetting::<bool>::new(false);
#[cfg(feature = "pg")]
static INTERNAL_FLUSH_CLEANUP: GucSetting<bool> = GucSetting::<bool>::new(false);
#[cfg(feature = "pg")]
static INTERNAL_ASYNC_MIRROR_WORKER: GucSetting<bool> = GucSetting::<bool>::new(true);
#[cfg(feature = "pg")]
static MIN_MAX_ROWS_PER_FILE: GucSetting<i32> =
    GucSetting::<i32>::new(settings::default_min_max_rows_per_file());
#[cfg(feature = "pg")]
static FAILPOINT: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(Some(c""));
#[cfg(feature = "pg")]
static PENDING_SEGMENT_TTL_SECONDS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_PENDING_SEGMENT_TTL_SECONDS);
#[cfg(feature = "pg")]
static FLUSH_CHECK_INTERVAL_SECONDS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_FLUSH_CHECK_INTERVAL_SECONDS);
#[cfg(feature = "pg")]
static MAX_PARALLEL_FLUSH_JOBS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_MAX_PARALLEL_FLUSH_JOBS);
#[cfg(feature = "pg")]
static FLUSH_JOB_MAX_RUNTIME_SECONDS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_FLUSH_JOB_MAX_RUNTIME_SECONDS);
#[cfg(feature = "pg")]
static JOB_RETENTION_DAYS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_JOB_RETENTION_DAYS);
#[cfg(feature = "pg")]
static FLUSH_EXECUTION: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"queue"));
#[cfg(feature = "pg")]
static ASYNC_APPLY_WATCHDOG_INTERVAL_MS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_ASYNC_APPLY_WATCHDOG_INTERVAL_MS);
#[cfg(feature = "pg")]
static ASYNC_APPLY_MAX_ROWS_PER_TICK: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_ASYNC_APPLY_MAX_ROWS_PER_TICK);
#[cfg(feature = "pg")]
static ASYNC_APPLY_MAX_MS_PER_TICK: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_ASYNC_APPLY_MAX_MS_PER_TICK);
#[cfg(feature = "pg")]
static FLUSH_PRELOCK_MAX_PASSES: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_FLUSH_PRELOCK_MAX_PASSES);
#[cfg(feature = "pg")]
static FLUSH_PRELOCK_MAX_MS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_FLUSH_PRELOCK_MAX_MS);
#[cfg(feature = "pg")]
static ASYNC_MIRROR_MAX_RETAINED_BYTES: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_ASYNC_MIRROR_MAX_RETAINED_BYTES);

/// Defines pg-koldstore configuration variables.
#[cfg(feature = "pg")]
pub fn define_gucs() {
    let flags = GucFlags::default();
    GucRegistry::define_string_guc(
        c"koldstore.user_id",
        c"Active user/tenant scope for user-scoped managed tables.",
        c"Fail-closed session scope for user-typed tables. Must be set before scoped DML, SELECT, and changes_since. Empty means unset.",
        &USER_ID,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_string_guc(
        c"koldstore.cold_reads",
        c"Controls KoldStore cold reads.",
        c"Controls whether KoldStore reads cold Parquet data. Supported values are auto, on, and off.",
        &COLD_READS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.max_open_parquet_readers",
        c"Maximum open KoldStore Parquet readers.",
        c"Caps concurrent open Parquet readers per PostgreSQL backend (fail-fast when exceeded).",
        &MAX_OPEN_PARQUET_READERS,
        settings::MIN_CONCURRENCY_LIMIT,
        settings::MAX_CONCURRENCY_LIMIT,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.max_merge_seen_keys",
        c"Maximum exact PK identities retained by one KoldMergeScan.",
        c"Fail-closed per-scan cap on the compact winner seen-set. Protects backends from accidental full-table scans. 0 disables the cap.",
        &MAX_MERGE_SEEN_KEYS,
        settings::MIN_MAX_MERGE_SEEN_KEYS,
        settings::MAX_MAX_MERGE_SEEN_KEYS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.object_store_timeout_ms",
        c"Timeout for one ObjectStore or Parquet segment operation.",
        c"Fail-fast wall-clock budget for cold reads and flush object I/O. 0 disables the timeout (query cancel still aborts in-flight waits). Clamped to 0..=600000 milliseconds.",
        &OBJECT_STORE_TIMEOUT_MS,
        settings::MIN_OBJECT_STORE_TIMEOUT_MS,
        settings::MAX_OBJECT_STORE_TIMEOUT_MS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_string_guc(
        c"koldstore.log_level",
        c"KoldStore log level.",
        c"Controls KoldStore logging verbosity. Intended values are error, warn, info, debug, and trace.",
        &LOG_LEVEL,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.enable_merge_scan",
        c"Enables KoldStore merge scans.",
        c"Required for managed-table SELECT. When off, KoldMergeScan errors instead of allowing an incorrect heap-only read.",
        &ENABLE_MERGE_SCAN,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.allow_same_txn_cold_reads",
        c"Allows reading cold data of a managed table already modified in this transaction.",
        c"By default a managed-table read that must consult cold storage fails once the same transaction has modified that table: logical decoding cannot see uncommitted work, so a cold row for a key changed in this transaction could be returned stale (upstream #121). Turn on to accept that risk.",
        &ALLOW_SAME_TXN_COLD_READS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.guard_scan_writes",
        c"Rejects UPDATE/DELETE whose WHERE clause also matches cold-only rows.",
        c"A plain UPDATE/DELETE only sees the hot heap. When on (default), after such a statement on a managed table with cold data KoldStore counts how many cold rows its WHERE clause matches and rejects the statement if any were left untouched (upstream #122). This reads cold storage for every non-primary-key UPDATE/DELETE on such a table; turn off to skip the check and accept silently partial writes.",
        &GUARD_SCAN_WRITES,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.hydrate_on_write",
        c"EXPERIMENTAL: lets UPDATE/DELETE change cold-only rows by hydrating them first.",
        c"Before a single-table UPDATE/DELETE on a managed table scans, the cold-only rows its WHERE clause matches are inserted into the heap (up to koldstore.max_hydrate_rows) so the native statement can act on them. READ COMMITTED only; other statements keep being rejected by the write guards (upstream #122).",
        &HYDRATE_ON_WRITE,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.hydrate_fence_mirror",
        c"Applies committed WAL to the async mirror before hydrate-on-write looks for cold rows.",
        c"A delete that committed moments ago is not masked from cold reads until the asynchronous mirror applies its tombstone; without this fence a hydrating statement in that window sees the stale cold copy, re-hydrates it and acts on a row that is already gone (an UPDATE succeeds after the DELETE, or a deleted row reappears). The fence costs a mirror apply pass per hydrating statement. Turn off only if you accept that window.",
        &HYDRATE_FENCE_MIRROR,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.hydrate_take_job_lock",
        c"Makes hydrate-on-write hold the table's flush/maintenance job lock for the statement.",
        c"Off by default: a hydrated row is uncommitted, so a concurrent flush cannot see or prune it, and holding the lock serializes every hydrating statement behind any running flush (measured: ~10 tps against ~16000 tps with a continuously running flusher). Turn on to be conservative; waits are bounded (5 s) and fail with an ordinary error.",
        &HYDRATE_TAKE_JOB_LOCK,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.hydrating",
        c"On while KoldStore inserts a cold row back into the heap (hydration).",
        c"Set by KoldStore around the hydration INSERT (hydrate_pk, update_row, delete_row, hydrate-on-write) together with session_replication_role = replica, so ordinary user triggers do not fire for it. A trigger marked ENABLE ALWAYS/REPLICA can test current_setting('koldstore.hydrating', true) = 'on' to tell hydration from a real insert. Not meant to be set by applications.",
        &HYDRATING,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.capture_replicated_changes",
        c"Mirrors changes applied by logical replication (Spock, native subscriptions).",
        c"By default the async mirror ignores every change that carries a replication origin, which includes everything a Spock apply worker or a subscription writes. Turn on for a node that receives replicated writes: flush prunes are then stamped with a named origin (koldstore_flush_<dboid>) and skipped by name instead. Changing it needs a restart, and the async mirror must be fully caught up (koldstore.wait_for_async_mirror()) first, otherwise prune deletes still in the slot are mirrored as tombstones.",
        &CAPTURE_REPLICATED_CHANGES,
        GucContext::Postmaster,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.spock_reconcile_interval_seconds",
        c"Seconds between background runs of koldstore.reconcile_spock_conflicts(); 0 disables.",
        c"When positive, a background worker per database named in koldstore.spock_reconcile_databases calls koldstore.reconcile_spock_conflicts() on this interval. Needs koldstore.capture_replicated_changes = on. Changing it requires a restart.",
        &SPOCK_RECONCILE_INTERVAL,
        0,
        86_400,
        GucContext::Postmaster,
        flags,
    );
    GucRegistry::define_string_guc(
        c"koldstore.spock_reconcile_databases",
        c"Comma-separated databases that get a Spock-conflict reconciler worker.",
        c"One background worker is started per listed database (typically the Spock database). Changing it requires a restart.",
        &SPOCK_RECONCILE_DATABASES,
        GucContext::Postmaster,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.hydrate_slot_lock_poll_ms",
        c"Polls (instead of queueing) for the async-mirror slot lock during a hydrate-on-write fence, for at most this many milliseconds.",
        c"0 (default) queues on the lock, so the deadlock detector resolves lock cycles. A positive value polls, which keeps the WAL applier and flush from being starved by many concurrent hydrating transactions (about 1.5-3x the throughput at 8 clients) but fails the statement with a retryable serialization_failure when the lock is still busy at the deadline.",
        &HYDRATE_SLOT_LOCK_POLL_MS,
        0,
        60_000,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.max_hydrate_rows",
        c"Most cold rows one statement may hydrate.",
        c"Statements matching more cold-only rows are rejected instead of hydrating.",
        &MAX_HYDRATE_ROWS,
        1,
        10_000_000,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.reject_serializable_cold_reads",
        c"Rejects cold reads under SERIALIZABLE isolation.",
        c"Cold rows live in Parquet segments and take no SSI predicate locks, so a SERIALIZABLE transaction that reads them does not get PostgreSQL's serializability guarantee for those rows. Default on (2026-09-27, matching upstream #121's same-transaction guard): fail such reads rather than silently running them with a weaker guarantee. Turn off to accept the weaker guarantee instead.",
        &REJECT_SERIALIZABLE_COLD_READS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.internal_system_write",
        c"Allows internal KoldStore system writes.",
        c"Internal guard used by extension-owned maintenance paths.",
        &INTERNAL_SYSTEM_WRITE,
        GucContext::Suset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.internal_flush_cleanup",
        c"Allows internal KoldStore flush cleanup.",
        c"Internal guard used while pruning flushed hot and mirror rows.",
        &INTERNAL_FLUSH_CLEANUP,
        GucContext::Suset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.internal_async_mirror_worker",
        c"Enables automatic async mirror worker registration.",
        c"Internal benchmark control. Keep enabled in production so async mirrors apply committed WAL automatically.",
        &INTERNAL_ASYNC_MIRROR_WORKER,
        GucContext::Suset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.min_max_rows_per_file",
        c"Minimum allowed max_rows_per_file for managed tables.",
        c"Rejects manage_table (and ALTER) settings below this floor. Already-persisted catalog policies are trusted at flush time so queue executors need not inherit the managing session's SET. Lower temporarily for tests with SET / ALTER DATABASE koldstore.min_max_rows_per_file = <value>.",
        &MIN_MAX_ROWS_PER_FILE,
        settings::MIN_MIN_MAX_ROWS_PER_FILE,
        settings::MAX_MIN_MAX_ROWS_PER_FILE,
        GucContext::Userset,
        flags,
    );
    // Test-only: empty default keeps production paths inert unless explicitly armed.
    // wait/panic/sleep require the test-failpoints build so production cannot park.
    GucRegistry::define_string_guc(
        c"koldstore.failpoint",
        c"Test-only KoldStore flush failpoint.",
        c"Arms a named flush failpoint (error:<name>, wait:<name>, panic:<name>, or sleep:<name>). Empty disables. wait/panic/sleep require a test-failpoints build. For crash-recovery and isolation suites only.",
        &FAILPOINT,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.pending_segment_ttl_seconds",
        c"TTL for pending cold segments before recovery expiry.",
        c"recover_segments quarantines object-store blobs and deletes catalog rows for pending segments older than this many seconds.",
        &PENDING_SEGMENT_TTL_SECONDS,
        settings::MIN_PENDING_SEGMENT_TTL_SECONDS,
        settings::MAX_PENDING_SEGMENT_TTL_SECONDS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.flush_check_interval_seconds",
        c"Interval between built-in auto-flush eligibility checks.",
        c"Database worker wakes on this cadence to evaluate auto_flush managed tables, enqueue flush jobs when needed, and spawn flush executors. SET / ALTER SYSTEM + reload; workers pick up changes on SIGHUP.",
        &FLUSH_CHECK_INTERVAL_SECONDS,
        settings::MIN_FLUSH_CHECK_INTERVAL_SECONDS,
        settings::MAX_FLUSH_CHECK_INTERVAL_SECONDS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.max_parallel_flush_jobs",
        c"Maximum concurrent one-shot flush executor workers per database.",
        c"Caps how many koldstore flush executor background workers may run at once. Default stays at 2 until broader failure-sweep coverage lands. Clamped to 1..=16.",
        &MAX_PARALLEL_FLUSH_JOBS,
        settings::MIN_MAX_PARALLEL_FLUSH_JOBS,
        settings::MAX_MAX_PARALLEL_FLUSH_JOBS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.flush_job_max_runtime_seconds",
        c"Wall-clock budget for one flush job attempt.",
        c"Flush aborts with an error when a single attempt exceeds this many seconds (checked between passes and between streamed batches within a pass). 0 disables. Default 1800 (30 minutes). Clamped to 0..=86400.",
        &FLUSH_JOB_MAX_RUNTIME_SECONDS,
        settings::MIN_FLUSH_JOB_MAX_RUNTIME_SECONDS,
        settings::MAX_FLUSH_JOB_MAX_RUNTIME_SECONDS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.job_retention_days",
        c"Days to retain terminal KoldStore jobs before purge.",
        c"Coordinator ticks delete completed/cancelled/error jobs whose finished_at is older than this many days. 0 disables purge. Jobs still referenced by pending cold segments are never deleted. Clamped to 0..=3650.",
        &JOB_RETENTION_DAYS,
        settings::MIN_JOB_RETENTION_DAYS,
        settings::MAX_JOB_RETENTION_DAYS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_string_guc(
        c"koldstore.flush_execution",
        c"How flush_table runs after enqueueing a durable job.",
        c"queue (default): enqueue UUID and spawn a one-shot flush executor. inline: enqueue then run flush in the calling backend (required for pg_test SPI transactions).",
        &FLUSH_EXECUTION,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.async_apply_watchdog_interval_ms",
        c"Idle wake interval for the persistent WAL applier; also its missed-wake safety net.",
        c"Managed commits normally wake the database worker immediately; this interval only matters while it is otherwise idle. Two roles: (1) recovers a missed in-memory notification (COMMIT PREPARED, a lost wake), and (2) bounds how far this database's slot can fall behind unrelated WAL from other databases on the same PostgreSQL instance (logical decoding reads and filters the whole shared WAL stream, so an idle koldstore database still pays to skip past it eventually) by running a normal drain pass on every idle wake too, not just when this database has its own work due -- a no-op pass is cheap, so this only ever pays for a real backlog, in bounded per-wake increments instead of one unbounded catch-up on this database's own next commit. Lower for a tighter backlog bound on a busy shared instance; raise to reduce idle wakeups. SET / ALTER SYSTEM + reload; the worker picks up changes on SIGHUP. Clamped to 1000..=300000 milliseconds.",
        &ASYNC_APPLY_WATCHDOG_INTERVAL_MS,
        settings::MIN_ASYNC_APPLY_WATCHDOG_INTERVAL_MS,
        settings::MAX_ASYNC_APPLY_WATCHDOG_INTERVAL_MS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.async_apply_max_rows_per_tick",
        c"Maximum source row changes applied in one async mirror tick.",
        c"Bounds work per apply transaction. 0 drains all peekable WAL in the tick (legacy). Explicit fences (wait_for_async_mirror / flush) may loop with a higher effective budget.",
        &ASYNC_APPLY_MAX_ROWS_PER_TICK,
        settings::MIN_ASYNC_APPLY_MAX_ROWS_PER_TICK,
        settings::MAX_ASYNC_APPLY_MAX_ROWS_PER_TICK,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.async_apply_max_ms_per_tick",
        c"Maximum wall-clock milliseconds for one async mirror tick.",
        c"Bounds apply transaction duration. 0 disables the time budget. Commit applied_lsn and continue on the next wake when the budget is exhausted.",
        &ASYNC_APPLY_MAX_MS_PER_TICK,
        settings::MIN_ASYNC_APPLY_MAX_MS_PER_TICK,
        settings::MAX_ASYNC_APPLY_MAX_MS_PER_TICK,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.flush_prelock_max_passes",
        c"Maximum phase-5.5 pre-lock async apply passes during flush.",
        c"Finite catch-up after Parquet upload and before SHARE ROW EXCLUSIVE. Fail closed when the budget is exhausted rather than holding writers indefinitely.",
        &FLUSH_PRELOCK_MAX_PASSES,
        settings::MIN_FLUSH_PRELOCK_MAX_PASSES,
        settings::MAX_FLUSH_PRELOCK_MAX_PASSES,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.flush_prelock_max_ms",
        c"Maximum wall-clock milliseconds for flush phase-5.5 pre-lock catch-up.",
        c"Combined budget across pre-lock passes. Exceeding this fails the flush before taking the source relation lock.",
        &FLUSH_PRELOCK_MAX_MS,
        settings::MIN_FLUSH_PRELOCK_MAX_MS,
        settings::MAX_FLUSH_PRELOCK_MAX_MS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.async_mirror_max_retained_bytes",
        c"Unhealthy threshold for async mirror retained WAL bytes (default 1 GiB).",
        c"When > 0 and pg_wal_lsn_diff(current, confirmed_flush_lsn) exceeds this, async_mirror_status reports unhealthy. Apply always remains enabled so it can drain the slot. Default 1073741824 (1 GiB). 0 disables this health threshold. Never silently drops WAL.",
        &ASYNC_MIRROR_MAX_RETAINED_BYTES,
        settings::MIN_ASYNC_MIRROR_MAX_RETAINED_BYTES,
        settings::MAX_ASYNC_MIRROR_MAX_RETAINED_BYTES,
        GucContext::Userset,
        flags,
    );
}

/// No-op placeholder for non-PostgreSQL tests.
#[cfg(not(feature = "pg"))]
pub fn define_gucs() {}

/// Static description of a pg-koldstore GUC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GucDefinition {
    /// GUC name.
    pub name: &'static str,
    /// Whether normal application roles are forbidden from setting it.
    pub internal: bool,
    /// Default value.
    pub default_value: &'static str,
}

/// Returns all GUC definitions.
#[must_use]
pub const fn definitions() -> &'static [GucDefinition] {
    &[
        GucDefinition {
            name: USER_ID_GUC,
            internal: false,
            default_value: "",
        },
        GucDefinition {
            name: ENABLE_MERGE_SCAN_GUC,
            internal: false,
            default_value: "on",
        },
        GucDefinition {
            name: ALLOW_SAME_TXN_COLD_READS_GUC,
            internal: false,
            default_value: "off",
        },
        GucDefinition {
            name: GUARD_SCAN_WRITES_GUC,
            internal: false,
            default_value: "on",
        },
        GucDefinition {
            name: HYDRATE_ON_WRITE_GUC,
            internal: false,
            default_value: "off",
        },
        GucDefinition {
            name: HYDRATE_FENCE_MIRROR_GUC,
            internal: false,
            default_value: "on",
        },
        GucDefinition {
            name: HYDRATE_TAKE_JOB_LOCK_GUC,
            internal: false,
            default_value: "off",
        },
        GucDefinition {
            name: HYDRATING_GUC,
            internal: false,
            default_value: "off",
        },
        GucDefinition {
            name: CAPTURE_REPLICATED_CHANGES_GUC,
            internal: false,
            default_value: "off",
        },
        GucDefinition {
            name: SPOCK_RECONCILE_INTERVAL_GUC,
            internal: false,
            default_value: "0",
        },
        GucDefinition {
            name: SPOCK_RECONCILE_DATABASES_GUC,
            internal: false,
            default_value: "",
        },
        GucDefinition {
            name: HYDRATE_SLOT_LOCK_POLL_MS_GUC,
            internal: false,
            default_value: "0",
        },
        GucDefinition {
            name: MAX_HYDRATE_ROWS_GUC,
            internal: false,
            default_value: "10000",
        },
        GucDefinition {
            name: REJECT_SERIALIZABLE_COLD_READS_GUC,
            internal: false,
            default_value: "on",
        },
        GucDefinition {
            name: settings::COLD_READS_GUC,
            internal: false,
            default_value: settings::DEFAULT_COLD_READS,
        },
        GucDefinition {
            name: settings::MAX_OPEN_PARQUET_READERS_GUC,
            internal: false,
            default_value: "32",
        },
        GucDefinition {
            name: settings::MAX_MERGE_SEEN_KEYS_GUC,
            internal: false,
            default_value: "1000000",
        },
        GucDefinition {
            name: settings::OBJECT_STORE_TIMEOUT_MS_GUC,
            internal: false,
            default_value: "30000",
        },
        GucDefinition {
            name: settings::LOG_LEVEL_GUC,
            internal: false,
            default_value: settings::DEFAULT_LOG_LEVEL,
        },
        GucDefinition {
            name: settings::MIN_MAX_ROWS_PER_FILE_GUC,
            internal: false,
            default_value: "1000",
        },
        GucDefinition {
            name: INTERNAL_SYSTEM_WRITE_GUC,
            internal: true,
            default_value: "off",
        },
        GucDefinition {
            name: INTERNAL_FLUSH_CLEANUP_GUC,
            internal: true,
            default_value: "off",
        },
        GucDefinition {
            name: INTERNAL_ASYNC_MIRROR_WORKER_GUC,
            internal: true,
            default_value: "on",
        },
        GucDefinition {
            name: settings::FAILPOINT_GUC,
            internal: false,
            default_value: settings::DEFAULT_FAILPOINT,
        },
        GucDefinition {
            name: settings::PENDING_SEGMENT_TTL_SECONDS_GUC,
            internal: false,
            default_value: "3600",
        },
        GucDefinition {
            name: settings::FLUSH_CHECK_INTERVAL_SECONDS_GUC,
            internal: false,
            default_value: "30",
        },
        GucDefinition {
            name: settings::MAX_PARALLEL_FLUSH_JOBS_GUC,
            internal: false,
            default_value: "2",
        },
        GucDefinition {
            name: settings::FLUSH_JOB_MAX_RUNTIME_SECONDS_GUC,
            internal: false,
            default_value: "1800",
        },
        GucDefinition {
            name: settings::JOB_RETENTION_DAYS_GUC,
            internal: false,
            default_value: "30",
        },
        GucDefinition {
            name: settings::FLUSH_EXECUTION_GUC,
            internal: false,
            default_value: settings::DEFAULT_FLUSH_EXECUTION,
        },
        GucDefinition {
            name: settings::ASYNC_APPLY_WATCHDOG_INTERVAL_MS_GUC,
            internal: false,
            default_value: "30000",
        },
        GucDefinition {
            name: settings::ASYNC_APPLY_MAX_ROWS_PER_TICK_GUC,
            internal: false,
            default_value: "0",
        },
        GucDefinition {
            name: settings::ASYNC_APPLY_MAX_MS_PER_TICK_GUC,
            internal: false,
            default_value: "0",
        },
        GucDefinition {
            name: settings::FLUSH_PRELOCK_MAX_PASSES_GUC,
            internal: false,
            default_value: "3",
        },
        GucDefinition {
            name: settings::FLUSH_PRELOCK_MAX_MS_GUC,
            internal: false,
            default_value: "5000",
        },
        GucDefinition {
            name: settings::ASYNC_MIRROR_MAX_RETAINED_BYTES_GUC,
            internal: false,
            default_value: "1073741824",
        },
    ]
}

/// Names of GUCs owned by pg-koldstore.
pub const USER_ID_GUC: &str = "koldstore.user_id";
pub const ENABLE_MERGE_SCAN_GUC: &str = "koldstore.enable_merge_scan";
pub const ALLOW_SAME_TXN_COLD_READS_GUC: &str = "koldstore.allow_same_txn_cold_reads";
pub const GUARD_SCAN_WRITES_GUC: &str = "koldstore.guard_scan_writes";
pub const HYDRATE_ON_WRITE_GUC: &str = "koldstore.hydrate_on_write";
pub const MAX_HYDRATE_ROWS_GUC: &str = "koldstore.max_hydrate_rows";
pub const HYDRATE_SLOT_LOCK_POLL_MS_GUC: &str = "koldstore.hydrate_slot_lock_poll_ms";
pub const SPOCK_RECONCILE_INTERVAL_GUC: &str = "koldstore.spock_reconcile_interval_seconds";
pub const SPOCK_RECONCILE_DATABASES_GUC: &str = "koldstore.spock_reconcile_databases";
pub const CAPTURE_REPLICATED_CHANGES_GUC: &str = "koldstore.capture_replicated_changes";
pub const HYDRATING_GUC: &str = "koldstore.hydrating";
pub const HYDRATE_TAKE_JOB_LOCK_GUC: &str = "koldstore.hydrate_take_job_lock";
pub const HYDRATE_FENCE_MIRROR_GUC: &str = "koldstore.hydrate_fence_mirror";
pub const REJECT_SERIALIZABLE_COLD_READS_GUC: &str = "koldstore.reject_serializable_cold_reads";
pub const INTERNAL_SYSTEM_WRITE_GUC: &str = "koldstore.internal_system_write";
pub const INTERNAL_FLUSH_CLEANUP_GUC: &str = "koldstore.internal_flush_cleanup";
pub const INTERNAL_ASYNC_MIRROR_WORKER_GUC: &str = "koldstore.internal_async_mirror_worker";

/// Active `koldstore.user_id` when set to a non-empty value.
#[must_use]
pub fn user_id() -> Option<String> {
    #[cfg(feature = "pg")]
    {
        let from_setting = USER_ID.get().and_then(|value| {
            value
                .to_str()
                .ok()
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .map(str::to_string)
        });
        from_setting.or_else(read_user_id_config_option)
    }

    #[cfg(not(feature = "pg"))]
    {
        None
    }
}

/// Fallback for placeholder GUCs set before the extension registered the setting.
#[cfg(feature = "pg")]
fn read_user_id_config_option() -> Option<String> {
    let setting = unsafe {
        let name = c"koldstore.user_id";
        let value = pgrx::pg_sys::GetConfigOption(name.as_ptr(), true, false);
        if value.is_null() {
            return None;
        }
        std::ffi::CStr::from_ptr(value)
            .to_string_lossy()
            .into_owned()
    };
    let trimmed = setting.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// Whether UPDATE/DELETE hydrate the cold-only rows they match (experimental).
#[must_use]
pub fn hydrate_on_write() -> bool {
    #[cfg(feature = "pg")]
    {
        HYDRATE_ON_WRITE.get()
    }

    #[cfg(not(feature = "pg"))]
    {
        false
    }
}

/// Whether the async mirror also captures changes that carry a replication origin.
#[must_use]
pub fn capture_replicated_changes() -> bool {
    #[cfg(feature = "pg")]
    {
        CAPTURE_REPLICATED_CHANGES.get()
    }

    #[cfg(not(feature = "pg"))]
    {
        false
    }
}

/// Seconds between background Spock-conflict reconciliations (0 = off).
#[must_use]
pub fn spock_reconcile_interval_seconds() -> i32 {
    #[cfg(feature = "pg")]
    {
        SPOCK_RECONCILE_INTERVAL.get()
    }

    #[cfg(not(feature = "pg"))]
    {
        0
    }
}

/// Databases that get a Spock-conflict reconciler worker.
#[must_use]
pub fn spock_reconcile_databases() -> Vec<String> {
    #[cfg(feature = "pg")]
    {
        SPOCK_RECONCILE_DATABASES
            .get()
            .and_then(|value| value.to_str().ok().map(str::to_string))
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .collect()
    }

    #[cfg(not(feature = "pg"))]
    {
        Vec::new()
    }
}

/// Whether hydrate-on-write fences on the async mirror before locating cold rows.
#[must_use]
pub fn hydrate_fence_mirror() -> bool {
    #[cfg(feature = "pg")]
    {
        HYDRATE_FENCE_MIRROR.get()
    }

    #[cfg(not(feature = "pg"))]
    {
        true
    }
}

/// Whether hydrate-on-write holds the table job lock for the statement.
#[must_use]
pub fn hydrate_take_job_lock() -> bool {
    #[cfg(feature = "pg")]
    {
        HYDRATE_TAKE_JOB_LOCK.get()
    }

    #[cfg(not(feature = "pg"))]
    {
        false
    }
}

/// Milliseconds a hydrate-on-write fence polls for the slot lock (0 = queue on it).
#[must_use]
pub fn hydrate_slot_lock_poll_ms() -> u64 {
    #[cfg(feature = "pg")]
    {
        u64::try_from(HYDRATE_SLOT_LOCK_POLL_MS.get()).unwrap_or(0)
    }

    #[cfg(not(feature = "pg"))]
    {
        0
    }
}

/// Most cold rows a single statement may hydrate.
#[must_use]
pub fn max_hydrate_rows() -> usize {
    #[cfg(feature = "pg")]
    {
        usize::try_from(MAX_HYDRATE_ROWS.get()).unwrap_or(10_000)
    }

    #[cfg(not(feature = "pg"))]
    {
        10_000
    }
}

/// Whether cold reads are refused under SERIALIZABLE isolation.
#[must_use]
pub fn reject_serializable_cold_reads() -> bool {
    #[cfg(feature = "pg")]
    {
        REJECT_SERIALIZABLE_COLD_READS.get()
    }

    #[cfg(not(feature = "pg"))]
    {
        false
    }
}

/// Whether UPDATE/DELETE statements are checked for cold-only matches of their
/// WHERE clause (upstream #122).
#[must_use]
pub fn guard_scan_writes() -> bool {
    #[cfg(feature = "pg")]
    {
        GUARD_SCAN_WRITES.get()
    }

    #[cfg(not(feature = "pg"))]
    {
        true
    }
}

/// Whether a read may consult cold data of a managed table this transaction
/// already modified (upstream #121 fail-closed check disabled).
#[must_use]
pub fn allow_same_txn_cold_reads() -> bool {
    #[cfg(feature = "pg")]
    {
        ALLOW_SAME_TXN_COLD_READS.get()
    }

    #[cfg(not(feature = "pg"))]
    {
        false
    }
}

/// Whether the planner may inject KoldMergeScan paths.
#[must_use]
pub fn enable_merge_scan() -> bool {
    #[cfg(feature = "pg")]
    {
        ENABLE_MERGE_SCAN.get()
    }

    #[cfg(not(feature = "pg"))]
    {
        true
    }
}

/// Whether async capture should register the bounded-lag database worker.
///
/// This is disabled only by deterministic benchmarks that account for each
/// explicit catch-up phase. Production sessions keep the default enabled.
#[must_use]
pub fn async_mirror_worker_enabled() -> bool {
    #[cfg(feature = "pg")]
    {
        INTERNAL_ASYNC_MIRROR_WORKER.get()
    }

    #[cfg(not(feature = "pg"))]
    {
        true
    }
}

/// Current cold-read mode.
#[must_use]
pub fn cold_reads_mode() -> settings::ColdReadsMode {
    #[cfg(feature = "pg")]
    {
        let value = COLD_READS
            .get()
            .and_then(|value| value.to_str().ok().map(str::to_string))
            .unwrap_or_else(|| settings::DEFAULT_COLD_READS.to_string());
        settings::ColdReadsMode::parse(&value).unwrap_or(settings::ColdReadsMode::Auto)
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::ColdReadsMode::Auto
    }
}

/// Current maximum open Parquet readers.
#[must_use]
pub fn max_open_parquet_readers() -> i32 {
    #[cfg(feature = "pg")]
    {
        settings::bounded_concurrency_limit(MAX_OPEN_PARQUET_READERS.get())
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::DEFAULT_MAX_OPEN_PARQUET_READERS
    }
}

/// Current per-scan merge seen-key cap (`0` = unlimited).
#[must_use]
pub fn max_merge_seen_keys() -> i32 {
    #[cfg(feature = "pg")]
    {
        settings::bounded_max_merge_seen_keys(MAX_MERGE_SEEN_KEYS.get())
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::DEFAULT_MAX_MERGE_SEEN_KEYS
    }
}

/// ObjectStore / Parquet operation timeout (`None` when disabled / `0`).
#[must_use]
pub fn object_store_timeout() -> Option<std::time::Duration> {
    let ms = object_store_timeout_ms();
    (ms > 0).then(|| std::time::Duration::from_millis(ms))
}

/// ObjectStore / Parquet operation timeout in milliseconds (`0` = disabled).
#[must_use]
pub fn object_store_timeout_ms() -> u64 {
    #[cfg(feature = "pg")]
    {
        u64::try_from(settings::bounded_object_store_timeout_ms(
            OBJECT_STORE_TIMEOUT_MS.get(),
        ))
        .unwrap_or(0)
    }

    #[cfg(not(feature = "pg"))]
    {
        u64::try_from(settings::DEFAULT_OBJECT_STORE_TIMEOUT_MS).unwrap_or(30_000)
    }
}

/// Current minimum allowed `max_rows_per_file` for managed tables.
#[must_use]
pub fn min_max_rows_per_file() -> i32 {
    #[cfg(feature = "pg")]
    {
        settings::bounded_min_max_rows_per_file(MIN_MAX_ROWS_PER_FILE.get())
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::default_min_max_rows_per_file()
    }
}

/// Current test-only failpoint arming value, owned but unparsed.
///
/// Returns the raw `CString` rather than an allocated `String` so the hot
/// `failpoints::hit` call sites (invoked at every flush phase and mirror
/// apply tick) can borrow the text with zero extra allocations instead of
/// paying for a fresh UTF-8 copy on every disarmed check.
#[must_use]
pub fn failpoint_value() -> Option<std::ffi::CString> {
    #[cfg(feature = "pg")]
    {
        FAILPOINT.get()
    }

    #[cfg(not(feature = "pg"))]
    {
        None
    }
}

/// TTL in seconds for pending cold segments before recover_segments expires them.
#[must_use]
pub fn pending_segment_ttl_seconds() -> i64 {
    #[cfg(feature = "pg")]
    {
        i64::from(
            PENDING_SEGMENT_TTL_SECONDS
                .get()
                .max(settings::MIN_PENDING_SEGMENT_TTL_SECONDS),
        )
    }

    #[cfg(not(feature = "pg"))]
    {
        i64::from(settings::DEFAULT_PENDING_SEGMENT_TTL_SECONDS)
    }
}

/// Seconds between built-in auto-flush eligibility checks in the database worker.
#[must_use]
pub fn flush_check_interval_seconds() -> i64 {
    #[cfg(feature = "pg")]
    {
        let value = FLUSH_CHECK_INTERVAL_SECONDS.get();
        i64::from(value.clamp(
            settings::MIN_FLUSH_CHECK_INTERVAL_SECONDS,
            settings::MAX_FLUSH_CHECK_INTERVAL_SECONDS,
        ))
    }

    #[cfg(not(feature = "pg"))]
    {
        i64::from(settings::DEFAULT_FLUSH_CHECK_INTERVAL_SECONDS)
    }
}

/// Maximum concurrent one-shot flush executor workers for this database.
#[must_use]
pub fn max_parallel_flush_jobs() -> i32 {
    #[cfg(feature = "pg")]
    {
        settings::bounded_max_parallel_flush_jobs(MAX_PARALLEL_FLUSH_JOBS.get())
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::DEFAULT_MAX_PARALLEL_FLUSH_JOBS
    }
}

/// Wall-clock budget for one flush job attempt (`0` = disabled).
#[must_use]
pub fn flush_job_max_runtime_seconds() -> i32 {
    #[cfg(feature = "pg")]
    {
        settings::bounded_flush_job_max_runtime_seconds(FLUSH_JOB_MAX_RUNTIME_SECONDS.get())
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::DEFAULT_FLUSH_JOB_MAX_RUNTIME_SECONDS
    }
}

/// Days to retain terminal jobs before coordinator purge (`0` = disabled).
#[must_use]
pub fn job_retention_days() -> i32 {
    #[cfg(feature = "pg")]
    {
        settings::bounded_job_retention_days(JOB_RETENTION_DAYS.get())
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::DEFAULT_JOB_RETENTION_DAYS
    }
}

/// Whether `flush_table` should run inline or enqueue for background executors.
#[must_use]
pub fn flush_execution_mode() -> settings::FlushExecutionMode {
    #[cfg(feature = "pg")]
    {
        let value = FLUSH_EXECUTION
            .get()
            .and_then(|value| value.to_str().ok().map(str::to_string))
            .unwrap_or_else(|| settings::DEFAULT_FLUSH_EXECUTION.to_string());
        settings::FlushExecutionMode::parse(&value).unwrap_or(settings::FlushExecutionMode::Queue)
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::FlushExecutionMode::Queue
    }
}

/// Idle wake interval for the persistent WAL applier (missed-wake safety net and
/// idle-backlog-bounding nudge interval; see the GUC's own registration doc).
#[must_use]
pub fn async_apply_watchdog_interval_ms() -> u64 {
    #[cfg(feature = "pg")]
    {
        let value = ASYNC_APPLY_WATCHDOG_INTERVAL_MS.get();
        u64::try_from(value.clamp(
            settings::MIN_ASYNC_APPLY_WATCHDOG_INTERVAL_MS,
            settings::MAX_ASYNC_APPLY_WATCHDOG_INTERVAL_MS,
        ))
        .unwrap_or(u64::from(
            settings::DEFAULT_ASYNC_APPLY_WATCHDOG_INTERVAL_MS as u32,
        ))
    }

    #[cfg(not(feature = "pg"))]
    {
        u64::try_from(settings::DEFAULT_ASYNC_APPLY_WATCHDOG_INTERVAL_MS).unwrap_or(30_000)
    }
}

/// Maximum source row changes applied in one background apply tick (`0` = unlimited).
#[must_use]
pub fn async_apply_max_rows_per_tick() -> i64 {
    #[cfg(feature = "pg")]
    {
        i64::from(ASYNC_APPLY_MAX_ROWS_PER_TICK.get().clamp(
            settings::MIN_ASYNC_APPLY_MAX_ROWS_PER_TICK,
            settings::MAX_ASYNC_APPLY_MAX_ROWS_PER_TICK,
        ))
    }

    #[cfg(not(feature = "pg"))]
    {
        i64::from(settings::DEFAULT_ASYNC_APPLY_MAX_ROWS_PER_TICK)
    }
}

/// Maximum wall-clock milliseconds for one background apply tick (`0` = unlimited).
#[must_use]
pub fn async_apply_max_ms_per_tick() -> i64 {
    #[cfg(feature = "pg")]
    {
        i64::from(ASYNC_APPLY_MAX_MS_PER_TICK.get().clamp(
            settings::MIN_ASYNC_APPLY_MAX_MS_PER_TICK,
            settings::MAX_ASYNC_APPLY_MAX_MS_PER_TICK,
        ))
    }

    #[cfg(not(feature = "pg"))]
    {
        i64::from(settings::DEFAULT_ASYNC_APPLY_MAX_MS_PER_TICK)
    }
}

/// Maximum phase-5.5 pre-lock apply passes during flush.
#[must_use]
pub fn flush_prelock_max_passes() -> i32 {
    #[cfg(feature = "pg")]
    {
        FLUSH_PRELOCK_MAX_PASSES.get().clamp(
            settings::MIN_FLUSH_PRELOCK_MAX_PASSES,
            settings::MAX_FLUSH_PRELOCK_MAX_PASSES,
        )
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::DEFAULT_FLUSH_PRELOCK_MAX_PASSES
    }
}

/// Combined wall-clock budget (ms) for flush phase-5.5 pre-lock catch-up.
#[must_use]
pub fn flush_prelock_max_ms() -> i64 {
    #[cfg(feature = "pg")]
    {
        i64::from(FLUSH_PRELOCK_MAX_MS.get().clamp(
            settings::MIN_FLUSH_PRELOCK_MAX_MS,
            settings::MAX_FLUSH_PRELOCK_MAX_MS,
        ))
    }

    #[cfg(not(feature = "pg"))]
    {
        i64::from(settings::DEFAULT_FLUSH_PRELOCK_MAX_MS)
    }
}

/// Retained-WAL unhealthy threshold in bytes (`0` = disabled).
#[must_use]
pub fn async_mirror_max_retained_bytes() -> i64 {
    #[cfg(feature = "pg")]
    {
        i64::from(ASYNC_MIRROR_MAX_RETAINED_BYTES.get().clamp(
            settings::MIN_ASYNC_MIRROR_MAX_RETAINED_BYTES,
            settings::MAX_ASYNC_MIRROR_MAX_RETAINED_BYTES,
        ))
    }

    #[cfg(not(feature = "pg"))]
    {
        i64::from(settings::DEFAULT_ASYNC_MIRROR_MAX_RETAINED_BYTES)
    }
}
