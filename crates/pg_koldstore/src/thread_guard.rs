//! Keeps KoldStore's hooks and callbacks inert on threads other than the backend's own.
//!
//! Some extensions (pg_duckdb, pg_ducklake) run Postgres work on a helper thread while the
//! backend's thread waits. Postgres then calls KoldStore's globally registered hooks and
//! transaction callbacks on that helper thread. pgrx aborts the whole process when any
//! Postgres function is called from a second thread, and KoldStore's per-backend state is
//! thread-local, so nothing it did there could be consistent anyway. Every hook and callback
//! therefore checks `is_foreign()` first and, on a helper thread, only chains to the previous
//! hook without making a single pgrx-wrapped call. Work done on a helper thread is invisible
//! to KoldStore (no write tracking, no cold-DML guard, no `KoldMergeScan` injection).

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, ThreadId};

static BACKEND_THREAD: OnceLock<ThreadId> = OnceLock::new();
static INVALIDATE_ALL_DEFERRED: AtomicBool = AtomicBool::new(false);

/// Remembers the thread running `_PG_init` as the backend thread (fork keeps its id).
pub(crate) fn init() {
    let _ = BACKEND_THREAD.set(thread::current().id());
}

#[inline]
pub(crate) fn is_foreign() -> bool {
    BACKEND_THREAD.get().is_some_and(|id| *id != thread::current().id())
}

/// A relcache invalidation arrived on a helper thread, where the thread-local caches are
/// not the backend's. Applied by the backend thread at its next hook entry.
pub(crate) fn defer_invalidate_all() {
    INVALIDATE_ALL_DEFERRED.store(true, Ordering::Relaxed);
}

#[inline]
pub(crate) fn apply_deferred_invalidations() {
    if INVALIDATE_ALL_DEFERRED.load(Ordering::Relaxed) && INVALIDATE_ALL_DEFERRED.swap(false, Ordering::Relaxed) {
        crate::catalog::cache::invalidate_all();
    }
}

/// The `standard_*` hook fallbacks, declared directly: the pgrx-wrapped versions in `pg_sys`
/// run the thread check, which is exactly what a pass-through on a helper thread must avoid.
pub(crate) mod standard {
    use pgrx::pg_sys;
    use std::ffi::{c_char, c_int};

    unsafe extern "C-unwind" {
        pub(crate) fn standard_planner(
            parse: *mut pg_sys::Query,
            query_string: *const c_char,
            cursor_options: c_int,
            bound_params: pg_sys::ParamListInfo,
        ) -> *mut pg_sys::PlannedStmt;

        pub(crate) fn standard_ExecutorStart(query_desc: *mut pg_sys::QueryDesc, eflags: c_int);

        pub(crate) fn standard_ExecutorEnd(query_desc: *mut pg_sys::QueryDesc);

        #[allow(clippy::too_many_arguments)]
        pub(crate) fn standard_ProcessUtility(
            pstmt: *mut pg_sys::PlannedStmt,
            query_string: *const c_char,
            read_only_tree: bool,
            context: pg_sys::ProcessUtilityContext::Type,
            params: pg_sys::ParamListInfo,
            query_env: *mut pg_sys::QueryEnvironment,
            dest: *mut pg_sys::DestReceiver,
            qc: *mut pg_sys::QueryCompletion,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_threads_other_than_the_initializing_one_are_foreign() {
        init();
        assert!(!is_foreign());
        let foreign = std::thread::spawn(is_foreign).join().unwrap();
        assert!(foreign);
        assert!(!is_foreign());
    }
}
