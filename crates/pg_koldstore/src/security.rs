//! Caller-authorization helpers for `SECURITY DEFINER` SQL entrypoints.
//!
//! `SECURITY DEFINER` functions run with the *function owner's* effective
//! privileges (`pg_sys::GetUserId()`), not the invoking role's -- an
//! authorization check written against `GetUserId()` from inside one of
//! these functions is a silent no-op, since it always reports the owner
//! regardless of who actually called the function. Confirmed empirically:
//! a session connected as an unprivileged role with zero grants on a table
//! (no SELECT, nothing) successfully called `manage_table`/`register_storage`/
//! `flush_table` and forced a real flush of that table's data to arbitrary
//! storage the caller itself registered -- none of the existing checks in
//! those functions ever consulted the real caller's identity.
//!
//! The real caller's identity survives the security-definer switch in
//! `GetOuterUserId()` -- verified live: a session as a low-privileged role
//! shows `GetUserId() == <function owner>` but `GetOuterUserId() == <the
//! real calling role>` from inside the same security_definer function body.
//! This mirrors the check `hooks/ddl.rs`'s `ALTER TABLE ... SET
//! (koldstore_enabled = ...)` path already does correctly (that path runs
//! as the ordinary invoking backend, no security-definer switch involved,
//! so `GetUserId()` is already correct there) -- these helpers give
//! `SECURITY DEFINER` SQL entrypoints the equivalent guarantee.

use pgrx::pg_sys;

/// Returns the role that actually issued the current SQL call, independent
/// of any `SECURITY DEFINER` switch already in effect inside this function.
fn calling_role() -> pg_sys::Oid {
    unsafe { pg_sys::GetOuterUserId() }
}

/// Errors unless the real caller owns `oid` or is a superuser.
///
/// `oid` may be any relation-like object `pg_class_ownercheck` accepts (a
/// plain heap table here in every call site). Intentionally takes a bare
/// OID rather than an already-opened `PgRelation` so call sites that must
/// defer opening the relation (see `RegClassOid`'s own doc comment) can
/// still authorize first.
pub(crate) fn require_relation_owner_or_superuser(oid: pg_sys::Oid, action: &str) {
    let caller = calling_role();
    let authorized = unsafe { pg_sys::superuser_arg(caller) || relation_ownercheck(oid, caller) };
    if !authorized {
        pgrx::error!("must be owner of relation or superuser to {action}");
    }
}

/// True when `role` owns relation `oid` (PG15 vs PG16+ ACL helper names
/// differ -- mirrors `hooks/ddl.rs`'s identically-named private helper for
/// its own, already-correct `ALTER TABLE` ownership check).
unsafe fn relation_ownercheck(oid: pg_sys::Oid, role: pg_sys::Oid) -> bool {
    #[cfg(feature = "pg15")]
    unsafe {
        pg_sys::pg_class_ownercheck(oid, role)
    }
    #[cfg(not(feature = "pg15"))]
    unsafe {
        pg_sys::object_ownercheck(pg_sys::RelationRelationId, oid, role)
    }
}

/// Errors unless the real caller is a superuser.
///
/// For entrypoints with no single relation to check ownership against --
/// registering/altering a cluster-wide storage backend can point anywhere
/// the PostgreSQL server process can reach (filesystem path, S3/GCS/Azure
/// credentials), the same class of action PostgreSQL itself reserves to
/// superuser (or an explicitly granted role) for e.g. `COPY TO/FROM` a
/// server-side file.
pub(crate) fn require_superuser(action: &str) {
    if !unsafe { pg_sys::superuser_arg(calling_role()) } {
        pgrx::error!("must be superuser to {action}");
    }
}
