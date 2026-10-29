//! Leader lease with epoch fencing for a tiering leader that shares cold storage with followers
//! (docs/plans/multi-master-single-tiering-leader.md, stage 4).
//!
//! Storage offers create-if-absent and overwrite but no compare-and-swap, so the lease is a series
//! of epoch objects, `_leader_lease/<schema>/<table>/<epoch>.json`. Claiming epoch N+1 is a
//! create-if-absent put, so exactly one claimer wins. The holder renews by overwriting its own
//! epoch object with a fresh heartbeat; the highest epoch is the current lease. A node enforces the
//! lease only after it has acquired one (`options.lease_epoch`): flush, compaction, reaping and
//! catalog publishing then require that the highest epoch is still its own and unexpired, which
//! fences a deposed leader. Expiry compares the holder's heartbeat clock with the reader's, so keep
//! the ttl well above clock skew.

use pgrx::datum::DatumWithOid;
use serde_json::{Value, json};

use super::backup::authorize;
use koldstore_storage::{PutPrecondition, StorageClient};

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
}

pub(crate) fn node_id() -> Result<String, String> {
    pgrx::Spi::get_one::<String>("SELECT (SELECT system_identifier::text FROM pg_control_system())")
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "cannot read the cluster identifier".to_string())
}

fn lease_prefix(namespace: &str, name: &str) -> String {
    format!("_leader_lease/{namespace}/{name}/")
}

fn epoch_key(prefix: &str, epoch: i64) -> String {
    format!("{prefix}{epoch:020}.json")
}

struct Lease {
    epoch: i64,
    holder: String,
    heartbeat_ms: i64,
    ttl_s: i64,
}

impl Lease {
    fn expired(&self, now: i64) -> bool {
        self.heartbeat_ms == 0 || now.saturating_sub(self.heartbeat_ms) > self.ttl_s.saturating_mul(1000)
    }

    fn to_json(&self, now: i64, me: &str) -> Value {
        json!({
            "epoch": self.epoch, "holder": self.holder, "mine": self.holder == me,
            "heartbeat_age_ms": now.saturating_sub(self.heartbeat_ms), "ttl_seconds": self.ttl_s,
            "expired": self.expired(now),
        })
    }
}

struct Target {
    client: koldstore_storage::ObjectStoreClient,
    prefix: String,
}

fn target(table_oid: pgrx::pg_sys::Oid) -> Result<Target, String> {
    let t = super::backup::table_target(table_oid)?;
    Ok(Target {
        client: super::backup::storage_client(&t.storage.storage_id)?,
        prefix: lease_prefix(&t.namespace, &t.name),
    })
}

/// The highest-epoch lease object, if any.
fn current(target: &Target) -> Result<Option<Lease>, String> {
    let objects = target.client.list(&target.prefix).map_err(|error| error.to_string())?;
    let Some(key) = objects
        .iter()
        .filter_map(|object| {
            let epoch = object.key.rsplit('/').next()?.strip_suffix(".json")?.parse::<i64>().ok()?;
            Some((epoch, object.key.clone()))
        })
        .max()
        .map(|(_, key)| key)
    else {
        return Ok(None);
    };
    let bytes = target.client.get(&key).map_err(|error| error.to_string())?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|error| format!("lease object is not valid: {error}"))?;
    Ok(Some(Lease {
        epoch: value["epoch"].as_i64().ok_or("lease object has no epoch")?,
        holder: value["holder"].as_str().unwrap_or_default().to_string(),
        heartbeat_ms: value["heartbeat_ms"].as_i64().unwrap_or(0),
        ttl_s: value["ttl_s"].as_i64().unwrap_or(0),
    }))
}

fn write(target: &Target, lease: &Lease, mode: PutPrecondition) -> Result<(), String> {
    let body = serde_json::to_vec(&json!({
        "epoch": lease.epoch, "holder": lease.holder, "heartbeat_ms": lease.heartbeat_ms, "ttl_s": lease.ttl_s,
    }))
    .map_err(|error| error.to_string())?;
    target
        .client
        .put(&epoch_key(&target.prefix, lease.epoch), &body, mode)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn local_epoch(table_oid: pgrx::pg_sys::Oid) -> Result<Option<i64>, String> {
    pgrx::Spi::get_one_with_args::<i64>(
        "SELECT (SELECT (options->>'lease_epoch')::bigint FROM koldstore.schemas WHERE table_oid = $1 AND active)",
        &[DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())
}

fn set_local_epoch(table_oid: pgrx::pg_sys::Oid, epoch: Option<i64>) -> Result<(), String> {
    pgrx::Spi::run_with_args(
        "UPDATE koldstore.schemas SET options = CASE WHEN $2::bigint IS NULL THEN options - 'lease_epoch' \
             ELSE jsonb_set(coalesce(options, '{}'::jsonb), '{lease_epoch}', to_jsonb($2::bigint), true) END, \
             updated_at = now() WHERE table_oid = $1 AND active",
        &[DatumWithOid::from(table_oid), DatumWithOid::from(epoch)],
    )
    .map_err(|error| error.to_string())?;
    crate::catalog::cache::invalidate_table_globally(table_oid);
    Ok(())
}

/// Errors unless this node may tier `table_oid`. A node that never acquired a lease is not
/// restricted; once it has, the shared highest epoch must still be its own and unexpired.
pub(crate) fn require_lease(table_oid: pgrx::pg_sys::Oid, action: &str) -> Result<(), String> {
    let Some(epoch) = local_epoch(table_oid)? else {
        return Ok(());
    };
    let me = node_id()?;
    let lease = current(&target(table_oid)?)?;
    match lease {
        Some(lease) if lease.epoch == epoch && lease.holder == me && !lease.expired(now_ms()) => Ok(()),
        Some(lease) => Err(format!(
            "cannot {action}: this node no longer holds the leader lease (held epoch {epoch}; shared lease is epoch {} held by {}{})",
            lease.epoch,
            if lease.holder == me { "this node" } else { &lease.holder },
            if lease.expired(now_ms()) { ", expired" } else { "" },
        )),
        None => Err(format!("cannot {action}: the leader lease object is gone (held epoch {epoch})")),
    }
}

/// The lease epoch to stamp on published catalogs, if this node holds one.
pub(crate) fn published_epoch(table_oid: pgrx::pg_sys::Oid) -> Result<Option<i64>, String> {
    local_epoch(table_oid)
}

fn acquire_value(table_oid: pgrx::pg_sys::Oid, ttl_s: i32) -> Result<Value, String> {
    if ttl_s < 3 {
        return Err("ttl_seconds must be at least 3".into());
    }
    if super::table_metadata::is_follower(table_oid)? {
        return Err("the table is a follower; turn follower mode off first".into());
    }
    let me = node_id()?;
    let target = target(table_oid)?;
    let now = now_ms();
    let existing = current(&target)?;
    let next = match existing {
        Some(lease) if lease.holder == me && !lease.expired(now) => {
            // Renewal of the lease this node already holds.
            let renewed = Lease { heartbeat_ms: now, ttl_s: i64::from(ttl_s), ..lease };
            write(&target, &renewed, PutPrecondition::Overwrite)?;
            set_local_epoch(table_oid, Some(renewed.epoch))?;
            return Ok(renewed.to_json(now, &me));
        }
        Some(lease) if !lease.expired(now) => {
            return Err(format!(
                "the leader lease (epoch {}) is held by {} for another {} ms",
                lease.epoch,
                lease.holder,
                lease.ttl_s * 1000 - (now - lease.heartbeat_ms)
            ));
        }
        Some(lease) => lease.epoch + 1,
        None => 1,
    };
    let claimed = Lease { epoch: next, holder: me.clone(), heartbeat_ms: now, ttl_s: i64::from(ttl_s) };
    write(&target, &claimed, PutPrecondition::CreateIfAbsent)
        .map_err(|error| format!("lost the race for lease epoch {next}: {error}"))?;
    set_local_epoch(table_oid, Some(next))?;
    Ok(claimed.to_json(now, &me))
}

fn renew_value(table_oid: pgrx::pg_sys::Oid) -> Result<Value, String> {
    let epoch = local_epoch(table_oid)?.ok_or("this node holds no leader lease (acquire_leader_lease)")?;
    let me = node_id()?;
    let target = target(table_oid)?;
    let now = now_ms();
    let lease = current(&target)?.ok_or("the leader lease object is gone")?;
    if lease.epoch != epoch || lease.holder != me {
        return Err(format!(
            "leader lease lost: shared lease is epoch {} held by {} (this node held epoch {epoch})",
            lease.epoch, lease.holder
        ));
    }
    if lease.expired(now) {
        return Err(format!(
            "leader lease epoch {epoch} expired before it was renewed; acquire_leader_lease() to claim a new epoch"
        ));
    }
    let renewed = Lease { heartbeat_ms: now, ..lease };
    write(&target, &renewed, PutPrecondition::Overwrite)?;
    // A challenger may have claimed the next epoch while this write was in flight.
    match current(&target)? {
        Some(after) if after.epoch == epoch => Ok(renewed.to_json(now, &me)),
        Some(after) => Err(format!("leader lease lost during renewal to epoch {}", after.epoch)),
        None => Err("the leader lease object is gone".into()),
    }
}

/// Claims the table's leader lease, or renews it when this node already holds it.
///
/// SQL contract: `koldstore.acquire_leader_lease(table_name regclass, ttl_seconds integer default 30)
/// → jsonb` (`{epoch, holder, mine, heartbeat_age_ms, ttl_seconds, expired}`).
///
/// The lease lives on the table's shared storage. It succeeds when no lease exists, the current one
/// is expired or released, or this node holds it; otherwise it errors. Once acquired, flush,
/// compaction, reaping and catalog publishing on this node require the lease to stay valid, so a
/// deposed leader is fenced. Keep it alive with `renew_leader_lease()` (the catalog sync worker
/// does this on every tick for leader tables, so its interval must be well below the ttl).
#[pgrx::pg_extern(name = "acquire_leader_lease", schema = "koldstore", security_definer)]
pub fn acquire_leader_lease_pg(table_name: pgrx::PgRelation, ttl_seconds: pgrx::default!(i32, 30)) -> pgrx::JsonB {
    let oid = table_name.oid();
    authorize(Some(oid), "acquire the leader lease");
    acquire_value(oid, ttl_seconds)
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("acquire leader lease failed: {error}"))
}

/// Extends the lease this node holds.
///
/// SQL contract: `koldstore.renew_leader_lease(table_name regclass) → jsonb`, same shape as
/// `acquire_leader_lease`. Errors when the lease was lost to another node or expired.
#[pgrx::pg_extern(name = "renew_leader_lease", schema = "koldstore", security_definer)]
pub fn renew_leader_lease_pg(table_name: pgrx::PgRelation) -> pgrx::JsonB {
    let oid = table_name.oid();
    authorize(Some(oid), "renew the leader lease");
    renew_value(oid)
        .map(pgrx::JsonB)
        .unwrap_or_else(|error| pgrx::error!("renew leader lease failed: {error}"))
}

/// Gives up the lease so another node can acquire it immediately.
///
/// SQL contract: `koldstore.release_leader_lease(table_name regclass) → boolean` (true when this
/// node held the lease). The node stops enforcing a lease afterwards.
#[pgrx::pg_extern(name = "release_leader_lease", schema = "koldstore", security_definer)]
pub fn release_leader_lease_pg(table_name: pgrx::PgRelation) -> bool {
    let oid = table_name.oid();
    authorize(Some(oid), "release the leader lease");
    let run = || -> Result<bool, String> {
        let Some(epoch) = local_epoch(oid)? else {
            return Ok(false);
        };
        let me = node_id()?;
        let target = target(oid)?;
        let mut held = false;
        if let Some(lease) = current(&target)? {
            if lease.epoch == epoch && lease.holder == me {
                write(&target, &Lease { heartbeat_ms: 0, ..lease }, PutPrecondition::Overwrite)?;
                held = true;
            }
        }
        set_local_epoch(oid, None)?;
        Ok(held)
    };
    run().unwrap_or_else(|error| pgrx::error!("release leader lease failed: {error}"))
}

/// Shows the shared lease and whether this node enforces one.
///
/// SQL contract: `koldstore.leader_lease_status(table_name regclass) → jsonb`
/// (`{lease: {...} | null, enforced, local_epoch}`).
#[pgrx::pg_extern(name = "leader_lease_status", schema = "koldstore", security_definer)]
pub fn leader_lease_status_pg(table_name: pgrx::PgRelation) -> pgrx::JsonB {
    let oid = table_name.oid();
    authorize(Some(oid), "read the leader lease");
    let run = || -> Result<Value, String> {
        let me = node_id()?;
        let local = local_epoch(oid)?;
        let lease = current(&target(oid)?)?.map(|lease| lease.to_json(now_ms(), &me));
        Ok(json!({ "lease": lease, "enforced": local.is_some(), "local_epoch": local }))
    };
    pgrx::JsonB(run().unwrap_or_else(|error| pgrx::error!("leader lease status failed: {error}")))
}
