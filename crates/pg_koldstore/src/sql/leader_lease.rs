//! Leader lease with epoch fencing for a tiering leader that shares cold storage with followers
//! (docs/plans/multi-master-single-tiering-leader.md, stage 4).
//!
//! Storage offers create-if-absent and overwrite but no compare-and-swap, so the lease is a series
//! of epoch objects, `_leader_lease/<schema>/<table>/<epoch>.json`. Claiming epoch N+1 is a
//! create-if-absent put, so exactly one claimer wins. The holder renews by overwriting its own
//! epoch object with a fresh heartbeat; the highest epoch is the current lease. A node enforces the
//! lease only after it has acquired one (`options.lease_epoch`): flush, compaction, reaping and
//! catalog publishing then require that the highest epoch is still its own and unexpired, which
//! fences a deposed leader.
//!
//! Clocks: the holder judges its own lease by its own clock (heartbeat and check are on one
//! machine). A node that wants to take a lease over never compares the holder's heartbeat with its
//! own wall clock; it watches the lease object and counts the time (monotonic clock) during which
//! the holder's heartbeat did not change, so a fast, slow or stepped clock on either side cannot
//! make a live lease look dead or a dead one look alive. Only clock *rate* matters. A node that
//! just started has seen nothing yet, so it needs one full ttl of observation before a takeover.

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

/// Start of the renewal error that says another node holds the lease now.
const LOST: &str = "leader lease lost";
/// Start of the renewal error for a lease that ran out before it was renewed.
const EXPIRED: &str = "leader lease epoch";

fn lease_prefix(namespace: &str, name: &str) -> String {
    format!("_leader_lease/{namespace}/{name}/")
}

fn epoch_key(prefix: &str, epoch: i64) -> String {
    format!("{prefix}{epoch:020}.json")
}

#[derive(Clone)]
struct Lease {
    epoch: i64,
    holder: String,
    heartbeat_ms: i64,
    ttl_s: i64,
    /// SHA-256 of the published leader catalog when this epoch was claimed: the last catalog the
    /// previous epoch is allowed to have written.
    sealed_sha: Option<String>,
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
    /// Identifies the lease for the observation table (storage and prefix).
    key: String,
}

/// Per process: the last (epoch, heartbeat) seen for each lease and when it was first seen.
static OBSERVED: std::sync::Mutex<Vec<(String, i64, i64, std::time::Instant)>> = std::sync::Mutex::new(Vec::new());

/// Milliseconds this process has seen `lease` unchanged (0 the first time it is seen or after it moved).
fn silent_ms(target: &Target, lease: &Lease) -> i64 {
    silent_for(&target.key, lease.epoch, lease.heartbeat_ms)
}

/// Milliseconds the observation `(epoch, heartbeat)` under `key` has stayed unchanged here.
fn silent_for(key: &str, epoch: i64, heartbeat_ms: i64) -> i64 {
    let mut seen = OBSERVED.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let now = std::time::Instant::now();
    match seen.iter_mut().find(|entry| entry.0 == key) {
        Some(entry) if entry.1 == epoch && entry.2 == heartbeat_ms => {
            i64::try_from(now.duration_since(entry.3).as_millis()).unwrap_or(i64::MAX)
        }
        Some(entry) => {
            *entry = (key.to_string(), epoch, heartbeat_ms, now);
            0
        }
        None => {
            seen.push((key.to_string(), epoch, heartbeat_ms, now));
            0
        }
    }
}

/// Per process: when each table's lease was first seen electable by this node.
static ELECTABLE: std::sync::Mutex<Vec<(String, std::time::Instant)>> = std::sync::Mutex::new(Vec::new());

fn candidate_prefix(namespace: &str, name: &str) -> String {
    format!("_leader_candidates/{namespace}/{name}/")
}

fn held_and_seen(table_oid: pgrx::pg_sys::Oid) -> Result<(i64, i64), String> {
    let (held, seen) = pgrx::Spi::get_two_with_args::<i64, i64>(
        "SELECT (SELECT coalesce(max(generation), 0)::bigint FROM koldstore.manifest WHERE table_oid = $1), \
                (SELECT coalesce((options->>'seen_leader_epoch')::bigint, 0) FROM koldstore.schemas \
                 WHERE table_oid = $1 AND active)",
        &[DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?;
    Ok((held.unwrap_or(0), seen.unwrap_or(0)))
}

/// A follower advertises itself as an election candidate: its priority and how much of the
/// leader's state it holds. Called by the catalog sync worker on every follower tick.
pub(crate) fn publish_candidacy(table_oid: pgrx::pg_sys::Oid) -> Result<(), String> {
    use koldstore_storage::StorageClient;

    let t = super::backup::table_target(table_oid)?;
    let client = super::backup::storage_client(&t.storage.storage_id)?;
    let (held, seen) = held_and_seen(table_oid)?;
    let node = node_id()?;
    let body = serde_json::to_vec(&json!({
        "node": node, "priority": crate::guc::election_priority(), "generation": held, "epoch": seen,
        "heartbeat_ms": now_ms(),
    }))
    .map_err(|error| error.to_string())?;
    client
        .put(&format!("{}{node}.json", candidate_prefix(&t.namespace, &t.name)), &body, PutPrecondition::Overwrite)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// This node's place in the election order (0 is the preferred candidate) among the live
/// followers that hold at least `(generation, epoch)`. A candidate is live while its heartbeat
/// keeps changing as seen on this node's own clock. `None` when this node opted out.
fn election_rank(t: &super::backup::TableTarget, client: &impl koldstore_storage::StorageClient, held: i64, generation: i64, epoch: i64, ttl_s: i64) -> Result<Option<usize>, String> {
    let me = node_id()?;
    let priority = i64::from(crate::guc::election_priority());
    if priority == 0 {
        return Ok(None);
    }
    let prefix = candidate_prefix(&t.namespace, &t.name);
    let mut field = vec![(priority, held, me.clone())];
    for object in client.list(&prefix).map_err(|error| error.to_string())? {
        let Ok(bytes) = client.get(&object.key) else { continue };
        let Ok(value) = serde_json::from_slice::<Value>(&bytes) else { continue };
        let node = value["node"].as_str().unwrap_or_default().to_string();
        if node.is_empty() || node == me {
            continue;
        }
        let key = format!("{}|{}", t.storage.storage_id, object.key);
        let silent = silent_for(&key, 0, value["heartbeat_ms"].as_i64().unwrap_or(0));
        let caught_up = value["generation"].as_i64().unwrap_or(0) >= generation && value["epoch"].as_i64().unwrap_or(0) >= epoch;
        let priority = value["priority"].as_i64().unwrap_or(0);
        if caught_up && priority > 0 && silent < ttl_s.saturating_mul(3000) {
            field.push((priority, value["generation"].as_i64().unwrap_or(0), node));
        }
    }
    field.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)).then(a.2.cmp(&b.2)));
    Ok(field.iter().position(|entry| entry.2 == me))
}

/// Whether a lease held by another node may be taken over, and how long until it may be.
/// Released leases (heartbeat 0) are free at once; otherwise the holder must have been silent
/// for a full ttl on this process's monotonic clock.
fn takeover_wait_ms(target: &Target, lease: &Lease) -> i64 {
    if lease.heartbeat_ms == 0 {
        return 0;
    }
    (lease.ttl_s.saturating_mul(1000) - silent_ms(target, lease)).max(0)
}

fn target(table_oid: pgrx::pg_sys::Oid) -> Result<Target, String> {
    let t = super::backup::table_target(table_oid)?;
    let prefix = lease_prefix(&t.namespace, &t.name);
    Ok(Target {
        client: super::backup::storage_client(&t.storage.storage_id)?,
        key: format!("{}|{prefix}", t.storage.storage_id),
        prefix,
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
        sealed_sha: value["sealed_sha"].as_str().map(str::to_string),
    }))
}

fn write(target: &Target, lease: &Lease, mode: PutPrecondition) -> Result<(), String> {
    let body = serde_json::to_vec(&json!({
        "epoch": lease.epoch, "holder": lease.holder, "heartbeat_ms": lease.heartbeat_ms, "ttl_s": lease.ttl_s,
        "sealed_sha": lease.sealed_sha,
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

/// SHA-256 of the leader catalog object as it is now, `None` when none was published.
fn published_catalog_sha(table_oid: pgrx::pg_sys::Oid) -> Result<Option<String>, String> {
    let t = super::backup::table_target(table_oid)?;
    let client = super::backup::storage_client(&t.storage.storage_id)?;
    match client.get(&super::table_metadata::leader_catalog_key(&t.namespace, &t.name)) {
        Ok(bytes) => Ok(Some(koldstore_storage::content_checksum_sha256_hex(&bytes))),
        Err(koldstore_storage::StorageClientError::NotFound { .. }) => Ok(None),
        Err(error) => Err(format!("cannot seal the previous epoch's catalog: {error}")),
    }
}

/// Fencing check for a reader of the leader catalog. Storage cannot make "still the leader" and
/// "write the catalog" one atomic step, so a deposed leader that passed its lease check and then
/// stalled can still overwrite the catalog after its successor claimed the lease. Claiming epoch
/// N+1 records the checksum of the catalog as it was, so a catalog stamped with epoch N is
/// accepted only if it is exactly that content; anything written later is ignored.
pub(crate) fn verify_catalog_sealed(
    table_oid: pgrx::pg_sys::Oid,
    catalog_bytes: &[u8],
    catalog_epoch: Option<i64>,
) -> Result<(), String> {
    let Some(epoch) = catalog_epoch else {
        return Ok(());
    };
    let target = target(table_oid)?;
    let key = epoch_key(&target.prefix, epoch + 1);
    let bytes = match target.client.get(&key) {
        Ok(bytes) => bytes,
        Err(koldstore_storage::StorageClientError::NotFound { .. }) => return Ok(()),
        Err(error) => return Err(format!("cannot read lease epoch {}: {error}", epoch + 1)),
    };
    let sealed = serde_json::from_slice::<Value>(&bytes)
        .map_err(|error| format!("lease object is not valid: {error}"))?["sealed_sha"]
        .as_str()
        .map(str::to_string);
    match sealed {
        Some(sha) if sha != koldstore_storage::content_checksum_sha256_hex(catalog_bytes) => Err(format!(
            "the leader catalog (lease epoch {epoch}) was modified after lease epoch {} was claimed; \
             ignoring it until the new leader publishes",
            epoch + 1
        )),
        _ => Ok(()),
    }
}

fn acquire_value(table_oid: pgrx::pg_sys::Oid, ttl_s: i32, wait: bool) -> Result<Value, String> {
    if ttl_s < 3 {
        return Err("ttl_seconds must be at least 3".into());
    }
    if super::table_metadata::is_follower(table_oid)? {
        return Err("the table is a follower; turn follower mode off first".into());
    }
    let me = node_id()?;
    let target = target(table_oid)?;
    let mut now = now_ms();
    let mut existing = current(&target)?;
    // Waiting is for a lease that looks dead by the wall clock; the worker never waits (it keeps
    // its observations between ticks), and a live-looking lease fails at once.
    let patience = std::time::Instant::now();
    let next = loop {
        match &existing {
            Some(lease) if lease.holder == me && !lease.expired(now) => {
                // Renewal of the lease this node already holds.
                let renewed = Lease { heartbeat_ms: now, ttl_s: i64::from(ttl_s), ..lease.clone() };
                write(&target, &renewed, PutPrecondition::Overwrite)?;
                set_local_epoch(table_oid, Some(renewed.epoch))?;
                return Ok(renewed.to_json(now, &me));
            }
            // This node's own lease ran out: its clock judges its own heartbeat.
            Some(lease) if lease.holder == me => break lease.epoch + 1,
            Some(lease) => {
                let remaining = takeover_wait_ms(&target, lease);
                if remaining == 0 {
                    break lease.epoch + 1;
                }
                let looks_dead = lease.expired(now);
                if !(wait && looks_dead) || patience.elapsed() > std::time::Duration::from_millis(u64::try_from(lease.ttl_s.saturating_mul(1000)).unwrap_or(0) + 10_000) {
                    return Err(format!(
                        "the leader lease (epoch {}) is held by {} and was last seen alive {} ms ago by this node; it can be taken over in {remaining} ms",
                        lease.epoch,
                        lease.holder,
                        lease.ttl_s * 1000 - remaining
                    ));
                }
                pgrx::check_for_interrupts!();
                std::thread::sleep(std::time::Duration::from_millis(500));
                now = now_ms();
                existing = current(&target)?;
            }
            None => break 1,
        }
    };
    let claimed = Lease {
        epoch: next,
        holder: me.clone(),
        heartbeat_ms: now,
        ttl_s: i64::from(ttl_s),
        sealed_sha: published_catalog_sha(table_oid)?,
    };
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
            "{LOST}: shared lease is epoch {} held by {} (this node held epoch {epoch})",
            lease.epoch, lease.holder
        ));
    }
    if lease.expired(now) {
        return Err(format!(
            "{EXPIRED} {epoch} expired before it was renewed; acquire_leader_lease() to claim a new epoch"
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

/// What the worker did for a leader table with `koldstore.auto_elect` on.
pub(crate) enum Maintained {
    Renewed,
    /// The lease had expired unclaimed, so this node took the next epoch.
    Reclaimed(i64),
    /// Another node holds the lease; this table is a follower again.
    Demoted(String),
}

/// Renews this leader's lease. A lease that expired unclaimed is claimed again (the catalog is
/// this node's own), and a lease another node holds demotes the table to follower. Errors that
/// do not say who holds the lease (storage down) are returned and change nothing.
pub(crate) fn maintain_leader(table_oid: pgrx::pg_sys::Oid) -> Result<Maintained, String> {
    let error = match renew_value(table_oid) {
        Ok(_) => return Ok(Maintained::Renewed),
        Err(error) => error,
    };
    let lost = if error.starts_with(LOST) {
        error.clone()
    } else if error.starts_with(EXPIRED) {
        match acquire_value(table_oid, crate::guc::leader_lease_ttl_seconds(), false) {
            Ok(lease) => return Ok(Maintained::Reclaimed(lease["epoch"].as_i64().unwrap_or(0))),
            Err(error) if error.contains("is held by") || error.starts_with("lost the race") => error,
            Err(error) => return Err(error),
        }
    } else {
        return Err(error);
    };
    super::table_metadata::set_leader_value(table_oid, false)?;
    super::table_metadata::set_follower_value(table_oid, true)?;
    set_local_epoch(table_oid, None)?;
    Ok(Maintained::Demoted(lost))
}

/// Claims leadership of a follower table when the shared lease has expired and this node has
/// followed everything the last leader published. `Ok(None)` means "not now" (lease alive, no
/// lease ever existed); being behind or losing the race is an error.
pub(crate) fn try_elect(table_oid: pgrx::pg_sys::Oid) -> Result<Option<Value>, String> {
    use koldstore_storage::StorageClient;

    let t = super::backup::table_target(table_oid)?;
    let client = super::backup::storage_client(&t.storage.storage_id)?;
    let prefix = lease_prefix(&t.namespace, &t.name);
    let tgt = Target { client, key: format!("{}|{prefix}", t.storage.storage_id), prefix };
    // Without a lease object the leader never enforced one, so nobody may be taken over.
    let Some(lease) = current(&tgt)? else {
        return Ok(None);
    };
    let clear = || ELECTABLE.lock().unwrap_or_else(std::sync::PoisonError::into_inner).retain(|entry| entry.0 != tgt.key);
    if lease.holder != node_id()? && takeover_wait_ms(&tgt, &lease) > 0 {
        clear();
        return Ok(None);
    }
    if lease.holder == node_id()? && !lease.expired(now_ms()) {
        clear();
        return Ok(None);
    }
    let bytes = tgt
        .client
        .get(&super::table_metadata::leader_catalog_key(&t.namespace, &t.name))
        .map_err(|error| format!("cannot elect: no leader catalog ({error})"))?;
    let published: Value = serde_json::from_slice(&bytes).map_err(|error| format!("leader catalog is not valid: {error}"))?;
    let (held, seen) = held_and_seen(table_oid)?;
    let behind = held < super::table_metadata::max_generation(&published)
        || seen < published["lease_epoch"].as_i64().unwrap_or(0);
    if behind {
        return Err(format!(
            "cannot elect: this node holds generation {held} (leader epoch {seen}) but the leader published \
             generation {} (epoch {}); it must catch up first",
            super::table_metadata::max_generation(&published),
            published["lease_epoch"].as_i64().unwrap_or(0)
        ));
    }
    // The preferred candidate claims at once; each later one waits a stagger per rank, in case
    // the better ones are gone. The lease claim below still arbitrates, so this only orders tries.
    let first = {
        let mut seen_at = ELECTABLE.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match seen_at.iter().find(|entry| entry.0 == tgt.key) {
            Some(entry) => entry.1,
            None => {
                let now = std::time::Instant::now();
                seen_at.push((tgt.key.clone(), now));
                now
            }
        }
    };
    let ttl = i64::from(crate::guc::leader_lease_ttl_seconds());
    let Some(rank) = election_rank(&t, &tgt.client, held, super::table_metadata::max_generation(&published), published["lease_epoch"].as_i64().unwrap_or(0), ttl)? else {
        return Ok(None);
    };
    let stagger_ms = (ttl * 200).max(2000);
    let waited = i64::try_from(first.elapsed().as_millis()).unwrap_or(i64::MAX);
    if waited < stagger_ms.saturating_mul(i64::try_from(rank).unwrap_or(i64::MAX)) {
        return Ok(None);
    }
    clear();
    super::table_metadata::set_follower_value(table_oid, false)?;
    super::table_metadata::set_leader_value(table_oid, true)?;
    // A lost create-if-absent race errors here, so the caller's transaction rolls the flags back.
    acquire_value(table_oid, crate::guc::leader_lease_ttl_seconds(), false).map(Some)
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
    acquire_value(oid, ttl_seconds, true)
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
