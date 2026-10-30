//! Follower acknowledgements: lets a leader tell whether every follower has moved past the
//! segments it is about to reap.
//!
//! After each sync a follower records the leader manifest generation it holds in
//! `_follower_acks/<schema>/<table>/<node>.json` on the shared storage. Before reaping, the leader
//! lists them and defers while a live (younger than `koldstore.follower_ack_max_age_seconds`)
//! acknowledgement is behind the leader's own generation. A follower that never acknowledged is not
//! known to the leader, and one that went silent for longer than the max age is treated as gone.
//!
//! "Silent" is judged on the leader's own clock alone. The acknowledgement carries the follower's
//! wall-clock `synced_ms`, but comparing that with the leader's clock would let a skewed follower
//! look dead (and its segments get reaped) or immortal. Instead the leader records when it first saw
//! each distinct acknowledgement in `_follower_seen/<schema>/<table>/<node>.json` and measures age
//! from there. That record lives on the shared storage so every backend, a restart, and a successor
//! leader share it (a new observer starts the age at zero, the conservative side).
//!
//! The acknowledgement is written after the follower's transaction commits, so a sync that rolled
//! back never claims a generation the node does not hold. `set_table_follower(true)` records one at
//! once, so the leader waits for a follower even before its first sync; `set_table_follower(false)`
//! (promotion or decommissioning) removes it, so a node that left does not hold reaping back. It also
//! removes the leader's observation record of that node, and the leader sweeps any observation record
//! left without an acknowledgement each time it checks the followers.

use std::sync::Mutex;

use serde_json::{Value, json};

use koldstore_storage::{PutPrecondition, StorageClient};

/// A follower rewrites an unchanged acknowledgement at most this often.
const REFRESH_MS: i64 = 30_000;

/// `(table oid, generation, written at)` of the acknowledgements this process wrote.
static WRITTEN: Mutex<Vec<(u32, i64, i64)>> = Mutex::new(Vec::new());

fn seen_key(namespace: &str, name: &str, node: &str) -> String {
    format!("_follower_seen/{namespace}/{name}/{node}.json")
}

/// Milliseconds this leader has seen the acknowledgement `(generation, synced_ms)` of `node`
/// unchanged; 0 when it is new to this leader.
fn observed_age_ms(
    client: &impl StorageClient,
    key: &str,
    me: &str,
    generation: i64,
    synced_ms: i64,
    now: i64,
) -> i64 {
    let previous = client
        .get(key)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    if let Some(seen) = previous {
        if seen["observed_by"].as_str() == Some(me)
            && seen["generation"].as_i64() == Some(generation)
            && seen["synced_ms"].as_i64() == Some(synced_ms)
        {
            return now.saturating_sub(seen["seen_ms"].as_i64().unwrap_or(now));
        }
    }
    let body = json!({ "observed_by": me, "generation": generation, "synced_ms": synced_ms, "seen_ms": now });
    if let Ok(bytes) = serde_json::to_vec(&body) {
        if let Err(error) = client.put(key, &bytes, PutPrecondition::Overwrite) {
            pgrx::log!("koldstore: could not record the follower observation {key}: {error}");
        }
    }
    0
}

pub(crate) struct Laggard {
    pub node: String,
    pub generation: i64,
    pub age_s: i64,
}

fn prefix(namespace: &str, name: &str) -> String {
    format!("_follower_acks/{namespace}/{name}/")
}

/// Removes this node's acknowledgement (best effort); it is rewritten at the next sync if the
/// node is still a follower.
pub(crate) fn forget(table_oid: pgrx::pg_sys::Oid) {
    let result = (|| -> Result<(), String> {
        let target = super::backup::table_target(table_oid)?;
        let client = super::backup::storage_client(&target.storage.storage_id)?;
        let node = super::leader_lease::node_id()?;
        // The leader's observation of this node goes too, or it would outlive the follower.
        let _ = client.delete(&seen_key(&target.namespace, &target.name, &node));
        client
            .delete(&format!("{}{node}.json", prefix(&target.namespace, &target.name)))
            .map_err(|error| error.to_string())
    })();
    if let Err(error) = result {
        pgrx::warning!("koldstore: could not remove the follower acknowledgement: {error}");
    }
    if let Ok(mut written) = WRITTEN.lock() {
        written.retain(|(o, _, _)| *o != table_oid.to_u32());
    }
}

/// Records that this follower holds leader generation `generation`, once the current
/// transaction has committed.
pub(crate) fn record(table_oid: pgrx::pg_sys::Oid, generation: i64) -> Result<(), String> {
    let now = super::leader_lease::now_ms();
    let oid = table_oid.to_u32();
    if let Ok(written) = WRITTEN.lock() {
        if written
            .iter()
            .any(|(o, g, at)| *o == oid && *g == generation && now.saturating_sub(*at) < REFRESH_MS)
        {
            return Ok(());
        }
    }
    let target = super::backup::table_target(table_oid)?;
    let client = super::backup::storage_client(&target.storage.storage_id)?;
    let node = super::leader_lease::node_id()?;
    let body = serde_json::to_vec(&json!({ "node": node, "generation": generation, "synced_ms": now }))
        .map_err(|error| error.to_string())?;
    let job = std::panic::AssertUnwindSafe((client, format!("{}{node}.json", prefix(&target.namespace, &target.name)), body));
    pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Commit, move || {
        let job = job;
        let std::panic::AssertUnwindSafe((client, key, body)) = job;
        match client.put(&key, &body, PutPrecondition::Overwrite) {
            Ok(_) => {
                if let Ok(mut written) = WRITTEN.lock() {
                    written.retain(|(o, _, _)| *o != oid);
                    written.push((oid, generation, now));
                }
            }
            Err(error) => pgrx::log!("koldstore: could not write the follower acknowledgement {key}: {error}"),
        }
    });
    Ok(())
}

/// Deletes the observation records of nodes that no longer have an acknowledgement (a follower that
/// left, or whose own cleanup failed). A node that comes back writes a new acknowledgement and starts
/// at age zero either way, so nothing is lost. Best effort.
fn sweep_observations(client: &impl StorageClient, namespace: &str, name: &str, acks: &[koldstore_storage::StorageObject]) {
    let ack_prefix = prefix(namespace, name);
    let seen_prefix = seen_key(namespace, name, "");
    let seen_prefix = seen_prefix.trim_end_matches(".json");
    let Ok(seen) = client.list(seen_prefix) else { return };
    for object in seen {
        let Some(node) = object.key.strip_prefix(seen_prefix).and_then(|rest| rest.strip_suffix(".json")) else { continue };
        let acked = format!("{ack_prefix}{node}.json");
        if !acks.iter().any(|ack| ack.key == acked) {
            let _ = client.delete(&object.key);
        }
    }
}

/// Live followers whose acknowledged generation is below `generation`. Empty when the check is off
/// (`koldstore.follower_ack_max_age_seconds = 0`).
pub(crate) fn laggards(table_oid: pgrx::pg_sys::Oid, generation: i64) -> Result<Vec<Laggard>, String> {
    let max_age_ms = i64::from(crate::guc::follower_ack_max_age_seconds()).saturating_mul(1000);
    if max_age_ms == 0 {
        return Ok(Vec::new());
    }
    let target = super::backup::table_target(table_oid)?;
    let client = super::backup::storage_client(&target.storage.storage_id)?;
    let now = super::leader_lease::now_ms();
    let me = super::leader_lease::node_id()?;
    let mut out = Vec::new();
    let acks = client
        .list(&prefix(&target.namespace, &target.name))
        .map_err(|error| error.to_string())?;
    sweep_observations(&client, &target.namespace, &target.name, &acks);
    for object in acks {
        let Ok(bytes) = client.get(&object.key) else { continue };
        let Ok(ack) = serde_json::from_slice::<Value>(&bytes) else { continue };
        let node = ack["node"].as_str().unwrap_or_default().to_string();
        let held = ack["generation"].as_i64().unwrap_or(0);
        if node == me || held >= generation {
            continue;
        }
        let age = observed_age_ms(
            &client,
            &seen_key(&target.namespace, &target.name, &node),
            &me,
            held,
            ack["synced_ms"].as_i64().unwrap_or(0),
            now,
        );
        if age <= max_age_ms {
            out.push(Laggard { node, generation: held, age_s: age / 1000 });
        }
    }
    Ok(out)
}

/// The generation every live follower has acknowledged (the lowest), or `None` when there is no
/// live follower or the check is off. A segment retired at a generation at or below it is
/// referenced by no follower's catalog.
pub(crate) fn ack_floor(table_oid: pgrx::pg_sys::Oid) -> Result<Option<i64>, String> {
    // Asking for "behind i64::MAX" lists every live follower.
    Ok(laggards(table_oid, i64::MAX)?.iter().map(|l| l.generation).min())
}

/// Compacted segments of `table_oid` that a live follower may still reference.
pub(crate) fn held_back(table_oid: pgrx::pg_sys::Oid) -> Result<i64, String> {
    let Some(floor) = ack_floor(table_oid)? else { return Ok(0) };
    pgrx::Spi::get_one_with_args::<i64>(
        "SELECT count(*) FROM koldstore.cold_segments cs WHERE cs.table_oid = $1 AND cs.status = 'compacted' \
         AND COALESCE(cs.retired_generation, (SELECT max(m.generation) FROM koldstore.manifest m WHERE m.table_oid = cs.table_oid)) > $2",
        &[pgrx::datum::DatumWithOid::from(table_oid), pgrx::datum::DatumWithOid::from(floor)],
    )
    .map_err(|error| error.to_string())
    .map(|n| n.unwrap_or(0))
}

/// Errors naming the followers still behind `table_oid`'s current generation, if any.
pub(crate) fn require_followers_caught_up(table_oid: pgrx::pg_sys::Oid, action: &str) -> Result<(), String> {
    let generation = pgrx::Spi::get_one_with_args::<i64>(
        "SELECT COALESCE(max(generation), 0)::bigint FROM koldstore.manifest WHERE table_oid = $1",
        &[pgrx::datum::DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(0);
    require_followers_caught_up_to(table_oid, action, generation)
}

/// Errors naming the followers still behind `generation`, if any.
pub(crate) fn require_followers_caught_up_to(table_oid: pgrx::pg_sys::Oid, action: &str, generation: i64) -> Result<(), String> {
    let behind = laggards(table_oid, generation)?;
    if behind.is_empty() {
        return Ok(());
    }
    let list = behind
        .iter()
        .map(|l| format!("node {} holds generation {} (acknowledged {}s ago)", l.node, l.generation, l.age_s))
        .collect::<Vec<_>>()
        .join(", ");
    Err(format!(
        "cannot {action}: a follower has not caught up to generation {generation}: {list}; retry after it syncs, \
         or wait for its acknowledgement to age out (koldstore.follower_ack_max_age_seconds)"
    ))
}
