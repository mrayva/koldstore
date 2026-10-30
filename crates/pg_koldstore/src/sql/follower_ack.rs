//! Follower acknowledgements: lets a leader tell whether every follower has moved past the
//! segments it is about to reap.
//!
//! After each sync a follower records the leader manifest generation it holds in
//! `_follower_acks/<schema>/<table>/<node>.json` on the shared storage. Before reaping, the leader
//! lists them and defers while a live (younger than `koldstore.follower_ack_max_age_seconds`)
//! acknowledgement is behind the leader's own generation. A follower that never acknowledged is not
//! known to the leader, and one that went silent for longer than the max age is treated as gone.
//!
//! The acknowledgement is written after the follower's transaction commits, so a sync that rolled
//! back never claims a generation the node does not hold. `set_table_follower(true)` records one at
//! once, so the leader waits for a follower even before its first sync; `set_table_follower(false)`
//! (promotion or decommissioning) removes it, so a node that left does not hold reaping back.

use std::sync::Mutex;

use serde_json::{Value, json};

use koldstore_storage::{PutPrecondition, StorageClient};

/// A follower rewrites an unchanged acknowledgement at most this often.
const REFRESH_MS: i64 = 30_000;

/// `(table oid, generation, written at)` of the acknowledgements this process wrote.
static WRITTEN: Mutex<Vec<(u32, i64, i64)>> = Mutex::new(Vec::new());

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
    for object in client
        .list(&prefix(&target.namespace, &target.name))
        .map_err(|error| error.to_string())?
    {
        let Ok(bytes) = client.get(&object.key) else { continue };
        let Ok(ack) = serde_json::from_slice::<Value>(&bytes) else { continue };
        let node = ack["node"].as_str().unwrap_or_default().to_string();
        let held = ack["generation"].as_i64().unwrap_or(0);
        let age = now.saturating_sub(ack["synced_ms"].as_i64().unwrap_or(0));
        if node != me && held < generation && age <= max_age_ms {
            out.push(Laggard { node, generation: held, age_s: age / 1000 });
        }
    }
    Ok(out)
}

/// Errors naming the followers still behind `table_oid`'s current generation, if any.
pub(crate) fn require_followers_caught_up(table_oid: pgrx::pg_sys::Oid, action: &str) -> Result<(), String> {
    let generation = pgrx::Spi::get_one_with_args::<i64>(
        "SELECT COALESCE(max(generation), 0)::bigint FROM koldstore.manifest WHERE table_oid = $1",
        &[pgrx::datum::DatumWithOid::from(table_oid)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(0);
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
