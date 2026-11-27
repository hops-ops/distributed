//! Zone ownership as celld cells (`CellZoneOwnership` → `ZoneLease:{zone}`)
//! on an isolated `celld dev`: the ownership conformance suite, expiry
//! declared by the cell alarm when a holder stops renewing, renewals that
//! keep the lease, and a `GameServer` running a zone on it.
//!
//!   (cd tests/celld/fact-worker && worker-build --release)
//!   cargo test -p distributed_game --features celld,runtime,net --test celld_zone_lease -- --ignored
//!
//! The test is live (`#[ignore = "live: ..."]`): without a built worker or
//! celld it fails.

use std::sync::Arc;
use std::time::{Duration, Instant};

use distributed::microsvc::cell_host::InternalHttpSecret;
use distributed_game::celld::CellZoneOwnership;
use distributed_game::net::{GameServer, ServerConfig, TicketKey, ZoneFactory};
use distributed_game::world::{
    ownership_conformance, restart_conformance, OwnershipError, ZoneOwnership,
};
use serde_json::Value;

#[path = "../support/arena.rs"]
mod arena;
#[path = "../support/celld.rs"]
mod celld_support;
#[path = "../support/unique.rs"]
mod unique;
use arena::{Arena, ArenaConfig};
use celld_support::{Celld, SECRET};

fn ownership(base: &str, host: &str) -> CellZoneOwnership {
    CellZoneOwnership::new(
        base,
        InternalHttpSecret::new(SECRET).unwrap(),
        "zone-lease-test",
        host,
    )
    .unwrap()
}

async fn lease(base: &str, zone: &str) -> Value {
    reqwest::Client::new()
        .get(format!("{base}/zone-lease/{zone}"))
        .header("x-distributed-internal-secret", SECRET)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn count(state: &Value, event: &str) -> usize {
    state["eventNames"].as_array().map_or(0, |names| {
        names.iter().filter(|name| *name == event).count()
    })
}

async fn until(what: &str, within: Duration, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out: {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live: needs a built tests/celld/fact-worker and celld"]
async fn zone_leases_live_in_celld_cells() {
    let celld = Celld::start_new();
    let base = celld.base();
    let own = ownership(&base, "host-a");

    // 1. Fenced, exclusive, strictly increasing tokens; Held and Lost.
    ownership_conformance(&own, "zl-conformance").await;
    let conformance = lease(&base, "zl-conformance").await;
    assert_eq!(count(&conformance, "zone.activated"), 3);
    assert_eq!(count(&conformance, "zone.released"), 3);
    assert_eq!(count(&conformance, "zone.lease_expired"), 0);

    // 2. A holder that stops renewing: exactly one zone.lease_expired after
    //    the TTL, declared by the cell's own alarm (nobody calls the cell).
    let dead = own
        .acquire("zl-expire", "a#1", Duration::from_millis(1_500))
        .await
        .unwrap();
    until("lease expiry", Duration::from_secs(20), async || {
        lease(&base, "zl-expire").await["state"]["holder"].is_null()
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let expired = lease(&base, "zl-expire").await;
    assert_eq!(count(&expired, "zone.lease_expired"), 1, "{expired}");
    assert_eq!(expired["armedAlarms"], 0);
    assert_eq!(
        own.renew(&dead, Duration::from_secs(1)).await,
        Err(OwnershipError::Lost)
    );
    let next = own
        .acquire("zl-expire", "b#1", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(next.token > dead.token);
    own.release(&next).await;

    // 2b. A crash-restarted holder (same host, new incarnation): Held while
    //     the dead claim lives, a larger token once the cell expired it.
    let base_for_expiry = base.clone();
    restart_conformance(
        &own,
        "zl-restart",
        Duration::from_millis(1_500),
        until("restart expiry", Duration::from_secs(20), async move || {
            lease(&base_for_expiry, "zl-restart").await["state"]["holder"].is_null()
        }),
    )
    .await;

    // 3. Renewals keep the lease past many TTLs, without expiry or outward
    //    events.
    let mut live = own
        .acquire("zl-renew", "a#1", Duration::from_millis(1_500))
        .await
        .unwrap();
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(400)).await;
        live = own
            .renew(&live, Duration::from_millis(1_500))
            .await
            .unwrap();
    }
    let renewed = lease(&base, "zl-renew").await;
    assert_eq!(renewed["state"]["holder"], "a#1");
    assert_eq!(count(&renewed, "zone.lease_expired"), 0);
    assert_eq!(count(&renewed, "zone_lease.renewed"), 10);
    own.release(&live).await;

    // 4. A zone server runs a zone under the celld lease: the cell sees the
    //    holder while the server renews, and the lease ends when it stops.
    let mut config = ServerConfig::new(TicketKey::new("celld-lease-test-ticket-secret"));
    config.ownership_ttl = Duration::from_secs(3);
    config.ownership_renew_every = Duration::from_millis(500);
    let server =
        GameServer::<Arena>::new(config, ZoneFactory::sync(|_| Some(ArenaConfig::default())))
            .with_host_id("A")
            .with_ownership(Arc::new(ownership(&base, "host-a")));
    assert!(server.activate("zl-game").await, "zone activated");
    let held = lease(&base, "zl-game").await;
    let holder = held["state"]["holder"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(holder.starts_with("A#"), "{held}");
    tokio::time::sleep(Duration::from_secs(5)).await;
    let kept = lease(&base, "zl-game").await;
    assert_eq!(
        kept["state"]["holder"],
        holder.as_str(),
        "renewed past the TTL"
    );
    assert_eq!(count(&kept, "zone.lease_expired"), 0);
    server.shutdown();
    until(
        "lease end after shutdown",
        Duration::from_secs(20),
        async || lease(&base, "zl-game").await["state"]["holder"].is_null(),
    )
    .await;
    let ended = lease(&base, "zl-game").await;
    assert_eq!(
        count(&ended, "zone.released") + count(&ended, "zone.lease_expired"),
        1,
        "exactly one terminal event: {ended}"
    );
    drop(celld);
}
