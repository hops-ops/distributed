//! Hibernation snapshots in celld cells (`CellZoneStateStore` →
//! `ZoneState:{zone}`) on an isolated `celld dev`: the zone-state fencing
//! contract for an inline snapshot and for one above the inline limit (in
//! celld's R2 object storage), and both surviving a celld crash.
//!
//!   (cd tests/celld/fact-worker && worker-build --release)
//!   cargo test -p distributed_game --features celld,runtime --test celld_zone_state

use distributed::cell_host::InternalHttpSecret;
use distributed_game::celld::CellZoneStateStore;
use distributed_game::world::zone_state::ZONE_STATE_INLINE_MAX;
use distributed_game::world::{state_store_conformance, StoredZoneState, ZoneStateStore};

#[path = "../support/celld.rs"]
mod celld_support;
use celld_support::{Celld, SECRET};

fn store(base: &str) -> CellZoneStateStore {
    CellZoneStateStore::new(
        base,
        InternalHttpSecret::new(SECRET).unwrap(),
        "zone-state-test",
        "host-a",
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshots_are_fenced_inline_and_in_object_storage_and_survive_a_crash() {
    let Some(mut celld) = Celld::start_new() else {
        return;
    };
    let states = store(&celld.base());
    state_store_conformance(&states, "grid.0.0", vec![1; 512]).await;
    let big: Vec<u8> = (0..ZONE_STATE_INLINE_MAX * 3)
        .map(|i| (i % 253) as u8)
        .collect();
    state_store_conformance(&states, "grid.0.1", big.clone()).await;

    // celld crashes (process tree killed) and restarts on its state.
    celld.crash();
    celld.start().expect("celld restarts");
    let states = store(&celld.base());
    let small = states.load("grid.0.0").await.unwrap().unwrap();
    assert_eq!((small.token, small.state.len()), (5, 512));
    let large = states.load("grid.0.1").await.unwrap().unwrap();
    assert_eq!(large.token, 5);
    assert_eq!(large.state, big, "the object came back byte for byte");
    // Fencing state survived too.
    assert!(states
        .save(
            "grid.0.1",
            &StoredZoneState {
                token: 4,
                content_version: "c1".into(),
                state: vec![0; 10],
            },
        )
        .await
        .is_err());
}
