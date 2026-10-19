//! Endless Grid smoke tests: two in-process hosts (A, B) on a hash ring.
//!
//! - a walker crosses several cell borders, at least one of them between
//!   hosts, following every Handoff (and Redirect) without losing its
//!   session, and ends in the right zone;
//! - a cell hibernates into SQLite and restores critters and markers;
//! - dev routes: world map across hosts, names shared with peers.

mod support;

use std::time::Duration;

use distributed_game::world::ZoneStateStore;
use distributed_game::{FieldValue, Role, ServerMessage};
use distributed_game::{GameInput, Replicated};
use game_demos::grid::world::{self, MoveInput, BUTTON_MARK, BUTTON_SPRINT};
use game_demos::grid::{CritterView, GridDemo, GridHello, MarkerView};
use serde_json::{json, Value};
use support::{eventually, http, join, start_cluster, ticket, Client};

fn east(buttons: u8) -> Vec<u8> {
    MoveInput {
        mx: 127,
        my: 0,
        buttons: BUTTON_SPRINT | buttons,
    }
    .to_payload()
}

fn f(fields: &[FieldValue], i: usize) -> f32 {
    match fields[i] {
        FieldValue::F(v) => v,
        _ => f32::NAN,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn walker_crosses_borders_and_hosts_without_losing_its_session() {
    let cluster = start_cluster::<GridDemo>(&["A", "B"], Duration::from_secs(4)).await;
    let world = &cluster.hosts[0].world;
    // Walk east along the y = 16 trail until we crossed a host boundary.
    let owner = |x: i32| world.owner(&world::zone_id((x, 0)));
    let last = (1..=16)
        .find(|k| (0..*k).any(|i| owner(i) != owner(i + 1)) && *k >= 2)
        .expect("a cross-host border within 16 cells");
    let base = cluster.host("A").demo.ws_url();

    // Interest across the border: an observer of the east neighbour sees its critters.
    let mut observer = join(
        &base,
        "grid.1.0",
        &ticket("guest-eye", "grid.1.0", Role::Observer),
    )
    .await
    .unwrap();
    let hello = observer.wait_event::<GridHello>(|_| true).await;
    assert_eq!(hello.host, owner(1));
    assert!(hello.entity.is_none(), "observers control nothing");
    observer
        .until("critters replicated to the observer", |c| {
            c.count(CritterView::KIND_ID) >= 2
        })
        .await;

    let mut client = join(
        &base,
        "grid.0.0",
        &ticket("guest-walker", "grid.0.0", Role::Player),
    )
    .await
    .unwrap();
    let hello = client.wait_event::<GridHello>(|_| true).await;
    let name = format!("Guest {}", &"guest-walker"[6..10]);
    assert_eq!(hello.cell, [0, 0]);
    assert!(hello.entity.is_some());

    let mut zone = "grid.0.0".to_string();
    let mut handoffs = 0;
    let mut cross_host = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    let target = world::zone_id((last, 0));
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "walk timed out in {zone}"
        );
        client.input(east(0)).await;
        let mut handoff = None;
        while let Some(msg) = client.next(Duration::from_millis(30)).await {
            match msg {
                ServerMessage::Handoff {
                    target_zone,
                    transfer_ticket,
                } => {
                    handoff = Some((target_zone, transfer_ticket));
                    break;
                }
                ServerMessage::Error { code, message } => panic!("error {code}: {message}"),
                ServerMessage::Kick { message, .. } => panic!("kicked: {message}"),
                _ => {}
            }
        }
        if let Some((to, transfer)) = handoff {
            let from_owner = world.owner(&zone);
            let to_owner = world.owner(&to);
            let mut next: Client = join(&base, &to, &transfer).await.expect("target admits");
            let hello = next.wait_event::<GridHello>(|_| true).await;
            assert_eq!(hello.zone, to);
            assert_eq!(hello.host, to_owner);
            assert_eq!(
                next.redirects > 0,
                to_owner != "A",
                "redirected to the owner"
            );
            handoffs += 1;
            if from_owner != to_owner {
                cross_host += 1;
            }
            zone = to;
            client = next;
            continue;
        }
        if zone == target {
            let owner = cluster.owner(&zone);
            let walkers = owner
                .demo
                .server
                .inspect(&zone, |h| h.zone().walkers())
                .await
                .unwrap();
            if walkers.first().is_some_and(|(_, x, _)| *x > 4.0) {
                assert_eq!(walkers.len(), 1);
                assert_eq!(walkers[0].0, name, "same walker, name carried across hosts");
                assert!((walkers[0].2 - 16.0).abs() < 0.01, "stayed on the trail");
                break;
            }
        }
    }
    assert_eq!(handoffs, last, "one handoff per border");
    assert!(cross_host >= 1, "crossed between hosts");
    for x in 0..last {
        let id = world::zone_id((x, 0));
        let walkers = cluster
            .owner(&id)
            .demo
            .server
            .inspect(&id, |h| h.zone().walkers().len())
            .await;
        assert!(
            matches!(walkers, Some(0) | None),
            "{id} released the walker"
        );
    }
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_cell_hibernates_and_restores_critters_and_markers() {
    let cluster = start_cluster::<GridDemo>(&["A", "B"], Duration::from_millis(300)).await;
    let zone = "grid.5.5";
    let owner = cluster.owner(zone);
    let base = cluster.host("A").demo.ws_url();

    let mut client = join(&base, zone, &ticket("guest-marker", zone, Role::Player))
        .await
        .unwrap();
    let hello = client.wait_event::<GridHello>(|_| true).await;
    assert!(!hello.restored);
    client.input(east(BUTTON_MARK)).await;
    client
        .until("marker replicated", |c| c.count(MarkerView::KIND_ID) == 1)
        .await;
    client.leave().await;
    drop(client);

    eventually("hibernation", || {
        let server = owner.demo.server.clone();
        async move { !server.is_active(zone) }
    })
    .await;
    let saved = owner
        .world
        .state
        .load(zone)
        .unwrap()
        .expect("snapshot stored");
    let snapshot: Value = serde_json::from_slice(&saved.state).unwrap();
    assert_eq!(snapshot["markers"].as_array().unwrap().len(), 1);
    let critters = snapshot["critters"].as_array().unwrap().clone();
    assert!(critters.len() >= 2);

    // The world map shows it hibernated (asked on the other host too).
    let other = if owner.world.host.id == "A" { "B" } else { "A" };
    let (status, map) = http(
        cluster.host(other).demo.addr,
        "GET",
        "/dev/grid/map?x=5&y=5&r=0",
        None,
    )
    .await;
    assert_eq!(status, 200, "{map}");
    assert_eq!(map["cells"][0]["state"], "hibernated", "{map}");
    assert_eq!(map["cells"][0]["owner"], owner.world.host.id.as_str());

    let mut again = join(&base, zone, &ticket("guest-marker", zone, Role::Player))
        .await
        .unwrap();
    let hello = again.wait_event::<GridHello>(|_| true).await;
    assert!(hello.restored);
    assert_eq!(hello.restores, 1);
    assert_eq!(hello.restored_markers, 1);
    assert_eq!(hello.restored_critters as usize, critters.len());
    let n = critters.len();
    again
        .until("restored entities replicated", |c| {
            c.count(CritterView::KIND_ID) == n && c.count(MarkerView::KIND_ID) == 1
        })
        .await;
    // Critters resume exactly where they slept (first delta after restore).
    for c in &critters {
        let id = c["id"].as_u64().unwrap() as u32;
        let fields = &again.mirror.entities[&id].fields;
        let (x, y) = (
            c["x"].as_f64().unwrap() as f32,
            c["y"].as_f64().unwrap() as f32,
        );
        assert!(
            (f(fields, 0) - x).abs() < 0.2 && (f(fields, 1) - y).abs() < 0.2,
            "critter {id}"
        );
    }
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dev_routes_share_names_and_map_both_hosts() {
    let cluster = start_cluster::<GridDemo>(&["A", "B"], Duration::from_secs(4)).await;
    let world = &cluster.hosts[0].world;
    let on_b = (0..64)
        .map(|x| world::zone_id((x, 3)))
        .find(|z| world.owner(z) == "B")
        .unwrap();
    let a = cluster.host("A").demo.addr;
    let (status, ticket) = http(
        a,
        "POST",
        "/dev/ticket",
        Some(json!({ "zone": on_b, "name": "Ana" })),
    )
    .await;
    assert_eq!(status, 200, "{ticket}");
    let principal = ticket["principal"].as_str().unwrap();
    assert_eq!(
        cluster.host("B").demo.guests.name(principal).as_deref(),
        Some("Ana")
    );

    // The ticket from A works on B (shared secret) and B greets by name.
    let mut client = join(
        &cluster.host("A").demo.ws_url(),
        &on_b,
        ticket["ticket"].as_str().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(client.redirects, 1);
    client.wait_event::<GridHello>(|_| true).await;
    let walkers = cluster
        .host("B")
        .demo
        .server
        .inspect(&on_b, |h| h.zone().walkers())
        .await
        .unwrap();
    assert_eq!(walkers[0].0, "Ana");

    let (status, map) = http(a, "GET", "/dev/grid/map?x=0&y=3&r=2", None).await;
    assert_eq!(status, 200);
    let cells = map["cells"].as_array().unwrap();
    assert_eq!(cells.len(), 25);
    let owners: std::collections::BTreeSet<&str> =
        cells.iter().filter_map(|c| c["owner"].as_str()).collect();
    assert_eq!(owners.len(), 2, "both hosts own cells nearby: {owners:?}");
    let mine = cells.iter().find(|c| c["zone"] == on_b.as_str());
    if let Some(cell) = mine {
        assert_eq!(cell["state"], "active");
        assert_eq!(cell["players"], 1);
    }
    assert!(map["hosts"]
        .as_array()
        .unwrap()
        .iter()
        .all(|h| h["reachable"] == true));

    // The peer route needs the admin secret.
    let (status, _) = http(
        cluster.host("B").demo.addr,
        "POST",
        "/dev/grid/local",
        Some(json!({ "zones": [] })),
    )
    .await;
    assert_eq!(status, 401);
    cluster.stop().await;
}
