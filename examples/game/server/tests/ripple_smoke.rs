//! Boss Ripple smoke test: two in-process hosts, placement alternating by
//! depth so every tier-up hop crosses hosts. Defeating bosses must cascade
//! hood → borough → city → realm with the right state at every tier.

mod support;

use std::time::Duration;

use distributed_game::{FieldValue, Role, ServerMessage};
use game_demos::ripple::{
    self, encode_input, RippleDemo, ACTION_ATTACK, ACTION_SMITE, BOROUGHS, CITY, EVENT_BANNER,
    EVENT_BOSS, EVENT_HELLO, EVENT_RIPPLE, EVENT_WEATHER, REALM,
};
use serde_json::Value;
use support::{eventually, join, start_cluster, ticket, Client};

/// Field `i` of the tier entity (id 1) as u32.
fn tier_field(client: &Client, i: usize) -> u32 {
    match client.mirror.entities.get(&1).map(|e| &e.fields[i]) {
        Some(FieldValue::U(v)) => *v,
        Some(FieldValue::Bool(b)) => u32::from(*b),
        _ => u32::MAX,
    }
}

async fn observe(base: &str, zone: &str) -> Client {
    let mut c = join(base, zone, &ticket("guest-watch", zone, Role::Observer))
        .await
        .unwrap();
    let hello = c.json_event(EVENT_HELLO).await;
    assert_eq!(hello["zone"], zone);
    c.until("tier entity", |c| c.mirror.entities.contains_key(&1))
        .await;
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boss_defeats_ripple_up_every_tier_across_hosts() {
    let cluster = start_cluster::<RippleDemo>(&["A", "B"], Duration::from_secs(20)).await;
    let world = &cluster.hosts[0].world;
    assert_eq!(world.owner("realm.city.borough-1.hood-1"), "A");
    assert_eq!(world.owner(BOROUGHS[0]), "B");
    assert_eq!(world.owner(CITY), "A");
    assert_eq!(world.owner(REALM), "B");
    let base = cluster.host("A").demo.ws_url();

    let mut realm = observe(&base, REALM).await;
    let mut city = observe(&base, CITY).await;
    let mut borough = observe(&base, BOROUGHS[0]).await;
    assert_eq!(realm.redirects, 1, "realm lives on B");
    assert_eq!(realm.welcome.tick_rate, 2, "slow realm clock");
    assert_eq!(city.welcome.tick_rate, 5);
    assert_eq!(borough.welcome.tick_rate, 10);
    // Upper tiers are observed, never joined.
    let refused = join(&base, CITY, &ticket("guest-x", CITY, Role::Player)).await;
    assert!(matches!(refused, Err(ServerMessage::Error { .. })));

    // Fight the first boss by hand: walk to it and attack.
    let hood1 = "realm.city.borough-1.hood-1";
    let mut fighter = join(&base, hood1, &ticket("guest-fighter", hood1, Role::Player))
        .await
        .unwrap();
    assert_eq!(fighter.welcome.tick_rate, 30, "hot neighborhood");
    let hello = fighter.json_event(EVENT_HELLO).await;
    assert_eq!(hello["tier"], "hood");
    assert!(hello["entity"].as_u64().is_some());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut defeated = None;
    while defeated.is_none() {
        assert!(tokio::time::Instant::now() < deadline, "boss not defeated");
        // Steer toward the boss at the center, attacking once in reach.
        let me = hello["entity"].as_u64().unwrap() as u32;
        let (x, y) = match fighter
            .mirror
            .entities
            .get(&me)
            .map(|e| (&e.fields[0], &e.fields[1]))
        {
            Some((FieldValue::F(x), FieldValue::F(y))) => (*x, *y),
            _ => (0.0, 0.0),
        };
        let d = (x * x + y * y).sqrt().max(1e-3);
        let k = if d > 3.5 { 127.0 / d } else { 0.0 };
        fighter
            .input(encode_input((-x * k) as i8, (-y * k) as i8, ACTION_ATTACK))
            .await;
        while let Some(msg) = fighter.next(Duration::from_millis(33)).await {
            if let ServerMessage::Event { type_id, payload } = msg {
                if type_id == EVENT_BOSS {
                    defeated = Some(serde_json::from_slice::<Value>(&payload).unwrap());
                }
            }
        }
    }
    assert_eq!(defeated.unwrap()["kind"], "defeated");

    // The rest of borough-1 by smiting.
    for hood in &ripple::hoods(BOROUGHS[0])[1..] {
        let mut c = join(&base, hood, &ticket("guest-smiter", hood, Role::Player))
            .await
            .unwrap();
        c.json_event(EVENT_HELLO).await;
        c.input(encode_input(0, 0, ACTION_SMITE)).await;
        assert_eq!(c.json_event(EVENT_BOSS).await["kind"], "defeated");
        let state = cluster
            .host("A")
            .demo
            .server
            .inspect(hood, |h| h.zone().state().hp)
            .await;
        assert_eq!(state, Some(0));
    }
    borough
        .until("borough tallied all three", |c| {
            tier_field(c, 0) == 3 && tier_field(c, 2) == 1
        })
        .await;
    let ripples = borough.seen(EVENT_RIPPLE);
    city.until("city saw the borough cleared", |c| {
        tier_field(c, 2) == 1 && tier_field(c, 0) == 3
    })
    .await;
    assert_eq!(tier_field(&city, 4), 1, "weather: rain with 3 bosses left");
    realm
        .until("realm counted", |c| {
            tier_field(c, 0) == 3 && tier_field(c, 2) == 1
        })
        .await;
    assert_eq!(tier_field(&realm, 3), 0, "city not liberated yet");

    // Borough-2 falls too: the city is liberated and the realm says so.
    for hood in ripple::hoods(BOROUGHS[1]) {
        let mut c = join(&base, &hood, &ticket("guest-smiter", &hood, Role::Player))
            .await
            .unwrap();
        c.json_event(EVENT_HELLO).await;
        c.input(encode_input(0, 0, ACTION_SMITE)).await;
        c.json_event(EVENT_BOSS).await;
    }
    let banner = realm.json_event(EVENT_BANNER).await;
    assert!(
        banner["text"].as_str().unwrap().contains("liberated"),
        "{banner}"
    );
    realm
        .until("realm state", |c| {
            tier_field(c, 0) == 6 && tier_field(c, 2) == 2 && tier_field(c, 3) == 1
        })
        .await;
    city.until("city liberated", |c| {
        tier_field(c, 3) == 1 && tier_field(c, 4) == 3
    })
    .await;
    let weather: Vec<String> = city
        .seen(EVENT_WEATHER)
        .iter()
        .map(|w| w["weather"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        weather.last().map(String::as_str),
        Some("clear"),
        "{weather:?}"
    );
    assert!(weather.iter().any(|w| w == "rain"), "{weather:?}");

    // Borough-1's ripple events name each hood as origin and the borough as receiver.
    let origins: std::collections::BTreeSet<String> = ripples
        .iter()
        .filter(|r| r["kind"] == "boss_defeated")
        .map(|r| r["origin"].as_str().unwrap().to_string())
        .collect();
    assert!(origins.len() >= 2, "{ripples:?}");
    assert!(ripples.iter().all(|r| r["at"] == BOROUGHS[0]));

    // The cascade's state lives where each tier runs.
    let city_state = cluster
        .host("A")
        .demo
        .server
        .inspect(CITY, |h| h.zone().state().clone())
        .await
        .unwrap();
    assert!(city_state.liberated);
    assert_eq!(city_state.cleared.len(), 2);
    eventually("realm state on B", || {
        let server = cluster.host("B").demo.server.clone();
        async move {
            server
                .inspect(REALM, |h| h.zone().state().liberations)
                .await
                == Some(1)
        }
    })
    .await;
    cluster.stop().await;
}
