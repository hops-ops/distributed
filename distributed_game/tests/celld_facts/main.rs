//! Zone facts over the network into celld cells (`CelldFactSink`).
//!
//! Owns an isolated `celld dev` process (no Docker) serving
//! `tests/celld/fact-worker` (Kills, Match and Presence cells), and kills only
//! the processes it starts. Build the worker first:
//!
//!   (cd tests/celld/fact-worker && worker-build --release)
//!   cargo test -p distributed_game --features celld,runtime --test celld_facts
//!
//! `CELLD` selects the binary (default `celld` on PATH); `celld dev` needs
//! esbuild (`CELLD_ESBUILD`). Without a built worker the test skips, unless
//! `CELLD_FACTS_REQUIRED=1` (CI), which turns that into a failure.

use std::time::{Duration, Instant};

use distributed::cell_host::InternalHttpSecret;
use distributed_game::celld::{CelldFactRoute, CelldFactSink};
use distributed_game::lifecycle::{JoinVia, LeftReason, PlayerJoined, PlayerLeft};
use distributed_game::{
    fact_channel, spawn_fact_worker, EmitError, Fact, FactCommand, FactError, FactOutcome,
    FactQueue, FactSettlement, FactSink, FactStamp, RetryPolicy,
};
use serde_json::{json, Value};

#[path = "../support/celld.rs"]
mod celld_support;
use celld_support::{Celld, SECRET};
const SERVICE: &str = "zone-server-test";

#[derive(serde::Serialize, FactCommand)]
#[fact(command = "kills.record")]
struct Kill {
    mob: String,
}

#[derive(serde::Serialize, FactCommand)]
#[fact(command = "match.record_result")]
struct MatchResult {
    match_id: String,
    winner_slot: u32,
}

fn sink(base: &str, timeout: Duration) -> CelldFactSink {
    CelldFactSink::with_timeout(
        base,
        InternalHttpSecret::new(SECRET).unwrap(),
        SERVICE,
        "zone:pit-1",
        timeout,
    )
    .unwrap()
    .route(CelldFactRoute::by_principal::<Kill>("kills"))
    .route(CelldFactRoute::by_input_field::<MatchResult>(
        "match", "match_id",
    ))
    .lifecycle_route("presence")
}

fn stamp(tick: u64) -> FactStamp {
    FactStamp {
        zone_id: "pit-1".into(),
        tick,
        content_version: "c1".into(),
        emitted_at_ms: 1_700_000_000_000 + tick,
        activation: 1,
        config_version: "cfg-1".into(),
        seq: tick,
    }
}

fn fact<C: FactCommand>(key: &str, input: &C, principal: Option<&str>, tick: u64) -> Fact {
    Fact::trusted(
        key,
        C::COMMAND,
        serde_json::to_value(input).unwrap(),
        principal.map(str::to_string),
        stamp(tick),
    )
}

async fn state(base: &str, kind: &str, shard: &str) -> Value {
    reqwest::Client::new()
        .get(format!("{base}/{kind}/{shard}"))
        .header("x-distributed-internal-secret", SECRET)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Drain settlements until `count` arrive (or time out).
async fn settle(queue: &mut FactQueue, count: usize, within: Duration) -> Vec<FactSettlement> {
    let deadline = Instant::now() + within;
    let mut settled = Vec::new();
    while settled.len() < count && Instant::now() < deadline {
        settled.extend(queue.drain());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    settled
}

fn committed(settlement: &FactSettlement) -> bool {
    matches!(settlement.outcome, FactOutcome::Committed(_))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zone_facts_route_to_three_cell_kinds_and_survive_a_celld_outage() {
    let Some(mut celld) = Celld::start_new() else {
        return;
    };
    let base = celld.base();
    let policy = RetryPolicy::until_settled(Duration::from_millis(100), Duration::from_millis(500));
    let (mut queue, feed) = fact_channel(4, policy);
    let worker = spawn_fact_worker(
        feed.with_max_concurrent_commits(2),
        sink(&base, Duration::from_secs(10)),
    );

    // 1. One zone, one sink, three aggregate kinds (lifecycle -> presence).
    let joined = PlayerJoined {
        zone: "pit-1".into(),
        via: JoinVia::Ticket,
        from_zone: None,
    };
    queue
        .try_send(fact(
            "zone.player_joined:pit-1:1:p0:0",
            &joined,
            Some("alice"),
            1,
        ))
        .unwrap();
    queue
        .try_send(fact(
            "kills.record:pit-1:1:k1",
            &Kill { mob: "m1".into() },
            Some("alice"),
            2,
        ))
        .unwrap();
    queue
        .try_send(fact(
            "kills.record:pit-1:1:k2",
            &Kill { mob: "m2".into() },
            Some("bob"),
            3,
        ))
        .unwrap();
    queue
        .try_send(fact(
            "match.record_result:pit-1:1:m-7",
            &MatchResult {
                match_id: "m-7".into(),
                winner_slot: 2,
            },
            None,
            4,
        ))
        .unwrap();
    let settled = settle(&mut queue, 4, Duration::from_secs(30)).await;
    assert_eq!(settled.len(), 4, "{settled:?}");
    assert!(settled.iter().all(committed), "{settled:?}");
    let left = PlayerLeft {
        zone: "pit-1".into(),
        reason: LeftReason::Left,
    };
    queue
        .try_send(fact(
            "zone.player_left:pit-1:1:p0:1",
            &left,
            Some("alice"),
            5,
        ))
        .unwrap();
    assert!(settle(&mut queue, 1, Duration::from_secs(30))
        .await
        .iter()
        .all(committed));

    assert_eq!(
        state(&base, "kills", "alice").await["state"]["mobs"],
        json!(["m1"])
    );
    assert_eq!(
        state(&base, "kills", "bob").await["state"]["mobs"],
        json!(["m2"])
    );
    assert_eq!(
        state(&base, "match", "m-7").await["state"]["results"],
        json!([2])
    );
    assert_eq!(
        state(&base, "presence", "alice").await["state"]["transitions"],
        json!(["joined:pit-1:ticket", "left:pit-1:left"]),
        "lifecycle facts go to presence, not to the player's kill cell"
    );

    // 2. The same key twice (a retry after a lost response) commits once and
    //    replays the ledger receipt.
    let direct = sink(&base, Duration::from_secs(10));
    let again = fact(
        "kills.record:pit-1:1:k1",
        &Kill { mob: "m1".into() },
        Some("alice"),
        2,
    );
    let replay = direct.commit(&again).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(state(&base, "kills", "alice").await["events"], 1);
    //    Forced timeout after commit: the first attempt gives up before the
    //    response; the retry finds the commit in the ledger.
    let hasty = sink(&base, Duration::from_millis(1));
    let late = fact(
        "kills.record:pit-1:1:k3",
        &Kill { mob: "m3".into() },
        Some("alice"),
        6,
    );
    assert!(matches!(
        hasty.commit(&late).await,
        Err(FactError::Retryable(_))
    ));
    tokio::time::sleep(Duration::from_millis(500)).await;
    let receipt = direct.commit(&late).await.unwrap();
    assert_eq!(
        state(&base, "kills", "alice").await["state"]["mobs"],
        json!(["m1", "m3"]),
        "exactly one commit for the key (replayed: {})",
        receipt.replayed
    );

    // 3. An unknown command settles Permanent without a network call.
    queue
        .try_send(Fact::trusted(
            "guild.promote:pit-1:1:x",
            "guild.promote",
            json!({}),
            Some("alice".into()),
            stamp(7),
        ))
        .unwrap();
    let unknown = settle(&mut queue, 1, Duration::from_secs(10)).await;
    assert!(
        matches!(
            &unknown[0].outcome,
            FactOutcome::Failed {
                error: FactError::Permanent(_),
                attempts: 1
            }
        ),
        "{unknown:?}"
    );

    // 4. The cell worker rejects requests without the internal secret.
    let forged = reqwest::Client::new()
        .post(format!("{base}/kills/alice/kills.record"))
        .header("x-distributed-service-id", SERVICE)
        .header("x-distributed-principal-partition", "alice")
        .header("x-user-id", "alice")
        .json(&json!({ "commandId": "0190a000-0000-7000-8000-000000000001", "input": { "mob": "x" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(forged.status(), 401);
    let forged_secret = reqwest::Client::new()
        .post(format!("{base}/kills/alice/kills.record"))
        .header("x-distributed-internal-secret", "forged-secret-for-red-team-request-0000")
        .json(&json!({ "commandId": "0190a000-0000-7000-8000-000000000002", "input": { "mob": "x" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(forged_secret.status(), 401);
    //    With the secret but without the fact headers, handlers refuse.
    let not_a_fact = reqwest::Client::new()
        .post(format!("{base}/kills/alice/kills.record"))
        .header("x-distributed-internal-secret", SECRET)
        .header("x-distributed-service-id", SERVICE)
        .header("x-distributed-principal-partition", "alice")
        .header("x-user-id", "alice")
        .json(&json!({ "commandId": "0190a000-0000-7000-8000-000000000003", "input": { "mob": "x" } }))
        .send()
        .await
        .unwrap();
    assert!(
        not_a_fact.status().is_client_error(),
        "{}",
        not_a_fact.status()
    );
    assert_eq!(
        state(&base, "kills", "alice").await["state"]["mobs"],
        json!(["m1", "m3"])
    );

    // 5. celld down: facts stay pending, the zone is never blocked, the
    //    queue reports Backpressure at capacity.
    celld.crash();
    let outage = [
        ("alice", "a1", 10),
        ("bob", "b1", 11),
        ("alice", "a2", 12),
        ("bob", "b2", 13),
    ];
    for (principal, mob, tick) in outage {
        let started = Instant::now();
        queue
            .try_send(fact(
                &format!("kills.record:pit-1:1:{mob}"),
                &Kill { mob: mob.into() },
                Some(principal),
                tick,
            ))
            .unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "emitting never waits on celld"
        );
    }
    assert_eq!(
        queue.try_send(fact(
            "kills.record:pit-1:1:a3",
            &Kill { mob: "a3".into() },
            Some("alice"),
            14
        )),
        Err(EmitError::Backpressure)
    );
    assert!(
        settle(&mut queue, 1, Duration::from_secs(3))
            .await
            .is_empty(),
        "nothing settles while celld is down"
    );
    assert_eq!(queue.in_flight(), 4);

    //    celld back: the backlog drains, each principal in emission order.
    celld.start().expect("celld restarts");
    let drained = settle(&mut queue, 4, Duration::from_secs(60)).await;
    assert_eq!(drained.len(), 4, "{drained:?}");
    assert!(drained.iter().all(committed), "{drained:?}");
    assert_eq!(
        state(&base, "kills", "alice").await["state"]["mobs"],
        json!(["m1", "m3", "a1", "a2"])
    );
    assert_eq!(
        state(&base, "kills", "bob").await["state"]["mobs"],
        json!(["m2", "b1", "b2"])
    );
    let counts = drained
        .iter()
        .map(|settlement| settlement.key.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(counts.len(), 4);

    drop(queue);
    let _ = tokio::time::timeout(Duration::from_secs(5), worker).await;
}
