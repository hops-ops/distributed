//! Host-emitted player lifecycle facts (`HostConfig::lifecycle_facts`):
//! the five transitions, restart-safe keys, per-principal order under
//! concurrent commits, backpressure backlog and the connection hooks.

#[path = "../zone_runtime/arena.rs"]
mod arena;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arena::{Arena, ArenaConfig};
use distributed_game::lifecycle::{
    JoinVia, LeftReason, Lifecycle, LostCause, CONNECTION_LOST, PLAYER_DISCONNECTED, PLAYER_JOINED,
    PLAYER_RECONNECTED,
};
use distributed_game::protocol::ServerMessage;
use distributed_game::{
    fact_channel, spawn_fact_worker, BoxFuture, ClientId, Fact, FactError, FactFeed, FactReceipt,
    FactSink, HostConfig, ManualClock, MemoryFactSink, Outbound, RetryPolicy, Role, TicketClaims,
    ZoneHost,
};

const MS: fn(u64) -> Duration = Duration::from_millis;

fn claims(principal: &str, nonce: &str) -> TicketClaims {
    TicketClaims::new(principal, "z1", u64::MAX, nonce, Role::Player)
}

fn config(boot_epoch: u64) -> HostConfig {
    HostConfig {
        lifecycle_facts: true,
        boot_epoch,
        reconnect_grace: MS(500),
        ..HostConfig::default()
    }
}

fn token(out: &[Outbound], client: ClientId) -> String {
    out.iter()
        .find_map(|o| match o {
            Outbound::Frame { client: c, frame } if *c == client => {
                match ServerMessage::decode(frame, &()).ok()? {
                    ServerMessage::Welcome(w) => Some(w.reconnect_token),
                    _ => None,
                }
            }
            _ => None,
        })
        .expect("welcome")
}

/// Drive a whole scripted session and return the committed facts.
fn run_session(boot_epoch: u64) -> (Vec<Fact>, Vec<String>) {
    let clock = ManualClock::new();
    let (queue, feed) = fact_channel(64, RetryPolicy::default());
    let mut h = ZoneHost::<Arena>::new(
        "z1",
        config(boot_epoch),
        Arc::new(clock.clone()),
        ArenaConfig::default(),
    )
    .with_facts(queue);

    // ana: join, drop, reconnect, reload the page (fresh ticket), drop,
    // grace expires.
    let ana = h.join(&claims("ana", "a1"), 0, "").unwrap();
    let ana_token = token(&h.drain_outbound(), ana.client);
    h.disconnect(ana.client, false);
    let back = h.reconnect(&ana_token, 0).unwrap();
    h.drain_outbound();
    let again = h.join(&claims("ana", "a2"), 0, "").unwrap();
    assert_ne!(back.client, again.client);
    h.disconnect(again.client, false);
    clock.advance(MS(600));
    h.advance();
    // bo: kicked. cy: leaves gracefully. dee: still here at dispose.
    let bo = h.join(&claims("bo", "b1"), 0, "").unwrap();
    let Some(distributed_game::Viewer::Player(bo_id)) = h.viewer_of(bo.client) else {
        panic!("bo is a player")
    };
    h.kick(bo_id, "cheating");
    let cy = h.join(&claims("cy", "c1"), 0, "").unwrap();
    h.disconnect(cy.client, true);
    h.join(&claims("dee", "d1"), 0, "").unwrap();
    h.dispose();
    let hooks = h.zone().connection_events.clone();
    drop(h);
    let sink = MemoryFactSink::new();
    block_on(feed.run(sink.clone(), |_| async {}));
    (sink.committed(), hooks)
}

fn block_on(f: impl std::future::Future<Output = ()>) {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let mut f = std::pin::pin!(f);
    while f.as_mut().poll(&mut cx).is_pending() {}
}

fn of(facts: &[Fact], principal: &str) -> Vec<Lifecycle> {
    facts
        .iter()
        .filter(|f| f.principal() == Some(principal))
        .map(|f| Lifecycle::of(f).expect("only lifecycle facts"))
        .collect()
}

#[test]
fn the_host_emits_every_transition_bound_to_the_verified_principal() {
    let (facts, hooks) = run_session(7);
    assert!(facts.iter().all(|f| f.stamp().zone_id == "z1"));
    let ana = of(&facts, "ana");
    let commands: Vec<_> = facts
        .iter()
        .filter(|f| f.principal() == Some("ana"))
        .map(Fact::command)
        .collect();
    assert_eq!(
        commands,
        [
            PLAYER_JOINED,
            CONNECTION_LOST,
            PLAYER_RECONNECTED,
            PLAYER_JOINED,
            CONNECTION_LOST,
            PLAYER_DISCONNECTED
        ]
    );
    let Lifecycle::Joined(first) = &ana[0] else {
        panic!()
    };
    assert_eq!(first.via, JoinVia::Ticket);
    let Lifecycle::ConnectionLost(lost) = &ana[1] else {
        panic!()
    };
    assert_eq!((lost.grace_ms, lost.cause), (500, LostCause::Dropped));
    let Lifecycle::Joined(rejoin) = &ana[3] else {
        panic!()
    };
    assert_eq!(rejoin.via, JoinVia::Rejoin);
    assert!(ana[5].is_terminal());

    let reasons: Vec<_> = ["bo", "cy", "dee"]
        .iter()
        .map(|p| match of(&facts, p).as_slice() {
            [Lifecycle::Joined(_), Lifecycle::Left(left)] => left.reason,
            other => panic!("{p}: {other:?}"),
        })
        .collect();
    assert_eq!(
        reasons,
        [LeftReason::Kicked, LeftReason::Left, LeftReason::Disposed]
    );

    // Keys: command, zone, boot epoch, slot, transition counter (ana's
    // slot is PlayerId(2): ids are shared with connection ids).
    let keys: Vec<_> = facts
        .iter()
        .filter(|f| f.principal() == Some("ana"))
        .map(|f| f.key().to_string())
        .collect();
    assert_eq!(keys[0], format!("{PLAYER_JOINED}:z1:7:p2:1"));
    assert_eq!(keys[5], format!("{PLAYER_DISCONNECTED}:z1:7:p2:6"));
    // The payload names no identity; the principal is the binding.
    assert!(facts.iter().all(|f| f.input().get("principal").is_none()));

    // Connection hooks: grace start and reconnect.
    assert_eq!(hooks, ["lost:2", "back:2", "lost:2"]);
}

#[test]
fn keys_are_stable_per_run_and_distinct_after_a_restart() {
    let (first, _) = run_session(100);
    let (same, _) = run_session(100);
    let (restarted, _) = run_session(101);
    let keys = |facts: &[Fact]| {
        facts
            .iter()
            .map(|f| f.key().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(keys(&first), keys(&same), "deterministic per activation");
    let before: std::collections::HashSet<_> = keys(&first).into_iter().collect();
    assert_eq!(before.len(), first.len(), "unique within a run");
    assert!(
        keys(&restarted).iter().all(|k| !before.contains(k)),
        "a restart never reuses a key"
    );
}

#[test]
fn off_by_default() {
    let clock = ManualClock::new();
    let (queue, feed) = fact_channel(16, RetryPolicy::default());
    let mut h = ZoneHost::<Arena>::new(
        "z1",
        HostConfig::default(),
        Arc::new(clock.clone()),
        ArenaConfig::default(),
    )
    .with_facts(queue);
    let ana = h.join(&claims("ana", "a1"), 0, "").unwrap();
    h.disconnect(ana.client, false);
    let bo = h.join(&claims("bo", "b1"), 0, "").unwrap();
    h.disconnect(bo.client, true);
    assert_eq!(h.zone().connection_events, ["lost:2"], "hooks still run");
    drop(h);
    let sink = MemoryFactSink::new();
    block_on(feed.run(sink.clone(), |_| async {}));
    assert!(sink.committed().is_empty());
}

/// Commits slowly for "joined" (so a later fact of the same principal would
/// overtake it if lanes were not respected) and fails the first attempt of
/// every reconnect (retries keep the key).
#[derive(Clone, Default)]
struct SlowSink {
    order: Arc<Mutex<Vec<(String, String, String)>>>,
    attempts: Arc<Mutex<BTreeMap<String, u32>>>,
}

impl FactSink for SlowSink {
    fn commit<'a>(&'a self, fact: &'a Fact) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
        Box::pin(async move {
            let attempt = {
                let mut attempts = self.attempts.lock().unwrap();
                let n = attempts.entry(fact.key().to_string()).or_default();
                *n += 1;
                *n
            };
            let delay = match fact.command() {
                PLAYER_JOINED => 40,
                CONNECTION_LOST => 5,
                _ => 1,
            };
            tokio::time::sleep(MS(delay)).await;
            if fact.command() == PLAYER_RECONNECTED && attempt == 1 {
                return Err(FactError::Retryable("flaky".into()));
            }
            self.order.lock().unwrap().push((
                fact.principal().unwrap_or_default().to_string(),
                fact.command().to_string(),
                fact.key().to_string(),
            ));
            Ok(FactReceipt {
                payload: serde_json::Value::Null,
                replayed: false,
            })
        })
    }
}

fn concurrent(feed: FactFeed) -> FactFeed {
    feed.with_max_concurrent_commits(4)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_principal_order_holds_with_concurrent_commits_and_backpressure() {
    let clock = ManualClock::new();
    // Capacity 2: most lifecycle facts wait in the host's backlog.
    let policy = RetryPolicy {
        base_backoff: MS(2),
        ..RetryPolicy::default()
    };
    let (queue, feed) = fact_channel(2, policy);
    let sink = SlowSink::default();
    let worker = spawn_fact_worker(concurrent(feed), sink.clone());
    let mut h = ZoneHost::<Arena>::new(
        "z1",
        config(9),
        Arc::new(clock.clone()),
        ArenaConfig::default(),
    )
    .with_facts(queue);

    let mut emitted: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for principal in ["ana", "bo", "cy"] {
        let joined = h.join(&claims(principal, principal), 0, "").unwrap();
        let token = token(&h.drain_outbound(), joined.client);
        h.disconnect(joined.client, false);
        h.reconnect(&token, 0).unwrap();
        h.drain_outbound();
        emitted.insert(
            principal.into(),
            vec![PLAYER_JOINED, CONNECTION_LOST, PLAYER_RECONNECTED],
        );
    }
    assert!(h.facts_in_flight() > 2, "backlog beyond the queue capacity");
    // The host keeps flushing its backlog as the queue drains.
    for _ in 0..400 {
        if h.facts_in_flight() == 0 {
            break;
        }
        tokio::time::sleep(MS(5)).await;
        clock.advance(MS(5));
        h.advance();
    }
    assert_eq!(h.facts_in_flight(), 0, "every lifecycle fact settled");
    drop(h);
    worker.await.unwrap();

    let order = sink.order.lock().unwrap().clone();
    assert_eq!(order.len(), 9, "nothing dropped: {order:?}");
    for (principal, expected) in &emitted {
        let committed: Vec<_> = order
            .iter()
            .filter(|(p, _, _)| p == principal)
            .map(|(_, command, _)| command.as_str())
            .collect();
        assert_eq!(&committed, expected, "{principal}");
    }
    // The flaky reconnects were retried under the same key.
    let attempts = sink.attempts.lock().unwrap();
    for (_, command, key) in order.iter().filter(|(_, c, _)| c == PLAYER_RECONNECTED) {
        assert_eq!(attempts[key], 2, "{command} retried once with key {key}");
    }
}

#[test]
fn handoff_and_reload_are_lifecycle_transitions_too() {
    let clock = ManualClock::new();
    let (queue, feed) = fact_channel(16, RetryPolicy::default());
    let mut h = ZoneHost::<Arena>::new(
        "z1",
        HostConfig {
            content_version: "v1".into(),
            ..config(3)
        },
        Arc::new(clock.clone()),
        ArenaConfig::default(),
    )
    .with_facts(queue);
    let transfer = distributed_game::Transfer {
        claims: claims("ana", "t1"),
        source_zone: "z0".into(),
        issuer: "h0".into(),
        generation: 1,
        carried: Vec::new(),
        departed_at_ms: 0,
    };
    let joined = h.join_transfer(&transfer, 0, "v1").unwrap();
    let Some(distributed_game::Viewer::Player(ana)) = h.viewer_of(joined.client) else {
        panic!()
    };
    h.reload_player(ana);
    h.handoff(ana, "z2", "ticket");
    drop(h);
    let sink = MemoryFactSink::new();
    block_on(feed.run(sink.clone(), |_| async {}));
    match of(&sink.committed(), "ana").as_slice() {
        [Lifecycle::Joined(j), Lifecycle::ConnectionLost(l), Lifecycle::Left(left)] => {
            assert_eq!(
                (j.via, j.from_zone.as_deref()),
                (JoinVia::Handoff, Some("z0"))
            );
            assert_eq!(l.cause, LostCause::Reload);
            assert_eq!(left.reason, LeftReason::Handoff);
        }
        other => panic!("{other:?}"),
    }
}
