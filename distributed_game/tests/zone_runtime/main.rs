//! P2 zone runtime behaviour: lifecycle, cadence, tiers, inputs,
//! determinism and non-blocking facts.

mod arena;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arena::{Arena, ArenaConfig};
use distributed_game::protocol::{error, ServerMessage};
use distributed_game::{
    fact_channel, ClientId, ClientMirror, EmitError, Fact, FactError, FactOutcome, FactReceipt,
    FactSink, HostConfig, LeaveReason, ManualClock, MemoryFactSink, Outbound, PushOutcome,
    RetryPolicy, Role, SystemClock, TicketClaims, Tier, ZoneHost,
};

const MS: fn(u64) -> Duration = Duration::from_millis;

fn claims(principal: &str, zone: &str) -> TicketClaims {
    TicketClaims {
        principal: principal.into(),
        zone: zone.into(),
        expires_at: u64::MAX,
        nonce: format!("n-{principal}"),
        role: Role::Player,
    }
}

fn host(config: HostConfig, clock: &ManualClock, cfg: ArenaConfig) -> ZoneHost<Arena> {
    ZoneHost::new("z1", config, Arc::new(clock.clone()), cfg)
}

fn frames_for(out: &[Outbound], client: ClientId) -> Vec<Vec<u8>> {
    out.iter()
        .filter_map(|o| match o {
            Outbound::Frame { client: c, frame } if *c == client => Some(frame.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn lifecycle_hooks_capacity_and_auth() {
    let clock = ManualClock::new();
    let config = HostConfig {
        max_players: 2,
        ..HostConfig::default()
    };
    let mut h = host(
        config,
        &clock,
        ArenaConfig {
            reject_principal: Some("mallory".into()),
            spawn_every: Some(MS(100)),
            ..Default::default()
        },
    );
    let a = h.join(&claims("alice", "z1"), 0, "").unwrap();
    let err = h.join(&claims("mallory", "z1"), 0, "").unwrap_err();
    assert_eq!(err.code, error::REJECTED);
    let err = h.join(&claims("bob", "other-zone"), 0, "").unwrap_err();
    assert_eq!(err.code, error::UNAUTHORIZED);
    h.join(&claims("bob", "z1"), 0, "").unwrap();
    let err = h.join(&claims("carol", "z1"), 0, "").unwrap_err();
    assert_eq!(err.code, error::ZONE_FULL);

    // Welcome is the first frame for a joined client.
    let out = h.drain_outbound();
    let mut mirror = ClientMirror::new();
    let first = frames_for(&out, a.client).remove(0);
    assert!(matches!(
        mirror.apply(&first).unwrap(),
        ServerMessage::Welcome(_)
    ));

    clock.advance(MS(500));
    h.advance();
    assert!(h.zone().timer_fires > 0);
    h.disconnect(a.client, true);
    h.dispose();
    assert!(h.is_disposed());
    let fires = h.zone().timer_fires;
    clock.advance(MS(1_000));
    assert_eq!(h.advance().ticks, 0, "disposed zones never tick");
    assert_eq!(h.zone().timer_fires, fires, "timers cleared on dispose");
    assert_eq!(
        h.zone().log,
        vec![
            "create",
            "join:alice",
            "join:bob",
            "leave:Left",
            "leave:Disposed",
            "dispose"
        ]
    );
}

#[test]
fn fixed_timestep_caps_catch_up_and_patches_independently() {
    let clock = ManualClock::new();
    let config = HostConfig {
        tick_rate: 50,
        patch_rate: 10,
        max_catch_up: 5,
        ..HostConfig::default()
    };
    let mut h = host(config, &clock, ArenaConfig::default());
    h.join(&claims("alice", "z1"), 0, "").unwrap();
    h.advance();
    h.drain_outbound();

    // A 2s stall runs at most 5 steps and drops the rest.
    clock.advance(Duration::from_secs(2));
    let report = h.advance();
    assert_eq!(report.ticks, 5);
    assert!(report.dropped >= Duration::from_millis(1_900));

    // Steady state: 1s in 2 ms frames = 50 ticks, 10 patches.
    let (mut ticks, mut patches) = (0, 0);
    let start = h.tick();
    for _ in 0..500 {
        clock.advance(MS(2));
        let r = h.advance();
        ticks += r.ticks;
        patches += u32::from(r.patched);
    }
    assert_eq!(ticks, 50);
    assert_eq!(h.tick() - start, 50);
    assert!((9..=11).contains(&patches), "patches = {patches}");
}

#[test]
fn activity_tiers_hot_idle_dormant_and_wake() {
    let clock = ManualClock::new();
    let config = HostConfig {
        tick_rate: 20,
        idle_tick_rate: Some(2),
        dormant_after: Duration::from_secs(5),
        ..HostConfig::default()
    };
    let mut h = host(config.clone(), &clock, ArenaConfig::default());
    let a = h.join(&claims("alice", "z1"), 0, "").unwrap();
    assert_eq!(h.tier(), Tier::Hot);
    let mut ticks = 0;
    for _ in 0..20 {
        clock.advance(MS(50));
        ticks += h.advance().ticks;
    }
    assert_eq!(ticks, 20);

    h.disconnect(a.client, true);
    let r = h.advance();
    assert_eq!(h.tier(), Tier::Idle);
    assert_eq!(r.transitions, vec![(Tier::Hot, Tier::Idle)]);
    clock.advance(Duration::from_secs(2));
    assert_eq!(h.advance().ticks, 4, "idle runs the slow clock");

    clock.advance(Duration::from_secs(4));
    let r = h.advance();
    assert_eq!(h.tier(), Tier::Dormant);
    assert_eq!(r.transitions, vec![(Tier::Idle, Tier::Dormant)]);
    assert_eq!(r.next_wake, None);
    clock.advance(Duration::from_secs(10));
    assert_eq!(h.advance().ticks, 0, "dormant zones do not tick");

    h.join(&claims("bob", "z1"), 0, "").unwrap();
    assert_eq!(h.tier(), Tier::Hot);
    let r = h.advance();
    assert_eq!(r.transitions, vec![(Tier::Dormant, Tier::Hot)]);
    assert_eq!(r.ticks, 0, "no catch-up for dormant time");
    assert_eq!(
        h.zone().tiers,
        vec![
            (Tier::Hot, Tier::Idle),
            (Tier::Idle, Tier::Dormant),
            (Tier::Dormant, Tier::Hot)
        ]
    );

    // Background zones stay Idle instead of going Dormant.
    let mut bg = host(
        config,
        &clock,
        ArenaConfig {
            background: true,
            ..Default::default()
        },
    );
    bg.advance();
    clock.advance(Duration::from_secs(60));
    bg.advance();
    assert_eq!(bg.tier(), Tier::Idle);
}

#[test]
fn reconnect_within_grace_keeps_player_then_expires() {
    let clock = ManualClock::new();
    let config = HostConfig {
        reconnect_grace: Duration::from_secs(3),
        ..HostConfig::default()
    };
    let mut h = host(config, &clock, ArenaConfig::default());
    let a = h.join(&claims("alice", "z1"), 0, "").unwrap();
    let out = h.drain_outbound();
    let mut mirror = ClientMirror::new();
    mirror.apply(&frames_for(&out, a.client)[0]).unwrap();
    let token = mirror.welcome.as_ref().unwrap().reconnect_token.clone();
    assert!(!token.is_empty());

    h.disconnect(a.client, false);
    clock.advance(Duration::from_secs(1));
    h.advance();
    assert_eq!(h.player_count(), 1);
    let b = h.reconnect(&token, 0).unwrap();
    assert_eq!(b.viewer, a.viewer, "same player after reconnect");
    assert!(h.reconnect(&token, 0).is_err(), "tokens rotate on use");
    assert_eq!(h.zone().log, vec!["create", "join:alice"]);

    h.disconnect(b.client, false);
    clock.advance(Duration::from_secs(4));
    h.advance();
    assert_eq!(h.player_count(), 0);
    assert_eq!(h.zone().log.last().unwrap(), "leave:Disconnected");
    let _ = LeaveReason::Disconnected;
}

#[test]
fn inputs_are_sanitized_deduped_and_acked_in_deltas() {
    let clock = ManualClock::new();
    let mut h = host(HostConfig::default(), &clock, ArenaConfig::default());
    let a = h.join(&claims("alice", "z1"), 0, "").unwrap();
    assert_eq!(h.push_input(a.client, 1, 0, &[1, 0]), PushOutcome::Accepted);
    assert_eq!(
        h.push_input(a.client, 1, 0, &[1, 0]),
        PushOutcome::Duplicate
    );
    assert_eq!(h.push_input(a.client, 2, 0, &[9, 0]), PushOutcome::Rejected);
    assert_eq!(h.push_input(a.client, 3, 0, &[1]), PushOutcome::Rejected);
    assert_eq!(h.push_input(a.client, 4, 0, &[0, 1]), PushOutcome::Accepted);
    assert_eq!(
        h.push_input(ClientId(999), 1, 0, &[0, 1]),
        PushOutcome::UnknownClient
    );

    clock.advance(MS(100));
    let r = h.advance();
    assert!(r.ticks > 0 && r.patched);
    assert_eq!(h.push_input(a.client, 4, 0, &[0, 1]), PushOutcome::Stale);
    let mut mirror = ClientMirror::new();
    for frame in frames_for(&h.drain_outbound(), a.client) {
        mirror.apply(&frame).unwrap();
    }
    assert_eq!(mirror.ack_seq, 4);
    assert_eq!(mirror.entities.len(), 1, "own avatar replicated");
}

fn run_scripted(seed: u64) -> (u64, u64) {
    let clock = ManualClock::new();
    let config = HostConfig {
        world_seed: seed,
        ..HostConfig::default()
    };
    let (queue, _feed) = fact_channel(10_000, RetryPolicy::default());
    let mut h = host(
        config,
        &clock,
        ArenaConfig {
            spawn_every: Some(MS(200)),
            ..Default::default()
        },
    )
    .with_facts(queue);
    let a = h.join(&claims("alice", "z1"), 0, "").unwrap();
    let b = h.join(&claims("bob", "z1"), 0, "").unwrap();
    let mut seq = 0;
    for step in 0..600u32 {
        if step % 3 == 0 {
            seq += 1;
            let dx = ((step / 7) % 3) as i8 - 1;
            let dy = ((step / 11) % 3) as i8 - 1;
            h.push_input(a.client, seq, step, &[dx as u8, dy as u8]);
            h.push_input(b.client, seq, step, &[dy as u8, dx as u8]);
        }
        clock.advance(MS(7));
        h.advance();
        h.drain_outbound();
    }
    (h.zone().state_hash(), h.tick())
}

#[test]
fn same_seed_and_input_log_reproduce_state() {
    let first = run_scripted(42);
    let second = run_scripted(42);
    assert_eq!(first, second);
    assert_ne!(run_scripted(7).0, first.0, "different seed diverges");
}

/// Sink that takes `delay` per commit (simulated slow storage).
struct SlowSink {
    delay: Duration,
    inner: MemoryFactSink,
}

impl FactSink for SlowSink {
    fn commit<'a>(
        &'a self,
        fact: &'a Fact,
    ) -> distributed_game::facts::BoxFuture<'a, Result<FactReceipt, FactError>> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            self.inner.commit(fact).await
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_sink_never_stalls_the_tick() {
    let sink = MemoryFactSink::new();
    let (queue, feed) = fact_channel(10_000, RetryPolicy::default());
    tokio::spawn(feed.run(
        SlowSink {
            delay: MS(100),
            inner: sink.clone(),
        },
        tokio::time::sleep,
    ));
    let config = HostConfig {
        tick_rate: 50,
        ..HostConfig::default()
    };
    let clock = Arc::new(SystemClock::new());
    let mut h = ZoneHost::<Arena>::new(
        "z1",
        config,
        clock,
        ArenaConfig {
            fact_every_tick: true,
            ..Default::default()
        },
    )
    .with_facts(queue);
    h.join(&claims("alice", "z1"), 0, "").unwrap();

    let start = Instant::now();
    let mut worst = Duration::ZERO;
    let mut ticks = 0;
    while start.elapsed() < MS(600) {
        let t0 = Instant::now();
        let r = h.advance();
        worst = worst.max(t0.elapsed());
        ticks += r.ticks;
        h.drain_outbound();
        tokio::time::sleep(MS(5)).await;
    }
    // 600 ms at 50 Hz = 30 ticks; storage at 10 commits/s must not slow it.
    assert!(ticks >= 27, "ticks = {ticks}");
    assert!(worst < MS(25), "an advance took {worst:?}");
    assert!(
        h.facts_in_flight() > 10,
        "facts are still queued behind slow storage"
    );
    assert!(h.zone().emit_errors.is_empty());
    let settled = h.zone().settled.len();
    assert!(
        settled >= 2,
        "settlements flow back on later ticks ({settled})"
    );
    assert!(h
        .zone()
        .settled
        .iter()
        .all(|s| matches!(s.outcome, FactOutcome::Committed(_))));
    assert!(sink.committed().len() >= settled);
}

struct FlakySink {
    failures_left: AtomicU32,
    permanent_key: String,
}

impl FactSink for FlakySink {
    fn commit<'a>(
        &'a self,
        fact: &'a Fact,
    ) -> distributed_game::facts::BoxFuture<'a, Result<FactReceipt, FactError>> {
        Box::pin(async move {
            if fact.key() == self.permanent_key {
                return Err(FactError::Permanent("no such player".into()));
            }
            if self.failures_left.load(Ordering::SeqCst) > 0 {
                self.failures_left.fetch_sub(1, Ordering::SeqCst);
                return Err(FactError::Retryable("busy".into()));
            }
            Ok(FactReceipt {
                payload: serde_json::json!({ "ok": fact.key() }),
                replayed: false,
            })
        })
    }
}

#[tokio::test]
async fn retries_backoff_permanent_failure_backpressure_and_principal_binding() {
    let (mut queue, feed) = fact_channel(
        2,
        RetryPolicy {
            max_attempts: 5,
            base_backoff: MS(1),
            max_backoff: MS(4),
        },
    );
    let worker = tokio::spawn(feed.run(
        FlakySink {
            failures_left: AtomicU32::new(2),
            permanent_key: "bad".into(),
        },
        tokio::time::sleep,
    ));
    queue
        .try_send(Fact::new("good", "credit_xp", serde_json::json!({})))
        .unwrap();
    assert_eq!(
        queue.try_send(Fact::new("good", "credit_xp", serde_json::json!({}))),
        Err(EmitError::DuplicateKey)
    );
    queue
        .try_send(Fact::new("bad", "credit_xp", serde_json::json!({})))
        .unwrap();
    assert_eq!(
        queue.try_send(Fact::new("third", "credit_xp", serde_json::json!({}))),
        Err(EmitError::Backpressure)
    );
    let mut settled = Vec::new();
    while settled.len() < 2 {
        settled.extend(queue.drain());
        tokio::time::sleep(MS(2)).await;
    }
    assert!(matches!(settled[0].outcome, FactOutcome::Committed(_)));
    assert!(matches!(
        settled[1].outcome,
        FactOutcome::Failed {
            error: FactError::Permanent(_),
            attempts: 1
        }
    ));
    assert_eq!(queue.in_flight(), 0);
    drop(queue);
    worker.await.unwrap();

    // Principal and stamp are bound by the host from the verified ticket.
    let clock = ManualClock::new();
    let sink = MemoryFactSink::new();
    let (queue, feed) = fact_channel(100, RetryPolicy::default());
    let worker = tokio::spawn(feed.run(sink.clone(), tokio::time::sleep));
    let mut h = host(
        HostConfig {
            content_version: "c1".into(),
            ..HostConfig::default()
        },
        &clock,
        ArenaConfig::default(),
    )
    .with_facts(queue);
    let a = h.join(&claims("alice", "z1"), 0, "c1").unwrap();
    let player = match a.viewer {
        distributed_game::Viewer::Player(p) => p,
        _ => unreachable!(),
    };
    let (x, y) = {
        let av = &h.zone().avatars[&player];
        (av.x, av.y)
    };
    h.zone_mut().coins.insert(77, (x, y));
    clock.advance(MS(40));
    h.advance();
    let tick = h.tick();
    drop(h);
    worker.await.unwrap();
    let committed = sink.committed();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].principal(), Some("alice"));
    assert_eq!(committed[0].key(), "coin:z1:77");
    assert_eq!(committed[0].stamp().zone_id, "z1");
    assert_eq!(committed[0].stamp().content_version, "c1");
    assert!(committed[0].stamp().tick >= 1 && committed[0].stamp().tick <= tick);
}

#[test]
fn observers_receive_deltas_without_owning_a_player() {
    let clock = ManualClock::new();
    let mut h = host(HostConfig::default(), &clock, ArenaConfig::default());
    h.join(&claims("alice", "z1"), 0, "").unwrap();
    let mut obs_claims = claims("watcher", "z1");
    obs_claims.role = Role::Observer;
    let o = h.join(&obs_claims, 0, "").unwrap();
    assert_eq!(h.player_count(), 1);
    assert_eq!(
        h.push_input(o.client, 1, 0, &[1, 0]),
        PushOutcome::UnknownClient
    );
    clock.advance(MS(100));
    h.advance();
    let mut mirror = ClientMirror::new();
    for frame in frames_for(&h.drain_outbound(), o.client) {
        mirror.apply(&frame).unwrap();
    }
    assert_eq!(mirror.welcome.as_ref().unwrap().role, Role::Observer);
    assert_eq!(mirror.entities.len(), 1);
    h.disconnect(o.client, false);
    assert!(h.zone().log.contains(&"unobserve".to_string()));
}

#[test]
fn same_principal_replaces_previous_connection() {
    let clock = ManualClock::new();
    let mut h = host(HostConfig::default(), &clock, ArenaConfig::default());
    let a = h.join(&claims("alice", "z1"), 0, "").unwrap();
    let b = h.join(&claims("alice", "z1"), 0, "").unwrap();
    assert_eq!(a.viewer, b.viewer);
    assert_eq!(h.client_count(), 1);
    let out = h.drain_outbound();
    assert!(out.contains(&Outbound::Close { client: a.client }));
    assert_eq!(h.zone().log, vec!["create", "join:alice"]);
}
