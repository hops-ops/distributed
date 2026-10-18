//! P8 netcode: lag compensation through `Ctx::rewind`, and deterministic
//! record/replay with divergence detection.

#[path = "../zone_runtime/arena.rs"]
mod arena;

use std::sync::Arc;
use std::time::Duration;

use arena::{Arena, ArenaConfig};
use distributed_game::replay::{replay, DivergenceKind, Event, JoinResult, Recorder, Recording};
use distributed_game::rewind::{Historic, HitQuery, RewindConfig, RewindFrame, ShotTiming};
use distributed_game::{
    fact_channel, spawn_fact_worker, ClientMirror, ConsumePolicy, Ctx, EntitySchema, EntityState,
    FieldType, FieldValue, HostConfig, KindDef, ManualClock, MemoryFactSink, Outbound, PlayerId,
    RetryPolicy, Role, ServerMessage, TicketClaims, WorldView, Zone, ZoneHost, ZoneRng,
};

fn claims(principal: &str, zone: &str) -> TicketClaims {
    TicketClaims {
        principal: principal.into(),
        zone: zone.into(),
        expires_at: u64::MAX,
        nonce: format!("n-{principal}"),
        role: Role::Player,
    }
}

// ---------------------------------------------------------------- rewind

const TARGET: u32 = 100;
const SPEED: f32 = 10.0;

#[derive(Debug, Clone, PartialEq)]
struct ShotResult {
    rewound: Option<u32>,
    present: Option<u32>,
    clamped: bool,
    age: Duration,
}

/// A target crosses the map along y = 10 at 10 u/s; players shoot straight
/// up (+y) from (aim_x, 0) and report their interpolation delay in the
/// payload: `aim_x: f32 LE ‖ interp_ms: u16 LE`.
struct Range {
    target_x: f32,
    shots: Vec<ShotResult>,
}

impl Zone for Range {
    type Config = ();
    type Input = (f32, u16);
    type Timer = ();

    fn schema() -> EntitySchema {
        EntitySchema::new().kind(KindDef::new(1, "target").field("x", FieldType::F32))
    }

    fn on_create(_ctx: &mut Ctx<'_, Self>, _: ()) -> Self {
        Range {
            target_x: 0.0,
            shots: Vec::new(),
        }
    }

    fn decode_input(&self, _p: PlayerId, b: &[u8]) -> Option<(f32, u16)> {
        let b: &[u8; 6] = b.try_into().ok()?;
        Some((
            f32::from_le_bytes([b[0], b[1], b[2], b[3]]),
            u16::from_le_bytes([b[4], b[5]]),
        ))
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        let players: Vec<_> = ctx.players().map(|p| p.id).collect();
        for p in players {
            for input in ctx.take_inputs(p, ConsumePolicy::Drain) {
                let (aim_x, interp_ms) = input.input;
                let timing = ShotTiming {
                    client_tick: input.client_tick,
                    interp_delay: Duration::from_millis(u64::from(interp_ms)),
                };
                let ray = HitQuery::ray([aim_x, 0.0, 0.0], [0.0, 1.0, 0.0], 50.0);
                let rewind = ctx.rewind();
                let resolved = rewind.resolve(timing).expect("history");
                let present = rewind.latest().unwrap().time();
                self.shots.push(ShotResult {
                    rewound: rewind.hit_test(resolved.time, &ray, |_| true).map(|h| h.id),
                    present: rewind.hit_test(present, &ray, |_| true).map(|h| h.id),
                    clamped: resolved.clamped,
                    age: resolved.age,
                });
            }
        }
        self.target_x += SPEED * ctx.dt_secs();
    }

    fn record_rewind(&self, frame: &mut RewindFrame) {
        frame.push(Historic::sphere(TARGET, [self.target_x, 10.0, 0.0], 0.5));
    }

    fn replicate(&self, out: &mut WorldView) {
        out.push(EntityState::new(
            TARGET,
            1,
            self.target_x,
            10.0,
            vec![FieldValue::F(self.target_x)],
        ));
    }
}

fn shot(aim_x: f32, interp_ms: u16) -> Vec<u8> {
    let mut b = aim_x.to_le_bytes().to_vec();
    b.extend_from_slice(&interp_ms.to_le_bytes());
    b
}

#[test]
fn ctx_rewind_hits_at_view_time_and_clamps_cheats() {
    let clock = ManualClock::new();
    let config = HostConfig {
        rewind: RewindConfig {
            max_rewind: Duration::from_millis(300),
            max_frames: 128,
        },
        ..HostConfig::default()
    };
    let mut h = ZoneHost::<Range>::new("range", config, Arc::new(clock.clone()), ());
    let joined = h.join(&claims("sniper", "range"), 0, "").unwrap();
    let step = Duration::from_secs(1) / 30;
    for _ in 0..90 {
        clock.advance(step);
        h.advance();
    }
    let latest = h.rewind().latest().unwrap().clone();
    assert!(h.rewind().len() <= h.rewind().capacity());

    // The client was rendering tick latest-4 with a 100 ms interpolation
    // delay and aimed exactly at the target as drawn (~233 ms in the past).
    let client_tick = (latest.tick() - 4) as u32;
    let view = h.rewind().time_of(client_tick).unwrap() - Duration::from_millis(100);
    let seen_x = h.rewind().entity_at(TARGET, view).unwrap().pos[0];
    h.push_input(joined.client, 1, client_tick, &shot(seen_x, 100));

    // A cheater claims a 2 s interpolation delay to hit an old position.
    let old_x = (latest.time() - Duration::from_secs(2)).as_secs_f32() * SPEED;
    h.push_input(joined.client, 2, latest.tick() as u32, &shot(old_x, 2000));

    clock.advance(step);
    h.advance();
    let shots = &h.zone().shots;
    assert_eq!(shots.len(), 2);
    assert_eq!(
        shots[0].rewound,
        Some(TARGET),
        "laggy shot hits at view time"
    );
    assert_eq!(shots[0].present, None, "same shot misses at present time");
    assert!(!shots[0].clamped);
    assert!(shots[0].age > Duration::from_millis(200) && shots[0].age < Duration::from_millis(300));
    assert!(shots[1].clamped, "2 s rewind is clamped");
    assert_eq!(shots[1].age, Duration::from_millis(300));
    assert_eq!(
        shots[1].rewound, None,
        "clamped rewind cannot reach 2 s ago"
    );
}

// ---------------------------------------------------------------- replay

struct Session {
    recording: Recording,
    /// Final `ZoneHost::state_hash` of the live run.
    final_hash: u64,
}

fn reconnect_token(out: &[Outbound], client: u32) -> Option<String> {
    out.iter().rev().find_map(|o| match o {
        Outbound::Frame { client: c, frame } if c.0 == client => {
            match ClientMirror::new().apply(frame) {
                Ok(ServerMessage::Welcome(w)) => Some(w.reconnect_token),
                _ => None,
            }
        }
        _ => None,
    })
}

/// Four players move randomly over uneven clock steps; one drops and
/// reconnects with its token, one leaves; heartbeat facts settle through a
/// real async worker.
async fn record_session() -> Session {
    let clock = ManualClock::new();
    let config = HostConfig {
        world_seed: 0xdead_beef,
        ..HostConfig::default()
    };
    let cfg = ArenaConfig {
        fact_every_tick: true,
        spawn_every: Some(Duration::from_millis(150)),
        ..ArenaConfig::default()
    };
    let (queue, feed) = fact_channel(64, RetryPolicy::default());
    spawn_fact_worker(feed, MemoryFactSink::new());
    let mut rec = Recorder::<Arena>::new("arena-1", config, Arc::new(clock.clone()), cfg, vec![])
        .with_facts(queue, 64);
    let mut rng = ZoneRng::from_seed(42);
    let mut clients = Vec::new();
    let mut seqs = [0u32; 4];
    let mut token = None;
    for step in 0..120u32 {
        if step < 4 * 5 && step % 5 == 0 {
            let i = (step / 5) as usize;
            let joined = rec
                .join(&claims(&format!("p{i}"), "arena-1"), 0, "")
                .unwrap();
            clients.push(joined.client);
            let out = rec.drain_outbound();
            if i == 2 {
                token = reconnect_token(&out, joined.client.0);
            }
        }
        if step == 40 {
            rec.disconnect(clients[2], false);
        }
        if step == 55 {
            let joined = rec.reconnect(token.as_deref().unwrap(), 0).unwrap();
            clients[2] = joined.client;
        }
        if step == 90 {
            rec.disconnect(clients[3], true);
        }
        for (i, client) in clients.iter().enumerate() {
            if (step == 90 && i == 3) || step > 90 && i == 3 || (40..55).contains(&step) && i == 2 {
                continue;
            }
            if rng.chance(0.7) {
                seqs[i] += 1;
                let dx = rng.range_u32(0, 3) as i8 - 1;
                let dy = rng.range_u32(0, 3) as i8 - 1;
                rec.push_input(*client, seqs[i], step, &[dx as u8, dy as u8]);
            }
        }
        clock.advance(Duration::from_millis(u64::from(rng.range_u32(10, 70))));
        rec.advance();
        rec.drain_outbound();
        if step % 7 == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    // Let outstanding facts settle, then deliver them.
    tokio::time::sleep(Duration::from_millis(20)).await;
    clock.advance(Duration::from_millis(40));
    rec.advance();
    let final_hash = rec.host().state_hash();
    Session {
        recording: rec.finish(),
        final_hash,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recorded_session_replays_identically() {
    let session = record_session().await;
    let rec = &session.recording;
    let settled: usize = rec
        .events
        .iter()
        .map(|e| match e {
            Event::Advance { settled, .. } => settled.len(),
            _ => 0,
        })
        .sum();
    assert!(settled > 0, "facts settled during the recording");
    assert!(rec.events.iter().any(|e| matches!(
        e,
        Event::Reconnect {
            result: JoinResult::Joined(_),
            ..
        }
    )));
    assert!(rec.tick_count() > 120, "{}", rec.tick_count());

    // Compact file round trip.
    let bytes = rec.encode();
    let path = std::env::temp_dir().join(format!("dg-replay-{}.dgr", std::process::id()));
    rec.save(&path).unwrap();
    let loaded = Recording::load(&path).unwrap();
    std::fs::remove_file(&path).ok();
    assert_eq!(&loaded, rec);
    let per_tick = bytes.len() as f64 / rec.tick_count() as f64;
    // ~3 inputs, one heartbeat settlement and an 8-byte hash per tick.
    assert!(per_tick < 96.0, "{} bytes, {per_tick:.1}/tick", bytes.len());

    let cfg = ArenaConfig {
        fact_every_tick: true,
        spawn_every: Some(Duration::from_millis(150)),
        ..ArenaConfig::default()
    };
    let summary = replay::<Arena>(&loaded, cfg).unwrap_or_else(|d| panic!("{d}"));
    assert_eq!(summary.ticks as usize, rec.tick_count());
    assert_eq!(summary.final_hash, session.final_hash);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutated_input_diverges_at_the_consuming_tick() {
    let mut rec = record_session().await.recording;
    // Flip the x direction of a mid-session input from p0 (never drops).
    let p0 = match &rec.events[0] {
        Event::Join {
            result: JoinResult::Joined(c),
            ..
        } => *c,
        other => panic!("{other:?}"),
    };
    let (index, seq) = rec
        .events
        .iter()
        .enumerate()
        .skip(rec.events.len() / 2)
        .find_map(|(i, e)| match e {
            Event::Input {
                client,
                payload,
                seq,
                ..
            } if *client == p0 && payload[0] != 0 => Some((i, *seq)),
            _ => None,
        })
        .expect("an input to mutate");
    if let Event::Input { payload, .. } = &mut rec.events[index] {
        payload[0] = (-(payload[0] as i8)) as u8;
    }
    let consuming_tick = rec.events[index..]
        .iter()
        .find_map(|e| match e {
            Event::Advance { hashes, .. } => hashes.first().map(|(t, _)| *t),
            _ => None,
        })
        .unwrap();

    let cfg = ArenaConfig {
        fact_every_tick: true,
        spawn_every: Some(Duration::from_millis(150)),
        ..ArenaConfig::default()
    };
    let divergence = replay::<Arena>(&rec, cfg).expect_err("mutation must be detected");
    assert_eq!(divergence.tick, consuming_tick);
    assert_eq!(divergence.last_good_tick, consuming_tick - 1);
    assert!(matches!(divergence.kind, DivergenceKind::StateHash { .. }));
    let report = divergence.to_string();
    println!("{report}");
    assert!(
        report.contains(&format!("diverged at tick {consuming_tick}")),
        "{report}"
    );
    assert!(
        report.contains(&format!("client {p0} seq {seq} ")),
        "report names the suspect input:\n{report}"
    );
}
