//! The room zone on the reference host (`ZoneHost`, manual clock): what a
//! celld room object runs, without celld.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed_game::protocol::ServerMessage;
use distributed_game::{
    ClientId, FactQueue, GameEvent, HandoffRequest, HostConfig, ManualClock, Outbound, Role,
    TicketClaims, Transfer, Viewer, ZoneHost,
};
use mobpit_rooms::{encode_bot_input, MobPit, PitEvent, PitInput, RoomConfig, DEFAULT_SEED};

type Issued = Arc<Mutex<Option<HandoffRequest>>>;

fn host(zone: &str, clock: &ManualClock) -> ZoneHost<MobPit> {
    host_issuing(zone, clock, Issued::default())
}

fn host_issuing(zone: &str, clock: &ManualClock, issued: Issued) -> ZoneHost<MobPit> {
    let (facts, _script) = FactQueue::scripted(64);
    ZoneHost::<MobPit>::new(
        zone,
        HostConfig {
            boot_epoch: 7,
            ..HostConfig::default()
        },
        Arc::new(clock.clone()),
        RoomConfig::for_zone(zone, DEFAULT_SEED).expect("room"),
    )
    .with_facts(facts)
    .with_transfer_issuer(move |request| {
        *issued.lock().expect("issued") = Some(request.clone());
        Some(format!("transfer:{}", request.target_zone))
    })
}

fn join(host: &mut ZoneHost<MobPit>, who: &str) -> (ClientId, distributed_game::PlayerId) {
    let claims = TicketClaims::new(who, host.zone_id(), u64::MAX, who, Role::Player).with_name(who);
    let joined = host.join(&claims, 0, "").expect("join");
    let Viewer::Player(player) = joined.viewer else {
        panic!("player")
    };
    (joined.client, player)
}

fn tick(host: &mut ZoneHost<MobPit>, clock: &ManualClock) -> Vec<Outbound> {
    clock.advance(Duration::from_micros(33_334));
    host.advance();
    host.drain_outbound()
}

fn messages(out: &[Outbound]) -> Vec<ServerMessage> {
    out.iter()
        .filter_map(|o| match o {
            Outbound::Frame { frame, .. } => ServerMessage::decode(frame, &()).ok(),
            _ => None,
        })
        .collect()
}

fn hellos(out: &[Outbound]) -> Vec<PitEvent> {
    messages(out)
        .into_iter()
        .filter_map(|m| match m {
            ServerMessage::Event {
                type_id, payload, ..
            } => PitEvent::decode_event(type_id, &payload).ok().flatten(),
            _ => None,
        })
        .filter(|e| matches!(e, PitEvent::Hello { .. }))
        .collect()
}

#[test]
fn a_room_greets_with_its_coordinates_and_hands_off_through_doorways() {
    let clock = ManualClock::new();
    let issued = Issued::default();
    let mut room = host_issuing("pit.0.0", &clock, issued.clone());
    let (client, player) = join(&mut room, "ana");
    let out = tick(&mut room, &clock);
    let hello = hellos(&out);
    assert!(matches!(
        hello.first(),
        Some(PitEvent::Hello {
            room_x: 0,
            room_y: 0,
            restored: false,
            ..
        })
    ));
    // Walk to the east doorway (beside the pillar on its axis) and push
    // through it, no attack, until the room hands the player over.
    let mut target = None;
    let mut last = (0.0, 0.0);
    for seq in 1..600u32 {
        let me = room.zone().player_position(player).expect("in the room");
        last = me;
        let (dx, dy) = (21.0 - me.0, 2.0 - me.1);
        let len = (dx * dx + dy * dy).sqrt().max(1e-3);
        let east = PitInput {
            mx: (dx / len * 127.0) as i8,
            my: (dy / len * 127.0) as i8,
            buttons: 0,
            ..PitInput::default()
        };
        room.push_input(
            client,
            seq,
            seq,
            &distributed_game::GameInput::to_payload(&east),
        );
        let out = tick(&mut room, &clock);
        target = messages(&out).into_iter().find_map(|m| match m {
            ServerMessage::Handoff {
                target_zone,
                transfer_ticket,
            } => Some((target_zone, transfer_ticket)),
            _ => None,
        });
        if target.is_some() {
            break;
        }
    }
    let (target, ticket) = target.expect("walked through the east doorway");
    assert_eq!(target, "pit.1.0");
    assert_eq!(ticket, "transfer:pit.1.0");
    assert!(room.zone().player_position(player).is_none(), "left");
    // Through the passage, past the wall line (x = 20) by the hysteresis.
    assert!(last.0 > 19.5 && last.1.abs() < 2.5, "{last:?}");

    // The neighbour admits the player at the same world point: room 1's
    // local x is 40 m less (one step further at most).
    let request = issued
        .lock()
        .expect("issued")
        .take()
        .expect("handoff request");
    let mut next = host("pit.1.0", &clock);
    let claims = TicketClaims::new("ana", "pit.1.0", u64::MAX, "t2", Role::Player).with_name("ana");
    let transfer = Transfer {
        claims,
        source_zone: "pit.0.0".into(),
        issuer: "test".into(),
        generation: 0,
        carried: request.carried,
        departed_at_ms: 0,
    };
    let joined = next.join_transfer(&transfer, 0, "").expect("transfer join");
    let Viewer::Player(arrived) = joined.viewer else {
        panic!("player")
    };
    let at = next.zone().player_position(arrived).expect("arrived");
    let world_before = (last.0, last.1);
    let world_after = (40.0 + at.0, at.1);
    let moved = ((world_after.0 - world_before.0).powi(2)
        + (world_after.1 - world_before.1).powi(2))
    .sqrt();
    assert!(
        moved < 0.5,
        "same world point: {world_before:?} → {world_after:?}"
    );
    assert!(at.0 < -19.0, "just inside the west wall of pit.1.0: {at:?}");
}

#[test]
fn kills_emit_facts_and_hibernation_keeps_the_mobs() {
    let clock = ManualClock::new();
    let mut room = host("pit.2.1", &clock);
    let (client, player) = join(&mut room, "bo");
    let mobs = room.zone().stats().mobs_alive;
    // Chase and swing until something dies.
    let mut seq = 0;
    while room.zone().stats().kills == 0 && seq < 3000 {
        seq += 1;
        let me = room.zone().player_position(player).unwrap_or((0.0, 0.0));
        let nearest = room
            .zone()
            .mob_positions()
            .into_iter()
            .filter(|m| m.3)
            .min_by(|a, b| {
                let d = |m: &(u32, f32, f32, bool)| (m.1 - me.0).powi(2) + (m.2 - me.1).powi(2);
                d(a).total_cmp(&d(b))
            })
            .map(|m| (m.1, m.2))
            .unwrap_or((0.0, 0.0));
        let (dx, dy) = (nearest.0 - me.0, nearest.1 - me.1);
        let len = (dx * dx + dy * dy).sqrt().max(1e-3);
        room.push_input(client, seq, seq, &encode_bot_input([dx / len, dy / len]));
        tick(&mut room, &clock);
    }
    let stats = room.zone().stats();
    assert!(stats.kills >= 1, "a mob died");
    assert!(stats.pending >= 1, "its fact is in flight");
    assert_eq!(stats.mobs_alive, mobs - stats.kills as usize);

    // Sleep, wake: the dead stay dead, the count survives.
    let snapshot = room.hibernate_state().expect("snapshot");
    let mut woke = host("pit.2.1", &clock);
    woke.restore(&snapshot, "");
    let restored = woke.zone().stats();
    assert!(restored.restored);
    assert_eq!(restored.kills, stats.kills);
    assert_eq!(restored.mobs_alive, stats.mobs_alive);
    join(&mut woke, "bo");
    let out = tick(&mut woke, &clock);
    assert!(matches!(
        hellos(&out).first(),
        Some(PitEvent::Hello { restored: true, room_kills, .. }) if *room_kills == stats.kills
    ));
}

/// Walk `player` (client `client`) with `dir` for `frames` ticks.
fn walk(
    room: &mut ZoneHost<MobPit>,
    clock: &ManualClock,
    client: ClientId,
    seq: &mut u32,
    dir: [f32; 2],
    frames: u32,
) {
    for _ in 0..frames {
        *seq += 1;
        let input = PitInput {
            mx: (dir[0] * 127.0) as i8,
            my: (dir[1] * 127.0) as i8,
            buttons: 0,
            ..PitInput::default()
        };
        room.push_input(
            client,
            *seq,
            *seq,
            &distributed_game::GameInput::to_payload(&input),
        );
        tick(room, clock);
    }
}

fn near(a: (f32, f32), b: (f32, f32)) -> bool {
    ((a.0 - b.0).powi(2) + (a.1 - b.1).powi(2)).sqrt() < 0.05
}

#[test]
fn a_reload_takes_over_the_same_body() {
    // A page reload: a fresh ticket while the old connection still holds
    // the slot (or is inside its reconnect grace).
    let clock = ManualClock::new();
    let mut room = host("pit.0.0", &clock);
    let (client, player) = join(&mut room, "ana");
    tick(&mut room, &clock);
    let mut seq = 0;
    walk(&mut room, &clock, client, &mut seq, [1.0, 0.0], 20);
    let before = room.zone().player_position(player).expect("in the room");
    room.disconnect(client, false);
    tick(&mut room, &clock);
    let (_, again) = join(&mut room, "ana");
    assert_eq!(again, player, "same slot");
    let out = tick(&mut room, &clock);
    let after = room
        .zone()
        .player_position(player)
        .expect("still in the room");
    assert!(near(before, after), "same body: {before:?} → {after:?}");
    assert!(
        matches!(hellos(&out).first(), Some(PitEvent::Hello { entity, .. }) if *entity == 1000 + player.0),
        "the new client session is greeted with its own entity"
    );
    assert_eq!(room.zone().stats().players, 1);
}

#[test]
fn a_player_who_left_comes_back_where_it_stood() {
    let clock = ManualClock::new();
    let mut room = host("pit.0.0", &clock);
    let (client, player) = join(&mut room, "ana");
    tick(&mut room, &clock);
    let mut seq = 0;
    walk(&mut room, &clock, client, &mut seq, [0.0, 1.0], 25);
    let before = room.zone().player_position(player).expect("in the room");
    // Gone for good (closed the tab): the slot ends.
    room.disconnect(client, true);
    tick(&mut room, &clock);
    assert!(room.zone().player_position(player).is_none(), "slot gone");
    let (_, back) = join(&mut room, "ana");
    tick(&mut room, &clock);
    let at = room.zone().player_position(back).expect("back");
    assert!(near(before, at), "where it stood: {before:?} → {at:?}");
    // Someone else still starts at a spawn point.
    let (_, other) = join(&mut room, "bo");
    tick(&mut room, &clock);
    let theirs = room.zone().player_position(other).expect("bo");
    assert!(!near(before, theirs));
}

#[test]
fn positions_survive_hibernation_and_a_crash_snapshot() {
    let clock = ManualClock::new();
    let mut room = host("pit.1.0", &clock);
    let (ana, ana_player) = join(&mut room, "ana");
    let (bo, bo_player) = join(&mut room, "bo");
    tick(&mut room, &clock);
    let mut seq = 0;
    walk(&mut room, &clock, ana, &mut seq, [-1.0, 0.5], 20);
    let mut seq_bo = 0;
    walk(&mut room, &clock, bo, &mut seq_bo, [0.3, -1.0], 20);
    let ana_at = room.zone().player_position(ana_player).expect("ana");
    let bo_at = room.zone().player_position(bo_player).expect("bo");
    // Ana left before the room slept; Bo was still connected when the
    // last periodic snapshot was written (the node then crashed).
    room.disconnect(ana, true);
    tick(&mut room, &clock);
    let snapshot = room.hibernate_state().expect("snapshot");
    let mut woke = host("pit.1.0", &clock);
    woke.restore(&snapshot, "");
    let (_, ana_back) = join(&mut woke, "ana");
    let (_, bo_back) = join(&mut woke, "bo");
    tick(&mut woke, &clock);
    let ana_now = woke.zone().player_position(ana_back).expect("ana back");
    let bo_now = woke.zone().player_position(bo_back).expect("bo back");
    assert!(near(ana_at, ana_now), "ana: {ana_at:?} → {ana_now:?}");
    assert!(near(bo_at, bo_now), "bo: {bo_at:?} → {bo_now:?}");
    // A snapshot written before parking existed still restores.
    let mut old = host("pit.1.0", &clock);
    old.restore(br#"{"kills":3,"mobs":[]}"#, "");
    assert_eq!(old.zone().stats().kills, 3);
}

/// Every event of `out` (decoded).
fn events(out: &[Outbound]) -> Vec<PitEvent> {
    messages(out)
        .into_iter()
        .filter_map(|m| match m {
            ServerMessage::Event {
                type_id, payload, ..
            } => PitEvent::decode_event(type_id, &payload).ok().flatten(),
            _ => None,
        })
        .collect()
}

#[test]
fn a_boss_falls_to_a_party_its_hood_hears_and_it_stays_down() {
    use distributed_game::ZoneInbound;
    use game_sim::mobpit::{boss_room, room_zone, BOSS_TIER};
    use mobpit_rooms::{PitMessage, WorldTiers};

    let clock = ManualClock::new();
    let zone = room_zone(boss_room(DEFAULT_SEED, (0, 0)));
    let (facts, _script) = FactQueue::scripted(256);
    let mut room = ZoneHost::<MobPit>::new(
        &zone,
        HostConfig::default(),
        Arc::new(clock.clone()),
        RoomConfig::for_zone(&zone, DEFAULT_SEED).expect("room"),
    )
    .with_facts(facts.with_journal());
    let party: Vec<_> = ["ana", "bob", "cy", "di"]
        .iter()
        .map(|who| join(&mut room, who))
        .collect();
    let boss = |room: &ZoneHost<MobPit>| *room.zone().mob_positions().last().expect("mobs");
    let mut emitted = Vec::new();
    let mut seen = Vec::new();
    for seq in 1..3000u32 {
        let (_, bx, by, alive) = boss(&room);
        if !alive {
            break;
        }
        for (client, player) in &party {
            let Some(me) = room.zone().player_position(*player) else {
                continue;
            };
            let (dx, dy) = (bx - me.0, by - me.1);
            let d = (dx * dx + dy * dy).sqrt().max(1e-3);
            let near = d < 2.6;
            let k = if near { 40.0 } else { 127.0 } / d;
            let input = PitInput {
                mx: (dx * k).round().clamp(-127.0, 127.0) as i8,
                my: (dy * k).round().clamp(-127.0, 127.0) as i8,
                buttons: if near { PitInput::ATTACK } else { 0 },
                ..PitInput::default()
            };
            room.push_input(
                *client,
                seq,
                seq,
                &distributed_game::GameInput::to_payload(&input),
            );
        }
        let out = tick(&mut room, &clock);
        seen.extend(events(&out));
        emitted.extend(room.take_emitted_facts());
    }
    assert!(!boss(&room).3, "the party felled the boss");
    for _ in 0..3 {
        seen.extend(events(&tick(&mut room, &clock)));
        emitted.extend(room.take_emitted_facts());
    }
    let reports: Vec<_> = emitted
        .iter()
        .filter(|f| f.command() == mobpit_tiers::BOSS_DEFEATED)
        .collect();
    assert_eq!(reports.len(), 1, "one report of the fall");
    assert_eq!(reports[0].input()["hood"], "0.0");
    assert_eq!(reports[0].input()["room"], zone.as_str());
    let kills: Vec<_> = emitted
        .iter()
        .filter(|f| f.command() == mobpit_crawler::RECORD_KILL && f.input()["tier"] == BOSS_TIER)
        .collect();
    assert_eq!(
        kills.len(),
        1,
        "the slayer is credited (XP decided by the cell)"
    );
    assert!(seen
        .iter()
        .any(|e| matches!(e, PitEvent::BossDown { hood_tier: 1, .. })));
    // The hood has not heard yet (tier 0): the room keeps what it knows.
    room.deliver(ZoneInbound::Message(PitMessage::World(
        WorldTiers::default(),
    )));
    let out = tick(&mut room, &clock);
    assert!(!events(&out)
        .iter()
        .any(|e| matches!(e, PitEvent::World(w) if w.hood_tier == 0 || !w.boss_down)));
    // Long past its local respawn: still down while the hood says so.
    for _ in 0..(30 * 95) {
        tick(&mut room, &clock);
    }
    let down = WorldTiers {
        hood_tier: 1,
        boss_down: true,
        ..WorldTiers::default()
    };
    room.deliver(ZoneInbound::Message(PitMessage::World(down.clone())));
    tick(&mut room, &clock);
    tick(&mut room, &clock);
    assert!(!boss(&room).3, "down while its hood says so");
    // The hood brings it back.
    room.deliver(ZoneInbound::Message(PitMessage::World(WorldTiers {
        boss_down: false,
        ..down
    })));
    tick(&mut room, &clock);
    tick(&mut room, &clock);
    assert!(boss(&room).3, "back when its hood says so");
}

/// Walk `player` toward the nearest living mob until one winds up a slam
/// next to it; then, with `jump`, take off so the slam lands mid-air.
/// Returns the player's HP before and after the slam.
fn face_a_slam(jump: bool) -> (i32, i32) {
    let clock = ManualClock::new();
    let mut room = host("pit.0.0", &clock);
    let (client, player) = join(&mut room, "ana");
    let mut seq = 0u32;
    let mut send = |room: &mut ZoneHost<MobPit>, input: PitInput| {
        seq += 1;
        room.push_input(
            client,
            seq,
            seq,
            &distributed_game::GameInput::to_payload(&input),
        );
    };
    for _ in 0..900 {
        let (me, hp) = room.zone().player_state(player).expect("in the room");
        if let Some(&(_, _, _, left)) = room.zone().mob_windups().first() {
            if left <= 12 {
                let before = hp;
                send(
                    &mut room,
                    PitInput {
                        buttons: if jump { PitInput::JUMP } else { 0 },
                        ..PitInput::default()
                    },
                );
                // One command frame per tick, as clients send them (the
                // step, gravity included, runs per input).
                for _ in 0..20 {
                    tick(&mut room, &clock);
                    send(&mut room, PitInput::default());
                }
                let (_, after) = room.zone().player_state(player).expect("in the room");
                return (before, after);
            }
            send(&mut room, PitInput::default());
        } else {
            let target = room
                .zone()
                .mob_positions()
                .into_iter()
                .filter(|m| m.3)
                .min_by(|a, b| {
                    let d = |m: &(u32, f32, f32, bool)| (m.1 - me.x).powi(2) + (m.2 - me.y).powi(2);
                    d(a).total_cmp(&d(b))
                })
                .expect("a mob");
            let (dx, dy) = (target.1 - me.x, target.2 - me.y);
            let d = (dx * dx + dy * dy).sqrt().max(1e-3);
            let k = if d < 1.6 { 0.0 } else { 127.0 / d };
            send(
                &mut room,
                PitInput {
                    mx: (dx * k) as i8,
                    my: (dy * k) as i8,
                    buttons: 0,
                    ..PitInput::default()
                },
            );
        }
        tick(&mut room, &clock);
    }
    panic!("no slam");
}

#[test]
fn a_jump_clears_a_mobs_slam() {
    let (before, after) = face_a_slam(false);
    assert!(
        after < before,
        "standing in the ring hurts: {before} → {after}"
    );
    let (before, after) = face_a_slam(true);
    assert!(
        after >= before,
        "jumping over it does not: {before} → {after}"
    );
}

#[test]
fn an_arrow_hits_the_mob_it_was_aimed_at_and_obstacles_stop_it() {
    let clock = ManualClock::new();
    let mut room = host("pit.0.0", &clock);
    let (client, player) = join(&mut room, "ana");
    for _ in 0..3 {
        tick(&mut room, &clock);
    }
    let me = room.zone().player_state(player).expect("in").0;
    // The nearest living mob with nothing in between.
    let (id, mx, my, _) = room
        .zone()
        .mob_positions()
        .into_iter()
        .filter(|m| m.3)
        .min_by(|a, b| {
            let d = |m: &(u32, f32, f32, bool)| (m.1 - me.x).powi(2) + (m.2 - me.y).powi(2);
            d(a).total_cmp(&d(b))
        })
        .expect("a mob");
    let yaw = (my - me.y).atan2(mx - me.x);
    let d = ((mx - me.x).powi(2) + (my - me.y).powi(2)).sqrt();
    // Aim at the mob's middle from the eye.
    let pitch = ((0.45 - mobpit_rooms::EYE_HEIGHT) / d).atan();
    let shot = |n: u16, flags: u8| PitInput {
        buttons: PitInput::FIRE | flags,
        yaw,
        pitch,
        shot: n,
        delay_ms: 100,
        ..PitInput::default()
    };
    let mut verdicts = Vec::new();
    for (seq, flags) in [(1u32, 0u8), (2, PitInput::NO_COMP)] {
        room.push_input(
            client,
            seq,
            room.tick() as u32,
            &distributed_game::GameInput::to_payload(&shot(seq as u16, flags)),
        );
        for _ in 0..14 {
            let out = tick(&mut room, &clock);
            verdicts.extend(events(&out).into_iter().filter_map(|e| match e {
                PitEvent::Shot {
                    shot,
                    hit,
                    compensated,
                    age_ms,
                    ..
                } => Some((shot, hit, compensated, age_ms)),
                _ => None,
            }));
        }
    }
    assert_eq!(verdicts.len(), 2, "{verdicts:?}");
    assert_eq!(verdicts[0].1, Some(id), "compensated: hit {verdicts:?}");
    assert!(
        verdicts[0].2 && verdicts[0].3 > 50.0,
        "rewound ~100 ms: {verdicts:?}"
    );
    assert!(
        !verdicts[1].2 && verdicts[1].3 == 0.0,
        "uncompensated: no rewind"
    );
    // Straight into the central pillar (0, 0, r 2.5): stopped short.
    let into_pillar = PitInput {
        buttons: PitInput::FIRE,
        yaw: (0.0 - me.y).atan2(0.0 - me.x),
        pitch: 0.0,
        shot: 9,
        ..PitInput::default()
    };
    room.push_input(
        client,
        3,
        room.tick() as u32,
        &distributed_game::GameInput::to_payload(&into_pillar),
    );
    let mut stopped = None;
    for _ in 0..3 {
        let out = tick(&mut room, &clock);
        for e in events(&out) {
            if let PitEvent::Shot {
                shot: 9, hit, x, y, ..
            } = e
            {
                stopped = Some((hit, (x * x + y * y).sqrt()));
            }
        }
    }
    let (hit, from_centre) = stopped.expect("verdict");
    assert_eq!(hit, None);
    assert!(
        (from_centre - 2.5).abs() < 0.2,
        "stopped at the pillar: {from_centre}"
    );
}
