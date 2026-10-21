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
