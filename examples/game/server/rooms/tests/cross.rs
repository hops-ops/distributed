//! Play across room boundaries on the reference host: two rooms next door
//! and a pump carrying their zone messages (what the celld objects do with
//! `POST /{zone}/__zone`). A laser shot through a doorway kills a mob in
//! the next room for the shooter; a mob follows a player across and is
//! never live in two rooms, even when the messages stop.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed_game::collide::layer;
use distributed_game::protocol::ServerMessage;
use distributed_game::{
    ClientId, FactQueue, GameEvent, GameInput, HandoffRequest, HostConfig, ManualClock, Outbound,
    PlayerId, Role, TicketClaims, Viewer, ZoneHost,
};
use game_sim::mobpit::room_ground;
use mobpit_rooms::{MobPit, PitEvent, PitInput, RoomConfig, DEFAULT_SEED, EYE_HEIGHT};

fn host(zone: &str, clock: &ManualClock) -> ZoneHost<MobPit> {
    let (facts, _script) = FactQueue::scripted(64);
    let issued: Arc<Mutex<Option<HandoffRequest>>> = Arc::default();
    ZoneHost::<MobPit>::new(
        zone,
        HostConfig {
            boot_epoch: 7,
            ..HostConfig::default()
        },
        Arc::new(clock.clone()),
        RoomConfig::for_zone(zone, DEFAULT_SEED).expect("room"),
    )
    .with_facts(facts.with_journal())
    .with_transfer_issuer(move |request| {
        *issued.lock().expect("issued") = Some(request.clone());
        Some(format!("transfer:{}", request.target_zone))
    })
}

fn join(host: &mut ZoneHost<MobPit>, who: &str) -> (ClientId, PlayerId) {
    let claims = TicketClaims::new(who, host.zone_id(), u64::MAX, who, Role::Player).with_name(who);
    let joined = host.join(&claims, 0, "").expect("join");
    let Viewer::Player(player) = joined.viewer else {
        panic!("player")
    };
    (joined.client, player)
}

/// One tick of both rooms on one clock.
fn tick(a: &mut ZoneHost<MobPit>, b: &mut ZoneHost<MobPit>, clock: &ManualClock) -> Vec<PitEvent> {
    clock.advance(Duration::from_micros(33_334));
    a.advance();
    b.advance();
    a.drain_outbound();
    events(&b.drain_outbound())
}

/// Carry zone messages between the two rooms; returns how many.
fn pump(a: &mut ZoneHost<MobPit>, b: &mut ZoneHost<MobPit>) -> usize {
    let mut n = 0;
    for envelope in a.take_zone_messages() {
        if envelope.to == b.zone_id() {
            let _ = b.deliver_zone(envelope);
            n += 1;
        }
    }
    for envelope in b.take_zone_messages() {
        if envelope.to == a.zone_id() {
            let _ = a.deliver_zone(envelope);
            n += 1;
        }
    }
    n
}

fn events(out: &[Outbound]) -> Vec<PitEvent> {
    out.iter()
        .filter_map(|o| match o {
            Outbound::Frame { frame, .. } => ServerMessage::decode(frame, &()).ok(),
            _ => None,
        })
        .filter_map(|m| match m {
            ServerMessage::Event {
                type_id, payload, ..
            } => PitEvent::decode_event(type_id, &payload).ok().flatten(),
            _ => None,
        })
        .collect()
}

fn alive(room: &ZoneHost<MobPit>, id: u32) -> bool {
    room.zone()
        .mob_positions()
        .into_iter()
        .any(|m| m.0 == id && m.3)
}

/// Guest slots (visitors) alive in `room`, beyond its own `own` mobs.
fn visitors(room: &ZoneHost<MobPit>, own: usize) -> usize {
    room.zone()
        .mob_positions()
        .into_iter()
        .filter(|m| m.0 as usize > own && m.3)
        .count()
}

#[test]
fn a_laser_through_a_doorway_kills_a_mob_next_door_for_the_shooter() {
    let clock = ManualClock::new();
    let mut a = host("pit.0.0", &clock);
    let mut b = host("pit.1.0", &clock);
    let (client, ana) = join(&mut a, "ana");
    // Someone in the room next door (it ticks; in the game, the shooter's
    // page keeps it awake from beside the doorway).
    join(&mut b, "bo");
    for _ in 0..3 {
        tick(&mut a, &mut b, &clock);
    }
    // Ana by the east doorway; a mob of pit.1.0 across it, on the axis.
    a.zone_mut().place_player(ana, 17.0, 0.0);
    let (id, _, _, _) = b
        .zone()
        .mob_positions()
        .into_iter()
        .find(|m| m.3)
        .expect("a mob");
    let tier = b
        .zone()
        .mob_tiers()
        .into_iter()
        .find(|t| t.0 == id)
        .expect("tier")
        .1;
    b.zone_mut().place_mob(id, -16.0, 0.0);
    let ground = room_ground(DEFAULT_SEED, (1, 0))
        .ground_at([-16.0, 0.0], layer::ALL)
        .map_or(0.0, |g| g.height);
    let half = match tier {
        3 => 1.0,
        2 => 0.75,
        _ => 0.45,
    };
    // From x = 17 here to x = 40 − 16 = 24 in this room's frame.
    let dist = (40.0 - 16.0) - 17.0;
    let pitch = ((ground + half - EYE_HEIGHT) / dist).atan();
    let mut beams = 0;
    let mut killed_by = None;
    for n in 1..=20u16 {
        a.push_input(
            client,
            u32::from(n),
            a.tick() as u32,
            &PitInput {
                buttons: PitInput::FIRE | PitInput::NO_COMP,
                yaw: 0.0,
                pitch,
                shot: n,
                ..PitInput::default()
            }
            .to_payload(),
        );
        for _ in 0..14 {
            for e in tick(&mut a, &mut b, &clock) {
                match e {
                    PitEvent::Beam { by, hit, .. } => {
                        assert_eq!(by, "ana");
                        beams += usize::from(hit == Some(id));
                    }
                    PitEvent::Kill {
                        mob, killer_name, ..
                    } if mob == id => {
                        killed_by = Some(killer_name);
                    }
                    _ => {}
                }
            }
            pump(&mut a, &mut b);
            b.zone_mut().place_mob(id, -16.0, 0.0);
        }
        if killed_by.is_some() {
            break;
        }
    }
    assert!(beams >= 1, "the beam came through and hit");
    assert_eq!(killed_by.as_deref(), Some("ana"));
    // The kill fact: in pit.1.0, on behalf of ana (her verified principal,
    // carried by the zone message), keyed by the mob's own room.
    let facts = b.take_emitted_facts();
    let kill = facts
        .iter()
        .find(|f| f.principal() == Some("ana"))
        .expect("a kill fact for ana");
    assert!(kill.key().contains("pit.1.0"), "{}", kill.key());
}

#[test]
fn a_chasing_mob_follows_through_a_doorway_and_is_never_in_two_rooms() {
    let clock = ManualClock::new();
    let mut a = host("pit.0.0", &clock);
    let mut b = host("pit.1.0", &clock);
    let own_b = b.zone().mob_positions().len();
    let (client, ana) = join(&mut a, "ana");
    for _ in 0..3 {
        tick(&mut a, &mut b, &clock);
    }
    // A grunt (tier 1) by the east doorway, ana just ahead of it.
    let (id, _) = a
        .zone()
        .mob_tiers()
        .into_iter()
        .find(|t| t.1 == 1)
        .expect("a grunt");
    a.zone_mut().place_mob(id, 15.0, 0.5);
    a.zone_mut().place_player(ana, 18.5, 0.0);
    for _ in 0..5 {
        tick(&mut a, &mut b, &clock);
    }
    // Ana walks east through the doorway; her client would rejoin next door.
    let mut in_b = false;
    for seq in 1..=200u32 {
        if a.zone().player_position(ana).is_some() {
            a.push_input(
                client,
                seq,
                a.tick() as u32,
                &PitInput {
                    mx: 127,
                    ..PitInput::default()
                }
                .to_payload(),
            );
        } else if !in_b {
            let (_, bo) = join(&mut b, "ana");
            b.zone_mut().place_player(bo, -15.0, 0.0);
            in_b = true;
        }
        tick(&mut a, &mut b, &clock);
        pump(&mut a, &mut b);
        let (home, away) = (alive(&a, id), visitors(&b, own_b));
        assert!(!(home && away > 0), "tick {seq}: in both rooms");
        if away > 0 {
            break;
        }
    }
    assert_eq!(
        visitors(&b, own_b),
        1,
        "the grunt followed ana into pit.1.0"
    );
    assert!(!alive(&a, id), "away from pit.0.0");
    // The lease holds while both rooms talk.
    for _ in 0..200 {
        tick(&mut a, &mut b, &clock);
        pump(&mut a, &mut b);
        assert!(
            !(alive(&a, id) && visitors(&b, own_b) > 0),
            "never in two rooms"
        );
    }
    assert_eq!(
        visitors(&b, own_b),
        1,
        "still visiting while the lease holds"
    );
    // The messages stop (pit.1.0 crashed, or the network): the visitor
    // lapses first, then the grunt is back home; never both, never gone for
    // good.
    let mut back = None;
    for t in 0..200u32 {
        tick(&mut a, &mut b, &clock);
        a.take_zone_messages();
        b.take_zone_messages();
        assert!(
            !(alive(&a, id) && visitors(&b, own_b) > 0),
            "tick {t}: in both rooms"
        );
        if alive(&a, id) {
            back = Some(t);
            break;
        }
    }
    let t = back.expect("home again after the grace");
    eprintln!("messages stopped: home again after {t} ticks");
    assert!((60..=130).contains(&t), "back after {t} ticks");
}
