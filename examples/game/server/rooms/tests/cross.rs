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

/// A celld activation's boot epoch: microseconds since 1970 (past 2^53
/// once shifted).
const BOOT: u64 = 1_793_000_000_000_000;

fn host(zone: &str, clock: &ManualClock) -> ZoneHost<MobPit> {
    host_at(zone, clock, BOOT)
}

/// A room activation booted at `boot_epoch` (a restart boots a later one).
fn host_at(zone: &str, clock: &ManualClock, boot_epoch: u64) -> ZoneHost<MobPit> {
    let (facts, _script) = FactQueue::scripted(64);
    let issued: Arc<Mutex<Option<HandoffRequest>>> = Arc::default();
    ZoneHost::<MobPit>::new(
        zone,
        HostConfig {
            boot_epoch,
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

/// A grunt of `a` (pit.0.0) chases ana east through the doorway into `b`
/// (pit.1.0): returns its id and `b`'s own mob count.
fn follow_across(
    a: &mut ZoneHost<MobPit>,
    b: &mut ZoneHost<MobPit>,
    clock: &ManualClock,
) -> (u32, usize) {
    let own_b = b.zone().mob_positions().len();
    let (client, ana) = join(a, "ana");
    for _ in 0..3 {
        tick(a, b, clock);
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
        tick(a, b, clock);
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
            let (_, bo) = join(b, "ana");
            b.zone_mut().place_player(bo, -15.0, 0.0);
            in_b = true;
        }
        tick(a, b, clock);
        pump(a, b);
        let (home, away) = (alive(a, id), visitors(b, own_b));
        assert!(!(home && away > 0), "tick {seq}: in both rooms");
        if away > 0 {
            break;
        }
    }
    assert_eq!(visitors(b, own_b), 1, "the grunt followed ana into pit.1.0");
    assert!(!alive(a, id), "away from pit.0.0");
    (id, own_b)
}

#[test]
fn a_chasing_mob_follows_through_a_doorway_and_is_never_in_two_rooms() {
    let clock = ManualClock::new();
    let mut a = host("pit.0.0", &clock);
    let mut b = host("pit.1.0", &clock);
    let (id, own_b) = follow_across(&mut a, &mut b, &clock);
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

#[test]
fn the_home_room_restarts_while_its_mob_is_away_and_keeps_the_lease() {
    let clock = ManualClock::new();
    let mut a = host("pit.0.0", &clock);
    let mut b = host("pit.1.0", &clock);
    let (id, own_b) = follow_across(&mut a, &mut b, &clock);
    for _ in 0..10 {
        tick(&mut a, &mut b, &clock);
        pump(&mut a, &mut b);
    }
    // pit.0.0 saves and stops (evicted, or its node went down); messages
    // for it are lost while it is gone. A new activation restores it.
    let snapshot = a.hibernate_state().expect("a snapshot");
    drop(a);
    for _ in 0..20 {
        clock.advance(Duration::from_micros(33_334));
        b.advance();
        b.drain_outbound();
        b.take_zone_messages();
    }
    let mut a = host_at("pit.0.0", &clock, BOOT + 1);
    a.restore(&snapshot, "");
    assert!(!alive(&a, id), "still away after the restart");
    // The visitor's next beat finds the restored lease and renews it.
    for t in 0..300u32 {
        tick(&mut a, &mut b, &clock);
        pump(&mut a, &mut b);
        assert!(
            !(alive(&a, id) && visitors(&b, own_b) > 0),
            "tick {t}: in both rooms"
        );
    }
    assert_eq!(visitors(&b, own_b), 1, "the lease survived the restart");
    assert!(!alive(&a, id));
}

#[test]
fn the_room_holding_a_visitor_restarts_and_the_mob_goes_home() {
    let clock = ManualClock::new();
    let mut a = host("pit.0.0", &clock);
    let mut b = host("pit.1.0", &clock);
    let (id, own_b) = follow_across(&mut a, &mut b, &clock);
    for _ in 0..10 {
        tick(&mut a, &mut b, &clock);
        pump(&mut a, &mut b);
    }
    // pit.1.0 restarts from its snapshot: visitors are not saved there.
    let snapshot = b.hibernate_state().expect("a snapshot");
    drop(b);
    let mut b = host_at("pit.1.0", &clock, BOOT + 1);
    b.restore(&snapshot, "");
    assert_eq!(visitors(&b, own_b), 0, "a restarted room holds no visitor");
    // No more beats: the grunt is home after the grace, and only there.
    let mut back = None;
    for t in 0..200u32 {
        tick(&mut a, &mut b, &clock);
        pump(&mut a, &mut b);
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
    assert!(t <= 130, "back after {t} ticks");
    for _ in 0..100 {
        tick(&mut a, &mut b, &clock);
        pump(&mut a, &mut b);
    }
    assert_eq!(visitors(&b, own_b), 0, "and never back in pit.1.0");
}

#[test]
fn the_largest_crossing_id_survives_the_message_codec() {
    use distributed_game::GameEvent;
    use mobpit_rooms::PitMessage;
    // A celld activation's boot epoch is microseconds since 1970.
    let transfer = (1u64 << 53) - 1;
    let beat = PitMessage::MobBeat { transfer };
    let back = PitMessage::decode_event(beat.event_id(), &beat.to_payload())
        .expect("decode")
        .expect("a message");
    assert!(
        matches!(back, PitMessage::MobBeat { transfer: t } if t == transfer),
        "{back:?}"
    );
}

#[test]
fn a_mob_shot_from_next_door_comes_after_the_shooter() {
    let clock = ManualClock::new();
    let mut a = host("pit.0.0", &clock);
    let mut b = host("pit.1.0", &clock);
    let own_a = a.zone().mob_positions().len();
    let (client, ana) = join(&mut a, "ana");
    let (_, bo) = join(&mut b, "bo");
    for _ in 0..3 {
        tick(&mut a, &mut b, &clock);
    }
    // Bo far off in pit.1.0's east (the mob is not busy with him); ana by
    // the east doorway of pit.0.0, a grunt across it.
    b.zone_mut().place_player(bo, 17.0, 15.0);
    a.zone_mut().place_player(ana, 17.0, 0.0);
    let (id, _) = b
        .zone()
        .mob_tiers()
        .into_iter()
        .find(|t| t.1 == 1)
        .expect("a grunt");
    b.zone_mut().place_mob(id, -16.0, 0.0);
    let ground = room_ground(DEFAULT_SEED, (1, 0))
        .ground_at([-16.0, 0.0], layer::ALL)
        .map_or(0.0, |g| g.height);
    let dist = (40.0 - 16.0) - 17.0;
    let pitch = ((ground + 0.45 - EYE_HEIGHT) / dist).atan();
    // Shoot until one beam hits (a grunt takes three).
    let mut hit = false;
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
                if let PitEvent::Beam { hit: Some(h), .. } = e {
                    hit |= h == id;
                }
            }
            pump(&mut a, &mut b);
            if !hit {
                b.zone_mut().place_mob(id, -16.0, 0.0);
            }
        }
        if hit {
            break;
        }
    }
    assert!(hit, "a beam hit the grunt");
    assert!(alive(&b, id), "one hit does not kill a grunt");
    // It comes through the doorway after ana, and closes in on her.
    let mut closest = f32::MAX;
    let mut arrived = None;
    for t in 0..300u32 {
        tick(&mut a, &mut b, &clock);
        pump(&mut a, &mut b);
        assert!(
            !(alive(&b, id) && visitors(&a, own_a) > 0),
            "tick {t}: in both rooms"
        );
        let (ax, ay) = a.zone().player_position(ana).expect("ana");
        for m in a.zone().mob_positions() {
            if m.0 as usize > own_a && m.3 {
                arrived.get_or_insert(t);
                closest = closest.min(((m.1 - ax).powi(2) + (m.2 - ay).powi(2)).sqrt());
            }
        }
    }
    let t = arrived.expect("the grunt came into pit.0.0");
    eprintln!("shot grunt came through after {t} ticks; closest {closest:.2} m");
    assert!(closest < 2.0, "it reached ana: {closest} m");
}
