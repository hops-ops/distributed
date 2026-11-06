//! Zone-to-zone messages: a zone sends with `Ctx::send_zone` /
//! `send_zone_for`, the driver carries the envelope, the target runs
//! `on_zone_message` once (repeats dropped) and may emit facts as the
//! verified principal the message carried.

use std::sync::Arc;

use distributed_game::*;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, GameEvent)]
pub enum Relay {
    /// "Score this for whoever it is on behalf of."
    #[game_event(id = 1)]
    Score {
        /// Points.
        points: u32,
    },
}

#[derive(Serialize, FactCommand)]
#[fact(command = "scores.record")]
struct Scored {
    points: u32,
}

#[derive(Default)]
struct Room {
    /// `(from zone, principal, points)` received.
    got: Vec<(String, Option<String>, u32)>,
}

impl Zone for Room {
    type Config = ();
    type Input = u8;
    type Timer = ();
    type Message = Relay;

    fn schema() -> EntitySchema {
        EntitySchema::new()
    }

    fn on_create(_ctx: &mut Ctx<'_, Self>, _: ()) -> Self {
        Self::default()
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<u8> {
        payload.first().copied()
    }

    fn on_tick(&mut self, _ctx: &mut Ctx<'_, Self>) {}

    /// A player joining sends one message on its behalf and one plain.
    fn on_join(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        ctx.send_zone_for(player, "east", &Relay::Score { points: 3 });
        ctx.send_zone("east", &Relay::Score { points: 1 });
    }

    fn on_zone_message(&mut self, ctx: &mut Ctx<'_, Self>, from: &ZoneSender, message: Relay) {
        let Relay::Score { points } = message;
        self.got.push((
            from.zone.clone(),
            from.principal.as_ref().map(|p| p.as_str().to_string()),
            points,
        ));
        if let Some(principal) = &from.principal {
            let fact = ctx
                .fact_with_key(&Scored { points }, &[&from.zone, &self.got.len()])
                .unwrap();
            ctx.emit_fact_as(principal, fact).unwrap();
        }
    }
}

fn host(zone: &str) -> ZoneHost<Room> {
    let (queue, _feed) = fact_channel(16, RetryPolicy::default());
    ZoneHost::<Room>::new(
        zone,
        HostConfig::default(),
        Arc::new(ManualClock::new()),
        (),
    )
    .with_facts(queue.with_journal())
}

#[test]
fn a_message_reaches_its_zone_once_with_the_verified_principal() {
    let mut west = host("west");
    let mut east = host("east");
    west.join(
        &TicketClaims::new("ana", "west", u64::MAX, "n1", Role::Player),
        0,
        "",
    )
    .unwrap();
    let sent = west.take_zone_messages();
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().all(|m| m.from == "west" && m.to == "east"));
    assert_ne!(sent[0].id, sent[1].id, "ids are unique");
    assert_eq!(sent[0].principal.as_deref(), Some("ana"));
    assert_eq!(sent[1].principal, None);
    assert!(west.take_zone_messages().is_empty(), "taken once");

    for envelope in &sent {
        east.deliver_zone(envelope.clone()).unwrap();
    }
    // A resend (at-least-once transport) is dropped.
    assert_eq!(
        east.deliver_zone(sent[0].clone()),
        Err(ZoneMessageError::Duplicate)
    );
    assert_eq!(
        east.zone().got,
        [
            ("west".to_string(), Some("ana".to_string()), 3),
            ("west".to_string(), None, 1),
        ]
    );
    // The fact is bound to the principal the message carried.
    let facts = east.take_emitted_facts();
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].principal(), Some("ana"));

    // Not for this zone, or not this zone's message type: refused.
    assert_eq!(
        west.deliver_zone(sent[1].clone()),
        Err(ZoneMessageError::WrongZone)
    );
    let mut garbled = sent[1].clone();
    garbled.id = "west/x/99".into();
    garbled.type_id = 77;
    assert!(matches!(
        east.deliver_zone(garbled),
        Err(ZoneMessageError::Decode(_))
    ));
}

#[test]
fn a_new_activation_numbers_its_messages_apart_from_the_last() {
    let mut a = host("west");
    a.join(
        &TicketClaims::new("ana", "west", u64::MAX, "n1", Role::Player),
        0,
        "",
    )
    .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    let mut b = host("west");
    b.join(
        &TicketClaims::new("ana", "west", u64::MAX, "n2", Role::Player),
        0,
        "",
    )
    .unwrap();
    let (first, second) = (a.take_zone_messages(), b.take_zone_messages());
    assert_ne!(first[0].id, second[0].id);
    let mut east = host("east");
    east.deliver_zone(first[0].clone()).unwrap();
    east.deliver_zone(second[0].clone()).unwrap();
    assert_eq!(east.zone().got.len(), 2);
}
