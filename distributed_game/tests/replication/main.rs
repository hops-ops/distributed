//! P3 replication: interest enter/leave with hysteresis, global entities,
//! change detection, byte-budget fairness and distance-based update rates.

use std::collections::BTreeMap;

use distributed_game::protocol::{Delta, ServerMessage};
use distributed_game::replication::{ClientReplica, EncodedWorld};
use distributed_game::{
    ClientMirror, EntitySchema, EntityState, FieldType, FieldValue, InterestConfig, KindDef,
    UpdateRates, View, WorldView,
};

fn schema() -> EntitySchema {
    let pos = FieldType::QuantF32 {
        min: -1000.0,
        max: 1000.0,
        bits: 20,
    };
    EntitySchema::new()
        .kind(
            KindDef::new(1, "thing")
                .field("x", pos)
                .field("y", pos)
                .field("hp", FieldType::U16),
        )
        .kind(
            KindDef::new(2, "beacon")
                .field("x", pos)
                .field("y", pos)
                .field("hp", FieldType::U16)
                .every_patch(),
        )
}

fn thing(id: u32, x: f32, y: f32, hp: u32) -> EntityState {
    EntityState::new(
        id,
        1,
        x,
        y,
        vec![FieldValue::F(x), FieldValue::F(y), FieldValue::U(hp)],
    )
}

struct Client {
    replica: ClientReplica,
    mirror: ClientMirror,
    view: View,
    budget: usize,
    interest: InterestConfig,
    world: Option<EncodedWorld>,
}

impl Client {
    fn new(view: View, budget: usize) -> Self {
        Self::with_rates(view, budget, UpdateRates::every_patch())
    }

    fn with_rates(view: View, budget: usize, update_rates: UpdateRates) -> Self {
        // Seed the mirror with the schema as a Welcome would.
        let mut mirror = ClientMirror::new();
        mirror.welcome = Some(distributed_game::protocol::Welcome {
            protocol_version: distributed_game::PROTOCOL_VERSION,
            session_id: "s".into(),
            zone_id: "z".into(),
            role: distributed_game::Role::Player,
            tick_rate: 30,
            patch_rate: 20,
            server_tick: 0,
            server_time: 0.0,
            reconnect_token: String::new(),
            content_version: String::new(),
            schema: schema(),
            features: 0,
            origin: None,
        });
        Self {
            replica: ClientReplica::new(),
            mirror,
            view,
            budget,
            interest: InterestConfig {
                cell_size: 8.0,
                leave_margin: 0.1,
                update_rates,
            },
            world: None,
        }
    }

    fn patch(&mut self, entities: Vec<EntityState>, tick: u32) -> Delta {
        let mut view = WorldView::default();
        for e in entities {
            view.push(e);
        }
        let world = match &self.world {
            Some(last) => last.next(&schema(), view, &self.interest),
            None => EncodedWorld::build(&schema(), view, &self.interest),
        };
        let (frame, _) = self.replica.build_delta(
            &schema(),
            &world,
            &self.interest,
            &self.view,
            self.budget,
            tick,
            0,
        );
        self.world = Some(world);
        match self.mirror.apply(&frame).unwrap() {
            ServerMessage::Delta(d) => d,
            other => panic!("{other:?}"),
        }
    }
}

#[test]
fn entities_enter_and_leave_views_with_hysteresis() {
    let mut c = Client::new(View::new(0.0, 0.0, 10.0), 10_000);
    let far = thing(2, 50.0, 50.0, 1);

    let d = c.patch(vec![thing(1, 5.0, 0.0, 10), far.clone()], 1);
    assert_eq!(d.creates.iter().map(|e| e.id).collect::<Vec<_>>(), vec![1]);

    // Inside the leave margin (10 * 1.1 = 11): update, not remove.
    let d = c.patch(vec![thing(1, 10.5, 0.0, 10), far.clone()], 2);
    assert!(d.removes.is_empty());
    assert_eq!(d.updates.len(), 1);
    assert_eq!(d.updates[0].mask, 0b001, "only x changed");

    // Beyond the margin: remove.
    let d = c.patch(vec![thing(1, 12.0, 0.0, 10), far.clone()], 3);
    assert_eq!(d.removes, vec![1]);
    assert!(c.mirror.entities.is_empty());

    // Back inside the enter radius: full create again.
    let d = c.patch(vec![thing(1, 9.0, 0.0, 7), far.clone()], 4);
    assert_eq!(d.creates.len(), 1);
    assert_eq!(c.mirror.entities[&1].fields[2], FieldValue::U(7));

    // Despawn: remove.
    let d = c.patch(vec![far], 5);
    assert_eq!(d.removes, vec![1]);
}

#[test]
fn global_entities_ignore_distance_and_unchanged_state_is_free() {
    let mut c = Client::new(View::new(0.0, 0.0, 10.0), 10_000);
    let d = c.patch(vec![thing(9, 900.0, 900.0, 1).global()], 1);
    assert_eq!(d.creates.len(), 1);
    let d = c.patch(vec![thing(9, 900.0, 900.0, 1).global()], 2);
    assert!(d.creates.is_empty() && d.updates.is_empty() && d.removes.is_empty());
    // Sub-quantum jitter (20-bit over 2000 units ≈ 0.0019) is not a change.
    let d = c.patch(vec![thing(9, 900.000_1, 900.0, 1).global()], 3);
    assert!(d.updates.is_empty());
}

#[test]
fn byte_budget_prioritizes_but_never_starves() {
    // 60 entities all changing every patch; budget fits ~8 updates.
    let mut c = Client::new(View::new(0.0, 0.0, 100.0), 64);
    let n = 60u32;
    let mut last_sent: BTreeMap<u32, u32> = BTreeMap::new();
    let mut max_gap: BTreeMap<u32, u32> = BTreeMap::new();
    let mut sends: BTreeMap<u32, u32> = BTreeMap::new();
    for tick in 1..=200u32 {
        let entities = (1..=n)
            .map(|id| {
                // Entity 1 is close (high priority), the rest spread out.
                let d = if id == 1 { 1.0 } else { 10.0 + id as f32 };
                thing(id, d, 0.0, tick)
            })
            .collect();
        let delta = c.patch(entities, tick);
        let sent: Vec<u32> = delta
            .creates
            .iter()
            .map(|e| e.id)
            .chain(delta.updates.iter().map(|u| u.id))
            .collect();
        assert!(!sent.is_empty());
        for id in sent {
            let gap = tick - last_sent.get(&id).copied().unwrap_or(0);
            let g = max_gap.entry(id).or_insert(0);
            *g = (*g).max(gap);
            last_sent.insert(id, tick);
            *sends.entry(id).or_insert(0) += 1;
        }
    }
    assert_eq!(sends.len(), n as usize, "every entity was sent");
    let worst = max_gap.values().max().copied().unwrap();
    assert!(worst <= 30, "bounded staleness, worst gap {worst} patches");
    assert!(
        sends[&1] > sends[&n] * 4 / 3,
        "near entity sent more often ({} vs {})",
        sends[&1],
        sends[&n]
    );
    // Mirror converges to the server state for everything sent recently.
    assert_eq!(c.mirror.entities.len(), n as usize);
}

fn hp(c: &Client, id: u32) -> u32 {
    match c.mirror.entities[&id].fields[2] {
        FieldValue::U(hp) => hp,
        ref other => panic!("{other:?}"),
    }
}

#[test]
fn far_updates_thin_out_and_converge_within_their_interval() {
    // Defaults: within 50 every patch, within 80 every 2nd, beyond every 4th.
    let mut c = Client::with_rates(View::new(0.0, 0.0, 100.0), 1 << 20, UpdateRates::default());
    let world = |hp: u32| {
        vec![
            thing(1, 10.0, 0.0, hp),
            thing(2, 65.0, 0.0, hp),
            thing(3, 95.0, 0.0, hp),
            thing(4, 0.0, 95.0, hp).global(),
            EntityState::new(
                5,
                2,
                -95.0,
                0.0,
                vec![FieldValue::F(-95.0), FieldValue::F(0.0), FieldValue::U(hp)],
            ),
        ]
    };
    let d = c.patch(world(0), 0);
    assert_eq!(d.creates.len(), 5, "creates are never held");

    let mut updates: BTreeMap<u32, u32> = BTreeMap::new();
    for tick in 1..=40 {
        let d = c.patch(world(tick), tick);
        for u in &d.updates {
            *updates.entry(u.id).or_insert(0) += 1;
        }
        // Every entity shows a state at most its interval - 1 patches old.
        for (id, every) in [(1, 1), (2, 2), (3, 4), (4, 1), (5, 1)] {
            assert!(hp(&c, id) + every > tick, "entity {id} at tick {tick}");
        }
    }
    assert_eq!(updates[&1], 40, "near: every patch");
    assert_eq!(updates[&2], 20, "mid: every 2nd");
    assert_eq!(updates[&3], 10, "far: every 4th");
    assert_eq!(updates[&4], 40, "global: never held");
    assert_eq!(updates[&5], 40, "every_patch kind: never held");

    // The last change arrives within 4 patches, then nothing more is sent.
    for tick in 41..=44 {
        c.patch(world(40), tick);
    }
    assert!((1..=5).all(|id| hp(&c, id) == 40));
    let d = c.patch(world(40), 45);
    assert!(d.updates.is_empty());
}

#[test]
fn update_rates_never_hold_entering_spawning_or_leaving_entities() {
    let mut c = Client::with_rates(View::new(0.0, 0.0, 100.0), 1 << 20, UpdateRates::default());
    c.patch(vec![thing(1, 95.0, 0.0, 1)], 0);
    // A far spawn is created at once, whatever the patch.
    for tick in 1..=4 {
        let d = c.patch(
            vec![thing(1, 95.0, 0.0, 1), thing(10 + tick, 0.0, 95.0, 1)],
            tick,
        );
        assert_eq!(
            d.creates.iter().map(|e| e.id).collect::<Vec<_>>(),
            vec![10 + tick]
        );
        assert_eq!(d.removes, if tick == 1 { vec![] } else { vec![9 + tick] });
    }
    // Leaving the view (beyond 110) and despawning are removed at once.
    let d = c.patch(vec![thing(1, 111.0, 0.0, 2)], 5);
    assert_eq!(d.removes, vec![1, 14]);
    assert!(c.mirror.entities.is_empty());
}
