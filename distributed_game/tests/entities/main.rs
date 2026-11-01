//! Zone entities projected into views (`distributed_game::entity`):
//! `#[replicated(.., of = T)]` + `#[from]`, `Project`, and the host
//! projecting registered collections before `Zone::replicate`.

use std::collections::BTreeMap;
use std::sync::Arc;

use distributed_game::{
    schema, Ctx, Entities, Entity, EntityId, EntitySchema, FieldValue, HostConfig, ManualClock,
    PlayerId, Project, Replicated, WorldView, Zone, ZoneHost,
};

#[derive(Debug, Clone, Copy, Default)]
struct Body {
    x: f32,
    y: f32,
}

/// Server state: `home` and `hp` never leave the zone as such.
struct Mob {
    id: EntityId,
    body: Body,
    home: (f32, f32),
    hp: i32,
    max_hp: i32,
}

impl Entity for Mob {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "mob", id = 1, of = Mob)]
struct MobView {
    #[from(body.x)]
    x: f32,
    #[from(body.y)]
    y: f32,
    /// HP percent.
    hp: u8,
}

impl Project<MobView> for Mob {
    fn view(&self) -> Option<MobView> {
        (self.hp > 0).then(|| {
            MobView::project(
                self,
                MobViewRest {
                    hp: (self.hp * 100 / self.max_hp) as u8,
                },
            )
        })
    }
}

/// Every field `#[from]`: the `Project` impl is generated.
#[derive(Debug, Clone, Copy)]
struct Rock {
    id: EntityId,
    x: f32,
    y: f32,
    r: f32,
}

impl Entity for Rock {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "rock", id = 2, global, of = Rock)]
struct RockView {
    #[from]
    x: f32,
    #[from]
    y: f32,
    #[from]
    r: f32,
}

/// The zone projects its own HUD numbers.
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "hud", id = 3, global, of = Field)]
struct HudView {
    alive: u8,
}

/// A view without `of`, pushed by hand in `replicate`.
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "marker", id = 4)]
struct MarkerView {
    x: f32,
    y: f32,
}

struct Field {
    mobs: Vec<Mob>,
    rocks: BTreeMap<u8, Rock>,
    boss: Option<Mob>,
}

impl Entity for Field {
    fn entity_id(&self) -> EntityId {
        EntityId(900)
    }
}

impl Project<HudView> for Field {
    fn view(&self) -> Option<HudView> {
        let alive = self.mobs.iter().filter(|m| m.hp > 0).count();
        Some(HudView::project(self, HudViewRest { alive: alive as u8 }))
    }
}

fn mob(id: u32, x: f32, hp: i32) -> Mob {
    Mob {
        id: EntityId(id),
        body: Body { x, y: -x },
        home: (x, x),
        hp,
        max_hp: 40,
    }
}

impl Zone for Field {
    type Config = ();
    type Input = ();
    type Timer = ();
    type Message = distributed_game::NoMessages;

    fn schema() -> EntitySchema {
        schema![MobView, RockView, HudView, MarkerView]
    }

    fn entities() -> Entities<Self> {
        Entities::new()
            .each(|z: &Self| &z.mobs)
            .each(|z: &Self| &z.rocks)
            .each(|z: &Self| &z.boss)
            .one(|z: &Self| z)
    }

    fn on_create(_ctx: &mut Ctx<'_, Self>, _config: ()) -> Self {
        Self {
            mobs: vec![mob(1, 1.0, 40), mob(2, 2.0, 0), mob(3, 3.0, 10)],
            rocks: [(7u8, 5.0f32), (3, 4.0)]
                .into_iter()
                .map(|(k, x)| {
                    (
                        k,
                        Rock {
                            id: EntityId(100 + u32::from(k)),
                            x,
                            y: 0.0,
                            r: 0.5,
                        },
                    )
                })
                .collect(),
            boss: Some(mob(50, 9.0, 20)),
        }
    }

    fn decode_input(&self, _player: PlayerId, _payload: &[u8]) -> Option<()> {
        None
    }

    fn on_tick(&mut self, _ctx: &mut Ctx<'_, Self>) {}

    fn replicate(&self, out: &mut WorldView) {
        out.add(EntityId(77), &MarkerView { x: 0.5, y: 0.5 });
    }
}

#[test]
fn registered_collections_are_projected_in_order_then_replicate_runs() {
    let host = ZoneHost::<Field>::new(
        "field",
        HostConfig::default(),
        Arc::new(ManualClock::new()),
        (),
    );
    let view = host.world_view();
    let ids: Vec<(u32, u16)> = view.entities().iter().map(|e| (e.id, e.kind)).collect();
    assert_eq!(
        ids,
        [
            (1, 1),   // mobs in Vec order; mob 2 is dead: no view
            (3, 1),   //
            (103, 2), // rocks in BTreeMap key order (3, 7)
            (107, 2), //
            (50, 1),  // the Option
            (900, 3), // the zone itself
            (77, 4),  // replicate() last
        ]
    );
    let mob3 = &view.entities()[1];
    assert_eq!(
        mob3.fields,
        [FieldValue::F(3.0), FieldValue::F(-3.0), FieldValue::U(25)],
        "#[from(body.x)] copied, hp derived"
    );
    assert_eq!((mob3.x, mob3.y), (3.0, -3.0));
    assert!(view.entities()[2].global, "kind flags carry over");
    assert_eq!(view.entities()[5].fields, [FieldValue::U(2)]);
    // Unreplicated server state stays put.
    assert_eq!(host.zone().mobs[0].home, (1.0, 1.0));
}

#[test]
fn hand_written_replicate_can_project_entities_too() {
    let mut out = WorldView::default();
    let dead = mob(9, 0.0, 0);
    assert!(out.project::<MobView, _>(&dead).is_none(), "None omits");
    out.project::<MobView, _>(&mob(8, 1.0, 40))
        .expect("alive")
        .priority(3.0);
    let rock = Rock {
        id: EntityId(5),
        x: 1.0,
        y: 2.0,
        r: 0.25,
    };
    out.project::<RockView, _>(&rock);
    assert_eq!(out.entities().len(), 2);
    assert_eq!(out.entities()[0].priority, 3.0);
    assert_eq!(
        RockView::project(&rock),
        RockView {
            x: 1.0,
            y: 2.0,
            r: 0.25
        }
    );
    assert_eq!(rock.view(), Some(RockView::project(&rock)));
}

#[test]
fn entity_ids_convert_and_display_as_the_raw_id() {
    let id = EntityId::from(1002);
    assert_eq!(u32::from(id), 1002);
    assert_eq!(id.get(), 1002);
    assert_eq!(id.to_string(), "1002");
}
