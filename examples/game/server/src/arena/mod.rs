//! **Arena** — the baseline netcode demo.
//!
//! Mechanics on display: fixed 30 Hz tick / 20 Hz patches, binary deltas,
//! interest management (14 m view radius on a 40×40 m floor: far drones are
//! culled at the ring's edge; players are global so you always see everyone
//! in the room), client prediction + reconciliation of the
//! own capsule ([`movement`] is shared with the TypeScript client) and
//! interpolation of everything else (players, ~30 wandering drones).
//!
//! Entities:
//!
//! | kind | ids | fields | notes |
//! |---|---|---|---|
//! | 1 `player` | 1000 + player | x y vx vy dash cd name | global; full [`MoveState`] so clients can rebase |
//! | 2 `drone`  | 1..=30 | x y yaw | wander between random targets |
//! | 3 `pillar` | 100 + i | x y r | global (visible everywhere) |
//!
//! Event [`EVENT_HELLO`] (JSON) tells a joining player its entity and the
//! view radius. Inputs are 3 bytes ([`MoveInput`]); each input is one
//! command frame. A player earns one frame credit per tick (max
//! [`MAX_CREDITS`]), so a client cannot move faster than real time by
//! sending more inputs, but jitter bursts are absorbed.

pub mod movement;

use std::collections::BTreeMap;

use distributed_game::net::{GameServer, ServerConfig};
use distributed_game::{
    ConsumePolicy, Ctx, EntitySchema, EntityState, FieldType, FieldValue, KindDef, LeaveReason,
    PlayerId, View, WorldView, Zone,
};
use serde_json::json;

use crate::common::{send_json_event, Demo, Guests};
pub use movement::{MoveInput, MoveState};

/// Player kind.
pub const KIND_PLAYER: u16 = 1;
/// Drone kind.
pub const KIND_DRONE: u16 = 2;
/// Pillar kind.
pub const KIND_PILLAR: u16 = 3;
/// Event type: `{"entity": u32, "viewRadius": f32, "half": f32}` (JSON).
pub const EVENT_HELLO: u32 = 1;
/// Interest radius (m).
pub const VIEW_RADIUS: f32 = 14.0;
/// Wandering drones.
pub const DRONES: u32 = 30;
/// Most command frames a player may bank (absorbs input jitter).
pub const MAX_CREDITS: u8 = 4;
const DRONE_SPEED: f32 = 2.5;

struct Player {
    entity: u32,
    name: String,
    state: MoveState,
    credits: u8,
}

struct Drone {
    id: u32,
    x: f32,
    y: f32,
    tx: f32,
    ty: f32,
    yaw: f32,
}

/// Zone construction parameters.
#[derive(Clone, Default)]
pub struct ArenaConfig {
    /// Display names registered by the dev ticket endpoint.
    pub guests: Guests,
}

/// The Arena zone.
pub struct Arena {
    guests: Guests,
    players: BTreeMap<PlayerId, Player>,
    drones: Vec<Drone>,
}

/// Spawn points on a ring between the pillars.
fn spawn_point(player: PlayerId) -> (f32, f32) {
    const SPOTS: [(f32, f32); 8] = [
        (-5.0, -5.0),
        (5.0, -5.0),
        (5.0, 5.0),
        (-5.0, 5.0),
        (0.0, -7.0),
        (7.0, 0.0),
        (0.0, 7.0),
        (-7.0, 0.0),
    ];
    SPOTS[player.0 as usize % SPOTS.len()]
}

fn free_point(ctx: &mut Ctx<'_, Arena>) -> (f32, f32) {
    loop {
        let x = (ctx.rng().next_f32() * 2.0 - 1.0) * (movement::HALF - 1.0);
        let y = (ctx.rng().next_f32() * 2.0 - 1.0) * (movement::HALF - 1.0);
        let clear = movement::PILLARS
            .iter()
            .all(|(px, py, pr)| (x - px).powi(2) + (y - py).powi(2) > (pr + 1.0).powi(2));
        if clear {
            return (x, y);
        }
    }
}

impl Zone for Arena {
    type Config = ArenaConfig;
    type Input = MoveInput;
    type Timer = ();

    fn schema() -> EntitySchema {
        let small = FieldType::QuantF32 {
            min: -movement::HALF,
            max: movement::HALF,
            bits: 14,
        };
        EntitySchema::new()
            .kind(
                KindDef::new(KIND_PLAYER, "player")
                    .field("x", movement::POS)
                    .field("y", movement::POS)
                    .field("vx", movement::VEL)
                    .field("vy", movement::VEL)
                    .field("dash", FieldType::U8)
                    .field("cd", FieldType::U8)
                    .field_with_priority("name", FieldType::ShortStr, 0.2)
                    .priority(2.0),
            )
            .kind(
                KindDef::new(KIND_DRONE, "drone")
                    .field("x", small)
                    .field("y", small)
                    .field("yaw", FieldType::Angle { bits: 8 }),
            )
            .kind(
                KindDef::new(KIND_PILLAR, "pillar")
                    .field("x", FieldType::F32)
                    .field("y", FieldType::F32)
                    .field("r", FieldType::F32),
            )
    }

    fn on_create(ctx: &mut Ctx<'_, Self>, config: ArenaConfig) -> Self {
        let mut arena = Self {
            guests: config.guests,
            players: BTreeMap::new(),
            drones: Vec::new(),
        };
        for id in 1..=DRONES {
            let (x, y) = free_point(ctx);
            let (tx, ty) = free_point(ctx);
            arena.drones.push(Drone {
                id,
                x,
                y,
                tx,
                ty,
                yaw: 0.0,
            });
        }
        arena
    }

    fn on_join(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        let principal = ctx
            .player(player)
            .map(|p| p.principal.clone())
            .unwrap_or_default();
        let name = self.guests.name(&principal).unwrap_or(principal);
        let (x, y) = spawn_point(player);
        let entity = 1000 + player.0;
        self.players.insert(
            player,
            Player {
                entity,
                name,
                state: MoveState {
                    x,
                    y,
                    ..MoveState::default()
                },
                credits: MAX_CREDITS,
            },
        );
        ctx.set_view(player, View::new(x, y, VIEW_RADIUS));
        send_json_event(
            ctx,
            player,
            EVENT_HELLO,
            &json!({ "entity": entity, "viewRadius": VIEW_RADIUS, "half": movement::HALF }),
        );
    }

    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, player: PlayerId, _reason: LeaveReason) {
        self.players.remove(&player);
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<MoveInput> {
        MoveInput::decode(payload)
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        for (id, player) in &mut self.players {
            player.credits = (player.credits + 1).min(MAX_CREDITS);
            while player.credits > 0 {
                let Some(input) = ctx.take_inputs(*id, ConsumePolicy::Next).pop() else {
                    break;
                };
                player.state = movement::step(player.state, input.input);
                player.credits -= 1;
            }
            ctx.set_view(*id, View::new(player.state.x, player.state.y, VIEW_RADIUS));
        }
        let dt = ctx.dt_secs();
        for i in 0..self.drones.len() {
            let d = &self.drones[i];
            let (ox, oy) = (d.tx - d.x, d.ty - d.y);
            let dist = (ox * ox + oy * oy).sqrt();
            if dist < 0.3 {
                let (tx, ty) = free_point(ctx);
                let d = &mut self.drones[i];
                d.tx = tx;
                d.ty = ty;
                continue;
            }
            let d = &mut self.drones[i];
            let stride = (DRONE_SPEED * dt).min(dist);
            d.x += ox / dist * stride;
            d.y += oy / dist * stride;
            d.yaw = oy.atan2(ox);
        }
    }

    fn replicate(&self, out: &mut WorldView) {
        for p in self.players.values() {
            let s = p.state;
            out.push(
                EntityState::new(
                    p.entity,
                    KIND_PLAYER,
                    s.x,
                    s.y,
                    vec![
                        FieldValue::F(s.x),
                        FieldValue::F(s.y),
                        FieldValue::F(s.vx),
                        FieldValue::F(s.vy),
                        FieldValue::U(s.dash.into()),
                        FieldValue::U(s.cooldown.into()),
                        FieldValue::Str(p.name.clone()),
                    ],
                )
                // Players are global: a small room shows everyone; only drones are
                // culled by the view radius.
                .global(),
            );
        }
        for d in &self.drones {
            out.push(
                EntityState::new(
                    d.id,
                    KIND_DRONE,
                    d.x,
                    d.y,
                    vec![FieldValue::F(d.x), FieldValue::F(d.y), FieldValue::F(d.yaw)],
                )
                .priority(0.5),
            );
        }
        for (i, (x, y, r)) in movement::PILLARS.iter().enumerate() {
            out.push(
                EntityState::new(
                    100 + i as u32,
                    KIND_PILLAR,
                    *x,
                    *y,
                    vec![FieldValue::F(*x), FieldValue::F(*y), FieldValue::F(*r)],
                )
                .global(),
            );
        }
    }
}

/// Bot payload for `distributed_game::bots`: direction → [`MoveInput`].
pub fn encode_bot_input(dir: [f32; 2]) -> Vec<u8> {
    let axis = |v: f32| (v.clamp(-1.0, 1.0) * 127.0).round() as i8;
    MoveInput {
        mx: axis(dir[0]),
        my: axis(dir[1]),
        buttons: 0,
    }
    .encode()
    .to_vec()
}

/// Demo registration (`game-demos arena`).
pub struct ArenaDemo;

impl Demo for ArenaDemo {
    type Zone = Arena;
    const NAME: &'static str = "arena";
    const DEFAULT_ZONE: &'static str = "arena-1";

    fn hosts_zone(zone_id: &str) -> bool {
        zone_id.starts_with("arena-")
    }

    fn server(config: ServerConfig, guests: Guests) -> GameServer<Arena> {
        GameServer::new(config, move |zone_id| {
            Self::hosts_zone(zone_id).then(|| ArenaConfig {
                guests: guests.clone(),
            })
        })
    }
}
