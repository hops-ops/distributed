//! **Arena** — the baseline netcode demo.
//!
//! Mechanics on display: fixed 30 Hz tick / 20 Hz patches, binary deltas,
//! interest management (14 m view radius on a 40×40 m floor: far drones are
//! culled at the ring's edge; players are global so you always see everyone
//! in the room), client prediction + reconciliation of the
//! own capsule ([`movement`], the `game_sim` step the browser runs as
//! WebAssembly) and
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
//! Kinds are `#[derive(Replicated)]` views ([`PlayerView`], [`DroneView`],
//! [`PillarView`]); [`ArenaEvent::Hello`] tells a joining player its entity
//! and the view radius. Inputs are 3 bytes ([`MoveInput`], a
//! `#[derive(GameInput)]`); each input is one command frame. The browser
//! client is generated from [`game_surface`] (`run.sh gen`). A player earns one frame credit per tick (max
//! [`MAX_CREDITS`], `distributed_game::InputCredits`), so a client cannot
//! move faster than real time by sending more inputs, but jitter bursts are
//! absorbed.

/// The shared movement step and its `#[predict]` declaration.
pub use game_sim::arena as movement;

use std::collections::BTreeMap;

use distributed_game::net::{GameServer, ServerConfig, ZoneFactory};
use distributed_game::{
    schema, Ctx, Entity, EntityId, EntitySchema, GameEvent, GameInput, GameSurface, InputCredits,
    LeaveReason, PlayerId, Replicated, View, WorldView, Zone,
};

use crate::common::{Demo, Guests};
pub use movement::{MoveInput, MoveState};

/// A player's capsule: the full [`MoveState`] (so the client can rebase its
/// prediction) plus the display name. Global: everyone in the room is seen.
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "player", id = 1, priority = 2.0, global)]
pub struct PlayerView {
    /// Position x.
    #[field_type(movement::POS)]
    #[correct(ease = 150)]
    pub x: f32,
    /// Position y.
    #[field_type(movement::POS)]
    #[correct(ease = 150)]
    pub y: f32,
    /// Velocity x.
    #[field_type(movement::VEL)]
    #[correct(snap)]
    pub vx: f32,
    /// Velocity y.
    #[field_type(movement::VEL)]
    #[correct(snap)]
    pub vy: f32,
    /// Remaining dash frames.
    #[correct(snap)]
    pub dash: u8,
    /// Remaining dash cooldown frames.
    #[correct(snap)]
    pub cd: u8,
    /// Display name.
    #[priority(0.2)]
    #[correct(replace)]
    pub name: String,
}

impl PlayerView {
    /// The view of a capsule.
    pub fn of(state: &MoveState, name: &str) -> Self {
        Self {
            x: state.x,
            y: state.y,
            vx: state.vx,
            vy: state.vy,
            dash: state.dash,
            cd: state.cd,
            name: name.to_string(),
        }
    }
}

/// A wandering drone (culled by the view radius).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "drone", id = 2)]
pub struct DroneView {
    /// Position x.
    #[quant(min = -movement::HALF, max = movement::HALF, bits = 14)]
    pub x: f32,
    /// Position y.
    #[quant(min = -movement::HALF, max = movement::HALF, bits = 14)]
    pub y: f32,
    /// Heading.
    #[angle(bits = 8)]
    pub yaw: f32,
}

/// A static pillar of the shared floor ([`movement::PILLARS`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pillar {
    /// Entity id (100 + index).
    pub id: EntityId,
    /// Centre x.
    pub x: f32,
    /// Centre y.
    pub y: f32,
    /// Radius.
    pub r: f32,
}

impl Pillar {
    /// Every pillar of the floor.
    pub fn all() -> Vec<Pillar> {
        movement::PILLARS
            .iter()
            .enumerate()
            .map(|(i, &(x, y, r))| Pillar {
                id: EntityId(100 + i as u32),
                x,
                y,
                r,
            })
            .collect()
    }
}

impl Entity for Pillar {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

/// A static pillar (visible everywhere), a straight projection of [`Pillar`].
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "pillar", id = 3, global, of = Pillar)]
pub struct PillarView {
    /// Centre x.
    #[from]
    pub x: f32,
    /// Centre y.
    #[from]
    pub y: f32,
    /// Radius.
    #[from]
    pub r: f32,
}

/// The Arena's events.
#[derive(Debug, Clone, PartialEq, GameEvent)]
pub enum ArenaEvent {
    /// To a joining player: its entity, the interest radius and floor size.
    #[game_event(id = 1)]
    Hello {
        /// Own entity id.
        entity: u32,
        /// Interest radius (m).
        view_radius: f32,
        /// Floor half extent (m).
        half: f32,
    },
}

/// The Arena's client surface (`distributed game-manifest --entrypoint
/// game_demos::arena::game_surface`).
pub fn game_surface() -> GameSurface {
    GameSurface::for_zone::<Arena>("arena")
        .events::<ArenaEvent>()
        .predict::<movement::ArenaStep>()
        .constant("arena_floor", &movement::arena_floor())
}

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

impl Arena {
    /// Tell `player`'s client session its entity and set its view.
    fn greet(&self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        let Some(p) = self.players.get(&player) else {
            return;
        };
        ctx.set_view(player, View::new(p.state.x, p.state.y, VIEW_RADIUS));
        ctx.send(
            player,
            &ArenaEvent::Hello {
                entity: p.entity,
                view_radius: VIEW_RADIUS,
                half: movement::HALF,
            },
        );
    }
}

/// The Arena zone.
pub struct Arena {
    guests: Guests,
    players: BTreeMap<PlayerId, Player>,
    credits: InputCredits,
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
    type Message = distributed_game::NoMessages;

    fn schema() -> EntitySchema {
        schema![PlayerView, DroneView, PillarView]
    }

    fn on_create(ctx: &mut Ctx<'_, Self>, config: ArenaConfig) -> Self {
        let mut arena = Self {
            guests: config.guests,
            players: BTreeMap::new(),
            credits: InputCredits::new(u32::from(MAX_CREDITS)),
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
            },
        );
        self.greet(ctx, player);
    }

    /// A reloaded page (fresh ticket for a slot it still holds): keep the
    /// body where it is, greet the new client session.
    fn on_rejoin(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        if self.players.contains_key(&player) {
            self.greet(ctx, player);
        } else {
            self.on_join(ctx, player);
        }
    }

    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, player: PlayerId, _reason: LeaveReason) {
        self.players.remove(&player);
        self.credits.remove(player);
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<MoveInput> {
        MoveInput::from_payload(payload)
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        for (id, player) in &mut self.players {
            for input in self.credits.take(ctx, *id) {
                // The declared predicted step: what the client runs as WASM.
                player.state = movement::arena_step(&player.state, &input.input, &(), movement::DT);
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
            out.add(p.entity, &PlayerView::of(&p.state, &p.name));
        }
        for d in &self.drones {
            let view = DroneView {
                x: d.x,
                y: d.y,
                yaw: d.yaw,
            };
            out.add(d.id, &view).priority(0.5);
        }
        for pillar in Pillar::all() {
            out.project::<PillarView, _>(&pillar);
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
    .to_payload()
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
        GameServer::new(
            config,
            ZoneFactory::sync(move |activation| {
                let zone_id = activation.zone_id.as_str();
                Self::hosts_zone(zone_id).then(|| ArenaConfig {
                    guests: guests.clone(),
                })
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use distributed_game::{HostConfig, ManualClock, Role, TicketClaims, Viewer, ZoneHost};

    use super::*;

    #[test]
    fn a_reload_keeps_the_body() {
        let clock = ManualClock::new();
        let mut host = ZoneHost::<Arena>::new(
            "arena",
            HostConfig::default(),
            Arc::new(clock.clone()),
            ArenaConfig {
                guests: Guests::default(),
            },
        );
        let claims = TicketClaims::new("ana", "arena", u64::MAX, "n1", Role::Player);
        let joined = host.join(&claims, 0, "").expect("join");
        let Viewer::Player(player) = joined.viewer else {
            panic!("player")
        };
        for seq in 1..=20u32 {
            let input = MoveInput {
                mx: 127,
                my: 0,
                buttons: 0,
            };
            host.push_input(joined.client, seq, seq, &input.to_payload());
            clock.advance(Duration::from_millis(33));
            host.advance();
        }
        let before = host.zone().players[&player].state;
        host.disconnect(joined.client, false);
        let claims = TicketClaims::new("ana", "arena", u64::MAX, "n2", Role::Player);
        let again = host.join(&claims, 0, "").expect("rejoin");
        assert_eq!(again.viewer, Viewer::Player(player));
        clock.advance(Duration::from_millis(33));
        host.advance();
        let after = host.zone().players[&player].state;
        assert_eq!((before.x, before.y), (after.x, after.y), "same body");
    }
}
