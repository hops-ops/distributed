//! **Endless Grid** — an unbounded world of square cells, one zone per cell.
//!
//! Mechanics on display:
//!
//! - **Seeded procedural content** ([`world`]): terrain, biomes and props of
//!   a cell are a pure function of `(seed, cell, CONTENT_VERSION)`,
//!   generated identically by the server (collision) and the client
//!   (rendering); nothing static is replicated.
//! - **Spatial cells as zones**: zone `grid.<x>.<y>` simulates the walkers
//!   in its 32 m square plus a few wandering critters. Walking past a
//!   border (with [`world::HYSTERESIS`]) hands the walker off to the
//!   neighbour (`ctx.handoff`, carrying its position in the target's
//!   frame and its name); the client follows the Handoff.
//! - **Interest across borders**: clients observe the 8 neighbouring zones
//!   (observer connections), so critters and walkers across a border are
//!   visible before you cross.
//! - **Cross-host handoff**: zones are placed on hosts by a consistent-hash
//!   ring ([`HashRing`]); a Hello for a foreign zone gets `Redirect`, a
//!   handoff into it reserves the transfer ticket with the issuing host.
//! - **Hibernation**: a zone nobody is in or observing turns Dormant, then
//!   hibernates into SQLite (critter positions and dropped markers) and
//!   restores on the next activation; its hello says `restored`.
//!
//! Zone ids: `grid.<x>.<y>` with canonical integers, |x|, |y| ≤ 1 000 000.
//! The dots are not a hierarchy: the server uses `FlatTopology` (the
//! default `DottedTopology` would make `grid.3` the parent of `grid.3.5`).
//! The ring hashes the full id, so neighbouring cells land on either host.
//!
//! | kind | ids | fields | notes |
//! |---|---|---|---|
//! | 1 `walker` | 1000 + player | x y name hue | local coordinates |
//! | 2 `critter` | 1.. | x y yaw hue | wander, persisted on hibernation |
//! | 3 `marker` | 500.. | x y label hue | dropped with E, persisted |
//!
//! The kinds are `#[derive(Replicated)]` views, the hello a struct-form
//! `#[derive(GameEvent)]` ([`GridHello`]); the browser client is generated
//! from [`game_surface`].
//!
//! Every entity is global within its zone (a cell is small); interest
//! between zones is the client's choice of observed neighbours.

pub mod dev;
/// The procedural world, the walker step and its `#[predict]` declaration.
pub use game_sim::grid as world;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use distributed_game::net::{GameServer, ServerConfig, ZoneFactory};
use distributed_game::world::{FlatTopology, HashRing, HostAddr, ZoneDirectory};
use distributed_game::{
    schema, ConsumePolicy, Ctx, EntitySchema, GameEvent, GameInput, GameSurface, LeaveReason,
    ObserverId, PlayerId, Replicated, WorldView, Zone,
};
use serde::{Deserialize, Serialize};

use crate::cluster::{ClusterDemo, ClusterOptions, World};
use crate::common::{Demo, Guests};
use world::{MoveInput, Prop, CELL, POS};

/// A walker (a player), in cell-local coordinates.
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "walker", id = 1, priority = 2.0, global)]
pub struct WalkerView {
    /// Local x.
    #[field_type(POS)]
    #[correct(ease = 120)]
    pub x: f32,
    /// Local y.
    #[field_type(POS)]
    #[correct(ease = 120)]
    pub y: f32,
    /// Display name.
    #[priority(0.2)]
    #[correct(replace)]
    pub name: String,
    /// Colour.
    #[correct(replace)]
    pub hue: u8,
}

/// A wandering critter (persisted on hibernation).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "critter", id = 2, global)]
pub struct CritterView {
    /// Local x.
    #[field_type(POS)]
    pub x: f32,
    /// Local y.
    #[field_type(POS)]
    pub y: f32,
    /// Heading.
    #[angle(bits = 8)]
    pub yaw: f32,
    /// Colour.
    pub hue: u8,
}

/// A dropped marker (persisted on hibernation).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "marker", id = 3, global)]
pub struct MarkerView {
    /// Local x.
    #[field_type(POS)]
    pub x: f32,
    /// Local y.
    #[field_type(POS)]
    pub y: f32,
    /// Who dropped it.
    pub label: String,
    /// Colour.
    pub hue: u8,
}

/// Type id of [`GridHello`].
pub const EVENT_HELLO: u32 = GridHello::EVENT_ID;

/// The grid's client surface.
pub fn game_surface() -> GameSurface {
    GameSurface::for_zone::<GridZone>("grid")
        .events::<GridHello>()
        .predict::<world::GridStep>()
        .pure::<world::GridCellFn>()
        .pure::<world::GridSampleFn>()
        .constant("grid_world", &world::grid_world())
}
/// Most markers a cell keeps (oldest dropped first).
pub const MAX_MARKERS: usize = 12;
/// Most command frames a walker may bank.
pub const MAX_CREDITS: u8 = 4;
const CRITTER_SPEED: f32 = 1.6;
const CRITTER_RADIUS: f32 = 0.3;
const MARK_COOLDOWN: u8 = 15;

/// What a zone tells each player / observer on arrival.
#[derive(Debug, Clone, PartialEq, GameEvent)]
#[game_event(id = 1, name = "hello")]
pub struct GridHello {
    /// Zone id.
    pub zone: String,
    /// Cell coordinates `[x, y]`.
    pub cell: Vec<i32>,
    /// Host running the zone.
    pub host: String,
    /// World seed (the client generates the same content).
    pub seed: u32,
    /// Generation version.
    pub content_version: u32,
    /// Cell edge (m).
    pub cell_size: f32,
    /// The player's own walker entity (players only).
    pub entity: Option<u32>,
    /// This activation restored a hibernation snapshot.
    pub restored: bool,
    /// Times this cell was restored (persisted).
    pub restores: u32,
    /// How long the snapshot slept (ms), when restored.
    pub hibernated_for_ms: Option<u64>,
    /// Critters / markers in the restored snapshot.
    pub restored_critters: u32,
    /// Markers in the restored snapshot.
    pub restored_markers: u32,
    /// Activation start (Unix ms): identifies this activation.
    pub activated_at: u64,
}

struct Walker {
    entity: u32,
    name: String,
    hue: u8,
    x: f32,
    y: f32,
    credits: u8,
    mark_cooldown: u8,
    leaving: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Critter {
    id: u32,
    x: f32,
    y: f32,
    tx: f32,
    ty: f32,
    yaw: f32,
    hue: u8,
    rest: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Marker {
    id: u32,
    x: f32,
    y: f32,
    label: String,
    hue: u8,
}

/// Hibernation snapshot (JSON).
#[derive(Debug, Serialize, Deserialize)]
struct Snapshot {
    critters: Vec<Critter>,
    markers: Vec<Marker>,
    next_marker: u32,
    restores: u32,
    saved_at_ms: u64,
}

/// Zone construction parameters.
#[derive(Clone)]
pub struct GridConfig {
    /// Display names.
    pub guests: Guests,
    /// Host running the zone (shown to clients).
    pub host_id: String,
    /// World seed.
    pub seed: u32,
    /// The cell.
    pub cell: (i32, i32),
}

/// One cell of the grid.
pub struct GridZone {
    config: GridConfig,
    props: Vec<Prop>,
    walkers: BTreeMap<PlayerId, Walker>,
    critters: Vec<Critter>,
    markers: Vec<Marker>,
    next_marker: u32,
    restores: u32,
    restored: Option<(u64, u32, u32)>,
    activated_at: u64,
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn hue_of(principal: &str) -> u8 {
    let h = principal.bytes().fold(0x811c_9dc5u32, |h, b| {
        (h ^ u32::from(b)).wrapping_mul(0x0100_0193)
    });
    (world::hash32(h) >> 24) as u8
}

impl GridZone {
    /// Walkers as `(name, x, y)` (ops tooling and tests).
    pub fn walkers(&self) -> Vec<(String, f32, f32)> {
        self.walkers
            .values()
            .map(|w| (w.name.clone(), w.x, w.y))
            .collect()
    }

    /// Dropped markers.
    pub fn marker_count(&self) -> usize {
        self.markers.len()
    }

    fn free_point(&self, ctx: &mut Ctx<'_, Self>, margin: f32) -> (f32, f32) {
        for _ in 0..32 {
            let x = 1.0 + ctx.rng().next_f32() * (CELL - 2.0);
            let y = 1.0 + ctx.rng().next_f32() * (CELL - 2.0);
            let clear = self
                .props
                .iter()
                .filter(|p| p.solid)
                .all(|p| (x - p.x).powi(2) + (y - p.y).powi(2) > (p.r + margin).powi(2));
            if clear {
                return (x, y);
            }
        }
        // Sub-cell corners are always free.
        (CELL / 2.0, CELL / 2.0)
    }

    fn hello(&self, ctx: &Ctx<'_, Self>, entity: Option<u32>) -> GridHello {
        GridHello {
            zone: ctx.zone_id().to_string(),
            cell: vec![self.config.cell.0, self.config.cell.1],
            host: self.config.host_id.clone(),
            seed: self.config.seed,
            content_version: world::CONTENT_VERSION,
            cell_size: CELL,
            entity,
            restored: self.restored.is_some(),
            restores: self.restores,
            hibernated_for_ms: self.restored.map(|(ms, _, _)| ms),
            restored_critters: self.restored.map_or(0, |(_, c, _)| c),
            restored_markers: self.restored.map_or(0, |(_, _, m)| m),
            activated_at: self.activated_at,
        }
    }

    fn admit(
        &mut self,
        ctx: &mut Ctx<'_, Self>,
        player: PlayerId,
        at: (f32, f32),
        name: Option<String>,
    ) {
        let principal = ctx
            .player(player)
            .map(|p| p.principal.clone())
            .unwrap_or_default();
        let name = self
            .config
            .guests
            .name(&principal)
            .or(name)
            .unwrap_or_else(|| format!("Guest {}", principal.get(6..10).unwrap_or("")));
        let entity = 1000 + player.0;
        self.walkers.insert(
            player,
            Walker {
                entity,
                name,
                hue: hue_of(&principal),
                x: world::snap(at.0),
                y: world::snap(at.1),
                credits: MAX_CREDITS,
                mark_cooldown: 0,
                leaving: false,
            },
        );
        let hello = self.hello(ctx, Some(entity));
        ctx.send(player, &hello);
    }

    fn drop_marker(&mut self, x: f32, y: f32, label: String, hue: u8) {
        if self.markers.len() >= MAX_MARKERS {
            self.markers.remove(0);
        }
        let id = 500 + self.next_marker % 400;
        self.next_marker = self.next_marker.wrapping_add(1);
        self.markers.push(Marker {
            id,
            x,
            y,
            label,
            hue,
        });
    }

    fn wander(&mut self, ctx: &mut Ctx<'_, Self>) {
        let dt = ctx.dt_secs();
        for i in 0..self.critters.len() {
            let c = &self.critters[i];
            if c.rest > 0 {
                self.critters[i].rest -= 1;
                continue;
            }
            let (ox, oy) = (c.tx - c.x, c.ty - c.y);
            let dist = (ox * ox + oy * oy).sqrt();
            if dist < 0.2 {
                let (tx, ty) = self.free_point(ctx, 0.8);
                let rest = 10 + (ctx.rng().next_f32() * 50.0) as u16;
                let c = &mut self.critters[i];
                (c.tx, c.ty, c.rest) = (tx, ty, rest);
                continue;
            }
            let stride = (CRITTER_SPEED * dt).min(dist);
            let (x, y) = world::collide(
                c.x + ox / dist * stride,
                c.y + oy / dist * stride,
                CRITTER_RADIUS,
                &self.props,
            );
            let c = &mut self.critters[i];
            c.yaw = oy.atan2(ox);
            (c.x, c.y) = (
                world::snap(x.clamp(0.3, CELL - 0.3)),
                world::snap(y.clamp(0.3, CELL - 0.3)),
            );
        }
    }
}

/// Carried state of a handoff: position in the target frame and the name.
pub fn encode_carried(x: f32, y: f32, name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + name.len());
    out.extend_from_slice(&x.to_le_bytes());
    out.extend_from_slice(&y.to_le_bytes());
    out.extend_from_slice(name.as_bytes());
    out
}

/// Inverse of [`encode_carried`]; None for a malformed or out-of-range payload.
pub fn decode_carried(carried: &[u8]) -> Option<(f32, f32, String)> {
    let x = f32::from_le_bytes(carried.get(0..4)?.try_into().ok()?);
    let y = f32::from_le_bytes(carried.get(4..8)?.try_into().ok()?);
    let range = -world::OUTSIDE_LIMIT..=CELL + world::OUTSIDE_LIMIT;
    if !range.contains(&x) || !range.contains(&y) {
        return None;
    }
    let name = std::str::from_utf8(carried.get(8..)?).ok()?;
    Some((x, y, name.chars().take(16).collect()))
}

impl Zone for GridZone {
    type Config = GridConfig;
    type Input = MoveInput;
    type Timer = ();

    fn schema() -> EntitySchema {
        schema![WalkerView, CritterView, MarkerView]
    }

    fn on_create(ctx: &mut Ctx<'_, Self>, config: GridConfig) -> Self {
        let props = world::cell_props(config.seed, config.cell);
        let count = 2 + world::mix(config.seed, 9, config.cell.0, config.cell.1) % 3;
        let mut zone = Self {
            config,
            props,
            walkers: BTreeMap::new(),
            critters: Vec::new(),
            markers: Vec::new(),
            next_marker: 0,
            restores: 0,
            restored: None,
            activated_at: unix_ms(),
        };
        for id in 1..=count {
            let (x, y) = zone.free_point(ctx, 0.8);
            let (tx, ty) = zone.free_point(ctx, 0.8);
            let hue = (ctx.rng().next_f32() * 255.0) as u8;
            zone.critters.push(Critter {
                id,
                x: world::snap(x),
                y: world::snap(y),
                tx,
                ty,
                yaw: 0.0,
                hue,
                rest: 0,
            });
        }
        zone
    }

    fn on_join(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        // Sub-cell corners are always free of props.
        self.admit(ctx, player, (CELL / 2.0, CELL / 2.0), None);
    }

    fn on_handoff_join(
        &mut self,
        ctx: &mut Ctx<'_, Self>,
        player: PlayerId,
        _from: &str,
        carried: Vec<u8>,
    ) {
        match decode_carried(&carried) {
            Some((x, y, name)) => self.admit(ctx, player, (x, y), Some(name)),
            None => self.admit(ctx, player, (CELL / 2.0, CELL / 2.0), None),
        }
    }

    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, player: PlayerId, _reason: LeaveReason) {
        self.walkers.remove(&player);
    }

    fn on_observe(&mut self, ctx: &mut Ctx<'_, Self>, observer: ObserverId) {
        let hello = self.hello(ctx, None);
        ctx.send(observer, &hello);
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<MoveInput> {
        MoveInput::from_payload(payload)
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        let ids: Vec<PlayerId> = self.walkers.keys().copied().collect();
        for id in ids {
            let mut marks = Vec::new();
            {
                let Some(w) = self.walkers.get_mut(&id) else {
                    continue;
                };
                if w.leaving {
                    continue;
                }
                w.credits = (w.credits + 1).min(MAX_CREDITS);
                w.mark_cooldown = w.mark_cooldown.saturating_sub(1);
                while w.credits > 0 {
                    let Some(input) = ctx.take_inputs(id, ConsumePolicy::Next).pop() else {
                        break;
                    };
                    let input = input.input;
                    // `world::grid_step` (the declared predicted step) with this
                    // cell's props, which the zone already holds.
                    (w.x, w.y) = world::step(w.x, w.y, input, &self.props);
                    w.credits -= 1;
                    if input.buttons & world::BUTTON_MARK != 0 && w.mark_cooldown == 0 {
                        w.mark_cooldown = MARK_COOLDOWN;
                        marks.push((w.x, w.y, w.name.clone(), w.hue));
                    }
                    if world::crossing(w.x, w.y).is_some() {
                        break;
                    }
                }
            }
            for (x, y, label, hue) in marks {
                self.drop_marker(x, y, label, hue);
            }
            let w = &self.walkers[&id];
            let Some((dx, dy)) = world::crossing(w.x, w.y) else {
                continue;
            };
            let (cx, cy) = (self.config.cell.0 + dx, self.config.cell.1 + dy);
            let target = world::zone_id((cx, cy));
            let carried = encode_carried(w.x - dx as f32 * CELL, w.y - dy as f32 * CELL, &w.name);
            let inside = cx.abs() <= world::MAX_CELL && cy.abs() <= world::MAX_CELL;
            let handed = inside && ctx.handoff(id, &target, carried).is_ok();
            let w = self.walkers.get_mut(&id).expect("walker");
            if handed {
                w.leaving = true;
            } else {
                // Edge of the world (or no issuer): stay inside.
                w.x = w.x.clamp(-world::HYSTERESIS, CELL + world::HYSTERESIS);
                w.y = w.y.clamp(-world::HYSTERESIS, CELL + world::HYSTERESIS);
            }
        }
        self.wander(ctx);
    }

    fn replicate(&self, out: &mut WorldView) {
        for w in &self.walkers {
            let w = w.1;
            let view = WalkerView {
                x: w.x,
                y: w.y,
                name: w.name.clone(),
                hue: w.hue,
            };
            out.add(w.entity, &view);
        }
        for c in &self.critters {
            let view = CritterView {
                x: c.x,
                y: c.y,
                yaw: c.yaw,
                hue: c.hue,
            };
            out.add(c.id, &view).priority(0.5);
        }
        for m in &self.markers {
            let view = MarkerView {
                x: m.x,
                y: m.y,
                label: m.label.clone(),
                hue: m.hue,
            };
            out.add(m.id, &view);
        }
    }

    fn hibernate(&self) -> Option<Vec<u8>> {
        serde_json::to_vec(&Snapshot {
            critters: self.critters.clone(),
            markers: self.markers.clone(),
            next_marker: self.next_marker,
            restores: self.restores,
            saved_at_ms: unix_ms(),
        })
        .ok()
    }

    fn on_restore(&mut self, _ctx: &mut Ctx<'_, Self>, state: &[u8], _saved_version: &str) {
        let Ok(snapshot) = serde_json::from_slice::<Snapshot>(state) else {
            return;
        };
        self.restored = Some((
            unix_ms().saturating_sub(snapshot.saved_at_ms),
            snapshot.critters.len() as u32,
            snapshot.markers.len() as u32,
        ));
        self.critters = snapshot.critters;
        self.markers = snapshot.markers;
        self.next_marker = snapshot.next_marker;
        self.restores = snapshot.restores + 1;
    }
}

/// Bot payload: direction → sprinting [`MoveInput`].
pub fn encode_bot_input(dir: [f32; 2]) -> Vec<u8> {
    let axis = |v: f32| (v.clamp(-1.0, 1.0) * 127.0).round() as i8;
    MoveInput {
        mx: axis(dir[0]),
        my: axis(dir[1]),
        buttons: world::BUTTON_SPRINT,
    }
    .to_payload()
}

/// World seed: `DG_GRID_SEED` (decimal or 0x-hex) or [`world::DEFAULT_SEED`].
pub fn seed_from_env() -> u32 {
    std::env::var("DG_GRID_SEED")
        .ok()
        .and_then(|v| {
            let v = v.trim();
            match v.strip_prefix("0x") {
                Some(hex) => u32::from_str_radix(hex, 16).ok(),
                None => v.parse().ok(),
            }
        })
        .unwrap_or(world::DEFAULT_SEED)
}

/// Demo registration (`game-demos grid`).
pub struct GridDemo;

impl Demo for GridDemo {
    type Zone = GridZone;
    const NAME: &'static str = "grid";
    const DEFAULT_ZONE: &'static str = "grid.0.0";

    fn hosts_zone(zone_id: &str) -> bool {
        world::parse_zone(zone_id).is_some()
    }

    /// Single host (used by `common::spawn`); `game-demos grid` runs
    /// [`crate::cluster::run_cluster`] instead.
    fn server(config: ServerConfig, guests: Guests) -> GameServer<GridZone> {
        let options = ClusterOptions::single("local", std::time::Duration::from_secs(4));
        let world = crate::cluster::World::standalone::<Self>(&options, guests);
        Self::cluster_server(config, &world)
    }
}

impl ClusterDemo for GridDemo {
    fn directory(hosts: &[HostAddr]) -> Arc<dyn ZoneDirectory> {
        Arc::new(HashRing::with_hosts(64, hosts.iter().cloned()))
    }

    fn cluster_server(mut config: ServerConfig, world: &World) -> GameServer<GridZone> {
        let seed = seed_from_env();
        config.host.world_seed = u64::from(seed);
        config.host.idle_tick_rate = Some(2);
        config.host.max_observers = 64;
        let guests = world.guests.clone();
        let host_id = world.host.id.clone();
        GameServer::new(
            config,
            ZoneFactory::sync(move |activation| {
                let zone_id = activation.zone_id.as_str();
                world::parse_zone(zone_id).map(|cell| GridConfig {
                    guests: guests.clone(),
                    host_id: host_id.clone(),
                    seed,
                    cell,
                })
            }),
        )
        .with_topology(Arc::new(FlatTopology))
    }

    fn cluster_routes(server: GameServer<GridZone>, world: Arc<World>) -> axum::Router {
        dev::routes(server, world)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn carried_state_round_trips_and_is_bounded() {
        let bytes = encode_carried(0.5, 31.0, "Ana");
        assert_eq!(decode_carried(&bytes), Some((0.5, 31.0, "Ana".into())));
        assert_eq!(decode_carried(&encode_carried(99.0, 1.0, "x")), None);
        assert_eq!(decode_carried(&[1, 2, 3]), None);
    }
}
