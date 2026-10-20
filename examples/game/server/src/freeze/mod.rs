//! **The Freeze** — a world-version barrier as an in-world story beat.
//!
//! Three zones on two server processes: the **Town Hub** (a safe room,
//! [`Zone::is_gate`]) and **Whisper Meadow** on the primary host, **Ember
//! Fields** on the second host. Their rules come from the content pack of
//! the zone's content version ([`content`]): lava pools that burn (or ice
//! that does not), slime speed, gem value. An administrator rolls out the
//! next version with [`world`]'s `/dev/freeze/rollout` (admin secret):
//! `WorldCoordinator::rollout` freezes every zone on both hosts (clients
//! show "SYSTEM: an administrator has paused the world…", inputs are
//! dropped and counted), the update is applied, every zone resumes on the
//! new version with the change notes.
//!
//! - **Hard**: every client is told `Kick(RELOAD)`, reloads the pack and
//!   reconnects with its token.
//! - **Rolling + SafeRoom** (`StalePolicy::SafeRoom { roam: false }`):
//!   clients that patch content live keep going; an old client (the page's
//!   "simulate old client") keeps playing its old pack in a field zone and is
//!   only reloaded when it walks through a portal into the hub gate.
//! - **Abort**: a zone with a ritual in progress refuses
//!   [`Zone::freeze_ready`]; the barrier times out (`NotAcked`) and every
//!   zone resumes on the old version ("update aborted").
//!
//! Movement is the Arena's step ([`crate::arena::movement::arena_step`]),
//! which the client predicts with, compiled to WebAssembly.
//!
//! Entities: 1 `player` (the Arena's [`PlayerView`]), 2 `slime` (x y yaw),
//! 3 `gem` (x y), 4 `ritual` (global: x y stuck). Events: [`FreezeEvent`];
//! gameplay events carry `ctx.stamp()` (zone, tick, content version). Gems
//! and content switches are also typed durable facts ([`GemCollected`],
//! [`ContentApplied`]) stamped by the host. The browser client is generated
//! from [`game_surface`].

pub mod content;
pub mod world;

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed_game::net::{GameServer, ServerConfig};
use distributed_game::{
    schema, Ctx, EntitySchema, FactCommand, FactStamp, GameEvent, GameInput, GameRecord,
    GameSurface, InputCredits, LeaveReason, PlayerId, Replicated, StalePolicy, View, WorldView,
    Zone,
};
use serde::Serialize;

use crate::arena::movement::{self, MoveInput, MoveState};
use crate::arena::PlayerView;
use crate::common::{Demo, Guests};
use content::{Hazard, Pack, ZoneKind};

/// A wandering slime.
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "slime", id = 2)]
pub struct SlimeView {
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

/// A gem waiting to be collected (global).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "gem", id = 3, global)]
pub struct GemView {
    /// Position x.
    #[quant(min = -movement::HALF, max = movement::HALF, bits = 14)]
    pub x: f32,
    /// Position y.
    #[quant(min = -movement::HALF, max = movement::HALF, bits = 14)]
    pub y: f32,
}

/// The ritual circle (global; `stuck` = 1 while it holds the zone).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "ritual", id = 4, global)]
pub struct RitualView {
    /// Position x.
    #[quant(min = -movement::HALF, max = movement::HALF, bits = 14)]
    pub x: f32,
    /// Position y.
    #[quant(min = -movement::HALF, max = movement::HALF, bits = 14)]
    pub y: f32,
    /// 1 while the ritual refuses the freeze.
    pub stuck: u8,
}

/// Provenance of a gameplay event: which zone, tick and content version
/// produced it.
#[derive(Debug, Clone, PartialEq, GameRecord)]
pub struct Stamp {
    /// Zone id.
    pub zone: String,
    /// Zone tick.
    pub tick: u64,
    /// Content version in effect.
    pub content_version: String,
}

impl From<FactStamp> for Stamp {
    fn from(stamp: FactStamp) -> Self {
        Self {
            zone: stamp.zone_id,
            tick: stamp.tick,
            content_version: stamp.content_version,
        }
    }
}

/// The Freeze's events.
#[derive(Debug, Clone, PartialEq, GameEvent)]
pub enum FreezeEvent {
    /// To a joining player (also after a handoff).
    #[game_event(id = 1)]
    Hello {
        /// Own entity id.
        entity: u32,
        /// Zone id.
        zone: String,
        /// Zone title.
        title: String,
        /// This zone is the safe-room gate.
        gate: bool,
        /// Host id (`alpha` / `beta`).
        host: String,
        /// Floor half extent (m).
        half: f32,
        /// Content version the zone runs.
        content_version: String,
    },
    /// Someone collected a gem.
    #[game_event(id = 2)]
    Gem {
        /// Collector's name.
        by: String,
        /// Gem value under the zone's content pack.
        value: u32,
        /// Provenance.
        stamp: Stamp,
    },
    /// Someone stepped into lava.
    #[game_event(id = 3)]
    Burned {
        /// Who.
        name: String,
        /// Provenance.
        stamp: Stamp,
    },
    /// The zone switched content.
    #[game_event(id = 4)]
    Content {
        /// Previous version.
        from: String,
        /// New version.
        to: String,
        /// New pack name.
        name: String,
        /// Provenance.
        stamp: Stamp,
    },
}

/// The Freeze's client surface.
pub fn game_surface() -> GameSurface {
    GameSurface::for_zone::<FreezeZone>("freeze")
        .events::<FreezeEvent>()
        .predict::<movement::ArenaStep>()
        .record::<content::Pack>()
        .constant("freeze_world", &content::freeze_world())
}

/// Durable command recording a collected gem.
pub const GEM_COLLECTED: &str = "freeze.gem_collected";
/// Durable command recording a content switch.
pub const CONTENT_APPLIED: &str = "freeze.content_applied";

/// [`GEM_COLLECTED`] input: *which* gem spot was collected. Its value is a
/// rule of the content pack at the fact's stamped content version, for the
/// handler to apply — the zone never reports a reward amount.
#[derive(Debug, Clone, PartialEq, Serialize, FactCommand)]
#[fact(command = GEM_COLLECTED)]
pub struct GemCollected {
    /// Gem spot of the zone layout.
    pub spot: u32,
}

/// [`CONTENT_APPLIED`] input (a zone/system fact).
#[derive(Debug, Clone, PartialEq, Serialize, FactCommand)]
#[fact(command = CONTENT_APPLIED, key(to))]
pub struct ContentApplied {
    /// Previous version.
    pub from: String,
    /// New version.
    pub to: String,
}
/// Content version every host starts on.
pub const INITIAL_VERSION: &str = "v1";
const VIEW_RADIUS: f32 = 40.0;
const PORTAL_RADIUS: f32 = 1.2;
const GEM_RADIUS: f32 = 0.9;
const GEM_RESPAWN: Duration = Duration::from_secs(4);
const RITUAL_ID: u32 = 900;

/// Zones whose ritual blocks a freeze (the "stuck zone" toggle), shared by
/// a host's zones and its dev routes.
#[derive(Clone, Default)]
pub struct Rituals(Arc<Mutex<HashSet<String>>>);

impl Rituals {
    /// Start / stop the ritual of `zone`.
    pub fn set(&self, zone: &str, stuck: bool) {
        let mut set = self.0.lock().expect("rituals");
        if stuck {
            set.insert(zone.to_string());
        } else {
            set.remove(zone);
        }
    }

    /// True while `zone` refuses to freeze.
    pub fn is_stuck(&self, zone: &str) -> bool {
        self.0.lock().expect("rituals").contains(zone)
    }

    /// Stuck zones.
    pub fn list(&self) -> Vec<String> {
        let mut v: Vec<_> = self.0.lock().expect("rituals").iter().cloned().collect();
        v.sort();
        v
    }
}

/// Zone construction parameters.
#[derive(Clone)]
pub struct FreezeConfig {
    /// Which zone this is.
    pub kind: ZoneKind,
    /// Display names registered by the dev ticket endpoint (primary host).
    pub guests: Guests,
    /// Stuck-zone toggles of this host.
    pub rituals: Rituals,
    /// Host id shown to players (`alpha` / `beta`).
    pub host: String,
}

struct Player {
    entity: u32,
    name: String,
    state: MoveState,
    /// Just arrived through a portal: ignore portals until it stepped off.
    on_portal: bool,
}

struct Slime {
    x: f32,
    y: f32,
    tx: f32,
    ty: f32,
    yaw: f32,
}

struct Gem {
    spot: usize,
    /// Simulation time it respawns at (None = present).
    respawn_at: Option<Duration>,
}

/// One zone of The Freeze.
pub struct FreezeZone {
    config: FreezeConfig,
    zone_id: String,
    pack: Pack,
    players: BTreeMap<PlayerId, Player>,
    credits: InputCredits,
    slimes: Vec<Slime>,
    gems: Vec<Gem>,
    collected: u64,
}

impl FreezeZone {
    /// Content pack in effect.
    pub fn pack(&self) -> &Pack {
        &self.pack
    }

    /// Player position.
    pub fn player_position(&self, player: PlayerId) -> Option<(f32, f32)> {
        self.players.get(&player).map(|p| (p.state.x, p.state.y))
    }

    fn arrival_point(&self, from_zone: &str) -> (f32, f32) {
        self.config
            .kind
            .portals()
            .iter()
            .find(|(_, _, to)| *to == from_zone)
            .map(|(x, y, _)| (*x - x.signum() * 3.0, *y))
            .unwrap_or(content::SPAWNS[0])
    }

    fn add_player(
        &mut self,
        ctx: &mut Ctx<'_, Self>,
        player: PlayerId,
        name: String,
        at: (f32, f32),
    ) {
        let entity = 1000 + player.0;
        self.players.insert(
            player,
            Player {
                entity,
                name,
                state: MoveState {
                    x: at.0,
                    y: at.1,
                    ..MoveState::default()
                },
                on_portal: false,
            },
        );
        ctx.set_view(player, View::new(at.0, at.1, VIEW_RADIUS));
        let hello = FreezeEvent::Hello {
            entity,
            zone: self.zone_id.clone(),
            title: self.config.kind.title().into(),
            gate: self.config.kind == ZoneKind::Hub,
            host: self.config.host.clone(),
            half: movement::HALF,
            content_version: ctx.content_version().into(),
        };
        ctx.send(player, &hello);
    }

    fn random_point(ctx: &mut Ctx<'_, Self>) -> (f32, f32) {
        loop {
            let x = (ctx.rng().next_f32() * 2.0 - 1.0) * (movement::HALF - 2.0);
            let y = (ctx.rng().next_f32() * 2.0 - 1.0) * (movement::HALF - 2.0);
            if movement::PILLARS
                .iter()
                .all(|(px, py, pr)| (x - px).powi(2) + (y - py).powi(2) > (pr + 1.0).powi(2))
            {
                return (x, y);
            }
        }
    }
}

impl Zone for FreezeZone {
    type Config = FreezeConfig;
    type Input = MoveInput;
    type Timer = ();
    type Message = distributed_game::NoMessages;

    fn schema() -> EntitySchema {
        schema![PlayerView, SlimeView, GemView, RitualView]
    }

    fn on_create(ctx: &mut Ctx<'_, Self>, config: FreezeConfig) -> Self {
        let pack = content::pack(ctx.content_version())
            .unwrap_or_else(|| content::pack(INITIAL_VERSION).expect("v1"));
        let kind = config.kind;
        let mut slimes = Vec::new();
        for _ in 0..kind.slimes() {
            let (x, y) = Self::random_point(ctx);
            let (tx, ty) = Self::random_point(ctx);
            slimes.push(Slime {
                x,
                y,
                tx,
                ty,
                yaw: 0.0,
            });
        }
        Self {
            zone_id: ctx.zone_id().to_string(),
            config,
            pack,
            players: BTreeMap::new(),
            credits: InputCredits::new(u32::from(crate::arena::MAX_CREDITS)),
            slimes,
            gems: (0..kind.gem_spots().len())
                .map(|spot| Gem {
                    spot,
                    respawn_at: None,
                })
                .collect(),
            collected: 0,
        }
    }

    fn on_join(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        let principal = ctx
            .player(player)
            .map(|p| p.principal.clone())
            .unwrap_or_default();
        let name = self
            .config
            .guests
            .name(&principal)
            .unwrap_or_else(|| format!("Guest {}", principal.get(6..10).unwrap_or("")));
        let at = content::SPAWNS[player.0 as usize % content::SPAWNS.len()];
        self.add_player(ctx, player, name, at);
    }

    fn on_handoff_join(
        &mut self,
        ctx: &mut Ctx<'_, Self>,
        player: PlayerId,
        from_zone: &str,
        carried: Vec<u8>,
    ) {
        // Carried: the display name (the second host has no guest registry).
        let name = String::from_utf8(carried)
            .ok()
            .filter(|n| !n.is_empty())
            .unwrap_or_else(|| "Traveller".into());
        let at = self.arrival_point(from_zone);
        self.add_player(ctx, player, name, at);
    }

    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, player: PlayerId, _reason: LeaveReason) {
        self.players.remove(&player);
        self.credits.remove(player);
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<MoveInput> {
        MoveInput::from_payload(payload)
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        let kind = self.config.kind;
        let now = ctx.sim_time();
        let ids: Vec<PlayerId> = self.players.keys().copied().collect();
        for id in ids {
            let player = self.players.get_mut(&id).expect("present");
            for input in self.credits.take(ctx, id) {
                // The declared predicted step: what the client runs as WASM.
                player.state = movement::arena_step(&player.state, &input.input, &(), movement::DT);
            }
            let (x, y) = (player.state.x, player.state.y);
            // Lava burns (ice is safe): back to a spawn point.
            if self.pack.hazard == Hazard::Lava
                && kind
                    .pools()
                    .iter()
                    .any(|(px, py, pr)| (x - px).powi(2) + (y - py).powi(2) < (pr - 0.3).powi(2))
            {
                let (sx, sy) = content::SPAWNS[id.0 as usize % content::SPAWNS.len()];
                player.state = MoveState {
                    x: sx,
                    y: sy,
                    ..MoveState::default()
                };
                let event = FreezeEvent::Burned {
                    name: player.name.clone(),
                    stamp: ctx.stamp().into(),
                };
                ctx.broadcast(&event);
            }
            let player = self.players.get_mut(&id).expect("present");
            let (x, y) = (player.state.x, player.state.y);
            ctx.set_view(id, View::new(x, y, VIEW_RADIUS));
            // Gems.
            for gem in &mut self.gems {
                let (gx, gy) = kind.gem_spots()[gem.spot];
                if gem.respawn_at.is_none()
                    && (x - gx).powi(2) + (y - gy).powi(2) < GEM_RADIUS.powi(2)
                {
                    gem.respawn_at = Some(now + GEM_RESPAWN);
                    self.collected += 1;
                    // Keyed by tick + emission order (a spot respawns).
                    let spot = gem.spot as u32;
                    let _ = ctx.emit(id, GemCollected { spot });
                    let event = FreezeEvent::Gem {
                        by: player.name.clone(),
                        value: self.pack.gem_value,
                        stamp: ctx.stamp().into(),
                    };
                    ctx.broadcast(&event);
                }
            }
            // Portals.
            let portal = kind
                .portals()
                .iter()
                .find(|(px, py, _)| (x - px).powi(2) + (y - py).powi(2) < PORTAL_RADIUS.powi(2));
            match portal {
                Some((_, _, target)) if !player.on_portal => {
                    player.on_portal = true;
                    let _ = ctx.handoff(id, target, player.name.clone().into_bytes());
                }
                Some(_) => {}
                None => player.on_portal = false,
            }
        }
        for gem in &mut self.gems {
            if gem.respawn_at.is_some_and(|at| now >= at) {
                gem.respawn_at = None;
            }
        }
        let dt = ctx.dt_secs();
        let speed = self.pack.slime_speed;
        for i in 0..self.slimes.len() {
            let s = &self.slimes[i];
            let (ox, oy) = (s.tx - s.x, s.ty - s.y);
            let dist = (ox * ox + oy * oy).sqrt();
            if dist < 0.3 {
                let (tx, ty) = Self::random_point(ctx);
                self.slimes[i].tx = tx;
                self.slimes[i].ty = ty;
                continue;
            }
            let s = &mut self.slimes[i];
            let stride = (speed * dt).min(dist);
            s.x += ox / dist * stride;
            s.y += oy / dist * stride;
            s.yaw = oy.atan2(ox);
        }
    }

    fn replicate(&self, out: &mut WorldView) {
        for p in self.players.values() {
            // The Arena's capsule, but culled by the view radius here.
            out.add(p.entity, &PlayerView::of(&p.state, &p.name))
                .local();
        }
        for (i, s) in self.slimes.iter().enumerate() {
            let view = SlimeView {
                x: s.x,
                y: s.y,
                yaw: s.yaw,
            };
            out.add(1 + i as u32, &view);
        }
        let spots = self.config.kind.gem_spots();
        for gem in self.gems.iter().filter(|g| g.respawn_at.is_none()) {
            let (x, y) = spots[gem.spot];
            out.add(100 + gem.spot as u32, &GemView { x, y });
        }
        let (x, y) = self.config.kind.ritual();
        let stuck = u8::from(self.config.rituals.is_stuck(&self.zone_id));
        out.add(RITUAL_ID, &RitualView { x, y, stuck });
    }

    fn is_gate(&self) -> bool {
        self.config.kind == ZoneKind::Hub
    }

    fn freeze_ready(&self) -> bool {
        !self.config.rituals.is_stuck(&self.zone_id)
    }

    fn on_content_version_changed(&mut self, ctx: &mut Ctx<'_, Self>, from: &str, to: &str) {
        if let Some(pack) = content::pack(to) {
            self.pack = pack;
        }
        // A meaningful world transition: persisted (stamped with `to`).
        let _ = ctx.emit_zone(ContentApplied {
            from: from.into(),
            to: to.into(),
        });
        let event = FreezeEvent::Content {
            from: from.into(),
            to: to.into(),
            name: self.pack.name.clone(),
            stamp: ctx.stamp().into(),
        };
        ctx.broadcast(&event);
    }

    fn has_pending_work(&self) -> bool {
        false
    }
}

/// Host settings every Freeze server uses: content version `v1`, safe-room
/// stale policy.
pub fn freeze_config(mut config: ServerConfig) -> ServerConfig {
    config.host.content_version = INITIAL_VERSION.into();
    config.host.stale_policy = StalePolicy::SafeRoom { roam: false };
    config
}

/// Demo registration (`game-demos freeze`). [`Demo::server`] builds a
/// single-host world (every zone local); `game-demos freeze` runs the
/// two-host world of [`world`].
pub struct FreezeDemo;

impl Demo for FreezeDemo {
    type Zone = FreezeZone;
    const NAME: &'static str = "freeze";
    const DEFAULT_ZONE: &'static str = content::HUB;

    fn hosts_zone(zone_id: &str) -> bool {
        ZoneKind::of(zone_id).is_some()
    }

    fn server(config: ServerConfig, guests: Guests) -> GameServer<FreezeZone> {
        world::zone_server(freeze_config(config), guests, Rituals::default(), "local")
    }
}
