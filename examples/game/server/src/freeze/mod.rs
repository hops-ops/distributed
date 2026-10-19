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
//! Movement is the Arena's shared step ([`crate::arena::movement`], predicted
//! by the client with the same TypeScript twin).
//!
//! Entities: 1 `player` (Arena fields), 2 `slime` (x y yaw), 3 `gem` (x y),
//! 4 `ritual` (global: x y stuck). Events (JSON): [`EVENT_HELLO`],
//! [`EVENT_GEM`], [`EVENT_BURNED`], [`EVENT_CONTENT`]; gameplay events carry
//! `ctx.stamp()` (zone, tick, content version). Gems and content switches are
//! also durable facts stamped by the host.

pub mod content;
pub mod world;

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed_game::net::{GameServer, ServerConfig};
use distributed_game::{
    Ctx, EntitySchema, EntityState, Fact, FactStamp, FieldType, FieldValue, GameInput,
    InputCredits, KindDef, LeaveReason, PlayerId, StalePolicy, View, WorldView, Zone,
};
use serde_json::{json, Value};

use crate::arena::movement::{self, MoveInput, MoveState};
use crate::common::{broadcast_json_event, send_json_event, Demo, Guests};
use content::{Hazard, Pack, ZoneKind};

/// Player kind (Arena fields: the client predicts it with Arena's step).
pub const KIND_PLAYER: u16 = 1;
/// Slime kind.
pub const KIND_SLIME: u16 = 2;
/// Gem kind.
pub const KIND_GEM: u16 = 3;
/// Ritual circle (global; `stuck` = 1 while it holds the zone).
pub const KIND_RITUAL: u16 = 4;
/// `{"entity", "zone", "title", "gate", "host", "half", "contentVersion"}`.
pub const EVENT_HELLO: u32 = 1;
/// `{"by", "value", "stamp"}` — someone collected a gem.
pub const EVENT_GEM: u32 = 2;
/// `{"name", "stamp"}` — someone stepped into lava.
pub const EVENT_BURNED: u32 = 3;
/// `{"from", "to", "name", "stamp"}` — the zone switched content.
pub const EVENT_CONTENT: u32 = 4;
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

    fn stamp_json(stamp: &FactStamp) -> Value {
        json!({ "zone": stamp.zone_id, "tick": stamp.tick, "contentVersion": stamp.content_version })
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
        send_json_event(
            ctx,
            player,
            EVENT_HELLO,
            &json!({
                "entity": entity,
                "zone": self.zone_id,
                "title": self.config.kind.title(),
                "gate": self.config.kind == ZoneKind::Hub,
                "host": self.config.host,
                "half": movement::HALF,
                "contentVersion": ctx.content_version(),
            }),
        );
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
                KindDef::new(KIND_SLIME, "slime")
                    .field("x", small)
                    .field("y", small)
                    .field("yaw", FieldType::Angle { bits: 8 }),
            )
            .kind(
                KindDef::new(KIND_GEM, "gem")
                    .field("x", small)
                    .field("y", small),
            )
            .kind(
                KindDef::new(KIND_RITUAL, "ritual")
                    .field("x", small)
                    .field("y", small)
                    .field("stuck", FieldType::U8),
            )
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
                player.state = movement::step(player.state, input.input);
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
                let event = json!({ "name": player.name, "stamp": Self::stamp_json(&ctx.stamp()) });
                broadcast_json_event(ctx, EVENT_BURNED, &event);
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
                    let value = self.pack.gem_value;
                    let key = format!("gem:{}:{}:{}", self.zone_id, ctx.tick(), gem.spot);
                    let _ = ctx.emit_fact_for(
                        id,
                        Fact::new(key, "freeze.gem_collected", json!({ "value": value })),
                    );
                    let event = json!({
                        "by": player.name, "value": value, "stamp": Self::stamp_json(&ctx.stamp()),
                    });
                    broadcast_json_event(ctx, EVENT_GEM, &event);
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
            let s = p.state;
            out.push(EntityState::new(
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
            ));
        }
        for (i, s) in self.slimes.iter().enumerate() {
            out.push(EntityState::new(
                1 + i as u32,
                KIND_SLIME,
                s.x,
                s.y,
                vec![FieldValue::F(s.x), FieldValue::F(s.y), FieldValue::F(s.yaw)],
            ));
        }
        let spots = self.config.kind.gem_spots();
        for gem in self.gems.iter().filter(|g| g.respawn_at.is_none()) {
            let (x, y) = spots[gem.spot];
            out.push(
                EntityState::new(
                    100 + gem.spot as u32,
                    KIND_GEM,
                    x,
                    y,
                    vec![FieldValue::F(x), FieldValue::F(y)],
                )
                .global(),
            );
        }
        let (rx, ry) = self.config.kind.ritual();
        out.push(
            EntityState::new(
                RITUAL_ID,
                KIND_RITUAL,
                rx,
                ry,
                vec![
                    FieldValue::F(rx),
                    FieldValue::F(ry),
                    FieldValue::U(self.config.rituals.is_stuck(&self.zone_id).into()),
                ],
            )
            .global(),
        );
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
        let key = format!("content:{}:{to}", self.zone_id);
        let _ = ctx.emit_fact(Fact::new(
            key,
            "freeze.content_applied",
            json!({ "from": from, "to": to }),
        ));
        let event = json!({
            "from": from, "to": to, "name": self.pack.name, "stamp": Self::stamp_json(&ctx.stamp()),
        });
        broadcast_json_event(ctx, EVENT_CONTENT, &event);
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
