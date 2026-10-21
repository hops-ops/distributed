//! **Boss Ripple** — hierarchical zones and tier-up event propagation.
//!
//! ```text
//! realm                                (2 Hz, idle background clock)
//! └ realm.city                         (5 Hz)
//!   ├ realm.city.borough-1             (10 Hz)
//!   │ ├ realm.city.borough-1.hood-1    (30 Hz, a boss + players)
//!   │ ├ … hood-2, hood-3
//!   └ realm.city.borough-2 …
//! ```
//!
//! Every neighborhood has a boss players can fight (E near it) or smite
//! (demo button). The boss **stays in its zone**; only its effects travel
//! up as tier-up events (`ctx.emit_upward`, routed by the server through
//! `DottedTopology`, across hosts via `/admin/child-event`):
//!
//! 1. hood: boss falls → `boss_defeated` (to the root);
//! 2. borough: tallies its hoods; when all fell it unlocks and emits
//!    `borough_cleared` (to the root);
//! 3. city: weather follows the remaining bosses (storm → rain → clearing
//!    → clear) and is broadcast to everyone observing the city — the
//!    players in its neighborhoods observe it, so the weather reaches
//!    them; with every borough cleared it emits `city_liberated`;
//! 4. realm: counts everything and shows a global banner.
//!
//! A defeated boss respawns after [`RESPAWN`] (`boss_respawned`, which
//! re-locks the borough and darkens the sky again). Tiers tick at
//! different rates (`GameServer::with_host_config`): realm 2 Hz, city
//! 5 Hz, borough 10 Hz, hood 30 Hz. With two hosts, placement alternates
//! by depth (hoods and the city on the first host, boroughs and the realm
//! on the second), so every hop of the ripple crosses hosts. Each tier's
//! state survives hibernation.
//!
//! Entities: 1 `tier` (id 1, every zone), 2 `boss` (id 2, hoods), 3
//! `player` (1000 + player, hoods; Arena movement fields so the client
//! predicts with [`hood_step`], the Arena movement compiled to WebAssembly —
//! the boss stands on its central pillar).
//! They are `#[derive(Replicated)]` views; client events are the typed
//! [`RippleEvent`] and input the `#[derive(GameInput)]` [`HoodInput`]; the
//! browser client is generated from [`game_surface`]. (Tier-up payloads
//! between zones are server-internal JSON.)

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use distributed_game::net::{GameServer, ServerConfig, ZoneFactory};
use distributed_game::world::{HostAddr, ZoneDirectory};
use distributed_game::{
    schema, ChildEvent, ConsumePolicy, Ctx, EntitySchema, GameEvent, GameInput, GameRecord,
    GameSurface, LeaveReason, ObserverId, PlayerId, Replicated, TimerId, WorldView, Zone,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::cluster::{ClusterDemo, World};
use crate::common::{Demo, Guests};
use game_sim::arena::{self as movement, MoveState};

/// Root zone.
pub const REALM: &str = "realm";
/// The city.
pub const CITY: &str = "realm.city";
/// Boroughs of the city.
pub const BOROUGHS: [&str; 2] = ["realm.city.borough-1", "realm.city.borough-2"];
/// Neighborhoods per borough.
pub const HOODS_PER_BOROUGH: usize = 3;
/// Boss hit points.
pub const BOSS_HP: u32 = 240;
/// Damage per hit.
pub const HIT_DAMAGE: u32 = 12;
/// Reach from the boss center (pillar radius + player radius + 1.5 m).
pub const REACH: f32 = 4.5;
/// A defeated boss returns after this long (simulation time of its hood).
pub const RESPAWN: Duration = Duration::from_secs(60);

/// Kind: tier state (every zone).
/// A tier's tallies (every zone; global).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "tier", id = 1, global)]
pub struct TierView {
    /// Bosses defeated below this tier.
    pub defeated: u8,
    /// Bosses in the world.
    pub total: u8,
    /// Boroughs cleared.
    pub cleared: u8,
    /// The city is liberated.
    pub liberated: bool,
    /// Index into [`WEATHER`].
    pub weather: u8,
    /// Ticks of this zone.
    pub clock: u32,
    /// Tier-up events received.
    pub events: u32,
}

/// A neighbourhood's boss (global).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "boss", id = 2, global)]
pub struct BossView {
    /// Hit points.
    pub hp: u16,
    /// Max hit points.
    pub max: u16,
    /// Who defeated it ("" while alive).
    pub by: String,
}

/// A fighter (the Arena's movement fields; global within the hood).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "player", id = 3, priority = 2.0, global)]
pub struct FighterView {
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

/// The city's weather.
#[derive(Debug, Clone, PartialEq, GameRecord)]
pub struct Weather {
    /// One of [`WEATHER`].
    pub weather: String,
    /// Bosses still standing.
    pub remaining: u32,
}

/// Boss Ripple's client events.
#[derive(Debug, Clone, PartialEq, GameEvent)]
pub enum RippleEvent {
    /// To every joining player and observer.
    #[game_event(id = 1)]
    Hello {
        /// Zone id.
        zone: String,
        /// `realm` / `city` / `borough` / `hood`.
        tier: String,
        /// Parent zone.
        parent: Option<String>,
        /// Child zones.
        children: Vec<String>,
        /// Host running the zone.
        host: String,
        /// Tick rate of this tier.
        tick_rate: u32,
        /// Own fighter (players in hoods).
        entity: Option<u32>,
        /// Weather (the city only).
        weather: Option<Weather>,
        /// Current realm banner.
        banner: String,
    },
    /// A tier-up event arrived at this zone.
    #[game_event(id = 2)]
    Ripple {
        /// Receiving zone.
        at: String,
        /// Event kind (`boss_defeated` …).
        kind: String,
        /// Child zone that forwarded it.
        from: String,
        /// Zone where it started.
        origin: String,
        /// Tick of the origin zone.
        origin_tick: u64,
        /// Tick of the receiving zone.
        tick: u64,
    },
    /// The city's weather changed.
    #[game_event(id = 3)]
    Weather(Weather),
    /// The realm's banner changed.
    #[game_event(id = 4)]
    Banner {
        /// Banner text.
        text: String,
    },
    /// The hood's boss fell or respawned.
    #[game_event(id = 5)]
    Boss {
        /// `defeated` / `respawned`.
        kind: String,
        /// Who defeated it.
        by: Option<String>,
    },
}

/// Boss Ripple's client surface.
pub fn game_surface() -> GameSurface {
    GameSurface::for_zone::<Ripple>("ripple")
        .events::<RippleEvent>()
        .predict::<HoodStep>()
        .constant("ripple_world", &ripple_world())
        .constant("arena_floor", &game_sim::arena::arena_floor())
}

/// Kind id of [`TierView`] (tests mirror fields by it).
pub const KIND_TIER: u16 = TierView::KIND_ID;
/// Input action bit: attack (needs [`REACH`]).
pub const ACTION_ATTACK: u8 = HoodInput::ATTACK;
/// Input action bit: smite (demo shortcut: defeats the boss from anywhere).
pub const ACTION_SMITE: u8 = HoodInput::SMITE;
const ATTACK_COOLDOWN: u8 = 8;
const MAX_CREDITS: u8 = 4;
/// Weather names by index (city).
pub const WEATHER: [&str; 4] = ["storm", "rain", "clearing", "clear"];

/// Tier of a zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TierKind {
    /// Province / root.
    Realm,
    /// City.
    City,
    /// Borough.
    Borough,
    /// Neighborhood (has a boss).
    Hood,
}

impl TierKind {
    /// Depth in the hierarchy (realm = 1).
    pub fn depth(self) -> usize {
        match self {
            Self::Realm => 1,
            Self::City => 2,
            Self::Borough => 3,
            Self::Hood => 4,
        }
    }

    /// Lowercase name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Realm => "realm",
            Self::City => "city",
            Self::Borough => "borough",
            Self::Hood => "hood",
        }
    }

    /// (tick, patch, idle) rates.
    pub fn rates(self) -> (u32, u32, Option<u32>) {
        match self {
            Self::Realm => (2, 2, Some(1)),
            Self::City => (5, 5, Some(1)),
            Self::Borough => (10, 10, Some(1)),
            Self::Hood => (30, 20, Some(2)),
        }
    }
}

/// A borough and its neighborhoods.
#[derive(Debug, Clone, PartialEq, GameRecord)]
pub struct BoroughLayout {
    /// Borough zone id.
    pub id: String,
    /// Neighborhood zone ids.
    pub hoods: Vec<String>,
}

/// The zone hierarchy and weather names (generated `RIPPLE_WORLD`).
#[derive(Debug, Clone, PartialEq, GameRecord)]
pub struct RippleWorld {
    /// Root zone.
    pub realm: String,
    /// The city.
    pub city: String,
    /// Boroughs with their neighborhoods.
    pub boroughs: Vec<BoroughLayout>,
    /// [`WEATHER`] names by index.
    pub weather: Vec<String>,
}

/// The `RIPPLE_WORLD` constant.
pub fn ripple_world() -> RippleWorld {
    RippleWorld {
        realm: REALM.into(),
        city: CITY.into(),
        boroughs: BOROUGHS
            .iter()
            .map(|b| BoroughLayout {
                id: b.to_string(),
                hoods: hoods(b),
            })
            .collect(),
        weather: WEATHER.map(String::from).to_vec(),
    }
}

/// Neighborhoods of a borough.
pub fn hoods(borough: &str) -> Vec<String> {
    (1..=HOODS_PER_BOROUGH)
        .map(|i| format!("{borough}.hood-{i}"))
        .collect()
}

/// Every zone of the hierarchy, root first.
pub fn all_zones() -> Vec<String> {
    let mut zones = vec![REALM.to_string(), CITY.to_string()];
    for b in BOROUGHS {
        zones.push(b.to_string());
        zones.extend(hoods(b));
    }
    zones
}

/// Total bosses.
pub fn total_bosses() -> usize {
    BOROUGHS.len() * HOODS_PER_BOROUGH
}

/// Tier of a known zone id.
pub fn tier_of(zone_id: &str) -> Option<TierKind> {
    match zone_id {
        REALM => Some(TierKind::Realm),
        CITY => Some(TierKind::City),
        z if BOROUGHS.contains(&z) => Some(TierKind::Borough),
        z => BOROUGHS
            .iter()
            .any(|b| hoods(b).iter().any(|h| h == z))
            .then_some(TierKind::Hood),
    }
}

fn children(zone_id: &str) -> Vec<String> {
    match tier_of(zone_id) {
        Some(TierKind::Realm) => vec![CITY.into()],
        Some(TierKind::City) => BOROUGHS.iter().map(|b| b.to_string()).collect(),
        Some(TierKind::Borough) => hoods(zone_id),
        _ => Vec::new(),
    }
}

/// Placement: depth-alternating over the sorted hosts (hood → hosts[0],
/// borough → hosts[1], city → hosts[0], realm → hosts[1] with two hosts).
pub struct TierPlacement {
    hosts: Vec<HostAddr>,
}

impl TierPlacement {
    /// Over `hosts` (sorted by id).
    pub fn new(hosts: &[HostAddr]) -> Self {
        Self {
            hosts: hosts.to_vec(),
        }
    }
}

impl ZoneDirectory for TierPlacement {
    fn locate(&self, zone_id: &str) -> Option<HostAddr> {
        let n = self.hosts.len();
        if n == 0 {
            return None;
        }
        let depth = tier_of(zone_id).map_or(4, TierKind::depth);
        self.hosts.get((4 - depth) % n).cloned()
    }

    fn host(&self, host_id: &str) -> Option<HostAddr> {
        self.hosts.iter().find(|h| h.id == host_id).cloned()
    }

    fn hosts(&self) -> Vec<HostAddr> {
        self.hosts.clone()
    }
}

pub use game_sim::ripple::{hood_step, HoodInput, HoodStep};

/// Timers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RippleTimer {
    /// The boss comes back.
    Respawn,
}

struct Fighter {
    entity: u32,
    name: String,
    state: MoveState,
    credits: u8,
    cooldown: u8,
}

/// Persistent tier state (also the hibernation snapshot).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TierState {
    /// Boss hit points (hoods).
    pub hp: u32,
    /// Who landed the last blow (hoods, while defeated).
    pub defeated_by: Option<String>,
    /// Hoods whose boss is down, as seen by this tier.
    pub defeated: BTreeSet<String>,
    /// Boroughs cleared, as seen by this tier (city, realm).
    pub cleared: BTreeSet<String>,
    /// City liberated (city, realm).
    pub liberated: bool,
    /// Times the city was liberated (realm).
    pub liberations: u32,
    /// Child events received.
    pub events: u32,
    /// Realm banner.
    pub banner: String,
}

/// Zone construction parameters.
#[derive(Clone)]
pub struct RippleConfig {
    /// Display names.
    pub guests: Guests,
    /// Host running the zone.
    pub host_id: String,
    /// Tier.
    pub tier: TierKind,
}

/// One zone of the hierarchy (every tier).
pub struct Ripple {
    config: RippleConfig,
    state: TierState,
    fighters: BTreeMap<PlayerId, Fighter>,
    respawn: Option<TimerId>,
    /// Ticks simulated by this activation (the tier's clock).
    ticks: u32,
}

impl Ripple {
    /// Persistent state (tests, tooling).
    pub fn state(&self) -> &TierState {
        &self.state
    }

    /// Tier.
    pub fn tier(&self) -> TierKind {
        self.config.tier
    }

    fn weather(&self) -> usize {
        let remaining = total_bosses() - self.state.defeated.len().min(total_bosses());
        match remaining {
            0 => 3,
            1 | 2 => 2,
            3 | 4 => 1,
            _ => 0,
        }
    }

    fn weather_record(&self) -> Weather {
        Weather {
            weather: WEATHER[self.weather()].into(),
            remaining: (total_bosses() - self.state.defeated.len().min(total_bosses())) as u32,
        }
    }

    fn hello(&self, ctx: &Ctx<'_, Self>, entity: Option<u32>) -> RippleEvent {
        let tier = self.config.tier;
        RippleEvent::Hello {
            zone: ctx.zone_id().into(),
            tier: tier.name().into(),
            parent: ctx.zone_id().rsplit_once('.').map(|(p, _)| p.to_string()),
            children: children(ctx.zone_id()),
            host: self.config.host_id.clone(),
            tick_rate: tier.rates().0,
            entity,
            weather: (tier == TierKind::City).then(|| self.weather_record()),
            banner: self.state.banner.clone(),
        }
    }

    fn greet(&self, ctx: &mut Ctx<'_, Self>, to: distributed_game::Viewer, entity: Option<u32>) {
        let hello = self.hello(ctx, entity);
        ctx.send(to, &hello);
    }

    fn defeat(&mut self, ctx: &mut Ctx<'_, Self>, by: String) {
        self.state.hp = 0;
        self.state.defeated_by = Some(by.clone());
        self.state.defeated.insert(ctx.zone_id().to_string());
        let payload = json!({ "hood": ctx.zone_id(), "by": by });
        ctx.emit_upward(
            "boss_defeated",
            payload.to_string().into_bytes(),
            ChildEvent::TO_ROOT,
        );
        ctx.broadcast(&RippleEvent::Boss {
            kind: "defeated".into(),
            by: Some(by),
        });
        self.respawn = Some(ctx.set_timeout(RESPAWN, RippleTimer::Respawn));
    }

    fn hood_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        let mut blows: Vec<(u32, String)> = Vec::new();
        for (id, f) in &mut self.fighters {
            f.credits = (f.credits + 1).min(MAX_CREDITS);
            f.cooldown = f.cooldown.saturating_sub(1);
            while f.credits > 0 {
                let Some(input) = ctx.take_inputs(*id, ConsumePolicy::Next).pop() else {
                    break;
                };
                let input = input.input;
                // The declared predicted step: what the client runs as WASM.
                f.state = hood_step(&f.state, &input, &(), movement::DT);
                f.credits -= 1;
                if input.action & ACTION_SMITE != 0 {
                    blows.push((u32::MAX, f.name.clone()));
                } else if input.action & ACTION_ATTACK != 0 && f.cooldown == 0 {
                    let d = (f.state.x * f.state.x + f.state.y * f.state.y).sqrt();
                    if d <= REACH {
                        f.cooldown = ATTACK_COOLDOWN;
                        blows.push((HIT_DAMAGE, f.name.clone()));
                    }
                }
            }
        }
        for (damage, by) in blows {
            if self.state.hp == 0 {
                break;
            }
            self.state.hp = self.state.hp.saturating_sub(damage);
            if self.state.hp == 0 {
                self.defeat(ctx, by);
            }
        }
    }

    fn on_child(&mut self, ctx: &mut Ctx<'_, Self>, event: &ChildEvent) {
        let origin = event.origin_zone.clone();
        let tier = self.config.tier;
        let weather_before = self.weather();
        match event.kind.as_str() {
            "boss_defeated" => {
                self.state.defeated.insert(origin.clone());
            }
            "boss_respawned" => {
                self.state.defeated.remove(&origin);
            }
            "borough_cleared" => {
                self.state.cleared.insert(origin.clone());
            }
            "borough_lost" => {
                self.state.cleared.remove(&origin);
            }
            _ => {}
        }
        match tier {
            TierKind::Borough => {
                let all = hoods(ctx.zone_id())
                    .iter()
                    .all(|h| self.state.defeated.contains(h));
                let was = self.state.cleared.contains(ctx.zone_id());
                let me = ctx.zone_id().to_string();
                if all && !was {
                    self.state.cleared.insert(me.clone());
                    let payload = json!({ "borough": me }).to_string().into_bytes();
                    ctx.emit_upward("borough_cleared", payload, ChildEvent::TO_ROOT);
                } else if !all && was {
                    self.state.cleared.remove(&me);
                    let payload = json!({ "borough": me }).to_string().into_bytes();
                    ctx.emit_upward("borough_lost", payload, ChildEvent::TO_ROOT);
                }
            }
            TierKind::City => {
                let liberated = BOROUGHS.iter().all(|b| self.state.cleared.contains(*b));
                if liberated != self.state.liberated {
                    self.state.liberated = liberated;
                    let kind = if liberated {
                        "city_liberated"
                    } else {
                        "city_fallen"
                    };
                    let payload = json!({ "city": ctx.zone_id() }).to_string().into_bytes();
                    ctx.emit_upward(kind, payload, 1);
                }
                if self.weather() != weather_before {
                    let weather = self.weather_record();
                    ctx.broadcast(&RippleEvent::Weather(weather));
                }
            }
            TierKind::Realm => {
                let banner = match event.kind.as_str() {
                    "city_liberated" => {
                        self.state.liberated = true;
                        self.state.liberations += 1;
                        Some(format!(
                            "The city is liberated! (liberation #{})",
                            self.state.liberations
                        ))
                    }
                    "city_fallen" => {
                        self.state.liberated = false;
                        Some("Darkness returns to the city".to_string())
                    }
                    _ => None,
                };
                if let Some(text) = banner {
                    self.state.banner = text.clone();
                    ctx.broadcast(&RippleEvent::Banner { text });
                }
            }
            TierKind::Hood => {}
        }
        self.state.events += 1;
        let ripple = RippleEvent::Ripple {
            at: ctx.zone_id().into(),
            kind: event.kind.clone(),
            from: event.from_zone.clone(),
            origin: event.origin_zone.clone(),
            origin_tick: event.origin_tick,
            tick: ctx.tick(),
        };
        ctx.broadcast(&ripple);
    }
}

impl Zone for Ripple {
    type Config = RippleConfig;
    type Input = HoodInput;
    type Timer = RippleTimer;
    type Message = distributed_game::NoMessages;

    fn schema() -> EntitySchema {
        schema![TierView, BossView, FighterView]
    }

    fn on_create(_ctx: &mut Ctx<'_, Self>, config: RippleConfig) -> Self {
        Self {
            config,
            state: TierState {
                hp: BOSS_HP,
                ..TierState::default()
            },
            fighters: BTreeMap::new(),
            respawn: None,
            ticks: 0,
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
        let entity = 1000 + player.0;
        if self.config.tier == TierKind::Hood {
            const SPOTS: [(f32, f32); 4] = [(-6.0, -6.0), (6.0, -6.0), (6.0, 6.0), (-6.0, 6.0)];
            let (x, y) = SPOTS[player.0 as usize % SPOTS.len()];
            self.fighters.insert(
                player,
                Fighter {
                    entity,
                    name,
                    state: movement::canonicalize(MoveState {
                        x,
                        y,
                        ..MoveState::default()
                    }),
                    credits: MAX_CREDITS,
                    cooldown: 0,
                },
            );
        }
        let entity = (self.config.tier == TierKind::Hood).then_some(entity);
        self.greet(ctx, player.into(), entity);
    }

    fn on_auth(
        &mut self,
        _ctx: &mut Ctx<'_, Self>,
        request: &distributed_game::JoinRequest<'_>,
    ) -> distributed_game::Admission {
        // Only neighborhoods are playable; upper tiers are observed.
        if self.config.tier != TierKind::Hood
            && request.claims.role == distributed_game::Role::Player
        {
            return distributed_game::Admission::Reject("observe this tier".into());
        }
        distributed_game::Admission::Admit
    }

    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, player: PlayerId, _reason: LeaveReason) {
        self.fighters.remove(&player);
    }

    fn on_observe(&mut self, ctx: &mut Ctx<'_, Self>, observer: ObserverId) {
        self.greet(ctx, observer.into(), None);
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<HoodInput> {
        HoodInput::from_payload(payload)
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        self.ticks = self.ticks.wrapping_add(1);
        if self.config.tier == TierKind::Hood {
            self.hood_tick(ctx);
        }
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_, Self>, _id: TimerId, timer: RippleTimer) {
        match timer {
            RippleTimer::Respawn => {
                self.respawn = None;
                self.state.hp = BOSS_HP;
                self.state.defeated_by = None;
                self.state.defeated.remove(ctx.zone_id());
                let payload = json!({ "hood": ctx.zone_id() }).to_string().into_bytes();
                ctx.emit_upward("boss_respawned", payload, ChildEvent::TO_ROOT);
                ctx.broadcast(&RippleEvent::Boss {
                    kind: "respawned".into(),
                    by: None,
                });
            }
        }
    }

    fn on_child_event(&mut self, ctx: &mut Ctx<'_, Self>, event: &ChildEvent) {
        self.on_child(ctx, event);
    }

    fn wants_background(&self) -> bool {
        // The realm keeps its slow clock running with nobody watching.
        self.config.tier == TierKind::Realm
    }

    fn replicate(&self, out: &mut WorldView) {
        let s = &self.state;
        let tier = TierView {
            defeated: s.defeated.len() as u8,
            total: total_bosses() as u8,
            cleared: s.cleared.len() as u8,
            liberated: s.liberated,
            weather: self.weather() as u8,
            clock: self.ticks,
            events: s.events,
        };
        out.add(1, &tier);
        if self.config.tier != TierKind::Hood {
            return;
        }
        let boss = BossView {
            hp: s.hp as u16,
            max: BOSS_HP as u16,
            by: s.defeated_by.clone().unwrap_or_default(),
        };
        out.add(2, &boss);
        for f in self.fighters.values() {
            let m = f.state;
            let view = FighterView {
                x: m.x,
                y: m.y,
                vx: m.vx,
                vy: m.vy,
                dash: m.dash,
                cd: m.cd,
                name: f.name.clone(),
            };
            out.add(f.entity, &view);
        }
    }

    fn hibernate(&self) -> Option<Vec<u8>> {
        serde_json::to_vec(&self.state).ok()
    }

    fn on_restore(&mut self, ctx: &mut Ctx<'_, Self>, state: &[u8], _saved_version: &str) {
        if let Ok(state) = serde_json::from_slice::<TierState>(state) {
            self.state = state;
        }
        if self.config.tier == TierKind::Hood && self.state.hp == 0 {
            self.respawn = Some(ctx.set_timeout(RESPAWN, RippleTimer::Respawn));
        }
    }
}

/// Bot / test payload.
pub fn encode_input(mx: i8, my: i8, action: u8) -> Vec<u8> {
    HoodInput {
        mx,
        my,
        buttons: 0,
        action,
    }
    .to_payload()
}

/// Demo registration (`game-demos ripple`).
pub struct RippleDemo;

impl Demo for RippleDemo {
    type Zone = Ripple;
    const NAME: &'static str = "ripple";
    const DEFAULT_ZONE: &'static str = "realm.city.borough-1.hood-1";

    fn hosts_zone(zone_id: &str) -> bool {
        tier_of(zone_id).is_some()
    }

    fn server(config: ServerConfig, guests: Guests) -> GameServer<Ripple> {
        let world = World::standalone::<Self>("local", Duration::from_secs(20), guests);
        Self::cluster_server(config, &world)
    }
}

impl ClusterDemo for RippleDemo {
    fn directory(hosts: &[HostAddr]) -> Arc<dyn ZoneDirectory> {
        Arc::new(TierPlacement::new(hosts))
    }

    fn cluster_server(mut config: ServerConfig, world: &World) -> GameServer<Ripple> {
        config.host.max_observers = 64;
        let guests = world.guests.clone();
        let host_id = world.host.id.clone();
        GameServer::new(
            config,
            ZoneFactory::sync(move |activation| {
                let zone_id = activation.zone_id.as_str();
                tier_of(zone_id).map(|tier| RippleConfig {
                    guests: guests.clone(),
                    host_id: host_id.clone(),
                    tier,
                })
            }),
        )
        .with_host_config(|zone_id, host| {
            if let Some(tier) = tier_of(zone_id) {
                let (tick, patch, idle) = tier.rates();
                host.tick_rate = tick;
                host.patch_rate = patch;
                host.idle_tick_rate = idle;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hierarchy_and_placement() {
        assert_eq!(all_zones().len(), 2 + 2 + 6);
        assert_eq!(tier_of("realm.city.borough-2.hood-3"), Some(TierKind::Hood));
        assert_eq!(tier_of("realm.city.borough-3"), None);
        let hosts = [HostAddr::new("A", "a:1"), HostAddr::new("B", "b:1")];
        let place = TierPlacement::new(&hosts);
        let owner = |z: &str| place.locate(z).unwrap().id;
        assert_eq!(owner("realm.city.borough-1.hood-1"), "A");
        assert_eq!(owner("realm.city.borough-1"), "B");
        assert_eq!(owner(CITY), "A");
        assert_eq!(owner(REALM), "B");
        let one = TierPlacement::new(&hosts[..1]);
        assert!(all_zones().iter().all(|z| one.locate(z).unwrap().id == "A"));
    }

    #[test]
    fn input_round_trips() {
        let bytes = encode_input(-128, 5, 0xff);
        let input = HoodInput::from_payload(&bytes).unwrap();
        assert_eq!(input.movement().mx, -127);
        assert_eq!(input.action, ACTION_ATTACK | ACTION_SMITE);
        assert_eq!(HoodInput::from_payload(&bytes[..3]), None);
    }
}
