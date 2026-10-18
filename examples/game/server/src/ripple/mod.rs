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
//! predicts with the Arena step — the boss stands on its central pillar).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use distributed_game::net::{GameServer, ServerConfig};
use distributed_game::world::{HostAddr, ZoneDirectory};
use distributed_game::{
    ChildEvent, ConsumePolicy, Ctx, EntitySchema, EntityState, FieldType, FieldValue, KindDef,
    LeaveReason, ObserverId, PlayerId, TimerId, WorldView, Zone,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::arena::movement::{self, MoveInput, MoveState};
use crate::cluster::{ClusterDemo, ClusterOptions, World};
use crate::common::{broadcast_json_event, send_json_event, Demo, Guests};

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
pub const KIND_TIER: u16 = 1;
/// Kind: the boss (hoods).
pub const KIND_BOSS: u16 = 2;
/// Kind: a player (hoods).
pub const KIND_PLAYER: u16 = 3;
/// Event: `{zone, tier, parent, children, host, tickRate, entity?}` on join/observe.
pub const EVENT_HELLO: u32 = 1;
/// Event: a tier received a child event `{at, kind, from, origin, originTick}`.
pub const EVENT_RIPPLE: u32 = 2;
/// Event (city): `{weather, remaining}`.
pub const EVENT_WEATHER: u32 = 3;
/// Event (realm): `{text}`.
pub const EVENT_BANNER: u32 = 4;
/// Event (hood): `{kind: "defeated" | "respawned", by?}`.
pub const EVENT_BOSS: u32 = 5;
/// Input action bit: attack (needs [`REACH`]).
pub const ACTION_ATTACK: u8 = 1;
/// Input action bit: smite (demo shortcut: defeats the boss from anywhere).
pub const ACTION_SMITE: u8 = 2;
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

/// Hood input: Arena movement + action bits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HoodInput {
    /// Movement (Arena step).
    pub movement: MoveInput,
    /// [`ACTION_ATTACK`] | [`ACTION_SMITE`].
    pub action: u8,
}

impl HoodInput {
    /// Wire payload `[mx, my, buttons, action]`.
    pub fn encode(self) -> [u8; 4] {
        let [mx, my, buttons] = self.movement.encode();
        [mx, my, buttons, self.action]
    }

    /// Parse and sanitize.
    pub fn decode(payload: &[u8]) -> Option<Self> {
        let (movement, action) = payload.split_at_checked(3)?;
        let [action] = action else {
            return None;
        };
        Some(Self {
            movement: MoveInput::decode(movement)?,
            action: action & (ACTION_ATTACK | ACTION_SMITE),
        })
    }
}

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

    fn weather_json(&self) -> Value {
        json!({
            "weather": WEATHER[self.weather()],
            "remaining": total_bosses() - self.state.defeated.len().min(total_bosses()),
        })
    }

    fn hello(&self, ctx: &Ctx<'_, Self>, entity: Option<u32>) -> Value {
        let tier = self.config.tier;
        json!({
            "zone": ctx.zone_id(),
            "tier": tier.name(),
            "parent": ctx.zone_id().rsplit_once('.').map(|(p, _)| p),
            "children": children(ctx.zone_id()),
            "host": self.config.host_id,
            "tickRate": tier.rates().0,
            "entity": entity,
            "weather": (tier == TierKind::City).then(|| self.weather_json()),
            "banner": self.state.banner,
        })
    }

    fn greet(&self, ctx: &mut Ctx<'_, Self>, to: distributed_game::Viewer, entity: Option<u32>) {
        let hello = self.hello(ctx, entity);
        send_json_event(ctx, to, EVENT_HELLO, &hello);
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
        broadcast_json_event(ctx, EVENT_BOSS, &json!({ "kind": "defeated", "by": by }));
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
                f.state = movement::step(f.state, input.movement);
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
                    let weather = self.weather_json();
                    broadcast_json_event(ctx, EVENT_WEATHER, &weather);
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
                    broadcast_json_event(ctx, EVENT_BANNER, &json!({ "text": text }));
                }
            }
            TierKind::Hood => {}
        }
        self.state.events += 1;
        let ripple = json!({
            "at": ctx.zone_id(),
            "kind": event.kind,
            "from": event.from_zone,
            "origin": event.origin_zone,
            "originTick": event.origin_tick,
            "tick": ctx.tick(),
        });
        broadcast_json_event(ctx, EVENT_RIPPLE, &ripple);
    }
}

impl Zone for Ripple {
    type Config = RippleConfig;
    type Input = HoodInput;
    type Timer = RippleTimer;

    fn schema() -> EntitySchema {
        EntitySchema::new()
            .kind(
                KindDef::new(KIND_TIER, "tier")
                    .field("defeated", FieldType::U8)
                    .field("total", FieldType::U8)
                    .field("cleared", FieldType::U8)
                    .field("liberated", FieldType::Bool)
                    .field("weather", FieldType::U8)
                    .field("clock", FieldType::U32)
                    .field("events", FieldType::U32),
            )
            .kind(
                KindDef::new(KIND_BOSS, "boss")
                    .field("hp", FieldType::U16)
                    .field("max", FieldType::U16)
                    .field("by", FieldType::ShortStr),
            )
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
        HoodInput::decode(payload)
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
                broadcast_json_event(ctx, EVENT_BOSS, &json!({ "kind": "respawned" }));
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
        out.push(
            EntityState::new(
                1,
                KIND_TIER,
                0.0,
                0.0,
                vec![
                    FieldValue::U(s.defeated.len() as u32),
                    FieldValue::U(total_bosses() as u32),
                    FieldValue::U(s.cleared.len() as u32),
                    FieldValue::Bool(s.liberated),
                    FieldValue::U(self.weather() as u32),
                    FieldValue::U(self.ticks),
                    FieldValue::U(s.events),
                ],
            )
            .global(),
        );
        if self.config.tier != TierKind::Hood {
            return;
        }
        out.push(
            EntityState::new(
                2,
                KIND_BOSS,
                0.0,
                0.0,
                vec![
                    FieldValue::U(s.hp),
                    FieldValue::U(BOSS_HP),
                    FieldValue::Str(s.defeated_by.clone().unwrap_or_default()),
                ],
            )
            .global(),
        );
        for f in self.fighters.values() {
            let m = f.state;
            out.push(
                EntityState::new(
                    f.entity,
                    KIND_PLAYER,
                    m.x,
                    m.y,
                    vec![
                        FieldValue::F(m.x),
                        FieldValue::F(m.y),
                        FieldValue::F(m.vx),
                        FieldValue::F(m.vy),
                        FieldValue::U(m.dash.into()),
                        FieldValue::U(m.cooldown.into()),
                        FieldValue::Str(f.name.clone()),
                    ],
                )
                .global(),
            );
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
        movement: MoveInput { mx, my, buttons: 0 },
        action,
    }
    .encode()
    .to_vec()
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
        let options = ClusterOptions::single("local", Duration::from_secs(20));
        let world = World::standalone::<Self>(&options, guests);
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
        GameServer::new(config, move |zone_id| {
            tier_of(zone_id).map(|tier| RippleConfig {
                guests: guests.clone(),
                host_id: host_id.clone(),
                tier,
            })
        })
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
        let input = HoodInput::decode(&bytes).unwrap();
        assert_eq!(input.movement.mx, -127);
        assert_eq!(input.action, ACTION_ATTACK | ACTION_SMITE);
        assert_eq!(HoodInput::decode(&bytes[..3]), None);
    }
}
