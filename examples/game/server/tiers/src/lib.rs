//! **Mob Pit's world tiers.** Rooms group into hoods (3 × 3 rooms), hoods
//! into boroughs (3 × 3 hoods) and boroughs into one city
//! (`game_sim::mobpit::{hood_of, borough_of}`). Each tier is an aggregate
//! in a celld cell of its own (`examples/game/cells`):
//!
//! | cell | aggregate | commands | outward event |
//! |---|---|---|---|
//! | `hood/{hx.hy}` | [`Hood`] | [`BOSS_DEFEATED`] (zone fact of the boss room), [`BOSS_RESPAWN`] (its own schedule) | [`HoodState`] |
//! | `borough/{bx.by}` | [`Borough`] | [`HOOD_CHANGED`] (saga) | [`BoroughState`] |
//! | `city/city` | [`City`] | [`BOROUGH_CHANGED`] (saga) | [`CityState`] |
//!
//! **Up.** A boss falls in its room: the room emits [`BossDefeatedInput`]
//! as a zone fact (exactly once, keyed by the boss's generation) into its
//! hood, which raises its tier and schedules the boss's return
//! ([`BOSS_RESPAWN_MS`], `ctx.schedule`). Each tier change leaves through
//! the cell's outbox as a domain event; the worker's Queue consumer turns
//! a hood's event into [`HOOD_CHANGED`] on its borough and a borough's into
//! [`BOROUGH_CHANGED`] on the city — an explicit saga: each step is an
//! idempotent command (the event id is its command id) and carries the
//! source's stream sequence, so a late or repeated step changes nothing.
//!
//! **Down.** Rooms read their hood, borough and city cells (off the tick,
//! `CelldZone::refresh`) and tell their players: the hood's tier makes the
//! room harder, the borough's weather (storm → rain → clearing → clear as
//! its bosses fall) darkens or lifts the sky, the city carries the banner.
//!
//! Identity: [`BOSS_DEFEATED`] runs only for the room's own zone principal
//! (`zone:pit.<x>.<y>`, bound by the room's fact session) and only for the
//! hood that room belongs to; the saga commands only for [`SAGA_PRINCIPAL`],
//! a session the worker builds itself. No input names a reward.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use distributed::cell_host::AggregateCell;
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, CausalRouteDependencies, HandlerError, PortableCommand, Routes,
};
use distributed::schedule::{schedule_guard, Capability, Schedule};
use distributed::{sourced, Entity};
use game_sim::mobpit::{borough_of, hood_of, pair_shard, parse_pair, parse_room, BOROUGH_HOODS};
use serde::{Deserialize, Serialize};

/// Cell kind of the hoods: `hood/{hx.hy}`.
pub const HOOD_KIND: &str = "hood";
/// Cell kind of the boroughs: `borough/{bx.by}`.
pub const BOROUGH_KIND: &str = "borough";
/// Cell kind of the city: `city/city`.
pub const CITY_KIND: &str = "city";
/// The city's shard.
pub const CITY_SHARD: &str = "city";
/// A boss room's fact: its boss fell.
pub const BOSS_DEFEATED: &str = "hood.boss_defeated";
/// The hood's own schedule: its boss returns.
pub const BOSS_RESPAWN: &str = "hood.boss_respawn";
/// Saga step: a hood changed (into its borough).
pub const HOOD_CHANGED: &str = "borough.hood_changed";
/// Saga step: a borough changed (into the city).
pub const BOROUGH_CHANGED: &str = "city.borough_changed";
/// Outward event of a hood.
pub const HOOD_EVENT: &str = "hood.changed";
/// Outward event of a borough.
pub const BOROUGH_EVENT: &str = "borough.changed";
/// Outward event of the city.
pub const CITY_EVENT: &str = "city.changed";
/// The principal of the saga's sessions (built by the worker's Queue
/// consumer, never from a request).
pub const SAGA_PRINCIPAL: &str = "system:tiers";
/// A fallen boss returns after this long (cell clock).
pub const BOSS_RESPAWN_MS: u64 = 90_000;
/// Name of the respawn schedule.
const RESPAWN_ALARM: &str = "respawn";
/// Weather names, by index: the sky of a borough.
pub const WEATHER: [&str; 4] = ["storm", "rain", "clearing", "clear"];
/// Highest hood tier that still makes a room harder.
pub const MAX_TIER: u32 = 8;

/// The weather of a borough with `bosses_down` of its 9 bosses down.
pub fn weather_for(bosses_down: u32) -> u8 {
    match bosses_down {
        0..=2 => 0,
        3..=5 => 1,
        6..=8 => 2,
        _ => 3,
    }
}

/// The principal a zone's own facts carry (`distributed_game`'s zone
/// principal: `zone:{zone}`).
fn zone_principal(zone: &str) -> String {
    format!("zone:{zone}")
}

fn rejected(message: impl std::fmt::Display) -> HandlerError {
    HandlerError::Rejected(message.to_string())
}

// ---------------------------------------------------------------- hood

/// One hood: whether its boss is down and how often it fell (its tier).
#[derive(Clone, Debug, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct Hood {
    entity: Entity,
    /// The boss is down (until it respawns).
    pub boss_down: bool,
    /// Times the boss fell: the hood's tier.
    pub tier: u32,
    /// Display name of the last slayer (cosmetic).
    pub last_by: String,
    /// When the boss returns (cell clock, ms; 0 while it stands).
    pub respawn_at_ms: u64,
}

/// A hood as its outward events carry it (and rooms read it).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, distributed::DomainState)]
#[domain_state(version = 1)]
pub struct HoodState {
    /// `hx.hy`.
    pub hood: String,
    /// The boss is down.
    pub boss_down: bool,
    /// Times the boss fell.
    pub tier: u32,
    /// Last slayer's display name.
    pub last_by: String,
    /// When the boss returns (ms, 0 while it stands).
    pub respawn_at_ms: u64,
}

impl From<&Hood> for HoodState {
    fn from(hood: &Hood) -> Self {
        Self {
            hood: hood.entity.id().to_string(),
            boss_down: hood.boss_down,
            tier: hood.tier,
            last_by: hood.last_by.clone(),
            respawn_at_ms: hood.respawn_at_ms,
        }
    }
}

#[sourced(entity, aggregate_type = "Hood", events = "HoodEvent", domain_state = HoodState)]
impl Hood {
    #[event("hood.changed", version = 1, domain)]
    fn boss_fell(&mut self, hood: String, by: String, respawn_at_ms: u64) {
        if self.entity.id().is_empty() {
            self.entity.set_id(hood);
        }
        self.boss_down = true;
        self.tier += 1;
        self.last_by = by;
        self.respawn_at_ms = respawn_at_ms;
    }

    #[event("hood.boss_returned", version = 1, domain)]
    fn boss_returned(&mut self, hood: String) {
        if self.entity.id().is_empty() {
            self.entity.set_id(hood);
        }
        self.boss_down = false;
        self.respawn_at_ms = 0;
    }
}

impl Hood {
    /// The outward state.
    pub fn state(&self) -> HoodState {
        HoodState::from(self)
    }

    /// Committed stream version.
    pub fn version(&self) -> u64 {
        self.entity.version()
    }
}

/// [`BOSS_DEFEATED`]: what the boss room reports (which hood and room,
/// which life of the boss, who landed the blow — cosmetic). No reward.
#[derive(
    Debug, Clone, Serialize, Deserialize, distributed::CommandInput, distributed_game::FactCommand,
)]
#[fact(command = BOSS_DEFEATED)]
pub struct BossDefeatedInput {
    /// `hx.hy`: the hood's shard (the fact route's key).
    pub hood: String,
    /// The boss room's zone (`pit.<x>.<y>`).
    pub room: String,
    /// The boss's life (its generation in the room).
    pub generation: u32,
    /// Display name of the slayer (server-sanitized, cosmetic).
    pub by: String,
}

/// [`BOSS_RESPAWN`] input (the hood's own schedule).
#[derive(Debug, Clone, Serialize, Deserialize, distributed::CommandInput)]
pub struct RespawnInput {
    /// `hx.hy`.
    pub hood: String,
}

/// A hood command's answer.
#[derive(Debug, Clone, Serialize, Deserialize, distributed::CommandOutput)]
pub struct HoodReceipt {
    /// The boss is down after the command.
    pub boss_down: bool,
    /// The hood's tier after the command.
    pub tier: u32,
    /// The command changed the hood.
    pub changed: bool,
}

impl HoodReceipt {
    fn of(hood: &Hood, changed: bool) -> Self {
        Self {
            boss_down: hood.boss_down,
            tier: hood.tier,
            changed,
        }
    }
}

fn now_ms<A: distributed::Aggregate + Send + Sync + 'static>(
    ctx: &CausalCommandContext<'_, A>,
) -> Result<u64, HandlerError> {
    ctx.now()?
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|now| now.as_millis() as u64)
        .map_err(|_| rejected("host clock is before the Unix epoch"))
}

/// Only the boss room itself may report its boss, and only into its hood.
fn is_boss_room(ctx: &CausalCommandContext<'_, Hood>) -> bool {
    ctx.session()
        .user_id()
        .is_some_and(|p| p.starts_with("zone:pit."))
}

async fn handle_boss_defeated(
    ctx: &CausalCommandContext<'_, Hood>,
    input: BossDefeatedInput,
) -> Result<PreparedCommand<Succeeded<HoodReceipt>>, HandlerError> {
    let room = parse_room(&input.room).ok_or_else(|| rejected("not a room"))?;
    if ctx.session().user_id() != Some(zone_principal(&input.room).as_str()) {
        return Err(rejected("only the room itself reports its boss"));
    }
    if pair_shard(hood_of(room)) != input.hood {
        return Err(rejected("the room is not in this hood"));
    }
    let now = now_ms(ctx)?;
    let repo = ctx.repo();
    let mut hood = match repo.get(&input.hood).await? {
        Some(hood) => hood,
        None => repo.create(),
    };
    if hood.boss_down {
        // Already down (a late report of the same fall): nothing changes.
        let receipt = HoodReceipt::of(&hood, false);
        return repo.commit(hood)?.succeeded(receipt);
    }
    let by: String = input.by.chars().take(16).collect();
    let respawn_at_ms = now + BOSS_RESPAWN_MS;
    hood.boss_fell(input.hood.clone(), by, respawn_at_ms)
        .map_err(rejected)?;
    ctx.schedule(
        Schedule::at(
            RESPAWN_ALARM,
            SystemTime::UNIX_EPOCH + Duration::from_millis(respawn_at_ms),
            BOSS_RESPAWN,
            &RespawnInput {
                hood: input.hood.clone(),
            },
        )
        .map_err(rejected)?,
    )?;
    let receipt = HoodReceipt::of(&hood, true);
    repo.publish_events().commit(hood)?.succeeded(receipt)
}

async fn handle_respawn(
    ctx: &CausalCommandContext<'_, Hood>,
    input: RespawnInput,
) -> Result<PreparedCommand<Succeeded<HoodReceipt>>, HandlerError> {
    let repo = ctx.repo();
    let Some(mut hood) = repo.get(&input.hood).await? else {
        return Err(rejected("no hood"));
    };
    if !hood.boss_down {
        let receipt = HoodReceipt::of(&hood, false);
        return repo.commit(hood)?.succeeded(receipt);
    }
    hood.boss_returned(input.hood).map_err(rejected)?;
    let receipt = HoodReceipt::of(&hood, true);
    repo.publish_events().commit(hood)?.succeeded(receipt)
}

// ---------------------------------------------------------------- borough

/// One hood as its borough last heard of it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoodSeen {
    /// The boss is down.
    pub boss_down: bool,
    /// The hood's tier.
    pub tier: u32,
    /// Stream sequence of the hood event (later wins).
    pub sequence: u64,
}

/// One borough: its 9 hoods as last heard.
#[derive(Clone, Debug, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct Borough {
    entity: Entity,
    /// Hood shard → what was heard last.
    pub hoods: BTreeMap<String, HoodSeen>,
}

/// A borough as its outward events carry it (and rooms read it).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, distributed::DomainState)]
#[domain_state(version = 1)]
pub struct BoroughState {
    /// `bx.by`.
    pub borough: String,
    /// Bosses down among its hoods.
    pub bosses_down: u32,
    /// Index into [`WEATHER`].
    pub weather: u8,
    /// Sum of its hoods' tiers.
    pub tier: u32,
    /// Hoods whose boss is down (`hx.hy`).
    pub hoods_down: Vec<String>,
}

impl From<&Borough> for BoroughState {
    fn from(b: &Borough) -> Self {
        let hoods_down: Vec<String> = b
            .hoods
            .iter()
            .filter(|(_, h)| h.boss_down)
            .map(|(k, _)| k.clone())
            .collect();
        let bosses_down = hoods_down.len() as u32;
        Self {
            borough: b.entity.id().to_string(),
            bosses_down,
            weather: weather_for(bosses_down),
            tier: b.hoods.values().map(|h| h.tier).sum(),
            hoods_down,
        }
    }
}

#[sourced(
    entity,
    aggregate_type = "Borough",
    events = "BoroughEvent",
    domain_state = BoroughState
)]
impl Borough {
    #[event("borough.changed", version = 1, domain)]
    fn hood_seen(&mut self, borough: String, hood: String, seen: HoodSeen) {
        if self.entity.id().is_empty() {
            self.entity.set_id(borough);
        }
        self.hoods.insert(hood, seen);
    }
}

impl Borough {
    /// The outward state.
    pub fn state(&self) -> BoroughState {
        BoroughState::from(self)
    }
}

/// [`HOOD_CHANGED`] (saga step).
#[derive(Debug, Clone, Serialize, Deserialize, distributed::CommandInput)]
pub struct HoodChangedInput {
    /// `bx.by`.
    pub borough: String,
    /// `hx.hy`.
    pub hood: String,
    /// The hood's boss is down.
    pub boss_down: bool,
    /// The hood's tier.
    pub tier: u32,
    /// The hood event's stream sequence.
    pub sequence: u64,
}

/// A borough or city command's answer.
#[derive(Debug, Clone, Serialize, Deserialize, distributed::CommandOutput)]
pub struct TierReceipt {
    /// The command changed the tier.
    pub changed: bool,
}

fn is_saga<A: distributed::Aggregate + Send + Sync + 'static>(
    ctx: &CausalCommandContext<'_, A>,
) -> bool {
    ctx.session().user_id() == Some(SAGA_PRINCIPAL)
}

async fn handle_hood_changed(
    ctx: &CausalCommandContext<'_, Borough>,
    input: HoodChangedInput,
) -> Result<PreparedCommand<Succeeded<TierReceipt>>, HandlerError> {
    let hood = parse_pair(&input.hood).ok_or_else(|| rejected("not a hood"))?;
    if pair_shard(borough_of(hood)) != input.borough {
        return Err(rejected("the hood is not in this borough"));
    }
    let repo = ctx.repo();
    let mut borough = match repo.get(&input.borough).await? {
        Some(b) => b,
        None => repo.create(),
    };
    let stale = borough
        .hoods
        .get(&input.hood)
        .is_some_and(|seen| seen.sequence >= input.sequence);
    if stale {
        return repo
            .commit(borough)?
            .succeeded(TierReceipt { changed: false });
    }
    borough
        .hood_seen(
            input.borough,
            input.hood,
            HoodSeen {
                boss_down: input.boss_down,
                tier: input.tier,
                sequence: input.sequence,
            },
        )
        .map_err(rejected)?;
    repo.publish_events()
        .commit(borough)?
        .succeeded(TierReceipt { changed: true })
}

// ---------------------------------------------------------------- city

/// One borough as the city last heard of it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoroughSeen {
    /// Bosses down in it.
    pub bosses_down: u32,
    /// Stream sequence of the borough event (later wins).
    pub sequence: u64,
}

/// The city: every borough heard from.
#[derive(Clone, Debug, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct City {
    entity: Entity,
    /// Borough shard → what was heard last.
    pub boroughs: BTreeMap<String, BoroughSeen>,
}

/// The city as its outward events carry it (and rooms read it).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, distributed::DomainState)]
#[domain_state(version = 1)]
pub struct CityState {
    /// Bosses down anywhere.
    pub bosses_down: u32,
    /// Boroughs with every boss down.
    pub boroughs_cleared: u32,
    /// The banner every room shows.
    pub banner: String,
}

impl From<&City> for CityState {
    fn from(c: &City) -> Self {
        let full = (BOROUGH_HOODS * BOROUGH_HOODS) as u32;
        let bosses_down = c.boroughs.values().map(|b| b.bosses_down).sum();
        let boroughs_cleared = c
            .boroughs
            .values()
            .filter(|b| b.bosses_down >= full)
            .count() as u32;
        let banner = match (bosses_down, boroughs_cleared) {
            (0, _) => "The bosses hold the city".to_string(),
            (n, 0) => format!(
                "{n} boss{} down across the city",
                if n == 1 { "" } else { "es" }
            ),
            (n, c) => format!(
                "{c} borough{} liberated · {n} bosses down",
                if c == 1 { "" } else { "s" }
            ),
        };
        Self {
            bosses_down,
            boroughs_cleared,
            banner,
        }
    }
}

#[sourced(entity, aggregate_type = "City", events = "CityEvent", domain_state = CityState)]
impl City {
    #[event("city.changed", version = 1, domain)]
    fn borough_seen(&mut self, city: String, borough: String, seen: BoroughSeen) {
        if self.entity.id().is_empty() {
            self.entity.set_id(city);
        }
        self.boroughs.insert(borough, seen);
    }
}

impl City {
    /// The outward state.
    pub fn state(&self) -> CityState {
        CityState::from(self)
    }
}

/// [`BOROUGH_CHANGED`] (saga step).
#[derive(Debug, Clone, Serialize, Deserialize, distributed::CommandInput)]
pub struct BoroughChangedInput {
    /// Always [`CITY_SHARD`].
    pub city: String,
    /// `bx.by`.
    pub borough: String,
    /// Bosses down in the borough.
    pub bosses_down: u32,
    /// The borough event's stream sequence.
    pub sequence: u64,
}

async fn handle_borough_changed(
    ctx: &CausalCommandContext<'_, City>,
    input: BoroughChangedInput,
) -> Result<PreparedCommand<Succeeded<TierReceipt>>, HandlerError> {
    if input.city != CITY_SHARD || parse_pair(&input.borough).is_none() {
        return Err(rejected("not the city, or not a borough"));
    }
    let repo = ctx.repo();
    let mut city = match repo.get(&input.city).await? {
        Some(c) => c,
        None => repo.create(),
    };
    let stale = city
        .boroughs
        .get(&input.borough)
        .is_some_and(|seen| seen.sequence >= input.sequence);
    if stale {
        return repo.commit(city)?.succeeded(TierReceipt { changed: false });
    }
    city.borough_seen(
        input.city,
        input.borough,
        BoroughSeen {
            bosses_down: input.bosses_down,
            sequence: input.sequence,
        },
    )
    .map_err(rejected)?;
    repo.publish_events()
        .commit(city)?
        .succeeded(TierReceipt { changed: true })
}

// ---------------------------------------------------------------- mounts

macro_rules! tier_command {
    ($name:ident, $agg:ty, $command:expr, $input:ty, $output:ty, $guard:expr, $handler:expr, $($cap:expr),*) => {
        /// Mount of the command (see the module docs).
        pub struct $name;
        impl<D> PortableCommand<D> for $name
        where
            D: CausalRouteDependencies<Aggregate = $agg> + Send + Sync + 'static,
        {
            fn install(self, routes: Routes<D>) -> Routes<D> {
                routes
                    .typed_command(typed_command::<$input, Succeeded<$output>>($command))
                    $(.requires($cap))*
                    .guarded($guard, $handler)
            }
        }
    };
}

tier_command!(
    BossDefeated,
    Hood,
    BOSS_DEFEATED,
    BossDefeatedInput,
    HoodReceipt,
    is_boss_room,
    handle_boss_defeated,
    Capability::Scheduling,
    Capability::Clock
);
tier_command!(
    BossRespawn,
    Hood,
    BOSS_RESPAWN,
    RespawnInput,
    HoodReceipt,
    schedule_guard::<Hood>,
    handle_respawn,
    Capability::Scheduling,
    Capability::Clock
);
tier_command!(
    HoodChanged,
    Borough,
    HOOD_CHANGED,
    HoodChangedInput,
    TierReceipt,
    is_saga::<Borough>,
    handle_hood_changed,
);
tier_command!(
    BoroughChanged,
    City,
    BOROUGH_CHANGED,
    BoroughChangedInput,
    TierReceipt,
    is_saga::<City>,
    handle_borough_changed,
);

/// Mount the hood commands on a `Hood` cell.
pub fn mount_hood(cell: AggregateCell<Hood>) -> AggregateCell<Hood> {
    cell.mount(BossDefeated).mount(BossRespawn)
}

/// Mount the borough command on a `Borough` cell.
pub fn mount_borough(cell: AggregateCell<Borough>) -> AggregateCell<Borough> {
    cell.mount(HoodChanged)
}

/// Mount the city command on the `City` cell.
pub fn mount_city(cell: AggregateCell<City>) -> AggregateCell<City> {
    cell.mount(BoroughChanged)
}

/// The saga step a hood's outward state asks for: its borough hears of it.
pub fn hood_to_borough(state: &HoodState, sequence: u64) -> Option<(String, HoodChangedInput)> {
    let hood = parse_pair(&state.hood)?;
    let borough = pair_shard(borough_of(hood));
    Some((
        borough.clone(),
        HoodChangedInput {
            borough,
            hood: state.hood.clone(),
            boss_down: state.boss_down,
            tier: state.tier,
            sequence,
        },
    ))
}

/// The saga step a borough's outward state asks for: the city hears of it.
pub fn borough_to_city(state: &BoroughState, sequence: u64) -> BoroughChangedInput {
    BoroughChangedInput {
        city: CITY_SHARD.to_string(),
        borough: state.borough.clone(),
        bosses_down: state.bosses_down,
        sequence,
    }
}

/// One step of the tier saga: a command into a tier cell.
#[derive(Debug, Clone, PartialEq)]
pub struct SagaStep {
    /// Cell kind ([`BOROUGH_KIND`] or [`CITY_KIND`]).
    pub kind: &'static str,
    /// Cell shard.
    pub shard: String,
    /// Command name.
    pub command: &'static str,
    /// Command input.
    pub input: serde_json::Value,
}

/// The saga step an outward tier event asks for (`None` for any other
/// message, and for the city, the top): a hood's [`HOOD_EVENT`] or
/// `hood.boss_returned` → its borough, a borough's [`BOROUGH_EVENT`] → the
/// city. `payload` is the event's canonical occurrence (the outbox
/// message's bytes, as the Queue delivers them).
pub fn saga_step(name: &str, payload: &[u8]) -> Option<SagaStep> {
    if !matches!(name, HOOD_EVENT | "hood.boss_returned" | BOROUGH_EVENT) {
        return None;
    }
    let occurrence = distributed::DomainEventOccurrence::from_canonical_bytes(payload).ok()?;
    let sequence = occurrence.aggregate_sequence();
    if name == BOROUGH_EVENT {
        let state: BoroughState = occurrence.decode_body().ok()?;
        return Some(SagaStep {
            kind: CITY_KIND,
            shard: CITY_SHARD.to_string(),
            command: BOROUGH_CHANGED,
            input: serde_json::to_value(borough_to_city(&state, sequence)).ok()?,
        });
    }
    let state: HoodState = occurrence.decode_body().ok()?;
    let (borough, input) = hood_to_borough(&state, sequence)?;
    Some(SagaStep {
        kind: BOROUGH_KIND,
        shard: borough,
        command: HOOD_CHANGED,
        input: serde_json::to_value(input).ok()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weather_lifts_as_bosses_fall() {
        let names: Vec<&str> = (0..=9).map(|n| WEATHER[weather_for(n) as usize]).collect();
        assert_eq!(names[0], "storm");
        assert_eq!(names[4], "rain");
        assert_eq!(names[7], "clearing");
        assert_eq!(names[9], "clear");
    }

    #[test]
    fn saga_steps_follow_the_geometry() {
        let state = HoodState {
            hood: "4.-1".into(),
            boss_down: true,
            tier: 2,
            last_by: "ana".into(),
            respawn_at_ms: 9,
        };
        let (borough, step) = hood_to_borough(&state, 7).expect("hood");
        assert_eq!(borough, "1.-1");
        assert_eq!((step.tier, step.sequence, step.boss_down), (2, 7, true));
        let city = borough_to_city(
            &BoroughState {
                borough: borough.clone(),
                bosses_down: 9,
                weather: 3,
                tier: 9,
                hoods_down: vec![],
            },
            3,
        );
        assert_eq!((city.city.as_str(), city.bosses_down), (CITY_SHARD, 9));
    }
}
