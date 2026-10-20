//! The durable side of Mob Pit: one `Crawler` aggregate per player, each in
//! its own native SQLite cell (`distributed` feature `cell-sqlite`, opened by
//! `game_demos`).
//!
//! The only command is [`RECORD_KILL`]. It is dispatched by the zone's fact
//! worker through `distributed_game::cell::CellFactSink`, never by a client:
//!
//! - the **principal** comes from the trusted server-built session
//!   (`x-user-id`, bound by the host from the player's verified ticket); the
//!   input has no identity field and unknown fields are rejected;
//! - the **XP amount** is decided here from the mob tier ([`xp_for_tier`]);
//!   the zone only reports which mob died;
//! - the command id is derived from the fact key, so a retry or re-emission
//!   replays the stored receipt through the cell's command ledger instead of
//!   crediting twice;
//! - `crawler.mob_killed` is a **domain event**: the commit publishes the
//!   post-kill [`CrawlerState`] into the cell's outbox in the same SQLite
//!   transaction ([`CrawlerMobKilledDomainEvent`]). The zone server drains
//!   that outbox onto a bus and the read-model service (`mobpit_service`)
//!   projects it into the `player_stats` table behind GraphQL `@live`.

use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, CausalRouteDependencies, HandlerError, PortableCommand, Routes,
};
use distributed::{sourced, DomainState, Entity};
use serde::{Deserialize, Serialize};
use std::time::UNIX_EPOCH;

/// Aggregate type (cell instance names are `Crawler:{principal}`).
pub const AGGREGATE_TYPE: &str = "Crawler";
/// Command dispatched for every `MobKilled` fact.
pub const RECORD_KILL: &str = "crawler.record_kill";
/// Event recorded by [`RECORD_KILL`].
pub const MOB_KILLED_EVENT: &str = "crawler.mob_killed";

/// XP per mob tier — server-side game rules, never client input.
pub fn xp_for_tier(tier: u8) -> Option<u32> {
    match tier {
        1 => Some(10),
        2 => Some(25),
        3 => Some(60),
        _ => None,
    }
}

/// Level for a total XP: level `n` starts at `40 · (n − 1)²` XP.
pub fn level_for(xp: u64) -> u32 {
    let mut level = 1u32;
    while level_floor(level + 1) <= xp {
        level += 1;
    }
    level
}

/// First XP of `level`.
pub fn level_floor(level: u32) -> u64 {
    let n = u64::from(level.saturating_sub(1));
    40 * n * n
}

/// One player's durable progress.
#[derive(Clone, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct Crawler {
    entity: Entity,
    /// Last display name seen with a kill (cosmetic; identity is the id).
    pub name: String,
    /// Total XP.
    pub xp: u64,
    /// Mobs killed.
    pub kills: u32,
    /// Derived from `xp`.
    pub level: u32,
}

/// The public post-kill state carried by `crawler.mob_killed` domain events
/// (versioned independently of the replay event and the snapshot). Its
/// fields are the `player_stats` read-model row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, DomainState)]
#[domain_state(version = 1)]
pub struct CrawlerState {
    /// Principal (the cell's stream id).
    pub player_id: String,
    /// Display name at the last kill.
    pub name: String,
    /// Total XP.
    pub xp: u64,
    /// Mobs killed.
    pub kills: u32,
    /// Level for `xp`.
    pub level: u32,
}

impl From<&Crawler> for CrawlerState {
    fn from(crawler: &Crawler) -> Self {
        Self {
            player_id: crawler.id().to_string(),
            name: crawler.name.clone(),
            xp: crawler.xp,
            kills: crawler.kills,
            level: crawler.level,
        }
    }
}

#[sourced(
    entity,
    aggregate_type = "Crawler",
    events = "CrawlerEvent",
    domain_state = CrawlerState
)]
impl Crawler {
    #[event("crawler.mob_killed", version = 1, domain)]
    fn mob_killed(&mut self, id: String, name: String, mob: String, tier: u8, xp: u32) {
        let _ = (mob, tier);
        self.entity.set_id(id);
        self.name = name;
        self.xp += u64::from(xp);
        self.kills += 1;
        self.level = level_for(self.xp);
    }
}

impl Crawler {
    /// Principal (stream id).
    pub fn id(&self) -> &str {
        self.entity.id()
    }

    /// Committed stream version.
    pub fn version(&self) -> u64 {
        self.entity.version()
    }
}

/// Arguments of a recorded `crawler.mob_killed` event, in declaration order
/// (`EventRecord::decode` of the stored payload).
pub type MobKilledArgs = (String, String, String, u8, u32);

/// [`RECORD_KILL`] input. No principal, no XP: both are server-decided.
///
/// `FactCommand` binds it to [`RECORD_KILL`] (the name [`RecordKill`]
/// mounts it under), so the zone emits it typed — `ctx.emit_keyed(killer,
/// RecordKillInput { .. }, &[mob, generation])` — and the derive rejects
/// identity or reward fields at compile time.
#[derive(
    Debug, Clone, Serialize, Deserialize, distributed::CommandInput, distributed_game::FactCommand,
)]
#[fact(command = RECORD_KILL)]
pub struct RecordKillInput {
    /// Mob instance (`{zone}/m{id}`).
    pub mob: String,
    /// Mob tier (1..=3); XP comes from [`xp_for_tier`].
    pub tier: u8,
    /// Display name at the time of the kill (server-sanitized).
    pub name: String,
}

/// [`RECORD_KILL`] result, replayed verbatim for a repeated fact key.
#[derive(Debug, Clone, Serialize, Deserialize, distributed::CommandOutput)]
pub struct KillReceipt {
    /// Principal credited.
    pub crawler: String,
    /// Display name recorded.
    pub name: String,
    /// Mob instance.
    pub mob: String,
    /// Mob tier.
    pub tier: u8,
    /// XP credited by this kill.
    pub xp: u32,
    /// Total XP after it.
    pub total_xp: u64,
    /// Total kills after it.
    pub kills: u32,
    /// Level after it.
    pub level: u32,
    /// Stream sequence of the recorded event (leaderboard occurrence position).
    pub sequence: u64,
    /// Event time (Unix ms).
    pub at_ms: u64,
}

/// Mounts [`RECORD_KILL`] on a `Crawler` cell.
pub struct RecordKill;

impl<D> PortableCommand<D> for RecordKill
where
    D: CausalRouteDependencies<Aggregate = Crawler> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<RecordKillInput, Succeeded<KillReceipt>>(
                RECORD_KILL,
            ))
            .guarded(
                |ctx: &CausalCommandContext<'_, Crawler>| ctx.session().user_id().is_some(),
                handle_record_kill,
            )
    }
}

async fn handle_record_kill(
    ctx: &CausalCommandContext<'_, Crawler>,
    input: RecordKillInput,
) -> Result<PreparedCommand<Succeeded<KillReceipt>>, HandlerError> {
    // Identity: the trusted session only. The cell is the principal's own
    // shard, so a mismatched principal cannot load it either.
    let principal = ctx.session().user_id().unwrap_or_default().to_string();
    let xp = xp_for_tier(input.tier)
        .ok_or_else(|| HandlerError::Rejected(format!("unknown mob tier {}", input.tier)))?;
    let name: String = input.name.chars().take(16).collect();
    let repo = ctx.repo();
    let mut crawler = match repo.get(&principal).await? {
        Some(crawler) => crawler,
        None => repo.create(),
    };
    crawler
        .mob_killed(
            principal.clone(),
            name.clone(),
            input.mob.clone(),
            input.tier,
            xp,
        )
        .map_err(|e| HandlerError::Rejected(e.to_string()))?;
    let (sequence, at_ms) = crawler
        .entity
        .events()
        .last()
        .map(|record| {
            let at = record
                .timestamp
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64);
            (record.sequence, at)
        })
        .unwrap_or_default();
    let receipt = KillReceipt {
        crawler: principal,
        name,
        mob: input.mob,
        tier: input.tier,
        xp,
        total_xp: crawler.xp,
        kills: crawler.kills,
        level: crawler.level,
        sequence,
        at_ms,
    };
    // Event, ledger receipt, snapshot and the outbox row of the domain event
    // commit in one cell transaction.
    repo.publish_events().commit(crawler)?.succeeded(receipt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_follow_the_quadratic_curve() {
        assert_eq!(level_for(0), 1);
        assert_eq!(level_for(39), 1);
        assert_eq!(level_for(40), 2);
        assert_eq!(level_for(159), 2);
        assert_eq!(level_for(160), 3);
        assert_eq!(level_floor(3), 160);
        assert_eq!(xp_for_tier(0), None);
        assert_eq!(xp_for_tier(3), Some(60));
    }
}
