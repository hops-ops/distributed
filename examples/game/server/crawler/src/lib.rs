//! The durable side of Mob Pit: one `Crawler` aggregate per player, each in
//! its own celld cell (`examples/game/cells`, kind [`CRAWLER_KIND`]), fed by
//! the zone server's `distributed_game::celld::CelldFactSink`.
//!
//! The only command is [`RECORD_KILL`], declared on the aggregate method
//! [`Crawler::record_kill`] with `#[command]` (which generates the
//! [`RecordKill`] mount and its handler). It is dispatched by the zone's fact
//! worker through `distributed_game::celld::CelldFactSink`, never by a client:
//!
//! - the **principal** comes from the trusted server-built session
//!   (`x-user-id`, bound by the host from the player's verified ticket) into
//!   the method's `#[principal]` parameter; the input has no identity field
//!   (a field named like it would not compile) and unknown fields are
//!   rejected;
//! - the **XP amount** is decided here from the mob tier ([`xp_for_tier`]);
//!   the zone only reports which mob died;
//! - the command id is derived from the fact key, so a retry or re-emission
//!   replays the stored receipt through the cell's command ledger instead of
//!   crediting twice;
//! - `crawler.mob_killed` is a **domain event**: the commit publishes the
//!   post-kill [`CrawlerState`] into the cell's outbox in the same cell (SQLite)
//!   transaction ([`CrawlerMobKilledDomainEvent`]). celld drains that outbox
//!   to a Queue whose consumer relays it to the zone server, which publishes
//!   it onto a bus; [`stats`] is its `player_stats` read model, projected by
//!   the read-model service (`mobpit_service`, GraphQL `@live`) and by the
//!   zone server (its crawler list).

pub mod stats;

use distributed::{sourced, DomainState, Entity};
use serde::{Deserialize, Serialize};

/// Aggregate type (cell instance names are `Crawler:{principal}`).
pub const AGGREGATE_TYPE: &str = "Crawler";
/// celld cell kind (URL segment) of the Crawler cells: `crawler/{principal}`.
pub const CRAWLER_KIND: &str = "crawler";
/// celld cell kind of per-player presence (the runtime lifecycle facts).
pub const PRESENCE_KIND: &str = "presence";
/// Redis kills board (`+1` per kill), written by the zone server's
/// leaderboard projector and served by `mobpit_service`'s GraphQL roots.
pub const KILLS_BOARD: &str = "mobpit.kills";
/// Redis XP board (`+xp` per kill).
pub const XP_BOARD: &str = "mobpit.xp";
/// Default Redis key prefix of the boards (zone server and service must agree).
pub const BOARD_REDIS_PREFIX: &str = "mobpit";
/// Command dispatched for every `MobKilled` fact.
pub const RECORD_KILL: &str = RecordKill::COMMAND;
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
    /// Credit a kill to the session principal: XP from the mob tier, the
    /// name capped at 16 characters. The receipt carries the recorded
    /// event's stream position (the leaderboard occurrence).
    ///
    /// The cell is the principal's own shard, so a mismatched principal
    /// cannot load it either. Event, ledger receipt, snapshot and the outbox
    /// row of the domain event commit in one cell transaction.
    #[command(
        "crawler.record_kill",
        shard = principal,
        load = or_create,
        outcome = succeeded
    )]
    pub fn record_kill(
        &mut self,
        #[principal] principal: &str,
        input: RecordKillInput,
    ) -> Result<KillReceipt, String> {
        let xp =
            xp_for_tier(input.tier).ok_or_else(|| format!("unknown mob tier {}", input.tier))?;
        let name: String = input.name.chars().take(16).collect();
        self.mob_killed(
            principal.to_string(),
            name.clone(),
            input.mob.clone(),
            input.tier,
            xp,
        )
        .map_err(|e| e.to_string())?;
        let recorded = self.entity.last_recorded().ok_or("no event recorded")?;
        Ok(KillReceipt {
            crawler: principal.to_string(),
            name,
            mob: input.mob,
            tier: input.tier,
            xp,
            total_xp: self.xp,
            kills: self.kills,
            level: self.level,
            sequence: recorded.sequence,
            at_ms: recorded.unix_ms(),
        })
    }

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
/// `FactCommand` binds it to [`RECORD_KILL`] (the name the generated
/// [`RecordKill`] mounts it under), so the zone emits it typed — `ctx.emit_keyed(killer,
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
