//! A world's live content rollout as a cell (feature `cell`): the barrier
//! the native `WorldCoordinator` runs across hosts, for zones hosted as
//! celld objects, where no process sees every zone.
//!
//! One `ContentRollout` cell per world (kind [`CONTENT_ROLLOUT_KIND`],
//! usually shard `world`) holds the live content version and the phase of a
//! rollout. An admin starts one ([`START`]); the cell then drives itself on
//! its own schedule (`ctx.schedule`):
//!
//! | phase | when | what every zone does (it reads the cell off the tick) |
//! |---|---|---|
//! | [`PHASE_FREEZING`] | [`START`] | freeze (Freeze notice with the change notes) |
//! | [`PHASE_APPLIED`] | `freeze_ms` later ([`APPLY`]) | switch to the new version, resume (Resume notice) |
//! | [`PHASE_RUNNING`] | `settle_ms` later ([`SETTLE`]) | — (zones that slept through it wake on the new version) |
//!
//! Zones apply the version through `ZoneHost::set_content_version` (facts
//! and stamps carry it from then on) and the host's
//! [`StalePolicy`](crate::world::StalePolicy) decides what happens to
//! clients still on the old version (e.g. held until a safe room; a hard
//! rollout reloads them at once). Every phase change is an outward domain
//! event ([`ContentState`]).
//!
//! Unlike the native barrier there is no acknowledgement round: a zone
//! that is not ready to freeze ([`Zone::freeze_ready`](crate::Zone::freeze_ready))
//! keeps ticking until it is or until the version is applied.

use std::time::{Duration, SystemTime};

use distributed::cell_host::AggregateCell;
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, CausalRouteDependencies, HandlerError, PortableCommand, Routes,
};
use distributed::schedule::{schedule_guard, Capability, Schedule};
use distributed::{sourced, Entity};
use serde::{Deserialize, Serialize};

/// Cell kind (`content-rollout/{world}`).
pub const CONTENT_ROLLOUT_KIND: &str = "content-rollout";
/// Role an admin's session must carry to start a rollout.
pub const CONTENT_ADMIN_ROLE: &str = "content-admin";
/// Start a rollout (admin).
pub const START: &str = "content_rollout.start";
/// Schedule only: apply the new version.
pub const APPLY: &str = "content_rollout.apply";
/// Schedule only: the rollout is over.
pub const SETTLE: &str = "content_rollout.settle";
/// No rollout in progress.
pub const PHASE_RUNNING: &str = "running";
/// Zones freeze.
pub const PHASE_FREEZING: &str = "freezing";
/// The new version is live; zones resume on it.
pub const PHASE_APPLIED: &str = "applied";
const ALARM: &str = "rollout";

/// One world's content.
#[derive(Clone, Debug, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct ContentRollout {
    entity: Entity,
    /// Live content version ("" = the zones' built-in default).
    pub version: String,
    /// The version being rolled out (during a rollout).
    pub target: String,
    /// [`PHASE_RUNNING`], [`PHASE_FREEZING`] or [`PHASE_APPLIED`].
    pub phase: String,
    /// Change notes of the last rollout.
    pub notes: String,
    /// When the phase began (cell clock, ms).
    pub since_ms: u64,
    /// Rollouts so far.
    pub rollouts: u32,
    /// Hard rollout: stale clients reload at once instead of at a gate.
    pub hard: bool,
}

/// What zones and read models see of the rollout.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, distributed::DomainState)]
#[domain_state(version = 1)]
pub struct ContentState {
    /// The world (the cell's stream id).
    pub world: String,
    /// Live content version.
    pub version: String,
    /// The version being rolled out.
    pub target: String,
    /// The phase.
    pub phase: String,
    /// Change notes.
    pub notes: String,
    /// When the phase began (ms).
    pub since_ms: u64,
    /// Hard rollout: every stale client reloads on apply
    /// (`ZoneHost::reload_stale_players`); else they keep playing under the
    /// zones' `StalePolicy`.
    pub hard: bool,
}

impl From<&ContentRollout> for ContentState {
    fn from(c: &ContentRollout) -> Self {
        Self {
            world: c.entity.id().to_string(),
            version: c.version.clone(),
            target: c.target.clone(),
            phase: if c.phase.is_empty() {
                PHASE_RUNNING.to_string()
            } else {
                c.phase.clone()
            },
            notes: c.notes.clone(),
            since_ms: c.since_ms,
            hard: c.hard,
        }
    }
}

#[sourced(
    entity,
    aggregate_type = "ContentRollout",
    events = "ContentRolloutEvent",
    domain_state = ContentState
)]
impl ContentRollout {
    #[event("content.freezing", version = 1, domain)]
    fn freezing(&mut self, world: String, target: String, notes: String, hard: bool, at_ms: u64) {
        if self.entity.id().is_empty() {
            self.entity.set_id(world);
        }
        self.hard = hard;
        self.target = target;
        self.notes = notes;
        self.phase = PHASE_FREEZING.to_string();
        self.since_ms = at_ms;
    }

    #[event("content.applied", version = 1, domain)]
    fn applied(&mut self, at_ms: u64) {
        self.version = std::mem::take(&mut self.target);
        self.phase = PHASE_APPLIED.to_string();
        self.since_ms = at_ms;
        self.rollouts += 1;
    }

    #[event("content.settled", version = 1, domain)]
    fn settled(&mut self, at_ms: u64) {
        self.phase = PHASE_RUNNING.to_string();
        self.since_ms = at_ms;
    }
}

impl ContentRollout {
    /// The outward state.
    pub fn state(&self) -> ContentState {
        ContentState::from(self)
    }
}

/// [`START`] input.
#[derive(Debug, Clone, Serialize, Deserialize, distributed::CommandInput)]
pub struct StartInput {
    /// The world (the cell's shard).
    pub world: String,
    /// Version to roll out.
    pub version: String,
    /// Change notes shown to players.
    pub notes: String,
    /// How long zones stay frozen before the version is applied (ms).
    pub freeze_ms: u64,
    /// Reload every stale client at once (else: at a gate).
    pub hard: bool,
}

/// [`APPLY`] / [`SETTLE`] input.
#[derive(Debug, Clone, Serialize, Deserialize, distributed::CommandInput)]
pub struct PhaseInput {
    /// The world.
    pub world: String,
}

/// A rollout command's answer.
#[derive(Debug, Clone, Serialize, Deserialize, distributed::CommandOutput)]
pub struct RolloutReceipt {
    /// The phase after the command.
    pub phase: String,
    /// Live version after the command.
    pub version: String,
}

fn rejected(message: impl std::fmt::Display) -> HandlerError {
    HandlerError::Rejected(message.to_string())
}

fn now_ms(ctx: &CausalCommandContext<'_, ContentRollout>) -> Result<u64, HandlerError> {
    ctx.now()?
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|now| now.as_millis() as u64)
        .map_err(|_| rejected("host clock is before the Unix epoch"))
}

fn at(world: &str, at_ms: u64, command: &str) -> Result<Schedule, HandlerError> {
    Schedule::at(
        ALARM,
        SystemTime::UNIX_EPOCH + Duration::from_millis(at_ms),
        command,
        &PhaseInput {
            world: world.to_string(),
        },
    )
    .map_err(rejected)
}

/// How long zones keep resuming notices before the rollout is over (ms).
const SETTLE_MS: u64 = 3_000;

fn is_admin(ctx: &CausalCommandContext<'_, ContentRollout>) -> bool {
    ctx.session().has_role(CONTENT_ADMIN_ROLE)
}

async fn handle_start(
    ctx: &CausalCommandContext<'_, ContentRollout>,
    input: StartInput,
) -> Result<PreparedCommand<Succeeded<RolloutReceipt>>, HandlerError> {
    let version = input.version.trim().to_string();
    if version.is_empty() || version.len() > 64 {
        return Err(rejected("version must be 1..=64 characters"));
    }
    let freeze_ms = input.freeze_ms.clamp(500, 60_000);
    let now = now_ms(ctx)?;
    let repo = ctx.repo();
    let mut rollout = match repo.get(&input.world).await? {
        Some(r) => r,
        None => repo.create(),
    };
    if !rollout.phase.is_empty() && rollout.phase != PHASE_RUNNING {
        return Err(rejected("a rollout is in progress"));
    }
    if rollout.version == version {
        return Err(rejected("that version is live"));
    }
    let notes: String = input.notes.chars().take(500).collect();
    rollout
        .freezing(input.world.clone(), version, notes, input.hard, now)
        .map_err(rejected)?;
    ctx.schedule(at(&input.world, now + freeze_ms, APPLY)?)?;
    let receipt = RolloutReceipt {
        phase: rollout.phase.clone(),
        version: rollout.version.clone(),
    };
    repo.publish_events().commit(rollout)?.succeeded(receipt)
}

async fn handle_apply(
    ctx: &CausalCommandContext<'_, ContentRollout>,
    input: PhaseInput,
) -> Result<PreparedCommand<Succeeded<RolloutReceipt>>, HandlerError> {
    let now = now_ms(ctx)?;
    let repo = ctx.repo();
    let Some(mut rollout) = repo.get(&input.world).await? else {
        return Err(rejected("no rollout"));
    };
    if rollout.phase != PHASE_FREEZING {
        let receipt = RolloutReceipt {
            phase: rollout.phase.clone(),
            version: rollout.version.clone(),
        };
        return repo.commit(rollout)?.succeeded(receipt);
    }
    rollout.applied(now).map_err(rejected)?;
    ctx.schedule(at(&input.world, now + SETTLE_MS, SETTLE)?)?;
    let receipt = RolloutReceipt {
        phase: rollout.phase.clone(),
        version: rollout.version.clone(),
    };
    repo.publish_events().commit(rollout)?.succeeded(receipt)
}

async fn handle_settle(
    ctx: &CausalCommandContext<'_, ContentRollout>,
    input: PhaseInput,
) -> Result<PreparedCommand<Succeeded<RolloutReceipt>>, HandlerError> {
    let now = now_ms(ctx)?;
    let repo = ctx.repo();
    let Some(mut rollout) = repo.get(&input.world).await? else {
        return Err(rejected("no rollout"));
    };
    rollout.settled(now).map_err(rejected)?;
    let receipt = RolloutReceipt {
        phase: rollout.phase.clone(),
        version: rollout.version.clone(),
    };
    repo.publish_events().commit(rollout)?.succeeded(receipt)
}

macro_rules! rollout_command {
    ($name:ident, $command:expr, $input:ty, $guard:expr, $handler:expr) => {
        struct $name;
        impl<D> PortableCommand<D> for $name
        where
            D: CausalRouteDependencies<Aggregate = ContentRollout> + Send + Sync + 'static,
        {
            fn install(self, routes: Routes<D>) -> Routes<D> {
                routes
                    .typed_command(typed_command::<$input, Succeeded<RolloutReceipt>>($command))
                    .requires(Capability::Scheduling)
                    .requires(Capability::Clock)
                    .guarded($guard, $handler)
            }
        }
    };
}

rollout_command!(Start, START, StartInput, is_admin, handle_start);
rollout_command!(
    Apply,
    APPLY,
    PhaseInput,
    schedule_guard::<ContentRollout>,
    handle_apply
);
rollout_command!(
    Settle,
    SETTLE,
    PhaseInput,
    schedule_guard::<ContentRollout>,
    handle_settle
);

/// Mount every rollout command on a `ContentRollout` cell.
pub fn mount_content_rollout(cell: AggregateCell<ContentRollout>) -> AggregateCell<ContentRollout> {
    cell.mount(Start).mount(Apply).mount(Settle)
}

/// What a zone should do for a rollout state, given its own content
/// version and whether it is frozen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZoneStep {
    /// Nothing to do.
    Stay,
    /// Freeze with the notes (the rollout began).
    Freeze,
    /// Switch to `version` (and resume if frozen).
    Apply(String),
}

/// The zone side of the barrier (pure; the celld zone host runs it).
pub fn zone_step(state: &ContentState, zone_version: &str, frozen: bool) -> ZoneStep {
    match state.phase.as_str() {
        PHASE_FREEZING if !frozen => ZoneStep::Freeze,
        PHASE_FREEZING => ZoneStep::Stay,
        _ if state.version != zone_version || frozen => ZoneStep::Apply(state.version.clone()),
        _ => ZoneStep::Stay,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(phase: &str, version: &str) -> ContentState {
        ContentState {
            world: "world".into(),
            version: version.into(),
            target: String::new(),
            phase: phase.into(),
            notes: String::new(),
            since_ms: 0,
            hard: false,
        }
    }

    #[test]
    fn zones_follow_the_phases() {
        assert_eq!(
            zone_step(&state(PHASE_RUNNING, ""), "", false),
            ZoneStep::Stay
        );
        assert_eq!(
            zone_step(&state(PHASE_FREEZING, ""), "", false),
            ZoneStep::Freeze
        );
        assert_eq!(
            zone_step(&state(PHASE_FREEZING, ""), "", true),
            ZoneStep::Stay
        );
        assert_eq!(
            zone_step(&state(PHASE_APPLIED, "v2"), "", true),
            ZoneStep::Apply("v2".into())
        );
        // A zone that slept through the rollout wakes on the new version.
        assert_eq!(
            zone_step(&state(PHASE_RUNNING, "v2"), "", false),
            ZoneStep::Apply("v2".into())
        );
        assert_eq!(
            zone_step(&state(PHASE_RUNNING, "v2"), "v2", false),
            ZoneStep::Stay
        );
    }
}
