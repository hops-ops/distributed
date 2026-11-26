//! Portable aggregate schedules, backend capabilities and the schedule store.
//!
//! A command handler schedules work for its own aggregate with its events:
//!
//! ```rust,ignore
//! let now = ctx.now()?;
//! ctx.schedule(Schedule::at("expiry", now + ttl, "order.expire", &ExpireInput { order_id })?)?;
//! ```
//!
//! The schedule row commits in the **same transaction** as the command's
//! events, ledger receipt and outbox rows, on every
//! [`TransactionalCommit`](crate::TransactionalCommit) backend, so a failed
//! command leaves no schedule. The batch must commit exactly one aggregate
//! stream, which owns the schedule. At `due` the host dispatches `command`
//! into that aggregate with its trusted schedule session; the handler may
//! schedule the same name again (a periodic tick without a global cron).
//!
//! Semantics, identical on every backend that dispatches schedules:
//!
//! - One row per (owner, **name**). Scheduling an existing name replaces its
//!   due time, command and input and bumps its `generation`; cancelling
//!   disarms it.
//! - A fired schedule runs through the command ledger with a command id
//!   derived from `(name, generation)`, so each arm has its effects at most
//!   once even when the host crashes between the commit and the settlement.
//! - Settlement only disarms the generation that fired; a handler that
//!   re-schedules its own name produced a newer generation, which stays.
//! - Rejections are terminal for that arm; retryable failures back off
//!   exponentially from one second up to five minutes.
//!
//! **Backends declare [`Capabilities`].** Every backend stores schedule rows
//! (the in-memory repository in a per-stream table, SQL in
//! `aggregate_schedules`, an aggregate cell in its own table); only some
//! *dispatch* them ([`Capability::Scheduling`]): a host that fires its own
//! rows, and a shared store (SQL, in-memory) once a schedule worker is
//! attached to it. Every backend has a clock ([`Capability::Clock`]): the
//! in-memory repository's injected [`Clock`](crate::clock::Clock), the database
//! clock on SQL, the host clock elsewhere. Routes declare what they need
//! (`.requires(Capability::Scheduling)`); registering one on a host without
//! it fails at service construction, never at the first commit.

use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::repository::RepositoryError;
use crate::StreamIdentity;

/// Command-ledger service id of fired schedules.
pub const SCHEDULE_SERVICE_ID: &str = "distributed.schedule";
/// Longest schedule name.
pub const MAX_SCHEDULE_NAME_BYTES: usize = 128;
/// Largest serialized schedule input.
pub const MAX_SCHEDULE_INPUT_BYTES: usize = 16 * 1024;
/// Most schedule writes one command may stage.
pub const MAX_SCHEDULE_WRITES_PER_COMMAND: usize = 16;
/// Most schedules one aggregate may hold armed at once.
pub const MAX_ARMED_SCHEDULES: usize = 1024;
/// Most due schedules fired per wake; the rest fire on the next wake.
pub const SCHEDULE_FIRE_BATCH: usize = 32;
/// First retry delay after a retryable schedule failure.
pub const SCHEDULE_RETRY_BASE: Duration = Duration::from_secs(1);
/// Longest retry delay after repeated retryable schedule failures.
pub const SCHEDULE_RETRY_MAX: Duration = Duration::from_secs(300);

/// Something a host may or may not provide to the commands it runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Capability {
    /// The host dispatches due schedules (`ctx.schedule`) back into their
    /// aggregate.
    Scheduling,
    /// The host stamps its clock into each command (`ctx.now()`).
    Clock,
}

impl std::fmt::Display for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Scheduling => "scheduling",
            Self::Clock => "clock",
        })
    }
}

/// What a backend provides ([`Capability`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Capabilities {
    /// Due schedules are dispatched.
    pub scheduling: bool,
    /// A clock is stamped into commands.
    pub clock: bool,
}

impl Capabilities {
    /// Nothing.
    pub const NONE: Self = Self {
        scheduling: false,
        clock: false,
    };
    /// Everything (a host that fires its own schedules).
    pub const ALL: Self = Self {
        scheduling: true,
        clock: true,
    };

    /// Whether `capability` is provided.
    pub fn has(&self, capability: Capability) -> bool {
        match capability {
            Capability::Scheduling => self.scheduling,
            Capability::Clock => self.clock,
        }
    }

    /// The members of `required` this backend lacks.
    pub fn missing(&self, required: &[Capability]) -> Vec<Capability> {
        let mut missing: Vec<Capability> = required
            .iter()
            .copied()
            .filter(|capability| !self.has(*capability))
            .collect();
        missing.sort();
        missing.dedup();
        missing
    }
}

/// Why a schedule could not be armed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleError {
    InvalidName,
    EmptyCommand,
    InvalidInput,
    DueOutOfRange,
    TooManyWrites,
    TooManyArmed,
    /// The batch committed no aggregate, or several: a schedule belongs to
    /// exactly one.
    NoSingleOwner,
    /// The route did not declare `.requires(Capability::Scheduling)`.
    NotDeclared,
}

impl std::fmt::Display for ScheduleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidName => write!(
                f,
                "schedule name must be 1..={MAX_SCHEDULE_NAME_BYTES} bytes of [A-Za-z0-9._:-]"
            ),
            Self::EmptyCommand => f.write_str("schedule command must not be empty"),
            Self::InvalidInput => write!(
                f,
                "schedule input must be a JSON object of at most {MAX_SCHEDULE_INPUT_BYTES} bytes"
            ),
            Self::DueOutOfRange => f.write_str("schedule due time is out of range"),
            Self::TooManyWrites => write!(
                f,
                "a command may stage at most {MAX_SCHEDULE_WRITES_PER_COMMAND} schedule writes"
            ),
            Self::TooManyArmed => write!(
                f,
                "an aggregate may hold at most {MAX_ARMED_SCHEDULES} armed schedules"
            ),
            Self::NotDeclared => f.write_str(
                "ctx.schedule needs a route declared with .requires(Capability::Scheduling)",
            ),
            Self::NoSingleOwner => f.write_str(
                "a command that schedules must commit exactly one aggregate, which owns the schedule",
            ),
        }
    }
}

impl std::error::Error for ScheduleError {}

impl From<ScheduleError> for RepositoryError {
    fn from(error: ScheduleError) -> Self {
        RepositoryError::Model(error.to_string())
    }
}

/// When a schedule is due.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScheduleDue {
    /// Absolute epoch milliseconds.
    At(i64),
    /// Milliseconds after the commit, resolved by the backend's clock.
    After(u64),
}

/// Work a command schedules for its aggregate: when due, dispatch `command`
/// with `input` into it.
#[derive(Clone, Debug, PartialEq)]
pub struct Schedule {
    name: String,
    command: String,
    input: Value,
    due: ScheduleDue,
}

impl Schedule {
    /// Schedule `name` to dispatch `command` with `input` at `due`.
    ///
    /// Periodic handlers re-schedule from the fired due time
    /// (`ctx.scheduled_due()`), so ticks do not drift with handler latency.
    pub fn at(
        name: impl Into<String>,
        due: SystemTime,
        command: impl Into<String>,
        input: &impl Serialize,
    ) -> Result<Self, ScheduleError> {
        Self::build(
            name,
            ScheduleDue::At(system_time_to_ms(due)?),
            command,
            input,
        )
    }

    /// Schedule `name` to fire `delay` after the command commits, measured by
    /// the backend's clock (the injected [`Clock`](crate::clock::Clock) in memory,
    /// the database clock on SQL, the host clock elsewhere).
    pub fn after(
        name: impl Into<String>,
        delay: Duration,
        command: impl Into<String>,
        input: &impl Serialize,
    ) -> Result<Self, ScheduleError> {
        let delay = u64::try_from(delay.as_millis()).map_err(|_| ScheduleError::DueOutOfRange)?;
        Self::build(name, ScheduleDue::After(delay), command, input)
    }

    fn build(
        name: impl Into<String>,
        due: ScheduleDue,
        command: impl Into<String>,
        input: &impl Serialize,
    ) -> Result<Self, ScheduleError> {
        let input = serde_json::to_value(input).map_err(|_| ScheduleError::InvalidInput)?;
        let schedule = Self {
            name: name.into(),
            command: command.into(),
            input,
            due,
        };
        schedule.validate()?;
        Ok(schedule)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn input(&self) -> &Value {
        &self.input
    }

    /// Absolute due time, or `None` for a relative schedule not yet
    /// committed.
    pub fn due(&self) -> Option<SystemTime> {
        match self.due {
            ScheduleDue::At(ms) => Some(ms_to_system_time(ms)),
            ScheduleDue::After(_) => None,
        }
    }

    /// The relative delay, for backends that resolve it in SQL.
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn relative_delay_ms(&self) -> Option<u64> {
        match self.due {
            ScheduleDue::After(delay) => Some(delay),
            ScheduleDue::At(_) => None,
        }
    }

    /// Resolve a relative due time against the committing backend's clock.
    pub(crate) fn resolve(&mut self, now: SystemTime) -> Result<(), ScheduleError> {
        if let ScheduleDue::After(delay) = self.due {
            let now = system_time_to_ms(now)?;
            let due = now
                .checked_add(i64::try_from(delay).map_err(|_| ScheduleError::DueOutOfRange)?)
                .ok_or(ScheduleError::DueOutOfRange)?;
            if due > MAX_DUE_MS {
                return Err(ScheduleError::DueOutOfRange);
            }
            self.due = ScheduleDue::At(due);
        }
        Ok(())
    }

    /// Absolute due time; storage refuses an unresolved relative schedule.
    pub(crate) fn due_at_ms(&self) -> Result<i64, RepositoryError> {
        match self.due {
            ScheduleDue::At(ms) => Ok(ms),
            ScheduleDue::After(_) => Err(RepositoryError::Model(
                "relative schedule reached storage without being resolved".into(),
            )),
        }
    }

    #[cfg(any(
        feature = "sqlite",
        feature = "postgres",
        all(feature = "cloudflare-workers", target_arch = "wasm32")
    ))]
    pub(crate) fn input_json(&self) -> Result<String, RepositoryError> {
        serde_json::to_string(&self.input)
            .map_err(|error| RepositoryError::Model(format!("schedule input: {error}")))
    }

    pub(crate) fn validate(&self) -> Result<(), ScheduleError> {
        validate_name(&self.name)?;
        if self.command.trim().is_empty() {
            return Err(ScheduleError::EmptyCommand);
        }
        let size = serde_json::to_vec(&self.input)
            .map_err(|_| ScheduleError::InvalidInput)?
            .len();
        if !self.input.is_object() || size > MAX_SCHEDULE_INPUT_BYTES {
            return Err(ScheduleError::InvalidInput);
        }
        match self.due {
            ScheduleDue::At(ms) if !(0..=MAX_DUE_MS).contains(&ms) => {
                Err(ScheduleError::DueOutOfRange)
            }
            ScheduleDue::After(delay) if delay > MAX_DUE_MS as u64 => {
                Err(ScheduleError::DueOutOfRange)
            }
            _ => Ok(()),
        }
    }
}

/// Resolve every relative schedule of a batch against a backend clock.
pub(crate) fn resolve_schedule_writes(
    writes: &mut [ScheduleWrite],
    now: SystemTime,
) -> Result<(), RepositoryError> {
    for write in writes {
        if let ScheduleWrite::Arm(schedule) = write {
            schedule.resolve(now)?;
        }
    }
    Ok(())
}

/// Latest due time (epoch ms) a schedule may carry: 2^53 - 1, the largest
/// integer every backend stores and every JSON reader decodes exactly.
const MAX_DUE_MS: i64 = 9_007_199_254_740_991;

/// One schedule write staged by a command and committed with its events.
#[derive(Clone, Debug, PartialEq)]
pub enum ScheduleWrite {
    /// Schedule (or re-schedule) the name.
    Arm(Schedule),
    /// Disarm a name. Cancelling a name that is not armed is a no-op.
    Cancel(String),
}

impl ScheduleWrite {
    pub fn name(&self) -> &str {
        match self {
            Self::Arm(schedule) => schedule.name(),
            Self::Cancel(name) => name,
        }
    }
}

/// Validate a batch's schedule writes before any storage participant runs.
pub(crate) fn validate_schedule_writes(writes: &[ScheduleWrite]) -> Result<(), RepositoryError> {
    if writes.len() > MAX_SCHEDULE_WRITES_PER_COMMAND {
        return Err(ScheduleError::TooManyWrites.into());
    }
    for write in writes {
        match write {
            ScheduleWrite::Arm(schedule) => schedule.validate()?,
            ScheduleWrite::Cancel(name) => validate_name(name)?,
        }
    }
    Ok(())
}

/// The one stream of a batch that owns its schedule writes (None when the
/// batch has none).
pub(crate) fn schedule_owner(
    batch: &crate::repository::CommitBatch<'_>,
) -> Result<Option<StreamIdentity>, RepositoryError> {
    if batch.schedules.is_empty() {
        return Ok(None);
    }
    validate_schedule_writes(&batch.schedules)?;
    match batch.streams.as_slice() {
        [stream] => Ok(Some(stream.identity.clone())),
        _ => Err(ScheduleError::NoSingleOwner.into()),
    }
}

/// An armed schedule as stored.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArmedSchedule {
    pub name: String,
    pub command: String,
    pub input: Value,
    pub due_at_ms: i64,
    pub generation: u64,
    pub attempts: u32,
}

impl ArmedSchedule {
    pub fn due(&self) -> SystemTime {
        ms_to_system_time(self.due_at_ms)
    }
}

/// How a fired schedule settled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ScheduleSettlement {
    /// Ran (or replayed) or failed terminally: disarm this generation.
    Done,
    /// Retry this generation at `due_at_ms`.
    Retry { due_at_ms: i64 },
}

/// What a command backend provides to the commands it hosts
/// ([`Capabilities`]) and its clock. Implemented by the framework's
/// repository adapters; checked when a route is registered.
pub(crate) trait BackendHost: Send + Sync {
    /// The backend's capabilities.
    fn capabilities(&self) -> Capabilities;

    /// The backend's clock now (store clock, database clock, host clock).
    fn host_now(
        &self,
    ) -> impl std::future::Future<Output = Result<SystemTime, RepositoryError>> + Send + '_;
}

/// A store holding the schedule rows of many owners (SQL, in-memory) that
/// it does not fire itself: a schedule worker claims the due rows, runs
/// them and settles them here.
#[cfg(feature = "runtime-tokio")]
#[async_trait::async_trait]
pub(crate) trait ScheduleStore: Send + Sync + 'static {
    /// This store, declaring [`Capability::Scheduling`]: a worker fires its
    /// rows.
    fn with_schedule_dispatch(self) -> Self
    where
        Self: Sized;

    /// The store clock (the database clock on SQL).
    async fn schedule_now(&self) -> Result<SystemTime, RepositoryError>;

    /// Claim up to `limit` due, unclaimed armed schedules until
    /// `claim_until_ms`, earliest first.
    async fn claim_due_schedules(
        &self,
        now_ms: i64,
        claim_until_ms: i64,
        limit: usize,
    ) -> Result<Vec<(StreamIdentity, ArmedSchedule)>, RepositoryError>;

    /// Settle one fired arm (only that generation).
    async fn settle_schedule(
        &self,
        owner: &StreamIdentity,
        name: &str,
        generation: u64,
        settlement: &ScheduleSettlement,
    ) -> Result<(), RepositoryError>;
}

/// What one wake did with the due schedules.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScheduleFireReport {
    /// Schedules whose command committed (or replayed a committed receipt).
    pub fired: Vec<FiredSchedule>,
    /// Schedules rescheduled after a retryable failure.
    pub retried: Vec<FailedSchedule>,
    /// Schedules disarmed after a terminal failure.
    pub failed: Vec<FailedSchedule>,
}

impl ScheduleFireReport {
    pub fn is_empty(&self) -> bool {
        self.fired.is_empty() && self.retried.is_empty() && self.failed.is_empty()
    }
}

/// A fired schedule.
#[derive(Clone, Debug, PartialEq)]
pub struct FiredSchedule {
    pub name: String,
    pub generation: u64,
    pub command: String,
    /// The command had already committed; its receipt was replayed.
    pub replayed: bool,
    pub payload: Value,
}

/// A schedule whose command failed.
#[derive(Clone, Debug, PartialEq)]
pub struct FailedSchedule {
    pub name: String,
    pub generation: u64,
    pub command: String,
    pub code: &'static str,
    pub message: String,
}

/// Deterministic UUIDv7-shaped command id of one schedule arm, for an owner
/// whose command ledger is its own (the ledger scopes it to the owner).
pub(crate) fn schedule_command_id(name: &str, generation: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(SCHEDULE_SERVICE_ID.as_bytes());
    hasher.update([0u8]);
    hasher.update(name.as_bytes());
    hasher.update([0u8]);
    hasher.update(generation.to_be_bytes());
    crate::command_ledger::deterministic_command_id(hasher)
}

/// How long a schedule waits after a failed attempt: the same backoff type
/// the bus and the outbox use, from one second doubling to five minutes.
pub const SCHEDULE_RETRY: crate::bus::RetryBackoff =
    crate::bus::RetryBackoff::new(SCHEDULE_RETRY_BASE, SCHEDULE_RETRY_MAX);

/// Retry delay after `attempts` earlier failures (0-based).
pub(crate) fn retry_delay(attempts: u32) -> Duration {
    SCHEDULE_RETRY.delay(attempts.saturating_add(1))
}

pub(crate) fn system_time_to_ms(time: SystemTime) -> Result<i64, ScheduleError> {
    let millis = time
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| ScheduleError::DueOutOfRange)?
        .as_millis();
    if millis > MAX_DUE_MS as u128 {
        return Err(ScheduleError::DueOutOfRange);
    }
    Ok(millis as i64)
}

pub(crate) fn ms_to_system_time(ms: i64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(ms.max(0) as u64)
}

fn validate_name(name: &str) -> Result<(), ScheduleError> {
    if name.is_empty()
        || name.len() > MAX_SCHEDULE_NAME_BYTES
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(ScheduleError::InvalidName);
    }
    Ok(())
}

/// One owner's schedule table in memory (the in-memory repository, and any
/// host that keeps its working state in memory), with the same semantics as
/// the SQL tables.
#[derive(Clone, Debug, Default)]
pub(crate) struct MemorySchedules {
    rows: std::collections::BTreeMap<String, MemoryScheduleRow>,
}

#[derive(Clone, Debug)]
struct MemoryScheduleRow {
    schedule: ArmedSchedule,
    armed: bool,
    /// A schedule worker's claim (epoch ms); cleared by settling or
    /// re-arming. Transient: not part of restart exports.
    claimed_until_ms: Option<i64>,
}

impl MemorySchedules {
    /// Apply one command's writes all-or-nothing, as the SQL transaction does.
    pub(crate) fn plan(&self, writes: &[ScheduleWrite]) -> Result<Self, RepositoryError> {
        let mut next = self.clone();
        for write in writes {
            match write {
                ScheduleWrite::Arm(schedule) => {
                    let generation = next
                        .rows
                        .get(schedule.name())
                        .map_or(1, |row| row.schedule.generation + 1);
                    next.rows.insert(
                        schedule.name().to_string(),
                        MemoryScheduleRow {
                            schedule: ArmedSchedule {
                                name: schedule.name().to_string(),
                                command: schedule.command().to_string(),
                                input: schedule.input().clone(),
                                due_at_ms: schedule.due_at_ms()?,
                                generation,
                                attempts: 0,
                            },
                            armed: true,
                            claimed_until_ms: None,
                        },
                    );
                }
                ScheduleWrite::Cancel(name) => {
                    if let Some(row) = next.rows.get_mut(name) {
                        row.armed = false;
                        row.claimed_until_ms = None;
                    }
                }
            }
        }
        if next.rows.values().filter(|row| row.armed).count() > MAX_ARMED_SCHEDULES {
            return Err(ScheduleError::TooManyArmed.into());
        }
        Ok(next)
    }

    pub(crate) fn armed(&self) -> Vec<ArmedSchedule> {
        let mut armed = self
            .rows
            .values()
            .filter(|row| row.armed)
            .map(|row| row.schedule.clone())
            .collect::<Vec<_>>();
        armed.sort_by(|a, b| (a.due_at_ms, &a.name).cmp(&(b.due_at_ms, &b.name)));
        armed
    }

    pub(crate) fn due(&self, now_ms: i64, limit: usize) -> Vec<ArmedSchedule> {
        self.armed()
            .into_iter()
            .filter(|schedule| schedule.due_at_ms <= now_ms)
            .take(limit)
            .collect()
    }

    /// Claim up to `limit` due, unclaimed (or claim-expired) rows until
    /// `claim_until_ms`, earliest first (the schedule worker's claim).
    #[cfg_attr(not(feature = "runtime-tokio"), allow(dead_code))]
    pub(crate) fn claim_due(
        &mut self,
        now_ms: i64,
        claim_until_ms: i64,
        limit: usize,
    ) -> Vec<ArmedSchedule> {
        let mut due = self
            .rows
            .values()
            .filter(|row| {
                row.armed
                    && row.schedule.due_at_ms <= now_ms
                    && row.claimed_until_ms.is_none_or(|until| until <= now_ms)
            })
            .map(|row| row.schedule.clone())
            .collect::<Vec<_>>();
        due.sort_by(|a, b| (a.due_at_ms, &a.name).cmp(&(b.due_at_ms, &b.name)));
        due.truncate(limit);
        for schedule in &due {
            if let Some(row) = self.rows.get_mut(&schedule.name) {
                row.claimed_until_ms = Some(claim_until_ms);
            }
        }
        due
    }

    /// Drop a claim taken by [`claim_due`](Self::claim_due) without settling.
    #[cfg_attr(not(feature = "runtime-tokio"), allow(dead_code))]
    pub(crate) fn release_claim(&mut self, name: &str, generation: u64) {
        if let Some(row) = self.rows.get_mut(name) {
            if row.schedule.generation == generation {
                row.claimed_until_ms = None;
            }
        }
    }

    pub(crate) fn next_due_ms(&self) -> Option<i64> {
        self.rows
            .values()
            .filter(|row| row.armed)
            .map(|row| row.schedule.due_at_ms)
            .min()
    }

    pub(crate) fn settle(&mut self, name: &str, generation: u64, settlement: &ScheduleSettlement) {
        let Some(row) = self.rows.get_mut(name) else {
            return;
        };
        if !row.armed || row.schedule.generation != generation {
            return;
        }
        row.claimed_until_ms = None;
        match settlement {
            ScheduleSettlement::Done => {
                row.armed = false;
                row.schedule.attempts = 0;
            }
            ScheduleSettlement::Retry { due_at_ms } => {
                row.schedule.attempts = row.schedule.attempts.saturating_add(1);
                row.schedule.due_at_ms = *due_at_ms;
            }
        }
    }

    /// Every row (armed or not), for reference-host restart exports.
    pub(crate) fn export(&self) -> Vec<DurableSchedule> {
        self.rows
            .values()
            .map(|row| DurableSchedule {
                schedule: row.schedule.clone(),
                armed: row.armed,
            })
            .collect()
    }

    pub(crate) fn restore(rows: Vec<DurableSchedule>) -> Self {
        Self {
            rows: rows
                .into_iter()
                .map(|row| {
                    (
                        row.schedule.name.clone(),
                        MemoryScheduleRow {
                            schedule: row.schedule,
                            armed: row.armed,
                            claimed_until_ms: None,
                        },
                    )
                })
                .collect(),
        }
    }
}

/// One schedule row in a reference-host restart export. Disarmed rows are
/// kept so generations never repeat after a restore.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DurableSchedule {
    pub schedule: ArmedSchedule,
    pub armed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schedule(name: &str, due_ms: u64) -> Schedule {
        Schedule::at(
            name,
            SystemTime::UNIX_EPOCH + Duration::from_millis(due_ms),
            "test.tick",
            &serde_json::json!({ "n": 1 }),
        )
        .unwrap()
    }

    #[test]
    fn validates_names_inputs_and_write_counts() {
        let due = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        assert_eq!(
            Schedule::at("", due, "c", &serde_json::json!({})).unwrap_err(),
            ScheduleError::InvalidName
        );
        assert_eq!(
            Schedule::at("a b", due, "c", &serde_json::json!({})).unwrap_err(),
            ScheduleError::InvalidName
        );
        assert_eq!(
            Schedule::at("ok", due, " ", &serde_json::json!({})).unwrap_err(),
            ScheduleError::EmptyCommand
        );
        assert_eq!(
            Schedule::at("ok", due, "c", &1).unwrap_err(),
            ScheduleError::InvalidInput
        );
        let big = serde_json::json!({ "x": "y".repeat(MAX_SCHEDULE_INPUT_BYTES) });
        assert_eq!(
            Schedule::at("ok", due, "c", &big).unwrap_err(),
            ScheduleError::InvalidInput
        );
        let writes = (0..=MAX_SCHEDULE_WRITES_PER_COMMAND)
            .map(|index| ScheduleWrite::Cancel(format!("a{index}")))
            .collect::<Vec<_>>();
        assert!(validate_schedule_writes(&writes).is_err());
        assert!(validate_schedule_writes(&writes[1..]).is_ok());
    }

    #[test]
    fn capabilities_report_what_is_missing() {
        let sql = Capabilities {
            scheduling: false,
            clock: true,
        };
        assert_eq!(
            sql.missing(&[
                Capability::Clock,
                Capability::Scheduling,
                Capability::Scheduling
            ]),
            vec![Capability::Scheduling]
        );
        assert!(Capabilities::ALL
            .missing(&[Capability::Scheduling, Capability::Clock])
            .is_empty());
        assert_eq!(Capability::Scheduling.to_string(), "scheduling");
    }

    #[test]
    fn command_ids_are_uuidv7_and_unique_per_generation() {
        let first = schedule_command_id("tick", 1);
        assert_eq!(first, schedule_command_id("tick", 1));
        assert_ne!(first, schedule_command_id("tick", 2));
        assert_ne!(first, schedule_command_id("tock", 1));
        let parsed = uuid::Uuid::parse_str(&first).unwrap();
        assert_eq!(parsed.get_version_num(), 7);
    }

    #[test]
    fn retry_delay_doubles_and_caps() {
        assert_eq!(retry_delay(0), Duration::from_secs(1));
        assert_eq!(retry_delay(3), Duration::from_secs(8));
        assert_eq!(retry_delay(40), SCHEDULE_RETRY_MAX);
    }

    #[test]
    fn memory_table_reschedules_with_new_generation_and_settles_only_its_own() {
        let table = MemorySchedules::default();
        let table = table
            .plan(&[ScheduleWrite::Arm(schedule("tick", 10))])
            .unwrap();
        assert_eq!(table.armed()[0].generation, 1);
        let mut table = table
            .plan(&[ScheduleWrite::Arm(schedule("tick", 20))])
            .unwrap();
        assert_eq!(table.armed()[0].generation, 2);
        assert_eq!(table.next_due_ms(), Some(20));
        assert!(table.due(19, 10).is_empty());
        table.settle("tick", 1, &ScheduleSettlement::Done);
        assert_eq!(table.armed().len(), 1, "stale generation must not disarm");
        table.settle("tick", 2, &ScheduleSettlement::Retry { due_at_ms: 50 });
        assert_eq!(table.armed()[0].attempts, 1);
        assert_eq!(table.next_due_ms(), Some(50));
        table.settle("tick", 2, &ScheduleSettlement::Done);
        assert!(table.armed().is_empty());
        let table = table
            .plan(&[ScheduleWrite::Arm(schedule("tick", 60))])
            .unwrap();
        assert_eq!(
            table.armed()[0].generation,
            3,
            "generations never repeat after a disarm"
        );
        let restored = MemorySchedules::restore(table.export());
        assert_eq!(restored.armed(), table.armed());
        let cancelled = restored
            .plan(&[ScheduleWrite::Cancel("tick".into())])
            .unwrap();
        assert!(cancelled.armed().is_empty());
    }

    #[test]
    fn memory_table_caps_armed_schedules() {
        let mut table = MemorySchedules::default();
        for chunk in 0..(MAX_ARMED_SCHEDULES / 16) {
            let writes = (0..16)
                .map(|index| ScheduleWrite::Arm(schedule(&format!("a{chunk}-{index}"), 1)))
                .collect::<Vec<_>>();
            table = table.plan(&writes).unwrap();
        }
        let error = table
            .plan(&[ScheduleWrite::Arm(schedule("overflow", 1))])
            .unwrap_err();
        assert!(error.to_string().contains("armed schedules"));
    }
}
