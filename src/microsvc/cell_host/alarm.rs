//! Re-armable cell alarms.
//!
//! A cell command handler arms an alarm with its events:
//!
//! ```rust,ignore
//! ctx.arm_alarm(CellAlarm::at("tick", due, "matchmaker.tick", &TickInput { pool })?)?;
//! ```
//!
//! The alarm row commits in the **same transaction** as the command's events,
//! ledger receipt and outbox rows, so a failed command leaves no alarm. At
//! `due` the cell dispatches `command` into itself with a trusted system
//! session ([`cell_alarm_session`]); that handler may arm the same name again,
//! which gives a periodic tick without a global cron.
//!
//! Semantics, identical on celld and on the in-process reference host:
//!
//! - One row per alarm **name**. Arming an existing name replaces its due
//!   time, command and input and bumps its `generation`; cancelling disarms it.
//! - A fired alarm runs through the cell's command ledger with a command id
//!   derived from `(name, generation)`. A crash after the handler committed but
//!   before the alarm was settled replays the recorded receipt instead of
//!   running the handler twice, so each arm has its effects at most once.
//! - Settlement only disarms the generation that fired. A handler that re-arms
//!   its own name produced a newer generation, which stays armed.
//! - Rejections (domain errors, guard failures, bad input) are terminal for
//!   that arm. Retryable failures (in-progress, internal) back off
//!   exponentially from one second up to five minutes.
//!
//! On celld the Durable Object has a single alarm slot, owned jointly with the
//! outbox watchdog (`celld_outbox.rs`): every `setAlarm` targets the earliest
//! of the next domain alarm and the watchdog's next drain. Neither replaces the
//! other.

use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::causal::{CellCommandIdentity, CellDispatchError};
use crate::microsvc::session::{Session, ROLE_KEY, USER_ID_KEY};
use crate::repository::RepositoryError;

/// Role carried by the trusted session of a fired alarm. Commands that only
/// alarms may run declare `roles: ["cell-alarm"]` and
/// `guard: cell_alarm_guard`.
pub const CELL_ALARM_ROLE: &str = "cell-alarm";
/// User id of the trusted session of a fired alarm.
pub const CELL_ALARM_USER_ID: &str = "system:cell-alarm";
/// Session claim naming the alarm that fired. Set only by the cell host.
pub const CELL_ALARM_NAME_CLAIM: &str = "x-distributed-cell-alarm";
/// Session claim with the fired alarm's due time (milliseconds since the Unix
/// epoch). Periodic handlers re-arm relative to it so ticks do not drift.
pub const CELL_ALARM_DUE_CLAIM: &str = "x-distributed-cell-alarm-due-ms";
/// Session claim with the fired alarm's generation.
pub const CELL_ALARM_GENERATION_CLAIM: &str = "x-distributed-cell-alarm-generation";
/// Session claim with the cell clock's time at dispatch (epoch ms). The cell
/// host overwrites it on every command, so callers cannot choose it; read it
/// with `CausalCommandContext::cell_now`.
pub const CELL_NOW_CLAIM: &str = "x-distributed-cell-now-ms";
/// Command-ledger service id of fired alarms.
pub const CELL_ALARM_SERVICE_ID: &str = "distributed.cell-alarm";
/// Longest alarm name.
pub const MAX_CELL_ALARM_NAME_BYTES: usize = 128;
/// Largest serialized alarm input.
pub const MAX_CELL_ALARM_INPUT_BYTES: usize = 16 * 1024;
/// Most alarm writes one command may stage.
pub const MAX_CELL_ALARM_WRITES_PER_COMMAND: usize = 16;
/// Most alarms one cell may hold armed at once.
pub const MAX_ARMED_CELL_ALARMS: usize = 1024;
/// Most due alarms fired per wake; the rest fire on the next wake.
pub const CELL_ALARM_FIRE_BATCH: usize = 32;
/// First retry delay after a retryable alarm failure.
pub const CELL_ALARM_RETRY_BASE: Duration = Duration::from_secs(1);
/// Longest retry delay after repeated retryable alarm failures.
pub const CELL_ALARM_RETRY_MAX: Duration = Duration::from_secs(300);

/// Why an alarm could not be armed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CellAlarmError {
    InvalidName,
    EmptyCommand,
    InvalidInput,
    DueOutOfRange,
    TooManyWrites,
    TooManyArmed,
}

impl std::fmt::Display for CellAlarmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidName => write!(
                f,
                "cell alarm name must be 1..={MAX_CELL_ALARM_NAME_BYTES} bytes of [A-Za-z0-9._:-]"
            ),
            Self::EmptyCommand => f.write_str("cell alarm command must not be empty"),
            Self::InvalidInput => write!(
                f,
                "cell alarm input must be a JSON object of at most {MAX_CELL_ALARM_INPUT_BYTES} bytes"
            ),
            Self::DueOutOfRange => f.write_str("cell alarm due time is out of range"),
            Self::TooManyWrites => write!(
                f,
                "a command may stage at most {MAX_CELL_ALARM_WRITES_PER_COMMAND} cell alarm writes"
            ),
            Self::TooManyArmed => write!(
                f,
                "a cell may hold at most {MAX_ARMED_CELL_ALARMS} armed alarms"
            ),
        }
    }
}

impl std::error::Error for CellAlarmError {}

impl From<CellAlarmError> for RepositoryError {
    fn from(error: CellAlarmError) -> Self {
        RepositoryError::Model(error.to_string())
    }
}

/// When an alarm is due.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AlarmDue {
    /// Absolute epoch milliseconds.
    At(i64),
    /// Milliseconds after the commit, resolved by the cell's clock.
    After(u64),
}

/// An alarm a command arms: when due, dispatch `command` with `input`.
#[derive(Clone, Debug, PartialEq)]
pub struct CellAlarm {
    name: String,
    command: String,
    input: Value,
    due: AlarmDue,
}

impl CellAlarm {
    /// Arm `name` to dispatch `command` with `input` into this cell at `due`.
    ///
    /// Periodic handlers re-arm from the fired due time
    /// (`ctx.alarm_due()`), so ticks do not drift with handler latency.
    pub fn at(
        name: impl Into<String>,
        due: SystemTime,
        command: impl Into<String>,
        input: &impl Serialize,
    ) -> Result<Self, CellAlarmError> {
        Self::build(name, AlarmDue::At(system_time_to_ms(due)?), command, input)
    }

    /// Arm `name` to fire `delay` after the command commits, measured by the
    /// cell's clock (the Durable Object clock on celld, the injected
    /// [`Clock`](crate::Clock) on the reference host). Domain handlers have no
    /// wall clock of their own; this is how they schedule relative work.
    pub fn after(
        name: impl Into<String>,
        delay: Duration,
        command: impl Into<String>,
        input: &impl Serialize,
    ) -> Result<Self, CellAlarmError> {
        let delay = u64::try_from(delay.as_millis()).map_err(|_| CellAlarmError::DueOutOfRange)?;
        Self::build(name, AlarmDue::After(delay), command, input)
    }

    fn build(
        name: impl Into<String>,
        due: AlarmDue,
        command: impl Into<String>,
        input: &impl Serialize,
    ) -> Result<Self, CellAlarmError> {
        let input = serde_json::to_value(input).map_err(|_| CellAlarmError::InvalidInput)?;
        let alarm = Self {
            name: name.into(),
            command: command.into(),
            input,
            due,
        };
        alarm.validate()?;
        Ok(alarm)
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

    /// Absolute due time, or `None` for a relative alarm not yet committed.
    pub fn due(&self) -> Option<SystemTime> {
        match self.due {
            AlarmDue::At(ms) => Some(ms_to_system_time(ms)),
            AlarmDue::After(_) => None,
        }
    }

    /// Resolve a relative due time against the committing cell's clock.
    pub(crate) fn resolve(&mut self, now: SystemTime) -> Result<(), CellAlarmError> {
        if let AlarmDue::After(delay) = self.due {
            let now = system_time_to_ms(now)?;
            let due = now
                .checked_add(i64::try_from(delay).map_err(|_| CellAlarmError::DueOutOfRange)?)
                .ok_or(CellAlarmError::DueOutOfRange)?;
            if due > MAX_JS_SAFE_MS {
                return Err(CellAlarmError::DueOutOfRange);
            }
            self.due = AlarmDue::At(due);
        }
        Ok(())
    }

    /// Absolute due time; storage refuses an unresolved relative alarm.
    pub(crate) fn due_at_ms(&self) -> Result<i64, RepositoryError> {
        match self.due {
            AlarmDue::At(ms) => Ok(ms),
            AlarmDue::After(_) => Err(RepositoryError::Model(
                "relative cell alarm reached storage without being resolved".into(),
            )),
        }
    }

    pub(crate) fn input_json(&self) -> Result<String, RepositoryError> {
        serde_json::to_string(&self.input)
            .map_err(|error| RepositoryError::Model(format!("cell alarm input: {error}")))
    }

    pub(crate) fn validate(&self) -> Result<(), CellAlarmError> {
        validate_name(&self.name)?;
        if self.command.trim().is_empty() {
            return Err(CellAlarmError::EmptyCommand);
        }
        let size = serde_json::to_vec(&self.input)
            .map_err(|_| CellAlarmError::InvalidInput)?
            .len();
        if !self.input.is_object() || size > MAX_CELL_ALARM_INPUT_BYTES {
            return Err(CellAlarmError::InvalidInput);
        }
        match self.due {
            AlarmDue::At(ms) if !(0..=MAX_JS_SAFE_MS).contains(&ms) => {
                Err(CellAlarmError::DueOutOfRange)
            }
            AlarmDue::After(delay) if delay > MAX_JS_SAFE_MS as u64 => {
                Err(CellAlarmError::DueOutOfRange)
            }
            _ => Ok(()),
        }
    }
}

/// Resolve every relative alarm of a batch against the cell clock.
pub(crate) fn resolve_alarm_writes(
    writes: &mut [CellAlarmWrite],
    now: SystemTime,
) -> Result<(), RepositoryError> {
    for write in writes {
        if let CellAlarmWrite::Arm(alarm) = write {
            alarm.resolve(now)?;
        }
    }
    Ok(())
}

/// Durable Object SQL binds integers through JS numbers.
const MAX_JS_SAFE_MS: i64 = 9_007_199_254_740_991;

/// One alarm write staged by a command and committed with its events.
#[derive(Clone, Debug, PartialEq)]
pub enum CellAlarmWrite {
    /// Arm (or re-arm) the alarm's name.
    Arm(CellAlarm),
    /// Disarm a name. Cancelling a name that is not armed is a no-op.
    Cancel(String),
}

impl CellAlarmWrite {
    pub fn name(&self) -> &str {
        match self {
            Self::Arm(alarm) => alarm.name(),
            Self::Cancel(name) => name,
        }
    }
}

/// Validate a batch's alarm writes before any storage participant runs.
pub(crate) fn validate_alarm_writes(writes: &[CellAlarmWrite]) -> Result<(), RepositoryError> {
    if writes.len() > MAX_CELL_ALARM_WRITES_PER_COMMAND {
        return Err(CellAlarmError::TooManyWrites.into());
    }
    for write in writes {
        match write {
            CellAlarmWrite::Arm(alarm) => alarm.validate()?,
            CellAlarmWrite::Cancel(name) => validate_name(name)?,
        }
    }
    Ok(())
}

/// Ordinary repositories have no alarm slot; only aggregate cells do.
pub(crate) fn reject_alarm_writes(writes: &[CellAlarmWrite]) -> Result<(), RepositoryError> {
    if writes.is_empty() {
        Ok(())
    } else {
        Err(RepositoryError::Model(
            "cell alarms require an aggregate cell host".into(),
        ))
    }
}

/// An armed alarm as stored by the cell.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArmedCellAlarm {
    pub name: String,
    pub command: String,
    pub input: Value,
    pub due_at_ms: i64,
    pub generation: u64,
    pub attempts: u32,
}

impl ArmedCellAlarm {
    pub fn due(&self) -> SystemTime {
        ms_to_system_time(self.due_at_ms)
    }

    /// Stable command-ledger identity of this arm.
    pub fn command_identity(&self) -> Result<CellCommandIdentity, CellDispatchError> {
        CellCommandIdentity::new(
            CELL_ALARM_SERVICE_ID,
            CELL_ALARM_SERVICE_ID,
            alarm_command_id(&self.name, self.generation),
        )
    }
}

/// How a fired alarm settled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AlarmSettlement {
    /// Ran (or replayed) or failed terminally: disarm this generation.
    Done,
    /// Retry this generation at `due_at_ms`.
    Retry { due_at_ms: i64 },
}

/// What one wake did with the due alarms.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CellAlarmFireReport {
    /// Alarms whose command committed (or replayed a committed receipt).
    pub fired: Vec<FiredCellAlarm>,
    /// Alarms rescheduled after a retryable failure.
    pub retried: Vec<FailedCellAlarm>,
    /// Alarms disarmed after a terminal failure.
    pub failed: Vec<FailedCellAlarm>,
}

impl CellAlarmFireReport {
    pub fn is_empty(&self) -> bool {
        self.fired.is_empty() && self.retried.is_empty() && self.failed.is_empty()
    }
}

/// A fired alarm.
#[derive(Clone, Debug, PartialEq)]
pub struct FiredCellAlarm {
    pub name: String,
    pub generation: u64,
    pub command: String,
    /// The command had already committed; its receipt was replayed.
    pub replayed: bool,
    pub payload: Value,
}

/// An alarm whose command failed.
#[derive(Clone, Debug, PartialEq)]
pub struct FailedCellAlarm {
    pub name: String,
    pub generation: u64,
    pub command: String,
    pub code: &'static str,
    pub message: String,
}

/// The trusted system session a fired alarm dispatches with.
pub fn cell_alarm_session(alarm: &ArmedCellAlarm) -> Session {
    let mut session = Session::new();
    session.set(USER_ID_KEY, CELL_ALARM_USER_ID);
    session.set(ROLE_KEY, CELL_ALARM_ROLE);
    session.set(CELL_ALARM_NAME_CLAIM, alarm.name.clone());
    session.set(CELL_ALARM_DUE_CLAIM, alarm.due_at_ms.to_string());
    session.set(CELL_ALARM_GENERATION_CLAIM, alarm.generation.to_string());
    session
}

/// The cell clock's time at dispatch, when the command runs in a cell.
pub fn cell_now(session: &Session) -> Option<SystemTime> {
    session
        .get(CELL_NOW_CLAIM)?
        .parse::<i64>()
        .ok()
        .filter(|ms| *ms >= 0)
        .map(ms_to_system_time)
}

/// Whether `session` is the trusted session of a fired alarm.
pub fn is_cell_alarm_session(session: &Session) -> bool {
    session.user_id() == Some(CELL_ALARM_USER_ID)
        && session.has_role(CELL_ALARM_ROLE)
        && session.get(CELL_ALARM_NAME_CLAIM).is_some()
}

/// Due time of the alarm that fired this command, when it was an alarm.
pub fn cell_alarm_due(session: &Session) -> Option<SystemTime> {
    if !is_cell_alarm_session(session) {
        return None;
    }
    session
        .get(CELL_ALARM_DUE_CLAIM)?
        .parse::<i64>()
        .ok()
        .filter(|ms| *ms >= 0)
        .map(ms_to_system_time)
}

/// `portable_command!` guard admitting only fired alarms.
pub fn cell_alarm_guard<A>(ctx: &crate::microsvc::CausalCommandContext<'_, A>) -> bool
where
    A: crate::Aggregate + Send + Sync + 'static,
{
    is_cell_alarm_session(ctx.session())
}

/// Deterministic UUIDv7-shaped command id of one alarm arm.
pub(crate) fn alarm_command_id(name: &str, generation: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(CELL_ALARM_SERVICE_ID.as_bytes());
    hasher.update([0u8]);
    hasher.update(name.as_bytes());
    hasher.update([0u8]);
    hasher.update(generation.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes).hyphenated().to_string()
}

/// Exponential retry delay for the `attempts`-th failure (0-based).
pub(crate) fn retry_delay(attempts: u32) -> Duration {
    let factor = 1u32.checked_shl(attempts.min(16)).unwrap_or(u32::MAX);
    CELL_ALARM_RETRY_BASE
        .checked_mul(factor)
        .unwrap_or(CELL_ALARM_RETRY_MAX)
        .min(CELL_ALARM_RETRY_MAX)
}

/// Whether a dispatch failure should be retried rather than disarm the alarm.
pub(crate) fn is_retryable(error: &CellDispatchError) -> bool {
    matches!(
        error,
        CellDispatchError::InProgress | CellDispatchError::Internal(_)
    )
}

pub(crate) fn system_time_to_ms(time: SystemTime) -> Result<i64, CellAlarmError> {
    let millis = time
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| CellAlarmError::DueOutOfRange)?
        .as_millis();
    if millis > MAX_JS_SAFE_MS as u128 {
        return Err(CellAlarmError::DueOutOfRange);
    }
    Ok(millis as i64)
}

pub(crate) fn ms_to_system_time(ms: i64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(ms.max(0) as u64)
}

fn validate_name(name: &str) -> Result<(), CellAlarmError> {
    if name.is_empty()
        || name.len() > MAX_CELL_ALARM_NAME_BYTES
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(CellAlarmError::InvalidName);
    }
    Ok(())
}

/// The in-process reference host's alarm table, with the same semantics as
/// the SQL table used by Durable Object and native SQLite cells.
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
#[derive(Clone, Debug, Default)]
pub(crate) struct MemoryCellAlarms {
    rows: std::collections::BTreeMap<String, MemoryAlarmRow>,
}

#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
#[derive(Clone, Debug)]
struct MemoryAlarmRow {
    alarm: ArmedCellAlarm,
    armed: bool,
}

#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
impl MemoryCellAlarms {
    /// Apply one command's writes all-or-nothing, as the SQL transaction does.
    pub(crate) fn plan(&self, writes: &[CellAlarmWrite]) -> Result<Self, RepositoryError> {
        let mut next = self.clone();
        for write in writes {
            match write {
                CellAlarmWrite::Arm(alarm) => {
                    let generation = next
                        .rows
                        .get(alarm.name())
                        .map_or(1, |row| row.alarm.generation + 1);
                    next.rows.insert(
                        alarm.name().to_string(),
                        MemoryAlarmRow {
                            alarm: ArmedCellAlarm {
                                name: alarm.name().to_string(),
                                command: alarm.command().to_string(),
                                input: alarm.input().clone(),
                                due_at_ms: alarm.due_at_ms()?,
                                generation,
                                attempts: 0,
                            },
                            armed: true,
                        },
                    );
                }
                CellAlarmWrite::Cancel(name) => {
                    if let Some(row) = next.rows.get_mut(name) {
                        row.armed = false;
                    }
                }
            }
        }
        if next.rows.values().filter(|row| row.armed).count() > MAX_ARMED_CELL_ALARMS {
            return Err(CellAlarmError::TooManyArmed.into());
        }
        Ok(next)
    }

    pub(crate) fn armed(&self) -> Vec<ArmedCellAlarm> {
        let mut armed = self
            .rows
            .values()
            .filter(|row| row.armed)
            .map(|row| row.alarm.clone())
            .collect::<Vec<_>>();
        armed.sort_by(|a, b| (a.due_at_ms, &a.name).cmp(&(b.due_at_ms, &b.name)));
        armed
    }

    pub(crate) fn due(&self, now_ms: i64, limit: usize) -> Vec<ArmedCellAlarm> {
        self.armed()
            .into_iter()
            .filter(|alarm| alarm.due_at_ms <= now_ms)
            .take(limit)
            .collect()
    }

    pub(crate) fn next_due_ms(&self) -> Option<i64> {
        self.rows
            .values()
            .filter(|row| row.armed)
            .map(|row| row.alarm.due_at_ms)
            .min()
    }

    pub(crate) fn settle(&mut self, name: &str, generation: u64, settlement: &AlarmSettlement) {
        let Some(row) = self.rows.get_mut(name) else {
            return;
        };
        if !row.armed || row.alarm.generation != generation {
            return;
        }
        match settlement {
            AlarmSettlement::Done => {
                row.armed = false;
                row.alarm.attempts = 0;
            }
            AlarmSettlement::Retry { due_at_ms } => {
                row.alarm.attempts = row.alarm.attempts.saturating_add(1);
                row.alarm.due_at_ms = *due_at_ms;
            }
        }
    }

    /// Every row (armed or not), for reference-host restart exports.
    pub(crate) fn export(&self) -> Vec<DurableCellAlarm> {
        self.rows
            .values()
            .map(|row| DurableCellAlarm {
                alarm: row.alarm.clone(),
                armed: row.armed,
            })
            .collect()
    }

    pub(crate) fn restore(rows: Vec<DurableCellAlarm>) -> Self {
        Self {
            rows: rows
                .into_iter()
                .map(|row| {
                    (
                        row.alarm.name.clone(),
                        MemoryAlarmRow {
                            alarm: row.alarm,
                            armed: row.armed,
                        },
                    )
                })
                .collect(),
        }
    }
}

/// One alarm row in a reference-host restart export. Disarmed rows are kept so
/// generations never repeat after a restore.
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DurableCellAlarm {
    pub alarm: ArmedCellAlarm,
    pub armed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alarm(name: &str, due_ms: u64) -> CellAlarm {
        CellAlarm::at(
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
            CellAlarm::at("", due, "c", &serde_json::json!({})).unwrap_err(),
            CellAlarmError::InvalidName
        );
        assert_eq!(
            CellAlarm::at("a b", due, "c", &serde_json::json!({})).unwrap_err(),
            CellAlarmError::InvalidName
        );
        assert_eq!(
            CellAlarm::at("ok", due, " ", &serde_json::json!({})).unwrap_err(),
            CellAlarmError::EmptyCommand
        );
        assert_eq!(
            CellAlarm::at("ok", due, "c", &1).unwrap_err(),
            CellAlarmError::InvalidInput
        );
        let big = serde_json::json!({ "x": "y".repeat(MAX_CELL_ALARM_INPUT_BYTES) });
        assert_eq!(
            CellAlarm::at("ok", due, "c", &big).unwrap_err(),
            CellAlarmError::InvalidInput
        );
        let writes = (0..=MAX_CELL_ALARM_WRITES_PER_COMMAND)
            .map(|index| CellAlarmWrite::Cancel(format!("a{index}")))
            .collect::<Vec<_>>();
        assert!(validate_alarm_writes(&writes).is_err());
        assert!(validate_alarm_writes(&writes[1..]).is_ok());
        assert!(reject_alarm_writes(&writes).is_err());
        assert!(reject_alarm_writes(&[]).is_ok());
    }

    #[test]
    fn command_ids_are_uuidv7_and_unique_per_generation() {
        let first = alarm_command_id("tick", 1);
        assert_eq!(first, alarm_command_id("tick", 1));
        assert_ne!(first, alarm_command_id("tick", 2));
        assert_ne!(first, alarm_command_id("tock", 1));
        let parsed = uuid::Uuid::parse_str(&first).unwrap();
        assert_eq!(parsed.get_version_num(), 7);
        let armed = ArmedCellAlarm {
            name: "tick".into(),
            command: "c".into(),
            input: serde_json::json!({}),
            due_at_ms: 0,
            generation: 1,
            attempts: 0,
        };
        assert_eq!(armed.command_identity().unwrap().command_id(), first);
    }

    #[test]
    fn retry_delay_doubles_and_caps() {
        assert_eq!(retry_delay(0), Duration::from_secs(1));
        assert_eq!(retry_delay(3), Duration::from_secs(8));
        assert_eq!(retry_delay(40), CELL_ALARM_RETRY_MAX);
    }

    #[test]
    fn alarm_session_is_recognized_and_carries_due() {
        let armed = ArmedCellAlarm {
            name: "tick".into(),
            command: "c".into(),
            input: serde_json::json!({}),
            due_at_ms: 1_234,
            generation: 7,
            attempts: 0,
        };
        let session = cell_alarm_session(&armed);
        assert!(is_cell_alarm_session(&session));
        assert_eq!(
            cell_alarm_due(&session),
            Some(SystemTime::UNIX_EPOCH + Duration::from_millis(1_234))
        );
        let mut forged = Session::new();
        forged.set(USER_ID_KEY, "player-1");
        forged.set(ROLE_KEY, CELL_ALARM_ROLE);
        forged.set(CELL_ALARM_NAME_CLAIM, "tick");
        assert!(!is_cell_alarm_session(&forged));
        assert_eq!(cell_alarm_due(&forged), None);
    }

    #[test]
    fn memory_table_rearms_with_new_generation_and_settles_only_its_own() {
        let table = MemoryCellAlarms::default();
        let table = table
            .plan(&[CellAlarmWrite::Arm(alarm("tick", 10))])
            .unwrap();
        assert_eq!(table.armed()[0].generation, 1);
        let mut table = table
            .plan(&[CellAlarmWrite::Arm(alarm("tick", 20))])
            .unwrap();
        assert_eq!(table.armed()[0].generation, 2);
        assert_eq!(table.next_due_ms(), Some(20));
        assert!(table.due(19, 10).is_empty());
        table.settle("tick", 1, &AlarmSettlement::Done);
        assert_eq!(table.armed().len(), 1, "stale generation must not disarm");
        table.settle("tick", 2, &AlarmSettlement::Retry { due_at_ms: 50 });
        assert_eq!(table.armed()[0].attempts, 1);
        assert_eq!(table.next_due_ms(), Some(50));
        table.settle("tick", 2, &AlarmSettlement::Done);
        assert!(table.armed().is_empty());
        let table = table
            .plan(&[CellAlarmWrite::Arm(alarm("tick", 60))])
            .unwrap();
        assert_eq!(
            table.armed()[0].generation,
            3,
            "generations never repeat after a disarm"
        );
        let restored = MemoryCellAlarms::restore(table.export());
        assert_eq!(restored.armed(), table.armed());
        let cancelled = restored
            .plan(&[CellAlarmWrite::Cancel("tick".into())])
            .unwrap();
        assert!(cancelled.armed().is_empty());
    }

    #[test]
    fn memory_table_caps_armed_alarms() {
        let mut table = MemoryCellAlarms::default();
        for chunk in 0..(MAX_ARMED_CELL_ALARMS / 16) {
            let writes = (0..16)
                .map(|index| CellAlarmWrite::Arm(alarm(&format!("a{chunk}-{index}"), 1)))
                .collect::<Vec<_>>();
            table = table.plan(&writes).unwrap();
        }
        let error = table
            .plan(&[CellAlarmWrite::Arm(alarm("overflow", 1))])
            .unwrap_err();
        assert!(error.to_string().contains("armed alarms"));
    }
}
