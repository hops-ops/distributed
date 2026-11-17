use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Serialize;

use super::SagaError;

/// One forward step of a saga: a command sent to another cell, awaited for a
/// reply until `timeout` elapses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SagaStep {
    /// Stable step name (persisted in events and message metadata).
    pub name: &'static str,
    /// How long to wait for the step's reply before treating it as failed
    /// (outcome unknown → the step is compensated too).
    pub timeout: Duration,
    /// How long to wait for each compensation attempt's reply before retrying.
    pub compensation_timeout: Duration,
}

impl SagaStep {
    /// A step whose compensation uses the same timeout.
    pub const fn new(name: &'static str, timeout: Duration) -> Self {
        Self {
            name,
            timeout,
            compensation_timeout: timeout,
        }
    }

    /// Override the per-attempt compensation timeout.
    pub const fn with_compensation_timeout(mut self, timeout: Duration) -> Self {
        self.compensation_timeout = timeout;
        self
    }
}

/// A command the saga sends to a participant cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SagaCommand {
    /// Point-to-point destination (the participant cell's queue).
    pub destination: String,
    /// Command name (outbox `event_type` / bus message name).
    pub command_type: String,
    /// Encoded command payload.
    pub payload: Vec<u8>,
}

impl SagaCommand {
    /// A command with a raw payload.
    pub fn new(
        destination: impl Into<String>,
        command_type: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            destination: destination.into(),
            command_type: command_type.into(),
            payload,
        }
    }

    /// A command with a JSON payload (what `microsvc` handlers decode with
    /// `ctx.input`).
    pub fn json<T: Serialize>(
        destination: impl Into<String>,
        command_type: impl Into<String>,
        payload: &T,
    ) -> Result<Self, SagaError> {
        let payload = serde_json::to_vec(payload)
            .map_err(|err| SagaError::Definition(format!("encode saga command: {err}")))?;
        Ok(Self::new(destination, command_type, payload))
    }
}

/// Most commands one fan-out step may dispatch.
pub const MAX_SAGA_FAN_OUT: usize = 64;

/// What a forward step dispatches: one command, or a fan-out to N cells
/// decided at run time (for example one ticket reservation per party).
///
/// A fan-out step succeeds when **every** member replies `succeeded`. It
/// settles only once every member has replied or timed out; if any member
/// failed or timed out, the step fails and compensation runs for the members
/// that applied (or may have, after a timeout) — never for members that
/// definitively rejected. Each member has its own timeout timer and a
/// deterministic command id per `(step, member index)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SagaAction {
    /// A single command (`member` is `None` on its replies).
    Single(SagaCommand),
    /// `1..=MAX_SAGA_FAN_OUT` commands, member `i` is the `i`-th command.
    FanOut(Vec<SagaCommand>),
}

impl SagaAction {
    /// A fan-out over `commands` (validated when dispatched).
    pub fn fan_out(commands: impl IntoIterator<Item = SagaCommand>) -> Self {
        Self::FanOut(commands.into_iter().collect())
    }

    pub(super) fn into_commands(self) -> Result<(Vec<SagaCommand>, bool), SagaError> {
        match self {
            Self::Single(command) => Ok((vec![command], false)),
            Self::FanOut(commands) if commands.is_empty() => Err(SagaError::Definition(
                "a fan-out step needs at least one command".into(),
            )),
            Self::FanOut(commands) if commands.len() > MAX_SAGA_FAN_OUT => {
                Err(SagaError::Definition(format!(
                    "a fan-out step may dispatch at most {MAX_SAGA_FAN_OUT} commands, not {}",
                    commands.len()
                )))
            }
            Self::FanOut(commands) => Ok((commands, true)),
        }
    }
}

impl From<SagaCommand> for SagaAction {
    fn from(command: SagaCommand) -> Self {
        Self::Single(command)
    }
}

/// Static description of a saga (process manager).
///
/// The definition is pure: it maps the saga's data to the commands for each
/// step and compensation. The framework owns the state machine, persistence
/// (the [`Saga`](super::Saga) aggregate), timers and dedupe.
///
/// Everything a definition returns must be a deterministic function of its
/// inputs — [`on_step_succeeded`](Self::on_step_succeeded) in particular runs
/// again on every replay.
pub trait SagaDefinition: Send + Sync + 'static {
    /// Durable aggregate type of the saga streams (e.g. `"zone.transfer"`).
    const SAGA_TYPE: &'static str;
    /// Destination queue on which this saga receives replies and timers.
    const REPLY_TO: &'static str;
    /// Compensation attempts (dispatch + retries) before the saga is marked
    /// [`Stuck`](super::SagaStatus::Stuck) for operator attention.
    const MAX_COMPENSATION_ATTEMPTS: u32 = 5;

    /// The saga's own data (inputs plus anything folded in from replies).
    type Data: Serialize + DeserializeOwned + Clone + Default + Send + Sync + 'static;

    /// Forward steps, executed in order.
    fn steps() -> &'static [SagaStep];

    /// What `step` dispatches: a single command (`SagaCommand` converts
    /// with `.into()`) or a [`SagaAction::FanOut`]. Must be a deterministic
    /// function of the data.
    fn action(step: &str, saga_id: &str, data: &Self::Data) -> Result<SagaAction, SagaError>;

    /// The command that undoes `step` (for fan-out steps, member `member` of
    /// it; single steps pass `0`), or `None` when it needs no compensation.
    /// Compensations must be idempotent **and** must tolerate arriving before
    /// (or instead of) the action they undo — the participant should record a
    /// tombstone so a late action is rejected.
    fn compensation(
        step: &str,
        member: usize,
        saga_id: &str,
        data: &Self::Data,
    ) -> Result<Option<SagaCommand>, SagaError> {
        let _ = (step, member, saga_id, data);
        Ok(None)
    }

    /// Fold a successful reply payload into the saga data (for fan-out steps,
    /// once per member, `member` its index; single steps pass `0`). Replayed —
    /// keep it deterministic. Default: ignore the output.
    fn on_step_succeeded(step: &str, member: usize, data: &mut Self::Data, output: &[u8]) {
        let _ = (step, member, data, output);
    }
}
