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

    /// The command that performs `step`.
    fn action(step: &str, saga_id: &str, data: &Self::Data) -> Result<SagaCommand, SagaError>;

    /// The command that undoes `step`, or `None` when the step needs no
    /// compensation. Compensations must be idempotent **and** must tolerate
    /// arriving before (or instead of) the action they undo — the participant
    /// should record a tombstone so a late action is rejected.
    fn compensation(
        step: &str,
        saga_id: &str,
        data: &Self::Data,
    ) -> Result<Option<SagaCommand>, SagaError> {
        let _ = (step, saga_id, data);
        Ok(None)
    }

    /// Fold a successful step's reply payload into the saga data. Replayed —
    /// keep it deterministic. Default: ignore the output.
    fn on_step_succeeded(step: &str, data: &mut Self::Data, output: &[u8]) {
        let _ = (step, data, output);
    }
}
