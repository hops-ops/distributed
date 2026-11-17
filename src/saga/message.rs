//! Saga wire envelope: metadata keys, inbound [`SagaMessage`] parsing and the
//! participant-side [`SagaReply`] builder.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::SagaError;
use crate::outbox::OutboxMessage;

/// Message name of a participant's reply to a saga command.
pub const SAGA_REPLY: &str = "saga-reply";
/// Message name of a saga's own scheduled timeout.
pub const SAGA_TIMEOUT: &str = "saga-timeout";

/// Metadata key: saga aggregate type.
pub const META_SAGA_TYPE: &str = "saga-type";
/// Metadata key: saga id.
pub const META_SAGA_ID: &str = "saga-id";
/// Metadata key: step name.
pub const META_STEP: &str = "saga-step";
/// Metadata key: [`SagaPhase`] (`action` / `compensation`).
pub const META_PHASE: &str = "saga-phase";
/// Metadata key: dispatch attempt (1-based).
pub const META_ATTEMPT: &str = "saga-attempt";
/// Metadata key: fan-out member index (absent on single-command steps).
pub const META_MEMBER: &str = "saga-member";
/// Metadata key: where replies must be sent.
pub const META_REPLY_TO: &str = "saga-reply-to";
/// Metadata key: id of the command a reply/timeout belongs to.
pub const META_COMMAND_ID: &str = "saga-command-id";
/// Metadata key on replies: `succeeded` / `failed`.
pub const META_OUTCOME: &str = "saga-outcome";

const OUTCOME_SUCCEEDED: &str = "succeeded";
const OUTCOME_FAILED: &str = "failed";

/// Whether a dispatched command performs a step or undoes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SagaPhase {
    Action,
    Compensation,
}

impl SagaPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            SagaPhase::Action => "action",
            SagaPhase::Compensation => "compensation",
        }
    }

    fn parse(value: &str) -> Result<Self, SagaError> {
        match value {
            "action" => Ok(SagaPhase::Action),
            "compensation" => Ok(SagaPhase::Compensation),
            other => Err(SagaError::Malformed(format!(
                "unknown saga phase `{other}`"
            ))),
        }
    }
}

/// What an inbound saga message reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SagaSignal {
    /// The participant applied the command; `output` is its reply payload.
    Succeeded { output: Vec<u8> },
    /// The participant rejected the command (a definitive "did not apply").
    Failed { reason: String },
    /// The saga's own timer fired before a reply arrived.
    TimedOut,
}

/// An inbound message addressed to a saga (a reply or a timeout).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SagaMessage {
    /// Transport message id — the dedupe key.
    pub message_id: String,
    pub saga_type: String,
    pub saga_id: String,
    pub step: String,
    pub phase: SagaPhase,
    pub attempt: u32,
    /// Fan-out member index; `None` for single-command steps.
    pub member: Option<u32>,
    pub signal: SagaSignal,
}

impl SagaMessage {
    /// Whether a message name is one a saga consumes.
    pub fn is_saga_message(name: &str) -> bool {
        name == SAGA_REPLY || name == SAGA_TIMEOUT
    }

    /// Parse from transport parts (works for outbox rows and bus messages).
    pub fn from_parts<'a>(
        message_id: &str,
        name: &str,
        metadata: impl IntoIterator<Item = (&'a str, &'a str)>,
        payload: &[u8],
    ) -> Result<Self, SagaError> {
        let metadata: HashMap<&str, &str> = metadata.into_iter().collect();
        let get = |key: &str| {
            metadata
                .get(key)
                .copied()
                .ok_or_else(|| SagaError::Malformed(format!("missing `{key}` metadata")))
        };
        if message_id.is_empty() {
            return Err(SagaError::Malformed("saga message has no id".into()));
        }
        let signal = match name {
            SAGA_TIMEOUT => SagaSignal::TimedOut,
            SAGA_REPLY => match get(META_OUTCOME)? {
                OUTCOME_SUCCEEDED => SagaSignal::Succeeded {
                    output: payload.to_vec(),
                },
                OUTCOME_FAILED => SagaSignal::Failed {
                    reason: String::from_utf8_lossy(payload).into_owned(),
                },
                other => {
                    return Err(SagaError::Malformed(format!(
                        "unknown saga outcome `{other}`"
                    )))
                }
            },
            other => {
                return Err(SagaError::Malformed(format!(
                    "`{other}` is not a saga message"
                )))
            }
        };
        let attempt = get(META_ATTEMPT)?
            .parse::<u32>()
            .map_err(|_| SagaError::Malformed("invalid saga attempt".into()))?;
        let member = metadata
            .get(META_MEMBER)
            .map(|value| {
                value
                    .parse::<u32>()
                    .map_err(|_| SagaError::Malformed("invalid saga member".into()))
            })
            .transpose()?;
        Ok(Self {
            message_id: message_id.to_string(),
            saga_type: get(META_SAGA_TYPE)?.to_string(),
            saga_id: get(META_SAGA_ID)?.to_string(),
            step: get(META_STEP)?.to_string(),
            phase: SagaPhase::parse(get(META_PHASE)?)?,
            attempt,
            member,
            signal,
        })
    }

    /// Parse a delivered outbox row.
    pub fn from_outbox(message: &OutboxMessage) -> Result<Self, SagaError> {
        Self::from_parts(
            message.id(),
            &message.event_type,
            message
                .metadata
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
            &message.payload,
        )
    }

    /// Parse a bus/transport message.
    pub fn from_bus(message: &crate::bus::Message) -> Result<Self, SagaError> {
        let id = message
            .id()
            .ok_or_else(|| SagaError::Malformed("saga message has no id".into()))?;
        Self::from_parts(
            id,
            message.name(),
            message
                .metadata
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
            message.payload(),
        )
    }
}

/// Participant-side helper: build the reply to a saga command.
///
/// The reply id is derived from the command id, so a participant that
/// processes the same command twice (at-least-once delivery) emits the same
/// reply id and the saga dedupes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SagaReply {
    command_id: String,
    reply_to: String,
    metadata: HashMap<String, String>,
}

impl SagaReply {
    /// From a command's id and metadata (e.g. a received outbox row or bus
    /// message). Fails when the command was not sent by a saga.
    pub fn from_parts<'a>(
        command_id: &str,
        metadata: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self, SagaError> {
        let mut carried = HashMap::new();
        for (key, value) in metadata {
            if [
                META_SAGA_TYPE,
                META_SAGA_ID,
                META_STEP,
                META_PHASE,
                META_ATTEMPT,
                META_MEMBER,
                META_REPLY_TO,
            ]
            .contains(&key)
            {
                carried.insert(key.to_string(), value.to_string());
            }
        }
        let reply_to = carried
            .get(META_REPLY_TO)
            .cloned()
            .ok_or_else(|| SagaError::Malformed("command has no saga reply-to".into()))?;
        if command_id.is_empty() {
            return Err(SagaError::Malformed("saga command has no id".into()));
        }
        Ok(Self {
            command_id: command_id.to_string(),
            reply_to,
            metadata: carried,
        })
    }

    /// From a received outbox row.
    pub fn from_outbox(command: &OutboxMessage) -> Result<Self, SagaError> {
        Self::from_parts(
            command.id(),
            command
                .metadata
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
    }

    /// From a received bus message.
    pub fn from_bus(command: &crate::bus::Message) -> Result<Self, SagaError> {
        let id = command
            .id()
            .ok_or_else(|| SagaError::Malformed("saga command has no id".into()))?;
        Self::from_parts(
            id,
            command
                .metadata
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
    }

    /// The command id this reply answers (useful as the participant's own
    /// dedupe / tombstone key).
    pub fn command_id(&self) -> &str {
        &self.command_id
    }

    /// Saga id the command belongs to.
    pub fn saga_id(&self) -> Option<&str> {
        self.metadata.get(META_SAGA_ID).map(String::as_str)
    }

    /// "The command was applied" with a reply payload.
    pub fn succeeded(&self, output: Vec<u8>) -> Result<OutboxMessage, SagaError> {
        self.build(OUTCOME_SUCCEEDED, output)
    }

    /// "The command was applied" with a JSON reply payload.
    pub fn succeeded_json<T: Serialize>(&self, output: &T) -> Result<OutboxMessage, SagaError> {
        let bytes = serde_json::to_vec(output)
            .map_err(|err| SagaError::Malformed(format!("encode saga reply: {err}")))?;
        self.succeeded(bytes)
    }

    /// "The command was rejected" (definitively not applied).
    pub fn failed(&self, reason: impl Into<String>) -> Result<OutboxMessage, SagaError> {
        self.build(OUTCOME_FAILED, reason.into().into_bytes())
    }

    fn build(&self, outcome: &str, payload: Vec<u8>) -> Result<OutboxMessage, SagaError> {
        let mut message = OutboxMessage::create_to(
            format!("{}:reply", self.command_id),
            SAGA_REPLY,
            self.reply_to.clone(),
            payload,
        )?;
        for (key, value) in &self.metadata {
            message.set_meta(key.clone(), value.clone());
        }
        message.set_meta(META_COMMAND_ID, self.command_id.clone());
        message.set_meta(META_OUTCOME, outcome);
        if let Some(saga_id) = self.saga_id() {
            message.set_correlation_id(saga_id.to_string());
        }
        message.set_causation_id(self.command_id.clone());
        Ok(message)
    }
}
