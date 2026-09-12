//! Feature-free causal receipt and recovery for aggregate-cell wait paths.
//!
//! GraphQL owns its richer projection receipt, but the cell itself still owns
//! the durable command reservation and event/outbox commit. Keeping this layer
//! free of the `graphql` Cargo feature lets workers-rs cells use the exact same
//! fenced command ledger without pulling an HTTP server runtime into wasm.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::CellProjectionEventWireItem;

/// Bounded retry material, owned by the command ledger and expired with its
/// replay retention. It is not an outbox record or an event history archive.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CellCommandReplay {
    pub payload: Value,
    pub events: Vec<CellProjectionEventWireItem>,
}

use crate::command_ledger::{
    AttemptFence, CausalCommitBatch, CausalTransactionalCommit, CommandAttempt, CommandId,
    CommandLedgerError, CommandLedgerKey, CommandLedgerState, CommandLedgerStore, CommandLookup,
    CommandLookupScope, CommandReplay, PrincipalPartitionId, TerminalCommandState,
};
use crate::microsvc::HandlerError;
use crate::repository::CommitBatch;

/// Internal wait-path header carrying the executable service identity.
///
/// Public ingress must strip this header. The GraphQL celld command host sets
/// it from the locally bound [`Service`](crate::microsvc::Service).
pub const CELL_SERVICE_ID_HEADER: &str = "x-distributed-service-id";

/// Internal wait-path header carrying the verified-principal partition.
///
/// This is an opaque server-derived value, never a public command argument.
pub const CELL_PRINCIPAL_PARTITION_HEADER: &str = "x-distributed-principal-partition";

/// Internal wait-path header carrying the gateway's durable causation ID.
/// The shared internal secret authenticates this value; public clients cannot
/// select it.
pub const CELL_CAUSATION_ID_HEADER: &str = "x-distributed-causation-id";

/// Trusted command-ledger identity supplied by the cell's authenticated host.
///
/// `principal_partition` is the opaque, server-derived partition produced by
/// the verified ingress. A public client must never be allowed to choose it.
#[derive(Clone, Debug)]
pub struct CellCommandIdentity {
    key: CommandLedgerKey,
    causation_id: Option<crate::command_ledger::CausationId>,
}

impl CellCommandIdentity {
    pub fn new(
        service_id: impl Into<String>,
        principal_partition: impl Into<String>,
        command_id: impl AsRef<str>,
    ) -> Result<Self, CellDispatchError> {
        let command_id = CommandId::parse(command_id).map_err(internal_ledger_error)?;
        let principal_partition =
            PrincipalPartitionId::new(principal_partition).map_err(internal_ledger_error)?;
        let key = CommandLedgerKey::new(service_id, principal_partition, command_id)
            .map_err(internal_ledger_error)?;
        Ok(Self {
            key,
            causation_id: None,
        })
    }

    pub fn service_id(&self) -> &str {
        self.key.service_id()
    }

    pub fn command_id(&self) -> &str {
        self.key.command_id()
    }

    /// Bind a trusted gateway causation ID to the cell reservation. This is
    /// used only across the authenticated internal wait-path boundary; a cell
    /// identity without it retains the standalone cell-command behavior.
    pub fn with_causation_id(
        mut self,
        causation_id: impl AsRef<str>,
    ) -> Result<Self, CellDispatchError> {
        let causation_id = crate::command_ledger::CausationId::parse_stored(
            causation_id.as_ref().to_string(),
        )
        .map_err(internal_ledger_error)?;
        self.causation_id = Some(causation_id);
        Ok(self)
    }

    pub(crate) fn causation_id(&self) -> Option<&crate::command_ledger::CausationId> {
        self.causation_id.as_ref()
    }

    pub(crate) fn key(&self) -> &CommandLedgerKey {
        &self.key
    }
}

/// Exact terminal cell receipt recovered from the command ledger.
#[derive(Clone, Debug, PartialEq)]
pub struct CellDispatchResult {
    payload: Value,
    events: Vec<CellProjectionEventWireItem>,
    command_id: String,
    causation_id: String,
    state: String,
    replayed: bool,
}

impl CellDispatchResult {
    /// Exact confirmation evidence retained with this command's retry receipt.
    /// Delivery may have already removed all of the command's outbox rows.
    pub fn projection_events(&self) -> &[CellProjectionEventWireItem] {
        &self.events
    }
    pub fn payload(&self) -> &Value {
        &self.payload
    }

    pub fn command_id(&self) -> &str {
        &self.command_id
    }

    pub fn causation_id(&self) -> &str {
        &self.causation_id
    }

    pub fn state(&self) -> &str {
        &self.state
    }

    pub fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Stable error vocabulary for the feature-free cell wait path.
#[derive(Debug)]
pub enum CellDispatchError {
    BadRequest(String),
    Unauthorized,
    Forbidden,
    CommandIdReuse,
    InProgress,
    Expired,
    Rejected {
        code: &'static str,
        status: u16,
        message: String,
    },
    Internal(String),
}

impl CellDispatchError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "BAD_REQUEST",
            Self::Unauthorized => "UNAUTHORIZED",
            Self::Forbidden => "FORBIDDEN",
            Self::CommandIdReuse => "COMMAND_ID_REUSE",
            Self::InProgress => "COMMAND_IN_PROGRESS",
            Self::Expired => "COMMAND_EXPIRED",
            Self::Rejected { code, .. } => code,
            Self::Internal(_) => "INTERNAL",
        }
    }

    pub fn status_code(&self) -> u16 {
        match self {
            Self::BadRequest(_) => 400,
            Self::Unauthorized => 401,
            Self::Forbidden => 403,
            Self::CommandIdReuse | Self::InProgress => 409,
            Self::Expired => 410,
            Self::Rejected { status, .. } => *status,
            Self::Internal(_) => 500,
        }
    }

    pub fn client_message(&self) -> String {
        match self {
            Self::BadRequest(message) => message.clone(),
            Self::Unauthorized => "missing authenticated principal".into(),
            Self::Forbidden => "command is not allowed".into(),
            Self::CommandIdReuse => "command ID was already used for different input".into(),
            Self::InProgress => "command is already in progress".into(),
            Self::Expired => "command ID has expired".into(),
            Self::Rejected { message, .. } => message.clone(),
            Self::Internal(_) => "internal error".into(),
        }
    }
}

impl std::fmt::Display for CellDispatchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Internal(detail) => formatter.write_str(detail),
            _ => formatter.write_str(&self.client_message()),
        }
    }
}

impl std::error::Error for CellDispatchError {}

pub(crate) fn handler_error_code(error: &HandlerError) -> &'static str {
    match error.status_code() {
        400 => "BAD_REQUEST",
        401 => "UNAUTHORIZED",
        403 => "FORBIDDEN",
        404 => "NOT_FOUND",
        _ => "REJECTED",
    }
}

pub(crate) fn internal_ledger_error(error: CommandLedgerError) -> CellDispatchError {
    match error {
        CommandLedgerError::Invalid(message) => CellDispatchError::BadRequest(message),
        other => CellDispatchError::Internal(other.to_string()),
    }
}

pub(crate) fn replay_result(
    replay: CommandReplay,
    replayed: bool,
) -> Result<CellDispatchResult, CellDispatchError> {
    match replay.state {
        CommandLedgerState::Succeeded
        | CommandLedgerState::SucceededPendingProjection
        | CommandLedgerState::Atomic
        | CommandLedgerState::ProjectionFailed => {
            let receipt: CellCommandReplay =
                serde_json::from_value(replay.outcome).map_err(|error| {
                    CellDispatchError::Internal(format!("invalid cell command replay: {error}"))
                })?;
            // Validate persisted data just as strictly as the initial response.
            super::parse_cell_projection_events(&serde_json::json!({ "events": receipt.events }))
                .map_err(CellDispatchError::Internal)?;
            Ok(CellDispatchResult {
                payload: receipt.payload,
                events: receipt.events,
                command_id: replay.command_id.as_str().to_string(),
                causation_id: replay.causation_id.as_str().to_string(),
                state: replay.state.as_str().to_string(),
                replayed,
            })
        }
        CommandLedgerState::Rejected => replay_rejection(replay.outcome),
        CommandLedgerState::InProgress
        | CommandLedgerState::RetryableUnknown
        | CommandLedgerState::Expired => Err(CellDispatchError::Internal(
            "stored cell replay has a non-terminal state".into(),
        )),
    }
}

fn replay_rejection(outcome: Value) -> Result<CellDispatchResult, CellDispatchError> {
    let error = outcome
        .get("error")
        .and_then(Value::as_object)
        .ok_or_else(|| CellDispatchError::Internal("stored cell rejection is malformed".into()))?;
    let code = match error.get("code").and_then(Value::as_str) {
        Some("BAD_REQUEST") => "BAD_REQUEST",
        Some("UNAUTHORIZED") => "UNAUTHORIZED",
        Some("FORBIDDEN") => "FORBIDDEN",
        Some("NOT_FOUND") => "NOT_FOUND",
        Some("REJECTED") => "REJECTED",
        _ => {
            return Err(CellDispatchError::Internal(
                "stored cell rejection code is invalid".into(),
            ));
        }
    };
    let status = error
        .get("status")
        .and_then(Value::as_u64)
        .and_then(|status| u16::try_from(status).ok())
        .filter(|status| (400..500).contains(status))
        .ok_or_else(|| {
            CellDispatchError::Internal("stored cell rejection status is invalid".into())
        })?;
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            CellDispatchError::Internal("stored cell rejection message is invalid".into())
        })?
        .to_string();
    Err(CellDispatchError::Rejected {
        code,
        status,
        message,
    })
}

pub(crate) async fn commit_rejection<R>(
    repository: &R,
    attempt: CommandAttempt,
    retention: Duration,
    code: &'static str,
    status: u16,
    message: String,
) -> Result<CellDispatchResult, CellDispatchError>
where
    R: CommandLedgerStore + CausalTransactionalCommit + Send + Sync,
{
    let outcome = serde_json::json!({
        "error": {
            "code": code,
            "status": status,
            "message": message,
        }
    });
    let fence = attempt.fence();
    let completion = attempt
        .complete(TerminalCommandState::Rejected, outcome, retention)
        .map_err(internal_ledger_error)?;
    match repository
        .commit_causal_batch(CausalCommitBatch::new(CommitBatch::empty(), completion))
        .await
    {
        Ok(()) => Err(CellDispatchError::Rejected {
            code,
            status,
            message,
        }),
        Err(error) => recover_commit_error(repository, fence, error.to_string()).await,
    }
}

pub(crate) async fn load_committed_result<R>(
    repository: &R,
    fence: &AttemptFence,
    replayed: bool,
) -> Result<CellDispatchResult, CellDispatchError>
where
    R: CommandLedgerStore + Send + Sync,
{
    match repository
        .lookup_command(fence.key(), CommandLookupScope::Attempt(fence))
        .await
        .map_err(internal_ledger_error)?
    {
        CommandLookup::Replay(replay) => replay_result(replay, replayed),
        CommandLookup::Expired => Err(CellDispatchError::Expired),
        CommandLookup::InProgress { .. }
        | CommandLookup::RetryableUnknown { .. }
        | CommandLookup::Unknown => Err(CellDispatchError::Internal(
            "committed cell command has no exact durable replay receipt".into(),
        )),
    }
}

pub(crate) async fn abandon_attempt<R>(
    repository: &R,
    attempt: CommandAttempt,
    detail: String,
) -> Result<CellDispatchResult, CellDispatchError>
where
    R: CommandLedgerStore + Send + Sync,
{
    let fence = attempt.fence();
    match repository.mark_retryable_unknown(fence.clone()).await {
        Ok(()) => Err(CellDispatchError::Internal(detail)),
        Err(CommandLedgerError::AttemptFenced { .. }) => {
            resolve_ambiguous_lookup(repository, fence, detail).await
        }
        Err(error) => Err(CellDispatchError::Internal(format!(
            "{detail}; failed to mark cell command retryable: {error}"
        ))),
    }
}

pub(crate) async fn recover_commit_error<R>(
    repository: &R,
    fence: AttemptFence,
    detail: String,
) -> Result<CellDispatchResult, CellDispatchError>
where
    R: CommandLedgerStore + Send + Sync,
{
    resolve_ambiguous_lookup(repository, fence, detail).await
}

async fn resolve_ambiguous_lookup<R>(
    repository: &R,
    fence: AttemptFence,
    detail: String,
) -> Result<CellDispatchResult, CellDispatchError>
where
    R: CommandLedgerStore + Send + Sync,
{
    match repository
        .lookup_command(fence.key(), CommandLookupScope::Attempt(&fence))
        .await
    {
        Ok(CommandLookup::Replay(replay)) => replay_result(replay, false),
        Ok(CommandLookup::Expired) => Err(CellDispatchError::Expired),
        Ok(CommandLookup::RetryableUnknown { .. }) => Err(CellDispatchError::Internal(detail)),
        Ok(CommandLookup::InProgress { .. }) => {
            match repository.mark_retryable_unknown(fence).await {
                Ok(()) => Err(CellDispatchError::Internal(detail)),
                Err(CommandLedgerError::AttemptFenced { .. }) => Err(CellDispatchError::InProgress),
                Err(error) => Err(CellDispatchError::Internal(format!(
                    "{detail}; cell command recovery failed: {error}"
                ))),
            }
        }
        Ok(CommandLookup::Unknown) => Err(CellDispatchError::Internal(format!(
            "{detail}; cell command ledger row disappeared"
        ))),
        Err(error) => Err(CellDispatchError::Internal(format!(
            "{detail}; cell command outcome lookup failed: {error}"
        ))),
    }
}
