use std::time::{Duration, SystemTime};

use serde_json::Value;

use super::routes::CausalCommandPolicy;
use crate::command::{CommandConsistency, TypedCommandContract};
use crate::command_ledger::{
    AttemptFence, CanonicalInputHash, CausationId, CommandContractFingerprint, CommandId,
    CommandLedgerError, CommandLedgerKey, CommandLedgerState, CommandLedgerStore, CommandLookup,
    CommandLookupScope, CommandReplay, CommandReservation, PrincipalPartitionId,
};
#[cfg(feature = "graphql")]
use crate::command_ledger::{CommandAttempt, TerminalCommandState};
use crate::microsvc::cell_host::{CellCommandIdentity, CellProjectionEventWireItem};
use crate::microsvc::error::HandlerError;
use crate::microsvc::session::Session;
use crate::projection_protocol::SameTransactionProjectionEvidence;
#[cfg(feature = "graphql")]
use crate::projection_protocol::{ProjectionCausationEvidenceRequest, ProjectionObservationKind};
use crate::repository::StreamIdentity;

/// Stable transport classification for a typed causal command dispatch.
///
/// Public receipt/status envelopes map this private error set onto a stable
/// mutation edge without exposing repository details.
#[derive(Debug)]
pub enum CausalDispatchError {
    BadRequest(String),
    /// A caller the host could not authenticate (aggregate-cell ingress).
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
    Handler(HandlerError),
    /// The command kept losing an optimistic stream-version race to
    /// concurrent writers of its aggregate. Nothing was committed and the
    /// command ID is released: retrying it runs the command again.
    ConcurrentWrite,
    Internal(String),
}

impl CausalDispatchError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "BAD_REQUEST",
            Self::Unauthorized => "UNAUTHORIZED",
            Self::Forbidden => "FORBIDDEN",
            Self::CommandIdReuse => "COMMAND_ID_REUSE",
            Self::InProgress => "COMMAND_IN_PROGRESS",
            Self::Expired => "COMMAND_EXPIRED",
            Self::Rejected { code, .. } => code,
            Self::Handler(error) => match error.status_code() {
                400 => "BAD_REQUEST",
                401 => "UNAUTHORIZED",
                403 => "FORBIDDEN",
                404 => "NOT_FOUND",
                422 => "REJECTED",
                _ => "INTERNAL",
            },
            Self::ConcurrentWrite => "CONCURRENT_WRITE",
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
            Self::Handler(error) => error.status_code(),
            Self::ConcurrentWrite => 503,
            Self::Internal(_) => 500,
        }
    }

    pub fn client_message(&self) -> String {
        match self {
            Self::BadRequest(message) => message.clone(),
            Self::Rejected { message, .. } => message.clone(),
            Self::Unauthorized => "missing authenticated principal".into(),
            Self::Forbidden => "command is not allowed".into(),
            Self::CommandIdReuse => "command ID was already used for different input".into(),
            Self::InProgress => "command is already in progress".into(),
            Self::Expired => "command ID has expired".into(),
            Self::Handler(error) => error.client_facing_message(),
            Self::ConcurrentWrite => "command conflicted with concurrent writes; retry".into(),
            Self::Internal(_) => "internal error".into(),
        }
    }
}

impl std::fmt::Display for CausalDispatchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Internal(detail) => formatter.write_str(detail),
            _ => formatter.write_str(&self.client_message()),
        }
    }
}

impl std::error::Error for CausalDispatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Handler(error) => Some(error),
            _ => None,
        }
    }
}

impl From<HandlerError> for CausalDispatchError {
    fn from(error: HandlerError) -> Self {
        Self::Handler(error)
    }
}

/// Durable receipt material for one exact command attempt.
///
/// `direct_projection` is decoded from the versioned ledger replay envelope;
/// it is never reconstructed from the current read-model row.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CausalCommandReceiptSource {
    pub(crate) command_id: String,
    pub(crate) command_name: String,
    pub(crate) causation_id: String,
    pub(crate) consistency: CommandConsistency,
    pub(crate) state: CommandLedgerState,
    pub(crate) outcome: Value,
    /// Only a GraphQL host seals and reads modeled projection metadata.
    #[cfg(feature = "graphql")]
    pub(crate) projection_metadata: Option<crate::graphql::protocol::CommandProjectionMetadataV1>,
    pub(crate) direct_projection: Option<SameTransactionProjectionEvidence>,
}

impl CausalCommandReceiptSource {
    pub(super) fn from_replay(
        consistency: CommandConsistency,
        replay: CommandReplay,
    ) -> Result<Self, CausalDispatchError> {
        let direct_projection = replay
            .direct_projection
            .as_ref()
            .map(SameTransactionProjectionEvidence::from_replay_value)
            .transpose()
            .map_err(|error| {
                CausalDispatchError::Internal(format!(
                    "stored direct projection evidence is invalid: {error}"
                ))
            })?;
        #[cfg(feature = "graphql")]
        let projection_metadata = replay
            .projection_metadata
            .as_deref()
            .map(crate::graphql::protocol::CommandProjectionMetadataV1::from_json)
            .transpose()
            .map_err(|error| {
                CausalDispatchError::Internal(format!(
                    "stored command projection metadata is invalid: {error}"
                ))
            })?;
        Ok(Self {
            command_id: replay.command_id.as_str().to_string(),
            command_name: replay.command_name,
            causation_id: replay.causation_id.as_str().to_string(),
            consistency,
            state: replay.state,
            outcome: replay.outcome,
            #[cfg(feature = "graphql")]
            projection_metadata,
            direct_projection,
        })
    }
}

/// Successful typed causal dispatch plus its exact durable receipt source.
#[derive(Clone, Debug, PartialEq)]
pub struct CausalDispatchResult {
    pub(crate) payload: Value,
    pub(crate) receipt: CausalCommandReceiptSource,
    /// Domain-event evidence an aggregate cell committed with the command,
    /// used to seal wait-path projection metadata. Transport delivery remains
    /// owned by the aggregate cell and Queue.
    pub(crate) projection_events: Vec<CellProjectionEventWireItem>,
    /// The command ledger replayed an earlier receipt (the command id was
    /// already settled); the handler did not run again.
    pub(crate) replayed: bool,
}

/// Gateway-side capability for one externally dispatched command. The attempt
/// owns the durable reservation and is consumed only by terminal completion or
/// retryable-unknown recovery; it is never reconstructed from the wire.
#[cfg(feature = "graphql")]
pub(crate) struct ExternalCausalAttempt {
    pub(crate) command_name: String,
    pub(crate) attempt: CommandAttempt,
    pub(crate) retention: Duration,
}

#[cfg(feature = "graphql")]
pub(crate) enum ExternalCausalReservation {
    Acquired(ExternalCausalAttempt),
    Replay(CausalDispatchResult),
}

#[cfg(feature = "graphql")]
impl ExternalCausalAttempt {
    pub(crate) fn command_id(&self) -> &str {
        self.attempt.key().command_id()
    }

    pub(crate) fn causation_id(&self) -> &str {
        self.attempt.causation_id().as_str()
    }

    pub(crate) fn fence(&self) -> AttemptFence {
        self.attempt.fence()
    }

    pub(crate) fn validate_remote(
        &self,
        result: &CausalDispatchResult,
    ) -> Result<(), CausalDispatchError> {
        if result.command_id() != self.command_id() {
            return Err(CausalDispatchError::Internal(
                "cell wait-path returned a different command ID".into(),
            ));
        }
        if result.causation_id() != self.causation_id() {
            return Err(CausalDispatchError::Internal(
                "cell wait-path returned a different causation ID".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn into_external_completion(
        self,
        result: &CausalDispatchResult,
    ) -> Result<crate::command_ledger::ExternalCommandCompletion, CausalDispatchError> {
        self.validate_remote(result)?;
        let state = match result.receipt.state {
            CommandLedgerState::Succeeded => TerminalCommandState::Succeeded,
            CommandLedgerState::SucceededPendingProjection => {
                TerminalCommandState::SucceededPendingProjection
            }
            CommandLedgerState::Atomic => {
                return Err(CausalDispatchError::BadRequest(
                    "atomic commands cannot be dispatched to an aggregate cell".into(),
                ));
            }
            other => {
                return Err(CausalDispatchError::Internal(format!(
                    "cell wait-path returned non-terminal state `{}`",
                    other.as_str()
                )));
            }
        };
        let binding = self.attempt.external_binding().cloned().ok_or_else(|| {
            CausalDispatchError::Internal(
                "external command attempt lost its immutable route binding".into(),
            )
        })?;
        if let Some(metadata) = result.receipt.projection_metadata.as_ref() {
            let bytes = metadata.canonical_bytes().map_err(|error| {
                CausalDispatchError::Internal(format!(
                    "cell projection metadata could not be canonicalized: {error}"
                ))
            })?;
            let expires_at = metadata.expires_at().map_err(|error| {
                CausalDispatchError::Internal(format!(
                    "cell projection metadata retention deadline is invalid: {error}"
                ))
            })?;
            return self
                .attempt
                .complete_external_with_projection_metadata_until(
                    binding,
                    state,
                    result.payload().clone(),
                    bytes,
                    self.retention,
                    expires_at,
                )
                .map_err(internal_ledger_error);
        }
        self.attempt
            .complete_external(binding, state, result.payload().clone(), self.retention)
            .map_err(internal_ledger_error)
    }
}

impl CausalDispatchResult {
    /// Handler payload returned to the wait-path caller.
    pub fn payload(&self) -> &Value {
        &self.payload
    }

    /// Replace the handler payload (wait-path clients remapping wire JSON).
    pub fn with_payload(mut self, payload: Value) -> Self {
        self.payload = payload;
        self
    }

    /// Domain events an aggregate cell committed with the command: exact
    /// confirmation evidence retained with its retry receipt. Delivery may
    /// have already removed all of the command's outbox rows.
    pub fn projection_events(&self) -> &[CellProjectionEventWireItem] {
        &self.projection_events
    }

    /// Client-supplied durable command id.
    pub fn command_id(&self) -> &str {
        &self.receipt.command_id
    }

    /// Whether this result replays an earlier settled receipt.
    pub fn replayed(&self) -> bool {
        self.replayed
    }

    /// The receipt of the attempt that just committed it, not a replay.
    pub(crate) fn fresh(mut self) -> Self {
        self.replayed = false;
        self
    }

    /// Ledger causation id assigned on accept.
    pub fn causation_id(&self) -> &str {
        &self.receipt.causation_id
    }

    /// Stable ledger state name (`succeeded`, `atomic`, …).
    pub fn state(&self) -> &'static str {
        self.receipt.state.as_str()
    }

    /// The standard wait-path response body (`{payload, receipt: {commandId,
    /// causationId, state, replayed}, events}`) every command host decodes
    /// ([`Self::from_wait_path_wire`]).
    pub fn wait_path_wire(&self) -> Value {
        serde_json::json!({
            "payload": self.payload,
            "receipt": {
                "commandId": self.command_id(),
                "causationId": self.causation_id(),
                "state": self.state(),
                "replayed": self.replayed,
            },
            "events": self.projection_events,
        })
    }

    /// Rebuild a receipt from the HTTP/gRPC wait-path JSON envelope.
    pub fn from_wait_path_wire(body: Value) -> Result<Self, CausalDispatchError> {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct ReceiptWire {
            command_id: String,
            causation_id: String,
            state: String,
            #[serde(default)]
            replayed: Option<bool>,
        }

        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct ResponseWire {
            #[serde(default)]
            payload: Value,
            receipt: ReceiptWire,
            #[serde(default)]
            events: Vec<CellProjectionEventWireItem>,
        }

        let wire: ResponseWire = serde_json::from_value(body).map_err(|error| {
            CausalDispatchError::Internal(format!("invalid wait-path response: {error}"))
        })?;
        if wire.receipt.command_id.trim().is_empty()
            || wire.receipt.command_id.len() > 512
            || wire.receipt.causation_id.len() > 512
        {
            return Err(CausalDispatchError::Internal(
                "wait-path receipt contains an invalid identifier".into(),
            ));
        }
        let state = crate::command_ledger::CommandLedgerState::parse(&wire.receipt.state).map_err(
            |err| CausalDispatchError::Internal(format!("wait-path receipt state: {err}")),
        )?;
        let replayed = wire.receipt.replayed.unwrap_or(false);
        crate::microsvc::cell_host::parse_cell_projection_events(&serde_json::json!({
            "events": wire.events
        }))
        .map_err(|error| {
            CausalDispatchError::Internal(format!(
                "invalid cell projection event response: {error}"
            ))
        })?;
        Ok(Self {
            payload: wire.payload,
            projection_events: wire.events,
            replayed,
            receipt: CausalCommandReceiptSource {
                command_id: wire.receipt.command_id,
                command_name: String::new(),
                causation_id: wire.receipt.causation_id,
                consistency: crate::command::CommandConsistency::Succeeded,
                state,
                outcome: Value::Null,
                #[cfg(feature = "graphql")]
                projection_metadata: None,
                direct_projection: None,
            },
        })
    }
}

#[cfg(feature = "graphql")]
impl CausalDispatchResult {
    /// Fill Eventual modeled projection metadata from cell-committed domain
    /// events. The generated replica requires the same projection delta local
    /// dispatch records, independently of how those events are transported.
    pub(crate) fn seal_wait_path_protocol(
        mut self,
        protocol: &crate::graphql::protocol::ProtocolResponseAccumulator,
        contract: &TypedCommandContract,
        replay_retention: Duration,
    ) -> Result<Self, CausalDispatchError> {
        self.receipt.command_name = contract.name.clone();
        self.receipt.consistency = contract.consistency;
        if contract.consistency == CommandConsistency::Atomic
            || contract.projections.selectors.is_empty()
            || self.projection_events.is_empty()
        {
            return Ok(self);
        }
        let events = self
            .projection_events
            .iter()
            .cloned()
            .map(CellProjectionEventWireItem::try_into_message)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                CausalDispatchError::Internal(format!(
                    "invalid cell projection event response: {error}"
                ))
            })?;
        let occurrences = events
            .iter()
            .map(|row| {
                row.domain_event_occurrence().map_err(|error| {
                    CausalDispatchError::Internal(format!(
                        "wait-path projection event is not a domain occurrence: {error}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let causation = occurrences
            .iter()
            .find_map(|occurrence| occurrence.metadata().get("causation_id").cloned())
            .or_else(|| {
                events
                    .iter()
                    .find_map(|row| row.metadata.get("causation_id").cloned())
            })
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                CausalDispatchError::Internal(
                    "wait-path projection events missing causation_id for modeled projection"
                        .into(),
                )
            })?;
        let causation_id = CausationId::parse_stored(causation).map_err(|error| {
            CausalDispatchError::Internal(format!("wait-path causation id: {error}"))
        })?;
        self.receipt.causation_id = causation_id.as_str().to_string();
        let metadata = protocol
            .projection_metadata_for_actual(
                causation_id,
                replay_retention,
                &occurrences,
                &contract.projections.selectors,
            )
            .map_err(|error| {
                CausalDispatchError::Internal(format!("wait-path projection metadata: {error}"))
            })?;
        // The cell has durably committed the domain event, but the modeled
        // read-model projector is still asynchronous. Preserve the exact
        // event-derived obligations so the client can retire its accepted
        // optimistic layer only after a matching live/read observation. The
        // wait-path has no command-ledger observation rows of its own; that
        // affects status evidence below, not the modeled obligation contract.
        self.receipt.state = CommandLedgerState::Succeeded;
        self.receipt.projection_metadata = Some(metadata);
        Ok(self)
    }

    /// Status envelope matching this wait-path receipt so commandStatus can
    /// complete Eventual expects without a local command-ledger row.
    pub fn public_status(&self) -> CausalCommandPublicStatus {
        let state = match self.receipt.state {
            CommandLedgerState::InProgress | CommandLedgerState::RetryableUnknown => {
                CausalCommandPublicState::InProgress
            }
            CommandLedgerState::Succeeded => CausalCommandPublicState::Succeeded,
            CommandLedgerState::SucceededPendingProjection => {
                CausalCommandPublicState::SucceededPendingProjection
            }
            CommandLedgerState::Atomic => CausalCommandPublicState::Atomic,
            CommandLedgerState::Rejected => CausalCommandPublicState::Rejected,
            CommandLedgerState::ProjectionFailed => CausalCommandPublicState::ProjectionFailed,
            CommandLedgerState::Expired => CausalCommandPublicState::Expired,
        };
        // A wait-path receipt proves the cell command commit, not asynchronous
        // projector application. Keep modeled obligations in the metadata, but
        // never claim them observed without a read/live proof.
        let evidence = Vec::new();
        CausalCommandPublicStatus {
            state,
            command_id: self.receipt.command_id.clone(),
            command_name: (!self.receipt.command_name.is_empty())
                .then(|| self.receipt.command_name.clone()),
            causation_id: (!self.receipt.causation_id.is_empty())
                .then(|| self.receipt.causation_id.clone()),
            consistency: Some(self.receipt.consistency),
            outcome: Some(self.payload.clone()),
            projection_metadata: self.receipt.projection_metadata.clone(),
            projection_revalidate: false,
            evidence,
            direct_projection: self.receipt.direct_projection.clone(),
        }
    }
}

/// Stable public command-status vocabulary.
#[cfg(feature = "graphql")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CausalCommandPublicState {
    InProgress,
    Succeeded,
    SucceededPendingProjection,
    /// Terminal for an atomic command (same-tx read-model row sealed).
    Atomic,
    Rejected,
    ProjectionFailed,
    Expired,
    Unknown,
}

#[cfg(feature = "graphql")]
impl CausalCommandPublicState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::InProgress => "in_progress",
            Self::Succeeded => "succeeded",
            Self::SucceededPendingProjection => "succeeded_pending_projection",
            Self::Atomic => "atomic",
            Self::Rejected => "rejected",
            Self::ProjectionFailed => "projection_failed",
            Self::Expired => "expired",
            Self::Unknown => "unknown",
        }
    }
}

/// Sanitized evidence state for one durable obligation.
///
/// A terminal failure is intentionally only a semantic marker. Failure IDs,
/// codes, bytes, digests, source cursors, and repair generations never cross
/// this service boundary.
#[cfg(feature = "graphql")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CausalProjectionEvidenceState {
    Pending,
    Observed,
    TerminalFailure,
}

#[cfg(feature = "graphql")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CausalCommandProjectionEvidence {
    pub(crate) obligation_index: usize,
    pub(crate) state: CausalProjectionEvidenceState,
    pub(crate) incarnation: Option<u64>,
    pub(crate) revision: Option<u64>,
}

/// Authorized, non-enumerating status for a client-created command ID.
///
/// Typed scopes and observations are crate-private inputs to the opaque token
/// codec. This type is not serializable and contains no raw failure material.
#[cfg(feature = "graphql")]
#[derive(Clone, Debug, PartialEq)]
pub struct CausalCommandPublicStatus {
    pub(crate) state: CausalCommandPublicState,
    pub(crate) command_id: String,
    /// Trusted durable command identity for complete terminal receipts.
    pub(crate) command_name: Option<String>,
    pub(crate) causation_id: Option<String>,
    pub(crate) consistency: Option<CommandConsistency>,
    pub(crate) outcome: Option<Value>,
    pub(crate) projection_metadata: Option<crate::graphql::protocol::CommandProjectionMetadataV1>,
    /// Historical modeled work was authenticated against an exact Draining
    /// binding, but its old-scope delta is not applicable to this client.
    pub(crate) projection_revalidate: bool,
    pub(crate) evidence: Vec<CausalCommandProjectionEvidence>,
    pub(crate) direct_projection: Option<SameTransactionProjectionEvidence>,
}

#[cfg(feature = "graphql")]
impl CausalCommandPublicStatus {
    pub(crate) fn unknown(command_id: impl Into<String>) -> Self {
        Self {
            state: CausalCommandPublicState::Unknown,
            command_id: command_id.into(),
            command_name: None,
            causation_id: None,
            consistency: None,
            outcome: None,
            projection_metadata: None,
            projection_revalidate: false,
            evidence: Vec::new(),
            direct_projection: None,
        }
    }

    pub fn is_unknown(&self) -> bool {
        self.state == CausalCommandPublicState::Unknown
    }
}

/// Error returned when attaching a GraphQL engine whose typed command
/// inventory is not exactly the executable service inventory, or whose query
/// storage cannot prove the identity required by a `Projected` command.
#[cfg(feature = "graphql")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphqlServiceBindError(pub String);

#[cfg(feature = "graphql")]
impl std::fmt::Display for GraphqlServiceBindError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[cfg(feature = "graphql")]
impl std::error::Error for GraphqlServiceBindError {}

pub(super) fn ensure_causal_grant(
    contract: &TypedCommandContract,
    session: &Session,
) -> Result<(), CausalDispatchError> {
    // Identity is a set (`x-roles`). One admission law for GraphQL and
    // composition: allow when any asserted role is granted the command.
    match crate::application::admit_command_session(
        &contract.roles,
        session.user_id(),
        &session.roles(),
    ) {
        Ok(()) => Ok(()),
        Err("unauthenticated") => Err(CausalDispatchError::Handler(
            crate::microsvc::HandlerError::Unauthorized("missing authenticated principal".into()),
        )),
        Err(_) => Err(CausalDispatchError::Forbidden),
    }
}

/// A modeled command's projection metadata, as sealed into its ledger receipt.
#[cfg_attr(not(feature = "graphql"), allow(dead_code))]
pub(super) struct ModeledProjectionMetadata {
    pub(super) canonical_bytes: Vec<u8>,
    pub(super) expires_at: SystemTime,
    /// Projectors still owe obligations for this command.
    pub(super) pending: bool,
}

/// Seals the modeled projection metadata of the domain events a command is
/// about to commit. Only a GraphQL host holds the protocol authority for it.
pub(super) type SealModeledProjection<'a> = dyn Fn(
        CausationId,
        Duration,
        &[crate::DomainEventOccurrence],
        &[crate::ProjectionEventSelector],
    ) -> Result<ModeledProjectionMetadata, String>
    + Send
    + Sync
    + 'a;

/// Where a typed command entered the host. Everything the one command
/// pipeline (`execute_typed_command`) does differently per ingress is decided
/// by this value.
pub(super) enum CommandIngress<'a> {
    /// A service's GraphQL or wait-path ingress, for a verified principal.
    Service {
        service_id: &'a str,
        principal_partition: String,
        command_id: &'a str,
        policy: CausalCommandPolicy,
        seal_modeled_projection: Option<&'a SealModeledProjection<'a>>,
    },
    /// One aggregate cell. It commits only its own stream and keeps the
    /// command's domain events in the ledger receipt.
    Cell {
        identity: &'a CellCommandIdentity,
        shard: &'a StreamIdentity,
    },
}

impl CommandIngress<'_> {
    pub(super) fn policy(&self) -> CausalCommandPolicy {
        match self {
            Self::Service { policy, .. } => *policy,
            Self::Cell { .. } => CausalCommandPolicy::default(),
        }
    }

    /// Whether this ingress runs `contract` for `session` at all.
    pub(super) fn admit(
        &self,
        contract: &TypedCommandContract,
        session: &Session,
    ) -> Result<(), CausalDispatchError> {
        let granted = ensure_causal_grant(contract, session);
        match self {
            Self::Service { .. } => granted,
            Self::Cell { .. } => {
                granted.map_err(|error| match error {
                    CausalDispatchError::Handler(_) => CausalDispatchError::Unauthorized,
                    other => other,
                })?;
                if contract.consistency == CommandConsistency::Atomic {
                    return Err(CausalDispatchError::BadRequest(
                        "atomic typed commands require a same-transaction relational projection host"
                            .into(),
                    ));
                }
                Ok(())
            }
        }
    }

    /// The ledger reservation of this command: a service derives its key
    /// from the verified principal, a cell is handed a trusted identity.
    pub(super) fn reservation(
        &self,
        command_name: &str,
        contract_fingerprint: CommandContractFingerprint,
        input_hash: CanonicalInputHash,
    ) -> Result<CommandReservation, CausalDispatchError> {
        let (key, causation_id) = match self {
            Self::Service {
                service_id,
                principal_partition,
                command_id,
                ..
            } => {
                let command_id = CommandId::parse(command_id)
                    .map_err(|error| CausalDispatchError::BadRequest(error.to_string()))?;
                let partition = PrincipalPartitionId::new(principal_partition.clone())
                    .map_err(internal_ledger_error)?;
                let key = CommandLedgerKey::new(*service_id, partition, command_id)
                    .map_err(internal_ledger_error)?;
                (key, None)
            }
            Self::Cell { identity, .. } => {
                (identity.key().clone(), identity.causation_id().cloned())
            }
        };
        let policy = self.policy();
        let reservation = CommandReservation::new(
            key,
            command_name,
            contract_fingerprint,
            input_hash,
            policy.attempt_lease,
            policy.replay_retention,
        )
        .map_err(internal_ledger_error)?;
        Ok(match causation_id {
            Some(causation_id) => reservation.with_causation_id(causation_id),
            None => reservation,
        })
    }

    /// Render a ledger replay as this ingress's dispatch result.
    pub(super) fn replay(
        &self,
        consistency: CommandConsistency,
        replay: CommandReplay,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        let result = replay_result(consistency, replay)?;
        match self {
            Self::Service { .. } => Ok(result),
            Self::Cell { .. } => crate::microsvc::cell_host::causal::open_cell_receipt(result),
        }
    }
}

pub(super) fn handler_error_code(error: &HandlerError) -> &'static str {
    match error.status_code() {
        400 => "BAD_REQUEST",
        401 => "UNAUTHORIZED",
        403 => "FORBIDDEN",
        404 => "NOT_FOUND",
        _ => "REJECTED",
    }
}

pub(super) fn internal_ledger_error(error: CommandLedgerError) -> CausalDispatchError {
    CausalDispatchError::Internal(error.to_string())
}

#[cfg(feature = "graphql")]
pub(super) async fn abandon_external_attempt<R>(
    repository: &R,
    attempt: ExternalCausalAttempt,
    detail: String,
) -> Result<(), CausalDispatchError>
where
    R: CommandLedgerStore + Send + Sync,
{
    let fence = attempt.fence();
    match repository.mark_retryable_unknown(fence.clone()).await {
        Ok(()) => Err(CausalDispatchError::Internal(detail)),
        Err(CommandLedgerError::AttemptFenced { .. }) => match repository
            .lookup_command(fence.key(), CommandLookupScope::Attempt(&fence))
            .await
        {
            Ok(CommandLookup::Replay(_)) => Ok(()),
            Ok(CommandLookup::Expired) => Err(CausalDispatchError::Expired),
            Ok(CommandLookup::RetryableUnknown { .. }) => {
                Err(CausalDispatchError::Internal(detail))
            }
            Ok(CommandLookup::InProgress { .. }) | Ok(CommandLookup::Unknown) => {
                Err(CausalDispatchError::Internal(detail))
            }
            Err(error) => Err(CausalDispatchError::Internal(format!(
                "{detail}; external command recovery failed: {error}"
            ))),
        },
        Err(error) => Err(CausalDispatchError::Internal(format!(
            "{detail}; failed to mark external command retryable: {error}"
        ))),
    }
}

/// A ledger replay whose stored outcome is the handler payload. A rejection
/// replays as the error it was.
pub(super) fn replay_result(
    consistency: CommandConsistency,
    replay: CommandReplay,
) -> Result<CausalDispatchResult, CausalDispatchError> {
    match replay.state {
        CommandLedgerState::Succeeded
        | CommandLedgerState::SucceededPendingProjection
        | CommandLedgerState::Atomic
        | CommandLedgerState::ProjectionFailed => {
            let receipt = CausalCommandReceiptSource::from_replay(consistency, replay)?;
            Ok(CausalDispatchResult {
                payload: receipt.outcome.clone(),
                receipt,
                projection_events: Vec::new(),
                replayed: true,
            })
        }
        CommandLedgerState::Rejected => Err(replay_rejection(replay.outcome)),
        CommandLedgerState::InProgress
        | CommandLedgerState::RetryableUnknown
        | CommandLedgerState::Expired => Err(CausalDispatchError::Internal(
            "stored replay has a non-terminal state".into(),
        )),
    }
}

/// The ledger outcome of a terminal rejection.
pub(super) fn rejection_outcome(code: &str, status: u16, message: &str) -> Value {
    serde_json::json!({
        "error": {
            "code": code,
            "status": status,
            "message": message,
        }
    })
}

fn replay_rejection(outcome: Value) -> CausalDispatchError {
    let invalid = |what: &str| CausalDispatchError::Internal(format!("stored rejection {what}"));
    let Some(error) = outcome.get("error").and_then(Value::as_object) else {
        return invalid("is malformed");
    };
    let code = match error.get("code").and_then(Value::as_str) {
        Some("BAD_REQUEST") => "BAD_REQUEST",
        Some("UNAUTHORIZED") => "UNAUTHORIZED",
        Some("FORBIDDEN") => "FORBIDDEN",
        Some("NOT_FOUND") => "NOT_FOUND",
        Some("REJECTED") => "REJECTED",
        _ => return invalid("code is invalid"),
    };
    let Some(status) = error
        .get("status")
        .and_then(Value::as_u64)
        .and_then(|status| u16::try_from(status).ok())
        .filter(|status| (400..500).contains(status))
    else {
        return invalid("status is invalid");
    };
    let Some(message) = error.get("message").and_then(Value::as_str) else {
        return invalid("message is invalid");
    };
    CausalDispatchError::Rejected {
        code,
        status,
        message: message.to_string(),
    }
}

/// Release an attempt that produced nothing durable, so a retry with the same
/// command ID runs again instead of waiting out the attempt lease. `error` is
/// what the caller is told.
pub(super) async fn abandon_attempt<R>(
    repository: &R,
    ingress: &CommandIngress<'_>,
    consistency: CommandConsistency,
    fence: AttemptFence,
    error: CausalDispatchError,
) -> Result<CausalDispatchResult, CausalDispatchError>
where
    R: CommandLedgerStore + Send + Sync,
{
    match repository.mark_retryable_unknown(fence.clone()).await {
        Ok(()) => Err(error),
        Err(CommandLedgerError::AttemptFenced { .. }) => {
            resolve_ambiguous_lookup(repository, ingress, consistency, fence, error).await
        }
        Err(recovery) => Err(CausalDispatchError::Internal(format!(
            "{error}; failed to mark command retryable: {recovery}"
        ))),
    }
}

/// Settle an attempt whose fate this process does not know (a commit that
/// did not report success, or an abandonment that was fenced): the ledger
/// decides. `error` is what the caller is told if the attempt left nothing.
pub(super) async fn resolve_ambiguous_lookup<R>(
    repository: &R,
    ingress: &CommandIngress<'_>,
    consistency: CommandConsistency,
    fence: AttemptFence,
    error: CausalDispatchError,
) -> Result<CausalDispatchResult, CausalDispatchError>
where
    R: CommandLedgerStore + Send + Sync,
{
    match repository
        .lookup_command(fence.key(), CommandLookupScope::Attempt(&fence))
        .await
    {
        Ok(CommandLookup::Replay(replay)) => ingress
            .replay(consistency, replay)
            .map(CausalDispatchResult::fresh),
        Ok(CommandLookup::Expired) => Err(CausalDispatchError::Expired),
        Ok(CommandLookup::RetryableUnknown { .. }) => Err(error),
        Ok(CommandLookup::InProgress { .. }) => {
            match repository.mark_retryable_unknown(fence).await {
                Ok(()) => Err(error),
                Err(CommandLedgerError::AttemptFenced { .. }) => {
                    Err(CausalDispatchError::InProgress)
                }
                Err(recovery) => Err(CausalDispatchError::Internal(format!(
                    "{error}; command recovery failed: {recovery}"
                ))),
            }
        }
        Ok(CommandLookup::Unknown) => Err(CausalDispatchError::Internal(format!(
            "{error}; command ledger row disappeared"
        ))),
        Err(recovery) => Err(CausalDispatchError::Internal(format!(
            "{error}; command outcome lookup failed: {recovery}"
        ))),
    }
}

#[cfg(feature = "graphql")]
pub(super) async fn evaluate_causal_command_status<R>(
    repository: &R,
    command_id: &CommandId,
    consistency: CommandConsistency,
    lookup: CommandLookup,
    protocol: Option<&crate::graphql::protocol::ProtocolResponseAccumulator>,
) -> Result<CausalCommandPublicStatus, CausalDispatchError>
where
    R: CommandLedgerStore + crate::microsvc::dependencies::CausalHostProjections + Send + Sync,
{
    match lookup {
        CommandLookup::Unknown => Ok(CausalCommandPublicStatus::unknown(command_id.as_str())),
        CommandLookup::Expired => Ok(CausalCommandPublicStatus {
            state: CausalCommandPublicState::Expired,
            command_id: command_id.as_str().to_string(),
            command_name: None,
            causation_id: None,
            consistency: Some(consistency),
            outcome: None,
            projection_metadata: None,
            projection_revalidate: false,
            evidence: Vec::new(),
            direct_projection: None,
        }),
        CommandLookup::InProgress { causation_id }
        | CommandLookup::RetryableUnknown { causation_id } => Ok(CausalCommandPublicStatus {
            state: CausalCommandPublicState::InProgress,
            command_id: command_id.as_str().to_string(),
            command_name: None,
            causation_id: Some(causation_id.as_str().to_string()),
            consistency: Some(consistency),
            outcome: None,
            projection_metadata: None,
            projection_revalidate: false,
            evidence: Vec::new(),
            direct_projection: None,
        }),
        CommandLookup::Replay(replay) => {
            let receipt = CausalCommandReceiptSource::from_replay(consistency, replay)?;
            let modeled_plan = match receipt.projection_metadata.as_ref() {
                Some(metadata) => Some(
                    protocol
                        .ok_or_else(|| {
                            CausalDispatchError::Internal(
                                "modeled projection status requires authenticated protocol authority"
                                    .into(),
                            )
                        })?
                        .modeled_projection_evidence_topologies(
                            &receipt.command_name,
                            &receipt.causation_id,
                            metadata,
                        )
                        .map_err(|error| {
                            CausalDispatchError::Internal(format!(
                                "modeled projection status authority rejected its stored identity: {error}"
                            ))
                        })?,
                ),
                None => None,
            };
            let projection_revalidate = modeled_plan.as_ref().is_some_and(|plan| {
                plan.disposition
                    == crate::graphql::projection_delta::runtime::ModeledProjectionStatusDisposition::Revalidate
            });
            let (state, evidence) = match receipt.state {
                CommandLedgerState::Succeeded
                    if receipt
                        .projection_metadata
                        .as_ref()
                        .is_some_and(|metadata| !metadata.obligations.is_empty()) =>
                {
                    let (_, evidence) = evaluate_pending_projection_evidence(
                        repository,
                        &receipt,
                        protocol,
                        modeled_plan.as_ref(),
                    )
                    .await?;
                    // A succeeded ledger receipt proves the command commit.
                    // Retained projection obligations are only evidence detail;
                    // their asynchronous proof must not rewrite that public
                    // command state.
                    (CausalCommandPublicState::Succeeded, evidence)
                }
                CommandLedgerState::Succeeded => (CausalCommandPublicState::Succeeded, Vec::new()),
                CommandLedgerState::Atomic => (
                    CausalCommandPublicState::Atomic,
                    (0..receipt
                        .projection_metadata
                        .as_ref()
                        .map_or(0, |metadata| metadata.obligations.len()))
                        .map(|obligation_index| CausalCommandProjectionEvidence {
                            obligation_index,
                            state: CausalProjectionEvidenceState::Observed,
                            // The durable ledger state proves every finite
                            // obligation. Exact record positions are optional
                            // status detail and are not reconstructed from a
                            // later row head.
                            incarnation: None,
                            revision: None,
                        })
                        .collect(),
                ),
                CommandLedgerState::Rejected => (CausalCommandPublicState::Rejected, Vec::new()),
                CommandLedgerState::ProjectionFailed => {
                    (CausalCommandPublicState::ProjectionFailed, Vec::new())
                }
                CommandLedgerState::SucceededPendingProjection => {
                    evaluate_pending_projection_evidence(
                        repository,
                        &receipt,
                        protocol,
                        modeled_plan.as_ref(),
                    )
                    .await?
                }
                CommandLedgerState::InProgress
                | CommandLedgerState::RetryableUnknown
                | CommandLedgerState::Expired => {
                    return Err(CausalDispatchError::Internal(format!(
                        "stored replay has non-terminal state `{}`",
                        receipt.state.as_str()
                    )));
                }
            };
            Ok(CausalCommandPublicStatus {
                state,
                command_id: receipt.command_id,
                command_name: Some(receipt.command_name),
                causation_id: Some(receipt.causation_id),
                consistency: Some(receipt.consistency),
                outcome: Some(receipt.outcome),
                projection_metadata: receipt.projection_metadata,
                projection_revalidate,
                evidence,
                direct_projection: receipt.direct_projection,
            })
        }
    }
}

#[cfg(feature = "graphql")]
pub(super) async fn evaluate_pending_projection_evidence<R>(
    repository: &R,
    receipt: &CausalCommandReceiptSource,
    protocol: Option<&crate::graphql::protocol::ProtocolResponseAccumulator>,
    modeled_plan: Option<&crate::graphql::projection_delta::runtime::ModeledProjectionStatusPlan>,
) -> Result<
    (
        CausalCommandPublicState,
        Vec<CausalCommandProjectionEvidence>,
    ),
    CausalDispatchError,
>
where
    R: crate::microsvc::dependencies::CausalHostProjections + Send + Sync,
{
    let metadata = receipt.projection_metadata.as_ref().ok_or_else(|| {
        CausalDispatchError::Internal(
            "pending projection replay has no modeled projection metadata".into(),
        )
    })?;
    evaluate_pending_modeled_projection_evidence(
        repository,
        receipt,
        metadata,
        protocol.ok_or_else(|| {
            CausalDispatchError::Internal(
                "modeled projection status requires authenticated protocol authority".into(),
            )
        })?,
        modeled_plan.ok_or_else(|| {
            CausalDispatchError::Internal(
                "modeled projection status is missing its authenticated status plan".into(),
            )
        })?,
    )
    .await
}

#[cfg(feature = "graphql")]
async fn evaluate_pending_modeled_projection_evidence<R>(
    repository: &R,
    receipt: &CausalCommandReceiptSource,
    metadata: &crate::graphql::protocol::CommandProjectionMetadataV1,
    protocol: &crate::graphql::protocol::ProtocolResponseAccumulator,
    plan: &crate::graphql::projection_delta::runtime::ModeledProjectionStatusPlan,
) -> Result<
    (
        CausalCommandPublicState,
        Vec<CausalCommandProjectionEvidence>,
    ),
    CausalDispatchError,
>
where
    R: crate::microsvc::dependencies::CausalHostProjections + Send + Sync,
{
    let request = ProjectionCausationEvidenceRequest::new(
        receipt.causation_id.clone(),
        plan.topologies.clone(),
    )
    .map_err(|error| {
        CausalDispatchError::Internal(format!(
            "stored modeled projection causation is invalid: {error}"
        ))
    })?;
    let batch = repository
        .command_causation_evidence(&request)
        .await
        .map_err(|error| {
            CausalDispatchError::Internal(format!(
                "modeled projection causation evidence lookup failed: {error}"
            ))
        })?;
    let modeled = protocol
        .modeled_projection_evidence(
            &receipt.command_name,
            &receipt.causation_id,
            metadata,
            &batch,
            plan.disposition,
        )
        .map_err(|error| {
            CausalDispatchError::Internal(format!(
                "modeled projection evidence authority rejected durable candidates: {error}"
            ))
        })?;
    if modeled.len() != metadata.obligations.len() {
        return Err(CausalDispatchError::Internal(format!(
            "modeled projection evidence returned {} items for {} opaque obligations",
            modeled.len(),
            metadata.obligations.len()
        )));
    }
    let evidence = modeled
        .into_iter()
        .enumerate()
        .map(|(obligation_index, item)| Ok(match item {
            crate::graphql::projection_delta::runtime::ModeledProjectionEvidence::Pending => {
                CausalCommandProjectionEvidence {
                    obligation_index,
                    state: CausalProjectionEvidenceState::Pending,
                    incarnation: None,
                    revision: None,
                }
            }
            crate::graphql::projection_delta::runtime::ModeledProjectionEvidence::TerminalFailure => {
                CausalCommandProjectionEvidence {
                    obligation_index,
                    state: CausalProjectionEvidenceState::TerminalFailure,
                    incarnation: None,
                    revision: None,
                }
            }
            crate::graphql::projection_delta::runtime::ModeledProjectionEvidence::Observed(
                observation,
            ) => {
                let (incarnation, revision) = match observation.revision.as_ref() {
                    Some(record)
                        if observation.kind == ProjectionObservationKind::Record
                            && record.scope() == &observation.scope =>
                    {
                        (Some(record.incarnation()), Some(record.revision()))
                    }
                    None if observation.kind == ProjectionObservationKind::Dependency => {
                        (None, None)
                    }
                    _ => {
                        return Err(CausalDispatchError::Internal(
                            "modeled projection store returned invalid observation revision"
                                .into(),
                        ));
                    }
                };
                CausalCommandProjectionEvidence {
                    obligation_index,
                    state: CausalProjectionEvidenceState::Observed,
                    incarnation,
                    revision,
                }
            }
        }))
        .collect::<Result<Vec<_>, _>>()?;
    let state = collapse_projection_evidence(&evidence);
    Ok((state, evidence))
}

#[cfg(feature = "graphql")]
pub(super) fn collapse_projection_evidence(
    evidence: &[CausalCommandProjectionEvidence],
) -> CausalCommandPublicState {
    if evidence
        .iter()
        .any(|item| item.state == CausalProjectionEvidenceState::TerminalFailure)
    {
        CausalCommandPublicState::ProjectionFailed
    } else if !evidence.is_empty()
        && evidence
            .iter()
            .all(|item| item.state == CausalProjectionEvidenceState::Observed)
    {
        CausalCommandPublicState::Atomic
    } else {
        CausalCommandPublicState::SucceededPendingProjection
    }
}
