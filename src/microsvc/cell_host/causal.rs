//! What an aggregate cell adds to the typed command pipeline: its trusted
//! command-ledger identity and the receipt it stores as the ledger outcome.
//!
//! The pipeline itself (`execute_typed_command`) is shared with the service
//! ingress and free of the `graphql` Cargo feature, so workers-rs cells use
//! the exact same fenced command ledger without pulling an HTTP server
//! runtime into wasm.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::CellProjectionEventWireItem;
use crate::command_ledger::{
    CommandId, CommandLedgerError, CommandLedgerKey, PrincipalPartitionId,
};
use crate::microsvc::{CausalDispatchError, CausalDispatchResult};

/// Bounded retry material, owned by the command ledger and expired with its
/// replay retention. It is not an outbox record or an event history archive.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CellCommandReplay {
    pub payload: Value,
    pub events: Vec<CellProjectionEventWireItem>,
}

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
    ) -> Result<Self, CausalDispatchError> {
        let command_id = CommandId::parse(command_id).map_err(invalid_identity)?;
        let principal_partition =
            PrincipalPartitionId::new(principal_partition).map_err(invalid_identity)?;
        let key = CommandLedgerKey::new(service_id, principal_partition, command_id)
            .map_err(invalid_identity)?;
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
    ) -> Result<Self, CausalDispatchError> {
        let causation_id =
            crate::command_ledger::CausationId::parse_stored(causation_id.as_ref().to_string())
                .map_err(invalid_identity)?;
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

/// A caller-supplied identity the ledger refuses is the caller's mistake.
fn invalid_identity(error: CommandLedgerError) -> CausalDispatchError {
    match error {
        CommandLedgerError::Invalid(message) => CausalDispatchError::BadRequest(message),
        other => CausalDispatchError::Internal(other.to_string()),
    }
}

/// Unpack the receipt a cell stores as its ledger outcome: the handler
/// payload and the domain events committed with it.
pub(crate) fn open_cell_receipt(
    mut result: CausalDispatchResult,
) -> Result<CausalDispatchResult, CausalDispatchError> {
    let receipt: CellCommandReplay = serde_json::from_value(std::mem::take(&mut result.payload))
        .map_err(|error| {
            CausalDispatchError::Internal(format!("invalid cell command replay: {error}"))
        })?;
    // Validate persisted data just as strictly as the initial response.
    super::parse_cell_projection_events(&serde_json::json!({ "events": receipt.events }))
        .map_err(CausalDispatchError::Internal)?;
    result.receipt.outcome = receipt.payload.clone();
    result.payload = receipt.payload;
    result.projection_events = receipt.events;
    Ok(result)
}
