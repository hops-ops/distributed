#[cfg(feature = "graphql")]
use super::super::causal::{
    CausalCommandPublicStatus, ExternalCausalAttempt, ExternalCausalReservation,
};
use super::super::causal::{CausalDispatchError, CausalDispatchResult, SealModeledProjection};
use super::super::handlers::{HandlerFuture, ProjectorBootstrapFuture};
use super::outbox_wiring::DynBusPublisher;
use super::spec::HandlerSpec;
use crate::bus::{Message, OrderedDelivery};
#[cfg(feature = "graphql")]
use crate::command::CommandConsistency;
use crate::command::TypedCommandContract;
use crate::command_dispatch::VerifiedPrincipal;
use crate::command_ledger::CommandLookup;
#[cfg(any(feature = "graphql", feature = "runtime-tokio"))]
use crate::command_ledger::{CausalStorageIdentity, CommandLedgerError};
#[cfg(feature = "graphql")]
use crate::command_ledger::{
    CommandContractFingerprint, CommandId, CommandLedgerKey, ExternalDispatchBinding,
};
use crate::microsvc::cell_host::CellCommandIdentity;
use crate::microsvc::projector::{
    ProjectionRepairHandle, ProjectorRegistration, ProjectorRepairFuture,
    ProjectorRepairLookupFuture,
};
use crate::microsvc::session::Session;
use crate::repository::StreamIdentity;
use serde_json::Value;
#[cfg(feature = "graphql")]
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
#[cfg(any(feature = "graphql", feature = "runtime-tokio"))]
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy)]
pub(in crate::microsvc::service) struct CausalCommandPolicy {
    pub(in crate::microsvc::service) attempt_lease: Duration,
    pub(in crate::microsvc::service) replay_retention: Duration,
}

impl Default for CausalCommandPolicy {
    fn default() -> Self {
        Self {
            attempt_lease: Duration::from_secs(30),
            replay_retention: Duration::from_secs(30 * 24 * 60 * 60),
        }
    }
}

pub(in crate::microsvc::service) type CausalHandlerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CausalDispatchResult, CausalDispatchError>> + Send + 'a>>;

#[cfg(feature = "graphql")]
pub(in crate::microsvc::service) type CausalStatusFuture<'a> = Pin<
    Box<dyn Future<Output = Result<CausalCommandPublicStatus, CausalDispatchError>> + Send + 'a>,
>;

/// The consistency of the route a caller may read a ledger row through,
/// given the command name and contract fingerprint the row stores.
#[cfg(feature = "graphql")]
pub(in crate::microsvc::service) type CausalStatusRoute<'a> =
    dyn Fn(&str, &[u8; 32]) -> Option<CommandConsistency> + Send + Sync + 'a;

#[cfg(feature = "graphql")]
pub(in crate::microsvc::service) type ExternalReservationFuture<'a> = Pin<
    Box<dyn Future<Output = Result<ExternalCausalReservation, CausalDispatchError>> + Send + 'a>,
>;

#[cfg(feature = "graphql")]
pub(in crate::microsvc::service) type ExternalCompletionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), CausalDispatchError>> + Send + 'a>>;

#[cfg(feature = "graphql")]
pub(in crate::microsvc::service) type ExternalAbandonFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), CausalDispatchError>> + Send + 'a>>;

/// One bounded compaction pass over a command ledger, owning its repository
/// handle so a background worker can run it.
#[cfg(any(feature = "graphql", feature = "runtime-tokio"))]
pub(crate) type LedgerCompaction = Arc<
    dyn Fn(usize) -> Pin<Box<dyn Future<Output = Result<u64, CommandLedgerError>> + Send>>
        + Send
        + Sync,
>;

/// A command's declared shard over its wire input: the input is validated
/// and decoded as the command's own input type first.
#[cfg(feature = "graphql")]
pub(crate) type ErasedCommandShard =
    Arc<dyn Fn(&Value) -> Result<String, CausalDispatchError> + Send + Sync>;

pub(in crate::microsvc::service) trait ErasedCausalHandler<D>:
    Send + Sync
{
    fn contract(&self) -> &TypedCommandContract;

    /// The aggregate type the command commits to.
    #[cfg(feature = "graphql")]
    fn aggregate(&self) -> std::any::TypeId;

    /// The declared shard, when the command declares one.
    #[cfg(feature = "graphql")]
    fn shard(&self) -> Option<ErasedCommandShard>;

    /// The ledger fingerprint of [`Self::contract`].
    #[cfg(feature = "graphql")]
    fn contract_fingerprint(&self) -> &CommandContractFingerprint;

    /// Bind the Surface-resolved owner of an `Atomic` command's projection.
    /// The only way a registered contract changes; on error it is unchanged.
    #[cfg(feature = "graphql")]
    fn bind_direct_projection(
        &mut self,
        direct_projection: Option<crate::command::CommandDirectProjectionTarget>,
    ) -> Result<(), String>;

    #[cfg(any(feature = "graphql", feature = "runtime-tokio"))]
    fn storage_identity(&self, dependencies: &D) -> CausalStorageIdentity;

    #[cfg(any(feature = "graphql", feature = "runtime-tokio"))]
    fn ledger_compaction(&self, dependencies: &D) -> LedgerCompaction;

    #[allow(clippy::too_many_arguments)]
    fn dispatch<'a>(
        &'a self,
        dependencies: &'a D,
        service_id: &'a str,
        command_id: &'a str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        policy: CausalCommandPolicy,
        seal_modeled_projection: Option<&'a SealModeledProjection<'a>>,
    ) -> CausalHandlerFuture<'a>;

    fn dispatch_cell_causal<'a>(
        &'a self,
        dependencies: &'a D,
        identity: &'a CellCommandIdentity,
        input: Value,
        session: Session,
        shard: &'a StreamIdentity,
    ) -> CausalHandlerFuture<'a>;

    #[allow(dead_code)]
    fn lookup<'a>(
        &'a self,
        dependencies: &'a D,
        service_id: &'a str,
        command_id: &'a str,
        session: &'a Session,
        principal: VerifiedPrincipal,
    ) -> Pin<Box<dyn Future<Output = Result<CommandLookup, CausalDispatchError>> + Send + 'a>>;

    /// The status of the one ledger row under `key` in this handler's
    /// repository, whichever route wrote it: `route` names the route that
    /// may see a row.
    #[cfg(feature = "graphql")]
    fn status<'a>(
        &'a self,
        dependencies: &'a D,
        key: &'a CommandLedgerKey,
        command_id: &'a CommandId,
        route: &'a CausalStatusRoute<'a>,
        protocol: Option<crate::graphql::protocol::ProtocolResponseAccumulator>,
    ) -> CausalStatusFuture<'a>;

    #[cfg(feature = "graphql")]
    fn reserve_external<'a>(
        &'a self,
        dependencies: &'a D,
        service_id: &'a str,
        command_id: &'a str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        policy: CausalCommandPolicy,
        binding: ExternalDispatchBinding,
    ) -> ExternalReservationFuture<'a>;

    #[cfg(feature = "graphql")]
    fn complete_external<'a>(
        &'a self,
        dependencies: &'a D,
        attempt: ExternalCausalAttempt,
        result: &'a CausalDispatchResult,
    ) -> ExternalCompletionFuture<'a>;

    #[cfg(feature = "graphql")]
    fn abandon_external<'a>(
        &'a self,
        dependencies: &'a D,
        attempt: ExternalCausalAttempt,
    ) -> ExternalAbandonFuture<'a>;
}

pub(in crate::microsvc::service) trait ErasedRoutes:
    Send + Sync
{
    fn handler_specs(&self) -> &[HandlerSpec];

    fn typed_command_contracts(&self) -> Vec<&TypedCommandContract>;

    /// The contract of `command`, when this bundle registers it as a typed
    /// command.
    fn typed_command_contract(&self, command: &str) -> Option<&TypedCommandContract>;

    /// Every typed command of this bundle that commits to `aggregate`, with
    /// its declared shard.
    #[cfg(feature = "graphql")]
    fn aggregate_command_shards(
        &self,
        aggregate: std::any::TypeId,
    ) -> Vec<(&str, Option<ErasedCommandShard>)>;

    fn projector_registrations(&self) -> Vec<ProjectorRegistration>;

    fn modeled_local_services(&self) -> Vec<&str>;

    fn bootstrap_projectors(&self) -> ProjectorBootstrapFuture<'_>;

    fn is_causal_projector(&self, message: &Message) -> bool;

    fn repair_projection<'a>(
        &'a self,
        handle: &'a ProjectionRepairHandle,
    ) -> ProjectorRepairFuture<'a>;

    fn locates_projection_failure<'a>(
        &'a self,
        handle: &'a ProjectionRepairHandle,
    ) -> ProjectorRepairLookupFuture<'a>;

    #[cfg(feature = "graphql")]
    fn bind_typed_command_contracts(
        &mut self,
        contracts: &BTreeMap<String, TypedCommandContract>,
    ) -> Result<(), super::super::causal::GraphqlServiceBindError>;

    fn dispatch<'a>(
        &'a self,
        message: &'a Message,
        input: Value,
        session: Session,
        ordered: Option<&'a OrderedDelivery>,
    ) -> HandlerFuture<'a>;

    #[allow(clippy::too_many_arguments)]
    fn dispatch_causal<'a>(
        &'a self,
        command: &'a str,
        service_id: &'a str,
        command_id: &'a str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        policy: CausalCommandPolicy,
        seal_modeled_projection: Option<&'a SealModeledProjection<'a>>,
    ) -> CausalHandlerFuture<'a>;

    #[allow(dead_code)]
    fn lookup_causal<'a>(
        &'a self,
        command: &'a str,
        service_id: &'a str,
        command_id: &'a str,
        session: &'a Session,
        principal: VerifiedPrincipal,
    ) -> Pin<Box<dyn Future<Output = Result<CommandLookup, CausalDispatchError>> + Send + 'a>>;

    /// The repository every typed command of this bundle commits to.
    #[cfg(any(feature = "graphql", feature = "runtime-tokio"))]
    fn causal_storage_identity(&self) -> Option<CausalStorageIdentity>;

    /// Compaction of the ledger in that repository.
    #[cfg(any(feature = "graphql", feature = "runtime-tokio"))]
    fn ledger_compaction(&self) -> Option<LedgerCompaction>;

    /// The consistency of this bundle's `command`, if `session` is still
    /// granted it and its contract still has `fingerprint`.
    #[cfg(feature = "graphql")]
    fn causal_status_route(
        &self,
        command: &str,
        fingerprint: &[u8; 32],
        session: &Session,
    ) -> Option<CommandConsistency>;

    /// One ledger lookup of `key` in this bundle's repository.
    #[cfg(feature = "graphql")]
    fn causal_command_status<'a>(
        &'a self,
        key: &'a CommandLedgerKey,
        command_id: &'a CommandId,
        route: &'a CausalStatusRoute<'a>,
        protocol: Option<crate::graphql::protocol::ProtocolResponseAccumulator>,
    ) -> CausalStatusFuture<'a>;

    #[cfg(feature = "graphql")]
    fn reserve_external<'a>(
        &'a self,
        command: &'a str,
        service_id: &'a str,
        command_id: &'a str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        policy: CausalCommandPolicy,
        binding: ExternalDispatchBinding,
    ) -> ExternalReservationFuture<'a>;

    #[cfg(feature = "graphql")]
    fn complete_external<'a>(
        &'a self,
        command: &'a str,
        attempt: ExternalCausalAttempt,
        result: &'a CausalDispatchResult,
    ) -> ExternalCompletionFuture<'a>;

    #[cfg(feature = "graphql")]
    fn abandon_external<'a>(
        &'a self,
        command: &'a str,
        attempt: ExternalCausalAttempt,
    ) -> ExternalAbandonFuture<'a>;

    #[cfg(feature = "graphql")]
    fn projected_storage_identities(&self) -> Vec<crate::command_ledger::CausalStorageIdentity>;

    fn configure_outbox_publisher(
        &mut self,
        publisher: DynBusPublisher,
        worker_id: String,
        lease: Duration,
        max_attempts: u32,
        service_name: Option<String>,
    );
}
