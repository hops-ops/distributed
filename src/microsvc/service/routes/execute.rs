use super::super::causal::{
    abandon_attempt, ensure_causal_grant, handler_error_code, internal_ledger_error,
    rejection_outcome, replay_result, resolve_ambiguous_lookup, CausalDispatchError,
    CausalDispatchResult, CommandIngress, ExternalCausalAttempt, ExternalCausalReservation,
    ModeledProjectionMetadata, SealModeledProjection,
};
#[cfg(feature = "graphql")]
use super::super::causal::{evaluate_causal_command_status, CausalCommandPublicStatus};
use super::super::handlers::{CausalCommandContext, CausalGuardFn, PreparedHandlerFn};
use super::erased::{
    CausalCommandPolicy, CausalHandlerFuture, ErasedCausalHandler, ExternalAbandonFuture,
    ExternalCompletionFuture, ExternalReservationFuture, LedgerCompaction,
};
#[cfg(feature = "graphql")]
use super::erased::{CausalStatusFuture, CausalStatusRoute};
use crate::aggregate::Aggregate;
use crate::bus::{Message, MessageKind};
use crate::command::input::canonicalize_command_input;
use crate::command::{CommandConsistency, CommandOutcome, TypedCommandContract};
use crate::command_dispatch::VerifiedPrincipal;
use crate::command_ledger::{
    CanonicalInputHash, CausalCommitBatch, CommandAttempt, CommandContractFingerprint,
    CommandLedgerError, ReservationOutcome, TerminalCommandState,
};
use crate::command_ledger::{
    CausalRepositoryIdentity, CausalTransactionalCommit, CommandLedgerStore,
};
use crate::command_ledger::{
    CausalStorageIdentity, CommandId, CommandLedgerKey, CommandLookup, CommandLookupScope,
    CommandReservation, ExternalDispatchBinding, PrincipalPartitionId,
};
use crate::microsvc::causal::CausalWorkspace;
use crate::microsvc::cell_host::CellCommandIdentity;
#[cfg(feature = "graphql")]
use crate::microsvc::dependencies::CausalHostProjections;
use crate::microsvc::dependencies::{CausalRepositoryBackend, CausalRouteDependencies};
use crate::microsvc::error::HandlerError;
use crate::microsvc::session::Session;
use crate::outbox::OutboxPublisherConfig;
use crate::repository::{CommitBatch, StreamIdentity, TransactionalCommit};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub(super) struct RegisteredCausalHandler<A, I, K>
where
    A: Aggregate + Send + Sync + 'static,
    K: CommandOutcome,
{
    contract: TypedCommandContract,
    /// `contract.fingerprint_bytes()`, computed whenever the contract is set:
    /// at registration and in `bind_direct_projection`, its only mutation.
    fingerprint: CommandContractFingerprint,
    guard: Option<Arc<CausalGuardFn<A>>>,
    handle: Arc<PreparedHandlerFn<A, I, K>>,
    /// The route declared [`Capability::Clock`](crate::schedule::Capability):
    /// the dispatcher stamps the host clock into its session (`ctx.now()`).
    requires_clock: bool,
    /// The route declared [`Capability::Scheduling`](crate::schedule::Capability):
    /// only then may its handler stage schedule writes.
    requires_scheduling: bool,
    /// Retryable, fail-closed bootstrap for the bound projector's complete
    /// model/table ownership inventory. `get_or_try_init` leaves the cell empty
    /// after a transient registration failure.
    #[cfg(feature = "graphql")]
    direct_projection_bootstrap: tokio::sync::OnceCell<()>,
    _types: std::marker::PhantomData<fn(A, I) -> K>,
}

impl<A, I, K> RegisteredCausalHandler<A, I, K>
where
    A: Aggregate + Send + Sync + 'static,
    K: CommandOutcome,
{
    pub(super) fn new(
        contract: TypedCommandContract,
        guard: Option<Arc<CausalGuardFn<A>>>,
        handle: Arc<PreparedHandlerFn<A, I, K>>,
        requires_clock: bool,
        requires_scheduling: bool,
    ) -> Self {
        Self {
            fingerprint: CommandContractFingerprint::new(contract.fingerprint_bytes()),
            contract,
            guard,
            handle,
            requires_clock,
            requires_scheduling,
            #[cfg(feature = "graphql")]
            direct_projection_bootstrap: tokio::sync::OnceCell::new(),
            _types: std::marker::PhantomData,
        }
    }
}

/// The session a command runs with: any caller-supplied host-clock claim
/// removed, the backend's clock stamped when the route declared
/// [`Capability::Clock`](crate::schedule::Capability::Clock).
async fn stamped_session<R>(
    requires_clock: bool,
    repository: &R,
    mut session: Session,
) -> Result<Session, crate::RepositoryError>
where
    R: crate::microsvc::dependencies::BackendHost,
{
    let now = if requires_clock {
        Some(repository.host_now().await?)
    } else {
        None
    };
    crate::schedule::stamp_host_now(&mut session, now);
    Ok(session)
}

/// Why a reserved attempt stops before its commit.
enum Failure {
    /// The command was refused: record a terminal rejection every retry
    /// replays.
    Reject {
        code: &'static str,
        status: u16,
        message: String,
    },
    /// Nothing durable was produced: release the attempt so a retry runs.
    Abandon(String),
}

impl Failure {
    fn abandon(error: impl std::fmt::Display) -> Self {
        Self::Abandon(error.to_string())
    }
}

impl From<HandlerError> for Failure {
    fn from(error: HandlerError) -> Self {
        if error.status_code() < 500 {
            Self::Reject {
                code: handler_error_code(&error),
                status: error.status_code(),
                message: error.client_facing_message(),
            }
        } else {
            Self::abandon(error)
        }
    }
}

impl From<Failure> for CausalDispatchError {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Reject {
                code,
                status,
                message,
            } => Self::Rejected {
                code,
                status,
                message,
            },
            Failure::Abandon(detail) => Self::Internal(detail),
        }
    }
}

/// A handled command, validated and ready to commit under its attempt.
struct StagedCommand<'a, A, K: CommandOutcome> {
    parts: crate::microsvc::causal::CausalWorkspaceParts<A>,
    prepared: crate::command::PreparedCommand<K>,
    state: TerminalCommandState,
    modeled_projection: Option<ModeledProjectionMetadata>,
    direct_projection: Option<crate::projection_protocol::SameTransactionProjectionBatch>,
    /// A service hands the rows it commits to its outbox publisher.
    publisher: Option<&'a OutboxPublisherConfig>,
    outbox_ids: Vec<String>,
}

impl<A, K> StagedCommand<'_, A, K>
where
    A: Aggregate + Send + Sync + 'static,
    K: CommandOutcome,
{
    /// The one transaction that settles `attempt`. An error is the detail to
    /// abandon the attempt with.
    fn commit_batch<'b>(
        &'b mut self,
        ingress: &CommandIngress<'_>,
        attempt: &'b CommandAttempt,
        retention: Duration,
    ) -> Result<CausalCommitBatch<'b>, String> {
        let payload = self.prepared.serialized_payload().clone();
        let batch = self
            .parts
            .prepare_commit_batch()
            .map_err(|error| format!("causal commit batch preparation failed: {error}"))?;
        let outcome = match ingress {
            CommandIngress::Service { .. } => {
                // Rows are committed pending; the publisher claims them by ID.
                if self.publisher.is_some() {
                    self.outbox_ids.extend(
                        batch
                            .outbox_messages
                            .iter()
                            .map(|message| message.id().to_string()),
                    );
                }
                payload
            }
            CommandIngress::Cell { shard, .. } => {
                crate::microsvc::cell_host::validate_cell_projection_events(&batch.outbox_messages)
                    .map_err(|error| {
                        format!("cell projection events violate response bounds: {error}")
                    })?;
                if let Some(foreign) = batch
                    .streams
                    .iter()
                    .find(|stream| stream.identity != **shard)
                {
                    return Err(format!(
                        "cell `{shard}` cannot commit stream `{}`",
                        foreign.identity
                    ));
                }
                serde_json::to_value(crate::microsvc::cell_host::causal::CellCommandReplay {
                    payload,
                    events: batch
                        .outbox_messages
                        .iter()
                        .map(crate::microsvc::cell_host::CellProjectionEventWireItem::from_message)
                        .collect(),
                })
                .map_err(|error| format!("cell command replay could not be encoded: {error}"))?
            }
        };
        let completion = match self.modeled_projection.take() {
            Some(modeled) => attempt.complete_with_projection_metadata_until(
                self.state,
                outcome,
                modeled.canonical_bytes,
                retention,
                modeled.expires_at,
            ),
            None => attempt.complete(self.state, outcome, retention),
        }
        .map_err(|error| error.to_string())?;
        Ok(match self.direct_projection.take() {
            Some(direct_projection) => {
                CausalCommitBatch::with_direct_projection(batch, completion, direct_projection)
            }
            None => CausalCommitBatch::new(batch, completion),
        })
    }
}

impl<A, I, K> RegisteredCausalHandler<A, I, K>
where
    A: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + 'static,
    K: CommandOutcome,
{
    /// The typed command pipeline, for every ingress: admit, reserve the
    /// command ID in the ledger, run the handler, and commit its events with
    /// the terminal receipt in one transaction.
    ///
    /// Once the reservation is acquired every exit settles the attempt: a
    /// refusal commits a terminal rejection, any other failure before the
    /// commit abandons the attempt, and a commit that did not report success
    /// is resolved from the ledger.
    ///
    /// A commit that lost an optimistic stream-version race rolled back, so
    /// the attempt is still this caller's: the handler runs again on the
    /// reloaded aggregate, a bounded number of times.
    async fn execute_typed_command<D>(
        &self,
        dependencies: &D,
        ingress: CommandIngress<'_>,
        input: Value,
        session: Session,
    ) -> Result<CausalDispatchResult, CausalDispatchError>
    where
        D: CausalRouteDependencies<Aggregate = A> + Send + Sync + 'static,
        D::Backend: TransactionalCommit,
    {
        let consistency = self.contract.consistency;
        ingress.admit(&self.contract, &session)?;

        let canonical = canonicalize_command_input(&self.contract.input, input)
            .map_err(|error| CausalDispatchError::BadRequest(error.to_string()))?;
        let typed = canonical
            .decode::<I>()
            .map_err(|error| CausalDispatchError::BadRequest(error.to_string()))?;
        let (input, wire, payload, input_digest) = typed.into_parts();
        let direct_projection_target = self
            .direct_projection_target(&ingress, &wire, &session)
            .map_err(CausalDispatchError::Internal)?;
        let reservation = ingress.reservation(
            &self.contract.name,
            self.fingerprint,
            CanonicalInputHash::new(input_digest),
        )?;

        let aggregate_repository = dependencies.__causal_aggregate_repository();
        let repository = aggregate_repository.repo();
        let session = stamped_session(self.requires_clock, repository, session)
            .await
            .map_err(|error| CausalDispatchError::Internal(error.to_string()))?;
        #[cfg(feature = "graphql")]
        if let Some(target) = direct_projection_target.as_ref() {
            let (topology, ownership) = target.registration();
            self.direct_projection_bootstrap
                .get_or_try_init(|| async {
                    repository
                        .__register_direct_projection_models(topology, ownership)
                        .await
                        .map_err(|error| {
                            CausalDispatchError::Internal(format!(
                                "direct projection ownership bootstrap failed: {error}"
                            ))
                        })
                })
                .await?;
        }
        let attempt = match repository
            .reserve_command(reservation)
            .await
            .map_err(internal_ledger_error)?
        {
            ReservationOutcome::Acquired(attempt) => attempt,
            ReservationOutcome::InProgress { .. } => return Err(CausalDispatchError::InProgress),
            ReservationOutcome::Replay(replay) => return ingress.replay(consistency, replay),
            ReservationOutcome::Conflict => return Err(CausalDispatchError::CommandIdReuse),
            ReservationOutcome::Expired => return Err(CausalDispatchError::Expired),
        };

        let fence = attempt.fence();
        let retention = ingress.policy().replay_retention;
        let causation_id = attempt.causation_id();
        let mut metadata = session
            .variables()
            .iter()
            .filter(|(name, _)| !name.eq_ignore_ascii_case(crate::trace_context::CAUSATION_ID))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        metadata.push((
            crate::trace_context::CAUSATION_ID.to_string(),
            causation_id.as_str().to_string(),
        ));
        let message = Message {
            id: Some(attempt.key().command_id().to_string()),
            name: self.contract.name.clone(),
            kind: MessageKind::Command,
            payload,
            content_type: "application/json".into(),
            metadata,
        };

        // A run consumes its decoded input and projection target. Both are
        // pure functions of the canonical input, so a rerun derives them again.
        let mut decoded = Some((input, direct_projection_target));
        let mut runs = 0;
        let (staged, replay) = loop {
            runs += 1;
            let decoded = match decoded.take() {
                Some(decoded) => Ok(decoded),
                None => serde_json::from_value::<I>(wire.clone())
                    .map_err(Failure::abandon)
                    .and_then(|input| {
                        let target = self
                            .direct_projection_target(&ingress, &wire, &session)
                            .map_err(Failure::Abandon)?;
                        Ok((input, target))
                    }),
            };
            let mut staged = match decoded {
                Ok((input, direct_projection_target)) => {
                    self.stage_command(
                        &ingress,
                        aggregate_repository,
                        &message,
                        causation_id,
                        &session,
                        input,
                        direct_projection_target,
                    )
                    .await
                }
                Err(failure) => Err(failure),
            };
            let batch = match &mut staged {
                Ok(staged) => staged.commit_batch(&ingress, &attempt, retention),
                Err(Failure::Reject {
                    code,
                    status,
                    message,
                }) => attempt
                    .complete(
                        TerminalCommandState::Rejected,
                        rejection_outcome(code, *status, message),
                        retention,
                    )
                    .map(|completion| CausalCommitBatch::new(CommitBatch::empty(), completion))
                    .map_err(|error| error.to_string()),
                Err(Failure::Abandon(detail)) => Err(std::mem::take(detail)),
            };
            let batch = match batch {
                Ok(batch) => batch,
                Err(detail) => {
                    return abandon_attempt(
                        repository,
                        &ingress,
                        consistency,
                        fence,
                        CausalDispatchError::Internal(detail),
                    )
                    .await;
                }
            };
            match repository.commit_causal_batch(batch).await {
                Ok(completion) => {
                    let replay = completion.into_replay(&self.contract.name);
                    break (staged, replay);
                }
                // The only failure known to have rolled back.
                Err(CommandLedgerError::Storage(crate::RepositoryError::ConcurrentWrite {
                    ..
                })) => {
                    if runs < crate::repository::CONCURRENT_WRITE_RETRIES {
                        continue;
                    }
                    return abandon_attempt(
                        repository,
                        &ingress,
                        consistency,
                        fence,
                        CausalDispatchError::ConcurrentWrite,
                    )
                    .await;
                }
                Err(error) => {
                    return resolve_ambiguous_lookup(
                        repository,
                        &ingress,
                        consistency,
                        fence,
                        CausalDispatchError::Internal(error.to_string()),
                    )
                    .await;
                }
            }
        };

        // Committed: the attempt is terminal, as the rejection or the receipt.
        let mut staged = staged?;
        staged.parts.mark_committed_state().map_err(|error| {
            CausalDispatchError::Internal(format!(
                "committed causal workspace cleanup failed: {error}"
            ))
        })?;
        if let Some(publisher) = staged.publisher {
            publisher.publish_after_commit(staged.outbox_ids).await;
        }
        // The receipt is the committed completion itself, decoded exactly as
        // a later replay of this command ID decodes the stored row.
        ingress
            .replay(consistency, replay.map_err(internal_ledger_error)?)
            .map(CausalDispatchResult::fresh)
    }

    /// The projection row an `Atomic` service command owns for this input.
    fn direct_projection_target(
        &self,
        ingress: &CommandIngress<'_>,
        wire: &Value,
        session: &Session,
    ) -> Result<Option<crate::command::ResolvedDirectProjectionTarget>, String> {
        match ingress {
            CommandIngress::Service { .. } => self
                .contract
                .resolve_direct_projection_target_from_session(wire, Some(session))
                .map_err(|error| error.to_string()),
            CommandIngress::Cell { .. } => Ok(None),
        }
    }

    /// Run the guard and the handler on a fresh workspace and validate what
    /// they staged. Nothing is written: the caller commits or settles the
    /// failure.
    #[allow(clippy::too_many_arguments)]
    async fn stage_command<'a, R>(
        &self,
        ingress: &CommandIngress<'_>,
        aggregate_repository: &'a crate::aggregate::AggregateRepository<R, A>,
        message: &Message,
        causation_id: &crate::command_ledger::CausationId,
        session: &Session,
        input: I,
        direct_projection_target: Option<crate::command::ResolvedDirectProjectionTarget>,
    ) -> Result<StagedCommand<'a, A, K>, Failure>
    where
        R: CausalRepositoryBackend,
    {
        let workspace = CausalWorkspace::new(aggregate_repository)
            .with_scheduling_declared(self.requires_scheduling);
        let context = CausalCommandContext::new(message, session, &workspace);
        if self.guard.as_ref().is_some_and(|guard| !guard(&context)) {
            return Err(Failure::Reject {
                code: "REJECTED",
                status: 422,
                message: format!("guard rejected command: {}", self.contract.name),
            });
        }
        let mut prepared = (self.handle)(&context, input).await?;

        let mut parts = workspace.into_parts().map_err(Failure::abandon)?;
        parts
            .prepare_domain_publications(causation_id.as_str())
            .map_err(Failure::abandon)?;
        parts
            .validate_prepared(&self.contract, &mut prepared)
            .map_err(Failure::abandon)?;

        let (modeled_projection, direct_projection, publisher) = match ingress {
            CommandIngress::Service {
                policy,
                seal_modeled_projection,
                ..
            } => {
                let modeled_projection = match seal_modeled_projection {
                    Some(seal)
                        if self.contract.consistency != CommandConsistency::Atomic
                            && !self.contract.projections.selectors.is_empty() =>
                    {
                        let occurrences = parts
                            .prepared_domain_occurrences()
                            .map_err(Failure::abandon)?;
                        Some(
                            seal(
                                causation_id.clone(),
                                policy.replay_retention,
                                occurrences,
                                &self.contract.projections.selectors,
                            )
                            .map_err(Failure::Abandon)?,
                        )
                    }
                    _ => None,
                };
                let direct_projection = parts
                    .seal_direct_projection(
                        &prepared,
                        direct_projection_target,
                        causation_id.as_str(),
                    )
                    .map_err(Failure::abandon)?;
                (
                    modeled_projection,
                    direct_projection,
                    aggregate_repository.outbox_publisher(),
                )
            }
            // A cell seals no projections and its Queue owns delivery.
            CommandIngress::Cell { .. } => (None, None, None),
        };
        let pending_projection = modeled_projection.as_ref().map(|modeled| modeled.pending);
        let state = match (ingress, self.contract.consistency, pending_projection) {
            (CommandIngress::Cell { .. }, _, _) => TerminalCommandState::Succeeded,
            (_, CommandConsistency::Atomic, _) => TerminalCommandState::Atomic,
            (_, _, Some(false)) | (_, CommandConsistency::Succeeded, None) => {
                TerminalCommandState::Succeeded
            }
            (_, _, Some(true)) | (_, CommandConsistency::Eventual, None) => {
                TerminalCommandState::SucceededPendingProjection
            }
        };
        Ok(StagedCommand {
            parts,
            prepared,
            state,
            modeled_projection,
            direct_projection,
            publisher,
            outbox_ids: Vec::new(),
        })
    }
}

impl<D, A, I, K> ErasedCausalHandler<D> for RegisteredCausalHandler<A, I, K>
where
    D: CausalRouteDependencies<Aggregate = A> + Send + Sync + 'static,
    D::Backend: TransactionalCommit,
    A: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + 'static,
    K: CommandOutcome,
{
    fn contract(&self) -> &TypedCommandContract {
        &self.contract
    }

    fn dispatch_cell_causal<'a>(
        &'a self,
        dependencies: &'a D,
        identity: &'a CellCommandIdentity,
        input: Value,
        session: Session,
        shard: &'a StreamIdentity,
    ) -> CausalHandlerFuture<'a> {
        Box::pin(self.execute_typed_command(
            dependencies,
            CommandIngress::Cell { identity, shard },
            input,
            session,
        ))
    }

    #[cfg(feature = "graphql")]
    fn contract_fingerprint(&self) -> &CommandContractFingerprint {
        &self.fingerprint
    }

    #[cfg(feature = "graphql")]
    fn bind_direct_projection(
        &mut self,
        direct_projection: Option<crate::command::CommandDirectProjectionTarget>,
    ) {
        self.contract.direct_projection = direct_projection;
        self.fingerprint = CommandContractFingerprint::new(self.contract.fingerprint_bytes());
    }

    fn storage_identity(&self, dependencies: &D) -> CausalStorageIdentity {
        dependencies
            .__causal_aggregate_repository()
            .repo()
            .causal_storage_identity()
    }

    fn ledger_compaction(&self, dependencies: &D) -> LedgerCompaction {
        let repository = dependencies.__causal_aggregate_repository().repo().clone();
        Arc::new(move |limit| {
            let repository = repository.clone();
            Box::pin(async move { repository.compact_expired_commands(limit).await })
        })
    }

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
    ) -> CausalHandlerFuture<'a> {
        Box::pin(self.execute_typed_command(
            dependencies,
            CommandIngress::Service {
                service_id,
                principal_partition: principal.partition_for_service(service_id),
                command_id,
                policy,
                seal_modeled_projection,
            },
            input,
            session,
        ))
    }

    fn lookup<'a>(
        &'a self,
        dependencies: &'a D,
        service_id: &'a str,
        command_id: &'a str,
        session: &'a Session,
        principal: VerifiedPrincipal,
    ) -> Pin<Box<dyn Future<Output = Result<CommandLookup, CausalDispatchError>> + Send + 'a>> {
        Box::pin(async move {
            ensure_causal_grant(&self.contract, session)?;
            let command_id = CommandId::parse(command_id)
                .map_err(|error| CausalDispatchError::BadRequest(error.to_string()))?;
            let partition = PrincipalPartitionId::new(principal.partition_for_service(service_id))
                .map_err(internal_ledger_error)?;
            let key = CommandLedgerKey::new(service_id, partition, command_id)
                .map_err(internal_ledger_error)?;
            dependencies
                .__causal_aggregate_repository()
                .repo()
                .lookup_command(&key, CommandLookupScope::CommandName(&self.contract.name))
                .await
                .map_err(internal_ledger_error)
        })
    }

    #[cfg(feature = "graphql")]
    fn status<'a>(
        &'a self,
        dependencies: &'a D,
        key: &'a CommandLedgerKey,
        command_id: &'a CommandId,
        route: &'a CausalStatusRoute<'a>,
        protocol: Option<crate::graphql::protocol::ProtocolResponseAccumulator>,
    ) -> CausalStatusFuture<'a> {
        Box::pin(async move {
            let repository = dependencies.__causal_aggregate_repository().repo();
            // Status is deliberately non-enumerating: a row whose route the
            // caller may not see is answered as if no command had ever existed.
            let consistency = std::sync::OnceLock::new();
            let admits = |command: &str, fingerprint: &[u8; 32]| {
                route(command, fingerprint)
                    .map(|route| consistency.get_or_init(|| route))
                    .is_some()
            };
            let lookup = repository
                .lookup_command(key, CommandLookupScope::Route(&admits))
                .await
                .map_err(internal_ledger_error)?;
            let Some(consistency) = consistency.get() else {
                return Ok(CausalCommandPublicStatus::unknown(command_id.as_str()));
            };
            evaluate_causal_command_status(
                repository,
                command_id,
                *consistency,
                lookup,
                protocol.as_ref(),
            )
            .await
        })
    }

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
    ) -> ExternalReservationFuture<'a> {
        Box::pin(async move {
            ensure_causal_grant(&self.contract, &session)?;
            if self.contract.consistency == CommandConsistency::Atomic {
                return Err(CausalDispatchError::BadRequest(
                    "atomic typed commands cannot be dispatched to an aggregate cell".into(),
                ));
            }
            let canonical = canonicalize_command_input(&self.contract.input, input)
                .map_err(|error| CausalDispatchError::BadRequest(error.to_string()))?;
            let typed = canonical
                .decode::<I>()
                .map_err(|error| CausalDispatchError::BadRequest(error.to_string()))?;
            let (_, _, _, input_digest) = typed.into_parts();
            let command_id = CommandId::parse(command_id)
                .map_err(|error| CausalDispatchError::BadRequest(error.to_string()))?;
            let partition = PrincipalPartitionId::new(principal.partition_for_service(service_id))
                .map_err(internal_ledger_error)?;
            let key = CommandLedgerKey::new(service_id, partition, command_id)
                .map_err(internal_ledger_error)?;
            let reservation = CommandReservation::new(
                key,
                self.contract.name.clone(),
                self.fingerprint,
                CanonicalInputHash::new(input_digest),
                policy.attempt_lease,
                policy.replay_retention,
            )
            .map_err(internal_ledger_error)?
            .with_external_binding(binding);
            let repository = dependencies.__causal_aggregate_repository().repo();
            match repository
                .reserve_command(reservation)
                .await
                .map_err(internal_ledger_error)?
            {
                ReservationOutcome::Acquired(attempt) => {
                    Ok(ExternalCausalReservation::Acquired(ExternalCausalAttempt {
                        command_name: self.contract.name.clone(),
                        attempt,
                        retention: policy.replay_retention,
                    }))
                }
                ReservationOutcome::Replay(replay) => Ok(ExternalCausalReservation::Replay(
                    replay_result(self.contract.consistency, replay)?,
                )),
                ReservationOutcome::InProgress { .. } => Err(CausalDispatchError::InProgress),
                ReservationOutcome::Conflict => Err(CausalDispatchError::CommandIdReuse),
                ReservationOutcome::Expired => Err(CausalDispatchError::Expired),
            }
        })
    }

    fn complete_external<'a>(
        &'a self,
        dependencies: &'a D,
        attempt: ExternalCausalAttempt,
        result: &'a CausalDispatchResult,
    ) -> ExternalCompletionFuture<'a> {
        Box::pin(async move {
            if attempt.command_name != self.contract.name {
                return Err(CausalDispatchError::Internal(
                    "external command completion route does not match its reservation".into(),
                ));
            }
            let fence = attempt.fence();
            let completion = attempt.into_external_completion(result)?;
            let repository = dependencies.__causal_aggregate_repository().repo();
            match repository.complete_external_command(completion).await {
                Ok(()) => Ok(()),
                Err(error) => match repository
                    .lookup_command(&fence.key(), CommandLookupScope::Attempt(&fence))
                    .await
                {
                    Ok(CommandLookup::Replay(_)) => Ok(()),
                    Ok(CommandLookup::InProgress { .. }) => {
                        match repository.mark_retryable_unknown(fence).await {
                            Ok(()) => Err(CausalDispatchError::Internal(error.to_string())),
                            Err(recovery) => Err(CausalDispatchError::Internal(format!(
                                "{error}; external completion recovery failed: {recovery}"
                            ))),
                        }
                    }
                    Ok(CommandLookup::RetryableUnknown { .. }) => {
                        Err(CausalDispatchError::Internal(error.to_string()))
                    }
                    Ok(CommandLookup::Expired) => Err(CausalDispatchError::Expired),
                    Ok(CommandLookup::Unknown) => Err(CausalDispatchError::Internal(format!(
                        "{error}; external command ledger row disappeared"
                    ))),
                    Err(recovery) => Err(CausalDispatchError::Internal(format!(
                        "{error}; external completion lookup failed: {recovery}"
                    ))),
                },
            }
        })
    }

    fn abandon_external<'a>(
        &'a self,
        dependencies: &'a D,
        attempt: ExternalCausalAttempt,
    ) -> ExternalAbandonFuture<'a> {
        Box::pin(async move {
            crate::microsvc::service::causal::abandon_external_attempt(
                dependencies.__causal_aggregate_repository().repo(),
                attempt,
                "external cell dispatch did not produce a durable terminal receipt".into(),
            )
            .await
        })
    }

    fn invoke_cell<'a>(
        &'a self,
        dependencies: &'a D,
        input: Value,
        session: Session,
        shard: &'a StreamIdentity,
    ) -> Pin<Box<dyn Future<Output = Result<Value, HandlerError>> + Send + 'a>> {
        Box::pin(async move {
            let payload = serde_json::to_vec(&input)
                .map_err(|error| HandlerError::DecodeFailed(error.to_string()))?;
            let typed: I = serde_json::from_value(input)
                .map_err(|error| HandlerError::DecodeFailed(error.to_string()))?;
            let message = Message::new(self.contract.name.clone(), MessageKind::Command, payload);
            let aggregate_repository = dependencies.__causal_aggregate_repository();
            let session =
                stamped_session(self.requires_clock, aggregate_repository.repo(), session)
                    .await
                    .map_err(HandlerError::Repository)?;
            let workspace = CausalWorkspace::new(aggregate_repository)
                .with_scheduling_declared(self.requires_scheduling);
            let context = CausalCommandContext::new(&message, &session, &workspace);
            if self.guard.as_ref().is_some_and(|guard| !guard(&context)) {
                return Err(HandlerError::GuardRejected(self.contract.name.clone()));
            }
            let mut prepared = (self.handle)(&context, typed).await?;
            let mut parts = workspace
                .into_parts()
                .map_err(super::super::handlers::workspace_handler_error)?;
            let causation = uuid::Uuid::now_v7().hyphenated().to_string();
            parts
                .prepare_domain_publications(&causation)
                .map_err(super::super::handlers::workspace_handler_error)?;
            parts
                .validate_prepared(&self.contract, &mut prepared)
                .map_err(|error| HandlerError::Rejected(error.to_string()))?;
            {
                let batch = parts
                    .prepare_commit_batch()
                    .map_err(super::super::handlers::workspace_handler_error)?;
                for stream in &batch.streams {
                    if stream.identity != *shard {
                        return Err(HandlerError::Rejected(format!(
                            "cell `{shard}` cannot commit stream `{}`",
                            stream.identity
                        )));
                    }
                }
                TransactionalCommit::commit_batch(aggregate_repository.repo(), batch)
                    .await
                    .map_err(HandlerError::from)?;
            }
            parts
                .mark_committed_state()
                .map_err(super::super::handlers::workspace_handler_error)?;
            Ok(prepared.serialized_payload().clone())
        })
    }
}
