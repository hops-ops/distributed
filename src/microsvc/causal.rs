//! Framework-owned staging for typed causal command handlers.
//!
//! A workspace deliberately exposes neither its repository nor a commit
//! operation. Handlers receive owned aggregate checkouts and may only return
//! them to this staging area. The service dispatcher validates the resulting
//! evidence, wraps its ordinary [`CommitBatch`] in the command-ledger fence,
//! and is the only code allowed to commit it.

#![cfg_attr(not(feature = "graphql"), allow(dead_code))]

use std::collections::HashSet;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Mutex;

use serde::Serialize;

use crate::aggregate::{hydrate, Aggregate, AggregateRepository};
use crate::command::{
    validate_resolved_direct_plan, Atomic, CommandCommitProofError, CommandOutcome,
    PrepareCommandError, PreparedCommand, ProjectionCommitProof, ResolvedDirectProjectionTarget,
    TypedCommandContract,
};
use crate::command_ledger::CausalGetStream;
use crate::domain_event::{DomainEventCaptureError, DomainEventCommitGuardError};
use crate::outbox::{OutboxMessage, PreparedDomainEvent};
use crate::projection::lower::{
    DirectCandidate, LoweredProjectionPlan, ProjectionDescriptor,
    ProjectionServerExecutorDescriptor,
};
use crate::projection_protocol::SameTransactionProjectionBatch;
use crate::read_model::{ReadModelWritePlanBuilder, RelationalReadModel};
use crate::repository::{CommitBatch, RepositoryError, SnapshotWrite, StreamIdentity, StreamWrite};
use crate::table::{TableStoreError, TableWritePlan};

type LoadAggregateFuture<'a, A> =
    Pin<Box<dyn Future<Output = Result<Option<A>, RepositoryError>> + Send + 'a>>;

/// Erases the concrete backend from the handler-facing workspace while
/// retaining the repository's explicitly non-locking causal load capability.
trait CausalAggregateStore<A>: Send + Sync {
    fn load<'a>(&'a self, identity: &'a StreamIdentity) -> LoadAggregateFuture<'a, A>;

    fn snapshot_writes(
        &self,
        aggregate: &A,
    ) -> Result<(Vec<SnapshotWrite>, Option<u64>), RepositoryError>;
}

struct AggregateStoreRef<'repo, R, A> {
    repository: &'repo AggregateRepository<R, A>,
}

impl<R, A> CausalAggregateStore<A> for AggregateStoreRef<'_, R, A>
where
    R: CausalGetStream,
    A: Aggregate + Send + Sync + 'static,
{
    fn load<'a>(&'a self, identity: &'a StreamIdentity) -> LoadAggregateFuture<'a, A> {
        Box::pin(async move {
            let Some(entity) = self.repository.repo().get_causal_stream(identity).await? else {
                return Ok(None);
            };
            hydrate::<A>(entity).map(Some)
        })
    }

    fn snapshot_writes(
        &self,
        aggregate: &A,
    ) -> Result<(Vec<SnapshotWrite>, Option<u64>), RepositoryError> {
        self.repository.snapshot_writes_for(aggregate)
    }
}

#[derive(Clone, Debug)]
enum CheckoutOrigin {
    New,
    Loaded {
        identity: StreamIdentity,
        committed_version: u64,
    },
}

/// An aggregate value checked out of a causal workspace.
///
/// The value is owned, so no repository or queue lock survives across handler
/// awaits. Its original stream identity and committed version remain private
/// and are checked when the handler stages it back into the workspace.
pub struct AggregateCheckout<A: Aggregate> {
    aggregate: A,
    origin: CheckoutOrigin,
}

impl<A: Aggregate> AggregateCheckout<A> {
    fn loaded(identity: StreamIdentity, aggregate: A) -> Self {
        let committed_version = aggregate.entity().committed_version();
        Self {
            aggregate,
            origin: CheckoutOrigin::Loaded {
                identity,
                committed_version,
            },
        }
    }

    fn new(aggregate: A) -> Self {
        Self {
            aggregate,
            origin: CheckoutOrigin::New,
        }
    }

    pub fn aggregate(&self) -> &A {
        &self.aggregate
    }

    pub fn aggregate_mut(&mut self) -> &mut A {
        &mut self.aggregate
    }
}

impl<A: Aggregate> Deref for AggregateCheckout<A> {
    type Target = A;

    fn deref(&self) -> &Self::Target {
        &self.aggregate
    }
}

impl<A: Aggregate> DerefMut for AggregateCheckout<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.aggregate
    }
}

/// A deterministic staging error caught before command commit I/O.
#[derive(Debug)]
pub(crate) enum CausalWorkspaceError {
    Repository(RepositoryError),
    Table(TableStoreError),
    DomainEventCapture(DomainEventCaptureError),
    DomainEventGuard(DomainEventCommitGuardError),
    Prepare(PrepareCommandError),
    Poisoned,
    IdentityChanged {
        original: StreamIdentity,
        current: StreamIdentity,
    },
    CommittedVersionChanged {
        identity: StreamIdentity,
        original: u64,
        current: u64,
    },
    DuplicateStream(StreamIdentity),
    ProjectionAlreadyStaged,
    ModeledDirectProjection(String),
    DomainPublicationRequired(StreamIdentity),
    DomainPublicationsAlreadyPrepared,
    DomainPublicationsNotPrepared,
    CommitBatchAlreadyPrepared,
}

impl std::fmt::Display for CausalWorkspaceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Repository(error) => write!(formatter, "causal aggregate load failed: {error}"),
            Self::Table(error) => write!(formatter, "causal read-model staging failed: {error}"),
            Self::DomainEventCapture(error) => {
                write!(formatter, "causal domain-event publication failed: {error}")
            }
            Self::DomainEventGuard(error) => {
                write!(formatter, "causal domain-event commit guard failed: {error}")
            }
            Self::Prepare(error) => write!(formatter, "causal result preparation failed: {error}"),
            Self::Poisoned => formatter.write_str("causal workspace lock poisoned"),
            Self::IdentityChanged { original, current } => write!(
                formatter,
                "checked-out aggregate identity changed from `{original}` to `{current}`"
            ),
            Self::CommittedVersionChanged {
                identity,
                original,
                current,
            } => write!(
                formatter,
                "checked-out aggregate `{identity}` changed committed version from {original} to {current}"
            ),
            Self::DuplicateStream(identity) => {
                write!(formatter, "aggregate stream `{identity}` was staged more than once")
            }
            Self::ProjectionAlreadyStaged => formatter.write_str(
                "a causal command may prepare only one same-transaction projected result",
            ),
            Self::ModeledDirectProjection(error) => {
                write!(formatter, "modeled direct projection is invalid: {error}")
            }
            Self::DomainPublicationRequired(identity) => write!(
                formatter,
                "aggregate `{identity}` captured domain events; add `publish_events()` to its commit"
            ),
            Self::DomainPublicationsAlreadyPrepared => {
                formatter.write_str("causal domain-event publications were already prepared")
            }
            Self::DomainPublicationsNotPrepared => {
                formatter.write_str("causal domain-event publications are not prepared")
            }
            Self::CommitBatchAlreadyPrepared => {
                formatter.write_str("causal workspace commit batch was already prepared")
            }
        }
    }
}

impl std::error::Error for CausalWorkspaceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Repository(error) => Some(error),
            Self::Table(error) => Some(error),
            Self::DomainEventCapture(error) => Some(error),
            Self::DomainEventGuard(error) => Some(error),
            Self::Prepare(error) => Some(error),
            Self::Poisoned
            | Self::IdentityChanged { .. }
            | Self::CommittedVersionChanged { .. }
            | Self::DuplicateStream(_)
            | Self::ProjectionAlreadyStaged
            | Self::ModeledDirectProjection(_)
            | Self::DomainPublicationRequired(_)
            | Self::DomainPublicationsAlreadyPrepared
            | Self::DomainPublicationsNotPrepared
            | Self::CommitBatchAlreadyPrepared => None,
        }
    }
}

impl From<RepositoryError> for CausalWorkspaceError {
    fn from(error: RepositoryError) -> Self {
        Self::Repository(error)
    }
}

impl From<TableStoreError> for CausalWorkspaceError {
    fn from(error: TableStoreError) -> Self {
        Self::Table(error)
    }
}

impl From<DomainEventCaptureError> for CausalWorkspaceError {
    fn from(error: DomainEventCaptureError) -> Self {
        Self::DomainEventCapture(error)
    }
}

impl From<DomainEventCommitGuardError> for CausalWorkspaceError {
    fn from(error: DomainEventCommitGuardError) -> Self {
        Self::DomainEventGuard(error)
    }
}

impl From<PrepareCommandError> for CausalWorkspaceError {
    fn from(error: PrepareCommandError) -> Self {
        Self::Prepare(error)
    }
}

struct StagedAggregate<A> {
    identity: StreamIdentity,
    aggregate: A,
    snapshot_version: Option<u64>,
    publication: AggregatePublication,
}

/// Publication work attached to one exact staged aggregate.
#[derive(Clone, Debug, Default)]
pub(crate) struct AggregatePublication {
    pub(crate) publish_captured_events: bool,
    pub(crate) explicit_events: Vec<PreparedDomainEvent>,
}

struct WorkspaceState<A> {
    aggregates: Vec<StagedAggregate<A>>,
    identities: HashSet<StreamIdentity>,
    outbox_messages: Vec<OutboxMessage>,
    read_model_plans: Vec<TableWritePlan>,
    snapshots: Vec<SnapshotWrite>,
    projection_staged: bool,
    modeled_direct_projection: Option<ProjectionServerExecutorDescriptor>,
}

impl<A> Default for WorkspaceState<A> {
    fn default() -> Self {
        Self {
            aggregates: Vec::new(),
            identities: HashSet::new(),
            outbox_messages: Vec::new(),
            read_model_plans: Vec::new(),
            snapshots: Vec::new(),
            projection_staged: false,
            modeled_direct_projection: None,
        }
    }
}

/// A handler-scoped unit of work. The concrete repository is erased and never
/// exposed through this type.
pub(crate) struct CausalWorkspace<'repo, A>
where
    A: Aggregate + Send + Sync + 'static,
{
    store: Box<dyn CausalAggregateStore<A> + 'repo>,
    state: Mutex<WorkspaceState<A>>,
}

impl<'repo, A> CausalWorkspace<'repo, A>
where
    A: Aggregate + Send + Sync + 'static,
{
    pub(crate) fn new<R>(repository: &'repo AggregateRepository<R, A>) -> Self
    where
        R: CausalGetStream + 'repo,
    {
        Self {
            store: Box::new(AggregateStoreRef { repository }),
            state: Mutex::new(WorkspaceState::default()),
        }
    }

    /// Load a full stream through the explicit causal/non-locking capability.
    pub(crate) async fn load(
        &self,
        id: &str,
    ) -> Result<Option<AggregateCheckout<A>>, CausalWorkspaceError> {
        let identity = StreamIdentity::new(A::aggregate_type(), id)?;
        Ok(self
            .store
            .load(&identity)
            .await?
            .map(|aggregate| AggregateCheckout::loaded(identity, aggregate)))
    }

    /// Create an empty owned checkout. The aggregate must establish a valid ID
    /// before it is staged.
    pub(crate) fn create(&self) -> AggregateCheckout<A> {
        AggregateCheckout::new(A::new_empty())
    }

    /// Return a checkout to the unit of work after validating the original
    /// identity/version fence and duplicate stream membership.
    #[cfg(test)]
    pub(crate) fn stage(&self, checkout: AggregateCheckout<A>) -> Result<(), CausalWorkspaceError> {
        self.stage_with_publication(checkout, AggregatePublication::default())
    }

    pub(crate) fn stage_with_publication(
        &self,
        checkout: AggregateCheckout<A>,
        publication: AggregatePublication,
    ) -> Result<(), CausalWorkspaceError> {
        let AggregateCheckout { aggregate, origin } = checkout;
        let identity = StreamIdentity::new(A::aggregate_type(), aggregate.entity().id())?;
        let committed_version = aggregate.entity().committed_version();

        match origin {
            CheckoutOrigin::New => {
                if committed_version != 0 {
                    return Err(CausalWorkspaceError::CommittedVersionChanged {
                        identity,
                        original: 0,
                        current: committed_version,
                    });
                }
            }
            CheckoutOrigin::Loaded {
                identity: original_identity,
                committed_version: original_version,
            } => {
                if identity != original_identity {
                    return Err(CausalWorkspaceError::IdentityChanged {
                        original: original_identity,
                        current: identity,
                    });
                }
                if committed_version != original_version {
                    return Err(CausalWorkspaceError::CommittedVersionChanged {
                        identity,
                        original: original_version,
                        current: committed_version,
                    });
                }
            }
        }

        let (mut snapshots, snapshot_version) = self.store.snapshot_writes(&aggregate)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| CausalWorkspaceError::Poisoned)?;
        if !state.identities.insert(identity.clone()) {
            return Err(CausalWorkspaceError::DuplicateStream(identity));
        }
        state.snapshots.append(&mut snapshots);
        state.aggregates.push(StagedAggregate {
            identity,
            aggregate,
            snapshot_version,
            publication,
        });
        Ok(())
    }

    pub(crate) fn stage_outbox(&self, message: OutboxMessage) -> Result<(), CausalWorkspaceError> {
        self.state
            .lock()
            .map_err(|_| CausalWorkspaceError::Poisoned)?
            .outbox_messages
            .push(message);
        Ok(())
    }

    pub(crate) fn stage_read_models(
        &self,
        builder: ReadModelWritePlanBuilder,
    ) -> Result<(), CausalWorkspaceError> {
        let plan = builder.into_write_plan()?;
        self.stage_read_model_plan(plan)
    }

    pub(crate) fn stage_read_model_plan(
        &self,
        plan: TableWritePlan,
    ) -> Result<(), CausalWorkspaceError> {
        plan.validate()?;
        self.state
            .lock()
            .map_err(|_| CausalWorkspaceError::Poisoned)?
            .read_model_plans
            .push(plan);
        Ok(())
    }

    /// Atomically stage the returned view as one full-row upsert and prepare a
    /// non-forgeable `Atomic<M>` completion tied to that exact row.
    pub(crate) fn prepare_atomic<M>(
        &self,
        model: M,
    ) -> Result<PreparedCommand<Atomic<M>>, CausalWorkspaceError>
    where
        M: RelationalReadModel + Serialize + Send + Sync + 'static,
    {
        let mut builder = ReadModelWritePlanBuilder::new();
        builder.upsert(&model)?;
        let plan = builder.into_write_plan()?;
        let proof = ProjectionCommitProof::for_model(&model)?;
        let prepared = PreparedCommand::prepare_atomic(model, proof)?;

        let mut state = self
            .state
            .lock()
            .map_err(|_| CausalWorkspaceError::Poisoned)?;
        if state.projection_staged {
            return Err(CausalWorkspaceError::ProjectionAlreadyStaged);
        }
        state.projection_staged = true;
        state.read_model_plans.push(plan);
        Ok(prepared)
    }

    /// Prepare a projected result whose exact row will be resolved from the
    /// authoritative domain-event occurrence after the dispatcher stamps its
    /// ledger causation.
    pub(crate) fn prepare_modeled_atomic<M>(
        &self,
        projection: ProjectionDescriptor<DirectCandidate>,
    ) -> Result<PreparedCommand<Atomic<M>>, CausalWorkspaceError>
    where
        M: RelationalReadModel + Serialize + Send + Sync + 'static,
    {
        let executor = projection
            .server_executor()
            .map_err(|error| CausalWorkspaceError::ModeledDirectProjection(error.to_string()))?;
        self.prepare_modeled_atomic_from_executor(executor)
    }

    /// Placement-selected direct projection: the service registration chose the
    /// executor; command code names neither a projection nor a selector.
    pub(crate) fn prepare_placement_selected_atomic<M>(
        &self,
    ) -> Result<PreparedCommand<Atomic<M>>, CausalWorkspaceError>
    where
        M: RelationalReadModel + Serialize + Send + Sync + 'static,
    {
        let schema = M::schema();
        let executor =
            crate::projection::lower::placement_selected_direct_for_model(&schema.model_name)
                .ok_or_else(|| {
                    CausalWorkspaceError::ModeledDirectProjection(format!(
                        "no placement-selected direct projection is registered for model `{}`",
                        schema.model_name
                    ))
                })?;
        self.prepare_modeled_atomic_from_executor(executor)
    }

    fn prepare_modeled_atomic_from_executor<M>(
        &self,
        executor: ProjectionServerExecutorDescriptor,
    ) -> Result<PreparedCommand<Atomic<M>>, CausalWorkspaceError>
    where
        M: RelationalReadModel + Serialize + Send + Sync + 'static,
    {
        let [output] = executor.outputs.models.as_slice() else {
            return Err(CausalWorkspaceError::ModeledDirectProjection(
                "a direct descriptor must own exactly one output model".into(),
            ));
        };
        let schema = M::schema();
        if !executor.outputs.relationships.is_empty()
            || output.model != schema.model_name
            || output.storage != schema.table_name
            || output.schema != *schema
        {
            return Err(CausalWorkspaceError::ModeledDirectProjection(format!(
                "projection `{}` output does not exactly match returned model `{}`/`{}`",
                executor.name, schema.model_name, schema.table_name
            )));
        }
        let prepared = PreparedCommand::prepare_modeled_atomic();

        let mut state = self
            .state
            .lock()
            .map_err(|_| CausalWorkspaceError::Poisoned)?;
        if state.projection_staged {
            return Err(CausalWorkspaceError::ProjectionAlreadyStaged);
        }
        state.projection_staged = true;
        state.modeled_direct_projection = Some(executor);
        Ok(prepared)
    }

    pub(crate) fn into_parts(self) -> Result<CausalWorkspaceParts<A>, CausalWorkspaceError> {
        let state = self
            .state
            .into_inner()
            .map_err(|_| CausalWorkspaceError::Poisoned)?;
        Ok(CausalWorkspaceParts {
            aggregates: state.aggregates,
            outbox_messages: state.outbox_messages,
            read_model_plans: state.read_model_plans,
            snapshots: state.snapshots,
            domain_event_occurrences: Vec::new(),
            unpublished_direct_occurrences: HashSet::new(),
            modeled_direct_projection: state.modeled_direct_projection,
            resolved_direct_projection: None,
            publications_prepared: false,
            batch_prepared: false,
        })
    }
}

/// Owned staged work retained by the dispatcher between handler completion and
/// construction of the ledger-fenced commit batch.
pub(crate) struct CausalWorkspaceParts<A> {
    aggregates: Vec<StagedAggregate<A>>,
    outbox_messages: Vec<OutboxMessage>,
    read_model_plans: Vec<TableWritePlan>,
    snapshots: Vec<SnapshotWrite>,
    /// Exact ledger-causation-stamped outward occurrences in publication order.
    domain_event_occurrences: Vec<crate::DomainEventOccurrence>,
    /// Captured occurrences consumed only by a direct modeled projection.
    /// Every member must be the one selected occurrence or validation fails;
    /// no unrelated outward event may disappear merely because the command is
    /// projected.
    unpublished_direct_occurrences: HashSet<String>,
    modeled_direct_projection: Option<ProjectionServerExecutorDescriptor>,
    resolved_direct_projection: Option<LoweredProjectionPlan>,
    publications_prepared: bool,
    batch_prepared: bool,
}

impl<A> CausalWorkspaceParts<A>
where
    A: Aggregate,
{
    /// Bind publication intent to each aggregate's exact final transition after
    /// applying the ledger's authoritative causation ID.
    pub(crate) fn prepare_domain_publications(
        &mut self,
        causation_id: &str,
    ) -> Result<(), CausalWorkspaceError> {
        if self.publications_prepared {
            return Err(CausalWorkspaceError::DomainPublicationsAlreadyPrepared);
        }
        self.publications_prepared = true;
        let has_modeled_direct_projection = self.modeled_direct_projection.is_some();

        for staged in &mut self.aggregates {
            staged
                .aggregate
                .entity_mut()
                .overwrite_new_event_causation_id(causation_id);
            let entity = staged.aggregate.entity();
            entity.domain_event_commit_guard()?;
            let pending = entity.pending_domain_events_for_commit()?;
            if !pending.is_empty()
                && !staged.publication.publish_captured_events
                && !has_modeled_direct_projection
            {
                return Err(CausalWorkspaceError::DomainPublicationRequired(
                    staged.identity.clone(),
                ));
            }

            if staged.publication.publish_captured_events || has_modeled_direct_projection {
                self.domain_event_occurrences
                    .extend(pending.iter().cloned());
            }
            if staged.publication.publish_captured_events {
                self.outbox_messages.extend(
                    pending
                        .iter()
                        .map(OutboxMessage::from_domain_event_occurrence)
                        .collect::<Result<Vec<_>, _>>()?,
                );
            } else if has_modeled_direct_projection {
                self.unpublished_direct_occurrences
                    .extend(pending.iter().map(|occurrence| occurrence.id().to_owned()));
            }

            let current_sequence = staged.aggregate.entity().version();
            let ordinal_base = pending
                .iter()
                .filter(|occurrence| occurrence.aggregate_sequence() == current_sequence)
                .count();
            for (offset, event) in staged.publication.explicit_events.iter().enumerate() {
                let ordinal: u32 = ordinal_base
                    .checked_add(offset)
                    .and_then(|ordinal| ordinal.try_into().ok())
                    .ok_or(DomainEventCaptureError::PublicationOrdinalOverflow)?;
                let occurrence = event.bind(&staged.aggregate, ordinal)?;
                self.domain_event_occurrences.push(occurrence.clone());
                self.outbox_messages
                    .push(OutboxMessage::from_domain_event_occurrence(&occurrence)?);
            }
        }
        Ok(())
    }

    /// Return the exact outward occurrence sequence after authoritative
    /// causation and publication ordinals have been sealed.
    pub(crate) fn prepared_domain_occurrences(
        &self,
    ) -> Result<&[crate::DomainEventOccurrence], CausalWorkspaceError> {
        if !self.publications_prepared {
            return Err(CausalWorkspaceError::DomainPublicationsNotPrepared);
        }
        Ok(&self.domain_event_occurrences)
    }

    pub(crate) fn validate_prepared<K: CommandOutcome>(
        &mut self,
        contract: &TypedCommandContract,
        prepared: &mut PreparedCommand<K>,
    ) -> Result<(), CommandCommitProofError> {
        self.resolve_modeled_direct_projection()?;
        prepared.materialize_modeled_projection(self.resolved_direct_projection.as_ref())?;
        prepared.validate_commit_evidence(
            contract,
            self.aggregates
                .iter()
                .any(|staged| !staged.aggregate.entity().new_events().is_empty()),
            &self.outbox_messages,
            &self.read_model_plans,
            self.resolved_direct_projection.as_ref(),
        )
    }

    /// Seal the proof-matched projected row into the repository's private
    /// direct-projection participant. This must run after evidence validation
    /// and before the remaining ordinary table plans become a commit batch.
    pub(crate) fn seal_direct_projection<K: CommandOutcome>(
        &mut self,
        prepared: &PreparedCommand<K>,
        target: Option<ResolvedDirectProjectionTarget>,
        causation_id: &str,
    ) -> Result<Option<SameTransactionProjectionBatch>, CommandCommitProofError> {
        if self.modeled_direct_projection.is_some() && self.resolved_direct_projection.is_none() {
            return Err(CommandCommitProofError::DirectProjection(
                "modeled direct projection was not resolved before sealing".into(),
            ));
        }
        if let Some(executor) = self.modeled_direct_projection.as_ref() {
            target
                .as_ref()
                .ok_or(CommandCommitProofError::MissingDirectProjectionTarget)?
                .validate_modeled_owner(executor.name, executor.epoch, executor.program_id)
                .map_err(|error| CommandCommitProofError::DirectProjection(error.to_string()))?;
        }
        prepared.seal_direct_projection(
            target,
            &mut self.read_model_plans,
            self.resolved_direct_projection.take(),
            causation_id,
        )
    }

    fn resolve_modeled_direct_projection(&mut self) -> Result<(), CommandCommitProofError> {
        let Some(executor) = self.modeled_direct_projection.as_ref() else {
            return Ok(());
        };
        if self.resolved_direct_projection.is_some() {
            return Ok(());
        }
        if !self.publications_prepared {
            return Err(CommandCommitProofError::DirectProjection(
                "modeled direct projection requires sealed domain-event occurrences".into(),
            ));
        }

        let mut matches = self
            .domain_event_occurrences
            .iter()
            .filter(|occurrence| executor.matches(occurrence));
        let occurrence = matches.next().ok_or_else(|| {
            CommandCommitProofError::DirectProjection(format!(
                "modeled direct projection `{}` matched no committed domain-event occurrence",
                executor.name
            ))
        })?;
        if matches.next().is_some() {
            return Err(CommandCommitProofError::DirectProjection(format!(
                "modeled direct projection `{}` matched more than one domain-event occurrence",
                executor.name
            )));
        }
        if self
            .unpublished_direct_occurrences
            .iter()
            .any(|occurrence_id| occurrence_id != occurrence.id())
        {
            return Err(CommandCommitProofError::DirectProjection(
                "a projected command captured an unrelated unpublished domain event".into(),
            ));
        }

        let lowered = executor.plan(occurrence).map_err(|error| {
            CommandCommitProofError::DirectProjection(format!(
                "modeled direct projection `{}` could not lower its selected occurrence: {error}",
                executor.name
            ))
        })?;
        validate_resolved_direct_plan(&lowered)?;
        self.resolved_direct_projection = Some(lowered);
        Ok(())
    }

    /// Borrow staged aggregates into the existing public commit-batch shape.
    /// This is one-shot because its owned participants move into the batch.
    pub(crate) fn prepare_commit_batch(&mut self) -> Result<CommitBatch<'_>, CausalWorkspaceError> {
        if self.batch_prepared {
            return Err(CausalWorkspaceError::CommitBatchAlreadyPrepared);
        }
        self.batch_prepared = true;

        // Preserve ordinary AggregateCommit source metadata for low-level
        // messages only when the source is unambiguous. Canonical domain-event
        // rows already carry their exact occurrence source and are never
        // overwritten.
        if let [staged] = self.aggregates.as_slice() {
            for message in &mut self.outbox_messages {
                if message.source_aggregate_type.is_none()
                    && message.source_aggregate_id.is_none()
                    && message.source_sequence.is_none()
                {
                    message.set_source(&staged.aggregate);
                }
            }
        }

        let streams = self
            .aggregates
            .iter_mut()
            .map(|staged| StreamWrite::new(staged.identity.clone(), staged.aggregate.entity_mut()))
            .collect();
        let mut batch = CommitBatch::new(streams);
        batch.outbox_messages = std::mem::take(&mut self.outbox_messages);
        batch.read_model_plans = std::mem::take(&mut self.read_model_plans);
        batch.snapshots = std::mem::take(&mut self.snapshots);
        Ok(batch)
    }

    /// Apply snapshot cache bookkeeping only after the fenced commit succeeds.
    pub(crate) fn mark_committed_state(&mut self) -> Result<(), CausalWorkspaceError> {
        for staged in &mut self.aggregates {
            if let Some(version) = staged.snapshot_version {
                staged.aggregate.entity_mut().set_snapshot_version(version);
            }
            staged
                .aggregate
                .entity_mut()
                .mark_domain_events_committed()?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn aggregate_count(&self) -> usize {
        self.aggregates.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::command::CommandTypeDef;
    use crate::entity::{Entity, EventRecord};
    use crate::table::{
        ColumnType, PrimaryKey, RowKey, RowValue, RowValues, TableColumn, TableKind, TableMutation,
        TableSchema,
    };

    #[derive(Clone, Default)]
    struct TestAggregate {
        entity: Entity,
    }

    impl Aggregate for TestAggregate {
        type ReplayError = String;

        fn aggregate_type() -> &'static str {
            "workspace_test"
        }

        fn entity(&self) -> &Entity {
            &self.entity
        }

        fn entity_mut(&mut self) -> &mut Entity {
            &mut self.entity
        }

        fn replay_event(&mut self, _event: &EventRecord) -> Result<(), Self::ReplayError> {
            Ok(())
        }
    }

    #[derive(
        Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, crate::DomainState,
    )]
    #[domain_state(version = 1)]
    struct CausalPublishedState {
        id: String,
        title: String,
    }

    #[derive(Clone, Default)]
    struct CausalPublishedAggregate {
        entity: Entity,
        title: String,
    }

    impl From<&CausalPublishedAggregate> for CausalPublishedState {
        fn from(aggregate: &CausalPublishedAggregate) -> Self {
            Self {
                id: aggregate.entity.id().to_string(),
                title: aggregate.title.clone(),
            }
        }
    }

    #[crate::sourced(
        entity,
        aggregate_type = "causal_published",
        domain_state = CausalPublishedState
    )]
    impl CausalPublishedAggregate {
        #[event("causal.created", version = 1, domain)]
        fn create(&mut self, id: String, title: String) {
            self.entity.set_id(id);
            self.title = title;
        }

        #[event("causal.renamed", version = 1, domain)]
        fn rename(&mut self, title: String) {
            self.title = title;
        }
    }

    #[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize, crate::ReadModel)]
    #[readmodel(table = "modeled_direct_views", primary_key = ["id"])]
    struct ModeledDirectView {
        id: String,
        title: String,
    }

    fn modeled_direct_program() -> Result<crate::ProjectionProgram, crate::ProjectionProgramError> {
        use crate::mutation::{
            body_bindings_for_model, compile_projection, state_upsert_program_for_model,
            MutationEventBinding, ProjectionHandler,
        };
        use crate::projection::ProjectionEventSelector;

        let program = state_upsert_program_for_model::<ModeledDirectView>(
            "save_modeled_direct",
            1,
            "upsert-view",
            "view",
        )
        .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
            operation: "modeled-direct".into(),
            reason: e.to_string(),
        })?;
        let selector =
            ProjectionEventSelector::try_from_descriptor(&crate::DomainEventDescriptor::state::<
                CausalPublishedState,
            >("causal.created", 1))?;
        let binding = MutationEventBinding::try_new(
            selector,
            body_bindings_for_model::<ModeledDirectView>("view").map_err(|e| {
                crate::ProjectionProgramError::InvalidOperation {
                    operation: "modeled-direct".into(),
                    reason: e.to_string(),
                }
            })?,
            program,
        )
        .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
            operation: "modeled-direct".into(),
            reason: e.to_string(),
        })?;
        compile_projection(
            "modeled-direct-workspace",
            1,
            crate::ProjectionPartition::Unit,
            [ProjectionHandler::from_binding("created", binding)],
        )
        .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
            operation: "modeled-direct".into(),
            reason: e.to_string(),
        })
    }

    fn modeled_direct_resolve(
        occurrence: &crate::DomainEventOccurrence,
    ) -> Result<crate::ResolvedProjectionPlan, crate::ProjectionProgramError> {
        crate::mutation::resolve_mutation_program(&modeled_direct_program()?, occurrence)
    }

    fn modeled_direct_lower(
        plan: &crate::ResolvedProjectionPlan,
    ) -> Result<
        crate::projection::lower::LoweredProjectionPlan,
        crate::projection::lower::ProjectionLoweringError,
    > {
        crate::mutation::lower_single_model::<ModeledDirectView>(plan)
    }

    fn modeled_direct_inventory() -> Result<
        crate::projection::lower::ProjectionOutputInventory,
        crate::projection::lower::ProjectionLoweringError,
    > {
        crate::mutation::inventory_single_model::<ModeledDirectView>()
    }

    const MODELED_DIRECT: ProjectionDescriptor<DirectCandidate> =
        crate::mutation::descriptor_from_factories(
            "modeled-direct-workspace",
            1,
            "modeled-direct-workspace-v1",
            modeled_direct_program,
            modeled_direct_resolve,
            modeled_direct_lower,
            modeled_direct_inventory,
        );

    #[derive(Clone, Debug, Serialize, crate::DomainEvent)]
    #[domain_event(name = "causal.background", version = 1)]
    struct BackgroundEvent {
        id: String,
    }

    #[derive(crate::DomainEvent)]
    #[domain_event(name = "causal.poisoned", version = 1)]
    struct PoisonedOutwardEvent {
        marker: bool,
    }

    impl Serialize for PoisonedOutwardEvent {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let _ = self.marker;
            Err(serde::ser::Error::custom("intentional poison"))
        }
    }

    #[derive(Clone)]
    struct TestRepo {
        entity: Arc<Entity>,
    }

    impl CausalGetStream for TestRepo {
        async fn get_causal_stream<'a>(
            &'a self,
            identity: &'a StreamIdentity,
        ) -> Result<Option<Entity>, RepositoryError> {
            Ok((identity.aggregate_id() == self.entity.id()).then(|| (*self.entity).clone()))
        }
    }

    #[derive(Clone, Debug, Serialize)]
    struct TestView {
        id: String,
        title: String,
    }

    impl RelationalReadModel for TestView {
        fn schema() -> &'static TableSchema {
            static SCHEMA: std::sync::LazyLock<TableSchema> =
                std::sync::LazyLock::new(|| TableSchema {
                    model_name: "TestView".into(),
                    table_name: "test_views".into(),
                    columns: vec![
                        TableColumn {
                            primary_key: true,
                            ..TableColumn::new("id", "id", ColumnType::Text)
                        },
                        TableColumn::new("title", "title", ColumnType::Text),
                    ],
                    primary_key: PrimaryKey::new(["id"]),
                    version_column: None,
                    foreign_keys: Vec::new(),
                    indexes: Vec::new(),
                    relationships: Vec::new(),
                    kind: TableKind::ReadModel,
                });
            &SCHEMA
        }

        fn primary_key(&self) -> Result<RowKey, TableStoreError> {
            Ok(RowKey::new([(
                "id",
                crate::table::RowValue::String(self.id.clone()),
            )]))
        }

        fn to_row(&self) -> Result<RowValues, TableStoreError> {
            let mut values = RowValues::new();
            values.insert("id", crate::table::RowValue::String(self.id.clone()));
            values.insert("title", crate::table::RowValue::String(self.title.clone()));
            Ok(values)
        }

        fn from_row(row: RowValues) -> Result<Self, TableStoreError> {
            Ok(Self {
                id: row.get_serde("id")?,
                title: row.get_serde("title")?,
            })
        }
    }

    #[derive(serde::Deserialize)]
    struct TestInput {}

    impl crate::command::CommandInputType for TestInput {
        fn command_type() -> CommandTypeDef {
            CommandTypeDef::new("TestInput", Vec::new())
                .with_type_id(std::any::TypeId::of::<Self>())
        }
    }

    fn loaded_repo() -> AggregateRepository<TestRepo, TestAggregate> {
        let mut entity = Entity::with_id("a-1");
        entity
            .digest_empty("Created")
            .expect("test event should encode");
        entity.mark_committed();
        AggregateRepository::new(TestRepo {
            entity: Arc::new(entity),
        })
    }

    #[tokio::test]
    async fn load_is_owned_and_stage_rejects_identity_and_version_tampering() {
        let repository = loaded_repo();
        let workspace = CausalWorkspace::new(&repository);

        let mut renamed = workspace.load("a-1").await.unwrap().unwrap();
        renamed.entity_mut().set_id("other");
        assert!(matches!(
            workspace.stage(renamed),
            Err(CausalWorkspaceError::IdentityChanged { .. })
        ));

        let mut version_changed = workspace.load("a-1").await.unwrap().unwrap();
        version_changed.entity_mut().load_from_history(Vec::new());
        assert!(matches!(
            workspace.stage(version_changed),
            Err(CausalWorkspaceError::CommittedVersionChanged { .. })
        ));
    }

    #[tokio::test]
    async fn duplicate_streams_are_rejected_without_repository_locks() {
        let repository = loaded_repo();
        let workspace = CausalWorkspace::new(&repository);
        let first = workspace.load("a-1").await.unwrap().unwrap();
        let second = workspace.load("a-1").await.unwrap().unwrap();
        workspace.stage(first).unwrap();
        assert!(matches!(
            workspace.stage(second),
            Err(CausalWorkspaceError::DuplicateStream(_))
        ));
    }

    #[tokio::test]
    async fn create_stages_a_new_owned_aggregate_after_it_establishes_identity() {
        let repository = loaded_repo();
        let workspace = CausalWorkspace::new(&repository);
        let mut checkout = workspace.create();
        checkout.entity_mut().set_id("new-aggregate");
        checkout.entity_mut().digest_empty("Created").unwrap();

        workspace.stage(checkout).unwrap();

        let parts = workspace.into_parts().unwrap();
        assert_eq!(parts.aggregate_count(), 1);
    }

    #[tokio::test]
    async fn workspace_drains_all_commit_participants_once() {
        let repository = loaded_repo();
        let workspace = CausalWorkspace::new(&repository);
        let mut checkout = workspace.load("a-1").await.unwrap().unwrap();
        checkout.entity_mut().digest_empty("Changed").unwrap();
        workspace.stage(checkout).unwrap();
        workspace
            .stage_outbox(OutboxMessage::create("m-1", "test.changed", vec![1]).unwrap())
            .unwrap();
        let mut plan = ReadModelWritePlanBuilder::new();
        plan.upsert(&TestView {
            id: "v-1".into(),
            title: "first".into(),
        })
        .unwrap();
        workspace.stage_read_models(plan).unwrap();

        let mut parts = workspace.into_parts().unwrap();
        assert_eq!(parts.aggregate_count(), 1);
        {
            let batch = parts.prepare_commit_batch().unwrap();
            assert_eq!(batch.streams.len(), 1);
            assert_eq!(batch.outbox_messages.len(), 1);
            assert_eq!(batch.read_model_plans.len(), 1);
            assert_eq!(
                batch.outbox_messages[0].source_aggregate_type.as_deref(),
                Some(TestAggregate::aggregate_type())
            );
            assert_eq!(
                batch.outbox_messages[0].source_aggregate_id.as_deref(),
                Some("a-1")
            );
            assert_eq!(batch.outbox_messages[0].source_sequence, Some(2));
        }
        assert!(matches!(
            parts.prepare_commit_batch(),
            Err(CausalWorkspaceError::CommitBatchAlreadyPrepared)
        ));
    }

    #[tokio::test]
    async fn multiple_aggregates_publish_occurrences_with_their_exact_sources() {
        let repository = AggregateRepository::<_, CausalPublishedAggregate>::new(TestRepo {
            entity: Arc::new(Entity::with_id("unused")),
        });
        let workspace = CausalWorkspace::new(&repository);

        for (id, title) in [("causal-1", "first"), ("causal-2", "second")] {
            let mut aggregate = workspace.create();
            aggregate.create(id.into(), title.into()).unwrap();
            workspace
                .stage_with_publication(
                    aggregate,
                    AggregatePublication {
                        publish_captured_events: true,
                        explicit_events: Vec::new(),
                    },
                )
                .unwrap();
        }

        let mut parts = workspace.into_parts().unwrap();
        parts
            .prepare_domain_publications("ledger-attempt-1")
            .unwrap();
        let expected_ids = ["causal-1", "causal-2"];
        {
            let batch = parts.prepare_commit_batch().unwrap();
            assert_eq!(batch.streams.len(), 2);
            assert_eq!(batch.outbox_messages.len(), 2);
            for (message, expected_id) in batch.outbox_messages.iter().zip(expected_ids) {
                let occurrence = message.domain_event_occurrence().unwrap();
                assert_eq!(occurrence.aggregate_type(), "causal_published");
                assert_eq!(occurrence.aggregate_id(), expected_id);
                assert_eq!(
                    occurrence
                        .metadata()
                        .get(crate::CAUSATION_ID)
                        .map(String::as_str),
                    Some("ledger-attempt-1")
                );
                assert_eq!(message.source_aggregate_id.as_deref(), Some(expected_id));
                assert_eq!(message.source_sequence, Some(1));
            }
        }

        assert!(parts.aggregates.iter().all(|staged| staged
            .aggregate
            .entity
            .pending_domain_events()
            .len()
            == 1));
        parts.mark_committed_state().unwrap();
        assert!(parts.aggregates.iter().all(|staged| staged
            .aggregate
            .entity
            .pending_domain_events()
            .is_empty()));
    }

    #[tokio::test]
    async fn poisoned_capture_stops_causal_publication_before_batch_preparation() {
        let repository = loaded_repo();
        let workspace = CausalWorkspace::new(&repository);
        let mut aggregate = workspace.create();
        aggregate.entity_mut().set_id("poisoned");
        aggregate.entity_mut().digest_empty("Changed").unwrap();
        assert!(aggregate
            .entity_mut()
            .capture_domain_event(
                TestAggregate::aggregate_type(),
                &PoisonedOutwardEvent { marker: false },
            )
            .is_err());
        workspace
            .stage_with_publication(
                aggregate,
                AggregatePublication {
                    publish_captured_events: true,
                    explicit_events: Vec::new(),
                },
            )
            .unwrap();

        let mut parts = workspace.into_parts().unwrap();
        assert!(matches!(
            parts.prepare_domain_publications("ledger-attempt-poisoned"),
            Err(CausalWorkspaceError::DomainEventGuard(_))
        ));
        assert!(parts.outbox_messages.is_empty());
        assert_eq!(
            parts.aggregates[0]
                .aggregate
                .entity
                .pending_domain_events()
                .len(),
            0
        );
        assert!(parts.aggregates[0]
            .aggregate
            .entity
            .domain_event_poison()
            .is_some());
    }

    #[tokio::test]
    async fn projected_result_is_tied_to_one_exact_staged_upsert() {
        let repository = loaded_repo();
        let workspace = CausalWorkspace::new(&repository);
        let view = TestView {
            id: "v-1".into(),
            title: "projected".into(),
        };
        let mut prepared = workspace.prepare_atomic(view).unwrap();
        let mut aggregate = workspace.load("a-1").await.unwrap().unwrap();
        aggregate.entity_mut().digest_empty("Projected").unwrap();
        workspace.stage(aggregate).unwrap();
        let mut parts = workspace.into_parts().unwrap();

        let contract = crate::command::typed_command::<TestInput, Atomic<TestView>>("test.project")
            .into_contract();
        parts.validate_prepared(&contract, &mut prepared).unwrap();
    }

    #[tokio::test]
    async fn projected_result_requires_a_durable_domain_fact() {
        let repository = loaded_repo();
        let workspace = CausalWorkspace::new(&repository);
        let mut prepared = workspace
            .prepare_atomic(TestView {
                id: "v-1".into(),
                title: "projected".into(),
            })
            .unwrap();
        let mut parts = workspace.into_parts().unwrap();
        let contract = crate::command::typed_command::<TestInput, Atomic<TestView>>("test.project")
            .into_contract();

        assert!(matches!(
            parts.validate_prepared(&contract, &mut prepared),
            Err(CommandCommitProofError::DurableEventMissing)
        ));
    }

    #[tokio::test]
    async fn projected_proof_rejects_a_second_write_to_the_returned_key() {
        let repository = loaded_repo();
        let workspace = CausalWorkspace::new(&repository);
        let mut prepared = workspace
            .prepare_atomic(TestView {
                id: "v-1".into(),
                title: "returned".into(),
            })
            .unwrap();
        workspace
            .stage_outbox(OutboxMessage::create("projection-2", "test.changed", vec![]).unwrap())
            .unwrap();
        let mut conflicting = ReadModelWritePlanBuilder::new();
        conflicting
            .upsert(&TestView {
                id: "v-1".into(),
                title: "overwritten".into(),
            })
            .unwrap();
        workspace.stage_read_models(conflicting).unwrap();
        let mut parts = workspace.into_parts().unwrap();
        let contract = crate::command::typed_command::<TestInput, Atomic<TestView>>("test.project")
            .into_contract();

        assert!(matches!(
            parts.validate_prepared(&contract, &mut prepared),
            Err(CommandCommitProofError::ProjectionWriteConflict { .. })
        ));
    }

    #[tokio::test]
    async fn projected_proof_rejects_row_drift_after_preparation() {
        let repository = loaded_repo();
        let workspace = CausalWorkspace::new(&repository);
        let mut prepared = workspace
            .prepare_atomic(TestView {
                id: "v-1".into(),
                title: "returned".into(),
            })
            .unwrap();
        workspace
            .stage_outbox(OutboxMessage::create("projection-3", "test.changed", vec![]).unwrap())
            .unwrap();
        let mut parts = workspace.into_parts().unwrap();
        let TableMutation::UpsertRow(mutation) = &mut parts.read_model_plans[0].mutations[0] else {
            panic!("projected preparation must stage a full-row upsert");
        };
        mutation
            .values
            .insert("title", RowValue::String("different".into()));
        let contract = crate::command::typed_command::<TestInput, Atomic<TestView>>("test.project")
            .into_contract();

        assert!(matches!(
            parts.validate_prepared(&contract, &mut prepared),
            Err(CommandCommitProofError::ProjectionWriteMismatch { .. })
        ));
    }

    fn modeled_direct_repository() -> AggregateRepository<TestRepo, CausalPublishedAggregate> {
        AggregateRepository::new(TestRepo {
            entity: Arc::new(Entity::with_id("unused")),
        })
    }

    fn modeled_direct_contract() -> TypedCommandContract {
        crate::command::typed_command::<TestInput, Atomic<ModeledDirectView>>("test.modeled-direct")
            .into_contract()
    }

    #[test]
    fn placement_selected_projected_uses_registered_executor_without_project_selector() {
        use crate::projection::lower::register_placement_selected_direct_descriptor;

        register_placement_selected_direct_descriptor(&MODELED_DIRECT)
            .expect("register placement-selected direct");
        let repository = modeled_direct_repository();
        let workspace = CausalWorkspace::new(&repository);
        let mut aggregate = workspace.create();
        aggregate
            .create("placement-1".into(), "authoritative".into())
            .unwrap();
        workspace.stage(aggregate).unwrap();
        let mut prepared = workspace
            .prepare_placement_selected_atomic::<ModeledDirectView>()
            .expect("placement-selected projected without .project(MODELED_DIRECT)");

        let mut parts = workspace.into_parts().unwrap();
        parts
            .prepare_domain_publications("cause-placement")
            .unwrap();
        parts
            .validate_prepared(&modeled_direct_contract(), &mut prepared)
            .expect("placement-selected proof validates");
        assert!(parts.resolved_direct_projection.is_some());
    }

    #[test]
    fn modeled_direct_resolves_one_internal_state_occurrence_to_one_exact_upsert() {
        let repository = modeled_direct_repository();
        let workspace = CausalWorkspace::new(&repository);
        let mut aggregate = workspace.create();
        aggregate
            .create("direct-1".into(), "authoritative".into())
            .unwrap();
        workspace.stage(aggregate).unwrap();
        let mut prepared = workspace
            .prepare_modeled_atomic::<ModeledDirectView>(MODELED_DIRECT)
            .unwrap();

        let mut parts = workspace.into_parts().unwrap();
        parts
            .prepare_domain_publications("modeled-direct-causation")
            .unwrap();
        assert_eq!(parts.domain_event_occurrences.len(), 1);
        assert_eq!(parts.unpublished_direct_occurrences.len(), 1);
        assert!(
            parts.outbox_messages.is_empty(),
            "the selected state occurrence is internal unless publication was requested"
        );

        parts
            .validate_prepared(&modeled_direct_contract(), &mut prepared)
            .unwrap();
        assert_eq!(prepared.serialized_payload()["id"], "direct-1");
        assert_eq!(
            prepared.serialized_payload()["title"],
            serde_json::json!("authoritative")
        );
        let lowered = parts
            .resolved_direct_projection
            .as_ref()
            .expect("validation must retain the selected authoritative lowering");
        assert_eq!(lowered.resolved.mutations().len(), 1);
        let [TableMutation::UpsertRow(row)] = lowered.write_plan.mutations.as_slice() else {
            panic!("modeled direct lowering must contain one full-row upsert");
        };
        assert_eq!(row.schema.model_name, "ModeledDirectView");
        assert_eq!(row.schema.table_name, "modeled_direct_views");
        assert_eq!(
            row.values.get("title"),
            Some(&RowValue::String("authoritative".into()))
        );
    }

    #[test]
    fn modeled_direct_rejects_a_separate_same_transaction_read_model_plan() {
        let repository = modeled_direct_repository();
        let workspace = CausalWorkspace::new(&repository);
        let mut aggregate = workspace.create();
        aggregate
            .create("direct-mixed".into(), "authoritative".into())
            .unwrap();
        workspace.stage(aggregate).unwrap();
        let mut prepared = workspace
            .prepare_modeled_atomic::<ModeledDirectView>(MODELED_DIRECT)
            .unwrap();
        let mut separate = ReadModelWritePlanBuilder::new();
        separate
            .upsert(&TestView {
                id: "separate".into(),
                title: "not part of the modeled proof".into(),
            })
            .unwrap();
        workspace.stage_read_models(separate).unwrap();

        let mut parts = workspace.into_parts().unwrap();
        parts
            .prepare_domain_publications("modeled-direct-mixed")
            .unwrap();
        assert!(matches!(
            parts.validate_prepared(&modeled_direct_contract(), &mut prepared),
            Err(CommandCommitProofError::DirectProjection(message))
                if message.contains("separate read-model mutations")
        ));
    }

    #[test]
    fn modeled_direct_materializes_the_authoritative_row_without_a_handler_payload() {
        let repository = modeled_direct_repository();
        let workspace = CausalWorkspace::new(&repository);
        let mut aggregate = workspace.create();
        aggregate
            .create("direct-drift".into(), "authoritative".into())
            .unwrap();
        workspace.stage(aggregate).unwrap();
        let mut prepared = workspace
            .prepare_modeled_atomic::<ModeledDirectView>(MODELED_DIRECT)
            .unwrap();

        let mut parts = workspace.into_parts().unwrap();
        parts
            .prepare_domain_publications("modeled-direct-drift")
            .unwrap();
        parts
            .validate_prepared(&modeled_direct_contract(), &mut prepared)
            .unwrap();
        assert_eq!(
            prepared.serialized_payload()["title"],
            serde_json::json!("authoritative")
        );
    }

    #[test]
    fn modeled_direct_requires_exactly_one_matching_occurrence() {
        let repository = modeled_direct_repository();
        let workspace = CausalWorkspace::new(&repository);
        let mut aggregate = workspace.create();
        aggregate.entity_mut().set_id("direct-none");
        aggregate
            .entity_mut()
            .digest_empty("causal.unrelated")
            .unwrap();
        workspace.stage(aggregate).unwrap();
        let mut prepared = workspace
            .prepare_modeled_atomic::<ModeledDirectView>(MODELED_DIRECT)
            .unwrap();
        let mut parts = workspace.into_parts().unwrap();
        parts
            .prepare_domain_publications("modeled-direct-none")
            .unwrap();
        assert!(matches!(
            parts.validate_prepared(&modeled_direct_contract(), &mut prepared),
            Err(CommandCommitProofError::DirectProjection(message))
                if message.contains("matched no committed")
        ));

        let repository = modeled_direct_repository();
        let workspace = CausalWorkspace::new(&repository);
        let mut aggregate = workspace.create();
        aggregate
            .create("direct-many".into(), "first".into())
            .unwrap();
        aggregate
            .create("direct-many".into(), "second".into())
            .unwrap();
        workspace.stage(aggregate).unwrap();
        let mut prepared = workspace
            .prepare_modeled_atomic::<ModeledDirectView>(MODELED_DIRECT)
            .unwrap();
        let mut parts = workspace.into_parts().unwrap();
        parts
            .prepare_domain_publications("modeled-direct-many")
            .unwrap();
        assert!(matches!(
            parts.validate_prepared(&modeled_direct_contract(), &mut prepared),
            Err(CommandCommitProofError::DirectProjection(message))
                if message.contains("more than one")
        ));
    }

    #[test]
    fn modeled_direct_rejects_unpublished_side_events_but_allows_published_background_events() {
        let repository = modeled_direct_repository();
        let workspace = CausalWorkspace::new(&repository);
        let mut aggregate = workspace.create();
        aggregate
            .create("direct-side".into(), "first".into())
            .unwrap();
        aggregate.rename("renamed".into()).unwrap();
        workspace.stage(aggregate).unwrap();
        let mut prepared = workspace
            .prepare_modeled_atomic::<ModeledDirectView>(MODELED_DIRECT)
            .unwrap();
        let mut parts = workspace.into_parts().unwrap();
        parts
            .prepare_domain_publications("modeled-direct-side")
            .unwrap();
        assert!(matches!(
            parts.validate_prepared(&modeled_direct_contract(), &mut prepared),
            Err(CommandCommitProofError::DirectProjection(message))
                if message.contains("unrelated unpublished domain event")
        ));

        let repository = modeled_direct_repository();
        let workspace = CausalWorkspace::new(&repository);
        let mut aggregate = workspace.create();
        aggregate
            .create("direct-background".into(), "authoritative".into())
            .unwrap();
        workspace
            .stage_with_publication(
                aggregate,
                AggregatePublication {
                    publish_captured_events: true,
                    explicit_events: vec![PreparedDomainEvent::new(BackgroundEvent {
                        id: "background-1".into(),
                    })
                    .unwrap()],
                },
            )
            .unwrap();
        let mut prepared = workspace
            .prepare_modeled_atomic::<ModeledDirectView>(MODELED_DIRECT)
            .unwrap();
        let mut parts = workspace.into_parts().unwrap();
        parts
            .prepare_domain_publications("modeled-direct-background")
            .unwrap();
        assert_eq!(
            parts.outbox_messages.len(),
            2,
            "publishing the selected occurrence and an unrelated background event is explicit"
        );
        parts
            .validate_prepared(&modeled_direct_contract(), &mut prepared)
            .unwrap();
    }
}
