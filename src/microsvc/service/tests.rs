#[cfg(feature = "graphql")]
use super::causal::collapse_projection_evidence;
#[cfg(feature = "runtime-tokio")]
use super::routes::LedgerCompaction;
use super::*;
use crate::aggregate::Aggregate;
use crate::bus::RunOptions;
use crate::bus::{Message, MessageKind, SubscriptionPlan};
use crate::command::Eventual;
use crate::command::{typed_command, PreparedCommand, Succeeded};
#[cfg(feature = "graphql")]
use crate::command::{Atomic, CommandConsistency};
use crate::command::{CommandInputType, CommandOutputType, CommandTypeDef, CommandTypeField};
#[cfg(feature = "graphql")]
use crate::command_dispatch::SharedCommandHost;
use crate::command_dispatch::VerifiedPrincipal;
#[cfg(feature = "runtime-tokio")]
use crate::command_ledger::CommandLedgerState;
use crate::command_ledger::{
    AttemptFence, CausalCommitBatch, CausalGetStream, CausalRepositoryIdentity,
    CausalTransactionalCommit, CommandLedgerError, CommandLedgerKey, CommandLedgerStore,
    CommandLookup, CommandLookupScope, CommandReservation, ExternalCommandCompletion,
    ReservationOutcome,
};
#[cfg(feature = "graphql")]
use crate::graphql::{SurfaceDirectProjection, SurfaceProjector};
#[cfg(feature = "graphql")]
use crate::microsvc::cell_host::{CelldCommandHost, InternalHttpSecret};
use crate::microsvc::HasOutboxStore;
use crate::microsvc::{Context, HandlerError, RepoReadModelDependencies, Routes, Service, Session};
use crate::outbox::OutboxMessage;
use crate::outbox_worker::OutboxStore;
use crate::projection_protocol::{
    PartitionId, ProjectionCausationEvidenceBatch, ProjectionCausationEvidenceRequest,
    ProjectionChangeCursor, ProjectionChangeRead, ProjectionCheckpoint, ProjectionCommitBatch,
    ProjectionCommitResult, ProjectionExecutionSnapshotBatch,
    ProjectionExecutionSnapshotBatchRequest, ProjectionFailure, ProjectionFailureBatch,
    ProjectionFailureLocation, ProjectionGeneration, ProjectionInputCursor,
    ProjectionInputDisposition, ProjectionModelOwnership, ProjectionObservation,
    ProjectionObservationKind, ProjectionPartitionRuntimeState, ProjectionProtocolError,
    ProjectionProtocolStore, ProjectionQuerySnapshot, ProjectionQuerySnapshotRequest,
    ProjectionRecordMetadata, ProjectionRecordScope, ProjectorTopologyId, TrustedProjectionInput,
};
use crate::GetStream;
use crate::{
    sourced, AggregateBuilder, AggregateRepository, Entity, InMemoryRepository, Queueable,
    QueuedRepository,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(feature = "graphql")]
const TEST_PROTOCOL_TOKEN_KEY: [u8; 32] = [0x5a; 32];

#[derive(Deserialize)]
struct TypedInput {
    id: String,
}

#[derive(Serialize)]
struct TypedOutput {
    id: String,
}

fn one_string_field(name: &str, field: &str) -> CommandTypeDef {
    CommandTypeDef::new(
        name,
        vec![CommandTypeField {
            unsigned_integer: None,
            name: field.into(),
            type_name: "String".into(),
            nullable: false,
            list: false,
            item_nullable: false,
            nested: None,
        }],
    )
}

impl CommandInputType for TypedInput {
    fn command_type() -> CommandTypeDef {
        one_string_field("TypedInput", "id").with_type_id(std::any::TypeId::of::<Self>())
    }
}

impl CommandOutputType for TypedOutput {
    fn command_type() -> CommandTypeDef {
        one_string_field("TypedOutput", "id").with_type_id(std::any::TypeId::of::<Self>())
    }
}

#[derive(Deserialize)]
struct CausalTestInput {
    id: String,
    label: String,
}

impl CommandInputType for CausalTestInput {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "CausalTestInput",
            vec![
                CommandTypeField {
                    unsigned_integer: None,
                    name: "id".into(),
                    type_name: "String".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                },
                CommandTypeField {
                    unsigned_integer: None,
                    name: "label".into(),
                    type_name: "String".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                },
            ],
        )
        .with_type_id(std::any::TypeId::of::<Self>())
    }
}

#[cfg(feature = "graphql")]
#[derive(Clone, Deserialize, crate::command::CommandInput)]
struct CausalProjectionInput {
    #[serde(rename = "todoId")]
    id: String,
    #[serde(rename = "tenantPartition")]
    partition: String,
}

#[derive(Clone, Serialize, Deserialize, crate::read_model::ReadModel)]
#[readmodel(
        table = "causal_projection_obligation_views",
        primary_key = ["id"]
    )]
struct CausalProjectionObligationView {
    id: String,
}

#[derive(Clone, Serialize, Deserialize, crate::read_model::ReadModel)]
#[readmodel(table = "causal_projection_sibling_views", primary_key = ["id"])]
struct CausalProjectionSiblingView {
    id: String,
}

#[derive(Clone, Serialize, crate::domain_event::DomainEvent)]
#[domain_event(name = "causal.lifecycle-recorded", version = 1)]
struct CausalLifecycleRecorded {
    id: String,
    label: String,
}

#[derive(Clone, Serialize, Deserialize, crate::read_model::ReadModel)]
#[readmodel(table = "causal_lifecycle_views", primary_key = ["id"])]
struct CausalLifecycleView {
    id: String,
    label: String,
}

#[cfg(feature = "graphql")]
#[derive(Clone, Serialize, Deserialize, crate::domain_event::DomainState)]
#[domain_state(version = 1)]
struct CausalDirectState {
    id: String,
}

#[cfg(feature = "graphql")]
fn causal_direct_program(
    name: &'static str,
    version: u64,
    event_name: &'static str,
) -> Result<crate::projection::ProjectionProgram, crate::projection::ProjectionProgramError> {
    use crate::mutation::{
        bind_event_apply_mutation, compile_projection, state_upsert_program_for_model,
    };

    let program = state_upsert_program_for_model::<CausalProjectionObligationView>(
        "save_causal_direct",
        1,
        "upsert-view",
        "view",
    )?;
    let descriptor =
        crate::domain_event::DomainEventDescriptor::state::<CausalDirectState>(event_name, 1);
    let handler = bind_event_apply_mutation(
        &descriptor,
        program,
        "view",
        crate::mutation::ProjectionInputSource::Body,
    )?;
    compile_projection(
        name,
        version,
        crate::projection::ProjectionPartition::Unit,
        [handler],
    )
}

#[cfg(feature = "graphql")]
fn causal_direct_v1_program(
) -> Result<crate::projection::ProjectionProgram, crate::projection::ProjectionProgramError> {
    causal_direct_program("project_causal_direct", 1, "causal.direct-recorded")
}

#[cfg(feature = "graphql")]
crate::projection! {
    const CAUSAL_DIRECT_PROJECTION: crate::projection::lower::ProjectionDescriptor< crate::projection::lower::DirectCandidate, > = {
        name: "project_causal_direct",
        version: 1,
        model: CausalProjectionObligationView,
        program: causal_direct_v1_program,
    };
}

#[cfg(feature = "graphql")]
fn causal_direct_v2_program(
) -> Result<crate::projection::ProjectionProgram, crate::projection::ProjectionProgramError> {
    causal_direct_program("project_causal_direct", 2, "causal.direct-recorded")
}

#[cfg(feature = "graphql")]
crate::projection! {
    const CAUSAL_ROGUE_DIRECT_PROJECTION: crate::projection::lower::ProjectionDescriptor< crate::projection::lower::DirectCandidate, > = {
        name: "project_causal_direct",
        version: 2,
        model: CausalProjectionObligationView,
        program: causal_direct_v2_program,
    };
}

#[cfg(feature = "graphql")]
fn causal_sibling_program(
) -> Result<crate::projection::ProjectionProgram, crate::projection::ProjectionProgramError> {
    use crate::mutation::{
        bind_event_apply_mutation, compile_projection, state_upsert_program_for_model,
    };

    let program = state_upsert_program_for_model::<CausalProjectionSiblingView>(
        "save_causal_sibling",
        1,
        "upsert-view",
        "view",
    )?;
    let descriptor = crate::domain_event::DomainEventDescriptor::state::<CausalDirectState>(
        "causal.direct-sibling-recorded",
        1,
    );
    let handler = bind_event_apply_mutation(
        &descriptor,
        program,
        "view",
        crate::mutation::ProjectionInputSource::Body,
    )?;
    compile_projection(
        "project_causal_direct_sibling",
        1,
        crate::projection::ProjectionPartition::Unit,
        [handler],
    )
}

#[cfg(feature = "graphql")]
crate::projection! {
    const CAUSAL_SIBLING_DIRECT_PROJECTION: crate::projection::lower::ProjectionDescriptor< crate::projection::lower::DirectCandidate, > = {
        name: "project_causal_direct_sibling",
        version: 1,
        model: CausalProjectionSiblingView,
        program: causal_sibling_program,
    };
}

#[cfg(feature = "graphql")]
fn causal_lifecycle_program(
) -> Result<crate::projection::ProjectionProgram, crate::projection::ProjectionProgramError> {
    use crate::domain_event::DomainEventContract;
    use crate::mutation::{
        bind_event_apply_mutation, compile_projection, state_upsert_program_for_model,
    };

    let program = state_upsert_program_for_model::<CausalLifecycleView>(
        "save_causal_lifecycle",
        1,
        "upsert-view",
        "view",
    )?;
    let handler = bind_event_apply_mutation(
        &CausalLifecycleRecorded::descriptor(),
        program,
        "view",
        crate::mutation::ProjectionInputSource::Body,
    )?;
    compile_projection(
        "project_causal_lifecycle",
        1,
        crate::projection::ProjectionPartition::Unit,
        [handler],
    )
}

#[cfg(feature = "graphql")]
crate::projection! {
    const CAUSAL_LIFECYCLE_PROJECTION: crate::projection::lower::ProjectionDescriptor< crate::projection::lower::EventualOnly, > = {
        name: "project_causal_lifecycle",
        version: 1,
        model: CausalLifecycleView,
        program: causal_lifecycle_program,
    };
}

#[cfg(feature = "graphql")]
fn modeled_direct_registration(
    descriptor: crate::projection::lower::ProjectionDescriptor<
        crate::projection::lower::DirectCandidate,
    >,
    owner: &str,
    topology_name: &str,
    topology_digest: u8,
    schema: crate::table::TableSchema,
) -> crate::graphql::SurfaceModeledProjection {
    let model = schema.model_name.clone();
    let storage = schema.table_name.clone();
    let binding = crate::projection::placement::ProjectionBinding::materialize_direct(
        descriptor.direct(),
        crate::projection::placement::ProjectionSourceBinding::try_new(
            "causal-domain",
            "ordered-domain-events",
            1,
        )
        .unwrap(),
        crate::projection::placement::ProjectionOwner::try_new(owner).unwrap(),
        "distributed-projection-partition",
        crate::projection::placement::PROJECTION_PARTITION_CODEC_VERSION,
        vec![
            crate::projection::placement::ProjectionOutput::try_new(model, storage, schema)
                .unwrap(),
        ],
        vec![],
        Some(
            crate::projection::placement::ProjectionPhysicalTopology::from_protocol(
                &ProjectorTopologyId::new(1, topology_name, [topology_digest; 32]).unwrap(),
            ),
        ),
    )
    .unwrap();
    let catalog =
        crate::projection::catalog::ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
    let active = catalog
        .activate(vec![
            crate::projection::catalog::ProjectionBindingActivation::new(
                binding.id(),
                binding.program_id(),
                crate::projection::placement::ProjectionBindingState::Active,
                Some(
                    crate::projection::placement::ProjectionExecutorRoute::local("causal-direct")
                        .unwrap(),
                ),
            ),
        ])
        .unwrap();
    crate::graphql::SurfaceModeledProjection::try_from_descriptor(
        descriptor,
        &catalog,
        &active,
        binding.id(),
    )
    .unwrap()
}

#[cfg(feature = "graphql")]
fn modeled_lifecycle_projector(
    state: crate::projection::placement::ProjectionBindingState,
    route: &str,
    topology_name: &str,
    topology_digest: u8,
) -> SurfaceProjector {
    let schema = <CausalLifecycleView as crate::read_model::RelationalReadModel>::schema().clone();
    let binding = crate::projection::placement::ProjectionBinding::materialize_eventual(
        CAUSAL_LIFECYCLE_PROJECTION.eventual(),
        crate::projection::placement::ProjectionSourceBinding::try_new(
            "causal-domain",
            "ordered-domain-events",
            1,
        )
        .unwrap(),
        crate::projection::placement::ProjectionOwner::try_new("project_causal_lifecycle").unwrap(),
        "distributed-projection-partition",
        crate::projection::placement::PROJECTION_PARTITION_CODEC_VERSION,
        vec![crate::projection::placement::ProjectionOutput::try_new(
            schema.model_name.clone(),
            schema.table_name.clone(),
            schema,
        )
        .unwrap()],
        vec![],
        Some(
            crate::projection::placement::ProjectionPhysicalTopology::from_protocol(
                &ProjectorTopologyId::new(1, topology_name, [topology_digest; 32]).unwrap(),
            ),
        ),
    )
    .unwrap();
    let catalog =
        crate::projection::catalog::ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
    let active = catalog
        .activate(vec![
            crate::projection::catalog::ProjectionBindingActivation::new(
                binding.id(),
                binding.program_id(),
                state,
                Some(crate::projection::placement::ProjectionExecutorRoute::local(route).unwrap()),
            ),
        ])
        .unwrap();
    SurfaceProjector::new("project_causal_lifecycle").modeled(
        crate::graphql::SurfaceModeledProjection::try_from_descriptor(
            CAUSAL_LIFECYCLE_PROJECTION,
            &catalog,
            &active,
            binding.id(),
        )
        .unwrap(),
    )
}

impl CommandOutputType for CausalProjectionObligationView {
    fn command_type() -> CommandTypeDef {
        one_string_field("CausalProjectionObligationView", "id")
            .with_type_id(std::any::TypeId::of::<Self>())
    }
}

impl CommandOutputType for CausalProjectionSiblingView {
    fn command_type() -> CommandTypeDef {
        one_string_field("CausalProjectionSiblingView", "id")
            .with_type_id(std::any::TypeId::of::<Self>())
    }
}

impl CommandOutputType for CausalLifecycleView {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "CausalLifecycleView",
            vec![
                CommandTypeField {
                    unsigned_integer: None,
                    name: "id".into(),
                    type_name: "String".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                },
                CommandTypeField {
                    unsigned_integer: None,
                    name: "label".into(),
                    type_name: "String".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                },
            ],
        )
        .with_type_id(std::any::TypeId::of::<Self>())
    }
}

async fn typed_handler(
    _context: &CausalCommandContext<'_, RouteComboAggregate>,
    input: TypedInput,
) -> Result<PreparedCommand<Succeeded<TypedOutput>>, HandlerError> {
    Ok(PreparedCommand::prepare(TypedOutput { id: input.id }).unwrap())
}

#[derive(Default)]
struct RouteComboAggregate {
    entity: Entity,
}

#[sourced(entity, aggregate_type = "route_combo_aggregate")]
impl RouteComboAggregate {
    #[event("created")]
    fn create(&mut self) {
        self.entity.set_id("route-combo");
    }
}

#[derive(Default, crate::Snapshot)]
struct CausalDispatcherAggregate {
    entity: Entity,
}

impl CausalDispatcherAggregate {
    fn record(&mut self, id: String) -> crate::SourcedResult {
        self.entity.set_id(id);
        self.entity.digest_empty("causal.recorded")
    }

    #[cfg(feature = "graphql")]
    fn record_direct(&mut self, id: String) -> Result<(), HandlerError> {
        self.entity.set_id(id.clone());
        self.entity
            .digest_empty("causal.direct-recorded")
            .map_err(|error| HandlerError::Other(Box::new(error)))?;
        self.entity
            .capture_domain_state(
                Self::aggregate_type(),
                crate::domain_event::DomainEventDescriptor::state::<CausalDirectState>(
                    "causal.direct-recorded",
                    1,
                ),
                &CausalDirectState { id },
            )
            .map_err(|error| HandlerError::Other(Box::new(error)))?;
        Ok(())
    }

    fn record_lifecycle(&mut self, id: String, label: String) -> Result<(), HandlerError> {
        self.entity.set_id(id.clone());
        self.entity
            .digest_empty("causal.lifecycle-recorded")
            .map_err(|error| HandlerError::Other(Box::new(error)))?;
        self.entity
            .capture_domain_event(
                Self::aggregate_type(),
                &CausalLifecycleRecorded { id, label },
            )
            .map_err(|error| HandlerError::Other(Box::new(error)))?;
        Ok(())
    }
}

impl Aggregate for CausalDispatcherAggregate {
    type ReplayError = std::convert::Infallible;

    fn aggregate_type() -> &'static str {
        "service-causal-dispatcher-test"
    }

    fn entity(&self) -> &Entity {
        &self.entity
    }

    fn entity_mut(&mut self) -> &mut Entity {
        &mut self.entity
    }

    fn replay_event(&mut self, _event: &crate::EventRecord) -> Result<(), Self::ReplayError> {
        Ok(())
    }
}

fn causal_test_principal() -> VerifiedPrincipal {
    VerifiedPrincipal::test_oidc(
        "https://issuer.example/",
        "causal-test-subject",
        &["distributed-tests"],
    )
}

fn causal_test_command_id() -> String {
    uuid::Uuid::now_v7().hyphenated().to_string()
}

fn causal_test_input(id: &str, label: &str) -> Value {
    json!({ "id": id, "label": label })
}

fn session_with_role(role: &str) -> Session {
    let mut session = Session::new();
    session.set(crate::microsvc::ROLE_KEY, role);
    session.set(crate::microsvc::USER_ID_KEY, "causal-test-subject");
    session
}

#[cfg(feature = "graphql")]
fn command_host(service: &Arc<Service>) -> crate::command_dispatch::SharedCommandHost {
    Arc::clone(service) as crate::command_dispatch::SharedCommandHost
}

#[cfg(feature = "graphql")]
fn celld_status_host(service: &Arc<Service>) -> SharedCommandHost {
    Arc::new(
        CelldCommandHost::new(
            "http://127.0.0.1:1",
            Arc::clone(service),
            InternalHttpSecret::new("test-only-internal-secret-32-bytes")
                .expect("test internal secret should be valid"),
        )
        .expect("status-only celld host should accept a local URL"),
    )
}

#[derive(Clone, Copy)]
enum InjectedCommitBehavior {
    CommitThenErrorOnce,
    ErrorBeforeCommitOnce,
    Delegate,
}

#[derive(Clone)]
struct AmbiguousCommitRepository {
    inner: InMemoryRepository,
    behavior: Arc<Mutex<InjectedCommitBehavior>>,
}

impl AmbiguousCommitRepository {
    fn new(inner: InMemoryRepository, behavior: InjectedCommitBehavior) -> Self {
        Self {
            inner,
            behavior: Arc::new(Mutex::new(behavior)),
        }
    }

    fn injected_error() -> CommandLedgerError {
        CommandLedgerError::Storage(crate::RepositoryError::retryable_storage(
            "injected ambiguous causal commit",
            std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "injected transport acknowledgement loss",
            ),
        ))
    }
}

impl CausalGetStream for AmbiguousCommitRepository {
    fn get_causal_stream_tail<'a>(
        &'a self,
        identity: &'a crate::StreamIdentity,
        after_version: u64,
    ) -> impl Future<Output = Result<Option<Entity>, crate::RepositoryError>> + Send + 'a {
        self.inner.get_causal_stream_tail(identity, after_version)
    }
    fn get_causal_stream<'a>(
        &'a self,
        identity: &'a crate::StreamIdentity,
    ) -> impl Future<Output = Result<Option<Entity>, crate::RepositoryError>> + Send + 'a {
        CausalGetStream::get_causal_stream(&self.inner, identity)
    }
}

impl CausalRepositoryIdentity for AmbiguousCommitRepository {
    fn causal_storage_identity(&self) -> crate::command_ledger::CausalStorageIdentity {
        CausalRepositoryIdentity::causal_storage_identity(&self.inner)
    }
}

impl crate::microsvc::dependencies::CausalHostProjections for AmbiguousCommitRepository {
    crate::microsvc::dependencies::direct_causal_projection_methods!();
}

impl ProjectionProtocolStore for AmbiguousCommitRepository {
    async fn projection_rebuild_records(
        &self,
        context: &crate::projection::rebuild::RebuildContext,
    ) -> Result<Vec<ProjectionRecordMetadata>, ProjectionProtocolError> {
        self.inner.projection_rebuild_records(context).await
    }

    async fn commit_projection_rebuild(
        &self,
        plan: crate::projection::rebuild::SnapshotProjectionRebuildPlan,
    ) -> Result<usize, ProjectionProtocolError> {
        self.inner.commit_projection_rebuild(plan).await
    }

    fn register_projection_models<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        ownership: &'a [ProjectionModelOwnership],
    ) -> impl Future<Output = Result<(), ProjectionProtocolError>> + Send + 'a {
        self.inner.register_projection_models(topology, ownership)
    }

    fn commit_projection(
        &self,
        batch: ProjectionCommitBatch,
    ) -> impl Future<Output = Result<ProjectionCommitResult, ProjectionProtocolError>> + Send + '_
    {
        self.inner.commit_projection(batch)
    }

    fn record_projection_failure(
        &self,
        batch: ProjectionFailureBatch,
    ) -> impl Future<Output = Result<ProjectionFailure, ProjectionProtocolError>> + Send + '_ {
        self.inner.record_projection_failure(batch)
    }

    fn projection_checkpoint<'a>(
        &'a self,
        cursor_scope: &'a ProjectionInputCursor,
        generation: ProjectionGeneration,
    ) -> impl Future<Output = Result<Option<ProjectionCheckpoint>, ProjectionProtocolError>> + Send + 'a
    {
        self.inner.projection_checkpoint(cursor_scope, generation)
    }

    fn projection_record<'a>(
        &'a self,
        scope: &'a ProjectionRecordScope,
    ) -> impl Future<Output = Result<Option<ProjectionRecordMetadata>, ProjectionProtocolError>>
           + Send
           + 'a {
        self.inner.projection_record(scope)
    }

    fn projection_input_disposition<'a>(
        &'a self,
        input: &'a TrustedProjectionInput,
    ) -> impl Future<Output = Result<ProjectionInputDisposition, ProjectionProtocolError>> + Send + 'a
    {
        self.inner.projection_input_disposition(input)
    }

    fn projection_query_snapshot<'a>(
        &'a self,
        request: &'a ProjectionQuerySnapshotRequest,
    ) -> impl Future<Output = Result<ProjectionQuerySnapshot, ProjectionProtocolError>> + Send + 'a
    {
        self.inner.projection_query_snapshot(request)
    }

    fn projection_execution_snapshot_batch<'a>(
        &'a self,
        request: &'a ProjectionExecutionSnapshotBatchRequest,
    ) -> impl Future<Output = Result<ProjectionExecutionSnapshotBatch, ProjectionProtocolError>>
           + Send
           + 'a {
        self.inner.projection_execution_snapshot_batch(request)
    }

    fn projection_causation_evidence<'a>(
        &'a self,
        request: &'a ProjectionCausationEvidenceRequest,
    ) -> impl Future<Output = Result<ProjectionCausationEvidenceBatch, ProjectionProtocolError>>
           + Send
           + 'a {
        self.inner.projection_causation_evidence(request)
    }

    fn projection_partition_runtime_state<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a PartitionId,
    ) -> impl Future<Output = Result<Option<ProjectionPartitionRuntimeState>, ProjectionProtocolError>>
           + Send
           + 'a {
        self.inner
            .projection_partition_runtime_state(topology, partition)
    }

    fn projection_observation<'a>(
        &'a self,
        causation_id: &'a str,
        scope: &'a ProjectionRecordScope,
        kind: ProjectionObservationKind,
    ) -> impl Future<Output = Result<Option<ProjectionObservation>, ProjectionProtocolError>> + Send + 'a
    {
        self.inner.projection_observation(causation_id, scope, kind)
    }

    fn projection_changes<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a PartitionId,
        after: Option<&'a ProjectionChangeCursor>,
        limit: usize,
    ) -> impl Future<Output = Result<ProjectionChangeRead, ProjectionProtocolError>> + Send + 'a
    {
        self.inner
            .projection_changes(topology, partition, after, limit)
    }

    fn repair_projection<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a PartitionId,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<ProjectionGeneration, ProjectionProtocolError>> + Send + 'a
    {
        self.inner
            .repair_projection(topology, partition, failure_id)
    }

    fn compact_projection_changes<'a>(
        &'a self,
        through: &'a ProjectionChangeCursor,
    ) -> impl Future<Output = Result<u64, ProjectionProtocolError>> + Send + 'a {
        self.inner.compact_projection_changes(through)
    }

    fn projection_failure<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a PartitionId,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<Option<ProjectionFailure>, ProjectionProtocolError>> + Send + 'a
    {
        self.inner
            .projection_failure(topology, partition, failure_id)
    }

    fn projection_failure_location<'a>(
        &'a self,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<Option<ProjectionFailureLocation>, ProjectionProtocolError>>
           + Send
           + 'a {
        self.inner.projection_failure_location(failure_id)
    }

    fn stopped_projection_failures<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
    ) -> impl Future<Output = Result<Vec<String>, ProjectionProtocolError>> + Send + 'a {
        self.inner.stopped_projection_failures(topology)
    }
}

impl CommandLedgerStore for AmbiguousCommitRepository {
    fn reserve_command(
        &self,
        reservation: CommandReservation,
    ) -> impl Future<Output = Result<ReservationOutcome, CommandLedgerError>> + Send + '_ {
        CommandLedgerStore::reserve_command(&self.inner, reservation)
    }

    fn lookup_command<'a>(
        &'a self,
        key: &'a CommandLedgerKey,
        scope: CommandLookupScope<'a>,
    ) -> impl Future<Output = Result<CommandLookup, CommandLedgerError>> + Send + 'a {
        CommandLedgerStore::lookup_command(&self.inner, key, scope)
    }

    fn mark_retryable_unknown(
        &self,
        attempt: AttemptFence,
    ) -> impl Future<Output = Result<(), CommandLedgerError>> + Send + '_ {
        CommandLedgerStore::mark_retryable_unknown(&self.inner, attempt)
    }

    fn complete_external_command(
        &self,
        completion: ExternalCommandCompletion,
    ) -> impl Future<Output = Result<(), CommandLedgerError>> + Send + '_ {
        CommandLedgerStore::complete_external_command(&self.inner, completion)
    }

    fn compact_expired_commands(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<u64, CommandLedgerError>> + Send + '_ {
        CommandLedgerStore::compact_expired_commands(&self.inner, limit)
    }
}

impl crate::schedule::BackendHost for AmbiguousCommitRepository {
    fn capabilities(&self) -> crate::schedule::Capabilities {
        self.inner.capabilities()
    }

    fn host_now(
        &self,
    ) -> impl std::future::Future<Output = Result<std::time::SystemTime, crate::RepositoryError>>
           + Send
           + '_ {
        self.inner.host_now()
    }
}

impl crate::repository::TransactionalCommit for AmbiguousCommitRepository {
    fn commit_batch<'a>(
        &'a self,
        batch: crate::repository::CommitBatch<'a>,
    ) -> impl Future<Output = Result<(), crate::RepositoryError>> + Send + 'a {
        crate::repository::TransactionalCommit::commit_batch(&self.inner, batch)
    }
}

impl CausalTransactionalCommit for AmbiguousCommitRepository {
    async fn commit_causal_batch<'a>(
        &'a self,
        batch: CausalCommitBatch<'a>,
    ) -> Result<crate::command_ledger::CommandCompletion<'a>, CommandLedgerError> {
        let behavior = {
            let mut behavior = self.behavior.lock().map_err(|_| {
                CommandLedgerError::Storage(crate::RepositoryError::LockPoisoned(
                    "injected causal commit behavior",
                ))
            })?;
            std::mem::replace(&mut *behavior, InjectedCommitBehavior::Delegate)
        };
        match behavior {
            InjectedCommitBehavior::CommitThenErrorOnce => {
                CausalTransactionalCommit::commit_causal_batch(&self.inner, batch).await?;
                Err(Self::injected_error())
            }
            InjectedCommitBehavior::ErrorBeforeCommitOnce => Err(Self::injected_error()),
            InjectedCommitBehavior::Delegate => {
                CausalTransactionalCommit::commit_causal_batch(&self.inner, batch).await
            }
        }
    }
}

impl HasOutboxStore for AmbiguousCommitRepository {
    type OutboxStore = crate::InMemoryOutboxStore;

    fn outbox_store(&self) -> Self::OutboxStore {
        self.inner.outbox_store()
    }
}

type RouteComboRepo =
    AggregateRepository<QueuedRepository<InMemoryRepository>, RouteComboAggregate>;
type RouteComboDeps = RepoReadModelDependencies<RouteComboRepo, InMemoryRepository>;

fn test_routes() -> Routes<()> {
    Routes::new().with_dependencies(())
}

fn test_service(routes: Routes<()>) -> Service {
    Service::new().routes(routes)
}

/// Dependencies of a route bundle whose commands touch no aggregate.
type StoreDeps =
    RepoReadModelDependencies<AggregateRepository<InMemoryRepository, ()>, InMemoryRepository>;

fn command_routes() -> Routes<StoreDeps> {
    let repository = InMemoryRepository::new();
    Routes::for_stores(repository.clone(), repository)
}

fn command_service(routes: Routes<StoreDeps>) -> Service {
    Service::new().named("tests").routes(routes)
}

#[derive(Deserialize, crate::command::CommandInput)]
struct NoInput {}

#[derive(Serialize, crate::command::CommandOutput)]
struct Pong {
    pong: bool,
}

fn pong_command(name: &'static str) -> crate::command::TypedCommand<NoInput, Succeeded<Pong>> {
    typed_command::<NoInput, Succeeded<Pong>>(name)
}

async fn pong(
    _context: &CausalCommandContext<'_, ()>,
    _input: NoInput,
) -> Result<PreparedCommand<Succeeded<Pong>>, HandlerError> {
    Ok(PreparedCommand::prepare(Pong { pong: true }).unwrap())
}

/// Dispatch one new command as the test principal.
async fn dispatch_command(
    service: &Service,
    command: &str,
    input: Value,
    session: Session,
) -> Result<Value, CausalDispatchError> {
    service
        .dispatch_causal(
            command,
            &causal_test_command_id(),
            input,
            session,
            causal_test_principal(),
        )
        .await
}

#[test]
fn named_service_preserves_identity_with_route_bundles() {
    let routes = Routes::new().with_read_model_store(crate::InMemoryRepository::new());
    let service = Service::new().named("todo-api").routes(routes);

    assert_eq!(service.name(), Some("todo-api"));
    assert_eq!(
        crate::bus::MessageRouter::consumer_group(&service),
        Some("todo-api")
    );
}

#[test]
fn service_compiles_exact_application_modules_from_command_namespaces() {
    let repository = InMemoryRepository::new();
    let service = Service::new().named("catalog").routes(
        Routes::new()
            .with_repo(repository.queued().aggregate::<RouteComboAggregate>())
            .typed_command(
                typed_command::<TypedInput, Succeeded<TypedOutput>>("access.grant").roles(["user"]),
            )
            .handle(typed_handler)
            .typed_command(
                typed_command::<TypedInput, Succeeded<TypedOutput>>("repository.create")
                    .roles(["user"]),
            )
            .handle(typed_handler),
    );
    let surface = crate::graphql::build_surface(&[], &crate::graphql::SurfaceOptions::sqlite())
        .expect("empty read-model Surface should build")
        .with_service(&service)
        .expect("typed Service should bind to the Surface");
    let surface = crate::application::SurfaceSpec::from_surface("catalog", &surface)
        .expect("Surface should become a portable application contract");

    let application = service
        .application("catalog", surface.clone())
        .expect("Service and Surface should compile into one application");

    assert_eq!(
        application
            .modules()
            .iter()
            .map(crate::application::Module::id)
            .collect::<Vec<_>>(),
        ["access", "repository"]
    );
    assert_eq!(
        application
            .manifest()
            .commands
            .iter()
            .map(|command| command.id.as_str())
            .collect::<Vec<_>>(),
        ["access.grant", "repository.create"]
    );

    let mut missing = surface.clone();
    missing
        .contract
        .commands
        .retain(|command| command.id != "access.grant");
    assert!(matches!(
        service.application("catalog", missing),
        Err(crate::application::ApplicationError::Missing { kind: "surface command", identity })
            if identity == "access.grant"
    ));
    assert!(
        Service::new().application("catalog", surface).is_err(),
        "a Surface cannot expose commands absent from the owning Service"
    );
}

#[tokio::test]
async fn thin_complete_registers_without_a_handler_context_body() {
    let repository = InMemoryRepository::new();
    let routes = Routes::new()
        .with_repo(repository.aggregate::<CausalDispatcherAggregate>())
        .typed_command(
            typed_command::<CausalTestInput, crate::command::Succeeded<TypedOutput>>("todo.create")
                .roles(["user"]),
        )
        .create()
        .invoke(|aggregate, input, _owner| {
            aggregate.record(input.id.clone())?;
            Ok::<_, crate::EventRecordError>(())
        })
        .succeeded(|aggregate, _principal| TypedOutput {
            id: aggregate.entity().id().to_string(),
        })
        .typed_command(
            typed_command::<CausalTestInput, crate::command::Succeeded<TypedOutput>>(
                "todo.complete",
            )
            .roles(["user"]),
        )
        .load_by(|input: &CausalTestInput| input.id.clone())
        .invoke(|aggregate, input, _owner| {
            aggregate.record(input.id.clone())?;
            Ok::<_, crate::EventRecordError>(())
        })
        .succeeded(|aggregate, _principal| TypedOutput {
            id: aggregate.entity().id().to_string(),
        });
    let specs = routes.command_specs().expect("thin commands compile");
    assert!(specs.iter().any(|spec| spec.id == "todo.complete"));
    assert!(specs.iter().any(|spec| spec.id == "todo.create"));

    let service = Service::new().named("thin-complete").routes(routes);
    let mut session = session_with_role("user");
    session.set(crate::microsvc::USER_ID_KEY, "alice");
    let principal = causal_test_principal();

    let created = service
        .dispatch_causal(
            "todo.create",
            &causal_test_command_id(),
            causal_test_input("todo-1", "new"),
            session.clone(),
            principal.clone(),
        )
        .await
        .expect("thin create should commit");
    assert_eq!(created, json!({ "id": "todo-1" }));

    let completed = service
        .dispatch_causal(
            "todo.complete",
            &causal_test_command_id(),
            causal_test_input("todo-1", "done"),
            session,
            principal,
        )
        .await
        .expect("thin complete should load, invoke, and commit");
    assert_eq!(completed, json!({ "id": "todo-1" }));
}

fn thin_route<D>(
    routes: Routes<D>,
    name: &'static str,
) -> crate::microsvc::TypedRouteBuilder<D, CausalTestInput, Succeeded<TypedOutput>>
where
    D: crate::microsvc::CausalRouteDependencies<Aggregate = CausalDispatcherAggregate>
        + Send
        + Sync
        + 'static,
{
    routes.typed_command(
        typed_command::<CausalTestInput, Succeeded<TypedOutput>>(name).roles(["user"]),
    )
}

#[tokio::test]
async fn thin_commands_share_one_load_rule_guard_and_principal() {
    fn record(
        aggregate: &mut crate::microsvc::AggregateCheckout<CausalDispatcherAggregate>,
        input: &CausalTestInput,
        _principal: &str,
    ) -> Result<(), crate::EventRecordError> {
        aggregate.record(input.id.clone())
    }
    let id = |input: &CausalTestInput| input.id.clone();
    // The payload reports the principal, so the test can see what it was given.
    let who = |_: &crate::microsvc::AggregateCheckout<CausalDispatcherAggregate>,
               principal: &str| TypedOutput {
        id: principal.to_string(),
    };

    let routes =
        Routes::new().with_repo(InMemoryRepository::new().aggregate::<CausalDispatcherAggregate>());
    let routes = thin_route(routes, "todo.create")
        .create_by(id)
        .invoke(record)
        .succeeded(who);
    let routes = thin_route(routes, "todo.touch")
        .load_or_create_by(id)
        .invoke(record)
        .succeeded(who);
    let routes = thin_route(routes, "todo.complete")
        .load_by(id)
        .invoke(record)
        .succeeded(who);
    let routes = thin_route(routes, "todo.locked")
        .load_by(id)
        .guard(|_| false)
        .invoke(record)
        .succeeded(who);
    let service = Service::new().named("thin-load").routes(routes);
    let dispatch = |command: &'static str, id: &'static str| {
        let service = &service;
        async move {
            service
                .dispatch_causal(
                    command,
                    &causal_test_command_id(),
                    causal_test_input(id, "x"),
                    session_with_role("user"),
                    causal_test_principal(),
                )
                .await
        }
    };

    let principal = json!({ "id": "causal-test-subject" });
    assert_eq!(dispatch("todo.create", "todo-1").await.unwrap(), principal);
    let duplicate = dispatch("todo.create", "todo-1").await.unwrap_err();
    assert!(
        duplicate.to_string().contains("todo-1 already exists"),
        "create over an existing stream must reject: {duplicate}"
    );
    assert_eq!(dispatch("todo.touch", "todo-1").await.unwrap(), principal);
    assert_eq!(dispatch("todo.touch", "todo-2").await.unwrap(), principal);
    assert_eq!(
        dispatch("todo.complete", "todo-2").await.unwrap(),
        principal
    );
    let missing = dispatch("todo.complete", "todo-3").await.unwrap_err();
    assert!(
        missing.to_string() == "not found: todo-3",
        "a required load of a missing stream is NotFound: {missing}"
    );
    let refused = dispatch("todo.locked", "todo-1").await.unwrap_err();
    assert!(
        refused.to_string().contains("guard"),
        "a thin command's guard must refuse: {refused:?}"
    );
}

#[tokio::test]
async fn causal_dispatch_replays_canonical_equivalent_input_without_reinvoking_handler() {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let route_handler_calls = Arc::clone(&handler_calls);
    let repository = InMemoryRepository::new();
    let service = Service::new().named("causal-tests").routes(
        Routes::new()
            .with_repo(repository.clone().aggregate::<CausalDispatcherAggregate>())
            .typed_command(typed_command::<CausalTestInput, Succeeded<TypedOutput>>(
                "causal.replay",
            ))
            .handle(
                move |_context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                      input: CausalTestInput| {
                    let calls = Arc::clone(&route_handler_calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let _label = input.label;
                        Ok(
                            PreparedCommand::<Succeeded<TypedOutput>>::prepare(TypedOutput {
                                id: input.id,
                            })
                            .unwrap(),
                        )
                    }
                },
            ),
    );
    let command_id = causal_test_command_id();
    let principal = causal_test_principal();
    let first_input = serde_json::from_str(r#"{"id":"todo-1","label":"same"}"#).unwrap();
    let equivalent_input = serde_json::from_str(r#"{"label":"same","id":"todo-1"}"#).unwrap();

    let first = service
        .dispatch_causal(
            "causal.replay",
            &command_id,
            first_input,
            Session::new(),
            principal.clone(),
        )
        .await
        .unwrap();
    let replay = service
        .dispatch_causal(
            "causal.replay",
            &command_id,
            equivalent_input,
            Session::new(),
            principal,
        )
        .await
        .unwrap();

    assert_eq!(first, json!({ "id": "todo-1" }));
    assert_eq!(replay, first);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
}

/// The command host is core: it runs and replays a typed command in every
/// build, the GraphQL transport included or not.
#[tokio::test]
async fn service_command_host_runs_typed_commands_through_the_ledger() {
    use crate::command_dispatch::CommandHost;

    let service = Arc::new(
        Service::new().named("causal-tests").routes(
            Routes::new()
                .with_repo(InMemoryRepository::new().aggregate::<CausalDispatcherAggregate>())
                .typed_command(typed_command::<CausalTestInput, Succeeded<TypedOutput>>(
                    "causal.host",
                ))
                .handle(
                    |_context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                     input: CausalTestInput| async move {
                        Ok(
                            PreparedCommand::<Succeeded<TypedOutput>>::prepare(TypedOutput {
                                id: input.id,
                            })
                            .unwrap(),
                        )
                    },
                ),
        ),
    );
    let host = Arc::clone(&service);
    assert!(host
        .ledger()
        .is_some_and(|ledger| std::ptr::eq(ledger, service.as_ref())));

    let mut session = Session::new();
    session.set(crate::microsvc::USER_ID_KEY, "causal-test-subject");
    let command_id = causal_test_command_id();
    let invoke = || {
        host.invoke(
            "causal.host",
            &command_id,
            causal_test_input("todo-host", "label"),
            session.clone(),
            causal_test_principal(),
            None,
        )
    };

    let first = invoke().await.expect("the host runs the command");
    assert_eq!(first.payload(), &json!({ "id": "todo-host" }));
    assert_eq!(first.state(), "succeeded");
    assert!(!first.replayed());

    let replay = invoke().await.expect("the ledger replays the command ID");
    assert!(replay.replayed());
    assert_eq!(replay.causation_id(), first.causation_id());
}

#[cfg(feature = "graphql")]
#[tokio::test]
async fn causal_dispatch_receipt_and_status_use_the_exact_durable_replay() {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let route_handler_calls = Arc::clone(&handler_calls);
    let service = Service::new().named("causal-tests").routes(
        Routes::new()
            .with_repo(InMemoryRepository::new().aggregate::<CausalDispatcherAggregate>())
            .typed_command(typed_command::<CausalTestInput, Succeeded<TypedOutput>>(
                "causal.receipt",
            ))
            .handle(
                move |_context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                      input: CausalTestInput| {
                    let calls = Arc::clone(&route_handler_calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(
                            PreparedCommand::<Succeeded<TypedOutput>>::prepare(TypedOutput {
                                id: input.id,
                            })
                            .unwrap(),
                        )
                    }
                },
            ),
    );
    let command_id = causal_test_command_id();
    let principal = causal_test_principal();

    let first = service
        .dispatch_causal_with_receipt(
            "causal.receipt",
            &command_id,
            causal_test_input("todo-receipt", "first"),
            Session::new(),
            principal.clone(),
        )
        .await
        .expect("fresh dispatch should return its durable receipt source");
    let replay = service
        .dispatch_causal_with_receipt(
            "causal.receipt",
            &command_id,
            causal_test_input("todo-receipt", "first"),
            Session::new(),
            principal.clone(),
        )
        .await
        .expect("response-loss retry should recover the same receipt source");

    assert!(!first.replayed(), "the first dispatch ran the handler");
    assert!(replay.replayed(), "the retry replays the durable receipt");
    assert_eq!(first, replay.clone().fresh());
    assert_eq!(first.payload, json!({ "id": "todo-receipt" }));
    assert_eq!(first.receipt.command_id, command_id);
    assert_eq!(first.receipt.state, CommandLedgerState::Succeeded);
    assert_eq!(first.receipt.consistency, CommandConsistency::Succeeded);
    assert!(first.receipt.direct_projection.is_none());
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);

    let status = service
        .causal_command_status(&command_id, &Session::new(), principal, None)
        .await
        .expect("same principal and current grant should resolve status");
    assert_eq!(status.state, CausalCommandPublicState::Succeeded);
    assert_eq!(status.command_id, first.receipt.command_id);
    assert_eq!(
        status.causation_id.as_deref(),
        Some(first.receipt.causation_id.as_str())
    );
    assert_eq!(status.consistency, Some(CommandConsistency::Succeeded));
    assert_eq!(status.outcome, Some(first.payload));
    assert!(status.evidence.is_empty());
    assert!(status.direct_projection.is_none());
}

/// An eventual command needs modeled projection metadata for its terminal
/// receipt. Dispatched without a GraphQL request authority there is none, so
/// the attempt's completion is refused after the reservation was acquired.
/// That exit must release the attempt: the retry runs the handler again
/// instead of answering `InProgress` until the attempt lease expires.
#[tokio::test]
async fn a_failure_after_the_reservation_releases_the_attempt_for_an_immediate_retry() {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let route_calls = Arc::clone(&handler_calls);
    let service = Service::new().named("causal-tests").routes(
        Routes::new()
            .with_repo(InMemoryRepository::new().aggregate::<CausalDispatcherAggregate>())
            .typed_command(
                typed_command::<CausalTestInput, Eventual<TypedOutput>>("causal.unsealed")
                    .emits(crate::events![CausalLifecycleRecorded]),
            )
            .handle(
                move |context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                      input: CausalTestInput| {
                    let calls = Arc::clone(&route_calls);
                    let result = (|| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let mut checkout = context.create();
                        checkout.record_lifecycle(input.id.clone(), input.label)?;
                        context
                            .publish_events()
                            .commit(checkout)?
                            .eventual(TypedOutput { id: input.id })
                    })();
                    async move { result }
                },
            ),
    );
    let command_id = causal_test_command_id();
    let dispatch = || {
        service.dispatch_causal(
            "causal.unsealed",
            &command_id,
            causal_test_input("todo-unsealed", "first"),
            Session::new(),
            causal_test_principal(),
        )
    };

    let first = dispatch()
        .await
        .expect_err("the receipt cannot be completed");
    assert!(
        matches!(&first, CausalDispatchError::Internal(detail)
            if detail.contains("requires modeled projection metadata")),
        "unexpected first failure: {first:?}"
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);

    let retry = dispatch().await.expect_err("the retry fails the same way");
    assert!(
        matches!(&retry, CausalDispatchError::Internal(detail)
            if detail.contains("requires modeled projection metadata")),
        "the retry must run again, not find the first attempt in progress: {retry:?}"
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 2);
}

#[cfg(feature = "graphql")]
#[test]
fn causal_status_projection_failure_precedes_observed_and_pending_evidence() {
    let item = |obligation_index, state| CausalCommandProjectionEvidence {
        obligation_index,
        state,
        record: None,
    };

    assert_eq!(
        collapse_projection_evidence(&[
            item(0, CausalProjectionEvidenceState::Observed),
            item(1, CausalProjectionEvidenceState::TerminalFailure),
            item(2, CausalProjectionEvidenceState::Pending),
        ]),
        CausalCommandPublicState::ProjectionFailed
    );
    assert_eq!(
        collapse_projection_evidence(&[
            item(0, CausalProjectionEvidenceState::Observed),
            item(1, CausalProjectionEvidenceState::Observed),
        ]),
        CausalCommandPublicState::Atomic
    );
    assert_eq!(
        collapse_projection_evidence(&[
            item(0, CausalProjectionEvidenceState::Observed),
            item(1, CausalProjectionEvidenceState::Pending),
        ]),
        CausalCommandPublicState::SucceededPendingProjection
    );
    assert_eq!(
        collapse_projection_evidence(&[]),
        CausalCommandPublicState::SucceededPendingProjection
    );
}

#[tokio::test]
async fn causal_dispatch_rejects_same_command_id_with_different_input() {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let route_handler_calls = Arc::clone(&handler_calls);
    let repository = InMemoryRepository::new();
    let service = Service::new().named("causal-tests").routes(
        Routes::new()
            .with_repo(repository.clone().aggregate::<CausalDispatcherAggregate>())
            .typed_command(typed_command::<CausalTestInput, Succeeded<TypedOutput>>(
                "causal.reuse",
            ))
            .handle(
                move |_context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                      input: CausalTestInput| {
                    let calls = Arc::clone(&route_handler_calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let _label = input.label;
                        Ok(
                            PreparedCommand::<Succeeded<TypedOutput>>::prepare(TypedOutput {
                                id: input.id,
                            })
                            .unwrap(),
                        )
                    }
                },
            ),
    );
    let command_id = causal_test_command_id();
    let principal = causal_test_principal();

    service
        .dispatch_causal(
            "causal.reuse",
            &command_id,
            causal_test_input("todo-1", "first"),
            Session::new(),
            principal.clone(),
        )
        .await
        .unwrap();
    let error = service
        .dispatch_causal(
            "causal.reuse",
            &command_id,
            causal_test_input("todo-1", "changed"),
            Session::new(),
            principal,
        )
        .await
        .expect_err("different canonical input must conflict");

    assert_eq!(error.code(), "COMMAND_ID_REUSE");
    assert_eq!(error.status_code(), 409);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn causal_guard_rejection_is_replayed_without_guard_or_handler_callback() {
    let guard_calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let route_guard_calls = Arc::clone(&guard_calls);
    let route_handler_calls = Arc::clone(&handler_calls);
    let repository = InMemoryRepository::new();
    let service = Service::new().named("causal-tests").routes(
        Routes::new()
            .with_repo(repository.clone().aggregate::<CausalDispatcherAggregate>())
            .typed_command(typed_command::<CausalTestInput, Succeeded<TypedOutput>>(
                "causal.guard_rejection",
            ))
            .guarded(
                move |_| {
                    route_guard_calls.fetch_add(1, Ordering::SeqCst);
                    false
                },
                move |_context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                      input: CausalTestInput| {
                    let calls = Arc::clone(&route_handler_calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(
                            PreparedCommand::<Succeeded<TypedOutput>>::prepare(TypedOutput {
                                id: input.id,
                            })
                            .unwrap(),
                        )
                    }
                },
            ),
    );
    let command_id = causal_test_command_id();
    let principal = causal_test_principal();

    let first = service
        .dispatch_causal(
            "causal.guard_rejection",
            &command_id,
            causal_test_input("todo-1", "same"),
            Session::new(),
            principal.clone(),
        )
        .await
        .expect_err("guard should reject first attempt");
    let replay = service
        .dispatch_causal(
            "causal.guard_rejection",
            &command_id,
            causal_test_input("todo-1", "same"),
            Session::new(),
            principal,
        )
        .await
        .expect_err("guard rejection should replay");

    assert_eq!(first.code(), "REJECTED");
    assert_eq!(first.status_code(), 422);
    assert_eq!(replay.code(), first.code());
    assert_eq!(replay.status_code(), first.status_code());
    assert_eq!(replay.client_message(), first.client_message());
    assert_eq!(guard_calls.load(Ordering::SeqCst), 1);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn causal_handler_rejection_is_replayed_without_reinvoking_handler() {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let route_handler_calls = Arc::clone(&handler_calls);
    let repository = InMemoryRepository::new();
    let service = Service::new().named("causal-tests").routes(
        Routes::new()
            .with_repo(repository.clone().aggregate::<CausalDispatcherAggregate>())
            .typed_command(typed_command::<CausalTestInput, Succeeded<TypedOutput>>(
                "causal.handler_rejection",
            ))
            .handle(
                move |_context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                      _input: CausalTestInput| {
                    let calls = Arc::clone(&route_handler_calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err::<PreparedCommand<Succeeded<TypedOutput>>, HandlerError>(
                            HandlerError::Rejected("deterministic refusal".into()),
                        )
                    }
                },
            ),
    );
    let command_id = causal_test_command_id();
    let principal = causal_test_principal();

    let first = service
        .dispatch_causal(
            "causal.handler_rejection",
            &command_id,
            causal_test_input("todo-1", "same"),
            Session::new(),
            principal.clone(),
        )
        .await
        .expect_err("handler should reject first attempt");
    let replay = service
        .dispatch_causal(
            "causal.handler_rejection",
            &command_id,
            causal_test_input("todo-1", "same"),
            Session::new(),
            principal,
        )
        .await
        .expect_err("handler rejection should replay");

    assert_eq!(first.code(), "REJECTED");
    assert_eq!(first.status_code(), 422);
    assert_eq!(first.client_message(), "rejected: deterministic refusal");
    assert_eq!(replay.code(), first.code());
    assert_eq!(replay.status_code(), first.status_code());
    assert_eq!(replay.client_message(), first.client_message());
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn causal_dispatch_checks_current_role_before_reservation_guard_and_handler() {
    let guard_calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let route_guard_calls = Arc::clone(&guard_calls);
    let route_handler_calls = Arc::clone(&handler_calls);
    let repository = InMemoryRepository::new();
    let service = Service::new().named("causal-tests").routes(
        Routes::new()
            .with_repo(repository.clone().aggregate::<CausalDispatcherAggregate>())
            .typed_command(
                typed_command::<CausalTestInput, Succeeded<TypedOutput>>("causal.role_guarded")
                    .roles(["admin"]),
            )
            .guarded(
                move |_| {
                    route_guard_calls.fetch_add(1, Ordering::SeqCst);
                    true
                },
                move |_context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                      input: CausalTestInput| {
                    let calls = Arc::clone(&route_handler_calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(
                            PreparedCommand::<Succeeded<TypedOutput>>::prepare(TypedOutput {
                                id: input.id,
                            })
                            .unwrap(),
                        )
                    }
                },
            ),
    );
    let command_id = causal_test_command_id();
    let principal = causal_test_principal();

    let denied_before_reservation = service
        .dispatch_causal(
            "causal.role_guarded",
            &command_id,
            causal_test_input("todo-1", "same"),
            session_with_role("user"),
            principal.clone(),
        )
        .await
        .expect_err("current role must be denied before reservation");
    assert_eq!(denied_before_reservation.code(), "FORBIDDEN");
    assert_eq!(guard_calls.load(Ordering::SeqCst), 0);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 0);

    let accepted = service
        .dispatch_causal(
            "causal.role_guarded",
            &command_id,
            causal_test_input("todo-1", "same"),
            session_with_role("admin"),
            principal.clone(),
        )
        .await
        .expect("denied dispatch must not have reserved the command ID");
    assert_eq!(accepted, json!({ "id": "todo-1" }));

    let denied_before_replay = service
        .dispatch_causal(
            "causal.role_guarded",
            &command_id,
            causal_test_input("todo-1", "same"),
            session_with_role("user"),
            principal,
        )
        .await
        .expect_err("current role must be rechecked before replay");
    assert_eq!(denied_before_replay.code(), "FORBIDDEN");
    assert_eq!(guard_calls.load(Ordering::SeqCst), 1);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);

    let denied_lookup = service
        .lookup_causal_command(
            "causal.role_guarded",
            &command_id,
            &session_with_role("user"),
            causal_test_principal(),
        )
        .await
        .expect_err("current role must also be rechecked before status lookup");
    assert_eq!(denied_lookup.code(), "FORBIDDEN");
}

#[cfg(feature = "graphql")]
#[tokio::test]
async fn causal_status_lookup_does_not_disclose_another_routes_command() {
    let repository = InMemoryRepository::new();
    let service = Service::new().named("causal-tests").routes(
        Routes::new()
            .with_repo(repository.aggregate::<CausalDispatcherAggregate>())
            .typed_command(
                typed_command::<CausalTestInput, Succeeded<TypedOutput>>("causal.admin_only")
                    .roles(["admin"]),
            )
            .handle(
                |_context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                 input: CausalTestInput| async move {
                    Ok(
                        PreparedCommand::<Succeeded<TypedOutput>>::prepare(TypedOutput {
                            id: input.id,
                        })
                        .unwrap(),
                    )
                },
            )
            .typed_command(
                typed_command::<CausalTestInput, Succeeded<TypedOutput>>("causal.user_allowed")
                    .roles(["user"]),
            )
            .handle(
                |_context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                 input: CausalTestInput| async move {
                    Ok(
                        PreparedCommand::<Succeeded<TypedOutput>>::prepare(TypedOutput {
                            id: input.id,
                        })
                        .unwrap(),
                    )
                },
            ),
    );
    let command_id = causal_test_command_id();
    let principal = causal_test_principal();

    service
        .dispatch_causal(
            "causal.admin_only",
            &command_id,
            causal_test_input("todo-secret", "classified"),
            session_with_role("admin"),
            principal.clone(),
        )
        .await
        .expect("admin should be able to commit the protected command");

    let denied = service
        .lookup_causal_command(
            "causal.admin_only",
            &command_id,
            &session_with_role("user"),
            principal.clone(),
        )
        .await
        .expect_err("the current role must not retain access to the protected route");
    assert_eq!(denied.code(), "FORBIDDEN");

    let cross_route = service
        .lookup_causal_command(
            "causal.user_allowed",
            &command_id,
            &session_with_role("user"),
            principal.clone(),
        )
        .await
        .expect("the allowed route should produce a non-disclosing status result");
    assert_eq!(cross_route, CommandLookup::Unknown);

    let authorized = service
        .causal_command_status(
            &command_id,
            &session_with_role("admin"),
            principal.clone(),
            None,
        )
        .await
        .expect("current admin grant should recover the command without its route name");
    assert_eq!(authorized.state, CausalCommandPublicState::Succeeded);
    assert_eq!(authorized.command_id, command_id);

    let revoked = service
        .causal_command_status(
            &command_id,
            &session_with_role("user"),
            principal.clone(),
            None,
        )
        .await
        .expect("revoked routes must collapse to a non-enumerating status");
    assert_eq!(revoked.state, CausalCommandPublicState::Unknown);

    let other_principal = VerifiedPrincipal::test_oidc(
        "https://issuer.example/",
        "another-subject",
        &["distributed-tests"],
    );
    let wrong_principal = service
        .causal_command_status(
            &command_id,
            &session_with_role("admin"),
            other_principal,
            None,
        )
        .await
        .expect("another principal must not learn whether the command exists");
    assert_eq!(wrong_principal.state, CausalCommandPublicState::Unknown);

    let malformed = service
        .causal_command_status(
            "not-a-command-id",
            &session_with_role("admin"),
            principal.clone(),
            None,
        )
        .await
        .expect("malformed IDs are non-enumerating, not validation or storage errors");
    assert_eq!(malformed.state, CausalCommandPublicState::Unknown);
    assert_eq!(malformed.command_id, "not-a-command-id");

    let missing_id = causal_test_command_id();
    let missing = service
        .causal_command_status(&missing_id, &session_with_role("admin"), principal, None)
        .await
        .expect("absent IDs are non-enumerating");
    assert_eq!(missing.state, CausalCommandPublicState::Unknown);
}

#[tokio::test]
async fn causal_dispatch_overwrites_event_and_outbox_causation_with_ledger_identity() {
    let observed_causation = Arc::new(Mutex::new(None::<String>));
    let route_observed_causation = Arc::clone(&observed_causation);
    let projector_causation = Arc::new(Mutex::new(None::<String>));
    let route_projector_causation = Arc::clone(&projector_causation);
    let repository = InMemoryRepository::new();
    let service = Service::new().named("causal-tests").routes(
        Routes::new()
            .with_repo(repository.clone().aggregate::<CausalDispatcherAggregate>())
            .typed_command(typed_command::<CausalTestInput, Succeeded<TypedOutput>>(
                "causal.persist",
            ))
            .handle(
                move |context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                      input: CausalTestInput| {
                    let observed = Arc::clone(&route_observed_causation);
                    let result = (|| {
                        let causation = context
                            .causation_id()
                            .expect("reserved command causation")
                            .to_string();
                        *observed.lock().unwrap() = Some(causation);

                        let mut checkout = context.create();
                        checkout
                            .entity_mut()
                            .set_causation_id("handler-supplied-event-causation");
                        checkout
                            .record(input.id.clone())
                            .map_err(|error| HandlerError::Other(Box::new(error)))?;

                        let mut outbox = OutboxMessage::create(
                            format!("{}:fact", input.id),
                            "causal.recorded",
                            input.label.as_bytes().to_vec(),
                        )
                        .map_err(|error| HandlerError::Other(Box::new(error)))?;
                        outbox.set_causation_id("handler-supplied-outbox-causation");
                        context
                            .outbox(outbox)
                            .commit(checkout)?
                            .succeeded(TypedOutput { id: input.id })
                    })();
                    async move { result }
                },
            )
            .event("causal.recorded")
            .handle(
                move |context: &Context<
                    AggregateRepository<InMemoryRepository, CausalDispatcherAggregate>,
                >| {
                    let causation = context.message().causation_id().map(str::to_string);
                    let observed = Arc::clone(&route_projector_causation);
                    async move {
                        *observed.lock().unwrap() = causation;
                        Ok(json!({ "projected": true }))
                    }
                },
            ),
    );
    let command_id = causal_test_command_id();
    let mut session = Session::new();
    session.set(
        crate::trace_context::CAUSATION_ID,
        "caller-supplied-causation",
    );
    session.set(crate::trace_context::CORRELATION_ID, "caller-correlation");
    session.set(
        crate::trace_context::TRACEPARENT,
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    );
    session.set(crate::trace_context::TRACESTATE, "vendor=value");

    let result = service
        .dispatch_causal(
            "causal.persist",
            &command_id,
            causal_test_input("todo-causal", "payload"),
            session,
            causal_test_principal(),
        )
        .await
        .unwrap();
    assert_eq!(result, json!({ "id": "todo-causal" }));

    let causation = observed_causation
        .lock()
        .unwrap()
        .clone()
        .expect("handler observed causation");
    let parsed_causation = uuid::Uuid::parse_str(&causation).unwrap();
    assert_eq!(parsed_causation.get_version_num(), 7);
    assert_ne!(causation, command_id);
    assert_ne!(causation, "caller-supplied-causation");
    assert_ne!(causation, "handler-supplied-event-causation");
    assert_ne!(causation, "handler-supplied-outbox-causation");

    let identity =
        crate::StreamIdentity::new(CausalDispatcherAggregate::aggregate_type(), "todo-causal")
            .unwrap();
    let stored = repository
        .get_stream(&identity)
        .await
        .unwrap()
        .expect("causal aggregate stream");
    assert_eq!(stored.events().len(), 1);
    assert_eq!(stored.events()[0].causation_id(), Some(causation.as_str()));

    let outbox_store = repository.outbox_store();
    let pending = outbox_store.pending(10).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].causation_id(), Some(causation.as_str()));

    let projector_input = Message::from(pending[0].clone());
    service.dispatch_message(&projector_input).await.unwrap();
    assert_eq!(
        projector_causation.lock().unwrap().as_deref(),
        Some(causation.as_str())
    );
}

#[tokio::test]
async fn causal_dispatch_uses_the_configured_immediate_outbox_publisher() {
    let repository = InMemoryRepository::new();
    let observed_broker_metadata = Arc::new(Mutex::new(None::<[String; 6]>));
    let route_observed_broker_metadata = Arc::clone(&observed_broker_metadata);
    let bus = crate::bus::InMemoryBus::new();
    let service = Service::new()
        .named("causal-tests")
        .routes(
            Routes::new()
                .with_repo(repository.clone().aggregate::<CausalDispatcherAggregate>())
                .typed_command(typed_command::<CausalTestInput, Succeeded<TypedOutput>>(
                    "causal.publish_immediately",
                ))
                .handle(
                    |context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                     input: CausalTestInput| {
                        let result = (|| {
                            let mut checkout = context.create();
                            checkout
                                .record(input.id.clone())
                                .map_err(|error| HandlerError::Other(Box::new(error)))?;
                            let outbox = OutboxMessage::create(
                                format!("{}:immediate-fact", input.id),
                                "causal.immediate_fact",
                                input.label.as_bytes().to_vec(),
                            )
                            .map_err(|error| HandlerError::Other(Box::new(error)))?;
                            context
                                .outbox(outbox)
                                .commit(checkout)?
                                .succeeded(TypedOutput { id: input.id })
                        })();
                        async move { result }
                    },
                )
                .event("causal.immediate_fact")
                .handle(
                    move |context: &Context<
                        AggregateRepository<InMemoryRepository, CausalDispatcherAggregate>,
                    >| {
                        let message = context.message();
                        let metadata = [
                            message.id().expect("published message ID").to_string(),
                            message.name().to_string(),
                            message.causation_id().unwrap_or_default().to_string(),
                            message
                                .metadata("x-distributed-source-aggregate-type")
                                .unwrap_or_default()
                                .to_string(),
                            message
                                .metadata("x-distributed-source-aggregate-id")
                                .unwrap_or_default()
                                .to_string(),
                            message
                                .metadata("x-distributed-source-sequence")
                                .unwrap_or_default()
                                .to_string(),
                        ];
                        let observed = Arc::clone(&route_observed_broker_metadata);
                        async move {
                            *observed.lock().unwrap() = Some(metadata);
                            Ok(json!({ "projected": true }))
                        }
                    },
                ),
        )
        .with_bus(bus.clone());

    service
        .dispatch_causal(
            "causal.publish_immediately",
            &causal_test_command_id(),
            causal_test_input("todo-immediate", "payload"),
            Session::new(),
            causal_test_principal(),
        )
        .await
        .expect("causal dispatch should commit before immediate publication");

    let outbox = repository.outbox_store();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if !outbox.pending(usize::MAX).await.unwrap().is_empty() {
                tokio::task::yield_now().await;
                continue;
            }
            if outbox
                .messages_by_status(crate::outbox::OutboxMessageStatus::InFlight, usize::MAX)
                .await
                .unwrap()
                .is_empty()
                && bus
                    .published_ids()
                    .contains(&"todo-immediate:immediate-fact".to_string())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("immediate publish should settle the causal outbox row");
    assert!(
        outbox.pending(usize::MAX).await.unwrap().is_empty(),
        "delivered messages are deleted, not retained as evidence"
    );
    let stream = repository
        .get_stream(
            &crate::StreamIdentity::new(
                CausalDispatcherAggregate::aggregate_type(),
                "todo-immediate",
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let causation = stream.events()[0]
        .causation_id()
        .expect("committed event carries ledger causation")
        .to_string();

    service
        .run(RunOptions::until_idle())
        .await
        .expect("attached bus should deliver the immediately published fact");
    assert_eq!(
        observed_broker_metadata.lock().unwrap().as_ref(),
        Some(&[
            "todo-immediate:immediate-fact".to_string(),
            "causal.immediate_fact".to_string(),
            causation,
            CausalDispatcherAggregate::aggregate_type().to_string(),
            "todo-immediate".to_string(),
            "1".to_string(),
        ]),
        "the post-commit clone must carry authoritative causation and aggregate source metadata",
    );
}

#[tokio::test]
async fn causal_prepared_batch_failure_rolls_back_every_atomic_participant() {
    use crate::{CommitBatch, SnapshotStore, TransactionalCommit};

    let aggregate_id = "atomic-rollback-aggregate";
    let repository = InMemoryRepository::new();
    // An outbox row already holds the id the command stages, so its batch
    // fails after the stream, snapshot and outbox legs are staged.
    repository
        .commit_batch(CommitBatch {
            outbox_messages: vec![OutboxMessage::create(
                format!("{aggregate_id}:atomic-fact"),
                "causal.atomic_fact",
                b"earlier".to_vec(),
            )
            .unwrap()],
            ..CommitBatch::empty()
        })
        .await
        .unwrap();
    let service = Service::new().named("causal-atomic-rollback").routes(
        Routes::new()
            .with_repo(
                repository
                    .clone()
                    .aggregate::<CausalDispatcherAggregate>()
                    .with_snapshots(1),
            )
            .typed_command(typed_command::<CausalTestInput, Succeeded<TypedOutput>>(
                "causal.atomic_rollback",
            ))
            .handle(
                |context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                 input: CausalTestInput| {
                    let result = (|| {
                        let mut checkout = context.create();
                        checkout
                            .record(input.id.clone())
                            .map_err(|error| HandlerError::Other(Box::new(error)))?;

                        let outbox = OutboxMessage::create(
                            format!("{}:atomic-fact", input.id),
                            "causal.atomic_fact",
                            input.label.as_bytes().to_vec(),
                        )
                        .map_err(|error| HandlerError::Other(Box::new(error)))?;

                        context
                            .outbox(outbox)
                            .commit(checkout)?
                            .succeeded(TypedOutput { id: input.id })
                    })();
                    async move { result }
                },
            ),
    );
    let command_id = causal_test_command_id();
    let principal = causal_test_principal();

    let error = service
        .dispatch_causal(
            "causal.atomic_rollback",
            &command_id,
            causal_test_input(aggregate_id, "payload"),
            Session::new(),
            principal.clone(),
        )
        .await
        .expect_err("the duplicate outbox id must fail the batch");
    assert_eq!(error.code(), "INTERNAL");

    let identity =
        crate::StreamIdentity::new(CausalDispatcherAggregate::aggregate_type(), aggregate_id)
            .unwrap();
    assert!(
        repository.get_stream(&identity).await.unwrap().is_none(),
        "aggregate stream must roll back"
    );
    let pending = repository.outbox_store().pending(usize::MAX).await.unwrap();
    assert_eq!(pending.len(), 1, "the command's outbox row must roll back");
    assert_eq!(pending[0].payload, b"earlier");
    assert!(
        repository.get_snapshot(&identity).await.unwrap().is_none(),
        "snapshot must roll back"
    );
    assert!(matches!(
        service
            .lookup_causal_command(
                "causal.atomic_rollback",
                &command_id,
                &Session::new(),
                principal,
            )
            .await
            .unwrap(),
        CommandLookup::RetryableUnknown { .. }
    ));
}

#[cfg(all(feature = "graphql", feature = "sqlite"))]
#[tokio::test]
async fn succeeded_causal_dispatch_replays_and_stages_its_outbox_fact() {
    let repository = crate::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("framework migrations should apply");
    let service = Service::new().named("causal-tests").routes(
        Routes::new()
            .with_repo(repository.clone().aggregate::<CausalDispatcherAggregate>())
            .typed_command(
                typed_command::<CausalProjectionInput, Succeeded<TypedOutput>>(
                    "causal.projection_obligation",
                ),
            )
            .handle(
                |context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                 input: CausalProjectionInput| {
                    let result = (|| {
                        context.stage_outbox(
                            OutboxMessage::create(
                                format!("{}:obligation", input.id),
                                "causal.obligation_fact",
                                serde_json::to_vec(&json!({
                                    "tenantPartition": input.partition
                                }))
                                .map_err(|error| HandlerError::Other(Box::new(error)))?,
                            )
                            .map_err(|error| HandlerError::Other(Box::new(error)))?,
                        )?;
                        Ok(
                            PreparedCommand::<Succeeded<TypedOutput>>::prepare(TypedOutput {
                                id: input.id,
                            })
                            .unwrap(),
                        )
                    })();
                    async move { result }
                },
            ),
    );
    let engine = crate::graphql::GraphqlEngine::builder(&repository)
        .identity(crate::graphql::IdentityConfig::dev_headers())
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .model::<CausalProjectionObligationView>(
            crate::graphql::ModelPermissions::new()
                .grant("anonymous", crate::graphql::read().all_columns()),
        )
        .service(&service)
        .build()
        .expect("succeeded command without separate confirmations should compile");
    let service = service
        .try_with_graphql(engine)
        .expect("compiled service should bind");
    let command_id = causal_test_command_id();
    let principal = causal_test_principal();

    let result = service
        .dispatch_causal(
            "causal.projection_obligation",
            &command_id,
            json!({
                "todoId": "todo-obligation",
                "tenantPartition": "tenant-a"
            }),
            Session::new(),
            principal.clone(),
        )
        .await
        .expect("dispatch should commit");
    assert_eq!(result, json!({ "id": "todo-obligation" }));

    let status = service
        .causal_command_status(&command_id, &Session::new(), principal.clone(), None)
        .await
        .expect("status should load");
    assert_eq!(status.consistency, Some(CommandConsistency::Succeeded));

    let lookup = service
        .lookup_causal_command(
            "causal.projection_obligation",
            &command_id,
            &Session::new(),
            principal,
        )
        .await
        .expect("same principal should be able to recover its command");
    assert!(
        matches!(lookup, CommandLookup::Replay(_)),
        "completed command should be replayable"
    );

    let pending = repository.outbox_store().pending(10).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event_type, "causal.obligation_fact");
}

#[cfg(all(feature = "graphql", feature = "sqlite"))]
#[tokio::test]
async fn graphql_terminal_replay_revalidates_after_active_projection_starts_draining() {
    let repository = crate::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("framework migrations should apply");
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let build_service = |projector: SurfaceProjector| {
        let route_calls = Arc::clone(&handler_calls);
        Service::new().named("causal-lifecycle").routes(
            Routes::new()
                .with_repo(repository.clone().aggregate::<CausalDispatcherAggregate>())
                .with_read_model_store(repository.clone())
                .typed_command(
                    typed_command::<CausalTestInput, Eventual<TypedOutput>>("causal.lifecycle")
                        .roles(["user"])
                        .emits(crate::events![CausalLifecycleRecorded]),
                )
                .handle(
                    move |context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                          input: CausalTestInput| {
                        let calls = Arc::clone(&route_calls);
                        let result = (|| {
                            calls.fetch_add(1, Ordering::SeqCst);
                            let mut checkout = context.create();
                            checkout.record_lifecycle(input.id.clone(), input.label)?;
                            context
                                .publish_events()
                                .commit(checkout)?
                                .eventual(TypedOutput { id: input.id })
                        })();
                        async move { result }
                    },
                )
                .consume_projection(projector),
        )
    };
    let attach = |service: Service, projector: SurfaceProjector| {
        let opaque_fallback = SurfaceProjector::new("project_causal_lifecycle_opaque")
            .facts(["causal.lifecycle-recorded"])
            .models(["CausalProjectionSiblingView"]);
        let engine = crate::graphql::GraphqlEngine::builder(&repository)
            .identity(crate::graphql::IdentityConfig::dev_headers())
            .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
            .model::<CausalLifecycleView>(
                crate::graphql::ModelPermissions::new()
                    .grant("user", crate::graphql::read().all_columns()),
            )
            .model::<CausalProjectionSiblingView>(
                crate::graphql::ModelPermissions::new()
                    .grant("user", crate::graphql::read().all_columns()),
            )
            .service(&service)
            .client_projectors([projector, opaque_fallback])
            .build()
            .expect("lifecycle GraphQL engine should build");
        service.try_with_graphql(engine)
    };
    let active_projector = modeled_lifecycle_projector(
        crate::projection::placement::ProjectionBindingState::Active,
        "causal-lifecycle",
        "causal-lifecycle-topology",
        0x6c,
    );
    let active_service = Arc::new(
        attach(build_service(active_projector.clone()), active_projector)
            .expect("active modeled projection should attach"),
    );
    let command_id = causal_test_command_id();
    let mutation = format!(
        "mutation {{ causal_lifecycle(commandId: \"{command_id}\", input: {{ id: \"todo-lifecycle\", label: \"active\" }}) {{ id }} }}"
    );
    let session = session_with_role("user");
    let principal = causal_test_principal();
    let active_response = active_service
        .graphql_engine()
        .unwrap()
        .execute(
            &session,
            async_graphql::Request::new(&mutation)
                .data(command_host(&active_service))
                .data(principal.clone()),
        )
        .await;
    assert!(active_response.errors.is_empty(), "{active_response:?}");
    let active_data = active_response.data.clone().into_json().unwrap();
    let active_envelope = serde_json::to_value(
        active_response
            .extensions
            .get("distributed")
            .expect("active response should carry the protocol envelope"),
    )
    .unwrap();
    assert_eq!(
        active_data,
        json!({"causal_lifecycle": {"id": "todo-lifecycle"}})
    );
    assert_eq!(
        active_envelope["command"]["state"],
        "succeeded_pending_projection"
    );
    assert!(active_envelope["command"].get("projection").is_some());
    assert_eq!(
        active_envelope["command"]["projection"]["revalidate"], true,
        "the mixed opaque owner retains its fallback hint while Active modeled work applies"
    );
    assert!(active_envelope["command"]
        .get("projectionDisposition")
        .is_none());
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);

    let exact_replay_response = active_service
        .graphql_engine()
        .unwrap()
        .execute(
            &session,
            async_graphql::Request::new(&mutation)
                .data(command_host(&active_service))
                .data(principal.clone()),
        )
        .await;
    assert!(
        exact_replay_response.errors.is_empty(),
        "{exact_replay_response:?}"
    );
    assert_eq!(
        exact_replay_response.data.clone().into_json().unwrap(),
        active_data
    );
    let exact_replay_envelope = serde_json::to_value(
        exact_replay_response
            .extensions
            .get("distributed")
            .expect("exact terminal replay should carry the protocol envelope"),
    )
    .unwrap();
    assert!(exact_replay_envelope["command"].get("projection").is_some());
    assert!(exact_replay_envelope["command"]
        .get("projectionDisposition")
        .is_none());
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);

    let draining_projector = modeled_lifecycle_projector(
        crate::projection::placement::ProjectionBindingState::Draining,
        "causal-lifecycle",
        "causal-lifecycle-topology",
        0x6c,
    );
    let draining_service = Arc::new(
        attach(
            build_service(draining_projector.clone()),
            draining_projector,
        )
        .expect("draining modeled projection should attach for replay"),
    );
    let replay_response = draining_service
        .graphql_engine()
        .unwrap()
        .execute(
            &session,
            async_graphql::Request::new(mutation)
                .data(command_host(&draining_service))
                .data(principal.clone()),
        )
        .await;
    assert!(replay_response.errors.is_empty(), "{replay_response:?}");
    let replay_data = replay_response.data.clone().into_json().unwrap();
    let replay_envelope = serde_json::to_value(
        replay_response
            .extensions
            .get("distributed")
            .expect("terminal replay should carry the protocol envelope"),
    )
    .unwrap();
    assert_eq!(replay_data, active_data);
    assert_eq!(
        replay_envelope["command"]["state"],
        active_envelope["command"]["state"]
    );
    assert_eq!(
        replay_envelope["command"]["causationId"],
        active_envelope["command"]["causationId"]
    );
    assert_ne!(replay_envelope["cacheScope"], active_envelope["cacheScope"]);
    assert_eq!(
        replay_envelope["command"]["projectionDisposition"],
        "revalidate"
    );
    assert_eq!(replay_envelope["command"]["expects"], json!([]));
    assert!(replay_envelope["command"].get("projection").is_none());
    assert!(replay_envelope["command"].get("observations").is_none());
    assert!(replay_envelope["command"].get("records").is_none());
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);

    let fresh_command_id = causal_test_command_id();
    let fresh_mutation = format!(
        "mutation {{ causal_lifecycle(commandId: \"{fresh_command_id}\", input: {{ id: \"todo-lifecycle-fresh\", label: \"draining\" }}) {{ id }} }}"
    );
    let fresh_response = draining_service
        .graphql_engine()
        .unwrap()
        .execute(
            &session,
            async_graphql::Request::new(fresh_mutation)
                .data(command_host(&draining_service))
                .data(principal.clone()),
        )
        .await;
    assert!(fresh_response.errors.is_empty(), "{fresh_response:?}");
    assert_eq!(
        fresh_response.data.clone().into_json().unwrap(),
        json!({"causal_lifecycle": {"id": "todo-lifecycle-fresh"}})
    );
    let fresh_envelope = serde_json::to_value(
        fresh_response
            .extensions
            .get("distributed")
            .expect("fresh Draining command should carry the protocol envelope"),
    )
    .unwrap();
    assert_eq!(fresh_envelope["command"]["state"], "succeeded");
    assert_eq!(
        fresh_envelope["command"]["projectionDisposition"],
        "revalidate"
    );
    assert_eq!(fresh_envelope["command"]["expects"], json!([]));
    assert!(fresh_envelope["command"].get("projection").is_none());
    assert_eq!(
        handler_calls.load(Ordering::SeqCst),
        2,
        "fresh Draining command must execute rather than replay"
    );

    let status_query =
        format!("query {{ commandStatus(commandId: \"{fresh_command_id}\") {{ state }} }}");
    let status_response = draining_service
        .graphql_engine()
        .unwrap()
        .execute(
            &session,
            async_graphql::Request::new(status_query)
                .data(command_host(&draining_service))
                .data(principal),
        )
        .await;
    assert!(status_response.errors.is_empty(), "{status_response:?}");
    assert_eq!(
        status_response.data.into_json().unwrap(),
        json!({"commandStatus": {"state": "succeeded"}})
    );
    let status_envelope = serde_json::to_value(
        status_response
            .extensions
            .get("distributed")
            .expect("fresh Draining status should carry the terminal protocol envelope"),
    )
    .unwrap();
    assert_eq!(status_envelope["command"]["state"], "succeeded");
    assert_eq!(
        status_envelope["command"]["projectionDisposition"],
        "revalidate"
    );
    assert_eq!(status_envelope["command"]["expects"], json!([]));
    assert!(status_envelope["command"].get("projection").is_none());
    assert_eq!(handler_calls.load(Ordering::SeqCst), 2);
}

#[cfg(all(feature = "graphql", feature = "sqlite"))]
#[tokio::test]
async fn graphql_succeeded_status_evaluates_retained_projection_evidence() {
    let repository = crate::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("framework migrations should apply");
    let mut table_registry = crate::table::TableSchemaRegistry::new();
    table_registry
        .register_schema(
            <CausalLifecycleView as crate::read_model::RelationalReadModel>::schema().clone(),
        )
        .unwrap();
    repository
        .bootstrap_table_schema_for_dev(&table_registry)
        .await
        .unwrap();
    let projector = modeled_lifecycle_projector(
        crate::projection::placement::ProjectionBindingState::Active,
        "causal-succeeded-status",
        "causal-succeeded-status-topology",
        0x7a,
    );
    let service = Service::new().named("causal-succeeded-status").routes(
        Routes::new()
            .with_repo(repository.clone().aggregate::<CausalDispatcherAggregate>())
            .with_read_model_store(repository.clone())
            .typed_command(
                typed_command::<CausalTestInput, Eventual<TypedOutput>>("causal.lifecycle")
                    .roles(["user"])
                    .emits(crate::events![CausalLifecycleRecorded]),
            )
            .handle(
                |context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                 input: CausalTestInput| {
                    let result = (|| {
                        let mut checkout = context.create();
                        checkout.record_lifecycle(input.id.clone(), input.label)?;
                        context
                            .publish_events()
                            .commit(checkout)?
                            .eventual(TypedOutput { id: input.id })
                    })();
                    async move { result }
                },
            )
            .consume_projection(projector.clone()),
    );
    let engine = crate::graphql::GraphqlEngine::builder(&repository)
        .identity(crate::graphql::IdentityConfig::dev_headers())
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .model::<CausalLifecycleView>(
            crate::graphql::ModelPermissions::new()
                .grant("user", crate::graphql::read().all_columns()),
        )
        .service(&service)
        .client_projectors([projector])
        .build()
        .expect("active modeled projection should compile");
    let service = Arc::new(
        service
            .try_with_graphql(engine)
            .expect("compiled service should bind"),
    );
    let command_id = causal_test_command_id();
    let mutation = format!(
        "mutation {{ causal_lifecycle(commandId: \"{command_id}\", input: {{ id: \"todo-succeeded-status\", label: \"active\" }}) {{ id }} }}"
    );
    let session = session_with_role("user");
    let principal = causal_test_principal();
    let response = service
        .graphql_engine()
        .unwrap()
        .execute(
            &session,
            async_graphql::Request::new(&mutation)
                .data(command_host(&service))
                .data(principal.clone()),
        )
        .await;
    assert!(response.errors.is_empty(), "{response:?}");
    let envelope = serde_json::to_value(
        response
            .extensions
            .get("distributed")
            .expect("modeled command should carry the protocol envelope"),
    )
    .unwrap();
    assert_eq!(envelope["command"]["state"], "succeeded_pending_projection");
    assert_eq!(envelope["command"]["expects"].as_array().unwrap().len(), 1);
    let causation_id = envelope["command"]["causationId"]
        .as_str()
        .expect("command envelope should carry its causation")
        .to_string();

    // The wait-path cell commits a terminal succeeded receipt while retaining
    // modeled obligations. Before the projector writes proof, status remains
    // publicly succeeded but cannot claim an observation.
    let changed = sqlx::query(
        "UPDATE command_ledger SET state = 'succeeded' \
         WHERE service_id = ? AND command_id = ?",
    )
    .bind("causal-succeeded-status")
    .bind(&command_id)
    .execute(repository.pool())
    .await
    .unwrap();
    assert_eq!(changed.rows_affected(), 1);

    let status_query =
        format!("query {{ commandStatus(commandId: \"{command_id}\") {{ state }} }}");
    let before_projection = service
        .graphql_engine()
        .unwrap()
        .execute(
            &session,
            async_graphql::Request::new(&status_query)
                .data(command_host(&service))
                .data(principal.clone()),
        )
        .await;
    assert!(before_projection.errors.is_empty(), "{before_projection:?}");
    assert_eq!(
        before_projection.data.into_json().unwrap(),
        json!({"commandStatus": {"state": "succeeded"}})
    );
    let before_envelope = serde_json::to_value(
        before_projection
            .extensions
            .get("distributed")
            .expect("status should carry its protocol envelope"),
    )
    .unwrap();
    assert_eq!(before_envelope["command"]["state"], "succeeded");
    assert_eq!(
        before_envelope["command"]["expects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(before_envelope["command"].get("observations").is_none());

    // The celld host must use the same durable status evaluator. In
    // particular, a terminal cell receipt cannot make the gateway claim a
    // projection observation before the modeled projector has supplied proof.
    let before_celld = service
        .graphql_engine()
        .unwrap()
        .execute(
            &session,
            async_graphql::Request::new(&status_query)
                .data(celld_status_host(&service))
                .data(principal.clone()),
        )
        .await;
    assert!(before_celld.errors.is_empty(), "{before_celld:?}");
    let before_celld_envelope = serde_json::to_value(
        before_celld
            .extensions
            .get("distributed")
            .expect("celld status should carry its protocol envelope"),
    )
    .unwrap();
    assert_eq!(before_celld_envelope["command"]["state"], "succeeded");
    assert_eq!(
        before_celld_envelope["command"]["expects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(before_celld_envelope["command"]
        .get("observations")
        .is_none());

    let pending = repository
        .outbox_store()
        .pending(10)
        .await
        .expect("the committed domain event should be pending");
    assert_eq!(pending.len(), 1);
    let ordered = crate::bus::OrderedDelivery::new(
        crate::projection_protocol::ProjectionSource::new(
            "test-ordered-events",
            b"causal-succeeded-status".to_vec(),
        )
        .unwrap(),
        crate::projection_protocol::ProjectionEpoch::new("test-ordered-events-v1").unwrap(),
        1,
        true,
    )
    .unwrap();
    service
        .dispatch_projector(0, &Message::from(pending[0].clone()), Some(&ordered))
        .await
        .expect("the real modeled projector should commit its evidence");

    let after_projection = service
        .graphql_engine()
        .unwrap()
        .execute(
            &session,
            async_graphql::Request::new(&status_query)
                .data(command_host(&service))
                .data(principal.clone()),
        )
        .await;
    assert!(after_projection.errors.is_empty(), "{after_projection:?}");
    let after_envelope = serde_json::to_value(
        after_projection
            .extensions
            .get("distributed")
            .expect("status should carry its protocol envelope"),
    )
    .unwrap();
    assert_eq!(after_envelope["command"]["state"], "succeeded");
    assert_eq!(
        after_envelope["command"]["observations"]
            .as_array()
            .expect("matching durable proof should be exposed")
            .len(),
        1
    );
    assert_eq!(
        after_envelope["command"]["observations"][0]["causationId"],
        causation_id
    );

    let after_celld = service
        .graphql_engine()
        .unwrap()
        .execute(
            &session,
            async_graphql::Request::new(&status_query)
                .data(celld_status_host(&service))
                .data(principal.clone()),
        )
        .await;
    assert!(after_celld.errors.is_empty(), "{after_celld:?}");
    let after_celld_envelope = serde_json::to_value(
        after_celld
            .extensions
            .get("distributed")
            .expect("celld status should carry its protocol envelope"),
    )
    .unwrap();
    assert_eq!(after_celld_envelope["command"]["state"], "succeeded");
    assert_eq!(
        after_celld_envelope["command"]["observations"]
            .as_array()
            .expect("celld status should expose matching durable proof")
            .len(),
        1
    );

    // A proof authored by a different semantic program is not an observation
    // for this command, even when its physical topology and scope are equal.
    let wrong_program =
        "pp1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let changed =
        sqlx::query("UPDATE projection_observations SET program_id = ? WHERE causation_id = ?")
            .bind(wrong_program)
            .bind(&causation_id)
            .execute(repository.pool())
            .await
            .unwrap();
    assert_eq!(changed.rows_affected(), 1);

    let wrong_projection = service
        .graphql_engine()
        .unwrap()
        .execute(
            &session,
            async_graphql::Request::new(&status_query)
                .data(command_host(&service))
                .data(principal),
        )
        .await;
    assert!(wrong_projection.errors.is_empty(), "{wrong_projection:?}");
    let wrong_envelope = serde_json::to_value(
        wrong_projection
            .extensions
            .get("distributed")
            .expect("status should carry its protocol envelope"),
    )
    .unwrap();
    assert_eq!(wrong_envelope["command"]["state"], "succeeded");
    assert!(wrong_envelope["command"].get("observations").is_none());
    assert_eq!(
        wrong_envelope["command"]["expects"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "mismatched proof must leave the exact obligation pending"
    );
}

#[cfg(all(feature = "graphql", feature = "sqlite"))]
#[tokio::test]
async fn engine_rejects_incompatible_direct_owner_before_typed_command_binding() {
    let repository = crate::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("framework migrations should apply");
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let route_handler_calls = Arc::clone(&handler_calls);
    let service = Service::new().named("causal-direct").routes(
        Routes::new()
            .with_repo(repository.clone().aggregate::<CausalDispatcherAggregate>())
            .typed_command(typed_command::<
                CausalProjectionInput,
                Atomic<CausalProjectionObligationView>,
            >("causal.direct"))
            .handle(
                move |_context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                      input: CausalProjectionInput| {
                    let calls = Arc::clone(&route_handler_calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err(HandlerError::Rejected(format!(
                            "handler must not run for {}",
                            input.id
                        )))
                    }
                },
            ),
    );
    let first = modeled_direct_registration(
        CAUSAL_DIRECT_PROJECTION,
        "project_causal_direct",
        "first-direct-topology",
        1,
        <CausalProjectionObligationView as crate::read_model::RelationalReadModel>::schema()
            .clone(),
    );
    let second = modeled_direct_registration(
        CAUSAL_SIBLING_DIRECT_PROJECTION,
        "project_causal_direct",
        "second-direct-topology",
        2,
        <CausalProjectionSiblingView as crate::read_model::RelationalReadModel>::schema().clone(),
    );
    let owner = SurfaceDirectProjection::new("project_causal_direct")
        .modeled(first)
        .modeled(second)
        // If typed-command binding ran first, this deliberately incompatible
        // command partition would produce a different error.
        .partition_by(["must-not-bind"]);
    let result = crate::graphql::GraphqlEngine::builder(&repository)
        .identity(crate::graphql::IdentityConfig::dev_headers())
        .model::<CausalProjectionObligationView>(
            crate::graphql::ModelPermissions::new()
                .grant("anonymous", crate::graphql::read().all_columns()),
        )
        .model::<CausalProjectionSiblingView>(
            crate::graphql::ModelPermissions::new()
                .grant("anonymous", crate::graphql::read().all_columns()),
        )
        .service(&service)
        .client_projection_owners([owner.into()])
        .build();
    let error = match result {
        Ok(_) => panic!("incompatible direct owner must fail engine construction"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("incompatible active physical topologies"),
        "{error}"
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
}

#[cfg(all(feature = "graphql", feature = "sqlite"))]
#[tokio::test]
async fn projected_command_auto_binds_bootstraps_and_replays_exact_direct_evidence() {
    let repository = crate::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("framework migrations should apply");
    let mut registry = crate::table::TableSchemaRegistry::new();
    registry
        .register::<CausalProjectionObligationView>()
        .unwrap()
        .register::<CausalProjectionSiblingView>()
        .unwrap();
    for statement in
        crate::table::table_schema_statements(&registry, crate::table::TableSqlDialect::Sqlite)
            .unwrap()
    {
        sqlx::query(crate::sqlx_repo::audited_table_schema_sql(statement))
            .execute(repository.pool())
            .await
            .expect("test read-model schema should apply");
    }

    let handler_calls = Arc::new(AtomicUsize::new(0));
    let route_handler_calls = Arc::clone(&handler_calls);
    let service = Service::new().named("causal-direct").routes(
        Routes::new()
            .with_repo(repository.clone().aggregate::<CausalDispatcherAggregate>())
            // No direct-target/cache/projection call is present: the
            // `Atomic<M>` output and Surface owner are the complete
            // declaration.
            .typed_command(typed_command::<
                CausalProjectionInput,
                Atomic<CausalProjectionObligationView>,
            >("causal.direct"))
            .handle(
                move |context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                      input: CausalProjectionInput| {
                    let calls = Arc::clone(&route_handler_calls);
                    let result = (|| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let mut checkout = context.create();
                        checkout.record_direct(input.id.clone())?;
                        // Placement-selected: registration owns CAUSAL_DIRECT_PROJECTION.
                        context.commit(checkout)?.atomic()
                    })();
                    async move { result }
                },
            ),
    );
    crate::projection::lower::register_placement_selected_direct_descriptor(
        &CAUSAL_DIRECT_PROJECTION,
    )
    .expect("register placement-selected direct for test");
    let binding = crate::projection::placement::ProjectionBinding::materialize_direct(
        CAUSAL_DIRECT_PROJECTION.direct(),
        crate::projection::placement::ProjectionSourceBinding::try_new(
            "causal-domain",
            "ordered-domain-events",
            1,
        )
        .unwrap(),
        crate::projection::placement::ProjectionOwner::try_new("project_causal_direct").unwrap(),
        "distributed-projection-partition",
        crate::projection::placement::PROJECTION_PARTITION_CODEC_VERSION,
        vec![crate::projection::placement::ProjectionOutput::try_new(
            "CausalProjectionObligationView",
            "causal_projection_obligation_views",
            <CausalProjectionObligationView as crate::read_model::RelationalReadModel>::schema()
                .clone(),
        )
        .unwrap()],
        vec![],
        Some(
            crate::projection::placement::ProjectionPhysicalTopology::from_protocol(
                &ProjectorTopologyId::new(1, "project_causal_direct", [0x7a; 32]).unwrap(),
            ),
        ),
    )
    .unwrap();
    let catalog =
        crate::projection::catalog::ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
    let active = catalog
        .activate(vec![
            crate::projection::catalog::ProjectionBindingActivation::new(
                binding.id(),
                binding.program_id(),
                crate::projection::placement::ProjectionBindingState::Active,
                Some(
                    crate::projection::placement::ProjectionExecutorRoute::local("causal-direct")
                        .unwrap(),
                ),
            ),
        ])
        .unwrap();
    let modeled = crate::graphql::SurfaceModeledProjection::try_from_descriptor(
        CAUSAL_DIRECT_PROJECTION,
        &catalog,
        &active,
        binding.id(),
    )
    .unwrap();
    let projection = SurfaceDirectProjection::new("project_causal_direct").modeled(modeled);
    let engine = crate::graphql::GraphqlEngine::builder(&repository)
        .identity(crate::graphql::IdentityConfig::dev_headers())
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .model::<CausalProjectionObligationView>(
            crate::graphql::ModelPermissions::new()
                .grant("anonymous", crate::graphql::read().all_columns()),
        )
        .model::<CausalProjectionSiblingView>(
            crate::graphql::ModelPermissions::new()
                .grant("anonymous", crate::graphql::read().all_columns()),
        )
        .service(&service)
        .client_projection_owners([projection.into()])
        .build()
        .expect("ordinary Atomic<M> declaration should auto-bind its unique owner");
    let service = service
        .try_with_graphql(engine)
        .expect("bound direct target should attach to its executable route");
    let contract = &service.typed_command_contracts()[0];
    let target = contract
        .direct_projection
        .as_ref()
        .expect("engine binding must populate the private direct target");
    assert_eq!(target.projector, "project_causal_direct");
    assert_eq!(target.ownership.len(), 1);
    assert!(target.partition.is_none(), "zero-config partition is unit");
    let legitimate_executor = CAUSAL_DIRECT_PROJECTION.server_executor().unwrap();
    target
        .resolve(&json!({}), Some(&Session::new()))
        .unwrap()
        .validate_modeled_owner(legitimate_executor.name, legitimate_executor.program_id)
        .unwrap();
    let rogue_executor = CAUSAL_ROGUE_DIRECT_PROJECTION.server_executor().unwrap();
    let rogue_error = target
        .resolve(&json!({}), Some(&Session::new()))
        .unwrap()
        .validate_modeled_owner(rogue_executor.name, rogue_executor.program_id)
        .unwrap_err();
    assert!(
        rogue_error.to_string().contains("program"),
        "same name/epoch/schema must not authorize a different program: {rogue_error}"
    );

    let command_id = causal_test_command_id();
    let input = json!({
        "todoId": "todo-direct",
        "tenantPartition": "not-manual-projection-config"
    });
    let first = service
        .dispatch_causal(
            "causal.direct",
            &command_id,
            input.clone(),
            Session::new(),
            causal_test_principal(),
        )
        .await
        .expect("direct command should atomically commit");
    assert_eq!(first, json!({ "id": "todo-direct" }));
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);

    let direct_status = service
        .causal_command_status(&command_id, &Session::new(), causal_test_principal(), None)
        .await
        .expect("direct projected status should use ledger replay evidence");
    assert_eq!(direct_status.state, CausalCommandPublicState::Atomic);
    assert_eq!(direct_status.consistency, Some(CommandConsistency::Atomic));
    assert!(direct_status.evidence.is_empty());
    let direct_status_evidence = direct_status
        .direct_projection
        .expect("direct status must retain the exact same-attempt evidence");

    let stored_id: String =
        sqlx::query_scalar("SELECT id FROM causal_projection_obligation_views WHERE id = ?")
            .bind("todo-direct")
            .fetch_one(repository.pool())
            .await
            .expect("returned row should be visible through the GraphQL read database");
    assert_eq!(stored_id, "todo-direct");
    let registered: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM projection_registered_models WHERE model_name IN \
             ('CausalProjectionObligationView', 'CausalProjectionSiblingView')",
    )
    .fetch_one(repository.pool())
    .await
    .unwrap();
    assert_eq!(registered, 1);
    let sibling_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM causal_projection_sibling_views")
            .fetch_one(repository.pool())
            .await
            .unwrap();
    assert_eq!(
        sibling_rows, 0,
        "the direct participant must mutate only its returned output model"
    );

    let lookup = service
        .lookup_causal_command(
            "causal.direct",
            &command_id,
            &Session::new(),
            causal_test_principal(),
        )
        .await
        .unwrap();
    let CommandLookup::Replay(first_replay) = lookup else {
        panic!("projected command should be terminally replayable");
    };
    assert_eq!(first_replay.state, CommandLedgerState::Atomic);
    let evidence = first_replay
        .direct_projection
        .clone()
        .expect("replay must retain exact direct evidence");
    crate::projection_protocol::SameTransactionProjectionEvidence::validate_replay_value(&evidence)
        .unwrap();
    assert_eq!(direct_status_evidence.replay_value(), evidence);
    assert_eq!(evidence["records"].as_array().unwrap().len(), 1);
    assert_eq!(evidence["changes"].as_array().unwrap().len(), 1);
    assert_eq!(evidence["observations"].as_array().unwrap().len(), 1);
    let async_input_rows: i64 = sqlx::query_scalar(
        "SELECT \
             (SELECT COUNT(*) FROM projection_input_identities) + \
             (SELECT COUNT(*) FROM projection_input_cursors) + \
             (SELECT COUNT(*) FROM projection_input_receipts)",
    )
    .fetch_one(repository.pool())
    .await
    .unwrap();
    assert_eq!(
        async_input_rows, 0,
        "a direct-only projection must not create async input identities, cursors, or receipts"
    );
    let outbox_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_messages")
        .fetch_one(repository.pool())
        .await
        .unwrap();
    assert_eq!(
        outbox_rows, 0,
        "a direct-only projection must not require an outbox fact"
    );

    let replayed = service
        .dispatch_causal(
            "causal.direct",
            &command_id,
            input,
            Session::new(),
            causal_test_principal(),
        )
        .await
        .expect("response-loss retry should replay without invoking the handler");
    assert_eq!(replayed, first);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    let CommandLookup::Replay(second_replay) = service
        .lookup_causal_command(
            "causal.direct",
            &command_id,
            &Session::new(),
            causal_test_principal(),
        )
        .await
        .unwrap()
    else {
        panic!("replayed command should remain terminal");
    };
    assert_eq!(second_replay.direct_projection, Some(evidence));
}

#[tokio::test]
async fn causal_dispatch_recovers_committed_replay_after_commit_acknowledgement_loss() {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let route_handler_calls = Arc::clone(&handler_calls);
    let repository = AmbiguousCommitRepository::new(
        InMemoryRepository::new(),
        InjectedCommitBehavior::CommitThenErrorOnce,
    );
    let service = Service::new().named("causal-tests").routes(
        Routes::new()
            .with_repo(repository.aggregate::<CausalDispatcherAggregate>())
            .typed_command(typed_command::<CausalTestInput, Succeeded<TypedOutput>>(
                "causal.ambiguous_committed",
            ))
            .handle(
                move |_context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                      input: CausalTestInput| {
                    let calls = Arc::clone(&route_handler_calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(
                            PreparedCommand::<Succeeded<TypedOutput>>::prepare(TypedOutput {
                                id: input.id,
                            })
                            .unwrap(),
                        )
                    }
                },
            ),
    );
    let command_id = causal_test_command_id();
    let principal = causal_test_principal();
    let input = causal_test_input("todo-ambiguous", "same");

    let recovered = service
        .dispatch_causal(
            "causal.ambiguous_committed",
            &command_id,
            input.clone(),
            Session::new(),
            principal.clone(),
        )
        .await
        .expect("lookup should recover the committed outcome");
    assert_eq!(recovered, json!({ "id": "todo-ambiguous" }));
    assert!(matches!(
        service
            .lookup_causal_command(
                "causal.ambiguous_committed",
                &command_id,
                &Session::new(),
                principal.clone(),
            )
            .await
            .unwrap(),
        CommandLookup::Replay(_)
    ));

    let replay = service
        .dispatch_causal(
            "causal.ambiguous_committed",
            &command_id,
            input,
            Session::new(),
            principal,
        )
        .await
        .unwrap();
    assert_eq!(replay, recovered);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn causal_dispatch_reclaims_retryable_attempt_after_precommit_failure() {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let route_handler_calls = Arc::clone(&handler_calls);
    let repository = AmbiguousCommitRepository::new(
        InMemoryRepository::new(),
        InjectedCommitBehavior::ErrorBeforeCommitOnce,
    );
    let service = Service::new().named("causal-tests").routes(
        Routes::new()
            .with_repo(repository.aggregate::<CausalDispatcherAggregate>())
            .typed_command(typed_command::<CausalTestInput, Succeeded<TypedOutput>>(
                "causal.ambiguous_retry",
            ))
            .handle(
                move |_context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                      input: CausalTestInput| {
                    let calls = Arc::clone(&route_handler_calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(
                            PreparedCommand::<Succeeded<TypedOutput>>::prepare(TypedOutput {
                                id: input.id,
                            })
                            .unwrap(),
                        )
                    }
                },
            ),
    );
    let command_id = causal_test_command_id();
    let principal = causal_test_principal();
    let input = causal_test_input("todo-retry", "same");

    let first = service
        .dispatch_causal(
            "causal.ambiguous_retry",
            &command_id,
            input.clone(),
            Session::new(),
            principal.clone(),
        )
        .await
        .expect_err("pre-commit failure should remain unknown to the caller");
    assert_eq!(first.code(), "INTERNAL");
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        service
            .lookup_causal_command(
                "causal.ambiguous_retry",
                &command_id,
                &Session::new(),
                principal.clone(),
            )
            .await
            .unwrap(),
        CommandLookup::RetryableUnknown { .. }
    ));

    let retried = service
        .dispatch_causal(
            "causal.ambiguous_retry",
            &command_id,
            input.clone(),
            Session::new(),
            principal.clone(),
        )
        .await
        .expect("same-ID retry should reclaim and commit");
    assert_eq!(retried, json!({ "id": "todo-retry" }));
    assert_eq!(handler_calls.load(Ordering::SeqCst), 2);

    let replay = service
        .dispatch_causal(
            "causal.ambiguous_retry",
            &command_id,
            input,
            Session::new(),
            principal,
        )
        .await
        .unwrap();
    assert_eq!(replay, retried);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 2);
}

/// What a contended command's handler does between loading its aggregate and
/// staging its commit. Keyed by aggregate ID, so tests share nothing.
struct Contention {
    runs: AtomicUsize,
    after_load:
        Box<dyn Fn(usize) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
}

static CONTENTION: std::sync::LazyLock<Mutex<HashMap<String, Arc<Contention>>>> =
    std::sync::LazyLock::new(Default::default);

fn contend(
    id: &str,
    after_load: impl Fn(usize) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync
        + 'static,
) -> Arc<Contention> {
    let contention = Arc::new(Contention {
        runs: AtomicUsize::new(0),
        after_load: Box::new(after_load),
    });
    CONTENTION
        .lock()
        .unwrap()
        .insert(id.to_string(), Arc::clone(&contention));
    contention
}

/// Appends one event to the aggregate it loaded (or creates it).
async fn contended_handler(
    context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
    input: CausalTestInput,
) -> Result<PreparedCommand<Succeeded<TypedOutput>>, HandlerError> {
    let contention = Arc::clone(&CONTENTION.lock().unwrap()[&input.id]);
    let mut checkout = match context.get(&input.id).await? {
        Some(checkout) => checkout,
        None => context.create(),
    };
    (contention.after_load)(contention.runs.fetch_add(1, Ordering::SeqCst)).await;
    checkout
        .record(input.id.clone())
        .map_err(|error| HandlerError::Other(Box::new(error)))?;
    context
        .commit(checkout)?
        .succeeded(TypedOutput { id: input.id })
}

fn contended_route<D>(routes: Routes<D>) -> Routes<D>
where
    D: crate::microsvc::CausalRouteDependencies<Aggregate = CausalDispatcherAggregate>
        + Send
        + Sync
        + 'static,
{
    routes
        .typed_command(typed_command::<CausalTestInput, Succeeded<TypedOutput>>(
            "causal.contended",
        ))
        .handle(contended_handler)
}

/// Typed commands hold no lock across the handler: two of them on one
/// aggregate race on its stream version, and the loser must be re-run on the
/// winner's state rather than failed.
macro_rules! concurrent_write_tests {
    ($backend:ident, $repository:expr) => {
        mod $backend {
            use super::*;

            #[tokio::test]
            async fn concurrent_commands_on_one_aggregate_both_commit() {
                let repository = $repository;
                let id = concat!(stringify!($backend), "-raced");
                // Both first runs hold the same loaded version before either
                // commits. The loser's rerun does not wait.
                let loaded = Arc::new(tokio::sync::Barrier::new(2));
                let contention = contend(id, move |run| {
                    let loaded = Arc::clone(&loaded);
                    Box::pin(async move {
                        if run < 2 {
                            loaded.wait().await;
                        }
                    })
                });
                let service = Service::new()
                    .named("causal-tests")
                    .routes(contended_route(Routes::new().with_repo(
                        repository.clone().aggregate::<CausalDispatcherAggregate>(),
                    )));
                let (first_id, second_id) = (causal_test_command_id(), causal_test_command_id());
                let dispatch = |command_id| {
                    service.dispatch_causal_with_receipt(
                        "causal.contended",
                        command_id,
                        causal_test_input(id, "raced"),
                        Session::new(),
                        causal_test_principal(),
                    )
                };

                let (first, second) = tokio::join!(dispatch(&first_id), dispatch(&second_id));

                let first = first.expect("one command wins the stream version");
                let second = second.expect("the other is run again and commits");
                assert!(!first.replayed() && !second.replayed());
                assert_eq!(
                    contention.runs.load(Ordering::SeqCst),
                    3,
                    "exactly the loser ran again"
                );
                let committed = repository
                    .aggregate::<CausalDispatcherAggregate>()
                    .get(id)
                    .await
                    .unwrap()
                    .expect("both commands committed to one stream");
                assert_eq!(committed.entity().version(), 2);
            }

            #[tokio::test]
            async fn a_command_that_keeps_losing_is_released_as_concurrent_write() {
                let repository = $repository;
                let id = concat!(stringify!($backend), "-starved");
                // Another writer commits behind every run's loaded version.
                let interfering = Arc::new(AtomicBool::new(true));
                let contention = contend(id, {
                    let (repository, interfering) = (repository.clone(), Arc::clone(&interfering));
                    move |_run| {
                        let (repository, interfering) =
                            (repository.clone(), Arc::clone(&interfering));
                        Box::pin(async move {
                            if !interfering.load(Ordering::SeqCst) {
                                return;
                            }
                            let writer = repository.aggregate::<CausalDispatcherAggregate>();
                            let mut other = writer.get(id).await.unwrap().unwrap_or_default();
                            other.record(id.to_string()).unwrap();
                            writer.commit(&mut other).await.unwrap();
                        })
                    }
                });
                let service = Service::new()
                    .named("causal-tests")
                    .routes(contended_route(Routes::new().with_repo(
                        repository.clone().aggregate::<CausalDispatcherAggregate>(),
                    )));
                let command_id = causal_test_command_id();
                let dispatch = || {
                    service.dispatch_causal_with_receipt(
                        "causal.contended",
                        &command_id,
                        causal_test_input(id, "starved"),
                        Session::new(),
                        causal_test_principal(),
                    )
                };
                let runs = crate::repository::CONCURRENT_WRITE_RETRIES;

                let starved = dispatch().await.expect_err("every run lost the race");
                assert!(
                    matches!(starved, CausalDispatchError::ConcurrentWrite),
                    "unexpected failure: {starved:?}"
                );
                assert_eq!(starved.code(), "CONCURRENT_WRITE");
                assert_eq!(starved.status_code(), 503);
                assert_eq!(contention.runs.load(Ordering::SeqCst), runs);
                assert!(matches!(
                    service
                        .lookup_causal_command(
                            "causal.contended",
                            &command_id,
                            &Session::new(),
                            causal_test_principal(),
                        )
                        .await
                        .unwrap(),
                    CommandLookup::RetryableUnknown { .. }
                ));

                interfering.store(false, Ordering::SeqCst);
                let retried = dispatch()
                    .await
                    .expect("the same command ID runs again at once");
                assert!(!retried.replayed());
                assert_eq!(contention.runs.load(Ordering::SeqCst), runs + 1);
                let committed = repository
                    .aggregate::<CausalDispatcherAggregate>()
                    .get(id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    committed.entity().version(),
                    runs as u64 + 1,
                    "the other writer's events and this command's one"
                );
            }
        }
    };
}

concurrent_write_tests!(in_memory, InMemoryRepository::new());

#[cfg(feature = "sqlite")]
concurrent_write_tests!(
    sqlite,
    crate::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("framework migrations should apply")
);

#[tokio::test]
async fn service_collects_route_bundles_with_different_dependencies() {
    let service = Service::new()
        .named("tests")
        .routes(
            command_routes()
                .typed_command(pong_command("stores.dep"))
                .handle(pong),
        )
        .routes(
            Routes::new()
                .with_dependencies(String::from("orders"))
                .event("string.dep")
                .handle(|ctx: &Context<String>| {
                    let dep = ctx.dependencies().clone();
                    async move { Ok(json!({ "dependency": dep })) }
                }),
        )
        .routes(
            Routes::new()
                .with_dependencies(7_u32)
                .event("number.dep")
                .handle(|ctx: &Context<u32>| {
                    let dep = *ctx.dependencies();
                    async move { Ok(json!({ "dependency": dep })) }
                }),
        );

    let command = dispatch_command(&service, "stores.dep", json!({}), Session::new())
        .await
        .unwrap();
    let event = |name: &'static str| {
        let service = &service;
        async move {
            service
                .dispatch_message(&Message::new(name, MessageKind::Event, br#"{}"#.to_vec()))
                .await
                .unwrap()
        }
    };

    assert_eq!(command, json!({ "pong": true }));
    assert_eq!(event("string.dep").await, json!({ "dependency": "orders" }));
    assert_eq!(event("number.dep").await, json!({ "dependency": 7 }));
    assert_eq!(
        service.subscription_plan(),
        SubscriptionPlan {
            commands: vec!["stores.dep".to_string()],
            events: vec!["string.dep".to_string(), "number.dep".to_string()],
        }
    );
}

#[tokio::test]
async fn service_dispatches_all_route_dependency_builder_combinations() {
    let repo_only: RouteComboRepo = InMemoryRepository::new().queued().aggregate();
    let combo_repo: RouteComboRepo = InMemoryRepository::new().queued().aggregate();
    let typed = |name| typed_command::<TypedInput, Succeeded<TypedOutput>>(name);
    let service = Service::new()
        .named("tests")
        .routes(
            Routes::new()
                .with_dependencies(String::from("custom"))
                .event("custom.route")
                .handle(|ctx: &Context<String>| {
                    let dependency = ctx.dependencies().clone();
                    async move { Ok(json!({ "route": dependency })) }
                }),
        )
        .routes(
            Routes::new()
                .with_repo(repo_only)
                .typed_command(typed("repo.route"))
                .handle(typed_handler),
        )
        .routes(
            Routes::new()
                .with_read_model_store(InMemoryRepository::new())
                .event("read.route")
                .handle(|ctx: &Context<InMemoryRepository>| {
                    let _ = ctx.read_model_store();
                    async move { Ok(json!({ "route": "read" })) }
                }),
        )
        .routes(
            Routes::new()
                .with_repo(combo_repo)
                .with_read_model_store(InMemoryRepository::new())
                .typed_command(typed("repo-read.route"))
                .handle(typed_handler)
                .event("repo-read.event")
                .handle(|ctx: &Context<RouteComboDeps>| {
                    let _ = ctx.repo();
                    let _ = ctx.read_model_store();
                    async move { Ok(json!({ "route": "repo-read" })) }
                }),
        );
    let event = |name: &'static str| {
        let service = &service;
        async move {
            service
                .dispatch_message(&Message::new(name, MessageKind::Event, br#"{}"#.to_vec()))
                .await
                .unwrap()
        }
    };

    let repo = dispatch_command(
        &service,
        "repo.route",
        json!({ "id": "repo" }),
        Session::new(),
    )
    .await
    .unwrap();
    let repo_read = dispatch_command(
        &service,
        "repo-read.route",
        json!({ "id": "repo-read" }),
        Session::new(),
    )
    .await
    .unwrap();

    assert_eq!(event("custom.route").await, json!({ "route": "custom" }));
    assert_eq!(repo, json!({ "id": "repo" }));
    assert_eq!(event("read.route").await, json!({ "route": "read" }));
    assert_eq!(repo_read, json!({ "id": "repo-read" }));
    assert_eq!(
        event("repo-read.event").await,
        json!({ "route": "repo-read" })
    );
    assert_eq!(
        service.subscription_plan(),
        SubscriptionPlan {
            commands: vec!["repo.route".to_string(), "repo-read.route".to_string()],
            events: vec![
                "custom.route".to_string(),
                "read.route".to_string(),
                "repo-read.event".to_string(),
            ],
        }
    );
}

#[test]
fn duplicate_route_names_within_bundle_are_rejected() {
    let commands = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _routes = command_routes()
            .typed_command(pong_command("same"))
            .handle(pong)
            .typed_command(pong_command("same"))
            .handle(pong);
    }));
    let events = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _routes = test_routes()
            .event("same")
            .handle(|_: &Context<()>| async move { Ok(json!({})) })
            .event("same")
            .handle(|_: &Context<()>| async move { Ok(json!({})) });
    }));

    assert!(commands.is_err());
    assert!(events.is_err());
}

#[test]
fn duplicate_route_bundle_add_is_rejected_atomically() {
    let mut service = command_service(
        command_routes()
            .typed_command(pong_command("same"))
            .handle(pong),
    );
    let conflicting = command_routes()
        .typed_command(pong_command("same"))
        .handle(pong)
        .typed_command(pong_command("new"))
        .handle(pong);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        service.add_routes(conflicting);
    }));

    assert!(result.is_err());
    assert!(service.handles_message(MessageKind::Command, "same"));
    assert!(!service.handles_message(MessageKind::Command, "new"));
    assert_eq!(service.routes.len(), 1);
    assert_eq!(service.command_names(), vec!["same"]);
}

#[tokio::test]
async fn dispatch_returns_handler_result() {
    let service = command_service(
        command_routes()
            .typed_command(pong_command("ping"))
            .handle(pong),
    );
    let result = dispatch_command(&service, "ping", json!({}), Session::new())
        .await
        .unwrap();
    assert_eq!(result, json!({ "pong": true }));
}

#[tokio::test]
async fn unknown_command() {
    // This dispatch records an {unknown, unknown_command} series into the
    // process-global registry that
    // `metrics_bucket_unknown_command_under_fixed_message_label` resets and
    // asserts an exact count on — serialize against it.
    #[cfg(feature = "metrics")]
    let _guard = crate::metrics::async_lock_for_tests().await;

    let service = command_service(
        command_routes()
            .typed_command(pong_command("ping"))
            .handle(pong),
    );
    let result = dispatch_command(&service, "unknown", json!({}), Session::new()).await;
    assert!(
        matches!(result, Err(CausalDispatchError::BadRequest(ref s)) if s == "unknown typed command")
    );
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn metrics_bucket_unknown_command_under_fixed_message_label() {
    let _guard = crate::metrics::async_lock_for_tests().await;
    crate::metrics::reset_for_tests();

    let service = command_service(
        command_routes()
            .typed_command(pong_command("ping"))
            .handle(pong),
    );

    let result = dispatch_command(
        &service,
        "attacker-controlled-path",
        json!({}),
        Session::new(),
    )
    .await;
    assert!(matches!(result, Err(CausalDispatchError::BadRequest(_))));

    let text = crate::metrics::prometheus_text();
    assert!(
            text.contains(
                "distributed_microsvc_dispatch_total{service=\"tests\",message_kind=\"command\",message=\"unknown\",status=\"unknown_command\"} 1"
            ),
            "unknown commands should use a bounded message label:\n{text}"
        );
    assert!(
        !text.contains("attacker-controlled-path"),
        "unknown command input must not become a metric label:\n{text}"
    );
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn metrics_count_each_typed_dispatch_once_by_outcome() {
    let _guard = crate::metrics::async_lock_for_tests().await;
    crate::metrics::reset_for_tests();

    let service = command_service(
        command_routes()
            .typed_command(pong_command("ping"))
            .handle(pong)
            .typed_command(pong_command("fail"))
            .handle(|_: &CausalCommandContext<'_, ()>, _: NoInput| async move {
                Err::<PreparedCommand<Succeeded<Pong>>, _>(HandlerError::Rejected("nope".into()))
            })
            .typed_command(pong_command("bus.ping").roles([crate::microsvc::TRANSPORT_ROLE]))
            .handle(pong),
    );
    dispatch_command(&service, "ping", json!({}), Session::new())
        .await
        .unwrap();
    dispatch_command(&service, "fail", json!({}), Session::new())
        .await
        .unwrap_err();
    service
        .dispatch_message(
            &Message::new("bus.ping", MessageKind::Command, b"{}".to_vec()).with_id("msg-1"),
        )
        .await
        .unwrap();

    let text = crate::metrics::prometheus_text();
    for (message, status) in [
        ("ping", "success"),
        ("fail", "rejected"),
        ("bus.ping", "success"),
    ] {
        let series = format!(
            "distributed_microsvc_dispatch_total{{service=\"tests\",message_kind=\"command\",message=\"{message}\",status=\"{status}\"}} 1"
        );
        assert!(text.contains(&series), "missing `{series}`:\n{text}");
    }
}

#[tokio::test]
async fn handler_error_propagates() {
    let service = command_service(command_routes().typed_command(pong_command("fail")).handle(
        |_: &CausalCommandContext<'_, ()>, _: NoInput| async move {
            Err::<PreparedCommand<Succeeded<Pong>>, _>(HandlerError::Rejected("nope".into()))
        },
    ));
    let result = dispatch_command(&service, "fail", json!({}), Session::new()).await;
    assert!(matches!(
        result,
        Err(CausalDispatchError::Rejected { code: "REJECTED", status: 422, ref message })
            if message.contains("nope")
    ));
}

#[tokio::test]
async fn decode_error_from_bad_payload() {
    let service = Service::new().named("tests").routes(
        Routes::new()
            .with_repo(
                InMemoryRepository::new()
                    .queued()
                    .aggregate::<RouteComboAggregate>(),
            )
            .typed_command(typed_command::<TypedInput, Succeeded<TypedOutput>>("typed"))
            .handle(typed_handler),
    );
    let result = dispatch_command(&service, "typed", json!({ "wrong": 1 }), Session::new()).await;
    assert!(matches!(result, Err(CausalDispatchError::BadRequest(_))));
}

#[test]
fn command_names_list() {
    let service = command_service(
        command_routes()
            .typed_command(pong_command("a"))
            .handle(pong)
            .typed_command(pong_command("b"))
            .handle(pong),
    );
    let mut cmds = service.command_names();
    cmds.sort();
    assert_eq!(cmds, vec!["a", "b"]);
}

#[test]
fn subscription_plan_separates_commands_and_events() {
    const EVENTS: &[&str] = &["checkout.started", "seat.reserved"];

    let service = command_service(
        command_routes()
            .typed_command(pong_command("checkout.start"))
            .handle(pong)
            .events(EVENTS)
            .guarded(
                |_| true,
                |_: &Context<StoreDeps>| async move { Ok(json!({})) },
            ),
    );

    assert_eq!(
        service.subscription_plan(),
        SubscriptionPlan {
            commands: vec!["checkout.start".to_string()],
            events: vec!["checkout.started".to_string(), "seat.reserved".to_string()],
        }
    );
}

#[test]
fn event_conveniences_record_event_names() {
    const EVENTS: &[&str] = &["seat.added", "seat.reserved"];

    let service = test_service(
        test_routes()
            .event("checkout.started")
            .handle(|_: &Context<()>| async move { Ok(json!({})) })
            .events(EVENTS)
            .handle(|_: &Context<()>| async move { Ok(json!({})) }),
    );

    let mut events = service.event_names();
    events.sort();
    assert_eq!(
        events,
        vec!["checkout.started", "seat.added", "seat.reserved"]
    );
}

#[tokio::test]
async fn command_and_event_handlers_can_share_a_name() {
    let service = command_service(
        command_routes()
            .typed_command(pong_command("shared"))
            .handle(pong)
            .event("shared")
            .handle(|ctx: &Context<StoreDeps>| {
                let event_id = ctx.message().id().map(|s| s.to_string());
                async move { Ok(json!({ "event_id": event_id })) }
            }),
    );
    let event_message =
        Message::new("shared", MessageKind::Event, br#"{}"#.to_vec()).with_id("evt-1");

    let command_result = dispatch_command(&service, "shared", json!({}), Session::new())
        .await
        .unwrap();
    let event_result = service.dispatch_message(&event_message).await.unwrap();

    assert_eq!(command_result, json!({ "pong": true }));
    assert_eq!(event_result, json!({ "event_id": "evt-1" }));
    assert!(service.handles_message(MessageKind::Command, "shared"));
    assert!(service.handles_message(MessageKind::Event, "shared"));
}

#[tokio::test]
async fn dispatch_message_delivers_payload_json_by_default() {
    let service = test_service(test_routes().event("checkout.started").handle(
        |ctx: &Context<()>| {
            let has_checkout_id = ctx.has_fields(&["checkout_id"]);
            let event_id = ctx.message().id().map(|s| s.to_string());
            let checkout_id = ctx.raw_input()["checkout_id"]
                .as_str()
                .map(|s| s.to_string());
            let user_id = ctx.user_id().map(|s| s.to_string());
            async move {
                if !has_checkout_id {
                    return Err(HandlerError::Rejected("missing checkout_id".into()));
                }

                Ok(json!({
                    "event_id": event_id,
                    "checkout_id": checkout_id.unwrap(),
                    "user_id": user_id?,
                }))
            }
        },
    ));
    let message = Message {
        id: Some("evt-1".to_string()),
        name: "checkout.started".to_string(),
        kind: MessageKind::Event,
        payload: br#"{"checkout_id":"checkout-1"}"#.to_vec(),
        content_type: "application/json".to_string(),
        metadata: vec![("X-User-Id".to_string(), "user-1".to_string())],
    };

    let result = service.dispatch_message(&message).await.unwrap();

    assert_eq!(
        result,
        json!({ "event_id": "evt-1", "checkout_id": "checkout-1", "user_id": "user-1" })
    );
}

#[tokio::test]
async fn dispatch_message_surfaces_malformed_json_as_decode_error() {
    let service = test_service(test_routes().event("checkout.started").handle(
        |_ctx: &Context<()>| async move { panic!("handler must not run on a decode error") },
    ));
    let message = Message::new(
        "checkout.started",
        MessageKind::Event,
        br#"{"checkout_id": oops"#.to_vec(),
    );

    let err = service.dispatch_message(&message).await.unwrap_err();

    match err {
        HandlerError::DecodeFailed(detail) => {
            assert!(
                detail.contains("invalid JSON payload") && detail.contains("checkout.started"),
                "decode error should carry the parse failure, got: {detail}"
            );
        }
        other => panic!("expected DecodeFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn dispatch_message_nulls_input_for_non_json_payloads() {
    let service = test_service(
        test_routes()
            .event("blob.stored")
            .handle(|ctx: &Context<()>| {
                let input_is_null = ctx.raw_input().is_null();
                let payload = ctx.message().payload().to_vec();
                async move { Ok(json!({ "null_input": input_is_null, "len": payload.len() })) }
            }),
    );
    let mut message = Message::new("blob.stored", MessageKind::Event, vec![0, 159, 146, 150]);
    message.content_type = "application/octet-stream".to_string();

    let result = service.dispatch_message(&message).await.unwrap();

    assert_eq!(result, json!({ "null_input": true, "len": 4 }));
}

#[tokio::test]
async fn dispatch_message_always_exposes_message_metadata() {
    let service = test_service(test_routes().event("seat.reserved").guarded(
        |ctx| ctx.message().id().is_some(),
        |ctx: &Context<()>| {
            let input: Result<Value, _> = ctx.input();
            let message = ctx.message();
            let event_id = message.id().map(|s| s.to_string());
            let name = message.name().to_string();
            let correlation_id = message.correlation_id().map(|s| s.to_string());
            async move {
                let input = input?;
                Ok(json!({
                    "event_id": event_id,
                    "name": name,
                    "correlation_id": correlation_id,
                    "seat_id": input["seat_id"].as_str().unwrap(),
                }))
            }
        },
    ));
    let message = Message {
        id: Some("evt-2".to_string()),
        name: "seat.reserved".to_string(),
        kind: MessageKind::Event,
        payload: br#"{"seat_id":"A-7"}"#.to_vec(),
        content_type: "application/json".to_string(),
        metadata: vec![("Correlation_ID".to_string(), "checkout-1".to_string())],
    };

    let result = service.dispatch_message(&message).await.unwrap();

    assert_eq!(
        result,
        json!({
            "event_id": "evt-2",
            "name": "seat.reserved",
            "correlation_id": "checkout-1",
            "seat_id": "A-7",
        })
    );
}

#[tokio::test]
async fn dispatch_exposes_trace_context_from_session_metadata() {
    #[derive(Serialize, crate::command::CommandOutput)]
    struct Trace {
        traceparent: Option<String>,
        tracestate: Option<String>,
    }

    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let service = command_service(
        command_routes()
            .typed_command(typed_command::<NoInput, Succeeded<Trace>>("checkout.start"))
            .handle(|ctx: &CausalCommandContext<'_, ()>, _: NoInput| {
                let trace_context = ctx.trace_context();
                async move {
                    Ok(PreparedCommand::prepare(Trace {
                        traceparent: trace_context.traceparent,
                        tracestate: trace_context.tracestate,
                    })
                    .unwrap())
                }
            }),
    );
    let session = Session::from_map(HashMap::from([
        ("traceparent".to_string(), traceparent.to_string()),
        ("tracestate".to_string(), "vendor=value".to_string()),
    ]));

    let result = dispatch_command(&service, "checkout.start", json!({}), session)
        .await
        .unwrap();

    assert_eq!(
        result,
        json!({ "traceparent": traceparent, "tracestate": "vendor=value" })
    );
}

#[tokio::test]
async fn guard_passes() {
    let service = command_service(
        command_routes()
            .typed_command(pong_command("greet"))
            .guarded(|ctx| ctx.command_name() == "greet", pong),
    );
    let result = dispatch_command(&service, "greet", json!({}), Session::new())
        .await
        .unwrap();
    assert_eq!(result, json!({ "pong": true }));
}

#[tokio::test]
async fn guard_rejects() {
    let service = command_service(
        command_routes()
            .typed_command(pong_command("greet"))
            .guarded(
                |_| false,
                |_: &CausalCommandContext<'_, ()>, _: NoInput| async move {
                    panic!("handler should not run");
                    #[allow(unreachable_code)]
                    Ok(PreparedCommand::<Succeeded<Pong>>::prepare(Pong { pong: true }).unwrap())
                },
            ),
    );
    let result = dispatch_command(&service, "greet", json!({}), Session::new()).await;
    assert!(matches!(
        result,
        Err(CausalDispatchError::Rejected {
            code: "REJECTED",
            status: 422,
            ..
        })
    ));
}

#[tokio::test]
async fn guard_checks_session() {
    let service = command_service(
        command_routes()
            .typed_command(pong_command("admin"))
            .guarded(|ctx| ctx.role() == Some("admin"), pong),
    );

    // No role
    assert!(
        dispatch_command(&service, "admin", json!({}), Session::new())
            .await
            .is_err()
    );

    // Admin role
    let mut session = Session::new();
    session.set(crate::microsvc::ROLE_KEY, "admin");
    assert!(dispatch_command(&service, "admin", json!({}), session)
        .await
        .is_ok());
}

type LaneLog = std::sync::Arc<std::sync::Mutex<Vec<String>>>;

fn lane_log() -> LaneLog {
    std::sync::Arc::new(std::sync::Mutex::new(Vec::new()))
}

fn lane_recorder(log: &LaneLog, label: &'static str) -> Routes<()> {
    let log = log.clone();
    test_routes()
        .events(&["fact.recorded", "fact.other"])
        .handle(move |ctx: &Context<()>| {
            log.lock()
                .unwrap()
                .push(format!("{label}:{}", ctx.message().name()));
            async move { Ok(json!({})) }
        })
}

#[tokio::test]
async fn named_lanes_partition_routes_and_default_dispatch_runs_all_in_order() {
    use crate::bus::{LaneSet, MessageRouter};
    let log = lane_log();
    let service = Service::new()
        .routes(lane_recorder(&log, "projection-a"))
        .lane("process", lane_recorder(&log, "policy"))
        .routes(lane_recorder(&log, "projection-b"))
        .lane(
            "process",
            test_routes()
                .event("fact.process_only")
                .handle(|_: &Context<()>| async move { Ok(json!({})) }),
        );

    assert_eq!(
        service.delivery_lane_names(),
        &[DEFAULT_DELIVERY_LANE, "process"]
    );
    assert_eq!(MessageRouter::delivery_lanes(&service), 2);
    assert_eq!(
        MessageRouter::lanes_for(&service, MessageKind::Event, "fact.recorded"),
        LaneSet::single(0).with(1)
    );
    assert_eq!(
        MessageRouter::lanes_for(&service, MessageKind::Event, "fact.process_only"),
        LaneSet::single(1)
    );
    assert!(MessageRouter::lanes_for(&service, MessageKind::Event, "fact.unknown").is_empty());

    let message = Message::new("fact.recorded", MessageKind::Event, b"{}".to_vec()).with_id("e1");
    MessageRouter::dispatch_lane(&service, &message, None, 1)
        .await
        .unwrap();
    assert_eq!(*log.lock().unwrap(), ["policy:fact.recorded"]);
    log.lock().unwrap().clear();
    MessageRouter::dispatch_lane(&service, &message, None, 0)
        .await
        .unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        ["projection-a:fact.recorded", "projection-b:fact.recorded"]
    );
    // Without lanes (sequential transports), every route runs in
    // registration order exactly as before lanes existed.
    log.lock().unwrap().clear();
    service.dispatch_message(&message).await.unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        [
            "projection-a:fact.recorded",
            "policy:fact.recorded",
            "projection-b:fact.recorded"
        ]
    );
    // A lane with no route for the message is an unknown-route error, never a
    // silent success.
    let only = Message::new("fact.process_only", MessageKind::Event, b"{}".to_vec());
    assert!(MessageRouter::dispatch_lane(&service, &only, None, 0)
        .await
        .is_err());
}

#[test]
fn service_without_named_lanes_reports_one_lane() {
    use crate::bus::MessageRouter;
    let log = lane_log();
    let service = Service::new().routes(lane_recorder(&log, "only"));
    assert_eq!(MessageRouter::delivery_lanes(&service), 1);
}

#[test]
#[should_panic(expected = "named delivery lane")]
fn default_lane_name_cannot_be_used_as_a_named_lane() {
    let log = lane_log();
    let _ = Service::new().lane(DEFAULT_DELIVERY_LANE, lane_recorder(&log, "x"));
}

#[cfg(any(feature = "graphql", feature = "runtime-tokio"))]
macro_rules! ledger_compaction_routes {
    ($repository:expr, $command:literal) => {
        Routes::new()
            .with_repo($repository.clone().aggregate::<CausalDispatcherAggregate>())
            .typed_command(typed_command::<CausalTestInput, Succeeded<TypedOutput>>(
                $command,
            ))
            .handle(
                |_context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                 input: CausalTestInput| async move {
                    Ok(
                        PreparedCommand::<Succeeded<TypedOutput>>::prepare(TypedOutput {
                            id: input.id,
                        })
                        .unwrap(),
                    )
                },
            )
    };
}

#[cfg(feature = "runtime-tokio")]
macro_rules! ledger_compaction_service {
    ($repository:expr) => {
        Service::new()
            .named("ledger-compaction")
            .routes(ledger_compaction_routes!($repository, "ledger.compact"))
    };
}

#[cfg(feature = "runtime-tokio")]
async fn reserve_for_compaction<R: CommandLedgerStore>(
    repository: &R,
) -> (CommandLedgerKey, crate::command_ledger::CommandAttempt) {
    use crate::command_ledger::{
        CanonicalInputHash, CommandContractFingerprint, CommandId, PrincipalPartitionId,
    };
    let key = CommandLedgerKey::new(
        "ledger-compaction",
        PrincipalPartitionId::new("v1:sha256:principal").unwrap(),
        CommandId::parse(&causal_test_command_id()).unwrap(),
    )
    .unwrap();
    let reservation = CommandReservation::new(
        key.clone(),
        "ledger.compact",
        CommandContractFingerprint::new([1; 32]),
        CanonicalInputHash::new([2; 32]),
        std::time::Duration::from_secs(30),
        std::time::Duration::from_secs(300),
    )
    .unwrap();
    match repository.reserve_command(reservation).await.unwrap() {
        ReservationOutcome::Acquired(attempt) => (key, attempt),
        other => panic!("expected an acquired attempt, got {other:?}"),
    }
}

#[cfg(feature = "runtime-tokio")]
/// Writes a terminal row whose retention lapses 50 ms after commit, a
/// terminal row retained for 300 s and an in-progress row, then runs the
/// worker's pass until the first one is due. The ledger has no injectable
/// clock (the stores use their own), so the pass is polled instead of the
/// test sleeping for a fixed time.
async fn worker_pass_compacts_only_expired_rows<R>(service: &Service, repository: &R)
where
    R: CommandLedgerStore + CausalTransactionalCommit,
{
    use crate::command_ledger::TerminalCommandState;
    use std::time::{Duration, Instant};

    let scope = CommandLookupScope::CommandName("ledger.compact");
    let complete = |attempt: crate::command_ledger::CommandAttempt, retention: Duration| async move {
        let completion = attempt
            .complete(
                TerminalCommandState::Succeeded,
                json!({"id": "todo-1"}),
                retention,
            )
            .unwrap();
        repository
            .commit_causal_batch(CausalCommitBatch::new(
                crate::CommitBatch::empty(),
                completion,
            ))
            .await
            .unwrap();
    };
    let (expired, attempt) = reserve_for_compaction(repository).await;
    complete(attempt, Duration::from_millis(50)).await;
    let (retained, attempt) = reserve_for_compaction(repository).await;
    complete(attempt, Duration::from_secs(300)).await;
    let (in_progress, _attempt) = reserve_for_compaction(repository).await;

    let ledgers = service.command_ledgers();
    assert_eq!(ledgers.len(), 1);
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut compacted = 0;
    while compacted == 0 {
        assert!(
            Instant::now() < deadline,
            "the expired row was never compacted"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
        compacted = crate::microsvc::workers::compact_command_ledgers(&ledgers, 8).await;
    }

    assert_eq!(compacted, 1, "only the expired row is compacted");
    assert_eq!(
        crate::microsvc::workers::compact_command_ledgers(&ledgers, 8).await,
        0
    );
    assert_eq!(
        repository.lookup_command(&expired, scope).await.unwrap(),
        CommandLookup::Expired
    );
    assert!(matches!(
        repository.lookup_command(&retained, scope).await.unwrap(),
        CommandLookup::Replay(_)
    ));
    assert!(matches!(
        repository
            .lookup_command(&in_progress, scope)
            .await
            .unwrap(),
        CommandLookup::InProgress { .. }
    ));
}

#[cfg(feature = "runtime-tokio")]
#[tokio::test]
async fn ledger_worker_pass_compacts_only_expired_rows_in_memory() {
    let repository = InMemoryRepository::new();
    let service = ledger_compaction_service!(repository);
    worker_pass_compacts_only_expired_rows(&service, &repository).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn ledger_worker_pass_compacts_only_expired_rows_in_sqlite() {
    let repository = crate::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("framework migrations should apply");
    let service = ledger_compaction_service!(repository);
    worker_pass_compacts_only_expired_rows(&service, &repository).await;
}

#[cfg(any(feature = "graphql", feature = "runtime-tokio"))]
#[tokio::test]
async fn service_compacts_each_command_repository_once() {
    let shared = InMemoryRepository::new();
    let other = InMemoryRepository::new();
    let service = Service::new()
        .named("ledger-compaction")
        .routes(ledger_compaction_routes!(shared, "ledger.first"))
        .routes(ledger_compaction_routes!(shared, "ledger.second"))
        .routes(ledger_compaction_routes!(other, "ledger.third"))
        .routes(test_routes());

    assert_eq!(service.command_ledgers().len(), 2);
}

#[cfg(feature = "runtime-tokio")]
/// `with_bus` starts the worker: with no explicit pass, the expired row is
/// compacted. Tokio time is paused, so the worker's interval elapses as soon
/// as the runtime is idle; the row's retention is wall-clock time.
#[tokio::test(start_paused = true)]
async fn with_bus_runs_ledger_compaction() {
    use crate::command_ledger::TerminalCommandState;
    use std::time::{Duration, Instant};

    let repository = InMemoryRepository::new();
    let service = ledger_compaction_service!(repository).with_bus(crate::bus::InMemoryBus::new());
    let (key, attempt) = reserve_for_compaction(&repository).await;
    let completion = attempt
        .complete(
            TerminalCommandState::Succeeded,
            json!({"id": "todo-1"}),
            Duration::from_millis(50),
        )
        .unwrap();
    repository
        .commit_causal_batch(CausalCommitBatch::new(
            crate::CommitBatch::empty(),
            completion,
        ))
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    while repository.command_ledger_state(&key) != Some(CommandLedgerState::Expired) {
        assert!(
            Instant::now() < deadline,
            "the worker never compacted the row"
        );
        tokio::time::sleep(crate::microsvc::workers::LEDGER_COMPACTION_INTERVAL).await;
    }
    drop(service);
}

#[cfg(feature = "runtime-tokio")]
#[tokio::test]
async fn dropping_the_ledger_compaction_worker_stops_it() {
    let ledger: LedgerCompaction = Arc::new(|_| Box::pin(async { Ok(0) }));
    let worker = crate::microsvc::workers::LedgerCompactionWorker::spawn(vec![ledger.clone()]);
    drop(worker);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while Arc::strong_count(&ledger) > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the aborted worker releases its ledgers");
}

/// One closed gate, every way in: the service refuses the write, and each
/// transport answers the way it did when it checked the gate itself.
#[cfg(all(feature = "graphql", feature = "sqlite"))]
#[tokio::test]
async fn a_closed_lifecycle_gate_refuses_writes_on_every_transport() {
    let gated_service =
        |repository: &crate::SqliteRepository| {
            Service::new()
                .named("lifecycle-gate")
                .with_http_command_routes()
                .routes(
                    Routes::new()
                        .with_repo(repository.clone().aggregate::<CausalDispatcherAggregate>())
                        .typed_command(typed_command::<CausalTestInput, Succeeded<TypedOutput>>(
                            "gate.write",
                        ))
                        .handle(
                            |_context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
                             input: CausalTestInput| async move {
                                Ok(PreparedCommand::<Succeeded<TypedOutput>>::prepare(
                                    TypedOutput { id: input.id },
                                )
                                .unwrap())
                            },
                        ),
                )
        };
    let engine_for = |repository: &crate::SqliteRepository, service: &Service| {
        crate::graphql::GraphqlEngine::builder(repository)
            .identity(crate::graphql::IdentityConfig::dev_headers())
            .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
            .service(service)
            .build()
            .expect("the gated service's commands should compile")
    };
    let repository = crate::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("framework migrations should apply");

    let service =
        gated_service(&repository).with_lifecycle(crate::microsvc::LifecycleGate::closed());
    let engine = engine_for(&repository, &service);
    let service = Arc::new(service.try_with_graphql(engine).unwrap());
    let mut session = Session::new();
    session.set(crate::microsvc::USER_ID_KEY, "alice");
    let mutation = format!(
        "mutation {{ gate_write(commandId: \"{}\", input: {{ id: \"todo-1\", label: \"x\" }}) {{ id }} }}",
        causal_test_command_id()
    );

    // The service itself.
    let refused = service
        .dispatch_causal(
            "gate.write",
            &causal_test_command_id(),
            causal_test_input("todo-1", "x"),
            session.clone(),
            causal_test_principal(),
        )
        .await;
    assert!(matches!(
        refused,
        Err(CausalDispatchError::Handler(
            HandlerError::ApplicationReloading
        ))
    ));

    // The engine, as a WebSocket or an embedding caller reaches it.
    let engine = service.graphql_engine().unwrap();
    let response = engine
        .execute(&session, async_graphql::Request::new(mutation.clone()))
        .await;
    assert_eq!(response.errors.len(), 1);
    assert_eq!(
        response.errors[0].message,
        "application generation is reloading; mutation dispatch is unavailable"
    );
    assert!(response.errors[0].extensions.is_none());
    let read = engine
        .execute(&session, async_graphql::Request::new("{ __typename }"))
        .await;
    assert!(read.errors.is_empty(), "reads stay open: {read:?}");

    // HTTP.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = crate::microsvc::router(Arc::clone(&service));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::new();
    let post = |path: &str, body: Value| {
        let request = client
            .post(format!("{base}/{path}"))
            .header(crate::microsvc::USER_ID_KEY, "alice")
            .json(&body);
        async move {
            let response = request.send().await.unwrap();
            (
                response.status().as_u16(),
                response.json::<Value>().await.unwrap(),
            )
        }
    };

    assert_eq!(
        post("graphql", json!({ "query": mutation })).await,
        (
            503,
            json!({"errors": [{
                "message": "application generation is reloading",
                "extensions": { "code": "APPLICATION_RELOADING" }
            }]})
        )
    );
    assert_eq!(
        post("graphql", json!({ "query": "{ __typename }" }))
            .await
            .0,
        200
    );
    let reloading = json!({
        "code": "APPLICATION_RELOADING",
        "error": "application generation is not active"
    });
    // A body that is not a command envelope is refused as one, gate or not.
    let (status, body) = post("gate.write", json!({ "id": "todo-1", "label": "x" })).await;
    assert_eq!((status, &body["code"]), (400, &json!("BAD_REQUEST")));
    assert_eq!(
        post(
            "gate.write",
            json!({
                "commandId": causal_test_command_id(),
                "input": { "id": "todo-1", "label": "x" }
            })
        )
        .await,
        (503, reloading)
    );

    // An engine built before the service was given its gate has the wrong one.
    let ungated = gated_service(&repository);
    let engine = engine_for(&repository, &ungated);
    let mismatch = ungated
        .with_lifecycle(crate::microsvc::LifecycleGate::closed())
        .try_with_graphql(engine)
        .err()
        .expect("the engine does not share the service's gate");
    assert_eq!(
        mismatch,
        crate::microsvc::GraphqlServiceBindError::LifecycleMismatch
    );
}

#[derive(Debug, Deserialize, crate::command::CommandInput)]
struct StampedInput {
    id: String,
    at: String,
}

#[derive(Serialize, crate::command::CommandOutput)]
struct StampedOutput {
    id: String,
    at: String,
}

#[tokio::test]
async fn now_input_default_is_the_host_clock_never_the_callers_value() {
    use std::time::{Duration, UNIX_EPOCH};

    let clock =
        crate::clock::ManualClock::new(UNIX_EPOCH + Duration::from_millis(1_700_000_000_123));
    let repository = InMemoryRepository::new().with_clock(clock.clone());
    let routes = Routes::for_stores(repository.clone(), repository)
        .typed_command(
            typed_command::<StampedInput, Succeeded<StampedOutput>>("stamp.now").input_defaults(
                crate::command::__command_input_defaults(vec![
                    crate::command::__input_default_now::<
                        StampedInput,
                        __DistributedStampedInputEffectInputField_at,
                    >(),
                ]),
            ),
        )
        .handle(
            |_context: &CausalCommandContext<'_, ()>, input: StampedInput| async move {
                Ok(PreparedCommand::prepare(StampedOutput {
                    id: input.id,
                    at: input.at,
                })
                .unwrap())
            },
        );
    let service = command_service(routes);
    let command_id = causal_test_command_id();
    let dispatch = |at: &'static str| {
        service.dispatch_causal(
            "stamp.now",
            &command_id,
            json!({ "id": "a", "at": at }),
            Session::new(),
            causal_test_principal(),
        )
    };

    let first = dispatch("1").await.unwrap();
    assert_eq!(first, json!({ "id": "a", "at": "1700000000123" }));

    // A retry of the same command carries the same preview value: it replays
    // the stored outcome even though the clock moved, never a new stamp or a
    // command-id conflict.
    clock.advance(Duration::from_secs(60));
    assert_eq!(dispatch("1").await.unwrap(), first);
    // A different submitted input under the same id is still a conflict.
    assert!(matches!(
        dispatch("2").await,
        Err(CausalDispatchError::CommandIdReuse)
    ));
}
