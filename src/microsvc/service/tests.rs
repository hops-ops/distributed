#[cfg(feature = "graphql")]
use super::causal::collapse_projection_evidence;
use super::*;
#[cfg(feature = "graphql")]
use crate::aggregate::Aggregate;
#[cfg(feature = "graphql")]
use crate::bus::RunOptions;
use crate::bus::{Message, MessageKind, SubscriptionPlan};
#[cfg(all(feature = "graphql", feature = "sqlite"))]
use crate::command::Eventual;
use crate::command::{typed_command, PreparedCommand, Succeeded};
#[cfg(feature = "graphql")]
use crate::command::{Atomic, CommandConsistency};
use crate::command::{CommandInputType, CommandOutputType, CommandTypeDef, CommandTypeField};
#[cfg(feature = "graphql")]
use crate::command_ledger::{
    AttemptFence, CausalCommitBatch, CausalGetStream, CausalRepositoryIdentity,
    CausalTransactionalCommit, CommandLedgerError, CommandLedgerKey, CommandLedgerState,
    CommandLedgerStore, CommandLookup, CommandLookupScope, CommandReservation, ReservationOutcome,
};
#[cfg(feature = "graphql")]
use crate::graphql::identity::VerifiedPrincipal;
#[cfg(feature = "graphql")]
use crate::graphql::{SurfaceDirectProjection, SurfaceProjector};
#[cfg(feature = "graphql")]
use crate::microsvc::HasOutboxStore;
use crate::microsvc::{
    CommandRequest, Context, HandlerError, RepoReadModelDependencies, Routes, Service, Session,
};
#[cfg(feature = "graphql")]
use crate::outbox::OutboxMessage;
#[cfg(feature = "graphql")]
use crate::projection_protocol::{
    ProjectionChangeCursor, ProjectionChangeRead, ProjectionCheckpoint, ProjectionCommitBatch,
    ProjectionCommitResult, ProjectionFailure, ProjectionFailureBatch, ProjectionFailureLocation,
    ProjectionGeneration, ProjectionInputCursor, ProjectionInputDisposition,
    ProjectionLiveRecordBatch, ProjectionLiveRecordBatchRequest, ProjectionModelOwnership,
    ProjectionObligationEvidenceBatch, ProjectionObligationEvidenceBatchRequest,
    ProjectionObservation, ProjectionObservationKind, ProjectionPartition,
    ProjectionPartitionRuntimeState, ProjectionProtocolError, ProjectionProtocolStore,
    ProjectionQuerySnapshot, ProjectionQuerySnapshotBatch, ProjectionQuerySnapshotBatchRequest,
    ProjectionQuerySnapshotRequest, ProjectionRecordMetadata, ProjectionRecordScope,
    ProjectorTopologyId, TrustedProjectionInput,
};
use crate::{
    sourced, AggregateBuilder, AggregateRepository, Entity, InMemoryRepository, Queueable,
    QueuedRepository,
};
#[cfg(feature = "graphql")]
use crate::{GetStream, OutboxStore};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
#[cfg(feature = "graphql")]
use std::future::Future;
#[cfg(feature = "graphql")]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "graphql")]
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

#[cfg(feature = "graphql")]
#[derive(Deserialize)]
struct CausalTestInput {
    id: String,
    label: String,
}

#[cfg(feature = "graphql")]
impl CommandInputType for CausalTestInput {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "CausalTestInput",
            vec![
                CommandTypeField {
                    name: "id".into(),
                    type_name: "String".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                },
                CommandTypeField {
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
#[derive(Clone, Deserialize, crate::CommandInput)]
struct CausalProjectionInput {
    #[serde(rename = "todoId")]
    id: String,
    #[serde(rename = "tenantPartition")]
    partition: String,
}

#[cfg(feature = "graphql")]
#[derive(Clone, Serialize, Deserialize, crate::ReadModel)]
#[readmodel(
        table = "causal_projection_obligation_views",
        primary_key = ["id"]
    )]
struct CausalProjectionObligationView {
    id: String,
}

#[cfg(feature = "graphql")]
#[derive(Clone, Serialize, Deserialize, crate::ReadModel)]
#[readmodel(table = "causal_projection_sibling_views", primary_key = ["id"])]
struct CausalProjectionSiblingView {
    id: String,
}

#[cfg(feature = "graphql")]
#[derive(Clone, Serialize, crate::DomainEvent)]
#[domain_event(name = "causal.lifecycle-recorded", version = 1)]
struct CausalLifecycleRecorded {
    id: String,
    label: String,
}

#[cfg(feature = "graphql")]
#[derive(Clone, Serialize, Deserialize, crate::ReadModel)]
#[readmodel(table = "causal_lifecycle_views", primary_key = ["id"])]
struct CausalLifecycleView {
    id: String,
    label: String,
}

#[cfg(feature = "graphql")]
#[derive(Clone, Serialize, Deserialize, crate::DomainState)]
#[domain_state(version = 1)]
struct CausalDirectState {
    id: String,
}

#[cfg(feature = "graphql")]
fn causal_direct_program(
    name: &'static str,
    version: u64,
    event_name: &'static str,
) -> Result<crate::ProjectionProgram, crate::ProjectionProgramError> {
    use crate::mutation::{
        bind_state_body_to_mutation, compile_projection, state_upsert_program_for_model,
    };

    let program = state_upsert_program_for_model::<CausalProjectionObligationView>(
        "save_causal_direct",
        1,
        "upsert-view",
        "view",
    )
    .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
        operation: name.into(),
        reason: e.to_string(),
    })?;
    let descriptor = crate::DomainEventDescriptor::state::<CausalDirectState>(event_name, 1);
    let handler =
        bind_state_body_to_mutation::<CausalProjectionObligationView>(&descriptor, program, "view")
            .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
                operation: name.into(),
                reason: e.to_string(),
            })?;
    compile_projection(name, version, crate::ProjectionPartition::Unit, [handler]).map_err(|e| {
        crate::ProjectionProgramError::InvalidOperation {
            operation: name.into(),
            reason: e.to_string(),
        }
    })
}

#[cfg(feature = "graphql")]
fn causal_direct_v1_program() -> Result<crate::ProjectionProgram, crate::ProjectionProgramError> {
    causal_direct_program("project_causal_direct", 1, "causal.direct-recorded")
}

#[cfg(feature = "graphql")]
fn causal_direct_v1_resolve(
    occurrence: &crate::DomainEventOccurrence,
) -> Result<crate::ResolvedProjectionPlan, crate::ProjectionProgramError> {
    crate::mutation::resolve_mutation_program(&causal_direct_v1_program()?, occurrence)
}

#[cfg(feature = "graphql")]
fn causal_direct_v1_lower(
    plan: &crate::ResolvedProjectionPlan,
) -> Result<
    crate::projection::lower::LoweredProjectionPlan,
    crate::projection::lower::ProjectionLoweringError,
> {
    crate::mutation::lower_single_model::<CausalProjectionObligationView>(plan)
}

#[cfg(feature = "graphql")]
fn causal_direct_v1_inventory() -> Result<
    crate::projection::lower::ProjectionOutputInventory,
    crate::projection::lower::ProjectionLoweringError,
> {
    crate::mutation::inventory_single_model::<CausalProjectionObligationView>()
}

#[cfg(feature = "graphql")]
const CAUSAL_DIRECT_PROJECTION: crate::projection::lower::ProjectionDescriptor<
    crate::projection::lower::DirectCandidate,
> = crate::mutation::descriptor_from_factories(
    "project_causal_direct",
    1,
    "causal-direct-v1",
    causal_direct_v1_program,
    causal_direct_v1_resolve,
    causal_direct_v1_lower,
    causal_direct_v1_inventory,
);

#[cfg(feature = "graphql")]
fn causal_direct_v2_program() -> Result<crate::ProjectionProgram, crate::ProjectionProgramError> {
    causal_direct_program("project_causal_direct", 2, "causal.direct-recorded")
}

#[cfg(feature = "graphql")]
fn causal_direct_v2_resolve(
    occurrence: &crate::DomainEventOccurrence,
) -> Result<crate::ResolvedProjectionPlan, crate::ProjectionProgramError> {
    crate::mutation::resolve_mutation_program(&causal_direct_v2_program()?, occurrence)
}

#[cfg(feature = "graphql")]
fn causal_direct_v2_lower(
    plan: &crate::ResolvedProjectionPlan,
) -> Result<
    crate::projection::lower::LoweredProjectionPlan,
    crate::projection::lower::ProjectionLoweringError,
> {
    crate::mutation::lower_single_model::<CausalProjectionObligationView>(plan)
}

#[cfg(feature = "graphql")]
fn causal_direct_v2_inventory() -> Result<
    crate::projection::lower::ProjectionOutputInventory,
    crate::projection::lower::ProjectionLoweringError,
> {
    crate::mutation::inventory_single_model::<CausalProjectionObligationView>()
}

#[cfg(feature = "graphql")]
const CAUSAL_ROGUE_DIRECT_PROJECTION: crate::projection::lower::ProjectionDescriptor<
    crate::projection::lower::DirectCandidate,
> = crate::mutation::descriptor_from_factories(
    "project_causal_direct",
    2,
    "causal-direct-v1",
    causal_direct_v2_program,
    causal_direct_v2_resolve,
    causal_direct_v2_lower,
    causal_direct_v2_inventory,
);

#[cfg(feature = "graphql")]
fn causal_sibling_program() -> Result<crate::ProjectionProgram, crate::ProjectionProgramError> {
    use crate::mutation::{
        bind_state_body_to_mutation, compile_projection, state_upsert_program_for_model,
    };

    let program = state_upsert_program_for_model::<CausalProjectionSiblingView>(
        "save_causal_sibling",
        1,
        "upsert-view",
        "view",
    )
    .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
        operation: "project_causal_direct_sibling".into(),
        reason: e.to_string(),
    })?;
    let descriptor = crate::DomainEventDescriptor::state::<CausalDirectState>(
        "causal.direct-sibling-recorded",
        1,
    );
    let handler =
        bind_state_body_to_mutation::<CausalProjectionSiblingView>(&descriptor, program, "view")
            .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
                operation: "project_causal_direct_sibling".into(),
                reason: e.to_string(),
            })?;
    compile_projection(
        "project_causal_direct_sibling",
        1,
        crate::ProjectionPartition::Unit,
        [handler],
    )
    .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
        operation: "project_causal_direct_sibling".into(),
        reason: e.to_string(),
    })
}

#[cfg(feature = "graphql")]
fn causal_sibling_resolve(
    occurrence: &crate::DomainEventOccurrence,
) -> Result<crate::ResolvedProjectionPlan, crate::ProjectionProgramError> {
    crate::mutation::resolve_mutation_program(&causal_sibling_program()?, occurrence)
}

#[cfg(feature = "graphql")]
fn causal_sibling_lower(
    plan: &crate::ResolvedProjectionPlan,
) -> Result<
    crate::projection::lower::LoweredProjectionPlan,
    crate::projection::lower::ProjectionLoweringError,
> {
    crate::mutation::lower_single_model::<CausalProjectionSiblingView>(plan)
}

#[cfg(feature = "graphql")]
fn causal_sibling_inventory() -> Result<
    crate::projection::lower::ProjectionOutputInventory,
    crate::projection::lower::ProjectionLoweringError,
> {
    crate::mutation::inventory_single_model::<CausalProjectionSiblingView>()
}

#[cfg(feature = "graphql")]
const CAUSAL_SIBLING_DIRECT_PROJECTION: crate::projection::lower::ProjectionDescriptor<
    crate::projection::lower::DirectCandidate,
> = crate::mutation::descriptor_from_factories(
    "project_causal_direct_sibling",
    1,
    "causal-direct-v1",
    causal_sibling_program,
    causal_sibling_resolve,
    causal_sibling_lower,
    causal_sibling_inventory,
);

#[cfg(feature = "graphql")]
fn causal_lifecycle_program() -> Result<crate::ProjectionProgram, crate::ProjectionProgramError> {
    use crate::domain_event::DomainEventContract;
    use crate::mutation::{
        bind_state_body_to_mutation, compile_projection, state_upsert_program_for_model,
    };

    let program = state_upsert_program_for_model::<CausalLifecycleView>(
        "save_causal_lifecycle",
        1,
        "upsert-view",
        "view",
    )
    .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
        operation: "project_causal_lifecycle".into(),
        reason: e.to_string(),
    })?;
    let handler = bind_state_body_to_mutation::<CausalLifecycleView>(
        &CausalLifecycleRecorded::descriptor(),
        program,
        "view",
    )
    .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
        operation: "project_causal_lifecycle".into(),
        reason: e.to_string(),
    })?;
    compile_projection(
        "project_causal_lifecycle",
        1,
        crate::ProjectionPartition::Unit,
        [handler],
    )
    .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
        operation: "project_causal_lifecycle".into(),
        reason: e.to_string(),
    })
}

#[cfg(feature = "graphql")]
fn causal_lifecycle_resolve(
    occurrence: &crate::DomainEventOccurrence,
) -> Result<crate::ResolvedProjectionPlan, crate::ProjectionProgramError> {
    crate::mutation::resolve_mutation_program(&causal_lifecycle_program()?, occurrence)
}

#[cfg(feature = "graphql")]
fn causal_lifecycle_lower(
    plan: &crate::ResolvedProjectionPlan,
) -> Result<
    crate::projection::lower::LoweredProjectionPlan,
    crate::projection::lower::ProjectionLoweringError,
> {
    crate::mutation::lower_single_model::<CausalLifecycleView>(plan)
}

#[cfg(feature = "graphql")]
fn causal_lifecycle_inventory() -> Result<
    crate::projection::lower::ProjectionOutputInventory,
    crate::projection::lower::ProjectionLoweringError,
> {
    crate::mutation::inventory_single_model::<CausalLifecycleView>()
}

#[cfg(feature = "graphql")]
const CAUSAL_LIFECYCLE_PROJECTION: crate::projection::lower::ProjectionDescriptor<
    crate::projection::lower::EventualOnly,
> = crate::mutation::descriptor_from_factories(
    "project_causal_lifecycle",
    1,
    "causal-lifecycle-v1",
    causal_lifecycle_program,
    causal_lifecycle_resolve,
    causal_lifecycle_lower,
    causal_lifecycle_inventory,
);

#[cfg(feature = "graphql")]
fn modeled_direct_registration(
    descriptor: crate::projection::lower::ProjectionDescriptor<
        crate::projection::lower::DirectCandidate,
    >,
    owner: &str,
    epoch: &str,
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
        .activate(
            vec![
                crate::projection::catalog::ProjectionBindingActivation::new(
                    binding.id(),
                    binding.program_id(),
                    crate::projection_protocol::ProjectionEpoch::new(epoch).unwrap(),
                    crate::projection::placement::ProjectionBindingState::Active,
                    Some(
                        crate::projection::placement::ProjectionExecutorRoute::local(
                            "causal-direct",
                        )
                        .unwrap(),
                    ),
                ),
            ],
            None,
        )
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
        .activate(
            vec![
                crate::projection::catalog::ProjectionBindingActivation::new(
                    binding.id(),
                    binding.program_id(),
                    crate::projection_protocol::ProjectionEpoch::new("causal-lifecycle-v1")
                        .unwrap(),
                    state,
                    Some(
                        crate::projection::placement::ProjectionExecutorRoute::local(route)
                            .unwrap(),
                    ),
                ),
            ],
            None,
        )
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

#[cfg(feature = "graphql")]
impl CommandOutputType for CausalProjectionObligationView {
    fn command_type() -> CommandTypeDef {
        one_string_field("CausalProjectionObligationView", "id")
            .with_type_id(std::any::TypeId::of::<Self>())
    }
}

#[cfg(feature = "graphql")]
impl CommandOutputType for CausalProjectionSiblingView {
    fn command_type() -> CommandTypeDef {
        one_string_field("CausalProjectionSiblingView", "id")
            .with_type_id(std::any::TypeId::of::<Self>())
    }
}

#[cfg(feature = "graphql")]
impl CommandOutputType for CausalLifecycleView {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "CausalLifecycleView",
            vec![
                CommandTypeField {
                    name: "id".into(),
                    type_name: "String".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                },
                CommandTypeField {
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

static TYPED_HANDLER_INVOKED: AtomicBool = AtomicBool::new(false);
static TYPED_GUARD_INVOKED: AtomicBool = AtomicBool::new(false);

async fn typed_handler(
    _context: &CausalCommandContext<'_, RouteComboAggregate>,
    input: TypedInput,
) -> Result<PreparedCommand<Succeeded<TypedOutput>>, HandlerError> {
    TYPED_HANDLER_INVOKED.store(true, Ordering::SeqCst);
    Ok(PreparedCommand::prepare(TypedOutput { id: input.id }).unwrap())
}

#[cfg(all(feature = "graphql", feature = "application-runtime"))]
static GENERATED_MOUNT_HANDLER_INVOKED: AtomicUsize = AtomicUsize::new(0);

#[cfg(all(feature = "graphql", feature = "application-runtime"))]
#[distributed::command(
    id = "causal.generated_mount",
    roles(user),
    input = CausalTestInput,
    outcome = Succeeded<TypedOutput>
)]
async fn generated_mount_handler(
    _context: &CausalCommandContext<'_, CausalDispatcherAggregate>,
    input: CausalTestInput,
) -> Result<PreparedCommand<Succeeded<TypedOutput>>, HandlerError> {
    GENERATED_MOUNT_HANDLER_INVOKED.fetch_add(1, Ordering::SeqCst);
    Ok(PreparedCommand::prepare(TypedOutput { id: input.id }).unwrap())
}

#[derive(Default)]
struct RouteComboAggregate {
    entity: Entity,
}

#[sourced(entity)]
impl RouteComboAggregate {
    #[event("created")]
    fn create(&mut self) {
        self.entity.set_id("route-combo");
    }
}

#[cfg(feature = "graphql")]
#[derive(Default, crate::Snapshot)]
struct CausalDispatcherAggregate {
    entity: Entity,
}

#[cfg(feature = "graphql")]
impl CausalDispatcherAggregate {
    fn record(&mut self, id: String) -> crate::SourcedResult {
        self.entity.set_id(id);
        self.entity.digest_empty("causal.recorded")
    }

    fn record_direct(&mut self, id: String) -> Result<(), HandlerError> {
        self.entity.set_id(id.clone());
        self.entity
            .digest_empty("causal.direct-recorded")
            .map_err(|error| HandlerError::Other(Box::new(error)))?;
        self.entity
            .capture_domain_state(
                Self::aggregate_type(),
                crate::DomainEventDescriptor::state::<CausalDirectState>(
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

#[cfg(feature = "graphql")]
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

#[cfg(feature = "graphql")]
fn causal_test_principal() -> VerifiedPrincipal {
    VerifiedPrincipal::test_oidc(
        "https://issuer.example/",
        "causal-test-subject",
        &["distributed-tests"],
    )
}

#[cfg(feature = "graphql")]
fn causal_test_command_id() -> String {
    uuid::Uuid::now_v7().hyphenated().to_string()
}

#[cfg(feature = "graphql")]
fn causal_test_input(id: &str, label: &str) -> Value {
    json!({ "id": id, "label": label })
}

#[cfg(feature = "graphql")]
fn session_with_role(role: &str) -> Session {
    let mut session = Session::new();
    session.set(crate::microsvc::ROLE_KEY, role);
    session.set(crate::microsvc::USER_ID_KEY, "causal-test-subject");
    session
}

#[cfg(feature = "graphql")]
fn command_host(service: &Arc<Service>) -> crate::command_dispatch::SharedCommandHost {
    Arc::new(crate::command_dispatch::LocalCommandHost::new(Arc::clone(
        service,
    )))
}

#[cfg(feature = "graphql")]
#[derive(Clone, Copy)]
enum InjectedCommitBehavior {
    CommitThenErrorOnce,
    ErrorBeforeCommitOnce,
    Delegate,
}

#[cfg(feature = "graphql")]
#[derive(Clone)]
struct AmbiguousCommitRepository {
    inner: InMemoryRepository,
    behavior: Arc<Mutex<InjectedCommitBehavior>>,
}

#[cfg(feature = "graphql")]
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

#[cfg(feature = "graphql")]
impl CausalGetStream for AmbiguousCommitRepository {
    fn get_causal_stream<'a>(
        &'a self,
        identity: &'a crate::StreamIdentity,
    ) -> impl Future<Output = Result<Option<Entity>, crate::RepositoryError>> + Send + 'a {
        CausalGetStream::get_causal_stream(&self.inner, identity)
    }
}

#[cfg(feature = "graphql")]
impl CausalRepositoryIdentity for AmbiguousCommitRepository {
    fn causal_storage_identity(&self) -> crate::command_ledger::CausalStorageIdentity {
        CausalRepositoryIdentity::causal_storage_identity(&self.inner)
    }
}

#[cfg(feature = "graphql")]
impl ProjectionProtocolStore for AmbiguousCommitRepository {
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

    fn projection_query_snapshot_batch<'a>(
        &'a self,
        request: &'a ProjectionQuerySnapshotBatchRequest,
    ) -> impl Future<Output = Result<ProjectionQuerySnapshotBatch, ProjectionProtocolError>> + Send + 'a
    {
        self.inner.projection_query_snapshot_batch(request)
    }

    fn projection_obligation_evidence_batch<'a>(
        &'a self,
        request: &'a ProjectionObligationEvidenceBatchRequest,
    ) -> impl Future<Output = Result<ProjectionObligationEvidenceBatch, ProjectionProtocolError>>
           + Send
           + 'a {
        self.inner.projection_obligation_evidence_batch(request)
    }

    fn projection_live_record_batch<'a>(
        &'a self,
        request: &'a ProjectionLiveRecordBatchRequest,
    ) -> impl Future<Output = Result<ProjectionLiveRecordBatch, ProjectionProtocolError>> + Send + 'a
    {
        self.inner.projection_live_record_batch(request)
    }

    fn projection_partition_runtime_state<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a ProjectionPartition,
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
        partition: &'a ProjectionPartition,
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
        partition: &'a ProjectionPartition,
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
        partition: &'a ProjectionPartition,
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
}

#[cfg(feature = "graphql")]
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

    fn compact_expired_commands(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<u64, CommandLedgerError>> + Send + '_ {
        CommandLedgerStore::compact_expired_commands(&self.inner, limit)
    }
}

#[cfg(feature = "graphql")]
impl crate::repository::TransactionalCommit for AmbiguousCommitRepository {
    fn commit_batch<'a>(
        &'a self,
        batch: crate::repository::CommitBatch<'a>,
    ) -> impl Future<Output = Result<(), crate::RepositoryError>> + Send + 'a {
        crate::repository::TransactionalCommit::commit_batch(&self.inner, batch)
    }
}

#[cfg(feature = "graphql")]
impl CausalTransactionalCommit for AmbiguousCommitRepository {
    async fn commit_causal_batch<'a>(
        &'a self,
        batch: CausalCommitBatch<'a>,
    ) -> Result<(), CommandLedgerError> {
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

#[cfg(feature = "graphql")]
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

#[cfg(feature = "graphql")]
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
        .commands
        .retain(|command| command.id != "access.grant");
    assert!(matches!(
        service.application("catalog", missing),
        Err(crate::ApplicationError::Missing { kind: "surface command", identity })
            if identity == "access.grant"
    ));
    assert!(
        Service::new().application("catalog", surface).is_err(),
        "a Surface cannot expose commands absent from the owning Service"
    );
}

#[tokio::test]
async fn typed_direct_dispatch_fails_before_invoking_handler() {
    TYPED_HANDLER_INVOKED.store(false, Ordering::SeqCst);
    let service = Service::new().named("todos").routes(
        Routes::new()
            .with_repo(
                InMemoryRepository::new()
                    .queued()
                    .aggregate::<RouteComboAggregate>(),
            )
            .typed_command(typed_command::<TypedInput, Succeeded<TypedOutput>>(
                "todo.create",
            ))
            .handle(typed_handler),
    );

    let error = service
        .dispatch("todo.create", json!({ "id": "todo-1" }), Session::new())
        .await
        .expect_err("typed causal commands must reject direct dispatch");

    assert!(error.to_string().contains("verified GraphQL bearer"));
    assert!(!TYPED_HANDLER_INVOKED.load(Ordering::SeqCst));
}

#[tokio::test]
async fn typed_direct_dispatch_fails_before_invoking_guard_or_handler() {
    TYPED_GUARD_INVOKED.store(false, Ordering::SeqCst);
    TYPED_HANDLER_INVOKED.store(false, Ordering::SeqCst);
    let service = Service::new().named("todos").routes(
        Routes::new()
            .with_repo(
                InMemoryRepository::new()
                    .queued()
                    .aggregate::<RouteComboAggregate>(),
            )
            .typed_command(typed_command::<TypedInput, Succeeded<TypedOutput>>(
                "todo.guarded_create",
            ))
            .guarded(
                |_| {
                    TYPED_GUARD_INVOKED.store(true, Ordering::SeqCst);
                    true
                },
                typed_handler,
            ),
    );

    let error = service
        .dispatch(
            "todo.guarded_create",
            json!({ "id": "todo-1" }),
            Session::new(),
        )
        .await
        .expect_err("typed causal commands must reject before application guards");

    assert!(error.to_string().contains("verified GraphQL bearer"));
    assert!(!TYPED_GUARD_INVOKED.load(Ordering::SeqCst));
    assert!(!TYPED_HANDLER_INVOKED.load(Ordering::SeqCst));
}

#[cfg(feature = "graphql")]
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
        .succeeded(|aggregate| TypedOutput {
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
        .succeeded(|aggregate| TypedOutput {
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

#[cfg(all(feature = "graphql", feature = "application-runtime"))]
#[tokio::test]
async fn generated_mount_registers_and_executes_original_handler_through_causal_protocol() {
    GENERATED_MOUNT_HANDLER_INVOKED.store(0, Ordering::SeqCst);
    let repository = InMemoryRepository::new();
    let routes = generated_mount_handler_register(
        Routes::new().with_repo(repository.aggregate::<CausalDispatcherAggregate>()),
    );
    let service = Service::new().named("generated-mounts").routes(routes);

    assert_eq!(service.registered_command_mounts().len(), 1);
    assert_eq!(
        service.registered_command_mounts()[0].spec().id,
        GENERATED_MOUNT_HANDLER_MOUNT.spec().id
    );

    let request = CommandRequest {
        command: "causal.generated_mount".into(),
        input: json!({"id": "generated-1", "label": "mounted"}),
        session_variables: HashMap::new(),
    };
    let result = service.registered_command_mounts()[0]
        .invoke_with(
            &service,
            &request,
            crate::application::CommandMountInvocation::Authenticated {
                command_id: causal_test_command_id(),
                session: session_with_role("user"),
                principal: causal_test_principal(),
            },
        )
        .await;
    let crate::application::CommandMountExecutionResult::Causal(result) =
        result.expect("authenticated causal mount dispatch should commit")
    else {
        panic!("typed mount must use the causal execution result");
    };
    assert_eq!(result.payload, json!({"id": "generated-1"}));
    assert_eq!(result.receipt.command_name, "causal.generated_mount");
    assert_eq!(GENERATED_MOUNT_HANDLER_INVOKED.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "graphql")]
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

    assert_eq!(first, replay);
    assert_eq!(first.payload, json!({ "id": "todo-receipt" }));
    assert_eq!(first.receipt.command_id, command_id);
    assert_eq!(first.receipt.state, CommandLedgerState::Succeeded);
    assert_eq!(first.receipt.consistency, CommandConsistency::Succeeded);
    assert!(first.receipt.obligations.is_empty());
    assert!(first.receipt.direct_projection.is_none());
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);

    let status = service
        .causal_command_status(&command_id, &Session::new(), principal)
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
    assert!(status.obligations.is_empty());
    assert!(status.evidence.is_empty());
    assert!(status.direct_projection.is_none());
}

#[cfg(feature = "graphql")]
#[test]
fn causal_status_projection_failure_precedes_observed_and_pending_evidence() {
    let item = |obligation_index, state| CausalCommandProjectionEvidence {
        obligation_index,
        state,
        incarnation: (state == CausalProjectionEvidenceState::Observed).then_some(1),
        revision: (state == CausalProjectionEvidenceState::Observed).then_some(2),
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

#[cfg(feature = "graphql")]
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

#[cfg(feature = "graphql")]
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

#[cfg(feature = "graphql")]
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

#[cfg(feature = "graphql")]
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
        .causal_command_status(&command_id, &session_with_role("admin"), principal.clone())
        .await
        .expect("current admin grant should recover the command without its route name");
    assert_eq!(authorized.state, CausalCommandPublicState::Succeeded);
    assert_eq!(authorized.command_id, command_id);

    let revoked = service
        .causal_command_status(&command_id, &session_with_role("user"), principal.clone())
        .await
        .expect("revoked routes must collapse to a non-enumerating status");
    assert_eq!(revoked.state, CausalCommandPublicState::Unknown);

    let other_principal = VerifiedPrincipal::test_oidc(
        "https://issuer.example/",
        "another-subject",
        &["distributed-tests"],
    );
    let wrong_principal = service
        .causal_command_status(&command_id, &session_with_role("admin"), other_principal)
        .await
        .expect("another principal must not learn whether the command exists");
    assert_eq!(wrong_principal.state, CausalCommandPublicState::Unknown);

    let malformed = service
        .causal_command_status(
            "not-a-command-id",
            &session_with_role("admin"),
            principal.clone(),
        )
        .await
        .expect("malformed IDs are non-enumerating, not validation or storage errors");
    assert_eq!(malformed.state, CausalCommandPublicState::Unknown);
    assert_eq!(malformed.command_id, "not-a-command-id");

    let missing_id = causal_test_command_id();
    let missing = service
        .causal_command_status(&missing_id, &session_with_role("admin"), principal)
        .await
        .expect("absent IDs are non-enumerating");
    assert_eq!(missing.state, CausalCommandPublicState::Unknown);
}

#[cfg(feature = "graphql")]
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

#[cfg(feature = "graphql")]
#[tokio::test]
async fn causal_dispatch_uses_the_configured_immediate_outbox_publisher() {
    let repository = InMemoryRepository::new();
    let observed_broker_metadata = Arc::new(Mutex::new(None::<[String; 4]>));
    let route_observed_broker_metadata = Arc::clone(&observed_broker_metadata);
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
                            message.causation_id().unwrap_or_default().to_string(),
                            message
                                .metadata("x-sourced-source-aggregate-type")
                                .unwrap_or_default()
                                .to_string(),
                            message
                                .metadata("x-sourced-source-aggregate-id")
                                .unwrap_or_default()
                                .to_string(),
                            message
                                .metadata("x-sourced-source-sequence")
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
        .with_bus(crate::bus::InMemoryBus::new());

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
            if !outbox
                .messages_by_status(crate::outbox::OutboxMessageStatus::Published, usize::MAX)
                .await
                .unwrap()
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("immediate publish should settle the causal outbox row");
    assert!(outbox.pending(usize::MAX).await.unwrap().is_empty());
    let published = outbox
        .messages_by_status(crate::outbox::OutboxMessageStatus::Published, usize::MAX)
        .await
        .unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].id(), "todo-immediate:immediate-fact");
    assert_eq!(published[0].event_type, "causal.immediate_fact");
    let causation = published[0]
        .causation_id()
        .expect("persisted outbox row should retain ledger causation")
        .to_string();
    assert_eq!(
        published[0].source_aggregate_type.as_deref(),
        Some(CausalDispatcherAggregate::aggregate_type())
    );
    assert_eq!(
        published[0].source_aggregate_id.as_deref(),
        Some("todo-immediate")
    );
    assert_eq!(published[0].source_sequence, Some(1));

    service
        .run(RunOptions::idempotent())
        .await
        .expect("attached bus should deliver the immediately published fact");
    assert_eq!(
        observed_broker_metadata.lock().unwrap().as_ref(),
        Some(&[
            causation,
            CausalDispatcherAggregate::aggregate_type().to_string(),
            "todo-immediate".to_string(),
            "1".to_string(),
        ]),
        "the post-commit clone must carry authoritative causation and aggregate source metadata",
    );
}

#[cfg(feature = "graphql")]
#[tokio::test]
async fn causal_prepared_batch_failure_rolls_back_every_atomic_participant() {
    use crate::{ReadModelWorkspaceExt, RowKey, RowPatch, RowValue, SnapshotStore};

    let repository = InMemoryRepository::new();
    repository
        .model_store()
        .register_schema::<CausalProjectionObligationView>()
        .unwrap();
    repository
        .model_store()
        .register_schema::<CausalProjectionSiblingView>()
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

                        let committed_row_id = format!("{}:committed-row", input.id);
                        let mut read_models = crate::read_model::ReadModelWritePlanBuilder::new();
                        read_models
                            .upsert(&CausalProjectionObligationView {
                                id: committed_row_id,
                            })
                            .map_err(|error| HandlerError::Other(Box::new(error)))?;
                        read_models
                            .upsert(&CausalProjectionSiblingView {
                                id: format!("{}:sibling-row", input.id),
                            })
                            .map_err(|error| HandlerError::Other(Box::new(error)))?;
                        read_models
                            .patch::<CausalProjectionObligationView>(
                                RowKey::new([(
                                    "id",
                                    RowValue::String(format!("{}:missing-row", input.id)),
                                )]),
                                RowPatch::new().set(
                                    "id",
                                    RowValue::String(format!("{}:still-missing", input.id)),
                                ),
                            )
                            .map_err(|error| HandlerError::Other(Box::new(error)))?;

                        let outbox = OutboxMessage::create(
                            format!("{}:atomic-fact", input.id),
                            "causal.atomic_fact",
                            input.label.as_bytes().to_vec(),
                        )
                        .map_err(|error| HandlerError::Other(Box::new(error)))?;

                        context
                            .outbox(outbox)
                            .read_models(read_models)
                            .commit(checkout)?
                            .succeeded(TypedOutput { id: input.id })
                    })();
                    async move { result }
                },
            ),
    );
    let command_id = causal_test_command_id();
    let principal = causal_test_principal();
    let aggregate_id = "atomic-rollback-aggregate";

    let error = service
        .dispatch_causal(
            "causal.atomic_rollback",
            &command_id,
            causal_test_input(aggregate_id, "payload"),
            Session::new(),
            principal.clone(),
        )
        .await
        .expect_err("the update-existing patch against a missing row must fail the batch");
    assert_eq!(error.code(), "INTERNAL");

    let identity =
        crate::StreamIdentity::new(CausalDispatcherAggregate::aggregate_type(), aggregate_id)
            .unwrap();
    assert!(
        repository.get_stream(&identity).await.unwrap().is_none(),
        "aggregate stream must roll back"
    );
    assert!(
        repository
            .outbox_store()
            .pending(usize::MAX)
            .await
            .unwrap()
            .is_empty(),
        "outbox row must roll back"
    );
    assert!(
        repository.get_snapshot(&identity).await.unwrap().is_none(),
        "snapshot must roll back"
    );
    let projected = repository
        .model_store()
        .workspace()
        .load::<CausalProjectionObligationView>(RowKey::new([(
            "id",
            RowValue::String(format!("{aggregate_id}:committed-row")),
        )]))
        .one()
        .await
        .unwrap();
    assert!(
        projected.is_none(),
        "earlier read-model writes must roll back"
    );
    let sibling = repository
        .model_store()
        .workspace()
        .load::<CausalProjectionSiblingView>(RowKey::new([(
            "id",
            RowValue::String(format!("{aggregate_id}:sibling-row")),
        )]))
        .one()
        .await
        .unwrap();
    assert!(
        sibling.is_none(),
        "multi-table writes must roll back together"
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
async fn causal_dispatch_replay_contains_resolved_projection_obligation() {
    // Separately authored command_confirmations! is removed. Succeeded commands
    // without modeled `.emits` finish without finite projection obligations; the
    // outbox fact still stages for downstream projectors.
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
        .causal_command_status(&command_id, &Session::new(), principal.clone())
        .await
        .expect("status should load");
    assert_eq!(status.consistency, Some(CommandConsistency::Succeeded));
    assert!(status.obligations.is_empty());

    let lookup = service
        .lookup_causal_command(
            "causal.projection_obligation",
            &command_id,
            &Session::new(),
            principal,
        )
        .await
        .expect("same principal should be able to recover its command");
    let CommandLookup::Replay(replay) = lookup else {
        panic!("completed command should be replayable");
    };
    assert!(replay.projection_obligations.is_empty());

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
        "causal-direct-v1",
        "first-direct-topology",
        1,
        <CausalProjectionObligationView as crate::read_model::RelationalReadModel>::schema()
            .clone(),
    );
    let second = modeled_direct_registration(
        CAUSAL_SIBLING_DIRECT_PROJECTION,
        "project_causal_direct",
        "causal-direct-v1",
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
    let mut registry = crate::TableSchemaRegistry::new();
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
        .activate(
            vec![
                crate::projection::catalog::ProjectionBindingActivation::new(
                    binding.id(),
                    binding.program_id(),
                    crate::projection_protocol::ProjectionEpoch::new("causal-direct-v1").unwrap(),
                    crate::projection::placement::ProjectionBindingState::Active,
                    Some(
                        crate::projection::placement::ProjectionExecutorRoute::local(
                            "causal-direct",
                        )
                        .unwrap(),
                    ),
                ),
            ],
            None,
        )
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
        .validate_modeled_owner(
            legitimate_executor.name,
            legitimate_executor.epoch,
            legitimate_executor.program_id,
        )
        .unwrap();
    let rogue_executor = CAUSAL_ROGUE_DIRECT_PROJECTION.server_executor().unwrap();
    let rogue_error = target
        .resolve(&json!({}), Some(&Session::new()))
        .unwrap()
        .validate_modeled_owner(
            rogue_executor.name,
            rogue_executor.epoch,
            rogue_executor.program_id,
        )
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
        .causal_command_status(&command_id, &Session::new(), causal_test_principal())
        .await
        .expect("direct projected status should use ledger replay evidence");
    assert_eq!(direct_status.state, CausalCommandPublicState::Atomic);
    assert_eq!(direct_status.consistency, Some(CommandConsistency::Atomic));
    assert!(direct_status.obligations.is_empty());
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

#[cfg(feature = "graphql")]
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

#[cfg(feature = "graphql")]
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

#[tokio::test]
async fn service_collects_route_bundles_with_different_dependencies() {
    let service = Service::new()
        .routes(
            Routes::new()
                .with_dependencies(String::from("orders"))
                .command("string.dep")
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

    let command = service
        .dispatch("string.dep", json!({}), Session::new())
        .await
        .unwrap();
    let event = service
        .dispatch_message(&Message::new(
            "number.dep",
            MessageKind::Event,
            br#"{}"#.to_vec(),
        ))
        .await
        .unwrap();

    assert_eq!(command, json!({ "dependency": "orders" }));
    assert_eq!(event, json!({ "dependency": 7 }));
    assert_eq!(
        service.subscription_plan(),
        SubscriptionPlan {
            commands: vec!["string.dep".to_string()],
            events: vec!["number.dep".to_string()],
        }
    );
}

#[tokio::test]
async fn service_dispatches_all_route_dependency_builder_combinations() {
    let repo_only = InMemoryRepository::new().queued().aggregate();
    let combo_repo = InMemoryRepository::new().queued().aggregate();
    let service = Service::new()
        .routes(
            Routes::new()
                .with_dependencies(String::from("custom"))
                .command("custom.route")
                .handle(|ctx: &Context<String>| {
                    let dependency = ctx.dependencies().clone();
                    async move { Ok(json!({ "route": dependency })) }
                }),
        )
        .routes(
            Routes::new()
                .with_repo(repo_only)
                .command("repo.route")
                .handle(|ctx: &Context<RouteComboRepo>| {
                    let _ = ctx.repo();
                    async move { Ok(json!({ "route": "repo" })) }
                }),
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
                .command("repo-read.route")
                .handle(|ctx: &Context<RouteComboDeps>| {
                    let _ = ctx.repo();
                    let _ = ctx.read_model_store();
                    async move { Ok(json!({ "route": "repo-read" })) }
                }),
        );

    let custom = service
        .dispatch("custom.route", json!({}), Session::new())
        .await
        .unwrap();
    let repo = service
        .dispatch("repo.route", json!({}), Session::new())
        .await
        .unwrap();
    let read = service
        .dispatch_message(&Message::new(
            "read.route",
            MessageKind::Event,
            br#"{}"#.to_vec(),
        ))
        .await
        .unwrap();
    let repo_read = service
        .dispatch("repo-read.route", json!({}), Session::new())
        .await
        .unwrap();

    assert_eq!(custom, json!({ "route": "custom" }));
    assert_eq!(repo, json!({ "route": "repo" }));
    assert_eq!(read, json!({ "route": "read" }));
    assert_eq!(repo_read, json!({ "route": "repo-read" }));
    assert_eq!(
        service.subscription_plan(),
        SubscriptionPlan {
            commands: vec![
                "custom.route".to_string(),
                "repo.route".to_string(),
                "repo-read.route".to_string(),
            ],
            events: vec!["read.route".to_string()],
        }
    );
}

#[test]
fn duplicate_route_names_within_bundle_are_rejected() {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _routes = test_routes()
            .command("same")
            .handle(|_: &Context<()>| async move { Ok(json!({})) })
            .command("same")
            .handle(|_: &Context<()>| async move { Ok(json!({})) });
    }));

    assert!(result.is_err());
}

#[test]
fn duplicate_route_bundle_add_is_rejected_atomically() {
    let mut service = Service::new().routes(
        test_routes()
            .command("same")
            .handle(|_: &Context<()>| async move { Ok(json!({})) }),
    );
    let conflicting = Routes::new()
        .with_dependencies(7_u32)
        .command("same")
        .handle(|_: &Context<u32>| async move { Ok(json!({})) })
        .command("new")
        .handle(|_: &Context<u32>| async move { Ok(json!({})) });

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
    let service = test_service(
        test_routes()
            .command("ping")
            .handle(|_ctx: &Context<()>| async move { Ok(json!({ "pong": true })) }),
    );
    let result = service
        .dispatch("ping", json!({}), Session::new())
        .await
        .unwrap();
    assert_eq!(result, json!({ "pong": true }));
}

#[tokio::test]
async fn unknown_command() {
    // This dispatch records the same {unnamed, unknown, unknown_command}
    // series into the process-global registry that
    // `metrics_bucket_unknown_command_under_fixed_message_label` asserts
    // an exact count on — serialize against it.
    #[cfg(feature = "metrics")]
    let _guard = crate::metrics::async_lock_for_tests().await;

    let service = test_service(
        test_routes()
            .command("ping")
            .handle(|_ctx: &Context<()>| async move { Ok(json!({})) }),
    );
    let result = service.dispatch("unknown", json!({}), Session::new()).await;
    assert!(matches!(result, Err(HandlerError::UnknownCommand(ref s)) if s == "unknown"));
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn metrics_bucket_unknown_command_under_fixed_message_label() {
    let _guard = crate::metrics::async_lock_for_tests().await;
    crate::metrics::reset_for_tests();

    let service = test_service(
        test_routes()
            .command("ping")
            .handle(|_ctx: &Context<()>| async move { Ok(json!({})) }),
    );

    let result = service
        .dispatch("attacker-controlled-path", json!({}), Session::new())
        .await;
    assert!(matches!(result, Err(HandlerError::UnknownCommand(_))));

    let text = crate::metrics::prometheus_text();
    assert!(
            text.contains(
                "distributed_microsvc_dispatch_total{service=\"unnamed\",message_kind=\"command\",message=\"unknown\",status=\"unknown_command\"} 1"
            ),
            "unknown commands should use a bounded message label:\n{text}"
        );
    assert!(
        !text.contains("attacker-controlled-path"),
        "unknown command input must not become a metric label:\n{text}"
    );
}

#[tokio::test]
async fn handler_error_propagates() {
    let service = test_service(
        test_routes()
            .command("fail")
            .handle(|_ctx: &Context<()>| async move { Err(HandlerError::Rejected("nope".into())) }),
    );
    let result = service.dispatch("fail", json!({}), Session::new()).await;
    assert!(matches!(result, Err(HandlerError::Rejected(ref s)) if s == "nope"));
}

#[tokio::test]
async fn decode_error_from_bad_payload() {
    #[derive(serde::Deserialize)]
    struct Input {
        _name: String,
    }

    let service = test_service(test_routes().command("typed").handle(|ctx: &Context<()>| {
        let input = ctx.input::<Input>();
        async move {
            let _input = input?;
            Ok(json!({}))
        }
    }));
    let result = service
        .dispatch("typed", json!({ "wrong": 1 }), Session::new())
        .await;
    assert!(matches!(result, Err(HandlerError::DecodeFailed(_))));
}

#[test]
fn command_names_list() {
    let service = test_service(
        test_routes()
            .command("a")
            .handle(|_: &Context<()>| async move { Ok(json!({})) })
            .command("b")
            .handle(|_: &Context<()>| async move { Ok(json!({})) }),
    );
    let mut cmds = service.command_names();
    cmds.sort();
    assert_eq!(cmds, vec!["a", "b"]);
}

#[test]
fn subscription_plan_separates_commands_and_events() {
    const EVENTS: &[&str] = &["checkout.started", "seat.reserved"];

    let service = test_service(
        test_routes()
            .command("checkout.start")
            .handle(|_: &Context<()>| async move { Ok(json!({})) })
            .events(EVENTS)
            .guarded(|_| true, |_: &Context<()>| async move { Ok(json!({})) }),
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
    let service = test_service(
        test_routes()
            .command("shared")
            .handle(|ctx: &Context<()>| {
                let kind = format!("{:?}", ctx.message().kind);
                async move { Ok(json!({ "kind": kind })) }
            })
            .event("shared")
            .handle(|ctx: &Context<()>| {
                let event_id = ctx.message().id().map(|s| s.to_string());
                async move { Ok(json!({ "event_id": event_id })) }
            }),
    );
    let event_message =
        Message::new("shared", MessageKind::Event, br#"{}"#.to_vec()).with_id("evt-1");

    let command_result = service
        .dispatch("shared", json!({}), Session::new())
        .await
        .unwrap();
    let event_result = service.dispatch_message(&event_message).await.unwrap();

    assert_eq!(command_result, json!({ "kind": "Command" }));
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
    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let service = test_service(test_routes().command("checkout.start").handle(
        |ctx: &Context<()>| {
            let trace_context = ctx.message().trace_context();
            async move {
                Ok(json!({
                    "traceparent": trace_context.traceparent,
                    "tracestate": trace_context.tracestate,
                }))
            }
        },
    ));
    let session = Session::from_map(HashMap::from([
        ("traceparent".to_string(), traceparent.to_string()),
        ("tracestate".to_string(), "vendor=value".to_string()),
    ]));

    let result = service
        .dispatch("checkout.start", json!({}), session)
        .await
        .unwrap();

    assert_eq!(
        result,
        json!({ "traceparent": traceparent, "tracestate": "vendor=value" })
    );
}

#[tokio::test]
async fn guard_passes() {
    let service = test_service(test_routes().command("greet").guarded(
        |ctx| ctx.has_fields(&["name"]),
        |ctx: &Context<()>| {
            let name = ctx.raw_input()["name"].as_str().map(|s| s.to_string());
            async move { Ok(json!({ "hello": name.unwrap() })) }
        },
    ));
    let result = service
        .dispatch("greet", json!({ "name": "Pat" }), Session::new())
        .await
        .unwrap();
    assert_eq!(result, json!({ "hello": "Pat" }));
}

#[tokio::test]
async fn guard_rejects() {
    let service = test_service(test_routes().command("greet").guarded(
        |ctx| ctx.has_fields(&["name"]),
        |_ctx: &Context<()>| async move {
            panic!("handler should not run");
            #[allow(unreachable_code)]
            Ok(json!({}))
        },
    ));
    let result = service
        .dispatch("greet", json!({ "wrong": 1 }), Session::new())
        .await;
    assert!(matches!(result, Err(HandlerError::GuardRejected(ref s)) if s == "greet"));
}

#[tokio::test]
async fn guard_checks_session() {
    let service = test_service(test_routes().command("admin").guarded(
        |ctx| ctx.role() == Some("admin"),
        |_ctx: &Context<()>| async move { Ok(json!({ "ok": true })) },
    ));

    // No role
    assert!(service
        .dispatch("admin", json!({}), Session::new())
        .await
        .is_err());

    // Admin role
    let mut session = Session::new();
    session.set(crate::microsvc::ROLE_KEY, "admin");
    assert!(service.dispatch("admin", json!({}), session).await.is_ok());
}

#[tokio::test]
async fn dispatch_request_success() {
    let service = test_service(
        test_routes()
            .command("ping")
            .handle(|_ctx: &Context<()>| async move { Ok(json!({ "pong": true })) }),
    );
    let request = CommandRequest {
        command: "ping".to_string(),
        input: json!({}),
        session_variables: HashMap::new(),
    };
    let response = service.dispatch_request(&request).await;
    assert_eq!(response.status, 200);
    assert_eq!(response.body, json!({ "pong": true }));
}

#[tokio::test]
async fn dispatch_request_error_codes() {
    let service = test_service(
        test_routes()
            .command("reject")
            .handle(|_: &Context<()>| async move { Err(HandlerError::Rejected("no".into())) })
            .command("unauth")
            .handle(|ctx: &Context<()>| {
                let user_id = ctx.user_id().map(|s| s.to_string());
                async move {
                    let _ = user_id?;
                    Ok(json!({}))
                }
            }),
    );

    let resp = service
        .dispatch_request(&CommandRequest {
            command: "unknown".to_string(),
            input: json!({}),
            session_variables: HashMap::new(),
        })
        .await;
    assert_eq!(resp.status, 404);

    let resp = service
        .dispatch_request(&CommandRequest {
            command: "reject".to_string(),
            input: json!({}),
            session_variables: HashMap::new(),
        })
        .await;
    assert_eq!(resp.status, 422);

    let resp = service
        .dispatch_request(&CommandRequest {
            command: "unauth".to_string(),
            input: json!({}),
            session_variables: HashMap::new(),
        })
        .await;
    assert_eq!(resp.status, 401);
}

#[tokio::test]
async fn dispatch_request_passes_session() {
    let service = test_service(test_routes().command("whoami").handle(|ctx: &Context<()>| {
        let user_id = ctx.user_id().map(|s| s.to_string());
        async move {
            let user_id = user_id?;
            Ok(json!({ "user_id": user_id }))
        }
    }));
    let mut vars = HashMap::new();
    vars.insert(
        crate::microsvc::USER_ID_KEY.to_string(),
        "user-99".to_string(),
    );
    let request = CommandRequest {
        command: "whoami".to_string(),
        input: json!({}),
        session_variables: vars,
    };
    let response = service.dispatch_request(&request).await;
    assert_eq!(response.status, 200);
    assert_eq!(response.body, json!({ "user_id": "user-99" }));
}

#[test]
fn command_request_requires_session_variables_field() {
    let json = r#"{"command":"ping","input":{}}"#;
    let result: Result<CommandRequest, _> = serde_json::from_str(json);
    assert!(result.is_err());
}
