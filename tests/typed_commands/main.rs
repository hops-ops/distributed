#![cfg(all(feature = "graphql", feature = "sqlite"))]
#![allow(dead_code)]

use std::any::TypeId;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_graphql::futures_util::StreamExt;
use async_graphql::Request;
use axum::body::Body;
use axum::http::Request as HttpRequest;
use distributed::command::{
    typed_command, Atomic, CommandInputType, CommandOutputType, CommandTypeDef, CommandTypeField,
    EffectInputFieldMarker, EffectModelFieldMarker, Eventual, PreparedCommand, Succeeded,
};
use distributed::graphql::{
    build_surface, graphql_router_with_service, read, surface_for_role, ClientProjectionAssignment,
    ClientProjectionExecutionClass, ClientProjectionExpression, ClientProjectionFallback,
    ClientProjectionInvalidation, ClientProjectionMutationKind, ClientProjectionPartition,
    ClientProjectionPreviewSource, ClientProjectionValue, ClientProjectionValueType,
    DistributedClientSurfaceExport, GraphqlEngine, ModelPermissions, RoleGrant,
    SurfaceDirectProjection, SurfaceModeledProjection, SurfaceOptions, SurfaceProjector,
};
use distributed::microsvc::{CausalCommandContext, HandlerError, Routes, Service};
use distributed::microsvc::{Context, Session};
use distributed::projection::catalog::{ProjectionBindingActivation, ProjectionCatalog};
use distributed::projection::lower::{DirectCandidate, EventualOnly, ProjectionDescriptor};
use distributed::projection::lower::{
    LoweredProjectionPlan, ProjectionLoweringError, ProjectionOutputInventory,
};
use distributed::projection::placement::{
    ProjectionBinding, ProjectionBindingState, ProjectionEpoch, ProjectionExecutorRoute,
    ProjectionOutput, ProjectionOwner, ProjectionPhysicalTopology, ProjectionSourceBinding,
    PROJECTION_PARTITION_CODEC_VERSION,
};
use distributed::projection::ProjectionEventSelector;
use distributed::projection_protocol::ProjectorTopologyId;
use distributed::{
    body_bindings_for_model, body_field_binding, command_input_defaults, compile_projection,
    descriptor_from_factories, inventory_single_model, lower_single_model,
    resolve_mutation_program, state_upsert_program_for_model, Aggregate, AggregateRepository,
    CommandInput, CommandOutput, DomainEventDescriptor, DomainEventOccurrence, Entity, EventRecord,
    InMemoryRepository, MutationAssignment, MutationEventBinding, MutationExpression,
    MutationField, MutationKeyField, MutationKind, MutationOperation, MutationProgram,
    MutationProgramError, ProjectionExpression, ProjectionHandler, ProjectionPartition,
    ProjectionProgram, ProjectionProgramError, ProjectionValue, ProjectionValueType, ReadModel,
    ReadModelCatalog, RelationalReadModel, ResolvedProjectionPlan, SqliteRepository,
};
use serde::{Deserialize, Serialize};
use tower::util::ServiceExt;

static GRAPHQL_TYPED_GUARD_INVOKED: AtomicBool = AtomicBool::new(false);
static GRAPHQL_TYPED_HANDLER_INVOKED: AtomicBool = AtomicBool::new(false);
const TEST_PROTOCOL_TOKEN_KEY: [u8; 32] = [0x5a; 32];

#[derive(Default)]
struct FixtureAggregate {
    entity: Entity,
}

impl Aggregate for FixtureAggregate {
    type ReplayError = String;

    fn aggregate_type() -> &'static str {
        "typed-command-fixture"
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

fn causal_routes() -> Routes<AggregateRepository<InMemoryRepository, FixtureAggregate>> {
    Routes::new().with_repo(AggregateRepository::new(InMemoryRepository::new()))
}

#[derive(Deserialize)]
struct InputA {
    id: String,
}

#[derive(Serialize)]
struct OutputA {
    id: String,
}

#[derive(Deserialize)]
struct InputB {
    id: String,
}

#[derive(Serialize)]
struct OutputB {
    id: String,
}

#[derive(Clone, Deserialize, CommandInput)]
struct PlanInput {
    id: String,
    title: String,
}

#[derive(Clone, Deserialize, CommandInput)]
struct RenamedDefaultInput {
    #[serde(rename = "todoId")]
    id: String,
}

#[derive(Serialize, CommandOutput)]
struct PlanOutput {
    id: String,
}

#[derive(Clone, Serialize, Deserialize, ReadModel, distributed::DomainState)]
#[readmodel(table = "plan_views", primary_key = ["id"])]
#[domain_state(version = 1)]
struct PlanView {
    id: String,
    title: String,
    count: i64,
    #[readmodel(text)]
    status: PlanStatus,
}

#[derive(Clone, Serialize, Deserialize)]
enum PlanStatus {
    Open,
    Closed,
}

impl CommandOutputType for PlanView {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "PlanView",
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
                    name: "title".into(),
                    type_name: "String".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                },
                CommandTypeField {
                    name: "count".into(),
                    type_name: "BigInt".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                },
                CommandTypeField {
                    name: "status".into(),
                    type_name: "String".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                },
            ],
        )
        .with_type_id(TypeId::of::<Self>())
    }
}

#[derive(Clone, Deserialize, CommandInput)]
struct ForgedInput {
    id: String,
    title: String,
}

#[derive(Clone, Serialize, Deserialize, ReadModel)]
#[readmodel(table = "forged_views", primary_key = ["id"])]
struct ForgedView {
    id: String,
    count: i64,
}

#[derive(Clone, Serialize, Deserialize, CommandInput)]
struct JsonDocument {
    label: String,
}

#[derive(Clone, Deserialize, CommandInput)]
struct JsonPatchInput {
    id: String,
    tags: Vec<String>,
    details: JsonDocument,
}

#[derive(Clone, Serialize, Deserialize, ReadModel, distributed::DomainState)]
#[readmodel(table = "json_views", primary_key = ["id"])]
#[domain_state(version = 1)]
struct JsonView {
    id: String,
    #[readmodel(jsonb)]
    tags: Vec<String>,
    #[readmodel(jsonb)]
    details: JsonDocument,
}

#[derive(Clone, Deserialize, CommandInput)]
struct BigIntKeyInput {
    key: i64,
    title: String,
}

#[derive(Clone, Serialize, Deserialize, ReadModel, distributed::DomainState)]
#[readmodel(table = "bigint_key_views", primary_key = ["key"])]
#[domain_state(version = 1)]
struct BigIntKeyView {
    id: String,
    key: i64,
    title: String,
}

#[derive(Clone, Deserialize, CommandInput)]
struct BigIntRelationshipInput {
    source_key: i64,
    target_id: String,
}

#[derive(Clone, Serialize, Deserialize, ReadModel)]
#[readmodel(table = "bigint_relation_targets", primary_key = ["id"])]
struct BigIntRelationshipTarget {
    id: String,
    #[readmodel(
        foreign_key = "bigint_relation_sources.key",
        delegated_from = "BigIntRelationshipSource.key"
    )]
    source_key: i64,
}

#[derive(Clone, Serialize, Deserialize, ReadModel)]
#[readmodel(table = "bigint_relation_sources", primary_key = ["key"])]
struct BigIntRelationshipSource {
    id: String,
    key: i64,
    #[readmodel(has_many = "BigIntRelationshipTarget", foreign_key = "source_key")]
    targets: Vec<BigIntRelationshipTarget>,
}

#[derive(Clone, Deserialize, CommandInput)]
struct NullableKeyInput {
    key: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, ReadModel)]
#[readmodel(table = "nullable_key_views", primary_key = ["key"])]
struct NullableKeyView {
    id: String,
    key: Option<String>,
    title: String,
}

#[derive(Clone, Deserialize, CommandInput)]
struct CompositeKeyInput {
    tenant_id: String,
    id: String,
    title: String,
}

#[derive(Clone, Serialize, Deserialize, ReadModel, distributed::DomainState)]
#[readmodel(
    table = "composite_key_views",
    primary_key = ["tenant_id", "id"]
)]
#[domain_state(version = 1)]
struct CompositeKeyView {
    tenant_id: String,
    id: String,
    title: String,
}

#[derive(Clone, Deserialize, CommandInput)]
struct FloatEffectInput {
    id: String,
}

#[derive(Clone, Serialize, Deserialize, ReadModel)]
#[readmodel(table = "float_effect_views", primary_key = ["id"])]
struct FloatEffectView {
    id: String,
    value: Option<f64>,
}

#[derive(Clone, Serialize, Deserialize)]
struct NestedJsonFloat {
    value: f64,
}

#[derive(Clone, Serialize, Deserialize)]
struct NestedJsonDocument {
    nested: NestedJsonFloat,
}

const NONFINITE_JSON_DOCUMENT: NestedJsonDocument = NestedJsonDocument {
    nested: NestedJsonFloat {
        value: f64::NEG_INFINITY,
    },
};

#[derive(Clone, Serialize, Deserialize, ReadModel)]
#[readmodel(table = "json_float_effect_views", primary_key = ["id"])]
struct JsonFloatEffectView {
    id: String,
    #[readmodel(jsonb)]
    value_f32: f32,
    #[readmodel(jsonb)]
    value_f64: f64,
    #[readmodel(jsonb)]
    document: serde_json::Value,
    #[readmodel(jsonb)]
    nested_document: NestedJsonDocument,
}

#[derive(Clone, Debug)]
struct BrokenText;

impl Serialize for BrokenText {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom("broken constant serializer"))
    }
}

impl<'de> Deserialize<'de> for BrokenText {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _ = serde::de::IgnoredAny::deserialize(deserializer)?;
        Ok(Self)
    }
}

#[derive(Clone, Serialize, Deserialize, ReadModel)]
#[readmodel(table = "broken_constant_views", primary_key = ["id"])]
struct BrokenConstantView {
    id: String,
    #[readmodel(text)]
    value: BrokenText,
}

macro_rules! state_event_contract {
    ($marker:ident, $state:ty, $name:literal) => {
        enum $marker {}

        impl distributed::domain_event::DomainEventContract for $marker {
            const EVENT_NAME: &'static str = $name;
            const EVENT_VERSION: u64 = 1;

            fn descriptor() -> distributed::DomainEventDescriptor {
                <$state as distributed::DomainState>::DESCRIPTOR
                    .clone()
                    .event(Self::EVENT_NAME, Self::EVENT_VERSION)
            }
        }

        impl distributed::domain_event::DomainEventBodyContract<$state> for $marker {}
    };
}

state_event_contract!(PlanChangedDomainEvent, PlanView, "plan.changed");
state_event_contract!(PlanRenamedDomainEvent, PlanView, "plan.renamed");

fn map_mut_err(operation: &str, err: MutationProgramError) -> ProjectionProgramError {
    ProjectionProgramError::InvalidOperation {
        operation: operation.into(),
        reason: err.to_string(),
    }
}

fn state_selector<S: distributed::DomainState>(
    name: &'static str,
    version: u64,
) -> Result<ProjectionEventSelector, ProjectionProgramError> {
    ProjectionEventSelector::try_from_descriptor(&DomainEventDescriptor::state::<S>(name, version))
}

fn plan_projection_program() -> Result<ProjectionProgram, ProjectionProgramError> {
    let program = state_upsert_program_for_model::<PlanView>("save_plan", 1, "upsert-plan", "plan")
        .map_err(|e| map_mut_err("typed_commands_plan", e))?;
    let binding = MutationEventBinding::try_new(
        state_selector::<PlanView>("plan.changed", 1)?,
        body_bindings_for_model::<PlanView>("plan")
            .map_err(|e| map_mut_err("typed_commands_plan", e))?,
        program,
    )
    .map_err(|e| map_mut_err("typed_commands_plan", e))?;
    let partition = ProjectionPartition::Expression(ProjectionExpression::body_path(
        ProjectionValueType::String,
        ["id"],
    )?);
    compile_projection(
        "typed_commands_plan",
        1,
        partition,
        [ProjectionHandler::from_binding("changed", binding)],
    )
}

fn plan_projection_resolve(
    occurrence: &DomainEventOccurrence,
) -> Result<ResolvedProjectionPlan, ProjectionProgramError> {
    resolve_mutation_program(&plan_projection_program()?, occurrence)
}

fn plan_projection_lower(
    plan: &ResolvedProjectionPlan,
) -> Result<LoweredProjectionPlan, ProjectionLoweringError> {
    lower_single_model::<PlanView>(plan)
}

fn plan_projection_inventory() -> Result<ProjectionOutputInventory, ProjectionLoweringError> {
    inventory_single_model::<PlanView>()
}

const PLAN_PROJECTION: ProjectionDescriptor<DirectCandidate> = descriptor_from_factories(
    "typed_commands_plan",
    1,
    "typed-commands-plan-v1",
    plan_projection_program,
    plan_projection_resolve,
    plan_projection_lower,
    plan_projection_inventory,
);

fn patch_key_string(name: &str) -> Result<MutationKeyField, MutationProgramError> {
    MutationKeyField::try_new(
        0,
        name,
        MutationExpression::input_path(ProjectionValueType::String, [name])?,
    )
}

fn plan_title_program() -> Result<ProjectionProgram, ProjectionProgramError> {
    let target = distributed::ProjectionTarget::try_new("PlanView", "plan_views")?;
    let op = MutationOperation::try_new(
        "patch-title",
        0,
        MutationKind::Patch,
        target,
        vec![patch_key_string("id").map_err(|e| map_mut_err("typed_commands_plan_title", e))?],
        vec![MutationField::try_new(
            0,
            "title",
            MutationAssignment::set(
                MutationExpression::input_path(ProjectionValueType::String, ["title"])
                    .map_err(|e| map_mut_err("typed_commands_plan_title", e))?,
            ),
        )
        .map_err(|e| map_mut_err("typed_commands_plan_title", e))?],
        None,
        Vec::new(),
        Vec::new(),
        None,
    )
    .map_err(|e| map_mut_err("typed_commands_plan_title", e))?;
    let mutation = MutationProgram::try_new("patch_plan_title", 1, vec![op])
        .map_err(|e| map_mut_err("typed_commands_plan_title", e))?;
    let bindings = vec![
        body_field_binding(["id"], ["id"], ProjectionValueType::String)
            .map_err(|e| map_mut_err("typed_commands_plan_title", e))?,
        body_field_binding(["title"], ["title"], ProjectionValueType::String)
            .map_err(|e| map_mut_err("typed_commands_plan_title", e))?,
    ];
    let binding = MutationEventBinding::try_new(
        state_selector::<PlanView>("plan.changed", 1)?,
        bindings,
        mutation,
    )
    .map_err(|e| map_mut_err("typed_commands_plan_title", e))?;
    compile_projection(
        "typed_commands_plan_title",
        1,
        ProjectionPartition::Unit,
        [ProjectionHandler::from_binding("changed", binding)],
    )
}

fn plan_title_resolve(
    occurrence: &DomainEventOccurrence,
) -> Result<ResolvedProjectionPlan, ProjectionProgramError> {
    resolve_mutation_program(&plan_title_program()?, occurrence)
}

fn plan_title_lower(
    plan: &ResolvedProjectionPlan,
) -> Result<LoweredProjectionPlan, ProjectionLoweringError> {
    lower_single_model::<PlanView>(plan)
}

fn plan_title_inventory() -> Result<ProjectionOutputInventory, ProjectionLoweringError> {
    inventory_single_model::<PlanView>()
}

const PLAN_TITLE_PROJECTION: ProjectionDescriptor<EventualOnly> = descriptor_from_factories(
    "typed_commands_plan_title",
    1,
    "typed-commands-plan-title-v1",
    plan_title_program,
    plan_title_resolve,
    plan_title_lower,
    plan_title_inventory,
);

fn plan_close_program() -> Result<ProjectionProgram, ProjectionProgramError> {
    let target = distributed::ProjectionTarget::try_new("PlanView", "plan_views")?;
    let op = MutationOperation::try_new(
        "patch-status",
        0,
        MutationKind::Patch,
        target,
        vec![patch_key_string("id").map_err(|e| map_mut_err("typed_commands_plan_close", e))?],
        vec![MutationField::try_new(
            0,
            "status",
            MutationAssignment::set(
                MutationExpression::enum_variant("PlanStatus", "Closed")
                    .map_err(|e| map_mut_err("typed_commands_plan_close", e))?,
            ),
        )
        .map_err(|e| map_mut_err("typed_commands_plan_close", e))?],
        None,
        Vec::new(),
        Vec::new(),
        None,
    )
    .map_err(|e| map_mut_err("typed_commands_plan_close", e))?;
    let mutation = MutationProgram::try_new("patch_plan_close", 1, vec![op])
        .map_err(|e| map_mut_err("typed_commands_plan_close", e))?;
    let bindings = vec![
        body_field_binding(["id"], ["id"], ProjectionValueType::String)
            .map_err(|e| map_mut_err("typed_commands_plan_close", e))?,
    ];
    let binding = MutationEventBinding::try_new(
        state_selector::<PlanView>("plan.changed", 1)?,
        bindings,
        mutation,
    )
    .map_err(|e| map_mut_err("typed_commands_plan_close", e))?;
    compile_projection(
        "typed_commands_plan_close",
        1,
        ProjectionPartition::Unit,
        [ProjectionHandler::from_binding("changed", binding)],
    )
}

fn plan_close_resolve(
    occurrence: &DomainEventOccurrence,
) -> Result<ResolvedProjectionPlan, ProjectionProgramError> {
    resolve_mutation_program(&plan_close_program()?, occurrence)
}

fn plan_close_lower(
    plan: &ResolvedProjectionPlan,
) -> Result<LoweredProjectionPlan, ProjectionLoweringError> {
    lower_single_model::<PlanView>(plan)
}

fn plan_close_inventory() -> Result<ProjectionOutputInventory, ProjectionLoweringError> {
    inventory_single_model::<PlanView>()
}

const PLAN_CLOSE_PROJECTION: ProjectionDescriptor<EventualOnly> = descriptor_from_factories(
    "typed_commands_plan_close",
    1,
    "typed-commands-plan-close-v1",
    plan_close_program,
    plan_close_resolve,
    plan_close_lower,
    plan_close_inventory,
);

state_event_contract!(JsonChangedDomainEvent, JsonView, "json.changed");

fn json_projection_program() -> Result<ProjectionProgram, ProjectionProgramError> {
    let target = distributed::ProjectionTarget::try_new("JsonView", "json_views")?;
    let op = MutationOperation::try_new(
        "patch-json",
        0,
        MutationKind::Patch,
        target,
        vec![patch_key_string("id").map_err(|e| map_mut_err("typed_commands_json", e))?],
        vec![
            MutationField::try_new(
                0,
                "tags",
                MutationAssignment::set(
                    MutationExpression::input_path(ProjectionValueType::Json, ["tags"])
                        .map_err(|e| map_mut_err("typed_commands_json", e))?,
                ),
            )
            .map_err(|e| map_mut_err("typed_commands_json", e))?,
            MutationField::try_new(
                1,
                "details",
                MutationAssignment::set(
                    MutationExpression::input_path(ProjectionValueType::Json, ["details"])
                        .map_err(|e| map_mut_err("typed_commands_json", e))?,
                ),
            )
            .map_err(|e| map_mut_err("typed_commands_json", e))?,
        ],
        None,
        Vec::new(),
        Vec::new(),
        None,
    )
    .map_err(|e| map_mut_err("typed_commands_json", e))?;
    let mutation = MutationProgram::try_new("patch_json", 1, vec![op])
        .map_err(|e| map_mut_err("typed_commands_json", e))?;
    let bindings = vec![
        body_field_binding(["id"], ["id"], ProjectionValueType::String)
            .map_err(|e| map_mut_err("typed_commands_json", e))?,
        body_field_binding(["tags"], ["tags"], ProjectionValueType::Json)
            .map_err(|e| map_mut_err("typed_commands_json", e))?,
        body_field_binding(["details"], ["details"], ProjectionValueType::Json)
            .map_err(|e| map_mut_err("typed_commands_json", e))?,
    ];
    let binding = MutationEventBinding::try_new(
        state_selector::<JsonView>("json.changed", 1)?,
        bindings,
        mutation,
    )
    .map_err(|e| map_mut_err("typed_commands_json", e))?;
    compile_projection(
        "typed_commands_json",
        1,
        ProjectionPartition::Unit,
        [ProjectionHandler::from_binding("changed", binding)],
    )
}

fn json_projection_resolve(
    occurrence: &DomainEventOccurrence,
) -> Result<ResolvedProjectionPlan, ProjectionProgramError> {
    resolve_mutation_program(&json_projection_program()?, occurrence)
}

fn json_projection_lower(
    plan: &ResolvedProjectionPlan,
) -> Result<LoweredProjectionPlan, ProjectionLoweringError> {
    lower_single_model::<JsonView>(plan)
}

fn json_projection_inventory() -> Result<ProjectionOutputInventory, ProjectionLoweringError> {
    inventory_single_model::<JsonView>()
}

const JSON_PROJECTION: ProjectionDescriptor<EventualOnly> = descriptor_from_factories(
    "typed_commands_json",
    1,
    "typed-commands-json-v1",
    json_projection_program,
    json_projection_resolve,
    json_projection_lower,
    json_projection_inventory,
);

state_event_contract!(
    CompositeChangedDomainEvent,
    CompositeKeyView,
    "composite.changed"
);

fn composite_projection_program() -> Result<ProjectionProgram, ProjectionProgramError> {
    let target = distributed::ProjectionTarget::try_new("CompositeKeyView", "composite_key_views")?;
    let op = MutationOperation::try_new(
        "patch-composite",
        0,
        MutationKind::Patch,
        target,
        vec![
            MutationKeyField::try_new(
                0,
                "tenant_id",
                MutationExpression::input_path(ProjectionValueType::String, ["tenant_id"])
                    .map_err(|e| map_mut_err("typed_commands_composite", e))?,
            )
            .map_err(|e| map_mut_err("typed_commands_composite", e))?,
            MutationKeyField::try_new(
                1,
                "id",
                MutationExpression::input_path(ProjectionValueType::String, ["id"])
                    .map_err(|e| map_mut_err("typed_commands_composite", e))?,
            )
            .map_err(|e| map_mut_err("typed_commands_composite", e))?,
        ],
        vec![MutationField::try_new(
            0,
            "title",
            MutationAssignment::set(
                MutationExpression::input_path(ProjectionValueType::String, ["title"])
                    .map_err(|e| map_mut_err("typed_commands_composite", e))?,
            ),
        )
        .map_err(|e| map_mut_err("typed_commands_composite", e))?],
        None,
        Vec::new(),
        Vec::new(),
        None,
    )
    .map_err(|e| map_mut_err("typed_commands_composite", e))?;
    let mutation = MutationProgram::try_new("patch_composite", 1, vec![op])
        .map_err(|e| map_mut_err("typed_commands_composite", e))?;
    let bindings = vec![
        body_field_binding(["tenant_id"], ["tenant_id"], ProjectionValueType::String)
            .map_err(|e| map_mut_err("typed_commands_composite", e))?,
        body_field_binding(["id"], ["id"], ProjectionValueType::String)
            .map_err(|e| map_mut_err("typed_commands_composite", e))?,
        body_field_binding(["title"], ["title"], ProjectionValueType::String)
            .map_err(|e| map_mut_err("typed_commands_composite", e))?,
    ];
    let binding = MutationEventBinding::try_new(
        state_selector::<CompositeKeyView>("composite.changed", 1)?,
        bindings,
        mutation,
    )
    .map_err(|e| map_mut_err("typed_commands_composite", e))?;
    compile_projection(
        "typed_commands_composite",
        1,
        ProjectionPartition::Unit,
        [ProjectionHandler::from_binding("changed", binding)],
    )
}

fn composite_projection_resolve(
    occurrence: &DomainEventOccurrence,
) -> Result<ResolvedProjectionPlan, ProjectionProgramError> {
    resolve_mutation_program(&composite_projection_program()?, occurrence)
}

fn composite_projection_lower(
    plan: &ResolvedProjectionPlan,
) -> Result<LoweredProjectionPlan, ProjectionLoweringError> {
    lower_single_model::<CompositeKeyView>(plan)
}

fn composite_projection_inventory() -> Result<ProjectionOutputInventory, ProjectionLoweringError> {
    inventory_single_model::<CompositeKeyView>()
}

const COMPOSITE_PROJECTION: ProjectionDescriptor<EventualOnly> = descriptor_from_factories(
    "typed_commands_composite",
    1,
    "typed-commands-composite-v1",
    composite_projection_program,
    composite_projection_resolve,
    composite_projection_lower,
    composite_projection_inventory,
);

state_event_contract!(BigIntChangedDomainEvent, BigIntKeyView, "bigint.changed");

fn bigint_projection_program() -> Result<ProjectionProgram, ProjectionProgramError> {
    let program =
        state_upsert_program_for_model::<BigIntKeyView>("save_bigint", 1, "upsert-view", "view")
            .map_err(|e| map_mut_err("typed_commands_bigint", e))?;
    let binding = MutationEventBinding::try_new(
        state_selector::<BigIntKeyView>("bigint.changed", 1)?,
        body_bindings_for_model::<BigIntKeyView>("view")
            .map_err(|e| map_mut_err("typed_commands_bigint", e))?,
        program,
    )
    .map_err(|e| map_mut_err("typed_commands_bigint", e))?;
    compile_projection(
        "typed_commands_bigint",
        1,
        ProjectionPartition::Unit,
        [ProjectionHandler::from_binding("changed", binding)],
    )
}

fn bigint_projection_resolve(
    occurrence: &DomainEventOccurrence,
) -> Result<ResolvedProjectionPlan, ProjectionProgramError> {
    resolve_mutation_program(&bigint_projection_program()?, occurrence)
}

fn bigint_projection_lower(
    plan: &ResolvedProjectionPlan,
) -> Result<LoweredProjectionPlan, ProjectionLoweringError> {
    lower_single_model::<BigIntKeyView>(plan)
}

fn bigint_projection_inventory() -> Result<ProjectionOutputInventory, ProjectionLoweringError> {
    inventory_single_model::<BigIntKeyView>()
}

const BIGINT_PROJECTION: ProjectionDescriptor<DirectCandidate> = descriptor_from_factories(
    "typed_commands_bigint",
    1,
    "typed-commands-bigint-v1",
    bigint_projection_program,
    bigint_projection_resolve,
    bigint_projection_lower,
    bigint_projection_inventory,
);

#[derive(Clone, Serialize, distributed::DomainState)]
#[domain_state(version = 1)]
struct FloatClearState {
    id: String,
}

state_event_contract!(FloatClearedDomainEvent, FloatClearState, "float.cleared");

fn float_clear_program() -> Result<ProjectionProgram, ProjectionProgramError> {
    let target = distributed::ProjectionTarget::try_new("FloatEffectView", "float_effect_views")?;
    let op = MutationOperation::try_new(
        "patch-float-clear",
        0,
        MutationKind::Patch,
        target,
        vec![patch_key_string("id").map_err(|e| map_mut_err("typed_commands_float_clear", e))?],
        vec![MutationField::try_new(
            0,
            "value",
            MutationAssignment::set(MutationExpression::constant(ProjectionValue::null())),
        )
        .map_err(|e| map_mut_err("typed_commands_float_clear", e))?],
        None,
        Vec::new(),
        Vec::new(),
        None,
    )
    .map_err(|e| map_mut_err("typed_commands_float_clear", e))?;
    let mutation = MutationProgram::try_new("patch_float_clear", 1, vec![op])
        .map_err(|e| map_mut_err("typed_commands_float_clear", e))?;
    let bindings = vec![
        body_field_binding(["id"], ["id"], ProjectionValueType::String)
            .map_err(|e| map_mut_err("typed_commands_float_clear", e))?,
    ];
    let binding = MutationEventBinding::try_new(
        state_selector::<FloatClearState>("float.cleared", 1)?,
        bindings,
        mutation,
    )
    .map_err(|e| map_mut_err("typed_commands_float_clear", e))?;
    compile_projection(
        "typed_commands_float_clear",
        1,
        ProjectionPartition::Unit,
        [ProjectionHandler::from_binding("cleared", binding)],
    )
}

fn float_clear_resolve(
    occurrence: &DomainEventOccurrence,
) -> Result<ResolvedProjectionPlan, ProjectionProgramError> {
    resolve_mutation_program(&float_clear_program()?, occurrence)
}

fn float_clear_lower(
    plan: &ResolvedProjectionPlan,
) -> Result<LoweredProjectionPlan, ProjectionLoweringError> {
    lower_single_model::<FloatEffectView>(plan)
}

fn float_clear_inventory() -> Result<ProjectionOutputInventory, ProjectionLoweringError> {
    inventory_single_model::<FloatEffectView>()
}

const FLOAT_CLEAR_PROJECTION: ProjectionDescriptor<EventualOnly> = descriptor_from_factories(
    "typed_commands_float_clear",
    1,
    "typed-commands-float-clear-v1",
    float_clear_program,
    float_clear_resolve,
    float_clear_lower,
    float_clear_inventory,
);

state_event_contract!(
    JsonFloatClearedDomainEvent,
    FloatClearState,
    "json-float.cleared"
);

fn json_float_clear_program() -> Result<ProjectionProgram, ProjectionProgramError> {
    let target =
        distributed::ProjectionTarget::try_new("JsonFloatEffectView", "json_float_effect_views")?;
    let op = MutationOperation::try_new(
        "patch-json-float-clear",
        0,
        MutationKind::Patch,
        target,
        vec![patch_key_string("id")
            .map_err(|e| map_mut_err("typed_commands_json_float_clear", e))?],
        vec![MutationField::try_new(
            0,
            "document",
            MutationAssignment::set(MutationExpression::constant(ProjectionValue::null())),
        )
        .map_err(|e| map_mut_err("typed_commands_json_float_clear", e))?],
        None,
        Vec::new(),
        Vec::new(),
        None,
    )
    .map_err(|e| map_mut_err("typed_commands_json_float_clear", e))?;
    let mutation = MutationProgram::try_new("patch_json_float_clear", 1, vec![op])
        .map_err(|e| map_mut_err("typed_commands_json_float_clear", e))?;
    let bindings = vec![
        body_field_binding(["id"], ["id"], ProjectionValueType::String)
            .map_err(|e| map_mut_err("typed_commands_json_float_clear", e))?,
    ];
    let binding = MutationEventBinding::try_new(
        state_selector::<FloatClearState>("json-float.cleared", 1)?,
        bindings,
        mutation,
    )
    .map_err(|e| map_mut_err("typed_commands_json_float_clear", e))?;
    compile_projection(
        "typed_commands_json_float_clear",
        1,
        ProjectionPartition::Unit,
        [ProjectionHandler::from_binding("cleared", binding)],
    )
}

fn json_float_clear_resolve(
    occurrence: &DomainEventOccurrence,
) -> Result<ResolvedProjectionPlan, ProjectionProgramError> {
    resolve_mutation_program(&json_float_clear_program()?, occurrence)
}

fn json_float_clear_lower(
    plan: &ResolvedProjectionPlan,
) -> Result<LoweredProjectionPlan, ProjectionLoweringError> {
    lower_single_model::<JsonFloatEffectView>(plan)
}

fn json_float_clear_inventory() -> Result<ProjectionOutputInventory, ProjectionLoweringError> {
    inventory_single_model::<JsonFloatEffectView>()
}

const JSON_FLOAT_CLEAR_PROJECTION: ProjectionDescriptor<EventualOnly> = descriptor_from_factories(
    "typed_commands_json_float_clear",
    1,
    "typed-commands-json-float-clear-v1",
    json_float_clear_program,
    json_float_clear_resolve,
    json_float_clear_lower,
    json_float_clear_inventory,
);

struct ForgedTitleMarker;

impl EffectInputFieldMarker for ForgedTitleMarker {
    type Input = ForgedInput;
    type Value = String;
    type NonNullValue = String;
    type Nullability = distributed::command::EffectRequired;
    type PathKind = distributed::command::EffectInputTerminalKind;
    type Wire = distributed::command::EffectWireString;
    type Nested = String;

    fn path() -> Vec<&'static str> {
        vec!["title"]
    }
}

struct ForgedCountMarker;

impl EffectModelFieldMarker for ForgedCountMarker {
    type Model = ForgedView;
    // Deliberately lies about the independently-derived SQL/GraphQL field.
    type Value = String;
    type Wire = distributed::command::EffectWireString;
    const FIELD: &'static str = "count";
}

struct ForgedDefaultMarker;

impl EffectInputFieldMarker for ForgedDefaultMarker {
    type Input = PlanInput;
    type Value = String;
    type NonNullValue = String;
    type Nullability = distributed::command::EffectRequired;
    type PathKind = distributed::command::EffectInputTerminalKind;
    type Wire = distributed::command::EffectWireString;
    type Nested = String;

    fn path() -> Vec<&'static str> {
        vec!["missing"]
    }
}

fn object_type<T: 'static>(name: &str) -> CommandTypeDef {
    CommandTypeDef::new(
        name,
        vec![CommandTypeField {
            name: "id".into(),
            type_name: "String".into(),
            nullable: false,
            list: false,
            item_nullable: false,
            nested: None,
        }],
    )
    .with_type_id(TypeId::of::<T>())
}

impl CommandInputType for InputA {
    fn command_type() -> CommandTypeDef {
        object_type::<Self>("CommandInput")
    }
}

impl CommandOutputType for OutputA {
    fn command_type() -> CommandTypeDef {
        object_type::<Self>("CommandOutput")
    }
}

impl CommandInputType for InputB {
    fn command_type() -> CommandTypeDef {
        object_type::<Self>("CommandInput")
    }
}

impl CommandOutputType for OutputB {
    fn command_type() -> CommandTypeDef {
        object_type::<Self>("CommandOutput")
    }
}

async fn handler_a(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    input: InputA,
) -> Result<PreparedCommand<Succeeded<OutputA>>, HandlerError> {
    Ok(PreparedCommand::prepare(OutputA { id: input.id }).unwrap())
}

async fn guarded_handler_a(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    input: InputA,
) -> Result<PreparedCommand<Succeeded<OutputA>>, HandlerError> {
    GRAPHQL_TYPED_HANDLER_INVOKED.store(true, Ordering::SeqCst);
    Ok(PreparedCommand::<Succeeded<OutputA>>::prepare(OutputA { id: input.id }).unwrap())
}

async fn handler_b(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    input: InputB,
) -> Result<PreparedCommand<Succeeded<OutputB>>, HandlerError> {
    Ok(PreparedCommand::<Succeeded<OutputB>>::prepare(OutputB { id: input.id }).unwrap())
}

async fn succeeded_plan_handler(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    input: PlanInput,
) -> Result<PreparedCommand<Succeeded<PlanOutput>>, HandlerError> {
    Ok(PreparedCommand::<Succeeded<PlanOutput>>::prepare(PlanOutput { id: input.id }).unwrap())
}

async fn renamed_default_handler(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    input: RenamedDefaultInput,
) -> Result<PreparedCommand<Succeeded<PlanOutput>>, HandlerError> {
    Ok(PreparedCommand::<Succeeded<PlanOutput>>::prepare(PlanOutput { id: input.id }).unwrap())
}

async fn fact_plan_handler(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    input: PlanInput,
) -> Result<PreparedCommand<Eventual<PlanOutput>>, HandlerError> {
    Ok(PreparedCommand::<Eventual<PlanOutput>>::prepare(PlanOutput { id: input.id }).unwrap())
}

async fn projected_plan_handler(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    _input: PlanInput,
) -> Result<PreparedCommand<Atomic<PlanView>>, HandlerError> {
    Err(HandlerError::Rejected(
        "atomic preparation requires CausalCommandContext::atomic".into(),
    ))
}

async fn forged_handler(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    input: ForgedInput,
) -> Result<PreparedCommand<Succeeded<PlanOutput>>, HandlerError> {
    Ok(PreparedCommand::<Succeeded<PlanOutput>>::prepare(PlanOutput { id: input.id }).unwrap())
}

async fn json_patch_handler(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    input: JsonPatchInput,
) -> Result<PreparedCommand<Succeeded<PlanOutput>>, HandlerError> {
    Ok(PreparedCommand::<Succeeded<PlanOutput>>::prepare(PlanOutput { id: input.id }).unwrap())
}

async fn bigint_key_handler(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    input: BigIntKeyInput,
) -> Result<PreparedCommand<Succeeded<PlanOutput>>, HandlerError> {
    Ok(
        PreparedCommand::<Succeeded<PlanOutput>>::prepare(PlanOutput {
            id: input.key.to_string(),
        })
        .unwrap(),
    )
}

async fn nullable_key_handler(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    input: NullableKeyInput,
) -> Result<PreparedCommand<Succeeded<PlanOutput>>, HandlerError> {
    Ok(
        PreparedCommand::<Succeeded<PlanOutput>>::prepare(PlanOutput {
            id: input.key.unwrap_or_default(),
        })
        .unwrap(),
    )
}

async fn bigint_relationship_handler(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    input: BigIntRelationshipInput,
) -> Result<PreparedCommand<Succeeded<PlanOutput>>, HandlerError> {
    Ok(
        PreparedCommand::<Succeeded<PlanOutput>>::prepare(PlanOutput {
            id: input.target_id,
        })
        .unwrap(),
    )
}

async fn composite_key_handler(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    input: CompositeKeyInput,
) -> Result<PreparedCommand<Succeeded<PlanOutput>>, HandlerError> {
    Ok(PreparedCommand::<Succeeded<PlanOutput>>::prepare(PlanOutput { id: input.id }).unwrap())
}

async fn float_effect_handler(
    _context: &CausalCommandContext<'_, FixtureAggregate>,
    input: FloatEffectInput,
) -> Result<PreparedCommand<Succeeded<PlanOutput>>, HandlerError> {
    Ok(PreparedCommand::<Succeeded<PlanOutput>>::prepare(PlanOutput { id: input.id }).unwrap())
}

fn plan_projector() -> SurfaceProjector {
    SurfaceProjector::new("project_plan")
        .facts(["plan.changed"])
        .models(["PlanView"])
        .partition_by(["id"])
}

fn modeled_projector<M, D>(descriptor: ProjectionDescriptor<D>) -> SurfaceProjector
where
    M: RelationalReadModel,
{
    let owner_name = descriptor.name();
    let schema = M::schema().clone();
    let output =
        ProjectionOutput::try_new(schema.model_name.clone(), schema.table_name.clone(), schema)
            .unwrap();
    let binding = ProjectionBinding::materialize_eventual(
        descriptor.eventual(),
        ProjectionSourceBinding::try_new("typed-commands", "domain-events", 1).unwrap(),
        ProjectionOwner::try_new(owner_name).unwrap(),
        "distributed-projection-partition",
        PROJECTION_PARTITION_CODEC_VERSION,
        vec![output],
        Vec::new(),
        Some(ProjectionPhysicalTopology::from_protocol(
            &ProjectorTopologyId::new(1, owner_name, [0x41; 32]).unwrap(),
        )),
    )
    .unwrap();
    let catalog = ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
    let active = catalog
        .activate(
            vec![ProjectionBindingActivation::new(
                binding.id(),
                binding.program_id(),
                ProjectionEpoch::new(descriptor.epoch()).unwrap(),
                ProjectionBindingState::Active,
                Some(ProjectionExecutorRoute::local("typed-commands").unwrap()),
            )],
            None,
        )
        .unwrap();
    SurfaceProjector::new(owner_name).modeled(
        SurfaceModeledProjection::try_from_descriptor(descriptor, &catalog, &active, binding.id())
            .unwrap(),
    )
}

fn direct_plan_projection() -> SurfaceDirectProjection {
    SurfaceDirectProjection::new("project_plan")
        .model::<PlanView>()
        .change_epoch("plan-direct-v1")
}

fn plan_input_defaults() -> distributed::command::CompiledInputDefaults<PlanInput> {
    command_input_defaults! {
        input: PlanInput;
        default input.id = uuid_v7();
    }
}

fn forged_input_defaults() -> distributed::command::CompiledInputDefaults<PlanInput> {
    distributed::command::__command_input_defaults::<PlanInput>([
        distributed::command::__input_default_uuid_v7::<PlanInput, ForgedDefaultMarker>(),
    ])
}

fn plan_permissions(role: &str) -> ModelPermissions<PlanView> {
    ModelPermissions::new().grant(role, read().all_columns())
}

fn service_a(service_id: &str) -> Service {
    Service::new().named(service_id).routes(
        causal_routes()
            .typed_command(typed_command::<InputA, Succeeded<OutputA>>("todo.create"))
            .handle(handler_a),
    )
}

fn service_b(service_id: &str) -> Service {
    Service::new().named(service_id).routes(
        causal_routes()
            .typed_command(typed_command::<InputB, Succeeded<OutputB>>("todo.create"))
            .handle(handler_b),
    )
}

fn guarded_service_a(service_id: &str) -> Service {
    Service::new().named(service_id).routes(
        causal_routes()
            .typed_command(typed_command::<InputA, Succeeded<OutputA>>("todo.create"))
            .guarded(
                |_| {
                    GRAPHQL_TYPED_GUARD_INVOKED.store(true, Ordering::SeqCst);
                    true
                },
                guarded_handler_a,
            ),
    )
}

fn projected_service(service_id: &str, repository: SqliteRepository) -> Service {
    Service::new().named(service_id).routes(
        Routes::new()
            .with_repo(AggregateRepository::<_, FixtureAggregate>::new(repository))
            .typed_command(typed_command::<PlanInput, Atomic<PlanView>>(
                "plan.projected",
            ))
            .handle(projected_plan_handler),
    )
}

fn pool() -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap()
}

#[tokio::test]
async fn service_id_mismatch_fails_in_both_builder_call_orders() {
    let service = service_a("todos");

    let before = GraphqlEngine::builder(pool())
        .service_id("wrong")
        .service(&service)
        .build()
        .err()
        .expect("service ID mismatch must fail");
    assert!(before
        .to_string()
        .contains("does not match executable service ID"));

    let after = GraphqlEngine::builder(pool())
        .service(&service)
        .service_id("wrong")
        .build()
        .err()
        .expect("service ID overwrite must fail");
    assert!(after
        .to_string()
        .contains("does not match bound executable service ID"));
}

#[tokio::test]
async fn attachment_checks_exact_rust_types_after_structural_parity() {
    let engine_source = service_a("todos");
    let engine = GraphqlEngine::builder(pool())
        .service(&engine_source)
        .build()
        .unwrap();
    let executable = service_b("todos");

    let error = executable
        .try_with_graphql(engine)
        .err()
        .expect("lookalike Rust types must not bind");
    assert!(error.to_string().contains("TypeId mismatch"));
}

#[tokio::test]
async fn attachment_checks_full_structure_and_service_identity() {
    let engine_source = service_a("todos");
    let engine = GraphqlEngine::builder(pool())
        .service(&engine_source)
        .build()
        .unwrap();
    let structurally_different = Service::new().named("todos").routes(
        causal_routes()
            .typed_command(
                typed_command::<InputA, Succeeded<OutputA>>("todo.create")
                    .field_name("createSomethingElse"),
            )
            .handle(handler_a),
    );
    let error = structurally_different
        .try_with_graphql(engine)
        .err()
        .expect("structural drift must not bind");
    assert!(error
        .to_string()
        .contains("structural fingerprint mismatch"));

    let engine_source = service_a("todos-a");
    let engine = GraphqlEngine::builder(pool())
        .service(&engine_source)
        .build()
        .unwrap();
    let wrong_service = service_a("todos-b");
    let error = wrong_service
        .try_with_graphql(engine)
        .err()
        .expect("service identity drift must not bind");
    assert!(error.to_string().contains("service ID mismatch"));
}

#[tokio::test]
async fn projected_command_binding_rejects_a_raw_pool_source() {
    let shared_pool = pool();
    let route_repository = SqliteRepository::new(shared_pool.clone());
    let service = projected_service("plans", route_repository);
    let engine = GraphqlEngine::builder(shared_pool)
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .model::<PlanView>(plan_permissions("anonymous"))
        .service(&service)
        .client_projection_owners([direct_plan_projection().into()])
        .build()
        .expect("a raw pool may build an engine before repository identity validation");

    let error = service
        .try_with_graphql(engine)
        .err()
        .expect("Atomic commands must reject a raw GraphQL pool");
    assert!(
        error
            .to_string()
            .contains("require a GraphQL pool derived from the same repository handle"),
        "{error}"
    );
}

#[tokio::test]
async fn projected_command_binding_rejects_an_independent_repository_over_the_same_pool() {
    let shared_pool = pool();
    let route_repository = SqliteRepository::new(shared_pool.clone());
    let graphql_repository = SqliteRepository::new(shared_pool);
    let service = projected_service("plans", route_repository);
    let engine = GraphqlEngine::builder(&graphql_repository)
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .model::<PlanView>(plan_permissions("anonymous"))
        .service(&service)
        .client_projection_owners([direct_plan_projection().into()])
        .build()
        .expect("the independently constructed repository still provides a valid pool");

    let error = service
        .try_with_graphql(engine)
        .err()
        .expect("Atomic commands must reject a different repository identity");
    assert!(
        error
            .to_string()
            .contains("repository and GraphQL query pool storage identities differ"),
        "{error}"
    );
}

#[tokio::test]
async fn projected_command_binding_accepts_a_clone_of_the_same_repository_handle() {
    let shared_pool = pool();
    let repository = SqliteRepository::new(shared_pool);
    let service = projected_service("plans", repository.clone());
    let engine = GraphqlEngine::builder(&repository)
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .model::<PlanView>(plan_permissions("anonymous"))
        .service(&service)
        .client_projection_owners([direct_plan_projection().into()])
        .build()
        .expect("the repository-derived GraphQL pool should build");

    service
        .try_with_graphql(engine)
        .expect("a repository clone must preserve the Atomic storage identity");
}

#[tokio::test]
async fn projector_topology_identity_drift_changes_service_binding_fingerprint() {
    // Modeled path: command contract fingerprint is driven by `.emits` selectors,
    // not separately authored confirmations.
    let engine_source = Service::new().named("plans").routes(
        causal_routes()
            .typed_command(
                typed_command::<PlanInput, Succeeded<PlanOutput>>("plan.create")
                    .emits(distributed::events![PlanChangedDomainEvent]),
            )
            .handle(succeeded_plan_handler),
    );
    let engine = GraphqlEngine::builder(pool())
        .model::<PlanView>(plan_permissions("anonymous"))
        .service(&engine_source)
        .client_projectors([modeled_projector::<PlanView, _>(PLAN_PROJECTION)])
        .build()
        .unwrap();

    let executable = Service::new().named("plans").routes(
        causal_routes()
            .typed_command(
                typed_command::<PlanInput, Succeeded<PlanOutput>>("plan.create")
                    .emits(distributed::events![PlanRenamedDomainEvent]),
            )
            .handle(succeeded_plan_handler),
    );
    let error = executable
        .try_with_graphql(engine)
        .err()
        .expect("different emitted event sets must change service identity");
    assert!(error
        .to_string()
        .contains("structural fingerprint mismatch"));
}

#[tokio::test]
async fn matched_typed_inventory_rejects_attachment_without_protocol_tokens() {
    let service = service_a("todos");
    let engine = GraphqlEngine::builder(pool())
        .service(&service)
        .build()
        .expect("typed inventory can be compiled before deployment protocol configuration");

    let error = service
        .try_with_graphql(engine)
        .err()
        .expect("causal mutations must not be served without opaque protocol tokens");
    assert!(
        error
            .to_string()
            .contains("require a configured GraphQL protocol token key"),
        "{error}"
    );
}

#[tokio::test]
async fn matched_typed_inventory_attaches_while_unverified_mutations_fail_closed() {
    let service = service_a("todos");
    let engine = GraphqlEngine::builder(pool())
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .service(&service)
        .build()
        .unwrap();
    let service = Arc::new(
        service
            .try_with_graphql(engine)
            .expect("validated causal inventory may attach to GraphQL"),
    );
    let engine = service.graphql_engine().unwrap();
    let query = engine
        .execute(&Session::new(), Request::new("{ __typename }"))
        .await;
    assert!(query.errors.is_empty(), "{query:?}");
    let mutation = engine
        .execute(
            &Session::new(),
            Request::new(
                "mutation { todo_create(commandId: \"0190a000-0000-7000-8000-000000000001\", input: { id: \"todo-1\" }) { id } }",
            )
            .data(Arc::new(distributed::LocalCommandHost::new(Arc::clone(&service)))
                as distributed::SharedCommandHost),
        )
        .await;
    assert_eq!(mutation.errors.len(), 1, "{mutation:?}");
    assert!(
        mutation.errors[0]
            .message
            .contains("durable commands require a verified OIDC bearer"),
        "{mutation:?}"
    );
}

#[tokio::test]
async fn every_graphql_dispatch_path_fences_before_typed_guards_and_handlers() {
    GRAPHQL_TYPED_GUARD_INVOKED.store(false, Ordering::SeqCst);
    GRAPHQL_TYPED_HANDLER_INVOKED.store(false, Ordering::SeqCst);
    let service = Arc::new(guarded_service_a("todos"));
    let engine = Arc::new(
        GraphqlEngine::builder(pool())
            .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
            .service(&service)
            .build()
            .unwrap(),
    );
    let mutation = "mutation { todo_create(commandId: \"0190a000-0000-7000-8000-000000000002\", input: { id: \"todo-1\" }) { id } }";

    let response = engine
        .execute(&Session::new(), Request::new(mutation))
        .await;
    assert_eq!(response.errors.len(), 1, "{response:?}");

    let streamed = engine
        .execute_stream(&Session::new(), Request::new(mutation))
        .next()
        .await
        .expect("mutation stream emits one fail-closed response");
    assert_eq!(streamed.errors.len(), 1, "{streamed:?}");

    let router = graphql_router_with_service(Arc::clone(&engine), Arc::clone(&service));
    let response = router
        .oneshot(
            HttpRequest::post("/graphql")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "query": mutation }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());
    assert!(!GRAPHQL_TYPED_GUARD_INVOKED.load(Ordering::SeqCst));
    assert!(!GRAPHQL_TYPED_HANDLER_INVOKED.load(Ordering::SeqCst));
}

#[tokio::test]
async fn router_construction_runs_full_binding_validation() {
    let engine_source = service_a("todos");
    let engine = Arc::new(
        GraphqlEngine::builder(pool())
            .service(&engine_source)
            .build()
            .unwrap(),
    );
    let executable = Arc::new(service_b("todos"));

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        graphql_router_with_service(engine, executable)
    }))
    .expect_err("router must not serve a mismatched typed inventory");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("unknown panic");
    assert!(
        message.contains("TypeId mismatch"),
        "unexpected panic: {message}"
    );
}

#[tokio::test]
async fn query_only_service_binding_executes_without_a_command_committer() {
    let service = Service::new().named("query-only");
    let engine = GraphqlEngine::builder(pool())
        .service(&service)
        .build()
        .unwrap();
    let response = engine
        .execute(&Session::new(), Request::new("{ __typename }"))
        .await;
    assert!(response.errors.is_empty(), "{response:?}");
    service
        .try_with_graphql(engine)
        .expect("query-only identity binding must attach without causal routes");
}

#[test]
fn pool_free_typed_export_preserves_service_provenance_and_rejects_relabeling() {
    let service = Service::new().named("plans").routes(
        causal_routes()
            .typed_command(typed_command::<PlanInput, Succeeded<PlanOutput>>(
                "plan.create",
            ))
            .handle(succeeded_plan_handler),
    );
    let catalog = build_surface(&[PlanView::schema().clone()], &SurfaceOptions::sqlite())
        .unwrap()
        .with_service(&service)
        .unwrap();
    let selected = surface_for_role(
        &catalog,
        "anonymous",
        &std::collections::BTreeMap::from([("PlanView".into(), RoleGrant::all_columns())]),
    )
    .unwrap();
    let manifest = DistributedClientSurfaceExport::from_selected("plans", selected.clone())
        .unwrap()
        .manifest()
        .unwrap();
    assert_eq!(manifest.service_id, "plans");
    assert_eq!(manifest.commands[0].name, "plan.create");

    let error = DistributedClientSurfaceExport::from_selected("other-plans", selected).unwrap_err();
    assert!(error
        .to_string()
        .contains("does not match typed Surface provenance"));
}

#[tokio::test]
async fn execute_stream_cannot_dispatch_through_an_injected_legacy_service() {
    let typed = service_a("todos");
    let engine = GraphqlEngine::builder(pool())
        .service(&typed)
        .build()
        .unwrap();
    let invoked = Arc::new(AtomicBool::new(false));
    let handler_flag = Arc::clone(&invoked);
    let raw_service = Arc::new(
        Service::new()
            .named("unrelated")
            .routes(
                Routes::new()
                    .command("todo.create")
                    .handle(move |_: &Context<()>| {
                        let handler_flag = Arc::clone(&handler_flag);
                        async move {
                            handler_flag.store(true, Ordering::SeqCst);
                            Ok(serde_json::json!({ "id": "forged" }))
                        }
                    }),
            ),
    );
    let request = Request::new(
        "mutation { todo_create(commandId: \"0190a000-0000-7000-8000-000000000003\", input: { id: \"todo-1\" }) { id } }",
    )
    .data(raw_service);
    let response = engine
        .execute_stream(&Session::new(), request)
        .next()
        .await
        .expect("fail-closed stream emits one response");
    assert_eq!(response.errors.len(), 1, "{response:?}");
    assert!(!response.errors.is_empty(), "{response:?}");
    assert!(!invoked.load(Ordering::SeqCst));
}

#[tokio::test]
async fn succeeded_command_without_preview_exports_modeled_revalidation_contract() {
    let service = Service::new().named("plans").routes(
        causal_routes()
            .typed_command(
                typed_command::<PlanInput, Succeeded<PlanOutput>>("plan.create")
                    .emits(distributed::events![PlanChangedDomainEvent]),
            )
            .handle(succeeded_plan_handler),
    );
    let engine = GraphqlEngine::builder(pool())
        .model::<PlanView>(plan_permissions("anonymous"))
        .service(&service)
        .client_projectors([modeled_projector::<PlanView, _>(PLAN_PROJECTION)])
        .build()
        .unwrap();
    let manifest = engine.client_manifest_for_role("anonymous").unwrap();
    let command = manifest
        .commands
        .iter()
        .find(|command| command.name == "plan.create")
        .unwrap();

    assert_eq!(command.extensions.consistency.kind, "succeeded");
    assert!(command.extensions.effects.is_none());
    assert!(command.extensions.confirmations.is_none());
    let projection = command.extensions.projection.as_ref().unwrap();
    assert_eq!(projection.fallback, ClientProjectionFallback::Revalidate);
    assert_eq!(projection.program_arms.len(), 1);
    // No explicit `.applies` — auto-optimism still invents one occurrence per
    // emitted event that has a matching projection arm. Unresolved slots stay
    // Unknown and fall back to revalidation (see client_manifest projections).
    assert_eq!(
        projection.preview_occurrences.len(),
        1,
        "emits + projection arms auto-derive preview occurrences when .applies is empty"
    );
    assert_eq!(
        manifest.projection_programs[0].arms[0].operations[0].kind,
        ClientProjectionMutationKind::Upsert
    );
    assert!(manifest.capabilities.causal_receipts);
    assert!(!manifest.capabilities.confirmed_persistence);
}

#[tokio::test]
async fn generated_input_default_is_reused_as_the_effect_key_and_fingerprinted() {
    let command_with_default = || {
        typed_command::<PlanInput, Succeeded<PlanOutput>>("plan.create")
            .input_defaults(plan_input_defaults())
            .emits(distributed::events![PlanChangedDomainEvent])
            .applies(distributed::state_preview! {
                PlanChangedDomainEvent => PlanView {
                    id: generated.id,
                    title: input.title,
                    ..unknown
                }
            })
    };
    let service_with_default = Service::new().named("plans").routes(
        causal_routes()
            .typed_command(command_with_default())
            .handle(succeeded_plan_handler),
    );
    let engine = GraphqlEngine::builder(pool())
        .model::<PlanView>(plan_permissions("anonymous"))
        .service(&service_with_default)
        .client_projectors([modeled_projector::<PlanView, _>(PLAN_PROJECTION)])
        .build()
        .unwrap();
    let manifest = engine.client_manifest_for_role("anonymous").unwrap();
    let command = &manifest.commands[0];
    let defaults = command.extensions.input_defaults.as_ref().unwrap();
    assert_eq!(defaults.version, 1);
    assert_eq!(defaults.defaults.len(), 1);
    assert_eq!(defaults.defaults[0]["path"], serde_json::json!(["id"]));
    assert_eq!(defaults.defaults[0]["generator"], "uuid_v7");
    assert!(command.extensions.effects.is_none());
    assert!(command.extensions.confirmations.is_none());
    let projection = command.extensions.projection.as_ref().unwrap();
    let arm = &manifest.projection_programs[0].arms[0];
    let ClientProjectionPartition::Expression {
        expression:
            ClientProjectionExpression::Slot {
                slot: partition_slot,
                value_type: ClientProjectionValueType::String,
            },
    } = &arm.partition
    else {
        panic!("plan projection must partition by its typed state id slot");
    };
    let id_key = arm.operations[0]
        .key
        .iter()
        .find(|field| field.name == "id")
        .expect("upsert must expose the PlanView identity");
    let ClientProjectionExpression::Slot {
        slot: key_slot,
        value_type: ClientProjectionValueType::String,
    } = &id_key.expression
    else {
        panic!("upsert id key must lower to a typed state slot");
    };
    for slot in [partition_slot, key_slot] {
        let generated_id = projection.preview_occurrences[0]
            .values
            .iter()
            .find(|value| value.slot == *slot)
            .expect("partition and upsert key slots must have preview provenance");
        assert!(matches!(
            &generated_id.source,
            ClientProjectionPreviewSource::GeneratedDefault { path }
                if path == &["id".to_string()]
        ));
    }
    assert_eq!(projection.program_arms.len(), 1);
    assert_eq!(projection.program_arms[0].event.name, "plan.changed");
    assert_eq!(
        manifest.projection_bindings[0].execution_class,
        ClientProjectionExecutionClass::Causal
    );

    let without_default = Service::new().named("plans").routes(
        causal_routes()
            .typed_command(
                typed_command::<PlanInput, Succeeded<PlanOutput>>("plan.create")
                    .emits(distributed::events![PlanChangedDomainEvent])
                    .applies(distributed::state_preview! {
                        PlanChangedDomainEvent => PlanView {
                            id: input.id,
                            title: input.title,
                            ..unknown
                        }
                    }),
            )
            .handle(succeeded_plan_handler),
    );
    let manifest_without_default = GraphqlEngine::builder(pool())
        .model::<PlanView>(plan_permissions("anonymous"))
        .service(&without_default)
        .client_projectors([modeled_projector::<PlanView, _>(PLAN_PROJECTION)])
        .build()
        .unwrap()
        .client_manifest_for_role("anonymous")
        .unwrap();
    assert_ne!(
        manifest.schema_fingerprint,
        manifest_without_default.schema_fingerprint
    );
    let error = without_default
        .try_with_graphql(engine)
        .err()
        .expect("input-default drift must change the service binding fingerprint");
    assert!(error
        .to_string()
        .contains("structural fingerprint mismatch"));
}

#[tokio::test]
async fn forged_input_default_marker_is_revalidated_against_wire_shape() {
    let service = Service::new().named("plans").routes(
        causal_routes()
            .typed_command(
                typed_command::<PlanInput, Succeeded<PlanOutput>>("plan.create")
                    .input_defaults(forged_input_defaults()),
            )
            .handle(succeeded_plan_handler),
    );
    let error = GraphqlEngine::builder(pool())
        .service(&service)
        .build()
        .err()
        .expect("forged default markers must not bypass Surface validation");
    assert!(error
        .to_string()
        .contains("references unknown field `missing`"));
}

#[tokio::test]
async fn generated_input_default_uses_the_canonical_renamed_wire_path() {
    let service = Service::new().named("todos").routes(
        causal_routes()
            .typed_command(
                typed_command::<RenamedDefaultInput, Succeeded<PlanOutput>>("todo.create")
                    .input_defaults(command_input_defaults! {
                        input: RenamedDefaultInput;
                        default input.id = uuid_v7();
                    }),
            )
            .handle(renamed_default_handler),
    );
    let manifest = GraphqlEngine::builder(pool())
        .service(&service)
        .build()
        .unwrap()
        .client_manifest_for_role("anonymous")
        .unwrap();
    let defaults = manifest.commands[0]
        .extensions
        .input_defaults
        .as_ref()
        .unwrap();
    assert_eq!(defaults.defaults[0]["path"], serde_json::json!(["todoId"]));
}

#[tokio::test]
async fn json_container_leaves_reach_the_manifest() {
    let json_service = Service::new().named("json").routes(
        causal_routes()
            .typed_command(
                typed_command::<JsonPatchInput, Succeeded<PlanOutput>>("json.patch")
                    .emits(distributed::events![JsonChangedDomainEvent])
                    .applies(distributed::state_preview! {
                        JsonChangedDomainEvent => JsonView {
                            id: input.id,
                            ..unknown
                        }
                    }),
            )
            .handle(json_patch_handler),
    );
    let manifest = GraphqlEngine::builder(pool())
        .model::<JsonView>(ModelPermissions::new().grant("anonymous", read().all_columns()))
        .service(&json_service)
        .client_projectors([modeled_projector::<JsonView, _>(JSON_PROJECTION)])
        .build()
        .unwrap()
        .client_manifest_for_role("anonymous")
        .unwrap();
    let command = &manifest.commands[0];
    assert!(command.extensions.effects.is_none());
    assert!(command.extensions.confirmations.is_none());
    let projection = command.extensions.projection.as_ref().unwrap();
    assert_eq!(projection.fallback, ClientProjectionFallback::Revalidate);
    let preview = &projection.preview_occurrences[0];
    let operation = &manifest.projection_programs[0].arms[0].operations[0];
    assert_eq!(operation.kind, ClientProjectionMutationKind::Patch);
    for field in ["tags", "details"] {
        let assignment = operation
            .fields
            .iter()
            .find(|candidate| candidate.name == field)
            .unwrap();
        let ClientProjectionAssignment::Set {
            expression:
                ClientProjectionExpression::Slot {
                    slot,
                    value_type: ClientProjectionValueType::Json,
                },
        } = &assignment.assignment
        else {
            panic!("{field} must compile from a typed JSON projection slot");
        };
        let source = preview
            .values
            .iter()
            .find(|value| value.slot == *slot)
            .expect("every structured JSON field slot must remain explicit");
        assert_eq!(source.source, ClientProjectionPreviewSource::Unknown);
    }
}

#[tokio::test]
async fn embedded_primary_keys_reject_keyed_effects_while_composite_keys_remain_normalized() {
    // Keyed optimistic command_effects! authoring is removed. Embedded BigInt models
    // still revalidate through modeled mutation lowering; composite keys stay normalized.
    let bigint_manifest = GraphqlEngine::builder(pool())
        .service_id("bigint-read")
        .model::<BigIntKeyView>(ModelPermissions::new().grant("anonymous", read().all_columns()))
        .build()
        .unwrap()
        .client_manifest_for_role("anonymous")
        .unwrap();
    assert_eq!(
        bigint_manifest.models[0].normalization,
        distributed::graphql::ModelNormalization::Embedded
    );

    let schema_error = NullableKeyView::schema()
        .validate()
        .expect_err("relational primary keys cannot be nullable");
    assert!(schema_error
        .to_string()
        .contains("primary-key column `key` must be non-null"));

    let composite_service = Service::new().named("composite").routes(
        causal_routes()
            .typed_command(
                typed_command::<CompositeKeyInput, Succeeded<PlanOutput>>("composite.patch")
                    .emits(distributed::events![CompositeChangedDomainEvent])
                    .applies(distributed::state_preview! {
                        CompositeChangedDomainEvent => CompositeKeyView {
                            tenant_id: input.tenant_id,
                            id: input.id,
                            title: input.title,
                        }
                    }),
            )
            .handle(composite_key_handler),
    );
    let composite_manifest = GraphqlEngine::builder(pool())
        .model::<CompositeKeyView>(ModelPermissions::new().grant("anonymous", read().all_columns()))
        .service(&composite_service)
        .client_projectors([modeled_projector::<CompositeKeyView, _>(
            COMPOSITE_PROJECTION,
        )])
        .build()
        .unwrap()
        .client_manifest_for_role("anonymous")
        .unwrap();
    let distributed::graphql::ModelNormalization::Normalized { fields, .. } =
        &composite_manifest.models[0].normalization
    else {
        panic!("ordinary non-null composite identity must remain normalized");
    };
    assert_eq!(
        fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        ["tenant_id", "id"]
    );
    let command = &composite_manifest.commands[0];
    assert!(command.extensions.effects.is_none());
    assert!(command.extensions.confirmations.is_none());
    assert!(command.extensions.projection.is_some());
    let operation = &composite_manifest.projection_programs[0].arms[0].operations[0];
    assert_eq!(operation.kind, ClientProjectionMutationKind::Patch);
    assert_eq!(
        operation
            .key
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        ["tenant_id", "id"]
    );
    assert_eq!(
        operation
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        ["title"]
    );
}

#[tokio::test]
async fn embedded_models_retain_global_revalidation_for_modeled_projections() {
    let service = Service::new().named("bigint").routes(
        causal_routes()
            .typed_command(
                typed_command::<BigIntKeyInput, Succeeded<PlanOutput>>("bigint.invalidate")
                    .emits(distributed::events![BigIntChangedDomainEvent])
                    .applies(distributed::state_preview! {
                        BigIntChangedDomainEvent => BigIntKeyView {
                            key: input.key,
                            title: input.title,
                            ..unknown
                        }
                    }),
            )
            .handle(bigint_key_handler),
    );
    let manifest = GraphqlEngine::builder(pool())
        .model::<BigIntKeyView>(
            ModelPermissions::new().grant("anonymous", read().columns(["title"])),
        )
        .service(&service)
        .client_projectors([modeled_projector::<BigIntKeyView, _>(BIGINT_PROJECTION)])
        .build()
        .unwrap()
        .client_manifest_for_role("anonymous")
        .unwrap();

    assert_eq!(
        manifest.models[0].normalization,
        distributed::graphql::ModelNormalization::Embedded
    );
    let command = &manifest.commands[0];
    assert!(command.extensions.effects.is_none());
    assert!(command.extensions.confirmations.is_none());
    assert_eq!(
        command.extensions.projection.as_ref().unwrap().fallback,
        ClientProjectionFallback::Revalidate
    );
    assert_eq!(
        manifest.projection_bindings[0].execution_class,
        ClientProjectionExecutionClass::Causal
    );
    let operation = &manifest.projection_programs[0].arms[0].operations[0];
    assert_eq!(
        operation.kind,
        ClientProjectionMutationKind::InvalidateModel
    );
    assert_eq!(operation.model, "BigIntKeyView");
    assert!(operation.key.is_empty());
    assert!(operation.fields.is_empty());
    assert!(operation.relationships.is_empty());
    assert!(matches!(
        operation.invalidations.as_slice(),
        [ClientProjectionInvalidation::Model { model }] if model == "BigIntKeyView"
    ));
    let preview = &command
        .extensions
        .projection
        .as_ref()
        .unwrap()
        .preview_occurrences;
    assert_eq!(preview.len(), 1);
    assert!(preview[0].values.is_empty());
}

#[tokio::test]
async fn succeeded_emitted_event_exports_a_modeled_causal_projection() {
    let command = typed_command::<PlanInput, Succeeded<PlanOutput>>("plan.create")
        .input_defaults(plan_input_defaults())
        .emits(distributed::events![PlanChangedDomainEvent])
        .applies(distributed::state_preview! {
            PlanChangedDomainEvent => PlanView {
                id: generated.id,
                title: input.title,
                ..unknown
            }
        });
    let service = Service::new().named("plans").routes(
        causal_routes()
            .typed_command(command)
            .handle(succeeded_plan_handler),
    );
    let engine = GraphqlEngine::builder(pool())
        .model::<PlanView>(plan_permissions("anonymous"))
        .service(&service)
        .client_projectors([modeled_projector::<PlanView, _>(PLAN_PROJECTION)])
        .build()
        .unwrap();
    let manifest = engine.client_manifest_for_role("anonymous").unwrap();
    let command = manifest
        .commands
        .iter()
        .find(|command| command.name == "plan.create")
        .unwrap();
    assert_eq!(command.extensions.consistency.kind, "succeeded");
    assert!(command.extensions.effects.is_none());
    assert!(command.extensions.confirmations.is_none());
    let projection = command.extensions.projection.as_ref().unwrap();
    assert_eq!(projection.program_arms.len(), 1);
    assert_eq!(projection.program_arms[0].event.name, "plan.changed");
    assert_eq!(
        manifest.projection_programs[0].arms[0].operations[0].kind,
        ClientProjectionMutationKind::Upsert
    );
    assert_eq!(
        manifest.projection_bindings[0].execution_class,
        ClientProjectionExecutionClass::Causal
    );
}

#[tokio::test]
async fn text_backed_enum_constant_reaches_a_valid_client_manifest() {
    let command = typed_command::<PlanInput, Succeeded<PlanOutput>>("plan.close")
        .emits(distributed::events![PlanChangedDomainEvent]);
    let service = Service::new().named("plans").routes(
        causal_routes()
            .typed_command(command)
            .handle(succeeded_plan_handler),
    );
    let engine = GraphqlEngine::builder(pool())
        .model::<PlanView>(plan_permissions("anonymous"))
        .service(&service)
        .client_projectors([modeled_projector::<PlanView, _>(PLAN_CLOSE_PROJECTION)])
        .build()
        .unwrap();
    let manifest = engine.client_manifest_for_role("anonymous").unwrap();
    let command = &manifest.commands[0];
    assert!(command.extensions.effects.is_none());
    assert!(command.extensions.confirmations.is_none());
    assert!(command.extensions.projection.is_some());
    let status = &manifest.projection_programs[0].arms[0].operations[0]
        .fields
        .iter()
        .find(|field| field.name == "status")
        .unwrap()
        .assignment;
    assert!(matches!(
        status,
        ClientProjectionAssignment::Set {
            expression: ClientProjectionExpression::Enum {
                enum_type,
                variant
            }
        } if enum_type == "PlanStatus" && variant == "Closed"
    ));
}

#[tokio::test]
async fn nonfinite_float_constant_is_rejected_but_explicit_null_is_portable() {
    // Non-finite constant rejection via command_effects! is gone with that API.
    // Explicit null remains portable through modeled mutation projection programs.
    let explicit_null = Service::new().named("floats").routes(
        causal_routes()
            .typed_command(
                typed_command::<FloatEffectInput, Succeeded<PlanOutput>>("float.clear")
                    .emits(distributed::events![FloatClearedDomainEvent]),
            )
            .handle(float_effect_handler),
    );
    let manifest = GraphqlEngine::builder(pool())
        .model::<FloatEffectView>(ModelPermissions::new().grant("anonymous", read().all_columns()))
        .service(&explicit_null)
        .client_projectors([modeled_projector::<FloatEffectView, _>(
            FLOAT_CLEAR_PROJECTION,
        )])
        .build()
        .unwrap()
        .client_manifest_for_role("anonymous")
        .unwrap();
    let command = &manifest.commands[0];
    assert!(command.extensions.effects.is_none());
    assert!(command.extensions.confirmations.is_none());
    assert!(command.extensions.projection.is_some());
    let value = &manifest.projection_programs[0].arms[0].operations[0]
        .fields
        .iter()
        .find(|field| field.name == "value")
        .unwrap()
        .assignment;
    assert!(matches!(
        value,
        ClientProjectionAssignment::Set {
            expression: ClientProjectionExpression::Constant {
                value: ClientProjectionValue::Null
            }
        }
    ));
}

#[tokio::test]
async fn json_backed_constants_reject_nonfinite_floats_but_preserve_json_null() {
    // Non-finite JSON constants via command_effects! removed with that authoring path.
    // Explicit JSON null remains portable through modeled mutation projection programs.
    let json_null = Service::new().named("json-floats").routes(
        causal_routes()
            .typed_command(
                typed_command::<FloatEffectInput, Succeeded<PlanOutput>>("json.clear")
                    .emits(distributed::events![JsonFloatClearedDomainEvent]),
            )
            .handle(float_effect_handler),
    );
    let manifest = GraphqlEngine::builder(pool())
        .model::<JsonFloatEffectView>(
            ModelPermissions::new().grant("anonymous", read().all_columns()),
        )
        .service(&json_null)
        .client_projectors([modeled_projector::<JsonFloatEffectView, _>(
            JSON_FLOAT_CLEAR_PROJECTION,
        )])
        .build()
        .unwrap()
        .client_manifest_for_role("anonymous")
        .unwrap();
    let command = &manifest.commands[0];
    assert!(command.extensions.effects.is_none());
    assert!(command.extensions.confirmations.is_none());
    assert!(command.extensions.projection.is_some());
    let value = &manifest.projection_programs[0].arms[0].operations[0]
        .fields
        .iter()
        .find(|field| field.name == "document")
        .unwrap()
        .assignment;
    assert!(matches!(
        value,
        ClientProjectionAssignment::Set {
            expression: ClientProjectionExpression::Constant {
                value: ClientProjectionValue::Null
            }
        }
    ));
}

#[tokio::test]
async fn consistency_confirmation_matrix_fails_closed() {
    // Eventual commands without `.emits` (modeled selectors) fail closed — separate
    // command_confirmations! authoring is gone.
    let missing_fact = Service::new().named("plans").routes(
        causal_routes()
            .typed_command(typed_command::<PlanInput, Eventual<PlanOutput>>(
                "plan.fact",
            ))
            .handle(fact_plan_handler),
    );
    let error = GraphqlEngine::builder(pool())
        .service(&missing_fact)
        .build()
        .err()
        .expect("fact without emits/modeled selectors must fail");
    assert!(error
        .to_string()
        .contains("must declare at least one expected projector confirmation"));
}

#[tokio::test]
async fn role_redaction_replaces_an_unsafe_modeled_projection_with_revalidation() {
    let command = typed_command::<PlanInput, Succeeded<PlanOutput>>("plan.create")
        .input_defaults(plan_input_defaults())
        .emits(distributed::events![PlanChangedDomainEvent])
        .applies(distributed::state_preview! {
            PlanChangedDomainEvent => PlanView {
                id: generated.id,
                title: input.title,
                ..unknown
            }
        });
    let service = Service::new().named("plans").routes(
        causal_routes()
            .typed_command(command)
            .handle(succeeded_plan_handler),
    );
    let engine = GraphqlEngine::builder(pool())
        .model::<PlanView>(ModelPermissions::new().grant("user", read().columns(["title"])))
        .service(&service)
        .client_projectors([modeled_projector::<PlanView, _>(PLAN_TITLE_PROJECTION)])
        .build()
        .unwrap();
    let manifest = engine.client_manifest_for_role("user").unwrap();
    let command = &manifest.commands[0];
    assert!(command.extensions.input_defaults.is_some());
    assert!(command.extensions.effects.is_none());
    assert!(command.extensions.confirmations.is_none());
    let projection = command.extensions.projection.as_ref().unwrap();
    assert_eq!(projection.fallback, ClientProjectionFallback::Revalidate);
    let operation = &manifest.projection_programs[0].arms[0].operations[0];
    assert_eq!(
        operation.kind,
        ClientProjectionMutationKind::InvalidateModel
    );
    assert_eq!(operation.model, "PlanView");
    assert!(operation.key.is_empty());
    assert!(operation.fields.is_empty());
    assert!(operation.relationships.is_empty());
    assert!(matches!(
        operation.invalidations.as_slice(),
        [ClientProjectionInvalidation::Model { model }] if model == "PlanView"
    ));
    assert_eq!(projection.preview_occurrences.len(), 1);
    assert!(projection.preview_occurrences[0].values.is_empty());
}
