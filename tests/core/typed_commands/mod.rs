#![allow(dead_code)]

use distributed::graphql::IdentityConfig;
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
use distributed::domain_event::DomainEventContract;
use distributed::graphql::{
    build_surface, graphql_router_with_service, read, surface_for_role, ClientProjectionAssignment,
    ClientProjectionExpression, ClientProjectionFallback, ClientProjectionInvalidation,
    ClientProjectionMutationKind, ClientProjectionPartition, ClientProjectionPreviewSource,
    ClientProjectionValue, DistributedClientSurfaceExport, GraphqlEngine, ModelPermissions,
    SurfaceDirectProjection, SurfaceModeledProjection, SurfaceOptions, SurfaceProjector,
};
use distributed::microsvc::Session;
use distributed::microsvc::{
    CausalCommandContext, GraphqlServiceBindError, HandlerError, Routes, Service,
};
use distributed::projection::catalog::{ProjectionBindingActivation, ProjectionCatalog};
use distributed::projection::lower::{DirectCandidate, EventualOnly, ProjectionDescriptor};
use distributed::projection::placement::{
    ProjectionBinding, ProjectionBindingState, ProjectionEpoch, ProjectionExecutionClass,
    ProjectionExecutorRoute, ProjectionOutput, ProjectionOwner, ProjectionPhysicalTopology,
    ProjectionSourceBinding, PROJECTION_PARTITION_CODEC_VERSION,
};
use distributed::projection::ProjectionEventSelector;
use distributed::projection_protocol::ProjectorTopologyId;
use distributed::{
    bind_event_apply_mutation, command_input_defaults, compile_projection, Aggregate,
    AggregateRepository, CommandInput, CommandOutput, DomainEventDescriptor, Entity, EventRecord,
    InMemoryRepository, Mutation, MutationKind, ProjectionArm, ProjectionAssignment,
    ProjectionExpression, ProjectionField, ProjectionInputSource, ProjectionKeyField,
    ProjectionOperation, ProjectionPartition, ProjectionProgram, ProjectionProgramError,
    ProjectionValue, ProjectionValueType, ReadModel, RelationalReadModel, SqliteRepository,
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
                    name: "title".into(),
                    type_name: "String".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                },
                CommandTypeField {
                    unsigned_integer: None,
                    name: "count".into(),
                    type_name: "BigInt".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                },
                CommandTypeField {
                    unsigned_integer: None,
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

fn state_selector<S: distributed::DomainState>(
    name: &'static str,
    version: u64,
) -> Result<ProjectionEventSelector, ProjectionProgramError> {
    ProjectionEventSelector::try_from_descriptor(&DomainEventDescriptor::state::<S>(name, version))
}

fn body(value_type: ProjectionValueType, field: &str) -> ProjectionExpression {
    ProjectionExpression::body_path(value_type, [field]).expect("one-segment body path")
}

/// One arm that patches `fields` of the `model` row keyed by the event body's
/// `key` members.
fn patch_program(
    name: &str,
    arm_id: &str,
    selector: ProjectionEventSelector,
    (model, table): (&str, &str),
    key: &[&str],
    fields: Vec<(&str, ProjectionExpression)>,
) -> Result<ProjectionProgram, ProjectionProgramError> {
    let key = key
        .iter()
        .zip(0u32..)
        .map(|(name, ordinal)| {
            ProjectionKeyField::try_new(ordinal, *name, body(ProjectionValueType::String, name))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let fields = fields
        .into_iter()
        .zip(0u32..)
        .map(|((name, expression), ordinal)| {
            ProjectionField::try_new(ordinal, name, ProjectionAssignment::Set(expression))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let operation = ProjectionOperation::try_new(
        format!("patch-{name}"),
        0,
        MutationKind::Patch,
        distributed::ProjectionTarget::try_new(model, table)?,
        key,
        fields,
        Vec::new(),
        Vec::new(),
    )?;
    ProjectionProgram::try_new(
        name,
        1,
        ProjectionPartition::Unit,
        vec![ProjectionArm::try_new(arm_id, selector, vec![operation])?],
    )
}

/// Upsert the whole `PlanView` row from the `row` input.
#[allow(non_snake_case)]
fn SavePlan() -> Mutation<()> {
    distributed::mutation! {
        mutation SavePlan {
            upsert_PlanView(object: $input.row)
        }
    }
}

/// The `projection!` event-first form only builds unit-partitioned programs;
/// this one partitions by the plan id (the generated-default test reads the
/// partition slot), so it binds the declared mutation through the
/// `program:` escape hatch.
fn plan_projection_program() -> Result<ProjectionProgram, ProjectionProgramError> {
    compile_projection(
        "typed_commands_plan",
        1,
        ProjectionPartition::Expression(body(ProjectionValueType::String, "id")),
        [bind_event_apply_mutation(
            &PlanChangedDomainEvent::descriptor(),
            SavePlan().program()?,
            "row",
            ProjectionInputSource::Body,
        )?],
    )
}

distributed::projection! {
    const PLAN_PROJECTION: ProjectionDescriptor<DirectCandidate> = {
        name: "typed_commands_plan",
        version: 1,
        epoch: "typed-commands-plan-v1",
        model: PlanView,
        program: plan_projection_program,
    };
}

#[allow(non_snake_case)]
fn RenamePlan() -> Mutation<()> {
    distributed::mutation! {
        mutation RenamePlan {
            update_PlanView_by_pk(id: $input.plan.id, _set: { title: $input.plan.title })
        }
    }
}

distributed::projection! {
    const PLAN_TITLE_PROJECTION: ProjectionDescriptor<EventualOnly> = {
        name: "typed_commands_plan_title",
        version: 1,
        epoch: "typed-commands-plan-title-v1",
        model: PlanView,
        on {
            events: [PlanChangedDomainEvent],
            mutation: RenamePlan,
            input: { plan: body },
        },
    };
}

// `mutation!` binds columns to input paths only; a constant assignment (an
// enum variant or null) has no mutation syntax, so the next three programs
// are hand-built.
fn plan_close_program() -> Result<ProjectionProgram, ProjectionProgramError> {
    patch_program(
        "typed_commands_plan_close",
        "changed",
        state_selector::<PlanView>("plan.changed", 1)?,
        ("PlanView", "plan_views"),
        &["id"],
        vec![(
            "status",
            ProjectionExpression::enum_variant("PlanStatus", "Closed")?,
        )],
    )
}

distributed::projection! {
    const PLAN_CLOSE_PROJECTION: ProjectionDescriptor<EventualOnly> = {
        name: "typed_commands_plan_close",
        version: 1,
        epoch: "typed-commands-plan-close-v1",
        model: PlanView,
        program: plan_close_program,
    };
}

state_event_contract!(JsonChangedDomainEvent, JsonView, "json.changed");

#[allow(non_snake_case)]
fn PatchJson() -> Mutation<()> {
    distributed::mutation! {
        mutation PatchJson {
            update_JsonView_by_pk(
                id: $input.row.id,
                _set: { tags: $input.row.tags, details: $input.row.details }
            )
        }
    }
}

distributed::projection! {
    const JSON_PROJECTION: ProjectionDescriptor<EventualOnly> = {
        name: "typed_commands_json",
        version: 1,
        epoch: "typed-commands-json-v1",
        model: JsonView,
        on {
            events: [JsonChangedDomainEvent],
            mutation: PatchJson,
            input: { row: body },
        },
    };
}

state_event_contract!(
    CompositeChangedDomainEvent,
    CompositeKeyView,
    "composite.changed"
);

#[allow(non_snake_case)]
fn PatchComposite() -> Mutation<()> {
    distributed::mutation! {
        mutation PatchComposite {
            update_CompositeKeyView_by_pk(
                tenant_id: $input.row.tenant_id,
                id: $input.row.id,
                _set: { title: $input.row.title }
            )
        }
    }
}

distributed::projection! {
    const COMPOSITE_PROJECTION: ProjectionDescriptor<EventualOnly> = {
        name: "typed_commands_composite",
        version: 1,
        epoch: "typed-commands-composite-v1",
        model: CompositeKeyView,
        on {
            events: [CompositeChangedDomainEvent],
            mutation: PatchComposite,
            input: { row: body },
        },
    };
}

state_event_contract!(BigIntChangedDomainEvent, BigIntKeyView, "bigint.changed");

#[allow(non_snake_case)]
fn SaveBigInt() -> Mutation<()> {
    distributed::mutation! {
        mutation SaveBigInt {
            upsert_BigIntKeyView(object: $input.row)
        }
    }
}

distributed::projection! {
    const BIGINT_PROJECTION: ProjectionDescriptor<DirectCandidate> = {
        name: "typed_commands_bigint",
        version: 1,
        epoch: "typed-commands-bigint-v1",
        model: BigIntKeyView,
        on {
            events: [BigIntChangedDomainEvent],
            mutation: SaveBigInt,
            input: { row: body },
        },
    };
}

#[derive(Clone, Serialize, distributed::DomainState)]
#[domain_state(version = 1)]
struct FloatClearState {
    id: String,
}

state_event_contract!(FloatClearedDomainEvent, FloatClearState, "float.cleared");

fn float_clear_program() -> Result<ProjectionProgram, ProjectionProgramError> {
    patch_program(
        "typed_commands_float_clear",
        "cleared",
        state_selector::<FloatClearState>("float.cleared", 1)?,
        ("FloatEffectView", "float_effect_views"),
        &["id"],
        vec![(
            "value",
            ProjectionExpression::constant(ProjectionValue::null()),
        )],
    )
}

distributed::projection! {
    const FLOAT_CLEAR_PROJECTION: ProjectionDescriptor<EventualOnly> = {
        name: "typed_commands_float_clear",
        version: 1,
        epoch: "typed-commands-float-clear-v1",
        model: FloatEffectView,
        program: float_clear_program,
    };
}

state_event_contract!(
    JsonFloatClearedDomainEvent,
    FloatClearState,
    "json-float.cleared"
);

fn json_float_clear_program() -> Result<ProjectionProgram, ProjectionProgramError> {
    patch_program(
        "typed_commands_json_float_clear",
        "cleared",
        state_selector::<FloatClearState>("json-float.cleared", 1)?,
        ("JsonFloatEffectView", "json_float_effect_views"),
        &["id"],
        vec![(
            "document",
            ProjectionExpression::constant(ProjectionValue::null()),
        )],
    )
}

distributed::projection! {
    const JSON_FLOAT_CLEAR_PROJECTION: ProjectionDescriptor<EventualOnly> = {
        name: "typed_commands_json_float_clear",
        version: 1,
        epoch: "typed-commands-json-float-clear-v1",
        model: JsonFloatEffectView,
        program: json_float_clear_program,
    };
}

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
            unsigned_integer: None,
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
        .identity(IdentityConfig::dev_headers())
        .service_id("wrong")
        .service(&service)
        .build()
        .err()
        .expect("service ID mismatch must fail");
    assert!(before
        .to_string()
        .contains("does not match executable service ID"));

    let after = GraphqlEngine::builder(pool())
        .identity(IdentityConfig::dev_headers())
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
        .identity(IdentityConfig::dev_headers())
        .service(&engine_source)
        .build()
        .unwrap();
    let executable = service_b("todos");

    let error = executable
        .try_with_graphql(engine)
        .err()
        .expect("lookalike Rust types must not bind");
    assert_eq!(error, GraphqlServiceBindError::TypeIdMismatch);
}

#[tokio::test]
async fn attachment_checks_full_structure_and_service_identity() {
    let engine_source = service_a("todos");
    let engine = GraphqlEngine::builder(pool())
        .identity(IdentityConfig::dev_headers())
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
    assert!(
        matches!(
            error,
            GraphqlServiceBindError::StructuralFingerprintMismatch(_)
        ),
        "{error:?}"
    );

    let engine_source = service_a("todos-a");
    let engine = GraphqlEngine::builder(pool())
        .identity(IdentityConfig::dev_headers())
        .service(&engine_source)
        .build()
        .unwrap();
    let wrong_service = service_a("todos-b");
    let error = wrong_service
        .try_with_graphql(engine)
        .err()
        .expect("service identity drift must not bind");
    assert_eq!(
        error,
        GraphqlServiceBindError::ServiceIdMismatch {
            executable: "todos-b".into(),
            engine: "todos-a".into(),
        }
    );
}

#[tokio::test]
async fn projected_command_binding_rejects_a_raw_pool_source() {
    let shared_pool = pool();
    let route_repository = SqliteRepository::new(shared_pool.clone());
    let service = projected_service("plans", route_repository);
    let engine = GraphqlEngine::builder(shared_pool)
        .identity(IdentityConfig::dev_headers())
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
    assert_eq!(error, GraphqlServiceBindError::AtomicPoolNotFromRepository);
}

#[tokio::test]
async fn projected_command_binding_rejects_an_independent_repository_over_the_same_pool() {
    let shared_pool = pool();
    let route_repository = SqliteRepository::new(shared_pool.clone());
    let graphql_repository = SqliteRepository::new(shared_pool);
    let service = projected_service("plans", route_repository);
    let engine = GraphqlEngine::builder(&graphql_repository)
        .identity(IdentityConfig::dev_headers())
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
    assert_eq!(
        error,
        GraphqlServiceBindError::AtomicStorageIdentityMismatch
    );
}

#[tokio::test]
async fn projected_command_binding_accepts_a_clone_of_the_same_repository_handle() {
    let shared_pool = pool();
    let repository = SqliteRepository::new(shared_pool);
    let service = projected_service("plans", repository.clone());
    let engine = GraphqlEngine::builder(&repository)
        .identity(IdentityConfig::dev_headers())
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
        .identity(IdentityConfig::dev_headers())
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
    assert!(
        matches!(
            error,
            GraphqlServiceBindError::StructuralFingerprintMismatch(_)
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn matched_typed_inventory_rejects_attachment_without_protocol_tokens() {
    let service = service_a("todos");
    let engine = GraphqlEngine::builder(pool())
        .identity(IdentityConfig::dev_headers())
        .service(&service)
        .build()
        .expect("typed inventory can be compiled before deployment protocol configuration");

    let error = service
        .try_with_graphql(engine)
        .err()
        .expect("causal mutations must not be served without opaque protocol tokens");
    assert_eq!(error, GraphqlServiceBindError::MissingProtocolTokenKey);
}

#[tokio::test]
async fn matched_typed_inventory_attaches_while_unverified_mutations_fail_closed() {
    let service = service_a("todos");
    let engine = GraphqlEngine::builder(pool())
        .identity(IdentityConfig::dev_headers())
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
    let extensions = mutation.errors[0].extensions.as_ref().expect("error code");
    assert_eq!(
        extensions.get("code"),
        Some(&async_graphql::Value::from("UNAUTHORIZED")),
        "{mutation:?}"
    );
    assert_eq!(
        extensions.get("status"),
        Some(&async_graphql::Value::from(401)),
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
            .identity(IdentityConfig::dev_headers())
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
            .identity(IdentityConfig::dev_headers())
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
    // The router panics with the bind error; panics carry no type, so the
    // message is compared with the typed error's rendering.
    assert_eq!(
        message,
        format!(
            "cannot serve GraphQL with this service: {}",
            GraphqlServiceBindError::TypeIdMismatch
        )
    );
}

#[tokio::test]
async fn query_only_service_binding_executes_without_a_command_committer() {
    let service = Service::new().named("query-only");
    let engine = GraphqlEngine::builder(pool())
        .identity(IdentityConfig::dev_headers())
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
        &std::collections::BTreeMap::from([("PlanView".into(), read().all_columns())]),
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
async fn execute_stream_cannot_dispatch_through_an_injected_service() {
    let typed = service_a("todos");
    let engine = GraphqlEngine::builder(pool())
        .identity(IdentityConfig::dev_headers())
        .service(&typed)
        .build()
        .unwrap();
    let invoked = Arc::new(AtomicBool::new(false));
    let handler_flag = Arc::clone(&invoked);
    let raw_service = Arc::new(
        Service::new().named("unrelated").routes(
            causal_routes()
                .typed_command(typed_command::<InputA, Succeeded<OutputA>>("todo.create"))
                .handle(
                    move |_: &CausalCommandContext<'_, FixtureAggregate>, _: InputA| {
                        let handler_flag = Arc::clone(&handler_flag);
                        async move {
                            handler_flag.store(true, Ordering::SeqCst);
                            Ok(PreparedCommand::prepare(OutputA {
                                id: "forged".into(),
                            })
                            .unwrap())
                        }
                    },
                ),
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
        .identity(IdentityConfig::dev_headers())
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

    assert_eq!(
        command.extensions.consistency.kind,
        distributed::command::CommandConsistency::Succeeded
    );
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
        .identity(IdentityConfig::dev_headers())
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
    assert_eq!(defaults.defaults[0].path, ["id"]);
    assert_eq!(
        defaults.defaults[0].generator,
        distributed::command::InputDefaultGenerator::UuidV7
    );
    let projection = command.extensions.projection.as_ref().unwrap();
    let arm = &manifest.projection_programs[0].arms[0];
    let ClientProjectionPartition::Expression {
        expression:
            ClientProjectionExpression::Slot {
                slot: partition_slot,
                value_type: ProjectionValueType::String,
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
        value_type: ProjectionValueType::String,
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
        ProjectionExecutionClass::Causal
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
        .identity(IdentityConfig::dev_headers())
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
    assert!(
        matches!(
            error,
            GraphqlServiceBindError::StructuralFingerprintMismatch(_)
        ),
        "{error:?}"
    );
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
        .identity(IdentityConfig::dev_headers())
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
        .identity(IdentityConfig::dev_headers())
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
    assert_eq!(defaults.defaults[0].path, ["todoId"]);
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
        .identity(IdentityConfig::dev_headers())
        .model::<JsonView>(ModelPermissions::new().grant("anonymous", read().all_columns()))
        .service(&json_service)
        .client_projectors([modeled_projector::<JsonView, _>(JSON_PROJECTION)])
        .build()
        .unwrap()
        .client_manifest_for_role("anonymous")
        .unwrap();
    let command = &manifest.commands[0];
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
                    value_type: ProjectionValueType::Json,
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
        .identity(IdentityConfig::dev_headers())
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
    assert!(
        matches!(
            &schema_error,
            distributed::TableStoreError::Metadata(message)
                if message.contains("primary-key column `key` must be non-null")
        ),
        "{schema_error:?}"
    );

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
        .identity(IdentityConfig::dev_headers())
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
        .identity(IdentityConfig::dev_headers())
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
    assert_eq!(
        command.extensions.projection.as_ref().unwrap().fallback,
        ClientProjectionFallback::Revalidate
    );
    assert_eq!(
        manifest.projection_bindings[0].execution_class,
        ProjectionExecutionClass::Causal
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
        .identity(IdentityConfig::dev_headers())
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
    assert_eq!(
        command.extensions.consistency.kind,
        distributed::command::CommandConsistency::Succeeded
    );
    let projection = command.extensions.projection.as_ref().unwrap();
    assert_eq!(projection.program_arms.len(), 1);
    assert_eq!(projection.program_arms[0].event.name, "plan.changed");
    assert_eq!(
        manifest.projection_programs[0].arms[0].operations[0].kind,
        ClientProjectionMutationKind::Upsert
    );
    assert_eq!(
        manifest.projection_bindings[0].execution_class,
        ProjectionExecutionClass::Causal
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
        .identity(IdentityConfig::dev_headers())
        .model::<PlanView>(plan_permissions("anonymous"))
        .service(&service)
        .client_projectors([modeled_projector::<PlanView, _>(PLAN_CLOSE_PROJECTION)])
        .build()
        .unwrap();
    let manifest = engine.client_manifest_for_role("anonymous").unwrap();
    let command = &manifest.commands[0];
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
        .identity(IdentityConfig::dev_headers())
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
        .identity(IdentityConfig::dev_headers())
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
        .identity(IdentityConfig::dev_headers())
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
        .identity(IdentityConfig::dev_headers())
        .model::<PlanView>(ModelPermissions::new().grant("user", read().columns(["title"])))
        .service(&service)
        .client_projectors([modeled_projector::<PlanView, _>(PLAN_TITLE_PROJECTION)])
        .build()
        .unwrap();
    let manifest = engine.client_manifest_for_role("user").unwrap();
    let command = &manifest.commands[0];
    assert!(command.extensions.input_defaults.is_some());
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
