use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::time::{Duration, UNIX_EPOCH};

use serde_json::{json, Value};

use crate::domain_event::{
    DomainEventBodyDescriptor, DomainEventBodyKind, DomainEventDescriptor, DomainEventEnvelope,
    DomainEventOccurrence, DOMAIN_EVENT_BODY_CODEC, DOMAIN_EVENT_BODY_CODEC_VERSION,
};

use super::*;

const STATE_FP: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const PATCH_FP: &str = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
const DELETE_FP: &str = "sha256:3333333333333333333333333333333333333333333333333333333333333333";
const RELATED_FP: &str = "sha256:4444444444444444444444444444444444444444444444444444444444444444";

struct GoldenEvents;

impl ProjectionEventSet for GoldenEvents {
    fn projection_event_selectors() -> Result<Vec<ProjectionEventSelector>, ProjectionProgramError>
    {
        [
            descriptor(
                "todo.state-published",
                DomainEventBodyKind::State,
                "TodoState",
                STATE_FP,
            ),
            descriptor(
                "todo.renamed",
                DomainEventBodyKind::Event,
                "TodoRenamed",
                PATCH_FP,
            ),
            descriptor(
                "todo.purged",
                DomainEventBodyKind::Deletion,
                "TodoDeleted",
                DELETE_FP,
            ),
            descriptor(
                "todo.reassigned",
                DomainEventBodyKind::State,
                "TodoState",
                RELATED_FP,
            ),
        ]
        .iter()
        .map(ProjectionEventSelector::try_from_descriptor)
        .collect()
    }
}

struct PatchEvents;

impl ProjectionEventSet for PatchEvents {
    fn projection_event_selectors() -> Result<Vec<ProjectionEventSelector>, ProjectionProgramError>
    {
        Ok(vec![ProjectionEventSelector::try_from_descriptor(
            &descriptor(
                "todo.renamed",
                DomainEventBodyKind::Event,
                "TodoRenamed",
                PATCH_FP,
            ),
        )?])
    }
}

struct DeleteEvents;

impl ProjectionEventSet for DeleteEvents {
    fn projection_event_selectors() -> Result<Vec<ProjectionEventSelector>, ProjectionProgramError>
    {
        Ok(vec![ProjectionEventSelector::try_from_descriptor(
            &descriptor(
                "todo.purged",
                DomainEventBodyKind::Deletion,
                "TodoDeleted",
                DELETE_FP,
            ),
        )?])
    }
}

fn descriptor(
    event_name: &'static str,
    kind: DomainEventBodyKind,
    body_name: &'static str,
    body_fingerprint: &'static str,
) -> DomainEventDescriptor {
    DomainEventDescriptor {
        name: Cow::Borrowed(event_name),
        version: 1,
        body: DomainEventBodyDescriptor {
            kind,
            type_name: Cow::Borrowed(body_name),
            version: 1,
            schema: Cow::Borrowed("urn:distributed:test:projection-body:v1"),
            fingerprint: Cow::Borrowed(body_fingerprint),
            codec: Cow::Borrowed(DOMAIN_EVENT_BODY_CODEC),
            codec_version: DOMAIN_EVENT_BODY_CODEC_VERSION,
        },
    }
}

fn occurrence(
    descriptor: DomainEventDescriptor,
    body: &Value,
) -> Result<DomainEventOccurrence, Box<dyn Error>> {
    Ok(DomainEventOccurrence::capture(
        descriptor,
        DomainEventEnvelope {
            aggregate_type: "todo".to_owned(),
            aggregate_id: "todo-001".to_owned(),
            aggregate_sequence: 7,
            publication_ordinal: 2,
            occurred_at: UNIX_EPOCH + Duration::from_millis(1_725_000_000_000),
            metadata: BTreeMap::from([
                (
                    "correlation-id".to_owned(),
                    "ignored-correlation".to_owned(),
                ),
                ("traceparent".to_owned(), "ignored-trace".to_owned()),
            ]),
        },
        body,
    )?)
}

fn body_path(path: &[&str]) -> Result<ProjectionExpression, ProjectionProgramError> {
    let value_type = match path.last().copied() {
        Some("owner_count" | "incarnation" | "position") => ProjectionValueType::U64,
        Some("priority") => ProjectionValueType::I64,
        Some("null_value" | "missing_value") => ProjectionValueType::Json,
        _ => ProjectionValueType::String,
    };
    ProjectionExpression::body_path(value_type, path.iter().copied())
}

fn key(
    ordinal: u32,
    name: &str,
    expression: ProjectionExpression,
) -> Result<ProjectionKeyField, ProjectionProgramError> {
    ProjectionKeyField::try_new(ordinal, name, expression)
}

fn field(
    ordinal: u32,
    name: &str,
    expression: ProjectionExpression,
) -> Result<ProjectionField, ProjectionProgramError> {
    ProjectionField::try_new(ordinal, name, ProjectionAssignment::Set(expression))
}

fn delete_operation(
    operation_id: &str,
    staging_ordinal: u32,
    key_value: ProjectionValue,
) -> Result<ProjectionOperation, ProjectionProgramError> {
    ProjectionOperation::try_new(
        operation_id,
        staging_ordinal,
        MutationKind::Delete,
        ProjectionTarget::try_new("Todos", "todos")?,
        vec![key(
            0,
            "todo_id",
            ProjectionExpression::constant(key_value),
        )?],
        vec![],
        vec![],
        vec![],
    )
}

fn golden_program() -> Result<ProjectionProgram, ProjectionProgramError> {
    let todos = ProjectionTarget::try_new("Todos", "todos")?;
    let owner_counts = ProjectionTarget::try_new("OwnerTodoCounts", "owner_todo_counts")?;
    let state = descriptor(
        "todo.state-published",
        DomainEventBodyKind::State,
        "TodoState",
        STATE_FP,
    );
    let patch_descriptor = descriptor(
        "todo.renamed",
        DomainEventBodyKind::Event,
        "TodoRenamed",
        PATCH_FP,
    );
    let deletion = descriptor(
        "todo.purged",
        DomainEventBodyKind::Deletion,
        "TodoDeleted",
        DELETE_FP,
    );
    let related_descriptor = descriptor(
        "todo.reassigned",
        DomainEventBodyKind::State,
        "TodoState",
        RELATED_FP,
    );

    let upsert = ProjectionOperation::try_new(
        "upsert-todo",
        0,
        MutationKind::Upsert,
        todos.clone(),
        vec![key(0, "todo_id", body_path(&["todo_id"])?)?],
        vec![
            field(3, "status", body_path(&["status"])?)?,
            field(0, "owner_id", body_path(&["owner_id"])?)?,
            field(
                2,
                "label",
                ProjectionExpression::constant(ProjectionValue::string("Résumé 🚀")),
            )?,
            field(1, "title", body_path(&["title"])?)?,
            field(4, "priority", body_path(&["priority"])?)?,
        ],
        vec![],
        vec![ProjectionInvalidation::model("TodoSearch")?],
    )?;
    let count = ProjectionOperation::try_new(
        "upsert-owner-count",
        1,
        MutationKind::UpsertRelated,
        owner_counts,
        vec![key(0, "owner_id", body_path(&["owner_id"])?)?],
        vec![field(0, "count", body_path(&["owner_count"])?)?],
        vec![ProjectionRelationshipEffect::invalidate(
            0,
            ProjectionRelationship::try_new("Owners", "todos", "Todos")?,
            vec![key(0, "owner_id", body_path(&["owner_id"])?)?],
        )?],
        vec![ProjectionInvalidation::relationship(
            "Owners", "todos", "Todos",
        )?],
    )?;
    let patch = ProjectionOperation::try_new(
        "patch-title",
        0,
        MutationKind::Patch,
        todos.clone(),
        vec![key(0, "todo_id", body_path(&["todo_id"])?)?],
        vec![
            field(0, "title", body_path(&["title"])?)?,
            ProjectionField::try_new(1, "legacy_title", ProjectionAssignment::Unset)?,
        ],
        vec![],
        vec![],
    )?;
    let delete = ProjectionOperation::try_new(
        "delete-todo",
        0,
        MutationKind::Delete,
        todos.clone(),
        vec![
            key(1, "incarnation", body_path(&["incarnation"])?)?,
            key(0, "todo_id", body_path(&["key"])?)?,
        ],
        vec![],
        vec![ProjectionRelationshipEffect::unlink(
            0,
            ProjectionRelationship::try_new("Owners", "todos", "Todos")?,
            vec![key(0, "owner_id", body_path(&["owner_id"])?)?],
            vec![key(0, "todo_id", body_path(&["key"])?)?],
        )?],
        vec![],
    )?;
    let related = ProjectionOperation::try_new(
        "insert-owner-todo",
        0,
        MutationKind::InsertRelated,
        ProjectionTarget::try_new("OwnerTodos", "owner_todos")?,
        vec![
            key(1, "todo_id", body_path(&["todo_id"])?)?,
            key(0, "owner_id", body_path(&["owner_id"])?)?,
        ],
        vec![
            field(
                0,
                "position",
                ProjectionExpression::constant(ProjectionValue::unsigned(u64::MAX)),
            )?,
            field(
                1,
                "signed_min",
                ProjectionExpression::constant(ProjectionValue::signed(i64::MIN)),
            )?,
        ],
        vec![ProjectionRelationshipEffect::link(
            0,
            ProjectionRelationship::try_new("Owners", "todos", "Todos")?,
            vec![key(0, "owner_id", body_path(&["owner_id"])?)?],
            vec![key(0, "todo_id", body_path(&["todo_id"])?)?],
        )?],
        vec![ProjectionInvalidation::model("TodoSearch")?],
    )?;

    ProjectionProgram::try_new(
        "todos",
        1,
        ProjectionPartition::Expression(ProjectionExpression::envelope(
            ProjectionEnvelopeField::AggregateId,
        )),
        vec![
            ProjectionArm::try_new(
                "reassigned",
                ProjectionEventSelector::try_from_descriptor(&related_descriptor)?,
                vec![related],
            )?,
            ProjectionArm::try_new(
                "purged",
                ProjectionEventSelector::try_from_descriptor(&deletion)?,
                vec![delete],
            )?,
            ProjectionArm::try_new(
                "state",
                ProjectionEventSelector::try_from_descriptor(&state)?,
                vec![count, upsert],
            )?,
            ProjectionArm::try_new(
                "renamed",
                ProjectionEventSelector::try_from_descriptor(&patch_descriptor)?,
                vec![patch],
            )?,
        ],
    )
}

fn simple_program(
    selector: ProjectionEventSelector,
) -> Result<ProjectionProgram, ProjectionProgramError> {
    ProjectionProgram::try_new(
        "identity-test",
        1,
        ProjectionPartition::Unit,
        vec![ProjectionArm::try_new(
            "event",
            selector,
            vec![delete_operation(
                "delete",
                0,
                ProjectionValue::string("one"),
            )?],
        )?],
    )
}

#[derive(Clone)]
struct IdentityProgramCase {
    name: &'static str,
    version: u64,
    partition: ProjectionPartition,
    arm_id: &'static str,
    operation_id: &'static str,
    primary_staging_ordinal: u32,
    kind: MutationKind,
    model: &'static str,
    storage: &'static str,
    key_value: &'static str,
    field_name: &'static str,
    field_value: &'static str,
    relationship_effect: bool,
    invalidation: bool,
}

impl IdentityProgramCase {
    fn baseline() -> Self {
        Self {
            name: "semantic-identity",
            version: 1,
            partition: ProjectionPartition::Unit,
            arm_id: "event",
            operation_id: "primary",
            primary_staging_ordinal: 0,
            kind: MutationKind::Patch,
            model: "Todos",
            storage: "todos",
            key_value: "one",
            field_name: "title",
            field_value: "alpha",
            relationship_effect: false,
            invalidation: false,
        }
    }
}

fn semantic_identity_program(
    case: &IdentityProgramCase,
) -> Result<ProjectionProgram, ProjectionProgramError> {
    let primary_relationship_effects = if case.relationship_effect {
        vec![ProjectionRelationshipEffect::link(
            0,
            ProjectionRelationship::try_new("Owners", "todos", "Todos")?,
            vec![key(
                0,
                "owner_id",
                ProjectionExpression::constant(ProjectionValue::string("owner-one")),
            )?],
            vec![key(
                0,
                "todo_id",
                ProjectionExpression::constant(ProjectionValue::string(case.key_value)),
            )?],
        )?]
    } else {
        vec![]
    };
    let primary_invalidations = if case.invalidation {
        vec![ProjectionInvalidation::model("TodoSearch")?]
    } else {
        vec![]
    };
    let primary = ProjectionOperation::try_new(
        case.operation_id,
        case.primary_staging_ordinal,
        case.kind,
        ProjectionTarget::try_new(case.model, case.storage)?,
        vec![key(
            0,
            "todo_id",
            ProjectionExpression::constant(ProjectionValue::string(case.key_value)),
        )?],
        vec![field(
            0,
            case.field_name,
            ProjectionExpression::constant(ProjectionValue::string(case.field_value)),
        )?],
        primary_relationship_effects,
        primary_invalidations,
    )?;
    let secondary = ProjectionOperation::try_new(
        "secondary",
        1 - case.primary_staging_ordinal,
        MutationKind::Patch,
        ProjectionTarget::try_new("Todos", "todos")?,
        vec![key(
            0,
            "todo_id",
            ProjectionExpression::constant(ProjectionValue::string("sentinel")),
        )?],
        vec![field(
            0,
            "title",
            ProjectionExpression::constant(ProjectionValue::string("sentinel")),
        )?],
        vec![],
        vec![],
    )?;
    let selector = ProjectionEventSelector::try_from_descriptor(&descriptor(
        "todo.renamed",
        DomainEventBodyKind::Event,
        "TodoRenamed",
        PATCH_FP,
    ))?;
    ProjectionProgram::try_new(
        case.name,
        case.version,
        case.partition.clone(),
        vec![ProjectionArm::try_new(
            case.arm_id,
            selector,
            vec![primary, secondary],
        )?],
    )
}

#[test]
fn canonical_program_matches_frozen_vector() -> Result<(), Box<dyn Error>> {
    let bytes = golden_program()?.canonical_bytes()?;
    let fixture = include_bytes!("../../tests/fixtures/projection-program-v1.json");
    let canonical_fixture = fixture
        .strip_suffix(b"\n")
        .ok_or("the frozen projection fixture must end with one newline")?;
    if canonical_fixture.ends_with(b"\n") {
        return Err("the frozen projection fixture must end with exactly one newline".into());
    }
    assert_eq!(bytes.as_slice(), canonical_fixture);
    Ok(())
}

#[test]
fn program_identity_binds_every_event_wire_contract_field() -> Result<(), Box<dyn Error>> {
    let baseline = ProjectionEventSelector::try_new(
        1,
        "todo.changed",
        1,
        DomainEventBodyKind::State,
        "TodoState",
        1,
        "urn:test:todo:v1",
        STATE_FP,
        DOMAIN_EVENT_BODY_CODEC,
        DOMAIN_EVENT_BODY_CODEC_VERSION,
    )?;
    let baseline_id = simple_program(baseline.clone())?.id();
    let variants = [
        ProjectionEventSelector::try_new(
            2,
            "todo.changed",
            1,
            DomainEventBodyKind::State,
            "TodoState",
            1,
            "urn:test:todo:v1",
            STATE_FP,
            DOMAIN_EVENT_BODY_CODEC,
            DOMAIN_EVENT_BODY_CODEC_VERSION,
        )?,
        ProjectionEventSelector::try_new(
            1,
            "todo.renamed",
            1,
            DomainEventBodyKind::State,
            "TodoState",
            1,
            "urn:test:todo:v1",
            STATE_FP,
            DOMAIN_EVENT_BODY_CODEC,
            DOMAIN_EVENT_BODY_CODEC_VERSION,
        )?,
        ProjectionEventSelector::try_new(
            1,
            "todo.changed",
            2,
            DomainEventBodyKind::State,
            "TodoState",
            1,
            "urn:test:todo:v1",
            STATE_FP,
            DOMAIN_EVENT_BODY_CODEC,
            DOMAIN_EVENT_BODY_CODEC_VERSION,
        )?,
        ProjectionEventSelector::try_new(
            1,
            "todo.changed",
            1,
            DomainEventBodyKind::Event,
            "TodoState",
            1,
            "urn:test:todo:v1",
            STATE_FP,
            DOMAIN_EVENT_BODY_CODEC,
            DOMAIN_EVENT_BODY_CODEC_VERSION,
        )?,
        ProjectionEventSelector::try_new(
            1,
            "todo.changed",
            1,
            DomainEventBodyKind::State,
            "PublicTodo",
            1,
            "urn:test:todo:v1",
            STATE_FP,
            DOMAIN_EVENT_BODY_CODEC,
            DOMAIN_EVENT_BODY_CODEC_VERSION,
        )?,
        ProjectionEventSelector::try_new(
            1,
            "todo.changed",
            1,
            DomainEventBodyKind::State,
            "TodoState",
            2,
            "urn:test:todo:v1",
            STATE_FP,
            DOMAIN_EVENT_BODY_CODEC,
            DOMAIN_EVENT_BODY_CODEC_VERSION,
        )?,
        ProjectionEventSelector::try_new(
            1,
            "todo.changed",
            1,
            DomainEventBodyKind::State,
            "TodoState",
            1,
            "urn:test:todo:v2",
            STATE_FP,
            DOMAIN_EVENT_BODY_CODEC,
            DOMAIN_EVENT_BODY_CODEC_VERSION,
        )?,
        ProjectionEventSelector::try_new(
            1,
            "todo.changed",
            1,
            DomainEventBodyKind::State,
            "TodoState",
            1,
            "urn:test:todo:v1",
            PATCH_FP,
            DOMAIN_EVENT_BODY_CODEC,
            DOMAIN_EVENT_BODY_CODEC_VERSION,
        )?,
        ProjectionEventSelector::try_new(
            1,
            "todo.changed",
            1,
            DomainEventBodyKind::State,
            "TodoState",
            1,
            "urn:test:todo:v1",
            STATE_FP,
            "application/example+json",
            DOMAIN_EVENT_BODY_CODEC_VERSION,
        )?,
        ProjectionEventSelector::try_new(
            1,
            "todo.changed",
            1,
            DomainEventBodyKind::State,
            "TodoState",
            1,
            "urn:test:todo:v1",
            STATE_FP,
            DOMAIN_EVENT_BODY_CODEC,
            2,
        )?,
    ];
    for selector in variants {
        assert_ne!(simple_program(selector)?.id(), baseline_id);
    }
    let signed_program = ProjectionProgram::try_new(
        "typed-number",
        1,
        ProjectionPartition::Unit,
        vec![ProjectionArm::try_new(
            "event",
            baseline.clone(),
            vec![delete_operation("delete", 0, ProjectionValue::signed(1))?],
        )?],
    )?;
    let unsigned_program = ProjectionProgram::try_new(
        "typed-number",
        1,
        ProjectionPartition::Unit,
        vec![ProjectionArm::try_new(
            "event",
            baseline,
            vec![delete_operation("delete", 0, ProjectionValue::unsigned(1))?],
        )?],
    )?;
    assert_ne!(signed_program.id(), unsigned_program.id());
    assert_eq!(
        ProjectionProgramId::parse(&baseline_id.to_string())?,
        baseline_id
    );
    Ok(())
}

#[test]
fn program_identity_binds_every_program_and_operation_semantic() -> Result<(), Box<dyn Error>> {
    let baseline = IdentityProgramCase::baseline();
    let baseline_id = semantic_identity_program(&baseline)?.id();
    let mut variants = Vec::new();

    let mut variant = baseline.clone();
    variant.name = "semantic-identity-renamed";
    variants.push(("program name", variant));
    let mut variant = baseline.clone();
    variant.version = 2;
    variants.push(("program version", variant));
    let mut variant = baseline.clone();
    variant.partition = ProjectionPartition::Expression(ProjectionExpression::constant(
        ProjectionValue::string("partition"),
    ));
    variants.push(("partition", variant));
    let mut variant = baseline.clone();
    variant.arm_id = "renamed-arm";
    variants.push(("arm ID", variant));
    let mut variant = baseline.clone();
    variant.operation_id = "renamed-operation";
    variants.push(("operation ID", variant));
    let mut variant = baseline.clone();
    variant.primary_staging_ordinal = 1;
    variants.push(("staging ordinal", variant));
    let mut variant = baseline.clone();
    variant.kind = MutationKind::UpsertPatch;
    variants.push(("operation kind", variant));
    let mut variant = baseline.clone();
    variant.model = "TodoCards";
    variants.push(("target model", variant));
    let mut variant = baseline.clone();
    variant.storage = "todo_cards";
    variants.push(("target storage", variant));
    let mut variant = baseline.clone();
    variant.key_value = "two";
    variants.push(("key", variant));
    let mut variant = baseline.clone();
    variant.field_name = "summary";
    variants.push(("field name", variant));
    let mut variant = baseline.clone();
    variant.field_value = "beta";
    variants.push(("field assignment", variant));
    let mut variant = baseline.clone();
    variant.relationship_effect = true;
    variants.push(("relationship effect", variant));
    let mut variant = baseline;
    variant.invalidation = true;
    variants.push(("invalidation", variant));

    for (semantic, variant) in variants {
        assert_ne!(
            semantic_identity_program(&variant)?.id(),
            baseline_id,
            "{semantic} was not bound into the program identity"
        );
    }
    Ok(())
}

#[test]
fn source_collection_order_does_not_change_canonical_program() -> Result<(), Box<dyn Error>> {
    let baseline = golden_program()?;
    let mut arms = baseline.arms().to_vec();
    arms.reverse();
    let reordered = ProjectionProgram::try_new(
        baseline.name(),
        baseline.version(),
        baseline.partition().clone(),
        arms,
    )?;
    assert_eq!(baseline.canonical_bytes()?, reordered.canonical_bytes()?);
    assert_eq!(baseline.id(), reordered.id());
    Ok(())
}

#[test]
fn resolution_preserves_state_upsert_scope_and_provenance() -> Result<(), Box<dyn Error>> {
    let descriptor = descriptor(
        "todo.state-published",
        DomainEventBodyKind::State,
        "TodoState",
        STATE_FP,
    );
    let occurrence = occurrence(
        descriptor,
        &json!({
            "todo_id": "todo-001",
            "owner_id": "owner-α",
            "owner_count": 9,
            "title": "Résumé 🚀",
            "status": "completed",
            "priority": 1
        }),
    )?;
    let plan =
        ProjectionPlanTemplate::<GoldenEvents>::try_new(golden_program()?)?.resolve(&occurrence)?;
    assert_eq!(plan.mutations().len(), 2);
    let todo = &plan.mutations()[0];
    let count = &plan.mutations()[1];
    assert_eq!(count.target().model(), "OwnerTodoCounts");
    let invalidation = &count.provenance().relationship_effects()[0];
    assert_eq!(
        invalidation.kind(),
        ProjectionRelationshipEffectKind::Invalidate
    );
    assert!(invalidation.source_key().is_some());
    assert!(invalidation.target_key().is_none());
    assert_eq!(todo.kind(), MutationKind::Upsert);
    assert_eq!(todo.scope().model(), "Todos");
    assert_eq!(todo.scope().storage(), "todos");
    assert_eq!(todo.scope().partition(), plan.partition());
    assert_eq!(
        todo.provenance().occurrence().occurrence_id(),
        occurrence.id()
    );
    assert_eq!(todo.provenance().arm_id(), "state");
    assert_eq!(todo.provenance().staging_ordinals(), &[0]);
    assert_eq!(todo.key().fields()[0].name(), "todo_id");
    Ok(())
}

#[test]
fn patch_keeps_null_absent_and_unset_distinct() -> Result<(), Box<dyn Error>> {
    let descriptor = descriptor(
        "todo.renamed",
        DomainEventBodyKind::Event,
        "TodoRenamed",
        PATCH_FP,
    );
    let target = ProjectionTarget::try_new("Todos", "todos")?;
    let operation = ProjectionOperation::try_new(
        "patch",
        0,
        MutationKind::Patch,
        target,
        vec![key(0, "todo_id", body_path(&["todo_id"])?)?],
        vec![
            field(0, "null_value", body_path(&["null_value"])?)?,
            field(1, "missing_value", body_path(&["missing_value"])?)?,
            ProjectionField::try_new(2, "removed", ProjectionAssignment::Unset)?,
        ],
        vec![],
        vec![],
    )?;
    let program = ProjectionProgram::try_new(
        "presence",
        1,
        ProjectionPartition::Unit,
        vec![ProjectionArm::try_new(
            "patch",
            ProjectionEventSelector::try_from_descriptor(&descriptor)?,
            vec![operation],
        )?],
    )?;
    let occurrence = occurrence(
        descriptor,
        &json!({"todo_id": "todo-001", "null_value": null}),
    )?;
    let plan = ProjectionPlanTemplate::<PatchEvents>::try_new(program)?.resolve(&occurrence)?;
    let fields = plan.mutations()[0].fields();
    assert_eq!(
        fields[0].value(),
        &ResolvedProjectionValue::Value(ProjectionValue::null())
    );
    assert_eq!(fields[1].value(), &ResolvedProjectionValue::Absent);
    assert_eq!(fields[2].value(), &ResolvedProjectionValue::Unset);
    Ok(())
}

#[test]
fn deletion_and_related_invalidation_are_modeled_without_link_ops() -> Result<(), Box<dyn Error>> {
    let delete_descriptor = descriptor(
        "todo.purged",
        DomainEventBodyKind::Deletion,
        "TodoDeleted",
        DELETE_FP,
    );
    let deletion = occurrence(
        delete_descriptor,
        &json!({"key": "todo-001", "incarnation": 3, "owner_id": "owner-001"}),
    )?;
    let delete_plan =
        ProjectionPlanTemplate::<GoldenEvents>::try_new(golden_program()?)?.resolve(&deletion)?;
    assert_eq!(delete_plan.mutations()[0].kind(), MutationKind::Delete);
    assert_eq!(delete_plan.mutations()[0].key().fields().len(), 2);
    let unlink = &delete_plan.mutations()[0]
        .provenance()
        .relationship_effects()[0];
    assert_eq!(unlink.kind(), ProjectionRelationshipEffectKind::Unlink);
    assert!(unlink.source_key().is_some());
    assert!(unlink.target_key().is_some());

    let related_descriptor = descriptor(
        "todo.reassigned",
        DomainEventBodyKind::State,
        "TodoState",
        RELATED_FP,
    );
    let related = occurrence(
        related_descriptor,
        &json!({"todo_id": "todo-001", "owner_id": "owner-002"}),
    )?;
    let related_plan =
        ProjectionPlanTemplate::<GoldenEvents>::try_new(golden_program()?)?.resolve(&related)?;
    let mutation = &related_plan.mutations()[0];
    assert_eq!(mutation.kind(), MutationKind::InsertRelated);
    assert_eq!(
        mutation.provenance().relationship_effects()[0]
            .relationship()
            .relationship(),
        "todos"
    );
    assert_eq!(mutation.provenance().invalidations().len(), 1);
    Ok(())
}

#[test]
fn reassignment_patch_retains_old_unlink_and_new_link_keys() -> Result<(), Box<dyn Error>> {
    let event = descriptor(
        "todo.renamed",
        DomainEventBodyKind::Event,
        "TodoRenamed",
        PATCH_FP,
    );
    let relationship = ProjectionRelationship::try_new("Owners", "todos", "Todos")?;
    let operation = ProjectionOperation::try_new(
        "reassign",
        0,
        MutationKind::Patch,
        ProjectionTarget::try_new("Todos", "todos")?,
        vec![key(0, "todo_id", body_path(&["todo_id"])?)?],
        vec![field(0, "owner_id", body_path(&["owner_id"])?)?],
        vec![
            ProjectionRelationshipEffect::link(
                1,
                relationship.clone(),
                vec![key(0, "owner_id", body_path(&["owner_id"])?)?],
                vec![key(0, "todo_id", body_path(&["todo_id"])?)?],
            )?,
            ProjectionRelationshipEffect::unlink(
                0,
                relationship,
                vec![key(0, "owner_id", body_path(&["old_owner_id"])?)?],
                vec![key(0, "todo_id", body_path(&["todo_id"])?)?],
            )?,
        ],
        vec![],
    )?;
    let program = ProjectionProgram::try_new(
        "reassign",
        1,
        ProjectionPartition::Unit,
        vec![ProjectionArm::try_new(
            "event",
            ProjectionEventSelector::try_from_descriptor(&event)?,
            vec![operation],
        )?],
    )?;
    let occurrence = occurrence(
        event,
        &json!({
            "todo_id": "todo-001",
            "old_owner_id": "owner-old",
            "owner_id": "owner-new"
        }),
    )?;
    let plan = ProjectionPlanTemplate::<PatchEvents>::try_new(program)?.resolve(&occurrence)?;
    let effects = plan.mutations()[0].provenance().relationship_effects();
    assert_eq!(effects.len(), 2);
    assert_eq!(effects[0].kind(), ProjectionRelationshipEffectKind::Unlink);
    assert_eq!(effects[1].kind(), ProjectionRelationshipEffectKind::Link);
    assert_ne!(
        effects[0]
            .source_key()
            .map(ResolvedProjectionKey::canonical_bytes),
        effects[1]
            .source_key()
            .map(ResolvedProjectionKey::canonical_bytes)
    );
    Ok(())
}

#[test]
fn relationship_invalidation_inventory_is_a_set_but_keyed_roots_remain_ordered(
) -> Result<(), Box<dyn Error>> {
    let event = descriptor(
        "todo.renamed",
        DomainEventBodyKind::Event,
        "TodoRenamed",
        PATCH_FP,
    );
    let relationship = ProjectionRelationship::try_new("Owners", "todos", "Todos")?;
    let make_operation =
        |mut invalidations: Vec<ProjectionInvalidation>| -> Result<_, ProjectionProgramError> {
            invalidations.reverse();
            ProjectionOperation::try_new(
                "invalidate-owner-todos",
                0,
                MutationKind::Patch,
                ProjectionTarget::try_new("Todos", "todos")?,
                vec![key(0, "todo_id", body_path(&["todo_id"])?)?],
                vec![field(0, "title", body_path(&["title"])?)?],
                vec![
                    ProjectionRelationshipEffect::invalidate(
                        1,
                        relationship.clone(),
                        vec![key(0, "owner_id", body_path(&["owner_b"])?)?],
                    )?,
                    ProjectionRelationshipEffect::invalidate(
                        0,
                        relationship.clone(),
                        vec![key(0, "owner_id", body_path(&["owner_a"])?)?],
                    )?,
                ],
                invalidations,
            )
        };
    let invalidations = vec![
        ProjectionInvalidation::model("TodoSearch")?,
        ProjectionInvalidation::relationship("Owners", "todos", "Todos")?,
    ];
    let program = |operation| -> Result<ProjectionProgram, ProjectionProgramError> {
        ProjectionProgram::try_new(
            "relationship-invalidations",
            1,
            ProjectionPartition::Unit,
            vec![ProjectionArm::try_new(
                "event",
                ProjectionEventSelector::try_from_descriptor(&event)?,
                vec![operation],
            )?],
        )
    };
    let reordered = program(make_operation(invalidations.clone())?)?;
    let original_order = program(make_operation(invalidations.into_iter().rev().collect())?)?;
    assert_eq!(
        reordered.canonical_bytes()?,
        original_order.canonical_bytes()?
    );

    let event_occurrence = occurrence(
        event,
        &json!({
            "todo_id": "todo-001",
            "title": "renamed",
            "owner_a": "owner-a",
            "owner_b": "owner-b"
        }),
    )?;
    let plan =
        ProjectionPlanTemplate::<PatchEvents>::try_new(reordered)?.resolve(&event_occurrence)?;
    let effects = plan.mutations()[0].provenance().relationship_effects();
    assert_eq!(effects.len(), 2);
    assert_eq!(effects[0].ordinal(), 0);
    assert_eq!(effects[1].ordinal(), 1);
    assert!(effects
        .iter()
        .all(|effect| effect.kind() == ProjectionRelationshipEffectKind::Invalidate));
    assert_ne!(
        effects[0]
            .source_key()
            .map(ResolvedProjectionKey::canonical_bytes),
        effects[1]
            .source_key()
            .map(ResolvedProjectionKey::canonical_bytes)
    );
    Ok(())
}

#[test]
fn compatible_patches_coalesce_and_conflicts_fail_at_registration() -> Result<(), Box<dyn Error>> {
    let event = descriptor(
        "todo.renamed",
        DomainEventBodyKind::Event,
        "TodoRenamed",
        PATCH_FP,
    );
    let target = ProjectionTarget::try_new("Todos", "todos")?;
    let key_fields = vec![key(0, "todo_id", body_path(&["todo_id"])?)?];
    let title = ProjectionOperation::try_new(
        "title",
        0,
        MutationKind::Patch,
        target.clone(),
        key_fields.clone(),
        vec![field(0, "title", body_path(&["title"])?)?],
        vec![],
        vec![],
    )?;
    let status = ProjectionOperation::try_new(
        "status",
        1,
        MutationKind::Patch,
        target.clone(),
        key_fields.clone(),
        vec![field(0, "status", body_path(&["status"])?)?],
        vec![],
        vec![],
    )?;
    let selector = ProjectionEventSelector::try_from_descriptor(&event)?;
    let arm = ProjectionArm::try_new("patch", selector.clone(), vec![status, title])?;
    let program = ProjectionProgram::try_new("coalesce", 1, ProjectionPartition::Unit, vec![arm])?;
    let event_occurrence = occurrence(
        event,
        &json!({"todo_id": "one", "title": "new", "status": "done"}),
    )?;
    let plan =
        ProjectionPlanTemplate::<PatchEvents>::try_new(program)?.resolve(&event_occurrence)?;
    assert_eq!(plan.mutations().len(), 1);
    assert_eq!(plan.mutations()[0].fields().len(), 2);
    assert_eq!(plan.mutations()[0].provenance().staging_ordinals(), &[0, 1]);

    let left = ProjectionOperation::try_new(
        "left",
        0,
        MutationKind::Patch,
        target.clone(),
        key_fields.clone(),
        vec![field(0, "title", body_path(&["title"])?)?],
        vec![],
        vec![],
    )?;
    let right = ProjectionOperation::try_new(
        "right",
        1,
        MutationKind::Patch,
        target,
        key_fields,
        vec![field(
            0,
            "title",
            ProjectionExpression::constant(ProjectionValue::string("different")),
        )?],
        vec![],
        vec![],
    )?;
    assert!(matches!(
        ProjectionArm::try_new("conflict", selector, vec![right, left]),
        Err(ProjectionProgramError::AmbiguousMutation { .. })
    ));

    let dynamic_left = ProjectionOperation::try_new(
        "dynamic-left",
        0,
        MutationKind::Delete,
        ProjectionTarget::try_new("Todos", "todos")?,
        vec![key(0, "todo_id", body_path(&["left_id"])?)?],
        vec![],
        vec![],
        vec![],
    )?;
    let dynamic_right = ProjectionOperation::try_new(
        "dynamic-right",
        1,
        MutationKind::Delete,
        ProjectionTarget::try_new("Todos", "todos")?,
        vec![key(0, "todo_id", body_path(&["right_id"])?)?],
        vec![],
        vec![],
        vec![],
    )?;
    assert!(matches!(
        ProjectionArm::try_new(
            "dynamic-overlap",
            ProjectionEventSelector::try_from_descriptor(&event_occurrence.descriptor().clone())?,
            vec![dynamic_left, dynamic_right],
        ),
        Err(ProjectionProgramError::AmbiguousMutation { .. })
    ));
    Ok(())
}

#[test]
fn complete_row_key_assignment_must_match_the_canonical_key_expression(
) -> Result<(), Box<dyn Error>> {
    let target = ProjectionTarget::try_new("Todos", "todos")?;
    let key_expression = body_path(&["todo_id"])?;
    let key_fields = vec![key(0, "todo_id", key_expression.clone())?];

    ProjectionOperation::try_new(
        "safe-upsert",
        0,
        MutationKind::Upsert,
        target.clone(),
        key_fields.clone(),
        vec![field(0, "todo_id", key_expression)?],
        vec![],
        vec![],
    )?;

    let error = ProjectionOperation::try_new(
        "unsafe-upsert",
        0,
        MutationKind::Upsert,
        target,
        key_fields,
        vec![field(
            0,
            "todo_id",
            ProjectionExpression::constant(ProjectionValue::string("different-row")),
        )?],
        vec![],
        vec![],
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("must use the exact key expression"));
    Ok(())
}

#[test]
fn unit_partition_is_distinct_and_event_marker_is_exact() -> Result<(), Box<dyn Error>> {
    let event = descriptor(
        "todo.purged",
        DomainEventBodyKind::Deletion,
        "TodoDeleted",
        DELETE_FP,
    );
    let arm = || -> Result<ProjectionArm, ProjectionProgramError> {
        ProjectionArm::try_new(
            "delete",
            ProjectionEventSelector::try_from_descriptor(&event)?,
            vec![delete_operation(
                "delete",
                0,
                ProjectionValue::string("todo-001"),
            )?],
        )
    };
    let unit = ProjectionProgram::try_new("unit", 1, ProjectionPartition::Unit, vec![arm()?])?;
    let string_unit = ProjectionProgram::try_new(
        "unit",
        1,
        ProjectionPartition::Expression(ProjectionExpression::constant(ProjectionValue::string(
            "unit",
        ))),
        vec![arm()?],
    )?;
    let occurrence = occurrence(event, &json!({"unused": true}))?;
    let unit = ProjectionPlanTemplate::<DeleteEvents>::try_new(unit)?.resolve(&occurrence)?;
    let string_unit =
        ProjectionPlanTemplate::<DeleteEvents>::try_new(string_unit)?.resolve(&occurrence)?;
    assert_eq!(
        unit.partition().as_ref(),
        ResolvedProjectionPartitionRef::Unit
    );
    assert!(matches!(
        string_unit.partition().as_ref(),
        ResolvedProjectionPartitionRef::Value(_)
    ));
    assert_ne!(
        unit.partition().canonical_bytes(),
        string_unit.partition().canonical_bytes()
    );
    assert!(matches!(
        ProjectionPlanTemplate::<PatchEvents>::try_new(golden_program()?),
        Err(ProjectionProgramError::EventSetMismatch)
    ));
    Ok(())
}

#[test]
fn expression_and_operation_limits_accept_boundary_and_reject_next() -> Result<(), Box<dyn Error>> {
    let limits = golden_program()?.limits();
    assert_eq!(limits.expression_value_levels(), 64);
    assert_eq!(limits.path_segments(), 32);
    assert_eq!(limits.operations_per_occurrence(), 128);
    assert_eq!(limits.key_bytes(), 4 * 1024);
    assert_eq!(limits.partition_bytes(), 4 * 1024);

    assert!(ProjectionExpression::body_path(ProjectionValueType::String, vec!["x"; 32]).is_ok());
    assert!(matches!(
        ProjectionExpression::body_path(ProjectionValueType::String, vec!["x"; 33]),
        Err(ProjectionProgramError::PathTooDeep { .. })
    ));

    let mut expression = ProjectionExpression::constant(ProjectionValue::null());
    for _ in 1..MAX_PROJECTION_EXPRESSION_DEPTH {
        expression = ProjectionExpression::list(vec![expression])?;
    }
    assert!(matches!(
        ProjectionExpression::list(vec![expression]),
        Err(ProjectionProgramError::ExpressionTooDeep { .. })
    ));

    let mut nested = Value::Null;
    for _ in 1..MAX_PROJECTION_EXPRESSION_DEPTH {
        nested = Value::Array(vec![nested]);
    }
    assert!(ProjectionValue::try_from_json(nested.clone()).is_ok());
    assert!(matches!(
        ProjectionValue::try_from_json(Value::Array(vec![nested])),
        Err(ProjectionProgramError::ExpressionTooDeep { .. })
    ));

    let selector = ProjectionEventSelector::try_from_descriptor(&descriptor(
        "todo.purged",
        DomainEventBodyKind::Deletion,
        "TodoDeleted",
        DELETE_FP,
    ))?;
    let operations = (0..MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE)
        .map(|ordinal| {
            delete_operation(
                &format!("delete-{ordinal}"),
                ordinal as u32,
                ProjectionValue::unsigned(ordinal as u64),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    assert!(ProjectionArm::try_new("limit", selector.clone(), operations.clone()).is_ok());
    let mut too_many = operations;
    too_many.push(delete_operation(
        "delete-overflow",
        MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE as u32,
        ProjectionValue::string("overflow"),
    )?);
    assert!(matches!(
        ProjectionArm::try_new("limit", selector, too_many),
        Err(ProjectionProgramError::TooManyOperations { .. })
    ));
    Ok(())
}

#[test]
fn key_partition_and_numeric_codecs_cover_exact_boundaries() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        ProjectionValue::try_float(-0.0)?,
        ProjectionValue::try_float(0.0)?
    );
    assert!(matches!(
        ProjectionValue::try_float(f64::INFINITY),
        Err(ProjectionProgramError::NonFiniteFloat)
    ));
    let numeric = ProjectionExpression::object([
        (
            "minimum",
            ProjectionExpression::constant(ProjectionValue::signed(i64::MIN)),
        ),
        (
            "maximum",
            ProjectionExpression::constant(ProjectionValue::unsigned(u64::MAX)),
        ),
    ])?;
    let _ = numeric;

    let numeric_descriptor = descriptor(
        "todo.renamed",
        DomainEventBodyKind::Event,
        "TodoRenamed",
        PATCH_FP,
    );
    let numeric_occurrence = occurrence(numeric_descriptor, &json!({"number": 1}))?;
    let numeric_body = json!({"number": 1});
    let signed_path = ProjectionExpression::body_path(ProjectionValueType::I64, ["number"])?;
    let unsigned_path = ProjectionExpression::body_path(ProjectionValueType::U64, ["number"])?;
    let ResolvedProjectionValue::Value(signed) =
        signed_path.resolve(&numeric_occurrence, &numeric_body)?
    else {
        return Err("signed body path did not resolve to a value".into());
    };
    let ResolvedProjectionValue::Value(unsigned) =
        unsigned_path.resolve(&numeric_occurrence, &numeric_body)?
    else {
        return Err("unsigned body path did not resolve to a value".into());
    };
    assert_eq!(signed.as_ref(), ProjectionValueRef::I64("1"));
    assert_eq!(unsigned.as_ref(), ProjectionValueRef::U64("1"));
    assert_ne!(serde_json::to_vec(&signed)?, serde_json::to_vec(&unsigned)?);
    let typed_path_program = |value_type| -> Result<ProjectionProgram, ProjectionProgramError> {
        let selector =
            ProjectionEventSelector::try_from_descriptor(numeric_occurrence.descriptor())?;
        let operation = ProjectionOperation::try_new(
            "typed-path",
            0,
            MutationKind::Patch,
            ProjectionTarget::try_new("Numbers", "numbers")?,
            vec![key(
                0,
                "id",
                ProjectionExpression::constant(ProjectionValue::string("one")),
            )?],
            vec![field(
                0,
                "number",
                ProjectionExpression::body_path(value_type, ["number"])?,
            )?],
            vec![],
            vec![],
        )?;
        ProjectionProgram::try_new(
            "typed-path",
            1,
            ProjectionPartition::Unit,
            vec![ProjectionArm::try_new("event", selector, vec![operation])?],
        )
    };
    assert_ne!(
        typed_path_program(ProjectionValueType::I64)?.id(),
        typed_path_program(ProjectionValueType::U64)?.id()
    );

    let key_limit = crate::projection_protocol::MAX_PROJECTION_RECORD_KEY_BYTES;
    let partition_limit = crate::projection_protocol::MAX_PROJECTION_PARTITION_BYTES;
    assert_exact_codec_boundary("key", key_limit)?;
    assert_exact_codec_boundary("partition", partition_limit)?;
    Ok(())
}

fn assert_exact_codec_boundary(kind: &str, limit: usize) -> Result<(), Box<dyn Error>> {
    let event = descriptor(
        "todo.purged",
        DomainEventBodyKind::Deletion,
        "TodoDeleted",
        DELETE_FP,
    );
    let selector = ProjectionEventSelector::try_from_descriptor(&event)?;
    let resolve = |len: usize| -> Result<ResolvedProjectionPlan, ProjectionProgramError> {
        let large = "x".repeat(len);
        let operation = delete_operation(
            "delete",
            0,
            ProjectionValue::string(if kind == "partition" {
                "one".to_owned()
            } else {
                large.clone()
            }),
        )?;
        let partition = if kind == "partition" {
            ProjectionPartition::Expression(ProjectionExpression::constant(
                ProjectionValue::string(large),
            ))
        } else {
            ProjectionPartition::Unit
        };
        let program = ProjectionProgram::try_new(
            "boundary",
            1,
            partition,
            vec![ProjectionArm::try_new(
                "delete",
                selector.clone(),
                vec![operation],
            )?],
        )?;
        let occurrence = occurrence(event.clone(), &json!({"key": "unused", "incarnation": 1}))
            .map_err(|error| ProjectionProgramError::CanonicalJson(error.to_string()))?;
        ProjectionPlanTemplate::<DeleteEvents>::try_new(program)?.resolve(&occurrence)
    };

    let mut low = 0;
    let mut high = limit;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if resolve(middle).is_ok() {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let accepted = resolve(low)?;
    let bytes = if kind == "partition" {
        accepted.partition().canonical_bytes()
    } else {
        accepted.mutations()[0].key().canonical_bytes()
    };
    assert_eq!(bytes.len(), limit);
    assert!(matches!(
        resolve(low + 1),
        Err(ProjectionProgramError::ValueTooLarge { .. })
    ));
    Ok(())
}

/// The programs of `tests/fixtures/resolved-projection-plan-v1.json`:
/// expression keys and an envelope partition (`golden`); mutation input keys
/// bound through arm bindings and a body partition (`input_keys`); a constant
/// key and the unit partition (`constant_key`).
fn resolved_plan_vector_programs(
) -> Result<Vec<(&'static str, ProjectionProgram)>, ProjectionProgramError> {
    use crate::mutation::{
        body_field_binding, compile_projection, envelope_binding, MutationColumn,
        MutationOperation, MutationProgram, ProjectionHandler,
    };

    let input_column = |name: &str, input: &str| {
        MutationColumn::try_new(name, ProjectionValueType::String, [input])
    };
    let input_keys = compile_projection(
        "todo-titles",
        1,
        ProjectionPartition::Expression(body_path(&["owner_id"])?),
        [ProjectionHandler::try_new(
            "renamed",
            ProjectionEventSelector::try_from_descriptor(&renamed_descriptor())?,
            vec![
                body_field_binding(["id"], ["todo_id"], ProjectionValueType::String)?,
                body_field_binding(["title"], ["title"], ProjectionValueType::String)?,
                envelope_binding(["stream"], ProjectionEnvelopeField::AggregateId)?,
            ],
            MutationProgram::try_new(
                "rename-todo",
                1,
                vec![MutationOperation::try_new(
                    "upsert-title",
                    MutationKind::Upsert,
                    ProjectionTarget::try_new("TodoTitles", "todo_titles")?,
                    vec![input_column("todo_id", "id")?],
                    vec![
                        input_column("todo_id", "id")?,
                        input_column("title", "title")?,
                        input_column("stream", "stream")?,
                    ],
                )?],
            )?,
        )?],
    )?;
    Ok(vec![
        ("golden", golden_program()?),
        ("input_keys", input_keys),
        (
            "constant_key",
            simple_program(ProjectionEventSelector::try_from_descriptor(
                &purged_descriptor(),
            )?)?,
        ),
    ])
}

fn renamed_descriptor() -> DomainEventDescriptor {
    descriptor(
        "todo.renamed",
        DomainEventBodyKind::Event,
        "TodoRenamed",
        PATCH_FP,
    )
}

fn purged_descriptor() -> DomainEventDescriptor {
    descriptor(
        "todo.purged",
        DomainEventBodyKind::Deletion,
        "TodoDeleted",
        DELETE_FP,
    )
}

/// The whole fixture document: the programs, and for each case the program
/// it runs, the occurrence and the plan that occurrence resolves to.
///
/// Cases cover a full upsert with model and relationship invalidations and
/// JSON string escapes, a patch with an unset field, a composite delete key
/// with an unlink, a related insert with a link and integer bounds, input
/// keys with a body partition, and a constant key in the unit partition.
fn resolved_plan_vector() -> Result<Value, Box<dyn Error>> {
    let state = descriptor(
        "todo.state-published",
        DomainEventBodyKind::State,
        "TodoState",
        STATE_FP,
    );
    let reassigned = descriptor(
        "todo.reassigned",
        DomainEventBodyKind::State,
        "TodoState",
        RELATED_FP,
    );
    let cases = [
        (
            "expression-keys-upsert-invalidations",
            "golden",
            occurrence(
                state,
                &json!({
                    "todo_id": "todo-001",
                    "owner_id": "owner-α",
                    "owner_count": 9,
                    "title": "Résumé 🚀 \"quoted\" \\ tab\t",
                    "status": "completed",
                    "priority": -1
                }),
            )?,
        ),
        (
            "expression-keys-patch-unset",
            "golden",
            occurrence(
                renamed_descriptor(),
                &json!({"todo_id": "todo-001", "title": "Renamed"}),
            )?,
        ),
        (
            "composite-key-delete-unlink",
            "golden",
            occurrence(
                purged_descriptor(),
                &json!({"key": "todo-001", "incarnation": 3, "owner_id": "owner-001"}),
            )?,
        ),
        (
            "related-insert-link",
            "golden",
            occurrence(
                reassigned,
                &json!({"todo_id": "todo-001", "owner_id": "owner-002"}),
            )?,
        ),
        (
            "input-keys-body-partition",
            "input_keys",
            occurrence(
                renamed_descriptor(),
                &json!({"todo_id": "todo-002", "owner_id": "owner-003", "title": "Bound"}),
            )?,
        ),
        (
            "constant-key-unit-partition",
            "constant_key",
            occurrence(purged_descriptor(), &json!({"key": "ignored"}))?,
        ),
    ];

    let programs = resolved_plan_vector_programs()?;
    let mut case_values = Vec::new();
    for (name, program_name, occurrence) in &cases {
        let (_, program) = programs
            .iter()
            .find(|(candidate, _)| candidate == program_name)
            .ok_or("unknown vector program")?;
        case_values.push(json!({
            "name": name,
            "program": program_name,
            "occurrence": occurrence,
            "plan": ResolvedProjectionPlan::resolve(program, occurrence)?,
        }));
    }
    let program_values = programs
        .iter()
        .map(|(name, program)| Ok(((*name).to_owned(), serde_json::to_value(program)?)))
        .collect::<Result<serde_json::Map<_, _>, Box<dyn Error>>>()?;
    Ok(json!({
        "fixture": "resolved-projection-plan",
        "version": 1,
        "programs": program_values,
        "cases": case_values,
    }))
}

#[test]
fn resolved_plans_match_the_shared_fixture() -> Result<(), Box<dyn Error>> {
    // The TypeScript suite (`js/tests/resolved-projection-plan.test.mjs`)
    // canonicalizes the same file to the same bytes, so no digest is pinned.
    let fixture = include_bytes!("../../tests/fixtures/resolved-projection-plan-v1.json");
    let fixture = fixture
        .strip_suffix(b"\n")
        .ok_or("the resolved-plan fixture must end with one newline")?;
    let expected = canonical::canonical_json_bytes(&resolved_plan_vector()?)?;
    assert_eq!(
        std::str::from_utf8(fixture)?,
        std::str::from_utf8(&expected)?
    );

    // Each case maps its occurrence, read back from the file, to its plan bytes.
    let document: Value = serde_json::from_slice(fixture)?;
    let programs = resolved_plan_vector_programs()?;
    let cases = document["cases"].as_array().ok_or("fixture cases")?;
    assert_eq!(cases.len(), 6);
    for case in cases {
        let program_name = case["program"].as_str().ok_or("case program")?;
        let (_, program) = programs
            .iter()
            .find(|(candidate, _)| *candidate == program_name)
            .ok_or("unknown vector program")?;
        assert_eq!(
            &serde_json::to_value(program)?,
            &document["programs"][program_name]
        );
        let occurrence = DomainEventOccurrence::from_canonical_bytes(
            &canonical::canonical_json_bytes(&case["occurrence"])?,
        )?;
        let plan = ResolvedProjectionPlan::resolve(program, &occurrence)?;
        assert_eq!(
            plan.canonical_bytes()?,
            canonical::canonical_json_bytes(&case["plan"])?,
            "case {}",
            case["name"]
        );
    }
    Ok(())
}
