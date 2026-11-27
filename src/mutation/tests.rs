use serde::{Deserialize, Serialize};

use crate::domain_event::{DomainEventBodyKind, DOMAIN_EVENT_OCCURRENCE_VERSION};
use crate::projection::{
    MutationKind, ProjectionEnvelopeField, ProjectionEventSelector, ProjectionExpression,
    ProjectionPartition, ProjectionProgramError, ProjectionTarget, ProjectionValueType,
    MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE,
};

use super::*;

const STATE_FP: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::read_model::ReadModel)]
#[readmodel(table = "mutation_todos", primary_key = ["todo_id"])]
struct Todos {
    todo_id: String,
    title: String,
    done: bool,
    rank: i64,
}

fn selector() -> ProjectionEventSelector {
    ProjectionEventSelector::try_new(
        DOMAIN_EVENT_OCCURRENCE_VERSION,
        "todo.state-published",
        1,
        DomainEventBodyKind::State,
        "TodoState",
        1,
        "urn:distributed:test:mutation-body:v1",
        STATE_FP,
        "distributed-json",
        1,
    )
    .unwrap()
}

fn schema() -> &'static crate::table::TableSchema {
    <Todos as crate::read_model::RelationalReadModel>::schema()
}

const SAVE: Mutation<()> = Mutation::declare(
    "SaveTodo",
    1,
    &[MutationStep::Upsert {
        model: schema,
        object: &["todo"],
    }],
);

const DELETE: Mutation<()> = Mutation::declare(
    "DeleteTodo",
    1,
    &[MutationStep::Delete {
        model: schema,
        key: &[MutationInput {
            column: "todo_id",
            path: &["todo_id"],
        }],
    }],
);

fn bound(
    mutation: Mutation<()>,
    inputs: Vec<MutationInputBinding>,
) -> Result<crate::projection::ProjectionProgram, ProjectionProgramError> {
    compile_projection(
        "todos",
        1,
        ProjectionPartition::Unit,
        [ProjectionHandler::try_new(
            "changed",
            selector(),
            inputs,
            mutation.program()?,
        )?],
    )
}

fn body(value_type: ProjectionValueType, path: &[&str]) -> ProjectionExpression {
    ProjectionExpression::body_path(value_type, path.iter().copied()).unwrap()
}

#[test]
fn a_declared_upsert_writes_every_column_typed_by_the_schema() {
    let program = SAVE.program().unwrap();
    let [operation] = program.operations() else {
        panic!("one statement declares one operation");
    };
    assert_eq!(operation.operation_id(), "upsert-0");
    assert_eq!(operation.kind(), MutationKind::Upsert);
    assert_eq!(operation.target().storage(), "mutation_todos");
    assert_eq!(operation.key()[0].input(), ["todo", "todo_id"]);
    let columns = operation
        .fields()
        .iter()
        .map(MutationColumn::name)
        .collect::<Vec<_>>();
    assert_eq!(columns, ["todo_id", "title", "done", "rank"]);

    let bound = bound(
        SAVE,
        vec![ProjectionInputSource::Body.binding("todo").unwrap()],
    )
    .unwrap();
    let operation = &bound.arms()[0].operations()[0];
    let assigned = |name: &str| {
        let field = operation
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .unwrap();
        field.assignment().clone()
    };
    assert_eq!(
        assigned("done"),
        crate::projection::ProjectionAssignment::Set(body(ProjectionValueType::Boolean, &["done"]))
    );
    assert_eq!(
        assigned("rank"),
        crate::projection::ProjectionAssignment::Set(body(ProjectionValueType::I64, &["rank"]))
    );
    assert_eq!(
        operation.key()[0].expression(),
        &body(ProjectionValueType::String, &["todo_id"])
    );
}

#[test]
fn an_input_takes_its_own_binding_before_a_bound_parent_object() {
    let program = bound(
        SAVE,
        vec![
            MutationInputBinding::try_new(
                vec!["todo".into()],
                body(ProjectionValueType::Json, &["after"]),
            )
            .unwrap(),
            body_field_binding(
                ["todo", "title"],
                ["renamed_to"],
                ProjectionValueType::String,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let operation = &program.arms()[0].operations()[0];
    let expression = |name: &str| {
        let field = operation
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .unwrap();
        let crate::projection::ProjectionAssignment::Set(expression) = field.assignment() else {
            panic!("a declared column is always set");
        };
        expression.clone()
    };
    assert_eq!(
        expression("title"),
        body(ProjectionValueType::String, &["renamed_to"])
    );
    assert_eq!(
        expression("rank"),
        body(ProjectionValueType::I64, &["after", "rank"])
    );
}

#[test]
fn a_delete_key_can_come_from_the_envelope() {
    let program = bound(
        DELETE,
        vec![ProjectionInputSource::AggregateId
            .binding("todo_id")
            .unwrap()],
    )
    .unwrap();
    let operation = &program.arms()[0].operations()[0];
    assert_eq!(operation.kind(), MutationKind::Delete);
    assert!(operation.fields().is_empty());
    assert_eq!(
        operation.key()[0].expression(),
        &ProjectionExpression::envelope(ProjectionEnvelopeField::AggregateId)
    );
}

#[test]
fn an_input_no_event_expression_fills_is_an_error_not_a_guess() {
    // `todo_id` is bound, but the mutation reads `todo.*`.
    let unrelated = bound(
        SAVE,
        vec![ProjectionInputSource::AggregateId
            .binding("todo_id")
            .unwrap()],
    );
    assert_eq!(
        unrelated,
        Err(ProjectionProgramError::MissingInput {
            path: "todo.todo_id".into()
        })
    );
    // An envelope field is not an object: nothing can be read through it.
    let through_scalar = bound(
        SAVE,
        vec![ProjectionInputSource::AggregateId.binding("todo").unwrap()],
    );
    assert!(matches!(
        through_scalar,
        Err(ProjectionProgramError::MissingInput { .. })
    ));
    assert!(bound(SAVE, Vec::new()).is_err());
}

#[test]
fn a_mutation_names_no_event() {
    let json = serde_json::to_string(&SAVE.program().unwrap()).unwrap();
    for event_word in ["selector", "event", "arm", "body", "envelope", "epoch"] {
        assert!(
            !json.contains(event_word),
            "`{event_word}` in a mutation: {json}"
        );
    }
}

#[test]
fn invalid_declarations_are_errors() {
    let column = |name: &str| MutationColumn::try_new(name, ProjectionValueType::String, [name]);
    let target = || ProjectionTarget::try_new("Todos", "mutation_todos").unwrap();
    let operation = |id: &str| {
        MutationOperation::try_new(
            id,
            MutationKind::Patch,
            target(),
            vec![column("todo_id").unwrap()],
            vec![column("title").unwrap()],
        )
        .unwrap()
    };

    assert!(MutationColumn::try_new("", ProjectionValueType::String, ["a"]).is_err());
    assert!(
        MutationColumn::try_new("a", ProjectionValueType::String, Vec::<String>::new()).is_err()
    );
    assert!(MutationColumn::try_new("a", ProjectionValueType::String, ["a", ""]).is_err());
    assert!(
        MutationOperation::try_new("no-key", MutationKind::Delete, target(), vec![], vec![])
            .is_err()
    );
    assert!(MutationProgram::try_new("", 1, vec![operation("a")]).is_err());
    assert!(MutationProgram::try_new("m", 0, vec![operation("a")]).is_err());
    assert!(MutationProgram::try_new("m", 1, Vec::new()).is_err());
    assert!(matches!(
        MutationProgram::try_new("m", 1, vec![operation("a"), operation("a")]),
        Err(ProjectionProgramError::DuplicateName { .. })
    ));
    let too_many = (0..=MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE)
        .map(|index| operation(&format!("op-{index}")))
        .collect();
    assert!(matches!(
        MutationProgram::try_new("m", 1, too_many),
        Err(ProjectionProgramError::TooManyOperations { .. })
    ));
    assert!(
        MutationInputBinding::try_new(Vec::new(), body(ProjectionValueType::Json, &[])).is_err()
    );
    let twice = vec![
        ProjectionInputSource::Body.binding("todo").unwrap(),
        ProjectionInputSource::AggregateId.binding("todo").unwrap(),
    ];
    assert!(matches!(
        ProjectionHandler::try_new("changed", selector(), twice, SAVE.program().unwrap()),
        Err(ProjectionProgramError::DuplicateName { .. })
    ));
    // A patch that declares no column is rejected when it is bound.
    let empty_patch = MutationOperation::try_new(
        "patch",
        MutationKind::Patch,
        target(),
        vec![column("todo_id").unwrap()],
        Vec::new(),
    )
    .unwrap();
    let program = MutationProgram::try_new("m", 1, vec![empty_patch]).unwrap();
    let handler = ProjectionHandler::try_new(
        "changed",
        selector(),
        vec![body_field_binding(["todo_id"], ["id"], ProjectionValueType::String).unwrap()],
        program,
    )
    .unwrap();
    assert!(handler.arm().is_err());
}

#[test]
fn from_state_materializes_the_row_the_mutation_writes() {
    #[derive(Serialize)]
    struct State {
        todo_id: String,
        title: String,
        done: bool,
        rank: i64,
    }
    let row: Todos = SAVE
        .from_state(&State {
            todo_id: "t1".into(),
            title: "Read".into(),
            done: true,
            rank: 3,
        })
        .unwrap();
    assert_eq!(
        row,
        Todos {
            todo_id: "t1".into(),
            title: "Read".into(),
            done: true,
            rank: 3,
        }
    );
    #[derive(Serialize)]
    struct Partial {
        todo_id: String,
    }
    assert!(SAVE
        .from_state::<_, Todos>(&Partial {
            todo_id: "t1".into()
        })
        .is_err());
}
