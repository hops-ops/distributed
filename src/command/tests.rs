use super::*;
use crate::command::{CommandInputType, CommandOutputType, CommandTypeDef, CommandTypeField};
use crate::outbox::OutboxMessage;
use serde::{Deserialize, Serialize};
use std::any::TypeId;

#[test]
fn command_ledger_fingerprint_preserves_the_v1_canonical_contract() {
    // The v1 digest includes historical field/scalar names. Module ownership
    // must not change the identity used to recognize a command retry.
    let (_, contract) = typed_command::<Input, Succeeded<Payload>>("core.fingerprint").into_parts();
    let digest = contract.fingerprint_bytes().unwrap();
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(
        hex,
        "fbc22caa3a5bd390cf7c17b63c0fc61304bc8706af47dd67e9ee40e63e40195a"
    );
}

#[test]
fn unsigned_range_is_part_of_command_retry_identity() {
    let mut signed = typed_command::<Input, Succeeded<Payload>>("revision.update").into_contract();
    signed.input.fields[0].type_name = "BigInt".into();
    let mut unsigned = signed.clone();
    unsigned.input.fields[0].unsigned_integer = Some(CommandUnsignedInteger::U64);
    assert_ne!(
        signed.fingerprint_bytes().unwrap(),
        unsigned.fingerprint_bytes().unwrap()
    );
    let mut narrower = unsigned.clone();
    narrower.input.fields[0].unsigned_integer = Some(CommandUnsignedInteger::U32);
    assert_ne!(
        unsigned.fingerprint_bytes().unwrap(),
        narrower.fingerprint_bytes().unwrap()
    );
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct Input {
    id: String,
}

impl CommandInputType for Input {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "Input",
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
        .with_type_id(TypeId::of::<Self>())
    }
}

#[derive(Serialize)]
struct Payload {
    id: String,
}

#[derive(Clone, Serialize, Deserialize, crate::read_model::ReadModel)]
#[readmodel(table = "projected_owners", primary_key = ["id"])]
struct ProjectedOwner {
    #[readmodel(id)]
    id: String,
}

#[derive(Clone, Serialize, Deserialize, crate::read_model::ReadModel)]
#[readmodel(table = "projected_rows", primary_key = ["id"])]
struct ProjectedRow {
    #[readmodel(id)]
    id: String,
    count: i64,
    note: Option<String>,
    owner_id: String,
    #[readmodel(belongs_to = "ProjectedOwner", foreign_key = "owner_id")]
    owner: Option<ProjectedOwner>,
}

#[derive(Serialize, crate::domain_event::DomainEvent)]
#[domain_event(name = "todo.completed", version = 1)]
struct TodoCompleted {
    todo_id: String,
    status: String,
}

#[derive(Serialize, crate::domain_event::DomainEvent)]
#[domain_event(name = "todo.completed", version = 1)]
struct ConflictingTodoCompleted {
    todo_id: String,
    completed_at: String,
}

#[derive(Serialize, crate::domain_event::DomainEvent)]
#[domain_event(name = "todo.renamed", version = 1)]
struct TodoRenamed {
    todo_id: String,
    title: String,
}

#[derive(Serialize, crate::domain_event::DomainState)]
#[domain_state(version = 1)]
struct TodoState {
    todo_id: String,
    status: String,
}

enum TodoStateChanged {}

impl crate::domain_event::DomainEventContract for TodoStateChanged {
    const EVENT_NAME: &'static str = "todo.state-changed";
    const EVENT_VERSION: u64 = 1;

    fn descriptor() -> crate::domain_event::DomainEventDescriptor {
        crate::domain_event::DomainEventDescriptor::state::<TodoState>(
            Self::EVENT_NAME,
            Self::EVENT_VERSION,
        )
    }
}

impl crate::domain_event::DomainEventBodyContract<TodoState> for TodoStateChanged {}

enum DishonestEventContract {}

impl crate::domain_event::DomainEventContract for DishonestEventContract {
    const EVENT_NAME: &'static str = "todo.lied";
    const EVENT_VERSION: u64 = 1;

    fn descriptor() -> crate::domain_event::DomainEventDescriptor {
        <TodoCompleted as crate::domain_event::DomainEvent>::DESCRIPTOR.clone()
    }
}

impl crate::domain_event::DomainEventBodyContract<TodoCompleted> for DishonestEventContract {}

enum DishonestStateContract {}

impl crate::domain_event::DomainEventContract for DishonestStateContract {
    const EVENT_NAME: &'static str = "todo.completed";
    const EVENT_VERSION: u64 = 1;

    fn descriptor() -> crate::domain_event::DomainEventDescriptor {
        <TodoCompleted as crate::domain_event::DomainEvent>::DESCRIPTOR.clone()
    }
}

impl crate::domain_event::DomainEventBodyContract<TodoState> for DishonestStateContract {}

impl CommandOutputType for Payload {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "Payload",
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
        .with_type_id(TypeId::of::<Self>())
    }
}

#[test]
fn preparation_serializes_the_payload_once_for_the_commit() {
    let prepared = PreparedCommand::<Eventual<Payload>>::prepare(Payload {
        id: "todo-1".into(),
    })
    .unwrap();
    assert_eq!(prepared.consistency(), CommandConsistency::Eventual);
    assert_eq!(prepared.serialized_payload()["id"], "todo-1");
    assert_eq!(prepared.finalize_after_commit()["id"], "todo-1");
}

#[test]
fn projected_output_is_generated_from_relational_schema_without_graphql_output_derive() {
    let contract = typed_command::<Input, Atomic<ProjectedRow>>("row.project").into_contract();

    assert_eq!(contract.output.name, "ProjectedRow");
    assert_eq!(
        contract
            .output
            .fields
            .iter()
            .map(|field| (
                field.name.as_str(),
                field.type_name.as_str(),
                field.nullable
            ))
            .collect::<Vec<_>>(),
        vec![
            ("id", "String", false),
            ("count", "BigInt", false),
            ("note", "String", true),
            ("owner_id", "String", false),
        ]
    );
    assert_eq!(contract.output.type_id, Some(TypeId::of::<ProjectedRow>()));
}

#[test]
fn successful_consistency_wire_vocabulary_is_exact_and_breaking() {
    let cases = [
        (CommandConsistency::Succeeded, "\"succeeded\""),
        (CommandConsistency::Eventual, "\"eventual\""),
        (CommandConsistency::Atomic, "\"atomic\""),
    ];

    for (consistency, encoded) in cases {
        assert_eq!(serde_json::to_string(&consistency).unwrap(), encoded);
        assert_eq!(
            serde_json::from_str::<CommandConsistency>(encoded).unwrap(),
            consistency
        );
    }
    // No wire aliases / back-compat names.
    assert!(serde_json::from_str::<CommandConsistency>("\"accepted\"").is_err());
    assert!(serde_json::from_str::<CommandConsistency>("\"fact\"").is_err());
    assert!(serde_json::from_str::<CommandConsistency>("\"causal\"").is_err());
    assert!(serde_json::from_str::<CommandConsistency>("\"projected\"").is_err());
}

#[test]
fn command_events_are_exact_values_independent_of_projector_declarations() {
    let contract = typed_command::<Input, Succeeded<Payload>>("todo.complete")
        .emits(crate::events![TodoCompleted])
        .into_contract();
    let binding =
        TypedServiceCommandBinding::from_contracts("todos", std::slice::from_ref(&contract));
    assert!(binding.is_ok());
    assert_eq!(contract.projections.selectors.len(), 1);
    assert_eq!(
        contract.projections.selectors[0].event_name(),
        "todo.completed"
    );
}

#[test]
fn command_transition_fills_emits_from_domain_event_set() {
    let from_transition =
        super::command_transition::<TodoCompleted, Input, Succeeded<Payload>>("todo.complete")
            .into_contract();
    let from_emits = typed_command::<Input, Succeeded<Payload>>("todo.complete")
        .emits(crate::events![TodoCompleted])
        .into_contract();
    assert_eq!(
        from_transition.projections.selectors,
        from_emits.projections.selectors
    );
    assert_eq!(
        from_transition.projections.selectors[0].event_name(),
        "todo.completed"
    );
}

#[test]
fn authenticated_user_field_reuses_generated_event_schema_metadata() {
    let contract =
        super::command_transition::<TodoStateChanged, Input, Succeeded<Payload>>("todo.complete")
            .authenticated_user_field::<TodoStateChanged, TodoState>("status")
            .into_contract();
    let field = contract.projections.inferred_values[0]
        .preview
        .fields
        .iter()
        .find(|field| field.body_path == ["status"])
        .expect("generated transition field must remain present");

    assert_eq!(
        field.source,
        CommandProjectionPreviewSource::trusted("x-user-id", "string")
    );
    assert_eq!(
        field.body_type,
        Some(crate::projection::lower::ProjectionPortableType::String)
    );
    assert_eq!(field.body_nullable, Some(false));
    assert_eq!(field.body_always_present, Some(true));
    let previous = __command_projection_state_preview::<TodoStateChanged, TodoState>(vec![(
        "status",
        CommandProjectionPreviewSource::trusted("x-user-id", "string"),
    )]);
    assert_eq!(contract.projections.inferred_values[0].preview, previous);
}

#[derive(Serialize, crate::domain_event::DomainEvent)]
#[domain_event(name = "membership.removed", version = 1)]
struct MembershipRemoved {
    #[serde(rename = "memberId")]
    user_id: String,
    revision: u64,
    #[serde(skip)]
    internal: String,
}

impl crate::domain_event::DomainEventBodyContract<MembershipRemoved> for TodoCompleted {}

#[test]
fn authenticated_flat_event_field_preserves_wire_name_and_rejects_invalid_bindings() {
    let body = MembershipRemoved {
        user_id: "member-1".into(),
        revision: 1,
        internal: "not serialized".into(),
    };
    assert_eq!(body.internal, "not serialized");
    assert!(serde_json::to_value(&body)
        .unwrap()
        .get("internal")
        .is_none());
    let command = |field| {
        super::command_transition::<MembershipRemoved, Input, Succeeded<Payload>>("member.leave")
            .authenticated_user_field::<MembershipRemoved, MembershipRemoved>(field)
            .into_contract()
    };
    let valid = command("user_id");
    TypedServiceCommandBinding::from_contracts("members", std::slice::from_ref(&valid)).unwrap();
    let field = &valid.projections.inferred_values[0].preview.fields[0];
    assert_eq!(field.body_path, ["memberId"]);
    assert_eq!(
        field.source,
        CommandProjectionPreviewSource::trusted("x-user-id", "string")
    );

    for name in ["missing", "internal", "memberId", "revision"] {
        assert!(
            TypedServiceCommandBinding::from_contracts("members", &[command(name)]).is_err(),
            "{name}"
        );
    }
    let mismatched = typed_command::<Input, Succeeded<Payload>>("todo.bad-body")
        .emits(crate::events![DishonestStateContract])
        .authenticated_user_field::<DishonestStateContract, TodoState>("status")
        .into_contract();
    assert!(
        TypedServiceCommandBinding::from_contracts("members", &[mismatched])
            .unwrap_err()
            .contains("differs from its exact typed body descriptor")
    );
    let mismatched_flat = typed_command::<Input, Succeeded<Payload>>("todo.bad-flat-body")
        .emits(crate::events![TodoCompleted])
        .authenticated_user_field::<TodoCompleted, MembershipRemoved>("user_id")
        .into_contract();
    assert!(
        TypedServiceCommandBinding::from_contracts("members", &[mismatched_flat])
            .unwrap_err()
            .contains("differs from its exact typed body descriptor")
    );
    let undeclared = typed_command::<Input, Succeeded<Payload>>("member.undeclared")
        .authenticated_user_field::<MembershipRemoved, MembershipRemoved>("user_id")
        .into_contract();
    assert!(
        TypedServiceCommandBinding::from_contracts("members", &[undeclared])
            .unwrap_err()
            .contains("outside its exact emitted event set")
    );
}

#[test]
fn authenticated_user_field_merges_with_known_values_and_rejects_unknown_fields() {
    let mut projections = CommandProjectionEvents::default();
    projections.add_event_set(crate::events![TodoStateChanged]);
    projections.add_inferred_values(__command_projection_state_preview::<
        TodoStateChanged,
        TodoState,
    >(vec![(
        "todo_id",
        CommandProjectionPreviewSource::constant(crate::projection::ProjectionValue::string(
            "todo-1",
        )),
    )]));
    projections.add_authenticated_user_field(
        "status",
        __command_projection_state_preview::<TodoStateChanged, TodoState>(vec![(
            "status",
            CommandProjectionPreviewSource::trusted("x-user-id", "string"),
        )]),
    );
    projections
        .canonicalize_and_validate("todo.complete")
        .expect("distinct inferred fields for one event must merge");
    assert_eq!(projections.inferred_values.len(), 1);
    assert_eq!(projections.inferred_values[0].preview.fields.len(), 2);

    let invalid = super::command_transition::<TodoStateChanged, Input, Succeeded<Payload>>(
        "todo.invalid-principal-field",
    )
    .authenticated_user_field::<TodoStateChanged, TodoState>("missing")
    .into_contract();
    let error = TypedServiceCommandBinding::from_contracts("todos", &[invalid]).unwrap_err();
    assert!(error.contains("is not one exact emitted-event body field"));
}

#[test]
fn command_event_registration_rejects_duplicates_and_conflicting_schemas() {
    let duplicate = typed_command::<Input, Succeeded<Payload>>("todo.duplicate")
        .emits(crate::events![TodoCompleted])
        .emits(crate::events![TodoCompleted])
        .into_contract();
    assert!(
        TypedServiceCommandBinding::from_contracts("todos", &[duplicate])
            .unwrap_err()
            .contains("repeats an exact emitted")
    );

    let conflicting = typed_command::<Input, Succeeded<Payload>>("todo.conflicting")
        .emits(crate::events![TodoCompleted, ConflictingTodoCompleted])
        .into_contract();
    assert!(
        TypedServiceCommandBinding::from_contracts("todos", &[conflicting])
            .unwrap_err()
            .contains("conflicting schemas")
    );
}

#[test]
fn command_registration_distrusts_manual_event_contracts() {
    let mismatched_name = typed_command::<Input, Succeeded<Payload>>("todo.dishonest-event")
        .emits(crate::events![DishonestEventContract])
        .into_contract();
    assert!(
        TypedServiceCommandBinding::from_contracts("todos", &[mismatched_name])
            .unwrap_err()
            .contains("differs from descriptor name")
    );

    let mismatched_state = typed_command::<Input, Succeeded<Payload>>("todo.dishonest-state")
        .emits(crate::events![DishonestStateContract])
        .applies(crate::state_preview! {
            DishonestStateContract => TodoState {
                todo_id: input.id,
                ..unknown
            }
        })
        .into_contract();
    assert!(
        TypedServiceCommandBinding::from_contracts("todos", &[mismatched_state])
            .unwrap_err()
            .contains("does not exactly describe")
    );
}

#[test]
fn command_preview_requires_membership_and_rejects_server_only_sources() {
    let outside = typed_command::<Input, Succeeded<Payload>>("todo.outside")
        .emits(crate::events![TodoCompleted])
        .applies(crate::event_preview! {
            TodoRenamed => TodoRenamed {
                todo_id: input.id,
                ..unknown
            }
        })
        .into_contract();
    assert!(
        TypedServiceCommandBinding::from_contracts("todos", &[outside])
            .unwrap_err()
            .contains("outside its exact emitted event set")
    );

    let server_only = typed_command::<Input, Succeeded<Payload>>("todo.server-only")
        .emits(crate::events![TodoCompleted])
        .applies(
            CommandProjectionPreview::new()
                .events(crate::events![TodoCompleted])
                .field(["todo_id"], CommandProjectionPreviewSource::ServerOnly),
        )
        .into_contract();
    assert!(
        TypedServiceCommandBinding::from_contracts("todos", &[server_only])
            .unwrap_err()
            .contains("server-only preview provenance")
    );
}

#[test]
fn partial_preview_retains_known_unknown_and_typed_constant_sources() {
    let contract = typed_command::<Input, Succeeded<Payload>>("todo.partial")
        .emits(crate::events![TodoCompleted])
        .applies(crate::event_preview! {
            TodoCompleted => TodoCompleted {
                todo_id: input.id,
                status: "completed",
                ..unknown
            }
        })
        .into_contract();
    TypedServiceCommandBinding::from_contracts("todos", std::slice::from_ref(&contract)).unwrap();
    assert_eq!(contract.projections.previews[0].preview.fields.len(), 2);
}

#[test]
fn repeated_preview_declarations_preserve_synthetic_occurrence_order() {
    let contract = typed_command::<Input, Succeeded<Payload>>("todo.repeated-preview")
        .emits(crate::events![TodoCompleted])
        .applies(crate::event_preview! {
            TodoCompleted => TodoCompleted {
                todo_id: "first",
                ..unknown
            }
        })
        .applies(crate::event_preview! {
            TodoCompleted => TodoCompleted {
                todo_id: input.id,
                ..unknown
            }
        })
        .into_contract();
    let mut projections = contract.projections.clone();
    projections
        .canonicalize_and_validate("todo.repeated-preview")
        .unwrap();

    assert_eq!(projections.previews.len(), 2);
    assert!(matches!(
        projections.previews[0].preview.fields[0].source,
        CommandProjectionPreviewSource::Constant { .. }
    ));
    assert!(matches!(
        projections.previews[1].preview.fields[0].source,
        CommandProjectionPreviewSource::InputPath { .. }
    ));
}

#[test]
fn succeeded_allows_an_empty_domain_batch() {
    let contract = typed_command::<Input, Succeeded<Payload>>("todo.check").into_contract();
    let prepared = PreparedCommand::<Succeeded<Payload>>::prepare(Payload {
        id: "todo-1".into(),
    })
    .unwrap();

    prepared
        .validate_commit_evidence(&contract, false, &[], &[], None)
        .unwrap();
}

#[test]
fn eventual_without_projection_selectors_fails_at_commit_validation() {
    let contract = typed_command::<Input, Eventual<Payload>>("todo.create").into_contract();
    let prepared = PreparedCommand::<Eventual<Payload>>::prepare(Payload {
        id: "todo-1".into(),
    })
    .unwrap();
    let fact = OutboxMessage::create("message-1", "todo.created", Vec::new()).unwrap();

    assert_eq!(
        prepared
            .validate_commit_evidence(&contract, false, &[fact], &[], None)
            .unwrap_err(),
        CommandCommitProofError::EventualHasNoProjectionSelectors
    );
}

#[test]
fn per_route_fingerprint_is_stable_and_contract_sensitive() {
    let first = typed_command::<Input, Succeeded<Payload>>("todo.create")
        .roles(["writer", "admin"])
        .into_contract();
    let reordered = typed_command::<Input, Succeeded<Payload>>("todo.create")
        .roles(["admin", "writer"])
        .into_contract();
    let renamed = typed_command::<Input, Succeeded<Payload>>("todo.rename")
        .roles(["admin", "writer"])
        .into_contract();

    assert_eq!(
        first.fingerprint_bytes().unwrap(),
        reordered.fingerprint_bytes().unwrap()
    );
    assert_ne!(
        first.fingerprint_bytes().unwrap(),
        renamed.fingerprint_bytes().unwrap()
    );
}

#[test]
fn binding_rejects_missing_command_type_ids() {
    let mut contract = typed_command::<Input, Succeeded<Payload>>("todo.create").into_contract();
    contract.input.type_id = None;
    let error = TypedServiceCommandBinding::from_contracts("todos", &[contract]).unwrap_err();
    assert!(error.contains("input command metadata is missing"));
}

#[test]
fn binding_canonicalizes_fields_and_roles() {
    let mut first = typed_command::<Input, Succeeded<Payload>>("todo.create")
        .roles(["writer", "admin"])
        .into_contract();
    first.input.fields.push(CommandTypeField {
        unsigned_integer: None,
        name: "z_extra".into(),
        type_name: "String".into(),
        nullable: true,
        list: false,
        item_nullable: false,
        nested: None,
    });

    let mut second = first.clone();
    second.roles.reverse();
    second.input.fields.reverse();
    let first = TypedServiceCommandBinding::from_contracts("todos", &[first]).unwrap();
    let second = TypedServiceCommandBinding::from_contracts("todos", &[second]).unwrap();
    assert_eq!(first, second);
}

#[test]
fn invalid_projection_declaration_is_an_error_not_a_panic() {
    // A hand-built or deserialized contract can carry declaration errors that
    // never went through validation; fingerprinting reports them.
    let mut contract =
        typed_command::<Input, Succeeded<Payload>>("core.invalid-projection").into_contract();
    contract
        .projections
        .declaration_errors
        .push("bad declaration".into());
    let error = contract.fingerprint_bytes().unwrap_err();
    assert!(error.contains("core.invalid-projection"), "{error}");
    assert!(error.contains("bad declaration"), "{error}");
    assert!(contract.canonical_value().is_err());
    assert!(crate::application::CommandSpec::from_contract(&contract).is_err());
}

mod wasm_pure_declarations {
    use super::super::{CommandProjectionEvents, CommandProjectionPureReduce};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Default, Serialize, Deserialize, crate::read_model::ReadModel)]
    #[readmodel(primary_key = ["board_id"])]
    struct Boards {
        board_id: String,
        cells: Vec<Vec<u8>>,
        moves: Vec<String>,
    }

    #[derive(Deserialize, crate::table::JsonType)]
    struct Row {
        cells: Vec<Vec<u8>>,
    }

    #[derive(Deserialize, crate::table::JsonType)]
    struct Args {
        step: String,
    }

    #[derive(Serialize, crate::table::JsonType)]
    struct Patch {
        cells: Vec<Vec<u8>>,
        moves: Vec<String>,
    }

    fn step(_: Row, _: Args) -> Option<Patch> {
        None
    }

    crate::wasm_pure! {
        const STEP = step(Row, Args) -> Patch;
    }

    #[derive(Serialize, crate::table::JsonType)]
    struct WrongPatch {
        cells: Vec<String>,
        inventory: Vec<String>,
        board_id: String,
    }

    fn wrong(_: Row, _: Args) -> Option<WrongPatch> {
        None
    }

    crate::wasm_pure! {
        const WRONG = wrong(Row, Args) -> WrongPatch;
    }

    fn validate(reduce: CommandProjectionPureReduce) -> Result<(), String> {
        let mut projections = CommandProjectionEvents::default();
        projections.add_pure_reduce(reduce);
        projections.canonicalize_and_validate("board.step")
    }

    #[test]
    fn known_record_derives_the_binding_from_rust_types() {
        let reduce = CommandProjectionPureReduce::known_record::<Boards>(STEP);
        let package = env!("CARGO_PKG_NAME");
        assert_eq!(reduce.fn_name, format!("{package}::step"));
        assert_eq!(reduce.wasm_export, "step");
        assert_eq!(reduce.wasm_rust_package, package);
        assert_eq!(reduce.wasm_package, format!("wasm/{package}/{package}"));
        assert_eq!(reduce.model, "Boards");
        assert_eq!(reduce.key[0].name, "board_id");
        assert_eq!(reduce.args[0].name, "step");
        assert_eq!(reduce.assign, ["cells", "moves"]);
        validate(reduce).unwrap();
    }

    #[test]
    fn a_pure_that_disagrees_with_its_model_fails_closed() {
        let error =
            validate(CommandProjectionPureReduce::known_record::<Boards>(WRONG)).unwrap_err();
        assert!(
            error.contains("does not have the JSON shape of column `Boards.cells`"),
            "{error}"
        );
        let reduce = CommandProjectionPureReduce::known_record::<Boards>(WRONG);
        assert!(reduce
            .declaration_errors
            .iter()
            .any(|e| e.contains("`inventory` is not a column of `Boards`")));
        assert!(reduce
            .declaration_errors
            .iter()
            .any(|e| e.contains("assigns key column `board_id`")));
    }
}

mod commit_evidence {
    use super::*;
    use crate::projection_protocol::{
        ProjectionModelOwnership, ProjectionPartitionSpec, ProjectorTopologyId,
    };
    use crate::read_model::RelationalReadModel;
    use crate::table::{
        DeleteTableRowMutation, ExpectedVersion, RowWriteMode, TableMutation, TableRowMutation,
        TableWritePlan,
    };

    fn row(id: &str, count: i64) -> ProjectedRow {
        ProjectedRow {
            id: id.into(),
            count,
            note: None,
            owner_id: "owner-1".into(),
            owner: None,
        }
    }

    fn write(row: &ProjectedRow, mode: RowWriteMode) -> TableMutation {
        TableMutation::UpsertRow(TableRowMutation {
            schema: ProjectedRow::schema(),
            key: row.primary_key().unwrap(),
            values: row.to_row().unwrap(),
            expected_version: ExpectedVersion::Any,
            mode,
        })
    }

    fn upsert(row: &ProjectedRow) -> TableMutation {
        write(row, RowWriteMode::Upsert)
    }

    fn proof(row: &ProjectedRow) -> ProjectionCommitProof {
        ProjectionCommitProof::for_model(row).unwrap()
    }

    fn atomic_contract() -> TypedCommandContract {
        typed_command::<Input, Atomic<ProjectedRow>>("row.project").into_contract()
    }

    fn direct_target(contract: &TypedCommandContract) -> CommandDirectProjectionTarget {
        contract.projected_model.as_ref().unwrap().bind(
            "rows",
            &["row.projected".to_string()],
            &["ProjectedRow".to_string()],
            &ProjectionPartitionSpec::Unit,
            vec![ProjectionModelOwnership::new("ProjectedRow", "projected_rows").unwrap()],
            Some(ProjectorTopologyId::new(1, "rows", [0x31; 32]).unwrap()),
            None,
        )
    }

    fn resolved_target() -> ResolvedDirectProjectionTarget {
        direct_target(&atomic_contract())
            .resolve(&serde_json::json!({}), None)
            .unwrap()
    }

    fn fact() -> OutboxMessage {
        OutboxMessage::create("message-1", "row.projected", Vec::new()).unwrap()
    }

    #[test]
    fn proof_matches_exactly_one_full_row_upsert_of_the_returned_row() {
        let returned = row("r1", 1);
        let delete = TableMutation::DeleteRow(DeleteTableRowMutation {
            schema: ProjectedRow::schema(),
            key: returned.primary_key().unwrap(),
            expected_version: ExpectedVersion::Any,
        });
        let missing = CommandCommitProofError::ProjectionWriteMissing {
            model: "ProjectedRow".into(),
        };
        let mismatch = CommandCommitProofError::ProjectionWriteMismatch {
            model: "ProjectedRow".into(),
        };
        let conflict = CommandCommitProofError::ProjectionWriteConflict {
            model: "ProjectedRow".into(),
        };
        type Case = (
            &'static str,
            TypeId,
            Vec<TableMutation>,
            Result<(), CommandCommitProofError>,
        );
        let cases: Vec<Case> = vec![
            (
                "exact upsert among other rows",
                TypeId::of::<ProjectedRow>(),
                vec![upsert(&row("r2", 9)), upsert(&returned)],
                Ok(()),
            ),
            (
                "proof for another Rust type",
                TypeId::of::<Payload>(),
                vec![upsert(&returned)],
                Err(CommandCommitProofError::ProjectionOutputTypeMismatch),
            ),
            (
                "nothing staged",
                TypeId::of::<ProjectedRow>(),
                vec![],
                Err(missing.clone()),
            ),
            (
                "only another key staged",
                TypeId::of::<ProjectedRow>(),
                vec![upsert(&row("r2", 1))],
                Err(missing),
            ),
            (
                "same key, different values",
                TypeId::of::<ProjectedRow>(),
                vec![upsert(&row("r1", 2))],
                Err(mismatch.clone()),
            ),
            (
                "insert instead of upsert",
                TypeId::of::<ProjectedRow>(),
                vec![write(&returned, RowWriteMode::Insert)],
                Err(mismatch.clone()),
            ),
            (
                "a delete of the returned key",
                TypeId::of::<ProjectedRow>(),
                vec![delete.clone()],
                Err(mismatch),
            ),
            (
                "a second mutation of the returned key",
                TypeId::of::<ProjectedRow>(),
                vec![upsert(&returned), delete],
                Err(conflict),
            ),
        ];
        for (name, output_type, mutations, expected) in cases {
            let plan = TableWritePlan::new(mutations);
            assert_eq!(
                proof(&returned).validate(output_type, std::iter::once(&plan)),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn extracting_the_upsert_removes_it_and_drops_an_emptied_plan() {
        let returned = row("r1", 1);
        let mut plans = vec![
            TableWritePlan::new(vec![upsert(&row("r2", 2)), upsert(&returned)]),
            TableWritePlan::new(vec![upsert(&row("r3", 3))]),
        ];
        let extracted = proof(&returned)
            .extract_exact_upsert(TypeId::of::<ProjectedRow>(), &mut plans)
            .unwrap();
        assert_eq!(extracted, upsert(&returned));
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].mutations, vec![upsert(&row("r2", 2))]);

        let mut alone = vec![TableWritePlan::new(vec![upsert(&returned)])];
        proof(&returned)
            .extract_exact_upsert(TypeId::of::<ProjectedRow>(), &mut alone)
            .unwrap();
        assert!(alone.is_empty(), "an emptied plan is removed");

        let mut none = Vec::new();
        assert_eq!(
            proof(&returned)
                .extract_exact_upsert(TypeId::of::<ProjectedRow>(), &mut none)
                .unwrap_err(),
            CommandCommitProofError::ProjectionWriteMissing {
                model: "ProjectedRow".into()
            }
        );
    }

    #[test]
    fn commit_evidence_rejects_a_preparation_that_breaks_the_declaration() {
        let payload = || Payload { id: "p".into() };
        let eventual = PreparedCommand::<Eventual<Payload>>::prepare(payload()).unwrap();
        let succeeded = typed_command::<Input, Succeeded<Payload>>("todo.check").into_contract();
        assert_eq!(
            eventual
                .validate_commit_evidence(&succeeded, true, &[], &[], None)
                .unwrap_err(),
            CommandCommitProofError::ConsistencyMismatch {
                declared: CommandConsistency::Succeeded,
                prepared: CommandConsistency::Eventual,
            }
        );

        let mut retyped = succeeded.clone();
        retyped.output_type_id = TypeId::of::<Input>();
        let prepared = PreparedCommand::<Succeeded<Payload>>::prepare(payload()).unwrap();
        assert_eq!(
            prepared
                .validate_commit_evidence(&retyped, true, &[], &[], None)
                .unwrap_err(),
            CommandCommitProofError::OutputTypeMismatch
        );
    }

    #[test]
    fn atomic_commit_evidence_needs_a_durable_event_and_the_staged_row() {
        let contract = atomic_contract();
        let returned = row("r1", 1);
        let prepared = PreparedCommand::<Atomic<ProjectedRow>>::prepare_atomic(
            returned.clone(),
            proof(&returned),
        )
        .unwrap();
        let staged = vec![TableWritePlan::new(vec![upsert(&returned)])];

        prepared
            .validate_commit_evidence(&contract, false, &[fact()], &staged, None)
            .unwrap();
        prepared
            .validate_commit_evidence(&contract, true, &[], &staged, None)
            .unwrap();
        assert_eq!(
            prepared
                .validate_commit_evidence(&contract, false, &[], &staged, None)
                .unwrap_err(),
            CommandCommitProofError::DurableEventMissing
        );
        assert_eq!(
            prepared
                .validate_commit_evidence(&contract, true, &[], &[], None)
                .unwrap_err(),
            CommandCommitProofError::ProjectionWriteMissing {
                model: "ProjectedRow".into()
            }
        );
        assert_eq!(
            PreparedCommand::<Atomic<ProjectedRow>>::prepare_modeled_atomic()
                .validate_commit_evidence(&contract, true, &[], &staged, None)
                .unwrap_err(),
            CommandCommitProofError::MissingProjectionProof
        );
    }

    #[test]
    fn sealing_moves_the_staged_row_into_the_direct_participant() {
        let returned = row("r1", 1);
        let prepared = PreparedCommand::<Atomic<ProjectedRow>>::prepare_atomic(
            returned.clone(),
            proof(&returned),
        )
        .unwrap();
        let mut staged = vec![
            TableWritePlan::new(vec![upsert(&returned)]),
            TableWritePlan::new(vec![upsert(&row("r2", 2))]),
        ];
        let batch = prepared
            .seal_direct_projection(Some(resolved_target()), &mut staged, None, "command-1")
            .unwrap()
            .expect("an atomic command seals one participant");
        assert_eq!(batch.mutations.len(), 1);
        assert_eq!(batch.mutations[0].mutation, upsert(&returned));
        assert_eq!(batch.causation_id, "command-1");
        assert_eq!(
            staged,
            vec![TableWritePlan::new(vec![upsert(&row("r2", 2))])],
            "the returned row commits only through the participant"
        );
    }

    #[test]
    fn sealing_rejects_a_missing_or_unexpected_target() {
        let returned = row("r1", 1);
        let atomic = PreparedCommand::<Atomic<ProjectedRow>>::prepare_atomic(
            returned.clone(),
            proof(&returned),
        )
        .unwrap();
        let staged = || vec![TableWritePlan::new(vec![upsert(&returned)])];
        assert_eq!(
            atomic
                .seal_direct_projection(None, &mut staged(), None, "c")
                .unwrap_err(),
            CommandCommitProofError::MissingDirectProjectionTarget
        );
        assert_eq!(
            PreparedCommand::<Atomic<ProjectedRow>>::prepare_modeled_atomic()
                .seal_direct_projection(Some(resolved_target()), &mut staged(), None, "c")
                .unwrap_err(),
            CommandCommitProofError::MissingProjectionProof
        );
        let succeeded =
            PreparedCommand::<Succeeded<Payload>>::prepare(Payload { id: "p".into() }).unwrap();
        assert_eq!(
            succeeded
                .seal_direct_projection(Some(resolved_target()), &mut staged(), None, "c")
                .unwrap_err(),
            CommandCommitProofError::UnexpectedDirectProjectionTarget
        );
        assert!(succeeded
            .seal_direct_projection(None, &mut staged(), None, "c")
            .unwrap()
            .is_none());
    }

    #[test]
    fn modeled_payload_is_materialized_only_from_a_resolved_plan() {
        let mut pending = PreparedCommand::<Atomic<ProjectedRow>>::prepare_modeled_atomic();
        assert!(matches!(
            pending.materialize_modeled_projection(None),
            Err(CommandCommitProofError::DirectProjection(_))
        ));
        let returned = row("r1", 1);
        let mut ready = PreparedCommand::<Atomic<ProjectedRow>>::prepare_atomic(
            returned.clone(),
            proof(&returned),
        )
        .unwrap();
        ready.materialize_modeled_projection(None).unwrap();
        assert_eq!(ready.serialized_payload()["id"], "r1");
    }

    #[test]
    fn binding_rejects_each_inconsistent_contract() {
        type Edit = fn(&mut Vec<TypedCommandContract>);
        let succeeded = || typed_command::<Input, Succeeded<Payload>>("todo.check").into_contract();
        let cases: Vec<(&str, Edit, &str)> = vec![
            (
                "empty command id",
                |c| c[0].name = " ".into(),
                "typed command id must not be empty",
            ),
            (
                "duplicate command id",
                |c| {
                    let copy = c[0].clone();
                    c.push(copy);
                },
                "duplicate typed command declaration",
            ),
            (
                "output metadata for another type",
                |c| c[0].output.type_id = Some(TypeId::of::<Input>()),
                "output command metadata is missing or has a different Rust TypeId",
            ),
            (
                "eventual without confirmations",
                |c| c[0].consistency = CommandConsistency::Eventual,
                "must declare at least one expected projector confirmation",
            ),
            (
                "atomic without a retained model",
                |c| {
                    c[1].projected_model = None;
                    c.remove(0);
                },
                "missing its compiler-retained relational model",
            ),
            (
                "succeeded with a retained model",
                |c| {
                    c[0].projected_model = c[1].projected_model.clone();
                    c.truncate(1);
                },
                "cannot carry direct projection metadata",
            ),
            (
                "retained model for another output type",
                |c| {
                    c[1].projected_model.as_mut().unwrap().output_type_id = TypeId::of::<Payload>();
                    c.remove(0);
                },
                "retained model has a different Rust output type",
            ),
            (
                "retained model named differently",
                |c| {
                    c[1].projected_model.as_mut().unwrap().model = "Other".into();
                    c.remove(0);
                },
                "retained model `Other` differs from output `ProjectedRow`",
            ),
            (
                "direct target for another output type",
                |c| {
                    c[1].direct_projection.as_mut().unwrap().output_type_id =
                        TypeId::of::<Payload>();
                    c.remove(0);
                },
                "direct target has a different Rust output type",
            ),
            (
                "direct target named differently",
                |c| {
                    c[1].direct_projection.as_mut().unwrap().model = "Other".into();
                    c.remove(0);
                },
                "direct target model `Other` differs from output `ProjectedRow`",
            ),
        ];
        let mut atomic = atomic_contract();
        atomic.direct_projection = Some(direct_target(&atomic));
        assert!(
            TypedServiceCommandBinding::from_contracts("rows", &[succeeded(), atomic.clone()])
                .is_ok(),
            "the unedited contracts bind"
        );
        for (name, edit, expected) in cases {
            let mut contracts = vec![succeeded(), atomic.clone()];
            edit(&mut contracts);
            let error = TypedServiceCommandBinding::from_contracts("rows", &contracts).unwrap_err();
            assert!(error.contains(expected), "{name}: {error}");
        }
        assert!(
            TypedServiceCommandBinding::from_contracts(" ", &[succeeded()])
                .unwrap_err()
                .contains("non-empty service ID")
        );
    }
}

mod pure_reduce_validation {
    use super::*;

    fn hand() -> CommandProjectionPureReduce {
        CommandProjectionPureReduce::client_module("todo::rename", "pures/todo", "rename", "Todo")
            .key_input("id", ["todoId"])
            .arg_input("title", ["title"])
            .assign(["title", "title"])
    }

    fn validate(
        reduces: Vec<CommandProjectionPureReduce>,
    ) -> Result<CommandProjectionEvents, String> {
        let mut command = typed_command::<Input, Succeeded<Payload>>("todo.rename");
        for reduce in reduces {
            command = command.preview_reduce_known_record(reduce);
        }
        let mut projections = command.into_contract().projections;
        projections
            .canonicalize_and_validate("todo.rename")
            .map(|()| projections)
    }

    fn arg(source: CommandProjectionPreviewSource) -> CommandProjectionPureArg {
        CommandProjectionPureArg {
            name: "extra".into(),
            source,
        }
    }

    #[test]
    fn a_hand_module_reduce_is_canonicalized() {
        let reduce = hand().arg_input("b_arg", ["b"]).arg_input("a_arg", ["a"]);
        let projections = validate(vec![reduce]).unwrap();
        let reduce = &projections.pure_reduces[0];
        assert_eq!(
            reduce
                .args
                .iter()
                .map(|arg| arg.name.as_str())
                .collect::<Vec<_>>(),
            ["a_arg", "b_arg", "title"]
        );
        assert_eq!(reduce.assign, ["title"]);
    }

    #[test]
    fn each_malformed_reduce_is_rejected() {
        type Edit = fn(&mut CommandProjectionPureReduce);
        let cases: Vec<(&str, Edit, &str)> = vec![
            (
                "empty fn name",
                |r| r.fn_name = " ".into(),
                "requires non-empty fn and model",
            ),
            (
                "empty model",
                |r| r.model = String::new(),
                "requires non-empty fn and model",
            ),
            (
                "neither a hand module nor wasm",
                |r| {
                    r.client_module.clear();
                    r.client_export.clear();
                },
                "must declare either client_module+client_export or wasm_package+wasm_export",
            ),
            (
                "both a hand module and wasm",
                |r| r.wasm_package = "wasm/todo".into(),
                "must declare either client_module+client_export or wasm_package+wasm_export",
            ),
            (
                "hand module without an export",
                |r| r.client_export.clear(),
                "client module requires non-empty client_module and client_export",
            ),
            (
                "wasm package without an export",
                |r| {
                    r.client_module.clear();
                    r.client_export.clear();
                    r.wasm_package = "wasm/todo".into();
                },
                "wasm package requires non-empty wasm_package and wasm_export",
            ),
            (
                "no key field",
                |r| r.key.clear(),
                "requires at least one key field",
            ),
            (
                "no assigned field",
                |r| r.assign.clear(),
                "requires at least one assign field",
            ),
            (
                "empty input path segment",
                |r| {
                    r.args
                        .push(arg(CommandProjectionPreviewSource::input(["a", " "])))
                },
                "pure reduce path must contain only non-empty segments",
            ),
            (
                "trusted preset without a codec",
                |r| {
                    r.args
                        .push(arg(CommandProjectionPreviewSource::TrustedPreset {
                            name: "tenant".into(),
                            codec: String::new(),
                        }))
                },
                "pure reduce trusted preset name and codec must not be empty",
            ),
            (
                "server-only source",
                |r| r.args.push(arg(CommandProjectionPreviewSource::ServerOnly)),
                "arg `extra` uses unsupported source ServerOnly",
            ),
            (
                "a typed declaration error",
                |r| r.declaration_errors.push("row type mismatch".into()),
                "pure reduce is invalid: row type mismatch",
            ),
        ];
        for (name, edit, expected) in cases {
            let mut reduce = hand();
            edit(&mut reduce);
            let error = validate(vec![reduce]).unwrap_err();
            assert!(error.contains(expected), "{name}: {error}");
        }
    }

    #[test]
    fn two_reduces_may_not_share_a_fn_name() {
        let error = validate(vec![hand(), hand()]).unwrap_err();
        assert!(error.contains("repeats pure reduce fn name"), "{error}");
        let mut other = hand();
        other.fn_name = "todo::retitle".into();
        assert!(validate(vec![hand(), other]).is_ok());
    }
}
