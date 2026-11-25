use std::collections::BTreeMap;

use super::command_manifest::validate_command_manifest;
use distributed::command::InputDefaultGenerator;

use super::manifest::{
    hash_bytes, ClientCommand, ClientCommandExtensionSlots, ClientCommandShape, ClientField,
    ClientFilterField, ClientFilterInput, ClientKeyField, ClientModel, ClientProjector,
    ClientProtocolOperation, ClientProtocolOperations, ClientRowPolicy,
    ClientTrustedPresetDescriptor, ClientTypeDef, ClientTypeField, CommandConsistency,
    CommandConsistencyExtension, CommandInputDefault, CommandInputDefaultsExtension,
    ModelNormalization,
};

fn field(
    name: &str,
    type_name: &str,
    codec: Option<&str>,
    nested: Option<ClientTypeDef>,
) -> ClientTypeField {
    ClientTypeField {
        name: name.into(),
        type_name: type_name.into(),
        nullable: false,
        list: false,
        item_nullable: false,
        codec: codec.map(str::to_string),
        nested: nested.map(Box::new),
    }
}

fn model() -> ClientModel {
    ClientModel {
        id: "Todo".into(),
        typename: "Todo".into(),
        source_table: "todos".into(),
        dependencies: vec!["todos".into()],
        normalization: ModelNormalization::Normalized {
            fields: vec![ClientKeyField {
                name: "id".into(),
                codec: "string".into(),
            }],
            encoding: "length_prefixed_v1".into(),
        },
        fields: vec![
            ClientField {
                name: "id".into(),
                scalar: "ID".into(),
                codec: "string".into(),
                nullable: false,
                json_type: None,
            },
            ClientField {
                name: "payload".into(),
                scalar: "Bytea".into(),
                codec: "base64".into(),
                nullable: false,
                json_type: None,
            },
            ClientField {
                name: "title".into(),
                scalar: "String".into(),
                codec: "string".into(),
                nullable: false,
                json_type: None,
            },
        ],
        relationships: Vec::new(),
        filter_input: ClientFilterInput {
            type_name: "todos_bool_exp".into(),
            fields: ["id", "payload", "title"]
                .into_iter()
                .map(|name| ClientFilterField {
                    name: name.into(),
                    operators: vec!["_eq".into()],
                })
                .collect(),
            relationships: Vec::new(),
        },
        row_policy: ClientRowPolicy::Unrestricted,
        record_revisions: true,
        tombstones: true,
    }
}

fn codecs() -> BTreeMap<String, String> {
    [
        ("Bytea".into(), "base64".into()),
        ("ID".into(), "string".into()),
        ("JSON".into(), "json".into()),
        ("String".into(), "string".into()),
    ]
    .into_iter()
    .collect()
}

fn input() -> ClientTypeDef {
    ClientTypeDef {
        name: "CreateTodoInput".into(),
        fields: vec![
            field("id", "ID", Some("string"), None),
            field(
                "metadata",
                "TodoMetadataInput",
                None,
                Some(ClientTypeDef {
                    name: "TodoMetadataInput".into(),
                    fields: vec![field("title", "String", Some("string"), None)],
                }),
            ),
        ],
    }
}

fn output() -> ClientTypeDef {
    ClientTypeDef {
        name: "CreateTodoPayload".into(),
        fields: vec![
            field("id", "ID", Some("string"), None),
            field(
                "todo",
                "TodoResult",
                None,
                Some(ClientTypeDef {
                    name: "TodoResult".into(),
                    fields: vec![
                        field("id", "ID", Some("string"), None),
                        field("title", "String", Some("string"), None),
                    ],
                }),
            ),
        ],
    }
}

fn command() -> ClientCommand {
    let operation = "mutation Client_createTodo($commandId: ID!, $input: CreateTodoInput!) { createTodo(commandId: $commandId, input: $input) { id todo { id title } } }";
    ClientCommand {
        version: 1,
        name: "CreateTodo".into(),
        mutation_field: "createTodo".into(),
        grants: vec!["user".into()],
        input: ClientCommandShape::Object {
            definition: input(),
        },
        output: ClientCommandShape::Object {
            definition: output(),
        },
        operation: operation.into(),
        operation_hash: hash_bytes(operation.as_bytes()),
        extensions: ClientCommandExtensionSlots {
            version: 2,
            consistency: CommandConsistencyExtension {
                version: 1,
                kind: CommandConsistency::Eventual,
            },
            direct_projection: None,
            input_defaults: Some(CommandInputDefaultsExtension {
                version: 1,
                defaults: vec![CommandInputDefault {
                    path: vec!["id".into()],
                    generator: InputDefaultGenerator::UuidV7,
                }],
            }),
            projection: None,
            trusted_presets: Vec::new(),
        },
    }
}

fn projector() -> ClientProjector {
    ClientProjector {
        version: 1,
        name: "todos".into(),
        facts: vec!["TodoCreated".into()],
        models: vec!["Todo".into()],
        dependencies: vec!["todos".into()],
    }
}

fn protocol() -> ClientProtocolOperations {
    let operation = "query Distributed_CommandStatus($commandId: ID!) { commandStatus(commandId: $commandId) { state } }";
    ClientProtocolOperations {
        version: 1,
        command_status: Some(ClientProtocolOperation {
            name: "Distributed_CommandStatus".into(),
            operation: operation.into(),
            operation_hash: hash_bytes(operation.as_bytes()),
        }),
    }
}

fn validate(
    command: &ClientCommand,
) -> Result<super::command_manifest::CommandManifestValidation, super::ClientCompileError> {
    validate_commands(std::slice::from_ref(command))
}

fn validate_commands(
    commands: &[ClientCommand],
) -> Result<super::command_manifest::CommandManifestValidation, super::ClientCompileError> {
    validate_command_manifest(
        commands,
        &[("Todo".into(), model())].into_iter().collect(),
        &BTreeMap::new(),
        &codecs(),
        &[projector()],
        true,
        &protocol(),
    )
}

fn command_with_identity(name: &str, mutation_field: &str) -> ClientCommand {
    let mut command = command();
    command.name = name.into();
    command.mutation_field = mutation_field.into();
    command.operation = format!(
        "mutation Client_{mutation_field}($commandId: ID!, $input: CreateTodoInput!) {{ {mutation_field}(commandId: $commandId, input: $input) {{ id todo {{ id title }} }} }}"
    );
    command.operation_hash = hash_bytes(command.operation.as_bytes());
    command
}

#[test]
fn validates_recursive_shapes_under_command_authority_v2() {
    let report = validate(&command()).expect("valid typed command");
    assert!(report.commands_requiring_revalidation.is_empty());
}

#[test]
fn requires_byte_exact_canonical_operation_and_recursive_codecs() {
    let mut drifted = command();
    drifted.operation.push(' ');
    drifted.operation_hash = hash_bytes(drifted.operation.as_bytes());
    let error = validate(&drifted).expect_err("whitespace drift must fail");
    assert_eq!(error.code, "client.manifest.command_operation");

    let mut wrong_codec = command();
    let ClientCommandShape::Object { definition } = &mut wrong_codec.input else {
        unreachable!()
    };
    definition.fields[1].nested.as_mut().expect("nested").fields[0].codec = Some("json".into());
    let error = validate(&wrong_codec).expect_err("recursive codec drift must fail");
    assert_eq!(error.code, "client.manifest.command_type_codec");
}

#[test]
fn unreferenced_trusted_preset_descriptors_fail_closed() {
    let mut trusted = command();
    trusted
        .extensions
        .trusted_presets
        .push(ClientTrustedPresetDescriptor {
            name: "subject".into(),
            codec: "string".into(),
        });
    let error = validate(&trusted).expect_err("unreferenced descriptor must fail closed");
    assert_eq!(error.code, "client.manifest.trusted_preset_inventory");
}

#[test]
fn command_extensions_v1_has_no_decoder() {
    let mut old_version = command();
    old_version.extensions.version = 1;
    let error = validate(&old_version).expect_err("extensions v1 must fail closed");
    assert_eq!(error.code, "client.manifest.command_extensions");
}

#[test]
fn rejects_empty_outputs_and_ambiguous_command_type_namespaces() {
    let mut empty = command();
    empty.output = ClientCommandShape::None;
    let error = validate(&empty).expect_err("GraphQL mutation output cannot be empty");
    assert_eq!(error.code, "client.manifest.command_output");

    let mut overlaps_input = command();
    overlaps_input.output = ClientCommandShape::Object {
        definition: input(),
    };
    let error = validate(&overlaps_input).expect_err("input/output type IDs cannot overlap");
    assert_eq!(error.code, "client.manifest.command_type_reference");

    let first = command();
    let second = command_with_identity("CreateTodoAgain", "createTodoAgain");
    validate_commands(&[first.clone(), second.clone()])
        .expect("identical output definitions may be shared across commands");

    let mut conflicting = second;
    let ClientCommandShape::Object { definition } = &mut conflicting.output else {
        unreachable!()
    };
    definition.fields[0].nullable = true;
    let error = validate_commands(&[first, conflicting])
        .expect_err("cross-command type definitions must be globally unambiguous");
    assert_eq!(error.code, "client.manifest.command_type_reference");

    let mut model_collision = command();
    let ClientCommandShape::Object { definition } = &mut model_collision.input else {
        unreachable!()
    };
    definition.name = "Todo".into();
    let error = validate(&model_collision)
        .expect_err("command input types cannot shadow an authorized model object");
    assert_eq!(error.code, "client.manifest.command_type_namespace");

    let mut scalar_collision = command();
    let ClientCommandShape::Object { definition } = &mut scalar_collision.input else {
        unreachable!()
    };
    definition.name = "String".into();
    let error = validate(&scalar_collision)
        .expect_err("command input types cannot shadow built-in scalar types");
    assert_eq!(error.code, "client.manifest.command_type_namespace");

    let mut non_projected_model_output = command();
    let ClientCommandShape::Object { definition } = &mut non_projected_model_output.output else {
        unreachable!()
    };
    definition.name = "Todo".into();
    let error = validate(&non_projected_model_output)
        .expect_err("only an exact Atomic<T> output may reuse its model object type");
    assert_eq!(error.code, "client.manifest.command_type_namespace");
}

#[test]
fn accepts_shapes_beyond_removed_compiler_only_size_limits() {
    let mut deep = command();
    let mut nested = ClientTypeDef {
        name: "Deep40".into(),
        fields: vec![field("value", "String", Some("string"), None)],
    };
    for depth in (0..40).rev() {
        let type_name = format!("Deep{depth}");
        let child_name = nested.name.clone();
        nested = ClientTypeDef {
            name: type_name,
            fields: vec![field("child", &child_name, None, Some(nested))],
        };
    }
    let ClientCommandShape::Object { definition } = &mut deep.input else {
        unreachable!()
    };
    let nested_name = nested.name.clone();
    definition
        .fields
        .push(field("zdeep", &nested_name, None, Some(nested)));
    validate(&deep).expect("Surface has no 32-level command type limit");

    let mut wide = command();
    let ClientCommandShape::Object { definition } = &mut wide.input else {
        unreachable!()
    };
    let mut fields = (0..4_097)
        .map(|index| field(&format!("a{index:04}"), "String", Some("string"), None))
        .collect::<Vec<_>>();
    fields.append(&mut definition.fields);
    definition.fields = fields;
    validate(&wide).expect("Surface has no 4,096-field command type limit");
}

#[test]
fn query_only_protocol_has_no_command_status_or_causal_capability() {
    let report = validate_command_manifest(
        &[],
        &BTreeMap::new(),
        &BTreeMap::new(),
        &codecs(),
        &[],
        false,
        &ClientProtocolOperations {
            version: 1,
            command_status: None,
        },
    )
    .expect("query-only contract");
    assert!(report.commands_requiring_revalidation.is_empty());
}
