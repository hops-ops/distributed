//! GraphQL command type metadata remains exact for typed causal commands.

#![cfg(feature = "graphql")]

use distributed::command::{CommandTypeDef, CommandTypeField};

#[test]
fn graphql_type_def_mapping_golden() {
    let input = CommandTypeDef::new(
        "CreateItemInput",
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
                name: "tags".into(),
                type_name: "String".into(),
                nullable: true,
                list: true,
                item_nullable: false,
                nested: None,
            },
        ],
    );
    assert_eq!(input.name, "CreateItemInput");
    assert_eq!(input.fields.len(), 2);
    assert!(!input.fields[0].nullable);
    assert!(input.fields[1].list);
}

#[derive(distributed::CommandInput)]
#[allow(dead_code)]
struct DerivedInput {
    id: String,
    count: i64,
    tags: Option<Vec<String>>,
}

#[derive(distributed::CommandOutput)]
#[allow(dead_code)]
struct DerivedOutput {
    ok: bool,
    id: String,
}

#[derive(distributed::CommandInput)]
#[allow(dead_code)]
struct UnsignedInput {
    tiny: u8,
    short: Option<u16>,
    revisions: Vec<Option<u32>>,
    revision: u64,
    signed: i64,
}

#[test]
fn unsigned_command_derive_preserves_range_and_portable_contract() {
    use distributed::command::{CommandInputType, CommandUnsignedInteger as Unsigned};
    let definition = UnsignedInput::command_type();
    assert_eq!(
        definition
            .fields
            .iter()
            .map(|field| field.unsigned_integer)
            .collect::<Vec<_>>(),
        vec![
            Some(Unsigned::U8),
            Some(Unsigned::U16),
            Some(Unsigned::U32),
            Some(Unsigned::U64),
            None
        ]
    );
    assert!(definition
        .fields
        .iter()
        .all(|field| field.type_name == "BigInt"));
    assert!(definition.fields[2].list && definition.fields[2].item_nullable);
    let portable = distributed::application::CommandTypeSpec::from(&definition);
    let encoded = serde_json::to_value(&portable).unwrap();
    assert_eq!(encoded["fields"][3]["unsigned_integer"], "u64");
    assert!(encoded["fields"][4].get("unsigned_integer").is_none());
}

#[derive(distributed::CommandInput, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct ScalarMatrixInput {
    #[serde(rename = "wireRequired")]
    required: String,
    optional: Option<String>,
    required_items: Vec<String>,
    optional_items: Option<Vec<String>>,
    nullable_items: Vec<Option<String>>,
    optional_nullable_items: Option<Vec<Option<String>>>,
}

#[derive(distributed::CommandOutput, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct ScalarMatrixOutput {
    #[serde(rename = "wireRequired")]
    required: String,
    optional: Option<String>,
    required_items: Vec<String>,
    optional_items: Option<Vec<String>>,
    nullable_items: Vec<Option<String>>,
    optional_nullable_items: Option<Vec<Option<String>>>,
}

#[derive(distributed::CommandInput)]
#[serde(rename_all(deserialize = "camelCase", serialize = "SCREAMING_SNAKE_CASE"))]
#[allow(dead_code)]
struct DirectionalInputNames {
    regular_field: String,
    #[serde(rename(deserialize = "inputID", serialize = "OUTPUT_ID"))]
    custom_id: String,
}

#[derive(distributed::CommandOutput)]
#[serde(rename_all(deserialize = "camelCase", serialize = "SCREAMING_SNAKE_CASE"))]
#[allow(dead_code)]
struct DirectionalOutputNames {
    regular_field: String,
    #[serde(rename(deserialize = "inputID", serialize = "OUTPUT_ID"))]
    custom_id: String,
}

#[test]
fn derive_mapping_golden() {
    use distributed::command::{CommandInputType, CommandOutputType};

    let input = DerivedInput::command_type();
    assert_eq!(input.name, "DerivedInput");
    assert_eq!(input.fields.len(), 3);
    assert_eq!(input.fields[0].type_name, "String");
    assert!(!input.fields[0].nullable);
    assert_eq!(input.fields[1].type_name, "BigInt");
    assert!(input.fields[2].list);
    assert!(input.fields[2].nullable);

    let output = DerivedOutput::command_type();
    assert_eq!(output.name, "DerivedOutput");
    assert_eq!(output.fields[0].type_name, "Boolean");
    assert_eq!(output.fields[1].type_name, "String");
}

#[test]
fn derive_preserves_outer_and_item_nullability_and_serde_names() {
    use distributed::command::{CommandInputType, CommandOutputType};

    let expected = [
        ("wireRequired", false, false, false),
        ("optional", true, false, false),
        ("requiredItems", false, true, false),
        ("optionalItems", true, true, false),
        ("nullableItems", false, true, true),
        ("optionalNullableItems", true, true, true),
    ];
    for definition in [
        ScalarMatrixInput::command_type(),
        ScalarMatrixOutput::command_type(),
    ] {
        for (name, nullable, list, item_nullable) in expected {
            let field = definition
                .fields
                .iter()
                .find(|field| field.name == name)
                .unwrap_or_else(|| panic!("missing {name} on {}", definition.name));
            assert_eq!(field.type_name, "String", "{name}");
            assert_eq!(field.nullable, nullable, "{name}");
            assert_eq!(field.list, list, "{name}");
            assert_eq!(field.item_nullable, item_nullable, "{name}");
        }
    }

    let input_names: Vec<_> = DirectionalInputNames::command_type()
        .fields
        .into_iter()
        .map(|field| field.name)
        .collect();
    assert_eq!(input_names, ["regularField", "inputID"]);

    let output_names: Vec<_> = DirectionalOutputNames::command_type()
        .fields
        .into_iter()
        .map(|field| field.name)
        .collect();
    assert_eq!(output_names, ["REGULAR_FIELD", "OUTPUT_ID"]);
}
