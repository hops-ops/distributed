#![allow(dead_code)]

// Derives in this package's integration targets resolve through the crate root.
pub use distributed::{command, graphql};

use distributed::application::{CommandDefinition, Module};
use distributed::command::{typed_command, CommandInputType, CommandOutputType, Succeeded};
use distributed::graphql::{build_surface, graphql_sdl_from_surface, SurfaceOptions};
use serde::{Deserialize, Serialize};

mod neutral {
    use super::*;
    use distributed::{CommandInput, CommandOutput};

    #[derive(Deserialize, CommandInput)]
    #[serde(rename_all = "camelCase")]
    pub struct ContractInput {
        record_id: String,
        count: i64,
        nested: Option<Vec<Option<NestedInput>>>,
    }
    #[derive(Deserialize, CommandInput)]
    pub struct NestedInput {
        enabled: bool,
    }
    #[derive(Serialize, CommandOutput)]
    pub struct ContractOutput {
        record_id: String,
        value: f64,
    }
}

fn artifact<I, O>() -> serde_json::Value
where
    I: CommandInputType + serde::de::DeserializeOwned + Send + 'static,
    O: CommandOutputType + Serialize + Send + Sync + 'static,
{
    let command = typed_command::<I, Succeeded<O>>("contract.test")
        .roles(["user"])
        .field_name("run_contract");
    let spec = command.spec().unwrap();
    let module = Module::new("contract")
        .command_definition(CommandDefinition::from_typed_command(command, None).unwrap())
        .build()
        .unwrap();
    let surface = build_surface(&[], &SurfaceOptions::sqlite())
        .unwrap()
        .with_module(&module)
        .unwrap();
    serde_json::json!({ "spec": spec, "sdl": graphql_sdl_from_surface(&surface).unwrap() })
}

#[test]
fn neutral_declarations_preserve_the_pre_extraction_artifact() {
    // Captured from d188010d before changing the command implementation.
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/core-command-contract-v1.json")).unwrap();
    assert_eq!(
        artifact::<neutral::ContractInput, neutral::ContractOutput>(),
        expected
    );
}

#[derive(Deserialize, distributed::CommandInput)]
#[serde(rename_all(deserialize = "kebab-case", serialize = "camelCase"))]
struct NonGraphqlInput {
    record_id: String,
}

#[test]
fn graphql_representability_is_checked_only_when_exposing_the_command() {
    let command = typed_command::<NonGraphqlInput, Succeeded<neutral::ContractOutput>>("core.only");
    assert_eq!(command.spec().unwrap().input.fields[0].name, "record-id");
    let module = Module::new("core-only")
        .command_definition(CommandDefinition::from_typed_command(command, None).unwrap())
        .build()
        .unwrap();
    let result = build_surface(&[], &SurfaceOptions::sqlite())
        .unwrap()
        .with_module(&module);
    assert!(result.unwrap_err().contains("record-id"));
}

#[derive(Deserialize, distributed::CommandInput)]
#[serde(rename_all(deserialize = "camelCase", serialize = "SCREAMING_SNAKE_CASE"))]
struct DirectionalInput {
    record_id: String,
    #[serde(rename(deserialize = "inputID", serialize = "OUTPUT_ID"))]
    alternate_id: String,
    values: Option<Vec<Option<String>>>,
}

#[derive(Serialize, distributed::CommandOutput)]
#[serde(rename_all(deserialize = "camelCase", serialize = "SCREAMING_SNAKE_CASE"))]
struct DirectionalOutput {
    record_id: String,
    #[serde(rename(deserialize = "inputID", serialize = "OUTPUT_ID"))]
    alternate_id: String,
    values: Option<Vec<Option<String>>>,
}

#[test]
fn neutral_shapes_follow_serde_direction_and_preserve_item_nullability() {
    for (definition, names) in [
        (
            DirectionalInput::command_type(),
            ["recordId", "inputID", "values"],
        ),
        (
            DirectionalOutput::command_type(),
            ["RECORD_ID", "OUTPUT_ID", "VALUES"],
        ),
    ] {
        assert_eq!(
            definition
                .fields
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
            names
        );
        let list = &definition.fields[2];
        assert!(list.list && list.nullable && list.item_nullable);
    }
}
