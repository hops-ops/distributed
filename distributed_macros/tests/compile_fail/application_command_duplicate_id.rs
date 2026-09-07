use std::sync::LazyLock;

use distributed::application::{CommandDefinition, CommandSpec, CommandTypeField, CommandTypeSpec};
use distributed::command::CommandConsistency;

fn spec() -> CommandSpec {
    CommandSpec::try_new(
        "todo.duplicate",
        "todo_duplicate",
        CommandTypeSpec {
            name: "DuplicateInput".into(),
            fields: vec![CommandTypeField {
                name: "id".into(),
                type_name: "String".into(),
                nullable: false,
                list: false,
                item_nullable: false,
                nested: None,
            }],
        },
        CommandTypeSpec {
            name: "DuplicateOutput".into(),
            fields: Vec::new(),
        },
        CommandConsistency::Eventual,
    )
    .unwrap()
}

static FIRST_DEFINITION: LazyLock<CommandDefinition> =
    LazyLock::new(|| CommandDefinition::contract(spec()));
static SECOND_DEFINITION: LazyLock<CommandDefinition> =
    LazyLock::new(|| CommandDefinition::contract(spec()));

const FIRST_COMMAND_ID: &str = "todo.duplicate";
const SECOND_COMMAND_ID: &str = "todo.duplicate";

distributed::module! {
    pub DUPLICATE_MODULE {
        id: "duplicates",
        commands: [FIRST_DEFINITION, SECOND_DEFINITION],
    }
}

fn main() {}
