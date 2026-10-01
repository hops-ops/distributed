//! Compile-pass checks for command_input_defaults!.

use distributed::command::{typed_command, Succeeded};
use distributed::{command_input_defaults, CommandInput};
use serde::Deserialize;

#[derive(Clone, Deserialize, CommandInput)]
#[allow(dead_code)]
struct PlanInput {
    id: String,
    title: String,
}

#[derive(Clone, serde::Serialize, distributed::CommandOutput)]
struct PlanOutput {
    id: String,
}

#[test]
fn generated_input_defaults_compile() {
    let _defaults = command_input_defaults! {
        input: PlanInput;
        default input.id = uuid_v7();
    };
    let _ =
        typed_command::<PlanInput, Succeeded<PlanOutput>>("plan.create").input_defaults(_defaults);
}
