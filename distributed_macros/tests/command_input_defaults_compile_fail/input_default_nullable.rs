use distributed::{command_input_defaults, CommandInput};
use serde::Deserialize;

#[derive(Deserialize, CommandInput)]
struct Input {
    id: Option<String>,
}

fn main() {
    let _ = command_input_defaults! {
        input: Input;
        default input.id = uuid_v7();
    };
}
