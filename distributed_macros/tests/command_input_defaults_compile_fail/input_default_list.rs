use distributed::{command_input_defaults, CommandInput};

#[derive(CommandInput)]
struct Input {
    ids: Vec<String>,
}

fn main() {
    let _ = command_input_defaults! {
        input: Input;
        default input.ids = ulid();
    };
}
