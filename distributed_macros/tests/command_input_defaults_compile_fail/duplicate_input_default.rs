use distributed::{command_input_defaults, CommandInput};

#[derive(CommandInput)]
struct Input {
    id: String,
}

fn main() {
    let _ = command_input_defaults! {
        input: Input;
        default input.id = uuid_v7();
        default input.id = ulid();
    };
}
