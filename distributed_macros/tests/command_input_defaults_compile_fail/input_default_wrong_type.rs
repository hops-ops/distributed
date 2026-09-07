use distributed::{command_input_defaults, CommandInput};

#[derive(CommandInput)]
struct Input {
    count: i64,
}

fn main() {
    let _ = command_input_defaults! {
        input: Input;
        default input.count = uuid_v7();
    };
}
