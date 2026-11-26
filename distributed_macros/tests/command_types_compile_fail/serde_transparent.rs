use distributed::command::CommandInput;

#[derive(CommandInput)]
#[serde(transparent)]
struct TransparentInput {
    value: String,
}

fn main() {}
