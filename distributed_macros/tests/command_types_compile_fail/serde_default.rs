use distributed::CommandInput;

#[derive(CommandInput)]
struct DefaultedInput {
    #[serde(default)]
    value: String,
}

fn main() {}
