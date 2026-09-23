use distributed::CommandInput;

#[derive(CommandInput)]
#[serde(default = "default_input")]
struct ContainerDefaultInput {
    value: String,
}

fn main() {}
