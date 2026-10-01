use distributed::CommandOutput;

#[derive(CommandOutput)]
struct CustomOutput {
    #[serde(with = "wire_value")]
    value: String,
}

fn main() {}
