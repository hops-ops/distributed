use distributed::command::CommandInput;

#[derive(CommandInput)]
struct NestedLists {
    values: Option<Vec<Option<Vec<String>>>>,
}

fn main() {}
