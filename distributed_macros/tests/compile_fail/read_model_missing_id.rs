use distributed::read_model::ReadModel;

// No field named `id` and no `#[readmodel(id)]` marker: the derive cannot
// know which field uniquely identifies the read model.
#[derive(ReadModel)]
struct CounterView {
    name: String,
    value: i32,
}

fn main() {}
