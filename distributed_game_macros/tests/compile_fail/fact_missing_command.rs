use distributed_game::FactCommand;

#[derive(serde::Serialize, FactCommand)]
struct RecordKill {
    mob: String,
}

fn main() {}
