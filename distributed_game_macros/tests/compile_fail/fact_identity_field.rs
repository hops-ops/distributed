use distributed_game::FactCommand;

#[derive(serde::Serialize, FactCommand)]
#[fact(command = "crawler.record_kill")]
struct RecordKill {
    mob: String,
    principal: String,
}

fn main() {}
