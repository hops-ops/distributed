use distributed_game::FactCommand;

#[derive(serde::Serialize, FactCommand)]
#[fact(command = "crawler.record_kill")]
struct RecordKill {
    mob: String,
    #[serde(rename = "XP")]
    amount: u32,
}

fn main() {}
