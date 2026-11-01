use distributed_game::FactCommand;

#[derive(serde::Serialize, FactCommand)]
#[fact(command = "crawler.record_kill")]
struct RecordKill {
    mob: String,
    #[serde(rename = "XP")]
    count: u32,
}

fn main() {}
