use distributed_game::FactCommand;

#[derive(serde::Serialize, FactCommand)]
#[fact(command = "crawler.record_kill")]
struct RecordKill {
    mob: String,
    UserId: String,
    #[serde(rename = "owner-id")]
    who: String,
    ACCOUNT_ID: String,
    #[serde(rename = "Experience")]
    gained: u32,
}

fn main() {}
