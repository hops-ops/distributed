use distributed_game::TicketExtension;

#[derive(serde::Serialize, serde::Deserialize)]
struct Extra {
    team: u8,
}

#[derive(serde::Serialize, serde::Deserialize, TicketExtension)]
struct MatchClaims {
    match_id: String,
    #[serde(flatten)]
    extra: Extra,
}

fn main() {}
