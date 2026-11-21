use distributed_game::TicketExtension;

#[derive(serde::Serialize, serde::Deserialize, TicketExtension)]
#[serde(rename_all = "camelCase")]
struct MatchClaims {
    match_id: String,
}

fn main() {}
