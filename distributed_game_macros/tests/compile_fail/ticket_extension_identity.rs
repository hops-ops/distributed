use distributed_game::TicketExtension;

// Identity only ever comes from the verified `sub`; amounts from handlers.
#[derive(serde::Serialize, serde::Deserialize, TicketExtension)]
struct MatchClaims {
    match_id: String,
    userId: String,
    #[serde(rename = "Gold")]
    purse: u32,
}

fn main() {}
