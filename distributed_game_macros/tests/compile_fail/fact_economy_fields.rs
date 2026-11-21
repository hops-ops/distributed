use distributed_game::FactCommand;

// Every economy name is denied, whatever its case or separators, and under
// a serde rename or alias too. Each one gets its own error.
#[derive(serde::Serialize, FactCommand)]
#[fact(command = "crawler.loot_found")]
struct LootFound {
    chest: String,
    amount: u32,
    Coins: u32,
    GOLD: u32,
    currency_: String,
    price: u32,
    cost: u32,
    bal_ance: u64,
    grant: u32,
    pay_out: u32,
    CREDIT: u32,
    debit: u32,
    #[serde(rename = "Pay-Out")]
    a: u32,
    #[serde(alias = "goldAmount", alias = "gold-")]
    b: u32,
}

fn main() {}
