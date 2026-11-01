use distributed::command;

pub struct Hero;

#[command("hero.equip", shard = hero_id, load = required, outcome = succeeded)]
pub fn equip(actor: &str) -> Result<(), String> {
    let _ = actor;
    Ok(())
}

fn main() {}
