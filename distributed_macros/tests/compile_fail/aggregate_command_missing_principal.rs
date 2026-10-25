use distributed::{sourced, Entity};
use serde::Deserialize;

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct EquipInput {
    pub hero_id: String,
    pub owner: String,
}

#[derive(Default)]
pub struct Hero {
    entity: Entity,
}

#[sourced(entity, aggregate_type = "hero")]
impl Hero {
    #[command("hero.equip", shard = hero_id, load = required, outcome = succeeded)]
    pub fn equip(&mut self, input: EquipInput) -> Result<(), String> {
        let _ = input.owner;
        Ok(())
    }
}

fn main() {
    let _ = Hero::default().entity;
}
