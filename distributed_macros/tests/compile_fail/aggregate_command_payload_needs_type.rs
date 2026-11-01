use distributed::{sourced, Entity};
use serde::Deserialize;

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct EquipInput {
    pub hero_id: String,
}

#[derive(Default)]
pub struct Hero {
    entity: Entity,
}

fn payload(_hero: &Hero) -> u32 {
    0
}

#[sourced(entity, aggregate_type = "hero")]
impl Hero {
    #[command("hero.equip", shard = hero_id, load = required, outcome = eventual, payload = payload)]
    pub fn equip(&mut self, #[principal] actor: &str, input: EquipInput) -> Result<(), String> {
        let _ = (actor, input);
        Ok(())
    }
}

fn main() {
    let _ = (Hero::default().entity, payload);
}
