use distributed::{sourced, Entity};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct EquipInput {
    pub hero_id: String,
    pub actor: String,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct Equipped {
    pub hero_id: String,
}

#[derive(Default)]
pub struct Hero {
    entity: Entity,
}

#[sourced(entity, aggregate_type = "hero")]
impl Hero {
    #[command("hero.equip", shard = hero_id, load = required, outcome = succeeded)]
    pub fn equip(&mut self, #[principal] actor: &str, input: EquipInput) -> Result<Equipped, String> {
        let _ = (actor, input.actor);
        self.touched().map_err(|e| e.to_string())?;
        Ok(Equipped { hero_id: input.hero_id })
    }

    #[event("hero.touched")]
    fn touched(&mut self) {}
}

fn main() {
    let _ = Hero::default().entity;
}
