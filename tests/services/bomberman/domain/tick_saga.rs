use distributed::{sourced, Entity};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Detonation {
    pub bomb_id: String,
    pub owner: String,
    pub explosion_id: String,
}

#[derive(Default)]
pub struct TickSaga {
    pub entity: Entity,
    pub game_id: String,
    pub bombs_ticked: usize,
    pub detonations: Vec<Detonation>,
    pub blocks_destroyed: Vec<(i32, i32)>,
    pub players_killed: Vec<String>,
    pub chain_detonations: Vec<String>,
    pub explosions_dissipated: Vec<String>,
    pub game_over: bool,
    pub winner: Option<String>,
}

#[sourced(entity)]
impl TickSaga {
    #[event("started")]
    pub fn start(&mut self, saga_id: String, game_id: String, bombs_ticked: usize) {
        self.entity.set_id(&saga_id);
        self.game_id = game_id;
        self.bombs_ticked = bombs_ticked;
    }

    #[event("detonation_recorded")]
    pub fn record_detonation(&mut self, detonation: Detonation) {
        self.detonations.push(detonation);
    }

    #[event("damage_recorded")]
    pub fn record_damage(
        &mut self,
        blocks_destroyed: Vec<(i32, i32)>,
        players_killed: Vec<String>,
        chain_detonations: Vec<String>,
    ) {
        self.blocks_destroyed.extend(blocks_destroyed);
        self.players_killed.extend(players_killed);
        self.chain_detonations.extend(chain_detonations);
    }

    #[event("dissipation_recorded")]
    pub fn record_dissipation(&mut self, explosion_id: String) {
        self.explosions_dissipated.push(explosion_id);
    }

    #[event("completed")]
    pub fn complete(&mut self, game_over: bool, winner: Option<String>) {
        self.game_over = game_over;
        self.winner = winner;
    }
}
