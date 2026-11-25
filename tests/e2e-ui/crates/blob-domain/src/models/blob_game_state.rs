use distributed::DomainState;
use serde::{Deserialize, Serialize};

use super::BlobGame;
use crate::core::{Direction, Tile};

/// Stable public post-transition state carried by Blob domain events.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, DomainState)]
#[domain_state(version = 2)]
pub struct BlobGameState {
    pub game_id: String,
    pub owner_id: String,
    pub score: i64,
    pub player_dead: bool,
    pub current_level: i64,
    pub current_level_completed: bool,
    /// The current level's board (empty before the first level).
    pub map: Vec<Vec<Tile>>,
    /// Directions the player can take without leaving the board.
    pub moves: Vec<Direction>,
    /// `active` | `dead` | `level_complete`
    pub status: String,
}

impl BlobGameState {
    /// Capture the aggregate's current public state.
    pub fn from_game(game: &BlobGame) -> Self {
        Self::from(game)
    }
}

impl From<&BlobGame> for BlobGameState {
    fn from(game: &BlobGame) -> Self {
        game.state()
    }
}
