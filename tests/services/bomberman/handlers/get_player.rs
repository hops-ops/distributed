use distributed::GetStream;

use super::shared::get_aggregate;
use crate::bomberman::domain::player::Player;
use crate::bomberman::error::GameError;

pub async fn get_player<R: GetStream>(repo: &R, player_id: &str) -> Result<Player, GameError> {
    get_aggregate::<R, Player>(repo, &format!("player:{}", player_id))
        .await?
        .ok_or(GameError::PlayerNotFound(player_id.to_string()))
}
