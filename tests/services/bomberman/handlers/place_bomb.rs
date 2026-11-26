use distributed::{CommitBuilderExt, GetStream, TransactionalCommit};

use super::shared::get_aggregate;
use crate::bomberman::domain::bomb::Bomb;
use crate::bomberman::domain::game_map::GameMap;
use crate::bomberman::domain::player::Player;
use crate::bomberman::error::GameError;

pub async fn place_bomb<R>(repo: &R, player_id: &str, game_id: &str) -> Result<(), GameError>
where
    R: GetStream + TransactionalCommit,
{
    // The game must exist; a bomb is the player's, not the map's.
    get_aggregate::<R, GameMap>(repo, game_id)
        .await?
        .ok_or(GameError::GameNotFound)?;

    let mut player: Player = get_aggregate::<R, Player>(repo, &format!("player:{}", player_id))
        .await?
        .ok_or(GameError::PlayerNotFound(player_id.to_string()))?;

    if !player.alive {
        return Err(GameError::PlayerDead(player_id.to_string()));
    }
    if !player.can_place_bomb() {
        return Err(GameError::NoBombsAvailable);
    }

    let bomb_num = player.bombs_placed + 1;

    player.place_bomb()?;

    let mut bomb = Bomb::default();
    bomb.create(
        format!("bomb:{}:{}", player_id, bomb_num),
        player_id.into(),
        player.x,
        player.y,
        player.blast_radius,
    )?;

    repo.aggregate(&mut player)
        .aggregate(&mut bomb)
        .commit()
        .await?;

    Ok(())
}
