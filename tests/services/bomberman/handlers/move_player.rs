use distributed::{GetStream, TransactionalCommit};

use super::shared::{commit_aggregates, get_aggregate, stream};
use crate::bomberman::domain::game_map::GameMap;
use crate::bomberman::domain::player::Player;
use crate::bomberman::domain::types::Direction;
use crate::bomberman::error::GameError;

pub async fn move_player<R>(
    repo: &R,
    player_id: &str,
    direction: Direction,
    game_id: &str,
) -> Result<(), GameError>
where
    R: GetStream + TransactionalCommit,
{
    let mut map: GameMap = get_aggregate::<R, GameMap>(repo, game_id)
        .await?
        .ok_or(GameError::GameNotFound)?;

    let mut player: Player = get_aggregate::<R, Player>(repo, &format!("player:{}", player_id))
        .await?
        .ok_or(GameError::PlayerNotFound(player_id.to_string()))?;

    if !player.alive {
        return Err(GameError::PlayerDead(player_id.to_string()));
    }

    let (nx, ny) = direction.apply(player.x, player.y);

    if !map.is_in_bounds(nx, ny) {
        return Err(GameError::OutOfBounds(nx, ny));
    }
    if !map.is_passable(nx, ny) {
        return Err(GameError::NotPassable(nx, ny));
    }

    player.move_to(nx, ny)?;

    if let Some(power_up) = map.collect_power_up(nx, ny)? {
        player.apply_power_up(power_up)?;
    }

    commit_aggregates(repo, vec![stream(&mut map)?, stream(&mut player)?]).await?;

    Ok(())
}
