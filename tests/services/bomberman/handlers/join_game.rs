use distributed::{GetStream, TransactionalCommit};

use super::shared::{commit_aggregates, get_aggregate, stream};
use crate::bomberman::domain::game_map::GameMap;
use crate::bomberman::domain::player::Player;
use crate::bomberman::error::GameError;

pub async fn join_game<R>(
    repo: &R,
    player_id: &str,
    name: &str,
    game_id: &str,
    spawn_index: usize,
) -> Result<(), GameError>
where
    R: GetStream + TransactionalCommit,
{
    let mut map: GameMap = get_aggregate::<R, GameMap>(repo, game_id)
        .await?
        .ok_or(GameError::GameNotFound)?;

    let (sx, sy) = map
        .spawn_points
        .get(spawn_index)
        .copied()
        .ok_or(GameError::InvalidSpawnIndex(spawn_index))?;

    let mut player = Player::default();
    player.join(format!("player:{}", player_id), name.into(), sx, sy)?;
    map.add_player(player.entity.id().to_string())?;

    commit_aggregates(repo, vec![stream(&mut map)?, stream(&mut player)?]).await?;

    Ok(())
}
