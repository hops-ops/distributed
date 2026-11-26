use distributed::{CommitBuilderExt, TransactionalCommit};

use crate::bomberman::domain::game_map::GameMap;
use crate::bomberman::error::GameError;

pub async fn create_game<R>(repo: &R, game_id: &str, ascii_map: &str) -> Result<GameMap, GameError>
where
    R: TransactionalCommit,
{
    let (width, height, tiles, spawn_points) = GameMap::from_ascii(ascii_map);

    let mut map = GameMap::default();
    map.create(game_id.into(), width, height, tiles, spawn_points)?;
    repo.aggregate(&mut map).commit().await?;

    Ok(map)
}
