use distributed::{ReadModelWritePlanCommitExt, TransactionalCommit};

use super::shared::board_write_plan;
use crate::bomberman::domain::game_map::GameMap;
use crate::bomberman::error::GameError;
use crate::bomberman::views::BoardView;

pub async fn create_game<R>(repo: &R, game_id: &str, ascii_map: &str) -> Result<GameMap, GameError>
where
    R: TransactionalCommit,
{
    let (width, height, tiles, spawn_points) = GameMap::from_ascii(ascii_map);

    let mut map = GameMap::default();
    map.create(game_id.into(), width, height, tiles.clone(), spawn_points)?;

    let board = BoardView::new(game_id, width, height, tiles);
    repo.read_models(board_write_plan(&board)?)
        .commit(&mut map)
        .await?;

    Ok(map)
}
