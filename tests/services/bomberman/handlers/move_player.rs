use distributed::{
    GetStream, ReadModelWritePlanCommitExt, ReadModelWritePlanStore, RelationalReadModelQueryStore,
    TransactionalCommit,
};

use super::shared::{
    board_write_plan, build_board_from_aggregates, get_aggregate, load_board, load_board_bombs,
    load_board_explosions, load_board_players,
};
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
    R: GetStream + TransactionalCommit + ReadModelWritePlanStore + RelationalReadModelQueryStore,
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

    let current_board = load_board(repo, game_id).await?;
    let mut all_players: Vec<Player> = load_board_players(repo, &current_board)
        .await?
        .into_iter()
        .filter(|existing| existing.entity.id() != player.entity.id())
        .collect();
    all_players.push(player.clone());

    let all_bombs = load_board_bombs(repo, &current_board).await?;
    let all_explosions = load_board_explosions(repo, &current_board).await?;
    let board = build_board_from_aggregates(
        game_id,
        &map,
        &all_players,
        &all_bombs,
        &all_explosions,
        current_board.turn,
        current_board.explosions_created,
    );

    repo.read_models(board_write_plan(&board)?)
        .aggregate(&mut map)
        .aggregate(&mut player)
        .commit()
        .await?;

    Ok(())
}
