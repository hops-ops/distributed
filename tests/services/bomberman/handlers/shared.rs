use distributed::read_model::ReadModelWritePlanBuilder;
use distributed::{
    hydrate, Aggregate, GetStream, ReadModelWorkspaceExt, ReadModelWritePlanStore,
    RelationalReadModelQueryStore, RepositoryError, RowKey, RowValue, StreamIdentity,
};

use crate::bomberman::domain::bomb::Bomb;
use crate::bomberman::domain::explosion::Explosion;
use crate::bomberman::domain::game_map::GameMap;
use crate::bomberman::domain::player::Player;
use crate::bomberman::error::GameError;
use crate::bomberman::views::{build_board, BoardView};

pub(crate) fn board_key(game_id: &str) -> RowKey {
    RowKey::new([("game_id", RowValue::String(game_id.into()))])
}

pub(crate) fn board_write_plan(board: &BoardView) -> Result<ReadModelWritePlanBuilder, GameError> {
    let mut read_models = ReadModelWritePlanBuilder::new();
    read_models.upsert(board).map_err(RepositoryError::from)?;
    Ok(read_models)
}

pub(crate) async fn load_board<R>(repo: &R, game_id: &str) -> Result<BoardView, GameError>
where
    R: ReadModelWritePlanStore + RelationalReadModelQueryStore,
{
    repo.workspace()
        .load::<BoardView>(board_key(game_id))
        .one()
        .await
        .map_err(RepositoryError::from)?
        .map(|board| board.data)
        .ok_or(GameError::GameNotFound)
}

/// Load and hydrate a single aggregate by its raw id, keyed under the
/// aggregate's own stream type. Works through `&R` (handlers borrow the repo).
pub(crate) async fn get_aggregate<R, A>(repo: &R, id: &str) -> Result<Option<A>, GameError>
where
    R: GetStream,
    A: Aggregate + Send,
{
    let identity = StreamIdentity::new(A::aggregate_type(), id).map_err(GameError::Repository)?;
    let Some(entity) = repo.get_stream(&identity).await? else {
        return Ok(None);
    };
    Ok(Some(hydrate::<A>(entity).map_err(GameError::Repository)?))
}

async fn load_indexed_aggregates<R, A, I>(repo: &R, ids: I) -> Result<Vec<A>, GameError>
where
    R: GetStream,
    A: Aggregate + Send,
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut results = Vec::new();
    for id in ids {
        let id = id.as_ref();
        let aggregate = get_aggregate::<R, A>(repo, id)
            .await?
            .ok_or_else(|| RepositoryError::NotFound { id: id.to_string() })?;
        results.push(aggregate);
    }
    Ok(results)
}

pub(crate) async fn load_board_players<R: GetStream>(
    repo: &R,
    board: &BoardView,
) -> Result<Vec<Player>, GameError> {
    load_indexed_aggregates(repo, board.players.iter().map(|player| player.id.as_str())).await
}

pub(crate) async fn load_board_bombs<R: GetStream>(
    repo: &R,
    board: &BoardView,
) -> Result<Vec<Bomb>, GameError> {
    load_indexed_aggregates(repo, board.bombs.iter().map(|bomb| bomb.id.as_str())).await
}

pub(crate) async fn load_board_explosions<R: GetStream>(
    repo: &R,
    board: &BoardView,
) -> Result<Vec<Explosion>, GameError> {
    load_indexed_aggregates(
        repo,
        board
            .explosions
            .iter()
            .map(|explosion| explosion.id.as_str()),
    )
    .await
}

pub(crate) fn build_board_from_aggregates(
    game_id: &str,
    map: &GameMap,
    players: &[Player],
    bombs: &[Bomb],
    explosions: &[Explosion],
    turn: u32,
    explosions_created: u64,
) -> BoardView {
    let player_refs: Vec<&Player> = players.iter().collect();
    let bomb_refs: Vec<&Bomb> = bombs.iter().collect();
    let explosion_refs: Vec<&Explosion> = explosions.iter().collect();

    build_board(
        game_id,
        map,
        &player_refs,
        &bomb_refs,
        &explosion_refs,
        turn,
        explosions_created,
    )
}
