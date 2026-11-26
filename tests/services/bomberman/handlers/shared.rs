use distributed::{
    hydrate, Aggregate, CommitBatch, GetStream, RepositoryError, StreamIdentity, StreamWrite,
    TransactionalCommit,
};

use crate::bomberman::domain::bomb::Bomb;
use crate::bomberman::domain::explosion::Explosion;
use crate::bomberman::domain::game_map::GameMap;
use crate::bomberman::domain::player::Player;
use crate::bomberman::error::GameError;

/// One aggregate's stream write, for [`commit_aggregates`].
pub(crate) fn stream<A: Aggregate>(aggregate: &mut A) -> Result<StreamWrite<'_>, GameError> {
    let identity = StreamIdentity::new(A::aggregate_type(), aggregate.entity().id())?;
    Ok(StreamWrite::new(identity, aggregate.entity_mut()))
}

/// Commit aggregates of different types in one transaction. The typed commit
/// builder commits one aggregate type; a raw batch runs the same validation.
pub(crate) async fn commit_aggregates<R: TransactionalCommit>(
    repo: &R,
    streams: Vec<StreamWrite<'_>>,
) -> Result<(), GameError> {
    repo.commit_batch(CommitBatch::new(streams)).await?;
    Ok(())
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

/// Every player who joined the game.
pub(crate) async fn load_game_players<R: GetStream>(
    repo: &R,
    map: &GameMap,
) -> Result<Vec<Player>, GameError> {
    load_indexed_aggregates(repo, &map.players).await
}

/// Every bomb still on the board. Bomb ids are numbered per player
/// (`bomb:{player}:{n}`), so a player's `bombs_placed` names them all.
pub(crate) async fn load_live_bombs<R: GetStream>(
    repo: &R,
    players: &[Player],
) -> Result<Vec<Bomb>, GameError> {
    let ids = players.iter().flat_map(|player| {
        let owner = player
            .entity
            .id()
            .strip_prefix("player:")
            .unwrap_or(player.entity.id())
            .to_string();
        (1..=player.bombs_placed).map(move |n| format!("bomb:{owner}:{n}"))
    });
    Ok(load_indexed_aggregates::<R, Bomb, _>(repo, ids)
        .await?
        .into_iter()
        .filter(|bomb| !bomb.exploded)
        .collect())
}

/// Every explosion still active after the last tick.
pub(crate) async fn load_active_explosions<R: GetStream>(
    repo: &R,
    map: &GameMap,
) -> Result<Vec<Explosion>, GameError> {
    load_indexed_aggregates(repo, &map.explosions).await
}
