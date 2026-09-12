use distributed::command::{Atomic, CommandProjectionPureReduce, PreparedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};
use distributed::portable_command;
use e2e_readmodels::BlobGames;
use serde::Deserialize;

use super::support::{authenticated_user, principal, rejected, sealed_row};
use crate::{domain_commands, BlobGame, Direction};

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct BlobMoveInput {
    pub game_id: String,
    pub direction: String,
}

pub async fn handle_move(
    ctx: &CausalCommandContext<'_, BlobGame>,
    input: BlobMoveInput,
) -> Result<PreparedCommand<Atomic<BlobGames>>, HandlerError> {
    let owner = principal(ctx)?;
    let direction = Direction::parse(&input.direction).ok_or_else(|| {
        HandlerError::Rejected(format!(
            "invalid direction `{}` (use up|down|left|right)",
            input.direction
        ))
    })?;
    let repo = ctx.repo();
    let mut game = repo
        .get(&input.game_id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.game_id.clone()))?;
    game.move_dir(&owner, direction).map_err(rejected)?;
    let row = sealed_row(&*game)?;
    repo.readmodel(row).publish_events().commit(game)?.atomic()
}

fn blob_preview() -> CommandProjectionPureReduce {
    CommandProjectionPureReduce::wasm(
        "blob.simulate_move",
        "blob/pkg/blob_wasm",
        "blobSimulateMove",
        "BlobGames",
    )
    .key_input("game_id", ["game_id"])
    .arg_input("direction", ["direction"])
    .assign([
        "map_json",
        "score",
        "player_dead",
        "current_level_completed",
        "status",
    ])
}

portable_command! {
    name: "blob.move",
    transition: domain_commands::MoveDir,
    aggregate: BlobGame,
    input: BlobMoveInput,
    outcome: Atomic<BlobGames>,
    shard: |input| input.game_id.clone(),
    roles: ["user", "admin"],
    field: "blob_games_move",
    constructor: move_dir,
    preview_reduce_known_record: blob_preview(),
    guard: authenticated_user,
    handle: handle_move,
}

#[cfg(test)]
mod tests {
    use super::*;
    use distributed::cell_host::instance_name;
    use distributed::Aggregate;

    #[test]
    fn shard_is_game_id() {
        let input = BlobMoveInput {
            game_id: "g1".into(),
            direction: "up".into(),
        };
        assert_eq!(Move::shard(&input), "g1");
    }

    #[test]
    fn cell_is_parent_game_shard() {
        let input = BlobMoveInput {
            game_id: "g1".into(),
            direction: "up".into(),
        };
        let shard = Move::shard(&input);
        assert_eq!(
            instance_name::<BlobGame>(&shard),
            format!("{}:{}", BlobGame::aggregate_type(), shard)
        );
        assert_eq!(
            instance_name::<BlobGame>(&shard),
            "blob:g1",
            "cell host addresses BlobGame as (aggregate_type, game_id)"
        );
    }
}
