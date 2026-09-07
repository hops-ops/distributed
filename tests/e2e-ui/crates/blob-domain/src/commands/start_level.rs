use distributed::command::{Atomic, PreparedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};
use distributed::portable_command;
use e2e_readmodels::BlobGames;
use serde::Deserialize;

use super::support::{authenticated_user, principal, rejected, sealed_row};
use crate::{domain_commands, BlobGame};

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct BlobStartLevelInput {
    pub game_id: String,
}

pub async fn handle_start_level(
    ctx: &CausalCommandContext<'_, BlobGame>,
    input: BlobStartLevelInput,
) -> Result<PreparedCommand<Atomic<BlobGames>>, HandlerError> {
    let owner = principal(ctx)?;
    let repo = ctx.repo();
    let mut game = repo
        .get(&input.game_id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.game_id.clone()))?;
    game.start_next_generated_level(&owner).map_err(rejected)?;
    let row = sealed_row(&*game)?;
    repo.readmodel(row).publish_events().commit(game)?.atomic()
}

portable_command! {
    name: "blob.start_level",
    transition: domain_commands::StartLevel,
    aggregate: BlobGame,
    input: BlobStartLevelInput,
    outcome: Atomic<BlobGames>,
    shard: |input| input.game_id.clone(),
    roles: ["user", "admin"],
    field: "blob_games_start_level",
    guard: authenticated_user,
    handle: handle_start_level,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_is_game_id() {
        let input = BlobStartLevelInput {
            game_id: "g1".into(),
        };
        assert_eq!(StartLevel::shard(&input), "g1");
    }
}
