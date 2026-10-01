use distributed::command::{Atomic, PreparedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};
use distributed::portable_command;
use e2e_readmodels::BlobGames;
use serde::Deserialize;

use super::support::{authenticated_user, principal, rejected, sealed_row};
use crate::{domain_commands, BlobGame};

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct BlobStartInput {
    pub game_id: String,
}

pub async fn handle_start(
    ctx: &CausalCommandContext<'_, BlobGame>,
    input: BlobStartInput,
) -> Result<PreparedCommand<Atomic<BlobGames>>, HandlerError> {
    let owner = principal(ctx)?;
    let repo = ctx.repo();
    if repo.get(&input.game_id).await?.is_some() {
        return Err(HandlerError::Rejected(format!(
            "game {} already exists",
            input.game_id
        )));
    }
    let mut game = repo.create();
    game.start_with_demo(&input.game_id, &owner)
        .map_err(rejected)?;
    let row = sealed_row(&*game)?;
    repo.readmodel(row).publish_events().commit(game)?.atomic()
}

portable_command! {
    name: "blob.start",
    transition: domain_commands::StartWithMap,
    aggregate: BlobGame,
    input: BlobStartInput,
    outcome: Atomic<BlobGames>,
    shard: |input| input.game_id.clone(),
    roles: ["user", "admin"],
    field: "blob_games_start",
    guard: authenticated_user,
    handle: handle_start,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_is_game_id() {
        let input = BlobStartInput {
            game_id: "g1".into(),
        };
        assert_eq!(Start::shard(&input), "g1");
    }
}
