use distributed::microsvc::HandlerError;
use distributed::{mutation_file, Mutation};
use e2e_readmodels::BlobGames;

use crate::{BlobGame, BlobGameState};

#[allow(non_snake_case)]
fn SaveBlobGame() -> Mutation<()> {
    mutation_file!("src/mutations/save_blob_game.mutation.graphql")
}

pub(super) fn sealed_row(game: &BlobGame) -> Result<BlobGames, HandlerError> {
    SaveBlobGame()
        .from_state(&BlobGameState::from(game))
        .map_err(|error| HandlerError::Other(Box::new(error)))
}
