use distributed::read_model::{ReadModelWorkspaceExt, Versioned};
use distributed::table::{RowKey, RowValue};
use distributed::{
    GetStream, ReadModelWritePlanStore, RelationalReadModelQueryStore, RepositoryError,
    TransactionalCommit,
};

use crate::bomberman::domain::player::Player;
use crate::bomberman::domain::tick_saga::TickSaga;
use crate::bomberman::domain::types::Direction;
use crate::bomberman::error::GameError;
use crate::bomberman::handlers;
use crate::bomberman::views::{project_board, BoardView};

/// A test game. Each command it runs is followed by the board projection,
/// standing in for the projector a deployment runs on the committed events.
pub struct Game<'a, R> {
    repo: &'a R,
    pub game_id: String,
}

impl<'a, R> Game<'a, R>
where
    R: GetStream + TransactionalCommit + ReadModelWritePlanStore + RelationalReadModelQueryStore,
{
    pub async fn new(repo: &'a R, game_id: &str, ascii: &str) -> Result<Self, GameError> {
        handlers::create_game(repo, game_id, ascii).await?;
        let game = Self {
            repo,
            game_id: game_id.to_string(),
        };
        game.project().await?;
        Ok(game)
    }

    async fn project(&self) -> Result<(), GameError> {
        project_board(self.repo, &self.game_id).await.map(|_| ())
    }

    pub fn sim(&self, id: &str, name: &str) -> PlayerSim<'_, R> {
        PlayerSim {
            game: self,
            id: id.to_string(),
            name: name.to_string(),
        }
    }

    pub async fn tick(&self) -> Result<TickSaga, GameError> {
        let saga = handlers::tick(self.repo, &self.game_id).await?;
        self.project().await?;
        Ok(saga)
    }

    pub async fn board(&self) -> Result<Versioned<BoardView>, GameError> {
        self.repo
            .workspace()
            .load::<BoardView>(RowKey::new([(
                "game_id",
                RowValue::String(self.game_id.clone()),
            )]))
            .one()
            .await
            .map_err(|e| GameError::Repository(RepositoryError::Model(e.to_string())))?
            .ok_or(GameError::GameNotFound)
    }
}

pub struct PlayerSim<'a, R> {
    game: &'a Game<'a, R>,
    pub id: String,
    pub name: String,
}

impl<'a, R> PlayerSim<'a, R>
where
    R: GetStream + TransactionalCommit + ReadModelWritePlanStore + RelationalReadModelQueryStore,
{
    pub async fn join(&self, spawn_index: usize) -> Result<(), GameError> {
        handlers::join_game(
            self.game.repo,
            &self.id,
            &self.name,
            &self.game.game_id,
            spawn_index,
        )
        .await?;
        self.game.project().await
    }

    pub async fn move_dir(&self, dir: Direction) -> Result<(), GameError> {
        handlers::move_player(self.game.repo, &self.id, dir, &self.game.game_id).await?;
        self.game.project().await
    }

    pub async fn place_bomb(&self) -> Result<(), GameError> {
        handlers::place_bomb(self.game.repo, &self.id, &self.game.game_id).await?;
        self.game.project().await
    }

    pub async fn is_alive(&self) -> Result<bool, GameError> {
        let player = self.player().await?;
        Ok(player.alive)
    }

    pub async fn player(&self) -> Result<Player, GameError> {
        handlers::get_player(self.game.repo, &self.id).await
    }
}
