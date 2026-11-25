use blob_domain::{Direction, Tile};
use distributed::graphql::{claim, col, read, AuthorizedReadModel, ModelPermissions};
use distributed::ReadModel;
use serde::{Deserialize, Serialize};

use super::AuthUsers;

/// Query-oriented Blob game row. The natural plural name infers `blob_games`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ReadModel)]
#[readmodel(primary_key = ["game_id"])]
pub struct BlobGames {
    #[readmodel(id)]
    pub game_id: String,
    pub owner_id: String,
    pub score: i64,
    pub player_dead: bool,
    pub current_level: i64,
    pub current_level_completed: bool,
    /// The board: a JSON column typed from [`Tile`] in generated clients.
    pub map: Vec<Vec<Tile>>,
    /// Directions the player can take without leaving the board.
    pub moves: Vec<Direction>,
    /// `active` | `dead` | `level_complete`
    pub status: String,
    #[readmodel(belongs_to = "AuthUsers", foreign_key = "owner_id")]
    pub owner: Option<AuthUsers>,
}

impl AuthorizedReadModel for BlobGames {
    /// Read authorization attached to the Blob game query model.
    fn permissions() -> ModelPermissions<Self> {
        ModelPermissions::new()
            .grant(
                "user",
                read()
                    .all_columns()
                    .rows(col("owner_id").eq(claim("x-user-id"))),
            )
            .grant("admin", read().all_columns())
    }
}
