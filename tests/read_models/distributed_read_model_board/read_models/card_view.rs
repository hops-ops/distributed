use distributed::ReadModel;
use serde::{Deserialize, Serialize};

use super::BoardView;

/// Structured JSONB payload stored in one column of the `cards` table.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CardPayload {
    pub labels: Vec<String>,
    pub assignee: Option<String>,
}

/// One card. Composite primary key `[board_id, card_id]`; `board_id` is a
/// delegated foreign key from the board; `payload` is a JSONB column; `board`
/// is a `belongs_to` include.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("cards")]
#[readmodel(primary_key = ["board_id", "card_id"])]
pub struct CardView {
    #[readmodel(foreign_key = "boards.board_id", delegated_from = "BoardView.board_id")]
    pub board_id: String,
    pub card_id: String,
    pub column: String,
    pub title: String,
    #[readmodel(jsonb)]
    pub payload: CardPayload,
    #[readmodel(belongs_to = "BoardView", foreign_key = "board_id")]
    pub board: Option<BoardView>,
}
