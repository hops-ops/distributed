use distributed::ReadModel;
use serde::{Deserialize, Serialize};

use super::CardView;

/// Board header row. `has_many` cards and a `source_version` guard for
/// out-of-order delivery.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("boards")]
pub struct BoardView {
    #[id("board_id")]
    pub board_id: String,
    pub name: String,
    pub source_version: i64,
    #[readmodel(has_many = "CardView", foreign_key = "board_id")]
    pub cards: Vec<CardView>,
}
