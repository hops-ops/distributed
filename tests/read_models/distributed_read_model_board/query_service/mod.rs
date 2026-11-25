//! Read-only query service for the board read model. Primary-key loads plus
//! `has_many` / `belongs_to` relationship includes.

use distributed::{InMemoryReadModelStore, ReadModelWorkspaceExt, TableStoreError};

use crate::distributed_read_model_board::read_models::{board_key, card_key, BoardView, CardView};

#[derive(Clone)]
pub struct BoardQueryService {
    store: InMemoryReadModelStore,
}

impl BoardQueryService {
    pub fn new(store: InMemoryReadModelStore) -> Self {
        Self { store }
    }

    /// Load a board with its cards (`has_many` include).
    pub async fn board_with_cards(
        &self,
        board_id: &str,
    ) -> Result<Option<BoardView>, TableStoreError> {
        let mut session = self.store.workspace();
        Ok(session
            .load::<BoardView>(board_key(board_id))
            .include("cards")
            .one()
            .await?
            .map(|view| view.data))
    }

    /// Load one card with its board (`belongs_to` include).
    pub async fn card_with_board(
        &self,
        board_id: &str,
        card_id: &str,
    ) -> Result<Option<CardView>, TableStoreError> {
        let mut session = self.store.workspace();
        Ok(session
            .load::<CardView>(card_key(board_id, card_id))
            .include("board")
            .one()
            .await?
            .map(|view| view.data))
    }
}
