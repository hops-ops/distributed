//! One projection service. Subscribes to every event type the projection
//! handlers consume and dispatches each event (a message) to the owning handler.

mod handlers;

use std::sync::Arc;

use distributed::microsvc::{HandlerError, Routes, Service};
use distributed::read_model::{InMemoryReadModelStore, ReadModelWorkspaceExt};
use distributed::table::TableStoreError;

use crate::distributed_read_model_board::read_models::{board_key, BoardView};

pub type ProjectionDependencies = InMemoryReadModelStore;

pub fn service(store: InMemoryReadModelStore) -> Arc<Service> {
    Arc::new(Service::new().routes(distributed::routes!(
        Routes::new().with_read_model_store(store),
        events handlers::board,
    )))
}

fn read_model_error(err: TableStoreError) -> HandlerError {
    HandlerError::Repository(err.into())
}

/// Load the projected board (with its cards) from the read store. After the
/// bus has been drained into the projection service, the board reflects every
/// processed event.
pub async fn load_board(store: &InMemoryReadModelStore, board_id: &str) -> Option<BoardView> {
    store
        .workspace()
        .load::<BoardView>(board_key(board_id))
        .include("cards")
        .one()
        .await
        .expect("board load should succeed")
        .map(|view| view.data)
}
