//! Normalized relational read models for the board example. `BoardView`
//! has_many `CardView`; `CardView` belongs_to `BoardView` and stores a JSONB
//! payload column.

mod board_view;
mod card_view;

pub use board_view::BoardView;
pub use card_view::{CardPayload, CardView};

use distributed::{InMemoryReadModelStore, RowKey, RowValue, TableStoreError};

pub fn register_schemas(store: &InMemoryReadModelStore) -> Result<(), TableStoreError> {
    store.register_schema::<BoardView>()?;
    store.register_schema::<CardView>()?;
    Ok(())
}

pub fn board_key(board_id: &str) -> RowKey {
    RowKey::new([("board_id", RowValue::String(board_id.into()))])
}

pub fn card_key(board_id: &str, card_id: &str) -> RowKey {
    RowKey::new([
        ("board_id", RowValue::String(board_id.into())),
        ("card_id", RowValue::String(card_id.into())),
    ])
}
