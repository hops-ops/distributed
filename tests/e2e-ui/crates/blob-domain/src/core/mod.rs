//! Pure board rules — no I/O, WASM-eligible.
//!
//! Shared by the aggregate (`models`) and the browser: [`PREVIEW_MOVE`] is the
//! same [`simulate_move`] compiled to WebAssembly for `blob.move` optimism.

mod direction;
mod preview;
mod simulate;
mod tile;

pub use direction::Direction;
pub use preview::{preview_move, MoveArgs, MoveRow, PREVIEW_MOVE};
pub use simulate::{legal_moves, simulate_move, status_of, MovePreview, SimulateError};
pub use tile::Tile;
