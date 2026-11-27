//! Blob game domain — one crate, two faces:
//!
//! - [`core`] — pure board rules and the [`PREVIEW_MOVE`] browser pure
//!   (always available; compiled to WASM with `--features wasm`)
//! - [`models`] / [`levels`] — the event-sourced aggregate (`feature = "domain"`, default)
//!
//! The `blob.*` commands, which seal `BlobGames` rows, live with the service
//! (`e2e-service`): the domain does not know the read models.

pub mod core;

#[cfg(feature = "domain")]
pub mod levels;
#[cfg(feature = "domain")]
pub mod models;

pub use core::{
    legal_moves, preview_move, simulate_move, Direction, MoveArgs, MovePreview, MoveRow,
    SimulateError, Tile, PREVIEW_MOVE,
};

#[cfg(feature = "domain")]
pub use levels::{demo_map, generate_level, generate_level_with, is_hamiltonian_passable};
#[cfg(feature = "domain")]
pub use models::{
    domain_commands, test_map_no_holes, test_map_with_hole, BlobError, BlobGame, BlobGameState,
    BlobInitializedDomainEvent, BlobLevelStartedDomainEvent, BlobMovedDomainEvent,
    BlobStartedDomainEvent,
};
