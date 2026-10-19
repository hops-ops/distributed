//! The deterministic, client-predicted part of the `distributed_game`
//! demos, shared by the server (`game_demos`, natively) and the browser
//! (compiled to WebAssembly by `distributed game-wasm`).
//!
//! Each demo that predicts movement declares its step here with
//! `#[predict]` and names it in its game surface
//! (`GameSurface::predict::<ArenaStep>()`):
//!
//! | step | kind | used by |
//! |---|---|---|
//! | [`arena::ArenaStep`] | `player` | Arena, The Freeze |
//! | [`mobpit::PitStep`] | `player` | Mob Pit |
//! | [`ripple::HoodStep`] | `player` | Boss Ripple (hoods) |
//! | [`grid::GridStep`] | `walker` | Endless Grid (context: seed + cell) |
//!
//! Nothing here may depend on server-only crates (Tokio, axum, SQLite):
//! the crate must compile for `wasm32-unknown-unknown`.

pub mod arena;
pub mod grid;
pub mod mobpit;
pub mod ripple;
