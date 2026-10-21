//! The deterministic, client-predicted part of the `distributed_game`
//! demos, shared by the server (`game_demos`, natively) and the browser
//! (compiled to WebAssembly by `distributed game-wasm`), and the pure
//! functions the browser shares with them.
//!
//! Each demo that predicts movement declares its step here with
//! `#[predict]` and names it in its game surface
//! (`GameSurface::predict::<ArenaStep>()`):
//!
//! | step | kind | used by |
//! |---|---|---|
//! | [`arena::ArenaStep`] | `player` | The Freeze |
//! | [`mobpit::PitStep`] | `player` | Mob Pit |
//! | [`ripple::HoodStep`] | `player` | Boss Ripple (hoods) |
//!
//! The seeded world generation ([`grid`]: noise, biomes, props) feeds Mob
//! Pit's rooms; [`mobpit::pit_room`] is a `#[game_pure]` function, so the
//! browser draws the server's own rooms. Static data the pages draw is exported as
//! surface constants ([`arena::arena_floor`], [`mobpit::pit_world`]).
//!
//! Nothing here may depend on server-only crates (Tokio, axum, SQLite):
//! the crate must compile for `wasm32-unknown-unknown`.

pub mod arena;
pub mod grid;
pub mod mobpit;
pub mod ripple;
