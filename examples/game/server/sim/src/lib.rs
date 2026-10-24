//! The deterministic, client-predicted part of the `distributed_game`
//! demos, shared by the server (`game_demos`, natively) and the browser
//! (compiled to WebAssembly by `distributed game-wasm`), and the pure
//! functions the browser shares with them.
//!
//! Mob Pit's predicted step is declared here with `#[predict]`
//! ([`mobpit::PitStep`], kind `player`, over the planar capsule of
//! [`arena`]) and named in its game surface
//! (`GameSurface::predict::<PitStep>()`).
//!
//! The seeded world generation ([`grid`]: noise, biomes, props) feeds Mob
//! Pit's rooms; [`mobpit::pit_room`] is a `#[game_pure]` function, so the
//! browser draws the server's own rooms. Static data the pages draw is exported as
//! surface constants ([`mobpit::pit_world`]).
//!
//! Nothing here may depend on server-only crates (Tokio, axum, SQLite):
//! the crate must compile for `wasm32-unknown-unknown`.

pub mod arena;
pub mod grid;
pub mod mobpit;
