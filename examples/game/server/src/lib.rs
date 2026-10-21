//! Playable demos of the `distributed_game` runtime (dev only).
//!
//! - [`common`]: what every demo shares — the [`common::Demo`] trait,
//!   startup, dev ticket / NetSim routes, guest names.
//! - One module per demo ([`mobpit`], …), each implementing `Demo` and
//!   exporting `game_surface()`: its kinds, input and events, authored with
//!   the `distributed_game` derives, from which `run.sh gen` generates the
//!   typed TypeScript client in `client/src/generated/<demo>/`.
//! - The client-predicted steps live in the `game_sim` crate (`sim/`),
//!   which `distributed game-wasm` compiles to WebAssembly;
//!   [`predict_trace`] is the native side of their exact-match proof.
//!
//! See `examples/game/README.md`.

pub mod cluster;
pub mod common;
pub mod devcelld;
pub mod freeze;
pub mod gallery;
pub mod grid;
pub mod mobpit;
pub mod predict_trace;
pub mod ripple;
