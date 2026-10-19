//! Playable demos of the `distributed_game` runtime (dev only).
//!
//! - [`common`]: what every demo shares — the [`common::Demo`] trait,
//!   startup, dev ticket / NetSim routes, guest names.
//! - One module per demo ([`arena`], …), each implementing `Demo` and
//!   exporting `game_surface()`: its kinds, input and events, authored with
//!   the `distributed_game` derives, from which `run.sh gen` generates the
//!   typed TypeScript client in `client/src/generated/<demo>/`.
//!
//! See `examples/game/README.md`.

pub mod arena;
pub mod cluster;
pub mod common;
pub mod freeze;
pub mod gallery;
pub mod grid;
pub mod mobpit;
pub mod ripple;
