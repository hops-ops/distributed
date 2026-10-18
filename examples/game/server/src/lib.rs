//! Playable demos of the `distributed_game` runtime (dev only).
//!
//! - [`common`]: what every demo shares — the [`common::Demo`] trait,
//!   startup, dev ticket / NetSim routes, guest names, JSON events.
//! - One module per demo ([`arena`], …), each implementing `Demo`.
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
