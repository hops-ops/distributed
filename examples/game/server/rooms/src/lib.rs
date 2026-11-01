//! **Mob Pit rooms**: a near-infinite seeded grid of rooms (`pit.<x>.<y>`)
//! joined by doorways, one zone per room ([`zone`]).
//!
//! On celld every room is a Durable Object (`celld` feature: [`celld`]
//! implements `distributed_game::celld_zone::CelldZone` for the zone; the
//! cells worker declares the class). celld decides where each room runs;
//! the rooms neither know nor care. A room that empties goes dormant, then
//! hibernates with its mobs; coming back restores them. Kills settle as
//! facts in the killer's Crawler cell from any room.
//!
//! The room contents (biome, danger, mobs) come from
//! `game_sim::mobpit::pit_room`, the same code the browser runs as
//! WebAssembly.

#[cfg(all(feature = "celld", target_arch = "wasm32"))]
pub mod celld;
pub mod zone;

pub use game_sim::mobpit::{
    parse_room, pit_room, room_zone, PitRoomContext, PitRoomLayout, DEFAULT_SEED, DOOR_HALF,
};
pub use zone::*;

/// The room every player starts in.
pub const ENTRANCE: &str = "pit.0.0";
/// Env var / CLI name of the rooms' world seed.
pub const SEED_VAR: &str = "MOBPIT_SEED";
