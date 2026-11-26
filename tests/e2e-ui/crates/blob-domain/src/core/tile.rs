//! Board tiles. Serialized as snake_case strings (`"player"`, `"hole"`, …), so
//! the client renders `tile-${cell}` with no tile table of its own.

use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, distributed::table::JsonType,
)]
#[serde(rename_all = "snake_case")]
pub enum Tile {
    Hole,
    Unvisited,
    Visited,
    DeadBySuicide,
    DeadByHole,
    Player,
}
