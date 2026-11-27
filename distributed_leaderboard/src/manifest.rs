//! The `leaderboards` section of the client manifest
//! (`DistributedClientManifest::external`), read by the client compiler to
//! generate `leaderboards.ts`.

use serde::{Deserialize, Serialize};

/// Section of the application contract and the client manifest that lists
/// leaderboard surfaces.
pub const LEADERBOARDS_SECTION: &str = "leaderboards";

/// One leaderboard surface on the client contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientLeaderboard {
    /// GraphQL base name (`xp` → `xpLeaderboard`, …).
    pub name: String,
    /// Object type of each row (`LeaderboardEntry`: `rank`, `member`, `score`).
    pub entry_typename: String,
    /// `high_first` or `low_first`.
    pub order: String,
    /// `single` (no `board` argument) or `family` (`board: String!`).
    pub boards: String,
    pub default_first: u32,
    pub max_first: u32,
    pub max_radius: u32,
    pub roots: Vec<ClientLeaderboardRoot>,
}

/// One leaderboard root the selected roles may run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientLeaderboardRoot {
    pub name: String,
    /// `top`, `around_me`, `my_rank` or `rank`.
    pub operation: String,
    /// `true`: `[LeaderboardEntry!]!`; `false`: nullable `LeaderboardEntry`.
    pub list: bool,
    /// Whether a same-named subscription root exists (`@live`).
    pub live: bool,
    pub arguments: Vec<ClientLeaderboardArgument>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientLeaderboardArgument {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
}
