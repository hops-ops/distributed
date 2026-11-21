//! The `player_stats` read model: one row per Crawler, the post-kill
//! [`CrawlerState`](crate::CrawlerState) carried by every
//! `crawler.mob_killed` domain event.
//!
//! Projected from the bus by every reader that needs it, each with its own
//! consumer identity and its own database: `mobpit_service` (GraphQL
//! `@live` leaderboard) and the zone server (the crawler list behind
//! `/dev/mobpit/crawlers` and the leaderboard rebuild). celld cannot list
//! cells, and a cell every player passes through would be a global hot
//! spot; the read model is the list.

use distributed::graphql::SurfaceProjector;
use distributed::projection::lower::{EventualOnly, ProjectionDescriptor};
use distributed::{
    mutation_file, LocalProjectionMountsBuilder, Mutation, ReadModel, ReadModelCatalog,
    RelationalReadModel,
};
use serde::{Deserialize, Serialize};

use crate::CrawlerMobKilledDomainEvent;

/// Application of the read model (catalog and projector owner).
pub const APPLICATION: &str = "mobpit";
/// Projection name.
pub const PROJECTOR: &str = "project_player_stats";
/// Projection epoch: bump to rebuild the table from the bus log.
pub const EPOCH: &str = "mobpit-player-stats-v1";

/// One player's settled progress (a projection of their Crawler cell).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ReadModel)]
#[table("player_stats")]
pub struct PlayerStats {
    /// Principal (the verified session's user id).
    #[id("player_id")]
    pub player_id: String,
    /// Display name at the last kill.
    pub name: String,
    /// Total XP (decided by the Crawler cell, never by a caller).
    pub xp: i64,
    /// Mobs killed.
    pub kills: i64,
    /// Level for `xp`.
    pub level: i64,
}

/// Upsert one `player_stats` row from the domain event's state body.
#[allow(non_snake_case)]
pub fn SavePlayerStats() -> Mutation<()> {
    mutation_file!("src/mutations/save_player_stats.mutation.graphql")
}

distributed::projection! {
    pub const PLAYER_STATS: ProjectionDescriptor<EventualOnly> = {
        name: "project_player_stats",
        version: 1,
        epoch: "mobpit-player-stats-v1",
        model: PlayerStats,
        on {
            events: [CrawlerMobKilledDomainEvent],
            mutation: SavePlayerStats,
            input: { player: body },
        },
    };
}

/// Physical read-model catalog (`player_stats`).
pub fn read_model_catalog() -> ReadModelCatalog {
    ReadModelCatalog::new(APPLICATION).table_schema(PlayerStats::schema().clone())
}

/// The modeled projector mount for [`PLAYER_STATS`].
pub fn projector() -> SurfaceProjector {
    LocalProjectionMountsBuilder::new(APPLICATION, "ordered-domain-events")
        .expect("projection source")
        .eventual_model::<PlayerStats, _>(PROJECTOR, PLAYER_STATS, EPOCH)
        .expect("player stats mount")
        .build()
        .expect("projection catalog")
        .projector(PROJECTOR)
        .expect("player stats projector")
}
