//! The zone server's list of crawlers: the `player_stats` read model
//! (`mobpit_crawler::stats`) projected from the projection bus into a
//! database of its own, under a consumer identity of its own
//! ([`CONSUMER`]), next to `mobpit-service`'s copy.
//!
//! celld cannot enumerate cells, and an index cell every new player passes
//! through would be a global hot spot; the Crawler cells' own
//! `crawler.mob_killed` domain events (cell outbox → Queue → relay → bus)
//! already carry every crawler's post-kill state. `/dev/mobpit/crawlers`
//! and the leaderboard rebuild read this projection. It is eventually
//! consistent: a kill shows up once its event has been relayed and
//! projected.

use distributed::bus::SqliteBus;
use distributed::microsvc::{
    spawn_service_consumer_loop, CausalProjectorContext, HandlerError, ModeledProjection, Routes,
    Service, CONSUMER_IDLE_POLL,
};
use distributed::SqliteRepository;
use mobpit_crawler::stats::{
    projector, read_model_catalog, PlayerStats, APPLICATION, PLAYER_STATS,
};

/// Bus consumer group of the zone server's projector (its offset in the
/// bus; `mobpit-service` consumes as `mobpit`). The service identity stays
/// the projector's owner application, `mobpit`.
pub const CONSUMER: &str = "mobpit-zone";

/// See the module docs.
#[derive(Clone)]
pub struct CrawlerDirectory {
    repo: SqliteRepository,
}

async fn project(
    context: CausalProjectorContext,
    projection: ModeledProjection,
) -> Result<(), HandlerError> {
    projection.apply(PLAYER_STATS, &context).await
}

impl CrawlerDirectory {
    /// Open (creating) the read model at `url` and project `bus` into it
    /// forever (the cursor lives in the read model, so a restart resumes).
    pub async fn open(url: &str, bus: sqlx::SqlitePool) -> Result<Self, String> {
        let repo = SqliteRepository::connect_and_migrate(url)
            .await
            .map_err(|e| format!("crawler directory {url}: {e}"))?;
        let registry = read_model_catalog()
            .table_registry()
            .map_err(|e| format!("crawler directory tables: {e}"))?;
        repo.bootstrap_table_schema_for_dev(&registry)
            .await
            .map_err(|e| format!("crawler directory tables: {e}"))?;
        let read_models = repo.clone();
        spawn_service_consumer_loop(move || {
            Service::new()
                .named(APPLICATION)
                .routes(
                    Routes::new()
                        .with_read_model_store(read_models.clone())
                        .modeled_projector(projector())
                        .handle(project),
                )
                .with_bus(
                    SqliteBus::new_with_group(bus.clone(), CONSUMER)
                        .with_idle_poll(CONSUMER_IDLE_POLL),
                )
        });
        Ok(Self { repo })
    }

    /// Every projected crawler, by principal.
    pub async fn list(&self) -> Result<Vec<PlayerStats>, String> {
        sqlx::query_as::<_, (String, String, i64, i64, i64)>(
            "SELECT player_id, name, xp, kills, level FROM player_stats ORDER BY player_id",
        )
        .fetch_all(self.repo.pool())
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|(player_id, name, xp, kills, level)| PlayerStats {
                    player_id,
                    name,
                    xp,
                    kills,
                    level,
                })
                .collect()
        })
        .map_err(|e| format!("crawler directory: {e}"))
    }
}
