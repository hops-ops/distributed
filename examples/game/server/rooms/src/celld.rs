//! Rooms as celld Durable Objects: the [`CelldZone`] hooks of the room zone.
//! The cells worker declares the class with
//! `distributed_game::celld_zone_object!(PitRoom, MobPit)`.

use distributed_game::celld_zone::{CelldZone, LocalFuture, ObjectEnv, ObjectFactSink};
use distributed_game::command_sink::FactRoute;
use distributed_game::HostConfig;
use mobpit_crawler::{RecordKillInput, CRAWLER_KIND, PRESENCE_KIND};

use crate::zone::{MobPit, PitMessage, RoomConfig};
use crate::{DEFAULT_SEED, SEED_VAR};

impl CelldZone for MobPit {
    fn object_config(zone_id: &str, env: &ObjectEnv) -> Result<RoomConfig, String> {
        let seed = env
            .var(SEED_VAR)
            .and_then(|seed| seed.trim().parse().ok())
            .unwrap_or(DEFAULT_SEED);
        RoomConfig::for_zone(zone_id, seed).ok_or_else(|| format!("no room `{zone_id}`"))
    }

    fn host_config(_zone_id: &str, _env: &ObjectEnv) -> HostConfig {
        HostConfig {
            // Joined / lost / reconnected / left facts for the presence cells.
            lifecycle_facts: true,
            max_players: 32,
            ..HostConfig::default()
        }
    }

    fn fact_routes(sink: ObjectFactSink) -> ObjectFactSink {
        sink.route(FactRoute::by_principal::<RecordKillInput>(CRAWLER_KIND))
            .lifecycle_route(PRESENCE_KIND)
    }

    /// Read the player's Crawler cell (off the tick) for its level and XP.
    fn after_join(env: &ObjectEnv, principal: &str) -> Option<LocalFuture<Option<PitMessage>>> {
        let env = env.clone();
        let principal = principal.to_string();
        Some(Box::pin(async move {
            let cell = env.read_cell(CRAWLER_KIND, &principal).await.ok()?;
            let state = &cell["state"];
            Some(PitMessage::Loaded {
                principal,
                xp: state["xp"].as_u64().unwrap_or(0),
                level: state["level"].as_u64().unwrap_or(1) as u32,
                kills: state["kills"].as_u64().unwrap_or(0) as u32,
            })
        }))
    }
}
