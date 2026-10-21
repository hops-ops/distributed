//! Rooms as celld Durable Objects: the [`CelldZone`] hooks of the room zone.
//! The cells worker declares the class with
//! `distributed_game::celld_zone_object!(PitRoom, MobPit)`.

use distributed_game::celld_zone::{CelldZone, LocalFuture, ObjectEnv, ObjectFactSink};
use distributed_game::command_sink::FactRoute;
use distributed_game::HostConfig;
use mobpit_crawler::{RecordKillInput, CRAWLER_KIND, PRESENCE_KIND};

use crate::zone::{MobPit, PitMessage, RoomConfig, WorldTiers};
use crate::{DEFAULT_SEED, SEED_VAR};
use game_sim::mobpit::pair_shard;
use mobpit_tiers::{BossDefeatedInput, BOROUGH_KIND, CITY_KIND, CITY_SHARD, HOOD_KIND};

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
            .route(FactRoute::by_input_field::<BossDefeatedInput>(
                HOOD_KIND, "hood",
            ))
            .lifecycle_route(PRESENCE_KIND)
    }

    /// The tiers above the room: its hood and borough every refresh (2 s),
    /// the city every 5th.
    fn refresh_every() -> Option<std::time::Duration> {
        Some(std::time::Duration::from_secs(2))
    }

    fn refresh(
        env: &ObjectEnv,
        zone_id: &str,
        round: u64,
    ) -> Option<LocalFuture<Option<PitMessage>>> {
        let room = game_sim::mobpit::parse_room(zone_id)?;
        let hood = game_sim::mobpit::hood_of(room);
        let borough = game_sim::mobpit::borough_of(hood);
        let env = env.clone();
        Some(Box::pin(async move {
            let hood_state = env.read_cell(HOOD_KIND, &pair_shard(hood)).await.ok()?;
            let borough_state = env
                .read_cell(BOROUGH_KIND, &pair_shard(borough))
                .await
                .ok()?;
            let city = if round % 5 == 0 {
                env.read_cell(CITY_KIND, CITY_SHARD).await.ok()
            } else {
                None
            };
            let h = &hood_state["state"];
            let b = &borough_state["state"];
            let mut world = WorldTiers {
                hood_tier: h["tier"].as_u64().unwrap_or(0) as u32,
                boss_down: h["boss_down"].as_bool().unwrap_or(false),
                last_by: h["last_by"].as_str().unwrap_or_default().to_string(),
                borough_bosses_down: b["bosses_down"].as_u64().unwrap_or(0) as u32,
                weather: b["weather"].as_u64().unwrap_or(0) as u8,
                hoods_down: b["hoods_down"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default(),
                ..WorldTiers::default()
            };
            match city {
                Some(c) => {
                    world.city_bosses_down = c["state"]["bosses_down"].as_u64().unwrap_or(0) as u32;
                    world.banner = c["state"]["banner"]
                        .as_str()
                        .unwrap_or("The bosses hold the city")
                        .to_string();
                }
                None => {
                    // Not read this round: keep what the room knows.
                    return Some(PitMessage::WorldKeepCity(world));
                }
            }
            Some(PitMessage::World(world))
        }))
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
