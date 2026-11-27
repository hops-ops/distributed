//! Rooms as celld Durable Objects: the [`CelldZone`] hooks of the room zone.
//! The cells worker declares the class with
//! `distributed_game::celld_zone_object!(PitRoom, MobPit)`.

use distributed_game::celld_zone::{CelldZone, LocalFuture, ObjectEnv, ObjectFactSink};
use distributed_game::command_sink::FactRoute;
use distributed_game::world::watch::Watches;
use distributed_game::HostConfig;
use mobpit_crawler::{RecordKillInput, CRAWLER_KIND, PRESENCE_KIND};

use crate::zone::{MobPit, PitMessage, RoomConfig};
use crate::{DEFAULT_SEED, SEED_VAR};
use game_sim::mobpit::pair_shard;
use mobpit_tiers::{
    BoroughRead, BossDefeatedInput, CityRead, HoodRead, BOROUGH_KIND, CITY_KIND, CITY_SHARD,
    HOOD_KIND,
};

/// Players a room admits (the game's cap).
pub const MAX_PLAYERS: usize = 32;
/// Worker var overriding [`MAX_PLAYERS`] (load tests).
pub const MAX_PLAYERS_VAR: &str = "ZONE_MAX_PLAYERS";

impl CelldZone for MobPit {
    fn object_config(zone_id: &str, env: &ObjectEnv) -> Result<RoomConfig, String> {
        let seed = env
            .var(SEED_VAR)
            .and_then(|seed| seed.trim().parse().ok())
            .unwrap_or(DEFAULT_SEED);
        RoomConfig::for_zone(zone_id, seed).ok_or_else(|| format!("no room `{zone_id}`"))
    }

    fn host_config(_zone_id: &str, env: &ObjectEnv) -> HostConfig {
        // Load tests raise the cap with `ZONE_MAX_PLAYERS`; the game's is 32.
        let max_players = env
            .var(MAX_PLAYERS_VAR)
            .and_then(|n| n.trim().parse().ok())
            .filter(|&n: &usize| n > 0)
            .unwrap_or(MAX_PLAYERS);
        HostConfig {
            // Joined / lost / reconnected / left facts for the presence cells.
            lifecycle_facts: true,
            max_players,
            // A doorway into a sleeping room waits for it to wake while the
            // client keeps predicting: keep up to 3 s of its inputs.
            input_capacity: 90,
            // A live content update keeps old clients playing where they
            // are; they reload when they walk into the entrance.
            stale_policy: distributed_game::StalePolicy::SafeRoom { roam: false },
            // Ranged shots are rewound at most this far (lag compensation).
            rewind: distributed_game::RewindConfig {
                max_rewind: std::time::Duration::from_millis(crate::zone::MAX_REWIND_MS),
                ..distributed_game::RewindConfig::default()
            },
            ..HostConfig::default()
        }
    }

    fn fact_routes(sink: ObjectFactSink) -> ObjectFactSink {
        sink.route(FactRoute::by_principal::<RecordKillInput>(CRAWLER_KIND))
            .route(FactRoute::by_input_field::<BossDefeatedInput>(
                HOOD_KIND, "hood",
            ))
            .route(FactRoute::by_input_field::<mobpit_tiers::RoomAwakeInput>(
                BOROUGH_KIND,
                "borough",
            ))
            .lifecycle_route(PRESENCE_KIND)
    }

    /// The tiers above the room: its hood, borough and the city, pushed by
    /// their cells after every change (each view a plain `mobpit_tiers`
    /// read struct; the room merges it into its world).
    fn watch(zone_id: &str) -> Watches<PitMessage> {
        let Some(room) = game_sim::mobpit::parse_room(zone_id) else {
            return Watches::new();
        };
        let hood = game_sim::mobpit::hood_of(room);
        let borough = game_sim::mobpit::borough_of(hood);
        Watches::new()
            .cell(HOOD_KIND, &pair_shard(hood), |read: HoodRead| {
                read.state.map(|h| PitMessage::Hood {
                    tier: h.tier,
                    boss_down: h.boss_down,
                    last_by: h.last_by,
                })
            })
            .cell(BOROUGH_KIND, &pair_shard(borough), |read: BoroughRead| {
                read.state.map(|b| PitMessage::Borough {
                    bosses_down: b.bosses_down,
                    weather: b.weather,
                    event: b.event,
                    event_until_ms: b.event_until_ms,
                    hoods_down: read.hoods_down,
                })
            })
            .cell(CITY_KIND, CITY_SHARD, |read: CityRead| {
                read.state.map(|c| PitMessage::City {
                    bosses_down: c.bosses_down,
                    banner: c.banner,
                })
            })
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
