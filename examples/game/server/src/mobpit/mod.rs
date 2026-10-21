//! **Mob Pit** (+ **Leaderboard**) — non-blocking durable facts, crash
//! recovery and authoritative combat; kills projected into sorted-set
//! leaderboards.
//!
//! - [`zone`]: the 30 Hz zone (players, 12 mobs with a telegraphed attack
//!   state machine, melee cones). A kill emits a fact and keeps ticking.
//! - [`crawler`]: the per-player `Crawler` aggregate and its one command;
//!   XP is decided there, the principal comes from the trusted session.
//! - [`store`]: the zone server's side of the celld cells
//!   (`examples/game/cells`): the `FactSink` (with the "slow storage"
//!   latency knob) that routes facts to them via
//!   `distributed_game::celld::CelldFactSink`, and the cell reader.
//! - [`directory`]: the crawler list, a projection of the cells' domain
//!   events (outbox → Queue → `crate::common::relay` → bus).
//! - [`board`]: kills/XP leaderboards (in-memory or Redis), fire-and-forget
//!   live updates, idempotent apply keys, rebuild from the cells' events.
//! - [`presence`]: who is online where, projected from the runtime's player
//!   lifecycle facts (`HostConfig::lifecycle_facts`), ordered by fact stamp.
//! - [`routes`]: dev routes (`/dev/mobpit/*`).
//!
//! `game-demos mobpit` and `game-demos leaderboard` start the same server
//! (one process hosts the pit and the boards); they differ only in the page
//! `run.sh` opens. Durable state is in celld (`--celld-url` / `CELLD_URL`,
//! secret `DISTRIBUTED_INTERNAL_SECRET`); `run.sh` and the tests start
//! `celld dev` with `examples/game/cells`. `--data-dir` / `MOBPIT_DATA_DIR`
//! keeps the persisted ticket secret (so guests keep their identity across
//! a restart, the crash-recovery demo) and the default projection bus.

pub mod board;
pub mod crawler;
pub mod directory;
pub mod presence;
pub mod routes;
pub mod store;
pub mod world;
pub mod zone;

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use distributed_game::net::{GameServer, ServerConfig};

use crate::common::{self, random_hex, Demo, DemoArgs, DemoOptions, Guests, RunningDemo};
pub use world::{MobPitWorld, WorldOptions};
pub use zone::{encode_bot_input, game_surface, MobPit};

/// `game-demos mobpit|leaderboard` options.
#[derive(Debug, Clone, clap::Args)]
pub struct MobPitArgs {
    /// Shared demo options.
    #[command(flatten)]
    pub common: DemoArgs,
    /// celld serving `examples/game/cells` (the Crawler and presence
    /// cells). The internal secret comes from
    /// `DISTRIBUTED_INTERNAL_SECRET`.
    #[arg(long, env = "CELLD_URL")]
    pub celld_url: String,
    /// Directory of local dev files: the persisted dev ticket secret, the
    /// default projection bus and the crawler directory's read models.
    /// Default: `$TMPDIR/game-demos-mobpit`.
    #[arg(long, env = "MOBPIT_DATA_DIR")]
    pub data_dir: Option<PathBuf>,
    /// Redis for the leaderboard (`redis://127.0.0.1:6379`); default in memory.
    #[arg(long, env = "REDIS_URL")]
    pub redis_url: Option<String>,
    /// Redis key prefix of the boards (`mobpit-service` reads the same).
    #[arg(long, env = "MOBPIT_REDIS_PREFIX", default_value = mobpit_crawler::BOARD_REDIS_PREFIX)]
    pub redis_prefix: String,
    /// Start with this artificial storage latency (ms).
    #[arg(long, env = "MOBPIT_STORAGE_LATENCY_MS", default_value_t = 0)]
    pub storage_latency_ms: u64,
    /// SQLite bus (`sqlite:/path/bus.db`) the Crawler cells'
    /// `crawler.mob_killed` domain events are published to (relayed from
    /// celld's outbox Queue); `mobpit-service` and this server's crawler
    /// directory project them. Default: `sqlite:{data-dir}/bus.db`.
    #[arg(long, env = "MOBPIT_PROJECTION_BUS")]
    pub projection_bus: Option<String>,
}

/// Demo registration.
pub struct MobPitDemo;

impl Demo for MobPitDemo {
    type Zone = MobPit;
    const NAME: &'static str = "mobpit";
    const DEFAULT_ZONE: &'static str = "pit-1";

    fn hosts_zone(zone_id: &str) -> bool {
        zone_id.starts_with("pit-")
    }

    /// Plain `common::spawn` gets a throwaway world (temp dir, in-memory
    /// board, no routes of its own) on the celld named by `CELLD_URL` and
    /// `DISTRIBUTED_INTERNAL_SECRET`; use [`spawn`] / [`run`] instead.
    fn server(config: ServerConfig, guests: Guests) -> GameServer<MobPit> {
        let celld = store::CelldOptions::from_env().expect("Mob Pit needs celld");
        let world = MobPitWorld::open(WorldOptions::temporary(celld))
            .expect("mobpit world (needs a Tokio runtime)");
        world.server(config, guests).0
    }
}

/// Start Mob Pit over `world` (tests; dev routes need `options.dev`).
pub fn spawn(
    listener: tokio::net::TcpListener,
    options: DemoOptions,
    world: Arc<MobPitWorld>,
    signal: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<RunningDemo<MobPitDemo>> {
    common::spawn_with::<MobPitDemo>(
        listener,
        options,
        move |config, guests| world.server(config, guests),
        signal,
    )
}

/// Dev ticket secret kept in the data dir, so guest tokens (and therefore
/// principals) survive a server restart.
fn persisted_secret(dir: &std::path::Path) -> std::io::Result<String> {
    let path = dir.join("dev-ticket-secret");
    if let Ok(secret) = std::fs::read_to_string(&path) {
        let secret = secret.trim().to_string();
        if secret.len() >= 32 {
            return Ok(secret);
        }
    }
    let secret = random_hex(64);
    std::fs::write(&path, &secret)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(secret)
}

/// CLI entry: connect to the cells, rebuild the board from them, serve.
pub async fn run(mut args: MobPitArgs) -> std::io::Result<()> {
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("game-demos-mobpit"));
    let secret = std::env::var(distributed::cell_host::CELL_INTERNAL_SECRET_ENV).map_err(|_| {
        std::io::Error::other(
            "DISTRIBUTED_INTERNAL_SECRET is required (shared with the celld cells worker)",
        )
    })?;
    let celld = store::CelldOptions::new(&args.celld_url, secret).map_err(std::io::Error::other)?;
    let mut options = WorldOptions::new(&data_dir, celld);
    options.redis_url = args.redis_url.clone();
    options.redis_prefix = args.redis_prefix.clone();
    options.projection_bus = args.projection_bus.clone();
    let world = MobPitWorld::start(options)
        .await
        .map_err(std::io::Error::other)?;
    world
        .sink_state()
        .set_latency(std::time::Duration::from_millis(args.storage_latency_ms));
    if args.common.secret.is_none() {
        args.common.secret = Some(persisted_secret(&data_dir)?);
    }
    // Board = projection of the cells: rebuild it from the crawler
    // directory before accepting players (the in-memory board starts
    // empty; Redis may have missed updates).
    match world.rebuild_board().await {
        Ok(report) => eprintln!(
            "mobpit: leaderboard rebuilt from cells: {}",
            serde_json::to_string(&report).unwrap_or_default()
        ),
        Err(error) => eprintln!("mobpit: leaderboard rebuild failed (serving anyway): {error}"),
    }
    let bus = world.bus_url().to_string();
    eprintln!(
        "mobpit: celld {} · projection bus {bus} · data dir {} · boot {} · leaderboard {}",
        args.celld_url,
        data_dir.display(),
        world.boot(),
        world.board().backend()
    );
    common::run_with::<MobPitDemo>(args.common, move |config, guests| {
        world.server(config, guests)
    })
    .await
}
