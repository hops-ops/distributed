//! **Mob Pit** (+ **Leaderboard**) — non-blocking durable facts, crash
//! recovery and authoritative combat; kills projected into sorted-set
//! leaderboards.
//!
//! - [`zone`]: the 30 Hz zone (players, 12 mobs with a telegraphed attack
//!   state machine, melee cones). A kill emits a fact and keeps ticking.
//! - [`crawler`]: the per-player `Crawler` aggregate and its one command;
//!   XP is decided there, the principal comes from the trusted session.
//! - [`store`]: one native SQLite cell per player + the `FactSink` (with the
//!   "slow storage" latency knob) that routes facts into them via
//!   `distributed_game::cell::CellFactSink`.
//! - [`board`]: kills/XP leaderboards (in-memory or Redis), fire-and-forget
//!   live updates, idempotent apply keys, rebuild from the cells' events.
//! - [`routes`]: dev routes (`/dev/mobpit/*`).
//!
//! `game-demos mobpit` and `game-demos leaderboard` start the same server
//! (one process hosts the pit and the boards); they differ only in the page
//! `run.sh` opens. Data lives in `--data-dir` / `MOBPIT_DATA_DIR`; the
//! ticket secret is persisted there too so guests keep their identity across
//! a restart (crash-recovery demo).

pub mod board;
pub mod crawler;
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
pub use zone::{encode_bot_input, MobPit};

/// `game-demos mobpit|leaderboard` options.
#[derive(Debug, Clone, clap::Args)]
pub struct MobPitArgs {
    /// Shared demo options.
    #[command(flatten)]
    pub common: DemoArgs,
    /// Directory of the per-player SQLite cells (and the persisted dev
    /// ticket secret). Default: `$TMPDIR/game-demos-mobpit`.
    #[arg(long, env = "MOBPIT_DATA_DIR")]
    pub data_dir: Option<PathBuf>,
    /// Redis for the leaderboard (`redis://127.0.0.1:6379`); default in memory.
    #[arg(long, env = "REDIS_URL")]
    pub redis_url: Option<String>,
    /// Start with this artificial storage latency (ms).
    #[arg(long, env = "MOBPIT_STORAGE_LATENCY_MS", default_value_t = 0)]
    pub storage_latency_ms: u64,
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
    /// board, no dev routes of its own); use [`spawn`] / [`run`] instead.
    fn server(config: ServerConfig, guests: Guests) -> GameServer<MobPit> {
        let world = MobPitWorld::open(WorldOptions::temporary())
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

/// CLI entry: open the cells, rebuild the board from them, serve.
pub async fn run(mut args: MobPitArgs) -> std::io::Result<()> {
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("game-demos-mobpit"));
    let mut options = WorldOptions::new(&data_dir);
    options.redis_url = args.redis_url.clone();
    let world = MobPitWorld::open(options).map_err(std::io::Error::other)?;
    world
        .sink_state()
        .set_latency(std::time::Duration::from_millis(args.storage_latency_ms));
    if args.common.secret.is_none() {
        args.common.secret = Some(persisted_secret(&data_dir)?);
    }
    // Board = projection of the cells: rebuild it before accepting players
    // (the in-memory board starts empty; Redis may have missed updates).
    match world.rebuild_board().await {
        Ok(report) => eprintln!("mobpit: leaderboard rebuilt from cells: {report}"),
        Err(error) => eprintln!("mobpit: leaderboard rebuild failed (serving anyway): {error}"),
    }
    eprintln!(
        "mobpit: data dir {} · boot {} · leaderboard {}",
        data_dir.display(),
        world.boot(),
        world.board().backend()
    );
    common::run_with::<MobPitDemo>(args.common, move |config, guests| {
        world.server(config, guests)
    })
    .await
}
