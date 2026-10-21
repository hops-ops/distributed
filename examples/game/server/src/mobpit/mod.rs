//! **Mob Pit** (+ **Leaderboard**) — a near-infinite grid of rooms joined by
//! doorways, authoritative melee combat, kills settled exactly once in the
//! players' durable cells, and live room status.
//!
//! The rooms run on **celld**: every room (`pit.<x>.<y>`) is a Durable
//! Object of the cells worker (`examples/game/cells`, zone code in
//! `mobpit_rooms`, hosted by `distributed_game::celld_zone`). celld decides
//! which node runs a room; the game never does. A room's kill facts go to
//! the killer's Crawler cell object to object; its lifecycle (active /
//! dormant / hibernated) goes to its zone-activity cell.
//!
//! This process (`game-demos mobpit|leaderboard`) is the services side, no
//! room runs here:
//! - [`routes`]: the dev routes — `/dev/ticket` mints room tickets for
//!   guests (signed with the secret the rooms verify), `/dev/mobpit/*`.
//! - [`world`]: the projection bus the cells' outbox Queue relays onto
//!   (`crate::common::relay`), the crawler directory ([`directory`]), the
//!   rooms' status (`zone_status`, for the minimap) and the leaderboards
//!   ([`board`]) fed from the directory.
//! - [`store`]: reads of the Crawler and presence cells.
//!
//! `--data-dir` / `MOBPIT_DATA_DIR` keeps the dev ticket secret (shared
//! with the rooms through celld's `GAME_TICKET_SECRET`; guests keep their
//! identity across restarts) and the default projection bus.

pub mod board;
pub mod crawler;
pub mod directory;
pub mod routes;
pub mod store;
pub mod world;
pub mod zone;

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use distributed_game::net::TicketKey;

use crate::common::relay::relay_routes;
use crate::common::{random_hex, shutdown_signal, DemoArgs};
pub use world::{MobPitWorld, WorldOptions};
pub use zone::{encode_bot_input, game_surface, MobPit};

/// Room ids this demo admits (`pit.<x>.<y>`).
pub fn hosts_zone(zone_id: &str) -> bool {
    mobpit_rooms::parse_room(zone_id).is_some()
}

/// `game-demos mobpit|leaderboard` options.
#[derive(Debug, Clone, clap::Args)]
pub struct MobPitArgs {
    /// Shared demo options (`--secret` / `DISTRIBUTED_GAME_SECRET` is the
    /// rooms' ticket secret, `GAME_TICKET_SECRET` on celld).
    #[command(flatten)]
    pub common: DemoArgs,
    /// celld serving `examples/game/cells` (the rooms and their cells).
    /// The internal secret comes from `DISTRIBUTED_INTERNAL_SECRET`.
    #[arg(long, env = "CELLD_URL")]
    pub celld_url: String,
    /// Directory of local dev files: the persisted dev ticket secret, the
    /// default projection bus and this server's read models.
    /// Default: `$TMPDIR/game-demos-mobpit`.
    #[arg(long, env = "MOBPIT_DATA_DIR")]
    pub data_dir: Option<PathBuf>,
    /// Redis for the leaderboard (`redis://127.0.0.1:6379`); default in memory.
    #[arg(long, env = "REDIS_URL")]
    pub redis_url: Option<String>,
    /// Redis key prefix of the boards (`mobpit-service` reads the same).
    #[arg(long, env = "MOBPIT_REDIS_PREFIX", default_value = mobpit_crawler::BOARD_REDIS_PREFIX)]
    pub redis_prefix: String,
    /// SQLite bus the cells' domain events are published to (relayed from
    /// celld's outbox Queue); `mobpit-service` and this server project it.
    /// Default: `sqlite:{data-dir}/bus.db`.
    #[arg(long, env = "MOBPIT_PROJECTION_BUS")]
    pub projection_bus: Option<String>,
}

/// The services running on a Tokio task.
pub struct RunningServices {
    /// Bound address.
    pub addr: SocketAddr,
    /// Resolves when the server stopped.
    pub task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl RunningServices {
    /// `http://addr`.
    pub fn http_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

/// Serve `world` on `listener` until `signal` (tests): the internal relay
/// route always, the dev routes when `dev`.
pub fn spawn(
    listener: tokio::net::TcpListener,
    key: TicketKey,
    dev: bool,
    world: Arc<MobPitWorld>,
    signal: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<RunningServices> {
    let addr = listener.local_addr()?;
    let mut app = relay_routes(world.celld().secret.clone(), world.projection());
    if dev {
        app = app.merge(routes::routes(world, key));
    }
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(signal)
            .await
    });
    Ok(RunningServices { addr, task })
}

/// The dev ticket secret kept in the data dir (`dev-ticket-secret`), so
/// guests keep their identity across restarts; `run.sh` gives the same
/// file to celld as the rooms' `GAME_TICKET_SECRET`.
pub fn persisted_secret(dir: &std::path::Path) -> std::io::Result<String> {
    let path = dir.join("dev-ticket-secret");
    if let Ok(secret) = std::fs::read_to_string(&path) {
        let secret = secret.trim().to_string();
        if secret.len() >= 32 {
            return Ok(secret);
        }
    }
    std::fs::create_dir_all(dir)?;
    let secret = random_hex(64);
    std::fs::write(&path, &secret)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(secret)
}

/// CLI entry: open the services, rebuild the board from the cells, serve.
pub async fn run(args: MobPitArgs) -> std::io::Result<()> {
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
    let ticket_secret = match args.common.secret.clone() {
        Some(secret) => secret,
        None => persisted_secret(&data_dir)?,
    };
    match world.rebuild_board().await {
        Ok(report) => eprintln!(
            "mobpit: leaderboard rebuilt from cells: {}",
            serde_json::to_string(&report).unwrap_or_default()
        ),
        Err(error) => eprintln!("mobpit: leaderboard rebuild failed (serving anyway): {error}"),
    }
    let listener =
        tokio::net::TcpListener::bind((args.common.host.as_str(), args.common.port)).await?;
    let running = spawn(
        listener,
        TicketKey::new(ticket_secret),
        args.common.dev,
        world.clone(),
        shutdown_signal(),
    )?;
    // Parsed by run.sh and the client tests: keep the format.
    println!("LISTENING http://{}", running.addr);
    println!(
        "demo mobpit · rooms on celld {} (ws …/rooms/{}) · projection bus {} · leaderboard {} · dev routes {}",
        args.celld_url,
        mobpit_rooms::ENTRANCE,
        world.bus_url(),
        world.board().backend(),
        if args.common.dev { "ON" } else { "off" }
    );
    running.task.await.map_err(std::io::Error::other)?
}
