//! Shared plumbing for every demo: CLI options, the [`Demo`] trait, server
//! startup ([`spawn`] / [`run`]), the dev-only routes ([`dev`]), guest display
//! names ([`Guests`]) and process helpers ([`shutdown_signal`],
//! [`random_hex`]). Zone events are typed (`#[derive(GameEvent)]`, sent with
//! `ctx.send` / `ctx.broadcast`) in each demo module.
//!
//! A new demo implements [`Demo`] in its own module and adds one subcommand
//! line in `main.rs`; everything else (ticket endpoint, NetSim control,
//! `/metrics`, graceful shutdown) comes from here.

pub mod dev;
mod guests;

use std::future::Future;
use std::net::SocketAddr;

use axum::Router;
use distributed_game::net::{GameServer, NetSim, ServerConfig, TicketKey};
use distributed_game::Zone;

pub use guests::{random_hex, Guests};

/// One playable demo hosted by `game-demos <NAME>`.
pub trait Demo: 'static {
    /// The zone type.
    type Zone: Zone<Config: Send>;
    /// Subcommand / URL path name (`arena` → `/arena/`).
    const NAME: &'static str;
    /// Zone the client joins by default.
    const DEFAULT_ZONE: &'static str;

    /// Zones this demo creates on demand (the dev ticket endpoint refuses
    /// others).
    fn hosts_zone(zone_id: &str) -> bool;

    /// Build the server. `config` already carries the ticket key, 30 Hz
    /// ticks, 20 Hz patches and any `DG_NET_*` simulation; adjust as needed.
    fn server(config: ServerConfig, guests: Guests) -> GameServer<Self::Zone>;
}

/// Options shared by every demo subcommand.
#[derive(Debug, Clone, clap::Args)]
pub struct DemoArgs {
    /// Interface to bind (demos are local-only by default).
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,
    /// Port (0 = any free port; the bound address is printed).
    #[arg(long, env = "GAME_DEMOS_PORT", default_value_t = 2567)]
    pub port: u16,
    /// Enable the dev-only routes (`/dev/ticket`, `/dev/netsim`). Never in
    /// production: anyone who can reach the port gets a guest ticket.
    #[arg(long, env = "DG_DEMO_DEV")]
    pub dev: bool,
    /// HS256 ticket secret. Default: random per process.
    #[arg(long, env = "DISTRIBUTED_GAME_SECRET", hide_env_values = true)]
    pub secret: Option<String>,
}

/// In-process startup options (tests and [`run`]).
#[derive(Clone)]
pub struct DemoOptions {
    /// Ticket signing key.
    pub key: TicketKey,
    /// Mount the dev routes.
    pub dev: bool,
    /// Initial network simulation.
    pub net_sim: Option<NetSim>,
}

impl DemoOptions {
    /// Dev routes on, the given secret, no simulation.
    pub fn dev(secret: &str) -> Self {
        Self {
            key: TicketKey::new(secret),
            dev: true,
            net_sim: None,
        }
    }
}

/// A demo server running on a Tokio task.
pub struct RunningDemo<D: Demo> {
    /// Bound address.
    pub addr: SocketAddr,
    /// The game server (inspect zones, change NetSim, shut down).
    pub server: GameServer<D::Zone>,
    /// Display names registered through `/dev/ticket`.
    pub guests: Guests,
    /// Resolves when the server stopped.
    pub task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl<D: Demo> RunningDemo<D> {
    /// `ws://addr` (append `/zones/{zone}`).
    pub fn ws_url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// `http://addr`.
    pub fn http_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

/// Routes a demo adds to its server: `dev` only with `--dev` (next to
/// `/dev/ticket`), `internal` always (each route authenticates itself, e.g.
/// Mob Pit's secret-protected celld Queue relay).
#[derive(Default)]
pub struct DemoRoutes {
    /// Dev-only routes.
    pub dev: Router,
    /// Always-mounted internal routes.
    pub internal: Router,
}

/// Server config every demo starts from.
pub fn server_config(key: TicketKey, net_sim: Option<NetSim>) -> ServerConfig {
    let mut config = ServerConfig::new(key);
    config.host.tick_rate = 30;
    config.host.patch_rate = 20;
    config.net_sim = net_sim;
    config
}

/// Start demo `D` on `listener` until `signal` resolves.
pub fn spawn<D: Demo>(
    listener: tokio::net::TcpListener,
    options: DemoOptions,
    signal: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<RunningDemo<D>> {
    spawn_with::<D>(
        listener,
        options,
        |config, guests| (D::server(config, guests), DemoRoutes::default()),
        signal,
    )
}

/// Like [`spawn`], but `build` makes the server and the demo's own routes
/// ([`DemoRoutes`]). For demos whose zones and routes share state (e.g. a
/// durable store) built by the caller.
pub fn spawn_with<D: Demo>(
    listener: tokio::net::TcpListener,
    options: DemoOptions,
    build: impl FnOnce(ServerConfig, Guests) -> (GameServer<D::Zone>, DemoRoutes),
    signal: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<RunningDemo<D>> {
    let addr = listener.local_addr()?;
    let guests = Guests::default();
    let (server, routes) = build(
        server_config(options.key.clone(), options.net_sim),
        guests.clone(),
    );
    let extra = if options.dev {
        dev::routes::<D>(server.clone(), options.key, guests.clone())
            .merge(routes.dev)
            .merge(routes.internal)
    } else {
        routes.internal
    };
    let task = tokio::spawn(server.clone().serve_with(listener, extra, signal));
    Ok(RunningDemo {
        addr,
        server,
        guests,
        task,
    })
}

/// CLI entry: bind, print the address, serve until Ctrl-C / SIGTERM.
pub async fn run<D: Demo>(args: DemoArgs) -> std::io::Result<()> {
    run_with::<D>(args, |config, guests| {
        (D::server(config, guests), DemoRoutes::default())
    })
    .await
}

/// [`run`] with a caller-built server and routes (see [`spawn_with`]).
pub async fn run_with<D: Demo>(
    args: DemoArgs,
    build: impl FnOnce(ServerConfig, Guests) -> (GameServer<D::Zone>, DemoRoutes),
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind((args.host.as_str(), args.port)).await?;
    let secret = args.secret.unwrap_or_else(|| guests::random_hex(32));
    let options = DemoOptions {
        key: TicketKey::new(secret),
        dev: args.dev,
        net_sim: NetSim::from_env(),
    };
    let running = spawn_with::<D>(listener, options, build, shutdown_signal())?;
    // Parsed by run.sh and the client tests: keep the format.
    println!("LISTENING http://{}", running.addr);
    println!(
        "demo {} · zone {} · ws {}/zones/{} · dev routes {}",
        D::NAME,
        D::DEFAULT_ZONE,
        running.ws_url(),
        D::DEFAULT_ZONE,
        if args.dev { "ON" } else { "off" }
    );
    running.task.await.map_err(std::io::Error::other)?
}

/// Resolves on Ctrl-C or SIGTERM (graceful shutdown for every demo binary).
pub async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}
