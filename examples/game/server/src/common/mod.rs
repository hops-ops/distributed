//! Shared plumbing of the demo binary: CLI options ([`DemoArgs`]), the
//! dev ticket wire types ([`dev`]), guest identities ([`guests`]), celld
//! options, the outbox relay, the zone status read model and process
//! helpers ([`shutdown_signal`], [`random_hex`]).

pub mod celld;
pub mod dev;
pub mod guests;
pub mod relay;
pub mod zone_status;

pub use celld::{CelldOptions, CELLD_URL_ENV};
pub use guests::{random_hex, Guests};

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
