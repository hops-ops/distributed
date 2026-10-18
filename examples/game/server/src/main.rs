//! `game-demos <demo> [--port N] [--dev]` — one subcommand per demo.

use clap::{Parser, Subcommand};
use game_demos::arena::ArenaDemo;
use game_demos::cluster::{run_cluster, ClusterArgs};
use game_demos::common::{run, DemoArgs};
use game_demos::grid::GridDemo;
use game_demos::ripple::RippleDemo;

#[derive(Parser)]
#[command(
    name = "game-demos",
    about = "distributed_game demo servers (dev only)"
)]
struct Cli {
    #[command(subcommand)]
    demo: DemoCommand,
}

#[derive(Subcommand)]
enum DemoCommand {
    /// Prediction, reconciliation, interpolation and interest management.
    Arena(DemoArgs),
    /// Endless Grid: procedural cells as zones, handoff across hosts, hibernation.
    Grid(ClusterArgs),
    /// Boss Ripple: realm > city > borough > hood, tier-up events across hosts.
    Ripple(ClusterArgs),
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    match Cli::parse().demo {
        DemoCommand::Arena(args) => run::<ArenaDemo>(args).await,
        DemoCommand::Grid(args) => run_cluster::<GridDemo>(args).await,
        DemoCommand::Ripple(args) => run_cluster::<RippleDemo>(args).await,
    }
}
