//! `game-demos <demo> [--port N] [--dev]` — one subcommand per demo.

use clap::{Parser, Subcommand};
use game_demos::cluster::{run_cluster, ClusterArgs};
use game_demos::common::{run, DemoArgs};
use game_demos::freeze::world::FreezeArgs;
use game_demos::gallery::GalleryDemo;
use game_demos::grid::GridDemo;
use game_demos::mobpit::{self, MobPitArgs};
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
    /// Non-blocking durable facts, crash recovery, authoritative combat.
    Mobpit(MobPitArgs),
    /// Kills/XP leaderboards (same server as `mobpit`).
    Leaderboard(MobPitArgs),
    /// Lag compensation: hitscan shots rewound to the shooter's view time.
    Gallery(DemoArgs),
    /// World-version barrier across two server processes, safe-room reloads.
    Freeze(FreezeArgs),
    /// Endless Grid: procedural cells as zones, handoff across hosts, hibernation.
    Grid(ClusterArgs),
    /// Boss Ripple: realm > city > borough > hood, tier-up events across hosts.
    Ripple(ClusterArgs),
    /// Print native traces of every predicted step over recorded input
    /// streams (JSON; the WASM exact-match test replays them).
    #[command(hide = true)]
    PredictTrace,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    match Cli::parse().demo {
        DemoCommand::Mobpit(args) | DemoCommand::Leaderboard(args) => mobpit::run(args).await,
        DemoCommand::Gallery(args) => run::<GalleryDemo>(args).await,
        DemoCommand::Freeze(args) => game_demos::freeze::world::run(args).await,
        DemoCommand::Grid(args) => run_cluster::<GridDemo>(args).await,
        DemoCommand::Ripple(args) => run_cluster::<RippleDemo>(args).await,
        DemoCommand::PredictTrace => {
            println!("{}", game_demos::predict_trace::traces());
            Ok(())
        }
    }
}
