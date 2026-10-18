//! `game-demos <demo> [--port N] [--dev]` — one subcommand per demo.

use clap::{Parser, Subcommand};
use game_demos::arena::ArenaDemo;
use game_demos::common::{run, DemoArgs};
use game_demos::freeze::world::FreezeArgs;
use game_demos::gallery::GalleryDemo;
use game_demos::mobpit::{self, MobPitArgs};

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
    /// Non-blocking durable facts, crash recovery, authoritative combat.
    Mobpit(MobPitArgs),
    /// Kills/XP leaderboards (same server as `mobpit`).
    Leaderboard(MobPitArgs),
    /// Lag compensation: hitscan shots rewound to the shooter's view time.
    Gallery(DemoArgs),
    /// World-version barrier across two server processes, safe-room reloads.
    Freeze(FreezeArgs),
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    match Cli::parse().demo {
        DemoCommand::Arena(args) => run::<ArenaDemo>(args).await,
        DemoCommand::Mobpit(args) | DemoCommand::Leaderboard(args) => mobpit::run(args).await,
        DemoCommand::Gallery(args) => run::<GalleryDemo>(args).await,
        DemoCommand::Freeze(args) => game_demos::freeze::world::run(args).await,
    }
}
