//! `game-demos <demo> [--port N] [--dev]` — one subcommand per demo.

use clap::{Parser, Subcommand};
use game_demos::arena::ArenaDemo;
use game_demos::common::{run, DemoArgs};
use game_demos::gallery::GalleryDemo;

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
    /// Lag compensation: hitscan shots rewound to the shooter's view time.
    Gallery(DemoArgs),
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    match Cli::parse().demo {
        DemoCommand::Arena(args) => run::<ArenaDemo>(args).await,
        DemoCommand::Gallery(args) => run::<GalleryDemo>(args).await,
    }
}
