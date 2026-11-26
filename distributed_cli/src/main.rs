use clap::Parser;
use distributed_cli::{DistributedArgs, DistributedCommands, LifecycleError, LifecycleOutput};

/// The `distributed` CLI: build/dev lifecycle, client compile,
/// describe manifests, and render schema artifacts.
#[derive(Parser, Debug)]
#[command(name = "distributed", version, about, long_about = None)]
struct Cli {
    #[command(flatten)]
    args: DistributedArgs,
}

fn main() {
    let cli = Cli::parse();
    if let Err(err) = distributed_cli::run(&cli.args, &[]) {
        if matches!(
            &cli.args.command,
            DistributedCommands::Build(args) if matches!(args.output, LifecycleOutput::Json)
        ) {
            if let Some(diagnostic) = err
                .downcast_ref::<LifecycleError>()
                .and_then(LifecycleError::diagnostic)
            {
                println!("{diagnostic}");
                std::process::exit(1);
            }
        }
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}
