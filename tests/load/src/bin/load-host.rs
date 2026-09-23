//! Serve the Counter aggregate HTTP API for load tests.
//!
//! ```text
//! cargo run --manifest-path tests/load/Cargo.toml --release --bin load-host -- \
//!   --repo memory --bind 127.0.0.1:8790
//! ```

use std::path::PathBuf;

use distributed_load::host::{bind_listener, serve_listener, CounterService, HostConfig};
use distributed_load::{LockKind, RepoKind};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = parse_args(std::env::args().skip(1))?;
    let host = CounterService::start(&config).await?;
    let listener = bind_listener(&config.bind).await?;
    let addr = listener.local_addr()?;
    eprintln!(
        "load-host repo={} lock={} snapshots={} commands=[{}, {}] listening on http://{addr}",
        config.repo.as_str(),
        config.lock.as_str(),
        config
            .snapshot_frequency
            .map(|n| n.to_string())
            .unwrap_or_else(|| "off".into()),
        distributed_load::host::INITIALIZE,
        distributed_load::host::INCREMENT,
    );
    serve_listener(host.service, listener).await?;
    Ok(())
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<HostConfig, String> {
    let mut config = HostConfig::default();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            "--repo" => {
                let value = expect_value(&arg, args.next())?;
                config.repo = RepoKind::parse(&value)?;
            }
            "--lock" => {
                let value = expect_value(&arg, args.next())?;
                config.lock = LockKind::parse(&value)?;
            }
            "--bind" => config.bind = expect_value(&arg, args.next())?,
            "--database-url" => config.database_url = Some(expect_value(&arg, args.next())?),
            "--sqlite-path" => config.sqlite_path = PathBuf::from(expect_value(&arg, args.next())?),
            "--snapshots" => {
                let n: u64 = expect_value(&arg, args.next())?
                    .parse()
                    .map_err(|_| "--snapshots requires an integer > 0")?;
                if n == 0 {
                    return Err("snapshot frequency must be > 0".into());
                }
                config.snapshot_frequency = Some(n);
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(config)
}

fn expect_value(flag: &str, value: Option<String>) -> Result<String, String> {
    value.ok_or_else(|| format!("{flag} requires a value"))
}

fn print_help() {
    eprintln!(
        "\
load-host — Counter aggregate HTTP command API

Options:
  --repo memory|sqlite|postgres   Persistence backend (default: memory)
  --lock memory|sqlite|postgres   Queued lock manager (default: memory)
  --snapshots N                   Enable with_snapshots(N); omit for no snapshots
  --bind HOST:PORT                Listen address (default: 127.0.0.1:8790)
  --database-url URL              Postgres URL (default: $DATABASE_URL or compose default)
  --sqlite-path PATH              SQLite file (default: target/load.sqlite); recreated on start

Commands:
  POST /counter.initialize   {{\"id\":\"...\"}}
  POST /counter.increment    {{\"id\":\"...\",\"amount\":1}}
  GET  /health"
    );
}
