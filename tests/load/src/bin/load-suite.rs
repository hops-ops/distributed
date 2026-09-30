//! Run the Counter aggregate load matrix.
//!
//! ```text
//! cargo run --manifest-path tests/load/Cargo.toml --release --bin load-suite -- \
//!   --duration 5s --concurrency 16
//! ```

use std::time::Duration;

use distributed_load::{default_cells, run_suite, Scenario, SuiteConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (config, filter, snapshots_only, locks_only) = parse_args(std::env::args().skip(1))?;
    let mut cells = default_cells(&config);
    if snapshots_only {
        cells.retain(|cell| cell.snapshot_frequency.is_some());
    }
    if locks_only {
        cells.retain(|cell| cell.lock != distributed_load::LockKind::Memory);
    }
    if let Some(filter) = filter {
        cells.retain(|cell| cell.name().contains(&filter));
    }
    if cells.is_empty() {
        return Err("no cells matched".into());
    }
    eprintln!("running {} cells", cells.len());
    let outcomes = run_suite(config, cells).await;
    println!("{}", serde_json::to_string_pretty(&outcomes)?);
    if outcomes.iter().any(|o| o.error.is_some()) {
        std::process::exit(1);
    }
    Ok(())
}

fn parse_args(
    args: impl IntoIterator<Item = String>,
) -> Result<(SuiteConfig, Option<String>, bool, bool), String> {
    let mut config = SuiteConfig::default();
    let mut filter = None;
    let mut snapshots_only = false;
    let mut locks_only = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            "--duration" => config.duration = parse_duration(&expect_value(&arg, args.next())?)?,
            "--warmup" => config.warmup = parse_duration(&expect_value(&arg, args.next())?)?,
            "--concurrency" => {
                config.concurrency = expect_value(&arg, args.next())?
                    .parse()
                    .map_err(|_| "concurrency must be an integer")?
            }
            "--database-url" => config.database_url = Some(expect_value(&arg, args.next())?),
            "--no-locks" => config.include_locks = false,
            "--locks-only" => {
                config.include_locks = true;
                locks_only = true;
            }
            "--no-snapshots" => config.include_snapshots = false,
            "--snapshots-only" => {
                config.include_snapshots = true;
                snapshots_only = true;
            }
            "--snapshot-frequency" => {
                config.snapshot_frequencies = parse_frequencies(&expect_value(&arg, args.next())?)?;
                config.include_snapshots = true;
            }
            "--snapshot-dispatch" => {
                config.snapshot_dispatches =
                    parse_snapshot_dispatches(&expect_value(&arg, args.next())?)?;
                config.include_snapshots = true;
            }
            "--no-external" => config.include_external = false,
            "--scenario" => {
                config.scenarios = vec![Scenario::parse(&expect_value(&arg, args.next())?)?];
            }
            "--both-scenarios" => {
                config.scenarios = vec![Scenario::UniqueCreate, Scenario::HotIncrement];
            }
            "--filter" => filter = Some(expect_value(&arg, args.next())?),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok((config, filter, snapshots_only, locks_only))
}

fn parse_snapshot_dispatches(raw: &str) -> Result<Vec<distributed_load::DispatchKind>, String> {
    use distributed_load::DispatchKind;
    let mut out = Vec::new();
    for part in raw.split(',') {
        let kind = DispatchKind::parse(part.trim())?;
        if kind == DispatchKind::Bus {
            return Err(
                "snapshot cells do not overlay bus (bus cells have their own pairing); \
                 use --snapshot-dispatch direct,http,grpc"
                    .into(),
            );
        }
        if !out.contains(&kind) {
            out.push(kind);
        }
    }
    if out.is_empty() {
        return Err("--snapshot-dispatch needs at least one of direct,http,grpc".into());
    }
    Ok(out)
}

fn parse_frequencies(raw: &str) -> Result<Vec<u64>, String> {
    let freqs: Result<Vec<u64>, _> = raw
        .split(',')
        .map(|part| {
            let n: u64 = part
                .trim()
                .parse()
                .map_err(|_| format!("not a snapshot frequency: {part}"))?;
            if n == 0 {
                return Err("snapshot frequency must be > 0".to_string());
            }
            Ok(n)
        })
        .collect();
    let freqs = freqs?;
    if freqs.is_empty() {
        return Err("--snapshot-frequency needs at least one value".into());
    }
    Ok(freqs)
}

fn expect_value(flag: &str, value: Option<String>) -> Result<String, String> {
    value.ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_duration(raw: &str) -> Result<Duration, String> {
    if let Some(secs) = raw.strip_suffix('s') {
        let secs = secs.parse().map_err(|_| format!("not a duration: {raw}"))?;
        return checked_duration(secs, raw);
    }
    if let Some(mins) = raw.strip_suffix('m') {
        let mins: f64 = mins.parse().map_err(|_| format!("not a duration: {raw}"))?;
        return checked_duration(mins * 60.0, raw);
    }
    let secs = raw.parse().map_err(|_| format!("not a duration: {raw}"))?;
    checked_duration(secs, raw)
}

fn checked_duration(secs: f64, raw: &str) -> Result<Duration, String> {
    Duration::try_from_secs_f64(secs).map_err(|_| format!("not a duration: {raw}"))
}

fn print_help() {
    eprintln!(
        "\
load-suite — Counter aggregate matrix (direct, http, grpc, bus, locks, snapshots)

Options:
  --duration 5s          Measured window (default: 5s)
  --warmup 1s            Unmeasured warmup (default: 1s)
  --concurrency N        Workers per cell (default: 16)
  --database-url URL     Postgres URL (default: $DATABASE_URL)
  --scenario unique-create|hot-increment
  --both-scenarios       Both scenarios (default)
  --no-locks             Skip suite 1a lock-manager cells
  --no-snapshots         Skip snapshot-frequency overlay
  --snapshots-only       Only snapshot-frequency cells
  --snapshot-frequency 1,10,100
                         Frequencies for with_snapshots(n) (default: 1,10,100)
  --snapshot-dispatch direct,http,grpc
                         Overlay snapshot frequencies on these ingress modes
                         (default: direct,http,grpc; buses are always overlayed)
  --no-external          Skip NATS/Kafka/RabbitMQ even if env is set
  --filter SUBSTRING     Keep cells whose name contains SUBSTRING

Env:
  DATABASE_URL           postgres://sourced:sourced@localhost:5432/distributed
  NATS_URL               nats://localhost:4222
  KAFKA_BROKERS          127.0.0.1:9092 (requires --features kafka)
  AMQP_URL               amqp://guest:guest@localhost:5672/%2f (requires --features rabbitmq)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locks_only_is_returned_as_an_active_filter() {
        let (config, filter, snapshots_only, locks_only) =
            parse_args(["--locks-only".to_string()]).unwrap();
        assert!(config.include_locks);
        assert!(locks_only);
        assert!(!snapshots_only);
        assert!(filter.is_none());
    }

    #[test]
    fn duration_rejects_non_finite_and_overflowing_values() {
        for raw in ["NaN", "inf", "1e308", "NaNm", "infm", "1e308m"] {
            assert!(parse_duration(raw).is_err(), "accepted {raw}");
        }
    }
}
