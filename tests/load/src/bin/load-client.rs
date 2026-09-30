//! Drive the Counter HTTP API and print a JSON throughput/latency report.
//!
//! ```text
//! cargo run --manifest-path tests/load/Cargo.toml --release --bin load-client -- \
//!   --url http://127.0.0.1:8790 --scenario unique-create --concurrency 32 --duration 15s
//! ```

use std::time::Duration;

use distributed_load::{run_client, ClientConfig, Scenario};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = parse_args(std::env::args().skip(1))?;
    let report = run_client(config).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if report.err > 0 && report.ok == 0 {
        std::process::exit(1);
    }
    Ok(())
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<ClientConfig, String> {
    let mut config = ClientConfig::default();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            "--url" => config.url = expect_value(&arg, args.next())?,
            "--scenario" => config.scenario = Scenario::parse(&expect_value(&arg, args.next())?)?,
            "--concurrency" => config.concurrency = parse_usize(&expect_value(&arg, args.next())?)?,
            "--duration" => config.duration = parse_duration(&expect_value(&arg, args.next())?)?,
            "--warmup" => config.warmup = parse_duration(&expect_value(&arg, args.next())?)?,
            "--repo" => config.repo = Some(expect_value(&arg, args.next())?),
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

fn parse_usize(raw: &str) -> Result<usize, String> {
    raw.parse()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("not a positive integer: {raw}"))
}

fn parse_duration(raw: &str) -> Result<Duration, String> {
    if let Some(secs) = raw.strip_suffix('s') {
        return parse_f64_secs(secs);
    }
    if let Some(mins) = raw.strip_suffix('m') {
        let mins: f64 = mins.parse().map_err(|_| format!("not a duration: {raw}"))?;
        return checked_duration(mins * 60.0, raw);
    }
    parse_f64_secs(raw)
}

fn parse_f64_secs(raw: &str) -> Result<Duration, String> {
    let secs: f64 = raw.parse().map_err(|_| format!("not a duration: {raw}"))?;
    if secs < 0.0 {
        return Err("duration must be >= 0".into());
    }
    checked_duration(secs, raw)
}

fn checked_duration(secs: f64, raw: &str) -> Result<Duration, String> {
    Duration::try_from_secs_f64(secs).map_err(|_| format!("not a duration: {raw}"))
}

fn print_help() {
    eprintln!(
        "\
load-client — HTTP load driver for load-host

Options:
  --url URL                         Host base URL (default: http://127.0.0.1:8790)
  --scenario unique-create|hot-increment
                                    unique-create: new counter id per request (no contention)
                                    hot-increment: one id, every request increments it
  --concurrency N                   Parallel workers (default: 32)
  --duration 15s                    Measured window after warmup (default: 15s)
  --warmup 2s                       Unmeasured warmup (default: 2s)
  --repo NAME                       Copied into the JSON report only
  --snapshots N                     Copied into the JSON report (host --snapshots N)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrency_rejects_zero() {
        assert_eq!(parse_usize("0").unwrap_err(), "not a positive integer: 0");
        assert_eq!(parse_usize("4").unwrap(), 4);
    }

    #[test]
    fn duration_rejects_non_finite_and_overflowing_values() {
        for raw in ["NaN", "inf", "1e308", "NaNm", "infm", "1e308m"] {
            assert!(parse_duration(raw).is_err(), "accepted {raw}");
        }
    }
}
