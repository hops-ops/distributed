//! Load-test bots against a running `distributed_game` server.
//!
//! ```sh
//! cargo run -p distributed_game --features bots --bin game-bots -- \
//!   --url ws://127.0.0.1:2567 --zone room-1 --bots 50 --rate 20 --secs 10 \
//!   --secret dev-secret --behaviour circle
//! ```
//!
//! Flags fall back to `DG_BOTS_URL`, `DG_BOTS_ZONE`, `DG_BOTS_COUNT`,
//! `DG_BOTS_RATE`, `DG_BOTS_SECS`, `DG_TICKET_SECRET`, `DG_BOTS_BEHAVIOUR`.
//! Inputs are encoded as an `(i8, i8)` direction pair.

use std::time::Duration;

use distributed_game::bots::{run_bots, BehaviourKind, BotConfig};
use distributed_game::net::TicketKey;

const USAGE: &str = "usage: game-bots [--url ws://host:port] [--zone id] [--bots n] [--rate hz] \
[--secs s] [--ramp-ms ms] [--secret s] [--behaviour random|circle] [--prefix p]";

fn main() {
    let mut args = std::env::args().skip(1);
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let mut url = env("DG_BOTS_URL", "ws://127.0.0.1:2567");
    let mut zone = env("DG_BOTS_ZONE", "room-1");
    let mut bots = env("DG_BOTS_COUNT", "10");
    let mut rate = env("DG_BOTS_RATE", "20");
    let mut secs = env("DG_BOTS_SECS", "10");
    let mut ramp = String::from("0");
    let mut secret = env("DG_TICKET_SECRET", "dev-secret");
    let mut behaviour = env("DG_BOTS_BEHAVIOUR", "random");
    let mut prefix = String::from("bot");
    while let Some(flag) = args.next() {
        let slot = match flag.as_str() {
            "--url" => &mut url,
            "--zone" => &mut zone,
            "--bots" => &mut bots,
            "--rate" => &mut rate,
            "--secs" => &mut secs,
            "--ramp-ms" => &mut ramp,
            "--secret" => &mut secret,
            "--behaviour" => &mut behaviour,
            "--prefix" => &mut prefix,
            _ => {
                eprintln!("{USAGE}");
                std::process::exit(if flag == "--help" || flag == "-h" {
                    0
                } else {
                    2
                });
            }
        };
        *slot = args.next().unwrap_or_else(|| {
            eprintln!("{flag} needs a value\n{USAGE}");
            std::process::exit(2)
        });
    }
    let parse = |name: &str, v: &str| -> f64 {
        v.parse().unwrap_or_else(|_| {
            eprintln!("bad {name}: {v}");
            std::process::exit(2)
        })
    };
    let kind = match behaviour.as_str() {
        "random" => BehaviourKind::RandomWalk,
        "circle" => BehaviourKind::Circle,
        other => {
            eprintln!("unknown behaviour {other}\n{USAGE}");
            std::process::exit(2)
        }
    };
    let mut config = BotConfig::new(url, zone, TicketKey::new(secret));
    config.bots = parse("bots", &bots) as usize;
    config.input_rate = parse("rate", &rate);
    config.duration = Duration::from_secs_f64(parse("secs", &secs));
    config.ramp_up = Duration::from_millis(parse("ramp-ms", &ramp) as u64);
    config.principal_prefix = prefix;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let summary = runtime.block_on(run_bots(config, move |i| kind.build(i)));
    println!("{summary}");
    if summary.failed() > 0 {
        std::process::exit(1);
    }
}
