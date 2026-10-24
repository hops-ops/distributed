//! Crude load test: how many moving players fit in a Mob Pit room, and how
//! many busy rooms fit on one celld.
//!
//! Each step puts `ZONES x BOTS` Rust bots (`distributed_game::bots`, real
//! WebSocket clients signing their own tickets) into `ZONES` rooms (a grid
//! from `pit.0.0`), `BOTS` per room; a bare `BOTS` is one room. Every bot
//! moves (circles near its spawn by default) and holds attack, at the tick
//! rate. For each step it reads every room's `/zones/{room}/info` once the
//! bots are in and again at the end of the hold, and prints one row:
//!
//! - dropped ticks over the hold (%) and late p99 (ms) of the worst room,
//!   and the players the rooms count;
//! - joined / failed bots, RTT p50/p99 (after each bot's first 5 s) and
//!   Delta bytes/s per bot, from the bots;
//! - locally, celld's CPU (cores busy over the hold) and RSS; against a
//!   fleet, optionally the output of `--stats-cmd`.
//!
//! A row is marked `DEGRADED` past 1 % dropped ticks, late p99 > 50 ms, RTT
//! p99 > 150 ms or a bot that did not join.
//!
//! Under each row, the work the rooms did over the hold (their `work`
//! counters, `distributed_game::WorkCounters`, and the watched cells'
//! `__watchers` counts) — exact, whatever the machine's load:
//!
//! - `enc/patch` records encoded per room patch (shared by all clients),
//!   `sent/patch` creates + updates sent, `held` the share of updates held
//!   back by update rates, `B/frame` Delta bytes per client frame;
//! - `facts/room-s`, `req/room-s` (cell requests; a `__batch` is one),
//!   `batches`, `facts/req`, `deletes/room-s` (outbox deletes) and
//!   `deletes/fact` (rates depend on the fighting; the ratios do not);
//! - `reads/room-min` (watched-cell reads by the rooms), `pushes/room-min`
//!   (received), and per minute the cells' `pushed` / `skipped` (unchanged)
//!   views and watching `reads`.
//!
//! Interleaved A/B (local only; same build, fresh celld per run): `--ab
//! VAR=VAL` (repeatable) is the B side's extra worker vars; `--pairs N`
//! runs A,B then B,A … and prints per step the median of each side and the
//! median of the per-pair change. Switches: `ZONE_FACT_BATCH=1` (one fact
//! per request; default 16), `ZONE_UPDATE_RATES=0` (every change every
//! patch), `ZONE_WORLD_PUSH=0` (rooms poll watched cells every second
//! instead of taking pushes).
//!
//! Local (starts its own `celld dev` with the cells worker; build it first
//! with `./run.sh build`):
//!
//! ```sh
//! cargo run --release --example mobpit-load -- --steps 8,16,32,64,96,128
//! cargo run --release --example mobpit-load -- --steps 16x1,64x1,64x8
//! cargo run --release --example mobpit-load -- --steps 32 --ab ZONE_UPDATE_RATES=0 --pairs 3
//! ```
//!
//! Against a running fleet (celld's public listener, the worker's ticket
//! secret, and `ZONE_MAX_PLAYERS` at least the largest step):
//!
//! ```sh
//! cargo run --release --example mobpit-load -- --url ws://celld:8080 \
//!     --secret "$GAME_TICKET_SECRET" --steps 16,32,64
//! ```

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use distributed::cell_host::CELL_INTERNAL_SECRET_HEADER;
use distributed_game::authoring::GameInput;
use distributed_game::bots::{run_bots, Behaviour, BotConfig, Circle, RandomWalk, Summary};
use distributed_game::net::TicketKey;
use distributed_game::netsim::{NETSIM_SHARD, ZONE_DEV_KIND};
use distributed_game::world::content_rollout::CONTENT_ROLLOUT_KIND;
use distributed_game::world::watch::{WatcherStats, STATS_ROUTE};
use distributed_game::WorkCounters;
use game_demos::devcelld::DevCelld;
use game_demos::mobpit::encode_bot_input;
use game_sim::mobpit::{borough_of, hood_of, pair_shard, parse_room, PitInput};
use mobpit_tiers::{BOROUGH_KIND, CITY_KIND, CITY_SHARD, HOOD_KIND};
use serde_json::{json, Value};

#[derive(Parser, Debug)]
#[command(about = "Ramp moving bots into one Mob Pit room until it degrades")]
struct Args {
    /// Steps, comma-separated: `BOTS` (one room) or `ZONESxBOTS`.
    #[arg(long, default_value = "8,16,32,48,64,96,128", value_delimiter = ',', value_parser = parse_step)]
    steps: Vec<(usize, usize)>,
    /// celld public listener (ws://…); default: start a local `celld dev`.
    #[arg(long)]
    url: Option<String>,
    /// Room info base (http://…); default: `--url` with http.
    #[arg(long)]
    info_url: Option<String>,
    /// Ticket secret of the worker (with `--url`).
    #[arg(long, env = "GAME_TICKET_SECRET")]
    secret: Option<String>,
    /// Seconds every bot stays once all have joined (the measured window).
    #[arg(long, default_value_t = 30)]
    hold_secs: u64,
    /// Seconds over which a step's bots join.
    #[arg(long, default_value_t = 5)]
    ramp_secs: u64,
    /// Seconds between steps (the room settles).
    #[arg(long, default_value_t = 5)]
    rest_secs: u64,
    /// Movement: `mix` (half random-walk, half circle), `walk` or `circle`.
    /// Walkers can leave through a doorway (a handoff closes them here).
    #[arg(long, default_value = "circle")]
    moves: String,
    /// Move without holding attack (no combat, so far fewer room writes).
    #[arg(long)]
    no_attack: bool,
    /// Inputs per second per bot (the room's tick rate).
    #[arg(long, default_value_t = 30.0)]
    rate: f64,
    /// Shell command run near the end of each hold; its output is reported
    /// (e.g. celld CPU / memory).
    #[arg(long)]
    stats_cmd: Option<String>,
    /// Extra worker var for the local celld, `KEY=VALUE` (repeatable), e.g.
    /// `ZONE_DEV_SETTINGS=0`.
    #[arg(long)]
    var: Vec<String>,
    /// Local celld project dir to keep (state and `celld-*.log`); default a
    /// temporary one, deleted at exit.
    #[arg(long)]
    celld_dir: Option<std::path::PathBuf>,
    /// Write every step as JSON lines here.
    #[arg(long)]
    json: Option<std::path::PathBuf>,
    /// Internal secret for the watched cells' counters (with `--url`;
    /// local runs know it).
    #[arg(long, env = "DISTRIBUTED_INTERNAL_SECRET")]
    internal_secret: Option<String>,
    /// A/B: the B side's extra worker var, `KEY=VALUE` (repeatable); the A
    /// side runs with `--var` only. Local only.
    #[arg(long)]
    ab: Vec<String>,
    /// A/B pairs (A,B then B,A, …).
    #[arg(long, default_value_t = 3)]
    pairs: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), String> {
    let args = Args::parse();
    let mut json_out = match &args.json {
        Some(path) => {
            Some(std::fs::File::create(path).map_err(|e| format!("{}: {e}", path.display()))?)
        }
        None => None,
    };
    if args.ab.is_empty() {
        run(&args, &[], "", 0, &mut json_out).await?;
        return Ok(());
    }
    if args.url.is_some() {
        return Err("--ab starts its own celld per run (local only)".into());
    }
    let b_vars: Vec<String> = args.ab.clone();
    println!("A/B: B = A + {} · {} pairs", b_vars.join(" "), args.pairs);
    let (mut a_runs, mut b_runs) = (Vec::new(), Vec::new());
    for pair in 0..args.pairs.max(1) {
        // Alternate the order so drift on the machine hits both sides.
        let order = if pair % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        };
        for b in order {
            tokio::time::sleep(Duration::from_secs(args.rest_secs)).await;
            let (vars, label) = if b {
                (&b_vars[..], "B")
            } else {
                (&[][..], "A")
            };
            let rows = run(&args, vars, label, pair, &mut json_out).await?;
            if b {
                b_runs.push(rows);
            } else {
                a_runs.push(rows);
            }
        }
    }
    print_ab(&args.steps, &a_runs, &b_runs);
    Ok(())
}

/// One run over every step (a fresh local celld with `extra` vars, unless
/// `--url`); prints each row as it finishes.
async fn run(
    args: &Args,
    extra: &[String],
    label: &str,
    pair: usize,
    json_out: &mut Option<std::fs::File>,
) -> Result<Vec<Row>, String> {
    let largest = args.steps.iter().map(|s| s.1).max().unwrap_or(32);
    // Local: our own fleet, the room cap raised to the largest step.
    let cap = largest.to_string();
    let local = match &args.url {
        Some(_) => None,
        None => {
            let mut vars = vec![("ZONE_MAX_PLAYERS", cap.as_str())];
            vars.extend(
                args.var
                    .iter()
                    .chain(extra)
                    .filter_map(|v| v.split_once('=')),
            );
            Some(match &args.celld_dir {
                Some(dir) if label.is_empty() => DevCelld::in_project_with(dir, None, &vars)?,
                Some(dir) => DevCelld::in_project_with(
                    &dir.join(format!("{}{pair}", label.to_lowercase())),
                    None,
                    &vars,
                )?,
                None => DevCelld::start_with(None, &vars)?,
            })
        }
    };
    let (url, secret) = match (&local, &args.url) {
        (Some(celld), _) => (celld.ws_url(), celld.ticket_secret().to_owned()),
        (None, Some(url)) => (
            url.clone(),
            args.secret
                .clone()
                .ok_or("--secret (or GAME_TICKET_SECRET) is required with --url")?,
        ),
        (None, None) => unreachable!(),
    };
    let internal = local
        .as_ref()
        .map(|celld| celld.secret().to_owned())
        .or_else(|| args.internal_secret.clone());
    let info_base = args.info_url.clone().unwrap_or_else(|| {
        url.replacen("ws://", "http://", 1)
            .replacen("wss://", "https://", 1)
    });
    let celld_pid = local.as_ref().and_then(DevCelld::pid);

    let tag = if label.is_empty() {
        String::new()
    } else {
        format!("[{label}{pair}] ")
    };
    println!(
        "{tag}{url} · hold {} s · {} inputs/s per bot · worst room's ticks{}",
        args.hold_secs,
        args.rate,
        if extra.is_empty() {
            String::new()
        } else {
            format!(" · {}", extra.join(" "))
        }
    );
    println!(
        "{:>9} {:>6} {:>6} {:>6} {:>8} {:>9} {:>8} {:>8} {:>9} {:>6} {:>7}  stats",
        "rooms x n",
        "bots",
        "joined",
        "room",
        "dropped",
        "late p99",
        "rtt p50",
        "rtt p99",
        "B/s/bot",
        "cores",
        "rss MB"
    );
    let mut rows = Vec::with_capacity(args.steps.len());
    for (step, &(zones, n)) in args.steps.iter().enumerate() {
        if step > 0 {
            tokio::time::sleep(Duration::from_secs(args.rest_secs)).await;
        }
        let rooms = grid(zones);
        let mut fleets = Vec::with_capacity(zones);
        for (z, room) in rooms.iter().enumerate() {
            let mut config =
                BotConfig::new(url.clone(), room.clone(), TicketKey::new(secret.as_bytes()));
            config.path = "/zones".into();
            config.bots = n;
            config.input_rate = args.rate;
            config.ramp_up = Duration::from_secs(args.ramp_secs);
            // The last bot joins at the end of the ramp; all hold together.
            config.duration = Duration::from_secs(args.hold_secs + args.ramp_secs);
            config.principal_prefix = format!("load{step}z{z}");
            config.seed = (step * 10_000 + z) as u64 + 1;
            config.encoder = if args.no_attack {
                Arc::new(encode_bot_move)
            } else {
                Arc::new(encode_bot_input)
            };
            let moves = args.moves.clone();
            fleets.push(tokio::spawn(run_bots(config, move |index| {
                behaviour(&moves, index)
            })));
        }
        let info_urls: Vec<String> = rooms
            .iter()
            .map(|room| format!("{info_base}/zones/{room}/info"))
            .collect();
        let cell_urls: Vec<String> = match &internal {
            Some(_) => watched_cells(&rooms)
                .iter()
                .map(|cell| format!("{info_base}/{cell}/{STATS_ROUTE}"))
                .collect(),
            None => Vec::new(),
        };

        // Window: from shortly after the last join to shortly before the
        // first leave.
        tokio::time::sleep(Duration::from_secs(args.ramp_secs + 2)).await;
        let start = get_all(&info_urls).await;
        let cells_start = get_cells(&cell_urls, internal.as_deref()).await;
        let window_start = unix_secs();
        let cpu_start = (celld_pid.and_then(tree_usage), std::time::Instant::now());
        tokio::time::sleep(Duration::from_secs(args.hold_secs.saturating_sub(5))).await;
        let stats = match &args.stats_cmd {
            Some(cmd) => run_stats(cmd).await,
            None => String::new(),
        };
        let end = get_all(&info_urls).await;
        let cells_end = get_cells(&cell_urls, internal.as_deref()).await;
        let window = (window_start, unix_secs());
        let usage = match (cpu_start, celld_pid.and_then(tree_usage)) {
            ((Some(before), at), Some(after)) => {
                Some(((after.0 - before.0) / at.elapsed().as_secs_f64(), after.1))
            }
            _ => None,
        };
        let mut summary = Summary {
            bots: Vec::new(),
            elapsed: Duration::ZERO,
        };
        // A bot stuck connecting would hold the step forever.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        for fleet in fleets {
            let Ok(part) = tokio::time::timeout_at(deadline, fleet).await else {
                eprintln!("bots still running 30 s after the hold; step cut short");
                continue;
            };
            let part = part.map_err(|e| format!("bots: {e}"))?;
            summary.bots.extend(part.bots);
            summary.elapsed = summary.elapsed.max(part.elapsed);
        }

        let mut row = Row::new((zones, n), &start, &end, &summary, usage, stats, window);
        row.cells = cells_since(&cells_start, &cells_end);
        println!("{tag}{}", row.line());
        println!("{tag}{:>9} {}", "", row.work_line());
        if let Some(out) = json_out.as_mut() {
            use std::io::Write;
            let mut json = row.json();
            json["run"] = json!(format!("{label}{pair}"));
            json["vars"] = json!(extra);
            writeln!(out, "{json}").map_err(|e| e.to_string())?;
        }
        rows.push(row);
    }
    Ok(rows)
}

/// The cells the rooms watch (`kind/shard`): each room's hood and
/// borough, the city, the content rollout and the dev settings.
fn watched_cells(rooms: &[String]) -> Vec<String> {
    let mut cells = std::collections::BTreeSet::new();
    for room in rooms {
        let Some(at) = parse_room(room) else {
            continue;
        };
        let hood = hood_of(at);
        cells.insert(format!("{HOOD_KIND}/{}", pair_shard(hood)));
        cells.insert(format!("{BOROUGH_KIND}/{}", pair_shard(borough_of(hood))));
    }
    cells.insert(format!("{CITY_KIND}/{CITY_SHARD}"));
    cells.insert(format!("{CONTENT_ROLLOUT_KIND}/world"));
    cells.insert(format!("{ZONE_DEV_KIND}/{NETSIM_SHARD}"));
    cells.into_iter().collect()
}

/// Each cell's [`WatcherStats`] (`None`: unreadable).
async fn get_cells(urls: &[String], secret: Option<&str>) -> Vec<Option<WatcherStats>> {
    let Some(secret) = secret else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(urls.len());
    for url in urls {
        let stats = client()
            .get(url)
            .header(CELL_INTERNAL_SECRET_HEADER, secret)
            .send()
            .await
            .ok()
            .filter(|r| r.status().is_success());
        out.push(match stats {
            Some(response) => response.json().await.ok(),
            None => None,
        });
    }
    out
}

/// The cells' work over the window, summed (`None` without readings).
fn cells_since(
    start: &[Option<WatcherStats>],
    end: &[Option<WatcherStats>],
) -> Option<WatcherStats> {
    let mut sum = WatcherStats::default();
    let mut any = false;
    for (start, end) in start.iter().zip(end) {
        // A cell that started in the window counts from zero.
        let (start, Some(end)) = (start.unwrap_or_default(), end) else {
            continue;
        };
        if end.reads < start.reads {
            continue;
        }
        any = true;
        sum.reads += end.reads - start.reads;
        sum.published += end.published - start.published;
        sum.skipped += end.skipped - start.skipped;
        sum.pushes += end.pushes - start.pushes;
    }
    any.then_some(sum)
}

fn median(mut values: Vec<f64>) -> Option<f64> {
    values.retain(|v| v.is_finite());
    values.sort_by(f64::total_cmp);
    let n = values.len();
    match n {
        0 => None,
        _ if n % 2 == 1 => Some(values[n / 2]),
        _ => Some((values[n / 2 - 1] + values[n / 2]) / 2.0),
    }
}

/// Per step and metric: A's median, B's median and the median of the
/// per-pair change (B vs A of the same pair).
fn print_ab(steps: &[(usize, usize)], a: &[Vec<Row>], b: &[Vec<Row>]) {
    let fmt = |v: Option<f64>| match v {
        None => "—".to_owned(),
        Some(v) if v.abs() >= 100.0 => format!("{v:.0}"),
        Some(v) => format!("{v:.2}"),
    };
    for (i, &(zones, n)) in steps.iter().enumerate() {
        println!("\nA/B {zones}x{n}: median A · median B · median pair change");
        let names: Vec<&str> = Row::metric_names().to_vec();
        for (m, name) in names.iter().enumerate() {
            let side = |runs: &[Vec<Row>]| -> Vec<Option<f64>> {
                runs.iter()
                    .map(|rows| rows.get(i).and_then(|row| row.metrics()[m]))
                    .collect()
            };
            let (sa, sb) = (side(a), side(b));
            let changes: Vec<f64> = sa
                .iter()
                .zip(&sb)
                .filter_map(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) if *a != 0.0 => Some((b - a) / a * 100.0),
                    _ => None,
                })
                .collect();
            let change = median(changes).map_or("—".to_owned(), |c| format!("{c:+.0} %"));
            println!(
                "  {name:>16} {:>10} {:>10} {:>8}",
                fmt(median(sa.into_iter().flatten().collect())),
                fmt(median(sb.into_iter().flatten().collect())),
                change
            );
        }
    }
}

/// Random walkers cross the room; circlers stay near where they spawn.
fn behaviour(moves: &str, index: usize) -> Box<dyn Behaviour> {
    let walk = match moves {
        "walk" => true,
        "circle" => false,
        _ => index.is_multiple_of(2),
    };
    if walk {
        Box::new(RandomWalk::new(0.05))
    } else {
        Box::new(Circle {
            period: Duration::from_millis(3000 + 250 * (index % 8) as u64),
            phase: index as f32 * 0.7,
        })
    }
}

fn unix_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

/// `ZONESxBOTS` or `BOTS` (one room).
fn parse_step(step: &str) -> Result<(usize, usize), String> {
    let number = |n: &str| {
        n.trim()
            .parse::<usize>()
            .map_err(|e| format!("{step}: {e}"))
    };
    match step.split_once('x') {
        Some((zones, bots)) => Ok((number(zones)?, number(bots)?)),
        None => Ok((1, number(step)?)),
    }
}

/// `zones` rooms in a square grid from `pit.0.0` (row by row).
fn grid(zones: usize) -> Vec<String> {
    let side = (1..).find(|s| s * s >= zones).unwrap_or(1);
    (0..zones)
        .map(|i| format!("pit.{}.{}", i % side, i / side))
        .collect()
}

async fn get_all(urls: &[String]) -> Vec<Value> {
    let mut out = Vec::with_capacity(urls.len());
    for url in urls {
        out.push(get(url).await);
    }
    out
}

/// RTT quantile without each bot's first 5 s (pings every 500 ms): the
/// room may still be waking or admitting the rest of the ramp.
fn steady_rtt(summary: &Summary, q: f64) -> Option<Duration> {
    const SKIP: usize = 10;
    let mut all: Vec<Duration> = summary
        .bots
        .iter()
        .flat_map(|b| b.rtts.iter().skip(SKIP).copied())
        .collect();
    all.sort();
    let i = ((all.len().checked_sub(1)?) as f64 * q).round() as usize;
    all.get(i).copied()
}

/// Bot payload: the direction as a move, no buttons.
fn encode_bot_move(dir: [f32; 2]) -> Vec<u8> {
    let axis = |v: f32| (v.clamp(-1.0, 1.0) * 127.0).round() as i8;
    PitInput {
        mx: axis(dir[0]),
        my: axis(dir[1]),
        ..PitInput::default()
    }
    .to_payload()
}

fn client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("http client")
    })
}

async fn get(url: &str) -> Value {
    match client().get(url).send().await {
        Ok(response) => response.json().await.unwrap_or(Value::Null),
        Err(_) => Value::Null,
    }
}

/// CPU seconds used so far and resident MB of `root` and its descendants.
fn tree_usage(root: u32) -> Option<(f64, f64)> {
    let out = std::process::Command::new("ps")
        .args(["-A", "-o", "pid=,ppid=,time=,rss="])
        .output()
        .ok()?;
    let rows: Vec<(u32, u32, f64, f64)> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| {
            let mut f = line.split_whitespace();
            let pid = f.next()?.parse().ok()?;
            let ppid = f.next()?.parse().ok()?;
            let time = cpu_seconds(f.next()?)?;
            let rss_kb: f64 = f.next()?.parse().ok()?;
            Some((pid, ppid, time, rss_kb))
        })
        .collect();
    let mut tree = vec![root];
    let mut i = 0;
    while i < tree.len() {
        let parent = tree[i];
        tree.extend(rows.iter().filter(|r| r.1 == parent).map(|r| r.0));
        i += 1;
    }
    let mine = rows.iter().filter(|r| tree.contains(&r.0));
    let (cpu, rss) = mine.fold((0.0, 0.0), |(c, m), r| (c + r.2, m + r.3));
    (cpu > 0.0).then_some((cpu, rss / 1024.0))
}

/// `ps` cumulative time: `[[dd-]hh:]mm:ss.ss`.
fn cpu_seconds(time: &str) -> Option<f64> {
    let (days, rest) = match time.split_once('-') {
        Some((d, rest)) => (d.parse::<f64>().ok()?, rest),
        None => (0.0, time),
    };
    let seconds = rest.split(':').try_fold(0.0, |acc, part| {
        part.parse::<f64>().ok().map(|v| acc * 60.0 + v)
    })?;
    Some(days * 86_400.0 + seconds)
}

async fn run_stats(cmd: &str) -> String {
    match tokio::process::Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .output()
        .await
    {
        Ok(out) => String::from_utf8_lossy(&out.stdout)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" "),
        Err(e) => format!("stats: {e}"),
    }
}

struct Row {
    zones: usize,
    per_zone: usize,
    bots: usize,
    joined: usize,
    failed: usize,
    first_error: Option<String>,
    players: u64,
    dropped_pct: Option<f64>,
    late_p99: f64,
    rtt_p50: Option<f64>,
    rtt_p99: Option<f64>,
    bytes_per_bot: f64,
    /// celld cores busy over the hold and resident MB (local runs).
    usage: Option<(f64, f64)>,
    stats: String,
    /// Measured window, Unix seconds (to line up external samples).
    window: (f64, f64),
    /// The rooms' work over the window, summed, and how many rooms it
    /// covers (rooms whose counters restarted are left out).
    work: Option<(WorkCounters, usize)>,
    /// The watched cells' work over the window, summed.
    cells: Option<WatcherStats>,
}

impl Row {
    fn new(
        (zones, per_zone): (usize, usize),
        start: &[Value],
        end: &[Value],
        summary: &Summary,
        usage: Option<(f64, f64)>,
        stats: String,
        window: (f64, f64),
    ) -> Self {
        let num = |v: &Value, key: &str| v[key].as_f64().unwrap_or(0.0);
        // Per room; counters restart with the activation, so only compare
        // within one. A room without both readings has no number (worst).
        let dropped = start.iter().zip(end).map(|(start, end)| {
            let same = start["activation"] == end["activation"] && !end["activation"].is_null();
            let iterations = num(end, "iterations") - num(start, "iterations");
            (same && iterations > 0.0).then(|| {
                (num(end, "droppedTicks") - num(start, "droppedTicks")) / iterations * 100.0
            })
        });
        let dropped_pct = dropped
            .collect::<Option<Vec<f64>>>()
            .map(|all| all.into_iter().fold(0.0, f64::max));
        let ms = |d: Option<Duration>| d.map(|d| d.as_secs_f64() * 1000.0);
        let counters = |v: &Value| serde_json::from_value::<WorkCounters>(v["work"].clone()).ok();
        let mut work: Option<(WorkCounters, usize)> = None;
        for (start, end) in start.iter().zip(end) {
            let Some(done) = counters(end).and_then(|end| end.since(&counters(start)?)) else {
                continue;
            };
            let (sum, rooms) = work.get_or_insert_with(Default::default);
            sum.add(&done);
            *rooms += 1;
        }
        Self {
            zones,
            per_zone,
            bots: zones * per_zone,
            joined: summary.joined(),
            failed: summary.failed(),
            first_error: summary.bots.iter().find_map(|b| b.error.clone()),
            players: end.iter().map(|e| e["players"].as_u64().unwrap_or(0)).sum(),
            dropped_pct,
            late_p99: end
                .iter()
                .map(|e| e["lateMs"]["p99"].as_f64().unwrap_or(0.0))
                .fold(0.0, f64::max),
            rtt_p50: ms(steady_rtt(summary, 0.5)),
            rtt_p99: ms(steady_rtt(summary, 0.99)),
            bytes_per_bot: summary.delta_bytes_per_sec_per_bot(),
            usage,
            stats,
            window,
            work,
            cells: None,
        }
    }

    /// Names of [`metrics`](Self::metrics), in order.
    fn metric_names() -> [&'static str; 18] {
        [
            "dropped %",
            "late p99 ms",
            "rtt p99 ms",
            "B/s/bot",
            "celld cores",
            "enc/patch",
            "sent/patch",
            "held %",
            "B/frame",
            "facts/room-s",
            "req/room-s",
            "deletes/room-s",
            "facts/req",
            "deletes/fact",
            "reads/room-min",
            "pushes/room-min",
            "cell pushed/min",
            "cell skipped/min",
        ]
    }

    /// The numbers A/B compares (see [`metric_names`](Self::metric_names)).
    fn metrics(&self) -> [Option<f64>; 18] {
        let secs = (self.window.1 - self.window.0).max(1e-3);
        let w = self.work.map(|(w, rooms)| (w, rooms as f64 * secs));
        let per = |num: fn(&WorkCounters) -> u64, den: fn(&WorkCounters) -> u64| {
            w.and_then(|(w, _)| (den(&w) > 0).then(|| num(&w) as f64 / den(&w) as f64))
        };
        let per_room_s =
            |num: fn(&WorkCounters) -> u64| w.map(|(w, room_s)| num(&w) as f64 / room_s);
        let cell_per_min =
            |num: fn(&WatcherStats) -> u64| self.cells.map(|c| num(&c) as f64 * 60.0 / secs);
        [
            self.dropped_pct,
            Some(self.late_p99),
            self.rtt_p99,
            Some(self.bytes_per_bot),
            self.usage.map(|u| u.0),
            per(|w| w.encodes, |w| w.patches),
            per(|w| w.creates + w.updates, |w| w.patches),
            per(|w| w.held * 100, |w| w.held + w.updates),
            per(|w| w.delta_bytes, |w| w.frames),
            per_room_s(|w| w.fact_commands),
            per_room_s(|w| w.fact_requests),
            per_room_s(|w| w.outbox_deletes),
            per(|w| w.fact_commands, |w| w.fact_requests),
            per(|w| w.outbox_deletes, |w| w.fact_commands),
            per_room_s(|w| w.world_reads * 60),
            per_room_s(|w| w.world_pushes * 60),
            cell_per_min(|c| c.pushes),
            cell_per_min(|c| c.skipped),
        ]
    }

    /// The work line under the row.
    fn work_line(&self) -> String {
        let Some((w, rooms)) = self.work else {
            return "work: — (no counters: an older worker build?)".into();
        };
        let m = self.metrics();
        let opt =
            |v: Option<f64>, digits: usize| v.map_or("—".to_owned(), |v| format!("{v:.digits$}"));
        let mut line = format!(
            "work ({rooms} rooms): enc/patch {} · sent/patch {} · held {}% · B/frame {} · \
             facts/room-s {} · req/room-s {} ({} batches, {} facts/req) · deletes/room-s {} · \
             reads/room-min {} · pushes/room-min {}",
            opt(m[5], 1),
            opt(m[6], 1),
            opt(m[7], 0),
            opt(m[8], 0),
            opt(m[9], 2),
            opt(m[10], 2),
            w.fact_batches,
            opt(m[12], 2),
            opt(m[11], 2),
            opt(m[14], 1),
            opt(m[15], 1),
        );
        if let Some(c) = self.cells {
            line.push_str(&format!(
                " · cells/min: pushed {} skipped {} reads {}",
                opt(m[16], 0),
                opt(m[17], 0),
                opt(
                    Some(c.reads as f64 * 60.0 / (self.window.1 - self.window.0).max(1e-3)),
                    0
                ),
            ));
        }
        line
    }

    fn degraded(&self) -> bool {
        self.joined < self.bots
            || self.dropped_pct.is_none_or(|d| d > 1.0)
            || self.late_p99 > 50.0
            || self.rtt_p99.is_none_or(|r| r > 150.0)
    }

    fn line(&self) -> String {
        let opt =
            |v: Option<f64>, digits: usize| v.map_or("—".to_owned(), |v| format!("{v:.digits$}"));
        let mut line = format!(
            "{:>9} {:>6} {:>6} {:>6} {:>7}% {:>9.1} {:>8} {:>8} {:>9.0} {:>6} {:>7}  {}",
            format!("{}x{}", self.zones, self.per_zone),
            self.bots,
            self.joined,
            self.players,
            opt(self.dropped_pct, 2),
            self.late_p99,
            opt(self.rtt_p50, 1),
            opt(self.rtt_p99, 1),
            self.bytes_per_bot,
            opt(self.usage.map(|u| u.0), 2),
            opt(self.usage.map(|u| u.1), 0),
            self.stats
        );
        if self.degraded() {
            line.push_str("  DEGRADED");
        }
        if let Some(error) = &self.first_error {
            line.push_str(&format!(" ({} failed: {error})", self.failed));
        }
        line
    }

    fn json(&self) -> Value {
        json!({
            "zones": self.zones,
            "perZone": self.per_zone,
            "bots": self.bots,
            "joined": self.joined,
            "failed": self.failed,
            "firstError": self.first_error,
            "players": self.players,
            "droppedPct": self.dropped_pct,
            "lateP99Ms": self.late_p99,
            "rttP50Ms": self.rtt_p50,
            "rttP99Ms": self.rtt_p99,
            "bytesPerSecPerBot": self.bytes_per_bot,
            "celldCores": self.usage.map(|u| u.0),
            "celldRssMb": self.usage.map(|u| u.1),
            "stats": self.stats,
            "windowStart": self.window.0,
            "windowEnd": self.window.1,
            "degraded": self.degraded(),
            "work": self.work.map(|(w, _)| w),
            "workRooms": self.work.map(|(_, rooms)| rooms),
            "cells": self.cells,
            "metrics": Row::metric_names()
                .iter()
                .zip(self.metrics())
                .map(|(name, value)| (name.to_string(), json!(value)))
                .collect::<serde_json::Map<_, _>>(),
        })
    }
}
