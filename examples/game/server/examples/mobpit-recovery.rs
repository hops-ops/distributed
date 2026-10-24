//! Crash recovery on a celld fleet: how long until every Mob Pit room takes
//! a player again after celld dies, and how the rooms tick afterwards.
//!
//! 1. Wake: one moving bot per room (`--rooms`, a grid from
//!    `pit.{x0}.{y0}`) plays for `--wake-secs`.
//! 2. Kill: runs `--kill-cmd`, or (bots in a cluster, the kill from outside)
//!    prints `READY` and waits for the bots to lose their sockets. The first
//!    lost socket is t0.
//! 3. Recover: from t0 every room is probed with a real join (Hello with a
//!    ticket, until Welcome; retried every 500 ms). A room's recovery time
//!    is t0 → its first Welcome; then a bot stays in it (players come back).
//!    celld's health URL is polled alongside (`--health-url`).
//! 4. Hold: once every room answered (or `--max-secs`), each room's ticks
//!    over `--hold-secs` (dropped %, late p99) from `/zones/{room}/info`.
//!
//! Prints the per-room distribution (p50/p90/max), the order rooms came
//! back in (per 10 s), and with `--json` every room's numbers.
//!
//! ```sh
//! mobpit-recovery --url ws://celld.mobpit-celld.svc.cluster.local:8080 \
//!     --secret "$GAME_TICKET_SECRET" --rooms 64 --wake-secs 60
//! ```

use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::Parser;
use distributed_game::authoring::GameInput;
use distributed_game::bots::{run_bot, BotConfig, Circle};
use distributed_game::net::TicketKey;
use game_demos::mobpit::encode_bot_input;
use serde_json::{json, Value};
use tokio::time::Instant;

#[derive(Parser, Debug)]
#[command(about = "Time Mob Pit rooms' recovery after celld dies")]
struct Args {
    /// celld public listener (ws://…).
    #[arg(long)]
    url: String,
    /// Room info base (http://…); default: `--url` with http.
    #[arg(long)]
    info_url: Option<String>,
    /// Ticket secret of the worker.
    #[arg(long, env = "GAME_TICKET_SECRET")]
    secret: String,
    /// Rooms (one bot each).
    #[arg(long, default_value_t = 64)]
    rooms: usize,
    /// Grid origin: rooms are `pit.{x0 + i % side}.{y0 + i / side}`.
    #[arg(long, default_value_t = 0, allow_hyphen_values = true)]
    x0: i32,
    #[arg(long, default_value_t = 0, allow_hyphen_values = true)]
    y0: i32,
    /// Seconds the bots play before the kill.
    #[arg(long, default_value_t = 60)]
    wake_secs: u64,
    /// Seconds over which the wake bots join.
    #[arg(long, default_value_t = 10)]
    ramp_secs: u64,
    /// Shell command that kills celld (else wait for an outside kill).
    #[arg(long)]
    kill_cmd: Option<String>,
    /// How long to wait for an outside kill.
    #[arg(long, default_value_t = 900)]
    kill_wait_secs: u64,
    /// Give up on rooms that have not answered this long after t0.
    #[arg(long, default_value_t = 900)]
    max_secs: u64,
    /// Tick window once the rooms are back.
    #[arg(long, default_value_t = 30)]
    hold_secs: u64,
    /// Polled during recovery (first 200 after t0 is reported).
    #[arg(long)]
    health_url: Option<String>,
    /// Move without holding attack.
    #[arg(long)]
    no_attack: bool,
    /// Hello content version (the game's; a room on another one reads the
    /// world's content rollout before it admits the player).
    #[arg(long, default_value = "emberfall")]
    content_version: String,
    /// Write the result as JSON here.
    #[arg(long)]
    json: Option<std::path::PathBuf>,
}

#[derive(Default, Clone)]
struct Room {
    zone: String,
    /// t0 → Welcome.
    recovered: Option<Duration>,
    attempts: u32,
    last_error: Option<String>,
    /// Join latency of the successful attempt (connect → Welcome).
    join: Option<Duration>,
}

fn unix_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

fn grid(n: usize, x0: i32, y0: i32) -> Vec<String> {
    let side = (1..).find(|s| s * s >= n).unwrap_or(1);
    (0..n)
        .map(|i| format!("pit.{}.{}", x0 + (i % side) as i32, y0 + (i / side) as i32))
        .collect()
}

fn config(args: &Args, zone: &str, prefix: String, seed: u64, duration: Duration) -> BotConfig {
    let mut config = BotConfig::new(
        args.url.clone(),
        zone.to_owned(),
        TicketKey::new(args.secret.as_bytes()),
    );
    config.path = "/zones".into();
    config.bots = 1;
    config.input_rate = 30.0;
    config.duration = duration;
    config.principal_prefix = prefix;
    config.seed = seed;
    config.content_version = args.content_version.clone();
    config.encoder = if args.no_attack {
        Arc::new(encode_move)
    } else {
        Arc::new(encode_bot_input)
    };
    config
}

fn encode_move(dir: [f32; 2]) -> Vec<u8> {
    let axis = |v: f32| (v.clamp(-1.0, 1.0) * 127.0).round() as i8;
    game_sim::mobpit::PitInput {
        mx: axis(dir[0]),
        my: axis(dir[1]),
        ..Default::default()
    }
    .to_payload()
}

fn circle(index: usize) -> Box<Circle> {
    Box::new(Circle {
        period: Duration::from_millis(3000 + 250 * (index % 8) as u64),
        phase: index as f32 * 0.7,
    })
}

const FOREVER: Duration = Duration::from_secs(24 * 3600);

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), String> {
    let args = Arc::new(Args::parse());
    let info_base = args.info_url.clone().unwrap_or_else(|| {
        args.url
            .replacen("ws://", "http://", 1)
            .replacen("wss://", "https://", 1)
    });
    let zones = grid(args.rooms, args.x0, args.y0);
    let run = unix_secs() as u64;

    // 1. Wake.
    println!(
        "waking {} rooms ({} … {}) for {} s",
        zones.len(),
        zones[0],
        zones[zones.len() - 1],
        args.wake_secs
    );
    let (lost_tx, mut lost_rx) = tokio::sync::mpsc::unbounded_channel::<(usize, Instant)>();
    let mut wake = Vec::new();
    for (i, zone) in zones.iter().enumerate() {
        let delay = args.ramp_secs as f64 * i as f64 / zones.len().max(1) as f64;
        let config = config(&args, zone, format!("rw{run}z{i}"), i as u64 + 1, FOREVER);
        let lost = lost_tx.clone();
        wake.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs_f64(delay)).await;
            let stats = run_bot(&config, 0, circle(i)).await;
            let _ = lost.send((i, Instant::now()));
            stats
        }));
    }
    tokio::time::sleep(Duration::from_secs(args.ramp_secs + 3)).await;
    let infos = get_all(&info_base, &zones).await;
    let players: u64 = infos
        .iter()
        .map(|v| v["players"].as_u64().unwrap_or(0))
        .sum();
    let restored = infos.iter().filter(|v| v["restored"] == true).count();
    println!(
        "{players} players in {} rooms ({restored} restored a snapshot)",
        zones.len()
    );
    tokio::time::sleep(Duration::from_secs(args.wake_secs.saturating_sub(3))).await;
    // A bot that already left (a room refused it) is not the kill.
    let mut early = 0;
    while let Ok((i, _)) = lost_rx.try_recv() {
        early += 1;
        eprintln!("{} lost its bot before the kill", zones[i]);
    }
    if early > 0 {
        eprintln!("{early} rooms lost their bot before the kill");
    }

    // 2. Kill.
    let kill_unix;
    if let Some(cmd) = &args.kill_cmd {
        kill_unix = unix_secs();
        let out = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .output()
            .await
            .map_err(|e| format!("kill: {e}"))?;
        println!("kill: {}", String::from_utf8_lossy(&out.stdout).trim());
    } else {
        kill_unix = 0.0;
        println!("READY {:.3}", unix_secs());
    }
    let wait = Duration::from_secs(args.kill_wait_secs);
    let (first, t0) = tokio::time::timeout(wait, lost_rx.recv())
        .await
        .map_err(|_| "no bot lost its socket: celld was not killed")?
        .ok_or("bots gone")?;
    let t0_unix = unix_secs();
    println!(
        "t0 {t0_unix:.3}: {} lost its socket{}",
        zones[first],
        if kill_unix > 0.0 {
            format!(" ({:.1} s after the kill command)", t0_unix - kill_unix)
        } else {
            String::new()
        }
    );
    for task in &wake {
        task.abort();
    }

    // 3. Recover.
    let rooms: Arc<Mutex<Vec<Room>>> = Arc::new(Mutex::new(
        zones
            .iter()
            .map(|z| Room {
                zone: z.clone(),
                ..Room::default()
            })
            .collect(),
    ));
    let health: Arc<Mutex<Option<Duration>>> = Arc::new(Mutex::new(None));
    let health_task = args.health_url.clone().map(|url| {
        let health = health.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .expect("http client");
            loop {
                let ok = client
                    .get(&url)
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success());
                if ok {
                    *health.lock().unwrap() = Some(t0.elapsed());
                    return;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
    });
    let deadline = t0 + Duration::from_secs(args.max_secs);
    let (back_tx, mut back_rx) = tokio::sync::mpsc::unbounded_channel::<usize>();
    // Rooms whose player was dropped again after it came back.
    let relost: Arc<Mutex<Vec<(usize, f64, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let mut stay = Vec::new();
    for (i, zone) in zones.iter().enumerate() {
        let rooms = rooms.clone();
        let args = args.clone();
        let zone = zone.clone();
        let back = back_tx.clone();
        let relost = relost.clone();
        stay.push(tokio::spawn(async move {
            let mut attempt = 0u32;
            loop {
                if Instant::now() > deadline {
                    return;
                }
                attempt += 1;
                let started = Instant::now();
                let probe = config(
                    &args,
                    &zone,
                    format!("rp{run}z{i}a{attempt}"),
                    i as u64 + 1,
                    Duration::ZERO,
                );
                let stats =
                    tokio::time::timeout(Duration::from_secs(15), run_bot(&probe, 0, circle(i)))
                        .await;
                let ok = match stats {
                    Ok(stats) if stats.joined => {
                        let join = stats.joined_after.unwrap_or_default();
                        let mut rooms = rooms.lock().unwrap();
                        let room = &mut rooms[i];
                        room.recovered = Some((started + join) - t0);
                        room.join = Some(join);
                        room.attempts = attempt;
                        true
                    }
                    Ok(stats) => {
                        let mut rooms = rooms.lock().unwrap();
                        rooms[i].last_error = stats.error;
                        rooms[i].attempts = attempt;
                        false
                    }
                    Err(_) => {
                        let mut rooms = rooms.lock().unwrap();
                        rooms[i].last_error = Some("no Welcome in 15 s".into());
                        rooms[i].attempts = attempt;
                        false
                    }
                };
                if ok {
                    let _ = back.send(i);
                    break;
                }
                let next = started + Duration::from_millis(500);
                tokio::time::sleep_until(next).await;
            }
            // Back: a player stays (until the hold ends).
            let config = config(&args, &zone, format!("rs{run}z{i}"), i as u64 + 7, FOREVER);
            let stats = run_bot(&config, 0, circle(i)).await;
            relost.lock().unwrap().push((
                i,
                t0.elapsed().as_secs_f64(),
                stats.error.unwrap_or_else(|| "no Welcome".into()),
            ));
        }));
    }
    drop(back_tx);
    let mut back = 0;
    let mut last_print = Instant::now();
    while back < zones.len() {
        match tokio::time::timeout_at(deadline, back_rx.recv()).await {
            Ok(Some(_)) => back += 1,
            Ok(None) | Err(_) => break,
        }
        if last_print.elapsed() > Duration::from_secs(10) {
            last_print = Instant::now();
            println!(
                "  {:>5.0} s: {back}/{} rooms back",
                t0.elapsed().as_secs_f64(),
                zones.len()
            );
        }
    }
    let all_back = t0.elapsed();
    if let Some(task) = health_task {
        task.abort();
    }
    println!(
        "{back}/{} rooms back after {:.1} s",
        zones.len(),
        all_back.as_secs_f64()
    );

    // 4. Hold.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let start = get_all(&info_base, &zones).await;
    tokio::time::sleep(Duration::from_secs(args.hold_secs)).await;
    let end = get_all(&info_base, &zones).await;
    for task in &stay {
        task.abort();
    }

    let rooms = rooms.lock().unwrap().clone();
    let relost = relost.lock().unwrap().clone();
    if !relost.is_empty() {
        let first = relost.iter().map(|r| r.1).fold(f64::MAX, f64::min);
        println!(
            "{} rooms lost their player again after recovering (first at {first:.1} s: {} — {})",
            relost.len(),
            zones[relost[0].0],
            relost[0].2
        );
    }
    let mut times: Vec<f64> = rooms
        .iter()
        .filter_map(|r| r.recovered.map(|d| d.as_secs_f64()))
        .collect();
    times.sort_by(f64::total_cmp);
    let q = |q: f64| -> Option<f64> {
        let i = ((times.len().checked_sub(1)?) as f64 * q).round() as usize;
        times.get(i).copied()
    };
    let fmt = |v: Option<f64>| v.map_or("—".to_owned(), |v| format!("{v:.1}"));
    let health = *health.lock().unwrap();
    println!(
        "recovery s: min {} p10 {} p50 {} p90 {} max {} · celld health {}",
        fmt(q(0.0)),
        fmt(q(0.1)),
        fmt(q(0.5)),
        fmt(q(0.9)),
        fmt(q(1.0)),
        fmt(health.map(|d| d.as_secs_f64()))
    );
    // Rooms back per 10 s.
    let mut buckets: Vec<usize> = Vec::new();
    for t in &times {
        let b = (*t / 10.0) as usize;
        if buckets.len() <= b {
            buckets.resize(b + 1, 0);
        }
        buckets[b] += 1;
    }
    let histogram: Vec<String> = buckets
        .iter()
        .enumerate()
        .filter(|(_, n)| **n > 0)
        .map(|(b, n)| format!("{}-{}s:{n}", b * 10, b * 10 + 10))
        .collect();
    println!("rooms back per 10 s: {}", histogram.join(" "));
    let missing: Vec<&Room> = rooms.iter().filter(|r| r.recovered.is_none()).collect();
    for r in missing.iter().take(10) {
        println!(
            "  {} never answered ({} attempts; last: {})",
            r.zone,
            r.attempts,
            r.last_error.as_deref().unwrap_or("?")
        );
    }
    let attempts: u32 = rooms.iter().map(|r| r.attempts).sum();
    let mut errors: std::collections::BTreeMap<String, usize> = Default::default();
    for r in &rooms {
        if let Some(e) = &r.last_error {
            *errors.entry(short(e)).or_default() += 1;
        }
    }
    println!("{attempts} join attempts; last failure per room: {errors:?}");

    // Ticks over the hold.
    let num = |v: &Value, key: &str| v[key].as_f64().unwrap_or(0.0);
    let mut drops = Vec::new();
    let mut lates = Vec::new();
    for (s, e) in start.iter().zip(&end) {
        let same = s["activation"] == e["activation"] && !e["activation"].is_null();
        let iterations = num(e, "iterations") - num(s, "iterations");
        if same && iterations > 0.0 {
            drops.push((num(e, "droppedTicks") - num(s, "droppedTicks")) / iterations * 100.0);
        }
        lates.push(e["lateMs"]["p99"].as_f64().unwrap_or(0.0));
    }
    drops.sort_by(f64::total_cmp);
    lates.sort_by(f64::total_cmp);
    let mid = |v: &[f64]| v.get(v.len() / 2).copied();
    println!(
        "hold {} s ({} rooms measured): dropped % median {} worst {} · late p99 ms median {} worst {}",
        args.hold_secs,
        drops.len(),
        fmt(mid(&drops).map(|d| d * 10.0).map(|d| d / 10.0)),
        fmt(drops.last().copied()),
        fmt(mid(&lates)),
        fmt(lates.last().copied())
    );
    let restored = end.iter().filter(|v| v["restored"] == true).count();
    println!("{restored}/{} rooms restored a snapshot", zones.len());

    if let Some(path) = &args.json {
        let out = json!({
            "rooms": zones.len(),
            "t0": t0_unix,
            "killUnix": kill_unix,
            "allBackSecs": all_back.as_secs_f64(),
            "healthSecs": health.map(|d| d.as_secs_f64()),
            "recoveredSecs": rooms.iter().map(|r| json!({
                "zone": r.zone,
                "secs": r.recovered.map(|d| d.as_secs_f64()),
                "joinMs": r.join.map(|d| d.as_secs_f64() * 1000.0),
                "attempts": r.attempts,
                "lastError": r.last_error,
            })).collect::<Vec<_>>(),
            "holdDroppedPct": drops,
            "holdLateP99Ms": lates,
            "relost": relost.iter().map(|r| json!({"zone": zones[r.0], "secs": r.1, "error": r.2})).collect::<Vec<_>>(),
            "infoEnd": end,
        });
        std::fs::write(path, serde_json::to_string_pretty(&out).unwrap_or_default())
            .map_err(|e| format!("{}: {e}", path.display()))?;
    }
    Ok(())
}

/// An error without its ids (for counting kinds).
fn short(e: &str) -> String {
    e.chars()
        .take_while(|c| *c != '(' && *c != ':')
        .collect::<String>()
        .trim()
        .to_owned()
        + &e.split_once(':')
            .map(|(_, rest)| format!(":{}", rest.chars().take(40).collect::<String>()))
            .unwrap_or_default()
}

async fn get_all(base: &str, zones: &[String]) -> Vec<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("http client");
    let tasks: Vec<_> = zones
        .iter()
        .map(|zone| {
            let client = client.clone();
            let url = format!("{base}/zones/{zone}/info");
            tokio::spawn(async move {
                match client.get(&url).send().await {
                    Ok(r) => r.json::<Value>().await.unwrap_or(Value::Null),
                    Err(_) => Value::Null,
                }
            })
        })
        .collect();
    let mut out = Vec::with_capacity(tasks.len());
    for task in tasks {
        out.push(task.await.unwrap_or(Value::Null));
    }
    out
}
