//! Mob Pit + Leaderboard smoke tests.
//!
//! 1. Facts → native SQLite cells exactly once (ledger replay), server-side
//!    XP, trusted principal; leaderboard occurrences have the same ids live
//!    and from history, so rebuild + replay never double count.
//! 2. Bots fight in-process; with 1.5 s of storage latency the tick stays
//!    flat while pending facts climb, then everything settles and the
//!    boards equal the cells (and a rebuild changes nothing).
//! 3. Crash recovery with the real binary: `kill -9` mid-fight with facts in
//!    flight, restart on the same data dir, verify the cells and that a
//!    replayed pre-crash fact key does not double-credit.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use distributed::leaderboard::LeaderboardStore;
use distributed_game::bots::{run_bots, Behaviour, BotConfig};
use distributed_game::net::TicketKey;
use distributed_game::{Fact, FactError, FactSink, FactStamp, ZoneRng};
use game_demos::common::{random_hex, DemoOptions, RunningDemo};
use game_demos::mobpit::board::{kill_occurrence, MobKilled};
use game_demos::mobpit::crawler::RECORD_KILL;
use game_demos::mobpit::{self, encode_bot_input, MobPitDemo, MobPitWorld, WorldOptions};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

const SECRET: &str = "mobpit-smoke-secret-0123456789abcdef";

/// Walk slowly while turning: the swing cone sweeps around, chasing mobs get hit.
struct Spin {
    period: Duration,
    phase: f32,
}

impl Behaviour for Spin {
    fn direction(&mut self, elapsed: Duration, _rng: &mut ZoneRng) -> [f32; 2] {
        let a =
            self.phase + elapsed.as_secs_f32() / self.period.as_secs_f32() * std::f32::consts::TAU;
        [0.35 * a.cos(), 0.35 * a.sin()]
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("mobpit-{tag}-{}", random_hex(10)))
}

async fn http(addr: SocketAddr, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let status = response[9..12].parse().unwrap();
    let json = response
        .split_once("\r\n\r\n")
        .and_then(|(_, b)| serde_json::from_str(b).ok())
        .unwrap_or(Value::Null);
    (status, json)
}

async fn eventually<F: std::future::Future<Output = bool>>(
    what: &str,
    timeout: Duration,
    mut check: impl FnMut() -> F,
) {
    let start = Instant::now();
    while !check().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn kill_fact(key: &str, principal: &str, input: Value) -> Fact {
    Fact::trusted(
        key,
        RECORD_KILL,
        input,
        Some(principal.into()),
        FactStamp::default(),
    )
}

/// Every cell's (kills, xp) equals both boards' scores.
async fn assert_board_matches_cells(world: &MobPitWorld) -> u64 {
    let mut total = 0;
    for c in world.crawlers().await.unwrap() {
        let principal = c["principal"].as_str().unwrap();
        let (kills, xp) = world.board().scores(principal).await.unwrap();
        assert_eq!(kills, c["kills"].as_f64().unwrap(), "kills of {principal}");
        assert_eq!(xp, c["xp"].as_f64().unwrap(), "xp of {principal}");
        total += c["kills"].as_u64().unwrap();
    }
    total
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn facts_commit_once_and_board_rebuilds_identically() {
    let dir = temp_dir("unit");
    let world = MobPitWorld::open(WorldOptions::new(&dir)).unwrap();
    let sink = world.fact_sink();
    let input = json!({ "mob": "pit-1/m3", "tier": 2, "name": "Ana" });

    let first = sink
        .commit(&kill_fact("kill:t:pit-1:m3:0", "guest-ana", input.clone()))
        .await
        .unwrap();
    assert!(!first.replayed);
    assert_eq!(
        first.payload["xp"], 25,
        "XP decided by the cell from the tier"
    );
    assert_eq!(
        first.payload["crawler"], "guest-ana",
        "principal from the session"
    );
    let replay = sink
        .commit(&kill_fact("kill:t:pit-1:m3:0", "guest-ana", input.clone()))
        .await
        .unwrap();
    assert!(replay.replayed, "same key replays the receipt");
    assert_eq!(replay.payload, first.payload);

    // Inputs cannot carry identity or rewards; facts need a principal.
    let forged = sink
        .commit(&kill_fact(
            "kill:t:pit-1:m4:0",
            "guest-ana",
            json!({ "mob": "pit-1/m4", "tier": 1, "name": "Ana", "xp": 9999 }),
        ))
        .await;
    assert!(matches!(forged, Err(FactError::Permanent(_))), "{forged:?}");
    let bad_tier = sink
        .commit(&kill_fact(
            "kill:t:pit-1:m5:0",
            "guest-ana",
            json!({ "mob": "pit-1/m5", "tier": 9, "name": "Ana" }),
        ))
        .await;
    assert!(matches!(bad_tier, Err(FactError::Permanent(_))));
    let anonymous = Fact::trusted("k", RECORD_KILL, input.clone(), None, FactStamp::default());
    assert!(matches!(
        sink.commit(&anonymous).await,
        Err(FactError::Permanent(_))
    ));
    sink.commit(&kill_fact(
        "kill:t:pit-1:m6:0",
        "guest-bo",
        json!({ "mob": "pit-1/m6", "tier": 3, "name": "Bo" }),
    ))
    .await
    .unwrap();

    let cell = world.cells().get("guest-ana").unwrap();
    let ana = cell.load().await.unwrap().unwrap();
    assert_eq!((ana.kills, ana.xp, ana.level), (1, 25, 1), "credited once");
    let history = cell.history().await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(
        history[0].sequence,
        first.payload["sequence"].as_u64().unwrap()
    );

    // Live updates are fire-and-forget; the replay was deduplicated.
    eventually("live board", Duration::from_secs(5), || async {
        world.board().scores("guest-bo").await.unwrap() == (1.0, 60.0)
    })
    .await;
    assert_eq!(
        world.board().scores("guest-ana").await.unwrap(),
        (1.0, 25.0)
    );
    assert_eq!(assert_board_matches_cells(&world).await, 2);

    let report = world.rebuild_board().await.unwrap();
    assert_eq!(report["events"], 2);
    assert_eq!(report["applied"], 4, "two boards per kill");
    assert_eq!(report["matchesCells"], true, "{report}");
    assert_eq!(report["identicalTop"], true, "{report}");

    // The live occurrence id equals the history one: replaying it after the
    // rebuild is a duplicate on both boards.
    let occurrence = kill_occurrence(
        "guest-ana",
        first.payload["sequence"].as_u64().unwrap(),
        first.payload["at_ms"].as_u64().unwrap(),
        &MobKilled {
            crawler: "guest-ana".into(),
            mob: "pit-1/m3".into(),
            tier: 2,
            xp: 25,
        },
    )
    .unwrap();
    let again = world
        .board()
        .projector()
        .apply_event(&occurrence)
        .await
        .unwrap();
    assert_eq!((again.applied, again.duplicates), (0, 2));
    assert_eq!(
        world.board().scores("guest-ana").await.unwrap(),
        (1.0, 25.0)
    );
    let (kills, _) = world.board().boards();
    assert_eq!(world.board().store().len(kills).await.unwrap(), 2);
    let _ = std::fs::remove_dir_all(dir);
}

async fn start(
    world: Arc<MobPitWorld>,
) -> (RunningDemo<MobPitDemo>, tokio::sync::oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let running = mobpit::spawn(listener, DemoOptions::dev(SECRET), world, async move {
        let _ = stopped.await;
    })
    .unwrap();
    (running, stop)
}

fn bot_config(ws: String, bots: usize, duration: Duration) -> BotConfig {
    let mut config = BotConfig::new(ws, "pit-1", TicketKey::new(SECRET));
    config.bots = bots;
    config.input_rate = 30.0;
    config.duration = duration;
    config.encoder = Arc::new(encode_bot_input);
    config
}

fn spin(i: usize) -> Box<dyn Behaviour> {
    Box::new(Spin {
        period: Duration::from_millis(900 + 150 * i as u64),
        phase: i as f32,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bots_fight_slow_storage_keeps_the_tick_flat_and_boards_match_cells() {
    let dir = temp_dir("bots");
    let world = MobPitWorld::open(WorldOptions::new(&dir)).unwrap();
    let (demo, stop) = start(world.clone()).await;

    // Unknown fields (e.g. an identity) are rejected on the dev routes too.
    let (status, _) = http(
        demo.addr,
        "POST",
        "/dev/mobpit/storage",
        Some(json!({ "latencyMs": 1, "principal": "x" })),
    )
    .await;
    assert_eq!(status, 422);

    let server = demo.server.clone();
    let addr = demo.addr;
    let probe = async move {
        // Fight at full speed first, then make storage slow.
        tokio::time::sleep(Duration::from_secs(4)).await;
        let (_, storage) = http(
            addr,
            "POST",
            "/dev/mobpit/storage",
            Some(json!({ "latencyMs": 1500 })),
        )
        .await;
        assert_eq!(storage["latencyMs"], 1500);
        let mut max_pending = 0;
        let mut max_tick: f32 = 0.0;
        let until = Instant::now() + Duration::from_secs(6);
        while Instant::now() < until {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if let Some(s) = server.inspect("pit-1", |h| h.zone().stats()).await {
                max_pending = max_pending.max(s.pending);
                if s.tick_max_ms > 0.0 {
                    max_tick = max_tick.max(s.tick_max_ms);
                }
            }
        }
        (max_pending, max_tick)
    };
    let (summary, (max_pending, max_tick)) = tokio::join!(
        run_bots(bot_config(demo.ws_url(), 8, Duration::from_secs(11)), spin),
        probe
    );
    assert_eq!(
        summary.joined(),
        8,
        "{:?}",
        summary.bots.iter().find(|b| b.error.is_some())
    );
    eprintln!("slow storage: max pending {max_pending}, max tick interval {max_tick:.1} ms");
    assert!(
        max_pending >= 1,
        "slow storage: facts wait (max {max_pending})"
    );
    assert!(
        max_tick < 120.0,
        "the 30 Hz tick never waits for storage (max interval {max_tick} ms)"
    );

    // Back to fast storage: everything settles.
    http(
        demo.addr,
        "POST",
        "/dev/mobpit/storage",
        Some(json!({ "latencyMs": 0 })),
    )
    .await;
    let w = world.clone();
    eventually("facts settled", Duration::from_secs(60), || {
        let w = w.clone();
        async move { w.zone_pending() == 0 && w.sink_state().stats().in_sink == 0 }
    })
    .await;
    let stats = world.sink_state().stats();
    assert!(stats.committed >= 3, "bots killed mobs: {stats:?}");
    assert_eq!(stats.errors, 0, "{stats:?}");

    // Boards = cells (fire-and-forget updates have drained), rebuild = same.
    let w = world.clone();
    eventually("board caught up", Duration::from_secs(10), || {
        let w = w.clone();
        async move {
            let mut ok = true;
            for c in w.crawlers().await.unwrap() {
                let (k, _) = w
                    .board()
                    .scores(c["principal"].as_str().unwrap())
                    .await
                    .unwrap();
                ok &= k == c["kills"].as_f64().unwrap();
            }
            ok
        }
    })
    .await;
    let total = assert_board_matches_cells(&world).await;
    eprintln!("bots: {stats:?}, kills on disk {total}");
    assert_eq!(total, stats.committed, "one kill per committed fact");
    let (_, rebuild) = http(demo.addr, "POST", "/dev/mobpit/leaderboard/rebuild", None).await;
    assert_eq!(rebuild["matchesCells"], true, "{rebuild}");
    assert_eq!(rebuild["identicalTop"], true, "{rebuild}");
    assert_eq!(rebuild["events"].as_u64(), Some(total));

    // Read routes.
    let (_, board) = http(
        demo.addr,
        "GET",
        "/dev/mobpit/leaderboard?member=bot-0",
        None,
    )
    .await;
    assert_eq!(board["snapshot"]["backend"], "memory");
    assert!(!board["snapshot"]["kills"].as_array().unwrap().is_empty());
    let (_, crawlers) = http(demo.addr, "GET", "/dev/mobpit/crawlers", None).await;
    assert_eq!(
        crawlers["crawlers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["kills"].as_u64().unwrap())
            .sum::<u64>(),
        total
    );

    stop.send(()).unwrap();
    demo.task.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

struct Binary {
    child: Child,
    addr: SocketAddr,
}

async fn start_binary(dir: &Path) -> Binary {
    let mut child = Command::new(env!("CARGO_BIN_EXE_game-demos"))
        .args([
            "mobpit",
            "--port",
            "0",
            "--dev",
            "--secret",
            SECRET,
            "--data-dir",
        ])
        .arg(dir)
        .env_remove("REDIS_URL")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn game-demos");
    let stdout = child.stdout.take().unwrap();
    let mut lines =
        tokio::io::BufReader::new(tokio::process::ChildStdout::from_std(stdout).unwrap()).lines();
    let addr = loop {
        let line = tokio::time::timeout(Duration::from_secs(60), lines.next_line())
            .await
            .expect("server start")
            .unwrap()
            .expect("server exited");
        if let Some(url) = line.strip_prefix("LISTENING http://") {
            break url.parse().unwrap();
        }
    };
    // Keep draining stdout so the child never blocks on a full pipe.
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    Binary { child, addr }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_dash_nine_mid_fight_keeps_settled_facts_exactly_once() {
    let dir = temp_dir("crash");
    let mut first = start_binary(&dir).await;
    let ws = format!("ws://{}", first.addr);
    // Settle some kills at full speed, then slow storage so facts are in
    // flight at the moment of the crash.
    let addr = first.addr;
    let fight = tokio::spawn(run_bots(bot_config(ws, 5, Duration::from_secs(30)), spin));
    let start = Instant::now();
    let before = loop {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (_, stats) = http(addr, "GET", "/dev/mobpit/stats", None).await;
        if stats["sink"]["committed"].as_u64().unwrap_or(0) >= 3 {
            break stats;
        }
        assert!(
            start.elapsed() < Duration::from_secs(25),
            "no kills: {stats}"
        );
    };
    http(
        addr,
        "POST",
        "/dev/mobpit/storage",
        Some(json!({ "latencyMs": 1500 })),
    )
    .await;
    let start = Instant::now();
    let at_crash = loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (_, stats) = http(addr, "GET", "/dev/mobpit/stats", None).await;
        if stats["sink"]["inSink"].as_u64().unwrap_or(0) >= 1
            || start.elapsed() > Duration::from_secs(10)
        {
            break stats;
        }
    };
    first.child.kill().unwrap(); // SIGKILL: no graceful drain
    first.child.wait().unwrap();
    fight.abort();
    let settled_before = at_crash["sink"]["committed"].as_u64().unwrap();
    let in_flight = at_crash["pending"].as_u64().unwrap();
    assert!(settled_before >= before["sink"]["committed"].as_u64().unwrap());

    // Restart on the same directory: the settled facts are all there; of the
    // in-flight ones each either committed atomically or not at all.
    let mut second = start_binary(&dir).await;
    let (_, crawlers) = http(second.addr, "GET", "/dev/mobpit/crawlers", None).await;
    let total: u64 = crawlers["crawlers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["kills"].as_u64().unwrap())
        .sum();
    eprintln!(
        "crash: settled {settled_before}, in flight {in_flight}, durable after restart {total}"
    );
    assert!(
        total >= settled_before && total <= settled_before + in_flight,
        "settled {settled_before} ≤ durable {total} ≤ settled + in flight ({in_flight})"
    );
    // The board was rebuilt from the cells at boot.
    let (_, board) = http(second.addr, "GET", "/dev/mobpit/leaderboard", None).await;
    let board_kills: f64 = board["snapshot"]["kills"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["score"].as_f64().unwrap())
        .sum();
    assert_eq!(board_kills as u64, total, "{board}");
    second.child.kill().unwrap();
    second.child.wait().unwrap();

    // Replay a pre-crash fact (same key, same input) against the files: the
    // command ledger replays it, nothing is credited twice.
    let recent = &before["recent"].as_array().unwrap()[0];
    let key = recent["key"].as_str().unwrap();
    let principal = recent["receipt"]["crawler"].as_str().unwrap();
    let world = MobPitWorld::open(WorldOptions::new(&dir)).unwrap();
    let cell = world.cells().get(principal).unwrap();
    let kills_before = cell.load().await.unwrap().unwrap().kills;
    let replay = world
        .fact_sink()
        .commit(&kill_fact(key, principal, recent["input"].clone()))
        .await
        .unwrap();
    assert!(replay.replayed, "pre-crash key replays after the restart");
    assert_eq!(replay.payload, recent["receipt"], "original receipt");
    assert_eq!(cell.load().await.unwrap().unwrap().kills, kills_before);
    let _ = std::fs::remove_dir_all(dir);
}

/// Same projection on Redis sorted sets. Skipped unless `REDIS_URL` is set
/// (`docker compose up -d redis`, then `REDIS_URL=redis://127.0.0.1:6379`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_board_projects_and_rebuilds_when_redis_url_is_set() {
    let Ok(url) = std::env::var("REDIS_URL") else {
        eprintln!("REDIS_URL unset: skipping the Redis leaderboard test");
        return;
    };
    let dir = temp_dir("redis");
    let mut options = WorldOptions::new(&dir);
    options.redis_url = Some(url);
    options.redis_prefix = format!("mobpit-test-{}", random_hex(8));
    let world = MobPitWorld::open(options).unwrap();
    assert_eq!(world.board().backend(), "redis");
    let sink = world.fact_sink();
    for (i, tier) in [1u8, 3].into_iter().enumerate() {
        let input = json!({ "mob": format!("pit-1/m{i}"), "tier": tier, "name": "Rae" });
        let fact = kill_fact(&format!("kill:r:pit-1:m{i}:0"), "guest-rae", input);
        sink.commit(&fact).await.unwrap();
        assert!(sink.commit(&fact).await.unwrap().replayed);
    }
    eventually("redis board", Duration::from_secs(10), || async {
        world.board().scores("guest-rae").await.unwrap() == (2.0, 70.0)
    })
    .await;
    let report = world.rebuild_board().await.unwrap();
    assert_eq!(report["matchesCells"], true, "{report}");
    assert_eq!(report["identicalTop"], true, "{report}");
    let (kills, xp) = world.board().boards();
    for board in [kills, xp] {
        world.board().store().reset(board).await.unwrap();
    }
    let _ = std::fs::remove_dir_all(dir);
}
