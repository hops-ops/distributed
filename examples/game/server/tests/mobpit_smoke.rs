//! Mob Pit + Leaderboard smoke tests, every one against its own `celld dev`
//! serving `examples/game/cells` — the rooms (`mobpit_rooms`, one Durable
//! Object per room) and their cells (`game_demos::devcelld`; `CELLD_BIN`
//! must name celld and the worker must be built — `run.sh smoke` does
//! both). celld relays its outbox Queue to the test's services, whose
//! crawler directory and room status project the relayed events
//! (eventually consistent with the cells).
//!
//! 1. Kill facts → Crawler cells exactly once (ledger replay), server-side
//!    XP, trusted principal; the board follows the directory, a rebuild
//!    from the cells' history matches it. celld down: the fact is
//!    retryable, never stored locally.
//! 2. Bots fight in a room on celld: kills settle in the cells, boards and
//!    rebuild equal the cells; the empty room goes dormant, then
//!    hibernated, and a new player wakes it with its mobs restored.
//! 3. celld crashes mid-fight (kill -9 of its process tree) and restarts:
//!    the room comes back from its snapshot, its unsettled kill facts
//!    settle from its outbox, and the cells, the directory and the board
//!    agree.
//! 4. Presence: the rooms' lifecycle facts reach the players' presence
//!    cells (online, offline, away).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use distributed::leaderboard::LeaderboardStore;
use distributed_game::bots::{run_bots, Behaviour, BotConfig};
use distributed_game::net::TicketKey;
use distributed_game::{Fact, FactError, FactSink, FactStamp, ZoneRng};
use game_demos::common::random_hex;
use game_demos::devcelld::DevCelld;
use game_demos::mobpit::board::{kill_occurrence, MobKilled};
use game_demos::mobpit::crawler::RECORD_KILL;
use game_demos::mobpit::store::kill_sink;
use game_demos::mobpit::{self, encode_bot_input, MobPitWorld, RunningServices, WorldOptions};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The services' admin secret in these tests.
const ADMIN_SECRET: &str = "smoke-admin-secret";

/// The rooms' empty → hibernated delay in these tests.
const HIBERNATE_AFTER_MS: &str = "1500";

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

/// A listener for the game server and a celld whose outbox Queue relays
/// to it.
async fn relayed_celld() -> (tokio::net::TcpListener, DevCelld) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay = format!(
        "http://{}{}",
        listener.local_addr().unwrap(),
        distributed::bus::CELLD_QUEUE_RELAY_PATH
    );
    let celld = DevCelld::start_with(
        Some(relay),
        &[
            ("ZONE_HIBERNATE_AFTER_MS", HIBERNATE_AFTER_MS),
            ("ZONE_PERSIST_MS", "1000"),
        ],
    )
    .unwrap_or_else(|error| panic!("celld dev: {error}"));
    (listener, celld)
}

/// The projected crawler list has caught up: `principals`, with their
/// cells' kills.
async fn crawlers_settled(world: &MobPitWorld, principals: &[&str]) {
    eventually("crawler directory", Duration::from_secs(30), || async {
        let Ok(crawlers) = world.crawlers().await else {
            return false;
        };
        let listed: Vec<&str> = crawlers
            .iter()
            .map(|c| c["principal"].as_str().unwrap_or_default())
            .collect();
        if listed != principals {
            return false;
        }
        for c in &crawlers {
            let principal = c["principal"].as_str().unwrap();
            let Ok(view) = world.cells().crawler(principal).await else {
                return false;
            };
            if view.state.map(|s| u64::from(s.kills)) != c["kills"].as_u64() {
                return false;
            }
        }
        true
    })
    .await;
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
    let (listener, mut celld) = relayed_celld().await;
    let world = MobPitWorld::start(WorldOptions::new(&dir, celld.options()))
        .await
        .unwrap();
    let (_demo, _stop) = start(listener, &celld, world.clone()).await;
    let sink = kill_sink(world.celld()).unwrap();
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
    sink.commit(&kill_fact(
        "kill:t:pit-1:m6:0",
        "guest-bo",
        json!({ "mob": "pit-1/m6", "tier": 3, "name": "Bo" }),
    ))
    .await
    .unwrap();

    let ana = world.cells().crawler("guest-ana").await.unwrap();
    let state = ana.state.clone().unwrap();
    assert_eq!(
        (state.kills, state.xp, state.level),
        (1, 25, 1),
        "credited once"
    );
    assert_eq!(ana.kills.len(), 1);
    assert_eq!(
        ana.kills[0].sequence,
        first.payload["sequence"].as_u64().unwrap()
    );
    // Listed by the directory once their kill events are projected.
    crawlers_settled(&world, &["guest-ana", "guest-bo"]).await;

    // The board follows the directory; the replay changed nothing.
    eventually("live board", Duration::from_secs(10), || async {
        world.board().scores("guest-bo").await.unwrap() == (1.0, 60.0)
    })
    .await;
    assert_eq!(
        world.board().scores("guest-ana").await.unwrap(),
        (1.0, 25.0)
    );
    assert_eq!(assert_board_matches_cells(&world).await, 2);

    let report = world.rebuild_board().await.unwrap();
    assert_eq!(report.events, 2);
    assert_eq!(report.applied, 4, "two boards per kill");
    assert!(report.matches_cells, "{report:?}");
    assert!(report.identical_top, "{report:?}");

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

    // celld down: the fact is retryable (the zone keeps it queued), and it
    // commits once celld is back.
    celld.crash();
    let later = kill_fact(
        "kill:t:pit-1:m7:0",
        "guest-ana",
        json!({ "mob": "pit-1/m7", "tier": 1, "name": "Ana" }),
    );
    let down = sink.commit(&later).await;
    assert!(matches!(down, Err(FactError::Retryable(_))), "{down:?}");
    celld.restart().unwrap();
    assert!(!sink.commit(&later).await.unwrap().replayed);
    let state = world
        .cells()
        .crawler("guest-ana")
        .await
        .unwrap()
        .state
        .unwrap();
    assert_eq!((state.kills, state.xp), (2, 35), "durable across the crash");
    let _ = std::fs::remove_dir_all(dir);
}

async fn start(
    listener: tokio::net::TcpListener,
    celld: &DevCelld,
    world: Arc<MobPitWorld>,
) -> (RunningServices, tokio::sync::oneshot::Sender<()>) {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let running = mobpit::spawn(
        listener,
        TicketKey::new(celld.ticket_secret()),
        true,
        ADMIN_SECRET.to_string(),
        world,
        None,
        async move {
            let _ = stopped.await;
        },
    )
    .unwrap();
    (running, stop)
}

/// Bots in room `room` on celld (`/zones/{room}`), with tickets the rooms
/// accept.
fn bot_config(celld: &DevCelld, room: &str, bots: usize, duration: Duration) -> BotConfig {
    let mut config = BotConfig::new(celld.ws_url(), room, TicketKey::new(celld.ticket_secret()));
    config.path = "/zones".into();
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

/// `GET {celld}/zones/{room}/info` (the room object's debug JSON).
async fn room_info(celld: &DevCelld, room: &str) -> Value {
    reqwest_get(&format!("{}/zones/{room}/info", celld.url())).await
}

async fn reqwest_get(url: &str) -> Value {
    match reqwest::get(url).await {
        Ok(response) => response.json().await.unwrap_or(Value::Null),
        Err(_) => Value::Null,
    }
}

/// The room's row in the services' room status (`/dev/mobpit/rooms`).
async fn room_status(services: SocketAddr, room: &str) -> Value {
    let (_, body) = http(services, "GET", "/dev/mobpit/rooms", None).await;
    body["rooms"]
        .as_array()
        .and_then(|rooms| rooms.iter().find(|r| r["zone"] == room).cloned())
        .unwrap_or(Value::Null)
}

/// Directory = cells, board = directory.
async fn settled_and_matching(world: &Arc<MobPitWorld>) -> u64 {
    let w = world.clone();
    eventually(
        "directory equals the cells",
        Duration::from_secs(60),
        || {
            let w = w.clone();
            async move {
                let Ok(crawlers) = w.crawlers().await else {
                    return false;
                };
                for c in &crawlers {
                    let principal = c["principal"].as_str().unwrap();
                    let Ok(view) = w.cells().crawler(principal).await else {
                        return false;
                    };
                    if view.state.map(|s| u64::from(s.kills)) != c["kills"].as_u64() {
                        return false;
                    }
                }
                !crawlers.is_empty()
            }
        },
    )
    .await;
    let w = world.clone();
    eventually(
        "board equals the directory",
        Duration::from_secs(20),
        || {
            let w = w.clone();
            async move {
                let mut ok = true;
                for c in w.crawlers().await.unwrap() {
                    let (k, x) = w
                        .board()
                        .scores(c["principal"].as_str().unwrap())
                        .await
                        .unwrap();
                    ok &= k == c["kills"].as_f64().unwrap() && x == c["xp"].as_f64().unwrap();
                }
                ok
            }
        },
    )
    .await;
    assert_board_matches_cells(world).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bots_fight_in_a_room_on_celld_and_the_room_sleeps_and_restores() {
    let dir = temp_dir("bots");
    let (listener, celld) = relayed_celld().await;
    let world = MobPitWorld::start(WorldOptions::new(&dir, celld.options()))
        .await
        .unwrap();
    let (services, stop) = start(listener, &celld, world.clone()).await;
    let room = "pit.0.0";

    let summary = run_bots(bot_config(&celld, room, 6, Duration::from_secs(10)), spin).await;
    assert_eq!(
        summary.joined(),
        6,
        "{:?}",
        summary.bots.iter().find(|b| b.error.is_some())
    );
    let info = room_info(&celld, room).await;
    assert!(info["iterations"].as_u64().unwrap() > 100, "{info}");
    assert!(info["tick"].as_u64().unwrap_or(0) > 0, "{info}");

    // The bots left: dormant, then hibernated (dropped from memory).
    eventually("room hibernated", Duration::from_secs(30), || async {
        room_status(services.addr, room).await["status"] == "hibernated"
    })
    .await;
    assert_eq!(room_info(&celld, room).await["awake"], false);

    // Kills settled exactly once in the cells; boards and rebuild agree.
    let total = settled_and_matching(&world).await;
    assert!(total >= 1, "bots killed mobs: {total}");
    let (_, rebuild) = http(
        services.addr,
        "POST",
        "/dev/mobpit/leaderboard/rebuild",
        None,
    )
    .await;
    assert_eq!(rebuild["matchesCells"], true, "{rebuild}");
    assert_eq!(rebuild["events"].as_u64(), Some(total));
    let (_, crawlers) = http(services.addr, "GET", "/dev/mobpit/crawlers", None).await;
    assert_eq!(
        crawlers["crawlers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["kills"].as_u64().unwrap())
            .sum::<u64>(),
        total
    );

    // A new player wakes the room: restored from its snapshot.
    let summary = run_bots(bot_config(&celld, room, 1, Duration::from_secs(2)), spin).await;
    assert_eq!(summary.joined(), 1);
    eventually("room restored", Duration::from_secs(20), || async {
        let row = room_status(services.addr, room).await;
        row["restored"] == true && row["activations"].as_i64().unwrap_or(0) >= 2
    })
    .await;

    stop.send(()).unwrap();
    services.task.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_celld_crash_mid_fight_keeps_kills_exactly_once() {
    let dir = temp_dir("crash");
    let (listener, mut celld) = relayed_celld().await;
    let world = MobPitWorld::start(WorldOptions::new(&dir, celld.options()))
        .await
        .unwrap();
    let (services, stop) = start(listener, &celld, world.clone()).await;
    let room = "pit.1.0";
    let fight = tokio::spawn(run_bots(
        bot_config(&celld, room, 5, Duration::from_secs(30)),
        spin,
    ));
    // Fight until kills have settled (the fight goes on: more are in
    // flight when celld dies).
    let w = world.clone();
    eventually("first kills settled", Duration::from_secs(40), || {
        let w = w.clone();
        async move { !w.crawlers().await.unwrap_or_default().is_empty() }
    })
    .await;
    // celld crashes (kill -9 of its process tree) and restarts on its state.
    celld.restart().unwrap();
    fight.abort();
    // A player wakes the room again: its snapshot and its outbox come back.
    let summary = run_bots(bot_config(&celld, room, 1, Duration::from_secs(2)), spin).await;
    assert_eq!(summary.joined(), 1, "{:?}", summary.bots);
    let total = settled_and_matching(&world).await;
    eprintln!("crash: durable kills {total}");
    // Every principal's kill history holds each fact once: the recorded
    // kills equal the cell's count (a replay records nothing).
    for c in world.crawlers().await.unwrap() {
        let view = world
            .cells()
            .crawler(c["principal"].as_str().unwrap())
            .await
            .unwrap();
        assert_eq!(
            view.kills.len() as u32,
            view.state.unwrap().kills,
            "one event per credited kill"
        );
    }
    let (_, rebuild) = http(
        services.addr,
        "POST",
        "/dev/mobpit/leaderboard/rebuild",
        None,
    )
    .await;
    assert_eq!(rebuild["matchesCells"], true, "{rebuild}");
    let _ = stop.send(());
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
    let (listener, celld) = relayed_celld().await;
    let mut options = WorldOptions::new(&dir, celld.options());
    options.redis_url = Some(url);
    options.redis_prefix = format!("mobpit-test-{}", random_hex(8));
    let world = MobPitWorld::start(options).await.unwrap();
    let (_services, _stop) = start(listener, &celld, world.clone()).await;
    assert_eq!(world.board().backend(), "redis");
    let sink = kill_sink(world.celld()).unwrap();
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
    crawlers_settled(&world, &["guest-rae"]).await;
    let report = world.rebuild_board().await.unwrap();
    assert!(report.matches_cells, "{report:?}");
    assert!(report.identical_top, "{report:?}");
    let (kills, xp) = world.board().boards();
    for board in [kills, xp] {
        world.board().store().reset(board).await.unwrap();
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn presence_is_settled_in_presence_cells_from_the_rooms_lifecycle_facts() {
    use distributed_game::protocol::Hello;
    use distributed_game::{ClientMessage, ClientMirror, Role, ServerMessage, TicketClaims};
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let dir = temp_dir("presence");
    let (listener, celld) = relayed_celld().await;
    let world = MobPitWorld::start(WorldOptions::new(&dir, celld.options()))
        .await
        .unwrap();
    let (services, stop) = start(listener, &celld, world.clone()).await;
    let room = "pit.0.2";
    let join = |principal: &'static str| {
        let url = format!("{}/zones/{room}", celld.ws_url());
        let key = TicketKey::new(celld.ticket_secret());
        async move {
            let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
            let ticket = key.sign(&TicketClaims::new(
                principal,
                room,
                4_000_000_000,
                format!("presence-{principal}"),
                Role::Player,
            ));
            let hello = ClientMessage::Hello(Hello {
                protocol_version: distributed_game::PROTOCOL_VERSION,
                content_version: String::new(),
                capabilities: 0,
                ticket,
                reconnect_token: String::new(),
                near: false,
            });
            ws.send(Message::Binary(hello.encode().into()))
                .await
                .unwrap();
            let mut mirror = ClientMirror::new();
            loop {
                match ws.next().await.unwrap().unwrap() {
                    Message::Binary(bytes) => {
                        if let ServerMessage::Welcome(_) = mirror.apply(&bytes).unwrap() {
                            break;
                        }
                    }
                    _ => continue,
                }
            }
            ws
        }
    };
    let status = |principal: &'static str| {
        let addr = services.addr;
        async move {
            let (_, body) = http(
                addr,
                "GET",
                &format!("/dev/mobpit/presence?principal={principal}"),
                None,
            )
            .await;
            (
                body["presence"]["zone"].as_str().map(str::to_string),
                body["presence"]["status"].as_str().map(str::to_string),
            )
        }
    };
    let mut ana = join("guest-ana").await;
    let bo = join("guest-bo").await;
    eventually("ana online", Duration::from_secs(20), || async {
        status("guest-ana").await == (Some(room.into()), Some("online".into()))
    })
    .await;
    // Graceful leave → offline; a dropped socket → away (grace running).
    ana.send(Message::Binary(ClientMessage::Leave.encode().into()))
        .await
        .unwrap();
    drop(bo);
    eventually("ana offline, bo away", Duration::from_secs(20), || async {
        status("guest-ana").await == (Some(room.into()), Some("offline".into()))
            && matches!(
                status("guest-bo").await.1.as_deref(),
                Some("away") | Some("offline")
            )
    })
    .await;
    // Lifecycle facts never touch the Crawler cells.
    assert!(world.crawlers().await.unwrap().is_empty());
    let _ = stop.send(());
    let _ = std::fs::remove_dir_all(dir);
}
