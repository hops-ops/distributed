//! The Freeze: two in-process hosts talking over their admin HTTP routes
//! (the binary runs the second host as a separate process; the client's
//! `freeze-headless` test covers that, handoffs and safe-room reloads).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use distributed_game::bots::{run_bots, BotConfig, RandomWalk};
use distributed_game::net::TicketKey;
use distributed_game::world::HostAddr;
use distributed_game::FreezeState;
use game_demos::arena;
use game_demos::freeze::content::{Hazard, Pack, EMBER, HUB, MEADOW};
use game_demos::freeze::world::{assignment, start_host, HostSpec, WorldHost};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SECRET: &str = "freeze-smoke-secret";
const ADMIN: &str = "freeze-smoke-admin";

async fn world() -> (WorldHost, WorldHost, Vec<tokio::sync::oneshot::Sender<()>>) {
    let la = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let lb = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = HostAddr::new("alpha", la.local_addr().unwrap().to_string());
    let b = HostAddr::new("beta", lb.local_addr().unwrap().to_string());
    let mut stops = Vec::new();
    let mut hosts = Vec::new();
    for (me, peer, listener) in [(a.clone(), b.clone(), la), (b, a, lb)] {
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        stops.push(stop);
        let spec = HostSpec {
            me,
            peers: vec![peer],
            assignment: assignment("alpha", "beta"),
            key: TicketKey::new(SECRET),
            admin_secret: ADMIN.into(),
            dev: true,
            stage: Duration::from_millis(600),
            ack_timeout: Duration::from_millis(800),
        };
        hosts.push(
            start_host(listener, spec, async move {
                let _ = stopped.await;
            })
            .unwrap(),
        );
    }
    let b = hosts.pop().unwrap();
    let a = hosts.pop().unwrap();
    (a, b, stops)
}

/// Minimal HTTP/1.1 client: `(status, JSON body or Null)`.
async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<Value>,
    admin: bool,
) -> (u16, Vec<u8>) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    let secret = if admin {
        format!("x-game-admin-secret: {ADMIN}\r\n")
    } else {
        String::new()
    };
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n{secret}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let status = std::str::from_utf8(&response[9..12])
        .unwrap()
        .parse()
        .unwrap();
    let body = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|at| response[at + 4..].to_vec())
        .unwrap_or_default();
    (status, body)
}

async fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<Value>,
    admin: bool,
) -> (u16, Value) {
    let (status, body) = request(addr, method, path, body, admin).await;
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

/// The content CDN: a pack in its payload encoding (what `decodePack` reads).
async fn content(addr: SocketAddr, version: &str) -> (u16, Option<Pack>) {
    let (status, body) = request(
        addr,
        "GET",
        &format!("/dev/freeze/content/{version}"),
        None,
        false,
    )
    .await;
    (status, distributed_game::codec::from_bytes(&body).ok())
}

fn bots(url: String, zone: &str) -> BotConfig {
    let mut config = BotConfig::new(url, zone, TicketKey::new(SECRET));
    config.bots = 2;
    config.input_rate = 30.0;
    config.duration = Duration::from_millis(2600);
    config.content_version = "v1".into();
    config.principal_prefix = format!("bot-{zone}");
    config.encoder = Arc::new(arena::encode_bot_input);
    config
}

async fn dropped(host: &WorldHost, zone: &str) -> u64 {
    host.server
        .inspect(zone, |h| h.inputs_dropped_frozen())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollout_spans_both_hosts_drops_frozen_inputs_and_stamps_the_new_version() {
    let (a, b, stops) = world().await;
    let ws_a = format!("ws://{}", a.addr);
    let ws_b = format!("ws://{}", b.addr);

    // Admin routes need the secret; content of the future is not published.
    let (status, _) = http(
        a.addr,
        "POST",
        "/dev/freeze/rollout",
        Some(json!({"mode": "hard"})),
        false,
    )
    .await;
    assert_eq!(status, 401);
    let (status, _) = content(a.addr, "v2").await;
    assert_eq!(status, 404);
    let (status, pack) = content(a.addr, "current").await;
    assert_eq!(
        (status, pack),
        (200, game_demos::freeze::content::pack("v1"))
    );

    // Bots keep sending inputs through the freeze on both hosts.
    let addr = a.addr;
    let (meadow, ember, rollout) = tokio::join!(
        run_bots(bots(ws_a, MEADOW), |_| Box::new(RandomWalk::new(0.1))),
        run_bots(bots(ws_b, EMBER), |_| Box::new(RandomWalk::new(0.1))),
        async move {
            tokio::time::sleep(Duration::from_millis(700)).await;
            http(
                addr,
                "POST",
                "/dev/freeze/rollout",
                Some(json!({"mode": "rolling"})),
                true,
            )
            .await
        }
    );
    assert_eq!(rollout.0, 200);
    assert_eq!(rollout.1["ok"], true, "{}", rollout.1);
    assert_eq!(rollout.1["version"], "v2");
    for summary in [&meadow, &ember] {
        assert_eq!(summary.joined(), 2, "{:?}", summary.bots);
        assert_eq!(summary.failed(), 0, "{:?}", summary.bots);
        assert!(summary.bots.iter().all(|bot| bot.last_ack > 10));
    }
    assert!(
        dropped(&a, MEADOW).await > 0,
        "frozen inputs counted on alpha"
    );
    assert!(
        dropped(&b, EMBER).await > 0,
        "frozen inputs counted on beta (remote barrier)"
    );

    assert_eq!(a.server.content_version(), "v2");
    assert_eq!(b.server.content_version(), "v2");
    for (host, zone) in [(&a, HUB), (&a, MEADOW), (&b, EMBER)] {
        let (version, pack) = host
            .server
            .inspect(zone, |h| {
                (
                    h.config().content_version.clone(),
                    h.zone().pack().name.clone(),
                )
            })
            .await
            .unwrap();
        assert_eq!((version.as_str(), pack.as_str()), ("v2", "Frostfall Patch"));
        assert!(host
            .sink
            .committed()
            .iter()
            .any(|f| f.command() == "freeze.content_applied"
                && f.stamp().zone_id == zone
                && f.stamp().content_version == "v2"));
    }
    let (_, pack) = content(a.addr, "current").await;
    assert_eq!(pack.unwrap().hazard, Hazard::Ice);
    let (_, status) = http(a.addr, "GET", "/dev/freeze/world", None, false).await;
    assert_eq!(status["hosts"][1]["host"], "beta");
    assert_eq!(status["hosts"][1]["zones"][0]["contentVersion"], "v2");
    for stop in stops {
        let _ = stop.send(());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stuck_zone_aborts_and_everyone_resumes_on_the_old_version() {
    let (a, b, stops) = world().await;
    // The stuck toggle is forwarded to the host owning the zone.
    let (status, reply) = http(
        a.addr,
        "POST",
        "/dev/freeze/stuck",
        Some(json!({"zone": EMBER, "stuck": true})),
        true,
    )
    .await;
    assert_eq!((status, reply["host"].as_str()), (200, Some("beta")));
    assert!(b.rituals.is_stuck(EMBER));
    let (_, result) = http(
        a.addr,
        "POST",
        "/dev/freeze/rollout",
        Some(json!({"mode": "hard"})),
        true,
    )
    .await;
    assert_eq!(result["ok"], false, "{result}");
    assert_eq!(result["missing"], json!([format!("beta/{EMBER}")]));
    assert_eq!(a.server.content_version(), "v1");
    assert_eq!(b.server.content_version(), "v1");
    for (host, zone) in [(&a, HUB), (&a, MEADOW), (&b, EMBER)] {
        let state = host.server.inspect(zone, |h| h.freeze_state()).await;
        assert_eq!(state, Some(FreezeState::Running), "{zone}");
    }
    // Unstuck: the next rollout goes through.
    http(
        a.addr,
        "POST",
        "/dev/freeze/stuck",
        Some(json!({"zone": EMBER, "stuck": false})),
        true,
    )
    .await;
    let (_, result) = http(
        a.addr,
        "POST",
        "/dev/freeze/rollout",
        Some(json!({"mode": "hard"})),
        true,
    )
    .await;
    assert_eq!(result["version"], "v2", "{result}");
    assert_eq!(b.server.content_version(), "v2");
    for stop in stops {
        let _ = stop.send(());
    }
}
