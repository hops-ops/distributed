//! Arena smoke tests: the real server in-process, dev routes over HTTP and
//! 20 WebSocket bots (`distributed_game::bots`).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use distributed_game::bots::{run_bots, BotConfig, RandomWalk};
use distributed_game::net::TicketKey;
use game_demos::arena::{self, ArenaDemo};
use game_demos::common::{spawn, DemoOptions, RunningDemo};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SECRET: &str = "smoke-secret";

async fn start(options: DemoOptions) -> (RunningDemo<ArenaDemo>, tokio::sync::oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let running = spawn::<ArenaDemo>(listener, options, async move {
        let _ = stopped.await;
    })
    .unwrap();
    (running, stop)
}

/// Minimal HTTP/1.1 client: `(status, JSON body or Null)`.
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

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dev_ticket_binds_a_server_generated_guest() {
    let (demo, stop) = start(DemoOptions::dev(SECRET)).await;
    let key = TicketKey::new(SECRET);

    let (status, first) = http(
        demo.addr,
        "POST",
        "/dev/ticket",
        Some(json!({ "zone": "arena-1", "name": "  <b>Ana</b> " })),
    )
    .await;
    assert_eq!(status, 200, "{first}");
    let principal = first["principal"].as_str().unwrap().to_string();
    assert!(principal.starts_with("guest-"));
    assert_eq!(first["name"], "bAnab");
    let claims = key
        .verify(first["ticket"].as_str().unwrap(), now())
        .unwrap();
    assert_eq!(claims.principal, principal);
    assert_eq!(claims.zone, "arena-1");
    assert_eq!(demo.guests.name(&principal).as_deref(), Some("bAnab"));

    // The signed guest token keeps the principal; a caller identity never does.
    let (_, again) = http(
        demo.addr,
        "POST",
        "/dev/ticket",
        Some(json!({ "zone": "arena-1", "guest": first["guest"] })),
    )
    .await;
    assert_eq!(again["principal"], principal.as_str());
    assert_eq!(again["name"], "bAnab", "name remembered");
    let (_, forged) = http(
        demo.addr,
        "POST",
        "/dev/ticket",
        Some(json!({ "zone": "arena-1", "guest": "guest-admin.00" })),
    )
    .await;
    assert_ne!(forged["principal"], "guest-admin");
    let (status, _) = http(
        demo.addr,
        "POST",
        "/dev/ticket",
        Some(json!({ "zone": "arena-1", "principal": "admin" })),
    )
    .await;
    assert_eq!(
        status, 422,
        "unknown fields (a caller identity) are rejected"
    );
    let (status, _) = http(
        demo.addr,
        "POST",
        "/dev/ticket",
        Some(json!({ "zone": "pit-1" })),
    )
    .await;
    assert_eq!(status, 400, "zones of other demos are refused");

    // Live NetSim control.
    let (status, sim) = http(
        demo.addr,
        "POST",
        "/dev/netsim",
        Some(json!({ "latencyMs": 40, "jitterMs": 10, "loss": 0.9 })),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(sim, json!({ "latencyMs": 40, "jitterMs": 10, "loss": 0.5 }));
    assert_eq!(
        demo.server.net_sim().unwrap().latency,
        Duration::from_millis(40)
    );
    let (_, off) = http(demo.addr, "POST", "/dev/netsim", Some(json!({}))).await;
    assert_eq!(off, json!({ "latencyMs": 0, "jitterMs": 0, "loss": 0.0 }));
    assert_eq!(demo.server.net_sim(), None);

    stop.send(()).unwrap();
    demo.task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dev_routes_are_off_by_default() {
    let options = DemoOptions {
        dev: false,
        ..DemoOptions::dev(SECRET)
    };
    let (demo, stop) = start(options).await;
    let (status, _) = http(
        demo.addr,
        "POST",
        "/dev/ticket",
        Some(json!({ "zone": "arena-1" })),
    )
    .await;
    assert_eq!(status, 404);
    let (status, _) = http(demo.addr, "GET", "/metrics", None).await;
    assert_eq!(status, 200, "metrics are always served");
    stop.send(()).unwrap();
    demo.task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twenty_bots_get_deltas_and_acks() {
    let (demo, stop) = start(DemoOptions::dev(SECRET)).await;
    let mut config = BotConfig::new(demo.ws_url(), "arena-1", TicketKey::new(SECRET));
    config.bots = 20;
    config.input_rate = 30.0;
    config.duration = Duration::from_secs(2);
    config.encoder = Arc::new(arena::encode_bot_input);
    let server = demo.server.clone();
    let (summary, players) = tokio::join!(
        run_bots(config, |_| Box::new(RandomWalk::new(0.1))),
        async move {
            tokio::time::sleep(Duration::from_millis(1200)).await;
            server.inspect("arena-1", |host| host.player_count()).await
        }
    );
    assert_eq!(players, Some(20), "all bots are players mid-run");
    assert_eq!(
        summary.joined(),
        20,
        "{:?}",
        summary.bots.iter().find(|b| b.error.is_some())
    );
    assert_eq!(summary.failed(), 0);
    for bot in &summary.bots {
        assert!(
            bot.deltas >= 20,
            "bot {} got {} deltas",
            bot.index,
            bot.deltas
        );
        assert!(bot.last_ack >= 20, "bot {} ack {}", bot.index, bot.last_ack);
        assert!(bot.entities >= 9, "pillars are global: {}", bot.entities);
    }
    assert!(summary.delta_bytes() > 0);
    let metrics = demo.server.metrics().render();
    assert!(metrics.contains("arena-1"), "per-zone metrics");
    stop.send(()).unwrap();
    demo.task.await.unwrap().unwrap();
}
