//! ~50 bots against an in-process server: every bot joins with a signed
//! ticket, sends inputs, receives Deltas and measures RTT; `/metrics`
//! reflects the load.

#[path = "../zone_runtime/arena.rs"]
mod arena;

use std::time::Duration;

use arena::{Arena, ArenaConfig};
use distributed_game::bots::{run_bots, BehaviourKind, BotConfig};
use distributed_game::net::{GameServer, NetSim, ServerConfig, TicketKey};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SECRET: &str = "bots-secret";

async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut body = String::new();
    stream.read_to_string(&mut body).await.unwrap();
    body
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fifty_bots_join_play_and_receive_deltas() {
    let mut config = ServerConfig::new(TicketKey::new(SECRET));
    config.host.tick_rate = 30;
    config.host.patch_rate = 20;
    config.host.max_players = 64;
    // Mild simulated network so RTT is measurable but the test stays fast.
    config.net_sim = Some(NetSim::new(
        Duration::from_millis(5),
        Duration::from_millis(5),
    ));
    let server = GameServer::<Arena>::new(config, |zone| {
        zone.starts_with("load").then(ArenaConfig::default)
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serving = server.clone();
    let handle = tokio::spawn(async move {
        serving
            .serve(listener, std::future::pending())
            .await
            .unwrap();
    });

    let mut bots = BotConfig::new(format!("ws://{addr}"), "load-1", TicketKey::new(SECRET));
    bots.bots = 50;
    bots.input_rate = 20.0;
    bots.ping_interval = Duration::from_millis(200);
    bots.duration = Duration::from_millis(1200);
    bots.ramp_up = Duration::from_millis(200);

    // Scrape while the bots are connected.
    let scrape = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(900)).await;
        http_get(addr, "/metrics").await
    });
    let summary = run_bots(bots, |i| {
        if i % 2 == 0 {
            BehaviourKind::RandomWalk.build(i)
        } else {
            BehaviourKind::Circle.build(i)
        }
    })
    .await;
    println!("{summary}");

    assert_eq!(summary.joined(), 50, "{summary}");
    assert_eq!(summary.failed(), 0, "{summary}");
    assert!(
        summary.min_deltas() > 5,
        "every bot received deltas: {summary}"
    );
    assert!(
        summary.bots.iter().all(|b| b.last_ack > 0),
        "inputs were acked"
    );
    assert!(summary.bots.iter().all(|b| b.entities > 0));
    let p50 = summary.rtt_quantile(0.5).expect("rtt samples");
    assert!(
        p50 >= Duration::from_millis(10),
        "netsim adds 2x5ms: {p50:?}"
    );
    assert!(summary.delta_bytes_per_sec_per_bot() > 0.0);

    let metrics = scrape.await.unwrap();
    assert!(metrics.starts_with("HTTP/1.1 200"), "{metrics}");
    assert!(metrics.contains("text/plain; version=0.0.4"));
    assert!(
        metrics.contains("distributed_game_zone_players{zone=\"load-1\"} 50"),
        "{metrics}"
    );
    assert!(metrics
        .contains("distributed_game_zone_inputs_total{zone=\"load-1\",outcome=\"accepted\"}"));
    assert!(metrics.contains("distributed_game_zone_delta_bytes_count{zone=\"load-1\"}"));

    server.shutdown();
    handle.await.unwrap();
}
