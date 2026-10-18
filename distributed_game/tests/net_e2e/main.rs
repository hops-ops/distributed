//! End-to-end: a real WebSocket client joins through signed-ticket
//! admission, sends inputs, receives deltas, reconnects and is shut down.

#[path = "../zone_runtime/arena.rs"]
mod arena;

use std::time::Duration;

use arena::{Arena, ArenaConfig, AVATAR};
use distributed_game::net::{GameServer, NetSim, ServerConfig, TicketKey};
use distributed_game::protocol::{error, kick, Hello};
use distributed_game::{
    ClientMessage, ClientMirror, FieldValue, MemoryFactSink, RetryPolicy, Role, ServerMessage,
    TicketClaims, PROTOCOL_VERSION,
};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const SECRET: &str = "dev-secret";

fn ticket(principal: &str, zone: &str, nonce: &str, role: Role) -> String {
    TicketKey::new(SECRET).sign(&TicketClaims {
        principal: principal.into(),
        zone: zone.into(),
        expires_at: 4_000_000_000,
        nonce: nonce.into(),
        role,
    })
}

async fn start(
    net_sim: Option<NetSim>,
) -> (String, GameServer<Arena>, tokio::task::JoinHandle<()>) {
    let mut config = ServerConfig::new(TicketKey::new(SECRET));
    config.host.tick_rate = 30;
    config.host.patch_rate = 20;
    config.net_sim = net_sim;
    let server = GameServer::<Arena>::new(config, |zone_id| {
        zone_id.starts_with('z').then(ArenaConfig::default)
    })
    .with_fact_sink(MemoryFactSink::new(), 64, RetryPolicy::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serving = server.clone();
    let handle = tokio::spawn(async move {
        serving
            .serve(listener, std::future::pending())
            .await
            .unwrap();
    });
    (format!("ws://{addr}"), server, handle)
}

async fn send(ws: &mut Ws, msg: ClientMessage) {
    ws.send(Message::Binary(msg.encode().into())).await.unwrap();
}

async fn hello(ws: &mut Ws, ticket: String, reconnect_token: String) {
    send(
        ws,
        ClientMessage::Hello(Hello {
            protocol_version: PROTOCOL_VERSION,
            content_version: String::new(),
            capabilities: 0,
            ticket,
            reconnect_token,
        }),
    )
    .await;
}

/// Next decoded server message (skipping non-binary frames).
async fn next(ws: &mut Ws, mirror: &mut ClientMirror) -> Option<ServerMessage> {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("server message within 5s")?;
        match frame {
            Ok(Message::Binary(bytes)) => return Some(mirror.apply(&bytes).unwrap()),
            Ok(Message::Close(_)) | Err(_) => return None,
            Ok(_) => continue,
        }
    }
}

async fn connect(url: &str, zone: &str) -> Ws {
    connect_async(format!("{url}/zones/{zone}"))
        .await
        .unwrap()
        .0
}

async fn expect_error(url: &str, zone: &str, ticket: String, code: u32) {
    let mut ws = connect(url, zone).await;
    hello(&mut ws, ticket, String::new()).await;
    let mut mirror = ClientMirror::new();
    match next(&mut ws, &mut mirror).await {
        Some(ServerMessage::Error { code: got, .. }) => assert_eq!(got, code),
        other => panic!("expected error {code}, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_joins_sends_inputs_receives_deltas_and_reconnects() {
    let (url, server, handle) = start(None).await;

    let mut ws = connect(&url, "z1").await;
    hello(
        &mut ws,
        ticket("alice", "z1", "n1", Role::Player),
        String::new(),
    )
    .await;
    let mut mirror = ClientMirror::new();
    let Some(ServerMessage::Welcome(welcome)) = next(&mut ws, &mut mirror).await else {
        panic!("expected Welcome")
    };
    assert_eq!(welcome.zone_id, "z1");
    assert_eq!(welcome.role, Role::Player);
    assert_eq!(welcome.tick_rate, 30);
    assert_eq!(welcome.schema.kinds().len(), 2);

    // Wait for the first delta carrying our avatar.
    while mirror.entities.is_empty() {
        next(&mut ws, &mut mirror).await.unwrap();
    }
    let (&avatar_id, avatar) = mirror.entities.iter().next().unwrap();
    assert_eq!(avatar.kind, AVATAR);
    let FieldValue::F(start_x) = avatar.fields[0] else {
        panic!()
    };

    for seq in 1..=10 {
        send(
            &mut ws,
            ClientMessage::Input {
                seq,
                client_tick: seq,
                payload: vec![1, 0],
            },
        )
        .await;
    }
    send(&mut ws, ClientMessage::Ping { client_time: 123.5 }).await;
    let mut pong = None;
    while mirror.ack_seq < 10 || pong.is_none() {
        match next(&mut ws, &mut mirror).await.unwrap() {
            ServerMessage::Pong {
                client_time,
                server_time,
            } => pong = Some((client_time, server_time)),
            ServerMessage::Delta(_) | ServerMessage::Event { .. } => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(pong.unwrap().0, 123.5);
    // Let the next patch carry the moved position.
    for _ in 0..3 {
        next(&mut ws, &mut mirror).await.unwrap();
    }
    let FieldValue::F(x) = mirror.entities[&avatar_id].fields[0] else {
        panic!()
    };
    assert!(
        x > start_x,
        "server-side movement replicated ({start_x} -> {x})"
    );

    // Text frames are refused.
    ws.send(Message::Text("hi".into())).await.unwrap();
    match next(&mut ws, &mut mirror).await {
        Some(ServerMessage::Error { code, .. }) => assert_eq!(code, error::BAD_MESSAGE),
        other => panic!("expected BAD_MESSAGE, got {other:?}"),
    }
    drop(ws); // abrupt drop: player stays for the grace window

    // Reconnect with the token from Welcome.
    let mut ws = connect(&url, "z1").await;
    hello(&mut ws, String::new(), welcome.reconnect_token.clone()).await;
    let mut mirror = ClientMirror::new();
    let Some(ServerMessage::Welcome(again)) = next(&mut ws, &mut mirror).await else {
        panic!("expected Welcome on reconnect")
    };
    assert_eq!(again.session_id, welcome.session_id);
    assert_ne!(again.reconnect_token, welcome.reconnect_token);
    while !mirror.entities.contains_key(&avatar_id) {
        next(&mut ws, &mut mirror).await.unwrap();
    }

    // Admission failures.
    expect_error(
        &url,
        "z1",
        ticket("bob", "z1", "n1", Role::Player),
        error::UNAUTHORIZED,
    )
    .await;
    expect_error(
        &url,
        "z1",
        ticket("bob", "z2", "n2", Role::Player),
        error::UNAUTHORIZED,
    )
    .await;
    expect_error(
        &url,
        "nope",
        ticket("bob", "nope", "n3", Role::Player),
        error::NOT_FOUND,
    )
    .await;
    let forged = TicketKey::new("wrong").sign(&TicketClaims {
        principal: "bob".into(),
        zone: "z1".into(),
        expires_at: 4_000_000_000,
        nonce: "n4".into(),
        role: Role::Player,
    });
    expect_error(&url, "z1", forged, error::UNAUTHORIZED).await;

    // Observers see the world without a player.
    let mut obs = connect(&url, "z1").await;
    hello(
        &mut obs,
        ticket("watcher", "z1", "n5", Role::Observer),
        String::new(),
    )
    .await;
    let mut obs_mirror = ClientMirror::new();
    let Some(ServerMessage::Welcome(w)) = next(&mut obs, &mut obs_mirror).await else {
        panic!()
    };
    assert_eq!(w.role, Role::Observer);
    while obs_mirror.entities.is_empty() {
        next(&mut obs, &mut obs_mirror).await.unwrap();
    }

    // Graceful shutdown kicks everyone.
    server.shutdown();
    let mut kicked = false;
    while let Some(msg) = next(&mut ws, &mut mirror).await {
        if let ServerMessage::Kick { code, .. } = msg {
            assert_eq!(code, kick::SHUTDOWN);
            kicked = true;
        }
    }
    assert!(kicked);
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("server stops")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn latency_simulation_delays_but_preserves_order() {
    let sim = NetSim {
        latency: Duration::from_millis(40),
        jitter: Duration::from_millis(20),
    };
    let (url, server, handle) = start(Some(sim)).await;
    let mut ws = connect(&url, "z9").await;
    hello(
        &mut ws,
        ticket("carol", "z9", "m1", Role::Player),
        String::new(),
    )
    .await;
    let mut mirror = ClientMirror::new();
    assert!(matches!(
        next(&mut ws, &mut mirror).await,
        Some(ServerMessage::Welcome(_))
    ));
    let start = std::time::Instant::now();
    send(&mut ws, ClientMessage::Ping { client_time: 1.0 }).await;
    let mut last_tick = 0;
    loop {
        match next(&mut ws, &mut mirror).await.unwrap() {
            ServerMessage::Pong { .. } => break,
            ServerMessage::Delta(d) => {
                assert!(d.server_tick >= last_tick, "ordered");
                last_tick = d.server_tick;
            }
            _ => {}
        }
    }
    assert!(
        start.elapsed() >= Duration::from_millis(80),
        "two one-way delays"
    );
    server.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
}
