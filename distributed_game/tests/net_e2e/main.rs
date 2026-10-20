//! End-to-end: a real WebSocket client joins through signed-ticket
//! admission, sends inputs, receives deltas, reconnects and is shut down.

#[path = "../zone_runtime/arena.rs"]
mod arena;

use std::time::Duration;

use arena::{Arena, ArenaConfig, AVATAR};
use distributed_game::net::{GameServer, NetSim, ServerConfig, TicketKey, ZoneFactory};
use distributed_game::protocol::{error, kick, Hello};
use distributed_game::{
    ClientMessage, ClientMirror, FieldValue, MemoryFactSink, RetryPolicy, Role, ServerMessage,
    TicketClaims, TicketExtension, TicketExtensions, PROTOCOL_VERSION,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
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
        name: None,
        extensions: Default::default(),
    })
}

async fn start(
    net_sim: Option<NetSim>,
) -> (String, GameServer<Arena>, tokio::task::JoinHandle<()>) {
    start_keyed(SECRET, net_sim).await
}

async fn start_keyed(
    secret: &str,
    net_sim: Option<NetSim>,
) -> (String, GameServer<Arena>, tokio::task::JoinHandle<()>) {
    let mut config = ServerConfig::new(TicketKey::new(secret));
    config.host.tick_rate = 30;
    config.host.patch_rate = 20;
    config.net_sim = net_sim;
    let server = GameServer::<Arena>::new(
        config,
        ZoneFactory::sync(|a| a.zone_id.starts_with('z').then(ArenaConfig::default)),
    )
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
        name: None,
        extensions: Default::default(),
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

/// Tickets minted in JavaScript by `@hops-ops/distributed/sveltekit/game/server`
/// (`mintGameTicket`, reproduced byte for byte by `js/tests/sveltekit-game.test.mjs`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admits_tickets_minted_by_the_sveltekit_handler() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sveltekit_tickets.json")).unwrap();
    let text = |path: &str| {
        fixture
            .pointer(path)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_else(|| panic!("fixture {path}"))
            .to_string()
    };
    let secret = text("/secret");
    let zone = text("/zone");

    // The same key verifies the claims exactly.
    let claims = TicketKey::new(secret.as_str())
        .verify(&text("/valid/ticket"), 1_700_000_000)
        .unwrap();
    assert_eq!(claims.principal, text("/valid/principal"));
    assert_eq!(claims.zone, zone);
    assert_eq!(claims.expires_at, 4_000_000_000);
    assert_eq!(claims.nonce, text("/valid/nonce"));
    assert_eq!(claims.role, Role::Player);
    assert_eq!(claims.name.as_deref(), Some("Ana"));
    assert!(claims.extensions.is_empty());

    // Extension claims from the handler's server-side `claims()` callback.
    let extended = TicketKey::new(secret.as_str())
        .verify(&text("/extended/ticket"), 1_700_000_000)
        .unwrap();
    assert_eq!(extended.principal, text("/extended/principal"));
    assert_eq!(
        extended.extensions.typed::<MatchClaims>().unwrap(),
        MatchClaims {
            match_id: "m-7".into(),
            side: 2,
            party: None,
        }
    );

    let (url, server, handle) = start_keyed(&secret, None).await;

    // A real GameServer admits the JS-minted player ticket ...
    let mut ws = connect(&url, &zone).await;
    hello(&mut ws, text("/valid/ticket"), String::new()).await;
    let mut mirror = ClientMirror::new();
    let Some(ServerMessage::Welcome(welcome)) = next(&mut ws, &mut mirror).await else {
        panic!("expected Welcome for the JS-minted ticket")
    };
    assert_eq!(welcome.zone_id, zone);
    assert_eq!(welcome.role, Role::Player);
    let players = server
        .inspect(&zone, |host| host.players().cloned().collect::<Vec<_>>())
        .await
        .expect("zone active");
    let player = players
        .iter()
        .find(|p| p.principal == text("/valid/principal"))
        .expect("the principal comes from the JS-minted ticket");
    assert_eq!(player.name.as_deref(), Some("Ana"));

    // ... and the observer ticket.
    let mut obs = connect(&url, &zone).await;
    hello(&mut obs, text("/observer/ticket"), String::new()).await;
    let Some(ServerMessage::Welcome(w)) = next(&mut obs, &mut ClientMirror::new()).await else {
        panic!("expected Welcome for the JS-minted observer ticket")
    };
    assert_eq!(w.role, Role::Observer);

    // Single use, signature, expiry and zone binding are enforced.
    for (what, ticket) in [
        ("replayed", text("/valid/ticket")),
        ("tampered", text("/tampered")),
        ("expired", text("/expired")),
        ("wrong zone", text("/wrongZone")),
        ("other key", text("/otherKey")),
    ] {
        let mut ws = connect(&url, &zone).await;
        hello(&mut ws, ticket, String::new()).await;
        match next(&mut ws, &mut ClientMirror::new()).await {
            Some(ServerMessage::Error { code, .. }) => {
                assert_eq!(code, error::UNAUTHORIZED, "{what}")
            }
            other => panic!("{what}: expected UNAUTHORIZED, got {other:?}"),
        }
    }

    server.shutdown();
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
        ..NetSim::default()
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

/// Raw HTTP/1.1 GET returning the whole response text.
async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_with_merges_extra_routes_and_shuts_down_gracefully() {
    let mut config = ServerConfig::new(TicketKey::new(SECRET));
    config.host.tick_rate = 30;
    let server = GameServer::<Arena>::new(
        config,
        ZoneFactory::sync(|a| a.zone_id.starts_with('z').then(ArenaConfig::default)),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let extra = axum::Router::new().route("/hello", axum::routing::get(|| async { "hi" }));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(server.clone().serve_with(listener, extra, async move {
        let _ = stopped.await;
    }));
    let hello = http_get(addr, "/hello").await;
    assert!(
        hello.starts_with("HTTP/1.1 200") && hello.ends_with("hi"),
        "{hello}"
    );
    assert!(http_get(addr, "/metrics").await.starts_with("HTTP/1.1 200"));

    // A joined player is kicked with SHUTDOWN when the signal fires.
    let url = format!("ws://{addr}");
    let mut ws = connect(&url, "z1").await;
    hello_join(&mut ws, "z1").await;
    let mut mirror = ClientMirror::new();
    assert!(matches!(
        next(&mut ws, &mut mirror).await,
        Some(ServerMessage::Welcome(_))
    ));
    stop.send(()).unwrap();
    let mut kicked = false;
    while let Some(msg) = next(&mut ws, &mut mirror).await {
        if let ServerMessage::Kick { code, .. } = msg {
            assert_eq!(code, kick::SHUTDOWN);
            kicked = true;
        }
    }
    assert!(kicked, "shutdown kick delivered");
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("serve_with returns after shutdown")
        .unwrap()
        .unwrap();
}

async fn hello_join(ws: &mut Ws, zone: &str) {
    hello(
        ws,
        ticket("serve-with", zone, "n-serve-with", Role::Player),
        String::new(),
    )
    .await;
}

// ---- Custom ticket verifier: a web BFF's own HS256 JWTs ----

const BFF_SECRET: &[u8] = b"bff-secret";

/// The downstream game's web-BFF ticket: HS256, `iss`
/// `dungeon-crawler-web`, `aud` `dungeon-crawler-zone`, `sub`
/// `human_<hex>`, `principal`, `name`, `jti`, `exp` at most 120 s out,
/// optional `zoneRoute`; no `zone` / `nonce` claims.
fn bff_verifier(
    token: &str,
    zone: &str,
    now: u64,
) -> Result<TicketClaims, distributed_game::net::TicketError> {
    use distributed_game::net::{decode_hs256, TicketError};
    let claims = decode_hs256(token, BFF_SECRET)?;
    let text = |name: &'static str| {
        claims
            .get(name)
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .ok_or(TicketError::MissingClaim(name))
    };
    if text("iss")? != "dungeon-crawler-web" || text("aud")? != "dungeon-crawler-zone" {
        return Err(TicketError::Invalid("issuer/audience".into()));
    }
    let sub = text("sub")?;
    let hex = sub
        .strip_prefix("human_")
        .filter(|h| !h.is_empty() && h.chars().all(|c| c.is_ascii_hexdigit()))
        .ok_or(TicketError::Invalid("sub".into()))?;
    if text("principal")? != sub {
        return Err(TicketError::Invalid("principal".into()));
    }
    let exp = claims
        .get("exp")
        .and_then(|v| v.as_u64())
        .ok_or(TicketError::MissingClaim("exp"))?;
    if exp > now + 120 {
        return Err(TicketError::Invalid("lifetime over 120s".into()));
    }
    if let Some(route) = claims.get("zoneRoute").and_then(|v| v.as_str()) {
        if route != zone {
            return Err(TicketError::WrongZone);
        }
    }
    let mut verified = TicketClaims::new(
        format!("human_{hex}"),
        zone,
        exp,
        text("jti")?,
        Role::Player,
    );
    verified.name = claims
        .get("name")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    Ok(verified)
}

fn bff_ticket(jti: &str, exp_in: u64, zone_route: Option<&str>) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut claims = serde_json::json!({
        "iss": "dungeon-crawler-web", "aud": "dungeon-crawler-zone",
        "sub": "human_0a1b2c", "principal": "human_0a1b2c", "name": "Ana",
        "jti": jti, "exp": now + exp_in,
    });
    if let Some(route) = zone_route {
        claims["zoneRoute"] = route.into();
    }
    TicketKey::new(BFF_SECRET).sign_json(&claims)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custom_verifier_admits_web_bff_tickets_with_replay_protection() {
    let config = ServerConfig::new(TicketKey::new(SECRET)).with_ticket_verifier(bff_verifier);
    let server = GameServer::<Arena>::new(
        config,
        ZoneFactory::sync(|a| a.zone_id.starts_with('z').then(ArenaConfig::default)),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let serving = server.clone();
    let handle = tokio::spawn(async move {
        serving
            .serve(listener, std::future::pending())
            .await
            .unwrap();
    });

    let mut ws = connect(&url, "z1").await;
    hello(&mut ws, bff_ticket("jti-1", 60, Some("z1")), String::new()).await;
    let mut mirror = ClientMirror::new();
    let Some(ServerMessage::Welcome(welcome)) = next(&mut ws, &mut mirror).await else {
        panic!("expected Welcome")
    };
    assert_eq!(welcome.zone_id, "z1");
    let players = server
        .inspect("z1", |host| {
            host.players()
                .map(|p| (p.principal.clone(), p.name.clone()))
                .collect::<Vec<_>>()
        })
        .await
        .unwrap();
    assert_eq!(
        players,
        vec![("human_0a1b2c".to_string(), Some("Ana".to_string()))]
    );

    // Replay of the same jti, wrong route, overlong lifetime, built-in
    // tickets (the custom verifier replaces them) and a forged signature.
    expect_error(
        &url,
        "z1",
        bff_ticket("jti-1", 60, Some("z1")),
        error::UNAUTHORIZED,
    )
    .await;
    expect_error(
        &url,
        "z2",
        bff_ticket("jti-2", 60, Some("z1")),
        error::UNAUTHORIZED,
    )
    .await;
    expect_error(
        &url,
        "z1",
        bff_ticket("jti-3", 600, None),
        error::UNAUTHORIZED,
    )
    .await;
    expect_error(
        &url,
        "z1",
        ticket("bob", "z1", "n-bob", Role::Player),
        error::UNAUTHORIZED,
    )
    .await;
    let forged = TicketKey::new("nope").sign_json(&serde_json::json!({"sub": "human_ff"}));
    expect_error(&url, "z1", forged, error::UNAUTHORIZED).await;
    // No zoneRoute: admitted to the zone the client asked for.
    let mut ws2 = connect(&url, "z2").await;
    hello(&mut ws2, bff_ticket("jti-4", 60, None), String::new()).await;
    let mut mirror2 = ClientMirror::new();
    assert!(matches!(
        next(&mut ws2, &mut mirror2).await,
        Some(ServerMessage::Welcome(_))
    ));

    server.shutdown();
    handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_config_hook_sets_per_zone_rates() {
    let mut config = ServerConfig::new(TicketKey::new(SECRET));
    config.host.tick_rate = 30;
    config.host.content_version = "c1".into();
    let server = GameServer::<Arena>::new(
        config,
        ZoneFactory::sync(|a| a.zone_id.starts_with('z').then(ArenaConfig::default)),
    )
    .with_host_config(|zone_id, host| {
        if zone_id == "z-slow" {
            host.tick_rate = 5;
            host.patch_rate = 2;
            host.content_version = "ignored".into();
        }
    });
    assert!(server.activate("z-slow").await);
    assert!(server.activate("z-hot").await);
    let slow = server
        .inspect("z-slow", |h| {
            (
                h.config().tick_rate,
                h.config().patch_rate,
                h.config().content_version.clone(),
            )
        })
        .await;
    assert_eq!(slow, Some((5, 2, "c1".to_string())), "world version wins");
    let hot = server.inspect("z-hot", |h| h.config().tick_rate).await;
    assert_eq!(hot, Some(30));
    server.shutdown();
}

/// A game's declared extension claims.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TicketExtension)]
struct MatchClaims {
    match_id: String,
    #[serde(rename = "team")]
    side: u8,
    #[serde(default)]
    party: Option<String>,
}

fn extended_ticket(nonce: &str, ext: serde_json::Value) -> String {
    TicketKey::new(SECRET).sign(&TicketClaims {
        principal: "ana".into(),
        zone: "z-match".into(),
        expires_at: 4_000_000_000,
        nonce: nonce.into(),
        role: Role::Player,
        name: None,
        extensions: TicketExtensions::from_json(ext).unwrap(),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extension_claims_reach_on_auth_and_player_info() {
    assert_eq!(MatchClaims::CLAIMS, ["match_id", "team", "party"]);
    let config = ServerConfig::new(TicketKey::new(SECRET)).with_ticket_extensions::<MatchClaims>();
    let server =
        GameServer::<Arena>::new(config, ZoneFactory::sync(|_| Some(ArenaConfig::default())));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let serving = server.clone();
    let handle = tokio::spawn(async move {
        serving
            .serve(listener, std::future::pending())
            .await
            .unwrap();
    });

    // Undeclared names and mistyped values are refused at admission.
    for (nonce, ext) in [
        (
            "x-unknown",
            serde_json::json!({"match_id": "m-7", "team": 2, "gold": 50}),
        ),
        (
            "x-typed",
            serde_json::json!({"match_id": "m-7", "team": "red"}),
        ),
        ("x-object", serde_json::json!(["m-7"])),
    ] {
        let ticket = if nonce == "x-object" {
            TicketKey::new(SECRET).sign_json(&serde_json::json!({
                "sub": "ana", "zone": "z-match", "exp": 4_000_000_000u64,
                "nonce": nonce, "ext": ext,
            }))
        } else {
            extended_ticket(nonce, ext)
        };
        expect_error(&url, "z-match", ticket, error::UNAUTHORIZED).await;
    }

    let expected = MatchClaims {
        match_id: "m-7".into(),
        side: 2,
        party: None,
    };
    let mut ws = connect(&url, "z-match").await;
    hello(
        &mut ws,
        extended_ticket("ok", serde_json::json!({"match_id": "m-7", "team": 2})),
        String::new(),
    )
    .await;
    let Some(ServerMessage::Welcome(_)) = next(&mut ws, &mut ClientMirror::new()).await else {
        panic!("expected Welcome")
    };
    let (auth, players) = server
        .inspect("z-match", |host| {
            (
                host.zone().auth_extensions.clone(),
                host.players().cloned().collect::<Vec<_>>(),
            )
        })
        .await
        .expect("zone active");
    assert_eq!(
        auth.last().unwrap().typed::<MatchClaims>().unwrap(),
        expected
    );
    assert_eq!(
        players[0].extensions.typed::<MatchClaims>().unwrap(),
        expected
    );

    server.shutdown();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("server stops")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extension_claims_are_refused_without_a_declared_schema() {
    let (url, server, handle) = start(None).await;
    let ticket = TicketKey::new(SECRET).sign(&TicketClaims {
        principal: "ana".into(),
        zone: "z1".into(),
        expires_at: 4_000_000_000,
        nonce: "undeclared".into(),
        role: Role::Player,
        name: None,
        extensions: TicketExtensions::from_json(serde_json::json!({"match_id": "m"})).unwrap(),
    });
    expect_error(&url, "z1", ticket, error::UNAUTHORIZED).await;
    server.shutdown();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("server stops")
        .unwrap();
}

async fn serve_arena(server: &GameServer<Arena>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let serving = server.clone();
    let handle = tokio::spawn(async move {
        serving
            .serve(listener, std::future::pending())
            .await
            .unwrap();
    });
    (url, handle)
}

async fn stop(server: GameServer<Arena>, handle: tokio::task::JoinHandle<()>) {
    server.shutdown();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("server stops")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_factory_reads_the_allocation_before_admitting_queued_joins() {
    use distributed_game::world::{MemoryZoneAllocationStore, ZoneAllocation, ZoneAllocationStore};
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::Arc;

    let store = Arc::new(MemoryZoneAllocationStore::new());
    let roster = serde_json::json!({ "mode": "duel", "roster": ["ana", "bo"] });
    store
        .put(
            "z-match",
            ZoneAllocation::new("game-services.match", 1, &roster).unwrap(),
        )
        .await
        .unwrap();
    let created = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicU32::new(0));
    let (done, calls_in) = (created.clone(), calls.clone());
    let server = GameServer::<Arena>::new(
        ServerConfig::new(TicketKey::new(SECRET)),
        ZoneFactory::new(move |activation| {
            let (done, calls) = (done.clone(), calls_in.clone());
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                assert!(activation.activation > 0, "the fencing token");
                // Storage is slow: both joins arrive while this awaits.
                tokio::time::sleep(Duration::from_millis(200)).await;
                done.store(true, Ordering::SeqCst);
                Ok(Some(ArenaConfig {
                    allocation: activation.allocation.map(|a| a.payload),
                    ..ArenaConfig::default()
                }))
            }
        }),
    )
    .with_allocation_store(store);
    let (url, handle) = serve_arena(&server).await;

    let join = |principal: &'static str, nonce: &'static str| {
        let url = url.clone();
        let created = created.clone();
        async move {
            let mut ws = connect(&url, "z-match").await;
            hello(
                &mut ws,
                ticket(principal, "z-match", nonce, Role::Player),
                String::new(),
            )
            .await;
            let msg = next(&mut ws, &mut ClientMirror::new()).await;
            assert!(
                created.load(Ordering::SeqCst),
                "admitted only after the factory finished"
            );
            matches!(msg, Some(ServerMessage::Welcome(_)))
        }
    };
    let (ana, bo) = tokio::join!(join("ana", "q1"), join("bo", "q2"));
    assert!(ana && bo, "both queued joins admitted");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "one activation");
    let allocation = server
        .inspect("z-match", |host| host.zone().cfg.allocation.clone())
        .await
        .unwrap();
    assert_eq!(allocation, Some(roster), "the allocation reached on_create");
    stop(server, handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn factory_failures_answer_joins_with_typed_errors() {
    use distributed_game::world::{
        AllocationError, BoxFuture, ZoneAllocation, ZoneAllocationStore, ZoneCreateError,
    };
    use std::sync::Arc;

    struct DownStore;
    impl ZoneAllocationStore for DownStore {
        fn load<'a>(
            &'a self,
            _: &'a str,
        ) -> BoxFuture<'a, Result<Option<ZoneAllocation>, AllocationError>> {
            Box::pin(async { Err(AllocationError("connection refused".into())) })
        }
        fn put<'a>(
            &'a self,
            _: &'a str,
            _: ZoneAllocation,
        ) -> BoxFuture<'a, Result<(), AllocationError>> {
            Box::pin(async { Ok(()) })
        }
        fn remove<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<(), AllocationError>> {
            Box::pin(async { Ok(()) })
        }
    }

    let mut config = ServerConfig::new(TicketKey::new(SECRET));
    config.activation_timeout = Duration::from_millis(300);
    let server = GameServer::<Arena>::new(
        config,
        ZoneFactory::new(|activation| async move {
            match activation.zone_id.as_str() {
                "z-broken" => Err(ZoneCreateError::new("roster store unavailable")),
                "z-hang" => std::future::pending().await,
                "z-ok" => Ok(Some(ArenaConfig::default())),
                _ => Ok(None),
            }
        }),
    );
    let (url, handle) = serve_arena(&server).await;
    let started = std::time::Instant::now();
    for (zone, code, nonce) in [
        ("z-broken", error::ZONE_UNAVAILABLE, "f1"),
        ("z-hang", error::ZONE_UNAVAILABLE, "f2"),
        ("z-unknown", error::NOT_FOUND, "f3"),
    ] {
        expect_error(&url, zone, ticket("ana", zone, nonce, Role::Player), code).await;
        assert!(!server.is_active(zone), "{zone} unregistered");
    }
    assert!(started.elapsed() < Duration::from_secs(3), "no hang");
    assert!(!server.activate("z-broken").await);
    assert!(server.activate("z-ok").await);
    stop(server, handle).await;

    // An allocation store failure refuses too, without calling the factory.
    let server = GameServer::<Arena>::new(
        ServerConfig::new(TicketKey::new(SECRET)),
        ZoneFactory::new(|_| async { panic!("factory must not run without its allocation") }),
    )
    .with_allocation_store(Arc::new(DownStore));
    let (url, handle) = serve_arena(&server).await;
    expect_error(
        &url,
        "z-match",
        ticket("ana", "z-match", "f4", Role::Player),
        error::ZONE_UNAVAILABLE,
    )
    .await;
    stop(server, handle).await;
}
