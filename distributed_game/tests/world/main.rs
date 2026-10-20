//! Two GameServers in one process on different ports: cross-host handoff
//! with a real WebSocket client, tier-up events across hosts, hibernation,
//! fenced ownership and the world content barrier.

mod realm;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed::lease::InMemoryLeaseStore;
use distributed_game::net::{
    AbortNotes, GameServer, RemoteHost, RolloutError, ServerConfig, TicketKey, WorldCoordinator,
};
use distributed_game::protocol::{error, kick, Hello, Notice};
use distributed_game::world::{
    FencedLeaseOwnership, HostAddr, LocalOwnership, SqliteZoneStateStore, StaticDirectory,
    ZoneDirectory, ZoneOwnership, ZoneStateStore,
};
use distributed_game::*;
use futures_util::{SinkExt, StreamExt};
use realm::*;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const SECRET: &str = "world-secret";
const ADMIN: &str = "admin-secret";

fn ticket(principal: &str, zone: &str, nonce: &str) -> String {
    TicketKey::new(SECRET).sign(&TicketClaims {
        principal: principal.into(),
        zone: zone.into(),
        expires_at: 4_000_000_000,
        nonce: nonce.into(),
        role: Role::Player,
        name: None,
    })
}

struct Node {
    server: GameServer<Realm>,
    addr: HostAddr,
    sink: MemoryFactSink,
}

type Factory = Arc<dyn Fn(&str) -> Option<RealmConfig> + Send + Sync>;

fn base_config() -> ServerConfig {
    let mut config = ServerConfig::new(TicketKey::new(SECRET));
    config.host.tick_rate = 30;
    config.host.patch_rate = 10;
    config.admin_secret = Some(ADMIN.into());
    config
}

async fn listen() -> (tokio::net::TcpListener, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    (listener, addr)
}

fn serve(server: &GameServer<Realm>, listener: tokio::net::TcpListener) {
    let serving = server.clone();
    tokio::spawn(async move {
        serving
            .serve(listener, std::future::pending())
            .await
            .unwrap();
    });
}

/// Start two hosts "A" and "B" sharing a directory and ownership.
async fn two_hosts(
    config: impl Fn() -> ServerConfig,
    assign: &[(&str, &str)],
    factory: Factory,
    ownership: Arc<dyn ZoneOwnership>,
) -> (Node, Node, Arc<StaticDirectory>) {
    let (la, a_addr) = listen().await;
    let (lb, b_addr) = listen().await;
    let a = HostAddr::new("A", &a_addr);
    let b = HostAddr::new("B", &b_addr);
    let dir = Arc::new(StaticDirectory::new());
    dir.add_host(a.clone()).add_host(b.clone());
    for (zone, host) in assign {
        dir.assign(*zone, *host);
    }
    dir.fallback("A");
    let mut nodes = Vec::new();
    for (host, listener) in [(a, la), (b, lb)] {
        let sink = MemoryFactSink::new();
        let f = factory.clone();
        let server = GameServer::<Realm>::new(config(), move |zone| f(zone))
            .with_fact_sink(sink.clone(), 64, RetryPolicy::default())
            .with_host_id(host.id.clone())
            .with_directory(dir.clone())
            .with_ownership(ownership.clone());
        serve(&server, listener);
        nodes.push(Node {
            server,
            addr: host,
            sink,
        });
    }
    let b = nodes.pop().unwrap();
    let a = nodes.pop().unwrap();
    (a, b, dir)
}

async fn send(ws: &mut Ws, msg: ClientMessage) {
    ws.send(Message::Binary(msg.encode().into())).await.unwrap();
}

async fn hello(ws: &mut Ws, ticket: &str, token: &str, version: &str) {
    send(
        ws,
        ClientMessage::Hello(Hello {
            protocol_version: PROTOCOL_VERSION,
            content_version: version.into(),
            capabilities: 0,
            ticket: ticket.into(),
            reconnect_token: token.into(),
        }),
    )
    .await;
}

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

/// Next message that is not a Delta.
async fn next_control(ws: &mut Ws, mirror: &mut ClientMirror) -> Option<ServerMessage> {
    loop {
        match next(ws, mirror).await? {
            ServerMessage::Delta(_) => continue,
            other => return Some(other),
        }
    }
}

async fn connect(url: &str) -> Ws {
    connect_async(url).await.unwrap().0
}

struct Client {
    ws: Ws,
    mirror: ClientMirror,
    welcome: protocol::Welcome,
}

/// Connect to `url` (a zone URL), following Redirects, and expect Welcome.
#[allow(clippy::result_large_err)]
async fn join_url(url: &str, ticket: &str, version: &str) -> Result<Client, ServerMessage> {
    let mut url = url.to_string();
    for _ in 0..3 {
        let mut ws = connect(&url).await;
        hello(&mut ws, ticket, "", version).await;
        let mut mirror = ClientMirror::new();
        match next_control(&mut ws, &mut mirror).await.expect("reply") {
            ServerMessage::Welcome(welcome) => {
                return Ok(Client {
                    ws,
                    mirror,
                    welcome,
                })
            }
            ServerMessage::Redirect { url: to, .. } => url = to,
            other => return Err(other),
        }
    }
    panic!("redirect loop")
}

async fn control(client: &mut Client) -> Option<ServerMessage> {
    next_control(&mut client.ws, &mut client.mirror).await
}

/// Next Freeze/Resume/Kick/Error (skips Deltas and game Events).
async fn next_notice(client: &mut Client) -> Option<ServerMessage> {
    loop {
        match control(client).await? {
            ServerMessage::Event { .. } => continue,
            other => return Some(other),
        }
    }
}

async fn input(client: &mut Client, seq: u32, cmd: u8) {
    send(
        &mut client.ws,
        ClientMessage::Input {
            seq,
            client_tick: 0,
            payload: vec![cmd],
        },
    )
    .await;
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..200 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {what}");
}

fn realm_factory(
    neighbours: &[(&'static str, &'static str)],
    rejecting: &'static [&'static str],
) -> Factory {
    let neighbours: Vec<(String, String)> = neighbours
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect();
    Arc::new(move |zone: &str| {
        if !zone.starts_with("realm") && !zone.starts_with('z') {
            return None;
        }
        Some(RealmConfig {
            neighbour: neighbours
                .iter()
                .find(|(from, _)| from == zone)
                .map(|(_, to)| to.clone()),
            reject_all: rejecting.contains(&zone),
            gate: zone.ends_with(".inn"),
            freeze_ready: !zone.ends_with(".stuck"),
            move_facts: false,
        })
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_host_handoff_follows_redirect_and_ticket_is_single_use() {
    let (a, b, _dir) = two_hosts(
        base_config,
        &[("realm.east", "A"), ("realm.west", "B")],
        realm_factory(&[("realm.east", "realm.west")], &[]),
        Arc::new(LocalOwnership::new()),
    )
    .await;

    // Asking B for an A zone redirects without consuming the ticket.
    let t = ticket("ana", "realm.east", "n1");
    let mut ana = join_url(&b.addr.zone_url("realm.east"), &t, "")
        .await
        .unwrap();
    assert_eq!(ana.welcome.zone_id, "realm.east");
    assert!(a.server.is_active("realm.east"));
    assert!(!b.server.is_active("realm.east"));

    for seq in 1..=2 {
        input(&mut ana, seq, CMD_COUNT).await;
    }
    input(&mut ana, 3, CMD_HANDOFF).await;
    let (target, transfer) = loop {
        match control(&mut ana).await {
            Some(ServerMessage::Handoff {
                target_zone,
                transfer_ticket,
            }) => break (target_zone, transfer_ticket),
            Some(_) => continue,
            None => panic!("closed before Handoff"),
        }
    };
    assert_eq!(target, "realm.west");
    // The source fenced the player out before the ticket was usable.
    assert_eq!(
        a.server.inspect("realm.east", |h| h.player_count()).await,
        Some(0)
    );

    // Client follows: same host first (Redirect to B), then B admits.
    let mut west = join_url(&a.addr.zone_url(&target), &transfer, "")
        .await
        .unwrap();
    assert_eq!(west.welcome.zone_id, "realm.west");
    let arrived = loop {
        match control(&mut west).await.unwrap() {
            ServerMessage::Event { type_id, payload } if type_id == EV_ARRIVED => break payload,
            _ => continue,
        }
    };
    assert_eq!(arrived, b"ana:gold=2|realm.east");
    let arrivals = b
        .server
        .inspect("realm.west", |h| h.zone().arrivals.clone())
        .await
        .unwrap();
    assert_eq!(arrivals[0].principal, "ana");

    // Single use: neither the target nor the source accepts it again.
    match join_url(&b.addr.zone_url("realm.west"), &transfer, "").await {
        Err(ServerMessage::Error { code, .. }) => assert_eq!(code, error::UNAUTHORIZED),
        other => panic!("replay admitted: {:?}", other.map(|c| c.welcome)),
    }
    match join_url(&a.addr.zone_url("realm.east"), &transfer, "").await {
        Err(ServerMessage::Error { code, .. }) => assert_eq!(code, error::UNAUTHORIZED),
        other => panic!(
            "return after arrival admitted: {:?}",
            other.map(|c| c.welcome)
        ),
    }
    // A forged transfer (wrong key) is refused.
    let forged = TicketKey::new("nope").sign(&TicketClaims {
        principal: "mallory".into(),
        zone: "realm.west".into(),
        expires_at: 4_000_000_000,
        nonce: "x".into(),
        role: Role::Player,
        name: None,
    });
    assert!(join_url(&b.addr.zone_url("realm.west"), &forged, "")
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redirects_use_the_configured_public_url() {
    let (_a, b, _dir) = two_hosts(
        || {
            base_config().with_redirect_url(|host, zone| {
                format!("wss://game.example/hosts/{}/zones/{zone}", host.id)
            })
        },
        &[("realm.east", "A")],
        realm_factory(&[], &[]),
        Arc::new(LocalOwnership::new()),
    )
    .await;
    let mut ws = connect(&b.addr.zone_url("realm.east")).await;
    hello(&mut ws, &ticket("ana", "realm.east", "pub-1"), "", "").await;
    let mut mirror = ClientMirror::new();
    match next_control(&mut ws, &mut mirror).await {
        Some(ServerMessage::Redirect { zone_id, url }) => {
            assert_eq!(zone_id, "realm.east");
            assert_eq!(url, "wss://game.example/hosts/A/zones/realm.east");
        }
        other => panic!("expected Redirect, got {other:?}"),
    }
    // Per-host advertised URL in the directory (no mapping configured).
    let proxied = HostAddr::new("A", "10.0.0.2:2567").with_ws_url("wss://game.example/a/");
    assert_eq!(proxied.zone_url("z"), "wss://game.example/a/zones/z");
    assert_eq!(proxied.admin_url, "http://10.0.0.2:2567");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_handoff_returns_to_source_with_carried_state() {
    let (a, b, _dir) = two_hosts(
        base_config,
        &[("realm.east", "A"), ("realm.west", "B")],
        realm_factory(&[("realm.east", "realm.west")], &["realm.west"]),
        Arc::new(LocalOwnership::new()),
    )
    .await;
    let mut ana = join_url(
        &a.addr.zone_url("realm.east"),
        &ticket("ana", "realm.east", "r1"),
        "",
    )
    .await
    .unwrap();
    input(&mut ana, 1, CMD_COUNT).await;
    input(&mut ana, 2, CMD_HANDOFF).await;
    let transfer = loop {
        if let Some(ServerMessage::Handoff {
            transfer_ticket, ..
        }) = control(&mut ana).await
        {
            break transfer_ticket;
        }
    };
    // Target refuses (zone closed) -> policy: return to the source.
    match join_url(&b.addr.zone_url("realm.west"), &transfer, "").await {
        Err(ServerMessage::Error { code, .. }) => assert_eq!(code, error::REJECTED),
        other => panic!("expected rejection: {:?}", other.map(|c| c.welcome)),
    }
    let mut back = join_url(&a.addr.zone_url("realm.east"), &transfer, "")
        .await
        .unwrap();
    let arrived = loop {
        match control(&mut back).await.unwrap() {
            ServerMessage::Event { type_id, payload } if type_id == EV_ARRIVED => break payload,
            _ => continue,
        }
    };
    // Carried state comes back; `from` is the zone it never reached.
    assert_eq!(arrived, b"ana:gold=1|realm.west");
    // Once returned, the ticket is dead at the target too.
    assert!(join_url(&b.addr.zone_url("realm.west"), &transfer, "")
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_handoff_returns_to_a_source_on_the_same_host() {
    // Source and target share a host (and its nonce replay cache): the
    // target saw the ticket first, the return must still be admitted once.
    let (a, _b, _dir) = two_hosts(
        base_config,
        &[("realm.east", "A"), ("realm.west", "A")],
        realm_factory(&[("realm.east", "realm.west")], &["realm.west"]),
        Arc::new(LocalOwnership::new()),
    )
    .await;
    let mut ana = join_url(
        &a.addr.zone_url("realm.east"),
        &ticket("ana", "realm.east", "s1"),
        "",
    )
    .await
    .unwrap();
    input(&mut ana, 1, CMD_HANDOFF).await;
    let transfer = loop {
        if let Some(ServerMessage::Handoff {
            transfer_ticket, ..
        }) = control(&mut ana).await
        {
            break transfer_ticket;
        }
    };
    match join_url(&a.addr.zone_url("realm.west"), &transfer, "").await {
        Err(ServerMessage::Error { code, .. }) => assert_eq!(code, error::REJECTED),
        other => panic!("expected rejection: {:?}", other.map(|c| c.welcome)),
    }
    let back = join_url(&a.addr.zone_url("realm.east"), &transfer, "").await;
    assert!(back.is_ok(), "return admitted: {:?}", back.err());
    // The return is single use.
    assert!(join_url(&a.addr.zone_url("realm.east"), &transfer, "")
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boss_defeat_tiers_up_to_parent_and_grandparent_across_hosts() {
    let (a, b, _dir) = two_hosts(
        base_config,
        &[
            ("realm", "A"),
            ("realm.east", "A"),
            ("realm.east.keep", "B"),
        ],
        realm_factory(&[], &[]),
        Arc::new(LocalOwnership::new()),
    )
    .await;
    let mut ana = join_url(
        &b.addr.zone_url("realm.east.keep"),
        &ticket("ana", "realm.east.keep", "b1"),
        "",
    )
    .await
    .unwrap();
    input(&mut ana, 1, CMD_BOSS).await;
    for zone in ["realm.east", "realm"] {
        let server = a.server.clone();
        eventually(zone, || {
            let server = server.clone();
            async move {
                server
                    .inspect(zone, |h| {
                        h.zone().child_events.iter().any(|e| {
                            e.kind == "boss_defeated" && e.origin_zone == "realm.east.keep"
                        })
                    })
                    .await
                    .unwrap_or(false)
            }
        })
        .await;
    }
    let grand = a
        .server
        .inspect("realm", |h| h.zone().child_events.clone())
        .await
        .unwrap();
    assert_eq!(grand.len(), 1);
    assert_eq!(grand[0].from_zone, "realm.east");
    assert_eq!(grand[0].payload, b"realm.east.keep");
    // Parents were activated on demand on their owning host only.
    assert!(!b.server.is_active("realm.east"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dormant_zone_hibernates_to_sqlite_and_restores() {
    let path = std::env::temp_dir().join(format!(
        "dg-world-hibernate-{}-{}.sqlite3",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let store = Arc::new(SqliteZoneStateStore::open(&path).unwrap());
    let mut config = base_config();
    config.host.idle_tick_rate = None;
    config.host.dormant_after = Duration::from_millis(50);
    config.hibernate_after = Some(Duration::from_millis(100));
    let sink = MemoryFactSink::new();
    let (listener, addr) = listen().await;
    let factory = realm_factory(&[], &[]);
    let server = GameServer::<Realm>::new(config, move |z| factory(z))
        .with_fact_sink(sink.clone(), 64, RetryPolicy::default())
        .with_state_store(store.clone());
    serve(&server, listener);
    let url = format!("ws://{addr}/zones/realm.vault");

    let mut ana = join_url(&url, &ticket("ana", "realm.vault", "h1"), "")
        .await
        .unwrap();
    for seq in 1..=3 {
        input(&mut ana, seq, CMD_COUNT).await;
    }
    eventually("three counts", || {
        let server = server.clone();
        async move { server.inspect("realm.vault", |h| h.zone().counter).await == Some(3) }
    })
    .await;
    send(&mut ana.ws, ClientMessage::Leave).await;
    // The zone leaves the active set before its snapshot is written, so wait
    // for the save itself.
    eventually("hibernation save", || {
        let server = server.clone();
        async move { !server.is_active("realm.vault") && hibernation_outcomes(&server) >= 1 }
    })
    .await;
    let saved = store.load("realm.vault").unwrap().expect("snapshot stored");
    assert_eq!(saved.state, 3u32.to_le_bytes());
    // Facts emitted while disposing were still committed.
    eventually("dispose fact", || {
        let sink = sink.clone();
        async move {
            sink.committed()
                .iter()
                .any(|f| f.key() == "dispose:realm.vault:3")
        }
    })
    .await;
    assert_eq!(
        sink.committed()
            .iter()
            .filter(|f| f.command() == "realm.count")
            .count(),
        3
    );

    // Next activation restores it.
    let mut again = join_url(&url, &ticket("ana", "realm.vault", "h2"), "")
        .await
        .unwrap();
    let counter = loop {
        match control(&mut again).await.unwrap() {
            ServerMessage::Event { type_id, payload } if type_id == EV_JOINED => {
                break u32::from_le_bytes(payload.try_into().unwrap())
            }
            _ => continue,
        }
    };
    assert_eq!(counter, 3);
    drop(store);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hibernation_saves_succeed_after_a_host_restart_on_the_same_store() {
    let path = std::env::temp_dir().join(format!(
        "dg-world-restart-{}-{}.sqlite3",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let url_of = |addr: &str| format!("ws://{addr}/zones/realm.keep");
    let mut epochs = Vec::new();
    let mut count = 0u32;
    for run in 1..=2 {
        // A fresh process: new store handle, new (default) LocalOwnership.
        let store = Arc::new(SqliteZoneStateStore::open(&path).unwrap());
        let mut config = base_config();
        config.host.idle_tick_rate = None;
        config.host.dormant_after = Duration::from_millis(50);
        config.hibernate_after = Some(Duration::from_millis(100));
        let (listener, addr) = listen().await;
        let factory = realm_factory(&[], &[]);
        let server =
            GameServer::<Realm>::new(config, move |z| factory(z)).with_state_store(store.clone());
        serve(&server, listener);
        // Two activations per run, so the first run's last token exceeds
        // the restarted run's first token unless tokens survive restarts.
        for cycle in 1..=2u64 {
            let nonce = format!("r{run}-{cycle}");
            let mut ana = join_url(&url_of(&addr), &ticket("ana", "realm.keep", &nonce), "")
                .await
                .unwrap();
            input(&mut ana, 1, CMD_COUNT).await;
            count += 1;
            let expected = count;
            eventually("count", || {
                let server = server.clone();
                async move {
                    server.inspect("realm.keep", |h| h.zone().counter).await == Some(expected)
                }
            })
            .await;
            epochs.push(
                server
                    .inspect("realm.keep", |h| h.config().boot_epoch)
                    .await
                    .unwrap(),
            );
            send(&mut ana.ws, ClientMessage::Leave).await;
            // Saved, stale or error: the assertions below say which.
            eventually("hibernation save", || {
                let server = server.clone();
                async move { !server.is_active("realm.keep") && hibernation_outcomes(&server) >= cycle }
            })
            .await;
            assert_eq!(server.metrics().hibernation_saves("stale"), 0, "run {run}");
            assert_eq!(
                server.metrics().hibernation_saves("saved"),
                cycle,
                "run {run}"
            );
            let saved = store.load("realm.keep").unwrap().expect("snapshot");
            assert_eq!(saved.state, count.to_le_bytes(), "run {run} saved");
        }
        assert!(server
            .metrics()
            .render()
            .contains("distributed_game_hibernation_saves_total{outcome=\"saved\"} 2"));
        server.shutdown();
    }
    assert!(
        epochs.windows(2).all(|w| w[1] > w[0]),
        "epochs grow: {epochs:?}"
    );
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fenced_lease_keeps_zone_on_one_host_and_loss_stops_it() {
    let clock = distributed::ManualClock::starting_now();
    let leases = InMemoryLeaseStore::with_clock(clock.clone());
    let ownership: Arc<dyn ZoneOwnership> = Arc::new(FencedLeaseOwnership::new(leases));
    let config = || {
        let mut c = base_config();
        c.ownership_ttl = Duration::from_secs(10);
        c.ownership_renew_every = Duration::from_millis(50);
        c.activation_timeout = Duration::from_millis(200);
        c
    };
    // No directory: both hosts would happily serve any zone.
    let mut nodes = Vec::new();
    for id in ["A", "B"] {
        let (listener, addr) = listen().await;
        let f = realm_factory(&[], &[]);
        let server = GameServer::<Realm>::new(config(), move |z| f(z))
            .with_host_id(id)
            .with_ownership(ownership.clone());
        serve(&server, listener);
        nodes.push((server, addr));
    }
    let url = |i: usize| format!("ws://{}/zones/zboss", nodes[i].1);

    let mut ana = join_url(&url(0), &ticket("ana", "zboss", "l1"), "")
        .await
        .unwrap();
    match join_url(&url(1), &ticket("bo", "zboss", "l2"), "").await {
        Err(ServerMessage::Error { code, message }) => {
            assert_eq!(code, error::NOT_FOUND);
            assert!(message.contains("owned by A#"), "{message}");
        }
        other => panic!("second host ran the zone: {:?}", other.map(|c| c.welcome)),
    }

    // A stalls past its TTL (e.g. paused); its next renewal fails and the
    // zone stops there.
    clock.advance(Duration::from_secs(11));
    let kicked = loop {
        match control(&mut ana).await {
            Some(ServerMessage::Kick { code, .. }) => break code,
            Some(_) => continue,
            None => panic!("closed without Kick"),
        }
    };
    assert_eq!(kicked, kick::ZONE_DISPOSED);
    let a = nodes[0].0.clone();
    eventually("A stops", || {
        let a = a.clone();
        async move { !a.is_active("zboss") }
    })
    .await;
    // Now B may own it.
    let bo = join_url(&url(1), &ticket("bo", "zboss", "l3"), "")
        .await
        .unwrap();
    assert_eq!(bo.welcome.zone_id, "zboss");
}

fn versioned(version: &'static str) -> impl Fn() -> ServerConfig {
    move || {
        let mut c = base_config();
        c.host.content_version = version.into();
        c
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rolling_commit_reloads_stale_clients_in_gates_and_apply_may_await() {
    let safe_room = || {
        let mut c = base_config();
        c.host.content_version = "v1".into();
        c.host.stale_policy = StalePolicy::SafeRoom { roam: false };
        c
    };
    let (a, _b, _dir) = two_hosts(
        safe_room,
        &[("realm.inn", "A"), ("realm.field", "A")],
        realm_factory(&[], &[]),
        Arc::new(LocalOwnership::new()),
    )
    .await;
    let mut inn = join_url(
        &a.addr.zone_url("realm.inn"),
        &ticket("ina", "realm.inn", "g1"),
        "v1",
    )
    .await
    .unwrap();
    let mut field = join_url(
        &a.addr.zone_url("realm.field"),
        &ticket("fey", "realm.field", "g2"),
        "v1",
    )
    .await
    .unwrap();
    let server = a.server.clone();
    let coordinator = WorldCoordinator::new(Duration::from_secs(2))
        .participant(Arc::new(a.server.clone()))
        .with_abort_notes(AbortNotes {
            not_acked: "the ritual held the world".into(),
            apply_failed: "CDN said: {error}".into(),
        });

    // Async apply that fails: everyone resumes on v1 with the custom note.
    let failed = coordinator
        .rollout_async("v2", "", BarrierMode::Rolling, |_v| async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Err("pack missing".to_string())
        })
        .await;
    assert_eq!(
        failed,
        Err(RolloutError::ApplyFailed("pack missing".into()))
    );
    for c in [&mut inn, &mut field] {
        assert!(matches!(
            next_notice(c).await,
            Some(ServerMessage::Freeze(_))
        ));
        let resumed = next_notice(c).await;
        assert!(
            matches!(&resumed, Some(ServerMessage::Resume(n))
                if n.version == "v1" && n.notes == "CDN said: pack missing"),
            "{resumed:?}"
        );
    }

    // Async apply observed while every zone is frozen, then a rolling commit.
    let report = coordinator
        .rollout_async("v2", "ice", BarrierMode::Rolling, |v| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let frozen = server.inspect("realm.inn", |h| h.is_frozen()).await;
            assert_eq!(frozen, Some(true), "zones stay frozen while applying {v}");
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(report.version, "v2");
    for c in [&mut inn, &mut field] {
        assert!(matches!(
            next_notice(c).await,
            Some(ServerMessage::Freeze(_))
        ));
        assert!(matches!(
            next_notice(c).await,
            Some(ServerMessage::Resume(n)) if n.version == "v2"
        ));
    }
    // The stale client already inside the gate reloads there ...
    match next_notice(&mut inn).await {
        Some(ServerMessage::Kick { code, .. }) => assert_eq!(code, kick::RELOAD),
        other => panic!("expected Kick(RELOAD), got {other:?}"),
    }
    // ... the one in the field keeps playing its old version.
    let field_connected = a
        .server
        .inspect("realm.field", |h| h.players().all(|p| p.connected))
        .await;
    assert_eq!(field_connected, Some(true));
    // The reloaded client reconnects with its token on v2.
    let mut back = connect(&a.addr.zone_url("realm.inn")).await;
    hello(&mut back, "", &inn.welcome.reconnect_token, "v2").await;
    let mut mirror = ClientMirror::new();
    assert!(matches!(
        next_control(&mut back, &mut mirror).await,
        Some(ServerMessage::Welcome(w)) if w.content_version == "v2"
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn world_barrier_freezes_applies_and_resumes_across_hosts() {
    let (a, b, _dir) = two_hosts(
        versioned("v1"),
        &[("realm.a1", "A"), ("realm.a2", "A"), ("realm.b1", "B")],
        realm_factory(&[], &[]),
        Arc::new(LocalOwnership::new()),
    )
    .await;
    let mut clients = Vec::new();
    for (i, zone) in ["realm.a1", "realm.a2", "realm.b1"].into_iter().enumerate() {
        let host = if zone.ends_with("b1") { &b } else { &a };
        let c = join_url(
            &host.addr.zone_url(zone),
            &ticket(&format!("p{i}"), zone, &format!("w{i}")),
            "v1",
        )
        .await
        .unwrap();
        clients.push(c);
    }

    // Prepare only on A: frozen zones drop (and count) inputs.
    let notice = Notice {
        reason: "content_update".into(),
        version: "v2".into(),
        notes: String::new(),
    };
    a.server
        .prepare_barrier(1, notice.clone(), Duration::from_secs(2))
        .await
        .unwrap();
    assert!(matches!(
        next_notice(&mut clients[0]).await,
        Some(ServerMessage::Freeze(n)) if n.version == "v2"
    ));
    input(&mut clients[0], 1, CMD_COUNT).await;
    let server = a.server.clone();
    eventually("dropped input counted", || {
        let server = server.clone();
        async move {
            server
                .inspect("realm.a1", |h| h.inputs_dropped_frozen())
                .await
                .unwrap_or(0)
                >= 1
        }
    })
    .await;
    // Per-zone metrics count frozen drops under their own outcome.
    assert!(a
        .server
        .metrics()
        .render()
        .contains("distributed_game_zone_inputs_total{zone=\"realm.a1\",outcome=\"frozen\"} 1"));
    // /metrics coexists with the admin routes on the same router.
    let (status, _) = distributed_game::net::peer::post_json(
        &a.addr.admin_url,
        "/admin/barrier/nope",
        ADMIN,
        &serde_json::json!({}),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    assert_eq!(status, 404);
    a.server.abort_barrier(1, "test").await;
    assert!(matches!(
        next_notice(&mut clients[0]).await,
        Some(ServerMessage::Resume(n)) if n.version == "v1"
    ));
    assert_eq!(
        a.server.inspect("realm.a1", |h| h.zone().counter).await,
        Some(0),
        "input sent while frozen was not simulated"
    );
    // realm.a2 (also on A) went through the same aborted barrier.
    assert!(matches!(
        next_notice(&mut clients[1]).await,
        Some(ServerMessage::Freeze(_))
    ));
    assert!(
        matches!(next_notice(&mut clients[1]).await, Some(ServerMessage::Resume(n)) if n.version == "v1")
    );

    // Full rollout through the coordinator: A local, B over admin HTTP.
    let applied = Arc::new(Mutex::new(Vec::new()));
    let log = applied.clone();
    let coordinator = WorldCoordinator::new(Duration::from_secs(2))
        .participant(Arc::new(a.server.clone()))
        .participant(Arc::new(RemoteHost::new(b.addr.clone(), ADMIN)));
    let report = coordinator
        .rollout("v2", "new loot tables", BarrierMode::Rolling, |v| {
            log.lock().unwrap().push(v.to_string());
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(report.version, "v2");
    assert_eq!(*applied.lock().unwrap(), vec!["v2".to_string()]);
    for c in &mut clients {
        assert!(matches!(
            next_notice(c).await,
            Some(ServerMessage::Freeze(n)) if n.version == "v2"
        ));
        let resumed = next_notice(c).await;
        assert!(
            matches!(
                &resumed,
                Some(ServerMessage::Resume(n)) if n.version == "v2" && n.notes == "new loot tables"
            ),
            "{resumed:?}"
        );
    }
    assert_eq!(a.server.content_version(), "v2");
    assert_eq!(b.server.content_version(), "v2");
    // Rolling: v1 clients keep playing; facts now carry v2.
    input(&mut clients[2], 2, CMD_COUNT).await;
    let sink = b.sink.clone();
    eventually("v2 fact", || {
        let sink = sink.clone();
        async move {
            sink.committed()
                .iter()
                .any(|f| f.command() == "realm.count" && f.stamp().content_version == "v2")
        }
    })
    .await;
    // New zones activate on v2: a v1 fresh join is told to reload.
    match join_url(
        &a.addr.zone_url("realm.a3"),
        &ticket("late", "realm.a3", "w9"),
        "v1",
    )
    .await
    {
        Err(ServerMessage::Error { code, .. }) => assert_eq!(code, error::VERSION_MISMATCH),
        other => panic!("stale join admitted: {:?}", other.map(|c| c.welcome)),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn barrier_aborts_and_resumes_old_version_when_a_zone_never_acks() {
    let (a, b, _dir) = two_hosts(
        versioned("v1"),
        &[("realm.ok", "A"), ("realm.stuck", "B")],
        realm_factory(&[], &[]),
        Arc::new(LocalOwnership::new()),
    )
    .await;
    let mut ok = join_url(
        &a.addr.zone_url("realm.ok"),
        &ticket("p1", "realm.ok", "s1"),
        "v1",
    )
    .await
    .unwrap();
    let mut stuck = join_url(
        &b.addr.zone_url("realm.stuck"),
        &ticket("p2", "realm.stuck", "s2"),
        "v1",
    )
    .await
    .unwrap();
    let coordinator = WorldCoordinator::new(Duration::from_millis(300))
        .participant(Arc::new(a.server.clone()))
        .participant(Arc::new(RemoteHost::new(b.addr.clone(), ADMIN)));
    let mut applied = false;
    let err = coordinator
        .rollout("v2", "", BarrierMode::Hard, |_| {
            applied = true;
            Ok(())
        })
        .await
        .unwrap_err();
    assert_eq!(err, RolloutError::NotAcked(vec!["B/realm.stuck".into()]));
    assert!(!applied);
    assert!(matches!(
        next_notice(&mut ok).await,
        Some(ServerMessage::Freeze(n)) if n.version == "v2"
    ));
    assert!(matches!(
        next_notice(&mut ok).await,
        Some(ServerMessage::Resume(n)) if n.version == "v1"
    ));
    assert_eq!(a.server.content_version(), "v1");
    assert_eq!(b.server.content_version(), "v1");
    // The stuck zone never froze, so its client saw nothing; it keeps
    // simulating on v1.
    input(&mut stuck, 1, CMD_COUNT).await;
    let server = b.server.clone();
    eventually("stuck zone still runs", || {
        let server = server.clone();
        async move {
            server
                .inspect("realm.stuck", |h| (h.zone().counter, h.freeze_state()))
                .await
                == Some((1, FreezeState::Running))
        }
    })
    .await;
    // A wrong admin secret is refused.
    let wrong = WorldCoordinator::new(Duration::from_millis(300))
        .participant(Arc::new(RemoteHost::new(b.addr.clone(), "wrong")));
    assert!(matches!(
        wrong
            .rollout("v3", "", BarrierMode::Rolling, |_| Ok(()))
            .await,
        Err(RolloutError::NotAcked(_))
    ));
    assert_eq!(b.server.content_version(), "v1");
}

#[test]
fn directory_types_are_object_safe() {
    let dir: Arc<dyn ZoneDirectory> = Arc::new(StaticDirectory::new());
    assert!(dir.locate("x").is_none());
}

/// Hibernation snapshot saves finished so far, whatever their outcome.
fn hibernation_outcomes(server: &GameServer<Realm>) -> u64 {
    let m = server.metrics();
    m.hibernation_saves("saved") + m.hibernation_saves("stale") + m.hibernation_saves("error")
}
