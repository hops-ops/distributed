//! Zones as celld Durable Objects (`celld_zone`), end to end against
//! `tests/celld/zone-worker` (a tally zone, its Tally cells and its
//! ZoneActivity cell):
//!
//! - `single_node`: `celld dev`. Join with a ticket, input, facts settle in
//!   the player's cell, the empty zone goes dormant then hibernated, a new
//!   join wakes it with its state, tickets and transfer tickets are single
//!   use, a doorway hands the player to the next zone.
//! - `fleet`: three celld nodes in production mode on one RustFS bucket
//!   (docker compose). A zone is reached through a node that does not own
//!   it, the owner is killed while 200 facts are in flight: the socket
//!   closes with 1012, the client rejoins through another node, the zone
//!   restores, and every fact reaches the cell exactly once.
//!
//! ```text
//! (cd tests/celld/zone-worker && worker-build --release)
//! CELLD_BIN=… CELLD_ESBUILD=… cargo test -p distributed_game \
//!   --features celld,net --test celld_zone
//! ```
//!
//! Skips without a built worker / celld (and, for `fleet`, docker and the
//! aws CLI) unless `CELLD_ZONE_REQUIRED=1`.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use distributed_game::protocol::{error, ClientMessage, Hello, ServerMessage, PROTOCOL_VERSION};
use distributed_game::ticket::TicketKey;
use distributed_game::{Role, TicketClaims};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;

#[path = "../support/celld.rs"]
mod celld_support;
#[path = "../support/fleet.rs"]
mod fleet;

const INTERNAL_SECRET: &str = "test-only-internal-secret-change-me-2026";
const TICKET_SECRET: &str = "test-only-ticket-secret-change-me-2026";
const REQUIRED: &str = "CELLD_ZONE_REQUIRED";

fn worker() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/celld/zone-worker")
}

fn ticket(principal: &str, zone: &str) -> String {
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 120;
    let nonce = format!(
        "{principal}-{zone}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    TicketKey::new(TICKET_SECRET)
        .sign(&TicketClaims::new(principal, zone, exp, nonce, Role::Player).with_name(principal))
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Debug)]
enum Got {
    Server(ServerMessage),
    Closed(Option<u16>),
}

struct Player {
    ws: Ws,
    seq: u32,
}

impl Player {
    async fn connect(base: &str, zone: &str) -> Self {
        Self::try_connect(base, zone).await.expect("connect")
    }

    async fn try_connect(base: &str, zone: &str) -> Result<Self, String> {
        let url = format!("{}/rooms/{zone}", base.replacen("http://", "ws://", 1));
        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Self { ws, seq: 0 })
    }

    async fn send(&mut self, message: ClientMessage) {
        self.ws
            .send(Message::Binary(message.encode().into()))
            .await
            .expect("send");
    }

    async fn hello(&mut self, ticket: &str) {
        self.send(ClientMessage::Hello(Hello {
            protocol_version: PROTOCOL_VERSION,
            content_version: String::new(),
            capabilities: 0,
            ticket: ticket.into(),
            reconnect_token: String::new(),
        }))
        .await;
    }

    async fn input(&mut self, payload: &[u8]) {
        self.seq += 1;
        self.send(ClientMessage::Input {
            seq: self.seq,
            client_tick: 0,
            payload: payload.to_vec(),
        })
        .await;
    }

    async fn next(&mut self, within: Duration) -> Option<Got> {
        let deadline = Instant::now() + within;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let frame = tokio::time::timeout(left, self.ws.next()).await.ok()?;
            match frame {
                Some(Ok(Message::Binary(bytes))) => match ServerMessage::decode(&bytes, &()) {
                    Ok(message) => return Some(Got::Server(message)),
                    // Deltas need the Welcome's schema; these tests skip them.
                    Err(_) => continue,
                },
                Some(Ok(Message::Close(frame))) => {
                    return Some(Got::Closed(frame.map(|f| u16::from(f.code))))
                }
                Some(Ok(_)) => continue,
                Some(Err(_)) | None => return Some(Got::Closed(None)),
            }
        }
    }

    /// The first message matching `pick`, skipping others (Deltas, Pongs).
    async fn expect<T>(
        &mut self,
        within: Duration,
        what: &str,
        mut pick: impl FnMut(&Got) -> Option<T>,
    ) -> T {
        let deadline = Instant::now() + within;
        while let Some(got) = self
            .next(deadline.saturating_duration_since(Instant::now()))
            .await
        {
            if let Some(value) = pick(&got) {
                return value;
            }
            if let Got::Closed(code) = got {
                panic!("closed ({code:?}) while waiting for {what}");
            }
        }
        panic!("timed out waiting for {what}");
    }

    async fn welcome(&mut self) {
        self.expect(Duration::from_secs(20), "Welcome", |got| match got {
            Got::Server(ServerMessage::Welcome(_)) => Some(()),
            Got::Server(ServerMessage::Error { code, message }) => {
                panic!("join refused: {code} {message}")
            }
            _ => None,
        })
        .await
    }

    /// Event `type_id` carrying a u64 total.
    async fn total(&mut self, type_id: u32) -> u64 {
        self.expect(Duration::from_secs(20), "a total event", |got| match got {
            Got::Server(ServerMessage::Event {
                type_id: t,
                payload,
                ..
            }) if *t == type_id => Some(u64::from_le_bytes(payload[..8].try_into().unwrap())),
            _ => None,
        })
        .await
    }

    async fn refused(&mut self) -> u32 {
        self.expect(Duration::from_secs(20), "an Error", |got| match got {
            Got::Server(ServerMessage::Error { code, .. }) => Some(*code),
            Got::Server(ServerMessage::Welcome(_)) => panic!("admitted a used ticket"),
            _ => None,
        })
        .await
    }
}

/// GET a JSON route with the internal secret (Null on any failure: the
/// callers poll).
async fn get(base: &str, path: &str) -> Value {
    let response = reqwest::Client::new()
        .get(format!("{base}{path}"))
        .header("x-distributed-internal-secret", INTERNAL_SECRET)
        .send()
        .await
        .and_then(|r| r.error_for_status());
    match response {
        Ok(response) => response.json().await.unwrap_or(Value::Null),
        Err(_) => Value::Null,
    }
}

/// Poll `path` until `ok` (or panic after `within`).
async fn until(base: &str, path: &str, within: Duration, ok: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + within;
    loop {
        let value = get(base, path).await;
        if ok(&value) {
            return value;
        }
        assert!(Instant::now() < deadline, "{path}: last {value}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn status(value: &Value) -> &str {
    value["activity"]["status"].as_str().unwrap_or("")
}

fn exactly_once(value: &Value, count: u64) -> bool {
    let hits: Vec<&str> = value["hits"]
        .as_array()
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let distinct: std::collections::HashSet<_> = hits.iter().collect();
    assert_eq!(
        distinct.len(),
        hits.len(),
        "a fact committed twice: {value}"
    );
    hits.len() as u64 == count
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node() {
    let Some(celld) = celld_support::Celld::start_worker(&worker(), REQUIRED) else {
        return;
    };
    let base = celld.base();
    let long = Duration::from_secs(30);

    // Join, input, facts.
    let mut ana = Player::connect(&base, "room-a").await;
    let first = ticket("ana", "room-a");
    ana.hello(&first).await;
    ana.welcome().await;
    assert_eq!(ana.total(1).await, 0);
    let active = until(&base, "/zone-activity/room-a", long, |v| {
        status(v) == "active"
    })
    .await;
    assert_eq!(active["activity"]["restored"], false);
    ana.input(&[1, 5]).await;
    assert_eq!(ana.total(2).await, 5);
    until(&base, "/tally/ana", long, |v| exactly_once(v, 5)).await;

    // A used ticket is refused.
    let mut replay = Player::connect(&base, "room-a").await;
    replay.hello(&first).await;
    assert_eq!(replay.refused().await, error::UNAUTHORIZED);

    // Empty: dormant, then hibernated (dropped from memory).
    ana.send(ClientMessage::Leave).await;
    until(&base, "/zone-activity/room-a", long, |v| {
        status(v) == "dormant"
    })
    .await;
    until(&base, "/zone-activity/room-a", long, |v| {
        status(v) == "hibernated"
    })
    .await;
    let info = get(&base, "/rooms/room-a/info").await;
    assert_eq!(info["awake"], false, "{info}");

    // Wake with state.
    let mut ana = Player::connect(&base, "room-a").await;
    ana.hello(&ticket("ana", "room-a")).await;
    ana.welcome().await;
    assert_eq!(ana.total(1).await, 5, "restored from the snapshot");
    let woke = until(&base, "/zone-activity/room-a", long, |v| {
        status(v) == "active"
    })
    .await;
    assert_eq!(woke["activity"]["restored"], true);
    assert!(woke["activity"]["activations"].as_u64().unwrap() >= 2);

    // A doorway: handoff to room-b with a single-use transfer ticket.
    ana.input(&[2, b'b']).await;
    let transfer = ana
        .expect(long, "Handoff", |got| match got {
            Got::Server(ServerMessage::Handoff {
                target_zone,
                transfer_ticket,
            }) => {
                assert_eq!(target_zone, "room-b");
                Some(transfer_ticket.clone())
            }
            _ => None,
        })
        .await;
    let mut ana_b = Player::connect(&base, "room-b").await;
    ana_b.hello(&transfer).await;
    ana_b.welcome().await;
    let mut again = Player::connect(&base, "room-b").await;
    again.hello(&transfer).await;
    assert_eq!(again.refused().await, error::UNAUTHORIZED);
    ana_b.input(&[1, 3]).await;
    assert_eq!(ana_b.total(2).await, 3);
    until(&base, "/tally/ana", long, |v| exactly_once(v, 8)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fleet() {
    let Some(mut fleet) =
        fleet::Fleet::start(&worker(), 3, &[("CELLD_ZONE_NODES", "{nodes}")], REQUIRED)
    else {
        return;
    };
    let long = Duration::from_secs(60);
    // First touch through node-a places the zone (celld: first touch).
    let a = fleet.base(0);
    let mut probe = Player::connect(&a, "room-x").await;
    probe.hello(&ticket("probe", "room-x")).await;
    probe.welcome().await;
    let info = until(&a, "/rooms/room-x/info", long, |v| {
        v["node"].as_str().is_some_and(|n| !n.is_empty())
    })
    .await;
    let owner = fleet
        .index(info["node"].as_str().unwrap())
        .expect("known node");
    probe.send(ClientMessage::Leave).await;
    // Empty: the zone writes its snapshot and goes dormant.
    until(&a, "/zone-activity/room-x", long, |v| {
        status(v) == "dormant"
    })
    .await;
    let via = (owner + 1) % 3;
    let rejoin_via = (owner + 2) % 3;

    // Play through a node that does not own the zone (forwarded).
    let mut bo = Player::connect(&fleet.base(via), "room-x").await;
    bo.hello(&ticket("bo", "room-x")).await;
    bo.welcome().await;
    bo.input(&[1, 200]).await;
    assert_eq!(bo.total(2).await, 200);
    // Crash the owner once the 200 facts are in the zone's outbox (written
    // right after the tick that emitted them); some may already be
    // committed, most are in flight.
    until(&a, "/rooms/room-x/info", long, |v| {
        v["spooledFacts"].as_u64().unwrap_or(0) >= 200
    })
    .await;
    fleet.kill(owner);
    let closed = bo
        .expect(Duration::from_secs(30), "the close", |got| match got {
            Got::Closed(code) => Some(*code),
            _ => None,
        })
        .await;
    assert_eq!(
        closed,
        Some(u16::from(CloseCode::Restart)),
        "a forwarded socket of a crashed zone closes with 1012"
    );

    // Rejoin through the third node: celld recovers the zone elsewhere.
    let base = fleet.base(rejoin_via);
    let deadline = Instant::now() + long;
    let (_bo, restored) = loop {
        let mut bo = match Player::try_connect(&base, "room-x").await {
            Ok(bo) => bo,
            // celld answers 500 until the dead owner's lease expires.
            Err(_) => {
                assert!(Instant::now() < deadline, "the zone never came back");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        bo.hello(&ticket("bo", "room-x")).await;
        let got = bo
            .expect(
                Duration::from_secs(20),
                "Welcome or Error",
                |got| match got {
                    Got::Server(ServerMessage::Welcome(_)) => Some(true),
                    Got::Server(ServerMessage::Error { .. }) => Some(false),
                    Got::Closed(_) => Some(false),
                    _ => None,
                },
            )
            .await;
        if got {
            let total = bo.total(1).await;
            break (bo, total);
        }
        assert!(Instant::now() < deadline, "the zone never came back");
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert!(restored <= 200, "restored total {restored}");
    // Every hit the zone emitted reaches the cell exactly once.
    until(&base, "/tally/bo", long, |v| exactly_once(v, 200)).await;
    let activity = until(&base, "/zone-activity/room-x", long, |v| {
        status(v) == "active" && v["activity"]["restored"] == true
    })
    .await;
    assert_ne!(
        activity["activity"]["node"].as_str().unwrap_or(""),
        fleet.nodes[owner].name,
        "recovered on a live node"
    );
}
