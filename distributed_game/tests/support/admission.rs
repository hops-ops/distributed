//! The admission conformance suite: one set of cases every driver (the
//! `net` server, celld zone objects) must pass over a real WebSocket.
//! Both run [`distributed_game::admission`]; this checks they wire it the
//! same way. The target declares no ticket extensions, hosts every
//! `room-*` zone and keeps the default message rate (120/s, bursts of
//! 240).

#![allow(dead_code)]

use std::future::Future;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use distributed_game::protocol::{error, kick, ClientMessage, Hello, ServerMessage};
use distributed_game::ticket::TicketKey;
use distributed_game::{ClientMirror, Role, TicketClaims, TicketExtensions, PROTOCOL_VERSION};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A driver under test.
pub struct Target<A> {
    /// `ws://host:port` (zones at `{base}/zones/{zone}`).
    pub base: String,
    /// Its ticket key.
    pub key: TicketKey,
    /// Its Hello deadline.
    pub hello_timeout: Duration,
    /// Whether a zone is running (activated, awake) on the target.
    pub awake: A,
}

#[derive(Debug, PartialEq)]
enum Got {
    Message(ServerMessage),
    Closed,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn claims(zone: &str, case: &str) -> TicketClaims {
    let nonce = format!(
        "{case}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    TicketClaims::new("ana", zone, now() + 120, nonce, Role::Player)
}

async fn connect(base: &str, zone: &str) -> Ws {
    tokio_tungstenite::connect_async(format!("{base}/zones/{zone}"))
        .await
        .expect("connect")
        .0
}

async fn send(ws: &mut Ws, message: ClientMessage) {
    let _ = ws.send(Message::Binary(message.encode().into())).await;
}

fn hello(ticket: &str) -> ClientMessage {
    ClientMessage::Hello(Hello {
        protocol_version: PROTOCOL_VERSION,
        ticket: ticket.into(),
        ..Hello::default()
    })
}

/// Next server message other than Delta/Event/Pong, or the close.
async fn next(ws: &mut Ws, within: Duration) -> Got {
    let deadline = Instant::now() + within;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(left, ws.next()).await {
            Err(_) => panic!("no answer within {within:?}"),
            Ok(Some(Ok(Message::Binary(bytes)))) => {
                match ServerMessage::decode(&bytes, &ClientMirror::new()) {
                    Ok(ServerMessage::Event { .. } | ServerMessage::Pong { .. }) | Err(_) => {
                        continue
                    }
                    Ok(message) => return Got::Message(message),
                }
            }
            Ok(Some(Ok(Message::Close(_)))) | Ok(Some(Err(_))) | Ok(None) => return Got::Closed,
            Ok(Some(Ok(_))) => continue,
        }
    }
}

const ANSWER: Duration = Duration::from_secs(15);

async fn refused(target_base: &str, zone: &str, first: ClientMessage) -> u32 {
    let mut ws = connect(target_base, zone).await;
    send(&mut ws, first).await;
    match next(&mut ws, ANSWER).await {
        Got::Message(ServerMessage::Error { code, .. }) => code,
        other => panic!("{zone}: expected an Error, got {other:?}"),
    }
}

async fn joins(base: &str, zone: &str, ticket: &str) -> Ws {
    let mut ws = connect(base, zone).await;
    send(&mut ws, hello(ticket)).await;
    match next(&mut ws, ANSWER).await {
        Got::Message(ServerMessage::Welcome(_)) => ws,
        other => panic!("{zone}: expected Welcome, got {other:?}"),
    }
}

/// Run every case against `target`.
pub async fn run<A, F>(target: Target<A>)
where
    A: Fn(String) -> F,
    F: Future<Output = bool>,
{
    let Target {
        base,
        key,
        hello_timeout,
        awake,
    } = target;
    let zone = "room-admission";

    // Refused tickets: nothing wakes.
    let expired = key.sign(&TicketClaims {
        expires_at: now() - 1,
        ..claims("room-adm-expired", "expired")
    });
    let forged = TicketKey::new("not-the-zone-key-at-all").sign(&claims("room-adm-forged", "f"));
    let elsewhere = key.sign(&claims("room-adm-other", "elsewhere"));
    let ext = key.sign(&TicketClaims {
        extensions: TicketExtensions::from_json(serde_json::json!({"team": 1})).unwrap(),
        ..claims("room-adm-ext", "ext")
    });
    for (room, ticket) in [
        ("room-adm-garbage", "garbage".to_string()),
        ("room-adm-expired", expired),
        ("room-adm-forged", forged),
        ("room-adm-wrong", elsewhere),
        ("room-adm-ext", ext),
    ] {
        assert_eq!(
            refused(&base, room, hello(&ticket)).await,
            error::UNAUTHORIZED,
            "{room}"
        );
        assert!(
            !awake(room.to_string()).await,
            "{room} woke for a refused Hello"
        );
    }

    // Another protocol version: refused before the ticket is used.
    let ticket = key.sign(&claims(zone, "version"));
    let future = ClientMessage::Hello(Hello {
        protocol_version: PROTOCOL_VERSION + 1,
        ticket: ticket.clone(),
        ..Hello::default()
    });
    assert_eq!(refused(&base, zone, future).await, error::VERSION_MISMATCH);

    // The first frame must be Hello: an unauthenticated Ping is refused.
    assert_eq!(
        refused(&base, zone, ClientMessage::Ping { client_time: 1.0 }).await,
        error::BAD_MESSAGE
    );
    assert!(!awake(zone.to_string()).await);

    // Single use: the same ticket joins once.
    let mut first = joins(&base, zone, &ticket).await;
    assert_eq!(
        refused(&base, zone, hello(&ticket)).await,
        error::UNAUTHORIZED,
        "replayed ticket"
    );

    // Silence after the upgrade: closed at the Hello deadline.
    let mut silent = connect(&base, zone).await;
    let started = Instant::now();
    let got = next(&mut silent, hello_timeout + Duration::from_secs(10)).await;
    assert!(
        matches!(got, Got::Closed | Got::Message(ServerMessage::Error { .. })),
        "{got:?}"
    );
    assert!(started.elapsed() >= hello_timeout, "closed early");

    // Every message after the Welcome is rate limited: a flood of Pings
    // (not inputs) is kicked.
    for _ in 0..2_000 {
        send(&mut first, ClientMessage::Ping { client_time: 0.0 }).await;
    }
    let kicked = loop {
        match next(&mut first, ANSWER).await {
            Got::Message(ServerMessage::Kick { code, .. }) => break Some(code),
            Got::Message(_) => continue,
            Got::Closed => break None,
        }
    };
    assert_eq!(kicked, Some(kick::RATE_LIMITED));
}
