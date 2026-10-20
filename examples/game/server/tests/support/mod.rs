//! Shared helpers for the multi-host smoke tests: start N in-process hosts
//! of a cluster demo on ephemeral ports, sharing one private `celld dev`
//! (zone leases and hibernation snapshots; `game_demos::devcelld`), and a
//! minimal protocol client that follows Redirects.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use distributed_game::net::TicketKey;
use distributed_game::protocol::{Hello, Welcome};
use distributed_game::world::HostAddr;
use distributed_game::{
    ClientMessage, ClientMirror, GameEvent, Role, ServerMessage, TicketClaims, PROTOCOL_VERSION,
};
use futures_util::{SinkExt, StreamExt};
use game_demos::cluster::{spawn_cluster, ClusterDemo, ClusterOptions, RunningHost};
use game_demos::common::DemoOptions;
use game_demos::devcelld::DevCelld;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

pub const SECRET: &str = "cluster-smoke-secret";

pub type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct Cluster<D: ClusterDemo> {
    pub hosts: Vec<RunningHost<D>>,
    stops: Vec<tokio::sync::oneshot::Sender<()>>,
    pub celld: DevCelld,
}

impl<D: ClusterDemo> Cluster<D> {
    /// Host by id.
    pub fn host(&self, id: &str) -> &RunningHost<D> {
        self.hosts
            .iter()
            .find(|h| h.world.host.id == id)
            .expect("host")
    }

    /// The host owning `zone`.
    pub fn owner(&self, zone: &str) -> &RunningHost<D> {
        self.host(&self.hosts[0].world.owner(zone))
    }

    pub async fn stop(self) {
        for stop in self.stops {
            let _ = stop.send(());
        }
        for host in self.hosts {
            let _ = host.demo.task.await;
        }
        drop(self.celld);
    }
}

/// Start hosts `ids` (all peers of each other) on 127.0.0.1:0.
pub async fn start_cluster<D: ClusterDemo>(ids: &[&str], hibernate_after: Duration) -> Cluster<D> {
    let mut listeners = Vec::new();
    for id in ids {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        listeners.push((id.to_string(), addr, listener));
    }
    let all: Vec<HostAddr> = listeners
        .iter()
        .map(|(id, addr, _)| HostAddr::new(id.clone(), addr.to_string()))
        .collect();
    let celld = DevCelld::start(None).unwrap_or_else(|error| panic!("celld dev: {error}"));
    let mut hosts = Vec::new();
    let mut stops = Vec::new();
    for (id, _, listener) in listeners {
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let options = ClusterOptions {
            host_id: id,
            peers: all.clone(),
            hibernate_after,
            celld: celld.options(),
        };
        let host = spawn_cluster::<D>(listener, DemoOptions::dev(SECRET), options, async move {
            let _ = stopped.await;
        })
        .unwrap();
        hosts.push(host);
        stops.push(stop);
    }
    Cluster {
        hosts,
        stops,
        celld,
    }
}

pub fn ticket(principal: &str, zone: &str, role: Role) -> String {
    static NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    TicketKey::new(SECRET).sign(&TicketClaims {
        principal: principal.into(),
        zone: zone.into(),
        expires_at: 4_000_000_000,
        nonce: format!("n-{principal}-{n}"),
        role,
        name: None,
        extensions: Default::default(),
    })
}

/// A connected protocol client.
pub struct Client {
    pub ws: Ws,
    pub mirror: ClientMirror,
    pub welcome: Welcome,
    /// URL that finally accepted the Hello.
    pub url: String,
    /// Redirects followed to get there.
    pub redirects: u32,
    pub seq: u32,
    /// Messages received after Welcome but not yet consumed.
    pub backlog: Vec<ServerMessage>,
    /// Every game Event received (type id, payload), in order.
    pub events: Vec<(u32, Vec<u8>)>,
}

async fn hello(ws: &mut Ws, ticket: &str) {
    let msg = ClientMessage::Hello(Hello {
        protocol_version: PROTOCOL_VERSION,
        content_version: String::new(),
        capabilities: 0,
        ticket: ticket.to_string(),
        reconnect_token: String::new(),
    });
    ws.send(Message::Binary(msg.encode().into())).await.unwrap();
}

/// Next decoded server message, None on close / timeout.
pub async fn next(ws: &mut Ws, mirror: &mut ClientMirror, wait: Duration) -> Option<ServerMessage> {
    loop {
        let frame = tokio::time::timeout(wait, ws.next()).await.ok()??;
        match frame {
            Ok(Message::Binary(bytes)) => return Some(mirror.apply(&bytes).unwrap()),
            Ok(Message::Close(_)) | Err(_) => return None,
            Ok(_) => continue,
        }
    }
}

/// Connect to `base/zones/zone`, following Redirects, until Welcome.
#[allow(clippy::result_large_err)]
pub async fn join(base: &str, zone: &str, ticket: &str) -> Result<Client, ServerMessage> {
    let mut url = format!("{base}/zones/{zone}");
    let mut redirects = 0;
    for _ in 0..4 {
        let (mut ws, _) = connect_async(&url).await.unwrap();
        hello(&mut ws, ticket).await;
        let mut mirror = ClientMirror::new();
        loop {
            match next(&mut ws, &mut mirror, Duration::from_secs(5))
                .await
                .expect("reply to Hello")
            {
                ServerMessage::Welcome(welcome) => {
                    return Ok(Client {
                        ws,
                        mirror,
                        welcome,
                        url,
                        redirects,
                        seq: 0,
                        backlog: Vec::new(),
                        events: Vec::new(),
                    })
                }
                ServerMessage::Redirect { url: to, .. } => {
                    url = to;
                    redirects += 1;
                    break;
                }
                ServerMessage::Pong { .. } => continue,
                other => return Err(other),
            }
        }
    }
    panic!("redirect loop")
}

impl Client {
    pub async fn input(&mut self, payload: Vec<u8>) {
        self.seq += 1;
        let msg = ClientMessage::Input {
            seq: self.seq,
            client_tick: 0,
            payload,
        };
        self.ws
            .send(Message::Binary(msg.encode().into()))
            .await
            .unwrap();
    }

    pub async fn leave(&mut self) {
        let _ = self
            .ws
            .send(Message::Binary(ClientMessage::Leave.encode().into()))
            .await;
    }

    /// Next message (backlog first).
    pub async fn next(&mut self, wait: Duration) -> Option<ServerMessage> {
        if !self.backlog.is_empty() {
            return Some(self.backlog.remove(0));
        }
        let msg = next(&mut self.ws, &mut self.mirror, wait).await;
        if let Some(ServerMessage::Event { type_id, payload }) = &msg {
            self.events.push((*type_id, payload.clone()));
        }
        msg
    }

    /// Typed events of `E` received so far.
    pub fn seen_events<E: GameEvent>(&self) -> Vec<E> {
        self.events
            .iter()
            .filter_map(|(t, p)| E::decode_event(*t, p).expect("event decodes"))
            .collect()
    }

    /// Wait (5 s) for the next event of `E` that `pick` accepts.
    pub async fn wait_event<E: GameEvent>(&mut self, pick: impl Fn(&E) -> bool) -> E {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if let ServerMessage::Event { type_id, payload } =
                self.next(left).await.expect("event within 5 s")
            {
                if let Some(event) = E::decode_event(type_id, &payload).expect("event decodes") {
                    if pick(&event) {
                        return event;
                    }
                }
            }
        }
    }

    /// Read messages until `pred` holds (10 s).
    pub async fn until(&mut self, what: &str, pred: impl Fn(&Client) -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !pred(self) {
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            if let Some(ServerMessage::Error { code, message }) =
                self.next(Duration::from_millis(100)).await
            {
                panic!("{what}: error {code}: {message}");
            }
        }
    }

    /// Entities of `kind` currently mirrored.
    pub fn count(&self, kind: u16) -> usize {
        self.mirror
            .entities
            .values()
            .filter(|e| e.kind == kind)
            .count()
    }
}

/// Minimal HTTP/1.1 client: `(status, JSON body or Null)`.
pub async fn http(addr: SocketAddr, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
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

/// Poll `check` until true (10 s).
pub async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !check().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
