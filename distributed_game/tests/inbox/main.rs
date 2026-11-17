//! The zone inbox: typed messages and the built-in kick, delivered on the
//! zone task between ticks; `GameServer::send`, `/admin/zone-message` and
//! `ZoneInboxClient` (auth, inactive zones, wrong owner).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed_game::lifecycle::{LeftReason, Lifecycle};
use distributed_game::net::peer::post_json;
use distributed_game::net::{
    GameServer, InboxError, ServerConfig, TicketKey, ZoneFactory, ZoneInboxClient,
    ZONE_MESSAGE_PATH,
};
use distributed_game::protocol::kick;
use distributed_game::world::{HostAddr, StaticDirectory};
use distributed_game::*;
use serde_json::json;

const SECRET: &str = "inbox-secret";
const ADMIN: &str = "inbox-admin";

/// Messages a service may send into a hall.
#[derive(Debug, Clone, PartialEq, GameEvent)]
pub enum HallMessage {
    /// Change the message of the day.
    #[game_event(id = 1)]
    SetMotd {
        /// New text.
        #[long]
        motd: String,
    },
    /// Numbered probe (ordering / tick checks).
    #[game_event(id = 2)]
    Probe {
        /// Sequence.
        n: u32,
    },
}

#[derive(Default)]
struct Shared {
    /// `(n, tick seen, delivered inside a tick?)` per probe.
    probes: Vec<(u32, u64, bool)>,
}

struct Hall {
    motd: String,
    in_tick: bool,
    ticks: u64,
    left: Vec<LeaveReason>,
    shared: Arc<Mutex<Shared>>,
}

impl Zone for Hall {
    type Config = Arc<Mutex<Shared>>;
    type Input = u8;
    type Timer = ();
    type Message = HallMessage;

    fn schema() -> EntitySchema {
        EntitySchema::new()
    }

    fn on_create(_ctx: &mut Ctx<'_, Self>, shared: Arc<Mutex<Shared>>) -> Self {
        Self {
            motd: String::new(),
            in_tick: false,
            ticks: 0,
            left: Vec::new(),
            shared,
        }
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<u8> {
        payload.first().copied()
    }

    fn on_tick(&mut self, _ctx: &mut Ctx<'_, Self>) {
        self.in_tick = true;
        self.ticks += 1;
        // A little work so a concurrent delivery would have a window.
        std::thread::sleep(Duration::from_micros(200));
        self.in_tick = false;
    }

    fn on_message(&mut self, ctx: &mut Ctx<'_, Self>, message: HallMessage) {
        match message {
            HallMessage::SetMotd { motd } => self.motd = motd,
            HallMessage::Probe { n } => {
                self.shared
                    .lock()
                    .unwrap()
                    .probes
                    .push((n, ctx.tick(), self.in_tick));
            }
        }
    }

    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, _player: PlayerId, reason: LeaveReason) {
        self.left.push(reason);
    }

    /// Keep ticking at the Hot rate with nobody connected.
    fn has_pending_work(&self) -> bool {
        true
    }

    fn replicate(&self, _out: &mut WorldView) {}
}

fn claims(principal: &str, nonce: &str) -> TicketClaims {
    TicketClaims::new(principal, "hall", u64::MAX, nonce, Role::Player)
}

#[test]
fn kick_removes_the_principal_with_a_lifecycle_fact_and_messages_reach_on_message() {
    let clock = ManualClock::new();
    let (queue, feed) = fact_channel(16, RetryPolicy::default());
    let mut h = ZoneHost::<Hall>::new(
        "hall",
        HostConfig {
            lifecycle_facts: true,
            ..HostConfig::default()
        },
        Arc::new(clock.clone()),
        Arc::default(),
    )
    .with_facts(queue);
    let ana = h.join(&claims("ana", "a"), 0, "").unwrap();
    h.join(&claims("bo", "b"), 0, "").unwrap();
    h.drain_outbound();

    let delivered = h.deliver(ZoneInbound::Kick {
        principal: "ana".into(),
        reason: "banned".into(),
    });
    assert_eq!(delivered.players_kicked, 1);
    assert_eq!(h.zone().left, [LeaveReason::Kicked]);
    assert_eq!(h.player_count(), 1);
    let out = h.drain_outbound();
    let kicked = out.iter().any(|o| match o {
        Outbound::Frame { client, frame } if *client == ana.client => matches!(
            ServerMessage::decode(frame, &()),
            Ok(ServerMessage::Kick { code, ref message }) if code == kick::BY_ZONE && message == "banned"
        ),
        _ => false,
    });
    assert!(kicked, "the client is told why");
    // Kicking someone absent is a no-op.
    assert_eq!(
        h.deliver(ZoneInbound::Kick {
            principal: "nobody".into(),
            reason: String::new(),
        }),
        Delivered::default()
    );
    h.deliver(ZoneInbound::Message(HallMessage::SetMotd {
        motd: "welcome".into(),
    }));
    assert_eq!(h.zone().motd, "welcome");

    drop(h);
    let sink = MemoryFactSink::new();
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let mut run = std::pin::pin!(feed.run(sink.clone(), |_| async {}));
    while std::future::Future::poll(run.as_mut(), &mut cx).is_pending() {}
    let ana: Vec<_> = sink
        .committed()
        .iter()
        .filter(|f| f.principal() == Some("ana"))
        .filter_map(Lifecycle::of)
        .collect();
    assert!(
        matches!(ana.last(), Some(Lifecycle::Left(l)) if l.reason == LeftReason::Kicked),
        "{ana:?}"
    );
}

#[test]
fn messages_are_in_the_surface_with_their_codec() {
    let surface = GameSurface::new("hall").messages::<<Hall as Zone>::Message>();
    let names: Vec<_> = surface.messages.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["set_motd", "probe"]);
    assert!(surface.to_json().contains("\"messages\""));
    // No messages: nothing in the JSON (older surfaces stay identical).
    let quiet = GameSurface::new("quiet").messages::<NoMessages>();
    assert!(!quiet.to_json().contains("messages"));
    let message = HallMessage::Probe { n: 7 };
    let back = HallMessage::decode_event(message.event_id(), &message.to_payload()).unwrap();
    assert_eq!(back, Some(message));
}

async fn serve(
    server: &GameServer<Hall>,
    listener: tokio::net::TcpListener,
) -> tokio::task::JoinHandle<()> {
    let serving = server.clone();
    tokio::spawn(async move {
        serving
            .serve(listener, std::future::pending())
            .await
            .unwrap();
    })
}

fn server(shared: Arc<Mutex<Shared>>) -> GameServer<Hall> {
    let mut config = ServerConfig::new(TicketKey::new(SECRET));
    config.admin_secret = Some(ADMIN.into());
    config.host.tick_rate = 120;
    GameServer::new(
        config,
        ZoneFactory::sync(move |a| a.zone_id.starts_with("hall").then(|| shared.clone())),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_message_runs_on_the_zone_task_between_ticks() {
    let shared = Arc::new(Mutex::new(Shared::default()));
    let server = server(shared.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let handle = serve(&server, listener).await;
    assert!(server.activate("hall").await);
    let senders: Vec<_> = (0..64u32)
        .map(|n| {
            let server = server.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(u64::from(n % 16) * 3)).await;
                server
                    .send("hall", ZoneInbound::Message(HallMessage::Probe { n }))
                    .await
            })
        })
        .collect();
    for sender in senders {
        sender.await.unwrap().unwrap();
    }
    let probes = shared.lock().unwrap().probes.clone();
    assert_eq!(probes.len(), 64);
    assert!(
        probes.iter().all(|(_, _, in_tick)| !in_tick),
        "never inside a tick"
    );
    let ticks: std::collections::BTreeSet<_> = probes.iter().map(|(_, t, _)| *t).collect();
    assert!(ticks.len() > 1, "delivered across several ticks: {ticks:?}");

    // A message for a zone that is not running is refused, never activates it.
    assert_eq!(
        server
            .send(
                "hall-cold",
                ZoneInbound::Message(HallMessage::Probe { n: 0 })
            )
            .await,
        Err(InboxError::Inactive)
    );
    assert!(!server.is_active("hall-cold"));
    server.shutdown();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_route_authenticates_refuses_inactive_and_redirects_to_the_owner() {
    // Two hosts; the shared directory places `hall.a` on A.
    let (la, lb) = (
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
    );
    let a_addr = HostAddr::new("A", la.local_addr().unwrap().to_string());
    let b_addr = HostAddr::new("B", lb.local_addr().unwrap().to_string());
    let directory = Arc::new(StaticDirectory::new());
    directory.add_host(a_addr.clone()).add_host(b_addr.clone());
    directory.assign("hall.a", "A").assign("hall.cold", "A");
    let shared = Arc::new(Mutex::new(Shared::default()));
    let a = server(shared.clone())
        .with_host_id("A")
        .with_directory(directory.clone());
    let b = server(shared.clone())
        .with_host_id("B")
        .with_directory(directory.clone());
    let (ha, hb) = (serve(&a, la).await, serve(&b, lb).await);
    assert!(a.activate("hall.a").await);
    let probe = |n: u32| {
        let m = HallMessage::Probe { n };
        json!({
            "zone": "hall.a",
            "message": {
                "type_id": m.event_id(),
                "payload": base64_url(&m.to_payload()),
            }
        })
    };
    let post = |url: String, secret: &'static str, body: serde_json::Value| async move {
        post_json(
            &url,
            ZONE_MESSAGE_PATH,
            secret,
            &body,
            Duration::from_secs(3),
        )
        .await
        .unwrap()
    };

    // Unauthenticated (wrong or empty secret) is refused before anything.
    for secret in ["", "wrong"] {
        let (status, body) = post(a_addr.admin_url.clone(), secret, probe(1)).await;
        assert_eq!(
            (status, body["error"].as_str()),
            (401, Some("unauthorized"))
        );
    }
    // The owner delivers.
    let (status, _) = post(a_addr.admin_url.clone(), ADMIN, probe(2)).await;
    assert_eq!(status, 200);
    // The wrong host answers with the owner instead of delivering.
    let (status, body) = post(b_addr.admin_url.clone(), ADMIN, probe(3)).await;
    assert_eq!(status, 421, "{body}");
    assert_eq!(body["error"], "wrong_owner");
    assert_eq!(body["owner"], "A");
    assert_eq!(body["admin_url"], a_addr.admin_url);
    // Inactive zones: 404, and still inactive.
    let mut cold = probe(4);
    cold["zone"] = json!("hall.cold");
    let (status, body) = post(a_addr.admin_url.clone(), ADMIN, cold).await;
    assert_eq!((status, body["error"].as_str()), (404, Some("inactive")));
    assert!(!a.is_active("hall.cold"));
    // Unknown message ids and malformed payloads: 400.
    let bad = json!({"zone": "hall.a", "message": {"type_id": 99, "payload": ""}});
    let (status, _) = post(a_addr.admin_url.clone(), ADMIN, bad).await;
    assert_eq!(status, 400);
    let short = json!({"zone": "hall.a", "message": {"type_id": 2, "payload": ""}});
    let (status, _) = post(a_addr.admin_url.clone(), ADMIN, short).await;
    assert_eq!(status, 400);

    // A service whose directory is stale (says B) follows the redirect.
    let stale = Arc::new(StaticDirectory::new());
    stale.add_host(b_addr.clone()).assign("hall.a", "B");
    let client = ZoneInboxClient::new(stale, ADMIN);
    client
        .send("hall.a", &HallMessage::SetMotd { motd: "hi".into() })
        .await
        .unwrap();
    assert_eq!(
        a.inspect("hall.a", |h| h.zone().motd.clone())
            .await
            .unwrap(),
        "hi"
    );
    client
        .send("hall.a", &HallMessage::Probe { n: 5 })
        .await
        .unwrap();
    assert_eq!(
        client.kick("hall.a", "nobody", "bye").await.unwrap(),
        Delivered::default()
    );
    assert_eq!(
        client.send("hall.cold", &HallMessage::Probe { n: 6 }).await,
        Err(InboxError::Inactive)
    );
    let unauthorized = ZoneInboxClient::new(directory.clone(), "wrong");
    assert_eq!(
        unauthorized
            .send("hall.a", &HallMessage::Probe { n: 7 })
            .await,
        Err(InboxError::Unauthorized)
    );
    let ns: Vec<_> = shared.lock().unwrap().probes.iter().map(|p| p.0).collect();
    assert_eq!(ns, [2, 5], "only the authorized, owned deliveries ran");

    a.shutdown();
    b.shutdown();
    for handle in [ha, hb] {
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .unwrap()
            .unwrap();
    }
}

fn base64_url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |acc, (i, b)| acc | (u32::from(*b) << (16 - 8 * i)));
        for i in 0..=chunk.len() {
            out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
        }
    }
    out
}
