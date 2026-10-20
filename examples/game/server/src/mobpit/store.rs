//! Mob Pit's durable state lives in celld cells (`examples/game/cells`);
//! this module is the zone server's side of them.
//!
//! - [`MobPitFactSink`] is the game's `FactSink`: optional artificial
//!   latency (the "slow storage" toggle), then
//!   `distributed_game::celld::CelldFactSink` sends the fact over the
//!   internal path to its cell — `crawler.record_kill` to
//!   `crawler/{principal}` (`CelldFactRoute::by_principal`), the runtime's
//!   player lifecycle facts to `presence/{principal}` (`lifecycle_route`) —
//!   then a fire-and-forget leaderboard update. The command id comes from
//!   the fact key, so a retry replays through the cell's command ledger
//!   instead of crediting twice; celld slow or down keeps facts in the
//!   zone's queue (`RetryPolicy::until_settled`), never in a local store.
//! - celld cannot enumerate cells, so before a principal's first kill
//!   commits the sink enlists it in the `crawler-roster/all` cell
//!   (`crawler.enlist`, idempotent by principal). Every committed Crawler
//!   cell is therefore on the roster; [`CrawlerCells`] reads both over the
//!   internal path (profiles, `/dev/mobpit/crawlers`, the board rebuild).
//! - Each kill commit also writes the `crawler.mob_killed` domain event to
//!   the cell's outbox in the same cell transaction. celld drains it to the
//!   `mobpit-outbox` Queue; the worker's consumer relays every message to
//!   this server's authenticated `/internal/celld-queue/relay`, which
//!   publishes it onto the [`ProjectionBus`] (a SQLite bus read by
//!   `mobpit-service`). Delivery is at-least-once with stable message ids;
//!   the projector deduplicates. A relay that fails is retried by the Queue.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed::bus::{celld_queue_relay_handler, CelldQueueRelayHandler, SqliteBus};
use distributed::cell_host::{InternalHttpSecret, CELL_INTERNAL_SECRET_HEADER};
use distributed::BusPublisher;
use distributed_game::celld::{CelldFactRoute, CelldFactSink};
use distributed_game::lifecycle::Lifecycle;
use distributed_game::{BoxFuture, Fact, FactError, FactReceipt, FactSink};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::board::Leaderboards;
use super::crawler::{
    KillReceipt, RecordKillInput, CRAWLER_KIND, ENLIST, PRESENCE_KIND, RECORD_KILL, ROSTER_KIND,
    ROSTER_SHARD,
};
use super::presence::Presence;

/// Command-ledger service id for game facts.
pub const SERVICE_ID: &str = "mobpit";
/// Principal for facts without a player (none are emitted today).
pub const SYSTEM_PRINCIPAL: &str = "mobpit-zone";
/// Env var of the celld base URL.
pub const CELLD_URL_ENV: &str = "CELLD_URL";

/// Where the cells are: the celld base URL and the internal secret shared
/// with the cells worker (`DISTRIBUTED_INTERNAL_SECRET`).
#[derive(Clone)]
pub struct CelldOptions {
    /// `http(s)://host:port` of celld serving `examples/game/cells`.
    pub url: String,
    /// Internal-path secret (also authenticates the Queue relay route).
    pub secret: InternalHttpSecret,
}

impl CelldOptions {
    /// Options for `url` and `secret`.
    pub fn new(url: impl Into<String>, secret: impl Into<String>) -> Result<Self, String> {
        let url = url.into().trim_end_matches('/').to_string();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(format!("celld URL must be http(s): {url}"));
        }
        Ok(Self {
            url,
            secret: InternalHttpSecret::new(secret.into())?,
        })
    }

    /// `CELLD_URL` and `DISTRIBUTED_INTERNAL_SECRET` from the environment.
    pub fn from_env() -> Result<Self, String> {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| format!("{name} is required (Mob Pit's cells run on celld)"))
        };
        Self::new(
            var(CELLD_URL_ENV)?,
            var(distributed::cell_host::CELL_INTERNAL_SECRET_ENV)?,
        )
    }
}

/// A Crawler cell's state as the cells worker reports it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CrawlerSummary {
    /// Display name at the last kill.
    pub name: String,
    /// Total XP.
    pub xp: u64,
    /// Mobs killed.
    pub kills: u32,
    /// Level for `xp`.
    pub level: u32,
    /// Committed stream version.
    pub version: u64,
}

/// One `crawler.mob_killed` event of a Crawler cell's history.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KillRecord {
    /// Stream position (the leaderboard occurrence).
    pub sequence: u64,
    /// Cell time of the commit (Unix ms).
    pub at_ms: u64,
    /// Principal.
    pub crawler: String,
    /// Display name at the kill.
    pub name: String,
    /// Mob instance.
    pub mob: String,
    /// Mob tier.
    pub tier: u8,
    /// XP credited.
    pub xp: u32,
}

/// `GET crawler/{principal}`: state (None before the first kill) and the
/// kill history, oldest first.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CrawlerView {
    /// Current state.
    pub state: Option<CrawlerSummary>,
    /// Kill events.
    pub kills: Vec<KillRecord>,
}

/// Read side of the Crawler cells and the roster, over the internal path.
#[derive(Clone)]
pub struct CrawlerCells {
    client: reqwest::Client,
    base: String,
    secret: InternalHttpSecret,
}

impl CrawlerCells {
    /// Reader for the cells at `celld`.
    pub fn new(celld: &CelldOptions) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("celld client: {error}"))?;
        Ok(Self {
            client,
            base: celld.url.clone(),
            secret: celld.secret.clone(),
        })
    }

    /// celld base URL.
    pub fn url(&self) -> &str {
        &self.base
    }

    async fn get(&self, kind: &str, shard: &str) -> Result<Value, String> {
        let url = format!("{}/{}/{}", self.base, kind, encode_segment(shard));
        let response = self
            .client
            .get(&url)
            .header(CELL_INTERNAL_SECRET_HEADER, self.secret.header_value())
            .send()
            .await
            .map_err(|error| format!("celld unreachable at {url}: {error}"))?;
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(format!("GET {url}: HTTP {status}: {body}"));
        }
        Ok(body)
    }

    /// Principals with a Crawler cell (sorted), from the roster.
    pub async fn principals(&self) -> Result<Vec<String>, String> {
        let body = self.get(ROSTER_KIND, ROSTER_SHARD).await?;
        let mut out: Vec<String> = serde_json::from_value(body["principals"].clone())
            .map_err(|error| format!("roster: {error}"))?;
        out.sort();
        Ok(out)
    }

    /// The durable presence entry of `principal` (`null` when none).
    pub async fn presence(&self, principal: &str) -> Result<Value, String> {
        Ok(self.get(PRESENCE_KIND, principal).await?["current"].take())
    }

    /// State and kill history of `principal`'s cell.
    pub async fn crawler(&self, principal: &str) -> Result<CrawlerView, String> {
        let body = self.get(CRAWLER_KIND, principal).await?;
        serde_json::from_value(body).map_err(|error| format!("crawler {principal}: {error}"))
    }
}

/// Like `distributed_game::celld`: a shard cannot add path segments.
fn encode_segment(segment: &str) -> String {
    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b':') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

/// The SQLite bus Crawler domain events are published to (consumed by
/// `mobpit-service`'s projector).
#[derive(Clone)]
pub struct ProjectionBus {
    relay: CelldQueueRelayHandler,
}

impl ProjectionBus {
    /// Open (creating) the bus at a `sqlite:` URL shared with the service.
    pub async fn open(url: &str) -> Result<Self, String> {
        use std::str::FromStr;
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(url)
            .map_err(|e| format!("projection bus {url}: {e}"))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(10));
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .map_err(|e| format!("projection bus {url}: {e}"))?;
        let bus = SqliteBus::new(pool);
        bus.ensure_tables()
            .await
            .map_err(|e| format!("projection bus tables: {e}"))?;
        Ok(Self {
            relay: celld_queue_relay_handler(BusPublisher::new(Arc::new(bus))),
        })
    }

    /// The Queue relay handler publishing onto this bus.
    pub fn relay_handler(&self) -> &CelldQueueRelayHandler {
        &self.relay
    }
}

/// A committed kill as the dev routes report it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommittedFact {
    /// Fact idempotency key.
    pub key: String,
    /// Command input as committed (needed to replay the exact command).
    pub input: serde_json::Value,
    /// Receipt.
    pub receipt: KillReceipt,
    /// True when the ledger replayed an earlier commit.
    pub replayed: bool,
}

/// Counters of the fact sink.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkStats {
    /// Commit attempts started.
    pub attempts: u64,
    /// First-time commits.
    pub committed: u64,
    /// Ledger replays (same key seen again).
    pub replayed: u64,
    /// Failed attempts (retryable or permanent).
    pub errors: u64,
    /// Facts currently inside `commit` (incl. the artificial latency).
    pub in_sink: u64,
    /// Lifecycle facts settled in presence cells (not counted above).
    pub presence: u64,
}

/// Shared sink state (lives in the world).
#[derive(Default)]
pub struct SinkState {
    latency_ms: AtomicU64,
    stats: Mutex<SinkStats>,
    recent: Mutex<VecDeque<CommittedFact>>,
}

impl SinkState {
    /// Artificial storage latency.
    pub fn latency(&self) -> Duration {
        Duration::from_millis(self.latency_ms.load(Ordering::Relaxed))
    }

    /// Set the artificial storage latency (clamped to 5 s).
    pub fn set_latency(&self, latency: Duration) -> Duration {
        let ms = latency.as_millis().min(5_000) as u64;
        self.latency_ms.store(ms, Ordering::Relaxed);
        Duration::from_millis(ms)
    }

    /// Counter snapshot.
    pub fn stats(&self) -> SinkStats {
        self.stats.lock().expect("stats").clone()
    }

    /// Most recent commits, newest last (at most 64).
    pub fn recent(&self) -> Vec<CommittedFact> {
        self.recent
            .lock()
            .expect("recent")
            .iter()
            .cloned()
            .collect()
    }

    fn update(&self, f: impl FnOnce(&mut SinkStats)) {
        f(&mut self.stats.lock().expect("stats"));
    }
}

/// The game's fact sink (cloned into every zone's fact worker).
#[derive(Clone)]
pub struct MobPitFactSink {
    celld: CelldFactSink,
    state: Arc<SinkState>,
    board: Arc<Leaderboards>,
    presence: Arc<Presence>,
    /// Principals enlisted on the roster by this process.
    enlisted: Arc<Mutex<HashSet<String>>>,
}

impl MobPitFactSink {
    /// Sink into the cells at `celld`, reporting into `state`, feeding
    /// `board`, mirroring settled lifecycle facts into `presence`.
    pub fn new(
        celld: &CelldOptions,
        state: Arc<SinkState>,
        board: Arc<Leaderboards>,
        presence: Arc<Presence>,
    ) -> Result<Self, String> {
        let sink = CelldFactSink::new(
            celld.url.clone(),
            celld.secret.clone(),
            SERVICE_ID,
            SYSTEM_PRINCIPAL,
        )?
        .route(CelldFactRoute::by_principal::<RecordKillInput>(
            CRAWLER_KIND,
        ))
        .route(CelldFactRoute::new(ENLIST, ROSTER_KIND, |_, _| {
            Some(ROSTER_SHARD.to_string())
        }))
        .lifecycle_route(PRESENCE_KIND);
        Ok(Self {
            celld: sink,
            state,
            board,
            presence,
            enlisted: Arc::default(),
        })
    }

    /// Enlist `principal` on the roster before its first kill commits (once
    /// per process; the ledger makes a repeat a replay).
    async fn enlist(&self, principal: &str, fact: &Fact) -> Result<(), FactError> {
        if self.enlisted.lock().expect("enlisted").contains(principal) {
            return Ok(());
        }
        let enlist = Fact::trusted(
            format!("{ENLIST}:{principal}"),
            ENLIST,
            json!({}),
            Some(principal.to_string()),
            fact.stamp().clone(),
        );
        self.celld.commit(&enlist).await?;
        self.enlisted
            .lock()
            .expect("enlisted")
            .insert(principal.to_string());
        Ok(())
    }

    async fn commit_kill(&self, fact: &Fact) -> Result<FactReceipt, FactError> {
        let latency = self.state.latency();
        if !latency.is_zero() {
            tokio::time::sleep(latency).await;
        }
        let principal = fact
            .principal()
            .ok_or_else(|| FactError::Permanent("kill facts need a player principal".into()))?;
        self.enlist(principal, fact).await?;
        let receipt = self.celld.commit(fact).await?;
        if fact.command() == RECORD_KILL {
            if let Ok(kill) = serde_json::from_value::<KillReceipt>(receipt.payload.clone()) {
                // Fire-and-forget: never awaits the leaderboard store.
                self.board.submit_kill(&kill);
                let mut recent = self.state.recent.lock().expect("recent");
                if recent.len() == 64 {
                    recent.pop_front();
                }
                recent.push_back(CommittedFact {
                    key: fact.key().to_string(),
                    input: fact.input().clone(),
                    receipt: kill,
                    replayed: receipt.replayed,
                });
            }
        }
        Ok(receipt)
    }
}

impl FactSink for MobPitFactSink {
    fn commit<'a>(&'a self, fact: &'a Fact) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
        Box::pin(async move {
            // Runtime lifecycle facts go to the player's presence cell, not
            // a Crawler cell (and not the kill counters); the settled ones
            // are mirrored into the in-process presence view.
            if let (Some(event), Some(principal)) = (Lifecycle::of(fact), fact.principal()) {
                let receipt = self.celld.commit(fact).await?;
                self.presence.apply(principal, fact.stamp(), &event);
                self.state.update(|s| s.presence += 1);
                return Ok(receipt);
            }
            self.state.update(|s| {
                s.attempts += 1;
                s.in_sink += 1;
            });
            let result = self.commit_kill(fact).await;
            self.state.update(|s| {
                s.in_sink -= 1;
                match &result {
                    Ok(r) if r.replayed => s.replayed += 1,
                    Ok(_) => s.committed += 1,
                    Err(_) => s.errors += 1,
                }
            });
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shards_cannot_add_path_segments() {
        assert_eq!(encode_segment("guest-0a1b"), "guest-0a1b");
        assert_eq!(encode_segment("odd:name"), "odd:name");
        assert_eq!(encode_segment("a/b c"), "a%2Fb%20c");
    }

    #[test]
    fn options_require_http_and_a_strong_secret() {
        assert!(CelldOptions::new("ftp://x", "test-only-secret-0123456789abcdef").is_err());
        assert!(CelldOptions::new("http://127.0.0.1:1", "short").is_err());
        let ok =
            CelldOptions::new("http://127.0.0.1:1/", "test-only-secret-0123456789abcdef").unwrap();
        assert_eq!(ok.url, "http://127.0.0.1:1");
    }
}
