//! Per-player durable cells and the fact sink that routes into them.
//!
//! [`CrawlerCells`] keeps one native SQLite cell per principal under a data
//! directory (`Crawler%3A{principal}.sqlite3`, see `CellSqlitePath`). Every
//! command commits its event, the command-ledger receipt and snapshot rows in
//! one SQLite transaction with `synchronous=FULL`, so an acknowledged fact
//! survives `kill -9` (and power loss), and a half-applied one cannot exist.
//!
//! [`CrawlerFactSink`] is the game's `FactSink`: optional artificial latency
//! (the "slow storage" toggle), then `CellFactSink::commit` into the killer's
//! cell, then a fire-and-forget leaderboard update.
//!
//! Each commit also writes the `crawler.mob_killed` domain event into the
//! cell's outbox (same transaction). With a [`ProjectionBus`] attached
//! (`--projection-bus`), the sink drains that outbox onto the SQLite bus
//! after the commit, and a sweep drains every cell at startup and
//! periodically (a crash between commit and drain, a bus outage). Delivery
//! is at-least-once with stable message ids; the read-model service's
//! projector deduplicates. Without a bus the rows wait in the cells.

use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed::bus::SqliteBus;
use distributed::cell_host::{AggregateCell, CellSqlitePath, CellStreamStore};
use distributed::entity::EventRecord;
use distributed::BusPublisher;
use distributed::{RepositoryError, StreamIdentity};
use distributed_game::cell::CellFactSink;
use distributed_game::lifecycle::Lifecycle;
use distributed_game::{BoxFuture, Fact, FactError, FactReceipt, FactSink};
use serde::Serialize;

use super::board::Leaderboards;
use super::crawler::{Crawler, KillReceipt, RecordKill, AGGREGATE_TYPE, RECORD_KILL};
use super::presence::Presence;

/// Command-ledger service id for game facts.
pub const SERVICE_ID: &str = "mobpit";
/// Principal for facts without a player (none are emitted today).
pub const SYSTEM_PRINCIPAL: &str = "mobpit-zone";
const FILE_PREFIX: &str = "Crawler%3A";

/// One player's open cell.
pub struct CrawlerCell {
    cell: Arc<AggregateCell<Crawler>>,
    sink: CellFactSink<Crawler>,
    /// One drain at a time per cell keeps the published order = commit order.
    draining: tokio::sync::Mutex<()>,
}

impl CrawlerCell {
    /// Current state (`None` before the first kill).
    pub async fn load(&self) -> Result<Option<Crawler>, RepositoryError> {
        self.cell.load().await
    }

    /// The full durable event history of this cell, oldest first.
    pub async fn history(&self) -> Result<Vec<EventRecord>, RepositoryError> {
        self.cell.history().await
    }

    /// The cell (tests dispatch into it directly).
    pub fn cell(&self) -> &Arc<AggregateCell<Crawler>> {
        &self.cell
    }

    /// Publish this cell's pending outbox rows (its domain events) to `bus`,
    /// oldest first; returns how many were published.
    pub async fn drain_to(&self, bus: &ProjectionBus) -> Result<usize, String> {
        let _turn = self.draining.lock().await;
        let mut published = 0;
        loop {
            let outcome = self
                .cell
                .drain_outbox_with(
                    bus.publisher.clone(),
                    Duration::from_secs(30),
                    DRAIN_MAX_ATTEMPTS,
                    DRAIN_BATCH,
                )
                .await
                .map_err(|e| e.to_string())?;
            published += outcome.published;
            if outcome.released > 0 || outcome.failed > 0 {
                return Err(format!(
                    "projection bus publish failed ({} released, {} failed)",
                    outcome.released, outcome.failed
                ));
            }
            if outcome.claimed < DRAIN_BATCH {
                return Ok(published);
            }
        }
    }
}

const DRAIN_BATCH: usize = 32;
/// Rows are retried by later drains rather than dropped.
const DRAIN_MAX_ATTEMPTS: u32 = 10_000;

/// The SQLite bus Crawler domain events are published to (consumed by
/// `mobpit-service`'s projector).
#[derive(Clone)]
pub struct ProjectionBus {
    publisher: BusPublisher<SqliteBus>,
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
            publisher: BusPublisher::new(Arc::new(bus)),
        })
    }
}

/// Lazily opened `Crawler` cells under one directory.
pub struct CrawlerCells {
    root: PathBuf,
    open: Mutex<BTreeMap<String, Arc<CrawlerCell>>>,
}

impl CrawlerCells {
    /// Cells under `root` (created on first write).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            open: Mutex::new(BTreeMap::new()),
        }
    }

    /// Data directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Open (once per process) the cell of `principal`. Opening the same file
    /// twice in one process would give two connections; the cache prevents it.
    pub fn get(&self, principal: &str) -> Result<Arc<CrawlerCell>, RepositoryError> {
        let mut open = self.open.lock().expect("cells lock");
        if let Some(cell) = open.get(principal) {
            return Ok(cell.clone());
        }
        let identity = StreamIdentity::new(AGGREGATE_TYPE, principal.to_string())?;
        let store = CellStreamStore::open_sqlite(&CellSqlitePath::root(&self.root), identity)?;
        let cell = Arc::new(AggregateCell::from_store(store)?.mount(RecordKill));
        let entry = Arc::new(CrawlerCell {
            sink: CellFactSink::new(cell.clone(), SERVICE_ID, SYSTEM_PRINCIPAL),
            cell,
            draining: tokio::sync::Mutex::new(()),
        });
        open.insert(principal.to_string(), entry.clone());
        Ok(entry)
    }

    /// Whether `principal` already has a cell file (reads never create one).
    pub fn exists(&self, principal: &str) -> bool {
        CellSqlitePath::root(&self.root)
            .resolve(&format!("{AGGREGATE_TYPE}:{principal}"))
            .exists()
    }

    /// Principals with a cell file on disk (sorted).
    pub fn principals(&self) -> Vec<String> {
        let Ok(dir) = std::fs::read_dir(&self.root) else {
            return Vec::new();
        };
        let suffix = format!(".{}", distributed::cell_host::NATIVE_CELL_SQLITE_EXTENSION);
        let mut out: Vec<String> = dir
            .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
            .filter_map(|name| {
                let stem = name.strip_prefix(FILE_PREFIX)?.strip_suffix(&suffix)?;
                // Long names are hashed (`~`) and cannot be inverted; guest
                // and bot principals never are.
                percent_decode(stem)
            })
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Drain every cell on disk to `bus` (startup recovery and the periodic
    /// sweep). Returns rows published; per-cell errors are reported and
    /// retried by the next sweep.
    pub async fn drain_all(&self, bus: &ProjectionBus) -> usize {
        let mut published = 0;
        for principal in self.principals() {
            match self.get(&principal) {
                Ok(cell) => match cell.drain_to(bus).await {
                    Ok(n) => published += n,
                    Err(error) => eprintln!("mobpit: drain {principal}: {error}"),
                },
                Err(error) => eprintln!("mobpit: open {principal}: {error}"),
            }
        }
        published
    }
}

fn percent_decode(stem: &str) -> Option<String> {
    let bytes = stem.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = stem.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else if bytes[i] == b'~' {
            return None;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
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
    /// Lifecycle facts projected into presence (not counted above).
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
pub struct CrawlerFactSink {
    cells: Arc<CrawlerCells>,
    state: Arc<SinkState>,
    board: Arc<Leaderboards>,
    projection: Arc<std::sync::OnceLock<ProjectionBus>>,
    presence: Arc<Presence>,
}

impl CrawlerFactSink {
    /// Sink over `cells`, reporting into `state`, feeding `board`.
    pub fn new(
        cells: Arc<CrawlerCells>,
        state: Arc<SinkState>,
        board: Arc<Leaderboards>,
        projection: Arc<std::sync::OnceLock<ProjectionBus>>,
    ) -> Self {
        Self {
            cells,
            state,
            board,
            projection,
            presence: Arc::default(),
        }
    }

    /// Project lifecycle facts into `presence` (instead of a private one).
    pub fn with_presence(mut self, presence: Arc<Presence>) -> Self {
        self.presence = presence;
        self
    }

    async fn commit_inner(&self, fact: &Fact) -> Result<FactReceipt, FactError> {
        let latency = self.state.latency();
        if !latency.is_zero() {
            tokio::time::sleep(latency).await;
        }
        let principal = fact
            .principal()
            .ok_or_else(|| FactError::Permanent("kill facts need a player principal".into()))?;

        let cell = self
            .cells
            .get(principal)
            .map_err(|e| FactError::Retryable(format!("open cell: {e}")))?;
        let receipt = cell.sink.commit(fact).await?;
        if let Some(bus) = self.projection.get().cloned() {
            // Off the settlement path: the zone hears "saved" when the cell
            // commits; the read model follows from the outbox.
            let cell = cell.clone();
            tokio::spawn(async move {
                if let Err(error) = cell.drain_to(&bus).await {
                    eprintln!("mobpit: projection drain (retried by the sweep): {error}");
                }
            });
        }
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

impl FactSink for CrawlerFactSink {
    fn commit<'a>(&'a self, fact: &'a Fact) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
        Box::pin(async move {
            // Runtime lifecycle facts feed the presence read model, not a
            // cell (and not the kill counters).
            if let (Some(event), Some(principal)) = (Lifecycle::of(fact), fact.principal()) {
                let applied = self.presence.apply(principal, fact.stamp(), &event);
                self.state.update(|s| s.presence += 1);
                return Ok(FactReceipt {
                    payload: serde_json::json!({ "presence": applied }),
                    replayed: !applied,
                });
            }
            self.state.update(|s| {
                s.attempts += 1;
                s.in_sink += 1;
            });
            let result = self.commit_inner(fact).await;
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
    fn file_names_round_trip_to_principals() {
        let dir =
            std::env::temp_dir().join(format!("mobpit-cells-{}", crate::common::random_hex(12)));
        let cells = CrawlerCells::new(&dir);
        for p in ["guest-0a1b", "bot-3", "odd:name"] {
            cells.get(p).unwrap();
        }
        assert_eq!(cells.principals(), vec!["bot-3", "guest-0a1b", "odd:name"]);
        let _ = std::fs::remove_dir_all(dir);
    }
}
