//! **Leaderboard**: kills and XP boards projected from the Crawler cells.
//!
//! - Store: `InMemoryLeaderboard`, or `RedisLeaderboard` when `REDIS_URL`
//!   is set (sorted sets; every idempotent write is one Lua script).
//! - Live path (fire-and-forget): after a kill commits, the fact sink calls
//!   [`Leaderboards::submit_kill`], which enqueues one occurrence on a
//!   `BackgroundLeaderboard` and returns immediately. A full queue or a Redis
//!   outage drops the update (counted); a rebuild heals it.
//! - Idempotency: every kill becomes a `DomainEventOccurrence` whose id is
//!   derived from `(producer, "Crawler:{principal}", event sequence,
//!   "mob_killed")` — the same id whether it comes from the live receipt, a
//!   ledger replay of the same fact, or the cell's event history. The
//!   projector's apply keys (`"{id}#{change}"`) make every repeat a no-op.
//! - Rebuild ([`Leaderboards::rebuild`]): read every cell's durable event
//!   history, project it into the *staging* boards and atomically promote
//!   them. Runs at startup too (the in-memory board starts empty).

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use distributed::leaderboard::{
    BackgroundLeaderboard, BackgroundLeaderboardOptions, Board, InMemoryLeaderboard,
    LeaderboardChange, LeaderboardEntry, LeaderboardError, LeaderboardProjection,
    LeaderboardProjector, LeaderboardStore, RedisLeaderboard, ScorePolicy, ScoreSubmission,
};
use distributed::{DomainEventOccurrence, ExternalEventSource};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::crawler::{KillReceipt, MobKilledArgs, AGGREGATE_TYPE, MOB_KILLED_EVENT};
use super::store::CrawlerCells;

/// Kills board.
pub const KILLS_BOARD: &str = "mobpit.kills";
/// XP board.
pub const XP_BOARD: &str = "mobpit.xp";
/// Producer of kill occurrences.
pub const PRODUCER: &str = "mobpit.crawler-cell";
/// Rows pushed to clients.
pub const TOP_N: usize = 10;

/// Leaderboard projection input: one committed kill.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, distributed::DomainEvent)]
#[domain_event(name = "mobpit.mob_killed", version = 1)]
pub struct MobKilled {
    /// Principal credited.
    pub crawler: String,
    /// Mob instance.
    pub mob: String,
    /// Mob tier.
    pub tier: u8,
    /// XP credited.
    pub xp: u32,
}

/// The occurrence of the kill recorded at `sequence` in `principal`'s cell.
/// Deterministic: same inputs → same id → deduplicated by the projector.
pub fn kill_occurrence(
    principal: &str,
    sequence: u64,
    at_ms: u64,
    body: &MobKilled,
) -> Result<DomainEventOccurrence, String> {
    DomainEventOccurrence::capture_external(
        ExternalEventSource {
            producer: PRODUCER.into(),
            stream: format!("{AGGREGATE_TYPE}:{principal}"),
            position: sequence,
            key: "mob_killed".into(),
        },
        UNIX_EPOCH + Duration::from_millis(at_ms),
        Default::default(),
        body,
    )
    .map_err(|e| format!("capture kill occurrence: {e:?}"))
}

/// kills += 1, xp += xp.
pub struct KillProjection {
    kills: Board,
    xp: Board,
}

impl KillProjection {
    fn new() -> Self {
        Self {
            kills: Board::high_first(KILLS_BOARD).expect("board name"),
            xp: Board::high_first(XP_BOARD).expect("board name"),
        }
    }
}

impl LeaderboardProjection for KillProjection {
    fn handles(&self, event_name: &str) -> bool {
        event_name == <MobKilled as distributed::DomainEvent>::DESCRIPTOR.name
    }

    fn project(
        &self,
        event: &DomainEventOccurrence,
    ) -> Result<Vec<LeaderboardChange>, LeaderboardError> {
        let kill: MobKilled = event
            .decode_body()
            .map_err(|e| LeaderboardError::invalid(format!("mob_killed body: {e:?}")))?;
        Ok(vec![
            LeaderboardChange::new(
                self.kills.clone(),
                ScoreSubmission::new(&kill.crawler, 1.0, ScorePolicy::Increment),
            ),
            LeaderboardChange::new(
                self.xp.clone(),
                ScoreSubmission::new(&kill.crawler, f64::from(kill.xp), ScorePolicy::Increment),
            ),
        ])
    }
}

type Projector = LeaderboardProjector<dyn LeaderboardStore, KillProjection>;

/// Leaderboard service shared by the zone, the fact sink and the routes.
pub struct Leaderboards {
    store: Arc<dyn LeaderboardStore>,
    projector: Projector,
    background: BackgroundLeaderboard,
    backend: &'static str,
    kills: Board,
    xp: Board,
    names: Mutex<HashMap<String, String>>,
    snapshot: Mutex<Arc<Value>>,
    version: AtomicU64,
    dirty: AtomicBool,
    last_rebuild: Mutex<Option<Value>>,
    rebuilding: tokio::sync::Mutex<()>,
}

impl Leaderboards {
    /// Redis when `redis_url` is given, else in memory. Must run inside a
    /// Tokio runtime (spawns the background applier and the refresher).
    pub fn open(redis_url: Option<&str>, redis_prefix: &str) -> Result<Arc<Self>, String> {
        let (store, backend): (Arc<dyn LeaderboardStore>, _) = match redis_url {
            Some(url) => (
                Arc::new(
                    RedisLeaderboard::connect(url)
                        .map_err(|e| e.to_string())?
                        .with_prefix(redis_prefix),
                ),
                "redis",
            ),
            None => (Arc::new(InMemoryLeaderboard::new()), "memory"),
        };
        let projection = Arc::new(KillProjection::new());
        let projector: Projector = LeaderboardProjector::new(store.clone(), projection);
        let background = BackgroundLeaderboard::spawn(
            projector.clone(),
            BackgroundLeaderboardOptions::default(),
        );
        let boards = Arc::new(Self {
            store,
            projector,
            background,
            backend,
            kills: Board::high_first(KILLS_BOARD).expect("board name"),
            xp: Board::high_first(XP_BOARD).expect("board name"),
            names: Mutex::new(HashMap::new()),
            snapshot: Mutex::new(Arc::new(json!({ "version": 0, "kills": [], "xp": [] }))),
            version: AtomicU64::new(0),
            dirty: AtomicBool::new(true),
            last_rebuild: Mutex::new(None),
            rebuilding: tokio::sync::Mutex::new(()),
        });
        tokio::spawn(refresh_loop(Arc::downgrade(&boards)));
        Ok(boards)
    }

    /// `"memory"` or `"redis"`.
    pub fn backend(&self) -> &'static str {
        self.backend
    }

    /// The store (tests read scores directly).
    pub fn store(&self) -> &Arc<dyn LeaderboardStore> {
        &self.store
    }

    /// The live projector (tests replay occurrences through it).
    pub fn projector(&self) -> &Projector {
        &self.projector
    }

    /// Board handles `(kills, xp)`.
    pub fn boards(&self) -> (&Board, &Board) {
        (&self.kills, &self.xp)
    }

    /// Fire-and-forget live update for a committed (or replayed) kill.
    pub fn submit_kill(&self, kill: &KillReceipt) {
        self.remember_name(&kill.crawler, &kill.name);
        match kill_occurrence(
            &kill.crawler,
            kill.sequence,
            kill.at_ms,
            &MobKilled {
                crawler: kill.crawler.clone(),
                mob: kill.mob.clone(),
                tier: kill.tier,
                xp: kill.xp,
            },
        ) {
            Ok(occurrence) => {
                self.background.submit(occurrence);
            }
            Err(error) => eprintln!("mobpit: leaderboard update skipped: {error}"),
        }
    }

    fn remember_name(&self, principal: &str, name: &str) {
        if !name.is_empty() {
            self.names
                .lock()
                .expect("names")
                .insert(principal.to_string(), name.to_string());
        }
    }

    fn name_of(&self, principal: &str) -> String {
        self.names
            .lock()
            .expect("names")
            .get(principal)
            .cloned()
            .unwrap_or_else(|| principal.to_string())
    }

    /// Version of the pushed snapshot (bumps when it changes).
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Latest top-N snapshot (JSON pushed as `EVENT_BOARD`).
    pub fn snapshot(&self) -> Arc<Value> {
        self.snapshot.lock().expect("snapshot").clone()
    }

    /// Force a snapshot refresh on the next refresher pass.
    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    fn rows(&self, entries: Vec<LeaderboardEntry>) -> Value {
        Value::Array(
            entries
                .into_iter()
                .map(|e| {
                    json!({
                        "rank": e.rank + 1,
                        "member": e.member,
                        "name": self.name_of(&e.member),
                        "score": e.score,
                    })
                })
                .collect(),
        )
    }

    async fn top_rows(&self) -> Result<(Value, Value, u64), LeaderboardError> {
        let kills = self.store.top(&self.kills, TOP_N).await?;
        let xp = self.store.top(&self.xp, TOP_N).await?;
        let members = self.store.len(&self.kills).await?;
        Ok((self.rows(kills), self.rows(xp), members))
    }

    async fn refresh(&self) -> Result<(), LeaderboardError> {
        let (kills, xp, members) = self.top_rows().await?;
        let stats = self.background.stats();
        let mut body = json!({
            "backend": self.backend,
            "members": members,
            "kills": kills,
            "xp": xp,
            "background": {
                "enqueued": stats.enqueued,
                "applied": stats.applied,
                "dropped": stats.dropped,
                "failed": stats.failed,
            },
            "lastRebuild": *self.last_rebuild.lock().expect("rebuild"),
        });
        let mut snapshot = self.snapshot.lock().expect("snapshot");
        let mut current = (**snapshot).clone();
        if let Some(map) = current.as_object_mut() {
            map.remove("version");
        }
        if current != body {
            let version = self.version.load(Ordering::Acquire) + 1;
            body["version"] = json!(version);
            *snapshot = Arc::new(body);
            self.version.store(version, Ordering::Release);
        }
        Ok(())
    }

    /// Rank, score and neighbours of `member` on both boards.
    pub async fn around(&self, member: &str, radius: usize) -> Result<Value, LeaderboardError> {
        let mut out = serde_json::Map::new();
        for (key, board) in [("kills", &self.kills), ("xp", &self.xp)] {
            let rank = self.store.rank(board, member).await?;
            let score = self.store.score(board, member).await?;
            let around = self.store.around(board, member, radius).await?;
            out.insert(
                key.into(),
                json!({ "rank": rank.map(|r| r + 1), "score": score, "around": self.rows(around) }),
            );
        }
        Ok(Value::Object(out))
    }

    /// Score of `member` on (kills, xp).
    pub async fn scores(&self, member: &str) -> Result<(f64, f64), LeaderboardError> {
        Ok((
            self.store.score(&self.kills, member).await?.unwrap_or(0.0),
            self.store.score(&self.xp, member).await?.unwrap_or(0.0),
        ))
    }

    /// Rebuild both boards from every cell's durable event history and check
    /// the result against the cells' current state.
    pub async fn rebuild(&self, cells: &CrawlerCells) -> Result<Value, String> {
        let _one_at_a_time = self.rebuilding.lock().await;
        let started = Instant::now();
        let before = self.top_rows().await.map_err(|e| e.to_string())?;
        let mut occurrences = Vec::new();
        let mut expected: BTreeMap<String, (u32, u64)> = BTreeMap::new();
        let principals = cells.principals();
        for principal in &principals {
            let cell = cells.get(principal).map_err(|e| e.to_string())?;
            for record in cell.history().await.map_err(|e| e.to_string())? {
                if record.event_name != MOB_KILLED_EVENT {
                    continue;
                }
                let (crawler, name, mob, tier, xp): MobKilledArgs =
                    record.decode().map_err(|e| e.to_string())?;
                self.remember_name(&crawler, &name);
                let at_ms = record
                    .timestamp
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis() as u64);
                occurrences.push(kill_occurrence(
                    principal,
                    record.sequence,
                    at_ms,
                    &MobKilled {
                        crawler,
                        mob,
                        tier,
                        xp,
                    },
                )?);
            }
            if let Some(state) = cell.load().await.map_err(|e| e.to_string())? {
                expected.insert(principal.clone(), (state.kills, state.xp));
            }
        }
        let report = self
            .projector
            .rebuild(&[self.kills.clone(), self.xp.clone()], &occurrences)
            .await
            .map_err(|e| e.to_string())?;
        let after = self.top_rows().await.map_err(|e| e.to_string())?;
        let mut mismatches = Vec::new();
        for (principal, (kills, xp)) in &expected {
            let (k, x) = self.scores(principal).await.map_err(|e| e.to_string())?;
            if k != f64::from(*kills) || x != *xp as f64 {
                mismatches.push(json!({
                    "member": principal, "cell": { "kills": kills, "xp": xp }, "board": { "kills": k, "xp": x }
                }));
            }
        }
        let result = json!({
            "backend": self.backend,
            "crawlers": principals.len(),
            "events": occurrences.len(),
            "applied": report.applied,
            "duplicates": report.duplicates,
            "identicalTop": before.0 == after.0 && before.1 == after.1,
            "matchesCells": mismatches.is_empty(),
            "mismatches": mismatches,
            "tookMs": started.elapsed().as_secs_f64() * 1000.0,
        });
        *self.last_rebuild.lock().expect("rebuild") = Some(result.clone());
        self.mark_dirty();
        let _ = self.refresh().await;
        Ok(result)
    }
}

async fn refresh_loop(boards: std::sync::Weak<Leaderboards>) {
    let mut last_applied = u64::MAX;
    loop {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let Some(boards) = boards.upgrade() else {
            return;
        };
        let applied = boards.background.stats().applied;
        if applied != last_applied || boards.dirty.swap(false, Ordering::AcqRel) {
            if boards.refresh().await.is_ok() {
                last_applied = applied;
            } else {
                boards.mark_dirty();
            }
        }
    }
}
