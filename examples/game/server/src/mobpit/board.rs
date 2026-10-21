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
//! - Rebuild ([`Leaderboards::rebuild`]): read the durable event history
//!   of every crawler in the directory (`super::directory`) from its cell,
//!   project it into the *staging* boards and atomically promote them.
//!   Runs at startup too (the in-memory board starts empty).
//! - Snapshot ([`BoardSnapshot`]): a `#[derive(GameRecord)]` struct, pushed
//!   to clients as the zone's typed `board` event and served as camelCase
//!   JSON by `/dev/mobpit/leaderboard`; the generated TypeScript type
//!   describes both.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use distributed::leaderboard::{
    BackgroundLeaderboard, BackgroundLeaderboardOptions, Board, InMemoryLeaderboard,
    LeaderboardChange, LeaderboardChangeListener, LeaderboardEntry, LeaderboardError,
    LeaderboardProjection, LeaderboardProjector, LeaderboardStore, RedisLeaderboard, ScorePolicy,
    ScoreSubmission,
};
use distributed::{DomainEventOccurrence, ExternalEventSource};
use distributed_game::GameRecord;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::crawler::{KillReceipt, AGGREGATE_TYPE};
use super::store::CrawlerCells;

pub use mobpit_crawler::{KILLS_BOARD, XP_BOARD};
/// Producer of kill occurrences.
pub const PRODUCER: &str = "mobpit.crawler-cell";
/// Rows pushed to clients.
pub const TOP_N: usize = 10;

/// One leaderboard row.
#[derive(Debug, Clone, PartialEq, Serialize, GameRecord)]
#[serde(rename_all = "camelCase")]
pub struct BoardRow {
    /// 1-based rank.
    pub rank: u32,
    /// Principal.
    pub member: String,
    /// Last display name seen with a kill.
    pub name: String,
    /// Score (kills or XP).
    pub score: f64,
}

/// Live-update counters of the background applier.
#[derive(Debug, Clone, Default, PartialEq, Serialize, GameRecord)]
#[serde(rename_all = "camelCase")]
pub struct BackgroundStats {
    /// Occurrences enqueued.
    pub enqueued: u64,
    /// Applied to the store.
    pub applied: u64,
    /// Dropped (queue full / store outage).
    pub dropped: u64,
    /// Failed to apply.
    pub failed: u64,
}

/// Kills and XP of one principal.
#[derive(Debug, Clone, PartialEq, Serialize, GameRecord)]
#[serde(rename_all = "camelCase")]
pub struct Scores {
    /// Kills.
    pub kills: f64,
    /// XP.
    pub xp: f64,
}

/// A principal whose board scores disagree with its cell.
#[derive(Debug, Clone, PartialEq, Serialize, GameRecord)]
#[serde(rename_all = "camelCase")]
pub struct Mismatch {
    /// Principal.
    pub member: String,
    /// What the cell says.
    pub cell: Scores,
    /// What the board says.
    pub board: Scores,
}

/// Result of [`Leaderboards::rebuild`].
#[derive(Debug, Clone, PartialEq, Serialize, GameRecord)]
#[serde(rename_all = "camelCase")]
pub struct RebuildReport {
    /// `"memory"` or `"redis"`.
    pub backend: String,
    /// Cells read.
    pub crawlers: u32,
    /// Kill events replayed.
    pub events: u32,
    /// Changes applied.
    pub applied: u32,
    /// Changes already applied (idempotent repeats).
    pub duplicates: u32,
    /// The top rows were the same before and after.
    pub identical_top: bool,
    /// Every principal's scores equal its cell.
    pub matches_cells: bool,
    /// Principals whose scores did not.
    pub mismatches: Vec<Mismatch>,
    /// Duration (ms).
    pub took_ms: f64,
}

/// The top-N boards pushed to clients (`board` event) and served over HTTP.
#[derive(Debug, Clone, PartialEq, Serialize, GameRecord)]
#[serde(rename_all = "camelCase")]
pub struct BoardSnapshot {
    /// Bumps whenever anything else changes.
    pub version: u64,
    /// `"memory"` or `"redis"`.
    pub backend: String,
    /// Principals on the kills board.
    pub members: u64,
    /// Top kills.
    pub kills: Vec<BoardRow>,
    /// Top XP.
    pub xp: Vec<BoardRow>,
    /// Background applier counters.
    pub background: BackgroundStats,
    /// Last rebuild, if any.
    pub last_rebuild: Option<RebuildReport>,
}

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
    snapshot: Mutex<Arc<BoardSnapshot>>,
    version: AtomicU64,
    dirty: AtomicBool,
    last_rebuild: Mutex<Option<RebuildReport>>,
    rebuilding: tokio::sync::Mutex<()>,
}

impl Leaderboards {
    /// Redis when `redis_url` is given, else in memory. Must run inside a
    /// Tokio runtime (spawns the background applier and the refresher).
    /// `on_change` is told every board whose scores changed (applied kills
    /// and rebuild promotions): the zone server publishes it on the
    /// projection bus as `distributed.leaderboard.changed`, which drives
    /// `@live` boards in `mobpit-service`.
    pub fn open(
        redis_url: Option<&str>,
        redis_prefix: &str,
        on_change: Option<LeaderboardChangeListener>,
    ) -> Result<Arc<Self>, String> {
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
        let mut projector: Projector = LeaderboardProjector::new(store.clone(), projection);
        if let Some(listener) = on_change {
            projector = projector.with_change_listener(listener);
        }
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
            snapshot: Mutex::new(Arc::new(BoardSnapshot {
                version: 0,
                backend: backend.into(),
                members: 0,
                kills: Vec::new(),
                xp: Vec::new(),
                background: BackgroundStats::default(),
                last_rebuild: None,
            })),
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

    /// Latest top-N snapshot (pushed as the zone's `board` event).
    pub fn snapshot(&self) -> Arc<BoardSnapshot> {
        self.snapshot.lock().expect("snapshot").clone()
    }

    /// Force a snapshot refresh on the next refresher pass.
    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    fn rows(&self, entries: Vec<LeaderboardEntry>) -> Vec<BoardRow> {
        entries
            .into_iter()
            .map(|e| BoardRow {
                rank: u32::try_from(e.rank + 1).unwrap_or(u32::MAX),
                name: self.name_of(&e.member),
                member: e.member,
                score: e.score,
            })
            .collect()
    }

    async fn top_rows(&self) -> Result<(Vec<BoardRow>, Vec<BoardRow>, u64), LeaderboardError> {
        let kills = self.store.top(&self.kills, TOP_N).await?;
        let xp = self.store.top(&self.xp, TOP_N).await?;
        let members = self.store.len(&self.kills).await?;
        Ok((self.rows(kills), self.rows(xp), members))
    }

    async fn refresh(&self) -> Result<(), LeaderboardError> {
        let (kills, xp, members) = self.top_rows().await?;
        let stats = self.background.stats();
        let mut body = BoardSnapshot {
            version: 0,
            backend: self.backend.into(),
            members,
            kills,
            xp,
            background: BackgroundStats {
                enqueued: stats.enqueued,
                applied: stats.applied,
                dropped: stats.dropped,
                failed: stats.failed,
            },
            last_rebuild: self.last_rebuild.lock().expect("rebuild").clone(),
        };
        let mut snapshot = self.snapshot.lock().expect("snapshot");
        let current = BoardSnapshot {
            version: 0,
            ..(**snapshot).clone()
        };
        if current != body {
            let version = self.version.load(Ordering::Acquire) + 1;
            body.version = version;
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

    /// Rebuild both boards from the durable event history of `principals`'
    /// cells and check the result against the cells' current state.
    pub async fn rebuild(
        &self,
        principals: Vec<String>,
        cells: &CrawlerCells,
    ) -> Result<RebuildReport, String> {
        let _one_at_a_time = self.rebuilding.lock().await;
        let started = Instant::now();
        let before = self.top_rows().await.map_err(|e| e.to_string())?;
        let mut occurrences = Vec::new();
        let mut expected: BTreeMap<String, (u32, u64)> = BTreeMap::new();
        for principal in &principals {
            let view = cells.crawler(principal).await?;
            for kill in view.kills {
                self.remember_name(&kill.crawler, &kill.name);
                occurrences.push(kill_occurrence(
                    principal,
                    kill.sequence,
                    kill.at_ms,
                    &MobKilled {
                        crawler: kill.crawler,
                        mob: kill.mob,
                        tier: kill.tier,
                        xp: kill.xp,
                    },
                )?);
            }
            if let Some(state) = view.state {
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
                mismatches.push(Mismatch {
                    member: principal.clone(),
                    cell: Scores {
                        kills: f64::from(*kills),
                        xp: *xp as f64,
                    },
                    board: Scores { kills: k, xp: x },
                });
            }
        }
        let count = |n: u64| u32::try_from(n).unwrap_or(u32::MAX);
        let result = RebuildReport {
            backend: self.backend.into(),
            crawlers: count(principals.len() as u64),
            events: count(occurrences.len() as u64),
            applied: count(report.applied),
            duplicates: count(report.duplicates),
            identical_top: before.0 == after.0 && before.1 == after.1,
            matches_cells: mismatches.is_empty(),
            mismatches,
            took_ms: started.elapsed().as_secs_f64() * 1000.0,
        };
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
