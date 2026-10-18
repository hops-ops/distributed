//! Process-wide Mob Pit state shared by the zones, the fact worker and the
//! dev routes: the durable cells, the sink's latency/counters and the
//! leaderboards.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::Router;
use distributed_game::net::{GameServer, ServerConfig};
use distributed_game::RetryPolicy;
use serde_json::{json, Value};

use super::board::Leaderboards;
use super::store::{CrawlerCells, CrawlerFactSink, SinkState};
use super::zone::{MobPit, MobPitConfig};
use super::{routes, MobPitDemo};
use crate::common::{random_hex, Demo, Guests};

/// Unsettled facts one zone may hold before `emit_fact` reports backpressure.
pub const FACT_CAPACITY: usize = 256;

/// How to open the world.
#[derive(Debug, Clone)]
pub struct WorldOptions {
    /// Directory of the per-player cell files.
    pub data_dir: PathBuf,
    /// `redis://…` for a Redis leaderboard; None = in memory.
    pub redis_url: Option<String>,
    /// Redis key prefix (tests isolate themselves with it).
    pub redis_prefix: String,
}

impl WorldOptions {
    /// Cells in `data_dir`, in-memory leaderboard.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            redis_url: None,
            redis_prefix: "mobpit".into(),
        }
    }

    /// A fresh directory under the system temp dir.
    pub fn temporary() -> Self {
        Self::new(std::env::temp_dir().join(format!("game-demos-mobpit-{}", random_hex(12))))
    }
}

/// A loaded profile: principal and `(xp, level, kills)` (None = no cell yet).
pub type LoadedProfile = (String, Option<(u64, u32, u32)>);

/// See the module docs.
pub struct MobPitWorld {
    boot: String,
    cells: Arc<CrawlerCells>,
    sink: Arc<SinkState>,
    board: Arc<Leaderboards>,
    zone_pending: AtomicUsize,
    profiles: Arc<Mutex<Vec<LoadedProfile>>>,
}

impl MobPitWorld {
    /// Open the cells directory and the leaderboard. Must be called inside a
    /// Tokio runtime. Does not rebuild the board: call [`Self::rebuild_board`].
    pub fn open(options: WorldOptions) -> Result<Arc<Self>, String> {
        std::fs::create_dir_all(&options.data_dir)
            .map_err(|e| format!("create {}: {e}", options.data_dir.display()))?;
        let board = Leaderboards::open(options.redis_url.as_deref(), &options.redis_prefix)?;
        Ok(Arc::new(Self {
            boot: random_hex(8),
            cells: Arc::new(CrawlerCells::new(options.data_dir)),
            sink: Arc::new(SinkState::default()),
            board,
            zone_pending: AtomicUsize::new(0),
            profiles: Arc::default(),
        }))
    }

    /// Random id of this server process (shown by clients to spot restarts;
    /// fact keys use `Ctx::fact_key` / the boot epoch).
    pub fn boot(&self) -> &str {
        &self.boot
    }

    /// Cell files directory.
    pub fn data_dir(&self) -> &Path {
        self.cells.root()
    }

    /// The cells.
    pub fn cells(&self) -> &Arc<CrawlerCells> {
        &self.cells
    }

    /// Sink latency and counters.
    pub fn sink_state(&self) -> &Arc<SinkState> {
        &self.sink
    }

    /// Leaderboards.
    pub fn board(&self) -> &Arc<Leaderboards> {
        &self.board
    }

    /// The game's fact sink.
    pub fn fact_sink(&self) -> CrawlerFactSink {
        CrawlerFactSink::new(self.cells.clone(), self.sink.clone(), self.board.clone())
    }

    pub(crate) fn set_zone_pending(&self, n: usize) {
        self.zone_pending.store(n, Ordering::Relaxed);
    }

    /// Facts emitted by the zone and not yet settled (incl. backpressure backlog).
    pub fn zone_pending(&self) -> usize {
        self.zone_pending.load(Ordering::Relaxed)
    }

    /// Load `principal`'s cell on a background task; the zone picks the
    /// result up with [`Self::take_loaded_profiles`] on a later tick.
    pub fn request_profile(&self, principal: &str) {
        let cells = self.cells.clone();
        let profiles = self.profiles.clone();
        let principal = principal.to_string();
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        handle.spawn(async move {
            // A first-time player has no cell yet; do not create one to read.
            let state = match cells.exists(&principal).then(|| cells.get(&principal)) {
                Some(Ok(cell)) => cell.load().await.ok().flatten(),
                _ => None,
            };
            profiles
                .lock()
                .expect("profiles")
                .push((principal, state.map(|c| (c.xp, c.level, c.kills))));
        });
    }

    pub(crate) fn take_loaded_profiles(&self) -> Vec<LoadedProfile> {
        std::mem::take(&mut *self.profiles.lock().expect("profiles"))
    }

    /// Rebuild the leaderboards from the cells' event history.
    pub async fn rebuild_board(&self) -> Result<Value, String> {
        self.board.rebuild(&self.cells).await
    }

    /// Every crawler on disk: `{principal, name, xp, kills, level, version}`.
    pub async fn crawlers(&self) -> Result<Vec<Value>, String> {
        let mut out = Vec::new();
        for principal in self.cells.principals() {
            let cell = self.cells.get(&principal).map_err(|e| e.to_string())?;
            if let Some(c) = cell.load().await.map_err(|e| e.to_string())? {
                out.push(json!({
                    "principal": principal, "name": c.name, "xp": c.xp,
                    "kills": c.kills, "level": c.level, "version": c.version(),
                }));
            }
        }
        Ok(out)
    }

    /// Game server for `config` plus the Mob Pit dev routes.
    pub fn server(
        self: &Arc<Self>,
        config: ServerConfig,
        guests: Guests,
    ) -> (GameServer<MobPit>, Router) {
        let world = self.clone();
        let server = GameServer::new(config, move |zone_id| {
            MobPitDemo::hosts_zone(zone_id).then(|| MobPitConfig {
                guests: guests.clone(),
                world: world.clone(),
            })
        })
        .with_fact_sink(self.fact_sink(), FACT_CAPACITY, RetryPolicy::default());
        (server, routes::routes(self.clone()))
    }
}
