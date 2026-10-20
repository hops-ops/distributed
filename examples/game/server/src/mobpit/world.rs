//! Process-wide Mob Pit state shared by the zones, the fact worker and the
//! dev routes: the celld cells (fact sink and reader), the sink's
//! latency/counters, the leaderboards and the projection bus.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed_game::net::{GameServer, ServerConfig, ZoneFactory};
use distributed_game::RetryPolicy;
use serde_json::{json, Value};

use super::board::Leaderboards;
use super::presence::Presence;
use super::store::{CelldOptions, CrawlerCells, MobPitFactSink, ProjectionBus, SinkState};
use super::zone::{MobPit, MobPitConfig};
use super::{routes, MobPitDemo};
use crate::common::{random_hex, Demo, DemoRoutes, Guests};

/// Unsettled facts one zone may hold before `emit_fact` reports backpressure.
pub const FACT_CAPACITY: usize = 256;

/// How to open the world.
#[derive(Clone)]
pub struct WorldOptions {
    /// Directory of local dev files (the persisted ticket secret, the
    /// default projection bus). Durable game state is in celld.
    pub data_dir: PathBuf,
    /// celld serving `examples/game/cells`.
    pub celld: CelldOptions,
    /// `redis://…` for a Redis leaderboard; None = in memory.
    pub redis_url: Option<String>,
    /// Redis key prefix (tests isolate themselves with it).
    pub redis_prefix: String,
}

impl WorldOptions {
    /// Cells on `celld`, local files in `data_dir`, in-memory leaderboard.
    pub fn new(data_dir: impl Into<PathBuf>, celld: CelldOptions) -> Self {
        Self {
            data_dir: data_dir.into(),
            celld,
            redis_url: None,
            redis_prefix: "mobpit".into(),
        }
    }

    /// A fresh local directory under the system temp dir.
    pub fn temporary(celld: CelldOptions) -> Self {
        Self::new(
            std::env::temp_dir().join(format!("game-demos-mobpit-{}", random_hex(12))),
            celld,
        )
    }
}

/// A loaded profile: principal and `(xp, level, kills)` (None = no cell yet).
pub type LoadedProfile = (String, Option<(u64, u32, u32)>);

/// See the module docs.
pub struct MobPitWorld {
    boot: String,
    data_dir: PathBuf,
    celld: CelldOptions,
    cells: Arc<CrawlerCells>,
    sink: Arc<SinkState>,
    fact_sink: MobPitFactSink,
    board: Arc<Leaderboards>,
    zone_pending: AtomicUsize,
    profiles: Arc<Mutex<Vec<LoadedProfile>>>,
    projection: Arc<std::sync::OnceLock<ProjectionBus>>,
    presence: Arc<Presence>,
}

impl MobPitWorld {
    /// Open the world over celld. Must be called inside a Tokio runtime.
    /// Does not contact celld (cells are reached on first use) and does not
    /// rebuild the board: call [`Self::rebuild_board`].
    pub fn open(options: WorldOptions) -> Result<Arc<Self>, String> {
        std::fs::create_dir_all(&options.data_dir)
            .map_err(|e| format!("create {}: {e}", options.data_dir.display()))?;
        let board = Leaderboards::open(options.redis_url.as_deref(), &options.redis_prefix)?;
        let sink = Arc::new(SinkState::default());
        let presence = Arc::new(Presence::default());
        let fact_sink = MobPitFactSink::new(
            &options.celld,
            sink.clone(),
            board.clone(),
            presence.clone(),
        )?;
        Ok(Arc::new(Self {
            boot: random_hex(8),
            data_dir: options.data_dir,
            cells: Arc::new(CrawlerCells::new(&options.celld)?),
            celld: options.celld,
            sink,
            fact_sink,
            board,
            zone_pending: AtomicUsize::new(0),
            profiles: Arc::default(),
            projection: Arc::default(),
            presence,
        }))
    }

    /// Random id of this server process (shown by clients to spot restarts;
    /// fact keys use `Ctx::fact_key` / the boot epoch).
    pub fn boot(&self) -> &str {
        &self.boot
    }

    /// Local dev files directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Where the cells are.
    pub fn celld(&self) -> &CelldOptions {
        &self.celld
    }

    /// Read side of the cells.
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

    /// Presence mirrored from the lifecycle facts settled in presence cells.
    pub fn presence(&self) -> &Arc<Presence> {
        &self.presence
    }

    /// The game's fact sink.
    pub fn fact_sink(&self) -> MobPitFactSink {
        self.fact_sink.clone()
    }

    /// Publish the Crawler domain events that celld's outbox Queue relays
    /// to `/internal/celld-queue/relay` onto the SQLite bus at `url` (the
    /// read-model service consumes it). Until a bus is attached the relay
    /// answers 503 and the Queue keeps the messages. Call once.
    pub async fn attach_projection_bus(&self, url: &str) -> Result<(), String> {
        let bus = ProjectionBus::open(url).await?;
        self.projection
            .set(bus)
            .map_err(|_| "projection bus already attached".to_string())
    }

    /// The attached projection bus, if any.
    pub fn projection_bus(&self) -> Option<&ProjectionBus> {
        self.projection.get()
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
            let state = match cells.crawler(&principal).await {
                Ok(view) => view.state,
                Err(error) => {
                    eprintln!("mobpit: load profile {principal}: {error}");
                    None
                }
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
    pub async fn rebuild_board(&self) -> Result<super::board::RebuildReport, String> {
        self.board.rebuild(&self.cells).await
    }

    /// Every crawler on the roster: `{principal, name, xp, kills, level, version}`.
    pub async fn crawlers(&self) -> Result<Vec<Value>, String> {
        let mut out = Vec::new();
        for principal in self.cells.principals().await? {
            if let Some(c) = self.cells.crawler(&principal).await?.state {
                out.push(json!({
                    "principal": principal, "name": c.name, "xp": c.xp,
                    "kills": c.kills, "level": c.level, "version": c.version,
                }));
            }
        }
        Ok(out)
    }

    /// Game server for `config` plus the Mob Pit dev and internal routes.
    pub fn server(
        self: &Arc<Self>,
        config: ServerConfig,
        guests: Guests,
    ) -> (GameServer<MobPit>, DemoRoutes) {
        let world = self.clone();
        let mut config = config;
        // Joined / connection lost / reconnected / disconnected / left facts
        // for the presence cells.
        config.host.lifecycle_facts = true;
        let server = GameServer::new(
            config,
            ZoneFactory::sync(move |activation| {
                let zone_id = activation.zone_id.as_str();
                MobPitDemo::hosts_zone(zone_id).then(|| MobPitConfig {
                    guests: guests.clone(),
                    world: world.clone(),
                })
            }),
        )
        // celld down or slow: facts stay in the zone's queue until they
        // settle (backpressure at capacity), never in a local store.
        .with_fact_sink(
            self.fact_sink(),
            FACT_CAPACITY,
            RetryPolicy::until_settled(Duration::from_millis(50), Duration::from_secs(2)),
        );
        (
            server,
            DemoRoutes {
                dev: routes::routes(self.clone()),
                internal: routes::internal_routes(self.clone()),
            },
        )
    }
}
