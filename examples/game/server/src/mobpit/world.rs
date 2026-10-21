//! Mob Pit's services next to the rooms: the celld cells reader, the
//! projection bus the outbox Queue relays onto, the crawler directory and
//! the rooms' zone status projected from it, and the leaderboards fed by
//! the directory. The rooms themselves run on celld (`mobpit_rooms`); this
//! process never hosts one.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use serde_json::{json, Value};

use super::board::Leaderboards;
use super::directory::CrawlerDirectory;
use super::store::{CelldOptions, CrawlerCells};
use crate::common::random_hex;
use crate::common::relay::{ProjectionBus, ProjectionPublisher};
use crate::common::zone_status::ZoneStatusView;
use distributed::leaderboard::LeaderboardChangePublisher;

/// Bus consumer group of the rooms' zone status projection here.
pub const ZONE_STATUS_CONSUMER: &str = "mobpit-rooms";

/// How to open the world.
#[derive(Clone)]
pub struct WorldOptions {
    /// Directory of local dev files (the persisted ticket secret, the
    /// default projection bus). Durable game state is in celld.
    pub data_dir: PathBuf,
    /// celld serving `examples/game/cells` (the rooms and their cells).
    pub celld: CelldOptions,
    /// `redis://…` for a Redis leaderboard; None = in memory.
    pub redis_url: Option<String>,
    /// Redis key prefix (tests isolate themselves with it).
    pub redis_prefix: String,
    /// SQLite bus the relayed domain events are published onto (shared
    /// with `mobpit-service`). Default: `sqlite:{data_dir}/bus.db`.
    pub projection_bus: Option<String>,
    /// SQLite database of this server's read models (the crawler
    /// directory, the rooms' status). Default:
    /// `sqlite:{data_dir}/read-models.db`.
    pub read_models: Option<String>,
}

impl WorldOptions {
    /// Cells on `celld`, local files in `data_dir`, in-memory leaderboard.
    pub fn new(data_dir: impl Into<PathBuf>, celld: CelldOptions) -> Self {
        Self {
            data_dir: data_dir.into(),
            celld,
            redis_url: None,
            redis_prefix: mobpit_crawler::BOARD_REDIS_PREFIX.into(),
            projection_bus: None,
            read_models: None,
        }
    }

    fn bus_url(&self) -> String {
        self.projection_bus
            .clone()
            .unwrap_or_else(|| format!("sqlite:{}", self.data_dir.join("bus.db").display()))
    }

    fn read_models_url(&self) -> String {
        self.read_models.clone().unwrap_or_else(|| {
            format!(
                "sqlite:{}?mode=rwc",
                self.data_dir.join("read-models.db").display()
            )
        })
    }

    /// A fresh local directory under the system temp dir.
    pub fn temporary(celld: CelldOptions) -> Self {
        Self::new(
            std::env::temp_dir().join(format!("game-demos-mobpit-{}", random_hex(12))),
            celld,
        )
    }
}

/// See the module docs.
pub struct MobPitWorld {
    boot: String,
    data_dir: PathBuf,
    celld: CelldOptions,
    cells: Arc<CrawlerCells>,
    board: Arc<Leaderboards>,
    projection: Arc<OnceLock<ProjectionBus>>,
    directory: OnceLock<CrawlerDirectory>,
    rooms: OnceLock<ZoneStatusView>,
    bus_url: String,
    read_models_url: String,
}

impl MobPitWorld {
    /// Open the world over celld and attach its projection bus, crawler
    /// directory and room status ([`Self::attach_projection`]). Does not
    /// rebuild the board: call [`Self::rebuild_board`].
    pub async fn start(options: WorldOptions) -> Result<Arc<Self>, String> {
        let world = Self::open(options)?;
        world.attach_projection().await?;
        Ok(world)
    }

    /// Open the world over celld without its projection (the relay answers
    /// 503 until [`Self::attach_projection`]). Must be called inside a
    /// Tokio runtime. Does not contact celld.
    pub fn open(options: WorldOptions) -> Result<Arc<Self>, String> {
        std::fs::create_dir_all(&options.data_dir)
            .map_err(|e| format!("create {}: {e}", options.data_dir.display()))?;
        // Board changes go out on the projection bus (once attached) so the
        // GraphQL service's `@live` boards follow them.
        let projection: Arc<OnceLock<ProjectionBus>> = Arc::default();
        let notices = LeaderboardChangePublisher::spawn(ProjectionPublisher(projection.clone()));
        let board = Leaderboards::open(
            options.redis_url.as_deref(),
            &options.redis_prefix,
            Some(notices.listener()),
        )?;
        Ok(Arc::new(Self {
            bus_url: options.bus_url(),
            read_models_url: options.read_models_url(),
            directory: OnceLock::new(),
            rooms: OnceLock::new(),
            boot: random_hex(8),
            data_dir: options.data_dir,
            cells: Arc::new(CrawlerCells::new(&options.celld)?),
            celld: options.celld,
            board,
            projection,
        }))
    }

    /// Random id of this server process.
    pub fn boot(&self) -> &str {
        &self.boot
    }

    /// Local dev files directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Where the rooms and cells are.
    pub fn celld(&self) -> &CelldOptions {
        &self.celld
    }

    /// Read side of the cells.
    pub fn cells(&self) -> &Arc<CrawlerCells> {
        &self.cells
    }

    /// Leaderboards.
    pub fn board(&self) -> &Arc<Leaderboards> {
        &self.board
    }

    /// Open the projection bus the outbox Queue relay publishes onto
    /// (`/internal/celld-queue/relay`; 503 until this runs, so the Queue
    /// keeps the messages), project it into the crawler directory and the
    /// rooms' status, and feed the board from the directory. Call once.
    pub async fn attach_projection(self: &Arc<Self>) -> Result<(), String> {
        let bus = ProjectionBus::open(&self.bus_url).await?;
        let directory = CrawlerDirectory::open(&self.read_models_url, bus.pool().clone()).await?;
        let rooms = ZoneStatusView::open(
            &self.read_models_url,
            bus.pool().clone(),
            ZONE_STATUS_CONSUMER,
        )
        .await?;
        self.directory
            .set(directory)
            .map_err(|_| "projection already attached".to_string())?;
        self.rooms
            .set(rooms)
            .map_err(|_| "projection already attached".to_string())?;
        self.projection
            .set(bus)
            .map_err(|_| "projection already attached".to_string())?;
        tokio::spawn(feed_board(Arc::downgrade(self)));
        Ok(())
    }

    /// The attached projection bus, if any.
    pub fn projection_bus(&self) -> Option<&ProjectionBus> {
        self.projection.get()
    }

    /// The slot the relay route publishes through (empty until
    /// [`Self::attach_projection`]).
    pub fn projection(&self) -> Arc<OnceLock<ProjectionBus>> {
        self.projection.clone()
    }

    /// The bus URL (`--projection-bus`, default in the data dir).
    pub fn bus_url(&self) -> &str {
        &self.bus_url
    }

    fn directory(&self) -> Result<&CrawlerDirectory, String> {
        self.directory
            .get()
            .ok_or_else(|| "no projection attached: the crawler list is unavailable".into())
    }

    /// Rebuild the leaderboards from the event history of every crawler in
    /// the directory (read from each one's cell).
    pub async fn rebuild_board(&self) -> Result<super::board::RebuildReport, String> {
        let principals = self
            .directory()?
            .list()
            .await?
            .into_iter()
            .map(|row| row.player_id)
            .collect();
        self.board.rebuild(principals, &self.cells).await
    }

    /// Every crawler in the directory: `{principal, name, xp, kills, level}`
    /// (eventually consistent with the cells).
    pub async fn crawlers(&self) -> Result<Vec<Value>, String> {
        Ok(self
            .directory()?
            .list()
            .await?
            .into_iter()
            .map(|c| {
                json!({
                    "principal": c.player_id, "name": c.name, "xp": c.xp,
                    "kills": c.kills, "level": c.level,
                })
            })
            .collect())
    }

    /// Every room the read model knows (`zone_activities` rows of
    /// `pit.*`): its status (`active` / `dormant` / `hibernated`), players,
    /// whether the activation restored a snapshot, and — a debug detail —
    /// the celld node running it.
    pub async fn rooms(&self) -> Result<Vec<Value>, String> {
        let view = self
            .rooms
            .get()
            .ok_or("no projection attached: room status is unavailable")?;
        Ok(view
            .activities("pit.")
            .await?
            .into_iter()
            .filter_map(|row| {
                let (x, y) = mobpit_rooms::parse_room(&row.zone_id)?;
                Some(json!({
                    "zone": row.zone_id, "x": x, "y": y, "status": row.status,
                    "players": row.players, "restored": row.restored,
                    "activations": row.activations, "node": row.node,
                    "changedAtMs": row.changed_at_ms,
                }))
            })
            .collect())
    }
}

/// Keep the boards in step with the crawler directory: every change of a
/// crawler's totals becomes one idempotent `Max` update.
async fn feed_board(world: std::sync::Weak<MobPitWorld>) {
    let mut seen: HashMap<String, (i64, i64)> = HashMap::new();
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let Some(world) = world.upgrade() else {
            return;
        };
        let Ok(directory) = world.directory() else {
            continue;
        };
        let Ok(rows) = directory.list().await else {
            continue;
        };
        for row in rows {
            let totals = (row.kills, row.xp);
            if seen.get(&row.player_id) == Some(&totals) {
                continue;
            }
            world.board.submit_totals(
                &row.player_id,
                &row.name,
                u32::try_from(row.kills).unwrap_or(0),
                u64::try_from(row.xp).unwrap_or(0),
            );
            seen.insert(row.player_id, totals);
        }
    }
}
