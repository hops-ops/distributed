//! Mob Pit's business-side read model (dev demo).
//!
//! ```text
//! zone ──fact──▶ Crawler cell on celld (RecordKill: XP decided there; event
//!                + outbox row in one cell SQLite transaction)
//!        └─ celld drains the outbox ──▶ `game-demo-outbox` Queue ──▶ consumer
//!           relays to the zone server's /internal/celld-queue/relay
//!           ──▶ SQLite bus (`bus_log`)
//!                                                   │ ordered, at-least-once
//!   mobpit-service: modeled projector `project_player_stats` ◀──┘
//!        └─ upsert `player_stats` (checkpointed, deduplicated) ──▶ GraphQL
//!           `@live` (in-process change stream) ──▶ SvelteKit `$distributed`
//! ```
//!
//! - [`PlayerStats`] is the table: one row per principal, the Crawler's
//!   post-kill state ([`mobpit_crawler::CrawlerState`]). The leaderboard is a
//!   query over it (`order_by: xp desc`).
//! - [`ZoneActivities`] (`zone_activities`): every room's lifecycle as its
//!   zone-activity cell published it (`distributed_game::world::zone_status`):
//!   active / dormant / hibernated, players, restored, and — a debug
//!   detail — the celld node. The pages' minimap is a `@live` query over it.
//! - [`PLAYER_STATS`] maps `CrawlerMobKilledDomainEvent` to the
//!   [`SavePlayerStats`] upsert. The projector runtime keeps the cursor,
//!   skips redelivered messages and commits each projection atomically.
//! - The service is **read-only**: no commands, so no caller can name a
//!   player, an XP amount or a reward here. XP is decided by the Crawler
//!   cell's `RecordKill` handler from the mob tier; the principal comes from
//!   the zone ticket the SvelteKit server minted from the verified session.
//! - GraphQL identity is a verified Bearer JWT only (`OidcBearer`): the
//!   SvelteKit server's ES256 token in dev, or the OIDC provider's access
//!   token. There is no header-trust mode.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use distributed::bus::{BusConsumer, RunOptions, SqliteBus};
use distributed::graphql::surface::{LeaderboardGrant, LeaderboardSurface};
use distributed::graphql::{
    build_surface, read, surface_for_application_contract, DistributedClientSurfaceExport,
    GraphqlEngine, IdentityConfig, ModelPermissions, OidcConfig, RoleGrant, Surface,
    SurfaceOptions,
};
use distributed::leaderboard::{Board, LeaderboardStore, RedisLeaderboard};
use distributed::microsvc::{
    spawn_service_consumer_loop, CausalProjectorContext, HandlerError, ModeledProjection, Routes,
    Service, CONSUMER_IDLE_POLL,
};
use distributed::{
    Application, ApplicationManifest, RelationalReadModel, SqliteRepository, SurfaceSpec,
};

/// The rooms' status rows and their projection.
pub use distributed_game::world::zone_status::{ZoneActivities, ZONE_ACTIVITIES};
/// The read model and its projection (shared with the zone server).
pub use mobpit_crawler::stats::{
    projector, PlayerStats, SavePlayerStats, APPLICATION, EPOCH, PLAYER_STATS, PROJECTOR,
};

/// The boroughs (bosses down, weather, the world event running) and their
/// projection, from the borough cells' domain events.
pub use mobpit_tiers::events::{Boroughs, BOROUGHS};

/// The tables: `player_stats`, `zone_activities` and `boroughs`.
pub fn read_model_catalog() -> distributed::ReadModelCatalog {
    mobpit_crawler::stats::read_model_catalog()
        .table_schema(ZoneActivities::schema().clone())
        .table_schema(Boroughs::schema().clone())
}

/// The projector of `boroughs`, owned by this application.
pub fn boroughs_projector() -> distributed::graphql::SurfaceProjector {
    mobpit_tiers::events::projector(APPLICATION)
}

/// The boroughs (and their world events) are public to signed-in players.
pub fn borough_permissions() -> ModelPermissions<Boroughs> {
    ModelPermissions::new().grant(ROLE, read().all_columns())
}

async fn project_boroughs(
    context: CausalProjectorContext,
    projection: ModeledProjection,
) -> Result<(), HandlerError> {
    projection.apply(BOROUGHS, &context).await
}

/// The projector of `zone_activities`, owned by this application.
pub fn rooms_projector() -> distributed::graphql::SurfaceProjector {
    distributed_game::world::zone_status::activity_projector_for(APPLICATION)
}
/// The one engine role. Every signed-in player reads the whole board.
pub const ROLE: &str = "user";
/// Bus consumer group of this process's board change notices (every
/// GraphQL process subscribes under its own group).
pub const BOARD_NOTICE_GROUP: &str = "mobpit-service-boards";

/// The Redis boards the zone server projects kills into, as GraphQL roots:
/// `xpLeaderboard(first)`, `xpLeaderboardAround(radius)`, `xpMyRank` and
/// the `kills…` twins, all `@live`. `…MyRank` and `…Around` rank the viewer
/// (the session's user id); no root ranks an arbitrary member.
pub fn leaderboard_surfaces() -> Vec<LeaderboardSurface> {
    [
        ("xp", mobpit_crawler::XP_BOARD),
        ("kills", mobpit_crawler::KILLS_BOARD),
    ]
    .into_iter()
    .map(|(name, board)| {
        LeaderboardSurface::single(name, &Board::high_first(board).expect("board name"))
            .limits(5, 20, 5)
            .grant(ROLE, LeaderboardGrant::read())
    })
    .collect()
}

/// Read authorization: the leaderboard is public to signed-in players;
/// there is nothing to write (no commands, no mutations).
pub fn permissions() -> ModelPermissions<PlayerStats> {
    ModelPermissions::new().grant(ROLE, read().all_columns())
}

/// The rooms' status is public to signed-in players too.
pub fn room_permissions() -> ModelPermissions<ZoneActivities> {
    ModelPermissions::new().grant(ROLE, read().all_columns())
}

async fn project(
    context: CausalProjectorContext,
    projection: ModeledProjection,
) -> Result<(), HandlerError> {
    projection.apply(PLAYER_STATS, &context).await
}

async fn project_rooms(
    context: CausalProjectorContext,
    projection: ModeledProjection,
) -> Result<(), HandlerError> {
    projection.apply(ZONE_ACTIVITIES, &context).await
}

/// The consumer service (bus consumer identity = [`APPLICATION`]): the
/// modeled projector over `read_models`.
pub fn projection_service(read_models: SqliteRepository) -> Service {
    Service::new().named(APPLICATION).routes(
        Routes::new()
            .with_read_model_store(read_models)
            .modeled_projector(projector())
            .handle(project)
            .modeled_projector(rooms_projector())
            .handle(project_rooms)
            .modeled_projector(boroughs_projector())
            .handle(project_boroughs),
    )
}

/// Run the projector against the bus forever (restarted on errors; the
/// cursor lives in the read-model database, so a restart resumes).
pub fn spawn_projector(read_models: SqliteRepository, bus: sqlx::SqlitePool) {
    spawn_service_consumer_loop(move || {
        projection_service(read_models.clone())
            .with_bus(SqliteBus::new(bus.clone()).with_idle_poll(CONSUMER_IDLE_POLL))
    });
}

fn grants() -> BTreeMap<String, BTreeMap<String, RoleGrant>> {
    let mut grants: BTreeMap<String, BTreeMap<String, RoleGrant>> = BTreeMap::new();
    for (role, grant) in permissions().surface_grants() {
        grants
            .entry(role)
            .or_default()
            .insert(PlayerStats::schema().model_name.clone(), grant);
    }
    for (role, grant) in room_permissions().surface_grants() {
        grants
            .entry(role)
            .or_default()
            .insert(ZoneActivities::schema().model_name.clone(), grant);
    }
    for (role, grant) in borough_permissions().surface_grants() {
        grants
            .entry(role)
            .or_default()
            .insert(Boroughs::schema().model_name.clone(), grant);
    }
    grants
}

fn full_surface() -> Surface {
    build_surface(&read_model_catalog().tables, &SurfaceOptions::sqlite())
        .expect("mobpit Surface should build")
        .with_leaderboards(leaderboard_surfaces())
        .expect("mobpit boards should attach")
        .with_projection_owners([
            projector().into(),
            rooms_projector().into(),
            boroughs_projector().into(),
        ])
        .expect("mobpit projectors should bind")
}

/// Logical application artifact (`distributed describe`, the lifecycle).
pub fn application_manifest() -> ApplicationManifest {
    let surface = SurfaceSpec::from_surface(APPLICATION, &full_surface())
        .expect("mobpit Surface should compile into an application artifact");
    Application::new(APPLICATION)
        .surface(surface)
        .build()
        .expect("mobpit application manifest should compile")
        .manifest()
        .clone()
}

/// Pool-free client export consumed by `distributed client-manifest`.
pub fn distributed_client_surface() -> DistributedClientSurfaceExport {
    let roles = [ROLE.to_string()];
    let selected =
        surface_for_application_contract(&full_surface(), APPLICATION, &roles, &roles, &grants())
            .expect("mobpit application Surface should select");
    DistributedClientSurfaceExport::from_selected(APPLICATION, selected)
        .expect("mobpit application Surface should export")
}

/// GraphQL identity: a verified Bearer JWT, nothing else.
#[derive(Debug, Clone)]
pub struct BearerIdentity {
    /// Expected `iss`.
    pub issuer: String,
    /// Expected `aud`.
    pub audience: String,
    /// Static JWKS JSON (dev: the SvelteKit server's public key); None =
    /// discover from the issuer.
    pub jwks: Option<String>,
}

impl BearerIdentity {
    fn config(&self) -> IdentityConfig {
        let mut oidc = OidcConfig::new(&self.issuer, &self.audience).engine_roles(&[ROLE]);
        if let Some(jwks) = &self.jwks {
            oidc = oidc.with_static_jwks(jwks.clone());
        }
        IdentityConfig::oidc_bearer(oidc)
    }
}

/// The GraphQL engine over `repo` (`player_stats`, `@live` from its change
/// stream) and `boards` (the Redis leaderboards, `@live` from the zone
/// server's change notices: [`spawn_board_notices`]).
pub fn build_engine(
    repo: &SqliteRepository,
    boards: Arc<dyn LeaderboardStore>,
    identity: &BearerIdentity,
    protocol_key: [u8; 32],
) -> Result<GraphqlEngine, String> {
    let mut builder = GraphqlEngine::builder(repo.pool().clone());
    for surface in leaderboard_surfaces() {
        builder = builder.leaderboard(surface, boards.clone());
    }
    builder
        .service_id(APPLICATION)
        .protocol_token_key(protocol_key)
        .roles(&[ROLE])
        .client_application_surface(APPLICATION, [ROLE], [ROLE])
        .model::<PlayerStats>(permissions())
        .model::<ZoneActivities>(room_permissions())
        .model::<Boroughs>(borough_permissions())
        .client_projection_owners([
            projector().into(),
            rooms_projector().into(),
            boroughs_projector().into(),
        ])
        .identity(identity.config())
        .graphiql(false)
        .change_stream(repo.read_model_changes())
        .build()
        .map_err(|e| e.to_string())
}

/// Open (creating if needed) a SQLite pool another process may share: WAL
/// and a busy timeout, so the zone server's appends and this service's reads
/// of the bus wait for each other instead of failing.
pub async fn open_shared_sqlite(url: &str) -> Result<sqlx::SqlitePool, sqlx::Error> {
    use std::str::FromStr;
    let options = sqlx::sqlite::SqliteConnectOptions::from_str(url)?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(10));
    sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
}

/// Follow the zone server's `distributed.leaderboard.changed` notices on
/// the bus and refresh `engine`'s `@live` boards (restarted on errors).
pub fn spawn_board_notices(engine: Arc<GraphqlEngine>, bus: sqlx::SqlitePool) {
    tokio::spawn(async move {
        loop {
            let handlers = Arc::new(engine.leaderboard_change_handlers(BOARD_NOTICE_GROUP));
            let bus = SqliteBus::new(bus.clone()).with_idle_poll(CONSUMER_IDLE_POLL);
            if let Err(error) = bus.subscribe(handlers, RunOptions::default()).await {
                eprintln!("mobpit-service: board notices: {error}");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

/// The Redis boards at `url` under `prefix` (the zone server's).
pub fn open_boards(url: &str, prefix: &str) -> Result<Arc<dyn LeaderboardStore>, String> {
    Ok(Arc::new(
        RedisLeaderboard::connect(url)
            .map_err(|error| format!("REDIS_URL {url}: {error}"))?
            .with_prefix(prefix),
    ))
}

/// Runtime configuration.
#[derive(Debug, Clone)]
pub struct ServiceOptions {
    /// `sqlite:` URL of the read-model database.
    pub database_url: String,
    /// `sqlite:` URL of the bus the zone server publishes to.
    pub bus_url: String,
    /// `redis://…` of the leaderboards the zone server writes.
    pub redis_url: String,
    /// Their key prefix (the zone server's `--redis-prefix`).
    pub redis_prefix: String,
    /// Listen address.
    pub bind: String,
    /// Bearer verification.
    pub identity: BearerIdentity,
    /// Generated clients' protocol-token key (32 bytes, non-zero).
    pub protocol_key: [u8; 32],
    /// Browser origins allowed to call `/graphql` cross-origin (the UI).
    pub allowed_origins: Vec<String>,
}

/// Open the read model, start the projector, serve `/graphql` + `/graphql/ws`.
pub async fn run(options: ServiceOptions) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let repo = SqliteRepository::connect_and_migrate(&options.database_url).await?;
    repo.bootstrap_table_schema_for_dev(&read_model_catalog().table_registry()?)
        .await?;
    let bus_pool = open_shared_sqlite(&options.bus_url).await?;
    SqliteBus::new(bus_pool.clone()).ensure_tables().await?;

    let boards = open_boards(&options.redis_url, &options.redis_prefix)?;
    let engine = Arc::new(build_engine(
        &repo,
        boards,
        &options.identity,
        options.protocol_key,
    )?);
    spawn_projector(repo.clone(), bus_pool.clone());
    spawn_board_notices(engine.clone(), bus_pool);
    let app = http::router(engine, &options.allowed_origins);
    let listener = tokio::net::TcpListener::bind(&options.bind).await?;
    eprintln!(
        "mobpit-service: player_stats @live on http://{}/graphql",
        listener.local_addr()?
    );
    println!("LISTENING http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

pub mod http;
