//! Mob Pit's business-side read model (dev demo).
//!
//! ```text
//! zone ──fact──▶ Crawler cell (RecordKill: XP decided there; event + outbox
//!                row in one SQLite transaction)
//!        └─ zone server drains the cell outbox ──▶ SQLite bus (`bus_log`)
//!                                                   │ ordered, at-least-once
//!   mobpit-service: modeled projector `project_player_stats` ◀──┘
//!        └─ upsert `player_stats` (checkpointed, deduplicated) ──▶ GraphQL
//!           `@live` (in-process change stream) ──▶ SvelteKit `$distributed`
//! ```
//!
//! - [`PlayerStats`] is the table: one row per principal, the Crawler's
//!   post-kill state ([`mobpit_crawler::CrawlerState`]). The leaderboard is a
//!   query over it (`order_by: xp desc`).
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

use distributed::bus::SqliteBus;
use distributed::graphql::{
    build_surface, read, surface_for_application_contract, DistributedClientSurfaceExport,
    GraphqlEngine, IdentityConfig, ModelPermissions, OidcConfig, RoleGrant, Surface,
    SurfaceOptions, SurfaceProjector,
};
use distributed::microsvc::{
    spawn_service_consumer_loop, CausalProjectorContext, HandlerError, ModeledProjection, Routes,
    Service, CONSUMER_IDLE_POLL,
};
use distributed::projection::lower::{EventualOnly, ProjectionDescriptor};
use distributed::{
    mutation_file, Application, ApplicationManifest, LocalProjectionMountsBuilder, Mutation,
    ReadModel, ReadModelCatalog, RelationalReadModel, SqliteRepository, SurfaceSpec,
};
use mobpit_crawler::CrawlerMobKilledDomainEvent;
use serde::{Deserialize, Serialize};

/// Application, GraphQL service id and client surface name.
pub const APPLICATION: &str = "mobpit";
/// Projection name.
pub const PROJECTOR: &str = "project_player_stats";
/// Projection epoch: bump to rebuild the table from the bus log.
pub const EPOCH: &str = "mobpit-player-stats-v1";
/// The one engine role. Every signed-in player reads the whole board.
pub const ROLE: &str = "user";

/// One player's settled progress (a projection of their Crawler cell).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ReadModel)]
#[table("player_stats")]
pub struct PlayerStats {
    /// Principal (the verified session's user id).
    #[id("player_id")]
    pub player_id: String,
    /// Display name at the last kill.
    pub name: String,
    /// Total XP (decided by the Crawler cell, never by a caller).
    pub xp: i64,
    /// Mobs killed.
    pub kills: i64,
    /// Level for `xp`.
    pub level: i64,
}

impl PlayerStats {
    /// Read authorization: the leaderboard is public to signed-in players;
    /// there is nothing to write (no commands, no mutations).
    pub fn permissions() -> ModelPermissions<Self> {
        ModelPermissions::new().grant(ROLE, read().all_columns())
    }
}

/// Upsert one `player_stats` row from the domain event's state body.
#[allow(non_snake_case)]
pub fn SavePlayerStats() -> Mutation<()> {
    mutation_file!("src/mutations/save_player_stats.mutation.graphql")
}

distributed::projection! {
    pub const PLAYER_STATS: ProjectionDescriptor<EventualOnly> = {
        name: "project_player_stats",
        version: 1,
        epoch: "mobpit-player-stats-v1",
        model: PlayerStats,
        on {
            events: [CrawlerMobKilledDomainEvent],
            mutation: SavePlayerStats,
            input: { player: body },
        },
    };
}

/// Physical read-model catalog (`player_stats`).
pub fn read_model_catalog() -> ReadModelCatalog {
    ReadModelCatalog::new(APPLICATION).table_schema(PlayerStats::schema().clone())
}

/// The modeled projector mount for [`PLAYER_STATS`].
pub fn projector() -> SurfaceProjector {
    LocalProjectionMountsBuilder::new(APPLICATION, "ordered-domain-events")
        .expect("projection source")
        .eventual_model::<PlayerStats, _>(PROJECTOR, PLAYER_STATS, EPOCH)
        .expect("player stats mount")
        .build()
        .expect("projection catalog")
        .projector(PROJECTOR)
        .expect("player stats projector")
}

async fn project(
    context: CausalProjectorContext,
    projection: ModeledProjection,
) -> Result<(), HandlerError> {
    projection.apply(PLAYER_STATS, &context).await
}

/// The consumer service (bus consumer identity = [`APPLICATION`]): the
/// modeled projector over `read_models`.
pub fn projection_service(read_models: SqliteRepository) -> Service {
    Service::new().named(APPLICATION).routes(
        Routes::new()
            .with_read_model_store(read_models)
            .modeled_projector(projector())
            .handle(project),
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
    for (role, grant) in PlayerStats::permissions().surface_grants() {
        grants
            .entry(role)
            .or_default()
            .insert(PlayerStats::schema().model_name.clone(), grant);
    }
    grants
}

fn full_surface() -> Surface {
    build_surface(&read_model_catalog().tables, &SurfaceOptions::sqlite())
        .expect("mobpit Surface should build")
        .with_projection_owners([projector().into()])
        .expect("mobpit projector should bind")
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

/// The GraphQL engine over `repo` with `@live` pushes from its change stream.
pub fn build_engine(
    repo: &SqliteRepository,
    identity: &BearerIdentity,
    protocol_key: [u8; 32],
) -> Result<GraphqlEngine, String> {
    GraphqlEngine::builder(repo.pool().clone())
        .service_id(APPLICATION)
        .protocol_token_key(protocol_key)
        .roles(&[ROLE])
        .client_application_surface(APPLICATION, [ROLE], [ROLE])
        .model::<PlayerStats>(PlayerStats::permissions())
        .client_projection_owners([projector().into()])
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

/// Runtime configuration.
#[derive(Debug, Clone)]
pub struct ServiceOptions {
    /// `sqlite:` URL of the read-model database.
    pub database_url: String,
    /// `sqlite:` URL of the bus the zone server publishes to.
    pub bus_url: String,
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

    let engine = Arc::new(build_engine(
        &repo,
        &options.identity,
        options.protocol_key,
    )?);
    spawn_projector(repo.clone(), bus_pool);
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
