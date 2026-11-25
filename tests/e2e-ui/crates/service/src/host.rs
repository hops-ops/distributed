//! One-screen host bootstrap for the e2e-ui application.
//!
//! This playground is a single backend process plus the SvelteKit UI. Do not
//! add extra e2e-ui process topologies here. Optional celld+NATS is
//! `tests/e2e-ui/celld-nats-profile/` (`DCS-DEC-001`).
//!
//! Backend selection and identity remain here; the host body is written once
//! against [`SqlStores`]. The consumer loop is a framework worker helper.

use std::sync::Arc;

use distributed::gateway::DeliveryCapabilities;
use distributed::graphql::delivery::GatewayVersionStore;
use distributed::graphql::IdentityConfig;
use distributed::microsvc::{
    spawn_service_consumer_loop, LifecycleGate, PostgresStores, Service, SqlStores, SqliteStores,
};

use crate::modules::graphql::build_graphql_engine_with_delivery;
use crate::{
    build_service, read_models, serve, spawn_scrape_loop, ZitadelScrapeConfig, E2E_UI_APPLICATION,
};

const BUS_GROUP: &str = "e2e-ui";

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Bind address and identity for one local process host.
pub struct HostOptions {
    pub bind: String,
    pub identity: IdentityConfig,
    pub public_origin: String,
    pub ui_origin: String,
    pub delivery: DeliveryCapabilities,
}

/// Start the e2e-ui full-local process for SQLite or Postgres from `DATABASE_URL`.
pub async fn run(database_url: &str, options: HostOptions) -> Result<(), BoxError> {
    eprintln!(
        "e2e-ui host application=`{}` bind={}",
        E2E_UI_APPLICATION, options.bind
    );
    if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
        host(PostgresStores::connect(database_url).await?, options).await
    } else {
        host(SqliteStores::connect(database_url).await?, options).await
    }
}

/// One host path for every SQL backend: the stores, the bus and the GraphQL
/// pool all come from the one database.
async fn host<St: SqlStores>(stores: St, options: HostOptions) -> Result<(), BoxError> {
    let read_models = read_models();
    stores.bootstrap_read_models(read_models.catalog()).await?;

    let change_rx = stores.read_model_changes();
    // Under `distributed dev`, only the active generation may write.
    let lifecycle = LifecycleGate::from_env();
    let service = build_service(&stores)
        .with_lifecycle(lifecycle.clone())
        .with_bus(stores.bus(BUS_GROUP));
    let versions = if options.delivery != DeliveryCapabilities::default() {
        Some(
            GatewayVersionStore::install(
                stores.graphql_pool().pool(),
                "e2e-ui-gateway-v1",
                read_models.table_names(),
            )
            .await?,
        )
    } else {
        None
    };
    let gql = build_graphql_engine_with_delivery(
        stores.graphql_pool(),
        &service,
        options.identity.clone(),
        Some(change_rx),
        versions,
    )?;
    let service = Arc::new(service.try_with_graphql(gql)?);

    // `with_bus` also drains the outbox, so the consumer is the only loop.
    spawn_service_consumer_loop(move || {
        build_service(&stores)
            .with_lifecycle(lifecycle.clone())
            .with_bus(stores.bus(BUS_GROUP))
    });
    spawn_zitadel_scrape(service.clone());

    eprintln!("e2e-ui listening on http://{}", options.bind);
    serve(service, &options).await?;
    Ok(())
}

fn spawn_zitadel_scrape(service: Arc<Service>) {
    match ZitadelScrapeConfig::from_env() {
        Some(cfg) if cfg.background_enabled() || cfg.on_start => {
            eprintln!(
                "zitadel scrape: enabled (api={}, interval={}s, on_start={})",
                cfg.api_base,
                cfg.interval.as_secs(),
                cfg.on_start
            );
            spawn_scrape_loop(service, cfg);
        }
        Some(_) => {
            eprintln!(
                "zitadel scrape: credentials present, background off (interval=0); use POST /zitadel.scrape.v1"
            );
        }
        None => {
            eprintln!(
                "zitadel scrape: disabled (set ZITADEL_SERVICE_USER_TOKEN + OIDC_ISSUER/ZITADEL_API_URL)"
            );
        }
    }
}
