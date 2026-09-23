//! Celld example GraphQL host. Not the e2e-ui one-process playground.
//!
//! Todo create/complete and chat.post wait-dispatch to celld. GraphQL `@live`
//! and Eventual projectors stay in this process.

use std::sync::Arc;
use std::time::Duration;

use distributed::bus::celld_queue_relay_handler;
use distributed::bus::NatsBus;
use distributed::cell_host::{CelldCommandHost, InternalHttpSecret};
use distributed::command_dispatch::SharedCommandHost;
use distributed::graphql::IdentityConfig;
use distributed::microsvc::{spawn_outbox_publish_loop, spawn_service_consumer_loop, Service};
use distributed::BusPublisher;
use distributed::{PostgresLockManager, PostgresRepository};

use crate::http::serve;
use crate::{
    build_graphql_engine, build_service, distributed_manifest, spawn_scrape_loop,
    ZitadelScrapeConfig, E2E_UI_APPLICATION,
};

const BUS_GROUP: &str = "e2e-celld";

pub struct HostOptions {
    pub bind: String,
    pub identity: IdentityConfig,
    pub celld_url: String,
    pub nats_url: String,
    pub internal_secret: InternalHttpSecret,
}

pub async fn run(
    database_url: &str,
    options: HostOptions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let celld_url = options.celld_url.trim_end_matches('/').to_string();
    eprintln!(
        "e2e-celld graphql application=`{}` bind={} CELLD_URL={} NATS_URL={}",
        E2E_UI_APPLICATION, options.bind, celld_url, options.nats_url
    );
    if !(database_url.starts_with("postgres://") || database_url.starts_with("postgresql://")) {
        return Err("e2e-celld requires Postgres DATABASE_URL (make -C tests/e2e-ui up)".into());
    }
    run_postgres(database_url, options, celld_url).await
}

async fn run_postgres(
    database_url: &str,
    options: HostOptions,
    celld_url: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let repo = PostgresRepository::connect_and_migrate(database_url).await?;
    let registry = distributed_manifest()
        .table_registry()
        .map_err(|e| format!("manifest: {e}"))?;
    repo.bootstrap_table_schema_for_dev(&registry).await?;
    let locks = PostgresLockManager::new(repo.pool().clone());
    let nats = connect_nats(&options.nats_url).await?;

    let change_rx = repo.read_model_changes();
    let service = build_service(repo.clone(), locks.clone(), repo.clone()).with_bus(nats.clone());
    let gql = build_graphql_engine(&repo, &service, options.identity.clone(), Some(change_rx))?;
    let service = Arc::new(service.try_with_graphql(gql)?);
    let publisher = BusPublisher::new(Arc::new(nats.clone()));
    let celld_host = celld_command_host(
        celld_url.clone(),
        Arc::clone(&service),
        options.internal_secret.clone(),
    )?;
    let queue_relay = celld_queue_relay_handler(publisher);
    let host: SharedCommandHost = Arc::new(celld_host);

    spawn_outbox_publish_loop(
        repo.outbox_store(),
        Arc::new(nats.clone()),
        "e2e-celld",
        Duration::from_secs(30),
        5,
    );
    {
        let repo = repo.clone();
        let locks = locks.clone();
        let nats = nats.clone();
        spawn_service_consumer_loop(move || {
            build_service(repo.clone(), locks.clone(), repo.clone()).with_bus(nats.clone())
        });
    }
    spawn_zitadel_scrape(repo.clone());

    eprintln!(
        "e2e-celld (postgres) listening on http://{} — cell wait-path; bus drain; @live stays here",
        options.bind
    );
    serve(
        service,
        host,
        &options.bind,
        queue_relay,
        options.internal_secret,
    )
    .await?;
    Ok(())
}

fn celld_command_host(
    celld_url: String,
    service: Arc<Service>,
    internal_secret: InternalHttpSecret,
) -> Result<CelldCommandHost, distributed::microsvc::CausalDispatchError> {
    Ok(CelldCommandHost::new(celld_url, service, internal_secret)?
        .route(e2e_celld_todo::celld_route())
        .route(e2e_celld_chat::celld_route()))
}

async fn connect_nats(url: &str) -> Result<NatsBus, Box<dyn std::error::Error + Send + Sync>> {
    let bus = NatsBus::connect(url)
        .namespace("e2e-celld")
        .group(BUS_GROUP)
        .await?;
    bus.ensure_stream().await?;
    eprintln!("e2e-celld bus ready (nats {url}); swap connect_nats for Kafka/Rabbit");
    Ok(bus)
}

fn spawn_zitadel_scrape<R>(repo: R)
where
    R: distributed::TransactionalCommit
        + distributed::ReadModelWritePlanStore
        + Clone
        + Send
        + Sync
        + 'static,
{
    match ZitadelScrapeConfig::from_env() {
        Some(cfg) if cfg.background_enabled() || cfg.on_start => {
            eprintln!(
                "zitadel scrape: enabled (api={}, interval={}s, on_start={})",
                cfg.api_base,
                cfg.interval.as_secs(),
                cfg.on_start
            );
            spawn_scrape_loop(repo, cfg);
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
