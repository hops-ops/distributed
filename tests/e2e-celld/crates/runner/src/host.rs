//! The celld host of the e2e-ui application.
//!
//! The same `build_service` as e2e-ui; what differs is the command host and
//! the bus. Todo and Chat commands wait-dispatch to their celld cells, routed
//! from the mounted declarations; Blob and the Zitadel ingress run
//! in-process. GraphQL `@live` and the Eventual projectors stay in this
//! process, fed from NATS.

use std::sync::Arc;

use chat_domain::ChatMessage;
use distributed::bus::{celld_queue_relay_handler, NatsBus};
use distributed::graphql::IdentityConfig;
use distributed::microsvc::cell_host::{CelldCommandHost, InternalHttpSecret};
use distributed::microsvc::{
    spawn_service_consumer_loop, LifecycleGate, PostgresStores, SqlStores,
};
use distributed::outbox_worker::BusPublisher;
use e2e_service::{
    build_graphql_engine, build_service, read_models, spawn_scrape_from_env, E2E_UI_APPLICATION,
};
use todo_domain::Todo;

use crate::http::serve;

const BUS_GROUP: &str = "e2e-celld";

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub struct HostOptions {
    pub bind: String,
    pub public_origin: String,
    pub ui_origin: String,
    pub identity: IdentityConfig,
    pub celld_url: String,
    pub nats_url: String,
    pub internal_secret: InternalHttpSecret,
}

/// Start the celld host. Read models need Postgres: `@live` and the
/// projectors share it with e2e-ui's compose stack.
pub async fn run(database_url: &str, options: HostOptions) -> Result<(), BoxError> {
    eprintln!(
        "e2e-celld application=`{}` bind={} CELLD_URL={} NATS_URL={}",
        E2E_UI_APPLICATION, options.bind, options.celld_url, options.nats_url
    );
    if !(database_url.starts_with("postgres://") || database_url.starts_with("postgresql://")) {
        return Err("e2e-celld requires Postgres DATABASE_URL (make -C tests/e2e-ui up)".into());
    }
    let stores = PostgresStores::connect(database_url).await?;
    stores
        .bootstrap_read_models(read_models().catalog())
        .await?;
    let nats = connect_nats(&options.nats_url).await?;

    let change_rx = stores.read_model_changes();
    // Under `distributed dev`, only the active generation may write.
    let lifecycle = LifecycleGate::from_env();
    let service = build_service(&stores)
        .with_lifecycle(lifecycle.clone())
        .with_bus(nats.clone());
    let gql = build_graphql_engine(
        stores.graphql_pool(),
        &service,
        options.identity.clone(),
        Some(change_rx),
    )?;
    let service = Arc::new(service.try_with_graphql(gql)?);
    // Every mounted Todo and Chat command goes to its aggregate's cell.
    let commands = CelldCommandHost::new(
        &options.celld_url,
        Arc::clone(&service),
        options.internal_secret.clone(),
    )?
    .route_aggregate::<Todo>()?
    .route_aggregate::<ChatMessage>()?;
    let queue_relay = celld_queue_relay_handler(BusPublisher::new(Arc::new(nats.clone())));

    // `with_bus` also drains the outbox, so the consumer is the only loop.
    spawn_service_consumer_loop(move || {
        build_service(&stores)
            .with_lifecycle(lifecycle.clone())
            .with_bus(nats.clone())
    });
    spawn_scrape_from_env(service.clone());

    eprintln!(
        "e2e-celld (postgres) listening on http://{} — cell wait-path; bus drain; @live stays here",
        options.bind
    );
    serve(service, Arc::new(commands), queue_relay, &options).await?;
    Ok(())
}

async fn connect_nats(url: &str) -> Result<NatsBus, BoxError> {
    let bus = NatsBus::connect(url)
        .namespace("e2e-celld")
        .group(BUS_GROUP)
        .await?;
    bus.ensure_stream().await?;
    eprintln!("e2e-celld bus ready (nats {url}); swap connect_nats for Kafka/Rabbit");
    Ok(bus)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chat_domain::ChatMessage;
    use distributed::lock::InMemoryLockManager;
    use distributed::microsvc::cell_host::{CelldCommandHost, InternalHttpSecret};
    use distributed::microsvc::Stores;
    use distributed::InMemoryRepository;
    use e2e_service::build_service;
    use todo_domain::Todo;

    /// Every Todo and Chat command e2e-ui mounts has a cell: each declares
    /// the shard `route_aggregate` addresses.
    #[test]
    fn every_mounted_todo_and_chat_command_routes_to_a_cell() {
        let repository = InMemoryRepository::new();
        let service = build_service(&Stores::new(
            repository.clone(),
            InMemoryLockManager::new(),
            repository,
        ));
        CelldCommandHost::new(
            "http://127.0.0.1:1",
            Arc::new(service),
            InternalHttpSecret::new("test-only-internal-secret-32-bytes").unwrap(),
        )
        .unwrap()
        .route_aggregate::<Todo>()
        .expect("every Todo command declares its shard")
        .route_aggregate::<ChatMessage>()
        .expect("chat.post declares its shard");
    }
}
