//! Opt-in aggregate-command load harness.
//!
//! Not a workspace member — run via `--manifest-path tests/load/Cargo.toml`.

pub mod client;
pub mod counter;
pub mod host;
pub mod invoke;
pub mod kinds;
pub mod stats;
pub mod suite;

pub use client::{run_client, ClientConfig, Scenario};
pub use host::{bind_listener, serve_listener, CounterService, HostConfig};
pub use kinds::{BusKind, DispatchKind, LockKind, RepoKind};
pub use stats::{percentile_ms, RunReport};
pub use suite::{default_cells, run_suite, Cell, SuiteConfig};

#[cfg(test)]
mod smoke_tests {
    use super::*;
    use crate::host::{wait_for_health, INCREMENT, INITIALIZE};
    use crate::invoke::Invoker;
    use distributed::microsvc::Session;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn memory_host_initialize_and_increment() {
        let host = CounterService::start(&HostConfig::default())
            .await
            .expect("start memory host");
        let listener = bind_listener("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let base = format!("http://{addr}");
        tokio::spawn(async move {
            serve_listener(host.service, listener).await.unwrap();
        });
        wait_for_health(&base, Duration::from_secs(5))
            .await
            .expect("health");

        let client = reqwest::Client::new();
        let created = client
            .post(format!("{base}/{INITIALIZE}"))
            .json(&json!({ "id": "c1" }))
            .send()
            .await
            .expect("create send");
        assert_eq!(created.status(), 200);

        let incremented = client
            .post(format!("{base}/{INCREMENT}"))
            .json(&json!({ "id": "c1", "amount": 3 }))
            .send()
            .await
            .expect("increment send");
        assert_eq!(incremented.status(), 200);
        let body: serde_json::Value = incremented.json().await.expect("json");
        assert_eq!(body["value"], 3);
    }

    #[tokio::test]
    async fn memory_direct_dispatch_and_in_memory_bus() {
        let host = CounterService::start(&HostConfig::default())
            .await
            .expect("start");
        host.service
            .dispatch(INITIALIZE, json!({ "id": "direct-1" }), Session::new())
            .await
            .expect("direct initialize");

        let bus = distributed::bus::InMemoryBus::new();
        let runtime = crate::invoke::BusRuntime::start(bus, Arc::clone(&host.service), false);
        let invoker = Invoker::Bus(runtime.invoker.clone());
        invoker
            .invoke(INITIALIZE, json!({ "id": "bus-1" }))
            .await
            .expect("bus initialize");
        runtime.stop();
    }

    #[tokio::test]
    async fn memory_grpc_initialize_and_increment() {
        let host = CounterService::start(&HostConfig::default())
            .await
            .expect("start");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let grpc_svc = distributed::microsvc::grpc_server(host.service);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(grpc_svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let endpoint = format!("http://{addr}");
        let mut client = None;
        for _ in 0..50 {
            if let Ok(c) =
                distributed::microsvc::grpc::CommandServiceClient::connect(endpoint.clone()).await
            {
                client = Some(c);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let client = client.expect("grpc connect");
        let invoker = Invoker::Grpc(client);
        invoker
            .invoke(INITIALIZE, json!({ "id": "grpc-1" }))
            .await
            .expect("grpc initialize");
        invoker
            .invoke(INCREMENT, json!({ "id": "grpc-1", "amount": 4 }))
            .await
            .expect("grpc increment");
    }

    #[tokio::test]
    async fn sqlite_bus_completes_one_initialize() {
        let path =
            std::env::temp_dir().join(format!("load-sqlite-bus-{}.sqlite", uuid::Uuid::now_v7()));
        let inner = crate::host::connect_sqlite(&path, 4).await.expect("sqlite");
        let built = crate::host::build_service_from_sqlite(inner.clone(), LockKind::Memory, None)
            .expect("service");
        let plan = built.service.subscription_plan();
        assert!(
            plan.commands.iter().any(|n| n == INITIALIZE),
            "expected {INITIALIZE} in {:?}",
            plan.commands
        );
        let bus = distributed::bus::SqliteBus::new(inner.pool().clone()).group("load-test");
        bus.ensure_tables().await.expect("tables");
        let runtime = crate::invoke::BusRuntime::start(bus, Arc::clone(&built.service), false);
        let invoker = Invoker::Bus(runtime.invoker.clone());
        invoker
            .invoke(INITIALIZE, json!({ "id": "sqlite-bus-1" }))
            .await
            .expect("bus initialize");
        runtime.stop();
    }

    #[tokio::test]
    async fn memory_snapshots_every_event() {
        let mut config = HostConfig::default();
        config.snapshot_frequency = Some(1);
        let host = CounterService::start(&config).await.expect("start");
        host.service
            .dispatch(INITIALIZE, json!({ "id": "snap-1" }), Session::new())
            .await
            .expect("initialize");
        host.service
            .dispatch(
                INCREMENT,
                json!({ "id": "snap-1", "amount": 1 }),
                Session::new(),
            )
            .await
            .expect("increment");
    }
}
