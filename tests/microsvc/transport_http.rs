//! HTTP transport integration tests.
//!
//! Starts an axum server and exercises it with reqwest.

use std::sync::Arc;

use distributed::microsvc::{self, Routes, Service};
use distributed::{AggregateBuilder, InMemoryRepository, Queueable};
use serde_json::json;

use crate::handlers;
use crate::models::counter::Counter;

fn counter_service() -> Arc<Service> {
    Arc::new(
        Service::new()
            .with_http_command_routes()
            .routes(distributed::routes!(
                Routes::new().with_repo(InMemoryRepository::new().queued().aggregate::<Counter>()),
                command handlers::counter_create,
                command handlers::counter_increment,
                command handlers::whoami,
            )),
    )
}

/// Bind to port 0 and return the actual address.
async fn start_server(service: Arc<Service>) -> String {
    start_app(microsvc::router(service)).await
}

/// Bind an axum app to port 0 and return the actual address.
async fn start_app(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn health_check() {
    let service = counter_service();
    let base = start_server(service).await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{base}/health")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);

    let commands = body["commands"].as_array().unwrap();
    assert!(commands.iter().any(|c| c == "counter.initialize"));
    assert!(commands.iter().any(|c| c == "counter.increment"));
}

#[tokio::test]
async fn create_counter() {
    let service = counter_service();
    let base = start_server(service).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/counter.initialize"))
        .json(&json!({ "id": "c1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "id": "c1" }));
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn metrics_endpoint_exposes_dispatch_counters() {
    let service = counter_service();
    let base = start_server(service).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/counter.initialize"))
        .json(&json!({ "id": "metrics-c1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client.get(format!("{base}/metrics")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("distributed_microsvc_dispatch_total"),
        "metrics body should include dispatch counters:\n{body}"
    );
    assert!(
        body.contains("message=\"counter.initialize\""),
        "metrics body should include the dispatched command label:\n{body}"
    );
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn standalone_metrics_router_exposes_worker_metrics() {
    distributed::metrics::record_outbox_message(Some("orders-worker"), "published");
    let base = start_app(distributed::metrics::http_router_for_service(
        "orders-worker",
    ))
    .await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{base}/metrics")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("distributed_service_info{service=\"orders-worker\""),
        "metrics body should include the worker service label:\n{body}"
    );
    assert!(
        body.contains(
            "distributed_outbox_messages_total{service=\"orders-worker\",outcome=\"published\"}"
        ),
        "metrics body should include worker outbox metrics:\n{body}"
    );
}

#[tokio::test]
async fn create_and_increment_counter() {
    let service = counter_service();
    let base = start_server(service).await;
    let client = reqwest::Client::new();

    // Create
    let resp = client
        .post(format!("{base}/counter.initialize"))
        .json(&json!({ "id": "c1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Increment
    let resp = client
        .post(format!("{base}/counter.increment"))
        .json(&json!({ "id": "c1", "amount": 5 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "id": "c1", "value": 5 }));

    // Increment again
    let resp = client
        .post(format!("{base}/counter.increment"))
        .json(&json!({ "id": "c1", "amount": 3 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "id": "c1", "value": 8 }));
}

#[tokio::test]
async fn increment_nonexistent_returns_404() {
    let service = counter_service();
    let base = start_server(service).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/counter.increment"))
        .json(&json!({ "id": "nope", "amount": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn unknown_command_returns_404() {
    let service = counter_service();
    let base = start_server(service).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/nonexistent"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn headers_flow_to_session() {
    let service = counter_service();
    let base = start_server(service).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/session.identify"))
        .header("x-user-id", "user-42")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "user_id": "user-42" }));
}

/// Documents the HTTP trust boundary: request headers are copied into the
/// `Session` verbatim and trusted at face value — the framework does NOT
/// authenticate. A client-supplied identity header is reflected straight
/// through, which is precisely why a trusted proxy must strip/inject these
/// headers in production. See `session_from_headers` and the `Session` docs.
#[tokio::test]
async fn client_supplied_identity_header_is_trusted_verbatim() {
    let service = counter_service();
    let base = start_server(service).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/session.identify"))
        // No proxy in front: the client sets its own identity. The framework
        // trusts it as-is. In production a trusted proxy must overwrite this.
        .header("x-user-id", "client-claimed-id")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "user_id": "client-claimed-id" }));
}

#[tokio::test]
async fn missing_session_returns_401() {
    let service = counter_service();
    let base = start_server(service).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/session.identify"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}
