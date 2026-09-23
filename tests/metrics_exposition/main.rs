//! Prometheus exposition integration tests.
//!
//! Drives real traffic through an HTTP service and the outbox dispatcher so
//! every framework metric family (info gauge, counters, histogram, backlog
//! gauges) is populated, then scrapes `GET /metrics` over a real ephemeral
//! server and validates the exposition:
//!
//! - in-process assertions on content type and family presence (always run);
//! - `promtool check metrics` on the scraped body (skips when `PROMTOOL` is
//!   unset), which is the compatibility gate against real Prometheus.
//!
//! The metrics registry is process-global, so everything runs in one test to
//! keep the scraped snapshot deterministic.
#![cfg(all(feature = "http", feature = "metrics"))]

use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed::bus::{MessagePublisher, TransportError};
use distributed::microsvc::{self, Context, Message, Routes, Service};
use distributed::outbox_worker::OutboxDispatcher;
use distributed::{CommitBatch, InMemoryRepository, OutboxMessage, TransactionalCommit};
use serde_json::json;

#[path = "../support/env.rs"]
mod env_support;

/// Publisher that fails ids containing "poison" and accepts the rest.
struct SelectivePublisher {
    published: Mutex<Vec<String>>,
}

impl MessagePublisher for SelectivePublisher {
    async fn publish(&self, message: Message) -> Result<(), TransportError> {
        let id = message.id().unwrap_or_default().to_string();
        if id.contains("poison") {
            return Err(TransportError::retryable("selective publish failure"));
        }
        self.published.lock().unwrap().push(id);
        Ok(())
    }
}

async fn spawn_http_service(name: &str) -> String {
    let service = Arc::new(
        Service::new()
            .named(name)
            .with_http_command_routes()
            .routes(
                Routes::new()
                    .with_dependencies(())
                    .command("orders.create")
                    .handle(|_ctx: &Context<()>| async move { Ok(json!({"ok": true})) }),
            ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = microsvc::router(service);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// Stage outbox rows through a commit and drain them through the dispatcher so
/// `distributed_outbox_messages_total` and the backlog gauges are populated.
async fn drive_outbox(service_name: &str) {
    let repo = InMemoryRepository::new();
    let mut batch = CommitBatch::empty();
    for id in ["evt-ok-1", "evt-ok-2", "evt-poison"] {
        batch
            .outbox_messages
            .push(OutboxMessage::create(id, "orders.created", b"{}".to_vec()).unwrap());
    }
    repo.commit_batch(batch).await.unwrap();

    let dispatcher = OutboxDispatcher::new(
        repo.outbox_store(),
        SelectivePublisher {
            published: Mutex::new(Vec::new()),
        },
        "metrics-exposition:test",
        Duration::from_secs(60),
        3,
    )
    .with_service(service_name);

    let outcome = dispatcher.dispatch_batch(10).await.unwrap();
    assert_eq!(outcome.published, 2);
    assert_eq!(outcome.released, 1);
}

#[tokio::test]
async fn exposition_covers_framework_families_and_passes_promtool() {
    let base = spawn_http_service("orders-exposition").await;
    let client = reqwest::Client::new();

    // Successful dispatches populate the counter and duration histogram.
    for _ in 0..2 {
        let resp = client
            .post(format!("{base}/orders.create"))
            .json(&json!({"id": "o-1"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
    // An unknown command populates the failure status bucket.
    let resp = client
        .post(format!("{base}/orders.unknown"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_ne!(resp.status(), 200);

    drive_outbox("orders-exposition").await;

    let scrape = client.get(format!("{base}/metrics")).send().await.unwrap();
    assert_eq!(scrape.status(), 200);
    let content_type = scrape
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        content_type.starts_with("text/plain; version=0.0.4"),
        "scrape content type must be Prometheus text exposition: {content_type}"
    );

    let body = scrape.text().await.unwrap();
    for family in [
        "distributed_service_info{service=\"orders-exposition\"",
        "distributed_microsvc_dispatch_total{service=\"orders-exposition\"",
        "distributed_microsvc_dispatch_duration_seconds_bucket{service=\"orders-exposition\"",
        "distributed_microsvc_dispatch_duration_seconds_sum{service=\"orders-exposition\"",
        "distributed_microsvc_dispatch_duration_seconds_count{service=\"orders-exposition\"",
        "distributed_outbox_messages_total{service=\"orders-exposition\",outcome=\"published\"} 2",
        "distributed_outbox_messages_total{service=\"orders-exposition\",outcome=\"released\"} 1",
        "distributed_outbox_pending_messages{service=\"orders-exposition\"} 1",
        "distributed_outbox_oldest_pending_age_seconds{service=\"orders-exposition\"}",
    ] {
        assert!(
            body.contains(family),
            "scrape must contain `{family}`:\n{body}"
        );
    }

    promtool_check(&body);
}

/// Pipe the scraped exposition through `promtool check metrics`. This is the
/// real compatibility gate: it applies Prometheus's own parser and lint rules
/// (HELP/TYPE consistency, histogram bucket ordering, label escaping).
fn promtool_check(body: &str) {
    let Some(promtool) = env_support::broker_env("PROMTOOL", "promtool exposition lint") else {
        return;
    };

    let mut child = Command::new(&promtool)
        .args(["check", "metrics"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn promtool");
    child
        .stdin
        .take()
        .expect("promtool stdin")
        .write_all(body.as_bytes())
        .expect("write exposition to promtool");
    let output = child.wait_with_output().expect("promtool exit");
    assert!(
        output.status.success(),
        "promtool check metrics failed:\nstdout: {}\nstderr: {}\nexposition:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        body
    );
}
