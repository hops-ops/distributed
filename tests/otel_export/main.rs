//! OTLP export end-to-end test against a real OpenTelemetry Collector.
//!
//! The `otel` feature deliberately does not install a tracer or exporter — the
//! service binary owns that wiring. This test plays the service binary's role:
//! it builds a real OTLP pipeline (`opentelemetry-otlp` over HTTP/protobuf,
//! batch processor), dispatches a message carrying a W3C `traceparent` through
//! a `Service`, and asserts the collector received the framework's
//! `distributed.microsvc.dispatch` span with:
//!
//! - the trace id from the incoming `traceparent` (context extraction), and
//! - `parentSpanId` equal to the incoming parent span id (span parenting).
//!
//! Skips unless both env vars are set (see `.github/workflows/
//! integration-observability.yaml` for the collector container setup):
//!
//! - `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` — e.g. `http://localhost:4318/v1/traces`
//! - `OTEL_COLLECTOR_TRACES_FILE` — host path of the collector's file-exporter
//!   output (`/out/traces.json` in the container)
#![cfg(all(feature = "http", feature = "otel"))]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use distributed::microsvc::{Context, Message, MessageKind, Routes, Service};
use opentelemetry::propagation::{Extractor, TextMapPropagator as _};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use serde_json::{json, Value};
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::layer::SubscriberExt as _;

#[path = "../support/env.rs"]
mod env_support;

/// The parent span id we inject; the exported framework span must be its child.
const REMOTE_PARENT_SPAN_ID: &str = "00f067aa0ba902b7";

#[tokio::test]
async fn dispatch_span_reaches_collector_with_propagated_parent() {
    let Some(endpoint) = env_support::require_service(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "otel collector export test",
    ) else {
        return;
    };
    let Some(traces_file) =
        env_support::require_service("OTEL_COLLECTOR_TRACES_FILE", "otel collector export test")
    else {
        return;
    };

    // A fresh trace id per run so stale collector output can never satisfy the
    // assertion.
    let trace_id = fresh_trace_id(1);
    let traceparent = format!("00-{trace_id}-{REMOTE_PARENT_SPAN_ID}-01");

    // The exporter pipeline a service binary would install.
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()
        .expect("build OTLP span exporter");
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build();
    let tracer = provider.tracer("otel-export-test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    let _guard = tracing::subscriber::set_default(subscriber);

    let service = Arc::new(
        Service::new().named("otel-export").routes(
            Routes::new()
                .with_dependencies(())
                .command("orders.create")
                .handle(|_ctx: &Context<()>| async move { Ok(json!({"ok": true})) }),
        ),
    );
    let message = Message::new(
        "orders.create",
        MessageKind::Command,
        br#"{"id":"o-1"}"#.to_vec(),
    )
    .with_id("evt-otel-1")
    .with_metadata("traceparent", &traceparent);

    service
        .dispatch_message(&message)
        .await
        .expect("dispatch succeeds");

    let nested_trace_id = fresh_trace_id(17);
    let nested_traceparent = format!("00-{nested_trace_id}-{REMOTE_PARENT_SPAN_ID}-01");
    let outer_span = tracing::info_span!("test.outer");
    let parent_context = opentelemetry_sdk::propagation::TraceContextPropagator::new().extract(
        &TraceparentExtractor {
            traceparent: &nested_traceparent,
        },
    );
    let _ = outer_span.set_parent(parent_context);
    let nested_message = Message::new(
        "orders.create",
        MessageKind::Command,
        br#"{"id":"o-2"}"#.to_vec(),
    )
    .with_id("evt-otel-nested")
    .with_metadata("traceparent", &nested_traceparent);
    service
        .dispatch_message(&nested_message)
        .instrument(outer_span)
        .await
        .expect("nested dispatch succeeds");

    provider.force_flush().expect("flush spans to collector");
    provider.shutdown().expect("shutdown provider");

    let span = poll_for_span(
        &traces_file,
        &trace_id,
        "distributed.microsvc.dispatch",
        Duration::from_secs(20),
    );
    assert_eq!(
        span["parentSpanId"].as_str(),
        Some(REMOTE_PARENT_SPAN_ID),
        "dispatch span must be parented to the incoming traceparent: {span}"
    );

    let outer_span = poll_for_span(
        &traces_file,
        &nested_trace_id,
        "test.outer",
        Duration::from_secs(20),
    );
    let nested_dispatch_span = poll_for_span(
        &traces_file,
        &nested_trace_id,
        "distributed.microsvc.dispatch",
        Duration::from_secs(20),
    );
    assert_eq!(
        outer_span["parentSpanId"].as_str(),
        Some(REMOTE_PARENT_SPAN_ID),
        "outer span must keep the incoming traceparent as its parent: {outer_span}"
    );
    assert_eq!(
        nested_dispatch_span["parentSpanId"].as_str(),
        outer_span["spanId"].as_str(),
        "dispatch span must preserve the active local span hierarchy: {nested_dispatch_span}"
    );
}

/// Poll the collector's file-exporter output until the framework dispatch span
/// for `trace_id` appears, and return it.
fn poll_for_span(path: &str, trace_id: &str, span_name: &str, timeout: Duration) -> Value {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(contents) = std::fs::read_to_string(path) {
            for line in contents.lines() {
                let Ok(batch) = serde_json::from_str::<Value>(line) else {
                    continue;
                };
                if let Some(span) = find_span(&batch, trace_id, span_name) {
                    return span;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "collector never exported span {span_name} for trace {trace_id}; \
             file {path} contents:\n{}",
            std::fs::read_to_string(path).unwrap_or_else(|e| format!("<unreadable: {e}>"))
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn find_span(batch: &Value, trace_id: &str, span_name: &str) -> Option<Value> {
    for resource_spans in batch["resourceSpans"].as_array()? {
        let Some(scope_spans) = resource_spans["scopeSpans"].as_array() else {
            continue;
        };
        for scope_span in scope_spans {
            let Some(spans) = scope_span["spans"].as_array() else {
                continue;
            };
            for span in spans {
                if span["traceId"].as_str() == Some(trace_id)
                    && span["name"].as_str() == Some(span_name)
                {
                    return Some(span.clone());
                }
            }
        }
    }
    None
}

fn fresh_trace_id(salt: u128) -> String {
    format!(
        "{:032x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .wrapping_add(salt)
            | 1
    )
}

struct TraceparentExtractor<'a> {
    traceparent: &'a str,
}

impl Extractor for TraceparentExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        key.eq_ignore_ascii_case("traceparent")
            .then_some(self.traceparent)
    }

    fn keys(&self) -> Vec<&str> {
        vec!["traceparent"]
    }
}
