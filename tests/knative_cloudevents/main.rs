//! Knative / CloudEvents HTTP ingress integration tests.
//!
//! Drives `cloud_events_router` over a real ephemeral HTTP server and asserts
//! the CloudEvents binding (binary + structured) and the ack/retry/permanent
//! response-status mapping. Runs in-process — no external broker.
#![cfg(feature = "knative")]

use std::sync::{Arc, Mutex};

use distributed::bus::{Bus, KnativeBus};
use distributed::microsvc::cloud_events_router;
use distributed::microsvc::{
    Context, HandlerError, Message, MessageKind, Routes, Service, SubscriptionPlan,
};
use serde_json::json;

async fn spawn_server() -> (String, Arc<Mutex<Vec<String>>>) {
    let handled = Arc::new(Mutex::new(Vec::<String>::new()));
    let h = handled.clone();
    let service = Arc::new(
        Service::new().routes(
            Routes::new()
                .with_dependencies(())
                .event("order.initialized")
                .handle(move |ctx: &Context<()>| {
                    h.lock()
                        .unwrap()
                        .push(ctx.message().id().unwrap_or_default().to_string());
                    async move { Ok(json!({"ok": true})) }
                })
                .event("order.temporarily_failed")
                .handle(|_ctx: &Context<()>| async move {
                    // A transient storage outage (connection refused / pool timeout):
                    // the same message may succeed on redelivery, so it must classify
                    // retryable. A deterministic `Model` fault would (correctly) be
                    // permanent — that is the `order.rejected` path below.
                    let io = std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "connection refused",
                    );
                    Err(HandlerError::Repository(
                        distributed::RepositoryError::retryable_storage("load stream", io),
                    ))
                })
                .event("order.rejected")
                .handle(|_ctx: &Context<()>| async move {
                    Err(HandlerError::Rejected("order.permanently_failed".into()))
                }),
        ),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = cloud_events_router(service);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/"), handled)
}

#[tokio::test]
async fn binary_mode_success_returns_200_after_handler() {
    let (url, handled) = spawn_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .header("ce-id", "evt-1")
        .header("ce-type", "order.initialized")
        .header("ce-source", "/orders")
        .header("content-type", "application/json")
        .body(r#"{"order":"o1"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(handled.lock().unwrap().clone(), vec!["evt-1".to_string()]);
}

#[tokio::test]
async fn structured_mode_success_returns_200() {
    let (url, handled) = spawn_server().await;
    let client = reqwest::Client::new();
    let event = json!({
        "specversion": "1.0",
        "id": "evt-2",
        "type": "order.initialized",
        "source": "/orders",
        "datacontenttype": "application/json",
        "data": {"order": "o2"},
    });
    let resp = client
        .post(&url)
        .header("content-type", "application/cloudevents+json")
        .body(event.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(handled.lock().unwrap().clone(), vec!["evt-2".to_string()]);
}

#[tokio::test]
async fn retryable_failure_returns_503() {
    let (url, _) = spawn_server().await;
    let resp = reqwest::Client::new()
        .post(&url)
        .header("ce-id", "evt-3")
        .header("ce-type", "order.temporarily_failed")
        .body("{}")
        .send()
        .await
        .unwrap();
    // Knative should redeliver.
    assert_eq!(resp.status(), 503);
}

#[tokio::test]
async fn permanent_failure_returns_422() {
    let (url, _) = spawn_server().await;
    let resp = reqwest::Client::new()
        .post(&url)
        .header("ce-id", "evt-4")
        .header("ce-type", "order.rejected")
        .body("{}")
        .send()
        .await
        .unwrap();
    // Knative should not retry; its Delivery config dead-letters.
    assert_eq!(resp.status(), 422);
}

#[tokio::test]
async fn unknown_type_returns_422() {
    let (url, _) = spawn_server().await;
    let resp = reqwest::Client::new()
        .post(&url)
        .header("ce-id", "evt-5")
        .header("ce-type", "no.such.handler")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 422);
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn metrics_endpoint_exposes_cloud_event_dispatch_counters() {
    let (url, _) = spawn_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .header("ce-id", "evt-metrics")
        .header("ce-type", "order.initialized")
        .header("ce-source", "/orders")
        .header("content-type", "application/json")
        .body(r#"{"order":"o-metrics"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client.get(format!("{url}metrics")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("distributed_microsvc_dispatch_total{service=\"unnamed\",message_kind=\"event\",message=\"order.initialized\",status=\"success\"}"),
        "metrics body should include the CloudEvents dispatch counter:\n{body}"
    );
}

#[tokio::test]
async fn missing_id_returns_400() {
    let (url, _) = spawn_server().await;
    let resp = reqwest::Client::new()
        .post(&url)
        .header("ce-type", "order.initialized")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

// ---- KnativeBus: produce (POST CloudEvent) + manifest generation ----

/// `KnativeBus::publish` POSTs a binary CloudEvent to the broker-ingress URL;
/// pointed at the local router it round-trips into `Service::dispatch_message`.
#[tokio::test]
async fn knative_bus_publish_round_trips_through_router() {
    let (url, handled) = spawn_server().await;
    // ingress_base = the server (no broker/namespace) so the POST hits `POST /`.
    let bus = KnativeBus::new(url.trim_end_matches('/'), "", "orders-svc", "", "");
    bus.publish_message(
        Message::new(
            "order.initialized",
            MessageKind::Event,
            br#"{"order":"o9"}"#.to_vec(),
        )
        .with_id("evt-9"),
    )
    .await
    .expect("publish round-trips");
    // publish awaits the HTTP response, which the router returns only after
    // dispatch, so the handler has already recorded by now.
    assert_eq!(handled.lock().unwrap().clone(), vec!["evt-9".to_string()]);
}

/// A produce with no message id is rejected before any POST (CloudEvents
/// mandates `id`).
#[tokio::test]
async fn knative_bus_publish_without_id_is_rejected() {
    let bus = KnativeBus::new("http://127.0.0.1:1", "", "orders-svc", "", "");
    let err = bus
        .publish_message(Message::new(
            "order.initialized",
            MessageKind::Event,
            b"{}".to_vec(),
        ))
        .await
        .expect_err("missing id is rejected");
    assert!(
        err.to_string().contains("id"),
        "error mentions the missing id: {err}"
    );
}

/// `manifests` renders the role-based Brokers + per-name Triggers a service's
/// chart needs, with `/cloudevent/<type>` subscriber URIs.
#[test]
fn knative_bus_manifests_render_brokers_and_triggers() {
    let bus = KnativeBus::new(
        "http://broker-ingress.knative-eventing.svc.cluster.local",
        "game",
        "model-svc",
        "lobby-svc-commands",
        "model-svc-events",
    );
    let plan = SubscriptionPlan {
        commands: vec!["place.bet".to_string()],
        events: vec!["seat.reserved".to_string()],
    };
    let yaml = bus.manifests(&plan, &[("seat.reserved", "lobby-svc-events")]);

    // Own commands broker + a Trigger for the handled command.
    assert!(yaml.contains("kind: Broker"));
    assert!(yaml.contains("name: model-svc-commands"));
    assert!(yaml.contains("type: place.bet"));
    // Own events broker (this service publishes).
    assert!(yaml.contains("name: model-svc-events"));
    // Subscribed event → Trigger on the *producer's* broker, per-type subscriber.
    assert!(yaml.contains("broker: lobby-svc-events"));
    assert!(yaml.contains("type: seat.reserved"));
    assert!(yaml.contains("uri: /cloudevent/seat.reserved"));
    assert!(yaml.contains("namespace: game"));
}

/// A pure consumer (`publishes_events(false)`) creates no events broker; the
/// local flag points subscribers at a kubefwd address.
#[test]
fn knative_bus_manifests_pure_consumer_local_variant() {
    let bus = KnativeBus::new(
        "http://ingress",
        "game",
        "projection-svc",
        "",
        "projection-svc-events",
    )
    .publishes_events(false)
    .local("127.0.0.1:8080");
    let plan = SubscriptionPlan {
        commands: vec![],
        events: vec!["seat.reserved".to_string()],
    };
    let yaml = bus.manifests(&plan, &[("seat.reserved", "lobby-svc-events")]);

    assert!(
        !yaml.contains("projection-svc-events"),
        "pure consumer owns no broker"
    );
    assert!(
        !yaml.contains("kind: Broker"),
        "pure consumer creates no broker"
    );
    assert!(yaml.contains("kind: Trigger"));
    assert!(
        yaml.contains("uri: http://127.0.0.1:8080/cloudevent/seat.reserved"),
        "local flag points the subscriber at the kubefwd address"
    );
}
