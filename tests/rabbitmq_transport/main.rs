//! RabbitMQ transport adapter integration tests.
//!
//! Publishes via `RabbitPublisher` (publisher confirms) and consumes via
//! `RabbitSource` (`basic_get`) against a broker. Skips when `AMQP_URL` is unset.
#![cfg(feature = "rabbitmq")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use distributed::bus::{
    run_source, Bus, BusConsumer, Handlers, MessagePublisher, RabbitBus, RabbitPublisher,
    RabbitSource, RunOptions, TransportError,
};
use distributed::microsvc::{Context, Message, MessageKind, Routes, Service};
use distributed::TRACEPARENT;
use lapin::options::{BasicPublishOptions, QueueDeclareOptions};
use lapin::types::{FieldTable, ShortString};
use lapin::{BasicProperties, Connection, ConnectionProperties};
use serde_json::json;

// Shared broker-test helpers (recording_for, named_recording_for, unique, ...).
#[path = "../transport_conformance/mod.rs"]
mod conformance;
use conformance::{named_recording_for, recording_for, unique};
#[path = "../support/env.rs"]
mod env_support;

fn amqp_url() -> Option<String> {
    env_support::require_service("AMQP_URL", "rabbitmq transport test")
}

/// Options that drain to idle and keep dead letters where the test can read
/// them.
fn storing_dead_letters() -> (Arc<distributed::bus::InMemoryDeadLetters>, RunOptions) {
    let dead = Arc::new(distributed::bus::InMemoryDeadLetters::new());
    let options = RunOptions::until_idle().with_dead_letters(dead.clone());
    (dead, options)
}

/// The one stored dead letter: its message id (if any), payload and reason.
fn only_dead_letter(
    dead: &distributed::bus::InMemoryDeadLetters,
) -> (Option<String>, Vec<u8>, String) {
    let mut all = dead.all();
    assert_eq!(all.len(), 1, "exactly one message was dead-lettered");
    let dead = all.remove(0);
    assert_eq!(dead.attempts, 1);
    (
        dead.message.id().map(str::to_owned),
        dead.message.payload().to_vec(),
        dead.reason,
    )
}

#[tokio::test]
async fn publish_then_consume_round_trips_through_rabbitmq() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("order.initialized");

    // Declare the queue (source) before publishing so the default-exchange route
    // has a destination.
    let source = RabbitSource::connect(&url, &queue)
        .await
        .expect("connect source");
    let publisher = RabbitPublisher::connect(&url)
        .await
        .expect("connect publisher");
    for i in 0..3 {
        let message =
            Message::new(&queue, MessageKind::Event, b"{}".to_vec()).with_id(format!("m{i}"));
        publisher.publish(message).await.expect("publish");
    }

    let handled = Arc::new(Mutex::new(Vec::<String>::new()));
    let h = handled.clone();
    let service = Arc::new(
        Service::new().routes(
            Routes::new()
                .with_dependencies(())
                .event(Box::leak(queue.clone().into_boxed_str()))
                .handle(move |ctx: &Context<()>| {
                    h.lock()
                        .unwrap()
                        .push(ctx.message().id().unwrap_or_default().to_string());
                    async move { Ok(json!({})) }
                }),
        ),
    );
    run_source(service, source, RunOptions::until_idle())
        .await
        .expect("run_source drains the queue");

    let mut ids = handled.lock().unwrap().clone();
    ids.sort();
    assert_eq!(
        ids,
        vec!["m0".to_string(), "m1".to_string(), "m2".to_string()]
    );
}

#[tokio::test]
async fn message_id_and_metadata_survive_the_round_trip() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("order.initialized");

    let source = RabbitSource::connect(&url, &queue)
        .await
        .expect("connect source");
    let publisher = RabbitPublisher::connect(&url)
        .await
        .expect("connect publisher");
    // Use a non-default content type to prove it survives the AMQP round-trip
    // (Message::new defaults to application/json).
    let mut message = Message::new(&queue, MessageKind::Event, br#"{"k":"v"}"#.to_vec())
        .with_id("evt-1")
        .with_metadata("correlation_id", "corr-9")
        .with_metadata(
            TRACEPARENT,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        );
    message.content_type = "application/cloudevents+json".to_string();
    publisher.publish(message).await.expect("publish");

    let observed = Arc::new(Mutex::new(None));
    let o = observed.clone();
    let service = Arc::new(
        Service::new().routes(
            Routes::new()
                .with_dependencies(())
                .event(Box::leak(queue.clone().into_boxed_str()))
                .handle(move |ctx: &Context<()>| {
                    let m = ctx.message();
                    let recorded = Some((
                        m.id().map(str::to_string),
                        m.correlation_id().map(str::to_string),
                        m.traceparent().map(str::to_string),
                        m.payload().to_vec(),
                        m.content_type.clone(),
                    ));
                    *o.lock().unwrap() = recorded;
                    async move { Ok(json!({})) }
                }),
        ),
    );
    run_source(service, source, RunOptions::until_idle())
        .await
        .unwrap();

    let got = observed.lock().unwrap().clone().expect("handler ran");
    assert_eq!(got.0.as_deref(), Some("evt-1"));
    assert_eq!(got.1.as_deref(), Some("corr-9"));
    assert_eq!(
        got.2.as_deref(),
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    );
    assert_eq!(got.3, br#"{"k":"v"}"#.to_vec());
    assert_eq!(
        got.4, "application/cloudevents+json",
        "content_type round-trips"
    );
}

// ---- RabbitBus: send/listen (default exchange) + publish/subscribe (topic exchange) ----

/// `send` + `listen`: a durable command queue is shared by replicas of a group,
/// so AMQP round-robins — each command handled exactly once (point-to-point).
/// Each factory call opens a separate connection, so the replicas genuinely
/// compete over the wire.
#[tokio::test]
async fn bus_send_listen_is_point_to_point_across_a_group() {
    let Some(url) = amqp_url() else { return };
    let ns = unique("ns").to_lowercase();
    conformance::bus_send_listen_is_point_to_point_across_a_group(|group| {
        let url = url.clone();
        let ns = ns.clone();
        async move {
            RabbitBus::connect(&url)
                .group(group)
                .namespace(&ns)
                .await
                .expect("connect bus")
        }
    })
    .await;
}

/// `publish` + `subscribe`: each group binds its own queue to the topic exchange,
/// so every group receives every event (fan-out).
#[tokio::test]
async fn bus_publish_subscribe_fans_out_across_groups() {
    let Some(url) = amqp_url() else { return };
    let ns = unique("ns").to_lowercase();

    let producer = RabbitBus::connect(&url)
        .group("producer")
        .namespace(&ns)
        .await
        .expect("connect producer");

    // Bind both subscribers' queues BEFORE publishing — the topic exchange drops
    // events with no matching binding.
    let proj_rec = Arc::new(Mutex::new(Vec::new()));
    let audit_rec = Arc::new(Mutex::new(Vec::new()));
    let svc_proj = recording_for("order.initialized", MessageKind::Event, proj_rec.clone());
    let svc_audit = recording_for("order.initialized", MessageKind::Event, audit_rec.clone());
    let bus_proj = RabbitBus::connect(&url)
        .group("projections")
        .namespace(&ns)
        .await
        .unwrap();
    let bus_audit = RabbitBus::connect(&url)
        .group("audit")
        .namespace(&ns)
        .await
        .unwrap();
    bus_proj
        .ensure_subscription(svc_proj.as_ref())
        .await
        .expect("bind proj");
    bus_audit
        .ensure_subscription(svc_audit.as_ref())
        .await
        .expect("bind audit");

    let total = 4;
    for i in 0..total {
        producer
            .publish_message(
                Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
                    .with_id(format!("e{i}")),
            )
            .await
            .expect("publish event");
    }

    bus_proj
        .subscribe(svc_proj, RunOptions::until_idle())
        .await
        .expect("proj drains");
    bus_audit
        .subscribe(svc_audit, RunOptions::until_idle())
        .await
        .expect("audit drains");

    let expected: Vec<String> = (0..total).map(|i| format!("e{i}")).collect();
    let mut proj_ids = proj_rec.lock().unwrap().clone();
    let mut audit_ids = audit_rec.lock().unwrap().clone();
    proj_ids.sort();
    audit_ids.sort();
    assert_eq!(proj_ids, expected, "projections sees every event");
    assert_eq!(audit_ids, expected, "audit sees every event");
}

#[tokio::test]
async fn bus_subscribe_uses_named_service_as_consumer_group() {
    let Some(url) = amqp_url() else { return };
    let ns = unique("ns").to_lowercase();

    let producer = RabbitBus::connect(&url)
        .namespace(&ns)
        .await
        .expect("connect producer");
    let bus = RabbitBus::connect(&url)
        .namespace(&ns)
        .await
        .expect("connect subscriber");
    let rec = Arc::new(Mutex::new(Vec::new()));
    let service = named_recording_for(
        "order-projection",
        "order.initialized",
        MessageKind::Event,
        rec.clone(),
    );
    bus.ensure_subscription(service.as_ref())
        .await
        .expect("bind subscriber");

    for i in 0..3 {
        producer
            .publish_message(
                Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
                    .with_id(format!("e{i}")),
            )
            .await
            .expect("publish event");
    }

    bus.subscribe(service, RunOptions::until_idle())
        .await
        .expect("subscriber drains");

    let mut ids = rec.lock().unwrap().clone();
    ids.sort();
    assert_eq!(
        ids,
        vec!["e0".to_string(), "e1".to_string(), "e2".to_string()]
    );
}

// ---- failure paths: redelivery, dead-letter routing, undecodable payloads ----

/// Declare the durable `queue` and return a source over it. The connection is
/// returned so it outlives the source's channel and the test can publish raw
/// bytes on it.
async fn source_with_connection(url: &str, queue: &str) -> (Connection, RabbitSource) {
    let connection = Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("amqp connect");
    let channel = connection.create_channel().await.expect("amqp channel");
    channel
        .queue_declare(
            ShortString::from(queue),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("declare queue");
    (connection, RabbitSource::new(channel, queue))
}

#[tokio::test]
async fn retryable_failure_is_redelivered_then_succeeds() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("delivery.retry");

    let source = RabbitSource::connect(&url, &queue)
        .await
        .expect("connect source");
    let publisher = RabbitPublisher::connect(&url)
        .await
        .expect("connect publisher");
    publisher
        .publish(Message::new(&queue, MessageKind::Event, b"{}".to_vec()).with_id("m1"))
        .await
        .expect("publish");

    let attempts = Arc::new(AtomicUsize::new(0));
    let seen = attempts.clone();
    let handlers = Arc::new(Handlers::new().on_event(queue.clone(), move |_: &Message| {
        let seen = seen.clone();
        async move {
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(TransportError::retryable("transient"))
            } else {
                Ok(())
            }
        }
    }));
    run_source(handlers, source, RunOptions::until_idle())
        .await
        .expect("run drains after the requeued redelivery succeeds");

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "the nacked message was requeued and redelivered exactly once"
    );
}

#[tokio::test]
async fn permanent_failure_routes_to_dead_letter_destination() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("delivery.poison");
    let (_connection, source) = source_with_connection(&url, &queue).await;

    let publisher = RabbitPublisher::connect(&url)
        .await
        .expect("connect publisher");
    for id in ["poison", "ok"] {
        publisher
            .publish(Message::new(&queue, MessageKind::Event, b"{}".to_vec()).with_id(id))
            .await
            .expect("publish");
    }

    let (dead, options) = storing_dead_letters();
    let rec = Arc::new(Mutex::new(Vec::new()));
    let seen = rec.clone();
    let handlers = Arc::new(
        Handlers::new().on_event(queue.clone(), move |message: &Message| {
            let id = message.id().unwrap_or_default().to_string();
            let seen = seen.clone();
            async move {
                if id == "poison" {
                    Err(TransportError::permanent("unprocessable"))
                } else {
                    seen.lock().unwrap().push(id);
                    Ok(())
                }
            }
        }),
    );
    run_source(handlers, source, options)
        .await
        .expect("run drains past the poison message");
    assert_eq!(
        rec.lock().unwrap().clone(),
        vec!["ok".to_string()],
        "subsequent messages still flow after the dead-letter"
    );

    let (id, _, reason) = only_dead_letter(&dead);
    assert_eq!(id.as_deref(), Some("poison"));
    assert!(reason.contains("unprocessable"), "{reason}");
}

#[tokio::test]
async fn undecodable_payload_dead_letters_without_blocking() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("delivery.garbage");
    let (connection, source) = source_with_connection(&url, &queue).await;

    // Raw garbage straight onto the queue: no properties, invalid JSON payload.
    let garbage: &[u8] = &[0xff, 0xfe, b'{'];
    let raw_channel = connection.create_channel().await.expect("raw channel");
    raw_channel
        .basic_publish(
            ShortString::from(""),
            ShortString::from(queue.as_str()),
            BasicPublishOptions::default(),
            garbage,
            BasicProperties::default(),
        )
        .await
        .expect("raw publish")
        .await
        .expect("raw publish resolves");

    let publisher = RabbitPublisher::connect(&url)
        .await
        .expect("connect publisher");
    publisher
        .publish(Message::new(&queue, MessageKind::Event, b"{}".to_vec()).with_id("ok"))
        .await
        .expect("publish ok");

    // A payload-decoding handler permanently fails the garbage (the handler is
    // the decode point: `Service::dispatch_message` substitutes Null input for
    // non-JSON payloads rather than failing), so it dead-letters instead of
    // being requeued forever.
    let (dead, options) = storing_dead_letters();
    let rec = Arc::new(Mutex::new(Vec::new()));
    let seen = rec.clone();
    let handlers = Arc::new(
        Handlers::new().on_event(queue.clone(), move |message: &Message| {
            let id = message.id().unwrap_or_default().to_string();
            let decoded = serde_json::from_slice::<serde_json::Value>(message.payload()).is_ok();
            let seen = seen.clone();
            async move {
                if !decoded {
                    return Err(TransportError::permanent("undecodable payload"));
                }
                seen.lock().unwrap().push(id);
                Ok(())
            }
        }),
    );
    run_source(handlers, source, options)
        .await
        .expect("run drains: the undecodable payload is rejected, not redelivered forever");
    assert_eq!(
        rec.lock().unwrap().clone(),
        vec!["ok".to_string()],
        "the message behind the garbage is still handled"
    );

    let (_, payload, reason) = only_dead_letter(&dead);
    assert_eq!(
        payload, garbage,
        "the undecodable payload itself was stored"
    );
    assert!(reason.contains("undecodable payload"), "{reason}");
}
