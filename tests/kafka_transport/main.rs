//! Kafka transport adapter integration tests.
//!
//! Publishes via `KafkaPublisher` (acks=all) and consumes via `KafkaSource`
//! (consumer group, offset commit on ack) against a broker. Skips when
//! `KAFKA_BROKERS` is unset.
#![cfg(feature = "kafka")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed::bus::{
    run_source, Bus, BusConsumer, Handlers, KafkaBus, KafkaPublisher, KafkaSource,
    MessagePublisher, RunOptions, TransportError,
};
use distributed::microsvc::{Context, Message, MessageKind, Routes, Service};
use distributed::TRACEPARENT;
use serde_json::json;

// Shared broker-test helpers (recording_for, bus scenarios). `unique` mixes
// wall-clock nanos into every name, so a fresh topic/group never reads stale
// messages from a previous run's same-named topic (Kafka persists topics).
#[path = "../transport_conformance/mod.rs"]
mod conformance;
use conformance::{recording_for, unique};
#[path = "../support/env.rs"]
mod env_support;

fn brokers() -> Option<String> {
    env_support::require_service("KAFKA_BROKERS", "kafka transport test")
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
async fn publish_then_consume_round_trips_through_kafka() {
    let Some(brokers) = brokers() else { return };
    let topic = unique("order.initialized");
    let group = unique("group");

    // Produce first so the topic auto-creates; then a fresh group reads from
    // earliest.
    let publisher = KafkaPublisher::connect(&brokers).await.expect("producer");
    for i in 0..3 {
        let message =
            Message::new(&topic, MessageKind::Event, b"{}".to_vec()).with_id(format!("m{i}"));
        publisher.publish(message).await.expect("publish");
    }

    let source = KafkaSource::connect(&brokers, &group, &[&topic])
        .await
        .expect("consumer")
        .with_fetch_timeout(Duration::from_secs(10));

    let handled = Arc::new(Mutex::new(Vec::<String>::new()));
    let h = handled.clone();
    let service = Arc::new(
        Service::new().routes(
            Routes::new()
                .with_dependencies(())
                .event(Box::leak(topic.clone().into_boxed_str()))
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
        .expect("run_source drains the topic");

    let mut ids = handled.lock().unwrap().clone();
    ids.sort();
    assert_eq!(
        ids,
        vec!["m0".to_string(), "m1".to_string(), "m2".to_string()]
    );
}

#[tokio::test]
async fn message_id_and_metadata_survive_the_round_trip() {
    let Some(brokers) = brokers() else { return };
    let topic = unique("order.initialized");
    let group = unique("group");

    let publisher = KafkaPublisher::connect(&brokers).await.expect("producer");
    publisher
        .publish(
            Message::new(&topic, MessageKind::Event, br#"{"k":"v"}"#.to_vec())
                .with_id("evt-1")
                .with_metadata("correlation_id", "corr-9")
                .with_metadata(
                    TRACEPARENT,
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                ),
        )
        .await
        .expect("publish");

    let source = KafkaSource::connect(&brokers, &group, &[&topic])
        .await
        .expect("consumer")
        .with_fetch_timeout(Duration::from_secs(10));

    let observed = Arc::new(Mutex::new(None));
    let o = observed.clone();
    let service = Arc::new(
        Service::new().routes(
            Routes::new()
                .with_dependencies(())
                .event(Box::leak(topic.clone().into_boxed_str()))
                .handle(move |ctx: &Context<()>| {
                    let m = ctx.message();
                    let recorded = Some((
                        m.id().map(str::to_string),
                        m.correlation_id().map(str::to_string),
                        m.traceparent().map(str::to_string),
                        m.payload().to_vec(),
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
}

// ---- KafkaBus: shared group = listen (point-to-point); group-per-service = subscribe (fan-out) ----

/// `send` + `listen`: a shared consumer group consumes each command once for the
/// group as a whole. Proven deterministically: a first listener drains every
/// command; a second listener in the **same group** then reads nothing, because
/// the group's offset is already committed past the end — point-to-point.
#[tokio::test]
async fn bus_listen_shared_group_consumes_each_command_once() {
    let Some(brokers) = brokers() else { return };
    let ns = unique("ns");

    let producer = KafkaBus::connect(&brokers)
        .group("orders")
        .namespace(&ns)
        .await
        .expect("connect producer");
    let total = 5;
    for i in 0..total {
        producer
            .send_message(
                Message::new("order.initialize", MessageKind::Command, b"{}".to_vec())
                    .with_id(format!("c{i}")),
            )
            .await
            .expect("send command");
    }

    // First member of group "orders" drains every command.
    let first = Arc::new(Mutex::new(Vec::new()));
    KafkaBus::connect(&brokers)
        .group("orders")
        .namespace(&ns)
        .await
        .unwrap()
        .with_fetch_timeout(Duration::from_secs(10))
        .listen(
            recording_for("order.initialize", MessageKind::Command, first.clone()),
            RunOptions::until_idle(),
        )
        .await
        .expect("first listener drains");
    let mut ids = first.lock().unwrap().clone();
    ids.sort();
    let expected: Vec<String> = (0..total).map(|i| format!("c{i}")).collect();
    assert_eq!(ids, expected, "the group consumes every command");

    // A second member of the SAME group sees nothing — the group already consumed
    // and committed past these records (point-to-point, not fan-out).
    let second = Arc::new(Mutex::new(Vec::new()));
    KafkaBus::connect(&brokers)
        .group("orders")
        .namespace(&ns)
        .await
        .unwrap()
        .with_fetch_timeout(Duration::from_secs(6))
        .listen(
            recording_for("order.initialize", MessageKind::Command, second.clone()),
            RunOptions::until_idle(),
        )
        .await
        .expect("second listener drains");
    assert!(
        second.lock().unwrap().is_empty(),
        "a second consumer in the same group re-consumes nothing"
    );
}

/// Build a namespaced `KafkaBus` for `group` (empty `group` = no group).
async fn kafka_bus(brokers: &str, ns: &str, group: &str) -> KafkaBus {
    let builder = KafkaBus::connect(brokers).namespace(ns);
    let builder = if group.is_empty() {
        builder
    } else {
        builder.group(group)
    };
    builder
        .await
        .expect("connect bus")
        .with_fetch_timeout(Duration::from_secs(10))
}

/// `publish` + `subscribe`: each `group` is a distinct Kafka consumer group, and
/// Kafka delivers every record to every group — so each group reads every event
/// (fan-out). A fresh group reads from earliest.
#[tokio::test]
async fn bus_subscribe_fans_out_across_groups() {
    let Some(brokers) = brokers() else { return };
    let ns = unique("ns");
    conformance::bus_publish_subscribe_fans_out_across_groups(|group| {
        kafka_bus(&brokers, &ns, group)
    })
    .await;
}

#[tokio::test]
async fn bus_subscribe_uses_named_service_as_consumer_group() {
    let Some(brokers) = brokers() else { return };
    let ns = unique("ns");
    conformance::bus_subscribe_uses_named_service_as_consumer_group(|| {
        kafka_bus(&brokers, &ns, "")
    })
    .await;
}

// ---- failure paths: seek-back redelivery, offset-commit skip, undecodable payloads ----

async fn failure_source(brokers: &str, group: &str, topic: &str, fetch: Duration) -> KafkaSource {
    KafkaSource::connect(brokers, group, &[topic])
        .await
        .expect("consumer")
        .with_fetch_timeout(fetch)
}

#[tokio::test]
async fn retryable_failure_is_redelivered_then_succeeds() {
    let Some(brokers) = brokers() else { return };
    let topic = unique("delivery.retry");
    let group = unique("group");

    let publisher = KafkaPublisher::connect(&brokers).await.expect("producer");
    publisher
        .publish(Message::new(&topic, MessageKind::Event, b"{}".to_vec()).with_id("m1"))
        .await
        .expect("publish");

    let source = failure_source(&brokers, &group, &topic, Duration::from_secs(10)).await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let seen = attempts.clone();
    let handlers = Arc::new(Handlers::new().on_event(topic.clone(), move |_: &Message| {
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
        .expect("run drains after the seek-back redelivery succeeds");

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "the nacked record was re-read after the seek and then committed"
    );
}

/// A poison record is stored as a dead letter, is not redelivered to the group,
/// and does not hold up later records.
#[tokio::test]
async fn permanent_failure_routes_to_dead_letter_destination() {
    let Some(brokers) = brokers() else { return };
    let topic = unique("delivery.poison");
    let group = unique("group");

    let publisher = KafkaPublisher::connect(&brokers).await.expect("producer");
    for id in ["poison", "ok"] {
        publisher
            .publish(Message::new(&topic, MessageKind::Event, b"{}".to_vec()).with_id(id))
            .await
            .expect("publish");
    }

    let source = failure_source(&brokers, &group, &topic, Duration::from_secs(10)).await;
    let (dead, options) = storing_dead_letters();
    let rec = Arc::new(Mutex::new(Vec::new()));
    let seen = rec.clone();
    let handlers = Arc::new(
        Handlers::new().on_event(topic.clone(), move |message: &Message| {
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
        .expect("run drains past the poison record");
    assert_eq!(
        rec.lock().unwrap().clone(),
        vec!["ok".to_string()],
        "subsequent records still flow after the dead letter"
    );
    let (id, _, reason) = only_dead_letter(&dead);
    assert_eq!(id.as_deref(), Some("poison"));
    assert!(reason.contains("unprocessable"), "{reason}");

    // The dead letter was acknowledged, committing past the poison record: a fresh consumer in the
    // SAME group re-reads nothing.
    let source = failure_source(&brokers, &group, &topic, Duration::from_secs(6)).await;
    let redelivered = Arc::new(Mutex::new(Vec::new()));
    run_source(
        recording_for(&topic, MessageKind::Event, redelivered.clone()),
        source,
        RunOptions::until_idle(),
    )
    .await
    .expect("redelivery-check run drains");
    assert!(
        redelivered.lock().unwrap().is_empty(),
        "a dead-lettered record must not be redelivered to the group"
    );
}

#[tokio::test]
async fn undecodable_payload_dead_letters_without_blocking() {
    let Some(brokers) = brokers() else { return };
    let topic = unique("delivery.garbage");
    let group = unique("group");

    let publisher = KafkaPublisher::connect(&brokers).await.expect("producer");
    // Garbage: invalid JSON payload.
    publisher
        .publish(
            Message::new(&topic, MessageKind::Event, vec![0xff, 0xfe, b'{']).with_id("garbage"),
        )
        .await
        .expect("publish garbage");
    publisher
        .publish(Message::new(&topic, MessageKind::Event, b"{}".to_vec()).with_id("ok"))
        .await
        .expect("publish ok");

    // A payload-decoding handler permanently fails the garbage (the handler is
    // the decode point: `Service::dispatch_message` substitutes Null input for
    // non-JSON payloads rather than failing), so the record is skipped via
    // offset commit instead of blocking the partition with endless seek-backs.
    let source = failure_source(&brokers, &group, &topic, Duration::from_secs(10)).await;
    let (dead, options) = storing_dead_letters();
    let rec = Arc::new(Mutex::new(Vec::new()));
    let seen = rec.clone();
    let handlers = Arc::new(
        Handlers::new().on_event(topic.clone(), move |message: &Message| {
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
        .expect("run drains: the undecodable payload is skipped, not re-read forever");
    assert_eq!(
        rec.lock().unwrap().clone(),
        vec!["ok".to_string()],
        "the record behind the garbage is still handled"
    );
    let (id, payload, reason) = only_dead_letter(&dead);
    assert_eq!(id.as_deref(), Some("garbage"));
    assert_eq!(payload, vec![0xff, 0xfe, b'{']);
    assert!(reason.contains("undecodable payload"), "{reason}");
}
