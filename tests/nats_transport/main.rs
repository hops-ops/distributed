//! NATS JetStream transport adapter integration tests.
//!
//! Publishes via `NatsPublisher` and consumes via `NatsJetStreamSource` against a
//! JetStream-enabled NATS server. Skips when `NATS_URL` is unset.
#![cfg(feature = "nats")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed::bus::{
    run_source, Handlers, MessagePublisher, NatsBus, NatsJetStreamSource, NatsPublisher,
    RunOptions, TransportError,
};
use distributed::microsvc::{Context, Message, MessageKind, Routes, Service};
use distributed::TRACEPARENT;
use futures::StreamExt;
use serde_json::json;

// Shared broker-test helpers (bus scenarios, unique, ...).
#[path = "../transport_conformance/mod.rs"]
mod conformance;
use conformance::{recording_for, unique};
#[path = "../support/env.rs"]
mod env_support;

fn nats_url() -> Option<String> {
    env_support::broker_env("NATS_URL", "nats transport test")
}

#[tokio::test]
async fn publish_then_consume_round_trips_through_jetstream() {
    let Some(url) = nats_url() else { return };
    let subject = unique("order.initialized");
    let stream = unique("STREAM");
    let durable = unique("consumer");

    // Create the stream + durable consumer first so the stream exists before we
    // publish (JetStream publish requires a stream bound to the subject).
    let source = NatsJetStreamSource::connect(&url, &stream, vec![subject.clone()], &durable)
        .await
        .expect("connect source")
        .with_fetch_timeout(Duration::from_millis(800));

    // Publish three events.
    let publisher = NatsPublisher::connect(&url)
        .await
        .expect("connect publisher");
    for i in 0..3 {
        let message =
            Message::new(&subject, MessageKind::Event, b"{}".to_vec()).with_id(format!("m{i}"));
        publisher.publish(message).await.expect("publish");
    }

    // Consume via the shared runner.
    let handled = Arc::new(Mutex::new(Vec::<String>::new()));
    let h = handled.clone();
    let subject_for_handler = subject.clone();
    let service = Arc::new(
        Service::new().routes(
            Routes::new()
                .with_dependencies(())
                .event(Box::leak(subject.clone().into_boxed_str()))
                .handle(move |ctx: &Context<()>| {
                    assert_eq!(ctx.message().name(), subject_for_handler);
                    h.lock()
                        .unwrap()
                        .push(ctx.message().id().unwrap_or_default().to_string());
                    async move { Ok(json!({})) }
                }),
        ),
    );

    run_source(service, source, RunOptions::idempotent())
        .await
        .expect("run_source drains the stream");

    let mut ids = handled.lock().unwrap().clone();
    ids.sort();
    assert_eq!(
        ids,
        vec!["m0".to_string(), "m1".to_string(), "m2".to_string()]
    );
}

#[tokio::test]
async fn message_id_and_metadata_survive_the_round_trip() {
    let Some(url) = nats_url() else { return };
    let subject = unique("order.initialized");
    let stream = unique("STREAM");
    let durable = unique("consumer");

    let source = NatsJetStreamSource::connect(&url, &stream, vec![subject.clone()], &durable)
        .await
        .expect("connect source")
        .with_fetch_timeout(Duration::from_millis(800));

    let publisher = NatsPublisher::connect(&url)
        .await
        .expect("connect publisher");
    let message = Message::new(&subject, MessageKind::Event, br#"{"k":"v"}"#.to_vec())
        .with_id("evt-1")
        .with_metadata("correlation_id", "corr-9")
        .with_metadata(
            TRACEPARENT,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        );
    let mut message = message;
    message.content_type = "application/vnd.example+binary".into();
    publisher.publish(message).await.expect("publish");

    let observed = Arc::new(Mutex::new(None));
    let o = observed.clone();
    let service = Arc::new(
        Service::new().routes(
            Routes::new()
                .with_dependencies(())
                .event(Box::leak(subject.clone().into_boxed_str()))
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
    run_source(service, source, RunOptions::idempotent())
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
    assert_eq!(got.4, "application/vnd.example+binary");
}

/// Build a namespaced `NatsBus` for `group` (empty `group` = no group), with
/// the stream ensured so publishes have a destination.
async fn nats_bus(url: &str, namespace: &str, group: &str) -> NatsBus {
    let builder = NatsBus::connect(url).namespace(namespace);
    let builder = if group.is_empty() {
        builder
    } else {
        builder.group(group)
    };
    let bus = builder
        .await
        .expect("connect bus")
        .with_fetch_timeout(Duration::from_millis(600));
    bus.ensure_stream().await.expect("ensure stream");
    bus
}

/// `send` + `listen`: replicas sharing a `group` compete for the command — each
/// message is handled exactly once across the pool (point-to-point).
#[tokio::test]
async fn bus_send_listen_is_point_to_point_across_a_group() {
    let Some(url) = nats_url() else { return };
    let namespace = unique("ns").to_lowercase();
    conformance::bus_send_listen_is_point_to_point_across_a_group(|group| {
        nats_bus(&url, &namespace, group)
    })
    .await;
}

/// `publish` + `subscribe`: distinct `group`s each get their own durable on the
/// shared stream, so every group sees every event (fan-out).
#[tokio::test]
async fn bus_publish_subscribe_fans_out_across_groups() {
    let Some(url) = nats_url() else { return };
    let namespace = unique("ns").to_lowercase();
    conformance::bus_publish_subscribe_fans_out_across_groups(|group| {
        nats_bus(&url, &namespace, group)
    })
    .await;
}

#[tokio::test]
async fn bus_subscribe_uses_named_service_as_consumer_group() {
    let Some(url) = nats_url() else { return };
    let namespace = unique("ns").to_lowercase();
    conformance::bus_subscribe_uses_named_service_as_consumer_group(|| {
        nats_bus(&url, &namespace, "")
    })
    .await;
}

// ---- failure paths: redelivery, termination (dead-letter), undecodable payloads ----

/// Connect a fresh stream + durable pull source for `subject`.
async fn failure_source(
    url: &str,
    subject: &str,
    stream: &str,
    durable: &str,
) -> NatsJetStreamSource {
    NatsJetStreamSource::connect(url, stream, vec![subject.to_string()], durable)
        .await
        .expect("connect source")
        .with_fetch_timeout(Duration::from_millis(800))
}

/// A handler set for `subject` that decodes its payload as JSON, permanently
/// failing (→ dead-letter) on garbage and recording the id on success.
///
/// The handler is the decode point on purpose: `Service::dispatch_message`
/// falls back to a `Null` input when the payload is not JSON instead of
/// failing, so payload validation is the consumer's contract.
fn json_decoding_handlers(subject: &str, rec: Arc<Mutex<Vec<String>>>) -> Arc<Handlers> {
    Arc::new(Handlers::new().on_event(subject, move |message: &Message| {
        let id = message.id().unwrap_or_default().to_string();
        let decoded = serde_json::from_slice::<serde_json::Value>(message.payload()).is_ok();
        let rec = rec.clone();
        async move {
            if !decoded {
                return Err(TransportError::permanent("undecodable payload"));
            }
            rec.lock().unwrap().push(id);
            Ok(())
        }
    }))
}

/// A handler set for `subject` that fails retryably on the first attempt and
/// succeeds on the second, counting attempts.
fn fail_once_handlers(subject: &str, attempts: Arc<AtomicUsize>) -> Arc<Handlers> {
    Arc::new(Handlers::new().on_event(subject, move |_: &Message| {
        let attempts = attempts.clone();
        async move {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(TransportError::retryable("transient"))
            } else {
                Ok(())
            }
        }
    }))
}

#[tokio::test]
async fn retryable_failure_is_redelivered_then_succeeds() {
    let Some(url) = nats_url() else { return };
    let subject = unique("delivery.retry");
    let stream = unique("STREAM");
    let durable = unique("consumer");
    let source = failure_source(&url, &subject, &stream, &durable).await;

    let publisher = NatsPublisher::connect(&url)
        .await
        .expect("connect publisher");
    publisher
        .publish(Message::new(&subject, MessageKind::Event, b"{}".to_vec()).with_id("m1"))
        .await
        .expect("publish");

    let attempts = Arc::new(AtomicUsize::new(0));
    run_source(
        fail_once_handlers(&subject, attempts.clone()),
        source,
        RunOptions::idempotent(),
    )
    .await
    .expect("run drains after the Nak redelivery succeeds");

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "the nacked message was redelivered exactly once before the ack"
    );
}

#[tokio::test]
async fn permanent_failure_routes_to_dead_letter_destination() {
    let Some(url) = nats_url() else { return };
    let subject = unique("delivery.poison");
    let stream = unique("STREAM");
    let durable = unique("consumer");
    let source = failure_source(&url, &subject, &stream, &durable).await;

    // JetStream's parking destination for a terminated message is the
    // MSG_TERMINATED advisory subject — subscribe before terminating.
    let advisory_client = async_nats::connect(&url)
        .await
        .expect("connect advisory client");
    let mut advisories = advisory_client
        .subscribe(format!(
            "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.{stream}.{durable}"
        ))
        .await
        .expect("subscribe to terminated advisories");

    let publisher = NatsPublisher::connect(&url)
        .await
        .expect("connect publisher");
    for id in ["poison", "ok"] {
        publisher
            .publish(Message::new(&subject, MessageKind::Event, b"{}".to_vec()).with_id(id))
            .await
            .expect("publish");
    }

    let rec = Arc::new(Mutex::new(Vec::new()));
    let seen = rec.clone();
    let handlers = Arc::new(
        Handlers::new().on_event(subject.clone(), move |message: &Message| {
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
    run_source(handlers, source, RunOptions::idempotent())
        .await
        .expect("run drains past the poison message");
    assert_eq!(
        rec.lock().unwrap().clone(),
        vec!["ok".to_string()],
        "subsequent messages still flow after the dead-letter"
    );

    // The parking destination actually received the termination.
    let advisory = tokio::time::timeout(Duration::from_secs(5), advisories.next())
        .await
        .expect("a MSG_TERMINATED advisory should arrive")
        .expect("advisory subscription should stay open");
    assert!(
        advisory.subject.contains("MSG_TERMINATED"),
        "unexpected advisory subject: {}",
        advisory.subject
    );

    // Term stops redelivery: a fresh run over the same durable sees nothing.
    let source = failure_source(&url, &subject, &stream, &durable).await;
    let redelivered = Arc::new(Mutex::new(Vec::new()));
    run_source(
        recording_for(&subject, MessageKind::Event, redelivered.clone()),
        source,
        RunOptions::idempotent(),
    )
    .await
    .expect("redelivery-check run drains");
    assert!(
        redelivered.lock().unwrap().is_empty(),
        "a terminated message must not be redelivered"
    );
}

#[tokio::test]
async fn undecodable_payload_dead_letters_without_blocking() {
    let Some(url) = nats_url() else { return };
    let subject = unique("delivery.garbage");
    let stream = unique("STREAM");
    let durable = unique("consumer");
    let source = failure_source(&url, &subject, &stream, &durable).await;

    // Raw garbage straight onto the stream subject: no headers, invalid JSON.
    let raw = async_nats::connect(&url).await.expect("connect raw client");
    raw.publish(subject.clone(), vec![0xff, 0xfe, b'{'].into())
        .await
        .expect("raw publish");
    raw.flush().await.expect("flush raw publish");

    let publisher = NatsPublisher::connect(&url)
        .await
        .expect("connect publisher");
    publisher
        .publish(Message::new(&subject, MessageKind::Event, b"{}".to_vec()).with_id("ok"))
        .await
        .expect("publish ok");

    // The decoding handler fails permanently on the garbage, so it is
    // dead-lettered (Term) instead of blocking the subject with endless
    // redeliveries.
    let rec = Arc::new(Mutex::new(Vec::new()));
    run_source(
        json_decoding_handlers(&subject, rec.clone()),
        source,
        RunOptions::idempotent(),
    )
    .await
    .expect("run drains: the undecodable payload is terminated, not redelivered forever");
    assert_eq!(
        rec.lock().unwrap().clone(),
        vec!["ok".to_string()],
        "the message behind the garbage is still handled"
    );
}
