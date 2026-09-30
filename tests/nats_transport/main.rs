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

#[derive(serde::Serialize, distributed::DomainEvent)]
#[domain_event(name = "ledger.external_balance", version = 1)]
struct ExternalBalance {
    value: u64,
}

fn external_balance(position: u64, value: u64) -> distributed::DomainEventOccurrence {
    distributed::DomainEventOccurrence::capture_external(
        distributed::ExternalEventSource {
            producer: "ledger-webhook".into(),
            stream: "account-one".into(),
            position,
            key: "balance".into(),
        },
        std::time::UNIX_EPOCH,
        Default::default(),
        &ExternalBalance { value },
    )
    .unwrap()
}

#[tokio::test]
async fn external_identity_content_conflicts_and_retained_archive_survive_broker_dedup() {
    use distributed::bus::{Bus, MessageSource, ReceivedMessage};
    let Some(url) = nats_url() else { return };
    let namespace = unique("external_archive");
    let bus = NatsBus::connect(&url).namespace(&namespace).await.unwrap();
    let stream = bus.ensure_stream().await.unwrap();
    let stream_name = stream.cached_info().config.name.clone();
    let mut source = NatsJetStreamSource::connect(
        &url,
        &stream_name,
        vec![format!("{namespace}.>")],
        &unique("external_consumer"),
    )
    .await
    .unwrap()
    .with_strip_prefix(format!("{namespace}.evt."));
    let first = external_balance(1, 10);
    let altered = external_balance(1, 11);
    let next = external_balance(2, 10);
    assert_eq!(first.id(), altered.id());
    for event in [&first, &first, &altered, &next] {
        bus.publish_message(
            distributed::OutboxMessage::from_domain_event_occurrence(event)
                .unwrap()
                .into(),
        )
        .await
        .unwrap();
    }
    // Identical retry deduplicates, altered bytes with the same logical source
    // identity must remain visible to the permanent projection conflict fence.
    for expected in [&first, &altered, &next] {
        let received = source.recv().await.unwrap().unwrap();
        assert_eq!(received.message().id(), Some(expected.id()));
        assert_eq!(
            received.message().payload(),
            expected.canonical_bytes().unwrap()
        );
        received.ack().await.unwrap();
    }
    assert!(source.recv().await.unwrap().is_none());
    let archive = bus.retained_domain_events().await.unwrap();
    assert_eq!(archive.len(), 3);
    assert_eq!(bus.retained_domain_events().await.unwrap(), archive);
    let js = async_nats::jetstream::new(async_nats::connect(&url).await.unwrap());
    js.delete_stream(&stream_name).await.unwrap();
}

#[tokio::test]
async fn archive_and_live_decode_reject_the_same_poisoned_identity_and_kind_headers() {
    use distributed::bus::MessageSource;
    use sha2::{Digest, Sha256};
    let Some(url) = nats_url() else { return };
    let js = async_nats::jetstream::new(async_nats::connect(&url).await.unwrap());
    for case in [
        "duplicate",
        "wrong-kind",
        "missing-kind",
        "lowercase-aggregate",
    ] {
        let namespace = unique("archive_poison");
        let bus = NatsBus::connect(&url).namespace(&namespace).await.unwrap();
        let stream = bus.ensure_stream().await.unwrap();
        let stream_name = stream.cached_info().config.name.clone();
        let mut source = NatsJetStreamSource::connect(
            &url,
            &stream_name,
            vec![format!("{namespace}.>")],
            &unique("consumer"),
        )
        .await
        .unwrap()
        .with_strip_prefix(format!("{namespace}.evt."));
        let mut event = external_balance(1, 10);
        if case == "lowercase-aggregate" {
            let mut entity = distributed::Entity::with_id("account");
            entity.digest("fixture", &()).unwrap();
            entity
                .capture_domain_event("ledger", &ExternalBalance { value: 10 })
                .unwrap();
            event = entity.pending_domain_events()[0].clone();
        }
        let bytes = event.canonical_bytes().unwrap();
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(
            "x-sourced-payload-codec",
            "distributed.domain-event-occurrence+json",
        );
        if case != "missing-kind" {
            headers.insert(
                "X-Sourced-Kind",
                if case == "wrong-kind" {
                    "command"
                } else {
                    "event"
                },
            );
        }
        if event.external_source().is_some() {
            let mut hash = Sha256::new();
            hash.update(b"distributed.nats.external-occurrence.v1\0");
            hash.update(&bytes);
            headers.insert(
                "Nats-Msg-Id",
                format!("external:sha256:{:x}", hash.finalize()),
            );
            headers.insert("X-Distributed-Occurrence-Id", event.id());
            if case == "duplicate" {
                headers.append("x-distributed-occurrence-id", "forged-extra");
            }
        } else {
            headers.insert("Nats-Msg-Id", event.id());
            headers.insert("x-distributed-occurrence-id", "forged-logical");
        }
        js.publish_with_headers(
            format!("{namespace}.evt.{}", event.descriptor().name),
            headers,
            bytes.into(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
        assert!(
            bus.retained_domain_events().await.is_err(),
            "archive accepted {case}"
        );
        assert!(source.recv().await.is_err(), "live accepted {case}");
        js.delete_stream(&stream_name).await.unwrap();
    }
}

#[tokio::test]
async fn malformed_external_headers_fail_closed_without_ack_or_false_progress() {
    use distributed::bus::MessageSource;
    let Some(url) = nats_url() else { return };
    let prefix = unique("external_poison");
    let subject = format!("{prefix}.ledger.external_balance");
    let stream_name = unique("EXTERNAL_POISON");
    let durable = unique("poison_consumer");
    let mut source =
        NatsJetStreamSource::connect(&url, &stream_name, vec![subject.clone()], &durable)
            .await
            .unwrap()
            .with_strip_prefix(format!("{prefix}."));
    let js = async_nats::jetstream::new(async_nats::connect(&url).await.unwrap());
    let event = external_balance(1, 10);
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", "forged");
    headers.insert("X-Sourced-Kind", "event");
    headers.append("X-Distributed-Occurrence-Id", event.id());
    headers.append("X-Distributed-Occurrence-Id", event.id());
    js.publish_with_headers(subject, headers, event.canonical_bytes().unwrap().into())
        .await
        .unwrap()
        .await
        .unwrap();
    let publisher = NatsPublisher::connect(&url)
        .await
        .unwrap()
        .with_subject_prefix(&prefix);
    publisher
        .publish(
            distributed::OutboxMessage::from_domain_event_occurrence(&external_balance(2, 20))
                .unwrap()
                .into(),
        )
        .await
        .unwrap();
    let error = match source.recv().await {
        Err(error) => error,
        _ => panic!("poison must fail closed"),
    };
    assert!(error.is_permanent());
    let mut stream = js.get_stream(&stream_name).await.unwrap();
    let mut consumer: async_nats::jetstream::consumer::Consumer<
        async_nats::jetstream::consumer::pull::Config,
    > = stream.get_consumer(&durable).await.unwrap();
    let info = consumer.info().await.unwrap();
    assert_eq!(info.ack_floor.stream_sequence, 0);
    assert_eq!(info.num_ack_pending, 1);
    assert_eq!(
        info.num_pending, 1,
        "later input was not dispatched past poison"
    );
    assert_eq!(stream.info().await.unwrap().state.messages, 2);
    js.delete_stream(&stream_name).await.unwrap();
}

#[tokio::test]
async fn derived_facts_round_trip_after_interrupted_publish_prefix() {
    let Some(url) = nats_url() else { return };
    #[derive(serde::Serialize, distributed::DomainEvent)]
    #[domain_event(name = "document.indexed", version = 1)]
    struct Indexed {
        document_id: String,
        words: u64,
    }
    let mut entity = distributed::Entity::with_id("document-1");
    entity.set_causation_id("0190a000-0000-7000-8000-000000000094");
    entity.digest("document.uploaded", &()).unwrap();
    entity
        .capture_domain_event(
            "document",
            &Indexed {
                document_id: "one".into(),
                words: 0,
            },
        )
        .unwrap();
    let parent = entity.pending_domain_events()[0].clone();
    let outputs = || {
        ["one", "two"].map(|key| {
            parent
                .derive(
                    "indexer",
                    key,
                    &Indexed {
                        document_id: key.into(),
                        words: 10,
                    },
                )
                .unwrap()
        })
    };
    let initial = outputs();
    let retry = outputs();
    assert_eq!(initial, retry);
    let subject = unique("derived.document.indexed");
    let source = NatsJetStreamSource::connect(
        &url,
        &unique("STREAM"),
        vec![subject.clone()],
        &unique("consumer"),
    )
    .await
    .unwrap()
    .with_fetch_timeout(Duration::from_millis(800));
    let publisher = NatsPublisher::connect(&url).await.unwrap();
    // Simulate stopping after one accepted output. A new attempt regenerates
    // both facts; JetStream receives the repeated stable message ID.
    for output in [&initial[0], &retry[0], &retry[1]] {
        publisher
            .publish(
                Message::new(
                    &subject,
                    MessageKind::Event,
                    output.canonical_bytes().unwrap(),
                )
                .with_id(output.id())
                .with_metadata(
                    distributed::trace_context::CAUSATION_ID,
                    output.causation_id().unwrap(),
                ),
            )
            .await
            .unwrap();
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    let service = Arc::new(
        Service::new().routes(
            Routes::new()
                .with_dependencies(())
                .event(Box::leak(subject.into_boxed_str()))
                .handle(move |ctx: &Context<()>| {
                    let decoded = distributed::DomainEventOccurrence::from_canonical_bytes(
                        ctx.message().payload(),
                    )
                    .unwrap();
                    assert_eq!(ctx.message().id(), Some(decoded.id()));
                    assert_eq!(ctx.message().causation_id(), decoded.causation_id());
                    captured.lock().unwrap().push(decoded);
                    async { Ok(json!({})) }
                }),
        ),
    );
    run_source(service, source, RunOptions::idempotent())
        .await
        .unwrap();
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2, "broker deduplicates the accepted prefix");
    assert_eq!(seen[0], initial[0]);
    assert_eq!(seen[1], initial[1]);
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

// ---- latency: idle delivery, retry backoff, delivery lanes ----
// See docs/consumer-delivery-lanes.md.

/// A long-lived consumer that has gone through several empty fetches must
/// still hand a newly published message to its handler immediately; it must
/// not sleep out an idle interval or a fetch expiry first.
#[tokio::test]
async fn message_published_after_empty_fetches_is_delivered_promptly() {
    let Some(url) = nats_url() else { return };
    let subject = unique("latency.idle");
    let source = NatsJetStreamSource::connect(
        &url,
        &unique("STREAM"),
        vec![subject.clone()],
        &unique("consumer"),
    )
    .await
    .expect("connect source")
    .with_fetch_timeout(Duration::from_millis(500))
    .with_idle_poll(Duration::from_millis(25));
    let handled = Arc::new(Mutex::new(None::<std::time::Instant>));
    let record = handled.clone();
    let router = Arc::new(Handlers::new().on_event(&subject, move |_: &Message| {
        record
            .lock()
            .unwrap()
            .get_or_insert_with(std::time::Instant::now);
        async { Ok(()) }
    }));
    let consumer = tokio::spawn(run_source(router, source, RunOptions::idempotent()));
    // Several empty 500 ms fetches expire first.
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    let publisher = NatsPublisher::connect(&url).await.expect("publisher");
    let published = std::time::Instant::now();
    publisher
        .publish(Message::new(&subject, MessageKind::Event, b"{}".to_vec()).with_id("late"))
        .await
        .expect("publish");
    let latency = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(at) = *handled.lock().unwrap() {
                return at.duration_since(published);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("idle consumer delivered the message");
    consumer.abort();
    assert!(
        latency < Duration::from_millis(250),
        "idle delivery took {latency:?}"
    );
}

/// A message that keeps failing retryably is redelivered with growing
/// delays instead of a hot loop, and a newer message is not delayed by it.
#[tokio::test]
async fn retryable_nack_backs_off_without_starving_newer_messages() {
    let Some(url) = nats_url() else { return };
    let subject = unique("latency.retry");
    let source = NatsJetStreamSource::connect(
        &url,
        &unique("STREAM"),
        vec![subject.clone()],
        &unique("consumer"),
    )
    .await
    .expect("connect source")
    .with_fetch_timeout(Duration::from_millis(500))
    .with_idle_poll(Duration::from_millis(25));
    let poison_attempts = Arc::new(AtomicUsize::new(0));
    let fresh_at = Arc::new(Mutex::new(None::<std::time::Instant>));
    let (attempts, fresh) = (poison_attempts.clone(), fresh_at.clone());
    let router = Arc::new(
        Handlers::new().on_event(&subject, move |message: &Message| {
            let poison = message.id() == Some("poison");
            if poison {
                attempts.fetch_add(1, Ordering::SeqCst);
            } else {
                fresh
                    .lock()
                    .unwrap()
                    .get_or_insert_with(std::time::Instant::now);
            }
            async move {
                if poison {
                    Err(TransportError::retryable("rejected until repaired"))
                } else {
                    Ok(())
                }
            }
        }),
    );
    let consumer = tokio::spawn(run_source(router, source, RunOptions::idempotent()));
    let publisher = NatsPublisher::connect(&url).await.expect("publisher");
    publisher
        .publish(Message::new(&subject, MessageKind::Event, b"{}".to_vec()).with_id("poison"))
        .await
        .expect("publish poison");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let published = std::time::Instant::now();
    publisher
        .publish(Message::new(&subject, MessageKind::Event, b"{}".to_vec()).with_id("fresh"))
        .await
        .expect("publish fresh");
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    consumer.abort();
    let fresh_latency = fresh_at
        .lock()
        .unwrap()
        .expect("fresh message handled")
        .duration_since(published);
    let attempts = poison_attempts.load(Ordering::SeqCst);
    assert!(
        fresh_latency < Duration::from_millis(250),
        "fresh message waited {fresh_latency:?}"
    );
    // 50 ms doubling over ~1.5 s allows at most ~6 attempts; an immediate
    // NAK loop redelivers hundreds of times in the same window.
    assert!(
        (2..=8).contains(&attempts),
        "poison retried {attempts} times: retries must continue but back off"
    );
}

/// With the process policy in its own lane, a slow projection route for an
/// earlier message does not delay the policy for a later message, and every
/// delivery is still acknowledged exactly once after all of its lanes ran.
#[tokio::test]
async fn slow_default_lane_does_not_delay_process_lane_over_jetstream() {
    let Some(url) = nats_url() else { return };
    let namespace = unique("lanes").to_lowercase();
    let bus = nats_bus(&url, &namespace, "lanes")
        .await
        .with_idle_poll(Duration::from_millis(25));
    let started = std::time::Instant::now();
    let projection_done = Arc::new(Mutex::new(Vec::<(String, Duration)>::new()));
    let policy_done = Arc::new(Mutex::new(Vec::<(String, Duration)>::new()));
    let (projected, policed) = (projection_done.clone(), policy_done.clone());
    let service = Service::new()
        .named("lanes")
        .routes(
            Routes::new()
                .with_dependencies(())
                .event("fact.recorded")
                .handle(move |ctx: &Context<()>| {
                    let id = ctx.message().id().unwrap_or_default().to_owned();
                    let projected = projected.clone();
                    async move {
                        if id == "m1" {
                            // A slow projection / external effect.
                            tokio::time::sleep(Duration::from_millis(1_500)).await;
                        }
                        projected.lock().unwrap().push((id, started.elapsed()));
                        Ok(json!({}))
                    }
                }),
        )
        .lane(
            "process",
            Routes::new()
                .with_dependencies(())
                .event("fact.recorded")
                .handle(move |ctx: &Context<()>| {
                    let id = ctx.message().id().unwrap_or_default().to_owned();
                    let policed = policed.clone();
                    async move {
                        policed.lock().unwrap().push((id, started.elapsed()));
                        Ok(json!({}))
                    }
                }),
        )
        .with_bus(bus.clone());
    let consumer = tokio::spawn(service.run(RunOptions::idempotent()));
    tokio::time::sleep(Duration::from_millis(700)).await;
    use distributed::bus::Bus;
    for id in ["m1", "m2"] {
        bus.publish_message(
            Message::new("fact.recorded", MessageKind::Event, b"{}".to_vec()).with_id(id),
        )
        .await
        .expect("publish");
    }
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    consumer.abort();
    let policed = policy_done.lock().unwrap().clone();
    let projected = projection_done.lock().unwrap().clone();
    assert_eq!(
        policed
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<Vec<_>>(),
        ["m1", "m2"],
        "process lane keeps delivery order"
    );
    assert_eq!(
        projected
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<Vec<_>>(),
        ["m1", "m2"],
        "default lane keeps delivery order"
    );
    let m2_policy = policed[1].1;
    let m1_projection = projected[0].1;
    assert!(
        m2_policy + Duration::from_millis(1_000) < m1_projection,
        "process lane waited for the slow lane: m2 policy at {m2_policy:?}, m1 projection at {m1_projection:?}"
    );
    // Both deliveries were acknowledged: nothing pending or awaiting ack.
    let stream = bus.ensure_stream().await.expect("stream");
    let mut durable = stream
        .get_consumer::<async_nats::jetstream::consumer::pull::Config>("lanes_evt")
        .await
        .expect("durable exists");
    let info = durable.info().await.expect("consumer info");
    assert_eq!(info.num_ack_pending, 0, "every delivery was settled");
    assert_eq!(info.num_pending, 0);
    assert_eq!(info.num_redelivered, 0, "no delivery ran twice");
}
