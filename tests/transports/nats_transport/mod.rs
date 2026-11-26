//! NATS JetStream transport adapter integration tests.
//!
//! Publishes via `NatsPublisher` and consumes via `NatsJetStreamSource` against a
//! JetStream-enabled NATS server. Skips when `NATS_URL` is unset.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed::bus::{
    run_source, Handlers, MessagePublisher, NatsBus, NatsJetStreamSource, NatsPublisher,
    RetryBackoff, RunOptions, TransportError,
};
use distributed::microsvc::{Context, Message, MessageKind, Routes, Service};
use serde_json::json;

// Shared broker-test helpers (bus scenarios, unique, ...).
use common::env as env_support;
use common::transport as conformance;
use conformance::unique;

fn nats_url() -> Option<String> {
    env_support::require_service("NATS_URL", "nats transport test")
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
            "x-distributed-payload-codec",
            "distributed.domain-event-occurrence+json",
        );
        if case != "missing-kind" {
            headers.insert(
                "x-distributed-kind",
                if case == "wrong-kind" {
                    "command"
                } else {
                    "event"
                },
            );
        }
        headers.insert("x-distributed-id", event.id());
        if event.external_source().is_some() {
            let mut hash = Sha256::new();
            hash.update(b"distributed.nats.external-occurrence.v1\0");
            hash.update(&bytes);
            headers.insert(
                "Nats-Msg-Id",
                format!("external:sha256:{:x}", hash.finalize()),
            );
            if case == "duplicate" {
                headers.append("X-Distributed-Id", "forged-extra");
            }
        } else {
            // An aggregate event whose broker dedup id is not its message id.
            headers.insert("Nats-Msg-Id", "forged-dedup");
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
    headers.insert("x-distributed-kind", "event");
    headers.append("X-Distributed-Id", event.id());
    headers.append("x-distributed-id", event.id());
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
    run_source(service, source, RunOptions::until_idle())
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

    run_source(service, source, RunOptions::until_idle())
        .await
        .expect("run_source drains the stream");

    let mut ids = handled.lock().unwrap().clone();
    ids.sort();
    assert_eq!(
        ids,
        vec!["m0".to_string(), "m1".to_string(), "m2".to_string()]
    );
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

/// One namespace (stream) per test.
struct Nats {
    url: String,
    namespace: String,
}

impl conformance::bus::BusHarness for Nats {
    type Bus = NatsBus;

    async fn start() -> Option<Self> {
        Some(Self {
            url: nats_url()?,
            namespace: unique("ns").to_lowercase(),
        })
    }

    async fn bus(&self, group: &str) -> NatsBus {
        nats_bus(&self.url, &self.namespace, group).await
    }
}

common::bus_conformance_tests!(Nats);

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
    // dead-lettered instead of blocking the subject with endless
    // redeliveries.
    let (dead, options) = storing_dead_letters();
    let rec = Arc::new(Mutex::new(Vec::new()));
    run_source(
        json_decoding_handlers(&subject, rec.clone()),
        source,
        options,
    )
    .await
    .expect("run drains: the undecodable payload is terminated, not redelivered forever");
    assert_eq!(
        rec.lock().unwrap().clone(),
        vec!["ok".to_string()],
        "the message behind the garbage is still handled"
    );
    let (_, payload, reason) = only_dead_letter(&dead);
    assert_eq!(
        payload,
        vec![0xff, 0xfe, b'{'],
        "the garbage itself was stored"
    );
    assert!(!reason.is_empty());
}

// ---- latency: idle delivery, retry backoff, delivery lanes ----
// See docs/consumer-delivery-lanes.md.

/// A long-lived consumer that has gone through several empty fetches must
/// still hand a newly published message to its handler immediately; it must
/// not sleep out an idle interval or a fetch expiry first.
///
/// The bound is the fetch expiry itself, not a latency budget: a consumer that
/// waited for its open fetch to expire, or idled between fetches, would take up
/// to one expiry, while a prompt one is a broker round trip.
#[tokio::test]
async fn message_published_after_empty_fetches_is_delivered_promptly() {
    let Some(url) = nats_url() else { return };
    const FETCH_EXPIRY: Duration = Duration::from_secs(2);
    let subject = unique("latency.idle");
    let source = NatsJetStreamSource::connect(
        &url,
        &unique("STREAM"),
        vec![subject.clone()],
        &unique("consumer"),
    )
    .await
    .expect("connect source")
    .with_fetch_timeout(FETCH_EXPIRY);
    let handled = Arc::new(Mutex::new(None::<std::time::Instant>));
    let record = handled.clone();
    let router = Arc::new(Handlers::new().on_event(&subject, move |_: &Message| {
        record
            .lock()
            .unwrap()
            .get_or_insert_with(std::time::Instant::now);
        async { Ok(()) }
    }));
    let consumer = tokio::spawn(run_source(router, source, RunOptions::default()));
    // Two empty fetches expire first; the message lands mid-way through a third.
    tokio::time::sleep(FETCH_EXPIRY * 2 + FETCH_EXPIRY / 4).await;
    let publisher = NatsPublisher::connect(&url).await.expect("publisher");
    let published = std::time::Instant::now();
    publisher
        .publish(Message::new(&subject, MessageKind::Event, b"{}".to_vec()).with_id("late"))
        .await
        .expect("publish");
    let latency = tokio::time::timeout(FETCH_EXPIRY * 5, async {
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
        latency < FETCH_EXPIRY,
        "idle delivery took {latency:?}, as long as a fetch expiry of {FETCH_EXPIRY:?}"
    );
}

/// A message that keeps failing retryably is redelivered after a delay
/// instead of in a hot loop, and a newer message is not held behind it.
///
/// Asserted by order, not by latency: with a 2 s first redelivery delay, the
/// fresh message published 300 ms after the poison must be handled before the
/// poison's second attempt. A hot NAK loop puts many poison attempts first; a
/// consumer that serialises behind the poison puts the fresh one after them.
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
    .with_fetch_timeout(Duration::from_millis(500));
    let handled = Arc::new(Mutex::new(Vec::<String>::new()));
    let log = handled.clone();
    let router = Arc::new(
        Handlers::new().on_event(&subject, move |message: &Message| {
            let id = message.id().unwrap_or_default().to_owned();
            let poison = id == "poison";
            log.lock().unwrap().push(id);
            async move {
                if poison {
                    Err(TransportError::retryable("rejected until repaired"))
                } else {
                    Ok(())
                }
            }
        }),
    );
    let mut options = RunOptions::default();
    options.backoff = RetryBackoff::new(Duration::from_secs(2), Duration::from_secs(2));
    let consumer = tokio::spawn(run_source(router, source, options));
    let publisher = NatsPublisher::connect(&url).await.expect("publisher");
    publisher
        .publish(Message::new(&subject, MessageKind::Event, b"{}".to_vec()).with_id("poison"))
        .await
        .expect("publish poison");
    tokio::time::sleep(Duration::from_millis(300)).await;
    publisher
        .publish(Message::new(&subject, MessageKind::Event, b"{}".to_vec()).with_id("fresh"))
        .await
        .expect("publish fresh");
    let order = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let order = handled.lock().unwrap().clone();
            if order.iter().filter(|id| *id == "poison").count() >= 2 {
                return order;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the poison message is retried");
    consumer.abort();
    assert_eq!(
        order[..3],
        ["poison", "fresh", "poison"],
        "the fresh message runs between the poison's first attempt and its delayed retry"
    );
}

/// With the process policy in its own lane, a slow projection route for an
/// earlier message does not delay the policy for a later message, and every
/// delivery is still acknowledged exactly once after all of its lanes ran.
///
/// The slow projection for `m1` waits until the policy for `m2` has run. If
/// the process lane waited for the default lane, neither would finish and the
/// deliveries would stay pending; no timing margin is involved.
#[tokio::test]
async fn slow_default_lane_does_not_delay_process_lane_over_jetstream() {
    let Some(url) = nats_url() else { return };
    let namespace = unique("lanes").to_lowercase();
    let bus = nats_bus(&url, &namespace, "lanes").await;
    let m2_policy_ran = Arc::new(tokio::sync::Notify::new());
    let projection_done = Arc::new(Mutex::new(Vec::<String>::new()));
    let policy_done = Arc::new(Mutex::new(Vec::<String>::new()));
    let (projected, policed) = (projection_done.clone(), policy_done.clone());
    let (wait_for_m2, signal_m2) = (m2_policy_ran.clone(), m2_policy_ran.clone());
    let service = Service::new()
        .named("lanes")
        .routes(
            Routes::new()
                .with_dependencies(())
                .event("fact.recorded")
                .handle(move |ctx: &Context<()>| {
                    let id = ctx.message().id().unwrap_or_default().to_owned();
                    let projected = projected.clone();
                    let wait_for_m2 = wait_for_m2.clone();
                    async move {
                        if id == "m1" {
                            // A slow projection / external effect.
                            wait_for_m2.notified().await;
                        }
                        projected.lock().unwrap().push(id);
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
                    let signal_m2 = signal_m2.clone();
                    async move {
                        if id == "m2" {
                            // `notify_one` stores a permit if m1 is not yet waiting.
                            signal_m2.notify_one();
                        }
                        policed.lock().unwrap().push(id);
                        Ok(json!({}))
                    }
                }),
        )
        .with_bus(bus.clone());
    let consumer = tokio::spawn(service.run(RunOptions::default()));
    tokio::time::sleep(Duration::from_millis(700)).await;
    use distributed::bus::Bus;
    for id in ["m1", "m2"] {
        bus.publish_message(
            Message::new("fact.recorded", MessageKind::Event, b"{}".to_vec()).with_id(id),
        )
        .await
        .expect("publish");
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        while projection_done.lock().unwrap().len() < 2 || policy_done.lock().unwrap().len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the process lane ran m2 while m1's projection waited for it");
    consumer.abort();
    let policed = policy_done.lock().unwrap().clone();
    let projected = projection_done.lock().unwrap().clone();
    assert_eq!(policed, ["m1", "m2"], "process lane keeps delivery order");
    assert_eq!(projected, ["m1", "m2"], "default lane keeps delivery order");
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
