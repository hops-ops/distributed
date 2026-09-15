//! Reusable async-transport conformance harness.
//!
//! Adapter-neutral fakes plus a contract suite that proves the shared transport
//! behaviour (source runner ack/nack/failure ordering, stable-id handling, and
//! the outbox publisher/dispatcher thresholds) before any real broker exists.
//!
//! Concrete adapters reuse the pieces here: a real *source* adapter can be
//! exercised against [`FakePublisher`], a real *publisher* adapter against
//! [`FakeSource`], and any [`OutboxStore`] against the dispatcher contract.
//! Other test targets include this module with
//! `#[path = "../transport_conformance/mod.rs"] mod conformance;`.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed::bus::{
    run_source, FailurePolicy, Handlers, MessagePublisher, MessageSource, ReceivedMessage,
    RunOptions, TransportError,
};
use distributed::microsvc::{Context, HandlerError, Message, MessageKind, Routes, Service};
use distributed::OutboxDispatcher;
use distributed::{
    CommitBatch, InMemoryOutboxStore, InMemoryRepository, OutboxMessage, OutboxMessageStatus,
    TransactionalCommit,
};
use serde_json::json;

/// One observable transport effect, recorded in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// A handler ran for the named message.
    Handled(String),
    Ack,
    Nack(String),
    DeadLetter(String),
    Park(String),
}

/// Ordered recorder shared by the service handlers and the fake transport.
#[derive(Default)]
pub struct Recorder {
    events: Mutex<Vec<Event>>,
}

impl Recorder {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub fn push(&self, event: Event) {
        self.events.lock().unwrap().push(event);
    }
    pub fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }
}

/// A received message that records how it was settled.
pub struct FakeReceived {
    message: Message,
    recorder: Arc<Recorder>,
    settle_ok: bool,
}

impl FakeReceived {
    fn settle(self, event: Event) -> Result<(), TransportError> {
        self.recorder.push(event);
        if self.settle_ok {
            Ok(())
        } else {
            Err(TransportError::retryable("settle failed"))
        }
    }
}

impl ReceivedMessage for FakeReceived {
    fn message(&self) -> &Message {
        &self.message
    }
    async fn ack(self) -> Result<(), TransportError> {
        self.settle(Event::Ack)
    }
    async fn nack(self, reason: &str) -> Result<(), TransportError> {
        self.settle(Event::Nack(reason.to_string()))
    }
    async fn dead_letter(self, reason: &str) -> Result<(), TransportError> {
        self.settle(Event::DeadLetter(reason.to_string()))
    }
    async fn park(self, reason: &str) -> Result<(), TransportError> {
        self.settle(Event::Park(reason.to_string()))
    }
}

/// A source that yields a preset queue of messages, then `None`.
pub struct FakeSource {
    queue: VecDeque<Message>,
    recorder: Arc<Recorder>,
    settle_ok: bool,
    recv_error: bool,
}

impl FakeSource {
    pub fn new(recorder: Arc<Recorder>, messages: Vec<Message>) -> Self {
        Self {
            queue: messages.into_iter().collect(),
            recorder,
            settle_ok: true,
            recv_error: false,
        }
    }
    pub fn with_settle_failure(mut self) -> Self {
        self.settle_ok = false;
        self
    }
    pub fn with_recv_error(mut self) -> Self {
        self.recv_error = true;
        self
    }
}

impl MessageSource for FakeSource {
    type Received = FakeReceived;
    async fn recv(&mut self) -> Result<Option<FakeReceived>, TransportError> {
        if self.recv_error {
            return Err(TransportError::retryable("recv failed"));
        }
        Ok(self.queue.pop_front().map(|message| FakeReceived {
            message,
            recorder: self.recorder.clone(),
            settle_ok: self.settle_ok,
        }))
    }
}

/// How a [`FakePublisher`] resolves each publish.
#[derive(Debug, Clone, Copy)]
pub enum PublishMode {
    Succeed,
    /// Unknown outcome: a retryable error (the row must stay retryable).
    FailUnknown,
}

/// A publisher that records published message ids and follows a [`PublishMode`].
pub struct FakePublisher {
    published: Mutex<Vec<String>>,
    mode: PublishMode,
}

impl FakePublisher {
    pub fn new(mode: PublishMode) -> Self {
        Self {
            published: Mutex::new(Vec::new()),
            mode,
        }
    }
    pub fn published_ids(&self) -> Vec<String> {
        self.published.lock().unwrap().clone()
    }
}

impl MessagePublisher for FakePublisher {
    async fn publish(&self, message: Message) -> Result<(), TransportError> {
        match self.mode {
            PublishMode::Succeed => {
                self.published
                    .lock()
                    .unwrap()
                    .push(message.id().unwrap_or_default().to_string());
                Ok(())
            }
            PublishMode::FailUnknown => Err(TransportError::retryable("publish outcome unknown")),
        }
    }
}

/// A service with conventional handlers: `ok` succeeds, `retryable` fails with a
/// retryable error, `permanent` fails with a permanent error. Each records that
/// it ran so tests can assert dispatch-before-ack ordering.
pub fn recording_service(recorder: &Arc<Recorder>) -> Arc<Service> {
    let ok = recorder.clone();
    let retryable = recorder.clone();
    let permanent = recorder.clone();
    Arc::new(
        Service::new().routes(
            Routes::new()
                .with_dependencies(())
                .event("delivery.succeeded")
                .handle(move |ctx: &Context<()>| {
                    ok.push(Event::Handled(ctx.message().name().to_string()));
                    async move { Ok(json!({})) }
                })
                .event("delivery.retry_requested")
                .handle(move |ctx: &Context<()>| {
                    retryable.push(Event::Handled(ctx.message().name().to_string()));
                    async move { Err(HandlerError::Other("infra".into())) }
                })
                .event("delivery.permanently_failed")
                .handle(move |ctx: &Context<()>| {
                    permanent.push(Event::Handled(ctx.message().name().to_string()));
                    async move { Err(HandlerError::Rejected("nope".into())) }
                }),
        ),
    )
}

pub fn event_message(name: &str, id: Option<&str>) -> Message {
    let mut message = Message::new(name, MessageKind::Event, b"{}".to_vec());
    if let Some(id) = id {
        message = message.with_id(id);
    }
    message
}

fn recording_source(messages: Vec<Message>) -> (Arc<Recorder>, Arc<Service>, FakeSource) {
    let recorder = Recorder::new();
    let service = recording_service(&recorder);
    let source = FakeSource::new(recorder.clone(), messages);
    (recorder, service, source)
}

// =============================================================================
// Source-runner contract
// =============================================================================

pub async fn source_dispatches_before_ack() {
    let (recorder, service, source) =
        recording_source(vec![event_message("delivery.succeeded", Some("m1"))]);
    run_source(service, source, RunOptions::idempotent())
        .await
        .unwrap();
    assert_eq!(
        recorder.events(),
        vec![Event::Handled("delivery.succeeded".into()), Event::Ack],
        "handler must run before ack"
    );
}

pub async fn source_retryable_failure_nacks_without_ack() {
    let (recorder, service, source) =
        recording_source(vec![event_message("delivery.retry_requested", Some("m1"))]);
    run_source(service, source, RunOptions::idempotent())
        .await
        .unwrap();
    let events = recorder.events();
    assert_eq!(
        events.first(),
        Some(&Event::Handled("delivery.retry_requested".into()))
    );
    assert!(matches!(events.get(1), Some(Event::Nack(_))));
    assert!(!events.contains(&Event::Ack));
}

pub async fn source_permanent_failure_dead_letters_by_default() {
    let (recorder, service, source) = recording_source(vec![event_message(
        "delivery.permanently_failed",
        Some("m1"),
    )]);
    run_source(service, source, RunOptions::idempotent())
        .await
        .unwrap();
    assert!(matches!(
        recorder.events().get(1),
        Some(Event::DeadLetter(_))
    ));
}

pub async fn source_permanent_failure_stops_under_stop_policy() {
    let (recorder, service, source) = recording_source(vec![
        event_message("delivery.permanently_failed", Some("m1")),
        event_message("delivery.succeeded", Some("m2")),
    ]);
    let outcome = run_source(
        service,
        source,
        RunOptions::idempotent().with_failure_policy(FailurePolicy::Stop),
    )
    .await;
    assert!(outcome.unwrap_err().is_permanent());
    // Second message never processed; first was not settled.
    assert_eq!(
        recorder.events(),
        vec![Event::Handled("delivery.permanently_failed".into())]
    );
}

pub async fn source_unhandled_message_is_acked_and_ignored() {
    let (recorder, service, source) =
        recording_source(vec![event_message("unrelated", Some("m1"))]);
    run_source(service, source, RunOptions::idempotent())
        .await
        .unwrap();
    // Acked without dispatching or dead-lettering.
    assert_eq!(recorder.events(), vec![Event::Ack]);
}

pub async fn source_inbox_mode_rejects_missing_stable_id() {
    // No id on the message; inbox mode requires a stable id.
    let (recorder, service, source) =
        recording_source(vec![event_message("delivery.succeeded", None)]);
    run_source(service, source, RunOptions::inbox(()))
        .await
        .unwrap();
    let events = recorder.events();
    // Handler never ran; the missing id is a permanent failure (dead-lettered).
    assert!(!events.iter().any(|e| matches!(e, Event::Handled(_))));
    assert!(matches!(events.first(), Some(Event::DeadLetter(_))));
}

pub async fn source_inbox_mode_dispatches_with_stable_id() {
    let (recorder, service, source) =
        recording_source(vec![event_message("delivery.succeeded", Some("m1"))]);
    run_source(service, source, RunOptions::inbox(()))
        .await
        .unwrap();
    assert_eq!(
        recorder.events(),
        vec![Event::Handled("delivery.succeeded".into()), Event::Ack]
    );
}

pub async fn source_propagates_recv_errors() {
    let (recorder, service, source) =
        recording_source(vec![event_message("delivery.succeeded", Some("m1"))]);
    let source = source.with_recv_error();
    let outcome = run_source(service, source, RunOptions::idempotent()).await;
    assert!(outcome.is_err(), "recv errors must not be swallowed");
    assert!(recorder.events().is_empty());
}

pub async fn source_propagates_settle_errors() {
    let (recorder, service, source) =
        recording_source(vec![event_message("delivery.succeeded", Some("m1"))]);
    let source = source.with_settle_failure();
    let outcome = run_source(service, source, RunOptions::idempotent()).await;
    assert!(outcome.is_err(), "settle errors must not be swallowed");
    // The ack was attempted before the error surfaced.
    assert_eq!(
        recorder.events(),
        vec![Event::Handled("delivery.succeeded".into()), Event::Ack]
    );
}

// =============================================================================
// Publisher / outbox dispatcher contract
// =============================================================================

async fn store_outbox(repo: &InMemoryRepository, id: &str) -> String {
    let message = OutboxMessage::create(id, "order.initialized", b"\x01".to_vec()).unwrap();
    let mut batch = CommitBatch::empty();
    batch.outbox_messages.push(message);
    repo.commit_batch(batch).await.unwrap();
    id.to_string()
}

async fn outbox_status(repo: &InMemoryRepository, id: &str) -> Option<OutboxMessageStatus> {
    outbox_support::outbox_status_by_id(&repo.outbox_store(), id).await
}

fn dispatcher(
    repo: &InMemoryRepository,
    mode: PublishMode,
    max_attempts: u32,
) -> OutboxDispatcher<InMemoryOutboxStore, FakePublisher> {
    OutboxDispatcher::new(
        repo.outbox_store(),
        FakePublisher::new(mode),
        "immediate:conformance",
        Duration::from_secs(60),
        max_attempts,
    )
}

pub async fn dispatcher_completes_only_after_publish_success() {
    let repo = InMemoryRepository::new();
    let id = store_outbox(&repo, "evt-1").await;
    let dispatcher = dispatcher(&repo, PublishMode::Succeed, 3);

    let outcome = dispatcher
        .dispatch_ids(std::slice::from_ref(&id))
        .await
        .unwrap();
    assert_eq!(outcome.published, 1);
    assert_eq!(
        dispatcher.publisher().published_ids(),
        vec!["evt-1".to_string()]
    );
    assert_eq!(outbox_status(&repo, &id).await, None);
}

pub async fn dispatcher_unknown_outcome_stays_retryable() {
    let repo = InMemoryRepository::new();
    let id = store_outbox(&repo, "evt-1").await;
    let dispatcher = dispatcher(&repo, PublishMode::FailUnknown, 3);

    let outcome = dispatcher
        .dispatch_ids(std::slice::from_ref(&id))
        .await
        .unwrap();
    assert_eq!(outcome.published, 0);
    assert_eq!(outcome.released, 1);
    assert_eq!(
        outbox_status(&repo, &id).await,
        Some(OutboxMessageStatus::Pending),
        "row must stay retryable"
    );
}

pub async fn dispatcher_claims_explicit_ids_before_publish() {
    let repo = InMemoryRepository::new();
    let wanted = store_outbox(&repo, "evt-1").await;
    let other = store_outbox(&repo, "evt-2").await;
    let dispatcher = dispatcher(&repo, PublishMode::Succeed, 3);

    let outcome = dispatcher
        .dispatch_ids(std::slice::from_ref(&wanted))
        .await
        .unwrap();
    assert_eq!(outcome.claimed, 1);
    assert_eq!(outcome.published, 1);
    assert_eq!(outbox_status(&repo, &wanted).await, None);
    // The unrequested row is untouched (claimed before publish, by id).
    assert_eq!(
        outbox_status(&repo, &other).await,
        Some(OutboxMessageStatus::Pending)
    );
}

// ============================================================================
// Composed at-least-once: crash between publish and complete → redelivery →
// consumer inbox dedupe
// ============================================================================

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use distributed::{
    ClaimOutboxMessages, InboxReceipt, OutboxClaimRef, OutboxStore, RepositoryError,
};

/// An `OutboxStore` whose `complete` fails once with a retryable storage error,
/// simulating a dispatcher crash AFTER the publish succeeded but BEFORE the
/// completion write landed. Everything else delegates to the real store.
struct CompleteOnceFailingStore {
    inner: InMemoryOutboxStore,
    fail_next_complete: AtomicBool,
}

impl OutboxStore for CompleteOnceFailingStore {
    fn messages_by_status(
        &self,
        status: OutboxMessageStatus,
        limit: usize,
    ) -> impl std::future::Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + '_
    {
        self.inner.messages_by_status(status, limit)
    }

    fn claim(
        &self,
        request: ClaimOutboxMessages,
    ) -> impl std::future::Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + '_
    {
        self.inner.claim(request)
    }

    async fn complete<'a>(&'a self, claim: &'a OutboxClaimRef) -> Result<(), RepositoryError> {
        if self.fail_next_complete.swap(false, Ordering::SeqCst) {
            return Err(RepositoryError::Storage {
                operation: "complete outbox row (simulated crash)".into(),
                retryable: true,
                source: None,
            });
        }
        self.inner.complete(claim).await
    }

    fn release<'a>(
        &'a self,
        claim: &'a OutboxClaimRef,
        error: &'a str,
    ) -> impl std::future::Future<Output = Result<(), RepositoryError>> + Send + 'a {
        self.inner.release(claim, error)
    }

    fn fail<'a>(
        &'a self,
        claim: &'a OutboxClaimRef,
        error: &'a str,
    ) -> impl std::future::Future<Output = Result<(), RepositoryError>> + Send + 'a {
        self.inner.fail(claim, error)
    }
}

/// A publisher that captures every published message (id AND body) into a
/// shared list, so deliveries from multiple dispatcher "workers" can be
/// replayed into a consumer.
struct CapturingPublisher {
    published: Arc<Mutex<Vec<Message>>>,
}

impl MessagePublisher for CapturingPublisher {
    async fn publish(&self, message: Message) -> Result<(), TransportError> {
        self.published.lock().unwrap().push(message);
        Ok(())
    }
}

/// End-to-end at-least-once: the publish succeeds but the completion write
/// fails (crash between publish and complete), the row is reclaimed and
/// republished after the lease expires — a duplicate delivery — and the
/// consumer's inbox receipt dedupes it, applying the effect exactly once.
///
/// The pieces are proven separately by the inbox conformance and the
/// dispatcher tests; this composes them across the wire.
pub async fn publish_then_crash_republishes_and_consumer_inbox_dedupes() {
    // Producer side: one committed outbox row.
    let producer = InMemoryRepository::new();
    let message_id = unique("evt");
    let mut batch = CommitBatch::empty();
    batch.outbox_messages.push(
        OutboxMessage::create(&message_id, "order.initialized", b"{}".to_vec())
            .expect("outbox message should be valid"),
    );
    producer
        .commit_batch(batch)
        .await
        .expect("outbox row commits");

    let published = Arc::new(Mutex::new(Vec::new()));
    let ids = [message_id.clone()];

    // Pass 1: a worker with a short lease publishes successfully, then the
    // completion write "crashes".
    let crash_lease = Duration::from_millis(100);
    let crashing_worker = OutboxDispatcher::new(
        CompleteOnceFailingStore {
            inner: producer.outbox_store(),
            fail_next_complete: AtomicBool::new(true),
        },
        CapturingPublisher {
            published: published.clone(),
        },
        "worker-crashed",
        crash_lease,
        5,
    );
    crashing_worker
        .dispatch_ids(&ids)
        .await
        .expect_err("the failed completion write surfaces as an error (the crash)");

    // The row survived the crash: still owed. Once the crashed worker's lease
    // expires, a fresh worker (with a comfortable lease) reclaims and
    // republishes it.
    tokio::time::sleep(crash_lease + Duration::from_millis(200)).await;
    let retry_worker = OutboxDispatcher::new(
        producer.outbox_store(),
        CapturingPublisher {
            published: published.clone(),
        },
        "worker-retry",
        Duration::from_secs(60),
        5,
    );
    let outcome = retry_worker
        .dispatch_ids(&ids)
        .await
        .expect("the retry pass dispatches cleanly");
    assert_eq!(outcome.published, 1, "the reclaimed row is republished");
    assert_eq!(
        outbox_status(&producer, &message_id).await,
        None,
        "the row completes only after the successful pass"
    );

    let deliveries = published.lock().unwrap().clone();
    assert_eq!(
        deliveries.len(),
        2,
        "at-least-once: the crash produced a duplicate delivery"
    );
    assert!(
        deliveries
            .iter()
            .all(|delivery| delivery.id() == Some(message_id.as_str())),
        "both deliveries carry the same stable message id"
    );

    // Consumer side: replay both deliveries through the runner in inbox mode.
    // The handler commits its effect atomically with an inbox receipt and treats
    // a duplicate receipt as already-applied (the dedupe).
    let consumer_repo = InMemoryRepository::new();
    let effects_applied = Arc::new(AtomicUsize::new(0));
    let effect_seq = Arc::new(AtomicUsize::new(0));
    let consumer = "checkout-consumer";
    let handlers = {
        let repo = consumer_repo.clone();
        let effects_applied = effects_applied.clone();
        let effect_seq = effect_seq.clone();
        Arc::new(
            Handlers::new().on_event("order.initialized", move |message: &Message| {
                let repo = repo.clone();
                let effects_applied = effects_applied.clone();
                let effect_seq = effect_seq.clone();
                let id = message.id().unwrap_or_default().to_string();
                async move {
                    let mut batch = CommitBatch::empty();
                    batch.inbox_receipts.push(InboxReceipt::new(consumer, &id));
                    // A fresh effect id per attempt, so only the receipt (not an
                    // outbox-id collision) can fence the duplicate.
                    let attempt = effect_seq.fetch_add(1, Ordering::SeqCst);
                    batch.outbox_messages.push(
                        OutboxMessage::create(
                            format!("effect-{attempt}"),
                            "effect.applied",
                            b"{}".to_vec(),
                        )
                        .expect("effect message should be valid"),
                    );
                    match repo.commit_batch(batch).await {
                        Ok(()) => {
                            effects_applied.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }
                        // Duplicate delivery: already applied — ack it.
                        Err(RepositoryError::DuplicateInboxReceipt { .. }) => Ok(()),
                        Err(other) => Err(TransportError::permanent(other.to_string())),
                    }
                }
            }),
        )
    };

    let recorder = Recorder::new();
    let source = FakeSource::new(recorder.clone(), deliveries);
    run_source(handlers, source, RunOptions::inbox(()))
        .await
        .expect("consumer drains both deliveries");

    // Both deliveries were acked, the effect applied exactly once, and only the
    // first attempt's effect row exists.
    assert_eq!(
        recorder.events(),
        vec![Event::Ack, Event::Ack],
        "both deliveries settle as acks (the duplicate is deduped, not nacked)"
    );
    assert_eq!(
        effects_applied.load(Ordering::SeqCst),
        1,
        "effectively-once: the effect committed exactly once"
    );
    assert!(
        consumer_repo.inbox_contains(consumer, &message_id),
        "the receipt is recorded for the consumer"
    );
    let consumer_outbox = consumer_repo.outbox_store();
    assert!(
        outbox_support::find_outbox_by_id(&consumer_outbox, "effect-0")
            .await
            .is_some(),
        "the first delivery's effect landed"
    );
    assert!(
        outbox_support::find_outbox_by_id(&consumer_outbox, "effect-1")
            .await
            .is_none(),
        "the duplicate delivery's effect was fenced by the inbox receipt"
    );
}

// ============================================================================
// Broker-test helpers
// ============================================================================
//
// Shared by the real-broker test mains (kafka/nats/rabbitmq/postgres), which
// include this module the same way the in-memory harness does. The
// per-transport scenarios stay in their own files — only the genuinely
// identical scaffolding lives here.

#[path = "../support/ids.rs"]
mod ids;
#[path = "../support/outbox.rs"]
pub mod outbox_support;

#[allow(unused_imports)] // each including target uses a subset
pub use ids::{run_token, unique};

/// A `Service` whose single handler records each message's id into `rec`;
/// `kind` selects command vs event registration.
pub fn recording_for(name: &str, kind: MessageKind, rec: Arc<Mutex<Vec<String>>>) -> Arc<Service> {
    let leaked: &'static str = Box::leak(name.to_string().into_boxed_str());
    let routes = Routes::new().with_dependencies(());
    let registered = match kind {
        MessageKind::Command => routes.command(leaked),
        MessageKind::Event => routes.event(leaked),
    };
    Arc::new(
        Service::new().routes(registered.handle(move |ctx: &Context<()>| {
            rec.lock()
                .unwrap()
                .push(ctx.message().id().unwrap_or_default().to_string());
            async move { Ok(json!({})) }
        })),
    )
}

/// Like [`recording_for`], but names the service so consumer-group / queue
/// identity is explicit (used where two replicas share a group).
pub fn named_recording_for(
    service_name: &str,
    name: &str,
    kind: MessageKind,
    rec: Arc<Mutex<Vec<String>>>,
) -> Arc<Service> {
    let leaked: &'static str = Box::leak(name.to_string().into_boxed_str());
    let routes = Routes::new().with_dependencies(());
    let registered = match kind {
        MessageKind::Command => routes.command(leaked),
        MessageKind::Event => routes.event(leaked),
    };
    Arc::new(
        Service::new()
            .named(service_name.to_string())
            .routes(registered.handle(move |ctx: &Context<()>| {
                rec.lock()
                    .unwrap()
                    .push(ctx.message().id().unwrap_or_default().to_string());
                async move { Ok(json!({})) }
            })),
    )
}

// ============================================================================
// Shared bus behaviour scenarios
// ============================================================================
//
// The point-to-point / fan-out / named-service contracts are identical across
// every `Bus + BusConsumer` transport; only construction differs, so each
// scenario takes a bus factory. Transport-specific variants (Kafka's
// offset-commit point-to-point proof, RabbitMQ's bind-before-publish fan-out)
// stay in their own mains — their transport semantics are the point.

use std::future::Future;

use distributed::bus::{Bus, BusConsumer};

pub const COMMAND_NAME: &str = "order.initialize";
pub const EVENT_NAME: &str = "order.initialized";
pub const PAYLOAD: &[u8] = b"{}";

pub fn command(id: impl Into<String>) -> Message {
    Message::new(COMMAND_NAME, MessageKind::Command, PAYLOAD.to_vec()).with_id(id)
}

pub fn event(id: impl Into<String>) -> Message {
    Message::new(EVENT_NAME, MessageKind::Event, PAYLOAD.to_vec()).with_id(id)
}

pub fn expected_ids(prefix: &str, total: usize) -> Vec<String> {
    (0..total).map(|i| format!("{prefix}{i}")).collect()
}

pub fn recorded_ids(rec: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    let mut ids = rec.lock().unwrap().clone();
    ids.sort();
    ids
}

pub async fn send_commands<B: Bus>(bus: &B, total: usize) {
    for message in expected_ids("c", total).into_iter().map(command) {
        bus.send_message(message).await.expect("send command");
    }
}

pub async fn publish_events<B: Bus>(bus: &B, total: usize) {
    for message in expected_ids("e", total).into_iter().map(event) {
        bus.publish_message(message).await.expect("publish event");
    }
}

/// `send` + `listen`: replicas sharing a `group` compete for the commands —
/// each command is handled exactly once across the pool (point-to-point).
///
/// `bus_for_group` returns a connected bus for the given consumer group. Each
/// call may open a fresh connection (RabbitMQ) or clone over a shared pool;
/// the factory owns any per-transport setup (`ensure_stream`/`ensure_tables`).
pub async fn bus_send_listen_is_point_to_point_across_a_group<B, F, Fut>(bus_for_group: F)
where
    B: Bus + BusConsumer,
    F: Fn(&'static str) -> Fut,
    Fut: Future<Output = B>,
{
    let producer = bus_for_group("orders").await;
    let total = 6;
    send_commands(&producer, total).await;

    // Two replicas of the same group drain concurrently.
    let rec = Arc::new(Mutex::new(Vec::new()));
    let bus_a = bus_for_group("orders").await;
    let bus_b = bus_for_group("orders").await;
    let (ra, rb) = tokio::join!(
        bus_a.listen(
            recording_for(COMMAND_NAME, MessageKind::Command, rec.clone()),
            RunOptions::idempotent()
        ),
        bus_b.listen(
            recording_for(COMMAND_NAME, MessageKind::Command, rec.clone()),
            RunOptions::idempotent()
        ),
    );
    ra.expect("replica a drains");
    rb.expect("replica b drains");

    assert_eq!(
        recorded_ids(&rec),
        expected_ids("c", total),
        "every command handled exactly once across the group"
    );
}

/// `publish` + `subscribe`: distinct `group`s each get their own durable
/// position, so every group sees every event (fan-out).
pub async fn bus_publish_subscribe_fans_out_across_groups<B, F, Fut>(bus_for_group: F)
where
    B: Bus + BusConsumer,
    F: Fn(&'static str) -> Fut,
    Fut: Future<Output = B>,
{
    let producer = bus_for_group("producer").await;
    let total = 4;
    publish_events(&producer, total).await;

    let expected = expected_ids("e", total);
    for group in ["projections", "audit"] {
        let bus = bus_for_group(group).await;
        let rec = Arc::new(Mutex::new(Vec::new()));
        bus.subscribe(
            recording_for(EVENT_NAME, MessageKind::Event, rec.clone()),
            RunOptions::idempotent(),
        )
        .await
        .expect("subscriber drains");
        assert_eq!(
            recorded_ids(&rec),
            expected,
            "group {group} sees every event"
        );
    }
}

/// With no explicit `group`, a named service's name becomes the consumer-group
/// identity, and the subscriber still drains every event.
///
/// `bus` returns a connected bus with NO group configured.
pub async fn bus_subscribe_uses_named_service_as_consumer_group<B, F, Fut>(bus: F)
where
    B: Bus + BusConsumer,
    F: Fn() -> Fut,
    Fut: Future<Output = B>,
{
    let producer = bus().await;
    publish_events(&producer, 3).await;

    let rec = Arc::new(Mutex::new(Vec::new()));
    bus()
        .await
        .subscribe(
            named_recording_for(
                "order-projection",
                EVENT_NAME,
                MessageKind::Event,
                rec.clone(),
            ),
            RunOptions::idempotent(),
        )
        .await
        .expect("subscriber drains");

    assert_eq!(recorded_ids(&rec), expected_ids("e", 3));
}
