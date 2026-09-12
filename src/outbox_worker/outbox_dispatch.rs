//! Outbox → transport bridge.
//!
//! Maps durable [`OutboxMessage`] rows to the canonical [`Message`] and dispatches
//! them through a [`MessagePublisher`]. The same claim → map → publish →
//! settle path is shared by background worker polling ([`dispatch_batch`]) and
//! after-commit immediate dispatch ([`dispatch_ids`]), so the two cannot diverge
//! and cannot publish the same row concurrently — both go through the outbox
//! claim lease.
//!
//! [`dispatch_batch`]: OutboxDispatcher::dispatch_batch
//! [`dispatch_ids`]: OutboxDispatcher::dispatch_ids

use std::num::NonZeroUsize;
use std::time::Duration;
#[cfg(feature = "metrics")]
use std::time::SystemTime;

use futures_util::stream::{self, StreamExt};

use super::{ClaimOutboxMessages, OutboxClaimRef, OutboxPublishFailureAction, OutboxStore};
use crate::bus::{Message, MessageKind, MessagePublisher, TransportError, TransportErrorKind};
use crate::outbox::OutboxMessage;
use crate::repository::RepositoryError;

/// Repository/store failures (lock contention, storage hiccups, stale-claim
/// conflicts) are retryable: usually transient, resolved by a later re-claim. This
/// conversion lives with the outbox bridge — which legitimately knows both the
/// store and the bus — so bus core stays free of `RepositoryError`.
impl From<RepositoryError> for TransportError {
    fn from(error: RepositoryError) -> Self {
        TransportError::new(TransportErrorKind::Retryable, error.to_string()).with_source(error)
    }
}

/// Content type for an outbox payload. Outbox payloads are codec-encoded bytes
/// (bitcode or raw), so the media type is binary; the exact codec travels in
/// metadata under [`SOURCED_METADATA_PREFIX`] for the consumer.
const OUTBOX_CONTENT_TYPE: &str = "application/octet-stream";

/// Reserved metadata key prefix for framework-derived keys. User metadata must
/// not use this prefix; keys here (payload codec, destination, source context)
/// carry decode/routing semantics and must not be shadowable by user metadata.
pub const SOURCED_METADATA_PREFIX: &str = "x-sourced-";

impl From<OutboxMessage> for Message {
    /// Map a durable outbox row to a canonical transport message.
    ///
    /// Consumes the row so the payload bytes and metadata strings move instead
    /// of being cloned — every dispatch path owns its row by the time it maps
    /// (claims are returned by value, and the after-commit hook is handed the
    /// claimed clone the commit staged).
    ///
    /// - `id` ← outbox message id (the stable durable id);
    /// - `name` ← `event_type`;
    /// - `kind` ← `Command` when a point-to-point `destination` is set, else `Event`;
    /// - `payload` ← raw codec bytes, `content_type` = `application/octet-stream`;
    /// - `metadata` ← the outbox metadata (correlation/causation/trace/auth) plus
    ///   framework-derived keys under the reserved [`SOURCED_METADATA_PREFIX`]
    ///   namespace (payload codec, destination, source-aggregate context) so
    ///   decode/routing context can never be shadowed by a user metadata key.
    fn from(outbox: OutboxMessage) -> Self {
        let kind = if outbox.destination.is_some() {
            MessageKind::Command
        } else {
            MessageKind::Event
        };

        // User metadata first (correlation/trace/auth), then framework-derived
        // keys under the reserved `x-sourced-` prefix. Any user key in the
        // reserved namespace is dropped so framework values stay authoritative
        // and cannot be shadowed on case-insensitive lookup.
        let mut metadata: Vec<(String, String)> = outbox
            .metadata
            .into_iter()
            .filter(|(key, _)| {
                !key.to_ascii_lowercase()
                    .starts_with(SOURCED_METADATA_PREFIX)
            })
            .collect();
        metadata.push((
            format!("{SOURCED_METADATA_PREFIX}payload-codec"),
            outbox.payload_codec,
        ));
        metadata.push((
            format!("{SOURCED_METADATA_PREFIX}payload-codec-version"),
            outbox.payload_codec_version.to_string(),
        ));
        if let Some(destination) = outbox.destination {
            metadata.push((format!("{SOURCED_METADATA_PREFIX}destination"), destination));
        }
        if let Some(source_type) = outbox.source_aggregate_type {
            metadata.push((
                format!("{SOURCED_METADATA_PREFIX}source-aggregate-type"),
                source_type,
            ));
        }
        if let Some(source_id) = outbox.source_aggregate_id {
            metadata.push((
                format!("{SOURCED_METADATA_PREFIX}source-aggregate-id"),
                source_id,
            ));
        }
        if let Some(sequence) = outbox.source_sequence {
            metadata.push((
                format!("{SOURCED_METADATA_PREFIX}source-sequence"),
                sequence.to_string(),
            ));
        }

        Message {
            id: Some(outbox.id),
            name: outbox.event_type,
            kind,
            payload: outbox.payload,
            content_type: OUTBOX_CONTENT_TYPE.to_string(),
            metadata,
        }
    }
}

/// Counts of what one dispatch pass did. Raced/unclaimable ids are reflected as
/// `claimed < requested`, not as an error; publish failures are `released`
/// (retryable) or `failed` (attempt ceiling reached), not errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OutboxDispatchOutcome {
    /// Rows asked for (id count for `dispatch_ids`, batch size for `dispatch_batch`).
    pub requested: usize,
    /// Rows actually claimed this pass.
    pub claimed: usize,
    /// Rows published and completed.
    pub published: usize,
    /// Rows released for retry after a publish failure.
    pub released: usize,
    /// Rows permanently failed after exhausting attempts.
    pub failed: usize,
}

/// Bridges outbox claims to a [`MessagePublisher`], shared by immediate
/// after-commit dispatch and background worker polling.
///
/// Publish failures are not errors — they are reflected in
/// [`OutboxDispatchOutcome`] (released/failed) and the pass continues. A *store*
/// error (claim/complete/record_failure) does abort the pass and is returned as
/// `Err`; the partial [`OutboxDispatchOutcome`] is not returned in that case, and
/// any already-settled rows keep their settled state (the next pass resumes the
/// rest). Rows still leased by the aborted pass become claimable again at lease
/// expiry.
pub struct OutboxDispatcher<S, P> {
    store: S,
    publisher: P,
    worker_id: String,
    lease: Duration,
    max_attempts: u32,
    publish_concurrency: NonZeroUsize,
    service_name: Option<String>,
}

impl<S, P> OutboxDispatcher<S, P>
where
    S: OutboxStore,
    P: MessagePublisher,
{
    /// Create a dispatcher. `worker_id` scopes claims (use a synthetic id such as
    /// `immediate:<process>` for after-commit dispatch); `max_attempts` is the
    /// publish-failure ceiling before a row is permanently failed.
    pub fn new(
        store: S,
        publisher: P,
        worker_id: impl Into<String>,
        lease: Duration,
        max_attempts: u32,
    ) -> Self {
        Self {
            store,
            publisher,
            worker_id: worker_id.into(),
            lease,
            max_attempts,
            publish_concurrency: NonZeroUsize::MIN,
            service_name: None,
        }
    }

    /// Set how many publishes may be in flight at once during a dispatch pass.
    ///
    /// Defaults to `1`, which publishes strictly in claim (created-at) order —
    /// the safe choice whenever downstream consumers rely on outbox ordering.
    /// Raising it overlaps publish round trips for higher throughput, but
    /// messages may reach the transport out of order, so only raise it when
    /// consumers are order-independent (or ordering is restored downstream,
    /// e.g. by per-key partitioning). Settlement is batched either way.
    pub fn with_publish_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.publish_concurrency = concurrency;
        self
    }

    /// Attach the logical service name to metrics emitted by this dispatcher.
    pub fn with_service(mut self, service_name: impl Into<String>) -> Self {
        self.service_name = Some(service_name.into());
        self
    }

    /// The publisher this dispatcher sends through.
    pub fn publisher(&self) -> &P {
        &self.publisher
    }

    /// The outbox store this dispatcher claims from.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Immediate after-commit dispatch of the explicit outbox ids a commit just
    /// inserted. Claims those ids (raced/unclaimable ids are skipped, not an
    /// error) before publishing, so it never races the polling worker.
    pub async fn dispatch_ids(
        &self,
        ids: &[String],
    ) -> Result<OutboxDispatchOutcome, TransportError> {
        let request =
            ClaimOutboxMessages::for_ids(self.worker_id.clone(), ids.to_vec(), self.lease);
        let claimed = self.store.claim(request).await?;
        let mut outcome = self.dispatch_claimed(claimed).await?;
        outcome.requested = ids.len();
        self.record_outbox_backlog().await;
        Ok(outcome)
    }

    /// Background worker dispatch of the next claimable batch.
    pub async fn dispatch_batch(
        &self,
        batch_size: usize,
    ) -> Result<OutboxDispatchOutcome, TransportError> {
        let request = ClaimOutboxMessages::new(self.worker_id.clone(), batch_size, self.lease);
        let claimed = self.store.claim(request).await?;
        let mut outcome = self.dispatch_claimed(claimed).await?;
        outcome.requested = batch_size;
        self.record_outbox_backlog().await;
        Ok(outcome)
    }

    /// The shared settle path: map → publish → complete on success, or
    /// record_failure (release/fail) on publish error. A row is completed ONLY
    /// after the publish threshold; an unknown/failed publish leaves it retryable.
    async fn dispatch_claimed(
        &self,
        claimed: Vec<OutboxMessage>,
    ) -> Result<OutboxDispatchOutcome, TransportError> {
        let claimed_count = claimed.len();
        let settled = publish_and_settle(
            &self.store,
            &self.publisher,
            claimed,
            self.max_attempts,
            self.publish_concurrency,
        )
        .await?;
        self.record_outbox_outcomes(&settled);
        Ok(OutboxDispatchOutcome {
            requested: 0,
            claimed: claimed_count,
            published: settled.published,
            released: settled.released,
            failed: settled.failed,
        })
    }

    fn record_outbox_outcomes(&self, settled: &SettleOutcome) {
        #[cfg(feature = "metrics")]
        {
            let service = self.service_name.as_deref();
            crate::metrics::record_outbox_messages(
                service,
                crate::telemetry::outbox_outcome::PUBLISHED,
                settled.published,
            );
            crate::metrics::record_outbox_messages(
                service,
                crate::telemetry::outbox_outcome::RELEASED,
                settled.released,
            );
            crate::metrics::record_outbox_messages(
                service,
                crate::telemetry::outbox_outcome::FAILED,
                settled.failed,
            );
        }
        #[cfg(not(feature = "metrics"))]
        let _ = settled;
    }

    async fn record_outbox_backlog(&self) {
        #[cfg(feature = "metrics")]
        record_backlog_gauges(&self.store, self.service_name.as_deref()).await;
    }
}

/// Set the outbox backlog gauges from a lightweight pending count/oldest read
/// ([`OutboxStore::backlog_stats`]). Shared by the dispatcher and after-commit
/// publish hook so both report the same series.
///
/// Runs on every settle pass, deliberately unthrottled: refreshes are
/// activity-driven (there is no timer in the crate), so skipping any pass —
/// the final drain especially — would freeze the gauges at stale values and
/// misfire backlog alerts in both directions. A store read failure skips the
/// update rather than failing dispatch.
#[cfg(feature = "metrics")]
pub(crate) async fn record_backlog_gauges<S: OutboxStore>(store: &S, service: Option<&str>) {
    if let Ok(stats) = store.backlog_stats().await {
        let oldest_pending_age = stats
            .oldest_created_at
            .and_then(|created_at| crate::time::now().duration_since(created_at).ok());
        crate::metrics::set_outbox_backlog(service, stats.pending, oldest_pending_age);
    }
}

/// Counts of what one [`publish_and_settle`] pass did with already-claimed rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct SettleOutcome {
    /// Rows published and completed.
    pub published: usize,
    /// Rows released for retry after a publish failure.
    pub released: usize,
    /// Rows permanently failed after exhausting attempts.
    pub failed: usize,
}

/// Publish already-claimed outbox rows through `publisher` and settle their
/// claims in `store`: successes are completed in one batched
/// [`complete_many`] call after the publish phase, publish failures are
/// settled individually through `record_failure` (release, or fail at the
/// `max_attempts` ceiling).
///
/// This is the one publish-then-settle path, shared by the dispatcher
/// (background polling and after-commit `dispatch_ids`) and by the
/// fallback publish hook. It never claims: callers must already hold the
/// claims — the dispatcher claims first, and a mailbox-less hook is handed
/// rows the test path spawned after commit.
///
/// `publish_concurrency` bounds how many publishes are in flight at once.
/// `1` preserves strict claim order; higher values overlap publish round
/// trips but may deliver out of order.
///
/// Publish failures are not errors — they are settled into the outcome. Only
/// a store failure returns `Err`. If the batched complete itself fails, the
/// published-but-unsettled rows stay in flight and are re-published after
/// lease expiry — the same at-least-once window a crash between publish and
/// settle always leaves.
///
/// [`complete_many`]: OutboxStore::complete_many
pub(crate) async fn publish_and_settle<S, P>(
    store: &S,
    publisher: &P,
    claimed: Vec<OutboxMessage>,
    max_attempts: u32,
    publish_concurrency: NonZeroUsize,
) -> Result<SettleOutcome, RepositoryError>
where
    S: OutboxStore,
    P: MessagePublisher,
{
    let mut work = Vec::with_capacity(claimed.len());
    for message in claimed {
        let claim = OutboxClaimRef::from_message(&message)?;
        work.push((claim, Message::from(message)));
    }

    // Publish phase: up to `publish_concurrency` publishes in flight. With the
    // default of 1 this awaits each publish before starting the next, so rows
    // go out strictly in claim (created-at) order.
    let results: Vec<(OutboxClaimRef, Result<(), TransportError>)> =
        stream::iter(work.into_iter().map(|(claim, message)| async move {
            let result = publish_with_span(publisher, message).await;
            (claim, result)
        }))
        .buffer_unordered(publish_concurrency.get())
        .collect()
        .await;

    // Settle phase: failures are the exception and settle individually;
    // successes settle in one batched complete.
    let mut outcome = SettleOutcome::default();
    let mut published = Vec::with_capacity(results.len());
    for (claim, result) in results {
        match result {
            Ok(()) => published.push(claim),
            Err(publish_error) => {
                match store
                    .record_failure(&claim, &publish_error.to_string(), max_attempts)
                    .await?
                {
                    OutboxPublishFailureAction::Released => outcome.released += 1,
                    OutboxPublishFailureAction::Failed => outcome.failed += 1,
                }
            }
        }
    }
    store.complete_many(&published).await?;
    outcome.published = published.len();
    Ok(outcome)
}

/// Publish one mapped outbox message, wrapped in a framework span when the
/// `otel` feature is enabled (parented from the message's trace metadata).
async fn publish_with_span<P: MessagePublisher>(
    publisher: &P,
    message: Message,
) -> Result<(), TransportError> {
    #[cfg(feature = "otel")]
    {
        use tracing::Instrument as _;

        let span = outbox_publish_span(&message);
        crate::trace_context::set_span_parent_from_metadata_if_no_current_span(
            &span,
            &message.metadata,
        );
        return publisher.publish(message).instrument(span).await;
    }

    #[cfg(not(feature = "otel"))]
    {
        publisher.publish(message).await
    }
}

#[cfg(feature = "otel")]
fn outbox_publish_span(message: &Message) -> tracing::Span {
    crate::telemetry::outbox_publish_span(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbox_worker::testing::block_on;
    use crate::{CommitBatch, InMemoryRepository, TransactionalCommit};
    use std::sync::Mutex;

    /// Publisher that records every published message id, optionally failing.
    struct RecordingPublisher {
        published: Mutex<Vec<String>>,
        fail: bool,
    }

    impl RecordingPublisher {
        fn new(fail: bool) -> Self {
            Self {
                published: Mutex::new(Vec::new()),
                fail,
            }
        }
        fn ids(&self) -> Vec<String> {
            self.published.lock().unwrap().clone()
        }
    }

    impl MessagePublisher for RecordingPublisher {
        async fn publish(&self, message: Message) -> Result<(), TransportError> {
            if self.fail {
                // Unknown outcome: surface as a retryable error.
                return Err(TransportError::retryable("publish failed"));
            }
            self.published
                .lock()
                .unwrap()
                .push(message.id().unwrap_or_default().to_string());
            Ok(())
        }
    }

    fn store_message(repo: &InMemoryRepository, message: OutboxMessage) -> String {
        let id = message.id().to_string();
        let mut batch = CommitBatch::empty();
        batch.outbox_messages.push(message);
        block_on(repo.commit_batch(batch)).unwrap();
        id
    }

    fn outbox(id: &str) -> OutboxMessage {
        OutboxMessage::create_with_metadata(
            id,
            "OrderCreated",
            b"\x01\x02".to_vec(),
            [
                ("correlation_id".to_string(), "corr-1".to_string()),
                (
                    "traceparent".to_string(),
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
                ),
                ("tracestate".to_string(), "vendor=value".to_string()),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap()
    }

    #[test]
    fn maps_outbox_row_to_canonical_message() {
        let message = Message::from(outbox("evt-1"));
        assert_eq!(message.id(), Some("evt-1"));
        assert_eq!(message.name(), "OrderCreated");
        assert_eq!(message.kind, MessageKind::Event);
        assert_eq!(message.payload(), b"\x01\x02");
        assert_eq!(message.content_type, "application/octet-stream");
        // User metadata preserved; codec carried (namespaced) for the consumer.
        assert_eq!(message.correlation_id(), Some("corr-1"));
        assert_eq!(
            message.traceparent(),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
        assert_eq!(message.tracestate(), Some("vendor=value"));
        assert_eq!(message.metadata("x-sourced-payload-codec"), Some("bytes"));
        assert_eq!(
            message.metadata("x-sourced-payload-codec-version"),
            Some("1")
        );
    }

    #[test]
    fn user_metadata_cannot_shadow_reserved_framework_keys() {
        // A user metadata key colliding with a reserved name does not override
        // the framework value, because framework keys are namespaced.
        let outbox = OutboxMessage::create_with_metadata(
            "evt-1",
            "OrderCreated",
            b"\x01".to_vec(),
            [("x-sourced-payload-codec".to_string(), "evil".to_string())]
                .into_iter()
                .collect(),
        )
        .unwrap();
        let message = Message::from(outbox);
        // The user's reserved-prefix key is dropped; lookup returns the
        // authoritative framework value, and there is exactly one such entry.
        assert_eq!(message.metadata("x-sourced-payload-codec"), Some("bytes"));
        assert_eq!(
            message
                .metadata
                .iter()
                .filter(|(k, _)| k == "x-sourced-payload-codec")
                .count(),
            1
        );
    }

    #[test]
    fn destination_maps_to_command_kind() {
        let outbox =
            OutboxMessage::create_to("cmd-1", "ShipOrder", "shipping", b"{}".to_vec()).unwrap();
        let message = Message::from(outbox);
        assert_eq!(message.kind, MessageKind::Command);
        assert_eq!(message.metadata("x-sourced-destination"), Some("shipping"));
    }

    fn dispatcher(
        repo: &InMemoryRepository,
        fail: bool,
        max_attempts: u32,
    ) -> OutboxDispatcher<crate::InMemoryOutboxStore, RecordingPublisher> {
        OutboxDispatcher::new(
            repo.outbox_store(),
            RecordingPublisher::new(fail),
            "immediate:test",
            Duration::from_secs(60),
            max_attempts,
        )
    }

    fn load(repo: &InMemoryRepository, id: &str) -> OutboxMessage {
        repo.outbox_storage()
            .read()
            .unwrap()
            .get(id)
            .unwrap()
            .clone()
    }

    #[test]
    fn dispatch_ids_claims_then_publishes_then_completes() {
        let repo = InMemoryRepository::new();
        let id = store_message(&repo, outbox("evt-1"));
        let dispatcher = dispatcher(&repo, false, 3);

        let outcome = block_on(dispatcher.dispatch_ids(std::slice::from_ref(&id))).unwrap();

        assert_eq!(
            outcome,
            OutboxDispatchOutcome {
                requested: 1,
                claimed: 1,
                published: 1,
                released: 0,
                failed: 0,
            }
        );
        // The publisher saw the message id, and the row is completed only after.
        assert_eq!(dispatcher.publisher.ids(), vec!["evt-1".to_string()]);
        assert!(!repo.outbox_storage().read().unwrap().contains_key(&id));
    }

    #[test]
    fn unknown_publish_outcome_leaves_row_retryable() {
        let repo = InMemoryRepository::new();
        let id = store_message(&repo, outbox("evt-1"));
        let dispatcher = dispatcher(&repo, true, 3);

        let outcome = block_on(dispatcher.dispatch_ids(std::slice::from_ref(&id))).unwrap();

        assert_eq!(outcome.published, 0);
        assert_eq!(outcome.released, 1);
        assert_eq!(outcome.failed, 0);
        // Released back to pending, not completed: still retryable.
        let row = load(&repo, &id);
        assert!(row.is_pending());
        assert_eq!(row.attempts, 1);
    }

    #[test]
    fn publish_failure_fails_row_at_attempt_ceiling() {
        let repo = InMemoryRepository::new();
        let id = store_message(&repo, outbox("evt-1"));
        let dispatcher = dispatcher(&repo, true, 1);

        let outcome = block_on(dispatcher.dispatch_ids(std::slice::from_ref(&id))).unwrap();

        assert_eq!(outcome.failed, 1);
        assert_eq!(outcome.released, 0);
        assert!(load(&repo, &id).is_failed());
    }

    #[test]
    fn dispatch_ids_only_claims_requested_ids() {
        let repo = InMemoryRepository::new();
        let wanted = store_message(&repo, outbox("evt-1"));
        let other = store_message(&repo, outbox("evt-2"));
        let dispatcher = dispatcher(&repo, false, 3);

        let outcome = block_on(dispatcher.dispatch_ids(std::slice::from_ref(&wanted))).unwrap();

        assert_eq!(outcome.claimed, 1);
        assert_eq!(outcome.published, 1);
        assert!(!repo.outbox_storage().read().unwrap().contains_key(&wanted));
        // The unrequested row is untouched.
        assert!(load(&repo, &other).is_pending());
    }

    #[test]
    fn raced_id_is_not_an_error() {
        let repo = InMemoryRepository::new();
        // No such row stored: claim returns nothing, dispatch is a clean no-op.
        let outcome =
            block_on(dispatcher(&repo, false, 3).dispatch_ids(&["missing".to_string()])).unwrap();
        assert_eq!(
            outcome,
            OutboxDispatchOutcome {
                requested: 1,
                claimed: 0,
                published: 0,
                released: 0,
                failed: 0,
            }
        );
    }

    #[test]
    fn worker_and_immediate_dispatch_share_state_transitions() {
        let repo = InMemoryRepository::new();
        let immediate_id = store_message(&repo, outbox("evt-1"));
        let _worker_id = store_message(&repo, outbox("evt-2"));

        // Immediate dispatch claims+completes evt-1; worker batch then drains the
        // rest. Both go through the same claim/publish/complete path.
        let dispatcher = dispatcher(&repo, false, 3);
        let immediate =
            block_on(dispatcher.dispatch_ids(std::slice::from_ref(&immediate_id))).unwrap();
        assert_eq!(immediate.published, 1);

        let drained = block_on(dispatcher.dispatch_batch(10)).unwrap();
        assert_eq!(drained.claimed, 1, "only evt-2 remains claimable");
        assert_eq!(drained.published, 1);
        assert_eq!(dispatcher.publisher.ids().len(), 2);
    }

    /// Publisher that fails one specific message id and records the rest.
    struct SelectiveFailPublisher {
        fail_id: String,
        published: Mutex<Vec<String>>,
    }

    impl MessagePublisher for SelectiveFailPublisher {
        async fn publish(&self, message: Message) -> Result<(), TransportError> {
            if message.id() == Some(self.fail_id.as_str()) {
                return Err(TransportError::retryable("selective publish failure"));
            }
            self.published
                .lock()
                .unwrap()
                .push(message.id().unwrap_or_default().to_string());
            Ok(())
        }
    }

    #[test]
    fn mixed_publish_outcomes_settle_successes_batched_and_failures_individually() {
        let repo = InMemoryRepository::new();
        for id in ["evt-1", "evt-2", "evt-3"] {
            store_message(&repo, outbox(id));
        }
        let dispatcher = OutboxDispatcher::new(
            repo.outbox_store(),
            SelectiveFailPublisher {
                fail_id: "evt-2".to_string(),
                published: Mutex::new(Vec::new()),
            },
            "immediate:test",
            Duration::from_secs(60),
            3,
        );

        let outcome = block_on(dispatcher.dispatch_batch(10)).unwrap();

        assert_eq!(outcome.claimed, 3);
        assert_eq!(outcome.published, 2);
        assert_eq!(outcome.released, 1);
        assert!(!repo.outbox_storage().read().unwrap().contains_key("evt-1"));
        assert!(!repo.outbox_storage().read().unwrap().contains_key("evt-3"));
        // The failed row is released for retry, untouched by the batched complete.
        let failed = load(&repo, "evt-2");
        assert!(failed.is_pending());
        assert_eq!(failed.attempts, 1);
    }

    #[test]
    fn publish_concurrency_above_one_still_settles_every_row() {
        let repo = InMemoryRepository::new();
        for id in ["evt-1", "evt-2", "evt-3"] {
            store_message(&repo, outbox(id));
        }
        let dispatcher =
            dispatcher(&repo, false, 3).with_publish_concurrency(NonZeroUsize::new(4).unwrap());

        let outcome = block_on(dispatcher.dispatch_batch(10)).unwrap();

        assert_eq!(outcome.claimed, 3);
        assert_eq!(outcome.published, 3);
        for id in ["evt-1", "evt-2", "evt-3"] {
            assert!(!repo.outbox_storage().read().unwrap().contains_key(id));
        }
        assert_eq!(dispatcher.publisher.ids().len(), 3);
    }

    #[test]
    fn default_concurrency_publishes_in_claim_order() {
        use std::time::SystemTime;
        let repo = InMemoryRepository::new();
        // Claim order is created-at then id; pin created-at so the id
        // tiebreaker decides, and store in shuffled id order.
        for id in ["evt-b", "evt-a", "evt-c"] {
            let mut message = outbox(id);
            message.created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
            store_message(&repo, message);
        }
        let dispatcher = dispatcher(&repo, false, 3);

        block_on(dispatcher.dispatch_batch(10)).unwrap();

        // created_at ties (same timestamp) break by id, and concurrency 1
        // preserves that claim order on the wire.
        assert_eq!(
            dispatcher.publisher.ids(),
            vec![
                "evt-a".to_string(),
                "evt-b".to_string(),
                "evt-c".to_string()
            ]
        );
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn metrics_record_publish_outcomes_and_backlog_gauges() {
        let _guard = crate::metrics::lock_for_tests();
        crate::metrics::reset_for_tests();

        let repo = InMemoryRepository::new();
        let id = store_message(&repo, outbox("evt-1"));
        let _pending = store_message(&repo, outbox("evt-2"));
        let dispatcher = dispatcher(&repo, false, 3).with_service("orders");

        block_on(dispatcher.dispatch_ids(std::slice::from_ref(&id))).unwrap();

        let text = crate::metrics::prometheus_text();
        assert!(
            text.contains(
                "distributed_outbox_messages_total{service=\"orders\",outcome=\"published\"} 1"
            ),
            "metrics should include published outbox count:\n{text}"
        );
        assert!(
            text.contains("distributed_outbox_pending_messages{service=\"orders\"} 1"),
            "metrics should include pending outbox gauge:\n{text}"
        );
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn backlog_gauges_observe_latest_backlog_on_every_refresh() {
        let _guard = crate::metrics::lock_for_tests();
        crate::metrics::reset_for_tests();

        let repo = InMemoryRepository::new();
        store_message(&repo, outbox("evt-1"));
        block_on(record_backlog_gauges(
            &repo.outbox_store(),
            Some("orders-refresh"),
        ));
        store_message(&repo, outbox("evt-2"));
        block_on(record_backlog_gauges(
            &repo.outbox_store(),
            Some("orders-refresh"),
        ));

        let text = crate::metrics::prometheus_text();
        assert!(
            text.contains("distributed_outbox_pending_messages{service=\"orders-refresh\"} 2"),
            "every refresh must observe the current backlog, never a stale snapshot:\n{text}"
        );
    }
}
