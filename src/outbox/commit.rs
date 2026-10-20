use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::aggregate::{Aggregate, AggregateRepository};
use crate::domain_event::{DomainEvent, DomainEventCaptureError, DomainEventCommitGuardError};
use crate::outbox::{OutboxMessage, PreparedDomainEvent};
use crate::read_model::ReadModelWritePlanBuilder;
use crate::repository::{
    CommitBatch, RepositoryError, StreamIdentity, StreamWrite, TransactionalCommit,
};
use crate::table::TableWritePlan;

/// Publishes already-committed outbox rows and settles their claims.
///
/// Implemented by the outbox → bus bridge. `Service::with_bus` prefers a
/// bounded worker mailbox over this hook; the hook is the fallback when no
/// mailbox is installed. It is object-safe so the repository can hold it
/// without naming the transport/store types.
pub trait OutboxPublishHook: Send + Sync {
    /// Publish committed, claimed outbox rows and settle their claims. Publish
    /// failures are absorbed (the rows stay retryable for the worker); only a
    /// store error surfaces.
    fn publish_claimed<'a>(
        &'a self,
        claimed: Vec<OutboxMessage>,
    ) -> Pin<Box<dyn Future<Output = Result<(), RepositoryError>> + Send + 'a>>;
}

/// Outbox publisher installed on a repository so commits publish immediately.
pub struct OutboxPublisherConfig {
    pub(crate) hook: Arc<dyn OutboxPublishHook>,
    pub(crate) worker_id: String,
    pub(crate) lease: Duration,
    /// Bounded worker mailbox. When set, commit `try_send`s ids and returns;
    /// the worker claims and publishes. When unset, the hook is the fallback
    /// (tests, no-runtime builds).
    pub(crate) schedule: Option<Arc<dyn Fn(Vec<String>) + Send + Sync>>,
}

/// Detach immediate publish from command completion.
///
/// Prefer `config.schedule`: enqueue ids onto the bounded worker and return.
/// Without a mailbox, the hook runs on a spawned task when a Tokio runtime is
/// current, or inline when it is not, so publish is not dropped.
pub(crate) async fn start_immediate_publish(
    config: &OutboxPublisherConfig,
    ids: Vec<String>,
    fallback_rows: Vec<OutboxMessage>,
) {
    if let Some(schedule) = &config.schedule {
        if !ids.is_empty() {
            schedule(ids);
        }
        return;
    }
    if fallback_rows.is_empty() {
        return;
    }
    let hook = Arc::clone(&config.hook);
    #[cfg(any(
        feature = "http",
        feature = "grpc",
        feature = "postgres",
        feature = "sqlite",
        feature = "nats",
        feature = "rabbitmq",
        feature = "kafka",
        test,
    ))]
    {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _ = handle.spawn(async move {
                let _ = hook.publish_claimed(fallback_rows).await;
            });
            return;
        }
        let _ = hook.publish_claimed(fallback_rows).await;
    }
    #[cfg(not(any(
        feature = "http",
        feature = "grpc",
        feature = "postgres",
        feature = "sqlite",
        feature = "nats",
        feature = "rabbitmq",
        feature = "kafka",
        test,
    )))]
    {
        let _ = hook.publish_claimed(fallback_rows).await;
    }
}

impl OutboxPublisherConfig {
    /// Build the config from a publish hook, the worker id used to scope
    /// claims, and the publish lease.
    pub fn new(
        hook: Arc<dyn OutboxPublishHook>,
        worker_id: impl Into<String>,
        lease: Duration,
    ) -> Self {
        Self {
            hook,
            worker_id: worker_id.into(),
            lease,
            schedule: None,
        }
    }

    /// Install a non-blocking after-commit scheduler (bounded worker mailbox).
    pub fn with_schedule(mut self, schedule: Arc<dyn Fn(Vec<String>) + Send + Sync>) -> Self {
        self.schedule = Some(schedule);
        self
    }
}

/// Outcome of an outbox-bearing commit.
///
/// Carries the ids of the outbox rows the transaction inserted so an
/// after-commit dispatcher knows exactly which rows to publish. This is the seam
/// the immediate-dispatch path hangs off (see
/// `specs/durable-enqueue-outbox-dispatch`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitReceipt {
    /// Ids of the outbox messages inserted by this commit, in insertion order.
    pub outbox_message_ids: Vec<String>,
}

impl CommitReceipt {
    /// The outbox message ids inserted by this commit.
    pub fn outbox_message_ids(&self) -> &[String] {
        &self.outbox_message_ids
    }

    /// Whether this commit inserted any outbox messages.
    pub fn has_outbox_messages(&self) -> bool {
        !self.outbox_message_ids.is_empty()
    }
}

/// Builder returned by [`AggregateRepository::outbox`] and
/// [`AggregateRepository::read_models`] that commits an aggregate together with
/// outbox rows, relational read-model writes, and (when the repository has
/// snapshots configured) a snapshot — all in one async transactional batch.
///
/// Borrows the repository so it can be called through `ctx.repo()` inside async
/// handlers. Chain `.outbox(..)` / `.read_models(..)` to stage more, then
/// `.commit(&mut aggregate)`.
pub struct AggregateCommit<'a, R, A> {
    repo: &'a AggregateRepository<R, A>,
    publish_captured_events: bool,
    explicit_domain_events: Vec<PreparedDomainEvent>,
    outbox_messages: Vec<OutboxMessage>,
    read_model_plans: Vec<TableWritePlan>,
    error: Option<RepositoryError>,
}

impl<'a, R, A> AggregateCommit<'a, R, A> {
    fn empty(repo: &'a AggregateRepository<R, A>) -> Self {
        Self {
            repo,
            publish_captured_events: false,
            explicit_domain_events: Vec::new(),
            outbox_messages: Vec::new(),
            read_model_plans: Vec::new(),
            error: None,
        }
    }

    /// Publish every canonical domain-event occurrence captured by the
    /// aggregate transitions committed by this unit of work.
    pub fn publish_events(mut self) -> Self {
        self.publish_captured_events = true;
        self
    }

    /// Publish one explicit outward event, bound to the aggregate's final newly
    /// recorded transition when [`commit`](Self::commit) is called.
    ///
    /// Serialization is attempted before repository I/O and retained as a
    /// builder error so the fluent chain remains linear.
    pub fn publish<E: DomainEvent>(mut self, event: E) -> Self {
        if self.error.is_none() {
            match PreparedDomainEvent::new(event) {
                Ok(event) => self.explicit_domain_events.push(event),
                Err(error) => self.error = Some(domain_event_repository_error(error)),
            }
        }
        self
    }

    /// Stage an outbox message to publish/enqueue with the commit.
    ///
    /// This is the low-level integration-envelope escape hatch. Ordinary
    /// aggregate-derived publication should use [`publish_events`](Self::publish_events)
    /// or [`publish`](Self::publish).
    pub fn outbox(mut self, message: OutboxMessage) -> Self {
        self.outbox_messages.push(message);
        self
    }

    /// Stage relational read-model writes to apply in the same transaction.
    pub fn read_models(mut self, read_models: ReadModelWritePlanBuilder) -> Self {
        if self.error.is_none() {
            match read_models.into_write_plan() {
                Ok(plan) => self.read_model_plans.push(plan),
                Err(err) => self.error = Some(err.into()),
            }
        }
        self
    }
}

impl<R, A> AggregateCommit<'_, R, A>
where
    R: TransactionalCommit,
    A: Aggregate + Send,
{
    /// Commit the aggregate together with the staged outbox rows, read-model
    /// writes, and a snapshot (when due) in one transaction. Command completion
    /// is this commit. When the repository has a bus (`Service::with_bus`),
    /// pending outbox ids are handed to a bounded worker after commit and do
    /// not delay the returned receipt.
    ///
    /// Rows stay **pending**. The worker claims them via `dispatch_ids` (or
    /// `dispatch_batch` on overflow/poll). A crash before publish leaves them
    /// pending for the drain loop. A publish failure leaves them retryable.
    /// Without a bus, rows stay pending for a separately operated worker.
    ///
    /// Returns a [`CommitReceipt`] carrying the inserted outbox message ids.
    pub async fn commit(mut self, aggregate: &mut A) -> Result<CommitReceipt, RepositoryError> {
        if let Some(err) = self.error.take() {
            return Err(err);
        }
        self.prepare_domain_publications(aggregate)?;
        for message in &mut self.outbox_messages {
            if message.source_aggregate_type.is_none()
                && message.source_aggregate_id.is_none()
                && message.source_sequence.is_none()
            {
                message.set_source(aggregate);
            }
        }
        let outbox_message_ids: Vec<String> = self
            .outbox_messages
            .iter()
            .map(|message| message.id().to_string())
            .collect();

        let publisher = self.repo.outbox_publisher();
        let mut fallback_rows = Vec::new();
        if let Some(config) = publisher {
            if config.schedule.is_none() {
                let now = crate::time::now();
                // Scheduled (not yet due) rows stay pending for the worker
                // that honours `deliver_at`; only due rows are pre-claimed.
                for message in &mut self.outbox_messages {
                    if message.is_due_at(now) {
                        message.claim_at(&config.worker_id, config.lease, now)?;
                        fallback_rows.push(message.clone());
                    }
                }
            }
        }

        let (snapshots, snapshot_version) = self.repo.snapshot_writes_for(aggregate)?;
        let identity = StreamIdentity::new(A::aggregate_type(), aggregate.entity().id())?;
        let stream = StreamWrite::new(identity, aggregate.entity_mut());
        self.repo
            .repo()
            .commit_batch(CommitBatch {
                streams: vec![stream],
                outbox_messages: self.outbox_messages,
                read_model_plans: self.read_model_plans,
                snapshots,
                inbox_receipts: Vec::new(),
                alarms: Vec::new(),
            })
            .await?;
        if let Some(version) = snapshot_version {
            aggregate.entity_mut().set_snapshot_version(version);
        }
        aggregate
            .entity_mut()
            .mark_domain_events_committed()
            .map_err(domain_event_guard_repository_error)?;

        // Best-effort after-commit publish. Command completion is this commit;
        // publish must not delay the caller.
        if let Some(config) = publisher {
            start_immediate_publish(config, outbox_message_ids.clone(), fallback_rows).await;
        }

        Ok(CommitReceipt { outbox_message_ids })
    }

    fn prepare_domain_publications(&mut self, aggregate: &A) -> Result<(), RepositoryError> {
        let entity = aggregate.entity();
        entity
            .domain_event_commit_guard()
            .map_err(domain_event_guard_repository_error)?;
        let pending = entity
            .pending_domain_events_for_commit()
            .map_err(domain_event_guard_repository_error)?;
        if !pending.is_empty() && !self.publish_captured_events {
            return Err(RepositoryError::Model(
                "aggregate has captured domain events; add `publish_events()` to the commit".into(),
            ));
        }

        if self.publish_captured_events {
            self.outbox_messages.extend(
                pending
                    .iter()
                    .map(OutboxMessage::from_domain_event_occurrence)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(domain_event_repository_error)?,
            );
        }

        let current_sequence = aggregate.entity().version();
        let ordinal_base = pending
            .iter()
            .filter(|occurrence| occurrence.aggregate_sequence() == current_sequence)
            .count();
        for (offset, event) in self.explicit_domain_events.iter().enumerate() {
            let ordinal: u32 = ordinal_base
                .checked_add(offset)
                .and_then(|ordinal| ordinal.try_into().ok())
                .ok_or_else(|| {
                    domain_event_repository_error(
                        DomainEventCaptureError::PublicationOrdinalOverflow,
                    )
                })?;
            let occurrence = event
                .bind(aggregate, ordinal)
                .map_err(domain_event_repository_error)?;
            self.outbox_messages.push(
                OutboxMessage::from_domain_event_occurrence(&occurrence)
                    .map_err(domain_event_repository_error)?,
            );
        }
        Ok(())
    }
}

impl<R, A> AggregateRepository<R, A> {
    /// Start an aggregate commit that publishes its exact captured domain
    /// events.
    pub fn publish_events(&self) -> AggregateCommit<'_, R, A> {
        AggregateCommit::empty(self).publish_events()
    }

    /// Start an aggregate commit with one explicitly authored outward event.
    pub fn publish<E: DomainEvent>(&self, event: E) -> AggregateCommit<'_, R, A> {
        AggregateCommit::empty(self).publish(event)
    }

    /// Start a commit with an outbox message attached.
    pub fn outbox(&self, message: OutboxMessage) -> AggregateCommit<'_, R, A> {
        AggregateCommit::empty(self).outbox(message)
    }

    /// Start a commit with relational read-model writes attached. Composes with
    /// `.outbox(..)`, the aggregate's events, and snapshots in one transaction.
    pub fn read_models(&self, read_models: ReadModelWritePlanBuilder) -> AggregateCommit<'_, R, A> {
        AggregateCommit::empty(self).read_models(read_models)
    }
}

fn domain_event_repository_error(error: DomainEventCaptureError) -> RepositoryError {
    RepositoryError::Model(format!(
        "domain-event publication preparation failed: {error}"
    ))
}

fn domain_event_guard_repository_error(error: DomainEventCommitGuardError) -> RepositoryError {
    RepositoryError::Model(format!("domain-event commit guard failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{sourced, AggregateBuilder, Entity, InMemoryRepository, OutboxStore};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Dummy {
        entity: Entity,
    }

    #[sourced(entity)]
    impl Dummy {
        #[event("Touched")]
        fn touch(&mut self) {
            if self.entity.id().is_empty() {
                self.entity.set_id("dummy-1");
            }
        }
    }

    #[derive(Default)]
    struct FailingOutboxRepo {
        seen_ids: Mutex<Vec<String>>,
        seen_outbox: Mutex<Vec<OutboxMessage>>,
    }

    impl TransactionalCommit for FailingOutboxRepo {
        async fn commit_batch<'a>(&'a self, batch: CommitBatch<'a>) -> Result<(), RepositoryError> {
            {
                *self.seen_ids.lock().unwrap() = batch
                    .streams
                    .iter()
                    .map(|stream| stream.entity.id().to_string())
                    .chain(
                        batch
                            .outbox_messages
                            .iter()
                            .map(|message| message.id().to_string()),
                    )
                    .collect();
                *self.seen_outbox.lock().unwrap() = batch.outbox_messages.clone();

                Err(RepositoryError::Model("outbox write failed".into()))
            }
        }
    }

    struct HoldingHook {
        started: tokio::sync::Notify,
        gate: tokio::sync::Notify,
        finished: Mutex<bool>,
    }

    impl OutboxPublishHook for HoldingHook {
        fn publish_claimed<'a>(
            &'a self,
            claimed: Vec<OutboxMessage>,
        ) -> Pin<Box<dyn Future<Output = Result<(), RepositoryError>> + Send + 'a>> {
            Box::pin(async move {
                let _ = claimed;
                self.started.notify_waiters();
                self.gate.notified().await;
                *self.finished.lock().unwrap() = true;
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn immediate_publish_does_not_delay_commit() {
        let hook = Arc::new(HoldingHook {
            started: tokio::sync::Notify::new(),
            gate: tokio::sync::Notify::new(),
            finished: Mutex::new(false),
        });
        let mut repo = InMemoryRepository::new().aggregate::<Dummy>();
        repo.set_outbox_publisher(OutboxPublisherConfig::new(
            Arc::clone(&hook) as Arc<dyn OutboxPublishHook>,
            "immediate:test",
            Duration::from_secs(5),
        ));

        let mut aggregate = Dummy::default();
        aggregate.touch().unwrap();
        let event = OutboxMessage::create("msg-hold", "DummyTouched", b"{}".to_vec()).unwrap();

        let started = hook.started.notified();
        let receipt = repo.outbox(event).commit(&mut aggregate).await.unwrap();
        assert_eq!(receipt.outbox_message_ids(), ["msg-hold".to_string()]);
        assert!(
            !*hook.finished.lock().unwrap(),
            "commit must return before the holding publish hook finishes"
        );

        tokio::time::timeout(Duration::from_secs(1), started)
            .await
            .expect("immediate publish should start");
        hook.gate.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if *hook.finished.lock().unwrap() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("immediate publish should finish after the gate opens");
    }

    struct RecordingHook {
        published: Mutex<Vec<String>>,
    }

    impl OutboxPublishHook for RecordingHook {
        fn publish_claimed<'a>(
            &'a self,
            claimed: Vec<OutboxMessage>,
        ) -> Pin<Box<dyn Future<Output = Result<(), RepositoryError>> + Send + 'a>> {
            Box::pin(async move {
                self.published
                    .lock()
                    .unwrap()
                    .extend(claimed.iter().map(|m| m.id().to_string()));
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn immediate_publish_leaves_scheduled_rows_pending() {
        let hook = Arc::new(RecordingHook {
            published: Mutex::new(Vec::new()),
        });
        let mut repo = InMemoryRepository::new().aggregate::<Dummy>();
        repo.set_outbox_publisher(OutboxPublisherConfig::new(
            Arc::clone(&hook) as Arc<dyn OutboxPublishHook>,
            "immediate:test",
            Duration::from_secs(5),
        ));
        let mut aggregate = Dummy::default();
        aggregate.touch().unwrap();
        let now = crate::time::now();
        let due = OutboxMessage::create("due", "T", vec![]).unwrap();
        let timer = OutboxMessage::create("timer", "T", vec![])
            .unwrap()
            .deliver_after(now, Duration::from_secs(3600));
        repo.outbox(due)
            .outbox(timer)
            .commit(&mut aggregate)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while hook.published.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(*hook.published.lock().unwrap(), vec!["due".to_string()]);
        let pending = repo.repo().outbox_store().pending(10).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id(), "timer");
        assert!(pending[0].deliver_at.is_some());
    }

    #[tokio::test]
    async fn outbox_helper_commits_both_entities() {
        let repo = InMemoryRepository::new().aggregate::<Dummy>();

        let mut aggregate = Dummy::default();
        aggregate.touch().unwrap();

        let event = OutboxMessage::create("msg-1", "DummyTouched", b"{}".to_vec()).unwrap();

        let receipt = repo.outbox(event).commit(&mut aggregate).await.unwrap();

        // The receipt reports the inserted outbox row so an after-commit
        // dispatcher knows what to publish.
        assert!(receipt.has_outbox_messages());
        assert_eq!(receipt.outbox_message_ids(), ["msg-1".to_string()]);

        let pending = repo
            .repo()
            .outbox_store()
            .pending(usize::MAX)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id(), "msg-1");
    }

    #[tokio::test]
    async fn outbox_helper_failure_leaves_entities_uncommitted() {
        let repo = AggregateRepository::<_, Dummy>::new(FailingOutboxRepo::default());

        let mut aggregate = Dummy::default();
        aggregate.touch().unwrap();

        let event = OutboxMessage::create("msg-fail", "DummyTouched", b"{}".to_vec()).unwrap();

        let err = repo.outbox(event).commit(&mut aggregate).await.unwrap_err();

        assert!(
            matches!(&err, RepositoryError::Model(message) if message == "outbox write failed"),
            "unexpected error: {err}"
        );
        assert_eq!(aggregate.entity.committed_version(), 0);
        assert_eq!(aggregate.entity.new_events().len(), 1);
        assert_eq!(
            repo.repo().seen_ids.lock().unwrap().as_slice(),
            &["dummy-1".to_string(), "msg-fail".to_string()]
        );
    }

    #[derive(
        Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, crate::DomainState,
    )]
    #[domain_state(version = 1)]
    struct PublishedCounterState {
        id: String,
        value: i64,
    }

    #[derive(Default)]
    struct PublishedCounter {
        entity: Entity,
        value: i64,
    }

    impl From<&PublishedCounter> for PublishedCounterState {
        fn from(counter: &PublishedCounter) -> Self {
            Self {
                id: counter.entity.id().to_string(),
                value: counter.value,
            }
        }
    }

    #[sourced(
        entity,
        aggregate_type = "published_counter",
        domain_state = PublishedCounterState
    )]
    impl PublishedCounter {
        #[event("counter.opened", version = 1, domain)]
        fn open(&mut self, id: String) {
            self.entity.set_id(id);
        }

        #[event("counter.bumped", version = 1, domain)]
        fn bump(&mut self) {
            self.value += 1;
        }
    }

    #[derive(
        Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, crate::DomainEvent,
    )]
    #[domain_event(name = "counter.notified", version = 1)]
    struct CounterNotified {
        public_value: i64,
    }

    #[tokio::test]
    async fn publish_events_commits_canonical_occurrences_and_clears_them_once() {
        let repo = InMemoryRepository::new().aggregate::<PublishedCounter>();
        let mut counter = PublishedCounter::default();
        counter.open("counter-1".into()).unwrap();
        counter.bump().unwrap();

        let captured = counter.entity.pending_domain_events().to_vec();
        assert_eq!(captured.len(), 2);

        let receipt = repo.publish_events().commit(&mut counter).await.unwrap();
        assert_eq!(receipt.outbox_message_ids().len(), 2);
        assert!(counter.entity.pending_domain_events().is_empty());

        let pending = repo
            .repo()
            .outbox_store()
            .pending(usize::MAX)
            .await
            .unwrap();
        assert_eq!(pending.len(), 2);
        for (message, expected) in pending.iter().zip(captured) {
            let occurrence = message.domain_event_occurrence().unwrap();
            assert_eq!(occurrence, expected);
            assert_eq!(
                message.source_aggregate_type.as_deref(),
                Some("published_counter")
            );
            assert_eq!(message.source_aggregate_id.as_deref(), Some("counter-1"));
        }
    }

    #[tokio::test]
    async fn publication_failure_retains_identical_occurrences_for_retry() {
        let repo = AggregateRepository::<_, PublishedCounter>::new(FailingOutboxRepo::default());
        let mut counter = PublishedCounter::default();
        counter.open("counter-fail".into()).unwrap();
        let before = counter.entity.pending_domain_events()[0].clone();
        let before_bytes = before.canonical_bytes().unwrap();

        let error = repo
            .publish_events()
            .commit(&mut counter)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RepositoryError::Model(ref message) if message == "outbox write failed"
        ));
        assert_eq!(counter.entity.committed_version(), 0);
        assert_eq!(counter.entity.pending_domain_events(), [before]);

        let seen = repo.repo().seen_outbox.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].payload, before_bytes);
        assert_eq!(
            seen[0].domain_event_occurrence().unwrap(),
            counter.entity.pending_domain_events()[0]
        );
    }

    #[tokio::test]
    async fn captured_occurrences_require_an_explicit_publication_leg() {
        let repo = InMemoryRepository::new().aggregate::<PublishedCounter>();
        let mut counter = PublishedCounter::default();
        counter.open("counter-guard".into()).unwrap();

        let error = repo
            .read_models(ReadModelWritePlanBuilder::new())
            .commit(&mut counter)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RepositoryError::Model(ref message)
                if message.contains("add `publish_events()`")
        ));
        assert_eq!(counter.entity.committed_version(), 0);
        assert_eq!(counter.entity.pending_domain_events().len(), 1);
        assert!(repo
            .repo()
            .outbox_store()
            .pending(usize::MAX)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn explicit_publish_serializes_its_dto_instead_of_replay_bytes() {
        use crate::DomainEventBodyKind;

        let repo = InMemoryRepository::new().aggregate::<Dummy>();
        let mut aggregate = Dummy::default();
        aggregate.touch().unwrap();
        let replay_bytes = aggregate.entity.new_events()[0].payload_bytes().to_vec();

        repo.publish(CounterNotified { public_value: 7 })
            .commit(&mut aggregate)
            .await
            .unwrap();

        let pending = repo
            .repo()
            .outbox_store()
            .pending(usize::MAX)
            .await
            .unwrap();
        let occurrence = pending[0].domain_event_occurrence().unwrap();
        assert_eq!(
            occurrence
                .decode_body::<CounterNotified>()
                .unwrap()
                .public_value,
            7
        );
        assert_eq!(
            occurrence.descriptor().body.kind,
            DomainEventBodyKind::Event
        );
        assert_ne!(occurrence.body_bytes(), replay_bytes);
        assert_eq!(occurrence.aggregate_id(), "dummy-1");
        assert_eq!(occurrence.aggregate_sequence(), 1);
    }

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, crate::ReadModel)]
    #[table("agg_commit_views")]
    struct ComposeView {
        #[id]
        id: String,
        n: i32,
    }

    #[derive(Default, crate::Snapshot)]
    struct ComposeCounter {
        entity: Entity,
        value: i64,
    }

    #[sourced(entity, aggregate_type = "agg_commit_counter")]
    impl ComposeCounter {
        #[event("bumped")]
        fn bump(&mut self, id: String) {
            self.entity.set_id(&id);
            self.value += 1;
        }
    }

    #[tokio::test]
    async fn read_models_and_snapshot_commit_in_one_transaction() {
        use crate::read_model::ReadModelWritePlanBuilder;
        use crate::{
            Aggregate, ReadModelWorkspaceExt, RowKey, RowValue, SnapshotStore, StreamIdentity,
        };

        let repo = InMemoryRepository::new()
            .aggregate::<ComposeCounter>()
            .with_snapshots(1);

        let mut counter = ComposeCounter::default();
        counter.bump("c1".to_string()).unwrap();

        let mut plan = ReadModelWritePlanBuilder::new();
        plan.upsert(&ComposeView {
            id: "c1".into(),
            n: 1,
        })
        .unwrap();

        // Read-model writes + the aggregate's events + a snapshot — one commit.
        repo.read_models(plan).commit(&mut counter).await.unwrap();

        // The read-model row is committed...
        let loaded = repo
            .repo()
            .model_store()
            .workspace()
            .load::<ComposeView>(RowKey::new([("id", RowValue::String("c1".into()))]))
            .one()
            .await
            .unwrap();
        assert!(loaded.is_some(), "read-model row should be committed");

        // ...and a snapshot was staged in the same transaction (frequency 1).
        let identity = StreamIdentity::new(ComposeCounter::aggregate_type(), "c1").unwrap();
        let snapshot = repo.repo().get_snapshot(&identity).await.unwrap();
        assert!(
            snapshot.is_some(),
            "snapshot should be staged alongside the read-model commit"
        );
    }

    #[tokio::test]
    async fn aggregate_outbox_read_model_and_snapshot_commit_in_one_transaction() {
        use crate::read_model::ReadModelWritePlanBuilder;
        use crate::{
            Aggregate, GetStream, ReadModelWorkspaceExt, RowKey, RowValue, SnapshotStore,
            StreamIdentity,
        };

        let repo = InMemoryRepository::new()
            .aggregate::<ComposeCounter>()
            .with_snapshots(1);

        let mut counter = ComposeCounter::default();
        counter.bump("c1".to_string()).unwrap();

        let mut plan = ReadModelWritePlanBuilder::new();
        plan.upsert(&ComposeView {
            id: "c1".into(),
            n: 1,
        })
        .unwrap();

        let message = OutboxMessage::create("evt-c1", "counter.bumped", b"{}".to_vec()).unwrap();

        // All four in one commit: aggregate events + outbox row + read-model
        // write + snapshot.
        let receipt = repo
            .outbox(message)
            .read_models(plan)
            .commit(&mut counter)
            .await
            .unwrap();
        assert_eq!(receipt.outbox_message_ids(), ["evt-c1".to_string()]);

        let identity = StreamIdentity::new(ComposeCounter::aggregate_type(), "c1").unwrap();

        // 1) aggregate stream committed
        assert!(
            repo.repo().get_stream(&identity).await.unwrap().is_some(),
            "aggregate stream should be committed"
        );

        // 2) outbox row present (pending — no bus attached here)
        let pending = repo
            .repo()
            .outbox_store()
            .pending(usize::MAX)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id(), "evt-c1");

        // 3) read-model row written
        let loaded = repo
            .repo()
            .model_store()
            .workspace()
            .load::<ComposeView>(RowKey::new([("id", RowValue::String("c1".into()))]))
            .one()
            .await
            .unwrap();
        assert!(loaded.is_some(), "read-model row should be committed");

        // 4) snapshot staged
        assert!(
            repo.repo().get_snapshot(&identity).await.unwrap().is_some(),
            "snapshot should be staged"
        );
    }
}
