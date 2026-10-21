//! CommitBuilder chains read models, write plans, outbox, and aggregates
//! into one transactional batch.
//!
//! ## Example
//!
//! ```ignore
//! let mut read_models = distributed::read_model::ReadModelWritePlanBuilder::new();
//! read_models.upsert(&player)?;
//! read_models.upsert_related(&player, "weapons", &weapon)?;
//!
//! repo
//!     .read_models(read_models)
//!     .commit(&mut game)
//!     .await?;
//! ```
//!
//! ## Outbox source stamping
//!
//! One rule, for every entry point: outbox messages in the batch are stamped
//! with a source aggregate exactly when **one distinct aggregate** is staged
//! (via [`CommitBuilder::commit`], [`StagedCommitBuilder::aggregate`], or a
//! single-element [`CommitBuilder::commit_many`]). With zero or several
//! distinct aggregates the source is ambiguous and messages keep whatever
//! source they already carry. Raw [`StagedCommitBuilder::entity`] writes never
//! stamp.

use crate::aggregate::Aggregate;
use crate::entity::Entity;
use crate::outbox::OutboxMessage;
use crate::read_model::ReadModelWritePlanBuilder;
use crate::repository::{
    CommitBatch, RepositoryError, StreamIdentity, StreamWrite, TransactionalCommit,
};
use crate::table::TableWritePlan;

/// The source-aggregate fields stamped onto outbox messages at commit time.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SourceStamp {
    aggregate_type: String,
    aggregate_id: String,
    source_sequence: u64,
}

impl SourceStamp {
    fn from_aggregate<A: Aggregate>(aggregate: &A) -> Self {
        Self {
            aggregate_type: A::aggregate_type().to_string(),
            aggregate_id: aggregate.entity().id().to_string(),
            source_sequence: aggregate.entity().version(),
        }
    }

    fn apply_to(&self, message: &mut OutboxMessage) {
        message.source_aggregate_type = Some(self.aggregate_type.clone());
        message.source_aggregate_id = Some(self.aggregate_id.clone());
        message.source_sequence = Some(self.source_sequence);
    }
}

/// The staged outbox source: the single distinct aggregate in the batch, if any.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
enum StagedOutboxSource {
    #[default]
    None,
    Single(SourceStamp),
    Ambiguous,
}

impl StagedOutboxSource {
    fn record(&mut self, source: SourceStamp) {
        match self {
            Self::None => *self = Self::Single(source),
            Self::Single(existing) if *existing == source => {}
            Self::Single(_) => *self = Self::Ambiguous,
            Self::Ambiguous => {}
        }
    }

    fn apply_to(&self, messages: &mut [OutboxMessage]) {
        let Self::Single(source) = self else {
            return;
        };

        for message in messages {
            source.apply_to(message);
        }
    }
}

/// Builder for chaining multiple items into one transactional commit batch.
///
/// This is the single builder behind every commit entry point; staging state is
/// the stream writes plus the [`StagedOutboxSource`]. [`aggregate`](Self::aggregate)
/// hands back a [`StagedCommitBuilder`], whose no-argument `commit()` finishes
/// the same batch.
pub struct CommitBuilder<'a, R> {
    repo: &'a R,
    streams: Vec<StreamWrite<'a>>,
    outbox_messages: Vec<OutboxMessage>,
    outbox_source: StagedOutboxSource,
    read_model_plans: Vec<TableWritePlan>,
    error: Option<RepositoryError>,
}

impl<'a, R> CommitBuilder<'a, R> {
    pub fn new(repo: &'a R) -> Self {
        Self {
            repo,
            streams: Vec::new(),
            outbox_messages: Vec::new(),
            outbox_source: StagedOutboxSource::default(),
            read_model_plans: Vec::new(),
            error: None,
        }
    }

    /// Add a read-model write plan builder to the commit.
    pub fn read_models(mut self, read_models: ReadModelWritePlanBuilder) -> Self {
        if self.error.is_some() {
            return self;
        }

        match read_models.into_write_plan() {
            Ok(plan) => self.read_model_plans.push(plan),
            Err(err) => self.error = Some(err.into()),
        }
        self
    }

    /// Add an outbox message to the commit.
    pub fn outbox(mut self, msg: OutboxMessage) -> Self {
        self.outbox_messages.push(msg);
        self
    }

    /// Stage an aggregate and switch to a no-argument staged commit builder.
    pub fn aggregate<A: Aggregate>(mut self, aggregate: &'a mut A) -> StagedCommitBuilder<'a, R> {
        self.push_aggregate(aggregate);
        StagedCommitBuilder(self)
    }

    /// Commit all items plus the primary aggregate.
    ///
    /// Sugar for [`aggregate(aggregate)`](Self::aggregate)`.commit()`.
    pub async fn commit<A: Aggregate + Send>(
        self,
        aggregate: &'a mut A,
    ) -> Result<(), RepositoryError>
    where
        R: TransactionalCommit,
    {
        self.aggregate(aggregate).commit().await
    }

    /// Commit multiple aggregates of the same type in one batch.
    pub async fn commit_many<'b, A: Aggregate + Send>(
        mut self,
        aggregates: &'a mut [&'b mut A],
    ) -> Result<(), RepositoryError>
    where
        R: TransactionalCommit,
    {
        for aggregate in aggregates.iter_mut() {
            self.push_aggregate(&mut **aggregate);
        }
        self.commit_all().await
    }

    /// Commit without a primary aggregate.
    pub async fn commit_all(mut self) -> Result<(), RepositoryError>
    where
        R: TransactionalCommit,
    {
        if let Some(err) = self.error.take() {
            return Err(err);
        }
        self.outbox_source.apply_to(&mut self.outbox_messages);
        self.repo
            .commit_batch(CommitBatch {
                streams: self.streams,
                outbox_messages: self.outbox_messages,
                read_model_plans: self.read_model_plans,
                snapshots: Vec::new(),
                inbox_receipts: Vec::new(),
                schedules: Vec::new(),
            })
            .await
    }

    fn push_aggregate<A: Aggregate>(&mut self, aggregate: &'a mut A) {
        if self.error.is_some() {
            return;
        }

        match StreamIdentity::new(A::aggregate_type(), aggregate.entity().id()) {
            Ok(identity) => {
                self.outbox_source
                    .record(SourceStamp::from_aggregate(aggregate));
                self.streams
                    .push(StreamWrite::new(identity, aggregate.entity_mut()));
            }
            Err(err) => self.error = Some(err),
        }
    }
}

/// [`CommitBuilder`] with at least one staged write, exposing the no-argument
/// [`commit`](Self::commit). Purely a naming state — all behavior lives on the
/// underlying builder.
pub struct StagedCommitBuilder<'a, R>(CommitBuilder<'a, R>);

impl<'a, R> StagedCommitBuilder<'a, R> {
    pub fn read_models(self, read_models: ReadModelWritePlanBuilder) -> Self {
        Self(self.0.read_models(read_models))
    }

    pub fn outbox(self, msg: OutboxMessage) -> Self {
        Self(self.0.outbox(msg))
    }

    pub fn aggregate<A: Aggregate>(mut self, aggregate: &'a mut A) -> Self {
        self.0.push_aggregate(aggregate);
        self
    }

    pub fn entity(mut self, identity: StreamIdentity, entity: &'a mut Entity) -> Self {
        self.0.streams.push(StreamWrite::new(identity, entity));
        self
    }

    pub async fn commit(self) -> Result<(), RepositoryError>
    where
        R: TransactionalCommit,
    {
        self.0.commit_all().await
    }
}

/// Extension trait to start an async commit builder chain from an outbox message.
pub trait CommitBuilderExt: TransactionalCommit + Sized {
    /// Start an async commit builder chain with an outbox message.
    fn outbox(&self, msg: OutboxMessage) -> CommitBuilder<'_, Self> {
        CommitBuilder::new(self).outbox(msg)
    }
}

impl<R: TransactionalCommit> CommitBuilderExt for R {}

/// Extension trait for async relational read-model write-plan commit entrypoints.
pub trait ReadModelWritePlanCommitExt: TransactionalCommit + Sized {
    /// Start an async commit builder chain with a relational read-model write plan.
    fn read_models(&self, read_models: ReadModelWritePlanBuilder) -> CommitBuilder<'_, Self> {
        CommitBuilder::new(self).read_models(read_models)
    }

    /// Start a staged async commit builder with an aggregate.
    fn aggregate<'a, A: Aggregate>(
        &'a self,
        aggregate: &'a mut A,
    ) -> StagedCommitBuilder<'a, Self> {
        CommitBuilder::new(self).aggregate(aggregate)
    }
}

impl<R: TransactionalCommit> ReadModelWritePlanCommitExt for R {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        sourced, Entity, InMemoryRepository, ReadModelWorkspaceExt, RowKey, RowValue,
        TransactionalCommit,
    };
    use serde::{Deserialize, Serialize};
    use std::sync::Mutex;

    type OutboxSourceRecord = (String, Option<String>, Option<String>, Option<u64>);

    #[derive(Default)]
    struct TestAggregate {
        entity: Entity,
    }

    #[sourced(entity)]
    impl TestAggregate {
        #[event("Touched")]
        fn touch(&mut self) {
            if self.entity.id().is_empty() {
                self.entity.set_id("agg-1");
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone, crate::ReadModel)]
    #[table("commit_builder_views")]
    struct RelationalView {
        #[id]
        id: String,
        counter: i32,
    }

    #[derive(Default)]
    struct RecordingBatchRepo {
        fail: bool,
        stream_ids: Mutex<Vec<(String, String)>>,
        outbox_ids: Mutex<Vec<String>>,
        outbox_sources: Mutex<Vec<OutboxSourceRecord>>,
        read_model_keys: Mutex<Vec<String>>,
    }

    impl TransactionalCommit for RecordingBatchRepo {
        async fn commit_batch<'a>(&'a self, batch: CommitBatch<'a>) -> Result<(), RepositoryError> {
            *self.stream_ids.lock().unwrap() = batch
                .streams
                .iter()
                .map(|stream| {
                    (
                        stream.identity.aggregate_type().to_string(),
                        stream.identity.aggregate_id().to_string(),
                    )
                })
                .collect();
            *self.outbox_ids.lock().unwrap() = batch
                .outbox_messages
                .iter()
                .map(|message| message.id().to_string())
                .collect();
            *self.outbox_sources.lock().unwrap() = batch
                .outbox_messages
                .iter()
                .map(|message| {
                    (
                        message.id().to_string(),
                        message.source_aggregate_type.clone(),
                        message.source_aggregate_id.clone(),
                        message.source_sequence,
                    )
                })
                .collect();
            *self.read_model_keys.lock().unwrap() = batch
                .read_model_plans
                .iter()
                .flat_map(|plan| {
                    plan.mutations
                        .iter()
                        .map(|mutation| mutation.lock_key())
                        .collect::<Vec<_>>()
                })
                .collect();

            if self.fail {
                return Err(RepositoryError::Model(
                    "injected async batch failure".into(),
                ));
            }

            for stream in batch.streams {
                stream.entity.mark_committed();
            }
            Ok(())
        }
    }

    fn read_models(view: &RelationalView) -> crate::read_model::ReadModelWritePlanBuilder {
        let mut read_models = crate::read_model::ReadModelWritePlanBuilder::new();
        read_models.upsert(view).unwrap();
        read_models
    }

    fn view_key(id: &str) -> RowKey {
        RowKey::new([("id", RowValue::String(id.into()))])
    }

    fn lock_key_for(view: &RelationalView) -> String {
        read_models(view)
            .into_write_plan()
            .unwrap()
            .mutations
            .into_iter()
            .next()
            .unwrap()
            .lock_key()
    }

    async fn loaded_view(repo: &InMemoryRepository, id: &str) -> Option<RelationalView> {
        repo.model_store()
            .workspace()
            .load::<RelationalView>(view_key(id))
            .one()
            .await
            .unwrap()
            .map(|versioned| versioned.data)
    }

    #[tokio::test]
    async fn commit_builder_ext_commits_read_models_and_aggregate() {
        let repo = InMemoryRepository::new();

        let view = RelationalView {
            id: "1".into(),
            counter: 42,
        };

        let mut agg = TestAggregate::default();
        agg.touch().unwrap();

        ReadModelWritePlanCommitExt::read_models(&repo, read_models(&view))
            .commit(&mut agg)
            .await
            .unwrap();

        let loaded = loaded_view(&repo, "1").await.unwrap();
        assert_eq!(loaded.counter, 42);
        assert_eq!(agg.entity().committed_version(), 1);
    }

    #[tokio::test]
    async fn commit_multiple_read_models() {
        let repo = InMemoryRepository::new();

        let view1 = RelationalView {
            id: "1".into(),
            counter: 10,
        };
        let view2 = RelationalView {
            id: "2".into(),
            counter: 20,
        };

        let mut agg = TestAggregate::default();
        agg.touch().unwrap();

        let mut read_models = crate::read_model::ReadModelWritePlanBuilder::new();
        read_models.upsert(&view1).unwrap().upsert(&view2).unwrap();

        ReadModelWritePlanCommitExt::read_models(&repo, read_models)
            .commit(&mut agg)
            .await
            .unwrap();

        assert_eq!(loaded_view(&repo, "1").await.unwrap().counter, 10);
        assert_eq!(loaded_view(&repo, "2").await.unwrap().counter, 20);
    }

    #[tokio::test]
    async fn commit_read_models_with_outbox() {
        let repo = InMemoryRepository::new();

        let view = RelationalView {
            id: "1".into(),
            counter: 42,
        };

        let outbox = OutboxMessage::create("msg-1", "TestEvent", b"{}".to_vec()).unwrap();

        let mut agg = TestAggregate::default();
        agg.touch().unwrap();

        ReadModelWritePlanCommitExt::read_models(&repo, read_models(&view))
            .outbox(outbox)
            .commit(&mut agg)
            .await
            .unwrap();

        assert_eq!(loaded_view(&repo, "1").await.unwrap().counter, 42);
    }

    #[tokio::test]
    async fn commit_outbox_then_read_models() {
        let repo = InMemoryRepository::new();

        let view = RelationalView {
            id: "1".into(),
            counter: 99,
        };

        let outbox = OutboxMessage::create("msg-2", "TestEvent", b"{}".to_vec()).unwrap();

        let mut agg = TestAggregate::default();
        agg.touch().unwrap();

        CommitBuilderExt::outbox(&repo, outbox)
            .read_models(read_models(&view))
            .commit(&mut agg)
            .await
            .unwrap();

        assert_eq!(loaded_view(&repo, "1").await.unwrap().counter, 99);
    }

    #[tokio::test]
    async fn commit_all_without_aggregate() {
        let repo = InMemoryRepository::new();

        let view1 = RelationalView {
            id: "standalone-1".into(),
            counter: 1,
        };
        let view2 = RelationalView {
            id: "standalone-2".into(),
            counter: 2,
        };

        let mut read_models = crate::read_model::ReadModelWritePlanBuilder::new();
        read_models.upsert(&view1).unwrap().upsert(&view2).unwrap();

        ReadModelWritePlanCommitExt::read_models(&repo, read_models)
            .commit_all()
            .await
            .unwrap();

        assert_eq!(
            loaded_view(&repo, "standalone-1").await.unwrap().id,
            "standalone-1"
        );
        assert_eq!(
            loaded_view(&repo, "standalone-2").await.unwrap().id,
            "standalone-2"
        );
    }

    #[tokio::test]
    async fn commit_many_multiple_aggregates() {
        let repo = InMemoryRepository::new();

        let view = RelationalView {
            id: "multi".into(),
            counter: 77,
        };

        let mut agg1 = TestAggregate::default();
        agg1.touch().unwrap();
        agg1.entity.set_id("agg-1");

        let mut agg2 = TestAggregate::default();
        agg2.touch().unwrap();
        agg2.entity.set_id("agg-2");

        ReadModelWritePlanCommitExt::read_models(&repo, read_models(&view))
            .commit_many(&mut [&mut agg1, &mut agg2])
            .await
            .unwrap();

        assert_eq!(loaded_view(&repo, "multi").await.unwrap().counter, 77);

        let agg_type = TestAggregate::aggregate_type();
        let e1 =
            crate::GetStream::get_stream(&repo, &StreamIdentity::new(agg_type, "agg-1").unwrap())
                .await
                .unwrap();
        assert!(e1.is_some());
        let e2 =
            crate::GetStream::get_stream(&repo, &StreamIdentity::new(agg_type, "agg-2").unwrap())
                .await
                .unwrap();
        assert!(e2.is_some());
    }

    #[tokio::test]
    async fn staged_builder_ordering_is_semantic_for_outbox_session_and_aggregate() {
        async fn record(order: u8) -> (Vec<(String, String)>, Vec<String>) {
            let repo = RecordingBatchRepo::default();
            let view = RelationalView {
                id: "ordered".into(),
                counter: 7,
            };
            let outbox = OutboxMessage::create("ordered-msg", "TestEvent", b"{}".to_vec()).unwrap();
            let mut agg = TestAggregate::default();
            agg.touch().unwrap();

            match order {
                0 => ReadModelWritePlanCommitExt::read_models(&repo, read_models(&view))
                    .outbox(outbox)
                    .aggregate(&mut agg)
                    .commit()
                    .await
                    .unwrap(),
                1 => repo
                    .outbox(outbox)
                    .read_models(read_models(&view))
                    .aggregate(&mut agg)
                    .commit()
                    .await
                    .unwrap(),
                _ => ReadModelWritePlanCommitExt::aggregate(&repo, &mut agg)
                    .read_models(read_models(&view))
                    .outbox(outbox)
                    .commit()
                    .await
                    .unwrap(),
            }

            let stream_ids = repo.stream_ids.lock().unwrap().clone();
            let read_model_keys = repo.read_model_keys.lock().unwrap().clone();
            (stream_ids, read_model_keys)
        }

        let baseline = record(0).await;
        assert_eq!(record(1).await, baseline);
        assert_eq!(record(2).await, baseline);
    }

    #[tokio::test]
    async fn staged_commit_sets_outbox_source_from_single_aggregate() {
        let repo = RecordingBatchRepo::default();
        let mut agg = TestAggregate::default();
        agg.touch().unwrap();
        let outbox = OutboxMessage::create("sourced-msg", "TestEvent", b"{}".to_vec()).unwrap();

        ReadModelWritePlanCommitExt::aggregate(&repo, &mut agg)
            .outbox(outbox)
            .commit()
            .await
            .unwrap();

        assert_eq!(
            repo.outbox_sources.lock().unwrap().as_slice(),
            &[(
                "sourced-msg".to_string(),
                Some(TestAggregate::aggregate_type().to_string()),
                Some("agg-1".to_string()),
                Some(1),
            )]
        );
    }

    #[tokio::test]
    async fn staged_builder_supports_multiple_aggregates() {
        let repo = RecordingBatchRepo::default();
        let view = RelationalView {
            id: "staged-multi".into(),
            counter: 77,
        };
        let mut agg1 = TestAggregate::default();
        agg1.touch().unwrap();
        agg1.entity.set_id("agg-1");
        let mut agg2 = TestAggregate::default();
        agg2.touch().unwrap();
        agg2.entity.set_id("agg-2");

        ReadModelWritePlanCommitExt::read_models(&repo, read_models(&view))
            .aggregate(&mut agg1)
            .aggregate(&mut agg2)
            .commit()
            .await
            .unwrap();

        assert_eq!(
            repo.read_model_keys.lock().unwrap().as_slice(),
            &[lock_key_for(&view)]
        );
        assert_eq!(
            repo.stream_ids.lock().unwrap().as_slice(),
            &[
                (
                    TestAggregate::aggregate_type().to_string(),
                    "agg-1".to_string()
                ),
                (
                    TestAggregate::aggregate_type().to_string(),
                    "agg-2".to_string()
                ),
            ]
        );
    }

    #[tokio::test]
    async fn commit_builder_failure_does_not_mark_aggregate_committed() {
        let repo = RecordingBatchRepo {
            fail: true,
            ..Default::default()
        };

        let view = RelationalView {
            id: "rollback".into(),
            counter: 1,
        };
        let outbox = OutboxMessage::create("msg-rollback", "TestEvent", b"{}".to_vec()).unwrap();
        let mut agg = TestAggregate::default();
        agg.touch().unwrap();

        let err = ReadModelWritePlanCommitExt::read_models(&repo, read_models(&view))
            .outbox(outbox)
            .commit(&mut agg)
            .await
            .unwrap_err();

        assert!(
            matches!(&err, RepositoryError::Model(message) if message == "injected async batch failure"),
            "unexpected error: {err}"
        );
        assert_eq!(agg.entity().committed_version(), 0);
        assert_eq!(agg.entity().new_events().len(), 1);
        assert_eq!(
            repo.read_model_keys.lock().unwrap().as_slice(),
            &[lock_key_for(&view)]
        );
        assert!(repo
            .stream_ids
            .lock()
            .unwrap()
            .iter()
            .any(|(_, id)| id == "agg-1"));
        assert!(repo
            .outbox_ids
            .lock()
            .unwrap()
            .iter()
            .any(|id| id == "msg-rollback"));
    }

    #[tokio::test]
    async fn commit_builder_empty_batch_succeeds() {
        let repo = RecordingBatchRepo::default();

        CommitBuilder::new(&repo).commit_all().await.unwrap();

        assert!(repo.stream_ids.lock().unwrap().is_empty());
        assert!(repo.read_model_keys.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn commit_read_models_and_aggregate() {
        let repo = RecordingBatchRepo::default();
        let view = RelationalView {
            id: "async-view".into(),
            counter: 42,
        };
        let mut agg = TestAggregate::default();
        agg.touch().unwrap();

        repo.read_models(read_models(&view))
            .commit(&mut agg)
            .await
            .unwrap();

        assert_eq!(
            repo.stream_ids.lock().unwrap().as_slice(),
            &[(
                TestAggregate::aggregate_type().to_string(),
                "agg-1".to_string()
            )]
        );
        assert_eq!(
            repo.read_model_keys.lock().unwrap().as_slice(),
            &[lock_key_for(&view)]
        );
        assert_eq!(agg.entity().committed_version(), 1);
    }

    #[tokio::test]
    async fn staged_builder_ordering_sets_outbox_source() {
        let repo = RecordingBatchRepo::default();
        let view = RelationalView {
            id: "async-staged".into(),
            counter: 7,
        };
        let mut agg = TestAggregate::default();
        agg.touch().unwrap();
        let outbox = OutboxMessage::create("async-msg", "TestEvent", b"{}".to_vec()).unwrap();

        repo.read_models(read_models(&view))
            .outbox(outbox)
            .aggregate(&mut agg)
            .commit()
            .await
            .unwrap();

        assert_eq!(
            repo.outbox_sources.lock().unwrap().as_slice(),
            &[(
                "async-msg".to_string(),
                Some(TestAggregate::aggregate_type().to_string()),
                Some("agg-1".to_string()),
                Some(1),
            )]
        );
        assert_eq!(
            repo.read_model_keys.lock().unwrap().as_slice(),
            &[lock_key_for(&view)]
        );
    }

    #[tokio::test]
    async fn commit_many_supports_same_type_aggregates() {
        let repo = RecordingBatchRepo::default();
        let mut agg1 = TestAggregate::default();
        agg1.touch().unwrap();
        agg1.entity.set_id("async-agg-1");
        let mut agg2 = TestAggregate::default();
        agg2.touch().unwrap();
        agg2.entity.set_id("async-agg-2");

        CommitBuilder::new(&repo)
            .commit_many(&mut [&mut agg1, &mut agg2])
            .await
            .unwrap();

        assert_eq!(
            repo.stream_ids.lock().unwrap().as_slice(),
            &[
                (
                    TestAggregate::aggregate_type().to_string(),
                    "async-agg-1".to_string(),
                ),
                (
                    TestAggregate::aggregate_type().to_string(),
                    "async-agg-2".to_string(),
                ),
            ]
        );
    }

    // The one stamping rule: exactly one distinct staged aggregate stamps the
    // outbox source; several distinct aggregates are ambiguous and stamp
    // nothing. This holds across every entry point (commit, commit_many, and
    // explicit staging).
    #[tokio::test]
    async fn outbox_source_stamping_requires_an_unambiguous_aggregate() {
        // Single aggregate via commit_many: stamped.
        let repo = RecordingBatchRepo::default();
        let mut agg = TestAggregate::default();
        agg.touch().unwrap();
        let outbox = OutboxMessage::create("solo-msg", "TestEvent", b"{}".to_vec()).unwrap();
        CommitBuilderExt::outbox(&repo, outbox)
            .commit_many(&mut [&mut agg])
            .await
            .unwrap();
        assert_eq!(
            repo.outbox_sources.lock().unwrap().as_slice(),
            &[(
                "solo-msg".to_string(),
                Some(TestAggregate::aggregate_type().to_string()),
                Some("agg-1".to_string()),
                Some(1),
            )]
        );

        // Two distinct aggregates: ambiguous, nothing stamped.
        let repo = RecordingBatchRepo::default();
        let mut agg1 = TestAggregate::default();
        agg1.touch().unwrap();
        agg1.entity.set_id("many-1");
        let mut agg2 = TestAggregate::default();
        agg2.touch().unwrap();
        agg2.entity.set_id("many-2");
        let outbox = OutboxMessage::create("many-msg", "TestEvent", b"{}".to_vec()).unwrap();
        CommitBuilderExt::outbox(&repo, outbox)
            .commit_many(&mut [&mut agg1, &mut agg2])
            .await
            .unwrap();
        assert_eq!(
            repo.outbox_sources.lock().unwrap().as_slice(),
            &[("many-msg".to_string(), None, None, None)]
        );
    }
}
