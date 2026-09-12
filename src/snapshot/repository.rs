use std::future::Future;
use std::pin::Pin;

use crate::aggregate::{hydrate, AggregateRepository, SnapshotPolicy, StreamReads};
use crate::entity::{upcast_events_for_replay, Entity, EventRecord};
use crate::repository::{GetStream, RepositoryError, SnapshotStore, StreamIdentity};

use super::snapshottable::Snapshottable;
use super::store::SnapshotRecord;

#[derive(Debug, PartialEq, Eq)]
enum SnapshotHydrationError {
    Cache(String),
    Replay(String),
}

fn snapshot_hydration_error_to_repository_error(err: SnapshotHydrationError) -> RepositoryError {
    match err {
        SnapshotHydrationError::Cache(message) | SnapshotHydrationError::Replay(message) => {
            RepositoryError::Replay(message)
        }
    }
}

fn snapshot_due(version: u64, snapshot_version: u64, frequency: u64) -> bool {
    version.saturating_sub(snapshot_version) >= frequency
}

/// Hydrate an aggregate from a snapshot cache record, replaying only events
/// after the snapshot version.
///
/// A stale or unusable snapshot (identity mismatch, unsupported codec, a
/// [`Snapshottable::SNAPSHOT_VERSION`] mismatch, or a decode failure) is treated
/// as a **cache miss** and the aggregate is rebuilt from the events already in
/// `entity` by full replay. This matches the internal load path: a snapshot is a
/// rebuildable cache, so an unusable one degrades to replay rather than failing
/// the load. Only a genuine replay failure (a post-snapshot event the aggregate
/// cannot apply) surfaces as [`RepositoryError::Replay`].
///
/// Because this entry point degrades gracefully, `entity` must carry the full
/// event history (so the cache-miss fallback can replay it). The optimized
/// snapshot+tail load lives in the repository's internal load path.
pub fn hydrate_from_snapshot<A: Snapshottable>(
    entity: Entity,
    snapshot: SnapshotRecord,
) -> Result<A, RepositoryError> {
    hydrate_with_optional_snapshot::<A>(entity, Some(snapshot))
}

fn validate_snapshot_for_entity<A: Snapshottable>(
    entity: &Entity,
    snapshot: &SnapshotRecord,
) -> Result<(), SnapshotHydrationError> {
    if snapshot.aggregate_id != entity.id() || snapshot.aggregate_type != A::aggregate_type() {
        return Err(SnapshotHydrationError::Cache(format!(
            "snapshot cache identity {}:{} does not match aggregate {}:{}",
            snapshot.aggregate_type,
            snapshot.aggregate_id,
            A::aggregate_type(),
            entity.id()
        )));
    }

    // Schema-version gate. bitcode is positional and not self-describing, so a
    // layout-compatible change to `A::Snapshot` would decode *successfully* into
    // the wrong state. Refuse to decode any snapshot whose stored schema version
    // does not match the aggregate's current `SNAPSHOT_VERSION`; treating it as
    // a cache miss rebuilds correct state by replay.
    if snapshot.snapshot_version != A::SNAPSHOT_VERSION {
        return Err(SnapshotHydrationError::Cache(format!(
            "snapshot schema version {} does not match aggregate {} version {} for {}:{}",
            snapshot.snapshot_version,
            A::aggregate_type(),
            A::SNAPSHOT_VERSION,
            snapshot.aggregate_type,
            snapshot.aggregate_id
        )));
    }

    // `entity.version()` is the true stream version in both load shapes: a
    // full-history entity has `version == events.len() == max(sequence)`, and a
    // tail-only entity (snapshot+tail load) records the real version via
    // `Entity::load_tail_from_history`.
    let stream_version = entity.version();
    if snapshot.version > stream_version {
        return Err(SnapshotHydrationError::Cache(format!(
            "snapshot cache version {} exceeds stream version {} for {}:{}",
            snapshot.version, stream_version, snapshot.aggregate_type, snapshot.aggregate_id
        )));
    }

    Ok(())
}

fn prepare_snapshot<A: Snapshottable>(
    entity: &Entity,
    snapshot: &SnapshotRecord,
) -> Result<A::Snapshot, SnapshotHydrationError> {
    validate_snapshot_for_entity::<A>(entity, snapshot)?;
    if !snapshot.has_supported_payload_codec() {
        return Err(SnapshotHydrationError::Cache(format!(
            "unsupported snapshot payload codec `{}` version {}",
            snapshot.payload_codec, snapshot.payload_codec_version
        )));
    }

    bitcode::deserialize(&snapshot.payload)
        .map_err(|e| SnapshotHydrationError::Cache(format!("snapshot deserialize: {e}")))
}

fn hydrate_prepared_snapshot<A: Snapshottable>(
    entity: Entity,
    snapshot: &SnapshotRecord,
    snapshot_payload: A::Snapshot,
) -> Result<A, SnapshotHydrationError> {
    let mut agg = A::new_empty();
    *agg.entity_mut() = entity;

    // Set snapshot_version so frequency check works on next commit
    agg.entity_mut().set_snapshot_version(snapshot.version);

    // Restore aggregate state from snapshot
    agg.restore_from_snapshot(snapshot_payload);

    // Replay only events AFTER the snapshot. Take the events out of the entity
    // so we can iterate them while holding a mutable borrow of `agg`, then put
    // the full history back unchanged (its version/committed_version invariants
    // must survive: the pre-snapshot prefix is still counted for `new_events`
    // slicing on the next commit).
    let history = agg.entity_mut().take_events();

    // Upcasters may rewrite the post-snapshot events; the upcasted view clones
    // only the events an upcaster rewrites. When none applies, replay straight
    // from the filtered borrow — no clone of the post-snapshot tail.
    let replay_result = match upcast_events_for_replay(
        post_snapshot_tail(&history, snapshot.version),
        A::upcasters(),
    ) {
        Ok(Some(upcasted)) => replay_events(&mut agg, upcasted.iter().map(|event| event.as_ref())),
        Ok(None) => replay_events(&mut agg, post_snapshot_tail(&history, snapshot.version)),
        Err(err) => Err(SnapshotHydrationError::Replay(err.to_string())),
    };

    // Restore the full history regardless of replay outcome before surfacing it.
    agg.entity_mut().restore_history(history);
    replay_result?;
    Ok(agg)
}

/// The post-snapshot tail of a borrowed history (`sequence > snapshot_version`).
fn post_snapshot_tail(
    history: &[EventRecord],
    snapshot_version: u64,
) -> impl Iterator<Item = &EventRecord> + Clone {
    history
        .iter()
        .filter(move |e| e.sequence > snapshot_version)
}

/// Replay a prepared event view — the borrowed post-snapshot tail, or an
/// upcasted copy of it — with no intermediate allocation of its own.
fn replay_events<'e, A: Snapshottable>(
    agg: &mut A,
    events: impl Iterator<Item = &'e EventRecord>,
) -> Result<(), SnapshotHydrationError> {
    agg.entity_mut().set_replaying(true);
    for event in events {
        if let Err(err) = agg.replay_event(event) {
            agg.entity_mut().set_replaying(false);
            return Err(SnapshotHydrationError::Replay(err.to_string()));
        }
    }
    agg.entity_mut().set_replaying(false);
    Ok(())
}

fn snapshot_record_for<A: Snapshottable>(aggregate: &A) -> Result<SnapshotRecord, RepositoryError> {
    let payload = bitcode::serialize(&aggregate.create_snapshot())
        .map_err(|e| RepositoryError::Replay(format!("snapshot serialize: {e}")))?;

    Ok(SnapshotRecord::new(
        A::aggregate_type(),
        aggregate.entity().id(),
        aggregate.entity().version(),
        A::SNAPSHOT_VERSION,
        payload,
    ))
}

fn hydrate_with_optional_snapshot<A: Snapshottable>(
    entity: Entity,
    snapshot: Option<SnapshotRecord>,
) -> Result<A, RepositoryError> {
    let Some(snapshot) = snapshot else {
        return hydrate::<A>(entity);
    };

    let snapshot_payload = match prepare_snapshot::<A>(&entity, &snapshot) {
        Ok(snapshot_payload) => snapshot_payload,
        Err(SnapshotHydrationError::Cache(_)) => return hydrate::<A>(entity),
        Err(SnapshotHydrationError::Replay(message)) => {
            return Err(RepositoryError::Replay(message))
        }
    };

    hydrate_prepared_snapshot::<A>(entity, &snapshot, snapshot_payload)
        .map_err(snapshot_hydration_error_to_repository_error)
}

impl<R, A> AggregateRepository<R, A>
where
    R: SnapshotStore + GetStream + Sync,
    A: Snapshottable + Send,
{
    /// Enable snapshot caching at the given event frequency.
    ///
    /// Snapshots are a transparent optimization: this configures snapshot
    /// behaviour on the **same** repository type and returns it. On commit a
    /// snapshot is staged (when due) in the same transaction; on load the
    /// aggregate is hydrated from a snapshot when one exists. Every other method
    /// behaves identically with or without snapshots.
    pub fn with_snapshots(mut self, frequency: u64) -> Self {
        assert!(
            frequency > 0,
            "snapshot frequency must be greater than zero; \
             frequency 0 would snapshot on every commit"
        );
        self.set_snapshot_policy(SnapshotPolicy::new(
            frequency,
            snapshot_record_if_due::<A>,
            hydrate_from_store::<R, A>,
            hydrate_all_from_store::<R, A>,
            load_from_store::<R, A>,
        ));
        self
    }
}

/// Build a snapshot cache record for `aggregate` when one is due at `frequency`.
/// Captured as the `record` hook of a `SnapshotPolicy`.
fn snapshot_record_if_due<A: Snapshottable>(
    aggregate: &A,
    frequency: u64,
) -> Result<Option<SnapshotRecord>, RepositoryError> {
    let version = aggregate.entity().version();
    let snap_version = aggregate.entity().snapshot_version();
    if snapshot_due(version, snap_version, frequency) {
        snapshot_record_for(aggregate).map(Some)
    } else {
        Ok(None)
    }
}

/// Load the snapshot cache record (if any) and hydrate `entity` from it.
/// Captured as the `hydrate` hook of a `SnapshotPolicy` — used where the full
/// stream is already in hand (batch loads, locked reads).
fn hydrate_from_store<'a, R, A>(
    repo: &'a R,
    identity: &'a StreamIdentity,
    entity: Entity,
) -> Pin<Box<dyn Future<Output = Result<A, RepositoryError>> + Send + 'a>>
where
    R: SnapshotStore + Sync,
    A: Snapshottable + Send,
{
    Box::pin(async move {
        let snapshot = repo.get_snapshot(identity).await?;
        hydrate_with_optional_snapshot::<A>(entity, snapshot)
    })
}

/// Load the snapshot cache records for a whole batch in one round trip and
/// hydrate each entity from its record (if any). Captured as the `hydrate_all`
/// hook of a `SnapshotPolicy` — fixes the N+1 of consulting the cache per
/// aggregate on batch loads.
fn hydrate_all_from_store<'a, R, A>(
    repo: &'a R,
    entities: Vec<(StreamIdentity, Entity)>,
) -> Pin<Box<dyn Future<Output = Result<Vec<A>, RepositoryError>> + Send + 'a>>
where
    R: SnapshotStore + Sync,
    A: Snapshottable + Send,
{
    Box::pin(async move {
        let identities: Vec<StreamIdentity> = entities
            .iter()
            .map(|(identity, _)| identity.clone())
            .collect();
        let mut snapshots = std::collections::HashMap::with_capacity(identities.len());
        for record in repo.get_snapshots(&identities).await? {
            let key =
                StreamIdentity::new(&record.aggregate_type, &record.aggregate_id)?.storage_key();
            snapshots.insert(key, record);
        }
        entities
            .into_iter()
            .map(|(identity, entity)| {
                hydrate_with_optional_snapshot::<A>(
                    entity,
                    snapshots.remove(&identity.storage_key()),
                )
            })
            .collect()
    })
}

/// Own the whole load, reading the snapshot **first** so only the post-snapshot
/// tail of the stream is fetched. Captured as the `load` hook of a
/// `SnapshotPolicy`.
///
/// Ordering matters: the previous design fetched the entire stream and *then*
/// filtered it against the snapshot, so a fresh snapshot over a long stream
/// still paid the full I/O and decode cost. Here the snapshot bounds the read.
///
/// Degrades gracefully on a cache miss: if there is no snapshot, or it is
/// unusable (identity/codec/schema-version mismatch, version past the
/// stream, or decode failure), the aggregate is rebuilt from a full stream
/// load — correct, just not optimized.
fn load_from_store<'a, R, A>(
    repo: &'a R,
    identity: &'a StreamIdentity,
    reads: StreamReads<R>,
) -> Pin<Box<dyn Future<Output = Result<Option<A>, RepositoryError>> + Send + 'a>>
where
    R: SnapshotStore + GetStream + Sync,
    A: Snapshottable + Send,
{
    Box::pin(async move {
        let Some(snapshot) = repo.get_snapshot(identity).await? else {
            // No snapshot: a plain full load (same as a snapshotless repo).
            return (reads.full)(repo, identity)
                .await?
                .map(hydrate::<A>)
                .transpose();
        };

        // Fetch only events after the snapshot. The returned entity records the
        // true stream version even though it holds only the tail.
        let Some(entity) = (reads.tail)(repo, identity, snapshot.version).await? else {
            // The stream is gone but a snapshot lingers; nothing to hydrate.
            return Ok(None);
        };

        // Decode and replay without crossing an await while holding the
        // (possibly non-`Send`) snapshot payload: resolve to a concrete result
        // first, then do any fallback I/O.
        let prepared = prepare_snapshot::<A>(&entity, &snapshot)
            .map(|payload| hydrate_prepared_snapshot::<A>(entity, &snapshot, payload));

        match prepared {
            Ok(Ok(aggregate)) => Ok(Some(aggregate)),
            Ok(Err(err)) => Err(snapshot_hydration_error_to_repository_error(err)),
            // Cache miss (stale/incompatible snapshot): the tail alone cannot
            // rebuild the aggregate, so fall back to a full stream load. This is
            // the I/O we hoped to avoid, but it only happens when the snapshot is
            // unusable — the correct, safe outcome.
            Err(SnapshotHydrationError::Cache(_)) => Ok((reads.full)(repo, identity)
                .await?
                .map(hydrate::<A>)
                .transpose()?),
            Err(SnapshotHydrationError::Replay(message)) => Err(RepositoryError::Replay(message)),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::{CommitBatch, TransactionalCommit};
    use crate::{sourced, Aggregate, EventRecord};

    #[derive(Default)]
    struct TestAggregate {
        entity: Entity,
        value: u32,
    }

    #[sourced(entity)]
    impl TestAggregate {
        #[event("Touched")]
        fn touch(&mut self) {
            if self.entity.id().is_empty() {
                self.entity.set_id("snap-1");
            }
            self.value += 1;
        }
    }

    impl Snapshottable for TestAggregate {
        type Snapshot = u32;

        fn create_snapshot(&self) -> Self::Snapshot {
            self.value
        }

        fn restore_from_snapshot(&mut self, snapshot: Self::Snapshot) {
            self.value = snapshot;
        }
    }

    #[derive(Default)]
    struct FailingSnapshotRepo {
        saw_snapshot: std::sync::atomic::AtomicBool,
    }

    struct CausalReadProbe {
        inner: crate::InMemoryRepository,
        reads: std::sync::Mutex<Vec<Option<u64>>>,
    }
    impl GetStream for CausalReadProbe {
        async fn get_stream(&self, _: &StreamIdentity) -> Result<Option<Entity>, RepositoryError> {
            panic!("causal snapshot loading must not use ordinary locking reads")
        }
    }
    impl crate::command_ledger::CausalGetStream for CausalReadProbe {
        async fn get_causal_stream(
            &self,
            identity: &StreamIdentity,
        ) -> Result<Option<Entity>, RepositoryError> {
            self.reads.lock().unwrap().push(None);
            self.inner.get_stream(identity).await
        }
        async fn get_causal_stream_tail(
            &self,
            identity: &StreamIdentity,
            after: u64,
        ) -> Result<Option<Entity>, RepositoryError> {
            self.reads.lock().unwrap().push(Some(after));
            self.inner.get_stream_tail(identity, after).await
        }
    }
    impl SnapshotStore for CausalReadProbe {
        async fn get_snapshot(
            &self,
            identity: &StreamIdentity,
        ) -> Result<Option<SnapshotRecord>, RepositoryError> {
            self.inner.get_snapshot(identity).await
        }
        async fn save_snapshot(
            &self,
            identity: &StreamIdentity,
            record: SnapshotRecord,
        ) -> Result<(), RepositoryError> {
            self.inner.save_snapshot(identity, record).await
        }
        async fn delete_snapshot(
            &self,
            identity: &StreamIdentity,
        ) -> Result<bool, RepositoryError> {
            self.inner.delete_snapshot(identity).await
        }
    }

    #[tokio::test]
    async fn causal_snapshots_use_only_tail_through_queued_wrappers() {
        let inner = crate::InMemoryRepository::new();
        let writer = AggregateRepository::new(inner.clone()).with_snapshots(2);
        let mut aggregate = TestAggregate::default();
        for _ in 0..3 {
            aggregate.touch().unwrap();
            writer.commit(&mut aggregate).await.unwrap();
        }
        let identity = StreamIdentity::new(TestAggregate::aggregate_type(), "snap-1").unwrap();
        let reader = AggregateRepository::<_, TestAggregate>::new(crate::QueuedRepository::new(
            CausalReadProbe {
                inner,
                reads: Default::default(),
            },
        ))
        .with_snapshots(2);
        // Repeated loads prove no queue lock leaks across a handler await.
        for _ in 0..2 {
            let loaded = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                reader.get_causal(&identity),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();
            assert_eq!(loaded.value, 3);
            assert_eq!(loaded.entity.version(), 3);
            assert_eq!(loaded.entity.snapshot_version(), 2);
            assert_eq!(loaded.entity.events().len(), 1);
        }
        assert_eq!(
            *reader.repo().inner().reads.lock().unwrap(),
            [Some(2), Some(2)]
        );

        // Invalid cache bytes retain the ordinary safe full-replay behavior,
        // while still bypassing locking reads on that recovery path.
        let mut snapshot = reader
            .repo()
            .get_snapshot(&identity)
            .await
            .unwrap()
            .unwrap();
        snapshot.payload = vec![255; 9];
        reader
            .repo()
            .save_snapshot(&identity, snapshot)
            .await
            .unwrap();
        let loaded = reader.get_causal(&identity).await.unwrap().unwrap();
        assert_eq!(loaded.value, 3);
        assert_eq!(
            *reader.repo().inner().reads.lock().unwrap(),
            [Some(2), Some(2), Some(2), None]
        );
    }

    impl TransactionalCommit for FailingSnapshotRepo {
        async fn commit_batch<'a>(&'a self, batch: CommitBatch<'a>) -> Result<(), RepositoryError> {
            {
                if !batch.snapshots.is_empty() {
                    self.saw_snapshot
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    return Err(RepositoryError::Model("snapshot write failed".into()));
                }

                for stream in batch.streams {
                    stream.entity.mark_committed();
                }
                Ok(())
            }
        }
    }

    // Snapshots can only be enabled on a repo that can both store them and read
    // streams; these stubs satisfy the bounds (the commit-failure and
    // zero-frequency tests never load).
    impl GetStream for FailingSnapshotRepo {
        async fn get_stream(
            &self,
            _identity: &StreamIdentity,
        ) -> Result<Option<Entity>, RepositoryError> {
            Ok(None)
        }
    }

    impl SnapshotStore for FailingSnapshotRepo {
        async fn get_snapshot(
            &self,
            _identity: &StreamIdentity,
        ) -> Result<Option<SnapshotRecord>, RepositoryError> {
            Ok(None)
        }
        async fn save_snapshot(
            &self,
            _identity: &StreamIdentity,
            _record: SnapshotRecord,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
        async fn delete_snapshot(
            &self,
            _identity: &StreamIdentity,
        ) -> Result<bool, RepositoryError> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn snapshot_batch_failure_leaves_aggregate_uncommitted() {
        let repo = FailingSnapshotRepo::default();
        let snapshot_repo = AggregateRepository::new(repo).with_snapshots(1);

        let mut aggregate = TestAggregate::default();
        aggregate.touch().unwrap();

        let err = snapshot_repo.commit(&mut aggregate).await.unwrap_err();

        assert!(
            matches!(&err, RepositoryError::Model(message) if message == "snapshot write failed"),
            "unexpected error: {err}"
        );
        assert!(snapshot_repo
            .repo()
            .saw_snapshot
            .load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(aggregate.entity.committed_version(), 0);
        assert_eq!(aggregate.entity.snapshot_version(), 0);
        assert_eq!(aggregate.entity.new_events().len(), 1);
    }

    #[test]
    #[should_panic(expected = "snapshot frequency must be greater than zero")]
    fn with_snapshots_rejects_zero_frequency() {
        let _: AggregateRepository<_, TestAggregate> =
            AggregateRepository::new(FailingSnapshotRepo::default()).with_snapshots(0);
    }

    #[test]
    fn snapshot_due_uses_saturating_version_distance() {
        assert!(snapshot_due(5, 2, 3));
        assert!(!snapshot_due(5, 3, 3));
        assert!(!snapshot_due(0, u64::MAX, 1));
        assert!(snapshot_due(u64::MAX, u64::MAX - 1, 1));
    }

    // A `TestAggregate` snapshot encodes only `value: u32`. Replaying the
    // single "Touched" event over the empty aggregate yields `value == 1`, so a
    // cache-miss fallback that ignores the snapshot is observable as value 1.
    fn snap1_entity() -> Entity {
        let mut entity = Entity::with_id("snap-1");
        entity.load_from_history(vec![EventRecord::new("Touched", vec![], 1)]);
        entity
    }

    #[test]
    fn hydrate_from_snapshot_falls_back_on_identity_mismatch() {
        // Snapshot carries value 99, but its identity does not match the entity.
        // The unusable snapshot is a cache miss → rebuild by replaying history,
        // so the snapshot's value is ignored.
        let snapshot = SnapshotRecord::new(
            TestAggregate::aggregate_type(),
            "other",
            1,
            1,
            bitcode::serialize(&99_u32).unwrap(),
        );

        let agg = hydrate_from_snapshot::<TestAggregate>(snap1_entity(), snapshot)
            .expect("identity mismatch should degrade to replay, not error");
        assert_eq!(
            agg.value, 1,
            "value should come from replay, not the snapshot"
        );
    }

    #[test]
    fn hydrate_from_snapshot_falls_back_when_snapshot_ahead_of_stream() {
        // Snapshot version 2 exceeds the single-event stream → cache miss →
        // replay history (value 1), ignoring the snapshot payload (value 99).
        let snapshot = SnapshotRecord::new(
            TestAggregate::aggregate_type(),
            "snap-1",
            2,
            1,
            bitcode::serialize(&99_u32).unwrap(),
        );

        let agg = hydrate_from_snapshot::<TestAggregate>(snap1_entity(), snapshot)
            .expect("future snapshot should degrade to replay, not error");
        assert_eq!(
            agg.value, 1,
            "value should come from replay, not the snapshot"
        );
    }

    #[test]
    fn hydrate_from_snapshot_falls_back_on_schema_version_mismatch() {
        // `TestAggregate::SNAPSHOT_VERSION` defaults to 1. A stored snapshot at
        // schema version 2 must NOT be decoded (its layout may differ): it is a
        // cache miss → replay history (value 1), not the snapshot's value 99.
        let snapshot = SnapshotRecord::new(
            TestAggregate::aggregate_type(),
            "snap-1",
            1,
            2,
            bitcode::serialize(&99_u32).unwrap(),
        );

        let agg = hydrate_from_snapshot::<TestAggregate>(snap1_entity(), snapshot)
            .expect("schema version mismatch should degrade to replay, not error");
        assert_eq!(
            agg.value, 1,
            "mismatched-version snapshot must not be decoded"
        );
    }

    #[test]
    fn hydrate_from_snapshot_uses_matching_snapshot() {
        // A valid snapshot (matching identity, codec, schema version, and not
        // ahead of the stream) is used: its value 99 restores, and there are no
        // post-snapshot events to replay.
        let snapshot = SnapshotRecord::new(
            TestAggregate::aggregate_type(),
            "snap-1",
            1,
            1,
            bitcode::serialize(&99_u32).unwrap(),
        );

        let agg = hydrate_from_snapshot::<TestAggregate>(snap1_entity(), snapshot)
            .expect("valid snapshot should hydrate");
        assert_eq!(agg.value, 99, "value should come from the snapshot");
    }
}
