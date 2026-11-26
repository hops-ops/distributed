//! Snapshot-then-tail reads: every read of a snapshotting aggregate
//! repository hydrates from the snapshot plus the stream tail after it, and a
//! snapshot that cannot be used falls back to the whole stream.
//!
//! The cases plant a snapshot whose state the events could never produce, so
//! a read that ignored the snapshot (or the tail) gives a different total.
//!
//! Every case runs on a queued repository too, where a loaded aggregate holds
//! its stream's lock until it is dropped: the cases drop each one before
//! reading its stream again.

use std::time::Duration;

use distributed::{
    sourced, AggregateBuilder, Entity, GetStream, QueuedRepository, ReadOpts, SnapshotRecord,
    SnapshotStore, Snapshottable, StreamIdentity, TransactionalCommit,
};
use distributed::{Aggregate, Snapshot};

use super::scenario::unique_id;

#[derive(Default, Snapshot)]
pub struct Tally {
    pub entity: Entity,
    pub total: i64,
}

#[sourced(entity, aggregate_type = "conformance.tally")]
impl Tally {
    #[event("opened", when = !id.is_empty())]
    pub fn open(&mut self, id: String) {
        self.entity.set_id(&id);
    }

    #[event("added")]
    pub fn add(&mut self, amount: i64) {
        self.total += amount;
    }
}

/// No automatic snapshots: the cases plant them.
const NEVER: u64 = 1_000;

/// The planted state: a total no sequence of the cases' events reaches.
const PLANTED: i64 = 1_000;

fn identity(id: &str) -> StreamIdentity {
    StreamIdentity::new(Tally::aggregate_type(), id).expect("tally identity should be valid")
}

/// Commit a tally with events `opened, added(1), added(2)`: version 3,
/// total 3.
async fn seed<R>(repo: &R) -> String
where
    R: TransactionalCommit + Clone + Send + Sync + 'static,
{
    let id = unique_id("tally");
    let mut tally = Tally::default();
    tally.open(id.clone()).unwrap();
    tally.add(1).unwrap();
    tally.add(2).unwrap();
    repo.clone()
        .aggregate::<Tally>()
        .commit(&mut tally)
        .await
        .expect("tally should commit");
    id
}

/// Save a snapshot of [`PLANTED`] covering events up to `version`.
async fn plant<R: SnapshotStore>(repo: &R, id: &str, version: u64) {
    let mut planted = Tally::default();
    planted.entity.set_id(id);
    planted.total = PLANTED;
    let payload = bitcode::serialize(&planted.create_snapshot()).unwrap();
    repo.save_snapshot(
        &identity(id),
        SnapshotRecord::new(
            Tally::aggregate_type(),
            id,
            version,
            Tally::SNAPSHOT_VERSION,
            payload,
        ),
    )
    .await
    .expect("snapshot should save");
}

/// Two seeded tallies: `snapshotted` has a snapshot at version 2 (it loads as
/// [`PLANTED`] + the tail's `added(2)`), `stale` one past its stream (a cache
/// miss: it loads from the whole stream, total 3).
async fn seed_snapshotted_and_stale<R>(repo: &R) -> (String, String)
where
    R: TransactionalCommit + SnapshotStore + Clone + Send + Sync + 'static,
{
    let snapshotted = seed(repo).await;
    let stale = seed(repo).await;
    plant(repo, &snapshotted, 2).await;
    plant(repo, &stale, 99).await;
    (snapshotted, stale)
}

fn totals(mut tallies: Vec<Tally>) -> Vec<(String, i64, u64)> {
    let mut totals: Vec<_> = tallies
        .drain(..)
        .map(|tally| {
            (
                tally.entity.id().to_string(),
                tally.total,
                tally.entity.version(),
            )
        })
        .collect();
    totals.sort();
    totals
}

fn expected(snapshotted: &str, stale: &str) -> Vec<(String, i64, u64)> {
    let mut expected = vec![
        (snapshotted.to_string(), PLANTED + 2, 3),
        (stale.to_string(), 3, 3),
    ];
    expected.sort();
    expected
}

/// `get` and `get_all` read the snapshot then the tail; a stale snapshot falls
/// back to the whole stream; a loaded aggregate commits at the true version.
pub async fn snapshotted_reads_hydrate_snapshot_then_tail<R>(repo: R)
where
    R: GetStream + TransactionalCommit + SnapshotStore + Clone + Send + Sync + 'static,
{
    let (snapshotted, stale) = seed_snapshotted_and_stale(&repo).await;
    let tallies = repo.clone().aggregate::<Tally>().with_snapshots(NEVER);

    let loaded = tallies.get(&snapshotted).await.unwrap().unwrap();
    assert_eq!(loaded.total, PLANTED + 2, "snapshot, then the tail");
    assert_eq!(loaded.entity.id(), snapshotted);
    assert_eq!(loaded.entity.version(), 3);
    drop(loaded);
    let loaded = tallies.get(&stale).await.unwrap().unwrap();
    assert_eq!(loaded.total, 3, "a snapshot past the stream is a miss");
    assert_eq!(loaded.entity.version(), 3);
    drop(loaded);
    assert!(tallies.get(&unique_id("absent")).await.unwrap().is_none());

    let all = tallies
        .get_all(&[&snapshotted, &stale, &unique_id("absent")])
        .await
        .unwrap();
    assert_eq!(totals(all), expected(&snapshotted, &stale));

    let mut loaded = tallies.get(&snapshotted).await.unwrap().unwrap();
    loaded.add(5).unwrap();
    tallies
        .commit(&mut loaded)
        .await
        .expect("a snapshot-loaded aggregate commits at the stream version");
    let reloaded = tallies.get(&snapshotted).await.unwrap().unwrap();
    assert_eq!(reloaded.total, PLANTED + 7);
    assert_eq!(reloaded.entity.version(), 4);
}

/// The queued repository's reads (`get`, `get_with`, `peek`, `get_all`,
/// `get_all_with`, `peek_all`) all read the snapshot then the tail. A locking
/// read still holds the stream's lock, and a non-locking one does not wait for
/// it.
pub async fn queued_snapshotted_reads_hydrate_snapshot_then_tail<R>(repo: QueuedRepository<R>)
where
    R: GetStream + TransactionalCommit + SnapshotStore + Clone + Send + Sync + 'static,
{
    let (snapshotted, stale) = seed_snapshotted_and_stale(&repo).await;
    let tallies = repo.clone().aggregate::<Tally>().with_snapshots(NEVER);
    let expected = expected(&snapshotted, &stale);
    let ids = [snapshotted.as_str(), stale.as_str()];

    for opts in [ReadOpts::default(), ReadOpts::no_lock()] {
        let one = tallies.get_with(&snapshotted, opts).await.unwrap().unwrap();
        assert_eq!(one.total, PLANTED + 2);
        drop(one);
        let all = tallies.get_all_with(&ids, opts).await.unwrap();
        assert_eq!(totals(all), expected);
    }
    assert_eq!(totals(tallies.peek_all(&ids).await.unwrap()), expected);
    assert_eq!(totals(tallies.get_all(&ids).await.unwrap()), expected);

    let held = tallies.get(&snapshotted).await.unwrap().unwrap();
    assert_eq!(held.total, PLANTED + 2);
    let peeked = tallies.peek(&snapshotted).await.unwrap().unwrap();
    assert_eq!(peeked.total, PLANTED + 2, "peek does not wait for the lock");
    let peeked_all = tallies.peek_all(&ids).await.unwrap();
    assert_eq!(totals(peeked_all), expected);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), tallies.get(&snapshotted))
            .await
            .is_err(),
        "a locking snapshotted read waits for the held lock"
    );
    drop(held);
    let next = tallies.get(&snapshotted).await.unwrap().unwrap();
    assert_eq!(next.total, PLANTED + 2);
}

/// A stream tail of a missing stream is `None`, whatever the lower bound; a
/// bound past the stream is clamped to its version. A snapshot lingering for
/// a missing stream loads nothing.
pub async fn stream_tail_of_missing_stream_is_none_and_past_bound_clamps<R>(repo: R)
where
    R: GetStream + TransactionalCommit + SnapshotStore + Clone + Send + Sync + 'static,
{
    let absent = identity(&unique_id("absent"));
    for after in [0, 2] {
        assert!(repo
            .get_stream_tail(&absent, after)
            .await
            .unwrap()
            .is_none());
    }

    let id = seed(&repo).await;
    let tail = repo
        .get_stream_tail(&identity(&id), 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tail.version(), 3);
    assert_eq!(tail.events().len(), 2);
    drop(tail);
    let past = repo
        .get_stream_tail(&identity(&id), 10)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(past.version(), 3, "a bound past the stream is clamped");
    assert!(past.events().is_empty());

    let lingering = unique_id("lingering");
    plant(&repo, &lingering, 2).await;
    let tallies = repo.aggregate::<Tally>().with_snapshots(NEVER);
    assert!(tallies.get(&lingering).await.unwrap().is_none());
    assert!(tallies.get_all(&[&lingering]).await.unwrap().is_empty());
}
