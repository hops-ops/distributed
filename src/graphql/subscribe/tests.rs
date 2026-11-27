//! The shared producer loop, driven by a counting read under paused time:
//! every assertion counts executions or entries, none waits on a clock.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use futures_util::{FutureExt, StreamExt};

use super::*;
use crate::graphql::protocol::{DistributedLiveCursor, ProtocolTokenCodec, ProtocolTokenPurpose};

/// The data a read sees, and what its executions did.
#[derive(Clone, Default)]
struct Store {
    data: Arc<AtomicU64>,
    fail: Arc<AtomicBool>,
    executions: Arc<AtomicUsize>,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
}

impl Store {
    fn executions(&self) -> usize {
        self.executions.load(Ordering::SeqCst)
    }

    fn set(&self, data: u64) {
        self.data.store(data, Ordering::SeqCst);
    }
}

struct Counted {
    store: Store,
    /// Holds every execution until a permit is added.
    gate: Option<Arc<Semaphore>>,
    /// Frames resume from a cursor at the data they read.
    resumable: bool,
}

fn cursor(data: u64) -> RequestedLiveResume {
    let token = ProtocolTokenCodec::new([7; 32])
        .issue(ProtocolTokenPurpose::LiveResume, &data)
        .unwrap();
    RequestedLiveResume::Cursors(vec![DistributedLiveCursor {
        projection: "p".into(),
        position: data.to_string(),
        token,
    }])
}

impl LiveRead for Counted {
    type Meta = u64;

    async fn execute(&self, _resume: RequestedLiveResume) -> LiveResult<u64> {
        let store = &self.store;
        store.executions.fetch_add(1, Ordering::SeqCst);
        let now = store.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        store.max_in_flight.fetch_max(now, Ordering::SeqCst);
        if let Some(gate) = &self.gate {
            gate.acquire().await.unwrap().forget();
        }
        store.in_flight.fetch_sub(1, Ordering::SeqCst);
        if store.fail.load(Ordering::SeqCst) {
            return Err(async_graphql::Error::new("boom"));
        }
        let data = store.data.load(Ordering::SeqCst);
        Ok(LiveFrame {
            value: Value::from(data),
            hash: data,
            next: if self.resumable {
                cursor(data)
            } else {
                RequestedLiveResume::Absent
            },
            meta: data,
        })
    }

    fn touched_by(&self, _last: &LiveFrame<u64>, change: &ReadModelChange) -> bool {
        change.tables.is_empty() || change.tables.contains("t")
    }
}

fn registry(concurrency: usize) -> LiveRegistry<&'static str, Counted> {
    LiveRegistry::new(Arc::new(Semaphore::new(concurrency)))
}

fn follow(
    registry: &LiveRegistry<&'static str, Counted>,
    hub: &ChangeHub,
    key: &'static str,
    store: &Store,
) -> LiveQueryStream {
    follow_read(
        registry,
        hub,
        key,
        Counted {
            store: store.clone(),
            gate: None,
            resumable: false,
        },
    )
}

fn follow_read(
    registry: &LiveRegistry<&'static str, Counted>,
    hub: &ChangeHub,
    key: &'static str,
    read: Counted,
) -> LiveQueryStream {
    registry.subscribe(
        hub,
        key,
        RequestedLiveResume::Absent,
        || read,
        Box::new(|_| Ok(())),
    )
}

fn touch(hub: &ChangeHub) {
    hub.publish(ReadModelChange::new(["t"]));
}

/// With the clock paused, this returns only once every task is idle.
async fn settle() {
    tokio::time::sleep(Duration::from_secs(1)).await;
}

async fn next_value(stream: &mut LiveQueryStream) -> u64 {
    let value = stream.next().await.expect("frame").expect("value");
    match value {
        Value::Number(number) => number.as_u64().unwrap(),
        other => panic!("{other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn subscribers_to_one_read_share_one_execution_per_change() {
    let (hub, registry, store) = (ChangeHub::new(), registry(8), Store::default());
    let mut streams = (0..10)
        .map(|_| follow(&registry, &hub, "q", &store))
        .collect::<Vec<_>>();
    for stream in &mut streams {
        assert_eq!(next_value(stream).await, 0);
    }
    // The producer's first read, then each later subscriber's own.
    assert_eq!(store.executions(), 10);
    assert_eq!((registry.len(), hub.subscriber_count()), (1, 1));

    for data in 1..=3 {
        store.set(data);
        touch(&hub);
        for stream in &mut streams {
            assert_eq!(next_value(stream).await, data);
        }
        assert_eq!(store.executions(), 10 + data as usize);
    }

    // An unrelated change and a redundant one cost nothing / push nothing.
    hub.publish(ReadModelChange::new(["other"]));
    touch(&hub);
    settle().await;
    assert_eq!(store.executions(), 14);
    for stream in &mut streams {
        assert!(stream.next().now_or_never().is_none(), "hash-gated");
    }
}

#[tokio::test(start_paused = true)]
async fn different_keys_never_share() {
    let (hub, registry, store) = (ChangeHub::new(), registry(8), Store::default());
    let mut a = follow(&registry, &hub, "a", &store);
    let mut b = follow(&registry, &hub, "b", &store);
    assert_eq!((next_value(&mut a).await, next_value(&mut b).await), (0, 0));
    assert_eq!((registry.len(), hub.subscriber_count()), (2, 2));
    store.set(1);
    touch(&hub);
    assert_eq!((next_value(&mut a).await, next_value(&mut b).await), (1, 1));
    assert_eq!(store.executions(), 4);
}

#[tokio::test(start_paused = true)]
async fn live_executions_never_exceed_the_engine_bound() {
    let (hub, registry, store) = (ChangeHub::new(), registry(2), Store::default());
    let gate = Arc::new(Semaphore::new(0));
    let readers = ["a", "b", "c", "d", "e"].map(|key| {
        let mut stream = follow_read(
            &registry,
            &hub,
            key,
            Counted {
                store: store.clone(),
                gate: Some(Arc::clone(&gate)),
                resumable: false,
            },
        );
        tokio::spawn(async move { next_value(&mut stream).await })
    });
    settle().await;
    assert_eq!(
        store.in_flight.load(Ordering::SeqCst),
        2,
        "two run, three wait"
    );
    assert_eq!(store.executions(), 2);
    for released in 1..=5 {
        gate.add_permits(1);
        settle().await;
        assert!(store.in_flight.load(Ordering::SeqCst) <= 2);
        assert_eq!(store.executions(), (released + 2).min(5));
    }
    for reader in readers {
        assert_eq!(reader.await.unwrap(), 0);
    }
    assert_eq!(store.max_in_flight.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn the_last_subscriber_leaving_tears_the_entry_down() {
    let (hub, registry, store) = (ChangeHub::new(), registry(8), Store::default());
    let mut first = follow(&registry, &hub, "q", &store);
    let mut second = follow(&registry, &hub, "q", &store);
    assert_eq!(
        (next_value(&mut first).await, next_value(&mut second).await),
        (0, 0)
    );

    drop(first);
    settle().await;
    assert_eq!((registry.len(), hub.subscriber_count()), (1, 1));

    drop(second);
    settle().await;
    assert_eq!((registry.len(), hub.subscriber_count()), (0, 0));
    let executions = store.executions();
    touch(&hub);
    settle().await;
    assert_eq!(store.executions(), executions, "no work after teardown");

    // The key is free again: a new subscriber creates a fresh entry.
    let mut again = follow(&registry, &hub, "q", &store);
    assert_eq!(next_value(&mut again).await, 0);
    assert_eq!(registry.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_slow_subscriber_skips_to_the_latest_frame_without_stalling_others() {
    let (hub, registry, store) = (ChangeHub::new(), registry(8), Store::default());
    let mut fast = follow(&registry, &hub, "q", &store);
    let mut slow = follow(&registry, &hub, "q", &store);
    assert_eq!(
        (next_value(&mut fast).await, next_value(&mut slow).await),
        (0, 0)
    );
    for data in 1..=3 {
        store.set(data);
        touch(&hub);
        assert_eq!(next_value(&mut fast).await, data);
    }
    assert_eq!(store.executions(), 2 + 3);
    // Snapshot frames need no replay: the slow subscriber takes the latest.
    assert_eq!(next_value(&mut slow).await, 3);
    assert_eq!(store.executions(), 5);
    assert!(slow.next().now_or_never().is_none());
}

#[tokio::test(start_paused = true)]
async fn a_subscriber_that_skipped_a_resumable_frame_catches_up_from_its_own_cursor() {
    let (hub, registry, store) = (ChangeHub::new(), registry(8), Store::default());
    let read = || Counted {
        store: store.clone(),
        gate: None,
        resumable: true,
    };
    let mut fast = follow_read(&registry, &hub, "q", read());
    let mut slow = follow_read(&registry, &hub, "q", read());
    assert_eq!(
        (next_value(&mut fast).await, next_value(&mut slow).await),
        (0, 0)
    );
    for data in 1..=2 {
        store.set(data);
        touch(&hub);
        assert_eq!(next_value(&mut fast).await, data);
    }
    assert_eq!(store.executions(), 4);
    // The latest frame replays from 1; the slow subscriber stands at 0.
    assert_eq!(next_value(&mut slow).await, 2);
    assert_eq!(store.executions(), 5, "one catch-up execution");

    // Both now stand where the producer does: the next frame is shared.
    store.set(3);
    touch(&hub);
    assert_eq!(
        (next_value(&mut fast).await, next_value(&mut slow).await),
        (3, 3)
    );
    assert_eq!(store.executions(), 6);
}

#[tokio::test(start_paused = true)]
async fn a_failed_shared_execution_ends_every_subscriber_and_the_entry() {
    let (hub, registry, store) = (ChangeHub::new(), registry(8), Store::default());
    let mut first = follow(&registry, &hub, "q", &store);
    let mut second = follow(&registry, &hub, "q", &store);
    assert_eq!(
        (next_value(&mut first).await, next_value(&mut second).await),
        (0, 0)
    );

    store.fail.store(true, Ordering::SeqCst);
    touch(&hub);
    for stream in [&mut first, &mut second] {
        assert!(stream.next().await.expect("failure frame").is_err());
        assert!(stream.next().await.is_none(), "a failure is terminal");
    }
    assert_eq!((registry.len(), hub.subscriber_count()), (0, 0));

    store.fail.store(false, Ordering::SeqCst);
    let mut fresh = follow(&registry, &hub, "q", &store);
    assert_eq!(next_value(&mut fresh).await, 0);
}

#[tokio::test(start_paused = true)]
async fn a_closed_hub_ends_every_subscriber() {
    let (registry, store) = (registry(8), Store::default());
    let hub = ChangeHub::new();
    let mut stream = follow(&registry, &hub, "q", &store);
    assert_eq!(next_value(&mut stream).await, 0);
    drop(hub);
    assert!(stream.next().await.is_none());
    assert_eq!(registry.len(), 0);
}
