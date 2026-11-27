//! `KafkaSource` against a broker: when it is idle, and that it waits
//! rather than spins once caught up. Skips when `KAFKA_BROKERS` is unset.
//!
//! A source is idle once its consumer has joined its group and read every
//! assigned partition to its end. The fetch timeout only bounds one wait on
//! the broker, so these tests set it far above anything they wait for: a
//! source that idled on quiet instead of on end of partition would take the
//! whole timeout, and one that idled before its group joined would miss the
//! records waiting for it.

use std::future::{ready, Ready};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::env as env_support;
use common::ids::unique;
use distributed::bus::{
    run_source, Handlers, IdleGroup, KafkaPublisher, KafkaSource, Message, MessageKind,
    MessageSource, ReceivedMessage, RunOptions, RunUntil, Shutdown, TransportError,
};

/// Far longer than joining a group and reading a topic take, even on a
/// loaded host.
const LONG: Duration = Duration::from_secs(60);
const NAME: &str = "source.probe";

struct Topic {
    brokers: String,
    name: String,
}

impl Topic {
    /// A fresh topic of `partitions` partitions, or `None` without a broker.
    async fn create(partitions: i32) -> Option<Self> {
        let brokers = env_support::require_service("KAFKA_BROKERS", "kafka source test")?;
        let name = unique("source");
        common::kafka::create_topic(&brokers, &name, partitions).await;
        Some(Self { brokers, name })
    }

    async fn source(&self, fetch_timeout: Duration) -> KafkaSource {
        self.source_in(&unique("group"), fetch_timeout).await
    }

    async fn source_in(&self, group: &str, fetch_timeout: Duration) -> KafkaSource {
        KafkaSource::connect(&self.brokers, group, &self.name)
            .await
            .expect("connect source")
            .with_fetch_timeout(fetch_timeout)
    }

    async fn produce(&self, ids: &[String]) {
        let publisher = KafkaPublisher::connect(&self.brokers)
            .await
            .expect("connect publisher");
        for id in ids {
            let message = Message::new(NAME, MessageKind::Event, b"{}".to_vec()).with_id(id);
            publisher
                .send_to(&self.name, message)
                .await
                .expect("produce");
        }
    }
}

fn ids(total: usize) -> Vec<String> {
    (0..total).map(|n| format!("m{n}")).collect()
}

/// Receive and acknowledge until the source is idle.
async fn drain(source: &mut KafkaSource) -> Vec<String> {
    let mut seen = Vec::new();
    while let Some(received) = source.recv().await.expect("recv") {
        seen.push(received.message().id().unwrap_or_default().to_string());
        received.ack().await.expect("ack");
    }
    seen
}

/// An empty topic is idle once the group has joined and every partition has
/// reported its end: well inside the fetch timeout, not after it.
#[tokio::test]
async fn an_empty_topic_is_idle_once_every_partition_is_at_its_end() {
    let Some(topic) = Topic::create(3).await else {
        return;
    };
    let mut source = topic.source(LONG).await;
    let started = Instant::now();
    assert!(
        source.recv().await.expect("recv").is_none(),
        "nothing to read"
    );
    let took = started.elapsed();
    assert!(
        took < LONG / 2,
        "idle after {took:?}, fetch timeout {LONG:?}"
    );
}

/// A topic that does not exist yet assigns the group no partition, which is
/// idle at once. The group still reads what is produced once the topic
/// exists.
#[tokio::test]
async fn a_topic_that_does_not_exist_yet_is_idle_and_read_once_it_exists() {
    let Some(brokers) = env_support::require_service("KAFKA_BROKERS", "kafka source test") else {
        return;
    };
    let topic = Topic {
        brokers,
        name: unique("source"),
    };
    let group = unique("group");
    let mut source = topic.source_in(&group, LONG).await;
    let started = Instant::now();
    assert!(
        source.recv().await.expect("recv").is_none(),
        "no topic, nothing to read"
    );
    let took = started.elapsed();
    assert!(
        took < LONG / 2,
        "idle after {took:?}, fetch timeout {LONG:?}"
    );
    drop(source);

    common::kafka::create_topic(&topic.brokers, &topic.name, 1).await;
    topic.produce(&ids(2)).await;
    let mut source = topic.source_in(&group, LONG).await;
    assert_eq!(drain(&mut source).await, ids(2));
}

/// Records produced before the consumer starts are all read before it is
/// idle: it is not idle before its group has assigned it partitions.
#[tokio::test]
async fn records_waiting_are_read_before_the_source_is_idle() {
    let Some(topic) = Topic::create(3).await else {
        return;
    };
    let expected = ids(12);
    topic.produce(&expected).await;
    let mut source = topic.source(LONG).await;

    let mut seen = drain(&mut source).await;
    seen.sort();
    let mut expected = expected;
    expected.sort();
    assert_eq!(seen, expected);
}

/// A record produced after the source reached the end is delivered: `ready`
/// waits for it, and the next `recv` hands it over.
#[tokio::test]
async fn a_record_produced_after_the_end_is_delivered() {
    let Some(topic) = Topic::create(1).await else {
        return;
    };
    let mut source = topic.source(LONG).await;
    assert!(
        drain(&mut source).await.is_empty(),
        "caught up on an empty topic"
    );

    topic.produce(&["late".to_string()]).await;
    let started = Instant::now();
    source.ready().await;
    let received = source
        .recv()
        .await
        .expect("recv")
        .expect("the late record is delivered");
    assert_eq!(received.message().id(), Some("late"));
    assert!(
        started.elapsed() < LONG,
        "ready returned with the record, not the timeout"
    );
    received.ack().await.expect("ack");
}

/// Counts `recv` calls of the source it wraps.
struct Counting<S> {
    source: S,
    calls: Arc<AtomicUsize>,
}

impl<S: MessageSource> MessageSource for Counting<S> {
    type Received = S::Received;

    async fn recv(&mut self) -> Result<Option<S::Received>, TransportError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.source.recv().await
    }

    async fn ready(&mut self) {
        self.source.ready().await
    }
}

/// A caught-up run until shutdown waits on the broker between receives: over
/// a few seconds with a one-second fetch timeout it calls `recv` a handful of
/// times, not in a loop. (Several worker threads, so a spinning run still
/// sees the shutdown and the test fails instead of hanging.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_caught_up_run_until_shutdown_does_not_spin() {
    let Some(topic) = Topic::create(1).await else {
        return;
    };
    let fetch_timeout = Duration::from_secs(1);
    let watched = Duration::from_secs(3);
    let calls = Arc::new(AtomicUsize::new(0));
    let source = Counting {
        source: topic.source(fetch_timeout).await,
        calls: calls.clone(),
    };
    let router = Arc::new(
        Handlers::new()
            .named("spin")
            .on_event(NAME, |_: &Message| -> Ready<Result<(), TransportError>> {
                ready(Ok(()))
            }),
    );
    let shutdown = Shutdown::new();
    let stop = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(watched).await;
        stop.trigger();
    });

    tokio::time::timeout(
        LONG,
        run_source(router, source, RunOptions::until_shutdown(shutdown)),
    )
    .await
    .expect("the run stopped")
    .expect("the run ended cleanly");

    // One wait of at most the fetch timeout per call: about four in three
    // seconds. A source that did not wait in `ready` makes thousands.
    let calls = calls.load(Ordering::Relaxed);
    assert!(calls <= 10, "{calls} recv calls in {watched:?}");
}

/// A source that has caught up sees a record produced since on its next
/// `recv`, without waiting in `ready` first: a drain looks again after
/// another consumer's dispatch (`RunUntil::AllIdle`), and what that dispatch
/// produced must be there.
#[tokio::test]
async fn a_caught_up_source_sees_a_record_produced_since_on_its_next_recv() {
    let Some(topic) = Topic::create(3).await else {
        return;
    };
    let mut source = topic.source(LONG).await;
    assert!(
        drain(&mut source).await.is_empty(),
        "caught up on an empty topic"
    );

    topic.produce(&["late".to_string()]).await;
    let started = Instant::now();
    assert_eq!(drain(&mut source).await, ["late"]);
    let took = started.elapsed();
    assert!(
        took < LONG / 2,
        "read after {took:?}, fetch timeout {LONG:?}"
    );
}

/// Two consumers drain together: one handles a trigger by producing to the
/// topic the other reads, after the other has already caught up. One drain
/// delivers that record, and ends well inside the fetch timeout.
#[tokio::test]
async fn a_drain_of_two_consumers_reads_what_one_produced_for_the_other() {
    let (Some(triggers), Some(facts)) = (Topic::create(1).await, Topic::create(1).await) else {
        return;
    };
    triggers.produce(&["trigger".to_string()]).await;
    let publisher = Arc::new(
        KafkaPublisher::connect(&facts.brokers)
            .await
            .expect("connect publisher"),
    );
    let facts_topic = facts.name.clone();
    let handler = Arc::new(Handlers::new().named("drain-handler").on_event(
        NAME,
        move |_: &Message| {
            let publisher = Arc::clone(&publisher);
            let topic = facts_topic.clone();
            async move {
                // Long enough for the other consumer to catch up first.
                tokio::time::sleep(Duration::from_millis(500)).await;
                let fact = Message::new(NAME, MessageKind::Event, b"{}".to_vec()).with_id("late");
                publisher.send_to(&topic, fact).await
            }
        },
    ));
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let record = Arc::clone(&seen);
    let projector = Arc::new(Handlers::new().named("drain-projector").on_event(
        NAME,
        move |message: &Message| {
            record
                .lock()
                .unwrap()
                .push(message.id().unwrap_or_default().to_string());
            ready(Ok(()))
        },
    ));
    let drain = IdleGroup::new();
    let options = || RunOptions {
        until: RunUntil::AllIdle(drain.member()),
        ..RunOptions::until_idle()
    };

    let started = Instant::now();
    let (handled, projected) = tokio::time::timeout(
        LONG,
        futures_util::future::join(
            run_source(handler, triggers.source(LONG).await, options()),
            run_source(projector, facts.source(LONG).await, options()),
        ),
    )
    .await
    .expect("the drain ended");
    handled.expect("handler run");
    projected.expect("projector run");

    assert_eq!(*seen.lock().unwrap(), ["late"]);
    let took = started.elapsed();
    assert!(
        took < LONG / 2,
        "drained after {took:?}, fetch timeout {LONG:?}"
    );
}
