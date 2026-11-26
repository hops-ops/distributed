//! Kafka topics for tests.
//!
//! A test creates the topics `KafkaBus` uses before it starts. Left to the
//! broker's auto-creation on the first send, creating the topic counts
//! against the producer's 10 s delivery deadline: on a busy host the broker
//! took up to 9 s to create one, and sends failed with `MessageTimedOut`.
//! Topic provisioning is not what these tests test.
//!
//! The admin API answers once the controller has recorded a topic, before
//! the broker has created its partitions: on a loaded host the broker
//! created the partitions of one topic up to 11 s after that answer. So a
//! topic counts as created once every partition answers a `ListOffsets`
//! (which only the partition's leader, holding its log, does).

use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::ClientConfig;

/// How long the broker may take to create a topic and its partitions.
const CREATE_TIMEOUT: Duration = Duration::from_secs(60);

/// Create `{namespace}.cmd` and `{namespace}.evt`, the topics a `KafkaBus`
/// with that namespace uses, with the broker's default partition count and
/// replication factor (what auto-creation would use), and wait until every
/// partition can be produced to.
pub async fn create_bus_topics(brokers: &str, namespace: &str) {
    let names = ["cmd", "evt"].map(|kind| format!("{namespace}.{kind}"));
    // -1: the broker's default.
    create_topics(brokers, &names.each_ref().map(|name| (name.as_str(), -1))).await;
}

/// Create `name` with `partitions` partitions (and the broker's default
/// replication factor), and wait until every partition can be produced to.
pub async fn create_topic(brokers: &str, name: &str, partitions: i32) {
    create_topics(brokers, &[(name, partitions)]).await;
}

async fn create_topics(brokers: &str, topics: &[(&str, i32)]) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("kafka admin client");
    let new_topics = topics
        .iter()
        .map(|(name, partitions)| NewTopic::new(name, *partitions, TopicReplication::Fixed(-1)))
        .collect::<Vec<_>>();
    let created = admin
        .create_topics(
            &new_topics,
            &AdminOptions::new().operation_timeout(Some(CREATE_TIMEOUT)),
        )
        .await
        .expect("create kafka topics");
    for result in created {
        result.unwrap_or_else(|(topic, error)| panic!("create topic {topic}: {error}"));
    }
    let brokers = brokers.to_string();
    let names: Vec<String> = topics.iter().map(|(name, _)| name.to_string()).collect();
    tokio::task::spawn_blocking(move || wait_until_led(&brokers, &names))
        .await
        .expect("wait for kafka partitions");
}

/// Block until every partition of `topics` answers a `ListOffsets`.
/// (`fetch_metadata` and `fetch_watermarks` block; the caller runs this on
/// the blocking pool.)
fn wait_until_led(brokers: &str, topics: &[String]) {
    let client: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("kafka metadata client");
    let deadline = Instant::now() + CREATE_TIMEOUT;
    let request = Duration::from_secs(5);
    for topic in topics {
        loop {
            let led = client
                .fetch_metadata(Some(topic), request)
                .ok()
                .and_then(|metadata| {
                    let found = metadata.topics().first()?;
                    let partitions = found.partitions();
                    (found.error().is_none() && !partitions.is_empty()).then(|| {
                        partitions.iter().all(|partition| {
                            client
                                .fetch_watermarks(topic, partition.id(), request)
                                .is_ok()
                        })
                    })
                })
                .unwrap_or(false);
            if led {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "kafka topic {topic} has no leader for every partition after {CREATE_TIMEOUT:?}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}
