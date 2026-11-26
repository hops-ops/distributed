//! Kafka topics for tests.
//!
//! A test creates the topics `KafkaBus` uses before it starts. Left to the
//! broker's auto-creation on the first send, creating the topic counts
//! against the producer's 10 s delivery deadline: on a busy host the broker
//! took up to 9 s to create one, and sends failed with `MessageTimedOut`.
//! Topic provisioning is not what these tests test.

use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::ClientConfig;

/// Create `{namespace}.cmd` and `{namespace}.evt`, the topics a `KafkaBus`
/// with that namespace uses, with the broker's default partition count and
/// replication factor (what auto-creation would use), and wait until the
/// broker has.
pub async fn create_bus_topics(brokers: &str, namespace: &str) {
    let names = ["cmd", "evt"].map(|kind| format!("{namespace}.{kind}"));
    // -1: the broker's default.
    create_topics(brokers, &names.each_ref().map(|name| (name.as_str(), -1))).await;
}

/// Create `name` with `partitions` partitions (and the broker's default
/// replication factor), and wait until the broker has.
pub async fn create_topic(brokers: &str, name: &str, partitions: i32) {
    create_topics(brokers, &[(name, partitions)]).await;
}

async fn create_topics(brokers: &str, topics: &[(&str, i32)]) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("kafka admin client");
    let topics = topics
        .iter()
        .map(|(name, partitions)| NewTopic::new(name, *partitions, TopicReplication::Fixed(-1)))
        .collect::<Vec<_>>();
    let created = admin
        .create_topics(
            &topics,
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(60))),
        )
        .await
        .expect("create kafka topics");
    for result in created {
        result.unwrap_or_else(|(topic, error)| panic!("create topic {topic}: {error}"));
    }
}
