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
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("kafka admin client");
    let names = ["cmd", "evt"].map(|kind| format!("{namespace}.{kind}"));
    // -1: the broker's defaults.
    let topics = names
        .iter()
        .map(|name| NewTopic::new(name, -1, TopicReplication::Fixed(-1)))
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
