//! Kafka bus: the bus contract over `KafkaBus` against a broker. Skips when
//! `KAFKA_BROKERS` is unset.
//!
//! `unique` mixes wall-clock nanos into every namespace, so a fresh topic
//! never holds a previous run's messages (Kafka keeps topics).
//!
//! Each test creates its topics first (`common::kafka::create_bus_topics`).

use std::time::Duration;

use common::env as env_support;
use common::ids::unique;
use common::transport::bus::{BusCapabilities, BusHarness, KindCapabilities};
use distributed::bus::KafkaBus;

/// One namespace (topic prefix) per test.
struct Kafka {
    brokers: String,
    namespace: String,
}

/// Kafka keeps no record of ids, keeps a partition in order (a nacked
/// record is read again from its offset), and keys records by stream, so a
/// stream's names share a partition.
const KAFKA: KindCapabilities = KindCapabilities {
    dedupes_ids: false,
    retry_holds_position: true,
    ordered_across_names: true,
};

impl BusHarness for Kafka {
    type Bus = KafkaBus;

    const CAPABILITIES: BusCapabilities = BusCapabilities {
        commands: KAFKA,
        events: KAFKA,
        // A record handled but not yet committed when a partition moves to
        // another group member is read again there.
        rebalance_may_redeliver: true,
        added_metadata: &[],
    };

    async fn start() -> Option<Self> {
        let kafka = Self {
            brokers: env_support::require_service("KAFKA_BROKERS", "kafka transport test")?,
            namespace: unique("ns"),
        };
        common::kafka::create_bus_topics(&kafka.brokers, &kafka.namespace).await;
        Some(kafka)
    }

    async fn bus(&self) -> KafkaBus {
        KafkaBus::connect(&self.brokers)
            .namespace(&self.namespace)
            .await
            .expect("connect bus")
            .with_fetch_timeout(Duration::from_secs(10))
    }
}

common::bus_conformance_tests!(Kafka);
