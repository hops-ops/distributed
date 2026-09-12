//! Kafka [`Bus`] + [`BusConsumer`].
//!
//! Kafka shines as a partitioned, replayable log, and the point-to-point vs
//! fan-out distinction is entirely a **consumer-group** choice:
//!
//! - **`send` / `listen` (point-to-point, competing):** commands go to topics
//!   `{ns}.cmd.{name}`. `listen` joins a **shared** consumer group
//!   `{ns}.{group}.cmd`, so Kafka distributes the topic partitions across the
//!   group's members — each record is handled by exactly one replica.
//! - **`publish` / `subscribe` (fan-out):** events go to topics `{ns}.evt.{name}`.
//!   `subscribe` joins a group **per service** (`{ns}.{group}.evt`). Kafka
//!   delivers every record to every group, so each distinct `group` sees every
//!   event (replicas within a group still share its partitions → competing).
//!
//! The dispatched message name is the topic with its `{ns}.cmd.`/`{ns}.evt.`
//! prefix stripped. `{ns}` (namespace) scopes topics and groups so runs/apps
//! don't collide.
//!
//! Requires the `kafka` feature. Integration-tested in `tests/kafka_transport`.

use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::StreamConsumer;
use tokio::sync::Mutex;

use super::kafka::{KafkaPublisher, KafkaSource};
use super::{
    run_source, Bus, BusConsumer, BusTopologyConfig, MessagePublisher, MessageRouter, RunOptions,
    TransportError,
};
use super::{Message, MessageKind};

const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(8);
type ConsumerKey = (String, Vec<String>);

fn consumer_key(group_id: String, mut topics: Vec<String>) -> ConsumerKey {
    topics.sort();
    topics.dedup();
    (group_id, topics)
}

/// Kafka [`Bus`] + [`BusConsumer`]. Cheap to clone.
#[derive(Clone)]
pub struct KafkaBus {
    brokers: String,
    publisher: Arc<KafkaPublisher>,
    topology: BusTopologyConfig,
    fetch_timeout: Duration,
    fetch_max: usize,
    consumers: Arc<Mutex<HashMap<ConsumerKey, Arc<StreamConsumer>>>>,
}

/// Awaitable builder returned by [`KafkaBus::connect`].
pub struct KafkaBusConnect {
    brokers: String,
    topology: BusTopologyConfig,
    fetch_timeout: Duration,
}

impl KafkaBusConnect {
    /// Set an explicit Kafka consumer group. Service consumers can usually omit
    /// this and use [`Service::named`](crate::microsvc::Service::named) instead.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.topology = self.topology.group(group);
        self
    }

    /// Set the topic/group-id namespace used on the shared Kafka cluster.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.topology = self.topology.namespace(namespace);
        self
    }

    /// Override how long a `listen`/`subscribe` poll waits before idling. Kafka
    /// group bootstrap/rebalance takes time, so this is generous by default.
    pub fn with_fetch_timeout(mut self, timeout: Duration) -> Self {
        self.fetch_timeout = timeout;
        self
    }

    async fn connect(self) -> Result<KafkaBus, TransportError> {
        let topology = self.topology.validate_for("kafka")?;
        let publisher = KafkaPublisher::connect(&self.brokers).await?;
        Ok(KafkaBus {
            brokers: self.brokers,
            publisher: Arc::new(publisher),
            topology,
            fetch_timeout: self.fetch_timeout,
            fetch_max: 32,
            consumers: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

impl IntoFuture for KafkaBusConnect {
    type Output = Result<KafkaBus, TransportError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.connect())
    }
}

impl KafkaBus {
    /// Start building a bus connected to Kafka brokers.
    ///
    /// The returned builder is awaitable:
    ///
    /// ```ignore
    /// let bus = KafkaBus::connect("localhost:9092")
    ///     .namespace("todos-prod")
    ///     .await?;
    /// ```
    pub fn connect(brokers: &str) -> KafkaBusConnect {
        KafkaBusConnect {
            brokers: brokers.to_string(),
            topology: BusTopologyConfig::default(),
            fetch_timeout: DEFAULT_FETCH_TIMEOUT,
        }
    }

    /// Connect with an explicit group and namespace for direct/low-level use.
    pub async fn connect_with(
        brokers: &str,
        group: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, TransportError> {
        Self::connect(brokers)
            .group(group)
            .namespace(namespace)
            .await
    }

    /// Set an explicit Kafka consumer group on an already-built bus.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.topology = self.topology.group(group);
        self
    }

    /// Set the topic/group-id namespace used on the shared Kafka cluster.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.topology = self.topology.namespace(namespace);
        self
    }

    /// Override how long a `listen`/`subscribe` poll waits before idling. Kafka
    /// group bootstrap/rebalance takes time, so this is generous by default.
    pub fn with_fetch_timeout(mut self, timeout: Duration) -> Self {
        self.fetch_timeout = timeout;
        self
    }

    fn validated_namespace(&self) -> Result<String, TransportError> {
        self.topology.namespace_for("kafka")
    }

    fn command_prefix(&self) -> Result<String, TransportError> {
        Ok(format!("{}.cmd.", self.validated_namespace()?))
    }

    fn event_prefix(&self) -> Result<String, TransportError> {
        Ok(format!("{}.evt.", self.validated_namespace()?))
    }

    /// Shared consume path for `listen` (commands) and `subscribe` (events):
    /// join group `{ns}.{group}.{cmd|evt}` on topics `{ns}.{cmd|evt}.{name}`.
    /// An empty plan returns `Ok(())` before any namespace/group resolution.
    async fn consume<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
        kind: MessageKind,
    ) -> Result<(), TransportError> {
        let plan = router.subscription_plan();
        let (names, suffix) = match kind {
            MessageKind::Command => (plan.commands, "cmd"),
            MessageKind::Event => (plan.events, "evt"),
        };
        if names.is_empty() {
            return Ok(());
        }
        let namespace = self.validated_namespace()?;
        let prefix = format!("{namespace}.{suffix}.");
        let topics: Vec<String> = names.iter().map(|name| format!("{prefix}{name}")).collect();
        let group = self
            .topology
            .resolve_consumer_group(router.as_ref(), "kafka")?;
        let group_id = format!("{namespace}.{group}.{suffix}");
        let consumer_key = consumer_key(group_id.clone(), topics);
        let topics = &consumer_key.1;
        let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
        let consumer = {
            let mut cache = self.consumers.lock().await;
            if let Some(existing) = cache.get(&consumer_key) {
                Arc::clone(existing)
            } else {
                let created = KafkaSource::connect(&self.brokers, &group_id, &topic_refs)
                    .await?
                    .consumer;
                cache.insert(consumer_key, Arc::clone(&created));
                created
            }
        };
        let source = KafkaSource::new(consumer)
            .with_fetch_timeout(self.fetch_timeout)
            .with_fetch_max(self.fetch_max)
            .with_strip_prefix(prefix);
        run_source(router, source, options).await
    }
}

impl Bus for KafkaBus {
    async fn send_message(&self, mut message: Message) -> Result<(), TransportError> {
        // The publisher uses the message name as the topic; namespace it.
        message.name = format!("{}{}", self.command_prefix()?, message.name);
        self.publisher.publish(message).await
    }

    async fn publish_message(&self, mut message: Message) -> Result<(), TransportError> {
        message.name = format!("{}{}", self.event_prefix()?, message.name);
        self.publisher.publish(message).await
    }
}

impl BusConsumer for KafkaBus {
    async fn listen<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
    ) -> Result<(), TransportError> {
        self.consume(router, options, MessageKind::Command).await
    }

    async fn subscribe<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
    ) -> Result<(), TransportError> {
        self.consume(router, options, MessageKind::Event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::SubscriptionPlan;
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::FutureProducer;

    struct EmptyRouter;

    impl MessageRouter for EmptyRouter {
        fn handles(&self, _kind: MessageKind, _name: &str) -> bool {
            false
        }

        fn subscription_plan(&self) -> SubscriptionPlan {
            SubscriptionPlan::default()
        }

        async fn dispatch(&self, _message: &Message) -> Result<(), TransportError> {
            Ok(())
        }
    }

    fn test_bus() -> KafkaBus {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:1")
            .create()
            .unwrap();
        KafkaBus {
            brokers: "localhost:1".to_string(),
            publisher: Arc::new(KafkaPublisher::new(producer)),
            topology: BusTopologyConfig::default(),
            fetch_timeout: Duration::from_millis(1),
            fetch_max: 32,
            consumers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[tokio::test]
    async fn listen_returns_ok_for_empty_plan_without_group() {
        let bus = test_bus();
        let router = Arc::new(EmptyRouter);
        bus.listen(router, RunOptions::idempotent()).await.unwrap();
    }

    #[tokio::test]
    async fn subscribe_returns_ok_for_empty_plan_without_group() {
        let bus = test_bus();
        let router = Arc::new(EmptyRouter);
        bus.subscribe(router, RunOptions::idempotent())
            .await
            .unwrap();
    }

    #[test]
    fn consumer_cache_key_is_independent_of_topic_order_and_duplicates() {
        assert_eq!(
            consumer_key(
                "orders".to_string(),
                vec![
                    "orders.cmd.create".to_string(),
                    "orders.cmd.cancel".to_string()
                ]
            ),
            consumer_key(
                "orders".to_string(),
                vec![
                    "orders.cmd.cancel".to_string(),
                    "orders.cmd.create".to_string(),
                    "orders.cmd.create".to_string(),
                ]
            )
        );
    }
}
