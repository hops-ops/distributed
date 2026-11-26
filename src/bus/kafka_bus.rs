//! Kafka [`Bus`] + [`BusConsumer`].
//!
//! Kafka shines as a partitioned, replayable log, and the point-to-point vs
//! fan-out distinction is entirely a **consumer-group** choice:
//!
//! - **`send` / `listen` (point-to-point, competing):** commands go to the
//!   topic `{ns}.cmd`. `listen` joins a **shared** consumer group
//!   `{ns}.{group}.cmd`, so Kafka distributes the topic partitions across the
//!   group's members — each record is handled by exactly one replica.
//! - **`publish` / `subscribe` (fan-out):** events go to the topic `{ns}.evt`.
//!   `subscribe` joins a group **per service** (`{ns}.{group}.evt`). Kafka
//!   delivers every record to every group, so each distinct `group` sees every
//!   event (replicas within a group still share its partitions → competing).
//!
//! One topic per kind, keyed by stream id, keeps one aggregate's messages in
//! order across names (`created` before `shipped`). The message name travels
//! in the [`WireEnvelope::NAME`](super::WireEnvelope::NAME) header; a consumer
//! receives every name of its kind and acknowledges the ones it does not
//! handle. `{ns}` (namespace) scopes topics and groups so runs/apps don't
//! collide.
//!
//! Requires the `kafka` feature. Integration-tested in `tests/transports/kafka_transport`.

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use super::kafka::{KafkaPublisher, KafkaSource, DEFAULT_FETCH_TIMEOUT};
use super::{
    run_source, Bus, BusConsumer, BusTopologyConfig, MessageRouter, PublishDeadLetters, RunOptions,
    TransportError,
};
use super::{Message, MessageKind};

/// Kafka [`Bus`] + [`BusConsumer`]. Cheap to clone.
#[derive(Clone)]
pub struct KafkaBus {
    brokers: String,
    publisher: Arc<KafkaPublisher>,
    topology: BusTopologyConfig,
    fetch_timeout: Duration,
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

    /// Bound one wait of a `listen`/`subscribe` consumer on the broker (8 s
    /// by default): for its group to be joined, then for the next record. A
    /// consumer is idle once it has read its partitions to their end, not
    /// after this much quiet; a wait that runs out also ends an
    /// [`until_idle`](super::RunOptions::until_idle) drain, so a broker that
    /// does not answer does not hang it.
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

    /// Bound one wait of a `listen`/`subscribe` consumer on the broker (8 s
    /// by default): for its group to be joined, then for the next record. A
    /// consumer is idle once it has read its partitions to their end, not
    /// after this much quiet; a wait that runs out also ends an
    /// [`until_idle`](super::RunOptions::until_idle) drain, so a broker that
    /// does not answer does not hang it.
    pub fn with_fetch_timeout(mut self, timeout: Duration) -> Self {
        self.fetch_timeout = timeout;
        self
    }

    /// The topic every message of `kind` goes to.
    fn topic(&self, kind: MessageKind) -> Result<String, TransportError> {
        Ok(format!(
            "{}.{}",
            self.topology.namespace_for("kafka")?,
            kind_suffix(kind)
        ))
    }

    /// Shared consume path for `listen` (commands) and `subscribe` (events):
    /// join group `{ns}.{group}.{cmd|evt}` on topic `{ns}.{cmd|evt}`. An empty
    /// plan returns `Ok(())` before any namespace/group resolution.
    async fn consume<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
        kind: MessageKind,
    ) -> Result<(), TransportError> {
        let plan = router.subscription_plan();
        let names = match kind {
            MessageKind::Command => plan.commands,
            MessageKind::Event => plan.events,
        };
        if names.is_empty() {
            return Ok(());
        }
        let topic = self.topic(kind)?;
        let group = self
            .topology
            .resolve_consumer_group(router.as_ref(), "kafka")?;
        let group_id = format!(
            "{}.{group}.{}",
            self.topology.namespace_for("kafka")?,
            kind_suffix(kind)
        );
        let source = KafkaSource::connect(&self.brokers, &group_id, &topic)
            .await?
            .with_fetch_timeout(self.fetch_timeout);
        let options = options.or_dead_letters(|| Arc::new(PublishDeadLetters(self.clone())));
        run_source(router, source, options).await
    }
}

fn kind_suffix(kind: MessageKind) -> &'static str {
    match kind {
        MessageKind::Command => "cmd",
        MessageKind::Event => "evt",
    }
}

impl Bus for KafkaBus {
    async fn send_message(&self, message: Message) -> Result<(), TransportError> {
        let topic = self.topic(MessageKind::Command)?;
        self.publisher.send_to(&topic, message).await
    }

    async fn publish_message(&self, message: Message) -> Result<(), TransportError> {
        let topic = self.topic(MessageKind::Event)?;
        self.publisher.send_to(&topic, message).await
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
        }
    }

    #[tokio::test]
    async fn listen_returns_ok_for_empty_plan_without_group() {
        let bus = test_bus();
        let router = Arc::new(EmptyRouter);
        bus.listen(router, RunOptions::until_idle()).await.unwrap();
    }

    #[tokio::test]
    async fn subscribe_returns_ok_for_empty_plan_without_group() {
        let bus = test_bus();
        let router = Arc::new(EmptyRouter);
        bus.subscribe(router, RunOptions::until_idle())
            .await
            .unwrap();
    }
}
