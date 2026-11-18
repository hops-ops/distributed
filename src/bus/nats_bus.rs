//! NATS JetStream [`Bus`] + [`BusConsumer`].
//!
//! One JetStream stream backs the bus, bound to `{namespace}.>`. The two bus
//! surfaces map onto subjects and durable consumers like this:
//!
//! - `send(name)` → subject `{namespace}.cmd.{name}` (command).
//! - `publish(name)` → subject `{namespace}.evt.{name}` (event).
//! - `listen` → a durable pull consumer named `{group}.cmd`, filtered to the
//!   service's command subjects. All replicas sharing a `group` bind the **same**
//!   durable, so JetStream load-balances commands across them — point-to-point /
//!   competing-consumer.
//! - `subscribe` → a durable pull consumer named `{group}.evt`, filtered to the
//!   service's event subjects. Each distinct `group` gets its **own** durable on
//!   the shared stream, so every group sees every event — fan-out.
//!
//! The `group` is the logical consumer identity (the service/deployment name).
//! Same group ⇒ competing; different groups ⇒ independent fan-out copies.
//!
//! Requires the `nats` feature. Integration-tested in `tests/nats_transport`.

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::stream::{Config as StreamConfig, Stream};

use super::nats::{NatsJetStreamSource, NatsPublisher};
use super::{
    retryable, run_source, Bus, BusConsumer, BusTopologyConfig, MessagePublisher, MessageRouter,
    RunOptions, TransportError,
};
use super::{Message, MessageKind};

const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_millis(500);

/// NATS JetStream [`Bus`] + [`BusConsumer`]. Cheap to clone.
#[derive(Clone)]
pub struct NatsBus {
    jetstream: jetstream::Context,
    cmd_publisher: Arc<NatsPublisher>,
    evt_publisher: Arc<NatsPublisher>,
    topology: BusTopologyConfig,
    fetch_timeout: Duration,
}

/// Awaitable builder returned by [`NatsBus::connect`].
pub struct NatsBusConnect {
    url: String,
    topology: BusTopologyConfig,
    fetch_timeout: Duration,
}

impl NatsBusConnect {
    /// Set an explicit durable consumer group. Service consumers can usually omit
    /// this and use [`Service::named`](crate::microsvc::Service::named) instead.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.topology = self.topology.group(group);
        self
    }

    /// Set the subject/stream namespace used on the shared NATS server.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.topology = self.topology.namespace(namespace);
        self
    }

    /// Override how long a `listen`/`subscribe` poll waits before idling.
    pub fn with_fetch_timeout(mut self, timeout: Duration) -> Self {
        self.fetch_timeout = timeout;
        self
    }

    async fn connect(self) -> Result<NatsBus, TransportError> {
        let topology = self.topology.validate_for("nats")?;
        let client = async_nats::connect(&self.url)
            .await
            .map_err(|err| retryable("nats connect", err))?;
        Ok(NatsBus::new(jetstream::new(client))
            .with_topology(topology)
            .with_fetch_timeout(self.fetch_timeout))
    }
}

impl IntoFuture for NatsBusConnect {
    type Output = Result<NatsBus, TransportError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.connect())
    }
}

impl NatsBus {
    /// Build a bus over an existing JetStream context.
    pub fn new(jetstream: jetstream::Context) -> Self {
        let namespace = BusTopologyConfig::default_namespace();
        let cmd_publisher =
            NatsPublisher::new(jetstream.clone()).with_subject_prefix(format!("{namespace}.cmd"));
        let evt_publisher =
            NatsPublisher::new(jetstream.clone()).with_subject_prefix(format!("{namespace}.evt"));
        Self {
            jetstream,
            cmd_publisher: Arc::new(cmd_publisher),
            evt_publisher: Arc::new(evt_publisher),
            topology: BusTopologyConfig::default(),
            fetch_timeout: DEFAULT_FETCH_TIMEOUT,
        }
    }

    /// Start building a bus connected to a NATS server URL.
    ///
    /// The returned builder is awaitable:
    ///
    /// ```ignore
    /// let bus = NatsBus::connect("nats://localhost:4222")
    ///     .namespace("todos-prod")
    ///     .await?;
    /// ```
    pub fn connect(url: &str) -> NatsBusConnect {
        NatsBusConnect {
            url: url.to_string(),
            topology: BusTopologyConfig::default(),
            fetch_timeout: DEFAULT_FETCH_TIMEOUT,
        }
    }

    /// Connect with an explicit group and namespace for direct/low-level use.
    pub async fn connect_with(
        url: &str,
        group: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, TransportError> {
        Self::connect(url).group(group).namespace(namespace).await
    }

    /// Set an explicit durable consumer group on an already-built bus.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.topology = self.topology.group(group);
        self
    }

    fn with_topology(mut self, topology: BusTopologyConfig) -> Self {
        self.update_publishers(topology.namespace_unchecked());
        self.topology = topology;
        self
    }

    /// Set the subject/stream namespace used on the shared NATS server.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        self.update_publishers(&namespace);
        self.topology = self.topology.namespace(namespace);
        self
    }

    /// Override how long a `listen`/`subscribe` poll waits before idling.
    pub fn with_fetch_timeout(mut self, timeout: Duration) -> Self {
        self.fetch_timeout = timeout;
        self
    }

    /// Sanitize the group into a valid NATS consumer-name token. Consumer names
    /// cannot contain `.`, `*`, `>`, or whitespace, so map them to `_`.
    fn durable_base(group: &str) -> String {
        group
            .chars()
            .map(|c| match c {
                '.' | '*' | '>' | ' ' | '\t' | '\n' | '/' | '\\' => '_',
                other => other,
            })
            .collect()
    }

    fn validated_namespace(&self) -> Result<String, TransportError> {
        self.topology.namespace_for("nats")
    }

    fn update_publishers(&mut self, namespace: &str) {
        self.cmd_publisher = Arc::new(
            NatsPublisher::new(self.jetstream.clone())
                .with_subject_prefix(format!("{namespace}.cmd")),
        );
        self.evt_publisher = Arc::new(
            NatsPublisher::new(self.jetstream.clone())
                .with_subject_prefix(format!("{namespace}.evt")),
        );
    }

    fn stream_name(namespace: &str) -> String {
        namespace.to_uppercase().replace(['.', '-'], "_")
    }

    /// Create-or-open the backing stream (`{namespace}.>`). Called by
    /// `listen`/`subscribe`; producers should ensure it exists (here, via IaC, or
    /// by a consumer) before publishing, since JetStream rejects a publish to an
    /// unbound subject.
    pub async fn ensure_stream(&self) -> Result<Stream, TransportError> {
        let namespace = self.validated_namespace()?;
        self.jetstream
            .get_or_create_stream(StreamConfig {
                name: Self::stream_name(&namespace),
                subjects: vec![format!("{namespace}.>")],
                ..Default::default()
            })
            .await
            .map_err(|err| retryable("nats get_or_create_stream", err))
    }

    /// Build a durable pull source over the bus stream, filtered to `subjects`,
    /// stripping `strip_prefix` so the dispatched message name is the bare name.
    async fn source(
        &self,
        durable: &str,
        subjects: Vec<String>,
        strip_prefix: String,
    ) -> Result<NatsJetStreamSource, TransportError> {
        let stream = self.ensure_stream().await?;
        let consumer = stream
            .get_or_create_consumer(
                durable,
                PullConfig {
                    durable_name: Some(durable.to_string()),
                    filter_subjects: subjects,
                    ..Default::default()
                },
            )
            .await
            .map_err(|err| retryable("nats get_or_create_consumer", err))?;
        Ok(NatsJetStreamSource::new(consumer)
            .with_fetch_timeout(self.fetch_timeout)
            .with_strip_prefix(strip_prefix))
    }

    /// Shared consume path for `listen` (commands) and `subscribe` (events):
    /// durable `{group}_{cmd|evt}` filtered to `{ns}.{cmd|evt}.{name}` subjects.
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
        let subjects: Vec<String> = names.iter().map(|name| format!("{prefix}{name}")).collect();
        let group = self
            .topology
            .resolve_consumer_group(router.as_ref(), "nats")?;
        let source = self
            .source(
                &format!("{}_{suffix}", Self::durable_base(&group)),
                subjects,
                prefix,
            )
            .await?;
        run_source(router, source, options).await
    }
}

impl Bus for NatsBus {
    async fn send_message(&self, message: Message) -> Result<(), TransportError> {
        self.validated_namespace()?;
        self.cmd_publisher.publish(message).await
    }

    async fn publish_message(&self, message: Message) -> Result<(), TransportError> {
        self.validated_namespace()?;
        self.evt_publisher.publish(message).await
    }
}

impl BusConsumer for NatsBus {
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
