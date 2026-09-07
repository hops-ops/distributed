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
//! Startup reconciles that durable's subject filters without resetting its
//! acknowledgement cursor or broker tuning. Replicas sharing a group must use
//! the same subscription inventory. Adding a filter does not replay messages
//! previously skipped by that consumer; historical catch-up is explicit.
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
    idle_poll: Duration,
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
            idle_poll: Duration::ZERO,
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

    /// Keep `listen`/`subscribe` running after an empty JetStream fetch.
    /// Drain-to-idle is for tests; long-running hosts must set this.
    pub fn with_idle_poll(mut self, idle_poll: Duration) -> Self {
        self.idle_poll = idle_poll;
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

    /// Read a bounded, stable archive of retained canonical domain events.
    ///
    /// Does not create streams/consumers, acknowledge deliveries, or publish.
    /// Rejects truncated, gapped, changing, or oversized streams. Noncanonical
    /// integration events and commands are excluded. A complete broker archive
    /// is NOT proof that every historical aggregate publication reached it:
    /// stop producers and drain outboxes before using this for maintenance.
    pub async fn retained_domain_events(
        &self,
    ) -> Result<Vec<crate::DomainEventOccurrence>, TransportError> {
        let namespace = self.validated_namespace()?;
        let mut stream = self
            .jetstream
            .get_stream(Self::stream_name(&namespace))
            .await
            .map_err(|e| retryable("open domain-event archive", e))?;
        let before = stream
            .info()
            .await
            .map_err(|e| retryable("read archive boundary", e))?
            .clone();
        let state = &before.state;
        if state.last_sequence > 100_000
            || state.bytes > 64 * 1024 * 1024
            || state.messages != state.last_sequence
            || (state.messages > 0 && state.first_sequence != 1)
        {
            return Err(TransportError::permanent(
                "domain-event archive is truncated, gapped, or exceeds 100000 messages / 64 MiB",
            ));
        }
        let prefix = format!("{namespace}.evt.");
        let mut events = Vec::new();
        let mut bytes = 0usize;
        for sequence in 1..=state.last_sequence {
            let message = stream
                .get_raw_message(sequence)
                .await
                .map_err(|e| retryable("read retained domain-event archive message", e))?;
            bytes = bytes
                .checked_add(message.payload.len())
                .ok_or_else(|| TransportError::permanent("domain-event archive size overflow"))?;
            if bytes > 64 * 1024 * 1024 {
                return Err(TransportError::permanent(
                    "domain-event archive exceeds 64 MiB",
                ));
            }
            if message.sequence != sequence {
                return Err(TransportError::permanent(
                    "domain-event archive returned a different sequence",
                ));
            }
            let Some(name) = message.subject.as_str().strip_prefix(&prefix) else {
                continue;
            };
            if message
                .headers
                .get("x-sourced-payload-codec")
                .map(|v| v.as_str())
                != Some("distributed.domain-event-occurrence+json")
            {
                continue;
            }
            let event = crate::DomainEventOccurrence::from_canonical_bytes(&message.payload)
                .map_err(|e| {
                    TransportError::permanent(format!("invalid archived occurrence: {e}"))
                })?;
            if event.descriptor().name != name
                || message.headers.get("Nats-Msg-Id").map(|v| v.as_str()) != Some(event.id())
            {
                return Err(TransportError::permanent(
                    "archived occurrence differs from its transport identity",
                ));
            }
            events.push(event);
        }
        let after = stream
            .info()
            .await
            .map_err(|e| retryable("verify archive boundary", e))?;
        if before.created != after.created
            || before.state.last_sequence != after.state.last_sequence
            || before.state.messages != after.state.messages
            || before.state.first_sequence != after.state.first_sequence
        {
            return Err(TransportError::permanent(
                "domain-event archive changed while reading; quiesce producers and retry",
            ));
        }
        Ok(events)
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
        let mut consumer = stream
            .get_or_create_consumer(
                durable,
                PullConfig {
                    durable_name: Some(durable.to_string()),
                    filter_subjects: subjects.clone(),
                    ..Default::default()
                },
            )
            .await
            .map_err(|err| retryable("nats get_or_create_consumer", err))?;
        // Opening a durable does not reconcile its existing configuration. An
        // added route otherwise never receives events after an application
        // upgrade. Update filters in place, preserving progress and tuning.
        let current = &consumer.cached_info().config;
        let mut actual = current.filter_subjects.clone();
        if !current.filter_subject.is_empty() {
            actual.push(current.filter_subject.clone());
        }
        actual.sort();
        actual.dedup();
        let mut desired = subjects;
        desired.sort();
        desired.dedup();
        if actual != desired {
            use async_nats::jetstream::consumer::FromConsumer;
            let mut config = PullConfig::try_from_consumer_config(current.clone())
                .map_err(|err| retryable("nats read durable config", err))?;
            config.filter_subject.clear();
            config.filter_subjects = desired;
            consumer = stream
                .update_consumer(config)
                .await
                .map_err(|err| retryable("nats update consumer filters", err))?;
        }
        Ok(NatsJetStreamSource::new(consumer)
            .with_fetch_timeout(self.fetch_timeout)
            .with_strip_prefix(strip_prefix)
            .with_idle_poll(self.idle_poll))
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

#[cfg(test)]
mod archive_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires DISTRIBUTED_ARCHIVE_TEST_NATS_URL with JetStream"]
    async fn durable_filter_updates_preserve_progress_and_broker_tuning() {
        use futures::StreamExt;
        let url = std::env::var("DISTRIBUTED_ARCHIVE_TEST_NATS_URL").expect("test JetStream URL");
        let namespace = format!("filter-test-{}", uuid::Uuid::now_v7().simple());
        let bus = NatsBus::connect(&url).namespace(&namespace).await.unwrap();
        let stream = bus.ensure_stream().await.unwrap();
        let first = format!("{namespace}.evt.first");
        let added = format!("{namespace}.evt.added");
        let mut consumer = stream
            .create_consumer(PullConfig {
                durable_name: Some("filter-proof".into()),
                filter_subjects: vec![first.clone()],
                ack_wait: Duration::from_secs(17),
                max_ack_pending: 29,
                ..Default::default()
            })
            .await
            .unwrap();
        let created = consumer.cached_info().created;
        bus.jetstream
            .publish(first.clone(), "first".into())
            .await
            .unwrap()
            .await
            .unwrap();
        let mut messages = consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_secs(2))
            .messages()
            .await
            .unwrap();
        let message = messages.next().await.unwrap().unwrap();
        message.double_ack().await.unwrap();
        drop(messages);
        let acknowledged = consumer.info().await.unwrap().ack_floor.stream_sequence;
        assert_eq!(acknowledged, 1);
        bus.jetstream
            .publish(added.clone(), "retained-new".into())
            .await
            .unwrap()
            .await
            .unwrap();
        // Simulate the old application polling past an unmatched input.
        let mut unmatched = consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_millis(100))
            .messages()
            .await
            .unwrap();
        assert!(unmatched.next().await.is_none());
        drop(unmatched);
        drop(
            bus.source(
                "filter-proof",
                vec![added.clone(), first.clone()],
                format!("{namespace}.evt."),
            )
            .await
            .unwrap(),
        );
        let info = consumer.info().await.unwrap();
        assert_eq!(
            info.created, created,
            "must update, not replace, the consumer"
        );
        assert_eq!(info.ack_floor.stream_sequence, acknowledged);
        assert_eq!(info.config.ack_wait, Duration::from_secs(17));
        assert_eq!(info.config.max_ack_pending, 29);
        assert_eq!(info.config.filter_subjects.len(), 2);
        bus.jetstream
            .publish(added.clone(), "future-new".into())
            .await
            .unwrap()
            .await
            .unwrap();
        let mut messages = consumer
            .fetch()
            .max_messages(2)
            .expires(Duration::from_secs(2))
            .messages()
            .await
            .unwrap();
        let mut delivered = Vec::new();
        while let Some(message) = messages.next().await {
            let message = message.unwrap();
            delivered.push(String::from_utf8(message.payload.to_vec()).unwrap());
            message.double_ack().await.unwrap();
        }
        assert!(delivered.contains(&"future-new".into()));
        assert!(
            !delivered.contains(&"first".into()),
            "acknowledged effects must not replay"
        );
        // NATS does not promise historical catch-up for an added filter. Keep
        // any observed retained delivery visible in the test output.
        eprintln!("filter update delivered: {delivered:?}");
        drop(
            bus.source(
                "filter-proof",
                vec![added.clone()],
                format!("{namespace}.evt."),
            )
            .await
            .unwrap(),
        );
        drop(
            bus.source(
                "filter-proof",
                vec![added.clone()],
                format!("{namespace}.evt."),
            )
            .await
            .unwrap(),
        );
        assert_eq!(
            consumer.info().await.unwrap().config.filter_subjects,
            vec![added]
        );
        assert_eq!(consumer.cached_info().created, created);
        // The stream was created only by this test under a fresh UUID namespace.
        bus.jetstream
            .delete_stream(NatsBus::stream_name(&namespace))
            .await
            .unwrap();
    }

    #[derive(serde::Serialize, serde::Deserialize, crate::DomainState)]
    #[domain_state(version = 1)]
    struct ArchiveState {
        title: String,
    }

    #[tokio::test]
    async fn retained_archive_is_non_consuming_and_rejects_missing_history() {
        let Ok(url) = std::env::var("DISTRIBUTED_ARCHIVE_TEST_NATS_URL") else {
            return;
        };
        let namespace = format!("archive-test-{}", uuid::Uuid::now_v7().simple());
        let bus = NatsBus::connect(&url).namespace(&namespace).await.unwrap();
        let mut stream = bus.ensure_stream().await.unwrap();
        for sequence in 1..=3 {
            let event = crate::DomainEventOccurrence::capture(
                crate::DomainEventDescriptor::state::<ArchiveState>("archive.changed", 1),
                crate::DomainEventEnvelope {
                    aggregate_type: "archive-item".into(),
                    aggregate_id: "a".into(),
                    aggregate_sequence: sequence,
                    publication_ordinal: 0,
                    occurred_at: std::time::UNIX_EPOCH,
                    metadata: Default::default(),
                },
                &ArchiveState {
                    title: sequence.to_string(),
                },
            )
            .unwrap();
            let mut headers = async_nats::HeaderMap::new();
            headers.insert(
                "x-sourced-payload-codec",
                "distributed.domain-event-occurrence+json",
            );
            headers.insert("Nats-Msg-Id", event.id());
            bus.jetstream
                .publish_with_headers(
                    format!("{namespace}.evt.archive.changed"),
                    headers,
                    event.canonical_bytes().unwrap().into(),
                )
                .await
                .unwrap()
                .await
                .unwrap();
        }
        let first = bus.retained_domain_events().await.unwrap();
        assert_eq!(first.len(), 3);
        assert_eq!(bus.retained_domain_events().await.unwrap(), first);
        let info = stream.info().await.unwrap();
        assert_eq!(info.state.consumer_count, 0);
        assert_eq!(info.state.messages, 3);
        // Delete only a message in this test's fresh UUID-scoped stream.
        stream.delete_message(2).await.unwrap();
        assert!(bus
            .retained_domain_events()
            .await
            .unwrap_err()
            .to_string()
            .contains("gapped"));
        bus.jetstream
            .delete_stream(NatsBus::stream_name(&namespace))
            .await
            .unwrap();
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
