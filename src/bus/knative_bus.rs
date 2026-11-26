//! Knative Eventing [`Bus`] — produce side + manifest generation.
//!
//! Knative is a GitOps/K8s transport: the platform delivers events over HTTP to
//! per-handler routes wired by `Trigger` manifests, so there is no in-process
//! consume loop. Per the locked spec (Decision #6), `KnativeBus` implements only
//! [`Bus`] (produce) and does **not** implement
//! [`BusConsumer`](super::BusConsumer). Consuming is two deploy-time artifacts:
//!
//! - [`KnativeBus::manifests`] — the role-based `Broker` + per-name `Trigger`
//!   YAML for a service (and a local/kubefwd variant);
//! - mounting [`cloud_events_router`](crate::microsvc::cloud_events_router) so the Triggers'
//!   `/cloudevent/<type>` subscriber URIs reach `Service::dispatch_message`.
//!
//! Producing POSTs a binary-mode CloudEvent to a broker-ingress URL:
//! `publish` → the service's own `{source}-events` broker; `send` → a target
//! model's `{target}-commands` broker. Owning a broker is only required for what
//! you *receive*; publishing just needs the ingress URL.
//!
//! Requires the `http` feature.

use std::time::Duration;

use std::collections::HashSet;

use super::knative::unique_k8s_name;
use super::{Bus, TransportError};
use super::{Message, SubscriptionPlan, WireEnvelope};

const SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// CloudEvents extension attribute carrying [`WireEnvelope::KIND`].
/// CloudEvents attribute names are lowercase letters and digits only, so the
/// envelope's reserved headers map onto attributes instead of travelling as
/// `ce-x-distributed-*`: the id is `ce-id`, the content type `content-type`.
pub(crate) const KIND_ATTRIBUTE: &str = "distributedkind";

/// Context attributes this adapter writes itself. Metadata travels as
/// `ce-<key>`, so a key named like one of these would override it.
const WRITTEN_ATTRIBUTES: [&str; 6] = [
    "id",
    "type",
    "source",
    "specversion",
    "datacontenttype",
    KIND_ATTRIBUTE,
];

/// The HTTP header a [`WireEnvelope`] header travels as.
fn cloud_event_header(key: &str) -> String {
    match key {
        WireEnvelope::ID => "ce-id".to_string(),
        WireEnvelope::CONTENT_TYPE => "content-type".to_string(),
        WireEnvelope::KIND => format!("ce-{KIND_ATTRIBUTE}"),
        metadata => format!("ce-{metadata}"),
    }
}

/// Knative Eventing [`Bus`] (produce) + manifest generator.
#[derive(Clone)]
pub struct KnativeBus {
    client: reqwest::Client,
    /// Broker-ingress base, e.g. `http://broker-ingress.knative-eventing.svc.cluster.local`.
    ingress_base: String,
    /// Kubernetes namespace (broker-ingress path segment + manifest `namespace`).
    namespace: String,
    /// This service's name — the CloudEvent `source` and the manifest subject.
    source: String,
    /// Target commands broker for `send` (a downstream model's `*-commands`).
    commands_broker: String,
    /// This service's own events broker for `publish` (`{source}-events`).
    events_broker: String,
    /// Whether `manifests` should emit this service's `{source}-events` broker.
    publishes_events: bool,
    /// When set, `manifests` points Trigger subscribers at this local address
    /// (kubefwd dev flow) instead of the in-cluster service `ref`.
    local: Option<String>,
}

impl KnativeBus {
    /// Build a bus. `ingress_base` is the Knative broker-ingress base URL,
    /// `source` is this service's name, and `commands_broker`/`events_broker` are
    /// the brokers `send`/`publish` POST to.
    pub fn new(
        ingress_base: impl Into<String>,
        namespace: impl Into<String>,
        source: impl Into<String>,
        commands_broker: impl Into<String>,
        events_broker: impl Into<String>,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            ingress_base: ingress_base.into(),
            namespace: namespace.into(),
            source: source.into(),
            commands_broker: commands_broker.into(),
            events_broker: events_broker.into(),
            publishes_events: true,
            local: None,
        }
    }

    /// Set whether [`manifests`](Self::manifests) emits the `{source}-events`
    /// broker (true for model/ingestor services that publish; false for pure
    /// consumers like projections). Default: true.
    pub fn publishes_events(mut self, publishes: bool) -> Self {
        self.publishes_events = publishes;
        self
    }

    /// Point generated Trigger subscribers at `addr` (e.g. `127.0.0.1:8080`) via
    /// a local URI instead of the in-cluster service `ref` — the kubefwd dev flow.
    pub fn local(mut self, addr: impl Into<String>) -> Self {
        self.local = Some(addr.into());
        self
    }

    /// Join `ingress_base` / `namespace` / `broker`, skipping empty segments.
    fn ingress_url(&self, broker: &str) -> String {
        let mut url = self.ingress_base.trim_end_matches('/').to_string();
        for segment in [self.namespace.as_str(), broker] {
            if !segment.is_empty() {
                url.push('/');
                url.push_str(segment);
            }
        }
        url
    }

    async fn post_cloud_event(&self, broker: &str, message: Message) -> Result<(), TransportError> {
        if message.id().is_none() {
            return Err(TransportError::permanent(
                "knative produce requires a message id (CloudEvents `id` is mandatory)",
            ));
        }
        if let Some((key, _)) = message.metadata.iter().find(|(key, _)| {
            WRITTEN_ATTRIBUTES
                .iter()
                .any(|attribute| key.eq_ignore_ascii_case(attribute))
        }) {
            return Err(TransportError::permanent(format!(
                "message metadata key {key:?} is a CloudEvents attribute this adapter writes"
            )));
        }
        let envelope = WireEnvelope::encode(message)?;
        let mut request = self
            .client
            .post(self.ingress_url(broker))
            .timeout(SEND_TIMEOUT)
            .header("ce-specversion", "1.0")
            .header("ce-type", envelope.name.as_str())
            .header("ce-source", self.source.as_str());
        for (key, value) in &envelope.headers {
            request = request.header(cloud_event_header(key), value);
        }
        let response = request
            .body(envelope.payload)
            .send()
            .await
            .map_err(|err| TransportError::retryable(format!("knative POST: {err}")))?;
        if !response.status().is_success() {
            return Err(TransportError::retryable(format!(
                "knative broker-ingress returned {}",
                response.status()
            )));
        }
        Ok(())
    }

    /// Render the role-based Knative manifests for `plan`: the `{source}-commands`
    /// broker + a Trigger per handled command (if any), the `{source}-events`
    /// broker (if [`publishes_events`](Self::publishes_events)), and a Trigger per
    /// subscribed event on its producer's broker. `subscriptions` maps each
    /// subscribed event name to the producing service's events broker. Subscriber
    /// URIs are `/cloudevent/<type>` (matching [`cloud_events_router`]).
    ///
    /// [`cloud_events_router`]: crate::microsvc::cloud_events_router
    pub fn manifests(&self, plan: &SubscriptionPlan, subscriptions: &[(&str, &str)]) -> String {
        let mut out = String::new();
        // One dedup set across commands and events: their Trigger names share the
        // `{source}-{name}` shape, so a command and an event whose names normalize
        // to the same label must not collide on `metadata.name`.
        let mut used = HashSet::new();
        if !plan.commands.is_empty() {
            // The broker a service *owns* for commands it handles is
            // `{source}-commands` — distinct from `commands_broker` (the
            // downstream target `send` produces to).
            let own_commands = format!("{}-commands", self.source);
            out.push_str(&self.broker_yaml(&own_commands));
            for command in &plan.commands {
                out.push_str(&self.trigger_yaml(&own_commands, command, &mut used));
            }
        }
        if self.publishes_events {
            out.push_str(&self.broker_yaml(&self.events_broker));
        }
        for event in &plan.events {
            let broker = subscriptions
                .iter()
                .find(|(name, _)| *name == event.as_str())
                .map(|(_, broker)| *broker)
                .unwrap_or("UNMAPPED-events");
            out.push_str(&self.trigger_yaml(broker, event, &mut used));
        }
        out
    }

    fn broker_yaml(&self, name: &str) -> String {
        format!(
            "apiVersion: eventing.knative.dev/v1\n\
             kind: Broker\n\
             metadata:\n\
             \x20 name: {name}\n\
             \x20 namespace: {ns}\n\
             ---\n",
            ns = self.namespace,
        )
    }

    fn trigger_yaml(&self, broker: &str, event: &str, used: &mut HashSet<String>) -> String {
        let trigger_name = unique_k8s_name(&format!("{}-{}", self.source, event), used);
        let subscriber = match &self.local {
            Some(addr) => format!(
                "\x20 subscriber:\n\
                 \x20   uri: http://{addr}/cloudevent/{event}\n"
            ),
            None => format!(
                "\x20 subscriber:\n\
                 \x20   ref:\n\
                 \x20     apiVersion: serving.knative.dev/v1\n\
                 \x20     kind: Service\n\
                 \x20     name: {source}\n\
                 \x20   uri: /cloudevent/{event}\n",
                source = self.source,
            ),
        };
        format!(
            "apiVersion: eventing.knative.dev/v1\n\
             kind: Trigger\n\
             metadata:\n\
             \x20 name: {trigger_name}\n\
             \x20 namespace: {ns}\n\
             spec:\n\
             \x20 broker: {broker}\n\
             \x20 filter:\n\
             \x20   attributes:\n\
             \x20     type: {event}\n\
             {subscriber}\
             ---\n",
            ns = self.namespace,
        )
    }
}

impl Bus for KnativeBus {
    async fn send_message(&self, message: Message) -> Result<(), TransportError> {
        self.post_cloud_event(&self.commands_broker, message).await
    }

    async fn publish_message(&self, message: Message) -> Result<(), TransportError> {
        self.post_cloud_event(&self.events_broker, message).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_headers_map_onto_cloud_event_attributes() {
        assert_eq!(cloud_event_header(WireEnvelope::ID), "ce-id");
        assert_eq!(
            cloud_event_header(WireEnvelope::CONTENT_TYPE),
            "content-type"
        );
        assert_eq!(cloud_event_header(WireEnvelope::KIND), "ce-distributedkind");
        assert_eq!(cloud_event_header("traceparent"), "ce-traceparent");
    }

    #[tokio::test]
    async fn metadata_that_would_override_an_attribute_is_refused_before_sending() {
        // Port 1 is never listening; a refusal must come before any request.
        let bus = KnativeBus::new("http://127.0.0.1:1", "ns", "orders", "c", "e");
        for key in ["type", "Source", "distributedkind", "x-distributed-kind"] {
            let message = Message::new("order.placed", crate::bus::MessageKind::Event, vec![])
                .with_id("m-1")
                .with_metadata(key, "forged");
            let error = bus.publish_message(message).await.unwrap_err();
            assert!(error.is_permanent(), "{key}: {error}");
        }
    }
}
