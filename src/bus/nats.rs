//! NATS JetStream transport adapter.
//!
//! Maps the canonical [`Message`] onto NATS JetStream: [`NatsPublisher`] publishes
//! to a subject (waiting for the JetStream publish ack — the durable publish
//! threshold), and [`NatsJetStreamSource`] pulls from a durable consumer and
//! settles via JetStream ack semantics (ack→`Ack`, nack→`Nak`, dead-letter/park→
//! `Term`). The stable message id rides as the `Nats-Msg-Id` header so JetStream
//! dedup and downstream `Message.id` agree.
//!
//! Requires the `nats` feature. Integration-tested in `tests/nats_transport`
//! against a JetStream-enabled server (see `compose.yaml`).

use std::time::Duration;

use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::consumer::Consumer;
use async_nats::jetstream::stream::Config as StreamConfig;
use async_nats::jetstream::{self, AckKind};
use futures::StreamExt;

use crate::projection_protocol::{ProjectionEpoch, ProjectionSource};

use super::source::{MessageSource, ReceivedMessage};
use super::{message_from_wire, strip_address_prefix, Message, OrderedDelivery};
use super::{retryable, MessagePublisher, TransportError};

/// Header carrying the stable message id (and JetStream dedup key).
const MESSAGE_ID_HEADER: &str = "Nats-Msg-Id";
/// Header carrying the canonical message kind.
const MESSAGE_KIND_HEADER: &str = "X-Sourced-Kind";
/// Header carrying the canonical payload media type.
const CONTENT_TYPE_HEADER: &str = "Content-Type";

/// Publishes canonical messages to a NATS JetStream subject.
///
/// The subject defaults to the message name; override with [`with_subject_prefix`]
/// to publish to `{prefix}.{name}`.
///
/// [`with_subject_prefix`]: NatsPublisher::with_subject_prefix
pub struct NatsPublisher {
    jetstream: jetstream::Context,
    subject_prefix: Option<String>,
}

impl NatsPublisher {
    /// Create a publisher over an existing JetStream context.
    pub fn new(jetstream: jetstream::Context) -> Self {
        Self {
            jetstream,
            subject_prefix: None,
        }
    }

    /// Connect to a NATS server URL and create a JetStream publisher.
    pub async fn connect(url: &str) -> Result<Self, TransportError> {
        let client = async_nats::connect(url)
            .await
            .map_err(|err| retryable("nats connect", err))?;
        Ok(Self::new(jetstream::new(client)))
    }

    /// Publish to `{prefix}.{message.name}` instead of `{message.name}`.
    pub fn with_subject_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.subject_prefix = Some(prefix.into());
        self
    }

    fn subject(&self, message: &Message) -> String {
        match &self.subject_prefix {
            Some(prefix) => format!("{prefix}.{}", message.name()),
            None => message.name().to_string(),
        }
    }
}

impl MessagePublisher for NatsPublisher {
    async fn publish(&self, mut message: Message) -> Result<(), TransportError> {
        let subject = self.subject(&message);
        let mut headers = async_nats::HeaderMap::new();
        for (key, value) in &message.metadata {
            if [MESSAGE_ID_HEADER, MESSAGE_KIND_HEADER, CONTENT_TYPE_HEADER]
                .iter()
                .any(|reserved| key.eq_ignore_ascii_case(reserved))
            {
                continue;
            }
            headers.insert(key.as_str(), value.as_str());
        }
        if let Some(id) = message.id() {
            headers.insert(MESSAGE_ID_HEADER, id);
        }
        headers.insert(MESSAGE_KIND_HEADER, message.kind.as_str());
        headers.insert(CONTENT_TYPE_HEADER, message.content_type.as_str());

        // `message` is owned and dropped here, so move its payload out instead of
        // cloning. `Bytes::from(Vec<u8>)` takes ownership of the buffer (no copy).
        let payload = std::mem::take(&mut message.payload).into();

        // Publish ack (the durable publish threshold): both awaits must succeed.
        let ack_future = self
            .jetstream
            .publish_with_headers(subject, headers, payload)
            .await
            .map_err(|err| retryable("nats publish", err))?;
        ack_future
            .await
            .map_err(|err| retryable("nats publish ack", err))?;
        Ok(())
    }
}

/// A pull-based JetStream source bound to a durable consumer.
pub struct NatsJetStreamSource {
    consumer: Consumer<PullConfig>,
    fetch_timeout: Duration,
    strip_prefix: Option<String>,
    idle_poll: Duration,
}

impl NatsJetStreamSource {
    /// Wrap an existing durable pull consumer.
    pub fn new(consumer: Consumer<PullConfig>) -> Self {
        Self {
            consumer,
            fetch_timeout: Duration::from_millis(500),
            strip_prefix: None,
            idle_poll: Duration::ZERO,
        }
    }

    /// How long `recv` waits for a message before returning `Ok(None)`.
    pub fn with_fetch_timeout(mut self, timeout: Duration) -> Self {
        self.fetch_timeout = timeout;
        self
    }

    /// Strip `prefix` from each delivered subject when deriving the message name,
    /// so a subject like `app.cmd.account.debit` becomes the name `account.debit`.
    ///
    /// Used by [`NatsBus`](super::NatsBus), which namespaces commands and events
    /// under `{ns}.cmd.` / `{ns}.evt.` subjects. Default: no stripping (the full
    /// subject is the name).
    pub fn with_strip_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.strip_prefix = Some(prefix.into());
        self
    }

    /// Keep `recv` retrying after an empty fetch instead of draining to idle.
    pub fn with_idle_poll(mut self, idle_poll: Duration) -> Self {
        self.idle_poll = idle_poll;
        self
    }

    /// Connect to a NATS server URL, then create/open the stream + consumer.
    pub async fn connect(
        url: &str,
        stream_name: &str,
        subjects: Vec<String>,
        durable: &str,
    ) -> Result<Self, TransportError> {
        let client = async_nats::connect(url)
            .await
            .map_err(|err| retryable("nats connect", err))?;
        let jetstream = jetstream::new(client);
        Self::from_context(&jetstream, stream_name, subjects, durable).await
    }

    /// Create or open a JetStream stream + durable pull consumer, then a source.
    ///
    /// `subjects` binds the stream; `durable` names the consumer so progress
    /// survives restarts.
    pub async fn from_context(
        jetstream: &jetstream::Context,
        stream_name: &str,
        subjects: Vec<String>,
        durable: &str,
    ) -> Result<Self, TransportError> {
        let stream = jetstream
            .get_or_create_stream(StreamConfig {
                name: stream_name.to_string(),
                subjects,
                ..Default::default()
            })
            .await
            .map_err(|err| retryable("nats get_or_create_stream", err))?;
        let consumer = stream
            .get_or_create_consumer(
                durable,
                PullConfig {
                    durable_name: Some(durable.to_string()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|err| retryable("nats get_or_create_consumer", err))?;
        Ok(Self::new(consumer))
    }
}

impl MessageSource for NatsJetStreamSource {
    type Received = NatsReceived;

    fn transport_name(&self) -> &'static str {
        "nats"
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        loop {
            let mut batch = self
                .consumer
                .batch()
                .max_messages(1)
                .expires(self.fetch_timeout)
                .messages()
                .await
                .map_err(|err| retryable("nats fetch", err))?;

            match batch.next().await {
                Some(Ok(message)) => {
                    return Ok(Some(NatsReceived::from_jetstream(
                        message,
                        self.strip_prefix.as_deref(),
                    )))
                }
                Some(Err(err)) => return Err(retryable("nats batch message", err)),
                None if self.idle_poll.is_zero() => return Ok(None),
                None => continue,
            }
        }
    }
}

/// A JetStream message plus the means to ack/nak/term it.
pub struct NatsReceived {
    raw: jetstream::Message,
    message: Message,
    ordered: Option<OrderedDelivery>,
}

impl NatsReceived {
    fn from_jetstream(raw: jetstream::Message, strip_prefix: Option<&str>) -> Self {
        let name = strip_address_prefix(raw.subject.to_string(), strip_prefix);
        let payload = raw.payload.to_vec();
        let headers: Vec<(String, String)> = raw
            .headers
            .as_ref()
            .into_iter()
            .flat_map(|headers| headers.iter())
            .filter_map(|(key, values)| {
                values
                    .last()
                    .map(|value| (key.to_string(), value.to_string()))
            })
            .collect();
        let mut message = message_from_wire(
            name,
            payload,
            Some(MESSAGE_ID_HEADER),
            MESSAGE_KIND_HEADER,
            headers,
        );
        take_content_type(&mut message);
        let ordered = jetstream_ordered(&raw);
        Self {
            raw,
            message,
            ordered,
        }
    }

    async fn settle(self, kind: AckKind) -> Result<(), TransportError> {
        match kind {
            AckKind::Ack => self
                .raw
                .ack()
                .await
                .map_err(|err| retryable("nats ack", err)),
            other => self
                .raw
                .ack_with(other)
                .await
                .map_err(|err| retryable("nats ack_with", err)),
        }
    }
}

/// Select the first inbound content type and remove every reserved spelling
/// from user-visible metadata.
fn take_content_type(message: &mut Message) {
    let mut content_type = None;
    message.metadata.retain(|(key, value)| {
        if key.eq_ignore_ascii_case(CONTENT_TYPE_HEADER) {
            if content_type.is_none() {
                content_type = Some(value.clone());
            }
            false
        } else {
            true
        }
    });
    if let Some(content_type) = content_type {
        message.content_type = content_type;
    }
}

fn jetstream_ordered(raw: &jetstream::Message) -> Option<OrderedDelivery> {
    let info = raw.info().ok()?;
    let source = ProjectionSource::new("nats.jetstream", info.stream.as_bytes()).ok()?;
    let epoch = ProjectionEpoch::new(format!("nats.{}", info.stream)).ok()?;
    OrderedDelivery::new(source, epoch, info.stream_sequence, false).ok()
}

impl ReceivedMessage for NatsReceived {
    fn message(&self) -> &Message {
        &self.message
    }

    fn ordered_delivery(&self) -> Option<&OrderedDelivery> {
        self.ordered.as_ref()
    }

    async fn ack(self) -> Result<(), TransportError> {
        self.settle(AckKind::Ack).await
    }

    async fn nack(self, _reason: &str) -> Result<(), TransportError> {
        // Nak with no delay: JetStream redelivers per the consumer policy.
        self.settle(AckKind::Nak(None)).await
    }

    async fn dead_letter(self, _reason: &str) -> Result<(), TransportError> {
        // Term: stop redelivery. A real DLQ bridge can subscribe to the stream's
        // advisory/max-deliver subjects; Term is the "do not redeliver" signal.
        self.settle(AckKind::Term).await
    }

    async fn park(self, _reason: &str) -> Result<(), TransportError> {
        self.settle(AckKind::Term).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::MessageKind;

    #[test]
    fn content_type_selection_removes_every_case_variant() {
        let mut message = Message::new("example.recorded", MessageKind::Event, Vec::new())
            .with_metadata("Content-Type", "application/vnd.example+binary")
            .with_metadata("x-correlation-id", "corr-1")
            .with_metadata("content-type", "application/json");

        take_content_type(&mut message);

        assert_eq!(message.content_type, "application/vnd.example+binary");
        assert_eq!(
            message.metadata,
            vec![("x-correlation-id".into(), "corr-1".into())]
        );
    }
}
