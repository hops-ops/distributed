//! NATS JetStream transport adapter.
//!
//! Maps the canonical [`Message`] onto NATS JetStream: [`NatsPublisher`] publishes
//! to a subject (waiting for the JetStream publish ack — the durable publish
//! threshold), and [`NatsJetStreamSource`] pulls from a durable consumer and
//! settles via JetStream ack semantics (ack→`Ack`, nack→`Nak` with the
//! requested delay). Messages travel as a [`WireEnvelope`]; the JetStream dedup
//! header `Nats-Msg-Id` is the message id, except for an external fact, whose
//! dedup id also binds its content so an altered retry reaches the durable
//! conflict fence instead of being deduplicated away.
//!
//! Requires the `nats` feature. Integration-tested in `tests/transports/nats_transport`
//! against a JetStream-enabled server (see `compose.yaml`).

use std::time::Duration;

use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::consumer::Consumer;
use async_nats::jetstream::stream::Config as StreamConfig;
use async_nats::jetstream::{self, AckKind};
use futures::StreamExt;

use crate::projection_protocol::{ProjectionEpoch, ProjectionSource};

use super::source::{MessageSource, ReceivedMessage};
use super::{retryable, MessagePublisher, TransportError};
use super::{strip_address_prefix, Message, MessageKind, OrderedDelivery, WireEnvelope};

/// JetStream's dedup header. Set by the publisher, never by metadata.
const DEDUP_HEADER: &str = "Nats-Msg-Id";

/// Whether `key` is a NATS control header (`Nats-*`), which changes how
/// JetStream stores a message and so may not come from metadata.
fn is_nats_header(key: &str) -> bool {
    key.get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("nats-"))
}

fn external_dedup_id(payload: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"distributed.nats.external-occurrence.v1\0");
    // Canonical occurrence bytes include the immutable logical source identity.
    digest.update(payload);
    format!("external:sha256:{:x}", digest.finalize())
}

/// The dedup id the publisher writes for `message`: the content-bound id for
/// an external fact (whose transport identity must match the fact), else the
/// message id.
fn dedup_id(message: &Message) -> Result<Option<String>, TransportError> {
    let Ok(event) = crate::DomainEventOccurrence::from_canonical_bytes(&message.payload) else {
        return Ok(message.id.clone());
    };
    if event.external_source().is_none() {
        return Ok(message.id.clone());
    }
    if message.kind != MessageKind::Event
        || message.id() != Some(event.id())
        || message.name() != event.descriptor().name
    {
        return Err(TransportError::permanent(
            "external occurrence differs from transport identity",
        ));
    }
    Ok(Some(external_dedup_id(&message.payload)))
}

fn is_external_occurrence(payload: &[u8]) -> bool {
    crate::DomainEventOccurrence::from_canonical_bytes(payload)
        .is_ok_and(|event| event.external_source().is_some())
}

pub(super) fn archived_identity_matches(
    event: &crate::DomainEventOccurrence,
    payload: &[u8],
    headers: &async_nats::HeaderMap,
) -> bool {
    decode_headers(
        event.descriptor().name.to_string(),
        payload.to_vec(),
        Some(headers),
    )
    .is_ok_and(|message| message.kind == MessageKind::Event && message.id() == Some(event.id()))
}

/// Decode a JetStream delivery. Any decode failure is an error, not a message
/// to settle: the delivery stays unacknowledged for repair, and no cursor
/// moves past it.
fn decode_headers(
    name: String,
    payload: Vec<u8>,
    headers: Option<&async_nats::HeaderMap>,
) -> Result<Message, TransportError> {
    let mut dedup = Vec::new();
    let mut pairs = Vec::new();
    for (key, values) in headers.into_iter().flat_map(|headers| headers.iter()) {
        let key = key.to_string();
        for value in values {
            if key.eq_ignore_ascii_case(DEDUP_HEADER) {
                dedup.push(value.to_string());
            } else if !is_nats_header(&key) {
                pairs.push((key.clone(), value.to_string()));
            }
        }
    }
    let external = is_external_occurrence(&payload);
    // External publishers always write the exact kind. Do not let the
    // envelope's missing-kind default turn poison into an event.
    if external
        && !pairs.iter().any(|(key, value)| {
            key.eq_ignore_ascii_case(WireEnvelope::KIND) && value == MessageKind::Event.as_str()
        })
    {
        return Err(TransportError::permanent(
            "external occurrence event kind is missing or invalid",
        ));
    }
    let (message, error) = WireEnvelope {
        name,
        headers: pairs,
        payload,
    }
    .decode();
    if let Some(error) = error {
        return Err(error);
    }
    verify_dedup(&message, &dedup, external)?;
    Ok(message)
}

/// The delivered dedup header must be the one the publisher writes for this
/// message. A foreign producer may leave it out, except for an external fact,
/// whose content binding depends on it.
fn verify_dedup(message: &Message, dedup: &[String], external: bool) -> Result<(), TransportError> {
    let expected = dedup_id(message)?;
    match dedup {
        [] if external => Err(TransportError::permanent(
            "external occurrence broker dedup id is missing",
        )),
        [] => Ok(()),
        [delivered] if expected.as_deref() == Some(delivered.as_str()) => Ok(()),
        _ => Err(TransportError::permanent(
            "broker dedup id does not match the message identity",
        )),
    }
}

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
    async fn publish(&self, message: Message) -> Result<(), TransportError> {
        if let Some((key, _)) = message.metadata.iter().find(|(key, _)| is_nats_header(key)) {
            return Err(TransportError::permanent(format!(
                "message metadata key {key:?} is a NATS control header"
            )));
        }
        let subject = self.subject(&message);
        let dedup = dedup_id(&message)?;
        let envelope = WireEnvelope::encode(message)?;
        let mut headers = async_nats::HeaderMap::new();
        for (key, value) in &envelope.headers {
            headers.append(key.as_str(), value.as_str());
        }
        if let Some(dedup) = dedup {
            headers.insert(DEDUP_HEADER, dedup);
        }
        // `Bytes::from(Vec<u8>)` takes ownership of the buffer (no copy).
        let payload = envelope.payload.into();

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
}

impl NatsJetStreamSource {
    /// Wrap an existing durable pull consumer.
    pub fn new(consumer: Consumer<PullConfig>) -> Self {
        Self {
            consumer,
            fetch_timeout: Duration::from_millis(500),
            strip_prefix: None,
        }
    }

    /// How long `recv` waits for a message before reporting that none is
    /// available.
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

    fn settles_independently(&self) -> bool {
        // JetStream explicit acks settle each delivery by its own reply subject.
        true
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        let mut batch = self
            .consumer
            .batch()
            .max_messages(1)
            .expires(self.fetch_timeout)
            .messages()
            .await
            .map_err(|err| retryable("nats fetch", err))?;

        match batch.next().await {
            // A delivery that cannot be decoded fails the receive: it is not
            // acknowledged, so it is retained for repair and no later cursor
            // is sealed past it.
            Some(Ok(message)) => {
                NatsReceived::from_jetstream(message, self.strip_prefix.as_deref()).map(Some)
            }
            Some(Err(err)) => Err(retryable("nats batch message", err)),
            None => Ok(None),
        }
    }
}

/// A JetStream message plus the means to ack or nak it.
pub struct NatsReceived {
    raw: jetstream::Message,
    message: Message,
    ordered: Option<OrderedDelivery>,
}

impl NatsReceived {
    fn from_jetstream(
        raw: jetstream::Message,
        strip_prefix: Option<&str>,
    ) -> Result<Self, TransportError> {
        let name = strip_address_prefix(raw.subject.to_string(), strip_prefix);
        let payload = raw.payload.to_vec();
        let message = decode_headers(name, payload, raw.headers.as_ref())?;
        let ordered = jetstream_ordered(&raw);
        Ok(Self {
            raw,
            message,
            ordered,
        })
    }

    /// Delivery attempt count reported by JetStream (1 for the first delivery).
    fn delivered(&self) -> u64 {
        self.raw
            .info()
            .map(|info| u64::try_from(info.delivered).unwrap_or(1))
            .unwrap_or(1)
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

    fn attempts(&self) -> u32 {
        u32::try_from(self.delivered()).unwrap_or(u32::MAX)
    }

    fn ordered_delivery(&self) -> Option<&OrderedDelivery> {
        self.ordered.as_ref()
    }

    async fn ack(self) -> Result<(), TransportError> {
        self.settle(AckKind::Ack).await
    }

    async fn nack(self, delay: Duration) -> Result<(), TransportError> {
        let delay = (!delay.is_zero()).then_some(delay);
        self.settle(AckKind::Nak(delay)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Serialize, serde::Deserialize, crate::DomainEvent)]
    #[domain_event(name = "external.balance_recorded", version = 1)]
    struct Balance {
        value: u64,
    }

    fn external_message(position: u64, value: u64) -> Message {
        let event = crate::DomainEventOccurrence::capture_external(
            crate::ExternalEventSource {
                producer: "ledger.adapter".into(),
                stream: "account-one".into(),
                position,
                key: "balance".into(),
            },
            std::time::UNIX_EPOCH,
            Default::default(),
            &Balance { value },
        )
        .unwrap();
        Message::new(
            event.descriptor().name.to_string(),
            MessageKind::Event,
            event.canonical_bytes().unwrap(),
        )
        .with_id(event.id())
    }

    /// The headers [`NatsPublisher::publish`] writes for `message`.
    fn published_headers(message: &Message) -> async_nats::HeaderMap {
        let dedup = dedup_id(message).unwrap();
        let envelope = WireEnvelope::encode(message.clone()).unwrap();
        let mut headers = async_nats::HeaderMap::new();
        for (key, value) in &envelope.headers {
            headers.append(key.as_str(), value.as_str());
        }
        if let Some(dedup) = dedup {
            headers.insert(DEDUP_HEADER, dedup);
        }
        headers
    }

    fn decode(
        message: &Message,
        headers: &async_nats::HeaderMap,
    ) -> Result<Message, TransportError> {
        decode_headers(message.name.clone(), message.payload.clone(), Some(headers))
    }

    #[test]
    fn messages_round_trip_through_nats_headers() {
        let mut ordinary = Message::new("record.changed", MessageKind::Command, b"{}".to_vec())
            .with_id("aggregate-message")
            .with_metadata("correlation_id", "corr-1");
        ordinary.content_type = "application/vnd.example+binary".into();
        let external = external_message(1, 10);
        for message in [ordinary, external] {
            let decoded = decode(&message, &published_headers(&message)).unwrap();
            assert_eq!(decoded.id, message.id);
            assert_eq!(decoded.kind, message.kind);
            assert_eq!(decoded.content_type, message.content_type);
            assert_eq!(decoded.payload, message.payload);
            assert_eq!(decoded.metadata, message.metadata);
        }
    }

    #[test]
    fn external_dedup_binds_logical_fact_and_content_without_changing_aggregate_ids() {
        let first = external_message(1, 10);
        let altered = external_message(1, 11);
        let next = external_message(2, 10);
        assert_eq!(first.id(), altered.id());
        let dedup = |message: &Message| dedup_id(message).unwrap().unwrap();
        assert_ne!(dedup(&first), dedup(&altered));
        assert_ne!(dedup(&first), dedup(&next));
        assert_ne!(dedup(&first).as_str(), first.id().unwrap());
        let ordinary = Message::new("record.changed", MessageKind::Event, b"{}".to_vec())
            .with_id("aggregate-message");
        assert_eq!(dedup(&ordinary), "aggregate-message");
    }

    #[test]
    fn forged_missing_or_ambiguous_identity_headers_fail_closed() {
        let external = external_message(1, 10);
        let ordinary = Message::new("record.changed", MessageKind::Event, b"{}".to_vec())
            .with_id("aggregate-message");
        let mut cases = Vec::new();
        for message in [&external, &ordinary] {
            let mut forged_dedup = published_headers(message);
            forged_dedup.insert(DEDUP_HEADER, "forged");
            cases.push((message, forged_dedup));
            let mut repeated_dedup = published_headers(message);
            repeated_dedup.append(DEDUP_HEADER.to_lowercase(), "extra");
            cases.push((message, repeated_dedup));
            let mut repeated_id = published_headers(message);
            repeated_id.append("X-Distributed-Id", message.id().unwrap());
            cases.push((message, repeated_id));
        }
        let mut missing_dedup = async_nats::HeaderMap::new();
        missing_dedup.insert(WireEnvelope::ID, external.id().unwrap());
        missing_dedup.insert(WireEnvelope::KIND, "event");
        cases.push((&external, missing_dedup));
        let mut missing_kind = async_nats::HeaderMap::new();
        missing_kind.insert(WireEnvelope::ID, external.id().unwrap());
        missing_kind.insert(DEDUP_HEADER, dedup_id(&external).unwrap().unwrap());
        cases.push((&external, missing_kind));
        for (message, headers) in cases {
            assert!(decode(message, &headers).is_err(), "accepted {headers:?}");
        }
    }

    #[test]
    fn retained_archive_rejects_same_ambiguity_as_live_delivery() {
        let message = external_message(1, 10);
        let event = crate::DomainEventOccurrence::from_canonical_bytes(&message.payload).unwrap();
        let headers = published_headers(&message);
        assert!(archived_identity_matches(
            &event,
            &message.payload,
            &headers
        ));
        for name in [
            WireEnvelope::ID,
            "X-Distributed-Id",
            DEDUP_HEADER,
            "nats-msg-id",
        ] {
            let mut forged = headers.clone();
            forged.append(name, "conflicting-extra-value");
            assert!(!archived_identity_matches(
                &event,
                &message.payload,
                &forged
            ));
        }
    }

    #[test]
    fn nats_control_headers_never_reach_metadata() {
        let message = Message::new("record.changed", MessageKind::Event, Vec::new())
            .with_id("m-1")
            .with_metadata("x-correlation-id", "corr-1");
        let mut headers = published_headers(&message);
        headers.insert("Nats-Expected-Stream", "other");
        let decoded = decode(&message, &headers).unwrap();
        assert_eq!(
            decoded.metadata,
            vec![("x-correlation-id".to_string(), "corr-1".to_string())]
        );
    }
}
