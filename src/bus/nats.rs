//! NATS JetStream transport adapter.
//!
//! Maps the canonical [`Message`] onto NATS JetStream: [`NatsPublisher`] publishes
//! to a subject (waiting for the JetStream publish ack — the durable publish
//! threshold), and [`NatsJetStreamSource`] pulls from a durable consumer and
//! settles via JetStream ack semantics (ack→`Ack`, nack→`Nak`, dead-letter/park→
//! `Term`). Aggregate message IDs use `Nats-Msg-Id` unchanged. External facts
//! retain their logical source identity separately; their broker dedup ID also
//! binds content so altered retries reach the durable conflict fence.
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
/// External occurrences separate immutable fact identity from broker dedup.
const LOGICAL_ID_HEADER: &str = "X-Distributed-Occurrence-Id";

fn external_dedup_id(payload: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"distributed.nats.external-occurrence.v1\0");
    // Canonical occurrence bytes include the immutable logical source identity.
    digest.update(payload);
    format!("external:sha256:{:x}", digest.finalize())
}

fn external_occurrence(
    message: &Message,
) -> Result<Option<crate::DomainEventOccurrence>, TransportError> {
    let Ok(event) = crate::DomainEventOccurrence::from_canonical_bytes(&message.payload) else {
        return Ok(None);
    };
    if event.external_source().is_none() {
        return Ok(None);
    }
    if message.kind != super::MessageKind::Event
        || message.id() != Some(event.id())
        || message.name() != event.descriptor().name
    {
        return Err(TransportError::permanent(
            "external occurrence differs from transport identity",
        ));
    }
    Ok(Some(event))
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
    .is_ok_and(|message| {
        message.kind == super::MessageKind::Event && message.id() == Some(event.id())
    })
}

fn unambiguous_identity_headers(headers: &async_nats::HeaderMap) -> bool {
    [LOGICAL_ID_HEADER, MESSAGE_ID_HEADER, MESSAGE_KIND_HEADER]
        .iter()
        .all(|reserved| {
            headers
                .iter()
                .filter(|(key, _)| key.to_string().eq_ignore_ascii_case(reserved))
                .map(|(_, values)| values.len())
                .sum::<usize>()
                <= 1
        })
}

fn decode_headers(
    name: String,
    payload: Vec<u8>,
    headers: Option<&async_nats::HeaderMap>,
) -> Result<Message, TransportError> {
    if headers.is_some_and(|headers| !unambiguous_identity_headers(headers)) {
        return Err(TransportError::permanent(
            "ambiguous occurrence identity header",
        ));
    }
    let values: Vec<_> = headers
        .into_iter()
        .flat_map(|headers| headers.iter())
        .flat_map(|(key, values)| {
            values
                .iter()
                .map(move |value| (key.to_string(), value.to_string()))
        })
        .collect();
    // External publishers always emit the exact kind. Do not let the generic
    // transport's permissive unknown-kind default turn poison into an event.
    if crate::DomainEventOccurrence::from_canonical_bytes(&payload)
        .is_ok_and(|event| event.external_source().is_some())
        && !values
            .iter()
            .any(|(key, value)| key.eq_ignore_ascii_case(MESSAGE_KIND_HEADER) && value == "event")
    {
        return Err(TransportError::permanent(
            "external occurrence event kind is missing or invalid",
        ));
    }
    let mut message = message_from_wire(
        name,
        payload,
        Some(MESSAGE_ID_HEADER),
        MESSAGE_KIND_HEADER,
        values,
    );
    take_content_type(&mut message);
    restore_external_identity(&mut message)?;
    Ok(message)
}

fn restore_external_identity(message: &mut Message) -> Result<(), TransportError> {
    let ids: Vec<_> = message
        .metadata
        .iter()
        .filter(|(key, _)| key.eq_ignore_ascii_case(LOGICAL_ID_HEADER))
        .map(|(_, value)| value.clone())
        .collect();
    message
        .metadata
        .retain(|(key, _)| !key.eq_ignore_ascii_case(LOGICAL_ID_HEADER));
    if ids.is_empty() {
        if crate::DomainEventOccurrence::from_canonical_bytes(&message.payload)
            .is_ok_and(|event| event.external_source().is_some())
        {
            return Err(TransportError::permanent(
                "external occurrence logical identity is missing",
            ));
        }
        return Ok(());
    }
    if ids.len() != 1 || message.id() != Some(external_dedup_id(&message.payload).as_str()) {
        return Err(TransportError::permanent(
            "invalid external occurrence transport identity",
        ));
    }
    let event = crate::DomainEventOccurrence::from_canonical_bytes(&message.payload)
        .map_err(|_| TransportError::permanent("invalid external occurrence payload"))?;
    if event.external_source().is_none()
        || event.id() != ids[0]
        || event.descriptor().name != message.name()
        || message.kind != super::MessageKind::Event
    {
        return Err(TransportError::permanent(
            "external occurrence differs from transport identity",
        ));
    }
    message.id = Some(ids[0].clone());
    Ok(())
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
    async fn publish(&self, mut message: Message) -> Result<(), TransportError> {
        let subject = self.subject(&message);
        let mut headers = async_nats::HeaderMap::new();
        for (key, value) in &message.metadata {
            if [
                MESSAGE_ID_HEADER,
                MESSAGE_KIND_HEADER,
                CONTENT_TYPE_HEADER,
                LOGICAL_ID_HEADER,
            ]
            .iter()
            .any(|reserved| key.eq_ignore_ascii_case(reserved))
            {
                continue;
            }
            headers.insert(key.as_str(), value.as_str());
        }
        if let Some(event) = external_occurrence(&message)? {
            headers.insert(MESSAGE_ID_HEADER, external_dedup_id(&message.payload));
            headers.insert(LOGICAL_ID_HEADER, event.id());
        } else if let Some(id) = message.id() {
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
                    // Fail closed before dispatch without acknowledging or overriding
                    // the supervisor's permanent-error policy. The durable delivery is
                    // retained for operator repair; no later cursor is falsely sealed.
                    return NatsReceived::from_jetstream(message, self.strip_prefix.as_deref())
                        .map(Some);
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

    #[test]
    fn external_nats_identity_binds_logical_fact_and_content_without_changing_aggregate_ids() {
        let first = external_message(1, 10);
        let altered = external_message(1, 11);
        let next = external_message(2, 10);
        assert_eq!(first.id(), altered.id());
        assert_ne!(
            external_dedup_id(&first.payload),
            external_dedup_id(&altered.payload)
        );
        assert_ne!(
            external_dedup_id(&first.payload),
            external_dedup_id(&next.payload)
        );
        let mut wire = first.clone();
        wire.id = Some(external_dedup_id(&wire.payload));
        wire.metadata
            .push((LOGICAL_ID_HEADER.into(), first.id().unwrap().into()));
        restore_external_identity(&mut wire).unwrap();
        assert_eq!(wire.id, first.id);
        assert_eq!(wire.payload, first.payload);
        assert_eq!(wire.metadata, first.metadata);
        let mut ordinary = Message::new("record.changed", MessageKind::Event, b"{}".to_vec())
            .with_id("aggregate-message");
        assert!(external_occurrence(&ordinary).unwrap().is_none());
        restore_external_identity(&mut ordinary).unwrap();
        assert_eq!(ordinary.id(), Some("aggregate-message"));
    }

    #[test]
    fn external_nats_forged_or_ambiguous_logical_headers_fail_closed() {
        let first = external_message(1, 10);
        for (logical, broker) in [
            ("forged".to_string(), external_dedup_id(&first.payload)),
            (first.id().unwrap().to_string(), "forged-broker".to_string()),
        ] {
            let mut wire = first.clone();
            wire.id = Some(broker);
            wire.metadata.push((LOGICAL_ID_HEADER.into(), logical));
            assert!(restore_external_identity(&mut wire).is_err());
        }
        let mut repeated = first.clone();
        repeated.id = Some(external_dedup_id(&first.payload));
        repeated.metadata = vec![
            (LOGICAL_ID_HEADER.into(), first.id().unwrap().into()),
            (LOGICAL_ID_HEADER.to_lowercase(), first.id().unwrap().into()),
        ];
        assert!(restore_external_identity(&mut repeated).is_err());
    }

    #[test]
    fn retained_archive_rejects_same_ambiguity_as_live_delivery() {
        let message = external_message(1, 10);
        let event = crate::DomainEventOccurrence::from_canonical_bytes(&message.payload).unwrap();
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(MESSAGE_ID_HEADER, external_dedup_id(&message.payload));
        headers.insert(LOGICAL_ID_HEADER, event.id());
        headers.insert(MESSAGE_KIND_HEADER, "event");
        assert!(archived_identity_matches(
            &event,
            &message.payload,
            &headers
        ));
        for name in [
            LOGICAL_ID_HEADER.to_string(),
            LOGICAL_ID_HEADER.to_lowercase(),
            MESSAGE_ID_HEADER.to_string(),
            MESSAGE_ID_HEADER.to_lowercase(),
        ] {
            let mut forged = headers.clone();
            forged.append(name, "conflicting-extra-value");
            assert!(!unambiguous_identity_headers(&forged));
            assert!(!archived_identity_matches(
                &event,
                &message.payload,
                &forged
            ));
        }
    }

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
