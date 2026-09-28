use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::aggregate::Aggregate;
use crate::domain_event::{
    DomainEvent, DomainEventCaptureError, DomainEventDescriptor, DomainEventEnvelope,
    DomainEventOccurrence,
};
use crate::entity::{BitcodePayloadCodec, Entity, EventRecordError, PayloadCodec};
use crate::trace_context::{TraceContext, CAUSATION_ID, CORRELATION_ID, TRACEPARENT, TRACESTATE};
use crate::SourcedResult;

/// An explicitly authored outward event waiting to be bound to the aggregate
/// transition committed by a fluent unit of work.
///
/// The value is serialized immediately from `E`; replay payload bytes and
/// aggregate snapshots never participate in this path.
#[derive(Clone, Debug)]
pub(crate) struct PreparedDomainEvent {
    descriptor: DomainEventDescriptor,
    body: serde_json::Value,
}

impl PreparedDomainEvent {
    pub(crate) fn new<E: DomainEvent>(event: E) -> Result<Self, DomainEventCaptureError> {
        let body = serde_json::to_value(event)
            .map_err(|error| DomainEventCaptureError::BodyEncoding(error.to_string()))?;
        Ok(Self {
            descriptor: E::DESCRIPTOR,
            body,
        })
    }

    pub(crate) fn bind<A: Aggregate>(
        &self,
        aggregate: &A,
        publication_ordinal: u32,
    ) -> Result<DomainEventOccurrence, DomainEventCaptureError> {
        let entity = aggregate.entity();
        let event = entity
            .new_events()
            .last()
            .ok_or(DomainEventCaptureError::NoPendingAggregateEvent)?;
        DomainEventOccurrence::capture(
            self.descriptor.clone(),
            DomainEventEnvelope {
                aggregate_type: A::aggregate_type().to_string(),
                aggregate_id: entity.id().to_string(),
                aggregate_sequence: event.sequence,
                publication_ordinal,
                occurred_at: event.timestamp,
                metadata: event
                    .metadata
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            },
            &self.body,
        )
    }
}

/// Status of an outbox message.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutboxMessageStatus {
    #[default]
    Pending,
    InFlight,
    Published,
    Failed,
}

impl OutboxMessageStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InFlight => "in_flight",
            Self::Published => "published",
            Self::Failed => "failed",
        }
    }
}

impl std::str::FromStr for OutboxMessageStatus {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "in_flight" => Ok(Self::InFlight),
            "published" => Ok(Self::Published),
            "failed" => Ok(Self::Failed),
            _ => Err(()),
        }
    }
}

/// Durable publication work item stored through the outbox pattern.
///
/// The message is an immutable publishable envelope plus mutable delivery state.
/// It is not an aggregate stream; repositories store it in their outbox storage
/// and workers update delivery state directly.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct OutboxMessage {
    pub id: String,
    pub event_type: String,
    pub payload: Vec<u8>,
    pub payload_codec: String,
    pub payload_codec_version: u16,
    pub status: OutboxMessageStatus,
    pub created_at: SystemTime,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub worker_id: Option<String>,
    pub leased_until: Option<SystemTime>,
    /// Optional destination queue for point-to-point delivery via `send/listen`.
    /// When set, the outbox worker uses `Sender::send(destination, event)` instead
    /// of `Publisher::publish(event)`.
    pub destination: Option<String>,
    /// Metadata propagated to the publisher (correlation IDs, trace context, etc.).
    pub metadata: HashMap<String, String>,
    pub source_aggregate_type: Option<String>,
    pub source_aggregate_id: Option<String>,
    pub source_sequence: Option<u64>,
}

impl Default for OutboxMessage {
    fn default() -> Self {
        Self {
            id: String::new(),
            event_type: String::new(),
            payload: Vec::new(),
            payload_codec: OutboxMessage::RAW_PAYLOAD_CODEC.to_string(),
            payload_codec_version: OutboxMessage::RAW_PAYLOAD_CODEC_VERSION,
            status: OutboxMessageStatus::default(),
            created_at: SystemTime::UNIX_EPOCH,
            attempts: 0,
            last_error: None,
            worker_id: None,
            leased_until: None,
            destination: None,
            metadata: HashMap::new(),
            source_aggregate_type: None,
            source_aggregate_id: None,
            source_sequence: None,
        }
    }
}

impl OutboxMessage {
    pub const RAW_PAYLOAD_CODEC: &'static str = "bytes";
    pub const RAW_PAYLOAD_CODEC_VERSION: u16 = 1;
    /// Canonical JSON encoding of a complete [`DomainEventOccurrence`].
    pub const DOMAIN_EVENT_PAYLOAD_CODEC: &'static str = "distributed.domain-event-occurrence+json";
    /// Version of [`Self::DOMAIN_EVENT_PAYLOAD_CODEC`].
    pub const DOMAIN_EVENT_PAYLOAD_CODEC_VERSION: u16 = 1;

    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new outbox message with raw bytes payload.
    ///
    /// The `event_type` names the publishable message, not an aggregate replay
    /// record unless the caller intentionally uses the same name.
    pub fn create(
        id: impl Into<String>,
        event_type: impl Into<String>,
        payload: Vec<u8>,
    ) -> SourcedResult<Self> {
        let mut message = Self::new();
        message.initialize(id.into(), event_type.into(), payload, None, HashMap::new())?;
        Ok(message)
    }

    /// Create a new outbox message with raw bytes payload and a destination queue.
    ///
    /// When a destination is set, the outbox worker uses `Sender::send(destination, event)`
    /// (point-to-point) instead of `Publisher::publish(event)` (fan-out).
    pub fn create_to(
        id: impl Into<String>,
        event_type: impl Into<String>,
        destination: impl Into<String>,
        payload: Vec<u8>,
    ) -> SourcedResult<Self> {
        let mut message = Self::new();
        message.initialize(
            id.into(),
            event_type.into(),
            payload,
            Some(destination.into()),
            HashMap::new(),
        )?;
        Ok(message)
    }

    /// Create a new outbox message with bitcode (fast binary) serialization.
    pub fn encode<T: Serialize>(
        id: impl Into<String>,
        event_type: impl Into<String>,
        payload: &T,
    ) -> SourcedResult<Self> {
        let bytes = BitcodePayloadCodec::encode(payload).map_err(EventRecordError::encode)?;
        Self::create_encoded_bytes(id, event_type, bytes, None, HashMap::new())
    }

    /// Create a new outbox message with bitcode serialization and a destination queue.
    ///
    /// When a destination is set, the outbox worker uses `Sender::send(destination, event)`
    /// (point-to-point) instead of `Publisher::publish(event)` (fan-out).
    pub fn encode_to<T: Serialize>(
        id: impl Into<String>,
        event_type: impl Into<String>,
        destination: impl Into<String>,
        payload: &T,
    ) -> SourcedResult<Self> {
        let bytes = BitcodePayloadCodec::encode(payload).map_err(EventRecordError::encode)?;
        Self::create_encoded_bytes(
            id,
            event_type,
            bytes,
            Some(destination.into()),
            HashMap::new(),
        )
    }

    /// Create a message with metadata and raw bytes payload.
    pub fn create_with_metadata(
        id: impl Into<String>,
        event_type: impl Into<String>,
        payload: Vec<u8>,
        metadata: HashMap<String, String>,
    ) -> SourcedResult<Self> {
        let mut message = Self::new();
        message.initialize(id.into(), event_type.into(), payload, None, metadata)?;
        Ok(message)
    }

    /// Create a message with metadata and bitcode-serialized payload.
    pub fn encode_with_metadata<T: Serialize>(
        id: impl Into<String>,
        event_type: impl Into<String>,
        payload: &T,
        metadata: HashMap<String, String>,
    ) -> SourcedResult<Self> {
        let bytes = BitcodePayloadCodec::encode(payload).map_err(EventRecordError::encode)?;
        Self::create_encoded_bytes(id, event_type, bytes, None, metadata)
    }

    /// Create a message that inherits metadata from an entity's context.
    ///
    /// This automatically propagates correlation IDs, trace context, and any
    /// other metadata set on the entity, so nothing gets lost between the
    /// event store and the bus.
    pub fn encode_for_entity<T: Serialize>(
        id: impl Into<String>,
        event_type: impl Into<String>,
        payload: &T,
        entity: &Entity,
    ) -> SourcedResult<Self> {
        let bytes = BitcodePayloadCodec::encode(payload).map_err(EventRecordError::encode)?;
        Self::create_encoded_bytes(id, event_type, bytes, None, entity.metadata().clone())
    }

    /// Create a durable outbox row from one exact canonical outward occurrence.
    ///
    /// The payload is the complete occurrence envelope, not an aggregate replay
    /// payload or snapshot. Its source identity is copied from the occurrence
    /// itself so a multi-aggregate commit cannot stamp one aggregate onto every
    /// message.
    ///
    /// # Errors
    ///
    /// Returns the occurrence's typed canonical-encoding error.
    pub fn from_domain_event_occurrence(
        occurrence: &DomainEventOccurrence,
    ) -> Result<Self, DomainEventCaptureError> {
        let bytes = occurrence.canonical_bytes()?;
        let mut message = Self::new();
        message
            .initialize_with_codec(
                occurrence.id().to_string(),
                occurrence.descriptor().name.to_string(),
                bytes,
                None,
                occurrence
                    .metadata()
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                (
                    Self::DOMAIN_EVENT_PAYLOAD_CODEC.to_string(),
                    Self::DOMAIN_EVENT_PAYLOAD_CODEC_VERSION,
                ),
            )
            .map_err(|error| DomainEventCaptureError::BodyEncoding(error.to_string()))?;
        if occurrence.external_source().is_none() {
            message.source_aggregate_type = Some(occurrence.aggregate_type().to_string());
            message.source_aggregate_id = Some(occurrence.aggregate_id().to_string());
            message.source_sequence = Some(occurrence.aggregate_sequence());
        }
        Ok(message)
    }

    /// Decode and validate this row's canonical outward occurrence.
    ///
    /// # Errors
    ///
    /// Rejects non-domain-event codecs and malformed or non-canonical payloads.
    pub fn domain_event_occurrence(
        &self,
    ) -> Result<DomainEventOccurrence, DomainEventCaptureError> {
        if self.payload_codec != Self::DOMAIN_EVENT_PAYLOAD_CODEC
            || self.payload_codec_version != Self::DOMAIN_EVENT_PAYLOAD_CODEC_VERSION
        {
            return Err(DomainEventCaptureError::UnsupportedBodyCodec {
                codec: self.payload_codec.clone(),
                version: self.payload_codec_version,
            });
        }
        DomainEventOccurrence::from_canonical_bytes(&self.payload)
    }

    /// Decode the payload from the default binary codec.
    pub fn decode<T: serde::de::DeserializeOwned>(&self) -> SourcedResult<T> {
        if self.payload_codec != BitcodePayloadCodec::NAME
            || self.payload_codec_version != BitcodePayloadCodec::VERSION
        {
            return Err(EventRecordError::unsupported_codec(
                &self.payload_codec,
                self.payload_codec_version,
            ));
        }

        BitcodePayloadCodec::decode(&self.payload).map_err(|e| {
            EventRecordError::decode(
                &self.event_type,
                &self.payload_codec,
                self.payload_codec_version,
                e,
            )
        })
    }

    // Getters
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn payload_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.payload).ok()
    }

    pub fn is_pending(&self) -> bool {
        self.status == OutboxMessageStatus::Pending
    }

    pub fn is_in_flight(&self) -> bool {
        self.status == OutboxMessageStatus::InFlight
    }

    pub fn is_published(&self) -> bool {
        self.status == OutboxMessageStatus::Published
    }

    pub fn is_failed(&self) -> bool {
        self.status == OutboxMessageStatus::Failed
    }

    pub fn has_expired_lease_at(&self, now: SystemTime) -> bool {
        self.is_in_flight() && self.leased_until.map(|until| until <= now).unwrap_or(true)
    }

    pub fn is_claimable_at(&self, now: SystemTime) -> bool {
        self.is_pending() || self.has_expired_lease_at(now)
    }

    pub fn is_claimed_by(&self, worker_id: &str) -> bool {
        self.worker_id.as_deref() == Some(worker_id)
    }

    fn is_claimable(&self) -> bool {
        self.is_claimable_at(crate::time::now())
    }

    // Commands
    pub fn initialize(
        &mut self,
        id: String,
        event_type: String,
        payload: Vec<u8>,
        destination: Option<String>,
        metadata: HashMap<String, String>,
    ) -> SourcedResult {
        self.initialize_with_codec(
            id,
            event_type,
            payload,
            destination,
            metadata,
            (
                Self::RAW_PAYLOAD_CODEC.to_string(),
                Self::RAW_PAYLOAD_CODEC_VERSION,
            ),
        )
    }

    fn initialize_with_codec(
        &mut self,
        id: String,
        event_type: String,
        payload: Vec<u8>,
        destination: Option<String>,
        metadata: HashMap<String, String>,
        payload_codec: (String, u16),
    ) -> SourcedResult {
        validate_non_empty("outbox message id", &id)?;
        validate_non_empty("outbox event type", &event_type)?;
        validate_non_empty("outbox payload codec", &payload_codec.0)?;
        if payload_codec.1 == 0 {
            return Err(EventRecordError {
                message: "outbox payload codec version must be greater than zero".into(),
            });
        }
        self.id = id;
        self.event_type = event_type;
        self.payload = payload;
        self.payload_codec = payload_codec.0;
        self.payload_codec_version = payload_codec.1;
        self.destination = destination;
        self.metadata = metadata;
        self.status = OutboxMessageStatus::Pending;
        self.created_at = crate::time::now();
        self.attempts = 0;
        self.last_error = None;
        self.worker_id = None;
        self.leased_until = None;
        self.source_aggregate_type = None;
        self.source_aggregate_id = None;
        self.source_sequence = None;
        Ok(())
    }

    fn create_encoded_bytes(
        id: impl Into<String>,
        event_type: impl Into<String>,
        payload: Vec<u8>,
        destination: Option<String>,
        metadata: HashMap<String, String>,
    ) -> SourcedResult<Self> {
        let mut message = Self::new();
        message.initialize_with_codec(
            id.into(),
            event_type.into(),
            payload,
            destination,
            metadata,
            (
                BitcodePayloadCodec::NAME.to_string(),
                BitcodePayloadCodec::VERSION,
            ),
        )?;
        Ok(message)
    }

    pub fn claim(&mut self, worker_id: String, leased_until: SystemTime) -> SourcedResult {
        if !self.is_claimable() {
            return Err(EventRecordError {
                message: format!("outbox message `{}` is not claimable", self.id),
            });
        }
        validate_non_empty("outbox worker id", &worker_id)?;

        self.status = OutboxMessageStatus::InFlight;
        self.attempts += 1;
        self.worker_id = Some(worker_id);
        self.leased_until = Some(leased_until);
        Ok(())
    }

    /// Claim with a Duration (convenience method that computes the deadline)
    pub fn claim_for(&mut self, worker_id: impl Into<String>, lease: Duration) -> SourcedResult {
        self.claim_at(worker_id, lease, crate::time::now())
    }

    /// Claim with an explicit clock value. This is useful for deterministic
    /// tests and repository implementations that capture time once per batch.
    pub fn claim_at(
        &mut self,
        worker_id: impl Into<String>,
        lease: Duration,
        now: SystemTime,
    ) -> SourcedResult {
        let leased_until = Self::lease_deadline(now, lease)?;
        self.claim(worker_id.into(), leased_until)
    }

    /// Compute `now + lease` at full `SystemTime` precision. Truncating the
    /// deadline to whole seconds would shave up to a second off every lease —
    /// and expire a sub-second lease at birth.
    fn lease_deadline(now: SystemTime, lease: Duration) -> SourcedResult<SystemTime> {
        let until = now.checked_add(lease).ok_or_else(|| EventRecordError {
            message: "failed to compute outbox lease deadline: timestamp overflow".into(),
        })?;

        until
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|err| EventRecordError {
                message: format!(
                    "failed to compute outbox lease deadline before UNIX epoch: {}",
                    err
                ),
            })?;

        Ok(until)
    }

    pub fn complete(&mut self) -> SourcedResult {
        if !self.is_in_flight() {
            return Err(EventRecordError {
                message: format!("outbox message `{}` is not in flight", self.id),
            });
        }
        self.status = OutboxMessageStatus::Published;
        self.worker_id = None;
        self.leased_until = None;
        Ok(())
    }

    pub fn release(&mut self, error: String) -> SourcedResult {
        if !self.is_in_flight() {
            return Err(EventRecordError {
                message: format!("outbox message `{}` is not in flight", self.id),
            });
        }
        self.status = OutboxMessageStatus::Pending;
        self.last_error = if error.is_empty() { None } else { Some(error) };
        self.worker_id = None;
        self.leased_until = None;
        Ok(())
    }

    pub fn fail(&mut self, error: String) -> SourcedResult {
        if !self.can_fail() {
            return Err(EventRecordError {
                message: format!("outbox message `{}` cannot be failed", self.id),
            });
        }
        self.status = OutboxMessageStatus::Failed;
        self.last_error = if error.is_empty() { None } else { Some(error) };
        self.worker_id = None;
        self.leased_until = None;
        Ok(())
    }

    fn can_fail(&self) -> bool {
        self.status != OutboxMessageStatus::Published && self.status != OutboxMessageStatus::Failed
    }

    /// Set a single metadata key-value pair.
    pub fn set_meta(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert(key.into(), value.into());
    }

    /// Set the correlation ID.
    pub fn set_correlation_id(&mut self, id: impl Into<String>) {
        self.set_meta(CORRELATION_ID, id);
    }

    /// Set the causation ID.
    pub fn set_causation_id(&mut self, id: impl Into<String>) {
        self.set_meta(CAUSATION_ID, id);
    }

    /// Replace causation metadata at the final causal-commit boundary.
    ///
    /// The ledger attempt, rather than handler-supplied metadata, is
    /// authoritative for every outbox fact committed by a causal command.
    #[cfg_attr(not(feature = "graphql"), allow(dead_code))]
    pub(crate) fn overwrite_causation_id(&mut self, id: &str) {
        self.metadata
            .retain(|key, _| !key.eq_ignore_ascii_case(CAUSATION_ID));
        self.metadata
            .insert(CAUSATION_ID.to_string(), id.to_string());
    }

    /// Set W3C trace context metadata.
    pub fn set_trace_context(&mut self, context: &TraceContext) {
        context.inject_map(&mut self.metadata);
    }

    /// Get a metadata value by key.
    pub fn meta(&self, key: &str) -> Option<&str> {
        self.metadata.get(key).map(|s| s.as_str())
    }

    /// Get the correlation ID, if set.
    pub fn correlation_id(&self) -> Option<&str> {
        self.meta(CORRELATION_ID)
    }

    /// Get the causation ID, if set.
    pub fn causation_id(&self) -> Option<&str> {
        self.meta(CAUSATION_ID)
    }

    /// Get the W3C `traceparent`, if set.
    pub fn traceparent(&self) -> Option<&str> {
        self.meta(TRACEPARENT)
    }

    /// Get the W3C `tracestate`, if set.
    pub fn tracestate(&self) -> Option<&str> {
        self.meta(TRACESTATE)
    }

    /// Extract W3C trace context from this outbox message's metadata.
    pub fn trace_context(&self) -> TraceContext {
        TraceContext::from_metadata(self.metadata.iter())
    }

    pub fn set_source<A: Aggregate>(&mut self, aggregate: &A) {
        self.source_aggregate_type = Some(A::aggregate_type().to_string());
        self.source_aggregate_id = Some(aggregate.entity().id().to_string());
        self.source_sequence = Some(aggregate.entity().version());
    }
}

fn validate_non_empty(field: &str, value: &str) -> SourcedResult {
    if value.trim().is_empty() {
        return Err(EventRecordError {
            message: format!("{field} must not be empty"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_message_is_pending() {
        let mut message = OutboxMessage::new();
        message
            .initialize(
                "msg-1".into(),
                "UserCreated".into(),
                br#"{"id":"123"}"#.to_vec(),
                None,
                HashMap::new(),
            )
            .unwrap();
        assert_eq!(message.event_type, "UserCreated");
        assert!(message.is_pending());
    }

    #[test]
    fn claim_and_complete() {
        let mut message = OutboxMessage::new();
        message
            .initialize(
                "msg-1".into(),
                "Event1".into(),
                b"{}".to_vec(),
                None,
                HashMap::new(),
            )
            .unwrap();
        message
            .claim_for("worker-1", Duration::from_secs(60))
            .unwrap();
        assert!(message.is_in_flight());
        assert_eq!(message.attempts, 1);

        message.complete().unwrap();
        assert!(message.is_published());
    }

    #[test]
    fn expired_in_flight_message_can_be_claimed_again() {
        let mut message = OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap();
        message
            .claim_at("worker-1", Duration::from_secs(1), SystemTime::UNIX_EPOCH)
            .unwrap();

        assert!(message.has_expired_lease_at(crate::time::now()));
        assert!(message.is_claimable_at(crate::time::now()));

        message
            .claim_for("worker-2", Duration::from_secs(60))
            .unwrap();

        assert_eq!(message.worker_id.as_deref(), Some("worker-2"));
        assert_eq!(message.attempts, 2);
    }

    #[test]
    fn sub_second_lease_keeps_full_precision_and_is_not_expired_at_birth() {
        let mut message = OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap();
        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(1_700);
        let lease = Duration::from_millis(400);

        message.claim_at("worker-1", lease, now).unwrap();

        // The deadline is exactly now + lease — no whole-second truncation,
        // which used to expire a sub-second lease the instant it was taken.
        assert_eq!(message.leased_until, Some(now + lease));
        assert!(!message.has_expired_lease_at(now));
        assert!(!message.has_expired_lease_at(now + Duration::from_millis(399)));
        assert!(message.has_expired_lease_at(now + lease));
    }

    #[test]
    fn claim_deadline_overflow_returns_error() {
        let err =
            OutboxMessage::lease_deadline(SystemTime::UNIX_EPOCH, Duration::from_secs(u64::MAX))
                .unwrap_err();

        assert!(err
            .message
            .contains("failed to compute outbox lease deadline"));
    }

    #[test]
    fn claim_deadline_before_epoch_returns_error() {
        let before_epoch = SystemTime::UNIX_EPOCH
            .checked_sub(Duration::from_secs(1))
            .unwrap();
        let err = OutboxMessage::lease_deadline(before_epoch, Duration::ZERO).unwrap_err();

        assert!(err
            .message
            .contains("failed to compute outbox lease deadline before UNIX epoch"));
    }

    #[test]
    fn initialize_resets_delivery_and_source_state() {
        let mut message = OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap();
        message
            .claim(
                "worker-1".into(),
                SystemTime::UNIX_EPOCH + Duration::from_secs(1),
            )
            .unwrap();
        message.last_error = Some("previous failure".into());
        message.source_aggregate_type = Some("todo".into());
        message.source_aggregate_id = Some("todo-1".into());
        message.source_sequence = Some(7);

        message
            .initialize(
                "msg-2".into(),
                "OtherEvent".into(),
                b"{}".to_vec(),
                None,
                HashMap::new(),
            )
            .unwrap();

        assert_eq!(message.attempts, 0);
        assert_eq!(message.last_error, None);
        assert_eq!(message.worker_id, None);
        assert_eq!(message.leased_until, None);
        assert_eq!(message.source_aggregate_type, None);
        assert_eq!(message.source_aggregate_id, None);
        assert_eq!(message.source_sequence, None);
    }

    #[test]
    fn create_with_metadata() {
        let mut meta = HashMap::new();
        meta.insert("correlation_id".to_string(), "req-abc".to_string());
        meta.insert("trace_id".to_string(), "t-999".to_string());

        let message =
            OutboxMessage::create_with_metadata("msg-1", "UserCreated", b"{}".to_vec(), meta)
                .unwrap();
        assert_eq!(message.correlation_id(), Some("req-abc"));
        assert_eq!(message.meta("trace_id"), Some("t-999"));
    }

    #[test]
    fn encode_with_metadata() {
        let mut meta = HashMap::new();
        meta.insert("correlation_id".to_string(), "req-456".to_string());

        let payload = ("hello", 42i32);
        let message =
            OutboxMessage::encode_with_metadata("msg-2", "SomeEvent", &payload, meta).unwrap();

        assert_eq!(message.correlation_id(), Some("req-456"));
        let decoded: (String, i32) = message.decode().unwrap();
        assert_eq!(decoded, ("hello".to_string(), 42));
    }

    #[test]
    fn set_metadata_individually() {
        let mut message = OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap();
        message.set_correlation_id("req-abc");
        message.set_causation_id("evt-prior");
        message.set_meta("tenant", "acme");

        assert_eq!(message.correlation_id(), Some("req-abc"));
        assert_eq!(message.causation_id(), Some("evt-prior"));
        assert_eq!(message.meta("tenant"), Some("acme"));
    }

    #[test]
    fn final_causal_stamp_overwrites_handler_metadata() {
        let mut message = OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap();
        message.set_causation_id("handler-supplied");
        message.set_meta("Causation_Id", "forged-case-alias");

        message.overwrite_causation_id("ledger-causation");

        assert_eq!(message.causation_id(), Some("ledger-causation"));
        assert_eq!(
            message
                .metadata
                .keys()
                .filter(|key| key.eq_ignore_ascii_case(CAUSATION_ID))
                .count(),
            1
        );

        let transport: crate::bus::Message = message.into();
        assert_eq!(transport.causation_id(), Some("ledger-causation"));
        assert_eq!(
            transport
                .metadata
                .iter()
                .filter(|(key, _)| key.eq_ignore_ascii_case(CAUSATION_ID))
                .count(),
            1
        );
    }

    #[test]
    fn set_trace_context_replaces_existing_trace_metadata() {
        let mut message = OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap();
        message.set_meta("TraceParent", "old");
        let context = TraceContext {
            traceparent: Some(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
            ),
            tracestate: Some("vendor=value".to_string()),
        };

        message.set_trace_context(&context);

        assert_eq!(message.trace_context(), context);
        assert!(!message.metadata.contains_key("TraceParent"));
    }

    #[test]
    fn release_and_fail() {
        let mut message = OutboxMessage::new();
        message
            .initialize(
                "msg-1".into(),
                "Event1".into(),
                b"{}".to_vec(),
                None,
                HashMap::new(),
            )
            .unwrap();
        message
            .claim_for("worker-1", Duration::from_secs(60))
            .unwrap();

        message.release("timeout".into()).unwrap();
        assert!(message.is_pending());
        assert_eq!(message.last_error.as_deref(), Some("timeout"));

        message
            .claim_for("worker-1", Duration::from_secs(60))
            .unwrap();
        message.fail("max retries".into()).unwrap();
        assert!(message.is_failed());
    }
}
