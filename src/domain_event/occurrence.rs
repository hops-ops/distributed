use std::collections::BTreeMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::bus::{
    validate_message_name, validate_stable_message_id, MessageNameError, StableMessageIdError,
};
use crate::trace_context::{TraceContext, CAUSATION_ID, CORRELATION_ID};

use super::{
    canonical_json_bytes, DomainEventBodyKind, DomainEventDescriptor, DomainStateDescriptor,
    DOMAIN_EVENT_BODY_CODEC, DOMAIN_EVENT_BODY_CODEC_VERSION, MAX_DOMAIN_EVENT_BODY_BYTES,
    MAX_DOMAIN_EVENT_OCCURRENCE_WIRE_BYTES,
};

/// Version of the canonical [`DomainEventOccurrence`] envelope.
pub const DOMAIN_EVENT_OCCURRENCE_VERSION: u16 = 1;

/// Provenance of a typed fact produced while handling an earlier occurrence.
/// The envelope's aggregate fields still describe the originating transition,
/// not a new aggregate commit by this producer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainEventDerivation {
    /// Immediate parent occurrence (which may itself be derived).
    pub source_occurrence_id: String,
    /// Stable effect-handler identity.
    pub producer: String,
    /// Stable output identity within this handler/source pair.
    pub output_key: String,
}

/// Immutable framework metadata captured with one outward event transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainEventEnvelope {
    /// Stable aggregate type name.
    pub aggregate_type: String,
    /// Stable aggregate stream identifier.
    pub aggregate_id: String,
    /// Aggregate event sequence that caused this occurrence.
    pub aggregate_sequence: u64,
    /// Zero-based publication position within one aggregate sequence.
    pub publication_ordinal: u32,
    /// Transition-time timestamp.
    pub occurred_at: SystemTime,
    /// Correlation, causation, trace, and application metadata.
    pub metadata: BTreeMap<String, String>,
}

/// One exact typed outward event captured at aggregate transition time.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct DomainEventOccurrence {
    #[serde(skip_serializing_if = "Option::is_none")]
    derivation: Option<DomainEventDerivation>,
    /// Canonical occurrence envelope version.
    occurrence_version: u16,
    /// Retry-stable occurrence identity.
    id: String,
    /// Semantic event and body schema.
    descriptor: DomainEventDescriptor,
    /// Stable aggregate type name.
    aggregate_type: String,
    /// Stable aggregate stream identifier.
    aggregate_id: String,
    /// Aggregate event sequence that caused this occurrence.
    aggregate_sequence: u64,
    /// Zero-based publication position within one aggregate sequence.
    publication_ordinal: u32,
    /// Milliseconds since Unix epoch at transition-time capture.
    occurred_at_unix_ms: u64,
    /// Canonically encoded typed body.
    #[serde(with = "base64_bytes")]
    body: Vec<u8>,
    /// Correlation, causation, trace, and application metadata.
    metadata: BTreeMap<String, String>,
}

impl fmt::Debug for DomainEventOccurrence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DomainEventOccurrence")
            .field("occurrence_version", &self.occurrence_version)
            .field("id", &self.id)
            .field("descriptor", &self.descriptor)
            .field("aggregate_type", &self.aggregate_type)
            .field("aggregate_id", &self.aggregate_id)
            .field("aggregate_sequence", &self.aggregate_sequence)
            .field("publication_ordinal", &self.publication_ordinal)
            .field("occurred_at_unix_ms", &self.occurred_at_unix_ms)
            .field("body_len", &self.body.len())
            .field("metadata_count", &self.metadata.len())
            .finish()
    }
}

impl DomainEventOccurrence {
    pub(crate) fn capture(
        descriptor: DomainEventDescriptor,
        envelope: DomainEventEnvelope,
        body: &impl Serialize,
    ) -> Result<Self, DomainEventCaptureError> {
        validate_descriptor(&descriptor)?;
        validate_message_name(&envelope.aggregate_type)
            .map_err(DomainEventCaptureError::AggregateType)?;
        validate_stable_message_id(Some(&envelope.aggregate_id))
            .map_err(DomainEventCaptureError::AggregateId)?;
        if envelope.aggregate_sequence == 0 {
            return Err(DomainEventCaptureError::ZeroAggregateSequence);
        }

        let body = canonical_json_bytes(body)?;
        if body.len() > MAX_DOMAIN_EVENT_BODY_BYTES {
            return Err(DomainEventCaptureError::BodyTooLarge { len: body.len() });
        }
        let occurred_at_unix_ms = envelope
            .occurred_at
            .duration_since(UNIX_EPOCH)
            .map_err(|_| DomainEventCaptureError::TimestampBeforeUnixEpoch)?
            .as_millis()
            .try_into()
            .map_err(|_| DomainEventCaptureError::TimestampOverflow)?;
        let id = occurrence_id(&descriptor, &envelope);
        validate_stable_message_id(Some(&id)).map_err(DomainEventCaptureError::OccurrenceId)?;

        let occurrence = Self {
            derivation: None,
            occurrence_version: DOMAIN_EVENT_OCCURRENCE_VERSION,
            id,
            descriptor,
            aggregate_type: envelope.aggregate_type,
            aggregate_id: envelope.aggregate_id,
            aggregate_sequence: envelope.aggregate_sequence,
            publication_ordinal: envelope.publication_ordinal,
            occurred_at_unix_ms,
            body,
            metadata: envelope.metadata,
        };
        occurrence.canonical_bytes()?;
        Ok(occurrence)
    }

    /// Return the immutable canonical body bytes.
    pub fn body_bytes(&self) -> &[u8] {
        &self.body
    }

    /// Derive a typed fact without inventing another aggregate transition.
    ///
    /// Repeat with the same source, producer, output key and body for identical
    /// canonical bytes. Changed bodies have distinct identities. Publishing is
    /// still at-least-once: acknowledge the source only after all outputs have
    /// been accepted by the bus, and retry accepted prefixes with these IDs.
    pub fn derive<T: super::DomainEvent + Serialize>(
        &self,
        producer: &str,
        output_key: &str,
        body: &T,
    ) -> Result<Self, DomainEventCaptureError> {
        self.validate()?;
        let mut result = self.clone();
        result.descriptor = T::DESCRIPTOR.clone();
        if result.descriptor.body.kind != DomainEventBodyKind::Event {
            return Err(DomainEventCaptureError::BodyKindMismatch {
                expected: DomainEventBodyKind::Event,
                actual: result.descriptor.body.kind,
            });
        }
        result.derivation = Some(DomainEventDerivation {
            source_occurrence_id: self.id.clone(),
            producer: producer.into(),
            output_key: output_key.into(),
        });
        result.body = canonical_json_bytes(body)?;
        result.id = result.derived_identity()?;
        result.canonical_bytes()?;
        Ok(result)
    }

    /// None for an aggregate's own captured transition facts.
    pub fn derivation(&self) -> Option<&DomainEventDerivation> {
        self.derivation.as_ref()
    }

    fn derived_identity(&self) -> Result<String, DomainEventCaptureError> {
        let derivation = self
            .derivation
            .as_ref()
            .ok_or(DomainEventCaptureError::InvalidDerivation)?;
        validate_message_name(&derivation.producer)
            .map_err(|_| DomainEventCaptureError::InvalidDerivation)?;
        validate_stable_message_id(Some(&derivation.source_occurrence_id))
            .map_err(|_| DomainEventCaptureError::InvalidDerivation)?;
        if derivation.output_key.trim().is_empty()
            || derivation.output_key.len() > 1024
            || derivation.output_key.chars().any(char::is_control)
        {
            return Err(DomainEventCaptureError::InvalidDerivation);
        }
        if self.descriptor.body.kind != DomainEventBodyKind::Event {
            return Err(DomainEventCaptureError::InvalidDerivation);
        }
        let mut hash = Sha256::new();
        hash.update(b"distributed.domain-event.derived/v1\0");
        for value in [
            &derivation.source_occurrence_id,
            &derivation.producer,
            &derivation.output_key,
            &self.aggregate_type,
            &self.aggregate_id,
        ] {
            hash_component(&mut hash, value.as_bytes());
        }
        hash.update(self.aggregate_sequence.to_be_bytes());
        hash.update(self.publication_ordinal.to_be_bytes());
        hash.update(self.occurred_at_unix_ms.to_be_bytes());
        hash_component(&mut hash, &canonical_json_bytes(&self.descriptor)?);
        hash_component(&mut hash, &self.body);
        Ok(format!("dd1:sha256:{:x}", hash.finalize()))
    }

    /// Return the canonical occurrence envelope version.
    pub fn occurrence_version(&self) -> u16 {
        self.occurrence_version
    }

    /// Return the retry-stable occurrence identity.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Return the semantic event and body schema.
    pub fn descriptor(&self) -> &DomainEventDescriptor {
        &self.descriptor
    }

    /// Return the stable aggregate type.
    pub fn aggregate_type(&self) -> &str {
        &self.aggregate_type
    }

    /// Return the stable aggregate stream ID.
    pub fn aggregate_id(&self) -> &str {
        &self.aggregate_id
    }

    /// Return the causing aggregate event sequence.
    pub fn aggregate_sequence(&self) -> u64 {
        self.aggregate_sequence
    }

    /// Return the zero-based publication ordinal within the aggregate sequence.
    pub fn publication_ordinal(&self) -> u32 {
        self.publication_ordinal
    }

    /// Return transition time as Unix epoch milliseconds.
    pub fn occurred_at_unix_ms(&self) -> u64 {
        self.occurred_at_unix_ms
    }

    /// Return captured envelope metadata.
    pub fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// Return one metadata value using case-insensitive key comparison.
    pub fn meta(&self, key: &str) -> Option<&str> {
        self.metadata
            .iter()
            .find(|(existing, _)| existing.eq_ignore_ascii_case(key))
            .map(|(_, value)| value.as_str())
    }

    /// Return the captured workflow correlation ID.
    pub fn correlation_id(&self) -> Option<&str> {
        self.meta(CORRELATION_ID)
    }

    /// Return the captured command causation ID.
    pub fn causation_id(&self) -> Option<&str> {
        self.meta(CAUSATION_ID)
    }

    /// Return the captured W3C trace context.
    pub fn trace_context(&self) -> TraceContext {
        TraceContext::from_metadata(self.metadata.iter())
    }

    /// Decode the canonical typed body.
    ///
    /// # Errors
    ///
    /// Returns a typed capture error when the descriptor codec is unsupported
    /// or the canonical JSON cannot decode as `T`.
    pub fn decode_body<T: DeserializeOwned>(&self) -> Result<T, DomainEventCaptureError> {
        validate_codec(&self.descriptor)?;
        serde_json::from_slice(&self.body)
            .map_err(|error| DomainEventCaptureError::BodyDecoding(error.to_string()))
    }

    /// Serialize this envelope with canonical object-key ordering.
    ///
    /// # Errors
    ///
    /// Returns a typed error if serialization fails or the bounded wire ceiling
    /// is exceeded.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DomainEventCaptureError> {
        self.validate()?;
        let bytes = canonical_json_bytes(self)?;
        if bytes.len() > MAX_DOMAIN_EVENT_OCCURRENCE_WIRE_BYTES {
            return Err(DomainEventCaptureError::OccurrenceTooLarge { len: bytes.len() });
        }
        Ok(bytes)
    }

    /// Parse and validate one bounded canonical occurrence.
    ///
    /// # Errors
    ///
    /// Rejects over-limit input before JSON allocation, malformed envelopes,
    /// non-canonical bytes, unsupported codecs, and invalid identities.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, DomainEventCaptureError> {
        if bytes.len() > MAX_DOMAIN_EVENT_OCCURRENCE_WIRE_BYTES {
            return Err(DomainEventCaptureError::OccurrenceTooLarge { len: bytes.len() });
        }
        let wire: DomainEventOccurrenceWire = serde_json::from_slice(bytes)
            .map_err(|error| DomainEventCaptureError::OccurrenceDecoding(error.to_string()))?;
        let occurrence = Self {
            derivation: wire.derivation,
            occurrence_version: wire.occurrence_version,
            id: wire.id,
            descriptor: wire.descriptor,
            aggregate_type: wire.aggregate_type,
            aggregate_id: wire.aggregate_id,
            aggregate_sequence: wire.aggregate_sequence,
            publication_ordinal: wire.publication_ordinal,
            occurred_at_unix_ms: wire.occurred_at_unix_ms,
            body: wire.body,
            metadata: wire.metadata,
        };
        occurrence.validate()?;
        let canonical = occurrence.canonical_bytes()?;
        if canonical != bytes {
            return Err(DomainEventCaptureError::NonCanonicalOccurrence);
        }
        Ok(occurrence)
    }

    fn validate(&self) -> Result<(), DomainEventCaptureError> {
        if self.occurrence_version != DOMAIN_EVENT_OCCURRENCE_VERSION {
            return Err(DomainEventCaptureError::UnsupportedOccurrenceVersion {
                version: self.occurrence_version,
            });
        }
        validate_descriptor(&self.descriptor)?;
        validate_message_name(&self.aggregate_type)
            .map_err(DomainEventCaptureError::AggregateType)?;
        validate_stable_message_id(Some(&self.aggregate_id))
            .map_err(DomainEventCaptureError::AggregateId)?;
        validate_stable_message_id(Some(&self.id))
            .map_err(DomainEventCaptureError::OccurrenceId)?;
        if self.aggregate_sequence == 0 {
            return Err(DomainEventCaptureError::ZeroAggregateSequence);
        }
        if self.body.len() > MAX_DOMAIN_EVENT_BODY_BYTES {
            return Err(DomainEventCaptureError::BodyTooLarge {
                len: self.body.len(),
            });
        }
        let value: serde_json::Value = serde_json::from_slice(&self.body)
            .map_err(|error| DomainEventCaptureError::BodyDecoding(error.to_string()))?;
        if canonical_json_bytes(&value)? != self.body {
            return Err(DomainEventCaptureError::NonCanonicalBody);
        }
        let envelope = DomainEventEnvelope {
            aggregate_type: self.aggregate_type.clone(),
            aggregate_id: self.aggregate_id.clone(),
            aggregate_sequence: self.aggregate_sequence,
            publication_ordinal: self.publication_ordinal,
            occurred_at: UNIX_EPOCH,
            metadata: BTreeMap::new(),
        };
        let expected_id = if self.derivation.is_some() {
            self.derived_identity()?
        } else {
            occurrence_id(&self.descriptor, &envelope)
        };
        if expected_id != self.id {
            return Err(DomainEventCaptureError::OccurrenceIdentityMismatch);
        }
        Ok(())
    }

    pub(crate) fn overwrite_causation_id(&mut self, id: &str) {
        self.metadata
            .retain(|key, _| !key.eq_ignore_ascii_case(CAUSATION_ID));
        self.metadata
            .insert(CAUSATION_ID.to_string(), id.to_string());
    }
}

#[derive(Deserialize)]
struct DomainEventOccurrenceWire {
    #[serde(default)]
    derivation: Option<DomainEventDerivation>,
    occurrence_version: u16,
    id: String,
    descriptor: DomainEventDescriptor,
    aggregate_type: String,
    aggregate_id: String,
    aggregate_sequence: u64,
    publication_ordinal: u32,
    occurred_at_unix_ms: u64,
    #[serde(with = "base64_bytes")]
    body: Vec<u8>,
    metadata: BTreeMap<String, String>,
}

fn occurrence_id(descriptor: &DomainEventDescriptor, envelope: &DomainEventEnvelope) -> String {
    let mut digest = Sha256::new();
    digest.update(b"distributed.domain-event.occurrence/v1\0");
    hash_component(&mut digest, envelope.aggregate_type.as_bytes());
    hash_component(&mut digest, envelope.aggregate_id.as_bytes());
    digest.update(envelope.aggregate_sequence.to_be_bytes());
    digest.update(envelope.publication_ordinal.to_be_bytes());
    hash_component(&mut digest, descriptor.name.as_bytes());
    digest.update(descriptor.version.to_be_bytes());
    hash_component(&mut digest, descriptor.body.fingerprint.as_bytes());
    let digest = digest.finalize();
    format!("de1:sha256:{digest:x}")
}

fn hash_component(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

fn validate_descriptor(descriptor: &DomainEventDescriptor) -> Result<(), DomainEventCaptureError> {
    validate_message_name(&descriptor.name).map_err(DomainEventCaptureError::EventName)?;
    if descriptor.version == 0 {
        return Err(DomainEventCaptureError::ZeroEventVersion);
    }
    if descriptor.body.type_name.trim().is_empty() {
        return Err(DomainEventCaptureError::EmptyBodyType);
    }
    if descriptor.body.version == 0 {
        return Err(DomainEventCaptureError::ZeroBodyVersion);
    }
    if descriptor.body.schema.trim().is_empty() {
        return Err(DomainEventCaptureError::EmptyBodySchema);
    }
    validate_fingerprint(&descriptor.body.fingerprint)?;
    validate_codec(descriptor)
}

fn validate_codec(descriptor: &DomainEventDescriptor) -> Result<(), DomainEventCaptureError> {
    if descriptor.body.codec != DOMAIN_EVENT_BODY_CODEC
        || descriptor.body.codec_version != DOMAIN_EVENT_BODY_CODEC_VERSION
    {
        return Err(DomainEventCaptureError::UnsupportedBodyCodec {
            codec: descriptor.body.codec.to_string(),
            version: descriptor.body.codec_version,
        });
    }
    Ok(())
}

fn validate_fingerprint(fingerprint: &str) -> Result<(), DomainEventCaptureError> {
    let Some(encoded) = fingerprint.strip_prefix("sha256:") else {
        return Err(DomainEventCaptureError::InvalidBodyFingerprint);
    };
    if encoded.len() != 64
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(DomainEventCaptureError::InvalidBodyFingerprint);
    }
    Ok(())
}

/// Typed failure while capturing or parsing an outward occurrence.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DomainEventCaptureError {
    /// Derived provenance has an invalid producer, output key or source identity.
    InvalidDerivation,
    /// Semantic event name violated transport naming rules.
    EventName(MessageNameError),
    /// Aggregate type violated transport naming rules.
    AggregateType(MessageNameError),
    /// Aggregate ID violated stable-ID rules.
    AggregateId(StableMessageIdError),
    /// Generated occurrence ID violated stable-ID rules.
    OccurrenceId(StableMessageIdError),
    /// Semantic event versions start at one.
    ZeroEventVersion,
    /// Body schema versions start at one.
    ZeroBodyVersion,
    /// Aggregate event sequences start at one.
    ZeroAggregateSequence,
    /// Body type name was empty.
    EmptyBodyType,
    /// Body schema identifier was empty.
    EmptyBodySchema,
    /// Body fingerprint was not canonical lowercase SHA-256.
    InvalidBodyFingerprint,
    /// Body codec is not supported by this runtime version.
    UnsupportedBodyCodec {
        /// Codec name.
        codec: String,
        /// Codec version.
        version: u16,
    },
    /// Typed body serialization failed.
    BodyEncoding(String),
    /// Typed body decoding failed.
    BodyDecoding(String),
    /// Canonical body exceeded the one-MiB ceiling.
    BodyTooLarge {
        /// Actual canonical body size.
        len: usize,
    },
    /// Serialized occurrence exceeded its bounded envelope ceiling.
    OccurrenceTooLarge {
        /// Actual serialized occurrence size.
        len: usize,
    },
    /// Occurrence timestamp predates the Unix epoch.
    TimestampBeforeUnixEpoch,
    /// Occurrence timestamp cannot fit the version-one millisecond field.
    TimestampOverflow,
    /// Occurrence JSON could not be decoded.
    OccurrenceDecoding(String),
    /// Occurrence bytes were valid JSON but not the canonical representation.
    NonCanonicalOccurrence,
    /// Body bytes were valid JSON but not the canonical representation.
    NonCanonicalBody,
    /// Serialized identity did not match canonical identity components.
    OccurrenceIdentityMismatch,
    /// Occurrence envelope version is unsupported.
    UnsupportedOccurrenceVersion {
        /// Encountered version.
        version: u16,
    },
    /// Capture was attempted without a new aggregate replay event.
    NoPendingAggregateEvent,
    /// More than `u32::MAX` outward events were attached to one sequence.
    PublicationOrdinalOverflow,
    /// Capture was attempted after an earlier capture poisoned the entity.
    EntityAlreadyPoisoned,
    /// A state descriptor did not match the state type being serialized.
    StateDescriptorMismatch,
    /// A capture method was used with the wrong body derivation kind.
    BodyKindMismatch {
        /// Required body kind.
        expected: DomainEventBodyKind,
        /// Descriptor body kind.
        actual: DomainEventBodyKind,
    },
}

impl fmt::Display for DomainEventCaptureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDerivation => formatter.write_str("invalid domain-event derivation"),
            Self::EventName(error) => write!(formatter, "invalid domain-event name: {error}"),
            Self::AggregateType(error) => write!(formatter, "invalid aggregate type: {error}"),
            Self::AggregateId(error) => write!(formatter, "invalid aggregate id: {error}"),
            Self::OccurrenceId(error) => write!(formatter, "invalid occurrence id: {error}"),
            Self::ZeroEventVersion => formatter.write_str("domain-event version must be non-zero"),
            Self::ZeroBodyVersion => {
                formatter.write_str("domain-event body version must be non-zero")
            }
            Self::ZeroAggregateSequence => {
                formatter.write_str("domain-event aggregate sequence must be non-zero")
            }
            Self::EmptyBodyType => formatter.write_str("domain-event body type is empty"),
            Self::EmptyBodySchema => formatter.write_str("domain-event body schema is empty"),
            Self::InvalidBodyFingerprint => formatter.write_str(
                "domain-event body fingerprint must be `sha256:` plus 64 lowercase hex digits",
            ),
            Self::UnsupportedBodyCodec { codec, version } => {
                write!(formatter, "unsupported domain-event body codec `{codec}` version {version}")
            }
            Self::BodyEncoding(message) => {
                write!(formatter, "failed to encode canonical domain-event body: {message}")
            }
            Self::BodyDecoding(message) => {
                write!(formatter, "failed to decode canonical domain-event body: {message}")
            }
            Self::BodyTooLarge { len } => write!(
                formatter,
                "domain-event body is {len} bytes, exceeding the maximum of {MAX_DOMAIN_EVENT_BODY_BYTES}"
            ),
            Self::OccurrenceTooLarge { len } => write!(
                formatter,
                "domain-event occurrence is {len} bytes, exceeding the maximum of {MAX_DOMAIN_EVENT_OCCURRENCE_WIRE_BYTES}"
            ),
            Self::TimestampBeforeUnixEpoch => {
                formatter.write_str("domain-event timestamp predates Unix epoch")
            }
            Self::TimestampOverflow => {
                formatter.write_str("domain-event timestamp exceeds version-one range")
            }
            Self::OccurrenceDecoding(message) => {
                write!(formatter, "failed to decode domain-event occurrence: {message}")
            }
            Self::NonCanonicalOccurrence => {
                formatter.write_str("domain-event occurrence bytes are not canonical")
            }
            Self::NonCanonicalBody => {
                formatter.write_str("domain-event body bytes are not canonical")
            }
            Self::OccurrenceIdentityMismatch => {
                formatter.write_str("domain-event occurrence identity does not match its envelope")
            }
            Self::UnsupportedOccurrenceVersion { version } => {
                write!(formatter, "unsupported domain-event occurrence version {version}")
            }
            Self::NoPendingAggregateEvent => {
                formatter.write_str("domain-event capture requires a new aggregate event")
            }
            Self::PublicationOrdinalOverflow => {
                formatter.write_str("domain-event publication ordinal overflow")
            }
            Self::EntityAlreadyPoisoned => {
                formatter.write_str("entity has an earlier domain-event capture poison")
            }
            Self::StateDescriptorMismatch => {
                formatter.write_str("domain-event descriptor does not match the domain-state type")
            }
            Self::BodyKindMismatch { expected, actual } => write!(
                formatter,
                "domain-event body kind mismatch: expected {expected:?}, found {actual:?}"
            ),
        }
    }
}

impl std::error::Error for DomainEventCaptureError {}

/// Sticky first failure that prevents committing an entity's outward events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainEventCapturePoison {
    /// Descriptor whose capture failed.
    pub descriptor: DomainEventDescriptor,
    /// Aggregate sequence being captured.
    pub aggregate_sequence: u64,
    /// Publication ordinal that would have been assigned.
    pub publication_ordinal: u32,
    /// Typed capture failure.
    pub error: DomainEventCaptureError,
}

/// Manual unit-of-work guard failure for a poisoned entity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainEventCommitGuardError {
    poison: Box<DomainEventCapturePoison>,
}

impl DomainEventCommitGuardError {
    /// Return the sticky first capture failure.
    pub fn poison(&self) -> &DomainEventCapturePoison {
        &self.poison
    }

    pub(crate) fn new(poison: DomainEventCapturePoison) -> Self {
        Self {
            poison: Box::new(poison),
        }
    }
}

impl fmt::Display for DomainEventCommitGuardError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "domain-event capture poisoned aggregate sequence {} ordinal {}: {}",
            self.poison.aggregate_sequence, self.poison.publication_ordinal, self.poison.error
        )
    }
}

impl std::error::Error for DomainEventCommitGuardError {}

/// Result of a transition-time capture attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DomainEventCaptureOutcome {
    /// An immutable occurrence was appended to the pending buffer.
    Captured {
        /// Retry-stable occurrence ID.
        id: String,
    },
    /// Aggregate replay intentionally suppressed outward capture.
    SuppressedDuringReplay,
}

pub(crate) fn state_descriptor_matches(
    event: &DomainEventDescriptor,
    state: &DomainStateDescriptor,
) -> bool {
    event.body.kind == DomainEventBodyKind::State
        && event.body.type_name == state.type_name
        && event.body.version == state.version
        && event.body.schema == state.schema
        && event.body.fingerprint == state.fingerprint
        && event.body.codec == state.codec
        && event.body.codec_version == state.codec_version
}

mod base64_bytes {
    use super::*;

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        STANDARD.encode(bytes).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        STANDARD.decode(encoded).map_err(serde::de::Error::custom)
    }
}
