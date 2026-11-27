use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::time::SystemTime;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::trace_context::{TraceContext, CAUSATION_ID, CORRELATION_ID, TRACEPARENT, TRACESTATE};

pub const BITCODE_PAYLOAD_CODEC: &str = "bitcode";
pub const BITCODE_PAYLOAD_CODEC_VERSION: u16 = 1;

/// The payload codec: bitcode, the only codec events and outbox messages use.
pub struct BitcodePayloadCodec;

impl BitcodePayloadCodec {
    pub const NAME: &'static str = BITCODE_PAYLOAD_CODEC;
    pub const VERSION: u16 = BITCODE_PAYLOAD_CODEC_VERSION;

    pub fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, bitcode::Error> {
        bitcode::serialize(value)
    }

    pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, bitcode::Error> {
        bitcode::deserialize(bytes)
    }
}

/// Error when serializing or deserializing event records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventRecordError {
    pub message: String,
}

impl EventRecordError {
    pub fn encode(source: impl fmt::Display) -> Self {
        Self {
            message: format!(
                "failed to encode payload with codec `{}` version {}: {}",
                BITCODE_PAYLOAD_CODEC, BITCODE_PAYLOAD_CODEC_VERSION, source
            ),
        }
    }

    pub fn decode(
        payload_type: impl fmt::Display,
        codec: impl fmt::Display,
        codec_version: u16,
        source: impl fmt::Display,
    ) -> Self {
        Self {
            message: format!(
                "failed to decode payload `{}` with codec `{}` version {}: {}",
                payload_type, codec, codec_version, source
            ),
        }
    }

    pub fn unsupported_codec(codec: impl fmt::Display, codec_version: u16) -> Self {
        Self {
            message: format!(
                "unsupported payload codec `{}` version {}",
                codec, codec_version
            ),
        }
    }
}

impl fmt::Display for EventRecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "event record error: {}", self.message)
    }
}

impl std::error::Error for EventRecordError {}

fn default_event_version() -> u64 {
    1
}
fn is_version_one(v: &u64) -> bool {
    *v == 1
}
fn default_payload_codec() -> Cow<'static, str> {
    Cow::Borrowed(BITCODE_PAYLOAD_CODEC)
}
fn default_payload_codec_version() -> u16 {
    BITCODE_PAYLOAD_CODEC_VERSION
}

/// Replayable aggregate event record stored in an event-sourced entity stream.
///
/// `EventRecord` is model history for aggregate hydration. It is not
/// automatically a domain event or integration message; publishable messages
/// belong in the outbox/bus boundary.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct EventRecord {
    pub event_name: String,
    /// Payload codec name. `Cow` because virtually every event carries the
    /// crate's own codec constant — the borrowed variant avoids one heap
    /// allocation per event created and per row decoded.
    #[serde(default = "default_payload_codec")]
    pub payload_codec: Cow<'static, str>,
    #[serde(default = "default_payload_codec_version")]
    pub payload_codec_version: u16,
    #[serde(with = "payload_serde")]
    pub payload: Vec<u8>,
    #[serde(
        default = "default_event_version",
        skip_serializing_if = "is_version_one"
    )]
    pub event_version: u64,
    pub sequence: u64,
    pub timestamp: SystemTime,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

mod payload_serde {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(payload: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        STANDARD.encode(payload).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        STANDARD.decode(&s).map_err(serde::de::Error::custom)
    }
}

impl EventRecord {
    pub fn new(event_name: impl Into<String>, payload: Vec<u8>, sequence: u64) -> Self {
        Self::new_versioned(event_name, payload, sequence, 1)
    }

    /// Create an event record at a specific version.
    pub fn new_versioned(
        event_name: impl Into<String>,
        payload: Vec<u8>,
        sequence: u64,
        version: u64,
    ) -> Self {
        EventRecord {
            event_name: event_name.into(),
            payload_codec: Cow::Borrowed(BITCODE_PAYLOAD_CODEC),
            payload_codec_version: BITCODE_PAYLOAD_CODEC_VERSION,
            payload,
            event_version: version,
            sequence,
            timestamp: crate::time::now(),
            metadata: HashMap::new(),
        }
    }

    /// Create an event record with metadata.
    pub fn with_metadata(
        event_name: impl Into<String>,
        payload: Vec<u8>,
        sequence: u64,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            metadata,
            ..Self::new(event_name, payload, sequence)
        }
    }

    /// Deserialize the payload into the specified type.
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, EventRecordError> {
        if self.payload_codec != BITCODE_PAYLOAD_CODEC
            || self.payload_codec_version != BITCODE_PAYLOAD_CODEC_VERSION
        {
            return Err(EventRecordError::unsupported_codec(
                &self.payload_codec,
                self.payload_codec_version,
            ));
        }

        BitcodePayloadCodec::decode(&self.payload).map_err(|e| {
            EventRecordError::decode(
                &self.event_name,
                &self.payload_codec,
                self.payload_codec_version,
                e,
            )
        })
    }

    /// Get the raw payload bytes.
    pub fn payload_bytes(&self) -> &[u8] {
        &self.payload
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

    /// Replace causation metadata at the final causal-commit boundary.
    ///
    /// This is deliberately crate-private: application metadata may be set
    /// while handling a command, but the ledger attempt is authoritative for
    /// every newly persisted event in that command transaction.
    #[cfg_attr(not(feature = "graphql"), allow(dead_code))]
    pub(crate) fn overwrite_causation_id(&mut self, id: &str) {
        self.metadata
            .retain(|key, _| !key.eq_ignore_ascii_case(CAUSATION_ID));
        self.metadata
            .insert(CAUSATION_ID.to_string(), id.to_string());
    }

    /// Get the W3C `traceparent`, if set.
    pub fn traceparent(&self) -> Option<&str> {
        self.meta(TRACEPARENT)
    }

    /// Get the W3C `tracestate`, if set.
    pub fn tracestate(&self) -> Option<&str> {
        self.meta(TRACESTATE)
    }

    /// Extract W3C trace context from event metadata.
    pub fn trace_context(&self) -> TraceContext {
        TraceContext::from_metadata(self.metadata.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_deserialize() {
        let payload = bitcode::serialize(&("arg1", "arg2")).unwrap();
        let event_record = EventRecord::new("test_event", payload.clone(), 1);
        let serialized = serde_json::to_string(&event_record).unwrap();
        let deserialized: EventRecord = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.event_name, "test_event");
        assert_eq!(deserialized.payload, payload);
        assert_eq!(deserialized.sequence, 1);
        assert_eq!(deserialized.timestamp, event_record.timestamp);
    }

    /// Version 1 is the default and is left out of the JSON form; reading the
    /// JSON back restores it.
    #[test]
    fn event_version_one_is_the_default_and_left_out_of_json() {
        let record = EventRecord::new("tested", vec![1, 2, 3], 1);
        assert_eq!(record.event_version, 1);

        let json = serde_json::to_string(&record).unwrap();
        assert!(!json.contains("event_version"), "{json}");

        let deserialized: EventRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.event_version, 1);
        assert_eq!(deserialized, record);
    }

    #[test]
    fn an_event_version_other_than_one_is_written_and_read_back() {
        let record = EventRecord::new_versioned("tested", vec![1, 2, 3], 1, 3);
        assert_eq!(record.event_version, 3);
        assert_eq!(record.event_name, "tested");

        let json = serde_json::to_string(&record).unwrap();
        assert!(json.contains("\"event_version\":3"), "{json}");

        let deserialized: EventRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.event_version, 3);
        assert_eq!(deserialized.event_name, "tested");
        assert_eq!(deserialized.sequence, 1);
    }

    #[test]
    fn decode_payload() {
        let payload = bitcode::serialize(&("hello", 42i32, true)).unwrap();
        let event_record = EventRecord::new("test_event", payload, 1);
        let decoded: (String, i32, bool) = event_record.decode().unwrap();
        assert_eq!(decoded, ("hello".to_string(), 42, true));
    }

    #[test]
    fn decode_unknown_codec_returns_error() {
        let mut event_record = EventRecord::new("test_event", vec![], 1);
        event_record.payload_codec = "json".into();

        let err = event_record.decode::<()>().unwrap_err();
        assert!(err.message.contains("unsupported payload codec `json`"));
    }

    #[test]
    fn with_metadata_constructor() {
        let mut meta = HashMap::new();
        meta.insert("correlation_id".to_string(), "req-123".to_string());
        meta.insert("user_id".to_string(), "u-1".to_string());

        let record = EventRecord::with_metadata("test_event", vec![], 1, meta);
        assert_eq!(record.correlation_id(), Some("req-123"));
        assert_eq!(record.meta("user_id"), Some("u-1"));
        assert_eq!(record.causation_id(), None);
    }

    #[test]
    fn trace_context_helpers_read_event_metadata() {
        let mut meta = HashMap::new();
        meta.insert(
            "traceparent".to_string(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
        );
        meta.insert("tracestate".to_string(), "vendor=value".to_string());

        let record = EventRecord::with_metadata("test_event", vec![], 1, meta);

        assert_eq!(
            record.traceparent(),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
        assert_eq!(record.tracestate(), Some("vendor=value"));
    }

    #[test]
    fn final_causal_stamp_removes_case_insensitive_aliases() {
        let mut metadata = HashMap::new();
        metadata.insert(CAUSATION_ID.to_string(), "handler-supplied".to_string());
        metadata.insert("Causation_Id".to_string(), "forged-case-alias".to_string());
        let mut record = EventRecord::with_metadata("test_event", vec![], 1, metadata);

        record.overwrite_causation_id("ledger-causation");

        assert_eq!(record.causation_id(), Some("ledger-causation"));
        assert_eq!(
            record
                .metadata
                .keys()
                .filter(|key| key.eq_ignore_ascii_case(CAUSATION_ID))
                .count(),
            1
        );
    }

    #[test]
    fn metadata_is_always_present_in_serialization() {
        let record = EventRecord::new("test_event", vec![], 1);
        let json = serde_json::to_string(&record).unwrap();
        assert!(json.contains("metadata"));

        let mut meta = HashMap::new();
        meta.insert("key".to_string(), "val".to_string());
        let record_with_meta = EventRecord::with_metadata("test_event", vec![], 1, meta);
        let json = serde_json::to_string(&record_with_meta).unwrap();
        assert!(json.contains("metadata"));
        assert!(json.contains("key"));
    }
}
