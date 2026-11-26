//! The one mapping between a [`Message`] and what a broker carries.
//!
//! Every adapter writes and reads messages through [`WireEnvelope`]. A header
//! transport (NATS, Kafka, RabbitMQ, Knative, Redis) carries the envelope's
//! ordered header pairs; a transport with typed storage (the SQL buses, the
//! celld Queue body, the in-memory bus) stores the message as it is but runs
//! the same [`WireEnvelope::validate`] at its publish boundary, so a message
//! one adapter accepts every adapter accepts.

use super::{
    validate_message_name, validate_stable_message_id, Message, MessageKind, TransportError,
};

/// A [`Message`] as a broker carries it: a name (the broker address the
/// adapter derives it from), ordered header pairs, and the payload.
///
/// The message id, kind and content type travel as the reserved headers
/// [`ID`](Self::ID), [`KIND`](Self::KIND) and
/// [`CONTENT_TYPE`](Self::CONTENT_TYPE), ahead of the metadata. Metadata may
/// not name a reserved header in any letter case, so it can never override
/// one; [`encode`](Self::encode) refuses such a message rather than dropping
/// the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireEnvelope {
    /// The message name.
    pub name: String,
    /// Reserved headers first, then the metadata in order.
    pub headers: Vec<(String, String)>,
    /// The payload bytes.
    pub payload: Vec<u8>,
}

impl WireEnvelope {
    /// Header carrying [`Message::id`]. Absent when the message has no id.
    pub const ID: &'static str = "x-distributed-id";
    /// Header carrying [`Message::kind`] as `command` or `event`. A delivery
    /// without it is an event.
    pub const KIND: &'static str = "x-distributed-kind";
    /// Header carrying [`Message::content_type`].
    pub const CONTENT_TYPE: &'static str = "x-distributed-content-type";
    /// Every reserved header.
    pub const RESERVED: [&'static str; 3] = [Self::ID, Self::KIND, Self::CONTENT_TYPE];

    /// Whether `key` names a reserved header (ASCII case-insensitive).
    pub fn is_reserved(key: &str) -> bool {
        Self::RESERVED
            .iter()
            .any(|reserved| key.eq_ignore_ascii_case(reserved))
    }

    /// Check that `message` can travel on any transport: a valid name, a
    /// stable id when it has one, a content type, and metadata keys that are
    /// non-empty and not reserved. Every failure is permanent.
    pub fn validate(message: &Message) -> Result<(), TransportError> {
        check_name(&message.name)?;
        if let Some(id) = message.id() {
            check_id(id)?;
        }
        check_content_type(&message.content_type)?;
        for (key, _) in &message.metadata {
            if key.is_empty() {
                return Err(TransportError::permanent("message metadata key is empty"));
            }
            if Self::is_reserved(key) {
                return Err(TransportError::permanent(format!(
                    "message metadata key {key:?} is a reserved wire header"
                )));
            }
        }
        Ok(())
    }

    /// Validate `message` and lay it out for the wire.
    pub fn encode(message: Message) -> Result<Self, TransportError> {
        Self::validate(&message)?;
        let mut headers = Vec::with_capacity(message.metadata.len() + 3);
        if let Some(id) = message.id {
            headers.push((Self::ID.to_string(), id));
        }
        headers.push((Self::KIND.to_string(), message.kind.as_str().to_string()));
        headers.push((Self::CONTENT_TYPE.to_string(), message.content_type));
        headers.extend(message.metadata);
        Ok(Self {
            name: message.name,
            headers,
            payload: message.payload,
        })
    }

    /// Read the message back.
    ///
    /// A reserved header given twice (in any letter case), an unknown kind, an
    /// unusable id or name, or an empty content type is a permanent error.
    /// The error comes with the delivery as received, every header as
    /// metadata, so a source can still settle it and a dead letter keeps what
    /// arrived.
    #[must_use = "a decode error must be settled, not dispatched"]
    pub fn decode(self) -> (Message, Option<TransportError>) {
        match self.check() {
            Ok(()) => (self.into_message(), None),
            Err(error) => {
                let mut raw = Message::new(self.name, MessageKind::Event, self.payload);
                raw.metadata = self.headers;
                (raw, Some(error))
            }
        }
    }

    fn check(&self) -> Result<(), TransportError> {
        check_name(&self.name)?;
        for reserved in Self::RESERVED {
            let mut values = self
                .headers
                .iter()
                .filter(|(key, _)| key.eq_ignore_ascii_case(reserved))
                .map(|(_, value)| value.as_str());
            let Some(value) = values.next() else {
                continue;
            };
            if values.next().is_some() {
                return Err(TransportError::permanent(format!(
                    "wire header {reserved} is given more than once"
                )));
            }
            match reserved {
                Self::ID => check_id(value)?,
                Self::KIND => {
                    parse_kind(value)?;
                }
                _ => check_content_type(value)?,
            }
        }
        Ok(())
    }

    /// Build the message from headers [`check`](Self::check) accepted.
    fn into_message(self) -> Message {
        let mut message = Message::new(self.name, MessageKind::Event, self.payload);
        for (key, value) in self.headers {
            if key.eq_ignore_ascii_case(Self::ID) {
                message.id = Some(value);
            } else if key.eq_ignore_ascii_case(Self::KIND) {
                message.kind = parse_kind(&value).unwrap_or(MessageKind::Event);
            } else if key.eq_ignore_ascii_case(Self::CONTENT_TYPE) {
                message.content_type = value;
            } else {
                message.metadata.push((key, value));
            }
        }
        message
    }
}

fn check_name(name: &str) -> Result<(), TransportError> {
    validate_message_name(name)
        .map(|_| ())
        .map_err(|error| TransportError::permanent(format!("invalid message name: {error}")))
}

fn check_id(id: &str) -> Result<(), TransportError> {
    validate_stable_message_id(Some(id))
        .map(|_| ())
        .map_err(|error| TransportError::permanent(format!("invalid message id: {error}")))
}

fn check_content_type(content_type: &str) -> Result<(), TransportError> {
    if content_type.trim().is_empty() || content_type.chars().any(char::is_control) {
        return Err(TransportError::permanent(format!(
            "invalid message content type {content_type:?}"
        )));
    }
    Ok(())
}

fn parse_kind(value: &str) -> Result<MessageKind, TransportError> {
    match value {
        "command" => Ok(MessageKind::Command),
        "event" => Ok(MessageKind::Event),
        other => Err(TransportError::permanent(format!(
            "wire header {} has unknown kind {other:?}",
            WireEnvelope::KIND
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> Message {
        let mut message = Message::new("order.placed", MessageKind::Command, b"\x00bin".to_vec())
            .with_id("order-1")
            .with_metadata("correlation_id", "corr-1")
            .with_metadata("traceparent", "00-trace");
        message.content_type = "application/octet-stream".into();
        message
    }

    fn assert_same(left: &Message, right: &Message) {
        assert_eq!(left.id, right.id);
        assert_eq!(left.name, right.name);
        assert_eq!(left.kind, right.kind);
        assert_eq!(left.payload, right.payload);
        assert_eq!(left.content_type, right.content_type);
        assert_eq!(left.metadata, right.metadata);
    }

    #[test]
    fn encode_then_decode_round_trips_every_field() {
        let original = message();
        let envelope = WireEnvelope::encode(original.clone()).unwrap();
        assert_eq!(
            envelope.headers[..3],
            [
                ("x-distributed-id".to_string(), "order-1".to_string()),
                ("x-distributed-kind".to_string(), "command".to_string()),
                (
                    "x-distributed-content-type".to_string(),
                    "application/octet-stream".to_string()
                ),
            ]
        );
        let (decoded, error) = envelope.decode();
        assert!(error.is_none());
        assert_same(&decoded, &original);
    }

    #[test]
    fn a_message_without_an_id_round_trips_without_one() {
        let original = Message::new("order.placed", MessageKind::Event, Vec::new());
        let (decoded, error) = WireEnvelope::encode(original.clone()).unwrap().decode();
        assert!(error.is_none());
        assert_same(&decoded, &original);
    }

    #[test]
    fn metadata_naming_a_reserved_header_in_any_case_is_refused() {
        for key in [
            "x-distributed-id",
            "X-Distributed-Kind",
            "X-DISTRIBUTED-CONTENT-TYPE",
        ] {
            let error = WireEnvelope::encode(message().with_metadata(key, "forged")).unwrap_err();
            assert!(error.is_permanent(), "{key}");
            assert!(error.to_string().contains("reserved"), "{key}: {error}");
        }
    }

    #[test]
    fn other_distributed_prefixed_metadata_travels() {
        let original = message().with_metadata("x-distributed-payload-codec", "bytes");
        let (decoded, error) = WireEnvelope::encode(original.clone()).unwrap().decode();
        assert!(error.is_none());
        assert_same(&decoded, &original);
    }

    #[test]
    fn invalid_names_ids_and_content_types_are_refused() {
        let mut bad_name = message();
        bad_name.name = "orders.*".into();
        let mut bad_content_type = message();
        bad_content_type.content_type = String::new();
        for invalid in [
            bad_name,
            message().with_id("line\nbreak"),
            message().with_id(" "),
            bad_content_type,
            message().with_metadata("", "empty key"),
        ] {
            assert!(WireEnvelope::encode(invalid).unwrap_err().is_permanent());
        }
    }

    #[test]
    fn decode_reads_reserved_headers_in_any_case() {
        let (decoded, error) = WireEnvelope {
            name: "order.placed".into(),
            headers: vec![
                ("X-Distributed-Id".into(), "order-1".into()),
                ("X-DISTRIBUTED-KIND".into(), "command".into()),
                ("x-correlation-id".into(), "corr-1".into()),
            ],
            payload: Vec::new(),
        }
        .decode();
        assert!(error.is_none());
        assert_eq!(decoded.id(), Some("order-1"));
        assert_eq!(decoded.kind, MessageKind::Command);
        assert_eq!(decoded.content_type, "application/json");
        assert_eq!(
            decoded.metadata,
            vec![("x-correlation-id".to_string(), "corr-1".to_string())]
        );
    }

    #[test]
    fn decode_without_a_kind_is_an_event() {
        let (decoded, error) = WireEnvelope {
            name: "order.placed".into(),
            headers: Vec::new(),
            payload: Vec::new(),
        }
        .decode();
        assert!(error.is_none());
        assert_eq!(decoded.kind, MessageKind::Event);
        assert_eq!(decoded.id(), None);
    }

    #[test]
    fn decode_refuses_ambiguous_or_invalid_reserved_headers_and_keeps_the_delivery() {
        for headers in [
            vec![
                ("x-distributed-id".to_string(), "a".to_string()),
                ("X-Distributed-Id".to_string(), "b".to_string()),
            ],
            vec![("x-distributed-kind".to_string(), "Command".to_string())],
            vec![("x-distributed-id".to_string(), String::new())],
            vec![("x-distributed-content-type".to_string(), " ".to_string())],
        ] {
            let (raw, error) = WireEnvelope {
                name: "order.placed".into(),
                headers: headers.clone(),
                payload: b"kept".to_vec(),
            }
            .decode();
            assert!(error.expect("refused").is_permanent(), "{headers:?}");
            assert_eq!(raw.id(), None);
            assert_eq!(raw.payload(), b"kept");
            assert_eq!(raw.metadata, headers);
        }
    }

    #[test]
    fn decode_refuses_an_invalid_name() {
        let (_, error) = WireEnvelope {
            name: "orders.>".into(),
            headers: Vec::new(),
            payload: Vec::new(),
        }
        .decode();
        assert!(error.expect("refused").is_permanent());
    }
}
