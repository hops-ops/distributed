//! Message-name validation for the canonical transport vocabulary.
//!
//! A [`Message.name`](super::Message) is the one transport identifier that flows,
//! unmodified, into broker routing primitives: it becomes the NATS subject
//! suffix, the Kafka topic, and the RabbitMQ routing/binding key. Unlike the
//! consumer group and namespace (validated in [`topology`](super::topology)), the
//! name had no rules at all — so a name carrying a broker wildcard or control
//! character could silently change routing.
//!
//! These rules mirror [`validate_stable_message_id`](super::validate_stable_message_id):
//! non-empty, length-capped, no control characters, and — the addition specific to
//! routing — no AMQP/NATS wildcard tokens.
//!
//! ## On `.` (the dot)
//!
//! `.` is **allowed**: dotted type names (`order.created`, `counter.incremented`)
//! are the established naming convention across this crate and form the routing
//! hierarchy brokers expect. Be aware that on AMQP/NATS topic routing the `.`
//! is a *segment separator*, so it changes routing granularity — a name is a
//! routing path, not an opaque label. Choose dotted names deliberately.
//!
//! ## Rejected wildcard tokens
//!
//! `*`, `#`, and `>` are rejected because they are pattern operators on the
//! brokers this crate targets. On a RabbitMQ topic **binding** key `*` (one
//! segment) and `#` (zero or more segments) are wildcards; on NATS `*` (one
//! token) and `>` (tail) are wildcards. A message *name* used as a publish
//! routing key with one of these would either mis-route or, if it ever reached a
//! binding, subscribe far more broadly than intended.

use std::error::Error;
use std::fmt;

/// Maximum length, in bytes, of a message name.
///
/// Bounds the size of a value that becomes a broker subject/topic/routing key.
/// Kafka topic names, NATS subjects, and AMQP routing keys all sit well under
/// this ceiling, and it matches the topology-name ceiling used for groups and
/// namespaces.
pub const MAX_MESSAGE_NAME_LEN: usize = 256;

/// Why a candidate message name is not usable as a transport identifier.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MessageNameError {
    /// The name was empty or only whitespace.
    Empty,
    /// The name exceeded [`MAX_MESSAGE_NAME_LEN`].
    TooLong {
        /// The actual length, in bytes.
        len: usize,
    },
    /// The name contained a control character (newline, NUL, etc.), which would
    /// corrupt subjects, routing keys, headers, or logs.
    ControlCharacter,
    /// The name contained a broker wildcard token (`*`, `#`, or `>`), which would
    /// change routing semantics.
    Wildcard {
        /// The offending wildcard character.
        ch: char,
    },
}

impl fmt::Display for MessageNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageNameError::Empty => write!(f, "message name is empty"),
            MessageNameError::TooLong { len } => write!(
                f,
                "message name is {len} bytes, exceeding the maximum of {MAX_MESSAGE_NAME_LEN}"
            ),
            MessageNameError::ControlCharacter => {
                write!(f, "message name contains a control character")
            }
            MessageNameError::Wildcard { ch } => write!(
                f,
                "message name contains broker wildcard `{ch}`; \
                 `*`, `#`, and `>` are reserved routing operators"
            ),
        }
    }
}

impl Error for MessageNameError {}

/// Validate a candidate message name for use as a transport routing identifier.
///
/// Returns the borrowed name when it is usable. Rejects empty/whitespace-only,
/// over-long, control-character-bearing, and wildcard-bearing names. `.` is
/// permitted (see the module docs); the value is returned unchanged so
/// callers route on exactly what was provided.
pub fn validate_message_name(name: &str) -> Result<&str, MessageNameError> {
    if name.trim().is_empty() {
        return Err(MessageNameError::Empty);
    }
    if name.len() > MAX_MESSAGE_NAME_LEN {
        return Err(MessageNameError::TooLong { len: name.len() });
    }
    for ch in name.chars() {
        if ch.is_control() {
            return Err(MessageNameError::ControlCharacter);
        }
        if matches!(ch, '*' | '#' | '>') {
            return Err(MessageNameError::Wildcard { ch });
        }
    }
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_dotted_type_names() {
        // Dotted names are the convention across the crate and must pass.
        assert_eq!(validate_message_name("order.created"), Ok("order.created"));
        assert_eq!(
            validate_message_name("counter.incremented"),
            Ok("counter.incremented")
        );
        assert_eq!(validate_message_name("work"), Ok("work"));
    }

    #[test]
    fn rejects_empty_or_whitespace() {
        assert_eq!(validate_message_name(""), Err(MessageNameError::Empty));
        assert_eq!(validate_message_name("   "), Err(MessageNameError::Empty));
    }

    #[test]
    fn rejects_over_long_names() {
        let name = "a".repeat(MAX_MESSAGE_NAME_LEN + 1);
        assert_eq!(
            validate_message_name(&name),
            Err(MessageNameError::TooLong {
                len: MAX_MESSAGE_NAME_LEN + 1
            })
        );
        let boundary = "a".repeat(MAX_MESSAGE_NAME_LEN);
        assert!(validate_message_name(&boundary).is_ok());
    }

    #[test]
    fn rejects_control_characters() {
        assert_eq!(
            validate_message_name("order\ncreated"),
            Err(MessageNameError::ControlCharacter)
        );
        assert_eq!(
            validate_message_name("order\u{0}created"),
            Err(MessageNameError::ControlCharacter)
        );
    }

    #[test]
    fn rejects_broker_wildcards() {
        // NATS: `*` (token) and `>` (tail). AMQP topic: `*` (segment) and `#`.
        assert_eq!(
            validate_message_name("order.*"),
            Err(MessageNameError::Wildcard { ch: '*' })
        );
        assert_eq!(
            validate_message_name("order.#"),
            Err(MessageNameError::Wildcard { ch: '#' })
        );
        assert_eq!(
            validate_message_name("orders>"),
            Err(MessageNameError::Wildcard { ch: '>' })
        );
    }
}
