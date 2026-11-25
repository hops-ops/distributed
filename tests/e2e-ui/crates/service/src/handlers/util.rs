//! Shared event-handler helpers.

use distributed::bus::Message;
use distributed::microsvc::HandlerError;
use distributed::{BitcodePayloadCodec, PayloadCodec};
use serde::de::DeserializeOwned;

/// Decode event payload as JSON (tests) or bitcode (outbox → bus).
pub fn decode_payload<T: DeserializeOwned>(message: &Message) -> Result<T, HandlerError> {
    let ct = message.content_type.as_str();
    if ct.contains("json") || looks_like_json(message.payload()) {
        return serde_json::from_slice(message.payload())
            .map_err(|e| HandlerError::DecodeFailed(format!("json payload: {e}")));
    }
    BitcodePayloadCodec::decode(message.payload())
        .map_err(|e| HandlerError::DecodeFailed(format!("bitcode payload: {e}")))
}

fn looks_like_json(bytes: &[u8]) -> bool {
    matches!(
        bytes.iter().find(|b| !b.is_ascii_whitespace()),
        Some(b'{' | b'[')
    )
}

pub fn read_model_error(e: impl std::fmt::Display) -> HandlerError {
    HandlerError::Other(Box::new(std::io::Error::other(e.to_string())))
}
