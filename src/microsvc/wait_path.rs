//! Shared wait-path envelope for HTTP and gRPC command ingress.
//!
//! `{ commandId, input }` selects causal invoke. Identity comes from the
//! trusted transport (headers/metadata), never from the JSON body.

use serde::Deserialize;
use serde_json::{json, Value};

use super::service::{CausalDispatchError, CausalDispatchResult, Service};
use super::session::Session;
use crate::command_dispatch::VerifiedPrincipal;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WaitPathBody {
    command_id: String,
    #[serde(default)]
    input: Value,
}

/// Parse a wait-path body into its command ID and input. Every command
/// carries a command ID; identity-shaped JSON fields are rejected rather than
/// ignored so a caller cannot depend on ambiguous authorization behavior.
pub(crate) fn parse_wait_path_body(value: &Value) -> Result<(String, Value), &'static str> {
    if value.get("commandId").is_none() {
        return Err("a command body is `{ \"commandId\", \"input\" }`: commandId is missing");
    }
    let parsed: WaitPathBody =
        serde_json::from_value(value.clone()).map_err(|_| "invalid wait-path envelope")?;
    if parsed.command_id.trim().is_empty()
        || parsed.command_id.len() > 512
        || parsed.command_id.chars().any(char::is_control)
    {
        return Err("invalid wait-path commandId");
    }
    let input = if parsed.input.is_null() {
        json!({})
    } else {
        parsed.input
    };
    Ok((parsed.command_id, input))
}

pub(crate) fn wait_path_response(result: &CausalDispatchResult) -> Value {
    json!({
        "payload": result.payload(),
        "receipt": {
            "commandId": result.command_id(),
            "causationId": result.causation_id(),
            "state": result.state(),
        }
    })
}

/// The body of a failed wait-path response. `code` is what a remote host
/// rebuilds the error from ([`wait_path_error`]); `error` is the client-safe
/// message.
pub(crate) fn wait_path_error_response(error: &CausalDispatchError) -> Value {
    json!({
        "code": error.code(),
        "error": error.client_message(),
    })
}

/// Rebuild the error of a failed wait-path response from its HTTP status and
/// body, as written by [`wait_path_error_response`] or an aggregate cell.
///
/// A server error that names no known code (a proxy, a crashed host) is
/// [`CausalDispatchError::Internal`], which callers retry; only a client
/// error is a rejection.
pub(crate) fn wait_path_error(status: u16, body: &Value) -> CausalDispatchError {
    let message = body
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("wait-path rejected")
        .to_string();
    let code = body.get("code").and_then(Value::as_str);
    let rejected = |code, message| CausalDispatchError::Rejected {
        code,
        status,
        message,
    };
    match code {
        Some("BAD_REQUEST") => CausalDispatchError::BadRequest(message),
        Some("FORBIDDEN") => CausalDispatchError::Forbidden,
        Some("COMMAND_ID_REUSE") => CausalDispatchError::CommandIdReuse,
        Some("COMMAND_IN_PROGRESS") => CausalDispatchError::InProgress,
        Some("COMMAND_EXPIRED") => CausalDispatchError::Expired,
        Some("CONCURRENT_WRITE") => CausalDispatchError::ConcurrentWrite,
        Some("UNAUTHORIZED") => rejected("UNAUTHORIZED", message),
        Some("NOT_FOUND") => rejected("NOT_FOUND", message),
        Some("REJECTED") => rejected("REJECTED", message),
        _ if status >= 500 || code == Some("INTERNAL") => {
            CausalDispatchError::Internal(format!("wait-path failed with HTTP {status}: {message}"))
        }
        _ => rejected("REJECTED", message),
    }
}

pub(crate) async fn dispatch_wait_path(
    service: &Service,
    command: &str,
    command_id: &str,
    input: Value,
    session: Session,
) -> Result<CausalDispatchResult, CausalDispatchError> {
    let principal = session
        .user_id()
        .and_then(|subject| VerifiedPrincipal::internal(subject).ok())
        .ok_or_else(|| CausalDispatchError::Rejected {
            code: "UNAUTHORIZED",
            status: 401,
            message: "durable commands require a verified transport identity".into(),
        })?;
    service
        .dispatch_causal_with_receipt(command, command_id, input, session, principal)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_envelope_does_not_fall_back_or_accept_identity_smuggling() {
        let bare = parse_wait_path_body(&json!({ "title": "no envelope" })).unwrap_err();
        assert!(bare.contains("commandId is missing"), "{bare}");
        let parsed = parse_wait_path_body(&json!({
            "commandId": "command-1",
            "input": { "title": "safe" }
        }))
        .expect("valid");
        assert_eq!(parsed.0, "command-1");
        assert_eq!(parsed.1, json!({ "title": "safe" }));

        assert!(parse_wait_path_body(&json!({
            "commandId": "command-1",
            "input": {},
            "roles": ["admin"]
        }))
        .is_err());
        assert!(parse_wait_path_body(&json!({ "commandId": " " })).is_err());
    }
}
