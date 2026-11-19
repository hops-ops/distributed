//! Shared wait-path envelope for HTTP and gRPC command ingress.
//!
//! `{ commandId, input }` selects causal invoke. Identity comes from the
//! trusted transport (headers/metadata), never from the JSON body.

use serde::Deserialize;
use serde_json::{json, Value};

use super::service::{CausalDispatchError, CausalDispatchResult, Service};
use super::session::Session;
use crate::graphql::identity::VerifiedPrincipal;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WaitPathBody {
    command_id: String,
    #[serde(default)]
    input: Value,
}

/// Parse a wait-path body. Identity-shaped JSON fields are rejected rather than
/// ignored so a caller cannot depend on ambiguous authorization behavior.
pub(crate) fn parse_wait_path_body(value: &Value) -> Result<Option<(String, Value)>, &'static str> {
    if value.get("commandId").is_none() {
        return Ok(None);
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
    Ok(Some((parsed.command_id, input)))
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
        assert!(parse_wait_path_body(&json!({ "title": "legacy" }))
            .expect("legacy")
            .is_none());
        let parsed = parse_wait_path_body(&json!({
            "commandId": "command-1",
            "input": { "title": "safe" }
        }))
        .expect("valid")
        .expect("wait path");
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
