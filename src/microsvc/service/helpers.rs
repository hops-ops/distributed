use std::collections::HashMap;

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::causal::CausalDispatchError;
use super::routes::HandlerSpec;
use crate::bus::{Message, MessageKind};
use crate::microsvc::error::HandlerError;
use crate::microsvc::session::{
    Session, HOST_CLAIM_PREFIX, ROLE_KEY, TRANSPORT_MESSAGE_ID_CLAIM, TRANSPORT_ROLE,
    TRANSPORT_USER_ID, USER_ID_KEY,
};

pub(super) fn names_by_kind(specs: &[HandlerSpec], kind: MessageKind) -> Vec<&str> {
    let mut names = Vec::new();

    for spec in specs.iter().filter(|spec| spec.kind == kind) {
        for name in spec.names() {
            if !names.contains(&name) {
                names.push(name);
            }
        }
    }

    names
}

/// Whether a content type declares a JSON payload (`application/json` or any
/// `+json` structured suffix), ignoring parameters like `;charset=utf-8`.
pub(super) fn is_json_content_type(content_type: &str) -> bool {
    let essence = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase();
    essence == "application/json" || essence.ends_with("+json")
}

#[cfg(feature = "otel")]
pub(super) fn microsvc_dispatch_span(message: &Message) -> tracing::Span {
    crate::telemetry::microsvc_dispatch_span(message)
}

#[cfg(feature = "otel")]
pub(super) fn microsvc_handler_span(message: &Message) -> tracing::Span {
    crate::telemetry::microsvc_handler_span(message)
}

pub(super) fn message_to_json_input(message: &Message) -> Result<Value, HandlerError> {
    serde_json::from_slice::<Value>(&message.payload).map_err(|e| {
        HandlerError::DecodeFailed(format!(
            "invalid JSON payload for message '{}': {}",
            message.name, e
        ))
    })
}

pub(super) fn message_to_session(message: &Message) -> Session {
    let vars: HashMap<String, String> = message
        .metadata
        .iter()
        .map(|(key, value)| (key.to_ascii_lowercase(), value.clone()))
        .collect();
    Session::from_map(vars)
}

/// The session a transport-delivered typed command runs with.
///
/// The message's metadata reaches the handler as claims (trace context,
/// correlation, saga headers), except what only the host may say: the user
/// id, the roles and every `x-distributed-*` claim a message carries are
/// dropped, then the transport identity and the message id are set. Keys are
/// lower-cased first, so no spelling of an identity key survives.
pub(super) fn transport_session(message: &Message, message_id: &str) -> Session {
    let mut session = Session::from_map(
        message
            .metadata
            .iter()
            .map(|(key, value)| (key.to_ascii_lowercase(), value.clone()))
            .filter(|(key, _)| !key.starts_with(HOST_CLAIM_PREFIX))
            .collect(),
    );
    session.set(USER_ID_KEY, TRANSPORT_USER_ID);
    session.set(ROLE_KEY, TRANSPORT_ROLE);
    session.set(TRANSPORT_MESSAGE_ID_CLAIM, message_id);
    session
}

/// The ledger command ID of one transport message delivered to one command
/// of one service. A redelivery derives the same ID, so the ledger replays
/// it; the service and command are part of the key, so one message ID used
/// for two commands (or by two services sharing a store) is two commands.
pub(super) fn transport_command_id(service_id: &str, command: &str, message_id: &str) -> String {
    let mut key = Sha256::new();
    key.update(b"distributed.transport-command");
    for part in [service_id, command, message_id] {
        key.update((part.len() as u64).to_be_bytes());
        key.update(part.as_bytes());
    }
    crate::command_ledger::deterministic_command_id(key)
}

/// What the transport does with a typed command that did not commit, in the
/// handler-error vocabulary every consumer already classifies: retry what a
/// later delivery can change, refuse permanently what it cannot.
pub(super) fn transport_command_error(error: CausalDispatchError) -> HandlerError {
    match error {
        // Raised before the ledger (lifecycle gate, missing principal):
        // already a handler error with its own classification.
        CausalDispatchError::Handler(error) => error,
        CausalDispatchError::BadRequest(message) => HandlerError::DecodeFailed(message),
        CausalDispatchError::Unauthorized | CausalDispatchError::Forbidden => {
            HandlerError::Unauthorized(error.client_message())
        }
        CausalDispatchError::Rejected { status, .. } if status < 500 => {
            HandlerError::Rejected(error.client_message())
        }
        CausalDispatchError::CommandIdReuse | CausalDispatchError::Expired => {
            HandlerError::Rejected(error.client_message())
        }
        // Another delivery holds the attempt, nothing was committed, or the
        // host failed: a redelivery replays the receipt or runs the command.
        CausalDispatchError::InProgress
        | CausalDispatchError::ConcurrentWrite
        | CausalDispatchError::Internal(_)
        | CausalDispatchError::Rejected { .. } => HandlerError::Other(Box::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::TransportError;
    use crate::command_ledger::CommandId;

    #[test]
    fn transport_command_ids_are_stable_ledger_ids_scoped_to_service_and_command() {
        let id = transport_command_id("billing", "invoice.pay", "example.saga:s-1:pay:action:1");
        assert_eq!(
            id,
            transport_command_id("billing", "invoice.pay", "example.saga:s-1:pay:action:1")
        );
        assert_eq!(CommandId::parse(&id).unwrap().as_str(), id);
        for other in [
            transport_command_id("ledger", "invoice.pay", "example.saga:s-1:pay:action:1"),
            transport_command_id("billing", "invoice.void", "example.saga:s-1:pay:action:1"),
            transport_command_id("billing", "invoice.pay", "example.saga:s-1:pay:action:2"),
            // Parts are length-prefixed: moving a boundary is another key.
            transport_command_id("billing", "invoice.payexample", ".saga:s-1:pay:action:1"),
        ] {
            assert_ne!(id, other);
        }
    }

    #[test]
    fn transport_session_keeps_claims_and_replaces_identity() {
        let message = Message::new("invoice.pay", MessageKind::Command, Vec::new())
            .with_metadata("X-User-Id", "admin")
            .with_metadata("x-roles", "admin")
            .with_metadata("X-ROLES", "admin")
            .with_metadata("X-Distributed-Schedule", "tick")
            .with_metadata("x-distributed-message-id", "forged")
            .with_metadata("Saga-Id", "s-1")
            .with_metadata("traceparent", "00-trace");
        let session = transport_session(&message, "msg-1");
        assert_eq!(session.user_id(), Some(TRANSPORT_USER_ID));
        assert_eq!(session.roles(), vec![TRANSPORT_ROLE]);
        assert_eq!(session.get(TRANSPORT_MESSAGE_ID_CLAIM), Some("msg-1"));
        assert_eq!(session.get("saga-id"), Some("s-1"));
        assert_eq!(session.get("traceparent"), Some("00-trace"));
        assert_eq!(session.variables().len(), 5, "{session:?}");
        assert!(!crate::microsvc::schedule::is_schedule_session(&session));
    }

    #[test]
    fn transport_retries_what_a_redelivery_can_change_and_refuses_the_rest() {
        let rejected = |status| CausalDispatchError::Rejected {
            code: "REJECTED",
            status,
            message: "no".into(),
        };
        for retryable in [
            CausalDispatchError::InProgress,
            CausalDispatchError::ConcurrentWrite,
            CausalDispatchError::Internal("storage".into()),
            rejected(503),
            CausalDispatchError::Handler(HandlerError::Other("outage".into())),
        ] {
            let error = TransportError::from(transport_command_error(retryable));
            assert!(error.is_retryable(), "{error}");
            assert!(!error.should_retain_and_stop(), "{error}");
        }
        for permanent in [
            rejected(422),
            CausalDispatchError::BadRequest("shape".into()),
            CausalDispatchError::Forbidden,
            CausalDispatchError::Unauthorized,
            CausalDispatchError::CommandIdReuse,
            CausalDispatchError::Expired,
            CausalDispatchError::Handler(HandlerError::Unauthorized("no principal".into())),
        ] {
            let error = TransportError::from(transport_command_error(permanent));
            assert!(error.is_permanent(), "{error}");
        }
        // A closed lifecycle gate keeps its retain-and-stop meaning.
        let reloading = TransportError::from(transport_command_error(
            CausalDispatchError::Handler(HandlerError::ApplicationReloading),
        ));
        assert!(reloading.is_retryable() && reloading.should_retain_and_stop());
    }
}
