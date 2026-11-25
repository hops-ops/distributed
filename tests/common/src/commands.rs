//! In-process typed command dispatch.

use std::sync::atomic::{AtomicU64, Ordering};

use distributed::command_dispatch::VerifiedPrincipal;
use distributed::microsvc::{CausalDispatchError, Service, Session};
use serde_json::Value;

/// Per-process sequence, so each call is a new command.
static NEXT: AtomicU64 = AtomicU64::new(1);

/// A command id no earlier call returned (the UUIDv7 shape the ledger
/// requires).
pub fn command_id() -> String {
    let next = NEXT.fetch_add(1, Ordering::Relaxed);
    format!("0190a000-0000-7000-8000-{next:012}")
}

/// Dispatch `command` once, as a trusted in-process caller acting for the
/// session's user. The service must be [`Service::named`].
///
/// Use [`dispatch_as`] to dispatch one command id twice.
pub async fn dispatch(
    service: &Service,
    command: &str,
    input: Value,
    session: Session,
) -> Result<Value, CausalDispatchError> {
    dispatch_as(service, command, &command_id(), input, session).await
}

/// [`dispatch`] with the caller's command id.
pub async fn dispatch_as(
    service: &Service,
    command: &str,
    command_id: &str,
    input: Value,
    session: Session,
) -> Result<Value, CausalDispatchError> {
    // The principal only selects the ledger partition; a command without
    // roles also admits a session with no user.
    let principal = VerifiedPrincipal::internal(session.user_id().unwrap_or("anonymous"))
        .expect("the subject is not empty");
    service
        .dispatch_causal(command, command_id, input, session, principal)
        .await
}
