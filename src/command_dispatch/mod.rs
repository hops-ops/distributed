//! The command host: the one boundary every ingress (GraphQL, the bus,
//! Knative, the schedule worker, cell callers) dispatches typed commands
//! through, in process or over HTTP.

mod host;
#[cfg(feature = "http")]
mod http;
pub(crate) mod principal;

#[cfg(feature = "http")]
pub(crate) use host::validate_principal_session;
#[cfg(feature = "graphql")]
pub(crate) use host::validate_principal_session_if_present;
pub use host::{CommandHost, ProtocolAuthority, SharedCommandHost};
#[cfg(feature = "http")]
pub use http::{CellRequestContext, HttpCommandHost, TrustedRequestMetadata};
pub use principal::{InvalidPrincipal, VerifiedPrincipal};

/// The command ID of work an ingress starts from a request that can be
/// delivered again (a provider webhook): one sequence of `parts` always
/// yields one ID, so the ledger replays a redelivery instead of running it.
///
/// `parts` must name the request and nothing that changes between
/// deliveries of it; start them with a tag of the ingress so two sources
/// cannot derive the same ID. The ledger scopes the ID to the service and the
/// principal. A dispatch nothing can repeat takes a fresh UUIDv7 instead.
pub fn deterministic_command_id(parts: &[&str]) -> String {
    use sha2::Digest;
    let mut key = sha2::Sha256::new();
    key.update(b"distributed.ingress-command");
    for part in parts {
        key.update((part.len() as u64).to_be_bytes());
        key.update(part.as_bytes());
    }
    crate::command_ledger::deterministic_command_id(key)
}
