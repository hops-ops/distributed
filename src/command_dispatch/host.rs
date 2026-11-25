//! The command host boundary, and its in-process host.

use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

use super::VerifiedPrincipal;
use crate::microsvc::{CausalDispatchError, CausalDispatchResult, Service, Session};

/// The GraphQL request authority a host seals the modeled projection
/// metadata of a command's domain events with.
///
/// Only the GraphQL transport makes one. Every other caller passes `None`,
/// and the command commits without modeled projection metadata.
#[derive(Clone)]
pub struct ProtocolAuthority {
    #[cfg(feature = "graphql")]
    pub(crate) protocol: crate::graphql::protocol::ProtocolResponseAccumulator,
    _private: (),
}

#[cfg(feature = "graphql")]
impl ProtocolAuthority {
    pub(crate) fn new(protocol: crate::graphql::protocol::ProtocolResponseAccumulator) -> Self {
        Self {
            protocol,
            _private: (),
        }
    }
}

/// Where a typed command runs: in process, over HTTP, or in a celld cell.
///
/// A host only runs commands. Command status is answered by the service that
/// owns the command ledger ([`ledger`](Self::ledger)), because only that
/// process holds both the ledger row and the GraphQL request's protocol
/// authority status is evaluated with.
#[async_trait]
pub trait CommandHost: Send + Sync {
    async fn invoke(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        authority: Option<ProtocolAuthority>,
    ) -> Result<CausalDispatchResult, CausalDispatchError>;

    /// The in-process service whose command ledger records the commands
    /// this host runs, if there is one.
    fn ledger(&self) -> Option<&Arc<Service>> {
        None
    }
}

pub type SharedCommandHost = Arc<dyn CommandHost>;

pub(crate) fn validate_principal_session(
    session: &Session,
    principal: &VerifiedPrincipal,
) -> Result<(), CausalDispatchError> {
    let subject = session
        .user_id()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| CausalDispatchError::Rejected {
            code: "UNAUTHORIZED",
            status: 401,
            message: "durable commands require an authenticated session subject".into(),
        })?;
    if !principal.subject_matches(subject) {
        return Err(CausalDispatchError::Rejected {
            code: "UNAUTHORIZED",
            status: 401,
            message: "session subject does not match the verified principal".into(),
        });
    }
    Ok(())
}

#[cfg(feature = "graphql")]
pub(crate) fn validate_principal_session_if_present(
    session: &Session,
    principal: &VerifiedPrincipal,
) -> Result<(), CausalDispatchError> {
    match session
        .user_id()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(subject) if principal.subject_matches(subject) => Ok(()),
        Some(_) => Err(CausalDispatchError::Rejected {
            code: "UNAUTHORIZED",
            status: 401,
            message: "session subject does not match the verified principal".into(),
        }),
        None => Ok(()),
    }
}

/// In-process host wrapping a writer [`Service`].
pub struct LocalCommandHost {
    service: Arc<Service>,
}

impl LocalCommandHost {
    pub fn new(service: Arc<Service>) -> Self {
        Self { service }
    }

    pub fn service(&self) -> &Arc<Service> {
        &self.service
    }
}

#[async_trait]
impl CommandHost for LocalCommandHost {
    async fn invoke(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        authority: Option<ProtocolAuthority>,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        validate_principal_session(&session, &principal)?;
        match authority {
            #[cfg(feature = "graphql")]
            Some(authority) => {
                self.service
                    .dispatch_causal_with_receipt_and_protocol(
                        command,
                        command_id,
                        input,
                        session,
                        principal,
                        authority.protocol,
                    )
                    .await
            }
            _ => {
                self.service
                    .dispatch_causal_with_receipt(command, command_id, input, session, principal)
                    .await
            }
        }
    }

    fn ledger(&self) -> Option<&Arc<Service>> {
        Some(&self.service)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::microsvc::USER_ID_KEY;

    #[test]
    fn verified_principal_must_match_any_session_subject() {
        let principal =
            VerifiedPrincipal::test_oidc("https://issuer.example", "alice", &["distributed-tests"]);
        let mut session = Session::new();
        session.set(USER_ID_KEY, "mallory");
        assert!(validate_principal_session(&session, &principal).is_err());
        #[cfg(feature = "graphql")]
        assert!(validate_principal_session_if_present(&session, &principal).is_err());

        session.set(USER_ID_KEY, "alice");
        assert!(validate_principal_session(&session, &principal).is_ok());
    }
}
