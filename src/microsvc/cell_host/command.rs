//! GraphQL [`CommandHost`] for the celld wait-path.
//!
//! Aggregate crates supply a [`CelldRoute`] (kind, shard, payload map). The
//! aggregate Worker owns outbox delivery through celld Queue; this host only
//! invokes commands and seals the returned projection evidence.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use super::InternalHttpSecret;
use crate::command_dispatch::{
    validate_principal_session, validate_principal_session_if_present, CommandHost,
    HttpCommandHost, LocalCommandHost,
};
use crate::graphql::identity::VerifiedPrincipal;
use crate::graphql::protocol::ProtocolResponseAccumulator;
use crate::microsvc::{
    CausalCommandPublicStatus, CausalDispatchError, CausalDispatchResult, ExternalCausalReservation,
    Service, Session,
};
use crate::command_ledger::ExternalDispatchBinding;

/// One aggregate's cell wait-path: command names, URL kind, shard id, payload.
#[derive(Clone, Copy)]
pub struct CelldRoute {
    pub commands: &'static [&'static str],
    /// Path segment `{CELLD_URL}/{kind}/{shard}/{command}`.
    pub kind: &'static str,
    pub shard: fn(&Value) -> Option<String>,
    pub payload: fn(command: &str, input: &Value, remote: &Value, session: &Session) -> Value,
}

impl CelldRoute {
    pub const fn new(
        commands: &'static [&'static str],
        kind: &'static str,
        shard: fn(&Value) -> Option<String>,
        payload: fn(command: &str, input: &Value, remote: &Value, session: &Session) -> Value,
    ) -> Self {
        Self {
            commands,
            kind,
            shard,
            payload,
        }
    }
}

/// Routes selected commands to celld; everything else stays on [`LocalCommandHost`].
pub struct CelldCommandHost {
    http: HttpCommandHost,
    local: LocalCommandHost,
    routes: Vec<CelldRoute>,
}

impl CelldCommandHost {
    pub fn new(
        celld_url: impl Into<String>,
        service: Arc<Service>,
        internal_secret: InternalHttpSecret,
    ) -> Result<Self, CausalDispatchError> {
        let celld_url = celld_url.into().trim_end_matches('/').to_string();
        let http = HttpCommandHost::new_internal(&celld_url, internal_secret)?;
        Ok(Self {
            http,
            local: LocalCommandHost::new(service),
            routes: Vec::new(),
        })
    }

    pub fn route(mut self, route: CelldRoute) -> Self {
        self.routes.push(route);
        self
    }

    fn route_for(&self, command: &str) -> Option<&CelldRoute> {
        self.routes
            .iter()
            .find(|route| route.commands.contains(&command))
    }

    fn service_id(&self) -> Result<&str, CausalDispatchError> {
        self.local.service().name().ok_or_else(|| {
            CausalDispatchError::Internal(
                "celld command host requires a named executable service".into(),
            )
        })
    }

}

fn remote_dispatch_error(status: u16, body: &Value) -> CausalDispatchError {
    let message = body
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("wait-path rejected")
        .to_string();
    match body.get("code").and_then(Value::as_str) {
        Some("BAD_REQUEST") => CausalDispatchError::BadRequest(message),
        Some("FORBIDDEN") => CausalDispatchError::Forbidden,
        Some("COMMAND_ID_REUSE") => CausalDispatchError::CommandIdReuse,
        Some("COMMAND_IN_PROGRESS") => CausalDispatchError::InProgress,
        Some("COMMAND_EXPIRED") => CausalDispatchError::Expired,
        Some("INTERNAL") => {
            CausalDispatchError::Internal(format!("cell wait-path failed with HTTP {status}"))
        }
        Some("UNAUTHORIZED") => CausalDispatchError::Rejected {
            code: "UNAUTHORIZED",
            status,
            message,
        },
        Some("NOT_FOUND") => CausalDispatchError::Rejected {
            code: "NOT_FOUND",
            status,
            message,
        },
        _ => CausalDispatchError::Rejected {
            code: "REJECTED",
            status,
            message,
        },
    }
}

#[async_trait]
impl CommandHost for CelldCommandHost {
    async fn invoke(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        protocol: Option<ProtocolResponseAccumulator>,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        validate_principal_session(&session, &principal)?;
        let Some(route) = self.route_for(command).copied() else {
            return self
                .local
                .invoke(command, command_id, input, session, principal, protocol)
                .await;
        };
        let shard = (route.shard)(&input)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                CausalDispatchError::BadRequest(format!(
                    "{} id required for celld wait-path",
                    route.kind
                ))
            })?;
        let service_id = self.service_id()?.to_string();
        let principal_partition = principal.partition_for_service(&service_id);
        let binding = ExternalDispatchBinding::new(route.kind, &shard)
            .map_err(|error| CausalDispatchError::BadRequest(error.to_string()))?;
        let reservation = self
            .local
            .service()
            .reserve_external_causal(
                command,
                command_id,
                input.clone(),
                session.clone(),
                principal,
                binding,
            )
            .await?;
        let attempt = match reservation {
            ExternalCausalReservation::Acquired(attempt) => attempt,
            ExternalCausalReservation::Replay(replay) => return Ok(replay),
        };
        let http = match self.http.retarget_segments(&[route.kind, &shard]) {
            Ok(http) => http,
            Err(error) => {
                let _ = self
                    .local
                    .service()
                    .abandon_external_causal(command, attempt)
                    .await;
                return Err(error);
            }
        };
        let (status, body) = match http
            .post_cell_wait_path_with_causation(
                command,
                command_id,
                input.clone(),
                &session,
                &service_id,
                &principal_partition,
                Some(attempt.causation_id()),
            )
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let _ = self
                    .local
                    .service()
                    .abandon_external_causal(command, attempt)
                    .await;
                return Err(error);
            }
        };
        if status >= 400 {
            let error = remote_dispatch_error(status, &body);
            let _ = self
                .local
                .service()
                .abandon_external_causal(command, attempt)
                .await;
            return Err(error);
        }
        let remote = match CausalDispatchResult::from_wait_path_wire(body) {
            Ok(remote) => remote,
            Err(error) => {
                let _ = self
                    .local
                    .service()
                    .abandon_external_causal(command, attempt)
                    .await;
                return Err(CausalDispatchError::Internal(format!(
                    "wait-path decode: {error}"
                )));
            }
        };
        if let Err(error) = attempt.validate_remote(&remote) {
            let _ = self
                .local
                .service()
                .abandon_external_causal(command, attempt)
                .await;
            return Err(error);
        }
        let payload = (route.payload)(command, &input, remote.payload(), &session);
        let mut remote = remote.with_payload(payload);
        if let Some(protocol) = protocol {
            remote = match self
                .local
                .service()
                .seal_wait_path_dispatch(command, &protocol, remote)
            {
                Ok(remote) => remote,
                Err(error) => {
                    let _ = self
                        .local
                        .service()
                        .abandon_external_causal(command, attempt)
                        .await;
                    return Err(error);
                }
            };
        }
        self.local
            .service()
            .complete_external_causal(command, attempt, &remote)
            .await?;
        Ok(remote)
    }

    async fn status(
        &self,
        command_id: &str,
        session: &Session,
        principal: VerifiedPrincipal,
        protocol: Option<ProtocolResponseAccumulator>,
    ) -> Result<CausalCommandPublicStatus, CausalDispatchError> {
        validate_principal_session_if_present(session, &principal)?;
        self.local
            .status(command_id, session, principal, protocol)
            .await
    }
}
