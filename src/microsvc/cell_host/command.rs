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
    validate_principal_session, validate_principal_session_if_present, CellRequestContext,
    CommandHost, HttpCommandHost, LocalCommandHost, TrustedRequestMetadata,
};
use crate::command_ledger::ExternalDispatchBinding;
use crate::graphql::identity::VerifiedPrincipal;
use crate::graphql::protocol::ProtocolResponseAccumulator;
use crate::microsvc::{
    CausalCommandPublicStatus, CausalDispatchError, CausalDispatchResult,
    ExternalCausalReservation, Service, Session,
};

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

type CellShardFn = Arc<dyn Fn(&Value, &Session) -> Option<String> + Send + Sync>;
type CellPayloadFn = fn(command: &str, input: &Value, remote: &Value, session: &Session) -> Value;

/// One registered cell route: commands, cell kind, shard of (input, session).
#[derive(Clone)]
struct CellRoute {
    commands: Vec<String>,
    kind: String,
    shard: CellShardFn,
    payload: Option<CellPayloadFn>,
}

/// Routes selected commands to celld cells.
///
/// - [`new`](Self::new): the GraphQL wait-path host. A named local
///   [`Service`] reserves each routed command in its ledger (status queries,
///   projection sealing); unrouted commands run on that service.
/// - [`remote`](Self::remote): a remote-only host for trusted internal
///   callers (a zone server's fact sink): routed commands go straight to the
///   cell, whose own command ledger makes them idempotent; anything unrouted
///   is refused.
pub struct CelldCommandHost {
    http: HttpCommandHost,
    local: Option<LocalCommandHost>,
    service_id: Option<String>,
    routes: Vec<CellRoute>,
    forwarded_prefixes: Vec<String>,
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
            local: Some(LocalCommandHost::new(service)),
            service_id: None,
            routes: Vec::new(),
            forwarded_prefixes: Vec::new(),
        })
    }

    /// A remote-only host under command-ledger `service_id` (see the type
    /// docs), with a per-request `timeout`.
    pub fn remote(
        celld_url: impl Into<String>,
        service_id: impl Into<String>,
        internal_secret: InternalHttpSecret,
        timeout: std::time::Duration,
    ) -> Result<Self, CausalDispatchError> {
        let service_id = service_id.into();
        if service_id.trim().is_empty() {
            return Err(CausalDispatchError::Internal(
                "a remote celld command host needs a service id".into(),
            ));
        }
        let celld_url = celld_url.into().trim_end_matches('/').to_string();
        let http =
            HttpCommandHost::new_internal_with_timeout(&celld_url, internal_secret, timeout)?;
        Ok(Self {
            http,
            local: None,
            service_id: Some(service_id),
            routes: Vec::new(),
            forwarded_prefixes: Vec::new(),
        })
    }

    pub fn route(mut self, route: CelldRoute) -> Self {
        let shard = route.shard;
        self.routes.push(CellRoute {
            commands: route
                .commands
                .iter()
                .map(|command| command.to_string())
                .collect(),
            kind: route.kind.to_string(),
            shard: Arc::new(move |input, _| shard(input)),
            payload: Some(route.payload),
        });
        self
    }

    /// Route `command` to `{kind}/{shard(input, session)}/{command}` and
    /// return the cell's payload unchanged. A later route for the same
    /// command replaces the earlier.
    pub fn cell_route(
        mut self,
        command: impl Into<String>,
        kind: impl Into<String>,
        shard: impl Fn(&Value, &Session) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        self.add_cell_route(command, kind, shard);
        self
    }

    /// [`cell_route`](Self::cell_route) on a host being configured in place.
    pub fn add_cell_route(
        &mut self,
        command: impl Into<String>,
        kind: impl Into<String>,
        shard: impl Fn(&Value, &Session) -> Option<String> + Send + Sync + 'static,
    ) {
        let command = command.into();
        for route in &mut self.routes {
            route.commands.retain(|existing| existing != &command);
        }
        self.routes.retain(|route| !route.commands.is_empty());
        self.routes.push(CellRoute {
            commands: vec![command],
            kind: kind.into(),
            shard: Arc::new(shard),
            payload: None,
        });
    }

    /// Forward the session variables whose names start with `prefix` (for
    /// example a game fact's `x-game-*` stamp) to the cell as trusted
    /// metadata headers. Only for callers that build the session from
    /// already-trusted facts: public request sessions stay identity-only.
    /// Framework identity and transport headers can never be forwarded.
    pub fn forward_session_prefix(
        mut self,
        prefix: impl Into<String>,
    ) -> Result<Self, CausalDispatchError> {
        let prefix = prefix.into();
        // A probe header under the prefix must be a legal, non-reserved name.
        TrustedRequestMetadata::new().try_insert(&format!("{prefix}probe"), "probe")?;
        self.forwarded_prefixes.push(prefix);
        Ok(self)
    }

    fn route_for(&self, command: &str) -> Option<&CellRoute> {
        self.routes
            .iter()
            .find(|route| route.commands.iter().any(|existing| existing == command))
    }

    fn service_id(&self) -> Result<&str, CausalDispatchError> {
        if let Some(service_id) = &self.service_id {
            return Ok(service_id);
        }
        self.local
            .as_ref()
            .and_then(|local| local.service().name())
            .ok_or_else(|| {
                CausalDispatchError::Internal(
                    "celld command host requires a named executable service".into(),
                )
            })
    }

    fn forwarded_metadata(
        &self,
        session: &Session,
    ) -> Result<TrustedRequestMetadata, CausalDispatchError> {
        let mut metadata = TrustedRequestMetadata::new();
        if self.forwarded_prefixes.is_empty() {
            return Ok(metadata);
        }
        let mut names: Vec<&String> = session
            .variables()
            .keys()
            .filter(|name| {
                self.forwarded_prefixes
                    .iter()
                    .any(|prefix| name.starts_with(prefix.as_str()))
            })
            .collect();
        names.sort();
        for name in names {
            if let Some(value) = session.get(name) {
                metadata.try_insert(name, value)?;
            }
        }
        Ok(metadata)
    }

    /// Remote-only dispatch: straight to the cell, idempotent by its ledger.
    #[allow(clippy::too_many_arguments)]
    async fn invoke_remote(
        &self,
        route: &CellRoute,
        shard: &str,
        command: &str,
        command_id: &str,
        input: Value,
        session: &Session,
        principal: &VerifiedPrincipal,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        let service_id = self.service_id()?.to_string();
        let context =
            CellRequestContext::new(&service_id, principal.partition_for_service(&service_id))
                .with_trusted_metadata(self.forwarded_metadata(session)?);
        let http = self.http.retarget_segments(&[&route.kind, shard])?;
        let (status, body) = http
            .post_cell_wait_path_with_context(command, command_id, input.clone(), session, &context)
            .await?;
        if status >= 400 {
            return Err(remote_dispatch_error(status, &body));
        }
        let remote = CausalDispatchResult::from_wait_path_wire(body).map_err(|error| {
            CausalDispatchError::Internal(format!("wait-path decode: {error:?}"))
        })?;
        Ok(match route.payload {
            Some(payload) => {
                let mapped = payload(command, &input, remote.payload(), session);
                remote.with_payload(mapped)
            }
            None => remote,
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
        let Some(route) = self.route_for(command).cloned() else {
            return match &self.local {
                Some(local) => {
                    local
                        .invoke(command, command_id, input, session, principal, protocol)
                        .await
                }
                None => Err(CausalDispatchError::BadRequest(format!(
                    "no celld route for command `{command}`"
                ))),
            };
        };
        let shard = (route.shard)(&input, &session)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                CausalDispatchError::BadRequest(format!(
                    "{} id required for celld wait-path",
                    route.kind
                ))
            })?;
        let Some(local) = &self.local else {
            return self
                .invoke_remote(
                    &route, &shard, command, command_id, input, &session, &principal,
                )
                .await;
        };
        let service_id = self.service_id()?.to_string();
        let principal_partition = principal.partition_for_service(&service_id);
        let binding = ExternalDispatchBinding::new(&route.kind, &shard)
            .map_err(|error| CausalDispatchError::BadRequest(error.to_string()))?;
        let reservation = local
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
        let http = match self.http.retarget_segments(&[&route.kind, &shard]) {
            Ok(http) => http,
            Err(error) => {
                let _ = local
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
                let _ = local
                    .service()
                    .abandon_external_causal(command, attempt)
                    .await;
                return Err(error);
            }
        };
        if status >= 400 {
            let error = remote_dispatch_error(status, &body);
            let _ = local
                .service()
                .abandon_external_causal(command, attempt)
                .await;
            return Err(error);
        }
        let remote = match CausalDispatchResult::from_wait_path_wire(body) {
            Ok(remote) => remote,
            Err(error) => {
                let _ = local
                    .service()
                    .abandon_external_causal(command, attempt)
                    .await;
                return Err(CausalDispatchError::Internal(format!(
                    "wait-path decode: {error}"
                )));
            }
        };
        if let Err(error) = attempt.validate_remote(&remote) {
            let _ = local
                .service()
                .abandon_external_causal(command, attempt)
                .await;
            return Err(error);
        }
        let mut remote = match route.payload {
            Some(payload) => {
                let mapped = payload(command, &input, remote.payload(), &session);
                remote.with_payload(mapped)
            }
            None => remote,
        };
        if let Some(protocol) = protocol {
            remote = match local
                .service()
                .seal_wait_path_dispatch(command, &protocol, remote)
            {
                Ok(remote) => remote,
                Err(error) => {
                    let _ = local
                        .service()
                        .abandon_external_causal(command, attempt)
                        .await;
                    return Err(error);
                }
            };
        }
        local
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
        match &self.local {
            Some(local) => local.status(command_id, session, principal, protocol).await,
            None => Ok(CausalCommandPublicStatus::unknown(command_id)),
        }
    }
}
