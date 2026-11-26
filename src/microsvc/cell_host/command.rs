//! [`CommandHost`] for the celld wait-path.
//!
//! [`CelldCommandHost::route_aggregate`] routes an aggregate's mounted
//! commands to its cells: the cell kind is the aggregate's
//! [`Aggregate::aggregate_type`], the shard each command's declared shard. The
//! cell's payload is the command's answer as-is; one that is not the declared
//! output fails the dispatch. The aggregate Worker owns outbox delivery
//! through celld Queue; this host only invokes commands and seals the
//! returned projection evidence.

use std::any::TypeId;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use super::InternalHttpSecret;
use crate::aggregate::Aggregate;
use crate::command_dispatch::VerifiedPrincipal;
use crate::command_dispatch::{
    validate_principal_session, CellRequestContext, CommandHost, HttpCommandHost, LocalCommandHost,
    ProtocolAuthority, TrustedRequestMetadata,
};
use crate::command_ledger::ExternalDispatchBinding;
use crate::microsvc::{
    CausalDispatchError, CausalDispatchResult, ExternalCausalReservation, Service, Session,
};

type CellShardFn =
    Arc<dyn Fn(&Value, &Session) -> Result<String, CausalDispatchError> + Send + Sync>;

/// One registered cell route: commands, cell kind, shard of (input, session).
#[derive(Clone)]
struct CellRoute {
    commands: Vec<String>,
    kind: String,
    shard: CellShardFn,
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

    /// Route every command of aggregate `A` mounted on this host's service
    /// to `{kind}/{shard}/{command}`: `kind` is `A::aggregate_type()`, `shard`
    /// the command's declared shard of its input (`shard:` in
    /// [`command!`](crate::command), or the id of `load_by` / `create_by`).
    ///
    /// The cell's payload is returned as-is, after it is checked against the
    /// command's declared output: a missing, undeclared or mistyped field
    /// fails the dispatch.
    ///
    /// Fails when this host has no local service ([`remote`](Self::remote)),
    /// when the service mounts no command of `A`, or when one of them declares
    /// no shard.
    pub fn route_aggregate<A: Aggregate + 'static>(mut self) -> Result<Self, CausalDispatchError> {
        let kind = A::aggregate_type();
        let local = self.local.as_ref().ok_or_else(|| {
            CausalDispatchError::Internal(format!(
                "route_aggregate::<{kind}> needs the host's local service; a remote host routes with cell_route"
            ))
        })?;
        let shards = local.service().aggregate_command_shards(TypeId::of::<A>());
        if shards.is_empty() {
            return Err(CausalDispatchError::Internal(format!(
                "the service mounts no command of aggregate `{kind}`"
            )));
        }
        for (command, shard) in shards {
            let shard = shard.ok_or_else(|| {
                CausalDispatchError::Internal(format!(
                    "command `{command}` of aggregate `{kind}` declares no shard, so it has no cell"
                ))
            })?;
            self.insert_route(
                command,
                kind.to_string(),
                Arc::new(move |input, _| shard(input)),
            );
        }
        Ok(self)
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
        let kind = kind.into();
        let missing = format!("{kind} id required for celld wait-path");
        self.insert_route(
            command.into(),
            kind,
            Arc::new(move |input, session| {
                shard(input, session)
                    .ok_or_else(|| CausalDispatchError::BadRequest(missing.clone()))
            }),
        );
    }

    /// A later route for the same command replaces the earlier.
    fn insert_route(&mut self, command: String, kind: String, shard: CellShardFn) {
        for route in &mut self.routes {
            route.commands.retain(|existing| existing != &command);
        }
        self.routes.retain(|route| !route.commands.is_empty());
        self.routes.push(CellRoute {
            commands: vec![command],
            kind,
            shard,
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
            .post_cell_wait_path_with_context(command, command_id, input, session, &context)
            .await?;
        if status >= 400 {
            return Err(crate::microsvc::wait_path::wait_path_error(status, &body));
        }
        CausalDispatchResult::from_wait_path_wire(body)
            .map_err(|error| CausalDispatchError::Internal(format!("wait-path decode: {error:?}")))
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
        authority: Option<ProtocolAuthority>,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        validate_principal_session(&session, &principal)?;
        let Some(route) = self.route_for(command).cloned() else {
            return match &self.local {
                Some(local) => {
                    local
                        .invoke(command, command_id, input, session, principal, authority)
                        .await
                }
                None => Err(CausalDispatchError::BadRequest(format!(
                    "no celld route for command `{command}`"
                ))),
            };
        };
        let shard = (route.shard)(&input, &session)?;
        if shard.is_empty() {
            return Err(CausalDispatchError::BadRequest(format!(
                "{} id required for celld wait-path",
                route.kind
            )));
        }
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
                input,
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
            let error = crate::microsvc::wait_path::wait_path_error(status, &body);
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
        #[cfg(feature = "graphql")]
        let remote = match authority {
            Some(authority) => {
                match local
                    .service()
                    .seal_wait_path_dispatch(command, &authority.protocol, remote)
                {
                    Ok(remote) => remote,
                    Err(error) => {
                        let _ = local
                            .service()
                            .abandon_external_causal(command, attempt)
                            .await;
                        return Err(error);
                    }
                }
            }
            None => remote,
        };
        local
            .service()
            .complete_external_causal(command, attempt, &remote)
            .await?;
        Ok(remote)
    }

    fn ledger(&self) -> Option<&Arc<Service>> {
        self.local.as_ref().map(LocalCommandHost::service)
    }
}
