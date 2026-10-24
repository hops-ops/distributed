//! [`CellFactSink`]: commit facts as idempotent commands into a
//! `distributed` [`AggregateCell`].
//!
//! For each fact the sink:
//! 1. takes the principal **only** from [`Fact::principal`] — bound by the
//!    host from the player's verified ticket — or a configured system
//!    principal for zone facts; client payloads never supply identity;
//! 2. builds a trusted server-side [`Session`] (`x-user-id` = principal, plus
//!    the [`FactStamp`](crate::FactStamp) as `x-game-*` variables so handlers
//!    can [`stamp_metadata`](crate::stamp_metadata) their events);
//! 3. derives a deterministic UUIDv7-shaped command id from
//!    `sha256(service_id ‖ 0x00 ‖ fact key)`, so retries and re-emissions of
//!    the same key replay through the cell's command ledger instead of
//!    committing twice;
//! 4. calls [`AggregateCell::dispatch_idempotent`].
//!
//! `InProgress` and `Internal` errors are retryable; everything else is
//! permanent.

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

use distributed::cell_host::{
    AggregateCell, CellBatchCommand, CellCommandIdentity, CellDispatchError, CellWaitPathRequest,
};
use distributed::microsvc::Session;
use distributed::Aggregate;
use distributed::USER_ID_KEY;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::facts::Fact;
#[cfg(not(target_arch = "wasm32"))]
use crate::facts::{BoxFuture, FactError, FactReceipt, FactSink};

/// Fact sink backed by an aggregate cell hosted in the zone server's own
/// process (native only: on wasm32 a cell is a Durable Object, reached over
/// the network with [`crate::celld::CelldFactSink`]).
#[cfg(not(target_arch = "wasm32"))]
pub struct CellFactSink<A>
where
    A: Aggregate + Send + Sync + 'static,
{
    cell: Arc<AggregateCell<A>>,
    service_id: String,
    system_principal: String,
}

#[cfg(not(target_arch = "wasm32"))]
impl<A> CellFactSink<A>
where
    A: Aggregate + Send + Sync + 'static,
{
    /// Sink dispatching into `cell` under command-ledger `service_id`.
    /// Zone facts without a player use `system_principal`.
    pub fn new(
        cell: Arc<AggregateCell<A>>,
        service_id: impl Into<String>,
        system_principal: impl Into<String>,
    ) -> Self {
        Self {
            cell,
            service_id: service_id.into(),
            system_principal: system_principal.into(),
        }
    }
}

/// Deterministic UUIDv7-shaped command id for a fact key.
pub fn command_id_for(service_id: &str, key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(service_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(key.as_bytes());
    let digest = hasher.finalize();
    let mut b = [0u8; 16];
    b.copy_from_slice(&digest[..16]);
    b[6] = (b[6] & 0x0f) | 0x70;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Trusted session for a fact: principal + stamp.
pub fn trusted_session(principal: &str, fact: &Fact) -> Session {
    let mut session = Session::new();
    session.set(USER_ID_KEY, principal);
    for (key, value) in fact.stamp().session_vars() {
        session.set(key, value);
    }
    session
}

/// The trusted session of a fact received by a celld cell over the internal
/// path ([`crate::celld::CelldFactSink`]): `x-user-id` plus the `x-game-*`
/// stamp headers. `header` reads one request header. Call it only after the
/// request authenticated with `DISTRIBUTED_INTERNAL_SECRET`; returns `None`
/// when the principal or a stamp field is missing (not a fact request).
pub fn fact_session_from_headers(header: impl Fn(&str) -> Option<String>) -> Option<Session> {
    let principal = header(USER_ID_KEY).filter(|value| !value.is_empty())?;
    let mut vars = std::collections::HashMap::new();
    for (name, _) in crate::FactStamp::default().session_vars() {
        vars.insert(name.clone(), header(&name)?);
    }
    let stamp = crate::FactStamp::from_session_vars(&vars)?;
    let mut session = Session::new();
    session.set(USER_ID_KEY, principal);
    for (key, value) in stamp.session_vars() {
        session.set(key, value);
    }
    Some(session)
}

/// The [`FactStamp`](crate::FactStamp) a [`trusted_session`] carries
/// (None for a session that did not come from a fact). Handlers use it to
/// order facts ([`FactStamp::emission_order`](crate::FactStamp::emission_order))
/// and to [`stamp_metadata`](crate::stamp_metadata) their events.
pub fn session_stamp(session: &Session) -> Option<crate::FactStamp> {
    crate::FactStamp::from_session_vars(session.variables())
}

/// Run a fact batch (`POST /{kind}/{shard}/__batch`, the celld object
/// host's batch request) against `cell`: `{ entries: [{ command,
/// commandId, input, partition, session }] }` → `{ results: [{ status,
/// body }] }`, each body as the entry's own wait-path response. Every entry
/// gets its own ledger id (`service` + `partition` + `commandId`) and its
/// session from its `session` variables through `sessions`, and runs in
/// order ([`AggregateCell::dispatch_idempotent_batch`]); the results end at
/// the first retryable one. Call it only after the request authenticated
/// with `DISTRIBUTED_INTERNAL_SECRET`.
pub async fn run_fact_batch<A>(
    cell: &AggregateCell<A>,
    service: &str,
    body: &Value,
    sessions: impl Fn(&dyn Fn(&str) -> Option<String>) -> Option<Session>,
) -> Result<Value, CellDispatchError>
where
    A: Aggregate + Send + Sync + 'static,
{
    let entries = body
        .get("entries")
        .and_then(Value::as_array)
        .ok_or_else(|| CellDispatchError::BadRequest("`entries` is required".into()))?;
    // An entry that does not parse never reaches the cell; it is answered
    // in its place.
    let mut refused = Vec::with_capacity(entries.len());
    let mut commands = Vec::with_capacity(entries.len());
    for entry in entries {
        match batch_command(service, entry, &sessions) {
            Ok(command) => {
                commands.push(command);
                refused.push(None);
            }
            Err(error) => refused.push(Some(error)),
        }
    }
    let mut dispatched = cell.dispatch_idempotent_batch(commands).await.into_iter();
    let mut results = Vec::with_capacity(entries.len());
    for refused in refused {
        let result = match refused {
            Some(error) => Err(error),
            None => match dispatched.next() {
                Some(result) => result.map(|dispatch| dispatch.wait_path_wire()),
                // Not attempted: an earlier entry ended the batch.
                None => break,
            },
        };
        let (status, body) = match result {
            Ok(body) => (200, body),
            Err(error) => (
                error.status_code(),
                json!({ "error": error.client_message(), "code": error.code() }),
            ),
        };
        let retryable = status >= 500
            || matches!(status, 408 | 429)
            || body.get("code").and_then(Value::as_str) == Some("COMMAND_IN_PROGRESS");
        results.push(json!({ "status": status, "body": body }));
        if retryable {
            break;
        }
    }
    Ok(json!({ "results": results }))
}

/// One `__batch` entry as its own wait-path command.
fn batch_command(
    service: &str,
    entry: &Value,
    sessions: &impl Fn(&dyn Fn(&str) -> Option<String>) -> Option<Session>,
) -> Result<CellBatchCommand, CellDispatchError> {
    let field = |name: &str| entry.get(name).and_then(Value::as_str);
    let (Some(command), Some(partition)) = (field("command"), field("partition")) else {
        return Err(CellDispatchError::BadRequest(
            "`command` and `partition` are required".into(),
        ));
    };
    let parsed = CellWaitPathRequest::parse(json!({
        "commandId": entry.get("commandId"),
        "input": entry.get("input"),
    }))
    .map_err(CellDispatchError::BadRequest)?;
    let identity = CellCommandIdentity::new(
        service.to_string(),
        partition.to_string(),
        &parsed.command_id,
    )?;
    let vars = entry.get("session").and_then(Value::as_object);
    let header = |name: &str| {
        vars.and_then(|vars| vars.get(name))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    let session = sessions(&header).ok_or(CellDispatchError::Forbidden)?;
    Ok(CellBatchCommand {
        command: command.to_string(),
        identity,
        input: parsed.input,
        session,
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn classify(error: CellDispatchError) -> FactError {
    match error {
        CellDispatchError::InProgress | CellDispatchError::Internal(_) => {
            FactError::Retryable(format!("{error:?}"))
        }
        other => FactError::Permanent(format!("{}: {other:?}", other.code())),
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<A> FactSink for CellFactSink<A>
where
    A: Aggregate + Send + Sync + 'static,
{
    fn commit<'a>(&'a self, fact: &'a Fact) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
        Box::pin(async move {
            let principal = fact.principal().unwrap_or(&self.system_principal);
            let identity = CellCommandIdentity::new(
                self.service_id.clone(),
                principal,
                command_id_for(&self.service_id, fact.key()),
            )
            .map_err(|e| FactError::Permanent(format!("{e:?}")))?;
            let result = self
                .cell
                .dispatch_idempotent(
                    fact.command(),
                    &identity,
                    fact.input().clone(),
                    trusted_session(principal, fact),
                )
                .await
                .map_err(classify)?;
            Ok(FactReceipt {
                payload: result.payload().clone(),
                replayed: result.replayed(),
            })
        })
    }
}
