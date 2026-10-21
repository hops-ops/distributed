//! Serving fact cells next to celld zones (wasm32): the internal wait-path
//! route every fact cell of a game worker needs, and the zone-activity cell.

use distributed::cell_host::{
    AggregateCell, CellCommandIdentity, CellDispatchError, CellWaitPathRequest, CelldOutbox,
    InternalHttpSecret, CELL_INTERNAL_SECRET_ENV, CELL_INTERNAL_SECRET_HEADER,
    CELL_PRINCIPAL_PARTITION_HEADER, CELL_SERVICE_ID_HEADER,
};
use distributed::microsvc::Session;
use distributed::Aggregate;
use serde_json::{json, Value};
use worker::{Env, Method, Request, Response, Result, State};

use crate::cell::fact_session_from_headers;
use crate::world::zone_activity::{mount_zone_activity, ZoneActivity};

/// Builds the handler session from trusted request headers (after the
/// internal secret was checked); `None` refuses the command.
pub type Sessions = fn(&dyn Fn(&str) -> Option<String>) -> Option<Session>;

/// Facts only: the principal and the `x-game-*` stamp
/// ([`fact_session_from_headers`]).
pub fn fact_sessions(header: &dyn Fn(&str) -> Option<String>) -> Option<Session> {
    fact_session_from_headers(header)
}

/// Check `x-distributed-internal-secret` against
/// `DISTRIBUTED_INTERNAL_SECRET`.
pub fn authenticate(req: &Request, env: &Env) -> std::result::Result<(), CellDispatchError> {
    let configured = env
        .secret(CELL_INTERNAL_SECRET_ENV)
        .map(|value| value.to_string())
        .or_else(|_| {
            env.var(CELL_INTERNAL_SECRET_ENV)
                .map(|value| value.to_string())
        })
        .map_err(|_| CellDispatchError::Internal("internal secret is required".into()))?;
    let secret = InternalHttpSecret::new(configured).map_err(CellDispatchError::Internal)?;
    let candidate = req
        .headers()
        .get(CELL_INTERNAL_SECRET_HEADER)
        .ok()
        .flatten()
        .ok_or(CellDispatchError::Unauthorized)?;
    if secret.matches(&candidate) {
        Ok(())
    } else {
        Err(CellDispatchError::Unauthorized)
    }
}

/// A JSON response with `status`.
pub fn json_status(body: Value, status: u16) -> Result<Response> {
    Ok(Response::from_json(&body)?.with_status(status))
}

/// The wire form of a dispatch error.
pub fn error_response(error: CellDispatchError) -> Result<Response> {
    json_status(
        json!({ "error": error.client_message(), "code": error.code() }),
        error.status_code(),
    )
}

/// One cell's internal routes: `GET /{kind}/{shard}` → `read(cell)`, `POST
/// /{kind}/{shard}/{command}` → the idempotent wait-path dispatch (identity
/// from the service-id and principal-partition headers, the session from
/// `sessions`), then a best-effort outbox drain. Every request must carry
/// the internal secret.
pub async fn serve_cell<A, F>(
    env: &Env,
    cell: &AggregateCell<A>,
    mut req: Request,
    read: F,
    sessions: Sessions,
) -> Result<Response>
where
    A: Aggregate + Send + Sync + 'static,
    F: AsyncFnOnce(&AggregateCell<A>) -> std::result::Result<Value, String>,
{
    if let Err(error) = authenticate(&req, env) {
        return error_response(error);
    }
    let url = req.url()?;
    let parts: Vec<String> = url
        .path()
        .split('/')
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect();
    match (req.method(), parts.get(2)) {
        (Method::Get, None) => match read(cell).await {
            Ok(body) => json_status(body, 200),
            Err(error) => error_response(CellDispatchError::Internal(error)),
        },
        (Method::Post, Some(command)) => {
            let command = command.clone();
            let body: Value = match req.json().await {
                Ok(body) => body,
                Err(_) => {
                    return error_response(CellDispatchError::BadRequest("invalid JSON".into()))
                }
            };
            let parsed = match CellWaitPathRequest::parse(body) {
                Ok(parsed) => parsed,
                Err(error) => return error_response(CellDispatchError::BadRequest(error)),
            };
            let header = |name: &str| req.headers().get(name).ok().flatten();
            let identity = match (
                header(CELL_SERVICE_ID_HEADER),
                header(CELL_PRINCIPAL_PARTITION_HEADER),
            ) {
                (Some(service), Some(partition)) => {
                    match CellCommandIdentity::new(service, partition, &parsed.command_id) {
                        Ok(identity) => identity,
                        Err(error) => return error_response(error),
                    }
                }
                _ => return error_response(CellDispatchError::Unauthorized),
            };
            let Some(session) = sessions(&header) else {
                return error_response(CellDispatchError::Forbidden);
            };
            let result = cell
                .dispatch_idempotent(&command, &identity, parsed.input, session)
                .await;
            if let Err(error) = cell.drain_outbox(env).await {
                worker::console_error!("post-commit drain deferred: {error}");
            }
            match result {
                Ok(dispatch) => json_status(dispatch.wait_path_wire(), 200),
                Err(error) => error_response(error),
            }
        }
        _ => json_status(json!({ "error": "not found" }), 404),
    }
}

/// A cell's alarm: due schedules, then the outbox drain.
pub async fn cell_alarm<A>(env: &Env, cell: &AggregateCell<A>) -> Result<Response>
where
    A: Aggregate + Send + Sync + 'static,
{
    let wake = cell
        .alarm(env)
        .await
        .map_err(|error| worker::Error::RustError(error.to_string()))?;
    for error in &wake.drain.deferred {
        worker::console_error!("drain deferred: {error}");
    }
    Response::ok("ok")
}

/// The zone-activity cell of a game worker (`ZoneActivity:{zone}`, kind
/// `zone-activity`, binding `ZONE_ACTIVITY`): the zone's own lifecycle
/// facts; its `zone.activity_changed` events leave through `outbox` (the
/// worker's Queue) for the zone status read model. `GET` returns the row.
pub struct ZoneActivityCell {
    cell: AggregateCell<ZoneActivity>,
    env: Env,
}

impl ZoneActivityCell {
    /// Open the cell of `state`, publishing its events to `outbox` (the
    /// worker's Queue producer binding; cells commit only with one).
    pub fn new(state: State, env: Env, outbox: CelldOutbox) -> Self {
        let cell = mount_zone_activity(
            AggregateCell::<ZoneActivity>::from_state_with_snapshots(state, 20)
                .expect("zone activity cell identity"),
        )
        .with_celld_outbox(outbox);
        Self { cell, env }
    }

    /// Durable Object `fetch`.
    pub async fn fetch(&self, req: Request) -> Result<Response> {
        serve_cell(&self.env, &self.cell, req, read_activity, fact_sessions).await
    }

    /// Durable Object `alarm`.
    pub async fn alarm(&self) -> Result<Response> {
        cell_alarm(&self.env, &self.cell).await
    }
}

async fn read_activity(cell: &AggregateCell<ZoneActivity>) -> std::result::Result<Value, String> {
    let activity = cell.load().await.map_err(|error| error.to_string())?;
    Ok(json!({ "activity": activity.map(|a| a.row()) }))
}
