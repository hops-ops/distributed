//! Ticker Durable Object backed by `AggregateCell`: re-armable cell alarms on
//! celld, sharing the one Durable Object alarm with the outbox watchdog.
//!
//! Routes (all require `DISTRIBUTED_INTERNAL_SECRET`):
//! - `POST /ticker/{id}/ticker.start` `{ commandId, input }` arms `tick`.
//! - `POST /ticker/{id}/ticker.touch` publishes one outbox event. With
//!   `x-distributed-test-defer-drain: 1` it skips the immediate drain, so only
//!   the alarm wake (watchdog) can deliver the row.
//! - `POST /ticker/{id}/ticker.tick` is the alarm-only command; ordinary
//!   callers are refused.
//! - `GET /ticker/{id}` returns ticks, armed alarms and the outbox backlog.

use std::time::Duration;

use distributed::cell_host::{
    cell_alarm_guard, AggregateCell, CellAlarm, CellCommandIdentity, CellDispatchError,
    CellWaitPathRequest, CelldOutbox, InternalHttpSecret, CELL_INTERNAL_SECRET_ENV,
    CELL_INTERNAL_SECRET_HEADER, CELL_PRINCIPAL_PARTITION_HEADER, CELL_SERVICE_ID_HEADER,
};
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, CausalRouteDependencies, HandlerError, PortableCommand, Routes, Session,
    ROLE_KEY, USER_ID_KEY,
};
use distributed::{sourced, Entity};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use worker::*;

const TICK: &str = "ticker.tick";

#[derive(Clone, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct Ticker {
    entity: Entity,
    ticks: u32,
    limit: u32,
    period_ms: u64,
    touches: u32,
}

#[sourced(entity, aggregate_type = "Ticker", events = "TickerEvent")]
impl Ticker {
    #[event("ticker.started", version = 1)]
    fn start(&mut self, id: String, limit: u32, period_ms: u64) {
        self.entity.set_id(id);
        self.limit = limit;
        self.period_ms = period_ms;
    }

    #[event("ticker.ticked", version = 1, domain = event)]
    fn tick(&mut self) {
        self.ticks += 1;
    }

    #[event("ticker.touched", version = 1, domain = event)]
    fn touch(&mut self) {
        self.touches += 1;
    }
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct StartInput {
    pub id: String,
    pub limit: u32,
    pub period_ms: u64,
    pub fail_after_arming: bool,
}

#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct TickerInput {
    pub id: String,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct TickerPayload {
    pub id: String,
    pub ticks: u32,
}

fn rejected(error: impl std::fmt::Display) -> HandlerError {
    HandlerError::Rejected(error.to_string())
}

fn is_user(ctx: &CausalCommandContext<'_, Ticker>) -> bool {
    ctx.session()
        .user_id()
        .is_some_and(|user| !user.starts_with("system:"))
}

struct Start;
impl<D> PortableCommand<D> for Start
where
    D: CausalRouteDependencies<Aggregate = Ticker> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<StartInput, Succeeded<TickerPayload>>(
                "ticker.start",
            ))
            .guarded(is_user, handle_start)
    }
}

async fn handle_start(
    ctx: &CausalCommandContext<'_, Ticker>,
    input: StartInput,
) -> std::result::Result<PreparedCommand<Succeeded<TickerPayload>>, HandlerError> {
    let repo = ctx.repo();
    if repo.get(&input.id).await?.is_some() {
        return Err(rejected("ticker already started"));
    }
    let mut ticker = repo.create();
    ticker
        .start(input.id.clone(), input.limit, input.period_ms)
        .map_err(rejected)?;
    ctx.arm_alarm(
        CellAlarm::after(
            "tick",
            Duration::from_millis(input.period_ms),
            TICK,
            &TickerInput {
                id: input.id.clone(),
            },
        )
        .map_err(rejected)?,
    )?;
    if input.fail_after_arming {
        return Err(rejected("start refused after arming"));
    }
    repo.commit(ticker)?.succeeded(TickerPayload {
        id: input.id,
        ticks: 0,
    })
}

struct Tick;
impl<D> PortableCommand<D> for Tick
where
    D: CausalRouteDependencies<Aggregate = Ticker> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<TickerInput, Succeeded<TickerPayload>>(TICK))
            .guarded(cell_alarm_guard::<Ticker>, handle_tick)
    }
}

async fn handle_tick(
    ctx: &CausalCommandContext<'_, Ticker>,
    input: TickerInput,
) -> std::result::Result<PreparedCommand<Succeeded<TickerPayload>>, HandlerError> {
    let repo = ctx.repo();
    let mut ticker = repo
        .get(&input.id)
        .await?
        .ok_or_else(|| rejected("no ticker"))?;
    ticker.tick().map_err(rejected)?;
    if ticker.ticks < ticker.limit {
        let due = ctx
            .alarm_due()
            .ok_or_else(|| rejected("tick outside an alarm"))?
            + Duration::from_millis(ticker.period_ms);
        ctx.arm_alarm(CellAlarm::at("tick", due, TICK, &input).map_err(rejected)?)?;
    }
    let ticks = ticker.ticks;
    repo.publish_events()
        .commit(ticker)?
        .succeeded(TickerPayload {
            id: input.id,
            ticks,
        })
}

struct Touch;
impl<D> PortableCommand<D> for Touch
where
    D: CausalRouteDependencies<Aggregate = Ticker> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<TickerInput, Succeeded<TickerPayload>>(
                "ticker.touch",
            ))
            .guarded(is_user, handle_touch)
    }
}

async fn handle_touch(
    ctx: &CausalCommandContext<'_, Ticker>,
    input: TickerInput,
) -> std::result::Result<PreparedCommand<Succeeded<TickerPayload>>, HandlerError> {
    let repo = ctx.repo();
    let mut ticker = repo
        .get(&input.id)
        .await?
        .ok_or_else(|| rejected("no ticker"))?;
    ticker.touch().map_err(rejected)?;
    let ticks = ticker.ticks;
    repo.publish_events()
        .commit(ticker)?
        .succeeded(TickerPayload {
            id: input.id,
            ticks,
        })
}

#[durable_object]
pub struct TickerCell {
    cell: AggregateCell<Ticker>,
    env: Env,
}

impl DurableObject for TickerCell {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let outbox = CelldOutbox::from_env(&env, "OUTBOX").expect("OUTBOX Queue binding");
        let cell = AggregateCell::<Ticker>::from_state(state)
            .expect("ticker cell identity")
            .mount(Start)
            .mount(Tick)
            .mount(Touch)
            .with_celld_outbox(outbox);
        Self { cell, env }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        if let Err(error) = authenticate_internal_request(&req, &self.env) {
            return error_response(error);
        }
        let url = req.url()?;
        let parts: Vec<String> = url
            .path()
            .split('/')
            .filter(|part| !part.is_empty())
            .map(str::to_string)
            .collect();
        let id = match parts.get(1) {
            Some(id) if parts.first().map(String::as_str) == Some("ticker") => id.clone(),
            _ => return json_status(json!({ "error": "missing ticker id" }), 400),
        };
        match (req.method(), parts.get(2).map(String::as_str)) {
            (Method::Get, None) => self.state(&id).await,
            (Method::Post, Some(command))
                if matches!(command, "ticker.start" | "ticker.touch" | TICK) =>
            {
                let command = command.to_string();
                self.command(&id, &command, &mut req).await
            }
            _ => json_status(json!({ "error": "not found" }), 404),
        }
    }

    async fn alarm(&self) -> Result<Response> {
        let wake = self
            .cell
            .alarm(&self.env)
            .await
            .map_err(|error| Error::RustError(error.to_string()))?;
        for failed in wake.alarms.failed.iter().chain(&wake.alarms.retried) {
            worker::console_error!(
                "cell alarm {} ({}) failed: {} {}",
                failed.name,
                failed.command,
                failed.code,
                failed.message
            );
        }
        for error in wake.drain.deferred {
            worker::console_error!("outbox drain deferred: {error}");
        }
        Response::ok("ok")
    }
}

impl TickerCell {
    async fn state(&self, id: &str) -> Result<Response> {
        let ticker = self
            .cell
            .load()
            .await
            .map_err(|error| Error::RustError(error.to_string()))?;
        let history = self
            .cell
            .history()
            .await
            .map_err(|error| Error::RustError(error.to_string()))?;
        let armed = self
            .cell
            .armed_alarms()
            .map_err(|error| Error::RustError(error.to_string()))?;
        let backlog = self
            .cell
            .outbox_backlog()
            .await
            .map_err(|error| Error::RustError(error.to_string()))?;
        json_status(
            json!({
                "id": id,
                "exists": ticker.is_some(),
                "ticks": ticker.as_ref().map_or(0, |t| t.ticks),
                "touches": ticker.as_ref().map_or(0, |t| t.touches),
                "tickEvents": history.iter().filter(|e| e.event_name == "ticker.ticked").count(),
                "armed": armed.iter().map(|alarm| json!({
                    "name": alarm.name,
                    "command": alarm.command,
                    "dueAtMs": alarm.due_at_ms,
                    "generation": alarm.generation,
                    "attempts": alarm.attempts,
                })).collect::<Vec<_>>(),
                "outboxPending": backlog.pending,
                "nowMs": Date::now().as_millis(),
            }),
            200,
        )
    }

    async fn command(&self, id: &str, command: &str, req: &mut Request) -> Result<Response> {
        let body: Value = match req.json().await {
            Ok(body) => body,
            Err(_) => return json_status(json!({ "error": "invalid JSON" }), 400),
        };
        let parsed = match CellWaitPathRequest::parse(body) {
            Ok(parsed) => parsed,
            Err(error) => return error_response(CellDispatchError::BadRequest(error)),
        };
        let identity = match request_identity(req, &parsed.command_id) {
            Ok(identity) => identity,
            Err(error) => return error_response(error),
        };
        let mut input = parsed.input;
        if let Some(object) = input.as_object_mut() {
            object.insert("id".into(), json!(id));
        }
        let result = self
            .cell
            .dispatch_idempotent(command, &identity, input, request_session(req))
            .await;
        let defer = req
            .headers()
            .get("x-distributed-test-defer-drain")
            .ok()
            .flatten()
            .as_deref()
            == Some("1");
        if !defer {
            match self.cell.drain_outbox(&self.env).await {
                Ok(outcome) => {
                    for error in outcome.deferred {
                        worker::console_error!("post-commit drain deferred: {error}");
                    }
                }
                Err(error) => worker::console_error!("post-commit drain deferred: {error}"),
            }
        }
        match result {
            Ok(dispatch) => json_status(
                json!({
                    "payload": dispatch.payload(),
                    "receipt": { "commandId": dispatch.command_id(), "replayed": dispatch.replayed() },
                }),
                200,
            ),
            Err(error) => error_response(error),
        }
    }
}

#[event(fetch)]
async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    console_error_panic_hook::set_once();
    let url = req.url()?;
    let path = url.path();
    if path == "/" || path == "/health" {
        return Response::ok("distributed ticker cell\n");
    }
    if let Err(error) = authenticate_internal_request(&req, &env) {
        return error_response(error);
    }
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    let id = match (parts.first().copied(), parts.get(1).copied()) {
        (Some("ticker"), Some(id)) => id,
        _ => return Response::error("ticker: authenticated internal routes\n", 404),
    };
    let stub = env.durable_object("TICKER")?.id_from_name(id)?.get_stub()?;
    stub.fetch_with_request(req).await
}

fn authenticate_internal_request(
    req: &Request,
    env: &Env,
) -> std::result::Result<(), CellDispatchError> {
    let configured = env
        .secret(CELL_INTERNAL_SECRET_ENV)
        .map(|value| value.to_string())
        .or_else(|_| {
            env.var(CELL_INTERNAL_SECRET_ENV)
                .map(|value| value.to_string())
        })
        .map_err(|_| {
            CellDispatchError::Internal(format!("{CELL_INTERNAL_SECRET_ENV} is required"))
        })?;
    let secret = InternalHttpSecret::new(configured).map_err(CellDispatchError::Internal)?;
    let candidate = req
        .headers()
        .get(CELL_INTERNAL_SECRET_HEADER)
        .map_err(|_| CellDispatchError::Unauthorized)?
        .ok_or(CellDispatchError::Unauthorized)?;
    if secret.matches(&candidate) {
        Ok(())
    } else {
        Err(CellDispatchError::Unauthorized)
    }
}

fn request_identity(
    req: &Request,
    command_id: &str,
) -> std::result::Result<CellCommandIdentity, CellDispatchError> {
    let header = |name: &str| {
        req.headers()
            .get(name)
            .ok()
            .flatten()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .ok_or(CellDispatchError::Unauthorized)
    };
    CellCommandIdentity::new(
        header(CELL_SERVICE_ID_HEADER)?,
        header(CELL_PRINCIPAL_PARTITION_HEADER)?,
        command_id,
    )
}

/// Only the verified user and roles cross the HTTP boundary; the alarm claims
/// that `cell_alarm_guard` checks are set by the cell host alone.
fn request_session(req: &Request) -> Session {
    let mut session = Session::new();
    for key in [USER_ID_KEY, ROLE_KEY] {
        if let Some(value) = req.headers().get(key).ok().flatten() {
            if !value.is_empty() {
                session.set(key, value);
            }
        }
    }
    session
}

fn error_response(error: CellDispatchError) -> Result<Response> {
    json_status(
        json!({ "error": error.client_message(), "code": error.code() }),
        error.status_code(),
    )
}

fn json_status(body: Value, status: u16) -> Result<Response> {
    Ok(Response::from_json(&body)?.with_status(status))
}
