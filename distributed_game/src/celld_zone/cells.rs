//! Serving fact cells next to celld zones (wasm32): the internal wait-path
//! route every fact cell of a game worker needs, watched world cells
//! ([`CellWatchers`]), and the zone-activity cell.

use std::cell::RefCell;

use distributed::cell_host::{
    AggregateCell, CellCommandIdentity, CellDispatchError, CellWaitPathRequest, CelldOutbox,
    InternalHttpSecret, CELL_INTERNAL_SECRET_ENV, CELL_INTERNAL_SECRET_HEADER,
    CELL_PRINCIPAL_PARTITION_HEADER, CELL_SERVICE_ID_HEADER,
};
use distributed::microsvc::Session;
use distributed::Aggregate;
use serde_json::{json, Value};
use worker::{Env, Headers, Method, Request, RequestInit, Response, Result, State};

use crate::cell::{fact_session_from_headers, run_fact_batch};
use crate::world::content_rollout::{mount_content_rollout, ContentRollout};
use crate::world::watch::{
    parse_watcher_address, topic, watch_query, WatchUpdate, Watchers, PUSH_CONCURRENCY, PUSH_ROUTE,
    STATS_ROUTE,
};
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
    let status = error.status_code();
    json_status(error_body(&error), status)
}

fn error_body(error: &CellDispatchError) -> Value {
    json!({ "error": error.client_message(), "code": error.code() })
}

/// The route of a fact batch: `POST /{kind}/{shard}/__batch` (see
/// [`serve_cell`]).
pub const BATCH_ROUTE: &str = "__batch";

/// One cell's internal routes: `GET /{kind}/{shard}` → `read(cell)`, `POST
/// /{kind}/{shard}/{command}` → the idempotent wait-path dispatch (identity
/// from the service-id and principal-partition headers, the session from
/// `sessions`), then a best-effort outbox drain. `POST
/// /{kind}/{shard}/__batch` ([`BATCH_ROUTE`]) runs a zone's consecutive
/// facts for this cell in one request ([`run_fact_batch`]: each entry as
/// its own wait-path command, with its own ledger id and session, in order,
/// stopping after the first retryable result), then one outbox drain.
/// Every request must carry the internal secret.
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
        (Method::Post, Some(route)) if route == BATCH_ROUTE => {
            serve_batch(env, cell, req, sessions).await
        }
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

async fn serve_batch<A>(
    env: &Env,
    cell: &AggregateCell<A>,
    mut req: Request,
    sessions: Sessions,
) -> Result<Response>
where
    A: Aggregate + Send + Sync + 'static,
{
    let Some(service) = req.headers().get(CELL_SERVICE_ID_HEADER).ok().flatten() else {
        return error_response(CellDispatchError::Unauthorized);
    };
    let body: Value = match req.json().await {
        Ok(body) => body,
        Err(_) => return error_response(CellDispatchError::BadRequest("invalid JSON".into())),
    };
    let results = run_fact_batch(cell, &service, &body, sessions).await;
    if let Err(error) = cell.drain_outbox(env).await {
        worker::console_error!("post-commit drain deferred: {error}");
    }
    match results {
        Ok(results) => json_status(results, 200),
        Err(error) => error_response(error),
    }
}

/// A watched world cell's watchers ([`crate::world::watch`]): zones that
/// read the cell with `?watch=` get its view pushed after every commit (and
/// every alarm) that moves its version. Memory only.
///
/// ```ignore
/// pub struct HoodCell { cell: AggregateCell<Hood>, env: Env, watchers: CellWatchers }
/// // fetch:  self.watchers.serve(&self.env, &self.cell, req, read_hood, sessions).await
/// // alarm:  self.watchers.alarm(&self.env, &self.cell, read_hood).await
/// ```
pub struct CellWatchers {
    topic: String,
    watchers: RefCell<Watchers>,
}

impl CellWatchers {
    /// The watchers of cell `kind` whose Durable Object is `state` (its
    /// name is the shard).
    pub fn new(kind: &str, state: &State) -> Self {
        Self::of(kind, &state.id().name().unwrap_or_default())
    }

    /// The watchers of cell `kind/shard`.
    pub fn of(kind: &str, shard: &str) -> Self {
        Self {
            topic: topic(kind, shard),
            watchers: RefCell::new(Watchers::new()),
        }
    }

    /// [`serve_cell`], plus: a read with `?watch=` leaves a lease and its
    /// body carries the cell's `version`; a command that changed the view
    /// is pushed to the live watchers; `GET /{kind}/{shard}/__watchers`
    /// ([`STATS_ROUTE`]) answers the live watchers and
    /// [`WatcherStats`](crate::world::watch::WatcherStats).
    pub async fn serve<A, F>(
        &self,
        env: &Env,
        cell: &AggregateCell<A>,
        req: Request,
        read: F,
        sessions: Sessions,
    ) -> Result<Response>
    where
        A: Aggregate + Send + Sync + 'static,
        F: AsyncFn(&AggregateCell<A>) -> std::result::Result<Value, String>,
    {
        if req.method() == Method::Get {
            if let Err(error) = authenticate(&req, env) {
                return error_response(error);
            }
            let url = req.url()?;
            if url.path().ends_with(&format!("/{STATS_ROUTE}")) {
                return json_status(self.stats(), 200);
            }
            if let Some((watcher, lease)) = url.query().and_then(watch_query) {
                self.watchers.borrow_mut().watch(&watcher, now_ms(), lease);
            }
        }
        let is_command = req.method() == Method::Post;
        let response = if req.method() == Method::Get {
            match versioned(cell, &read).await {
                Ok(body) => json_status(body, 200),
                Err(error) => error_response(CellDispatchError::Internal(error)),
            }
        } else {
            serve_cell(env, cell, req, &read, sessions).await
        };
        if is_command {
            self.publish(env, cell, &read).await;
        }
        response
    }

    /// The live watchers and what the cell did for them.
    fn stats(&self) -> Value {
        stats_body(&self.topic, &mut self.watchers.borrow_mut())
    }

    /// [`cell_alarm`], then a push if a schedule moved the version.
    pub async fn alarm<A, F>(&self, env: &Env, cell: &AggregateCell<A>, read: F) -> Result<Response>
    where
        A: Aggregate + Send + Sync + 'static,
        F: AsyncFn(&AggregateCell<A>) -> std::result::Result<Value, String>,
    {
        let response = cell_alarm(env, cell).await;
        self.publish(env, cell, &read).await;
        response
    }

    /// Push the cell's view to its live watchers when it changed (best
    /// effort, off the request; a lost push is repaired by the
    /// watcher's next reconcile read).
    async fn publish<A, F>(&self, env: &Env, cell: &AggregateCell<A>, read: &F)
    where
        A: Aggregate + Send + Sync + 'static,
        F: AsyncFn(&AggregateCell<A>) -> std::result::Result<Value, String>,
    {
        let live = self.watchers.borrow_mut().live(now_ms());
        if live.is_empty() {
            return;
        }
        let Ok(view) = versioned(cell, read).await else {
            return;
        };
        let version = crate::world::watch::view_version(&view);
        {
            let mut watchers = self.watchers.borrow_mut();
            if !watchers.publish(version, &view) {
                return;
            }
            watchers.pushed(live.len());
        }
        let update = WatchUpdate {
            topic: self.topic.clone(),
            version,
            view,
        };
        push_all(env.clone(), live, update);
    }
}

/// `{topic, live, ...WatcherStats}`.
fn stats_body(topic: &str, watchers: &mut Watchers) -> Value {
    let mut body = serde_json::to_value(watchers.stats()).unwrap_or_else(|_| json!({}));
    if let Value::Object(fields) = &mut body {
        fields.insert("topic".into(), json!(topic));
        fields.insert("live".into(), json!(watchers.live(now_ms()).len()));
    }
    body
}

/// `read(cell)` with the aggregate's version in its `version` field.
async fn versioned<A, F>(cell: &AggregateCell<A>, read: &F) -> std::result::Result<Value, String>
where
    A: Aggregate + Send + Sync + 'static,
    F: AsyncFn(&AggregateCell<A>) -> std::result::Result<Value, String>,
{
    let version = cell
        .load()
        .await
        .map_err(|error| error.to_string())?
        .map_or(0, |aggregate| aggregate.entity().version());
    let mut body = read(cell).await?;
    if let Value::Object(fields) = &mut body {
        fields.insert("version".into(), json!(version));
    }
    Ok(body)
}

/// Push `update` to `watchers`, [`PUSH_CONCURRENCY`] at a time, off the
/// caller.
pub(crate) fn push_all(env: Env, watchers: Vec<String>, update: WatchUpdate) {
    wasm_bindgen_futures::spawn_local(async move {
        let Some(secret) = internal_secret(&env) else {
            return;
        };
        let Ok(body) = serde_json::to_string(&update) else {
            return;
        };
        for chunk in watchers.chunks(PUSH_CONCURRENCY) {
            let pushes = chunk
                .iter()
                .map(|watcher| push(&env, watcher, &secret, &body));
            futures_util::future::join_all(pushes).await;
        }
    });
}

async fn push(env: &Env, watcher: &str, secret: &str, body: &str) {
    let Some((binding, name)) = parse_watcher_address(watcher) else {
        return;
    };
    let result: Result<()> = async {
        let stub = env
            .durable_object(binding)?
            .id_from_name(name)?
            .get_stub()?;
        let headers = Headers::new();
        headers.set(CELL_INTERNAL_SECRET_HEADER, secret)?;
        headers.set(super::ZONE_HEADER, name)?;
        headers.set("content-type", "application/json")?;
        let mut init = RequestInit::new();
        init.with_method(Method::Post)
            .with_headers(headers)
            .with_body(Some(body.into()));
        let url = format!("https://zone/{name}/{PUSH_ROUTE}");
        stub.fetch_with_request(Request::new_with_init(&url, &init)?)
            .await?;
        Ok(())
    }
    .await;
    if let Err(error) = result {
        worker::console_warn!("world push to {watcher}: {error}");
    }
}

fn internal_secret(env: &Env) -> Option<String> {
    env.secret(CELL_INTERNAL_SECRET_ENV)
        .map(|value| value.to_string())
        .or_else(|_| {
            env.var(CELL_INTERNAL_SECRET_ENV)
                .map(|value| value.to_string())
        })
        .ok()
}

fn now_ms() -> u64 {
    js_sys::Date::now() as u64
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

/// Dev-only settings shared by every zone object of a worker (kind
/// [`ZONE_DEV_KIND`](super::ZONE_DEV_KIND), binding `ZONE_DEV`), e.g. the
/// network simulation under shard [`NETSIM_SHARD`](super::NETSIM_SHARD).
/// `GET /zone-dev/{shard}` returns the stored JSON (`{}` when unset), `PUT`
/// (or `POST`) replaces it. Every request needs the internal secret. Zone
/// objects watch it only when `ZONE_DEV_SETTINGS=1` (a `GET` with `?watch=`;
/// each write is pushed to the watchers with the next `version`).
pub struct ZoneDevSettings {
    state: State,
    env: Env,
    watchers: RefCell<Watchers>,
    topic: String,
}

impl ZoneDevSettings {
    /// The settings object of `state`.
    pub fn new(state: State, env: Env) -> Self {
        let topic = topic(super::ZONE_DEV_KIND, &state.id().name().unwrap_or_default());
        Self {
            state,
            env,
            watchers: RefCell::new(Watchers::new()),
            topic,
        }
    }

    /// Durable Object `fetch`.
    pub async fn fetch(&self, mut req: Request) -> Result<Response> {
        if let Err(error) = authenticate(&req, &self.env) {
            return error_response(error);
        }
        const KEY: &str = "settings";
        const VERSION: &str = "version";
        match req.method() {
            Method::Get => {
                let url = req.url()?;
                if url.path().ends_with(&format!("/{STATS_ROUTE}")) {
                    return json_status(
                        stats_body(&self.topic, &mut self.watchers.borrow_mut()),
                        200,
                    );
                }
                if let Some((watcher, lease)) = url.query().and_then(watch_query) {
                    self.watchers.borrow_mut().watch(&watcher, now_ms(), lease);
                }
                let stored: Option<Value> = self.state.storage().get(KEY).await.ok().flatten();
                let version: u64 = self
                    .state
                    .storage()
                    .get(VERSION)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0);
                let mut body = stored.unwrap_or_else(|| json!({}));
                if let Value::Object(fields) = &mut body {
                    fields.insert(VERSION.into(), json!(version));
                }
                json_status(body, 200)
            }
            Method::Put | Method::Post => {
                let body: Value = match req.json().await {
                    Ok(body @ Value::Object(_)) => body,
                    _ => {
                        return error_response(CellDispatchError::BadRequest(
                            "a JSON object is required".into(),
                        ))
                    }
                };
                let version: u64 = self
                    .state
                    .storage()
                    .get(VERSION)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0)
                    + 1;
                self.state.storage().put(KEY, &body).await?;
                self.state.storage().put(VERSION, version).await?;
                let live = self.watchers.borrow_mut().live(now_ms());
                let mut view = body.clone();
                if let Value::Object(fields) = &mut view {
                    fields.insert(VERSION.into(), json!(version));
                }
                if !live.is_empty() && self.watchers.borrow_mut().publish(version, &view) {
                    self.watchers.borrow_mut().pushed(live.len());
                    push_all(
                        self.env.clone(),
                        live,
                        WatchUpdate {
                            topic: self.topic.clone(),
                            version,
                            view,
                        },
                    );
                }
                json_status(body, 200)
            }
            _ => json_status(json!({ "error": "not found" }), 404),
        }
    }
}

/// The content-rollout cell of a game worker (`ContentRollout:{world}`,
/// kind [`CONTENT_ROLLOUT_KIND`](crate::world::content_rollout::CONTENT_ROLLOUT_KIND),
/// binding `CONTENT_ROLLOUT`): the world's live content version and rollout
/// phase, read by every zone object ([`ObjectOptions::content_rollout`](super::ObjectOptions::content_rollout)).
/// `sessions` decides who may start a rollout: it must give the admin's
/// session the [`CONTENT_ADMIN_ROLE`](crate::world::content_rollout::CONTENT_ADMIN_ROLE)
/// (the cell's own schedules need no session). `GET` returns
/// `{"state": ContentState | null, "version": n}`; zones watch it
/// ([`CellWatchers`]).
pub struct ContentRolloutCell {
    cell: AggregateCell<ContentRollout>,
    env: Env,
    sessions: Sessions,
    watchers: CellWatchers,
}

impl ContentRolloutCell {
    /// Open the cell of `state`, publishing its events to `outbox`.
    pub fn new(state: State, env: Env, outbox: CelldOutbox, sessions: Sessions) -> Self {
        let watchers =
            CellWatchers::new(crate::world::content_rollout::CONTENT_ROLLOUT_KIND, &state);
        let cell = mount_content_rollout(
            AggregateCell::<ContentRollout>::from_state_with_snapshots(state, 20)
                .expect("content rollout cell identity"),
        )
        .with_celld_outbox(outbox);
        Self {
            cell,
            env,
            sessions,
            watchers,
        }
    }

    /// Durable Object `fetch` (zones watch it: every phase is pushed).
    pub async fn fetch(&self, req: Request) -> Result<Response> {
        self.watchers
            .serve(&self.env, &self.cell, req, read_rollout, self.sessions)
            .await
    }

    /// Durable Object `alarm` (the rollout's own schedule).
    pub async fn alarm(&self) -> Result<Response> {
        self.watchers
            .alarm(&self.env, &self.cell, read_rollout)
            .await
    }
}

async fn read_rollout(cell: &AggregateCell<ContentRollout>) -> std::result::Result<Value, String> {
    let rollout = cell.load().await.map_err(|error| error.to_string())?;
    Ok(json!({ "state": rollout.map(|r| r.state()) }))
}
