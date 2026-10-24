//! Durable aggregates of a zone server, as celld cells fed by
//! `distributed_game::celld::CelldFactSink` over the internal HTTP path.
//!
//! - `Kills:{principal}`: `kills.record` (one player's kills, in fact order).
//! - `Match:{match_id}`: `match.record_result`.
//! - `Presence:{principal}`: the runtime lifecycle facts `zone.player_joined`
//!   and `zone.player_left` (lifecycle facts go to presence, never to the
//!   player's gameplay cells).
//! - `ZoneLease:{zone_id}` (kind `zone-lease`): zone ownership
//!   (`distributed_game::world::zone_lease`) for `CellZoneOwnership`; its
//!   alarm declares `zone.lease_expired`.
//! - `ZoneState:{zone_id}` (kind `zone-state`): hibernation snapshots
//!   (`distributed_game::world::zone_state`) for `CellZoneStateStore`;
//!   snapshots above the inline limit go to the `ZONE_SNAPSHOTS` R2 bucket.
//!
//! Every route requires `DISTRIBUTED_INTERNAL_SECRET`. The handler's session
//! is rebuilt from the trusted fact headers (`fact_session_from_headers`):
//! the principal and the `FactStamp`; handlers refuse anything else.

use distributed::cell_host::{
    AggregateCell, CellCommandIdentity, CellDispatchError, CellWaitPathRequest, CelldOutbox,
    InternalHttpSecret, CELL_INTERNAL_SECRET_ENV, CELL_INTERNAL_SECRET_HEADER,
    CELL_PRINCIPAL_PARTITION_HEADER, CELL_SERVICE_ID_HEADER,
};
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, CausalRouteDependencies, HandlerError, PortableCommand, Routes, Session,
};
use distributed::{sourced, Aggregate, Entity};
use distributed_game::cell::{fact_session_from_headers, session_stamp};
use distributed_game::world::zone_lease::{mount_zone_lease, ZoneLease};
use distributed_game::world::zone_state::{mount_zone_state, ZoneState};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use worker::*;

fn rejected(error: impl std::fmt::Display) -> HandlerError {
    HandlerError::Rejected(error.to_string())
}

/// Only facts (principal + stamp from the trusted headers) may run.
fn is_fact<A: Aggregate + Send + Sync + 'static>(ctx: &CausalCommandContext<'_, A>) -> bool {
    ctx.session().user_id().is_some() && session_stamp(ctx.session()).is_some()
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct Recorded {
    pub count: u32,
}

// ---------------------------------------------------------------- Kills

#[derive(Clone, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct Kills {
    entity: Entity,
    mobs: Vec<String>,
}

#[sourced(entity, aggregate_type = "Kills", events = "KillsEvent")]
impl Kills {
    #[event("kills.recorded", version = 1)]
    fn record(&mut self, principal: String, mob: String) {
        if self.entity.id().is_empty() {
            self.entity.set_id(principal);
        }
        self.mobs.push(mob);
    }
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct KillInput {
    pub mob: String,
}

struct RecordKill;
impl<D> PortableCommand<D> for RecordKill
where
    D: CausalRouteDependencies<Aggregate = Kills> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<KillInput, Succeeded<Recorded>>(
                "kills.record",
            ))
            .guarded(is_fact::<Kills>, handle_kill)
    }
}

async fn handle_kill(
    ctx: &CausalCommandContext<'_, Kills>,
    input: KillInput,
) -> std::result::Result<PreparedCommand<Succeeded<Recorded>>, HandlerError> {
    let principal = ctx.user_id()?.to_string();
    let repo = ctx.repo();
    let mut kills = match repo.get(&principal).await? {
        Some(kills) => kills,
        None => repo.create(),
    };
    kills.record(principal, input.mob).map_err(rejected)?;
    let count = kills.mobs.len() as u32;
    repo.commit(kills)?.succeeded(Recorded { count })
}

// ---------------------------------------------------------------- Match

#[derive(Clone, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct Match {
    entity: Entity,
    results: Vec<u32>,
}

#[sourced(entity, aggregate_type = "Match", events = "MatchEvent")]
impl Match {
    #[event("match.result_recorded", version = 1)]
    fn record(&mut self, match_id: String, winner_slot: u32) {
        if self.entity.id().is_empty() {
            self.entity.set_id(match_id);
        }
        self.results.push(winner_slot);
    }
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct MatchResultInput {
    pub match_id: String,
    pub winner_slot: u32,
}

struct RecordResult;
impl<D> PortableCommand<D> for RecordResult
where
    D: CausalRouteDependencies<Aggregate = Match> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<MatchResultInput, Succeeded<Recorded>>(
                "match.record_result",
            ))
            .guarded(is_fact::<Match>, handle_result)
    }
}

async fn handle_result(
    ctx: &CausalCommandContext<'_, Match>,
    input: MatchResultInput,
) -> std::result::Result<PreparedCommand<Succeeded<Recorded>>, HandlerError> {
    let repo = ctx.repo();
    let mut found = match repo.get(&input.match_id).await? {
        Some(found) => found,
        None => repo.create(),
    };
    found
        .record(input.match_id, input.winner_slot)
        .map_err(rejected)?;
    let count = found.results.len() as u32;
    repo.commit(found)?.succeeded(Recorded { count })
}

// ---------------------------------------------------------------- Presence

#[derive(Clone, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct Presence {
    entity: Entity,
    transitions: Vec<String>,
}

#[sourced(entity, aggregate_type = "Presence", events = "PresenceEvent")]
impl Presence {
    #[event("presence.changed", version = 1)]
    fn change(&mut self, principal: String, transition: String) {
        if self.entity.id().is_empty() {
            self.entity.set_id(principal);
        }
        self.transitions.push(transition);
    }
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct JoinedInput {
    pub zone: String,
    pub via: String,
    pub from_zone: Option<String>,
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct LeftInput {
    pub zone: String,
    pub reason: String,
}

struct Joined;
impl<D> PortableCommand<D> for Joined
where
    D: CausalRouteDependencies<Aggregate = Presence> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<JoinedInput, Succeeded<Recorded>>(
                distributed_game::lifecycle::PLAYER_JOINED,
            ))
            .guarded(is_fact::<Presence>, handle_joined)
    }
}

struct Left;
impl<D> PortableCommand<D> for Left
where
    D: CausalRouteDependencies<Aggregate = Presence> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<LeftInput, Succeeded<Recorded>>(
                distributed_game::lifecycle::PLAYER_LEFT,
            ))
            .guarded(is_fact::<Presence>, handle_left)
    }
}

async fn handle_joined(
    ctx: &CausalCommandContext<'_, Presence>,
    input: JoinedInput,
) -> std::result::Result<PreparedCommand<Succeeded<Recorded>>, HandlerError> {
    let via = match input.from_zone {
        Some(from) => format!("{}<-{from}", input.via),
        None => input.via,
    };
    presence(ctx, format!("joined:{}:{via}", input.zone)).await
}

async fn handle_left(
    ctx: &CausalCommandContext<'_, Presence>,
    input: LeftInput,
) -> std::result::Result<PreparedCommand<Succeeded<Recorded>>, HandlerError> {
    presence(ctx, format!("left:{}:{}", input.zone, input.reason)).await
}

async fn presence(
    ctx: &CausalCommandContext<'_, Presence>,
    transition: String,
) -> std::result::Result<PreparedCommand<Succeeded<Recorded>>, HandlerError> {
    let principal = ctx.user_id()?.to_string();
    let repo = ctx.repo();
    let mut presence = match repo.get(&principal).await? {
        Some(presence) => presence,
        None => repo.create(),
    };
    presence.change(principal, transition).map_err(rejected)?;
    let count = presence.transitions.len() as u32;
    repo.commit(presence)?.succeeded(Recorded { count })
}

// ---------------------------------------------------------------- Hosting

async fn dispatch<A>(
    env: &Env,
    cell: &AggregateCell<A>,
    command: &str,
    req: &mut Request,
) -> Result<Response>
where
    A: Aggregate + Send + Sync + 'static,
{
    let body: Value = match req.json().await {
        Ok(body) => body,
        Err(_) => return error_response(CellDispatchError::BadRequest("invalid JSON".into())),
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
    // Facts carry principal + stamp; zone hosts (lease commands) carry a
    // user and role. Both arrive only on the authenticated internal path.
    let session = fact_session_from_headers(header).unwrap_or_else(|| {
        let mut session = Session::new();
        for key in [distributed::USER_ID_KEY, distributed::microsvc::ROLE_KEY] {
            if let Some(value) = header(key) {
                session.set(key, value);
            }
        }
        session
    });
    let result = cell
        .dispatch_idempotent(command, &identity, parsed.input, session)
        .await;
    if let Err(error) = cell.drain_outbox(env).await {
        worker::console_error!("post-commit drain deferred: {error}");
    }
    match result {
        Ok(dispatch) => json_status(dispatch.wait_path_wire(), 200),
        Err(error) => error_response(error),
    }
}

async fn route<A>(
    env: &Env,
    cell: &AggregateCell<A>,
    mut req: Request,
    state: impl Fn(&A) -> Value,
) -> Result<Response>
where
    A: Aggregate + Send + Sync + 'static,
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
        (Method::Get, None) => {
            let loaded = cell
                .load()
                .await
                .map_err(|error| Error::RustError(error.to_string()))?;
            let history = cell
                .history()
                .await
                .map_err(|error| Error::RustError(error.to_string()))?;
            let names = history
                .iter()
                .map(|event| event.event_name.clone())
                .collect::<Vec<_>>();
            let armed = cell
                .armed_schedules()
                .map_err(|error| Error::RustError(error.to_string()))?
                .len();
            json_status(
                json!({
                    "events": history.len(),
                    "eventNames": names,
                    "armedAlarms": armed,
                    "state": loaded.as_ref().map(state),
                }),
                200,
            )
        }
        (Method::Post, Some(command)) => {
            let command = command.clone();
            dispatch(env, cell, &command, &mut req).await
        }
        _ => json_status(json!({ "error": "not found" }), 404),
    }
}

macro_rules! cell_object {
    ($name:ident, $aggregate:ty, [$($command:expr),*], $state:expr) => {
        #[durable_object]
        pub struct $name {
            cell: AggregateCell<$aggregate>,
            env: Env,
        }

        impl DurableObject for $name {
            fn new(state: State, env: Env) -> Self {
                console_error_panic_hook::set_once();
                let outbox = CelldOutbox::from_env(&env, "OUTBOX").expect("OUTBOX Queue binding");
                let cell = AggregateCell::<$aggregate>::from_state(state)
                    .expect("cell identity")
                    $(.mount($command))*
                    .with_celld_outbox(outbox);
                Self { cell, env }
            }

            async fn fetch(&self, req: Request) -> Result<Response> {
                route(&self.env, &self.cell, req, $state).await
            }

            async fn alarm(&self) -> Result<Response> {
                self.cell
                    .alarm(&self.env)
                    .await
                    .map_err(|error| Error::RustError(error.to_string()))?
                    .drain
                    .deferred
                    .iter()
                    .for_each(|error| worker::console_error!("drain deferred: {error}"));
                Response::ok("ok")
            }
        }
    };
}

cell_object!(
    KillsCell,
    Kills,
    [RecordKill],
    |kills: &Kills| json!({ "mobs": kills.mobs })
);
cell_object!(
    MatchCell,
    Match,
    [RecordResult],
    |found: &Match| json!({ "results": found.results })
);
cell_object!(
    PresenceCell,
    Presence,
    [Joined, Left],
    |presence: &Presence| json!({ "transitions": presence.transitions })
);

#[durable_object]
pub struct ZoneLeaseCell {
    cell: AggregateCell<ZoneLease>,
    env: Env,
}

impl DurableObject for ZoneLeaseCell {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let outbox = CelldOutbox::from_env(&env, "OUTBOX").expect("OUTBOX Queue binding");
        // Renewals are internal events: snapshots keep loads short.
        let cell = mount_zone_lease(
            AggregateCell::<ZoneLease>::from_state_with_snapshots(state, 50)
                .expect("zone lease cell identity"),
        )
        .with_celld_outbox(outbox);
        Self { cell, env }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        route(
            &self.env,
            &self.cell,
            req,
            |lease: &ZoneLease| json!({ "holder": lease.holder(), "token": lease.token() }),
        )
        .await
    }

    async fn alarm(&self) -> Result<Response> {
        let wake = self
            .cell
            .alarm(&self.env)
            .await
            .map_err(|error| Error::RustError(error.to_string()))?;
        for failed in wake.schedules.failed.iter().chain(&wake.schedules.retried) {
            worker::console_error!("lease alarm failed: {} {}", failed.code, failed.message);
        }
        Response::ok("ok")
    }
}

#[durable_object]
pub struct ZoneStateCell {
    cell: AggregateCell<ZoneState>,
    env: Env,
}

impl DurableObject for ZoneStateCell {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let outbox = CelldOutbox::from_env(&env, "OUTBOX").expect("OUTBOX Queue binding");
        // Every save replaces the value: snapshot each commit.
        let cell = mount_zone_state(
            AggregateCell::<ZoneState>::from_state_with_snapshots(state, 1)
                .expect("zone state cell identity"),
        )
        .with_celld_outbox(outbox);
        Self { cell, env }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        if let Err(error) = authenticate(&req, &self.env) {
            return error_response(error);
        }
        let bucket =
            distributed_game::world::zone_state::celld_worker::bucket(&self.env, "ZONE_SNAPSHOTS")?;
        distributed_game::world::zone_state::celld_worker::fetch(&self.cell, &bucket, req).await
    }
}

#[event(fetch)]
async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    console_error_panic_hook::set_once();
    let url = req.url()?;
    let path = url.path();
    if path == "/" || path == "/health" {
        return Response::ok("distributed fact cells\n");
    }
    if let Err(error) = authenticate(&req, &env) {
        return error_response(error);
    }
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    let binding = match parts.first().copied() {
        Some("kills") => "KILLS",
        Some("match") => "MATCH",
        Some("presence") => "PRESENCE",
        Some("zone-lease") => "ZONE_LEASE",
        Some("zone-state") => "ZONE_STATE",
        _ => return json_status(json!({ "error": "unknown cell kind" }), 404),
    };
    let Some(shard) = parts.get(1) else {
        return json_status(json!({ "error": "missing shard" }), 400);
    };
    let stub = env
        .durable_object(binding)?
        .id_from_name(shard)?
        .get_stub()?;
    stub.fetch_with_request(req).await
}

fn authenticate(req: &Request, env: &Env) -> std::result::Result<(), CellDispatchError> {
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

fn error_response(error: CellDispatchError) -> Result<Response> {
    json_status(
        json!({ "error": error.client_message(), "code": error.code() }),
        error.status_code(),
    )
}

fn json_status(body: Value, status: u16) -> Result<Response> {
    Ok(Response::from_json(&body)?.with_status(status))
}
