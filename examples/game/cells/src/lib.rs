//! The game demos' durable cells on celld. Mob Pit's are fed by the zone
//! server's `distributed_game::celld::CelldFactSink` over the internal HTTP
//! path; every multi-host demo keeps its zone ownership and hibernation
//! snapshots here.
//!
//! - `crawler/{principal}` (`CrawlerCell`): the `mobpit_crawler::Crawler`
//!   aggregate with its `#[command]` `crawler.record_kill` (XP decided here
//!   from the mob tier; the principal comes from the trusted fact session).
//!   Each commit writes the `crawler.mob_killed` domain event to the cell
//!   outbox, which drains to the `game-demo-outbox` Queue.
//! - `presence/{principal}` (`PresenceCell`): the runtime's player lifecycle
//!   facts (`zone.*`). A fact emitted before the one that set the current
//!   entry (`FactStamp::emission_order`) is ignored, so a late "left pit-1"
//!   cannot mark a player who already joined pit-2 offline.
//! - `gem-pouch/{principal}` (`GemPouchCell`): The Freeze's
//!   `freeze.gem_collected` facts (which gem spot, at which content
//!   version; the zone reports no amount).
//! - `content-log/{zone}` (`ContentLogCell`): The Freeze's
//!   `freeze.content_applied` zone facts (every content switch of a zone).
//! - `zone-lease/{zone}` (`ZoneLeaseCell`): zone ownership
//!   (`distributed_game::world::zone_lease`, `CellZoneOwnership`); its
//!   alarm declares `zone.lease_expired`.
//! - `zone-state/{zone}` (`ZoneStateCell`): hibernation snapshots
//!   (`distributed_game::world::zone_state`, `CellZoneStateStore`); large
//!   ones in the `ZONE_SNAPSHOTS` R2 bucket.
//!
//! Every route requires `DISTRIBUTED_INTERNAL_SECRET`. Mob Pit commands also
//! require the trusted fact session (principal + `FactStamp` headers,
//! rebuilt with `fact_session_from_headers`), so nothing but a zone server's
//! fact worker can run them; zone lease and state commands require the
//! `zone-host` role.
//!
//! The Queue consumer relays each outbox message to `CELLD_QUEUE_RELAY_URL`
//! (the zone server's authenticated `/internal/celld-queue/relay`), which
//! publishes it onto the projection bus read by `mobpit-service`. Delivery
//! is at-least-once with stable message ids; the projector deduplicates.

use distributed::bus::{CelldQueueEnvelope, CelldQueueHttpPublisher, CelldQueueRelay};
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
use distributed_game::lifecycle::{
    Lifecycle, CONNECTION_LOST, PLAYER_DISCONNECTED, PLAYER_JOINED, PLAYER_LEFT, PLAYER_RECONNECTED,
};
use distributed_game::world::zone_lease::{mount_zone_lease, ZoneLease};
use distributed_game::world::zone_state::{celld_worker, mount_zone_state, ZoneState};
use distributed_game::FactStamp;
use mobpit_crawler::{
    Crawler, MobKilledArgs, RecordKill, CRAWLER_KIND, MOB_KILLED_EVENT, PRESENCE_KIND,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use worker::*;

fn rejected(error: impl std::fmt::Display) -> HandlerError {
    HandlerError::Rejected(error.to_string())
}

// ---------------------------------------------------------------- Presence

/// The ordering key of the fact that set a presence entry.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StampKey {
    pub zone_id: String,
    pub activation: u64,
    pub tick: u64,
    pub emitted_at_ms: u64,
    pub seq: u64,
}

impl StampKey {
    fn of(stamp: &FactStamp) -> Self {
        Self {
            zone_id: stamp.zone_id.clone(),
            activation: stamp.activation,
            tick: stamp.tick,
            emitted_at_ms: stamp.emitted_at_ms,
            seq: stamp.seq,
        }
    }

    fn stamp(&self) -> FactStamp {
        FactStamp {
            zone_id: self.zone_id.clone(),
            activation: self.activation,
            tick: self.tick,
            emitted_at_ms: self.emitted_at_ms,
            seq: self.seq,
            ..FactStamp::default()
        }
    }
}

/// One presence transition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresenceChange {
    pub zone: String,
    /// `online`, `away` or `offline`.
    pub status: String,
    pub since_ms: u64,
    pub stamp: StampKey,
}

#[derive(Clone, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct PlayerPresence {
    entity: Entity,
    current: Option<PresenceChange>,
}

#[sourced(
    entity,
    aggregate_type = "PlayerPresence",
    events = "PlayerPresenceEvent"
)]
impl PlayerPresence {
    #[event("presence.changed", version = 1)]
    fn changed(&mut self, principal: String, change: PresenceChange) {
        if self.entity.id().is_empty() {
            self.entity.set_id(principal);
        }
        self.current = Some(change);
    }
}

/// Any lifecycle fact input (the union of `distributed_game::lifecycle`'s
/// inputs; decoded per command with `Lifecycle::decode`).
#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct LifecycleInput {
    pub zone: String,
    pub via: Option<String>,
    pub from_zone: Option<String>,
    pub reason: Option<String>,
    pub grace_ms: Option<u64>,
    pub cause: Option<String>,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct PresenceApplied {
    /// False when a later-emitted fact already set the entry.
    pub applied: bool,
}

/// One mount per lifecycle command (handlers are plain `async fn`s).
macro_rules! lifecycle_command {
    ($mount:ident, $handler:ident, $command:expr) => {
        struct $mount;
        impl<D> PortableCommand<D> for $mount
        where
            D: CausalRouteDependencies<Aggregate = PlayerPresence> + Send + Sync + 'static,
        {
            fn install(self, routes: Routes<D>) -> Routes<D> {
                routes
                    .typed_command(typed_command::<LifecycleInput, Succeeded<PresenceApplied>>(
                        $command,
                    ))
                    .handle($handler)
            }
        }

        async fn $handler(
            ctx: &CausalCommandContext<'_, PlayerPresence>,
            input: LifecycleInput,
        ) -> std::result::Result<PreparedCommand<Succeeded<PresenceApplied>>, HandlerError> {
            handle_lifecycle(ctx, $command, input).await
        }
    };
}

lifecycle_command!(Joined, handle_joined, PLAYER_JOINED);
lifecycle_command!(Lost, handle_lost, CONNECTION_LOST);
lifecycle_command!(Reconnected, handle_reconnected, PLAYER_RECONNECTED);
lifecycle_command!(Disconnected, handle_disconnected, PLAYER_DISCONNECTED);
lifecycle_command!(Left, handle_left, PLAYER_LEFT);

async fn handle_lifecycle(
    ctx: &CausalCommandContext<'_, PlayerPresence>,
    command: &'static str,
    input: LifecycleInput,
) -> std::result::Result<PreparedCommand<Succeeded<PresenceApplied>>, HandlerError> {
    let principal = ctx.user_id()?.to_string();
    let stamp = session_stamp(ctx.session())
        .ok_or_else(|| rejected("lifecycle facts need a fact stamp"))?;
    let event = Lifecycle::decode(command, &serde_json::to_value(&input).map_err(rejected)?)
        .ok_or_else(|| rejected(format!("malformed `{command}` input")))?;
    let status = match event {
        Lifecycle::Joined(_) | Lifecycle::Reconnected(_) => "online",
        Lifecycle::ConnectionLost(_) => "away",
        Lifecycle::Left(_) | Lifecycle::Disconnected(_) => "offline",
    };
    let repo = ctx.repo();
    let mut presence = match repo.get(&principal).await? {
        Some(presence) => presence,
        None => repo.create(),
    };
    let newer = presence.current.as_ref().is_none_or(|current| {
        current.stamp.stamp().emission_order(&stamp) == std::cmp::Ordering::Less
    });
    if newer {
        presence
            .changed(
                principal,
                PresenceChange {
                    zone: event.zone().to_string(),
                    status: status.to_string(),
                    since_ms: stamp.emitted_at_ms,
                    stamp: StampKey::of(&stamp),
                },
            )
            .map_err(rejected)?;
    }
    repo.commit(presence)?
        .succeeded(PresenceApplied { applied: newer })
}

// ---------------------------------------------------------------- The Freeze

/// `freeze.gem_collected`.
pub const GEM_COLLECTED: &str = "freeze.gem_collected";
/// `freeze.content_applied`.
pub const CONTENT_APPLIED: &str = "freeze.content_applied";

/// One collected gem.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Gem {
    pub zone: String,
    pub spot: u32,
    pub content_version: String,
}

#[derive(Clone, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct GemPouch {
    entity: Entity,
    gems: u32,
    last: Option<Gem>,
}

#[sourced(entity, aggregate_type = "GemPouch", events = "GemPouchEvent")]
impl GemPouch {
    #[event("freeze.gem_recorded", version = 1)]
    fn recorded(&mut self, principal: String, gem: Gem) {
        if self.entity.id().is_empty() {
            self.entity.set_id(principal);
        }
        self.gems += 1;
        self.last = Some(gem);
    }
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct GemInput {
    pub spot: u32,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct Counted {
    pub count: u32,
}

struct CollectGem;
impl<D> PortableCommand<D> for CollectGem
where
    D: CausalRouteDependencies<Aggregate = GemPouch> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<GemInput, Succeeded<Counted>>(GEM_COLLECTED))
            .handle(handle_gem)
    }
}

async fn handle_gem(
    ctx: &CausalCommandContext<'_, GemPouch>,
    input: GemInput,
) -> std::result::Result<PreparedCommand<Succeeded<Counted>>, HandlerError> {
    let principal = ctx.user_id()?.to_string();
    let stamp =
        session_stamp(ctx.session()).ok_or_else(|| rejected("gem facts need a fact stamp"))?;
    let repo = ctx.repo();
    let mut pouch = match repo.get(&principal).await? {
        Some(pouch) => pouch,
        None => repo.create(),
    };
    pouch
        .recorded(
            principal,
            Gem {
                zone: stamp.zone_id,
                spot: input.spot,
                content_version: stamp.content_version,
            },
        )
        .map_err(rejected)?;
    let count = pouch.gems;
    repo.commit(pouch)?.succeeded(Counted { count })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentSwitch {
    pub from: String,
    pub to: String,
    pub tick: u64,
}

#[derive(Clone, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct ContentLog {
    entity: Entity,
    switches: Vec<ContentSwitch>,
}

#[sourced(entity, aggregate_type = "ContentLog", events = "ContentLogEvent")]
impl ContentLog {
    #[event("freeze.content_recorded", version = 1)]
    fn recorded(&mut self, zone: String, switch: ContentSwitch) {
        if self.entity.id().is_empty() {
            self.entity.set_id(zone);
        }
        self.switches.push(switch);
    }
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct ContentInput {
    pub from: String,
    pub to: String,
}

struct ApplyContent;
impl<D> PortableCommand<D> for ApplyContent
where
    D: CausalRouteDependencies<Aggregate = ContentLog> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<ContentInput, Succeeded<Counted>>(
                CONTENT_APPLIED,
            ))
            .handle(handle_content)
    }
}

async fn handle_content(
    ctx: &CausalCommandContext<'_, ContentLog>,
    input: ContentInput,
) -> std::result::Result<PreparedCommand<Succeeded<Counted>>, HandlerError> {
    let stamp =
        session_stamp(ctx.session()).ok_or_else(|| rejected("content facts need a fact stamp"))?;
    let repo = ctx.repo();
    // The zone's own log: the shard is the stamped zone.
    let mut log = match repo.get(&stamp.zone_id).await? {
        Some(log) => log,
        None => repo.create(),
    };
    log.recorded(
        stamp.zone_id.clone(),
        ContentSwitch {
            from: input.from,
            to: input.to,
            tick: stamp.tick,
        },
    )
    .map_err(rejected)?;
    let count = log.switches.len() as u32;
    repo.commit(log)?.succeeded(Counted { count })
}

async fn read_pouch(cell: &AggregateCell<GemPouch>) -> std::result::Result<Value, String> {
    let pouch = cell.load().await.map_err(|error| error.to_string())?;
    Ok(json!({
        "gems": pouch.as_ref().map_or(0, |p| p.gems),
        "last": pouch.and_then(|p| p.last),
    }))
}

async fn read_content_log(cell: &AggregateCell<ContentLog>) -> std::result::Result<Value, String> {
    let log = cell.load().await.map_err(|error| error.to_string())?;
    Ok(json!({ "switches": log.map(|l| l.switches).unwrap_or_default() }))
}

#[durable_object]
pub struct GemPouchCell {
    cell: AggregateCell<GemPouch>,
    env: Env,
}

impl DurableObject for GemPouchCell {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let cell = AggregateCell::<GemPouch>::from_state_with_snapshots(state, 50)
            .expect("gem pouch cell identity")
            .mount(CollectGem)
            .with_celld_outbox(outbox(&env));
        Self { cell, env }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        route(&self.env, &self.cell, req, read_pouch, fact_session).await
    }

    async fn alarm(&self) -> Result<Response> {
        alarm(&self.env, &self.cell).await
    }
}

#[durable_object]
pub struct ContentLogCell {
    cell: AggregateCell<ContentLog>,
    env: Env,
}

impl DurableObject for ContentLogCell {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let cell = AggregateCell::<ContentLog>::from_state_with_snapshots(state, 50)
            .expect("content log cell identity")
            .mount(ApplyContent)
            .with_celld_outbox(outbox(&env));
        Self { cell, env }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        route(&self.env, &self.cell, req, read_content_log, fact_session).await
    }

    async fn alarm(&self) -> Result<Response> {
        alarm(&self.env, &self.cell).await
    }
}

// ---------------------------------------------------------------- Hosting

/// Mob Pit cells run only facts: the principal and the stamp, from the
/// trusted headers of the authenticated internal path.
fn fact_session(header: &dyn Fn(&str) -> Option<String>) -> Option<Session> {
    fact_session_from_headers(header)
}

/// Zone lease cells: the zone server's identity and role (the handlers
/// require `zone-host`).
fn zone_host_session(header: &dyn Fn(&str) -> Option<String>) -> Option<Session> {
    let mut session = Session::new();
    for key in [distributed::USER_ID_KEY, distributed::microsvc::ROLE_KEY] {
        if let Some(value) = header(key) {
            session.set(key, value);
        }
    }
    Some(session)
}

type Sessions = fn(&dyn Fn(&str) -> Option<String>) -> Option<Session>;

async fn dispatch<A>(
    env: &Env,
    cell: &AggregateCell<A>,
    command: &str,
    req: &mut Request,
    sessions: Sessions,
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
    let Some(session) = sessions(&header) else {
        return error_response(CellDispatchError::Forbidden);
    };
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

/// `GET /{kind}/{shard}` → `read(cell)`; `POST /{kind}/{shard}/{command}`.
async fn route<A, F>(
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
            dispatch(env, cell, &command, &mut req, sessions).await
        }
        _ => json_status(json!({ "error": "not found" }), 404),
    }
}

async fn read_crawler(cell: &AggregateCell<Crawler>) -> std::result::Result<Value, String> {
    let state = cell.load().await.map_err(|error| error.to_string())?;
    let mut kills = Vec::new();
    for record in cell.history().await.map_err(|error| error.to_string())? {
        if record.event_name != MOB_KILLED_EVENT {
            continue;
        }
        let (crawler, name, mob, tier, xp): MobKilledArgs =
            record.decode().map_err(|error| error.to_string())?;
        let at_ms = record
            .timestamp
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis() as u64);
        kills.push(json!({
            "sequence": record.sequence, "atMs": at_ms, "crawler": crawler,
            "name": name, "mob": mob, "tier": tier, "xp": xp,
        }));
    }
    Ok(json!({
        "state": state.map(|c| json!({
            "name": c.name, "xp": c.xp, "kills": c.kills, "level": c.level,
            "version": c.version(),
        })),
        "kills": kills,
    }))
}

async fn read_presence(cell: &AggregateCell<PlayerPresence>) -> std::result::Result<Value, String> {
    let presence = cell.load().await.map_err(|error| error.to_string())?;
    Ok(json!({ "current": presence.and_then(|p| p.current) }))
}

fn outbox(env: &Env) -> CelldOutbox {
    CelldOutbox::from_env(env, "OUTBOX").expect("OUTBOX Queue binding")
}

#[durable_object]
pub struct CrawlerCell {
    cell: AggregateCell<Crawler>,
    env: Env,
}

impl DurableObject for CrawlerCell {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let cell = AggregateCell::<Crawler>::from_state_with_snapshots(state, 100)
            .expect("crawler cell identity")
            .mount(RecordKill)
            .with_celld_outbox(outbox(&env));
        Self { cell, env }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        route(&self.env, &self.cell, req, read_crawler, fact_session).await
    }

    async fn alarm(&self) -> Result<Response> {
        alarm(&self.env, &self.cell).await
    }
}

#[durable_object]
pub struct PresenceCell {
    cell: AggregateCell<PlayerPresence>,
    env: Env,
}

impl DurableObject for PresenceCell {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let cell = AggregateCell::<PlayerPresence>::from_state_with_snapshots(state, 50)
            .expect("presence cell identity")
            .mount(Joined)
            .mount(Lost)
            .mount(Reconnected)
            .mount(Disconnected)
            .mount(Left)
            .with_celld_outbox(outbox(&env));
        Self { cell, env }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        route(&self.env, &self.cell, req, read_presence, fact_session).await
    }

    async fn alarm(&self) -> Result<Response> {
        alarm(&self.env, &self.cell).await
    }
}

async fn alarm<A>(env: &Env, cell: &AggregateCell<A>) -> Result<Response>
where
    A: Aggregate + Send + Sync + 'static,
{
    let wake = cell
        .alarm(env)
        .await
        .map_err(|error| Error::RustError(error.to_string()))?;
    for error in &wake.drain.deferred {
        worker::console_error!("drain deferred: {error}");
    }
    Response::ok("ok")
}

#[durable_object]
pub struct ZoneLeaseCell {
    cell: AggregateCell<ZoneLease>,
    env: Env,
}

impl DurableObject for ZoneLeaseCell {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        // Renewals are internal events: snapshots keep loads short.
        let cell = mount_zone_lease(
            AggregateCell::<ZoneLease>::from_state_with_snapshots(state, 50)
                .expect("zone lease cell identity"),
        )
        .with_celld_outbox(outbox(&env));
        Self { cell, env }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        route(&self.env, &self.cell, req, read_lease, zone_host_session).await
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

async fn read_lease(cell: &AggregateCell<ZoneLease>) -> std::result::Result<Value, String> {
    let lease = cell.load().await.map_err(|error| error.to_string())?;
    Ok(json!({
        "holder": lease.as_ref().and_then(|l| l.holder().map(str::to_string)),
        "token": lease.as_ref().map(|l| l.token()),
    }))
}

#[durable_object]
pub struct ZoneStateCell {
    cell: AggregateCell<ZoneState>,
    env: Env,
}

impl DurableObject for ZoneStateCell {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        // Every save replaces the value: snapshot each commit.
        let cell = mount_zone_state(
            AggregateCell::<ZoneState>::from_state_with_snapshots(state, 1)
                .expect("zone state cell identity"),
        )
        .with_celld_outbox(outbox(&env));
        Self { cell, env }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        if let Err(error) = authenticate(&req, &self.env) {
            return error_response(error);
        }
        let bucket = celld_worker::bucket(&self.env, "ZONE_SNAPSHOTS")?;
        celld_worker::fetch(&self.cell, &bucket, req).await
    }

    async fn alarm(&self) -> Result<Response> {
        alarm(&self.env, &self.cell).await
    }
}

#[event(fetch)]
async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    console_error_panic_hook::set_once();
    let url = req.url()?;
    let path = url.path();
    if path == "/" || path == "/health" {
        return Response::ok("mobpit cells\n");
    }
    if let Err(error) = authenticate(&req, &env) {
        return error_response(error);
    }
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    let binding = match parts.first().copied() {
        Some(CRAWLER_KIND) => "CRAWLER",
        Some(PRESENCE_KIND) => "PRESENCE",
        Some("gem-pouch") => "GEM_POUCH",
        Some("content-log") => "CONTENT_LOG",
        Some("zone-lease") => "ZONE_LEASE",
        Some("zone-state") => "ZONE_STATE",
        _ => return json_status(json!({ "error": "unknown cell kind" }), 404),
    };
    let Some(shard) = parts.get(1) else {
        return json_status(json!({ "error": "missing shard" }), 400);
    };
    let shard = percent_decode(shard).ok_or_else(|| Error::RustError("bad shard".into()))?;
    let stub = env
        .durable_object(binding)?
        .id_from_name(&shard)?
        .get_stub()?;
    stub.fetch_with_request(req).await
}

/// Outbox Queue consumer: relay each canonical message to the zone server.
#[event(queue)]
pub async fn queue(batch: MessageBatch<CelldQueueEnvelope>, env: Env, _ctx: Context) -> Result<()> {
    console_error_panic_hook::set_once();
    let relay = match relay(&env) {
        Ok(relay) => relay,
        Err(error) => {
            // Keep the messages: they are retried until a relay is configured.
            worker::console_error!("mobpit outbox relay unavailable: {error}");
            batch.retry_all();
            return Ok(());
        }
    };
    for delivery in batch.messages()? {
        match relay.relay(delivery.body().clone()).await {
            Ok(()) => delivery.ack(),
            Err(error) => {
                worker::console_error!(
                    "mobpit outbox relay failed for {}: {}",
                    delivery.id(),
                    error
                );
                delivery.retry();
            }
        }
    }
    Ok(())
}

fn relay(env: &Env) -> std::result::Result<CelldQueueRelay<CelldQueueHttpPublisher>, String> {
    let var = |name: &str| {
        env.var(name)
            .map(|value| value.to_string())
            .or_else(|_| env.secret(name).map(|value| value.to_string()))
            .map_err(|_| format!("{name} is required"))
    };
    let endpoint = var("CELLD_QUEUE_RELAY_URL")?;
    let secret = var(CELL_INTERNAL_SECRET_ENV)?;
    // `http://127.0.0.1` relays are dev-only; anything else must be HTTPS.
    let local =
        endpoint.starts_with("http://127.0.0.1") || endpoint.starts_with("http://localhost");
    let publisher = if local {
        CelldQueueHttpPublisher::new_local_test(endpoint, &secret)
    } else {
        CelldQueueHttpPublisher::new(endpoint)
    }
    .map_err(|error| error.to_string())?
    .with_header(CELL_INTERNAL_SECRET_HEADER, secret);
    Ok(CelldQueueRelay::new(publisher))
}

fn percent_decode(segment: &str) -> Option<String> {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            out.push(u8::from_str_radix(segment.get(i + 1..i + 3)?, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
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
