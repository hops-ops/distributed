//! The game demos' durable cells on celld, and Mob Pit's rooms.
//!
//! - `GET /zones/{room}` (WebSocket) and `/zones/{room}/info`: `PitRoom`,
//!   one Durable Object per Mob Pit room (`pit.<x>.<y>`), hosted by
//!   `distributed_game::celld_zone` (`mobpit_rooms::MobPit`). celld places
//!   each room; its kill and lifecycle facts reach the Crawler and presence
//!   cells below object to object, exactly once.
//! - `zone-dev/{shard}` (`ZoneDevObject`, dev only): settings every room
//!   reads (`ZONE_DEV_SETTINGS=1`): the network simulation of the HUD's
//!   sliders.
//! - `zone-activity/{room}` (`ZoneActivityObject`): each room's lifecycle
//!   (active / dormant / hibernated); its `zone.activity_changed` events
//!   leave through the outbox Queue for the zone status read model.
//!
//! - `hood/{hx.hy}`, `borough/{bx.by}`, `city/city` (`HoodCell`,
//!   `BoroughCell`, `CityCell`): Mob Pit's world tiers (`mobpit_tiers`). A
//!   boss room's zone fact raises its hood; the Queue consumer below runs
//!   the saga that ripples hood events into the borough and borough events
//!   into the city (idempotent commands, ids derived from the event ids).
//!   Rooms watch them (`CellWatchers`): each change is pushed to the rooms
//!   holding a watch lease, which re-read every ~45 s (no polling).
//!
//! - `content-rollout/world` (`ContentRolloutObject`): the world's live
//!   content (`distributed_game::world::content_rollout`). The services'
//!   admin route starts a rollout (an `admin:*` session it built after
//!   checking the admin secret); the cell freezes, applies and resumes on
//!   its own schedule and every room follows it (`ZONE_CONTENT_ROLLOUT`).
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
//!
//! Every route requires `DISTRIBUTED_INTERNAL_SECRET`. Mob Pit commands also
//! require the trusted fact session (principal + `FactStamp` headers,
//! rebuilt with `fact_session_from_headers`), so nothing but a zone server's
//! fact worker can run them; tier saga steps require the worker's own saga
//! session (`mobpit_tiers::SAGA_PRINCIPAL`).
//!
//! The Queue consumer relays each outbox message to `CELLD_QUEUE_RELAY_URL`
//! (the zone server's authenticated `/internal/celld-queue/relay`), which
//! publishes it onto the projection bus read by `mobpit-service`. Delivery
//! is at-least-once with stable message ids; the projector deduplicates.

use distributed::bus::{CelldQueueEnvelope, CelldQueueHttpPublisher, CelldQueueRelay};
use distributed::cell_host::{
    AggregateCell, CellDispatchError, CelldOutbox, InternalHttpSecret, CELL_INTERNAL_SECRET_ENV,
    CELL_INTERNAL_SECRET_HEADER, CELL_PRINCIPAL_PARTITION_HEADER, CELL_SERVICE_ID_HEADER,
};
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, CausalRouteDependencies, HandlerError, PortableCommand, Routes, Session,
};
use distributed::{sourced, Aggregate, Entity};
use distributed_game::cell::{fact_session_from_headers, session_stamp};
use distributed_game::celld_zone::{
    forward_to_zone, serve_cell, CellWatchers, ContentRolloutCell, ZoneActivityCell,
    ZoneDevSettings,
};
use distributed_game::lifecycle::{
    Lifecycle, CONNECTION_LOST, PLAYER_DISCONNECTED, PLAYER_JOINED, PLAYER_LEFT, PLAYER_RECONNECTED,
};
use distributed_game::world::content_rollout::{CONTENT_ADMIN_ROLE, CONTENT_ROLLOUT_KIND};
use distributed_game::world::zone_activity::ZONE_ACTIVITY_KIND;
use distributed_game::FactStamp;
use mobpit_crawler::{
    Crawler, MobKilledArgs, RecordKill, CRAWLER_KIND, MOB_KILLED_EVENT, PRESENCE_KIND,
};
use mobpit_tiers::{
    mount_borough, mount_city, mount_hood, Borough, BoroughRead, City, CityRead, Hood, HoodRead,
    BOROUGH_KIND, CITY_KIND, HOOD_KIND, SAGA_PRINCIPAL,
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

// ---------------------------------------------------------------- Hosting

/// Mob Pit cells run only facts: the principal and the stamp, from the
/// trusted headers of the authenticated internal path.
fn fact_session(header: &dyn Fn(&str) -> Option<String>) -> Option<Session> {
    fact_session_from_headers(header)
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
        serve_cell(&self.env, &self.cell, req, read_crawler, fact_session).await
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
        serve_cell(&self.env, &self.cell, req, read_presence, fact_session).await
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

// ---------------------------------------------------------------- Rooms

distributed_game::celld_zone_object!(PitRoom, mobpit_rooms::MobPit);

// ---------------------------------------------------------------- Tiers

/// Tier saga steps run only for the worker's own session.
fn saga_session(header: &dyn Fn(&str) -> Option<String>) -> Option<Session> {
    let principal = header(distributed::USER_ID_KEY)?;
    (principal == SAGA_PRINCIPAL).then(|| {
        let mut session = Session::new();
        session.set(distributed::USER_ID_KEY, principal);
        session
    })
}

/// A borough hears the saga (its hoods) and its rooms' own facts (a room
/// woke: the world event cycle); each command checks which it accepts.
fn borough_session(header: &dyn Fn(&str) -> Option<String>) -> Option<Session> {
    saga_session(header).or_else(|| fact_session(header))
}

async fn read_hood(cell: &AggregateCell<Hood>) -> std::result::Result<Value, String> {
    let hood = cell.load().await.map_err(|error| error.to_string())?;
    serde_json::to_value(HoodRead::of(hood.as_ref())).map_err(|error| error.to_string())
}

async fn read_borough(cell: &AggregateCell<Borough>) -> std::result::Result<Value, String> {
    let borough = cell.load().await.map_err(|error| error.to_string())?;
    serde_json::to_value(BoroughRead::of(borough.as_ref())).map_err(|error| error.to_string())
}

async fn read_city(cell: &AggregateCell<City>) -> std::result::Result<Value, String> {
    let city = cell.load().await.map_err(|error| error.to_string())?;
    serde_json::to_value(CityRead::of(city.as_ref())).map_err(|error| error.to_string())
}

/// A hood: its boss rooms' facts and its own respawn schedule.
#[durable_object]
pub struct HoodCell {
    cell: AggregateCell<Hood>,
    env: Env,
    /// Rooms watching it (pushed after every change).
    watchers: CellWatchers,
}

impl DurableObject for HoodCell {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let watchers = CellWatchers::new(HOOD_KIND, &state);
        let cell = mount_hood(
            AggregateCell::<Hood>::from_state_with_snapshots(state, 50)
                .expect("hood cell identity"),
        )
        .with_celld_outbox(outbox(&env));
        Self {
            cell,
            env,
            watchers,
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        self.watchers
            .serve(&self.env, &self.cell, req, read_hood, fact_session)
            .await
    }

    async fn alarm(&self) -> Result<Response> {
        self.watchers.alarm(&self.env, &self.cell, read_hood).await
    }
}

/// A borough: its hoods as the saga reports them.
#[durable_object]
pub struct BoroughCell {
    cell: AggregateCell<Borough>,
    env: Env,
    /// Rooms watching it (pushed after every change).
    watchers: CellWatchers,
}

impl DurableObject for BoroughCell {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let watchers = CellWatchers::new(BOROUGH_KIND, &state);
        let cell = mount_borough(
            AggregateCell::<Borough>::from_state_with_snapshots(state, 50)
                .expect("borough cell identity"),
        )
        .with_celld_outbox(outbox(&env));
        Self {
            cell,
            env,
            watchers,
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        self.watchers
            .serve(&self.env, &self.cell, req, read_borough, borough_session)
            .await
    }

    async fn alarm(&self) -> Result<Response> {
        self.watchers
            .alarm(&self.env, &self.cell, read_borough)
            .await
    }
}

/// The city: its boroughs as the saga reports them.
#[durable_object]
pub struct CityCell {
    cell: AggregateCell<City>,
    env: Env,
    /// Rooms watching it (pushed after every change).
    watchers: CellWatchers,
}

impl DurableObject for CityCell {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let watchers = CellWatchers::new(CITY_KIND, &state);
        let cell = mount_city(
            AggregateCell::<City>::from_state_with_snapshots(state, 50)
                .expect("city cell identity"),
        )
        .with_celld_outbox(outbox(&env));
        Self {
            cell,
            env,
            watchers,
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        self.watchers
            .serve(&self.env, &self.cell, req, read_city, saga_session)
            .await
    }

    async fn alarm(&self) -> Result<Response> {
        self.watchers.alarm(&self.env, &self.cell, read_city).await
    }
}

/// The tier saga's next step for an outbox message (hood → borough →
/// city), dispatched object to object as an idempotent command whose id
/// derives from the message id.
async fn saga(env: &Env, envelope: &CelldQueueEnvelope) -> std::result::Result<(), String> {
    let message = &envelope.message;
    let Some(step) = mobpit_tiers::saga_step(message.name(), message.payload()) else {
        return Ok(());
    };
    let id = message.id().ok_or("tier event without an id")?;
    let secret = env
        .secret(CELL_INTERNAL_SECRET_ENV)
        .map(|v| v.to_string())
        .or_else(|_| env.var(CELL_INTERNAL_SECRET_ENV).map(|v| v.to_string()))
        .map_err(|_| "internal secret is required".to_string())?;
    let headers = Headers::new();
    let set = |name: &str, value: &str| headers.set(name, value).map_err(|e| e.to_string());
    set(CELL_INTERNAL_SECRET_HEADER, &secret)?;
    set(CELL_SERVICE_ID_HEADER, "mobpit-tiers")?;
    set(CELL_PRINCIPAL_PARTITION_HEADER, SAGA_PRINCIPAL)?;
    set(distributed::USER_ID_KEY, SAGA_PRINCIPAL)?;
    set("content-type", "application/json")?;
    let body = json!({
        "commandId": distributed_game::cell::command_id_for("mobpit-tiers", id),
        "input": step.input,
    });
    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_headers(headers)
        .with_body(Some(body.to_string().into()));
    let url = format!("https://cell/{}/{}/{}", step.kind, step.shard, step.command);
    let request = Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
    let binding = match step.kind {
        BOROUGH_KIND => "BOROUGH",
        _ => "CITY",
    };
    let namespace = env.durable_object(binding).map_err(|e| e.to_string())?;
    let stub = namespace
        .id_from_name(&step.shard)
        .and_then(|id| id.get_stub())
        .map_err(|e| e.to_string())?;
    let mut response = stub
        .fetch_with_request(request)
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status_code();
    if (200..300).contains(&status) {
        Ok(())
    } else {
        let body = response.text().await.unwrap_or_default();
        // A refused step (bad input) never succeeds on retry: drop it.
        if status == 400 || status == 403 || status == 422 {
            worker::console_error!("tier saga step {} refused: {status} {body}", step.command);
            Ok(())
        } else {
            Err(format!("{} → HTTP {status}: {body}", step.command))
        }
    }
}

// ---------------------------------------------------------------- Content

/// Principal prefix of the services' admin sessions.
pub const ADMIN_PREFIX: &str = "admin:";

/// A rollout starts only for an admin session the services built after
/// checking the admin's credential (an `admin:*` principal over the
/// internal path); it gets the content-admin role here. The rollout's own
/// schedule needs no session.
fn admin_session(header: &dyn Fn(&str) -> Option<String>) -> Option<Session> {
    let principal = header(distributed::USER_ID_KEY)?;
    principal.starts_with(ADMIN_PREFIX).then(|| {
        let mut session = Session::new();
        session.set(distributed::USER_ID_KEY, principal);
        session.set(distributed::ROLE_KEY, CONTENT_ADMIN_ROLE);
        session
    })
}

/// The world's live content (`content-rollout/world`): every room follows
/// it (`ZONE_CONTENT_ROLLOUT`).
#[durable_object]
pub struct ContentRolloutObject {
    cell: ContentRolloutCell,
}

impl DurableObject for ContentRolloutObject {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let outbox = outbox(&env);
        Self {
            cell: ContentRolloutCell::new(state, env, outbox, admin_session),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        self.cell.fetch(req).await
    }

    async fn alarm(&self) -> Result<Response> {
        self.cell.alarm().await
    }
}

/// The rooms' dev settings (`zone-dev/{shard}`: the network simulation the
/// rooms apply, written by the services' `/dev/netsim`).
#[durable_object]
pub struct ZoneDevObject {
    dev: ZoneDevSettings,
}

impl DurableObject for ZoneDevObject {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        Self {
            dev: ZoneDevSettings::new(state, env),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        self.dev.fetch(req).await
    }
}

#[durable_object]
pub struct ZoneActivityObject {
    cell: ZoneActivityCell,
}

impl DurableObject for ZoneActivityObject {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let outbox = outbox(&env);
        Self {
            cell: ZoneActivityCell::new(state, env, outbox),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        self.cell.fetch(req).await
    }

    async fn alarm(&self) -> Result<Response> {
        self.cell.alarm().await
    }
}

#[event(fetch)]
async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    console_error_panic_hook::set_once();
    let url = req.url()?;
    let path = url.path().to_string();
    if path == "/" || path == "/health" {
        return Response::ok("mobpit cells\n");
    }
    // A room is public: its WebSocket admits signed tickets only.
    if path.starts_with("/zones/") {
        return match forward_to_zone(req, &env, "PIT_ROOM", "/zones/").await? {
            Some(response) => Ok(response),
            None => json_status(json!({ "error": "no such room" }), 404),
        };
    }
    if let Err(error) = authenticate(&req, &env) {
        return error_response(error);
    }
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    let binding = match parts.first().copied() {
        Some(CRAWLER_KIND) => "CRAWLER",
        Some(PRESENCE_KIND) => "PRESENCE",
        Some(ZONE_ACTIVITY_KIND) => "ZONE_ACTIVITY",
        Some(distributed_game::netsim::ZONE_DEV_KIND) => "ZONE_DEV",
        Some(HOOD_KIND) => "HOOD",
        Some(BOROUGH_KIND) => "BOROUGH",
        Some(CITY_KIND) => "CITY",
        Some(CONTENT_ROLLOUT_KIND) => "CONTENT_ROLLOUT",
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
        // The tier saga first: a failed step retries the whole delivery
        // (each step is idempotent).
        if let Err(error) = saga(&env, delivery.body()).await {
            worker::console_error!("tier saga for {} deferred: {error}", delivery.id());
            delivery.retry();
            continue;
        }
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
    // `http://127.0.0.1` relays are dev-only. `CELLD_QUEUE_RELAY_MESH=1`
    // (Hops local: celld and Mob Pit with Istio sidecars) allows HTTP to a
    // cluster Service. Anything else must be HTTPS.
    let local =
        endpoint.starts_with("http://127.0.0.1") || endpoint.starts_with("http://localhost");
    let mesh = env
        .var("CELLD_QUEUE_RELAY_MESH")
        .is_ok_and(|value| value.to_string() == "1");
    let publisher = if local {
        CelldQueueHttpPublisher::new_local_test(endpoint, &secret)
    } else if mesh {
        CelldQueueHttpPublisher::new_mesh(endpoint)
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
