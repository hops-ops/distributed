//! A test zone hosted by `distributed_game::celld_zone` and the cells its
//! facts go to.
//!
//! - `GET /zones/{zone}` (WebSocket): `TallyRoom`, one object per zone
//!   `room-*`. Input `[1, n]` counts `n` hits, each a `tally.hit` fact for
//!   the player's `Tally` cell; `[2, c]` walks through a door into
//!   `room-{c}` (handoff). On join the player gets event 1 with the zone's
//!   total hits (u64 LE); after a hits input, event 2 with the new total.
//!   The zone's snapshot is its total.
//! - `GET /zones/{zone}/info`: the object's debug info.
//! - `GET|POST /tally/{principal}[/command]`, `/zone-activity/{zone}[/command]`:
//!   the cells (internal secret).

use distributed::cell_host::CelldOutbox;
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, CausalRouteDependencies, HandlerError, PortableCommand, Routes,
};
use distributed::{sourced, Entity};
use distributed_game::cell::session_stamp;
use distributed_game::celld_zone::{
    authenticate, cell_alarm, error_response, fact_sessions, forward_to_zone, json_status,
    serve_cell, CelldZone, ObjectEnv, ObjectFactSink, ZoneActivityCell,
};
use distributed_game::command_sink::FactRoute;
use distributed_game::{ConsumePolicy, Ctx, EntitySchema, PlayerId, Zone};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use worker::*;

// ---------------------------------------------------------------- zone

/// One counted hit (the cell counts it; the zone reports which hit).
#[derive(Debug, Clone, Serialize, Deserialize, distributed_game::FactCommand)]
#[fact(command = "tally.hit")]
pub struct TallyHit {
    /// The zone's running hit number.
    pub hit: u64,
    /// Zone it happened in.
    pub zone: String,
}

/// The test zone.
pub struct TallyZone {
    total: u64,
}

pub enum TallyInput {
    Hits(u8),
    Door(u8),
}

impl Zone for TallyZone {
    type Config = ();
    type Input = TallyInput;
    type Timer = ();
    type Message = distributed_game::NoMessages;

    fn schema() -> EntitySchema {
        EntitySchema::new()
    }

    fn on_create(_ctx: &mut Ctx<'_, Self>, _config: ()) -> Self {
        Self { total: 0 }
    }

    fn on_join(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        ctx.send_event(player, 1, self.total.to_le_bytes().to_vec());
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<TallyInput> {
        match payload {
            [1, n] => Some(TallyInput::Hits(*n)),
            [2, c] if c.is_ascii_lowercase() => Some(TallyInput::Door(*c)),
            _ => None,
        }
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        let players: Vec<PlayerId> = ctx.players().map(|p| p.id).collect();
        for player in players {
            for input in ctx.take_inputs(player, ConsumePolicy::Drain) {
                match input.input {
                    TallyInput::Hits(n) => {
                        for _ in 0..n {
                            self.total += 1;
                            let hit = TallyHit {
                                hit: self.total,
                                zone: ctx.zone_id().to_string(),
                            };
                            let total = self.total;
                            let _ = ctx.emit_keyed(player, hit, &[&total]);
                        }
                        ctx.send_event(player, 2, self.total.to_le_bytes().to_vec());
                    }
                    TallyInput::Door(c) => {
                        let target = format!("room-{}", c as char);
                        let _ = ctx.handoff(player, &target, Vec::new());
                    }
                }
            }
        }
    }

    fn hibernate(&self) -> Option<Vec<u8>> {
        Some(self.total.to_string().into_bytes())
    }

    fn on_restore(&mut self, _ctx: &mut Ctx<'_, Self>, state: &[u8], _saved_version: &str) {
        self.total = std::str::from_utf8(state)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
    }
}

impl CelldZone for TallyZone {
    fn object_config(zone_id: &str, _env: &ObjectEnv) -> std::result::Result<(), String> {
        zone_id
            .starts_with("room-")
            .then_some(())
            .ok_or_else(|| format!("no such zone {zone_id}"))
    }

    fn fact_routes(sink: ObjectFactSink) -> ObjectFactSink {
        sink.route(FactRoute::by_principal::<TallyHit>("tally"))
    }
}

distributed_game::celld_zone_object!(TallyRoom, TallyZone);

// ---------------------------------------------------------------- Tally cell

#[derive(Clone, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct Tally {
    entity: Entity,
    hits: Vec<String>,
}

#[sourced(entity, aggregate_type = "Tally", events = "TallyEvent")]
impl Tally {
    #[event("tally.counted", version = 1)]
    fn counted(&mut self, principal: String, hit: String) {
        if self.entity.id().is_empty() {
            self.entity.set_id(principal);
        }
        self.hits.push(hit);
    }
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct HitInput {
    pub hit: u64,
    pub zone: String,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct Counted {
    pub count: u32,
}

struct RecordHit;
impl<D> PortableCommand<D> for RecordHit
where
    D: CausalRouteDependencies<Aggregate = Tally> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<HitInput, Succeeded<Counted>>("tally.hit"))
            .handle(handle_hit)
    }
}

async fn handle_hit(
    ctx: &CausalCommandContext<'_, Tally>,
    input: HitInput,
) -> std::result::Result<PreparedCommand<Succeeded<Counted>>, HandlerError> {
    let principal = ctx.user_id()?.to_string();
    let stamp = session_stamp(ctx.session())
        .ok_or_else(|| HandlerError::Rejected("hits are facts".into()))?;
    let repo = ctx.repo();
    let mut tally = match repo.get(&principal).await? {
        Some(tally) => tally,
        None => repo.create(),
    };
    tally
        .counted(
            principal,
            format!("{}#{}@{}", input.zone, input.hit, stamp.activation),
        )
        .map_err(|e| HandlerError::Rejected(e.to_string()))?;
    let count = tally.hits.len() as u32;
    repo.commit(tally)?.succeeded(Counted { count })
}

async fn read_tally(
    cell: &distributed::cell_host::AggregateCell<Tally>,
) -> std::result::Result<Value, String> {
    let tally = cell.load().await.map_err(|e| e.to_string())?;
    let hits = tally.map(|t| t.hits).unwrap_or_default();
    Ok(json!({ "count": hits.len(), "hits": hits }))
}

#[durable_object]
pub struct TallyCell {
    cell: distributed::cell_host::AggregateCell<Tally>,
    env: Env,
}

impl DurableObject for TallyCell {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let cell =
            distributed::cell_host::AggregateCell::<Tally>::from_state_with_snapshots(state, 50)
                .expect("tally cell identity")
                .mount(RecordHit)
                .with_celld_outbox(CelldOutbox::from_env(&env, "OUTBOX").expect("OUTBOX Queue"));
        Self { cell, env }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        serve_cell(&self.env, &self.cell, req, read_tally, fact_sessions).await
    }

    async fn alarm(&self) -> Result<Response> {
        cell_alarm(&self.env, &self.cell).await
    }
}

#[durable_object]
pub struct ZoneActivityObject {
    cell: ZoneActivityCell,
}

impl DurableObject for ZoneActivityObject {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let outbox = CelldOutbox::from_env(&env, "OUTBOX").expect("OUTBOX Queue");
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

// ---------------------------------------------------------------- entry

#[event(fetch)]
async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    console_error_panic_hook::set_once();
    let path = req.url()?.path().to_string();
    if path == "/health" {
        return Response::ok("ok");
    }
    if path.starts_with("/zones/") {
        return match forward_to_zone(req, &env, "ROOM", "/zones/").await? {
            Some(response) => Ok(response),
            None => Response::error("bad zone", 400),
        };
    }
    if let Err(error) = authenticate(&req, &env) {
        return error_response(error);
    }
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    let binding = match parts.first().copied() {
        Some("tally") => "TALLY",
        Some("zone-activity") => "ZONE_ACTIVITY",
        _ => return json_status(json!({ "error": "unknown cell kind" }), 404),
    };
    let Some(shard) = parts.get(1) else {
        return json_status(json!({ "error": "missing shard" }), 400);
    };
    let shard = worker::js_sys::decode_uri_component(shard)
        .map(String::from)
        .unwrap_or_else(|_| shard.to_string());
    let stub = env
        .durable_object(binding)?
        .id_from_name(&shard)?
        .get_stub()?;
    stub.fetch_with_request(req).await
}
