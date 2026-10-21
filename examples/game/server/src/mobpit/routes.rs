//! **Dev-only** Mob Pit routes (mounted with `--dev`). None of them takes an
//! identity or a reward from the caller.
//!
//! - `POST /dev/ticket` `{zone, name?, role?, guest?}` → a single-use room
//!   ticket for a **server-generated** guest principal, signed with the
//!   rooms' ticket secret (see `crate::common::dev` for the fields). Only
//!   `pit.<x>.<y>` rooms. A real game mints tickets from its sign-in (the
//!   SvelteKit demo does).
//! - `POST /dev/mobpit/start` `{guest?}` → `{zone, resumed}`: the room a
//!   returning guest starts in — its last room from its presence cell (the
//!   host's verified lifecycle facts), else the entrance. The guest token is
//!   verified like `/dev/ticket`'s; the caller never names a principal.
//! - `GET /dev/mobpit/rooms` — every known room's status (`active` /
//!   `dormant` / `hibernated`, players, restored, debug node), the
//!   `zone_status` read model projected from the rooms' activity cells.
//! - `GET /dev/mobpit/stats` — boot id, celld, board backend.
//! - `GET /dev/mobpit/crawlers` — every crawler in the directory.
//! - `GET /dev/mobpit/leaderboard[?member=principal]` — the top-N snapshot,
//!   plus rank / neighbours of `member` on both boards.
//! - `POST /dev/mobpit/leaderboard/rebuild` — rebuild both boards from the
//!   event history of every crawler in the directory.
//! - `GET /dev/mobpit/presence?principal=` — the player's presence cell.
//! - `GET /dev/netsim` / `POST /dev/netsim` `{latencyMs, jitterMs, loss}` —
//!   read / replace the network simulation of **every** room (dev settings
//!   object on celld, read by each room within a second; applied per
//!   socket in both directions, so the RTT grows by 2 × latency).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use distributed_game::net::TicketKey;
use distributed_game::netsim::{NetSim, NetSimJson, NETSIM_SHARD};
use distributed_game::{Role, TicketClaims};
use serde_json::{json, Value};

use super::world::MobPitWorld;
use crate::common::dev::{TicketRequest, TicketResponse, TICKET_TTL_SECS};
use crate::common::guests;

type ApiResult = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn internal(error: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": error.to_string() })),
    )
}

fn bad_request(message: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": message })))
}

#[derive(Clone)]
struct Dev {
    world: Arc<MobPitWorld>,
    key: TicketKey,
}

/// The router.
pub fn routes(world: Arc<MobPitWorld>, key: TicketKey) -> Router {
    Router::new()
        .route("/dev/ticket", post(ticket))
        .route("/dev/mobpit/start", post(start))
        .route("/dev/mobpit/rooms", get(rooms))
        .route("/dev/mobpit/stats", get(stats))
        .route("/dev/mobpit/crawlers", get(crawlers))
        .route("/dev/mobpit/leaderboard", get(leaderboard))
        .route("/dev/mobpit/leaderboard/rebuild", post(rebuild))
        .route("/dev/mobpit/presence", get(presence))
        .route("/dev/netsim", get(get_netsim).post(set_netsim))
        .with_state(Dev { world, key })
}

async fn ticket(
    State(dev): State<Dev>,
    Json(request): Json<TicketRequest>,
) -> Result<Json<TicketResponse>, (StatusCode, Json<Value>)> {
    if !super::hosts_zone(&request.zone) {
        return Err(bad_request("not a Mob Pit room (pit.<x>.<y>)"));
    }
    let role = match request.role.as_deref() {
        None | Some("player") => Role::Player,
        Some("observer") => Role::Observer,
        Some(_) => return Err(bad_request("role must be player or observer")),
    };
    // Identity: a verified guest token, else a brand-new principal.
    let principal = request
        .guest
        .as_deref()
        .and_then(|token| guests::verify_guest_token(&dev.key, token))
        .unwrap_or_else(guests::new_principal);
    let name = request
        .name
        .as_deref()
        .map(guests::sanitize_name)
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| format!("Guest {}", principal.get(6..10).unwrap_or("")));
    let expires_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
        + TICKET_TTL_SECS;
    let ticket = dev.key.sign(
        &TicketClaims::new(
            principal.clone(),
            request.zone.clone(),
            expires_at,
            guests::random_hex(24),
            role,
        )
        .with_name(name.clone()),
    );
    Ok(Json(TicketResponse {
        ticket,
        guest: guests::guest_token(&dev.key, &principal),
        principal,
        name,
        zone: request.zone,
        expires_at,
    }))
}

/// `POST /dev/mobpit/start` body.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StartRequest {
    /// Guest token from an earlier `/dev/ticket` response.
    #[serde(default)]
    guest: Option<String>,
}

async fn start(State(dev): State<Dev>, Json(request): Json<StartRequest>) -> Json<Value> {
    let principal = request
        .guest
        .as_deref()
        .and_then(|token| guests::verify_guest_token(&dev.key, token));
    let last = match principal {
        Some(principal) => dev.world.last_room(&principal).await,
        None => None,
    };
    Json(json!({
        "resumed": last.is_some(),
        "zone": last.unwrap_or_else(|| mobpit_rooms::ENTRANCE.to_string()),
    }))
}

async fn rooms(State(dev): State<Dev>) -> ApiResult {
    Ok(Json(
        json!({ "rooms": dev.world.rooms().await.map_err(internal)? }),
    ))
}

async fn stats(State(dev): State<Dev>) -> Json<Value> {
    let world = &dev.world;
    Json(json!({
        "boot": world.boot(),
        "dataDir": world.data_dir().display().to_string(),
        "celld": world.cells().url(),
        "board": world.board().backend(),
    }))
}

async fn crawlers(State(dev): State<Dev>) -> ApiResult {
    Ok(Json(
        json!({ "crawlers": dev.world.crawlers().await.map_err(internal)? }),
    ))
}

async fn leaderboard(
    State(dev): State<Dev>,
    Query(query): Query<HashMap<String, String>>,
) -> ApiResult {
    let board = dev.world.board();
    let snapshot = (*board.snapshot()).clone();
    let me = match query.get("member").filter(|m| !m.is_empty()) {
        Some(member) => board.around(member, 2).await.map_err(internal)?,
        None => Value::Null,
    };
    Ok(Json(json!({ "snapshot": snapshot, "me": me })))
}

async fn rebuild(State(dev): State<Dev>) -> ApiResult {
    let report = dev.world.rebuild_board().await.map_err(internal)?;
    Ok(Json(serde_json::to_value(report).map_err(internal)?))
}

async fn presence(
    State(dev): State<Dev>,
    Query(query): Query<HashMap<String, String>>,
) -> ApiResult {
    let principal = query
        .get("principal")
        .filter(|p| !p.is_empty())
        .ok_or_else(|| bad_request("principal is required"))?;
    let current = dev
        .world
        .cells()
        .presence(principal)
        .await
        .map_err(internal)?;
    Ok(Json(json!({ "principal": principal, "presence": current })))
}

async fn get_netsim(State(dev): State<Dev>) -> Result<Json<NetSimJson>, (StatusCode, Json<Value>)> {
    let value = dev
        .world
        .cells()
        .dev_settings(NETSIM_SHARD)
        .await
        .map_err(internal)?;
    Ok(Json(serde_json::from_value(value).unwrap_or_default()))
}

async fn set_netsim(
    State(dev): State<Dev>,
    Json(body): Json<NetSimJson>,
) -> Result<Json<NetSimJson>, (StatusCode, Json<Value>)> {
    // Bounded and normalized (what the rooms will apply).
    let applied = NetSim::to_json(body.to_sim());
    let value = serde_json::to_value(applied).map_err(internal)?;
    dev.world
        .cells()
        .set_dev_settings(NETSIM_SHARD, &value)
        .await
        .map_err(internal)?;
    Ok(Json(applied))
}
