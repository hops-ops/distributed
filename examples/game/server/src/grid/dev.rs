//! **Dev-only** world map routes of the grid (mounted with `--dev`).
//!
//! - `GET /dev/grid/map?x=0&y=0&r=4` (any host): every cell within `r`
//!   (≤ 8) of `(x, y)` with its owning host and state — `active` (with
//!   tier, players, observers), `hibernated` (a snapshot is stored) or
//!   `cold` (never ran, or ran without being hibernated). Cells owned by
//!   a peer are answered by that peer (`/dev/grid/local`).
//! - `POST /dev/grid/local {zones: [...]}` (admin secret, host to host):
//!   the same facts for this host's cells.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::routing::{get, post};
use axum::{Json, Router};
use distributed_game::net::GameServer;
use distributed_game::world::ZoneStateStore;
use distributed_game::Tier;
use serde_json::{json, Value};

use super::{world, GridZone};
use crate::cluster::World;

/// Largest map radius (cells).
pub const MAX_RADIUS: i32 = 8;

#[derive(Clone)]
struct MapState {
    server: GameServer<GridZone>,
    world: Arc<World>,
}

/// The router.
pub fn routes(server: GameServer<GridZone>, world: Arc<World>) -> Router {
    Router::new()
        .route("/dev/grid/map", get(map))
        .route("/dev/grid/local", post(local))
        .with_state(MapState { server, world })
}

fn query_i32(uri: &Uri, key: &str, default: i32) -> i32 {
    uri.query()
        .unwrap_or_default()
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(k, _)| *k == key)
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(default)
}

fn tier_name(tier: Tier) -> &'static str {
    match tier {
        Tier::Hot => "hot",
        Tier::Idle => "idle",
        Tier::Dormant => "dormant",
    }
}

/// Facts about this host's cells among `zones`, keyed by zone id.
async fn local_cells(state: &MapState, zones: &[String]) -> serde_json::Map<String, Value> {
    let mut out = serde_json::Map::new();
    for zone in zones {
        if state.world.owner(zone) != state.world.host.id || world::parse_zone(zone).is_none() {
            continue;
        }
        let live = if state.server.is_active(zone) {
            let inspect = state.server.inspect(zone, |h| {
                (tier_name(h.tier()), h.player_count(), h.observer_count())
            });
            tokio::time::timeout(Duration::from_millis(300), inspect)
                .await
                .ok()
                .flatten()
        } else {
            None
        };
        let value = match live {
            Some((tier, players, observers)) => json!({
                "state": "active", "tier": tier, "players": players, "observers": observers
            }),
            None => {
                let stored = state.world.state.load(zone).ok().flatten();
                json!({ "state": if stored.is_some() { "hibernated" } else { "cold" } })
            }
        };
        out.insert(zone.clone(), value);
    }
    out
}

async fn map(State(state): State<MapState>, uri: Uri) -> Json<Value> {
    let x = query_i32(&uri, "x", 0).clamp(-world::MAX_CELL, world::MAX_CELL);
    let y = query_i32(&uri, "y", 0).clamp(-world::MAX_CELL, world::MAX_CELL);
    let r = query_i32(&uri, "r", 4).clamp(0, MAX_RADIUS);
    let mut cells = Vec::new();
    for cy in (y - r)..=(y + r) {
        for cx in (x - r)..=(x + r) {
            if cx.abs() <= world::MAX_CELL && cy.abs() <= world::MAX_CELL {
                cells.push((cx, cy, world::zone_id((cx, cy))));
            }
        }
    }
    let zones: Vec<String> = cells.iter().map(|(_, _, z)| z.clone()).collect();
    let mut facts = local_cells(&state, &zones).await;
    let mut totals = serde_json::Map::new();
    totals.insert(
        state.world.host.id.clone(),
        json!(state.server.zone_ids().len()),
    );
    for (peer, reply) in state
        .world
        .ask_peers("/dev/grid/local", &json!({ "zones": zones }))
        .await
    {
        if let Some(cells) = reply["cells"].as_object() {
            facts.extend(cells.clone());
        }
        totals.insert(peer, reply["activeTotal"].clone());
    }
    let count = |host: &str, wanted: &str| {
        zones
            .iter()
            .filter(|z| {
                state.world.owner(z) == host
                    && facts.get(*z).and_then(|f| f["state"].as_str()) == Some(wanted)
            })
            .count()
    };
    let hosts: Vec<Value> = state
        .world
        .hosts
        .iter()
        .map(|host| {
            json!({
                "id": host.id,
                "wsUrl": host.ws_url,
                "reachable": totals.contains_key(&host.id),
                "activeTotal": totals.get(&host.id).cloned().unwrap_or(Value::Null),
                "active": count(&host.id, "active"),
                "hibernated": count(&host.id, "hibernated"),
            })
        })
        .collect();
    let cells: Vec<Value> = cells
        .iter()
        .map(|(cx, cy, zone)| {
            let mut cell = facts
                .get(zone)
                .cloned()
                .unwrap_or_else(|| json!({ "state": "unknown" }));
            cell["x"] = json!(cx);
            cell["y"] = json!(cy);
            cell["zone"] = json!(zone);
            cell["owner"] = json!(state.world.owner(zone));
            cell
        })
        .collect();
    Json(json!({ "host": state.world.host.id, "hosts": hosts, "cells": cells }))
}

async fn local(
    State(state): State<MapState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    if !state.world.authorized(&headers) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let zones: Vec<String> = body["zones"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|z| z.as_str().map(str::to_string))
                .take(((2 * MAX_RADIUS + 1) * (2 * MAX_RADIUS + 1)) as usize)
                .collect()
        })
        .unwrap_or_default();
    let cells = local_cells(&state, &zones).await;
    Ok(Json(json!({
        "host": state.world.host.id,
        "activeTotal": state.server.zone_ids().len(),
        "cells": cells,
    })))
}
