//! **Dev-only** Mob Pit routes (mounted with `--dev`, next to `/dev/ticket`).
//! None of them takes an identity or a reward from the caller.
//!
//! - `GET /dev/mobpit/storage`, `POST /dev/mobpit/storage {latencyMs}` —
//!   artificial latency in front of every cell commit ("slow storage").
//! - `GET /dev/mobpit/stats` — boot id, sink counters, pending facts, recent
//!   commits (key, exact input, receipt).
//! - `GET /dev/mobpit/crawlers` — every Crawler cell on the roster (read
//!   over celld's internal path).
//! - `GET /dev/mobpit/leaderboard[?member=principal]` — the pushed top-N
//!   snapshot, plus rank / neighbours of `member` on both boards.
//! - `POST /dev/mobpit/leaderboard/rebuild` — rebuild both boards from the
//!   cells' event history; reports whether the result matches the cells.
//! - `GET /dev/mobpit/presence` — who is online / away / offline where,
//!   projected from the runtime's player lifecycle facts.
//!
//! [`internal_routes`] (always mounted): `POST /internal/celld-queue/relay`,
//! the celld outbox Queue consumer's hop, authenticated with
//! `DISTRIBUTED_INTERNAL_SECRET`; it publishes the Crawler domain event onto
//! the projection bus (503 until one is attached, so the Queue retries).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use distributed::bus::{CelldQueueEnvelope, CELLD_QUEUE_RELAY_PATH};
use distributed::cell_host::CELL_INTERNAL_SECRET_HEADER;
use serde::Deserialize;
use serde_json::{json, Value};

use super::world::MobPitWorld;

type ApiResult = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn internal(error: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": error.to_string() })),
    )
}

/// The router.
pub fn routes(world: Arc<MobPitWorld>) -> Router {
    Router::new()
        .route("/dev/mobpit/storage", get(get_storage).post(set_storage))
        .route("/dev/mobpit/stats", get(stats))
        .route("/dev/mobpit/crawlers", get(crawlers))
        .route("/dev/mobpit/leaderboard", get(leaderboard))
        .route("/dev/mobpit/leaderboard/rebuild", post(rebuild))
        .route("/dev/mobpit/presence", get(presence))
        .with_state(world)
}

/// Internal routes (see the module docs).
pub fn internal_routes(world: Arc<MobPitWorld>) -> Router {
    Router::new()
        .route(CELLD_QUEUE_RELAY_PATH, post(relay))
        .with_state(world)
}

async fn relay(
    State(world): State<Arc<MobPitWorld>>,
    headers: HeaderMap,
    Json(envelope): Json<CelldQueueEnvelope>,
) -> (StatusCode, Json<Value>) {
    let authorized = headers
        .get(CELL_INTERNAL_SECRET_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|candidate| world.celld().secret.matches(candidate));
    if !authorized {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "code": "UNAUTHORIZED", "error": "unauthorized" })),
        );
    }
    let Some(bus) = world.projection_bus() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "code": "UNAVAILABLE", "error": "no projection bus attached" })),
        );
    };
    match bus.relay_handler()(envelope).await {
        Ok(()) => (StatusCode::ACCEPTED, Json(json!({ "ok": true }))),
        Err(error) if error.is_retryable() => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "code": "UNAVAILABLE", "error": error.message() })),
        ),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "code": "BAD_REQUEST", "error": error.message() })),
        ),
    }
}

/// `POST /dev/mobpit/storage` body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StorageBody {
    latency_ms: u64,
}

fn storage_json(world: &MobPitWorld) -> Value {
    json!({ "latencyMs": world.sink_state().latency().as_millis() as u64 })
}

async fn get_storage(State(world): State<Arc<MobPitWorld>>) -> Json<Value> {
    Json(storage_json(&world))
}

async fn set_storage(
    State(world): State<Arc<MobPitWorld>>,
    Json(body): Json<StorageBody>,
) -> Json<Value> {
    world
        .sink_state()
        .set_latency(Duration::from_millis(body.latency_ms));
    Json(storage_json(&world))
}

async fn stats(State(world): State<Arc<MobPitWorld>>) -> Json<Value> {
    Json(json!({
        "boot": world.boot(),
        "dataDir": world.data_dir().display().to_string(),
        "celld": world.cells().url(),
        "latencyMs": world.sink_state().latency().as_millis() as u64,
        "pending": world.zone_pending(),
        "sink": world.sink_state().stats(),
        "recent": world.sink_state().recent(),
        "board": world.board().backend(),
    }))
}

async fn crawlers(State(world): State<Arc<MobPitWorld>>) -> ApiResult {
    Ok(Json(
        json!({ "crawlers": world.crawlers().await.map_err(internal)? }),
    ))
}

async fn leaderboard(
    State(world): State<Arc<MobPitWorld>>,
    Query(query): Query<HashMap<String, String>>,
) -> ApiResult {
    let snapshot = (*world.board().snapshot()).clone();
    let me = match query.get("member").filter(|m| !m.is_empty()) {
        Some(member) => world.board().around(member, 2).await.map_err(internal)?,
        None => Value::Null,
    };
    Ok(Json(json!({ "snapshot": snapshot, "me": me })))
}

async fn presence(State(world): State<Arc<MobPitWorld>>) -> Json<Value> {
    Json(json!({ "presence": world.presence().snapshot() }))
}

async fn rebuild(State(world): State<Arc<MobPitWorld>>) -> ApiResult {
    let report = world.rebuild_board().await.map_err(internal)?;
    Ok(Json(serde_json::to_value(report).map_err(internal)?))
}
