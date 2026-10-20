//! **Dev-only** Mob Pit routes (mounted with `--dev`, next to `/dev/ticket`).
//! None of them takes an identity or a reward from the caller.
//!
//! - `GET /dev/mobpit/storage`, `POST /dev/mobpit/storage {latencyMs}` —
//!   artificial latency in front of every cell commit ("slow storage").
//! - `GET /dev/mobpit/stats` — boot id, sink counters, pending facts, recent
//!   commits (key, exact input, receipt).
//! - `GET /dev/mobpit/crawlers` — every Crawler cell on disk (read path).
//! - `GET /dev/mobpit/leaderboard[?member=principal]` — the pushed top-N
//!   snapshot, plus rank / neighbours of `member` on both boards.
//! - `POST /dev/mobpit/leaderboard/rebuild` — rebuild both boards from the
//!   cells' event history; reports whether the result matches the cells.
//! - `GET /dev/mobpit/presence` — who is online / away / offline where,
//!   projected from the runtime's player lifecycle facts.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
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
