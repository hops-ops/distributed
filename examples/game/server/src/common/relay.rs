//! The demos' end of celld's outbox Queue: cells commit domain events into
//! their outbox, celld drains it to the cells worker's Queue, and the
//! worker's consumer relays every message to `CELLD_QUEUE_RELAY_URL` — a
//! demo server's authenticated `/internal/celld-queue/relay` (this module).
//! The route publishes the message onto a [`ProjectionBus`] (a SQLite bus),
//! where read-model projectors (each with its own consumer identity)
//! consume it.
//!
//! Delivery is at-least-once with stable message ids; the projectors
//! deduplicate. Until a bus is attached the route answers 503 and the Queue
//! keeps (retries) the message.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use distributed::bus::{
    celld_queue_relay_handler, Bus, CelldQueueEnvelope, CelldQueueRelayHandler, Message,
    MessagePublisher, SqliteBus, TransportError, CELLD_QUEUE_RELAY_PATH,
};
use distributed::cell_host::{InternalHttpSecret, CELL_INTERNAL_SECRET_HEADER};
use distributed::BusPublisher;
use serde_json::{json, Value};

/// Open (creating) a SQLite database another process may share: WAL and a
/// busy timeout, so writers and readers in different processes wait for
/// each other instead of failing.
pub async fn open_shared_sqlite(
    url: &str,
    max_connections: u32,
) -> Result<sqlx::SqlitePool, String> {
    use std::str::FromStr;
    let options = sqlx::sqlite::SqliteConnectOptions::from_str(url)
        .map_err(|e| format!("{url}: {e}"))?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(10));
    sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(options)
        .await
        .map_err(|e| format!("{url}: {e}"))
}

/// The SQLite bus relayed Queue messages are published onto.
#[derive(Clone)]
pub struct ProjectionBus {
    pool: sqlx::SqlitePool,
    relay: CelldQueueRelayHandler,
}

impl ProjectionBus {
    /// Open (creating) the bus at a `sqlite:` URL (shareable between
    /// processes). Hosts of one world open the same file at the same time:
    /// switching a new database to WAL and creating its tables take the
    /// write lock, so a `database is locked` at startup is retried (10 s).
    pub async fn open(url: &str) -> Result<Self, String> {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let (pool, bus) = loop {
            let attempt = async {
                let pool = open_shared_sqlite(url, 4)
                    .await
                    .map_err(|e| format!("projection bus {e}"))?;
                let bus = SqliteBus::new(pool.clone());
                bus.ensure_tables()
                    .await
                    .map_err(|e| format!("projection bus tables: {e}"))?;
                Ok::<_, String>((pool, bus))
            };
            match attempt.await {
                Ok(opened) => break opened,
                Err(error)
                    if error.contains("database is locked")
                        && std::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error),
            }
        };
        Ok(Self {
            relay: celld_queue_relay_handler(BusPublisher::new(Arc::new(bus))),
            pool,
        })
    }

    /// The bus database (projectors consume it with their own identity).
    pub fn pool(&self) -> &sqlx::SqlitePool {
        &self.pool
    }

    /// The Queue relay handler publishing onto this bus.
    pub fn relay_handler(&self) -> &CelldQueueRelayHandler {
        &self.relay
    }
}

/// Publishes onto the projection bus once it is attached (the slot of
/// `MobPitWorld::projection`); before that every publish is a retryable
/// error, so a retrying publisher (`LeaderboardChangePublisher`) waits for
/// the bus instead of dropping the message.
#[derive(Clone)]
pub struct ProjectionPublisher(pub Arc<OnceLock<ProjectionBus>>);

impl MessagePublisher for ProjectionPublisher {
    async fn publish(&self, message: Message) -> Result<(), TransportError> {
        let bus = self
            .0
            .get()
            .ok_or_else(|| TransportError::retryable("projection bus not attached yet"))?;
        SqliteBus::new(bus.pool().clone())
            .publish_message(message)
            .await
    }
}

#[derive(Clone)]
struct RelayState {
    secret: InternalHttpSecret,
    bus: Arc<OnceLock<ProjectionBus>>,
}

/// `POST /internal/celld-queue/relay` (`DISTRIBUTED_INTERNAL_SECRET`):
/// publish the relayed message onto `bus` (503 while it is unset).
pub fn relay_routes(secret: InternalHttpSecret, bus: Arc<OnceLock<ProjectionBus>>) -> Router {
    Router::new()
        .route(CELLD_QUEUE_RELAY_PATH, post(relay))
        .with_state(RelayState { secret, bus })
}

async fn relay(
    State(state): State<RelayState>,
    headers: HeaderMap,
    Json(envelope): Json<CelldQueueEnvelope>,
) -> (StatusCode, Json<Value>) {
    let authorized = headers
        .get(CELL_INTERNAL_SECRET_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|candidate| state.secret.matches(candidate));
    if !authorized {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "code": "UNAUTHORIZED", "error": "unauthorized" })),
        );
    }
    let Some(bus) = state.bus.get() else {
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
