//! HTTP transport for microsvc — maps HTTP requests to command dispatch.
//!
//! Requires the `http` feature. Uses axum for routing.
//!
//! ## Routes
//!
//! - `POST /{command}` for a command exposed with
//!   [`Service::with_http_command`]: its edge authenticates the request and
//!   says what to dispatch. No other command path is answered.
//! - `POST /{command}` for every command when
//!   [`Service::with_http_command_routes`] is set (trusted-proxy ingress).
//!   Body = `{ "commandId", "input" }`, request headers → Session. A body
//!   without a `commandId` is a 400.
//! - `GET /health` — health check returning `{ "ok": true, "commands": [...] }`.
//! - `GET /metrics` — Prometheus text metrics (requires the `metrics` feature);
//!   unauthenticated by design and intended for private scrape networks only.
//!
//! ## Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use distributed::{microsvc, InMemoryRepository};
//!
//! let service = Arc::new(
//!     microsvc::Service::new()
//!         .named("counters")
//!         .with_http_command_routes()
//!         .routes(
//!             microsvc::Routes::new()
//!                 .with_repo(InMemoryRepository::new().queued().aggregate::<Counter>())
//!                 .typed_command(counter_create::command())
//!                 .handle(counter_create::handle)
//!         )
//! );
//!
//! // Get the router to compose with other axum routes
//! let app = microsvc::router(service.clone());
//!
//! // Or serve directly
//! microsvc::serve(service, "0.0.0.0:3000").await?;
//! ```

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use super::error::HandlerError;
use super::service::{CausalDispatchError, CausalDispatchResult, Service};
use super::session::Session;
use super::MAX_HTTP_BODY_BYTES;
use crate::command_dispatch::VerifiedPrincipal;

/// What a command's HTTP edge ([`Service::with_http_command`]) dispatches
/// for one authenticated request.
#[derive(Debug)]
pub struct HttpCommandDispatch {
    /// The ledger command ID. A redelivery with the same ID replays the
    /// stored receipt.
    pub command_id: String,
    /// The command input.
    pub input: Value,
    /// The session the command runs with. Request headers reach the command
    /// only through what the edge puts here.
    pub session: Session,
    /// The principal the edge verified.
    pub principal: VerifiedPrincipal,
}

/// A command's HTTP edge: request headers and JSON body in, dispatch out.
pub(crate) type HttpCommandEdge = Arc<
    dyn Fn(&HeaderMap, Value) -> Result<HttpCommandDispatch, CausalDispatchError> + Send + Sync,
>;

/// Build an axum `Router` that dispatches commands via the given service.
///
/// Mounts [`command_router`], `GET /health`, metrics, and GraphQL when the
/// service has an engine.
pub fn router(service: Arc<Service>) -> Router {
    // Only metrics and GraphQL add routes after this.
    #[cfg_attr(not(any(feature = "metrics", feature = "graphql")), allow(unused_mut))]
    let mut router = command_router(Arc::clone(&service)).route("/health", get(health_handler));
    #[cfg(feature = "metrics")]
    {
        router = router.route("/metrics", get(metrics_handler));
    }

    // GraphQL must be registered before the body-limit layer so the limit wraps it.
    // POST /graphql: queries/mutations. GET /graphql: GraphiQL.
    // GET /graphql/ws: WebSocket subscriptions (graphql-ws protocol).
    #[cfg(feature = "graphql")]
    {
        if service.graphql_engine().is_some() {
            router = router
                .route(
                    "/graphql",
                    axum::routing::post(crate::graphql::http::microsvc_graphql_handler)
                        .get(crate::graphql::http::microsvc_graphql_get),
                )
                .route(
                    "/graphql/ws",
                    axum::routing::get(crate::graphql::http::microsvc_graphql_ws),
                );
        }
    }

    router
        // Pin the body limit explicitly rather than relying on axum's default;
        // the command handlers buffer the JSON body into memory.
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .with_state(service)
}

/// The command paths of `service`, and nothing else: `POST /{command}` for
/// each command exposed with [`Service::with_http_command`], and for every
/// command when [`Service::with_http_command_routes`] is set. A command path
/// it does not expose is not mounted, so it is a 404.
///
/// [`router`] includes it; mount it alone to compose the command paths into
/// another router (with that router's own health route or middleware). It
/// needs its state, so finish it with `.with_state(service)` there, or call
/// [`router`].
///
/// # Panics
/// When an edge names a command the service does not register.
pub fn command_router(service: Arc<Service>) -> Router<Arc<Service>> {
    let mut router = Router::new();
    if service.http_command_routes_enabled() {
        router = router.route("/{command}", post(command_handler));
    }
    let registered = service.command_names();
    for (command, edge) in service.http_commands() {
        assert!(
            registered.contains(&command),
            "HTTP edge for `{command}`, which the service does not register"
        );
        let command = command.to_string();
        let edge = Arc::clone(edge);
        router = router.route(
            &format!("/{command}"),
            post(
                move |State(service): State<Arc<Service>>, headers: HeaderMap, body: Bytes| {
                    let command = command.clone();
                    let edge = Arc::clone(&edge);
                    async move {
                        let result = match request_json(&body) {
                            Ok(body) => match edge(&headers, body) {
                                Ok(dispatch) => {
                                    service
                                        .dispatch_causal_with_receipt(
                                            &command,
                                            &dispatch.command_id,
                                            dispatch.input,
                                            dispatch.session,
                                            dispatch.principal,
                                        )
                                        .await
                                }
                                Err(error) => Err(error),
                            },
                            Err(error) => Err(error),
                        };
                        command_response(&command, result)
                    }
                },
            ),
        );
    }
    router
}

/// The JSON of a request body; `null` when it is empty.
fn request_json(body: &[u8]) -> Result<Value, CausalDispatchError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(Value::Null);
    }
    serde_json::from_slice(body)
        .map_err(|_| CausalDispatchError::BadRequest("request body is not JSON".into()))
}

/// Serve the service over HTTP at the given address (e.g. `"0.0.0.0:3000"`).
pub async fn serve(service: Arc<Service>, addr: &str) -> Result<(), std::io::Error> {
    let app = router(service);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await
}

/// `GET /health` — returns `{ "ok": true, "commands": [...] }`.
async fn health_handler(State(service): State<Arc<Service>>) -> impl IntoResponse {
    let commands: Vec<&str> = service.command_names();
    #[cfg(feature = "graphql")]
    let body = {
        let mut v = json!({ "ok": true, "commands": commands });
        if service.graphql_engine().is_some() {
            v.as_object_mut()
                .unwrap()
                .insert("graphql".into(), json!(true));
        }
        v
    };
    #[cfg(not(feature = "graphql"))]
    let body = json!({ "ok": true, "commands": commands });
    Json(body)
}

/// `GET /metrics` — returns Prometheus text metrics.
///
/// This endpoint is unauthenticated by design for Prometheus scraping. Keep it
/// behind a private listener, ingress policy, security group, or equivalent
/// network restriction.
#[cfg(feature = "metrics")]
async fn metrics_handler(State(service): State<Arc<Service>>) -> impl IntoResponse {
    crate::metrics::prometheus_response(service.name())
}

/// `POST /{command}` — dispatch a command with JSON body and headers as session.
///
/// The body is `{ "commandId", "input" }`; anything else is a 400. Identity
/// is taken from headers, never the body.
async fn command_handler(
    State(service): State<Arc<Service>>,
    Path(command): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let session = session_from_headers(&headers);
    let (command_id, input) = match super::wait_path::parse_wait_path_body(&body) {
        Ok(wait_path) => wait_path,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "code": "BAD_REQUEST", "error": error })),
            )
                .into_response();
        }
    };
    let result = super::wait_path::dispatch_wait_path(
        service.as_ref(),
        &command,
        &command_id,
        input,
        session,
    )
    .await;
    command_response(&command, result)
}

/// The HTTP answer of one dispatched command: the wait-path response, or its
/// error with the error's status.
fn command_response(
    command: &str,
    result: Result<CausalDispatchResult, CausalDispatchError>,
) -> Response {
    match result {
        Ok(result) => (
            StatusCode::OK,
            Json(super::wait_path::wait_path_response(&result)),
        )
            .into_response(),
        Err(CausalDispatchError::Handler(HandlerError::ApplicationReloading)) => {
            reloading_response()
        }
        Err(err) => {
            let status = StatusCode::from_u16(err.status_code())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            if status.is_server_error() {
                eprintln!("microsvc command `{command}` failed: {err}");
            }
            let body = super::wait_path::wait_path_error_response(&err);
            (status, Json(body)).into_response()
        }
    }
}

/// The response to a command refused by the service's
/// [`LifecycleGate`](super::LifecycleGate).
fn reloading_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "code": "APPLICATION_RELOADING",
            "error": "application generation is not active"
        })),
    )
        .into_response()
}

/// Extract session variables from HTTP headers.
///
/// **Trust boundary (security-critical):** every request header is copied
/// verbatim into the [`Session`] — including identity claims (e.g. the
/// convenience keys `x-user-id` / `x-roles`). The framework does NOT
/// authenticate. A trusted proxy in front of this service MUST strip any
/// client-supplied identity headers and inject only authenticated ones.
/// Without that proxy, any client can set those headers and assume any
/// identity/role. See the [`Session`] docs.
pub(crate) fn session_from_headers(headers: &HeaderMap) -> Session {
    let mut vars = std::collections::HashMap::new();
    for (name, value) in headers.iter() {
        if let Ok(v) = value.to_str() {
            vars.insert(name.as_str().to_string(), v.to_string());
        }
    }
    Session::from_map(vars)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::RepositoryError;

    #[test]
    fn handler_errors_map_to_http_statuses() {
        let cases = vec![
            (
                HandlerError::UnknownCommand("missing".into()),
                StatusCode::NOT_FOUND,
            ),
            (
                HandlerError::DecodeFailed("bad json".into()),
                StatusCode::BAD_REQUEST,
            ),
            (
                HandlerError::Rejected("invalid command".into()),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                HandlerError::NotFound("counter-1".into()),
                StatusCode::NOT_FOUND,
            ),
            (
                HandlerError::Unauthorized("missing user".into()),
                StatusCode::UNAUTHORIZED,
            ),
            (
                HandlerError::Repository(RepositoryError::Model("store failed".into())),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            (
                HandlerError::GuardRejected("counter.create".into()),
                StatusCode::BAD_REQUEST,
            ),
            (
                HandlerError::Other(Box::new(std::io::Error::other("handler failed"))),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];

        for (error, expected) in cases {
            let status = StatusCode::from_u16(error.status_code()).unwrap();
            assert_eq!(status, expected);
            assert!(!status.is_success());
        }
    }

    #[test]
    fn client_facing_message_preserves_client_errors() {
        let error = HandlerError::Rejected("invalid command".into());

        assert_eq!(error.client_facing_message(), "rejected: invalid command");
    }

    #[test]
    fn client_facing_message_hides_server_errors() {
        let errors = [
            HandlerError::Repository(RepositoryError::Model("store failed".into())),
            HandlerError::Other(Box::new(std::io::Error::other("handler failed"))),
        ];

        for error in errors {
            assert_eq!(error.client_facing_message(), "Internal server error");
        }
    }
}
