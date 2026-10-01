//! HTTP transport for microsvc — maps HTTP requests to command dispatch.
//!
//! Requires the `http` feature. Uses axum for routing.
//!
//! ## Routes
//!
//! - `POST /{command}` — dispatch a command when [`Service::with_http_command_routes`]
//!   is set. Body = JSON input, request headers → Session.
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
//!         .with_http_command_routes()
//!         .routes(
//!             microsvc::Routes::new()
//!                 .with_repo(InMemoryRepository::new().queued().aggregate::<Counter>())
//!                 .command("counter.create")
//!                 .handle(|ctx| { /* ... */ })
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

use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use super::error::HandlerError;
use super::service::Service;
use super::session::Session;
use super::MAX_HTTP_BODY_BYTES;

/// Build an axum `Router` that dispatches commands via the given service.
///
/// Does not mount `POST /{command}` unless [`Service::with_http_command_routes`]
/// was called. Health, metrics, and GraphQL stay available either way.
pub fn router(service: Arc<Service>) -> Router {
    let mut router = Router::new().route("/health", get(health_handler));
    if service.http_command_routes_enabled() {
        router = router.route("/{command}", axum::routing::post(command_handler));
    }
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
        // the command handler buffers the JSON body into memory.
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .with_state(service)
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
/// `{ "commandId", "input" }` is the causal wait-path. Other JSON is legacy
/// fire-and-forget `dispatch`. Identity is taken from headers, never the body.
async fn command_handler(
    State(service): State<Arc<Service>>,
    Path(command): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    if !super::lifecycle_mutations_open() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "code": "APPLICATION_RELOADING",
                "error": "application generation is not active"
            })),
        )
            .into_response();
    }
    let session = session_from_headers(&headers);
    #[cfg(feature = "graphql")]
    let wait_path = match super::wait_path::parse_wait_path_body(&body) {
        Ok(wait_path) => wait_path,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "code": "BAD_REQUEST", "error": error })),
            )
                .into_response();
        }
    };
    #[cfg(feature = "graphql")]
    if let Some((command_id, input)) = wait_path {
        return match super::wait_path::dispatch_wait_path(
            service.as_ref(),
            &command,
            &command_id,
            input,
            session,
        )
        .await
        {
            Ok(result) => (
                StatusCode::OK,
                Json(super::wait_path::wait_path_response(&result)),
            )
                .into_response(),
            Err(err) => {
                let status = StatusCode::from_u16(err.status_code())
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                if status.is_server_error() {
                    eprintln!("microsvc command `{command}` failed: {err}");
                }
                let body = json!({ "error": err.client_message() });
                (status, Json(body)).into_response()
            }
        };
    }
    match service.dispatch(&command, body, session).await {
        Ok(value) => (StatusCode::OK, Json(value)).into_response(),
        Err(err) => {
            let status = status_for_error(&err);
            if status.is_server_error() {
                eprintln!("microsvc command `{command}` failed: {err}");
            }
            let body = json!({ "error": err.client_facing_message() });
            (status, Json(body)).into_response()
        }
    }
}

fn status_for_error(error: &HandlerError) -> StatusCode {
    match error {
        HandlerError::ApplicationReloading => StatusCode::SERVICE_UNAVAILABLE,
        HandlerError::UnknownCommand(_) | HandlerError::NotFound(_) => StatusCode::NOT_FOUND,
        HandlerError::DecodeFailed(_) | HandlerError::GuardRejected(_) => StatusCode::BAD_REQUEST,
        HandlerError::Rejected(_) => StatusCode::UNPROCESSABLE_ENTITY,
        HandlerError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
        HandlerError::Repository(_)
        | HandlerError::Projection(_)
        | HandlerError::UnqualifiedProjectionDelivery(_)
        | HandlerError::ProjectionRepairPending { .. }
        | HandlerError::ProjectionTerminalRecorded { .. }
        | HandlerError::ProjectionDeliveryHalted { .. }
        | HandlerError::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
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
    fn status_for_error_maps_all_handler_errors() {
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
            let status = status_for_error(&error);
            assert_eq!(status, expected);
            assert_eq!(status.as_u16(), error.status_code());
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
