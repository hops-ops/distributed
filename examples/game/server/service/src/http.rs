//! HTTP surface: `POST /graphql`, `GET /graphql/ws` (`graphql-transport-ws`,
//! the `@live` channel) and `GET /health`, plus CORS for the UI origins.
//!
//! The browser calls this service directly (Bearer token from the SvelteKit
//! page data; no cookies), so only exact configured origins are allowed and
//! credentials mode is never enabled.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    ACCESS_CONTROL_MAX_AGE, ORIGIN, VARY,
};
use axum::http::{HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use distributed::graphql::{graphql_router_composed, GraphqlEngine};

const ALLOWED_HEADERS: &str = "authorization, content-type, accept, x-distributed-participant";

#[derive(Clone)]
struct Cors {
    origins: Arc<Vec<HeaderValue>>,
}

async fn cors(State(cors): State<Cors>, request: Request, next: Next) -> Response {
    let origin = request
        .headers()
        .get(ORIGIN)
        .filter(|origin| cors.origins.iter().any(|allowed| allowed == *origin))
        .cloned();
    let preflight = request.method() == Method::OPTIONS;
    let mut response = if preflight {
        if origin.is_some() {
            StatusCode::NO_CONTENT.into_response()
        } else {
            StatusCode::FORBIDDEN.into_response()
        }
    } else {
        next.run(request).await
    };
    let headers = response.headers_mut();
    headers.append(VARY, HeaderValue::from_static("origin"));
    if let Some(origin) = origin {
        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        if preflight {
            headers.insert(
                ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static("GET, POST, OPTIONS"),
            );
            headers.insert(
                ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_static(ALLOWED_HEADERS),
            );
            headers.insert(ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("600"));
        }
    }
    response
}

/// The service router.
pub fn router(engine: Arc<GraphqlEngine>, allowed_origins: &[String]) -> Router {
    let origins = allowed_origins
        .iter()
        .filter_map(|origin| HeaderValue::from_str(origin.trim_end_matches('/')).ok())
        .collect();
    graphql_router_composed(engine, None, None)
        .route("/health", get(|| async { "ok" }))
        .layer(middleware::from_fn_with_state(
            Cors {
                origins: Arc::new(origins),
            },
            cors,
        ))
}
