//! Process HTTP: GraphQL is the user edge (engine `OidcBearer`).
//!
//! Zitadel Action ingress/scrape and the celld Queue relay are internal —
//! shared secret or process-local, not a user Bearer. User writes stay on
//! `/graphql`.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use distributed::bus::{CelldQueueEnvelope, CelldQueueRelayHandler, CELLD_QUEUE_RELAY_PATH};
use distributed::cell_host::{InternalHttpSecret, CELL_INTERNAL_SECRET_HEADER};
use distributed::command_dispatch::SharedCommandHost;
use distributed::gateway::{native::*, *};
use distributed::graphql::identity::OidcGatewayProvider;
use distributed::microsvc::{self, Service};
use serde_json::json;

async fn require_internal(
    State(secret): State<InternalHttpSecret>,
    request: Request,
    next: Next,
) -> Response {
    if authorized_internal(request.headers(), &secret) {
        return next.run(request).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({ "code": "UNAUTHORIZED", "error": "unauthorized" })),
    )
        .into_response()
}

fn authorized_internal(headers: &HeaderMap, secret: &InternalHttpSecret) -> bool {
    headers
        .get(CELL_INTERNAL_SECRET_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|candidate| secret.matches(candidate))
}

/// GraphQL wait-dispatches through an explicit [`SharedCommandHost`].
///
/// Identity is the engine's (`OidcBearer` on `POST /graphql` / WS `connection_init`).
/// HTTP command routes stay off — `POST /todo.create` is 404.
pub async fn serve(
    service: Arc<Service>,
    host: SharedCommandHost,
    addr: &str,
    public_origin: &str,
    ui_origin: &str,
    queue_relay: CelldQueueRelayHandler,
    internal_secret: InternalHttpSecret,
) -> Result<(), std::io::Error> {
    let engine = service
        .graphql_engine()
        .ok_or_else(|| std::io::Error::other("serve requires GraphQL"))?;
    let commands: Vec<String> = service
        .command_names()
        .into_iter()
        .map(str::to_string)
        .collect();
    let health_body = json!({
        "ok": true,
        "profile": "celld",
        "graphql": true,
        "commands": commands,
    });
    let mut app = Router::new().route(
        "/health",
        get(move || {
            let body = health_body.clone();
            async move { Json(body) }
        }),
    );
    // The commands the service exposes (the Zitadel ingress and scrape, each
    // through its own edge) and the Queue relay are internal: the cell
    // secret first, then each edge's own check.
    let mut internal = microsvc::command_router(service.clone()).with_state(service.clone());
    internal = internal.route(
        CELLD_QUEUE_RELAY_PATH,
        post(move |Json(envelope): Json<CelldQueueEnvelope>| {
            let relay = Arc::clone(&queue_relay);
            async move {
                match relay(envelope).await {
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
        }),
    );
    app = app.merge(internal.route_layer(middleware::from_fn_with_state(
        internal_secret,
        require_internal,
    )));

    let capabilities = GraphqlCapabilities {
        queries: true,
        commands: true,
        live: true,
    };
    let mut routes = vec![
        Route::new("graphql", RoutePath::prefix("/graphql"), "graphql"),
        Route::new("ui", RoutePath::prefix("/"), "ui"),
    ];
    for path in ["/health", CELLD_QUEUE_RELAY_PATH] {
        routes.push(Route::new(
            format!("service-{}", routes.len()),
            RoutePath::exact(path),
            "service",
        ));
    }
    // Every command path is the service's: it answers the exposed Zitadel
    // commands, and no other command URL can become UI HTML.
    routes.extend(service.command_gateway_routes("service"));
    let gateway = GatewayConfig {
        bindings: vec![
            Binding::new(
                "graphql",
                BindingKind::Graphql {
                    executor: GraphqlExecutor::Embedded,
                    capabilities,
                    delivery: DeliveryCapabilities::default(),
                    schema_extensions: vec![],
                },
            ),
            Binding::new(
                "ui",
                BindingKind::UiProxy {
                    origin: ui_origin.into(),
                },
            ),
            Binding::new("service", BindingKind::Handler),
        ],
        routes,
    }
    .build()
    .map_err(std::io::Error::other)?;
    let auth = if let Some(config) = engine.identity_config().oidc().cloned() {
        let provider = Arc::new(OidcGatewayProvider::new(config, "e2e-celld-oidc-v1"));
        NativeAuth::new(move |credentials| {
            let provider = provider.clone();
            async move { provider.authenticate(&credentials).await }
        })
    } else {
        NativeAuth::anonymous()
    };
    // The remote command host and secret-protected Queue relay keep their
    // existing owners. UI, auth, HMR and lifecycle requests use this public edge.
    let app = NativeGateway::new(
        gateway,
        NativeOptions::new(public_origin),
        [
            (
                "graphql".into(),
                NativeBinding::Graphql(GraphqlBinding::Embedded(
                    EmbeddedGraphql::new(engine, Some(host), capabilities)
                        .map_err(std::io::Error::other)?,
                )),
            ),
            ("ui".into(), NativeBinding::UiProxy { websocket: true }),
            ("service".into(), NativeBinding::Handler(app)),
        ],
        auth,
    )
    .map_err(std::io::Error::other)?
    .router();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await
}
