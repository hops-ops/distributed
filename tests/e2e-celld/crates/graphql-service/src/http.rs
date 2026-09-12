//! Process HTTP: GraphQL is the user edge (engine `OidcBearer`).
//!
//! Zitadel Action ingress/scrape and the celld Queue relay are internal —
//! shared secret or process-local, not a user Bearer. User writes stay on
//! `/graphql`.

use std::collections::HashMap;
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
use distributed::microsvc::{HandlerError, Service, Session};
use serde_json::{json, Value};

fn session_from_headers(headers: &HeaderMap) -> Session {
    let mut vars = HashMap::new();
    for (name, value) in headers.iter() {
        if let Ok(v) = value.to_str() {
            vars.insert(name.as_str().to_string(), v.to_string());
        }
    }
    Session::from_map(vars)
}

fn status_for_error(error: &HandlerError) -> StatusCode {
    match error {
        HandlerError::UnknownCommand(_) | HandlerError::NotFound(_) => StatusCode::NOT_FOUND,
        HandlerError::DecodeFailed(_) | HandlerError::GuardRejected(_) => StatusCode::BAD_REQUEST,
        HandlerError::Rejected(_) => StatusCode::UNPROCESSABLE_ENTITY,
        HandlerError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
        HandlerError::Repository(_) | HandlerError::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn dispatch_named(
    service: Arc<Service>,
    headers: HeaderMap,
    input: Value,
    command: &'static str,
) -> impl IntoResponse {
    let session = session_from_headers(&headers);
    match service.dispatch(command, input, session).await {
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
    let ingress = service.clone();
    let scrape = service.clone();
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
    let mut internal = Router::new()
        .route(
            "/zitadel.ingress.v1",
            post(move |headers: HeaderMap, Json(input): Json<Value>| {
                let svc = ingress.clone();
                async move { dispatch_named(svc, headers, input, "zitadel.ingress.v1").await }
            }),
        )
        .route(
            "/zitadel.scrape.v1",
            post(move |headers: HeaderMap, Json(input): Json<Value>| {
                let svc = scrape.clone();
                async move { dispatch_named(svc, headers, input, "zitadel.scrape.v1").await }
            }),
        );
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
    for path in [
        "/health",
        "/zitadel.ingress.v1",
        "/zitadel.scrape.v1",
        CELLD_QUEUE_RELAY_PATH,
    ] {
        routes.push(Route::new(
            format!("service-{}", routes.len()),
            RoutePath::exact(path),
            "service",
        ));
    }
    for command in service.command_names() {
        let path = RoutePath::exact(format!("/{command}"));
        if !routes.iter().any(|route| route.path == path) {
            routes.push(Route::new(
                format!("closed-{}", routes.len()),
                path,
                "closed",
            ));
        }
    }
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
            Binding::new("closed", BindingKind::Handler),
        ],
        routes,
    }
    .build()
    .map_err(std::io::Error::other)?;
    let auth = if let Some(config) = engine.identity_config().oidc.clone() {
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
            ("closed".into(), NativeBinding::Handler(Router::new())),
        ],
        auth,
    )
    .map_err(std::io::Error::other)?
    .router();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await
}
