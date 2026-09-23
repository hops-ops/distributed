//! Process HTTP: GraphQL is the user edge (engine `OidcBearer`).
//!
//! App writes are GraphQL-only (HTTP command routes stay off). Zitadel Action
//! ingress still needs HTTP, so those two command names are mounted explicitly
//! — `POST /todo.create` stays 404 (suite T0).
//!
//! Note: `microsvc::router` already applies `.with_state(service)`, so handlers
//! cannot use `State<Arc<Service>>`. Capture the `Arc` in the route closures.

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Json;
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

/// Compose the application's explicit public gateway without opening a listener.
pub fn gateway_router(
    service: Arc<Service>,
    options: &crate::HostOptions,
) -> Result<axum::Router, std::io::Error> {
    use distributed::application::{MountSelector, Runtime};
    use distributed::command_dispatch::LocalCommandHost;
    use distributed::gateway::{delivery::*, native::*, *};
    use distributed::graphql::identity::OidcGatewayProvider;
    let engine = service
        .graphql_engine()
        .ok_or_else(|| std::io::Error::other("application GraphQL engine missing"))?;
    let capabilities = GraphqlCapabilities {
        queries: true,
        commands: true,
        live: true,
    };
    let mut routes = vec![
        Route::new("graphql", RoutePath::prefix("/graphql"), "graphql"),
        Route::new("auth", RoutePath::prefix("/auth"), "ui"),
        Route::new("auth-api", RoutePath::prefix("/api/auth"), "ui"),
        Route::new("ui", RoutePath::prefix("/"), "ui"),
    ];
    for path in [
        "/zitadel.ingress.v1",
        "/zitadel.scrape.v1",
        "/health",
        "/healthz",
        "/metrics",
        "/graphiql",
    ] {
        routes.push(Route::new(
            format!("http-{}", routes.len()),
            RoutePath::prefix(path),
            "service",
        ));
    }
    // Old HTTP command URLs retain API ownership and cannot become UI HTML.
    for command in service.command_specs().map_err(std::io::Error::other)? {
        let path = format!("/{}", command.id);
        if !routes
            .iter()
            .any(|route| route.path == RoutePath::prefix(&path))
        {
            routes.push(Route::new(
                format!("command-{}", routes.len()),
                RoutePath::exact(path),
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
                    delivery: options.delivery,
                    schema_extensions: vec![],
                },
            ),
            Binding::new(
                "ui",
                BindingKind::UiProxy {
                    origin: options.ui_origin.clone(),
                },
            ),
            Binding::new("service", BindingKind::Handler),
            Binding::new("closed", BindingKind::Handler),
        ],
        routes,
    }
    .build()
    .map_err(std::io::Error::other)?;
    let surface = crate::application_manifest()
        .surfaces
        .into_iter()
        .next()
        .ok_or_else(|| std::io::Error::other("application surface missing"))?;
    let application = service
        .application(crate::E2E_UI_APPLICATION, surface)
        .map_err(std::io::Error::other)?
        .with_gateway("public", &gateway)
        .map_err(std::io::Error::other)?;
    let selector = MountSelector::gateway("public").map_err(std::io::Error::other)?;
    let runtime = Runtime::default()
        .mount_gateway(&application, selector.clone(), gateway)
        .map_err(std::io::Error::other)?;
    runtime
        .bind_gateway(&selector, |gateway| {
            let graphql = GraphqlBinding::Embedded(
                EmbeddedGraphql::new(
                    engine.clone(),
                    Some(Arc::new(LocalCommandHost::new(service.clone()))),
                    capabilities,
                )
                .map_err(std::io::Error::other)?,
            );
            let graphql = if options.delivery == DeliveryCapabilities::default() {
                NativeBinding::Graphql(graphql)
            } else {
                let delivery = NativeDelivery::new(NativeDeliveryOptions {
                    snapshots: options.delivery.snapshots.then(SnapshotLimits::default),
                    coalescing: options.delivery.coalescing.then(FlightLimits::default),
                    live: options.delivery.live_sharing.then(LiveLimits::default),
                })
                .map_err(std::io::Error::other)?;
                NativeBinding::GraphqlWithDelivery(graphql, Arc::new(delivery))
            };
            let auth = if let Some(config) = engine.identity_config().oidc.clone() {
                let provider = Arc::new(OidcGatewayProvider::new(config, "e2e-ui-oidc-v1"));
                NativeAuth::new(move |credentials| {
                    let provider = provider.clone();
                    async move { provider.authenticate(&credentials).await }
                })
            } else {
                NativeAuth::anonymous()
            };

            let ingress = service.clone();
            let scrape = service.clone();
            let service_routes = distributed::microsvc::router(service)
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

            NativeGateway::new(
                gateway.clone(),
                NativeOptions::new(&options.public_origin),
                [
                    ("graphql".into(), graphql),
                    ("ui".into(), NativeBinding::UiProxy { websocket: true }),
                    ("service".into(), NativeBinding::Handler(service_routes)),
                    ("closed".into(), NativeBinding::Handler(axum::Router::new())),
                ],
                auth,
            )
            .map(NativeGateway::router)
            .map_err(std::io::Error::other)
        })
        .map(|adapter| adapter.expect("explicitly selected gateway"))
}

/// One backend owns the public listener; SvelteKit remains its UI/auth backend.
pub async fn serve(
    service: Arc<Service>,
    options: &crate::HostOptions,
) -> Result<(), std::io::Error> {
    let app = gateway_router(service, options)?;
    let listener = tokio::net::TcpListener::bind(&options.bind).await?;
    axum::serve(listener, app).await
}
