//! Process HTTP: GraphQL is the user edge (engine `OidcBearer`).
//!
//! App writes are GraphQL-only (HTTP command routes stay off). Zitadel Action
//! ingress still needs HTTP, so the service exposes those two commands
//! through their own edges (`Service::with_http_command` in `compose`): each
//! checks the shared secret and dispatches as the ingestor. Every other
//! command path is the service's 404 — `POST /todo.create` (suite T0) — and
//! never falls through to the UI.

use std::sync::Arc;

use distributed::microsvc::Service;

/// Compose the application's explicit public gateway without opening a listener.
pub fn gateway_router(
    service: Arc<Service>,
    options: &crate::HostOptions,
) -> Result<axum::Router, std::io::Error> {
    use distributed::gateway::{delivery::*, native::*, *};
    use distributed::graphql::OidcGatewayProvider;
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
    for path in ["/health", "/healthz", "/metrics", "/graphiql"] {
        routes.push(Route::new(
            format!("http-{}", routes.len()),
            RoutePath::prefix(path),
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
        ],
        routes,
    }
    .build()
    .map_err(std::io::Error::other)?;
    // The application declares the public gateway it is served through.
    crate::application(&service)
        .map_err(std::io::Error::other)?
        .with_gateway("public", &gateway)
        .map_err(std::io::Error::other)?;
    let graphql = GraphqlBinding::Embedded(
        EmbeddedGraphql::new(
            engine.clone(),
            Some(service.clone() as distributed::command_dispatch::SharedCommandHost),
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
    let auth = if let Some(config) = engine.identity_config().oidc().cloned() {
        let provider = Arc::new(OidcGatewayProvider::new(config, "e2e-ui-oidc-v1"));
        NativeAuth::new(move |credentials| {
            let provider = provider.clone();
            async move { provider.authenticate(&credentials).await }
        })
    } else {
        NativeAuth::anonymous()
    };

    let service_routes = distributed::microsvc::router(service);

    NativeGateway::new(
        gateway,
        NativeOptions::new(&options.public_origin),
        [
            ("graphql".into(), graphql),
            ("ui".into(), NativeBinding::UiProxy { websocket: true }),
            ("service".into(), NativeBinding::Handler(service_routes)),
        ],
        auth,
    )
    .map(NativeGateway::router)
    .map_err(std::io::Error::other)
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
