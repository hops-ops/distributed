//! Axum GraphQL router (POST /graphql, optional GraphiQL + websocket upgrade).

use std::sync::Arc;

use async_graphql::http::{GraphiQLSource, ALL_WEBSOCKET_PROTOCOLS};
use async_graphql::parser::types::OperationType;
use async_graphql::{Data, Executor, Request, Response as GqlResponse, ServerError};
use async_graphql_axum::{GraphQLProtocol, GraphQLRequest, GraphQLResponse, GraphQLWebSocket};
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use futures_util::stream::BoxStream;

use crate::command_dispatch::{LocalCommandDispatcher, LocalCommandHost, SharedCommandHost};
use crate::microsvc::{Service, Session, MAX_HTTP_BODY_BYTES, USER_ID_KEY};

use super::engine::GraphqlEngine;
use super::identity::{
    resolve_identity_with_validator, AuthError, IdentityMode, ResolvedIdentity, VerifiedPrincipal,
};

/// DevHeaders baked into GraphiQL for local exploration (not production security).
const GRAPHIQL_DEV_USER: &str = "demo";
const GRAPHIQL_DEV_ROLE: &str = "user";

/// HTML for the GraphiQL IDE served on `GET /graphql` when GraphiQL is enabled.
///
/// Default identity headers (`x-roles: user`, `x-user-id: demo`) are injected
/// only for local exploration. Production scaffolds use `OidcBearer` (D6) —
/// these headers are **not** a security mechanism. GraphiQL is off under
/// production env policy (`graphiql_enabled_from_env`).
///
/// **Subscriptions:** GraphiQL runs them over WebSocket at `/graphql/ws`.
/// Browsers cannot set custom WS headers; identity is supplied via
/// `wsConnectionParams` → `connection_init` (and DevHeaders defaults if empty).
///
/// Note: do **not** put `?x-user-id=` query params in `subscription_endpoint` —
/// GraphiQLSource HTML-escapes `=` / `&` (`&#x3D;`, `&amp;`), which breaks the URL.
pub fn graphiql_page() -> Html<String> {
    Html(
        GraphiQLSource::build()
            .endpoint("/graphql")
            .subscription_endpoint("/graphql/ws")
            .header("x-roles", GRAPHIQL_DEV_ROLE)
            .header(USER_ID_KEY, GRAPHIQL_DEV_USER)
            // Sent as connection_init payload for graphql-transport-ws.
            .ws_connection_param(USER_ID_KEY, GRAPHIQL_DEV_USER)
            .ws_connection_param("x-roles", GRAPHIQL_DEV_ROLE)
            .title("Distributed GraphQL")
            .finish(),
    )
}

/// Per-operation execution admission for a composed GraphQL surface. The
/// filter runs for HTTP and each WS operation, after ordinary authentication.
/// It may restrict the surface but never supplies identity or commit evidence.
pub type GraphqlOperationFilter = Arc<dyn Fn(&Request) -> Result<(), ServerError> + Send + Sync>;

/// Resource lease supplied only by the native gateway adapter.
pub struct GraphqlConnectionGuard {
    pub(crate) lifetime: std::time::Duration,
    pub(crate) _permit: tokio::sync::OwnedSemaphorePermit,
}
impl GraphqlConnectionGuard {
    /// Run a custom executor connection within its admitted lifetime. Keep this
    /// guard alive until completion to retain the gateway capacity permit.
    pub async fn run<F: std::future::Future>(&self, connection: F) -> Option<F::Output> {
        tokio::time::timeout(self.lifetime, connection).await.ok()
    }
}

#[derive(Default)]
struct GraphqlWsOptions {
    filter: Option<GraphqlOperationFilter>,
    guard: Option<Arc<GraphqlConnectionGuard>>,
}

/// [`Executor`] for WebSocket subscriptions.
///
/// Prefers a [`Session`] injected via `connection_init` / `session_data` (GraphiQL
/// wsConnectionParams), then falls back to the session from the HTTP upgrade.
#[derive(Clone)]
pub struct GraphqlSessionExecutor {
    engine: Arc<GraphqlEngine>,
    session: Session,
    principal: Option<VerifiedPrincipal>,
    host: Option<SharedCommandHost>,
    filter: Option<GraphqlOperationFilter>,
}

impl GraphqlSessionExecutor {
    pub fn new(engine: Arc<GraphqlEngine>, session: Session) -> Self {
        Self {
            engine,
            session,
            principal: None,
            host: None,
            filter: None,
        }
    }

    fn with_identity(
        engine: Arc<GraphqlEngine>,
        session: Session,
        principal: Option<VerifiedPrincipal>,
        host: Option<SharedCommandHost>,
        filter: Option<GraphqlOperationFilter>,
    ) -> Self {
        Self {
            engine,
            session,
            principal,
            host,
            filter,
        }
    }
}

impl Executor for GraphqlSessionExecutor {
    async fn execute(&self, request: Request) -> GqlResponse {
        if let Some(Err(error)) = self.filter.as_ref().map(|filter| filter(&request)) {
            return GqlResponse::from_errors(vec![error]);
        }
        // Subscriptions must not go through execute() — that yields
        // "Subscription root not found". GraphiQL should use the WS path only.
        self.engine
            .execute(
                &self.session,
                request_with_context(
                    request,
                    self.principal.clone(),
                    self.host.as_ref().map(Arc::clone),
                ),
            )
            .await
    }

    fn execute_stream(
        &self,
        request: Request,
        session_data: Option<Arc<Data>>,
    ) -> BoxStream<'static, GqlResponse> {
        use std::any::TypeId;
        if let Some(Err(error)) = self.filter.as_ref().map(|filter| filter(&request)) {
            return Box::pin(futures_util::stream::once(async move {
                GqlResponse::from_errors(vec![error])
            }));
        }
        let session = session_data
            .as_ref()
            .and_then(|d| d.get(&TypeId::of::<Session>()))
            .and_then(|b| b.downcast_ref::<Session>())
            .cloned()
            .unwrap_or_else(|| self.session.clone());
        let principal = session_data
            .as_ref()
            .and_then(|data| data.get(&TypeId::of::<VerifiedPrincipal>()))
            .and_then(|principal| principal.downcast_ref::<VerifiedPrincipal>())
            .cloned()
            .or_else(|| self.principal.clone());
        let operation_type = match websocket_operation_type(&request) {
            Ok(operation_type) => operation_type,
            Err(message) => {
                return Box::pin(futures_util::stream::once(async move {
                    GqlResponse::from_errors(vec![ServerError::new(message, None)])
                }));
            }
        };
        let host = self.host.as_ref().map(Arc::clone);
        if operation_type == OperationType::Subscription {
            return self
                .engine
                .execute_stream(&session, request_with_context(request, principal, host));
        }

        let engine = Arc::clone(&self.engine);
        Box::pin(futures_util::stream::once(async move {
            engine
                .execute(&session, request_with_context(request, principal, host))
                .await
        }))
    }
}

fn websocket_operation_type(request: &Request) -> Result<OperationType, &'static str> {
    let document = async_graphql::parser::parse_query(&request.query)
        .map_err(|_| "invalid GraphQL document")?;
    let mut operations = document.operations.iter();
    if let Some(requested_name) = request.operation_name.as_deref() {
        return operations
            .find(|(name, _)| name.map(|name| name.as_str()) == Some(requested_name))
            .map(|(_, operation)| operation.node.ty)
            .ok_or("GraphQL operation name was not found");
    }

    let (_, operation) = operations
        .next()
        .ok_or("GraphQL document contains no operation")?;
    if operations.next().is_some() {
        return Err("GraphQL operation name is required for multi-operation documents");
    }
    Ok(operation.node.ty)
}

fn request_with_principal(request: Request, principal: Option<VerifiedPrincipal>) -> Request {
    match principal {
        Some(principal) => request.data(principal),
        None => request,
    }
}

fn request_with_context(
    request: Request,
    principal: Option<VerifiedPrincipal>,
    host: Option<SharedCommandHost>,
) -> Request {
    let request = request_with_principal(request, principal);
    match host {
        Some(host) => request.data(host),
        None => request,
    }
}

/// Standalone GraphQL router with its own body limit.
pub fn graphql_router(engine: Arc<GraphqlEngine>) -> Router {
    let graphiql = engine.graphiql_enabled();
    let mut router = Router::new().route(
        "/graphql",
        post(graphql_handler).get(move || async move {
            if graphiql {
                graphiql_page().into_response()
            } else {
                axum::http::StatusCode::METHOD_NOT_ALLOWED.into_response()
            }
        }),
    );
    router = router.layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES));
    router.with_state(engine)
}

/// GraphQL router that wait-dispatches through a local [`Service`] wrapped as
/// a [`LocalCommandHost`]. Request data holds the host, not `Arc<Service>`.
pub fn graphql_router_with_service(engine: Arc<GraphqlEngine>, service: Arc<Service>) -> Router {
    service
        .validate_graphql_engine(&engine)
        .unwrap_or_else(|error| panic!("cannot serve GraphQL with this service: {error}"));
    graphql_router_with_host(engine, Arc::new(LocalCommandHost::new(service)))
}

/// GraphQL router whose mutations dispatch through a local
/// [`LocalCommandDispatcher`] as a [`crate::command_dispatch::CommandHost`].
///
/// Does **not** unwrap [`LocalCommandDispatcher::service`] into GraphQL
/// request data (`DCS-AC-007.1`). Schema/client compilation never requires
/// this handle. [`RemoteCommandDispatcher`] HTTPS-mTLS
/// (`APPROVED_REMOTE_DISPATCH_PROFILE`) stays the CMP task-20 envelope;
/// wait-path remote is [`crate::command_dispatch::HttpCommandHost`].
pub fn graphql_router_with_dispatcher(
    engine: Arc<GraphqlEngine>,
    dispatcher: Arc<LocalCommandDispatcher>,
) -> Router {
    graphql_router_with_host(engine, dispatcher)
}

/// GraphQL router that wait-dispatches through an explicit command host.
pub fn graphql_router_with_host(engine: Arc<GraphqlEngine>, host: SharedCommandHost) -> Router {
    graphql_router_composed(engine, Some(host), None)
}

/// Compose the existing HTTP and WS handlers with an operation filter. A
/// query-only executor can omit the command host entirely. Callers still own
/// schema construction; filters cannot hide fields from schema introspection.
pub fn graphql_router_composed(
    engine: Arc<GraphqlEngine>,
    host: Option<SharedCommandHost>,
    filter: Option<GraphqlOperationFilter>,
) -> Router {
    let graphiql = engine.graphiql_enabled();
    let state = GraphqlHttpState {
        engine,
        host,
        filter,
    };
    let mut router = Router::new()
        .route(
            "/graphql",
            post(graphql_handler_with_service).get(move || async move {
                if graphiql {
                    graphiql_page().into_response()
                } else {
                    axum::http::StatusCode::METHOD_NOT_ALLOWED.into_response()
                }
            }),
        )
        .route("/graphql/ws", get(graphql_ws_with_host));
    router = router.layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES));
    router.with_state(state)
}

#[derive(Clone)]
struct GraphqlHttpState {
    engine: Arc<GraphqlEngine>,
    host: Option<SharedCommandHost>,
    filter: Option<GraphqlOperationFilter>,
}

fn unauthorized_response() -> Response {
    (
        axum::http::StatusCode::UNAUTHORIZED,
        [("content-type", "application/json")],
        r#"{"errors":[{"message":"unauthorized","extensions":{"code":"UNAUTHENTICATED"}}]}"#,
    )
        .into_response()
}

fn lifecycle_reloading_response() -> Response {
    (
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        [("content-type", "application/json")],
        r#"{"errors":[{"message":"application generation is reloading","extensions":{"code":"APPLICATION_RELOADING"}}]}"#,
    )
        .into_response()
}

// Gateway-delivered WS operations carry the original connection_init payload.
// Only the origin's existing credential parser/validator resolves authority;
// the gateway neither decodes roles nor fabricates a provider identity.
async fn resolve_http_identity(
    engine: &GraphqlEngine,
    headers: &HeaderMap,
    request: &Request,
) -> Result<ResolvedIdentity, AuthError> {
    #[cfg(feature = "gateway-delivery")]
    if let Some(init) = request
        .extensions
        .get("gatewayDelivery")
        .and_then(|value| serde_json::to_value(value).ok())
        .and_then(|value| value.get("connectionInit").cloned())
    {
        return resolve_gateway_ws_identity(engine, headers, &init)
            .await
            .map_err(|_| AuthError::Unauthorized);
    }
    let _ = request;
    resolve_identity_with_validator(
        headers,
        engine.identity_config(),
        engine.identity_validator(),
    )
    .await
}
#[cfg(feature = "gateway-delivery")]
pub(crate) async fn resolve_gateway_ws_identity(
    engine: &GraphqlEngine,
    headers: &HeaderMap,
    init: &serde_json::Value,
) -> Result<ResolvedIdentity, String> {
    let base = match engine.identity_config().mode {
        IdentityMode::OidcBearer | IdentityMode::Hybrid => Session::new(),
        _ => {
            resolve_identity_with_validator(
                headers,
                engine.identity_config(),
                engine.identity_validator(),
            )
            .await
            .map_err(|_| "unauthorized".to_owned())?
            .into_parts()
            .0
        }
    };
    resolve_ws_identity(engine, headers, base, init).await
}

async fn graphql_handler(
    State(engine): State<Arc<GraphqlEngine>>,
    headers: axum::http::HeaderMap,
    req: GraphQLRequest,
) -> Response {
    let request = req.into_inner();
    if !crate::microsvc::lifecycle_mutations_open()
        && matches!(
            websocket_operation_type(&request),
            Ok(OperationType::Mutation)
        )
    {
        return lifecycle_reloading_response();
    }
    let identity = match resolve_http_identity(&engine, &headers, &request).await {
        Ok(identity) => identity,
        Err(AuthError::Unauthorized) => return unauthorized_response(),
    };
    let (session, principal) = identity.into_parts();
    // GraphiQL demo headers only apply under DevHeaders; other modes ignore them for AuthZ.
    let _ = engine.identity_config().mode == IdentityMode::DevHeaders;
    let request = request_with_principal(request, principal);
    let response = engine.execute(&session, request).await;
    GraphQLResponse::from(response).into_response()
}

async fn graphql_handler_with_service(
    State(state): State<GraphqlHttpState>,
    headers: axum::http::HeaderMap,
    req: GraphQLRequest,
) -> Response {
    let request = req.into_inner();
    if !crate::microsvc::lifecycle_mutations_open()
        && matches!(
            websocket_operation_type(&request),
            Ok(OperationType::Mutation)
        )
    {
        return lifecycle_reloading_response();
    }
    let identity = match resolve_http_identity(&state.engine, &headers, &request).await {
        Ok(identity) => identity,
        Err(AuthError::Unauthorized) => return unauthorized_response(),
    };
    let (session, principal) = identity.into_parts();
    if let Some(Err(error)) = state.filter.as_ref().map(|filter| filter(&request)) {
        return GraphQLResponse::from(GqlResponse::from_errors(vec![error])).into_response();
    }
    let mut request = request_with_principal(request, principal);
    if let Some(host) = &state.host {
        request = request.data(Arc::clone(host));
    }
    let response = state.engine.execute(&session, request).await;
    GraphQLResponse::from(response).into_response()
}

/// Handler used when GraphQL is mounted on the microsvc router.
pub async fn microsvc_graphql_handler(
    State(service): State<Arc<Service>>,
    headers: axum::http::HeaderMap,
    req: GraphQLRequest,
) -> Response {
    let request = req.into_inner();
    if !crate::microsvc::lifecycle_mutations_open()
        && matches!(
            websocket_operation_type(&request),
            Ok(OperationType::Mutation)
        )
    {
        return lifecycle_reloading_response();
    }
    let engine = service
        .graphql_engine()
        .expect("graphql route mounted without engine");
    let identity = match resolve_http_identity(&engine, &headers, &request).await {
        Ok(identity) => identity,
        Err(AuthError::Unauthorized) => return unauthorized_response(),
    };
    let (session, principal) = identity.into_parts();
    let host: SharedCommandHost = Arc::new(LocalCommandHost::new(Arc::clone(&service)));
    let request = request_with_principal(request, principal).data(host);
    let response = engine.execute(&session, request).await;
    GraphQLResponse::from(response).into_response()
}

/// `GET /graphql` — GraphiQL HTML when not a WebSocket upgrade.
pub async fn microsvc_graphql_get(State(service): State<Arc<Service>>) -> Response {
    let graphiql = service
        .graphql_engine()
        .map(|e| e.graphiql_enabled())
        .unwrap_or(false);
    if graphiql {
        graphiql_page().into_response()
    } else {
        StatusCode::METHOD_NOT_ALLOWED.into_response()
    }
}

/// WebSocket upgrade for GraphQL subscriptions (`graphql-ws` / `graphql-transport-ws`).
///
/// **Auth best practice (OIDC):** browsers cannot set `Authorization` on the WS
/// upgrade. Accept the upgrade, then require a Bearer access token in
/// `connection_init` (payload `authorization` / `accessToken` / nested
/// `headers.Authorization`). Validate with the same OidcBearer path as HTTP.
///
/// **DevHeaders (local):** identity from upgrade headers, query params, or
/// GraphiQL `wsConnectionParams` (`x-user-id` / `x-roles`). Empty clients remain
/// anonymous; the GraphiQL page sends its demo identity explicitly.
pub async fn microsvc_graphql_ws(
    State(service): State<Arc<Service>>,
    headers: HeaderMap,
    uri: axum::http::Uri,
    protocol: GraphQLProtocol,
    guard: Option<axum::Extension<Arc<GraphqlConnectionGuard>>>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let engine = match service.graphql_engine() {
        Some(e) => e,
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    let host: SharedCommandHost = Arc::new(LocalCommandHost::new(Arc::clone(&service)));
    graphql_ws_upgrade(
        engine,
        Some(host),
        GraphqlWsOptions {
            filter: None,
            guard: guard.map(|guard| guard.0),
        },
        headers,
        uri,
        protocol,
        upgrade,
    )
    .await
}

async fn graphql_ws_with_host(
    State(state): State<GraphqlHttpState>,
    headers: HeaderMap,
    uri: axum::http::Uri,
    protocol: GraphQLProtocol,
    guard: Option<axum::Extension<Arc<GraphqlConnectionGuard>>>,
    upgrade: WebSocketUpgrade,
) -> Response {
    graphql_ws_upgrade(
        state.engine,
        state.host,
        GraphqlWsOptions {
            filter: state.filter,
            guard: guard.map(|guard| guard.0),
        },
        headers,
        uri,
        protocol,
        upgrade,
    )
    .await
}

async fn graphql_ws_upgrade(
    engine: Arc<GraphqlEngine>,
    host: Option<SharedCommandHost>,
    options: GraphqlWsOptions,
    headers: HeaderMap,
    uri: axum::http::Uri,
    protocol: GraphQLProtocol,
    upgrade: WebSocketUpgrade,
) -> Response {
    let mut upgrade_headers = headers;
    merge_identity_query_params(&mut upgrade_headers, uri.query());
    let mode = engine.identity_config().mode;

    // OidcBearer: do not fail the upgrade without a token — auth happens in
    // connection_init (graphql-ws best practice). DevHeaders: resolve now.
    let upgrade_identity = if mode == IdentityMode::OidcBearer || mode == IdentityMode::Hybrid {
        ResolvedIdentity::unverified(Session::new())
    } else {
        match resolve_identity_with_validator(
            &upgrade_headers,
            engine.identity_config(),
            engine.identity_validator(),
        )
        .await
        {
            Ok(identity) => identity,
            Err(AuthError::Unauthorized) => return unauthorized_response(),
        }
    };

    let (upgrade_session, upgrade_principal) = upgrade_identity.into_parts();
    let executor = GraphqlSessionExecutor::with_identity(
        Arc::clone(&engine),
        upgrade_session.clone(),
        upgrade_principal,
        host,
        options.filter,
    );
    let engine_for_init = Arc::clone(&engine);
    upgrade
        .protocols(ALL_WEBSOCKET_PROTOCOLS)
        .on_upgrade(move |socket| {
            let base = upgrade_session;
            let upgrade_headers = upgrade_headers;
            let serve = GraphQLWebSocket::new(socket, executor, protocol)
                .on_connection_init(move |payload| {
                    let base = base.clone();
                    let engine = engine_for_init;
                    let upgrade_headers = upgrade_headers;
                    async move {
                        let identity =
                            match resolve_ws_identity(&engine, &upgrade_headers, base, &payload)
                                .await
                            {
                                Ok(identity) => identity,
                                Err(msg) => {
                                    return Err(async_graphql::Error::new(msg));
                                }
                            };
                        let (session, principal) = identity.into_parts();
                        let mut data = Data::default();
                        data.insert(session);
                        if let Some(principal) = principal {
                            data.insert(principal);
                        }
                        Ok(data)
                    }
                })
                .serve();
            async move {
                if let Some(guard) = options.guard {
                    let _ = tokio::time::timeout(guard.lifetime, serve).await;
                    drop(guard);
                } else {
                    serve.await;
                }
            }
        })
        .into_response()
}

/// Resolve session for a GraphQL WS connection after `connection_init`.
async fn resolve_ws_identity(
    engine: &GraphqlEngine,
    upgrade_headers: &HeaderMap,
    base: Session,
    payload: &serde_json::Value,
) -> Result<ResolvedIdentity, String> {
    let mode = engine.identity_config().mode;
    match mode {
        IdentityMode::OidcBearer | IdentityMode::Hybrid => {
            let mut headers = upgrade_headers.clone();
            if let Some(auth) = bearer_from_connection_init(payload) {
                if let Ok(val) = axum::http::HeaderValue::from_str(&auth) {
                    headers.insert(axum::http::header::AUTHORIZATION, val);
                }
            }
            resolve_identity_with_validator(
                &headers,
                engine.identity_config(),
                engine.identity_validator(),
            )
            .await
            .map_err(|_| {
                "unauthorized: provide Authorization Bearer access_token in connection_init".into()
            })
        }
        IdentityMode::DevHeaders | IdentityMode::TrustedProxy => Ok(ResolvedIdentity::unverified(
            session_from_connection_init(base, payload, mode),
        )),
    }
}

/// Extract Bearer token from connection_init payload (several client conventions).
fn bearer_from_connection_init(payload: &serde_json::Value) -> Option<String> {
    let pick = |v: &serde_json::Value| -> Option<String> {
        let s = v.as_str()?.trim();
        if s.is_empty() {
            return None;
        }
        if s.get(..7)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("bearer "))
        {
            Some(s.to_string())
        } else {
            Some(format!("Bearer {s}"))
        }
    };
    if let Some(s) = payload
        .get("Authorization")
        .or_else(|| payload.get("authorization"))
        .and_then(pick)
    {
        return Some(s);
    }
    if let Some(s) = payload
        .get("accessToken")
        .or_else(|| payload.get("access_token"))
        .and_then(pick)
    {
        return Some(s);
    }
    if let Some(headers) = payload.get("headers").and_then(|h| h.as_object()) {
        if let Some(s) = headers
            .get("Authorization")
            .or_else(|| headers.get("authorization"))
            .and_then(pick)
        {
            return Some(s);
        }
    }
    None
}

/// Merge `connection_init` / GraphiQL wsConnectionParams into the upgrade session.
fn session_from_connection_init(
    mut session: Session,
    payload: &serde_json::Value,
    mode: IdentityMode,
) -> Session {
    if mode != IdentityMode::DevHeaders {
        // OIDC: allow Authorization in connection_init for WS clients that put the
        // Bearer token there (GraphiQL Headers panel may not reach the upgrade).
        if let Some(auth) = payload
            .get("Authorization")
            .or_else(|| payload.get("authorization"))
            .and_then(|v| v.as_str())
        {
            session.set("authorization", auth);
        }
        if let Some(headers) = payload.get("headers").and_then(|h| h.as_object()) {
            if let Some(auth) = headers
                .get("Authorization")
                .or_else(|| headers.get("authorization"))
                .and_then(|v| v.as_str())
            {
                session.set("authorization", auth);
            }
        }
        return session;
    }

    let apply = |session: &mut Session, key: &str, val: &str| {
        if key.eq_ignore_ascii_case(USER_ID_KEY) || key.eq_ignore_ascii_case("x-user-id") {
            session.set(USER_ID_KEY, val);
        } else if key.eq_ignore_ascii_case("x-roles") || key.eq_ignore_ascii_case("X-Roles") {
            session.set("x-roles", val);
        }
    };

    if let Some(obj) = payload.as_object() {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                apply(&mut session, k, s);
            }
        }
        if let Some(headers) = obj.get("headers").and_then(|h| h.as_object()) {
            for (k, v) in headers {
                if let Some(s) = v.as_str() {
                    apply(&mut session, k, s);
                }
            }
        }
    }

    session
}

fn merge_identity_query_params(headers: &mut HeaderMap, query: Option<&str>) {
    let Some(q) = query else {
        return;
    };
    for pair in q.split('&') {
        let mut it = pair.splitn(2, '=');
        let k = it.next().unwrap_or("");
        let v = urlencoding_decode(it.next().unwrap_or(""));
        match k {
            "x-user-id" if !headers.contains_key("x-user-id") => {
                if let Ok(val) = axum::http::HeaderValue::from_str(&v) {
                    headers.insert("x-user-id", val);
                }
            }
            "x-roles" if !headers.contains_key("x-roles") => {
                if let Ok(val) = axum::http::HeaderValue::from_str(&v) {
                    headers.insert("x-roles", val);
                }
            }
            _ => {}
        }
    }
}

fn urlencoding_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => {
                out.push(' ');
                i += 1;
            }
            b'%' if i + 2 < b.len() => {
                let h = |c: u8| -> Option<u8> {
                    match c {
                        b'0'..=b'9' => Some(c - b'0'),
                        b'a'..=b'f' => Some(c - b'a' + 10),
                        b'A'..=b'F' => Some(c - b'A' + 10),
                        _ => None,
                    }
                };
                if let (Some(hi), Some(lo)) = (h(b[i + 1]), h(b[i + 2])) {
                    out.push((hi << 4 | lo) as char);
                    i += 3;
                } else {
                    out.push('%');
                    i += 1;
                }
            }
            c => {
                out.push(c as char);
                i += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod connection_init_tests {
    use super::*;
    use crate::graphql::IdentityMode;
    use serde_json::json;
    use std::any::TypeId;

    #[test]
    fn websocket_request_context_retains_command_host_not_service() {
        let service = Arc::new(Service::new());
        let host: SharedCommandHost = Arc::new(LocalCommandHost::new(Arc::clone(&service)));
        let request = request_with_context(
            Request::new("{ __typename }"),
            None,
            Some(Arc::clone(&host)),
        );
        assert!(request.data.get(&TypeId::of::<Arc<Service>>()).is_none());
        request
            .data
            .get(&TypeId::of::<SharedCommandHost>())
            .and_then(|host| host.downcast_ref::<SharedCommandHost>())
            .expect("command host request data");
    }

    #[test]
    fn dispatcher_as_command_host_does_not_put_service_in_request_data() {
        let service = Arc::new(Service::new());
        let dispatcher = Arc::new(LocalCommandDispatcher::new(Arc::clone(&service)));
        let host: SharedCommandHost = dispatcher;
        let request = request_with_context(
            Request::new("{ __typename }"),
            None,
            Some(Arc::clone(&host)),
        );
        assert!(request.data.get(&TypeId::of::<Arc<Service>>()).is_none());
        request
            .data
            .get(&TypeId::of::<SharedCommandHost>())
            .and_then(|host| host.downcast_ref::<SharedCommandHost>())
            .expect("command host request data");
    }

    #[test]
    fn websocket_operation_routing_is_explicit_and_unambiguous() {
        assert_eq!(
            websocket_operation_type(&Request::new("{ __typename }")),
            Ok(OperationType::Query)
        );
        assert_eq!(
            websocket_operation_type(&Request::new("mutation Named { __typename }")),
            Ok(OperationType::Mutation)
        );
        assert_eq!(
            websocket_operation_type(
                &Request::new("query Read { __typename } subscription Watch { __typename }")
                    .operation_name("Watch")
            ),
            Ok(OperationType::Subscription)
        );
        assert_eq!(
            websocket_operation_type(&Request::new(
                "query Read { __typename } mutation Write { __typename }"
            )),
            Err("GraphQL operation name is required for multi-operation documents")
        );
        assert_eq!(
            websocket_operation_type(
                &Request::new("query Read { __typename }").operation_name("Missing")
            ),
            Err("GraphQL operation name was not found")
        );
        assert_eq!(
            websocket_operation_type(&Request::new("not graphql")),
            Err("invalid GraphQL document")
        );
    }

    #[test]
    fn connection_init_sets_dev_headers() {
        let session = session_from_connection_init(
            Session::new(),
            &json!({"x-user-id": "alice", "x-roles": "user"}),
            IdentityMode::DevHeaders,
        );
        assert_eq!(session.user_id(), Some("alice"));
        assert_eq!(session.get("x-roles"), Some("user"));
    }

    #[test]
    fn connection_init_nested_headers() {
        let session = session_from_connection_init(
            Session::new(),
            &json!({"headers": {"x-user-id": "bob", "x-roles": "admin"}}),
            IdentityMode::DevHeaders,
        );
        assert_eq!(session.user_id(), Some("bob"));
        assert_eq!(session.get("x-roles"), Some("admin"));
    }

    #[test]
    fn empty_init_remains_anonymous_in_dev() {
        let session =
            session_from_connection_init(Session::new(), &json!({}), IdentityMode::DevHeaders);
        assert_eq!(session.user_id(), None);
        assert!(session.roles().is_empty());
    }

    #[test]
    fn bearer_from_connection_init_shapes() {
        assert_eq!(
            bearer_from_connection_init(&json!({"authorization": "Bearer abc"})).as_deref(),
            Some("Bearer abc")
        );
        assert_eq!(
            bearer_from_connection_init(&json!({"accessToken": "tok"})).as_deref(),
            Some("Bearer tok")
        );
        assert_eq!(
            bearer_from_connection_init(&json!({"headers": {"Authorization": "Bearer z"}}))
                .as_deref(),
            Some("Bearer z")
        );
        assert!(bearer_from_connection_init(&json!({})).is_none());
    }

    #[test]
    fn bearer_from_connection_init_accepts_non_ascii_authorization() {
        assert_eq!(
            bearer_from_connection_init(&json!({"authorization": "éééé"})).as_deref(),
            Some("Bearer éééé")
        );
    }

    #[test]
    fn bearer_from_connection_init_accepts_non_ascii_access_token() {
        assert_eq!(
            bearer_from_connection_init(&json!({"access_token": "éééé"})).as_deref(),
            Some("Bearer éééé")
        );
    }
}
