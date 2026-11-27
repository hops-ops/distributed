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

use crate::command_dispatch::SharedCommandHost;
use crate::microsvc::{Service, Session, MAX_HTTP_BODY_BYTES, USER_ID_KEY};

use super::engine::GraphqlEngine;
use super::identity::{AuthError, IdentityConfig, ResolvedIdentity};
use crate::command_dispatch::VerifiedPrincipal;

/// DevHeaders baked into GraphiQL for local exploration (not production security).
const GRAPHIQL_DEV_USER: &str = "demo";
const GRAPHIQL_DEV_ROLE: &str = "user";

/// HTML for the GraphiQL IDE served on `GET /graphql` when GraphiQL is enabled.
///
/// Default identity headers (`x-roles: user`, `x-user-id: demo`) are injected
/// only for local exploration. Production services use `OidcBearer` (D6) —
/// these headers are **not** a security mechanism. GraphiQL is off under
/// production env policy (`graphiql_enabled_from_env`).
///
/// **Subscriptions:** GraphiQL runs them over WebSocket at `/graphql/ws`.
/// Browsers cannot set custom WS headers; identity is supplied via
/// `wsConnectionParams` → `connection_init`. Identity is never read from the
/// URL query string, in any mode.
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

/// [`Executor`] for one WebSocket connection.
///
/// Operations run as the [`Session`] resolved at `connection_init`; until
/// then (and for a caller that supplies no session data) as `session`.
#[derive(Clone)]
pub struct GraphqlSessionExecutor {
    engine: Arc<GraphqlEngine>,
    session: Session,
    host: Option<SharedCommandHost>,
    filter: Option<GraphqlOperationFilter>,
    /// One permit per open subscription on this connection.
    live_queries: Arc<tokio::sync::Semaphore>,
}

impl GraphqlSessionExecutor {
    pub fn new(engine: Arc<GraphqlEngine>, session: Session) -> Self {
        let live_queries = engine.websocket_limits().max_live_queries;
        Self {
            engine,
            session,
            host: None,
            filter: None,
            live_queries: Arc::new(tokio::sync::Semaphore::new(live_queries)),
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
                request_with_context(request, None, self.host.as_ref().map(Arc::clone)),
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
            .cloned();
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
            let Ok(permit) = Arc::clone(&self.live_queries).try_acquire_owned() else {
                return Box::pin(futures_util::stream::once(async {
                    GqlResponse::from_errors(vec![ServerError::new(
                        "too many live queries on this connection",
                        None,
                    )])
                }));
            };
            let stream = self
                .engine
                .execute_stream(&session, request_with_context(request, principal, host));
            // The permit is released when the client stops the subscription
            // or the connection ends, whichever drops the stream.
            return Box::pin(futures_util::StreamExt::map(stream, move |response| {
                let _held = &permit;
                response
            }));
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

/// GraphQL router that wait-dispatches through a local [`Service`], the
/// in-process command host. Request data holds it as a [`SharedCommandHost`].
pub fn graphql_router_with_service(engine: Arc<GraphqlEngine>, service: Arc<Service>) -> Router {
    service
        .validate_graphql_engine(&engine)
        .unwrap_or_else(|error| panic!("cannot serve GraphQL with this service: {error}"));
    graphql_router_with_host(engine, service)
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
// Only the origin's own identity resolver decides authority; the gateway
// neither decodes roles nor fabricates a provider identity.
async fn resolve_http_identity(
    engine: &GraphqlEngine,
    headers: &HeaderMap,
    request: &Request,
) -> Result<ResolvedIdentity, AuthError> {
    #[cfg(feature = "gateway")]
    if let Some(init) = request
        .extensions
        .get("gatewayDelivery")
        .and_then(|value| serde_json::to_value(value).ok())
        .and_then(|value| value.get("connectionInit").cloned())
    {
        return resolve_ws_identity(engine, headers, &init).await;
    }
    let _ = request;
    engine.identity().resolve_identity(headers).await
}

async fn graphql_handler(
    State(engine): State<Arc<GraphqlEngine>>,
    headers: axum::http::HeaderMap,
    req: GraphQLRequest,
) -> Response {
    let mut request = req.into_inner();
    if engine.refuses_mutation(&mut request) {
        return lifecycle_reloading_response();
    }
    let identity = match resolve_http_identity(&engine, &headers, &request).await {
        Ok(identity) => identity,
        Err(AuthError::Unauthorized) => return unauthorized_response(),
    };
    let (session, principal) = identity.into_parts();
    let request = request_with_principal(request, principal);
    let response = engine.execute_admitted(&session, request).await;
    GraphQLResponse::from(response).into_response()
}

async fn graphql_handler_with_service(
    State(state): State<GraphqlHttpState>,
    headers: axum::http::HeaderMap,
    req: GraphQLRequest,
) -> Response {
    let mut request = req.into_inner();
    if state.engine.refuses_mutation(&mut request) {
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
    let response = state.engine.execute_admitted(&session, request).await;
    GraphQLResponse::from(response).into_response()
}

/// Handler used when GraphQL is mounted on the microsvc router.
pub async fn microsvc_graphql_handler(
    State(service): State<Arc<Service>>,
    headers: axum::http::HeaderMap,
    req: GraphQLRequest,
) -> Response {
    let mut request = req.into_inner();
    let engine = service
        .graphql_engine()
        .expect("graphql route mounted without engine");
    if engine.refuses_mutation(&mut request) {
        return lifecycle_reloading_response();
    }
    let identity = match resolve_http_identity(&engine, &headers, &request).await {
        Ok(identity) => identity,
        Err(AuthError::Unauthorized) => return unauthorized_response(),
    };
    let (session, principal) = identity.into_parts();
    let host: SharedCommandHost = Arc::clone(&service) as SharedCommandHost;
    let request = request_with_principal(request, principal).data(host);
    let response = engine.execute_admitted(&session, request).await;
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
/// **Gateway:** the upgrade request must carry the gateway secret; the
/// `connection_init` payload is the client's and is ignored.
///
/// **DevHeaders (local):** identity from upgrade headers or GraphiQL
/// `wsConnectionParams` (`x-user-id` / `x-roles`). Empty clients remain
/// anonymous; the GraphiQL page sends its demo identity explicitly.
///
/// Every connection is bounded by the engine's [`WebsocketLimits`](super::WebsocketLimits).
pub async fn microsvc_graphql_ws(
    State(service): State<Arc<Service>>,
    headers: HeaderMap,
    protocol: GraphQLProtocol,
    guard: Option<axum::Extension<Arc<GraphqlConnectionGuard>>>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let engine = match service.graphql_engine() {
        Some(e) => e,
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    let host: SharedCommandHost = Arc::clone(&service) as SharedCommandHost;
    graphql_ws_upgrade(
        engine,
        Some(host),
        GraphqlWsOptions {
            filter: None,
            guard: guard.map(|guard| guard.0),
        },
        headers,
        protocol,
        upgrade,
    )
    .await
}

async fn graphql_ws_with_host(
    State(state): State<GraphqlHttpState>,
    headers: HeaderMap,
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
    protocol: GraphQLProtocol,
    upgrade: WebSocketUpgrade,
) -> Response {
    // A gateway proves itself on the upgrade request, so refuse before
    // upgrading. The other modes authenticate at `connection_init`.
    if matches!(engine.identity_config(), IdentityConfig::Gateway(_))
        && engine.identity().resolve_identity(&headers).await.is_err()
    {
        return unauthorized_response();
    }
    let limits = engine.websocket_limits();
    let lifetime = options.guard.as_ref().map_or(limits.max_lifetime, |guard| {
        guard.lifetime.min(limits.max_lifetime)
    });
    let mut executor = GraphqlSessionExecutor::new(Arc::clone(&engine), Session::new());
    executor.host = host;
    executor.filter = options.filter;
    let guard = options.guard;
    upgrade
        .protocols(ALL_WEBSOCKET_PROTOCOLS)
        .on_upgrade(move |socket| async move {
            let _guard = guard;
            let (expiry, token_expiry) = tokio::sync::oneshot::channel();
            let (sink, stream) = futures_util::StreamExt::split(socket);
            let (outbound, queued) = futures_channel::mpsc::channel(16);
            let last_heard = Arc::new(std::sync::Mutex::new(tokio::time::Instant::now()));
            let heard = Arc::clone(&last_heard);
            let stream = futures_util::StreamExt::inspect(stream, move |_| {
                *heard
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = tokio::time::Instant::now();
            });
            let serve = GraphQLWebSocket::new_with_pair(outbound, stream, executor, protocol)
                .on_connection_init(move |payload| async move {
                    let identity = resolve_ws_identity(&engine, &headers, &payload)
                        .await
                        .map_err(|_| async_graphql::Error::new("unauthorized"))?;
                    let (session, principal) = identity.into_parts();
                    let _ = expiry.send(principal.as_ref().and_then(VerifiedPrincipal::expires_at));
                    let mut data = Data::default();
                    data.insert(session);
                    if let Some(principal) = principal {
                        data.insert(principal);
                    }
                    Ok(data)
                })
                .serve();
            serve_bounded(
                serve,
                sink,
                queued,
                last_heard,
                token_expiry,
                lifetime,
                limits.idle_timeout,
            )
            .await;
        })
        .into_response()
}

/// Drive one WebSocket until the GraphQL session ends, the connection
/// lifetime or the authenticating token runs out, or the peer goes quiet.
///
/// GraphQL frames are relayed through `queued` so this loop owns the socket's
/// write half and can interleave WebSocket pings. A live query whose result
/// never changes writes nothing, so without the pings a vanished client would
/// keep its producers alive indefinitely.
async fn serve_bounded<S>(
    serve: impl std::future::Future<Output = ()>,
    mut sink: S,
    mut queued: futures_channel::mpsc::Receiver<axum::extract::ws::Message>,
    last_heard: Arc<std::sync::Mutex<tokio::time::Instant>>,
    token_expiry: tokio::sync::oneshot::Receiver<Option<u64>>,
    lifetime: std::time::Duration,
    idle_timeout: std::time::Duration,
) where
    S: futures_util::Sink<axum::extract::ws::Message> + Unpin,
{
    use axum::extract::ws::{CloseFrame, Message};
    use futures_util::{SinkExt, StreamExt};
    use std::time::Duration;

    let token_expiry = async {
        match token_expiry.await {
            Ok(Some(expires_at)) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |elapsed| elapsed.as_secs());
                tokio::time::sleep(Duration::from_secs(expires_at.saturating_sub(now))).await;
            }
            // No expiring credential, or the connection ended before init.
            _ => std::future::pending().await,
        }
    };
    let lifetime = tokio::time::sleep(lifetime);
    let ping_every = idle_timeout / 3;
    let mut ping = tokio::time::interval_at(tokio::time::Instant::now() + ping_every, ping_every);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tokio::pin!(serve, token_expiry, lifetime);

    // A send that cannot complete within the idle timeout is a dead peer too.
    let mut serving = true;
    let close = loop {
        let outgoing = tokio::select! {
            _ = &mut serve, if serving => {
                serving = false;
                continue;
            }
            message = queued.next() => match message {
                Some(message) => message,
                None => break None,
            },
            _ = ping.tick() => {
                let heard = *last_heard.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                if heard.elapsed() >= idle_timeout {
                    break None;
                }
                Message::Ping(Default::default())
            }
            _ = &mut lifetime => break Some((1000, "connection lifetime reached")),
            _ = &mut token_expiry => break Some((4401, "token expired")),
        };
        if !matches!(
            tokio::time::timeout(idle_timeout, sink.send(outgoing)).await,
            Ok(Ok(()))
        ) {
            break None;
        }
    };
    if let Some((code, reason)) = close {
        let frame = Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        }));
        let _ = tokio::time::timeout(Duration::from_secs(1), sink.send(frame)).await;
    }
}

/// Resolve the identity of a WebSocket connection once `connection_init` has
/// arrived. `headers` are the upgrade request's.
pub(crate) async fn resolve_ws_identity(
    engine: &GraphqlEngine,
    headers: &HeaderMap,
    payload: &serde_json::Value,
) -> Result<ResolvedIdentity, AuthError> {
    let resolver = engine.identity();
    match resolver.config() {
        // Browsers cannot set `Authorization` on an upgrade, so the bearer
        // token travels in the init payload.
        IdentityConfig::OidcBearer(_) => {
            let mut headers = headers.clone();
            if let Some(bearer) = bearer_from_connection_init(payload)
                .and_then(|bearer| axum::http::HeaderValue::from_str(&bearer).ok())
            {
                headers.insert(axum::http::header::AUTHORIZATION, bearer);
            }
            resolver.resolve_identity(&headers).await
        }
        // Only what the gateway put on the upgrade is trusted; the payload
        // is written by the client.
        IdentityConfig::Gateway(_) => resolver.resolve_identity(headers).await,
        IdentityConfig::DevHeaders => {
            let (session, _) = resolver.resolve_identity(headers).await?.into_parts();
            Ok(ResolvedIdentity::unverified(
                dev_session_from_connection_init(session, payload),
            ))
        }
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

/// DevHeaders only: merge GraphiQL `wsConnectionParams` into the session.
fn dev_session_from_connection_init(mut session: Session, payload: &serde_json::Value) -> Session {
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

#[cfg(test)]
mod connection_init_tests {
    use super::*;
    use serde_json::json;
    use std::any::TypeId;

    #[test]
    fn websocket_request_context_retains_command_host_not_service() {
        let service = Arc::new(Service::new());
        let host: SharedCommandHost = Arc::clone(&service) as SharedCommandHost;
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
        let session = dev_session_from_connection_init(
            Session::new(),
            &json!({"x-user-id": "alice", "x-roles": "user"}),
        );
        assert_eq!(session.user_id(), Some("alice"));
        assert_eq!(session.get("x-roles"), Some("user"));
    }

    #[test]
    fn connection_init_nested_headers() {
        let session = dev_session_from_connection_init(
            Session::new(),
            &json!({"headers": {"x-user-id": "bob", "x-roles": "admin"}}),
        );
        assert_eq!(session.user_id(), Some("bob"));
        assert_eq!(session.get("x-roles"), Some("admin"));
    }

    #[test]
    fn empty_init_remains_anonymous_in_dev() {
        let session = dev_session_from_connection_init(Session::new(), &json!({}));
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
