use super::{
    proxy, response, Body, GatewayError, HeaderValue, NativeInner, Request, RequestContext,
    Response, Router, StatusCode, Url,
};
use crate::command_dispatch::SharedCommandHost;
use crate::gateway::{
    graphql::{
        admit_operation, admit_request, remove_client_control,
        ws::{self, ClientFrame, Operations, ServerFrame, WsProtocol},
        OperationError,
    },
    BindingKind, DeliveryCapabilities, Forwarding, GraphqlCapabilities, GraphqlExecutor,
};
use crate::graphql::http::{graphql_router_composed, GraphqlOperationFilter};
use crate::graphql::GraphqlEngine;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        FromRequestParts,
    },
    http::header,
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use std::{collections::BTreeSet, sync::Arc};
use tokio_tungstenite::{
    tungstenite::{protocol::Role, Message as UpstreamMessage},
    WebSocketStream,
};
use tower::ServiceExt;

/// Complete embedded executor, with explicit capabilities and extension
/// registration. The executor owns the composed schema and authorization.
#[derive(Clone)]
pub struct EmbeddedGraphql {
    pub(super) router: Router,
    pub(super) engine: Option<Arc<GraphqlEngine>>,
    capabilities: GraphqlCapabilities,
    extensions: BTreeSet<String>,
}
impl EmbeddedGraphql {
    /// Mount the framework's existing HTTP/WS handlers. Build query-only schemas
    /// without command inventory and with subscriptions(false). This validates
    /// absent capabilities against the actual role surfaces before serving.
    pub fn new(
        engine: Arc<GraphqlEngine>,
        host: Option<SharedCommandHost>,
        capabilities: GraphqlCapabilities,
    ) -> Result<Self, GatewayError> {
        for surface in engine.inner.role_surfaces.values() {
            if (!capabilities.commands && !surface.commands.is_empty())
                || (!capabilities.live && !surface.subscription_fields.is_empty())
            {
                return Err(GatewayError(
                    "embedded schema exposes an unmounted capability",
                ));
            }
        }
        if capabilities.commands && host.is_none() {
            return Err(GatewayError("command surface requires a command host"));
        }
        Ok(Self {
            engine: Some(engine.clone()),
            router: graphql_router_composed(engine, host, Some(operation_filter(capabilities))),
            capabilities,
            extensions: BTreeSet::new(),
        })
    }

    /// Bind an application-composed executor (including custom GraphQL fields).
    /// The factory must install the supplied filter in its HTTP and WS execution
    /// paths, just as graphql_router_composed does. Custom WS handlers must also
    /// retain the request extension `GraphqlConnectionGuard` through the socket
    /// lifetime and run the connection with its `run` method. It is a trusted executor
    /// extension seam, not arbitrary field federation. Register fields here at
    /// this executor; declare their registration IDs in the gateway config.
    pub fn custom(
        factory: impl FnOnce(GraphqlOperationFilter) -> Router,
        capabilities: GraphqlCapabilities,
        extensions: impl IntoIterator<Item = String>,
    ) -> Result<Self, GatewayError> {
        let mut registered = BTreeSet::new();
        for name in extensions {
            super::super::config::validate_id(&name)?;
            if !registered.insert(name) {
                return Err(GatewayError("duplicate schema extension registration"));
            }
        }
        Ok(Self {
            router: factory(operation_filter(capabilities)),
            engine: None,
            capabilities,
            extensions: registered,
        })
    }
}

/// Paths at a complete remote GraphQL executor. Its origin comes from the
/// validated portable binding, never from a caller request or query variable.
#[derive(Clone, Debug)]
pub struct RemoteGraphql {
    /// Remote HTTP operation path.
    pub http_path: String,
    /// Remote WebSocket operation path, required when live is mounted.
    pub live_path: Option<String>,
}
impl Default for RemoteGraphql {
    fn default() -> Self {
        Self {
            http_path: "/graphql".into(),
            live_path: Some("/graphql/ws".into()),
        }
    }
}

/// One executor resource for a portable GraphQL binding.
#[derive(Clone)]
pub enum GraphqlBinding {
    /// Executor in the same process, sharing its existing schema and command host.
    Embedded(EmbeddedGraphql),
    /// Whole-operation remote HTTP and WS transport.
    Remote(RemoteGraphql),
}

fn operation_filter(capabilities: GraphqlCapabilities) -> GraphqlOperationFilter {
    Arc::new(move |request| {
        admit_operation(
            &request.query,
            request.operation_name.as_deref(),
            capabilities,
        )
        .map(|_| ())
        .map_err(|error| {
            let mut result = async_graphql::ServerError::new(error.to_string(), None);
            let mut extensions = async_graphql::ErrorExtensionValues::default();
            extensions.set(
                "code",
                if error == OperationError::NotMounted {
                    "OPERATION_NOT_MOUNTED"
                } else {
                    "BAD_REQUEST"
                },
            );
            result.extensions = Some(extensions);
            result
        })
    })
}
fn error_response(error: OperationError) -> Response {
    axum::Json(error.envelope()).into_response()
}

impl GraphqlBinding {
    pub(super) fn validate(
        &self,
        executor: &GraphqlExecutor,
        capabilities: GraphqlCapabilities,
        delivery: DeliveryCapabilities,
        extensions: &[String],
        public: &Url,
    ) -> Result<(), GatewayError> {
        // Later delivery mounts provide these implementations. Reject enabling
        // a mount until it is bound; a configuration flag cannot pretend to cache.
        if delivery != DeliveryCapabilities::default() {
            return Err(GatewayError("delivery adapter is not bound"));
        }
        match (self, executor) {
            (Self::Embedded(embedded), GraphqlExecutor::Embedded) => {
                if embedded.capabilities != capabilities
                    || embedded.extensions != extensions.iter().cloned().collect()
                {
                    return Err(GatewayError(
                        "embedded schema registration does not match declaration",
                    ));
                }
            }
            (Self::Remote(remote), GraphqlExecutor::Remote { origin }) => {
                if Url::parse(origin)
                    .map_err(|_| GatewayError("invalid GraphQL origin"))?
                    .origin()
                    == public.origin()
                {
                    return Err(GatewayError("GraphQL upstream points to gateway"));
                }
                for path in std::iter::once(&remote.http_path).chain(remote.live_path.as_ref()) {
                    if super::super::route::normalize_path(path)? != *path {
                        return Err(GatewayError("invalid remote GraphQL path"));
                    }
                }
                if capabilities.live && remote.live_path.is_none() {
                    return Err(GatewayError("live gateway needs a remote live endpoint"));
                }
            }
            _ => return Err(GatewayError("GraphQL executor location mismatch")),
        }
        Ok(())
    }

    pub(super) async fn execute(
        &self,
        inner: &Arc<NativeInner>,
        declaration: &BindingKind,
        context: RequestContext,
        mut request: Request<Body>,
    ) -> Response {
        let BindingKind::Graphql {
            capabilities,
            executor,
            ..
        } = declaration
        else {
            return response(StatusCode::SERVICE_UNAVAILABLE);
        };
        let upgrade = request.headers().contains_key(header::UPGRADE);
        if upgrade {
            let shared = request
                .extensions()
                .get::<Arc<super::NativeDelivery>>()
                .cloned()
                .filter(|delivery| delivery.live.is_some());
            if !capabilities.live {
                return response(StatusCode::NOT_FOUND);
            }
            return match (self, executor) {
                (Self::Embedded(embedded), _) => {
                    if let Some(coordinator) = shared.filter(|_| embedded.engine.is_some()) {
                        let execution = super::live_transport::Execution {
                            binding: self.clone(),
                            inner: inner.clone(),
                            declaration: declaration.clone(),
                            context,
                        };
                        return super::live_transport::upgrade_embedded(
                            execution,
                            request,
                            embedded.clone(),
                            coordinator,
                        )
                        .await;
                    }
                    let permit = match inner.permits.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => return response(StatusCode::SERVICE_UNAVAILABLE),
                    };
                    let lifetime = context
                        .connection_lifetime(inner.options.limits.upgrade_lifetime, super::now());
                    request.extensions_mut().insert(Arc::new(
                        crate::graphql::http::GraphqlConnectionGuard {
                            lifetime,
                            _permit: permit,
                        },
                    ));
                    if proxy::prepare_headers(
                        request.headers_mut(),
                        inner,
                        &context,
                        Forwarding::Graphql,
                        true,
                    )
                    .is_err()
                    {
                        return response(StatusCode::BAD_REQUEST);
                    }
                    rewrite_path(&mut request, "/graphql/ws");
                    request.extensions_mut().insert(context);
                    match embedded.router.clone().oneshot(request).await {
                        Ok(response) => response,
                        Err(never) => match never {},
                    }
                }
                (Self::Remote(remote), GraphqlExecutor::Remote { origin }) => {
                    remote_websocket(
                        inner,
                        origin,
                        remote.live_path.as_deref().expect("validated live path"),
                        *capabilities,
                        context.clone(),
                        request,
                        shared.map(|coordinator| {
                            (
                                super::live_transport::Execution {
                                    binding: self.clone(),
                                    inner: inner.clone(),
                                    declaration: declaration.clone(),
                                    context,
                                },
                                coordinator,
                            )
                        }),
                    )
                    .await
                }
                _ => response(StatusCode::SERVICE_UNAVAILABLE),
            };
        }
        self.execute_operation(inner, declaration, context, request)
            .await
    }

    /// Execute a client's HTTP operation. Whatever the client wrote in the
    /// gateway's own control extension is removed here, at ingress.
    async fn execute_operation(
        &self,
        inner: &Arc<NativeInner>,
        declaration: &BindingKind,
        context: RequestContext,
        request: Request<Body>,
    ) -> Response {
        if request.method() != "POST" {
            let mut result = response(StatusCode::METHOD_NOT_ALLOWED);
            result
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("POST"));
            return result;
        }
        let permit = match inner.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return response(StatusCode::SERVICE_UNAVAILABLE),
        };
        let (parts, body) = request.into_parts();
        let body = match tokio::time::timeout(
            inner.options.limits.response_header_timeout,
            axum::body::to_bytes(body, inner.options.limits.request_body_bytes),
        )
        .await
        {
            Ok(Ok(body)) => body,
            Ok(Err(_)) => return response(StatusCode::PAYLOAD_TOO_LARGE),
            Err(_) => return response(StatusCode::REQUEST_TIMEOUT),
        };
        let mut value = match serde_json::from_slice(&body) {
            Ok(value) => value,
            Err(_) => return error_response(OperationError::InvalidRequest),
        };
        remove_client_control(&mut value);
        self.execute_value(inner, declaration, context, parts, value, permit)
            .await
    }

    /// Execute one operation the gateway holds as a value: a client's, after
    /// ingress, or one the gateway built for a WebSocket consumer.
    pub(super) async fn execute_value(
        &self,
        inner: &Arc<NativeInner>,
        declaration: &BindingKind,
        context: RequestContext,
        parts: axum::http::request::Parts,
        value: serde_json::Value,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Response {
        let BindingKind::Graphql {
            capabilities,
            executor,
            ..
        } = declaration
        else {
            return response(StatusCode::SERVICE_UNAVAILABLE);
        };
        if let Err(error) = admit_request(&value, *capabilities) {
            return error_response(error);
        }
        if let Some(coordinator) = parts
            .extensions
            .get::<Arc<super::NativeDelivery>>()
            .filter(|delivery| {
                let caps = delivery.capabilities();
                caps.snapshots || caps.coalescing
            })
            .cloned()
        {
            if super::super::graphql::operation_kind(
                value["query"].as_str().unwrap_or(""),
                value["operationName"].as_str(),
            ) == Ok(super::super::graphql::OperationKind::Query)
            {
                return coordinator
                    .execute(self, inner, executor, context, parts, value, permit)
                    .await;
            }
        }
        self.execute_http(
            inner,
            executor,
            context,
            json_request(&parts, &value),
            Some(permit),
        )
        .await
    }

    pub(super) async fn execute_http(
        &self,
        inner: &Arc<NativeInner>,
        executor: &GraphqlExecutor,
        context: RequestContext,
        mut request: Request<Body>,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Response {
        // Internal control/result calls share the outer admitted capacity slot.
        // Plain remote streaming responses retain their own permit in the body.
        match (self, executor) {
            (Self::Embedded(embedded), _) => {
                if proxy::prepare_headers(
                    request.headers_mut(),
                    inner,
                    &context,
                    Forwarding::Graphql,
                    false,
                )
                .is_err()
                {
                    return response(StatusCode::BAD_REQUEST);
                }
                rewrite_path(&mut request, "/graphql");
                request.extensions_mut().insert(context);
                match embedded.router.clone().oneshot(request).await {
                    Ok(response) => response,
                    Err(never) => match never {},
                }
            }
            (Self::Remote(remote), GraphqlExecutor::Remote { origin }) => {
                rewrite_path(&mut request, &remote.http_path);
                proxy::forward_with_permit(
                    inner,
                    origin,
                    false,
                    Forwarding::Graphql,
                    context,
                    request,
                    permit,
                )
                .await
            }
            _ => response(StatusCode::SERVICE_UNAVAILABLE),
        }
    }
}

/// One JSON operation addressed like the request it came from.
pub(super) fn json_request(
    parts: &axum::http::request::Parts,
    value: &serde_json::Value,
) -> Request<Body> {
    let mut request = Request::new(Body::from(value.to_string()));
    *request.method_mut() = parts.method.clone();
    *request.uri_mut() = parts.uri.clone();
    *request.version_mut() = parts.version;
    *request.headers_mut() = parts.headers.clone();
    *request.extensions_mut() = parts.extensions.clone();
    request.headers_mut().remove(header::CONTENT_LENGTH);
    request.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    request
}

/// Address the executor's own path. The client's query string is dropped: a
/// GraphQL operation carries nothing there, so nothing a client writes in it
/// may reach the executor.
fn rewrite_path(request: &mut Request<Body>, path: &str) {
    *request.uri_mut() = path.parse().expect("validated path");
}

async fn remote_websocket(
    inner: &Arc<NativeInner>,
    origin: &str,
    path: &str,
    capabilities: GraphqlCapabilities,
    context: RequestContext,
    request: Request<Body>,
    sharing: Option<(super::live_transport::Execution, Arc<super::NativeDelivery>)>,
) -> Response {
    let permit = match inner.permits.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return response(StatusCode::SERVICE_UNAVAILABLE),
    };
    let (mut parts, _) = request.into_parts();
    let offered = parts
        .headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let Some(protocol) = WsProtocol::negotiate(offered) else {
        return response(StatusCode::BAD_REQUEST);
    };
    let upgrade = match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
        Ok(upgrade) => upgrade,
        Err(rejection) => return rejection.into_response(),
    };
    if proxy::prepare_headers(
        &mut parts.headers,
        inner,
        &context,
        Forwarding::Graphql,
        true,
    )
    .is_err()
    {
        return response(StatusCode::BAD_REQUEST);
    }
    let sharing = sharing.map(|(execution, coordinator)| {
        (
            execution,
            coordinator,
            json_request(&parts, &serde_json::json!({})).into_parts().0,
        )
    });
    parts.headers.insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(protocol.name()),
    );
    let Some(key) = parts.headers.get(header::SEC_WEBSOCKET_KEY) else {
        return response(StatusCode::BAD_REQUEST);
    };
    let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
    if proxy::add_hop(&mut parts.headers, inner).is_err() {
        return response(StatusCode::BAD_REQUEST);
    }
    let pending = inner
        .client
        .get(format!("{}{path}", origin.trim_end_matches('/')))
        .headers(parts.headers)
        .send();
    let upstream =
        match tokio::time::timeout(inner.options.limits.response_header_timeout, pending).await {
            Ok(Ok(upstream)) => upstream,
            Err(_) => return response(StatusCode::GATEWAY_TIMEOUT),
            _ => return response(StatusCode::BAD_GATEWAY),
        };
    if upstream.status() != StatusCode::SWITCHING_PROTOCOLS {
        let status = upstream.status();
        let mut headers = upstream.headers().clone();
        proxy::strip_hop_headers(&mut headers);
        // The client already enforces idle read timeout. Retain capacity until
        // the terminal upstream body finishes or the consumer disconnects.
        let stream = upstream.bytes_stream().map(move |chunk| {
            let _ = &permit;
            chunk
        });
        let mut result = Response::new(Body::from_stream(stream));
        *result.status_mut() = status;
        *result.headers_mut() = headers;
        return result;
    }
    if upstream
        .headers()
        .get(header::SEC_WEBSOCKET_ACCEPT)
        .and_then(|v| v.to_str().ok())
        != Some(accept.as_str())
        || upstream
            .headers()
            .get(header::SEC_WEBSOCKET_PROTOCOL)
            .and_then(|v| v.to_str().ok())
            != Some(protocol.name())
    {
        return response(StatusCode::BAD_GATEWAY);
    }
    let mut headers = upstream.headers().clone();
    for name in [
        header::SEC_WEBSOCKET_ACCEPT,
        header::SEC_WEBSOCKET_PROTOCOL,
        header::CONNECTION,
        header::UPGRADE,
        header::CONTENT_LENGTH,
    ] {
        headers.remove(name);
    }
    let lifetime = context.connection_lifetime(inner.options.limits.upgrade_lifetime, super::now());
    let max_message = inner.options.limits.request_body_bytes;
    let mut response = upgrade
        .protocols([protocol.name()])
        .max_message_size(max_message)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            let _ = tokio::time::timeout(lifetime, async move {
                let Ok(upstream) = upstream.upgrade().await else {
                    return;
                };
                let upstream = WebSocketStream::from_raw_socket(
                    upstream,
                    Role::Client,
                    Some(
                        tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
                            .max_message_size(Some(max_message)),
                    ),
                )
                .await;
                if let Some((execution, coordinator, parts)) = sharing {
                    super::live_transport::remote(
                        socket,
                        upstream,
                        execution,
                        parts,
                        coordinator,
                        protocol,
                    )
                    .await;
                    return;
                }
                bridge(socket, upstream, capabilities, protocol).await;
            })
            .await;
        })
        .into_response();
    for (name, value) in &headers {
        response.headers_mut().append(name, value.clone());
    }
    response
}

async fn bridge(
    mut client: WebSocket,
    mut origin: WebSocketStream<reqwest::Upgraded>,
    capabilities: GraphqlCapabilities,
    protocol: WsProtocol,
) {
    let mut admitted = false;
    let mut active = Operations::<()>::default();
    loop {
        tokio::select! {
            message = client.recv() => {
                let Some(Ok(message)) = message else { break };
                let upstream = match message {
                    Message::Text(text) => {
                        let frame = match protocol.decode_client_text(&text) {
                            Ok(ClientFrame::Subscribe { id, mut payload }) => {
                                if !admitted || active.admit(&id).is_err() { break; }
                                if let Err(error) = admit_request(&payload, capabilities) {
                                    if client.send(Message::Text(protocol.next(&id, error.envelope()).to_string().into())).await.is_err() { break; }
                                    if client.send(Message::Text(ws::complete(&id).to_string().into())).await.is_err() { break; }
                                    continue;
                                }
                                remove_client_control(&mut payload);
                                let _ = active.open(id.clone(), ());
                                ClientFrame::Subscribe { id, payload }
                            }
                            Ok(ClientFrame::Complete { id }) => {
                                active.close(&id);
                                ClientFrame::Complete { id }
                            }
                            Ok(frame) => frame,
                            // Never bypass operation admission with a frame it cannot read.
                            Err(_) => break,
                        };
                        UpstreamMessage::Text(protocol.encode_client(&frame).to_string().into())
                    }
                    Message::Binary(_) => break, // GraphQL WS uses JSON text; never bypass operation admission.
                    Message::Ping(bytes) => UpstreamMessage::Ping(bytes),
                    Message::Pong(bytes) => UpstreamMessage::Pong(bytes),
                    Message::Close(frame) => {
                        let frame = frame.map(|frame| tokio_tungstenite::tungstenite::protocol::CloseFrame { code: frame.code.into(), reason: frame.reason.to_string().into() });
                        let _ = origin.send(UpstreamMessage::Close(frame)).await;
                        break;
                    },
                };
                if origin.send(upstream).await.is_err() { break; }
            }
            message = origin.next() => {
                let Some(Ok(message)) = message else { break };
                let downstream = match message {
                    UpstreamMessage::Text(text) => {
                        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(id) = ws::ended_operation(&value) { active.close(id); }
                            if ws::decode_server(value) == ServerFrame::Ack { admitted = true; }
                        }
                        Message::Text(text.to_string().into())
                    }
                    UpstreamMessage::Binary(bytes) => Message::Binary(bytes),
                    UpstreamMessage::Ping(bytes) => Message::Ping(bytes),
                    UpstreamMessage::Pong(bytes) => Message::Pong(bytes),
                    UpstreamMessage::Close(frame) => {
                        let frame = frame.map(|frame| axum::extract::ws::CloseFrame { code: frame.code.into(), reason: frame.reason.to_string().into() });
                        let _ = client.send(Message::Close(frame)).await;
                        break;
                    },
                    UpstreamMessage::Frame(_) => continue,
                };
                if client.send(downstream).await.is_err() { break; }
            }
        }
    }
    let _ = client.send(Message::Close(None)).await;
    let _ = origin.close(None).await;
}
