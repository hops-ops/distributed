use super::{
    graphql::{EmbeddedGraphql, GraphqlBinding, RemoteGraphql},
    live::{LiveSource, LiveSourceFactory},
    proxy, NativeDelivery, NativeInner, RequestContext,
};
use crate::gateway::{
    delivery::{mark_connection, FreshnessContext, LiveEnd},
    graphql::ws::{self, ClientFrame, Operations, ServerFrame, WsProtocol},
    BindingKind, Forwarding, GraphqlCapabilities, GraphqlExecutor,
};
use axum::{
    body::Bytes,
    extract::ws::{Message, WebSocket},
    http::{header, request::Parts, HeaderMap, HeaderValue, Method},
};
use futures_util::{FutureExt, SinkExt, StreamExt};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{
    tungstenite::{protocol::Role, Message as Upstream},
    WebSocketStream,
};

type OriginSocket = WebSocketStream<reqwest::Upgraded>;
#[derive(Clone)]
pub(super) struct Execution {
    pub binding: GraphqlBinding,
    pub inner: Arc<NativeInner>,
    pub declaration: BindingKind,
    pub context: RequestContext,
}
impl Execution {
    fn executor(&self) -> &GraphqlExecutor {
        let BindingKind::Graphql { executor, .. } = &self.declaration else {
            unreachable!()
        };
        executor
    }
    fn capabilities(&self) -> GraphqlCapabilities {
        let BindingKind::Graphql { capabilities, .. } = &self.declaration else {
            unreachable!()
        };
        *capabilities
    }
    fn source(&self, headers: HeaderMap, init: serde_json::Value) -> LiveSourceFactory {
        let execution = self.clone();
        Arc::new(move |mut request: serde_json::Value| {
            if let Some(extensions) = request
                .get_mut("extensions")
                .and_then(serde_json::Value::as_object_mut)
            {
                extensions.remove("gatewayDelivery");
            }
            let execution = execution.clone();
            let headers = headers.clone();
            let init = init.clone();
            async move {
                match &execution.binding {
                    GraphqlBinding::Embedded(embedded) => {
                        let engine = embedded.engine.as_ref().ok_or_else(|| {
                            "custom embedded live source is not eligible".to_owned()
                        })?;
                        let (session, principal) =
                            crate::graphql::http::resolve_ws_identity(engine, &headers, &init)
                                .await
                                .map_err(|_| "unauthorized".to_owned())?
                                .into_parts();
                        let mut request: async_graphql::Request =
                            serde_json::from_value(request)
                                .map_err(|_| "invalid live request".to_owned())?;
                        if let Some(principal) = principal {
                            request = request.data(principal);
                        }
                        Ok(engine
                            .execute_stream(&session, request)
                            .map(|response| {
                                serde_json::to_value(response)
                                    .map_err(|_| "invalid origin envelope".into())
                            })
                            .boxed())
                    }
                    GraphqlBinding::Remote(remote) => {
                        let GraphqlExecutor::Remote { origin } = execution.executor() else {
                            return Err("invalid origin binding".into());
                        };
                        let mut socket = connect(
                            &execution.inner,
                            origin,
                            remote,
                            &execution.context,
                            headers,
                            &init,
                        )
                        .await?;
                        let subscribe =
                            WsProtocol::TransportWs.encode_client(&ClientFrame::Subscribe {
                                id: UPSTREAM.into(),
                                payload: request,
                            });
                        socket
                            .send(Upstream::Text(subscribe.to_string().into()))
                            .await
                            .map_err(|_| "upstream subscribe failed")?;
                        Ok(origin_stream(socket))
                    }
                }
            }
            .boxed()
        })
    }
}
async fn connect(
    inner: &Arc<NativeInner>,
    origin: &str,
    remote: &RemoteGraphql,
    context: &RequestContext,
    mut headers: HeaderMap,
    init: &serde_json::Value,
) -> Result<OriginSocket, String> {
    let path = remote.live_path.as_deref().ok_or("live endpoint missing")?;
    proxy::prepare_headers(&mut headers, inner, context, Forwarding::Graphql, true)
        .map_err(|_| "invalid live headers")?;
    headers.remove(header::CONTENT_LENGTH);
    headers.remove(header::CONTENT_TYPE);
    let key = tokio_tungstenite::tungstenite::handshake::client::generate_key();
    headers.insert(
        header::SEC_WEBSOCKET_KEY,
        HeaderValue::from_str(&key).map_err(|_| "invalid websocket key")?,
    );
    headers.insert(
        header::SEC_WEBSOCKET_VERSION,
        HeaderValue::from_static("13"),
    );
    headers.insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static("graphql-transport-ws"),
    );
    proxy::add_hop(&mut headers, inner).map_err(|_| "gateway loop")?;
    let url = format!("{}{path}", origin.trim_end_matches('/'));
    let response = tokio::time::timeout(
        inner.options.limits.response_header_timeout,
        inner.client.get(url).headers(headers).send(),
    )
    .await
    .map_err(|_| "upstream handshake timed out")?
    .map_err(|_| "upstream handshake failed")?;
    if response.status() == axum::http::StatusCode::UNAUTHORIZED {
        return Err(LiveEnd::AuthExpired.code().into());
    }
    if response.status() != axum::http::StatusCode::SWITCHING_PROTOCOLS
        || response
            .headers()
            .get(header::SEC_WEBSOCKET_ACCEPT)
            .and_then(|v| v.to_str().ok())
            != Some(
                tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes())
                    .as_str(),
            )
        || response
            .headers()
            .get(header::SEC_WEBSOCKET_PROTOCOL)
            .and_then(|v| v.to_str().ok())
            != Some("graphql-transport-ws")
    {
        return Err("invalid upstream handshake".into());
    }
    let socket = response
        .upgrade()
        .await
        .map_err(|_| "upstream upgrade failed")?;
    let mut socket = WebSocketStream::from_raw_socket(
        socket,
        Role::Client,
        Some(
            tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
                .max_message_size(Some(inner.options.limits.request_body_bytes)),
        ),
    )
    .await;
    let init = WsProtocol::TransportWs.encode_client(&ClientFrame::Init(init.clone()));
    socket
        .send(Upstream::Text(init.to_string().into()))
        .await
        .map_err(|_| "upstream init failed")?;
    tokio::time::timeout(inner.options.limits.response_header_timeout, async {
        loop {
            match socket.next().await {
                Some(Ok(Upstream::Text(text))) => {
                    let value = serde_json::from_str(&text).map_err(|_| "invalid upstream init")?;
                    match ws::decode_server(value) {
                        ServerFrame::Ack => return Ok(()),
                        ServerFrame::ConnectionError => return Err(LiveEnd::AuthExpired.code()),
                        _ => {}
                    }
                }
                Some(Ok(Upstream::Ping(data))) => {
                    socket
                        .send(Upstream::Pong(data))
                        .await
                        .map_err(|_| "upstream ping failed")?;
                }
                _ => return Err(LiveEnd::AuthExpired.code()),
            }
        }
    })
    .await
    .map_err(|_| "upstream admission timed out")??;
    Ok(socket)
}
fn origin_stream(socket: OriginSocket) -> LiveSource {
    futures_util::stream::unfold(socket, |mut socket| async move {
        loop {
            match socket.next().await {
                Some(Ok(Upstream::Text(text))) => {
                    let Ok(value) = serde_json::from_str(&text) else {
                        return Some((Err("invalid upstream frame".into()), socket));
                    };
                    match ws::decode_server(value) {
                        ServerFrame::Next { id, payload } if id == UPSTREAM => {
                            return Some((Ok(payload), socket))
                        }
                        ServerFrame::Complete { id } if id == UPSTREAM => return None,
                        ServerFrame::Error { .. } => {
                            return Some((Err("upstream operation failed".into()), socket))
                        }
                        ServerFrame::Ping(payload) => {
                            let pong = ws::pong(payload).to_string();
                            let _ = socket.send(Upstream::Text(pong.into())).await;
                        }
                        _ => {}
                    }
                }
                Some(Ok(Upstream::Ping(data))) => {
                    let _ = socket.send(Upstream::Pong(data)).await;
                }
                Some(Ok(Upstream::Pong(_))) => {}
                Some(Ok(Upstream::Close(_))) | None => {
                    return Some((
                        Err("upstream disconnected before operation completion".into()),
                        socket,
                    ));
                }
                _ => return Some((Err("upstream stream failed".into()), socket)),
            }
        }
    })
    .boxed()
}
/// Operation id of the gateway's own single upstream subscription.
const UPSTREAM: &str = "upstream";

pub(super) async fn remote(
    mut client: WebSocket,
    mut origin: OriginSocket,
    execution: Execution,
    parts: Parts,
    coordinator: Arc<NativeDelivery>,
    protocol: WsProtocol,
) {
    let init = match initial_message(
        &mut client,
        protocol,
        execution.inner.options.limits.response_header_timeout,
    )
    .await
    {
        Some(init) => init,
        None => return,
    };
    let frame = protocol.encode_client(&ClientFrame::Init(init.clone()));
    if origin
        .send(Upstream::Text(frame.to_string().into()))
        .await
        .is_err()
    {
        return;
    }
    let ack = tokio::time::timeout(
        execution.inner.options.limits.response_header_timeout,
        async {
            loop {
                match origin.next().await {
                    Some(Ok(Upstream::Text(text))) => {
                        match ws::decode_server(serde_json::from_str(&text).ok()?) {
                            ServerFrame::Ack => return Some(text.to_string()),
                            ServerFrame::ConnectionError => {
                                let _ = client.send(Message::Text(text.to_string().into())).await;
                                return None;
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(Upstream::Ping(data))) => {
                        let _ = origin.send(Upstream::Pong(data)).await;
                    }
                    Some(Ok(Upstream::Close(frame))) => {
                        let _ = client
                            .send(Message::Close(frame.map(|frame| {
                                axum::extract::ws::CloseFrame {
                                    code: frame.code.into(),
                                    reason: frame.reason.to_string().into(),
                                }
                            })))
                            .await;
                        return None;
                    }
                    _ => return None,
                }
            }
        },
    )
    .await
    .ok()
    .flatten();
    let Some(ack) = ack else {
        return;
    };
    if client.send(Message::Text(ack.into())).await.is_err() {
        return;
    }
    // The temporary per-consumer origin connection authenticated connection_init.
    // It owns no subscription and is closed before shared operation admission.
    let _ = origin.close(None).await;
    drop(origin);
    client_loop(client, execution, parts, coordinator, protocol, init).await;
}
pub(super) async fn embedded(
    mut client: WebSocket,
    embedded: EmbeddedGraphql,
    execution: Execution,
    parts: Parts,
    coordinator: Arc<NativeDelivery>,
    protocol: WsProtocol,
) {
    let init = match initial_message(
        &mut client,
        protocol,
        execution.inner.options.limits.response_header_timeout,
    )
    .await
    {
        Some(init) => init,
        None => return,
    };
    let Some(engine) = &embedded.engine else {
        return;
    };
    if crate::graphql::http::resolve_ws_identity(engine, &parts.headers, &init)
        .await
        .is_err()
    {
        let _ = client
            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                code: 4401,
                reason: "unauthorized".into(),
            })))
            .await;
        return;
    }
    if client
        .send(Message::Text(ws::connection_ack().to_string().into()))
        .await
        .is_err()
    {
        return;
    }
    client_loop(client, execution, parts, coordinator, protocol, init).await;
}
async fn initial_message(
    client: &mut WebSocket,
    protocol: WsProtocol,
    timeout: Duration,
) -> Option<serde_json::Value> {
    let message = tokio::time::timeout(timeout, client.recv())
        .await
        .ok()??
        .ok()?;
    let Message::Text(text) = message else {
        return None;
    };
    if text.len() > ws::MAX_INIT_BYTES {
        return None;
    }
    match protocol.decode_client_text(&text) {
        Ok(ClientFrame::Init(payload)) => Some(payload),
        _ => None,
    }
}
struct Operation(tokio::task::JoinHandle<()>);
impl Drop for Operation {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn client_loop(
    client: WebSocket,
    execution: Execution,
    mut parts: Parts,
    coordinator: Arc<NativeDelivery>,
    protocol: WsProtocol,
    init: serde_json::Value,
) {
    let source = execution.source(parts.headers.clone(), init.clone());
    parts.method = Method::POST;
    proxy::strip_hop_headers(&mut parts.headers);
    let parts = Arc::new(parts);
    let write_timeout = execution.inner.options.limits.read_timeout;
    let (mut sink, mut stream) = client.split();
    let (out, mut output) = mpsc::channel::<serde_json::Value>(32);
    let (close, mut closed) = watch::channel(false);
    let mut operations = Operations::<Operation>::default();
    let mut close_frame = Some(axum::extract::ws::CloseFrame {
        code: 1013,
        reason: LiveEnd::ResetRequired.code().into(),
    });
    loop {
        tokio::select! {biased;
            _ = closed.changed() => break,
            next = output.recv() => {
                let Some(value) = next else { break };
                let terminal = ws::ended_operation(&value).map(str::to_owned);
                let sending = sink.send(Message::Text(value.to_string().into()));
                tokio::select! {
                    _ = closed.changed() => break,
                    sent = tokio::time::timeout(write_timeout, sending) => if !matches!(sent, Ok(Ok(()))) { break },
                }
                if let Some(id) = terminal {
                    operations.close(&id);
                }
            }
            next = stream.next() => {
                let Some(Ok(message)) = next else { break };
                let reply = match message {
                    Message::Text(text) => match protocol.decode_client_text(&text) {
                        Ok(ClientFrame::Subscribe { id, payload }) => {
                            if operations.admit(&id).is_err() {
                                break;
                            }
                            if let Err(error) = crate::gateway::graphql::admit_request(&payload, execution.capabilities()) {
                                let _ = out.try_send(protocol.next(&id, error.envelope()));
                                let _ = out.try_send(ws::complete(&id));
                                continue;
                            }
                            let task = tokio::spawn(operation(
                                id.clone(),
                                payload,
                                execution.clone(),
                                parts.clone(),
                                coordinator.clone(),
                                source.clone(),
                                Output { out: out.clone(), close: close.clone(), protocol },
                                init.clone(),
                            ));
                            let _ = operations.open(id, Operation(task));
                            continue;
                        }
                        Ok(ClientFrame::Complete { id }) => {
                            operations.close(&id);
                            continue;
                        }
                        Ok(ClientFrame::Ping(payload)) => Message::Text(ws::pong(payload).to_string().into()),
                        Ok(ClientFrame::Pong(_)) => continue,
                        Ok(ClientFrame::Terminate) => {
                            close_frame = Some(axum::extract::ws::CloseFrame { code: 1000, reason: "".into() });
                            break;
                        }
                        Ok(ClientFrame::Init(_)) | Err(_) => break,
                    },
                    Message::Ping(data) => Message::Pong(data),
                    Message::Pong(_) => continue,
                    Message::Close(frame) => {
                        close_frame = frame;
                        break;
                    }
                    Message::Binary(_) => break,
                };
                if !matches!(tokio::time::timeout(write_timeout, sink.send(reply)).await, Ok(Ok(()))) {
                    break;
                }
            }
        }
    }
    drop(operations);
    let _ = tokio::time::timeout(
        Duration::from_millis(100),
        sink.send(Message::Close(close_frame)),
    )
    .await;
}

/// Where one operation writes its frames.
#[derive(Clone)]
struct Output {
    out: mpsc::Sender<serde_json::Value>,
    close: watch::Sender<bool>,
    protocol: WsProtocol,
}
impl Output {
    async fn next(&self, id: &str, payload: serde_json::Value) -> bool {
        self.out.send(self.protocol.next(id, payload)).await.is_ok()
    }
    async fn complete(&self, id: &str) {
        let _ = self.out.send(ws::complete(id)).await;
    }
    async fn fail(&self, id: &str, end: LiveEnd) {
        let _ = self.out.send(ws::error(id, end.code())).await;
    }
    /// Answer once, then complete.
    async fn answer(&self, id: &str, payload: serde_json::Value) {
        if self.next(id, payload).await {
            self.complete(id).await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn operation(
    id: String,
    mut payload: serde_json::Value,
    execution: Execution,
    parts: Arc<Parts>,
    coordinator: Arc<NativeDelivery>,
    source: LiveSourceFactory,
    output: Output,
    init: serde_json::Value,
) {
    mark_connection(&mut payload, init);
    let kind = crate::gateway::graphql::operation_kind(
        payload["query"].as_str().unwrap_or(""),
        payload["operationName"].as_str(),
    );
    if kind != Ok(crate::gateway::graphql::OperationKind::Subscription) {
        let result = match execution.inner.permits.clone().try_acquire_owned() {
            Ok(permit) => {
                execution
                    .binding
                    .execute_value(
                        &execution.inner,
                        &execution.declaration,
                        execution.context,
                        Parts::clone(&parts),
                        payload,
                        permit,
                    )
                    .await
            }
            Err(_) => super::response(axum::http::StatusCode::SERVICE_UNAVAILABLE),
        };
        let value = axum::body::to_bytes(
            result.into_body(),
            execution.inner.options.limits.request_body_bytes,
        )
        .await
        .ok()
        .and_then(|body| serde_json::from_slice(&body).ok())
        .unwrap_or_else(origin_unavailable);
        output.answer(&id, value).await;
        return;
    }
    let freshness = match payload["extensions"].get("gatewayFreshness") {
        Some(value) => match FreshnessContext::parse(value) {
            Ok(value) => Some(value),
            Err(_) => {
                let _ = output
                    .out
                    .send(ws::error(&id, "FRESHNESS_SCOPE_CHANGED"))
                    .await;
                return;
            }
        },
        None => None,
    };
    let admission = super::delivery::validate(
        &execution.binding,
        &execution.inner,
        execution.executor(),
        &execution.context,
        &parts,
        &payload,
    )
    .await;
    match admission {
        super::delivery::AdmissionResult::Eligible(admission) => {
            let Some(live) = &coordinator.live else {
                return;
            };
            let mut lease = match live.join(admission, payload.clone(), freshness, source.clone()) {
                Ok(lease) => lease,
                // Sharing is an optimization. A full registry, or a group this
                // consumer cannot join, means this consumer runs alone.
                Err(_) => return independent(&id, payload, source, &output).await,
            };
            loop {
                match lease.next().await {
                    Ok(Some(frame)) => {
                        let message = output.protocol.next(&id, frame.payload().clone());
                        tokio::select! {
                            sent = output.out.send(message) => if sent.is_err() { break },
                            end = lease.interrupted() => {
                                let _ = output.out.try_send(ws::error(&id, end.code()));
                                let _ = output.close.send(true);
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        output.complete(&id).await;
                        break;
                    }
                    Err(end) => {
                        if output.out.try_send(ws::error(&id, end.code())).is_err() {
                            let _ = output.close.send(true);
                        }
                        break;
                    }
                }
            }
        }
        super::delivery::AdmissionResult::Bypass => {
            independent(&id, payload, source, &output).await
        }
        super::delivery::AdmissionResult::Error(response) => {
            let bytes = axum::body::to_bytes(
                response.into_body(),
                crate::gateway::delivery::MAX_VALIDATION_BYTES,
            )
            .await
            .unwrap_or_else(|_| Bytes::new());
            output
                .answer(
                    &id,
                    serde_json::from_slice(&bytes).unwrap_or_else(|_| origin_unavailable()),
                )
                .await;
        }
    }
}
fn origin_unavailable() -> serde_json::Value {
    serde_json::json!({"errors":[{"message":"origin unavailable"}]})
}

/// One consumer's own upstream execution, shared with nobody.
async fn independent(
    id: &str,
    payload: serde_json::Value,
    source: LiveSourceFactory,
    output: &Output,
) {
    let Ok(mut stream) = source(payload).await else {
        return output.fail(id, LiveEnd::ResetRequired).await;
    };
    while let Some(result) = stream.next().await {
        let Ok(payload) = result else {
            return output.fail(id, LiveEnd::ResetRequired).await;
        };
        if !output.next(id, payload).await {
            return;
        }
    }
    output.complete(id).await;
}

pub(super) async fn upgrade_embedded(
    execution: Execution,
    request: axum::http::Request<axum::body::Body>,
    embedded: EmbeddedGraphql,
    coordinator: Arc<NativeDelivery>,
) -> axum::response::Response {
    use axum::{
        extract::{ws::WebSocketUpgrade, FromRequestParts},
        response::IntoResponse,
    };
    let permit = match execution.inner.permits.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return super::response(axum::http::StatusCode::SERVICE_UNAVAILABLE),
    };
    let (mut parts, _) = request.into_parts();
    let offered = parts
        .headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let Some(protocol) = WsProtocol::negotiate(offered) else {
        return super::response(axum::http::StatusCode::BAD_REQUEST);
    };
    let upgrade = match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
        Ok(upgrade) => upgrade,
        Err(error) => return error.into_response(),
    };
    if proxy::prepare_headers(
        &mut parts.headers,
        &execution.inner,
        &execution.context,
        Forwarding::Graphql,
        false,
    )
    .is_err()
    {
        return super::response(axum::http::StatusCode::BAD_REQUEST);
    }
    let lifetime = execution.context.connection_lifetime(
        execution.inner.options.limits.upgrade_lifetime,
        super::now(),
    );
    upgrade
        .protocols([protocol.name()])
        .max_message_size(execution.inner.options.limits.request_body_bytes)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            let _ = tokio::time::timeout(
                lifetime,
                self::embedded(socket, embedded, execution, parts, coordinator, protocol),
            )
            .await;
        })
        .into_response()
}
