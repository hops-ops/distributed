use super::{
    graphql::{EmbeddedGraphql, GraphqlBinding, RemoteGraphql},
    live::{LiveSource, LiveSourceFactory},
    proxy, NativeDelivery, NativeInner, RequestContext,
};
use crate::gateway::{BindingKind, GraphqlCapabilities, GraphqlExecutor};
use axum::{
    body::Bytes,
    extract::ws::{Message, WebSocket},
    http::{header, request::Parts, HeaderMap, HeaderValue, Method},
};
use futures_util::{FutureExt, SinkExt, StreamExt};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
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
    fn source(
        &self,
        headers: HeaderMap,
        query: Option<String>,
        init: serde_json::Value,
    ) -> LiveSourceFactory {
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
            let query = query.clone();
            async move {
            match &execution.binding {
                GraphqlBinding::Embedded(embedded)=>{
                    let engine=embedded.engine.as_ref().ok_or_else(||"custom embedded live source is not eligible".to_owned())?;
                    let (session,principal)=crate::graphql::http::resolve_gateway_ws_identity(engine,&headers,&init).await?.into_parts();
                    let mut request:async_graphql::Request=serde_json::from_value(request).map_err(|_|"invalid live request".to_owned())?;
                    if let Some(principal)=principal {request=request.data(principal);}
                    Ok(engine.execute_stream(&session,request).map(|response|serde_json::to_value(response).map_err(|_|"invalid origin envelope".into())).boxed())
                }
                GraphqlBinding::Remote(remote)=>{
                    let GraphqlExecutor::Remote{origin}=execution.executor() else {return Err("invalid origin binding".into())};
                    let mut socket=connect(&execution.inner,origin,remote,&execution.context,headers,query.as_deref(),&init).await?;
                    socket.send(Upstream::Text(serde_json::json!({"id":"upstream","type":"subscribe","payload":request}).to_string().into())).await.map_err(|_|"upstream subscribe failed")?;
                    Ok(origin_stream(socket))
                }
            }
        }.boxed()
        })
    }
}
async fn connect(
    inner: &Arc<NativeInner>,
    origin: &str,
    remote: &RemoteGraphql,
    context: &RequestContext,
    mut headers: HeaderMap,
    query: Option<&str>,
    init: &serde_json::Value,
) -> Result<OriginSocket, String> {
    let path = remote.live_path.as_deref().ok_or("live endpoint missing")?;
    proxy::prepare_headers(&mut headers, inner, context, true)
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
    let url = format!(
        "{}{path}{}",
        origin.trim_end_matches('/'),
        query.map(|q| format!("?{q}")).unwrap_or_default()
    );
    let response = tokio::time::timeout(
        inner.options.limits.response_header_timeout,
        inner.client.get(url).headers(headers).send(),
    )
    .await
    .map_err(|_| "upstream handshake timed out")?
    .map_err(|_| "upstream handshake failed")?;
    if response.status() == axum::http::StatusCode::UNAUTHORIZED {
        return Err("AUTH_EXPIRED".into());
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
    socket
        .send(Upstream::Text(
            serde_json::json!({"type":"connection_init","payload":init})
                .to_string()
                .into(),
        ))
        .await
        .map_err(|_| "upstream init failed")?;
    tokio::time::timeout(inner.options.limits.response_header_timeout, async {
        loop {
            match socket.next().await {
                Some(Ok(Upstream::Text(text))) => {
                    let value: serde_json::Value =
                        serde_json::from_str(&text).map_err(|_| "invalid upstream init")?;
                    if value["type"] == "connection_ack" {
                        return Ok(());
                    }
                    if value["type"] == "connection_error" {
                        return Err("AUTH_EXPIRED");
                    }
                }
                Some(Ok(Upstream::Ping(data))) => {
                    socket
                        .send(Upstream::Pong(data))
                        .await
                        .map_err(|_| "upstream ping failed")?;
                }
                _ => return Err("AUTH_EXPIRED"),
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
                    let value = match serde_json::from_str::<serde_json::Value>(&text) {
                        Ok(value) => value,
                        Err(_) => return Some((Err("invalid upstream frame".into()), socket)),
                    };
                    match value["type"].as_str() {
                        Some("next") if value["id"] == "upstream" => {
                            return Some((Ok(value["payload"].clone()), socket))
                        }
                        Some("complete") if value["id"] == "upstream" => return None,
                        Some("error") => {
                            return Some((Err("upstream operation failed".into()), socket))
                        }
                        Some("ping") => {
                            let _ = socket
                                .send(Upstream::Text(
                                    serde_json::json!({"type":"pong","payload":value["payload"]})
                                        .to_string()
                                        .into(),
                                ))
                                .await;
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

pub(super) async fn remote(
    mut client: WebSocket,
    mut origin: OriginSocket,
    execution: Execution,
    parts: Parts,
    coordinator: Arc<NativeDelivery>,
    protocol: &'static str,
) {
    let init = match initial_message(
        &mut client,
        execution.inner.options.limits.response_header_timeout,
    )
    .await
    {
        Some(init) => init,
        None => return,
    };
    if origin
        .send(Upstream::Text(
            serde_json::json!({"type":"connection_init","payload":init})
                .to_string()
                .into(),
        ))
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
                        let value: serde_json::Value = serde_json::from_str(&text).ok()?;
                        if value["type"] == "connection_ack" {
                            return Some(text.to_string());
                        }
                        if value["type"] == "connection_error" {
                            let _ = client.send(Message::Text(text.to_string().into())).await;
                            return None;
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
    protocol: &'static str,
) {
    let init = match initial_message(
        &mut client,
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
    if crate::graphql::http::resolve_gateway_ws_identity(engine, &parts.headers, &init)
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
        .send(Message::Text(
            serde_json::json!({"type":"connection_ack"})
                .to_string()
                .into(),
        ))
        .await
        .is_err()
    {
        return;
    }
    client_loop(client, execution, parts, coordinator, protocol, init).await;
}
async fn initial_message(client: &mut WebSocket, timeout: Duration) -> Option<serde_json::Value> {
    let message = tokio::time::timeout(timeout, client.recv())
        .await
        .ok()??
        .ok()?;
    let Message::Text(text) = message else {
        return None;
    };
    if text.len() > 65536 {
        return None;
    }
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    (value["type"] == "connection_init").then(|| {
        value
            .get("payload")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}))
    })
}
struct Operation(tokio::task::JoinHandle<()>);
impl Drop for Operation {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn packet(id: &str, kind: &str, payload: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"id":id,"type":kind,"payload":payload})
}
fn failure(id: &str, code: &str) -> serde_json::Value {
    packet(
        id,
        "error",
        serde_json::json!([{"message":code,"extensions":{"code":code}}]),
    )
}
async fn client_loop(
    client: WebSocket,
    execution: Execution,
    mut parts: Parts,
    coordinator: Arc<NativeDelivery>,
    protocol: &'static str,
    init: serde_json::Value,
) {
    let source = execution.source(
        parts.headers.clone(),
        parts.uri.query().map(str::to_owned),
        init.clone(),
    );
    parts.method = Method::POST;
    proxy::strip_hop_headers(&mut parts.headers);
    let parts = Arc::new(parts);
    let (mut sink, mut stream) = client.split();
    let (out, mut output) = mpsc::channel::<serde_json::Value>(32);
    let (close, mut closed) = watch::channel(false);
    let mut operations = BTreeMap::<String, Operation>::new();
    let mut close_frame = Some(axum::extract::ws::CloseFrame {
        code: 1013,
        reason: "LIVE_RESET_REQUIRED".into(),
    });
    loop {
        tokio::select! {biased;
            _=closed.changed()=>break,
            next=output.recv()=>{
                let Some(value)=next else {break;};
                let id=value["id"].as_str().map(str::to_owned);
                let terminal=matches!(value["type"].as_str(),Some("complete"|"error"));
                let sending=sink.send(Message::Text(value.to_string().into()));
                tokio::select! { _=closed.changed()=>break, sent=tokio::time::timeout(execution.inner.options.limits.read_timeout,sending)=>{if !matches!(sent,Ok(Ok(()))){break;}} }
                if terminal {if let Some(id)=id {operations.remove(&id);}}
            }
            next=stream.next()=>{
                let Some(Ok(message))=next else {break;};
                match message {
                    Message::Text(text)=>{
                        let Ok(value)=serde_json::from_str::<serde_json::Value>(&text) else {break;};
                        match value["type"].as_str() {
                            Some(kind @ ("subscribe"|"start")) if kind == if protocol == "graphql-ws" {"start"} else {"subscribe"}=>{
                                let Some(id)=value["id"].as_str() else {break;};
                                if id.is_empty()||id.len()>256||operations.len()>=128||operations.contains_key(id){break;}
                                if let Err(error)=crate::gateway::graphql::admit_request(&value["payload"],execution.capabilities()) {let _=out.try_send(packet(id,if protocol=="graphql-ws"{"data"}else{"next"},error.envelope()));let _=out.try_send(serde_json::json!({"id":id,"type":"complete"}));continue;}
                                let id=id.to_owned();let payload=value["payload"].clone();let execution=execution.clone();let parts=parts.clone();let coordinator=coordinator.clone();let source=source.clone();let out=out.clone();let close=close.clone();let init=init.clone();let task_id=id.clone();
                                operations.insert(id,Operation(tokio::spawn(async move {operation(task_id,payload,execution,parts,coordinator,source,out,close,protocol,init).await;})));
                            }
                            Some(kind @ ("complete"|"stop")) if kind == if protocol == "graphql-ws" {"stop"} else {"complete"}=>{if let Some(id)=value["id"].as_str(){operations.remove(id);}}
                            Some("ping")=>{if !matches!(tokio::time::timeout(execution.inner.options.limits.read_timeout,sink.send(Message::Text(serde_json::json!({"type":"pong","payload":value["payload"]}).to_string().into()))).await,Ok(Ok(()))){break;}}
                            Some("pong")=>{},
                            Some("connection_terminate") if protocol == "graphql-ws"=>{close_frame=Some(axum::extract::ws::CloseFrame{code:1000,reason:"".into()});break;},
                            _=>break,
                        }
                    }
                    Message::Ping(data)=>{if !matches!(tokio::time::timeout(execution.inner.options.limits.read_timeout,sink.send(Message::Pong(data))).await,Ok(Ok(()))){break;}},
                    Message::Pong(_)=>{},Message::Close(frame)=>{close_frame=frame;break;},_=>break,
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
#[allow(clippy::too_many_arguments)]
async fn operation(
    id: String,
    mut payload: serde_json::Value,
    execution: Execution,
    parts: Arc<Parts>,
    coordinator: Arc<NativeDelivery>,
    source: LiveSourceFactory,
    out: mpsc::Sender<serde_json::Value>,
    close: watch::Sender<bool>,
    protocol: &str,
    init: serde_json::Value,
) {
    let next = if protocol == "graphql-ws" {
        "data"
    } else {
        "next"
    };
    if !payload["extensions"].is_object() {
        payload["extensions"] = serde_json::json!({});
    }
    payload["extensions"]["gatewayDelivery"] =
        serde_json::json!({"action":"execute","connectionInit":init});
    let kind = crate::gateway::graphql::operation_kind(
        payload["query"].as_str().unwrap_or(""),
        payload["operationName"].as_str(),
    );
    if kind != Ok(crate::gateway::graphql::OperationKind::Subscription) {
        let request = super::delivery::request(&parts, payload);
        let result = execution
            .binding
            .execute_operation(
                &execution.inner,
                &execution.declaration,
                execution.context,
                request,
            )
            .await;
        let value = match axum::body::to_bytes(
            result.into_body(),
            execution.inner.options.limits.request_body_bytes,
        )
        .await
        {
            Ok(body) => serde_json::from_slice(&body).unwrap_or_else(
                |_| serde_json::json!({"errors":[{"message":"origin unavailable"}]}),
            ),
            Err(_) => serde_json::json!({"errors":[{"message":"origin unavailable"}]}),
        };
        if out.send(packet(&id, next, value)).await.is_ok() {
            let _ = out
                .send(serde_json::json!({"id":id,"type":"complete"}))
                .await;
        }
        return;
    }
    let freshness = match payload["extensions"].get("gatewayFreshness") {
        Some(value) => match crate::gateway::delivery::FreshnessContext::parse(value) {
            Ok(value) => Some(value),
            Err(_) => {
                let _ = out.send(failure(&id, "FRESHNESS_SCOPE_CHANGED")).await;
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
            let mut lease = match live.join(admission, payload, freshness, source) {
                Ok(lease) => lease,
                Err(_) => {
                    let _ = out.send(failure(&id, "LIVE_RESET_REQUIRED")).await;
                    return;
                }
            };
            loop {
                match lease.next().await {
                    Ok(Some(frame)) => {
                        let message = packet(&id, next, frame.payload().clone());
                        tokio::select! {
                            sent=out.send(message)=>{if sent.is_err(){break;}},
                            reason=lease.interrupted()=>{let _=out.try_send(failure(&id,reason));let _=close.send(true);break;}
                        }
                    }
                    Ok(None) => {
                        let _ = out
                            .send(serde_json::json!({"id":id,"type":"complete"}))
                            .await;
                        break;
                    }
                    Err(reason) => {
                        if out.try_send(failure(&id, reason)).is_err() {
                            let _ = close.send(true);
                        }
                        break;
                    }
                }
            }
        }
        super::delivery::AdmissionResult::Bypass => match source(payload).await {
            Ok(mut stream) => {
                while let Some(result) = stream.next().await {
                    match result {
                        Ok(payload) => {
                            if out.send(packet(&id, next, payload)).await.is_err() {
                                return;
                            }
                        }
                        Err(_) => {
                            let _ = out.send(failure(&id, "LIVE_RESET_REQUIRED")).await;
                            return;
                        }
                    }
                }
                let _ = out
                    .send(serde_json::json!({"id":id,"type":"complete"}))
                    .await;
            }
            Err(_) => {
                let _ = out.send(failure(&id, "LIVE_RESET_REQUIRED")).await;
            }
        },
        super::delivery::AdmissionResult::Error(response) => {
            let bytes = axum::body::to_bytes(response.into_body(), 65536)
                .await
                .unwrap_or_else(|_| Bytes::new());
            let value = serde_json::from_slice(&bytes).unwrap_or_else(
                |_| serde_json::json!({"errors":[{"message":"origin unavailable"}]}),
            );
            let _ = out.send(packet(&id, next, value)).await;
            let _ = out
                .send(serde_json::json!({"id":id,"type":"complete"}))
                .await;
        }
    }
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
    let protocol = if offered
        .split(',')
        .any(|p| p.trim() == "graphql-transport-ws")
    {
        "graphql-transport-ws"
    } else if offered.split(',').any(|p| p.trim() == "graphql-ws") {
        "graphql-ws"
    } else {
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
        false,
    )
    .is_err()
    {
        return super::response(axum::http::StatusCode::BAD_REQUEST);
    }
    let lifetime = execution.context.identity().map_or(
        execution.inner.options.limits.upgrade_lifetime,
        |id| {
            execution
                .inner
                .options
                .limits
                .upgrade_lifetime
                .min(Duration::from_secs(
                    id.expires_at().saturating_sub(super::now()),
                ))
        },
    );
    upgrade
        .protocols([protocol])
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
