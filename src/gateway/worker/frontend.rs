use super::{
    coordinator::{OriginRequest, WorkerDeliveryBinding},
    live_transport,
    socket::Socket,
    WorkerGateway,
};
use crate::gateway::{
    delivery::{mark_connection, LiveEnd},
    graphql::{
        admit_request, operation_kind, remove_client_control,
        ws::{self, ClientFrame, Operations, WsProtocol},
        OperationKind,
    },
    GraphqlCapabilities,
};
use futures_channel::mpsc;
use futures_util::{
    future::{select, AbortHandle, Abortable, Either},
    SinkExt, StreamExt,
};
use std::time::Duration;
use worker::{Env, Request, RequestInit, RequestRedirect, Response, Result, WebSocketPair};

#[allow(clippy::too_many_arguments)]
pub(super) async fn upgrade(
    gateway: WorkerGateway,
    env: Env,
    request: Request,
    input: OriginRequest,
    live_path: String,
    capabilities: GraphqlCapabilities,
    delivery: Option<WorkerDeliveryBinding>,
    binding: String,
) -> Result<Response> {
    if request.method() != worker::Method::Get {
        return Response::error("invalid websocket method", 400);
    }
    let offered = request
        .headers()
        .get("sec-websocket-protocol")?
        .unwrap_or_default();
    let Some(protocol) = WsProtocol::negotiate(&offered) else {
        return Response::error("unsupported GraphQL protocol", 400);
    };
    let mut origin = match live_transport::handshake(&input, &live_path, protocol).await {
        Ok(socket) => socket,
        Err(_) => return Response::error("origin unavailable", 502),
    };
    let pair = WebSocketPair::new()?;
    let mut client = Socket::new(pair.server, input.options.limits.websocket_buffer_bytes, 32)?;
    let lifetime = input.context.connection_lifetime(
        Duration::from_millis(input.options.limits.websocket_lifetime_ms),
        super::now(),
    );
    worker::wasm_bindgen_futures::spawn_local(async move {
        let run = async {
            let first = super::timer::deadline(
                Duration::from_millis(input.options.limits.header_timeout_ms),
                client.next(),
            )
            .await;
            let Ok(Ok(first)) = first else {
                return;
            };
            let Ok(ClientFrame::Init(init)) = protocol.decode_client(first) else {
                return;
            };
            let ack = match live_transport::initialize(
                &mut origin,
                protocol,
                init.clone(),
                input.options.limits.header_timeout_ms,
            )
            .await
            {
                Ok(ack) => ack,
                Err(_) => {
                    let _ = client.ws.close(Some(4401), Some("unauthorized"));
                    return;
                }
            };
            if client.send(&ack).is_err() {
                return;
            }
            if protocol == WsProtocol::LegacyWs || delivery.is_none() {
                independent(
                    &mut client,
                    &mut origin,
                    capabilities,
                    input.options.limits.websocket_buffer_bytes,
                    protocol,
                )
                .await;
                return;
            }
            drop(origin);
            modern(
                client,
                gateway,
                env,
                input,
                live_path,
                capabilities,
                delivery,
                binding,
                init,
            )
            .await;
        };
        let _ = super::timer::deadline(lifetime, run).await;
    });
    let response = Response::from_websocket(pair.client)?;
    response
        .headers()
        .set("sec-websocket-protocol", protocol.name())?;
    Ok(response)
}
struct Operation {
    abort: AbortHandle,
    ack: mpsc::Sender<serde_json::Value>,
}
impl Drop for Operation {
    fn drop(&mut self) {
        self.abort.abort();
    }
}
#[allow(clippy::too_many_arguments)]
async fn modern(
    mut client: Socket,
    gateway: WorkerGateway,
    env: Env,
    input: OriginRequest,
    live_path: String,
    capabilities: GraphqlCapabilities,
    delivery: Option<WorkerDeliveryBinding>,
    binding: String,
    init: serde_json::Value,
) {
    let (out, mut outgoing) = mpsc::channel::<serde_json::Value>(32);
    let mut operations = Operations::<Operation>::default();
    let protocol = WsProtocol::TransportWs;
    loop {
        enum Event {
            Client(std::result::Result<serde_json::Value, String>),
            Output(Option<serde_json::Value>),
        }
        let event = match select(Box::pin(client.next()), Box::pin(outgoing.next())).await {
            Either::Left((value, _)) => Event::Client(value),
            Either::Right((value, _)) => Event::Output(value),
        };
        let value = match event {
            Event::Client(Ok(value)) => value,
            Event::Output(Some(value)) => {
                if client.send(&value).is_err() {
                    break;
                }
                if let Some(id) = ws::ended_operation(&value) {
                    operations.close(id);
                }
                continue;
            }
            _ => break,
        };
        match protocol.decode_client(value) {
            Ok(ClientFrame::Subscribe { id, payload }) => {
                if operations.admit(&id).is_err() {
                    let _ = client
                        .ws
                        .close(Some(4409), Some("duplicate or excessive operation ID"));
                    break;
                }
                if let Err(error) = admit_request(&payload, capabilities) {
                    let _ = client.send(&protocol.next(&id, error.envelope()));
                    let _ = client.send(&ws::complete(&id));
                    continue;
                }
                let (abort, registration) = AbortHandle::new_pair();
                let (ack, acks) = mpsc::channel(1);
                let _ = operations.open(id.clone(), Operation { abort, ack });
                let gateway = gateway.clone();
                let env = env.clone();
                let input = input.clone();
                let path = live_path.clone();
                let delivery = delivery.clone();
                let binding = binding.clone();
                let init = init.clone();
                let mut out = out.clone();
                worker::wasm_bindgen_futures::spawn_local(async move {
                    let task = async {
                        if let Err(reason) = operation(
                            &id, payload, gateway, env, input, path, delivery, binding, init,
                            &mut out, acks,
                        )
                        .await
                        {
                            let code = match reason.as_str() {
                                "AUTH_EXPIRED"
                                | "FRESHNESS_PENDING"
                                | "FRESHNESS_SCOPE_CHANGED" => reason.as_str(),
                                _ => LiveEnd::ResetRequired.code(),
                            };
                            let _ = out.send(ws::error(&id, code)).await;
                        }
                    };
                    let _ = Abortable::new(task, registration).await;
                });
            }
            Ok(ClientFrame::Complete { id }) => {
                operations.close(&id);
            }
            Ok(ClientFrame::Ping(payload)) => {
                if client.send(&ws::pong(payload)).is_err() {
                    break;
                }
            }
            // A delivery credit the gateway asked for on an operation's behalf.
            Ok(ClientFrame::Pong(mut payload)) => {
                let Some(id) = payload["gatewayOperation"].as_str().map(str::to_owned) else {
                    continue;
                };
                if let Some(operation) = operations.get_mut(&id) {
                    if let Some(payload) = payload.as_object_mut() {
                        payload.remove("gatewayOperation");
                    }
                    if operation.ack.try_send(ws::pong(payload)).is_err() {
                        break;
                    }
                }
            }
            Ok(ClientFrame::Init(_) | ClientFrame::Terminate) | Err(_) => break,
        }
    }
}
fn post_request(
    input: &OriginRequest,
    value: &serde_json::Value,
    upgrade: bool,
) -> Result<Request> {
    let mut init = RequestInit::new();
    init.method = if upgrade {
        worker::Method::Get
    } else {
        worker::Method::Post
    };
    init.redirect = RequestRedirect::Manual;
    for (name, value) in &input.headers {
        if !matches!(
            name.as_str(),
            "content-length"
                | "content-type"
                | "upgrade"
                | "connection"
                | "sec-websocket-key"
                | "sec-websocket-protocol"
                | "sec-websocket-version"
        ) {
            init.headers.append(name, value)?;
        }
    }
    init.headers.set("content-type", "application/json")?;
    if upgrade {
        init.headers.set("upgrade", "websocket")?;
    }
    if !upgrade {
        init.body = Some(value.to_string().into());
    }
    Request::new_with_init(&input.url, &init)
}
#[allow(clippy::too_many_arguments)]
async fn operation(
    id: &str,
    mut value: serde_json::Value,
    gateway: WorkerGateway,
    env: Env,
    input: OriginRequest,
    live_path: String,
    delivery: Option<WorkerDeliveryBinding>,
    binding: String,
    init: serde_json::Value,
    out: &mut mpsc::Sender<serde_json::Value>,
    mut acks: mpsc::Receiver<serde_json::Value>,
) -> std::result::Result<(), String> {
    mark_connection(&mut value, init.clone());
    let send = |payload| WsProtocol::TransportWs.next(id, payload);
    let kind = operation_kind(
        value["query"].as_str().unwrap_or(""),
        value["operationName"].as_str(),
    );
    // Command/status operations retain the origin's WebSocket identity path.
    // A query may use HTTP reuse only after the origin explicitly recognizes
    // its control protocol; older/custom origins remain independently executed.
    let reuse_query = kind == Ok(OperationKind::Query)
        && matches!(
            input.validate(&value).await,
            Ok(super::coordinator::Admitted::Eligible(_))
        );
    if kind != Ok(OperationKind::Subscription) && !reuse_query {
        let mut stream = live_transport::source(input.clone(), live_path, init)(value).await?;
        while let Some(frame) = stream.next().await {
            out.send(send(frame?)).await.map_err(|_| "consumer left")?;
        }
    } else if reuse_query {
        let request = post_request(&input, &value, false).map_err(|_| "invalid operation")?;
        let abort = super::cancellation::AbortOnDrop(
            worker::web_sys::AbortController::new().map_err(|_| "cancellation unavailable")?,
        );
        let request = super::cancellation::preserve_signal(request, &abort.0.signal())
            .map_err(|_| "invalid operation")?;
        let mut response = Box::pin(gateway.fetch(request, env))
            .await
            .map_err(|_| "origin unavailable")?;
        let bytes = super::coordinator::read_response(
            &mut response,
            input.options.limits.websocket_buffer_bytes,
        )
        .await
        .map_err(|_| "invalid origin response")?;
        let value = serde_json::from_slice::<serde_json::Value>(&bytes)
            .map_err(|_| "invalid origin response")?;
        out.send(send(value)).await.map_err(|_| "consumer left")?;
    } else if let Some(delivery) = delivery.filter(|delivery| delivery.options.live.is_some()) {
        let shard = delivery
            .shard(&binding, &value)
            .map_err(|_| "invalid shard")?;
        let namespace = env
            .durable_object(&delivery.namespace)
            .map_err(|_| "coordinator unavailable")?;
        let stub = namespace
            .id_from_name(&shard)
            .and_then(|id| id.get_stub())
            .map_err(|_| "coordinator unavailable")?;
        let response = stub
            .fetch_with_request(
                post_request(&input, &value, true).map_err(|_| "invalid operation")?,
            )
            .await
            .map_err(|_| "coordinator unavailable")?;
        if response.status_code() != 101 {
            return Err("live admission failed".into());
        }
        let mut socket = Socket::new(
            response.websocket().ok_or("coordinator stream missing")?,
            input.options.limits.websocket_buffer_bytes,
            32,
        )
        .map_err(|_| "invalid coordinator socket")?;
        socket.send(
            &WsProtocol::TransportWs.encode_client(&ClientFrame::Subscribe {
                id: id.to_owned(),
                payload: value,
            }),
        )?;
        loop {
            enum Event {
                Frame(std::result::Result<serde_json::Value, String>),
                Ack(Option<serde_json::Value>),
            }
            let event = match select(Box::pin(socket.next()), Box::pin(acks.next())).await {
                Either::Left((value, _)) => Event::Frame(value),
                Either::Right((value, _)) => Event::Ack(value),
            };
            match event {
                Event::Frame(Ok(mut value)) => match value["type"].as_str() {
                    Some("next") => out
                        .send(send(value["payload"].take()))
                        .await
                        .map_err(|_| "consumer left")?,
                    Some("ping") => {
                        value["payload"]["gatewayOperation"] = id.into();
                        out.send(value).await.map_err(|_| "consumer left")?;
                    }
                    Some("complete") => break,
                    Some("error") => {
                        return Err(value["payload"]
                            .as_str()
                            .unwrap_or(LiveEnd::ResetRequired.code())
                            .into())
                    }
                    _ => return Err(LiveEnd::ResetRequired.code().into()),
                },
                Event::Ack(Some(value)) => socket.send(&value)?,
                _ => return Err(LiveEnd::ResetRequired.code().into()),
            }
        }
    } else {
        let mut stream = live_transport::source(input.clone(), live_path, init)(value).await?;
        while let Some(frame) = stream.next().await {
            out.send(send(frame?)).await.map_err(|_| "consumer left")?;
            let nonce = uuid::Uuid::now_v7().to_string();
            out.send(ws::ping(
                serde_json::json!({"gatewayOperation":id,"gatewayDeliveryAck":nonce}),
            ))
            .await
            .map_err(|_| "consumer left")?;
            let ack = super::timer::deadline(
                Duration::from_millis(input.options.limits.read_timeout_ms),
                acks.next(),
            )
            .await
            .map_err(|_| "slow consumer")?
            .ok_or("consumer left")?;
            if ack["payload"]["gatewayDeliveryAck"] != nonce {
                return Err("invalid acknowledgement".into());
            }
        }
    }
    out.send(ws::complete(id))
        .await
        .map_err(|_| "consumer left".into())
}
// Independent forwarding works with ordinary origins without delivery control.
// Bound cumulative output because workerd has no socket backpressure API;
// exhaustion explicitly requires recovery. Legacy has no ping/pong credits.
async fn independent(
    client: &mut Socket,
    origin: &mut Socket,
    capabilities: GraphqlCapabilities,
    max_bytes: usize,
    protocol: WsProtocol,
) {
    let mut ids = Operations::<()>::default();
    let mut bytes = 0usize;
    loop {
        let (value, from_client) =
            match select(Box::pin(client.next()), Box::pin(origin.next())).await {
                Either::Left((value, _)) => (value, true),
                Either::Right((value, _)) => (value, false),
            };
        let Ok(value) = value else {
            return;
        };
        if from_client {
            let frame = match protocol.decode_client(value) {
                Ok(ClientFrame::Subscribe { id, mut payload }) => {
                    if ids.admit(&id).is_err() {
                        return;
                    }
                    if let Err(error) = admit_request(&payload, capabilities) {
                        let _ = client.send(&protocol.next(&id, error.envelope()));
                        let _ = client.send(&ws::complete(&id));
                        continue;
                    }
                    remove_client_control(&mut payload);
                    let _ = ids.open(id.clone(), ());
                    ClientFrame::Subscribe { id, payload }
                }
                Ok(ClientFrame::Complete { id }) => {
                    ids.close(&id);
                    ClientFrame::Complete { id }
                }
                Ok(frame @ (ClientFrame::Ping(_) | ClientFrame::Pong(_))) => frame,
                Ok(ClientFrame::Init(_) | ClientFrame::Terminate) | Err(_) => return,
            };
            if origin.send(&protocol.encode_client(&frame)).is_err() {
                return;
            }
        } else {
            bytes = bytes.saturating_add(value.to_string().len());
            if bytes > max_bytes {
                let _ = client
                    .ws
                    .close(Some(1013), Some(LiveEnd::ResetRequired.code()));
                return;
            }
            if let Some(id) = ws::ended_operation(&value) {
                ids.close(id);
            }
            if client.send(&value).is_err() {
                return;
            }
        }
    }
}
