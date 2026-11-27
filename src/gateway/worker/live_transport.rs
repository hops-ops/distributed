use super::{coordinator::OriginRequest, live::LiveSourceFactory, proxy, socket::Socket};
use crate::gateway::graphql::ws::{self, ClientFrame, ServerFrame, WsProtocol};
use futures_util::{FutureExt, StreamExt};
use std::rc::Rc;
use worker::{Fetch, Headers, Request, RequestInit, RequestRedirect, Result};

pub(super) async fn handshake(
    input: &OriginRequest,
    live_path: &str,
    protocol: WsProtocol,
) -> Result<Socket> {
    let headers = Headers::new();
    for (name, value) in &input.headers {
        if !matches!(
            name.as_str(),
            "content-length" | "content-type" | "sec-websocket-key"
        ) {
            headers.append(name, value)?;
        }
    }
    proxy::prepare_headers(
        &headers,
        crate::gateway::Forwarding::Graphql,
        &input.options,
        &input.context,
        true,
    )?;
    headers.set("sec-websocket-protocol", protocol.name())?;
    let url = format!("{}{}", input.origin.trim_end_matches('/'), live_path);
    let mut init = RequestInit::new();
    init.headers = headers;
    init.redirect = RequestRedirect::Manual;
    let response = proxy::timeout(
        input.options.limits.header_timeout_ms,
        Fetch::Request(Request::new_with_init(&url, &init)?).send(),
    )
    .await?;
    if response.status_code() != 101
        || response.headers().get("sec-websocket-protocol")?.as_deref() != Some(protocol.name())
    {
        return Err(worker::Error::RustError("upstream upgrade denied".into()));
    }
    let socket = response
        .websocket()
        .ok_or_else(|| worker::Error::RustError("upstream upgrade missing".into()))?;
    Socket::new(socket, input.options.limits.websocket_buffer_bytes, 32)
}
/// Send `connection_init`; the origin's `connection_ack` frame on success.
pub(super) async fn initialize(
    socket: &mut Socket,
    protocol: WsProtocol,
    init: serde_json::Value,
    milliseconds: u64,
) -> std::result::Result<serde_json::Value, String> {
    socket.send(&protocol.encode_client(&ClientFrame::Init(init)))?;
    super::timer::deadline(std::time::Duration::from_millis(milliseconds), async {
        loop {
            let value = socket.next().await?;
            match ws::decode_server(value.clone()) {
                ServerFrame::Ack => return Ok(value),
                ServerFrame::Ping(payload) => socket.send(&ws::pong(payload))?,
                ServerFrame::ConnectionError | ServerFrame::Error { .. } => {
                    return Err("origin admission denied".into())
                }
                _ => return Err("invalid origin initialization".into()),
            }
        }
    })
    .await
    .map_err(|_| "origin admission timed out".to_owned())?
}
pub(super) fn source(
    input: OriginRequest,
    live_path: String,
    init: serde_json::Value,
) -> LiveSourceFactory {
    Rc::new(move |mut request| {
        let input = input.clone();
        let live_path = live_path.clone();
        let init = init.clone();
        async move {
            let protocol = WsProtocol::TransportWs;
            let mut socket = handshake(&input, &live_path, protocol)
                .await
                .map_err(|_| "origin unavailable".to_owned())?;
            initialize(
                &mut socket,
                protocol,
                init,
                input.options.limits.header_timeout_ms,
            )
            .await?;
            if let Some(extensions) = request
                .get_mut("extensions")
                .and_then(serde_json::Value::as_object_mut)
            {
                extensions.remove("gatewayDelivery");
            }
            socket.send(&protocol.encode_client(&ClientFrame::Subscribe {
                id: UPSTREAM.into(),
                payload: request,
            }))?;
            Ok(
                futures_util::stream::unfold(socket, |mut socket| async move {
                    loop {
                        let value = match socket.next().await {
                            Ok(value) => value,
                            Err(error) => return Some((Err(error), socket)),
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
                                if let Err(error) = socket.send(&ws::pong(payload)) {
                                    return Some((Err(error), socket));
                                }
                            }
                            _ => {}
                        }
                    }
                })
                .boxed_local(),
            )
        }
        .boxed_local()
    })
}
/// Operation id of the gateway's own single upstream subscription.
const UPSTREAM: &str = "upstream";
