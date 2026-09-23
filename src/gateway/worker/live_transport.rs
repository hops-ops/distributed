use super::{coordinator::OriginRequest, live::LiveSourceFactory, proxy, socket::Socket};
use futures_util::{FutureExt, StreamExt};
use std::rc::Rc;
use worker::{Fetch, Headers, Request, RequestInit, RequestRedirect, Result};

pub(super) async fn handshake(
    input: &OriginRequest,
    live_path: &str,
    protocol: &str,
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
    proxy::prepare_headers(&headers, &input.options, &input.context, true)?;
    headers.set("sec-websocket-protocol", protocol)?;
    let public = worker::Url::parse(&input.url)?;
    let url = format!(
        "{}{}{}",
        input.origin.trim_end_matches('/'),
        live_path,
        public.query().map(|q| format!("?{q}")).unwrap_or_default()
    );
    let mut init = RequestInit::new();
    init.headers = headers;
    init.redirect = RequestRedirect::Manual;
    let response = proxy::timeout(
        input.options.limits.header_timeout_ms,
        Fetch::Request(Request::new_with_init(&url, &init)?).send(),
    )
    .await?;
    if response.status_code() != 101
        || response.headers().get("sec-websocket-protocol")?.as_deref() != Some(protocol)
    {
        return Err(worker::Error::RustError("upstream upgrade denied".into()));
    }
    let socket = response
        .websocket()
        .ok_or_else(|| worker::Error::RustError("upstream upgrade missing".into()))?;
    Socket::new(socket, input.options.limits.websocket_buffer_bytes, 32)
}
pub(super) async fn initialize(
    socket: &mut Socket,
    init: serde_json::Value,
    milliseconds: u64,
) -> std::result::Result<serde_json::Value, String> {
    socket.send(&serde_json::json!({"type":"connection_init","payload":init}))?;
    super::timer::deadline(std::time::Duration::from_millis(milliseconds), async {
        loop {
            let value = socket.next().await?;
            match value["type"].as_str() {
                Some("connection_ack") => return Ok(value),
                Some("ping") => {
                    socket.send(&serde_json::json!({"type":"pong","payload":value["payload"]}))?
                }
                Some("connection_error" | "error") => return Err("origin admission denied".into()),
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
            let mut socket = handshake(&input, &live_path, "graphql-transport-ws")
                .await
                .map_err(|_| "origin unavailable".to_owned())?;
            initialize(&mut socket, init, input.options.limits.header_timeout_ms).await?;
            if let Some(extensions) = request
                .get_mut("extensions")
                .and_then(serde_json::Value::as_object_mut)
            {
                extensions.remove("gatewayDelivery");
            }
            socket
                .send(&serde_json::json!({"id":"upstream","type":"subscribe","payload":request}))?;
            Ok(
                futures_util::stream::unfold(socket, |mut socket| async move {
                    loop {
                        let value = match socket.next().await {
                            Ok(value) => value,
                            Err(error) => return Some((Err(error), socket)),
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
                                if let Err(error) = socket.send(
                                    &serde_json::json!({"type":"pong","payload":value["payload"]}),
                                ) {
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
