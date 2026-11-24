//! A client's WebSocket: admission, then frames between the socket and
//! the zone task, through the dev network simulation when one is set.

use super::*;

pub(super) async fn upgrade<Z>(
    ws: WebSocketUpgrade,
    Path(zone_id): Path<String>,
    State(inner): State<Arc<Inner<Z>>>,
) -> Response
where
    Z: Zone,
    Z::Config: Send,
{
    let max = inner.config.max_frame_bytes;
    ws.max_message_size(max)
        .on_upgrade(move |socket| connection(inner, zone_id, socket))
}

pub(super) fn error_frame(code: u32, message: impl Into<String>) -> Vec<u8> {
    ServerMessage::Error {
        code,
        message: message.into(),
    }
    .encode(&())
    .expect("error encodes")
}

pub(super) async fn reject(mut socket: WebSocket, code: u32, message: impl Into<String>) {
    let _ = socket
        .send(Message::Binary(error_frame(code, message).into()))
        .await;
    close(socket).await;
}

/// How long a closing connection keeps reading for the client's Close.
const CLOSE_LINGER: Duration = Duration::from_secs(2);

/// Close so the frames sent before arrive: send Close, then read and
/// discard until the client answers (or hangs up, or [`CLOSE_LINGER`]
/// passes). A socket dropped with client frames still unread is reset
/// (TCP RST), and the client may lose what it had not read yet, such as
/// the Kick or Error saying why.
pub(super) async fn close(mut socket: WebSocket) {
    if socket.send(Message::Close(None)).await.is_err() {
        return;
    }
    let drain = async { while let Some(Ok(_)) = socket.recv().await {} };
    let _ = tokio::time::timeout(CLOSE_LINGER, drain).await;
}

pub(super) async fn sleep_until_opt(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

pub(super) enum Inbound {
    Input {
        seq: u32,
        client_tick: u32,
        payload: Vec<u8>,
    },
    Ping(f64),
    Content(String),
    Focus(f32, f32),
}

pub(super) async fn connection<Z>(inner: Arc<Inner<Z>>, zone_id: String, mut socket: WebSocket)
where
    Z: Zone,
    Z::Config: Send,
{
    let cfg = &inner.config;
    // 1. Hello, admitted before anything else happens.
    let first = tokio::time::timeout(cfg.admission.hello_timeout, socket.recv()).await;
    let hello = match first {
        Ok(Some(Ok(Message::Binary(bytes)))) => match ClientMessage::decode(&bytes) {
            Ok(ClientMessage::Hello(hello)) => hello,
            _ => return reject(socket, error::BAD_MESSAGE, "expected Hello").await,
        },
        Ok(Some(Ok(_))) => return reject(socket, error::BAD_MESSAGE, "binary frames only").await,
        _ => return,
    };
    let now = unix_now();
    let admitted = match admit(&cfg.admission, &hello, &zone_id, now) {
        Ok(admitted) => admitted,
        Err(refusal) => return reject(socket, refusal.code, refusal.message).await,
    };
    // Placement: another host owns this zone -> Redirect (ticket not consumed).
    if let Some(host) = remote_owner(&inner, &zone_id) {
        let url = match &inner.config.redirect_url {
            Some(map) => (map.0)(&host, &zone_id),
            None => host.zone_url(&zone_id),
        };
        let redirect = ServerMessage::Redirect {
            zone_id: zone_id.clone(),
            url,
        }
        .encode(&())
        .expect("redirect encodes");
        let _ = socket.send(Message::Binary(redirect.into())).await;
        return close(socket).await;
    }
    // A transfer is single use at its issuer: an arrival reserves it there
    // (settled once the zone answered), a refused one returning to its
    // source is recorded there as returned.
    let mut reserved: Option<(String, String)> = None;
    let custody = match &admitted {
        Admitted::Arrival(t) => {
            let result = transfer_op(&inner, &t.issuer, TransferOp::Reserve, &t.claims.nonce).await;
            if result.is_ok() {
                reserved = Some((t.issuer.clone(), t.claims.nonce.clone()));
            }
            result
        }
        Admitted::Return(t) => {
            transfer_op(&inner, &t.issuer, TransferOp::Return, &t.claims.nonce).await
        }
        Admitted::Join(_) | Admitted::Reconnect(_) => Ok(()),
    };
    if let Err(e) = custody {
        return reject(socket, error::UNAUTHORIZED, e).await;
    }
    let consumed = admitted.consume(&mut inner.nonces.lock().expect("nonces"), now);
    // Only now may the connection reach a zone: a ticket activates one on
    // demand, a reconnect token only finds a running one.
    let zone = match (&admitted, consumed) {
        (_, Err(refusal)) => Err((refusal.code, refusal.message)),
        (Admitted::Reconnect(_), Ok(())) => running_zone(&inner, &zone_id).ok_or((
            error::RECONNECT_FAILED,
            "zone restarted; rejoin".to_string(),
        )),
        (_, Ok(())) => activate_local(&inner, &zone_id)
            .map_err(|refused| (error::ZONE_UNAVAILABLE, refused.to_string())),
    };
    let mut zone = match zone {
        Ok(zone) => zone,
        Err((code, message)) => {
            if let Some((issuer, nonce)) = reserved.take() {
                let _ = transfer_op(&inner, &issuer, TransferOp::Release, &nonce).await;
            }
            return reject(socket, code, message).await;
        }
    };
    let reconnecting = matches!(admitted, Admitted::Reconnect(_));
    let (out_tx, mut out_rx) = mpsc::channel(cfg.outbound_queue.max(1));
    let (reply_tx, reply_rx) = oneshot::channel();
    let cmd = match admitted {
        Admitted::Join(claims) => ZoneCmd::Join {
            claims,
            transfer: None,
            capabilities: hello.capabilities,
            content_version: hello.content_version,
            out: out_tx,
            reply: reply_tx,
        },
        Admitted::Arrival(transfer) | Admitted::Return(transfer) => ZoneCmd::Join {
            claims: transfer.claims.clone(),
            transfer: Some(transfer),
            capabilities: hello.capabilities,
            content_version: hello.content_version,
            out: out_tx,
            reply: reply_tx,
        },
        Admitted::Reconnect(token) => ZoneCmd::Reconnect {
            token,
            capabilities: hello.capabilities,
            content_version: hello.content_version,
            out: out_tx,
            reply: reply_tx,
        },
    };
    // A zone that just hibernated closes its inbox; a join retries on the
    // next activation (which restores the snapshot first).
    let mut pending = cmd;
    let mut attempts = 0;
    loop {
        match zone.send(pending).await {
            Ok(()) => break,
            Err(mpsc::error::SendError(cmd)) => {
                attempts += 1;
                match activate_local(&inner, &zone_id) {
                    Ok(tx) if attempts < 4 && !reconnecting => {
                        zone = tx;
                        pending = cmd;
                    }
                    _ => {
                        if let Some((issuer, nonce)) = reserved.take() {
                            let _ = transfer_op(&inner, &issuer, TransferOp::Release, &nonce).await;
                        }
                        return reject(socket, error::NOT_FOUND, "zone stopped").await;
                    }
                }
            }
        }
    }
    let result = reply_rx.await;
    if let Some((issuer, nonce)) = reserved.take() {
        if matches!(result, Ok(Ok(_))) {
            let inner = inner.clone();
            tokio::spawn(async move {
                let _ = transfer_op(&inner, &issuer, TransferOp::Confirm, &nonce).await;
            });
        } else {
            // Release before the client hears the refusal: it may present
            // the same ticket to its source right away, and the issuer only
            // accepts that return once the reservation is released.
            let _ = transfer_op(&inner, &issuer, TransferOp::Release, &nonce).await;
        }
    }
    let client = match result {
        Ok(Ok(joined)) => joined.client,
        Ok(Err(e)) => return reject(socket, e.code, e.message).await,
        Err(_) => return reject(socket, error::NOT_FOUND, "zone stopped").await,
    };

    // 2. Session loop.
    let seed = u64::from(client.0) ^ unix_now();
    // Both directions' delay lines run on milliseconds since `origin`; the
    // simulation is read live, so a change applies to the next message.
    let sim = inner.net_sim.subscribe();
    let mut outbound: DelayLine<ConnOut> = DelayLine::new(seed);
    let mut inbound: DelayLine<Inbound> = DelayLine::new(!seed);
    let origin = Instant::now();
    let ms = || origin.elapsed().as_secs_f64() * 1000.0;
    let link = || Link {
        sim: *sim.borrow(),
        now_ms: ms(),
    };
    let mut gate = InputGate::new(&cfg.admission, Duration::ZERO);
    let mut graceful = false;
    let mut notify_zone = true;
    'session: loop {
        // Flush due delayed items.
        while let Some(item) = outbound.pop_due(ms()) {
            match send_out(&mut socket, item).await {
                Ok(true) => {}
                // The zone closed this connection and already forgot the client.
                Ok(false) => {
                    notify_zone = false;
                    break 'session;
                }
                // The socket died under a write: the zone must still learn
                // the connection is gone (grace + ConnectionLost).
                Err(_) => break 'session,
            }
        }
        while let Some(item) = inbound.pop_due(ms()) {
            if !forward_in(
                &inner,
                &zone,
                &mut socket,
                &mut outbound,
                link(),
                client,
                item,
            )
            .await
            {
                break 'session;
            }
        }
        let next = match (outbound.next_due(), inbound.next_due()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
        .map(|due| origin + Duration::from_secs_f64(due.max(0.0) / 1000.0));
        tokio::select! {
            out = out_rx.recv() => {
                let Some(out) = out else { notify_zone = false; break };
                let now = link();
                if let Some(out) = outbound.push(now.sim, now.now_ms, out) {
                    match send_out(&mut socket, out).await {
                        Ok(true) => {}
                        Ok(false) => {
                            notify_zone = false;
                            break;
                        }
                        Err(_) => break,
                    }
                }
            }
            msg = socket.recv() => {
                let bytes = match msg {
                    Some(Ok(Message::Binary(bytes))) => bytes,
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                    Some(Ok(Message::Text(_))) => {
                        let _ = socket.send(Message::Binary(error_frame(error::BAD_MESSAGE, "binary frames only").into())).await;
                        break;
                    }
                    _ => break,
                };
                let decoded = ClientMessage::decode(&bytes);
                // Every client message but Leave (undecodable ones too)
                // spends a token.
                if !matches!(decoded, Ok(ClientMessage::Leave)) {
                    match gate.take(origin.elapsed()) {
                        Gate::Pass => {}
                        Gate::Drop { warn } => {
                            inner.metrics.record_rate_limited(&zone_id);
                            if warn {
                                let _ = socket.send(Message::Binary(error_frame(error::RATE_LIMITED, "message dropped").into())).await;
                            }
                            continue;
                        }
                        Gate::Kick => {
                            let kick = ServerMessage::Kick { code: kick::RATE_LIMITED, message: "message flood".into() };
                            let _ = socket.send(Message::Binary(kick.encode(&()).expect("kick").into())).await;
                            break;
                        }
                    }
                }
                let item = match decoded {
                    Ok(ClientMessage::Input { seq, client_tick, payload }) => {
                        Inbound::Input { seq, client_tick, payload }
                    }
                    Ok(ClientMessage::Ping { client_time }) => Inbound::Ping(client_time),
                    Ok(ClientMessage::Content { content_version }) => Inbound::Content(content_version),
                    // Native zones keep observers' zones running already;
                    // nearness only matters to celld zones.
                    Ok(ClientMessage::Near { .. }) => continue,
                    Ok(ClientMessage::Focus { x, y }) => Inbound::Focus(x, y),
                    Ok(ClientMessage::Leave) => { graceful = true; break; }
                    Ok(ClientMessage::Hello(_)) => {
                        let _ = socket.send(Message::Binary(error_frame(error::BAD_MESSAGE, "already joined").into())).await;
                        continue;
                    }
                    Err(e) => {
                        let _ = socket.send(Message::Binary(error_frame(error::BAD_MESSAGE, e.to_string()).into())).await;
                        continue;
                    }
                };
                let now = link();
                if let Some(item) = inbound.push(now.sim, now.now_ms, item) {
                    if !forward_in(&inner, &zone, &mut socket, &mut outbound, link(), client, item).await {
                        break;
                    }
                }
            }
            _ = sleep_until_opt(next) => {}
        }
    }
    if notify_zone {
        let _ = zone.send(ZoneCmd::Disconnect { client, graceful }).await;
    }
    close(socket).await;
}

/// Ok(false) when the connection should end.
pub(super) async fn send_out(socket: &mut WebSocket, out: ConnOut) -> Result<bool, axum::Error> {
    match out {
        ConnOut::Frame(frame) => {
            socket.send(Message::Binary(frame.into())).await?;
            Ok(true)
        }
        ConnOut::Close => Ok(false),
    }
}

/// The simulated link at the moment an item is forwarded.
#[derive(Clone, Copy)]
pub(super) struct Link {
    pub(super) sim: Option<NetSim>,
    pub(super) now_ms: f64,
}

pub(super) async fn forward_in<Z: Zone>(
    inner: &Arc<Inner<Z>>,
    zone: &mpsc::Sender<ZoneCmd>,
    socket: &mut WebSocket,
    outbound: &mut DelayLine<ConnOut>,
    link: Link,
    client: ClientId,
    item: Inbound,
) -> bool {
    match item {
        Inbound::Input {
            seq,
            client_tick,
            payload,
        } => zone
            .send(ZoneCmd::Input {
                client,
                seq,
                client_tick,
                payload,
            })
            .await
            .is_ok(),
        Inbound::Content(version) => zone
            .send(ZoneCmd::Content { client, version })
            .await
            .is_ok(),
        Inbound::Focus(x, y) => zone.send(ZoneCmd::Focus { client, x, y }).await.is_ok(),
        Inbound::Ping(client_time) => {
            let pong = ServerMessage::Pong {
                client_time,
                server_time: inner.clock.now().as_secs_f64() * 1000.0,
            }
            .encode(&())
            .expect("pong encodes");
            match outbound.push(link.sim, link.now_ms, ConnOut::Frame(pong)) {
                Some(out) => send_out(socket, out).await.unwrap_or(false),
                None => true,
            }
        }
    }
}
