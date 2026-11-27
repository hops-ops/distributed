use super::{proxy, RequestContext, WorkerOptions};
use crate::gateway::{
    delivery::*,
    graphql::ws::{self, ClientFrame, WsProtocol},
    DeliveryCapabilities, GatewayError,
};
use futures_util::{
    future::{LocalBoxFuture, Shared, WeakShared},
    FutureExt, StreamExt,
};
use std::{cell::RefCell, rc::Rc};
use worker::{Headers, Request, RequestInit, RequestRedirect, Response, Result};

/// Optional delivery resources, allocated inside a Durable Object only.
#[derive(Clone, Copy, Debug, Default)]
pub struct WorkerDeliveryOptions {
    /// Bounded complete response cache.
    pub snapshots: Option<SnapshotLimits>,
    /// Bounded concurrent query sharing.
    pub coalescing: Option<FlightLimits>,
    /// Bounded live sharing.
    pub live: Option<LiveLimits>,
}
impl WorkerDeliveryOptions {
    pub(super) fn capabilities(&self) -> DeliveryCapabilities {
        DeliveryCapabilities {
            snapshots: self.snapshots.is_some(),
            coalescing: self.coalescing.is_some(),
            live_sharing: self.live.is_some(),
        }
    }
    pub(super) fn validate(&self) -> std::result::Result<(), GatewayError> {
        if self.snapshots.is_none() && self.coalescing.is_none() && self.live.is_none() {
            return Err(GatewayError("no Worker delivery capability selected"));
        }
        if let Some(limits) = self.snapshots {
            SnapshotCache::new(limits).map_err(|_| GatewayError("invalid snapshot limits"))?;
        }
        if let Some(limits) = self.coalescing {
            FlightRegistry::new(limits).map_err(|_| GatewayError("invalid flight limits"))?;
        }
        if let Some(limits) = self.live {
            limits
                .validate()
                .map_err(|_| GatewayError("invalid live limits"))?;
        }
        // Bound retained wire payload independently of the native defaults.
        // Parsed JSON, credentials and runtime socket overhead require additional
        // headroom; this is a payload budget, not the platform heap limit.
        let snapshots = self.snapshots.map_or(0, |l| l.bytes);
        let flights = self
            .coalescing
            .map_or(0, |l| l.groups.saturating_mul(l.response_bytes));
        let live = self.live.map_or(0, |l| {
            l.groups
                .saturating_mul(
                    l.queue_frames
                        .saturating_add(l.history_frames)
                        .saturating_add(2),
                )
                .saturating_mul(l.frame_bytes)
        });
        if snapshots.saturating_add(flights).saturating_add(live) > 16 * 1024 * 1024 {
            return Err(GatewayError(
                "Worker coordinator retained payload budget exceeds 16 MiB",
            ));
        }
        Ok(())
    }
}
/// Stable sharded namespace configuration, distinct from domain aggregate cells.
#[derive(Clone, Debug)]
pub struct WorkerDeliveryBinding {
    /// Application-declared Durable Object namespace binding name.
    pub namespace: String,
    /// Stable deployment/application namespace. Change to abandon prior coordinator state.
    pub epoch: String,
    /// Number of operation shards, 1..=1024; every ingress uses the same configuration.
    pub shards: u16,
    /// Independent resources and limits for each selected Durable Object.
    pub options: WorkerDeliveryOptions,
}
impl WorkerDeliveryBinding {
    pub(super) fn validate(&self) -> std::result::Result<(), GatewayError> {
        if self.namespace.is_empty()
            || self.namespace.len() > 256
            || self.epoch.is_empty()
            || self.epoch.len() > 256
            || self.shards == 0
            || self.shards > 1024
        {
            return Err(GatewayError("invalid Worker coordinator binding"));
        }
        self.options.validate()
    }
    pub(super) fn shard(&self, binding: &str, value: &serde_json::Value) -> Result<String> {
        use sha2::{Digest, Sha256};
        // Routing uses no caller scope assertion or bearer material. The DO
        // independently authenticates and derives exact scope before any reuse.
        let bytes = canonical_json(&serde_json::json!([
            binding,
            value["query"],
            value["operationName"],
            value["variables"]
        ]))
        .map_err(|_| worker::Error::RustError("invalid operation shard".into()))?;
        let hash = Sha256::digest(bytes);
        let shard = u16::from_be_bytes([hash[0], hash[1]]) % self.shards;
        Ok(format!(
            "gateway-delivery-v1:{}:{binding}:{shard}",
            self.epoch
        ))
    }
}
/// Volatile cache/work state owned by one platform Durable Object instance.
/// A restart starts empty; every reuse still requires current origin validation.
/// Do not keep this in ordinary ingress isolate memory.
pub struct WorkerCoordinator {
    bypasses: std::cell::Cell<u64>,
    options: WorkerDeliveryOptions,
    cache: Option<RefCell<SnapshotCache>>,
    flights: Option<Rc<Flights>>,
    live: Option<Rc<super::live::WorkerLive>>,
}
impl WorkerCoordinator {
    /// Construct only in an application's DurableObject::new implementation.
    pub fn new(options: WorkerDeliveryOptions) -> std::result::Result<Rc<Self>, GatewayError> {
        options.validate()?;
        Ok(Rc::new(Self {
            bypasses: std::cell::Cell::new(0),
            options,
            live: options
                .live
                .map(|limits| {
                    super::live::WorkerLive::new(
                        limits,
                        16 * 1024 * 1024
                            - options.snapshots.map_or(0, |l| l.bytes)
                            - options
                                .coalescing
                                .map_or(0, |l| l.groups * l.response_bytes),
                    )
                })
                .transpose()?,
            cache: options
                .snapshots
                .map(SnapshotCache::new)
                .transpose()
                .map_err(|_| GatewayError("invalid snapshot limits"))?
                .map(RefCell::new),
            flights: options.coalescing.map(Flights::new).transpose()?,
        }))
    }
    /// Identifier-free cache decisions and origin-ineligible query bypasses.
    pub fn metrics(&self) -> (Option<SnapshotMetrics>, u64) {
        (
            self.cache.as_ref().map(|cache| cache.borrow().metrics()),
            self.bypasses.get(),
        )
    }
    /// Forget cached data and fence in-progress fills after reset/lost feed.
    pub fn invalidate_all(&self) {
        if let Some(cache) = &self.cache {
            cache.borrow_mut().invalidate_all();
        }
    }
    /// Current cache entries, active query groups and query consumers; no identity values.
    pub fn counts(&self) -> (usize, usize, usize) {
        let (groups, consumers) = self
            .flights
            .as_ref()
            .map_or((0, 0), |flights| flights.table.borrow().counts());
        (
            self.cache.as_ref().map_or(0, |cache| cache.borrow().len()),
            groups,
            consumers,
        )
    }
    /// Active live groups and consumers, followed by source/reset/frame counters.
    pub fn live_counts(&self) -> (usize, usize, u64, u64, u64, u64, u64) {
        let c = self
            .live
            .as_ref()
            .map(|live| live.counts())
            .unwrap_or_default();
        (
            c.groups,
            c.consumers,
            c.upstreams,
            c.resets,
            c.frames,
            c.deduplicated,
            c.handoffs,
        )
    }
    pub(super) fn upgrade(
        self: &Rc<Self>,
        origin: OriginRequest,
        live_path: String,
        capabilities: crate::gateway::GraphqlCapabilities,
    ) -> Result<Response> {
        let pair = worker::WebSocketPair::new()?;
        let mut socket = super::socket::Socket::new(
            pair.server,
            origin.options.limits.websocket_buffer_bytes,
            4,
        )?;
        let owner = self.clone();
        worker::wasm_bindgen_futures::spawn_local(async move {
            let first = super::timer::deadline(
                std::time::Duration::from_millis(origin.options.limits.header_timeout_ms),
                socket.next(),
            )
            .await;
            let Ok(Ok(first)) = first else {
                return;
            };
            // The ingress's own subscribe frame: one operation per socket.
            let Ok(ClientFrame::Subscribe { payload: value, .. }) =
                WsProtocol::TransportWs.decode_client(first)
            else {
                return;
            };
            if crate::gateway::graphql::admit_request(&value, capabilities).is_err()
                || crate::gateway::graphql::operation_kind(
                    value["query"].as_str().unwrap_or(""),
                    value["operationName"].as_str(),
                ) != Ok(crate::gateway::graphql::OperationKind::Subscription)
            {
                return;
            }
            let _ = owner.subscribe(origin, live_path, value, socket).await;
        });
        Response::from_websocket(pair.client)
    }
    pub(super) async fn subscribe(
        self: &Rc<Self>,
        origin: OriginRequest,
        live_path: String,
        value: serde_json::Value,
        mut socket: super::socket::Socket,
    ) -> Result<()> {
        let freshness = value["extensions"]
            .get("gatewayFreshness")
            .map(FreshnessContext::parse)
            .transpose()
            .map_err(delivery_error)?;
        // The ingress recorded this consumer's connection_init here.
        let init = value["extensions"]["gatewayDelivery"]
            .get("connectionInit")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let source = super::live_transport::source(origin.clone(), live_path, init);
        let admission = origin.validate(&value).await?;
        let lifetime = origin.options.limits.websocket_lifetime_ms;
        let ack_timeout = origin.options.limits.read_timeout_ms;
        let lease = match admission {
            Admitted::Eligible(admission) => {
                let live = self
                    .live
                    .as_ref()
                    .ok_or_else(|| worker::Error::RustError("live coordinator absent".into()))?;
                // Sharing is an optimization. A full registry, or a group this
                // consumer cannot join, means this consumer runs alone.
                live.join(admission, value.clone(), freshness, source.clone())
                    .ok()
            }
            Admitted::Bypass => None,
            Admitted::Error(mut response) => {
                let bytes = read_response(&mut response, MAX_VALIDATION_BYTES).await?;
                let payload = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_else(
                    |_| serde_json::json!({"errors":[{"message":"origin admission failed"}]}),
                );
                let _ = socket.send(&serde_json::json!({"type":"next","payload":payload}));
                let _ = socket.send(&serde_json::json!({"type":"complete"}));
                return Ok(());
            }
        };
        match lease {
            Some(mut lease) => {
                worker::wasm_bindgen_futures::spawn_local(async move {
                    let run = async {
                        loop {
                            enum Event {
                                Frame(
                                    std::result::Result<
                                        Option<Rc<super::live::WorkerFrame>>,
                                        LiveEnd,
                                    >,
                                ),
                                Client(std::result::Result<serde_json::Value, String>),
                            }
                            let event = match futures_util::future::select(
                                Box::pin(lease.next()),
                                Box::pin(socket.next()),
                            )
                            .await
                            {
                                futures_util::future::Either::Left((frame, _)) => {
                                    Event::Frame(frame)
                                }
                                futures_util::future::Either::Right((client, _)) => {
                                    Event::Client(client)
                                }
                            };
                            let frame = match event {
                                Event::Frame(frame) => frame,
                                Event::Client(Ok(value)) => {
                                    match WsProtocol::TransportWs.decode_client(value) {
                                        Ok(ClientFrame::Ping(payload)) => {
                                            let _ = socket.send(&ws::pong(payload));
                                            continue;
                                        }
                                        _ => return,
                                    }
                                }
                                Event::Client(Err(_)) => return,
                            };
                            let frame = match frame {
                                Ok(Some(frame)) => frame,
                                Ok(None) => {
                                    let _ = socket.send(&serde_json::json!({"type":"complete"}));
                                    return;
                                }
                                Err(end) => {
                                    let _ = socket.send(
                                        &serde_json::json!({"type":"error","payload":end.code()}),
                                    );
                                    return;
                                }
                            };
                            if socket
                                .send(&serde_json::json!({"type":"next","payload":frame.payload()}))
                                .is_err()
                            {
                                return;
                            }
                            // Standard GraphQL ping/pong provides a delivery credit on
                            // workerd, whose WebSocket API has no bufferedAmount.
                            let nonce = uuid::Uuid::now_v7().to_string();
                            if socket
                                .send(&ws::ping(serde_json::json!({"gatewayDeliveryAck":nonce})))
                                .is_err()
                            {
                                return;
                            }
                            let acknowledged = {
                                let ack = wait_ack(&mut socket, &nonce, ack_timeout);
                                matches!(
                                    futures_util::future::select(
                                        Box::pin(ack),
                                        Box::pin(lease.interrupted())
                                    )
                                    .await,
                                    futures_util::future::Either::Left((Ok(()), _))
                                )
                            };
                            if !acknowledged {
                                let _ = socket
                                    .ws
                                    .close(Some(1013), Some(LiveEnd::ResetRequired.code()));
                                return;
                            }
                        }
                    };
                    let _ = super::timer::deadline(std::time::Duration::from_millis(lifetime), run)
                        .await;
                });
            }
            None => {
                worker::wasm_bindgen_futures::spawn_local(async move {
                    let run = async {
                        let Ok(mut stream) = source(value).await else {
                            return;
                        };
                        loop {
                            let frame = {
                                let result = futures_util::future::select(
                                    Box::pin(stream.next()),
                                    Box::pin(socket.next()),
                                )
                                .await;
                                match result {
                                    futures_util::future::Either::Left((frame, _)) => frame,
                                    _ => return,
                                }
                            };
                            let Some(frame) = frame else {
                                break;
                            };
                            let Ok(frame) = frame else {
                                let _ = socket
                                    .ws
                                    .close(Some(1013), Some(LiveEnd::ResetRequired.code()));
                                return;
                            };
                            if socket
                                .send(&serde_json::json!({"type":"next","payload":frame}))
                                .is_err()
                            {
                                return;
                            }
                            let nonce = uuid::Uuid::now_v7().to_string();
                            if socket
                                .send(&ws::ping(serde_json::json!({"gatewayDeliveryAck":nonce})))
                                .is_err()
                                || wait_ack(&mut socket, &nonce, ack_timeout).await.is_err()
                            {
                                return;
                            }
                        }
                        let _ = socket.send(&serde_json::json!({"type":"complete"}));
                    };
                    let _ = super::timer::deadline(std::time::Duration::from_millis(lifetime), run)
                        .await;
                });
            }
        }
        Ok(())
    }
    pub(super) fn capabilities(&self) -> DeliveryCapabilities {
        self.options.capabilities()
    }
    pub(super) async fn execute(
        self: &Rc<Self>,
        origin: OriginRequest,
        value: serde_json::Value,
    ) -> Result<Response> {
        let freshness = value["extensions"]
            .get("gatewayFreshness")
            .map(FreshnessContext::parse)
            .transpose()
            .map_err(|_| worker::Error::RustError("invalid freshness".into()))?;
        let admission = match origin.validate(&value).await? {
            Admitted::Eligible(admission) => admission,
            Admitted::Bypass => {
                self.bypasses.set(self.bypasses.get().saturating_add(1));
                return origin.execute(value).await;
            }
            Admitted::Error(response) => return Ok(response),
        };
        let ticket = if let Some(cache) = &self.cache {
            let mut cache = cache.borrow_mut();
            match cache
                .lookup(&admission, freshness.as_ref(), super::now())
                .map_err(delivery_error)?
            {
                Some(hit) => return render(hit),
                None => Some(
                    cache
                        .begin_fill(&admission, super::now())
                        .map_err(delivery_error)?,
                ),
            }
        } else {
            None
        };
        if let Some(flights) = &self.flights {
            let key = FlightKey::admitted(&admission, &value, freshness.as_ref(), super::now())
                .map_err(delivery_error)?;
            let owner = self.clone();
            let input = origin.clone();
            let request = value.clone();
            let admitted = admission.clone();
            let floor = freshness.clone();
            let expires = admission.expires_at;
            let limit = flights.limits;
            let lease = flights.join(key, move || {
                async move {
                    let work = async {
                        let result = owner
                            .fill(input, request, admitted.clone(), floor.clone(), ticket)
                            .await?;
                        let captured = capture(result, limit.response_bytes).await?;
                        Ok(match captured {
                            Captured::Bytes(snapshot)
                                if snapshot.shareable(&admitted, floor.as_ref()) =>
                            {
                                Outcome::Shared(Rc::new(snapshot))
                            }
                            Captured::Bytes(snapshot) => exclusive(render(snapshot)?),
                            Captured::Streaming(response) => exclusive(response),
                        })
                    };
                    proxy::timeout(limit.deadline_ms, work)
                        .await
                        .unwrap_or_else(|_| {
                            exclusive(
                                Response::error("origin unavailable", 502).expect("fixed status"),
                            )
                        })
                }
                .boxed_local()
            });
            // Coalescing is an optimization: with no room to join, run alone.
            let Some(lease) = lease else {
                return origin.execute(value).await;
            };
            let outcome = proxy::timeout(expires.saturating_sub(super::now()) * 1000, async {
                Ok(lease.work.clone().await)
            })
            .await;
            if super::now() >= expires {
                return Response::error("credential expired", 401);
            }
            match outcome? {
                Outcome::Shared(snapshot) => return render((*snapshot).clone()),
                Outcome::Exclusive(response) => {
                    if let Some(response) = response.borrow_mut().take() {
                        return Ok(response);
                    }
                }
            }
            return origin.execute(value).await;
        }
        self.fill(origin, value, admission, freshness, ticket).await
    }
    async fn fill(
        &self,
        origin: OriginRequest,
        value: serde_json::Value,
        admission: OriginAdmission,
        freshness: Option<FreshnessContext>,
        ticket: Option<FillTicket>,
    ) -> Result<Response> {
        let mut marked = value.clone();
        mark(&mut marked, "snapshot");
        let limit = self
            .options
            .coalescing
            .map(|l| l.response_bytes)
            .or(self.options.snapshots.map(|l| l.entry_bytes))
            .unwrap_or(1024 * 1024);
        let captured = capture(origin.execute(marked).await?, limit).await?;
        let snapshot = match captured {
            Captured::Bytes(snapshot) => snapshot,
            Captured::Streaming(response) => return Ok(response),
        };
        if snapshot.shareable(&admission, freshness.as_ref()) {
            match origin.validate(&value).await? {
                Admitted::Eligible(current) => {
                    if admission.same_scope(&current).is_err() {
                        return Response::error("origin scope changed", 409);
                    }
                    if let (Some(cache), Some(ticket)) = (&self.cache, ticket) {
                        cache
                            .borrow_mut()
                            .install(ticket, current, snapshot.clone(), super::now())
                            .map_err(delivery_error)?;
                    }
                }
                Admitted::Error(response) => return Ok(response),
                Admitted::Bypass => {}
            }
        }
        render(snapshot)
    }
}
#[derive(Clone)]
pub(super) struct OriginRequest {
    pub origin: String,
    pub path: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub options: WorkerOptions,
    pub context: RequestContext,
}
impl OriginRequest {
    pub(super) async fn execute(&self, value: serde_json::Value) -> Result<Response> {
        let mut init = RequestInit::new();
        init.method = worker::Method::Post;
        init.redirect = RequestRedirect::Manual;
        for (name, value) in &self.headers {
            if !matches!(
                name.to_ascii_lowercase().as_str(),
                "content-length"
                    | "connection"
                    | "upgrade"
                    | "sec-websocket-key"
                    | "sec-websocket-protocol"
                    | "sec-websocket-version"
            ) {
                init.headers.append(name, value)?;
            }
        }
        init.headers.set("content-type", "application/json")?;
        init.body = Some(value.to_string().into());
        proxy::forward(
            Request::new_with_init(&self.url, &init)?,
            &self.origin,
            Some(&self.path),
            false,
            crate::gateway::Forwarding::Graphql,
            &self.options,
            &self.context,
        )
        .await
    }
    pub(super) async fn validate(&self, value: &serde_json::Value) -> Result<Admitted> {
        let mut marked = value.clone();
        mark(&mut marked, "validate");
        let result = self.execute(marked).await?;
        let Captured::Bytes(snapshot) = capture(result, MAX_VALIDATION_BYTES).await? else {
            return Ok(Admitted::Error(Response::error(
                "oversized origin admission",
                502,
            )?));
        };
        let sets_cookie = snapshot
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("set-cookie"));
        Ok(
            match verdict(
                snapshot.status,
                sets_cookie,
                &snapshot.body,
                value,
                super::now(),
            ) {
                OriginVerdict::Eligible(admission) => Admitted::Eligible(admission),
                OriginVerdict::Bypass => Admitted::Bypass,
                OriginVerdict::Reject => Admitted::Error(render(snapshot)?),
                OriginVerdict::Malformed => {
                    Admitted::Error(Response::error("invalid origin admission", 502)?)
                }
                OriginVerdict::Unbound(_) => {
                    Admitted::Error(Response::error("origin admission unavailable", 503)?)
                }
            },
        )
    }
}
pub(super) enum Admitted {
    Eligible(OriginAdmission),
    Bypass,
    Error(Response),
}
fn delivery_error(_: DeliveryError) -> worker::Error {
    worker::Error::RustError("delivery unavailable".into())
}
fn render(snapshot: SnapshotResponse) -> Result<Response> {
    let headers = Headers::new();
    for (name, value) in snapshot.headers {
        headers.append(&name, &value)?;
    }
    Ok(Response::from_bytes(snapshot.body)?
        .with_status(snapshot.status)
        .with_headers(headers))
}
enum Captured {
    Bytes(SnapshotResponse),
    Streaming(Response),
}
async fn capture(mut response: Response, limit: usize) -> Result<Captured> {
    let status = response.status_code();
    let headers = response.headers().entries().collect::<Vec<_>>();
    match response.body() {
        worker::ResponseBody::Empty => {
            return Ok(Captured::Bytes(SnapshotResponse {
                status,
                headers,
                body: Vec::new(),
            }))
        }
        worker::ResponseBody::Body(body) if body.len() <= limit => {
            return Ok(Captured::Bytes(SnapshotResponse {
                status,
                headers,
                body: body.clone(),
            }))
        }
        worker::ResponseBody::Body(_) => return Ok(Captured::Streaming(response)),
        worker::ResponseBody::Stream(_) => {}
    }
    let stream = response.stream()?;
    let mut stream = stream;
    let mut chunks = Vec::new();
    let mut size = 0;
    while let Some(chunk) = stream.next().await {
        let failed = chunk.is_err();
        size += chunk.as_ref().map_or(0, Vec::len);
        chunks.push(chunk);
        if failed || size > limit {
            let head = Headers::new();
            for (name, value) in headers {
                head.append(&name, &value)?;
            }
            return Ok(Captured::Streaming(
                Response::from_stream(futures_util::stream::iter(chunks).chain(stream))?
                    .with_status(status)
                    .with_headers(head),
            ));
        }
    }
    let mut body = Vec::with_capacity(size);
    for chunk in chunks {
        body.extend(chunk?);
    }
    Ok(Captured::Bytes(SnapshotResponse {
        status,
        headers,
        body,
    }))
}
#[derive(Clone)]
enum Outcome {
    Shared(Rc<SnapshotResponse>),
    Exclusive(Rc<RefCell<Option<Response>>>),
}
fn exclusive(response: Response) -> Outcome {
    Outcome::Exclusive(Rc::new(RefCell::new(Some(response))))
}
type Work = LocalBoxFuture<'static, Outcome>;
struct Flights {
    table: RefCell<FlightTable<WeakShared<Work>>>,
    limits: FlightLimits,
}
struct Lease {
    owner: Rc<Flights>,
    ticket: Option<FlightTicket>,
    work: Shared<Work>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            self.owner.table.borrow_mut().leave(ticket);
        }
    }
}
impl Flights {
    fn new(limits: FlightLimits) -> std::result::Result<Rc<Self>, GatewayError> {
        Ok(Rc::new(Self {
            table: RefCell::new(
                FlightTable::new(limits).map_err(|_| GatewayError("invalid flight limits"))?,
            ),
            limits,
        }))
    }
    fn join(self: &Rc<Self>, key: FlightKey, start: impl FnOnce() -> Work) -> Option<Lease> {
        let (ticket, work) = self
            .table
            .borrow_mut()
            .join(
                key,
                worker::Date::now().as_millis(),
                || {
                    let work = start().shared();
                    let weak = work.downgrade().expect("unpolled shared work");
                    (work, weak)
                },
                WeakShared::upgrade,
            )
            .ok()?;
        Some(Lease {
            owner: self.clone(),
            ticket: Some(ticket),
            work,
        })
    }
}

async fn wait_ack(
    socket: &mut super::socket::Socket,
    nonce: &str,
    milliseconds: u64,
) -> std::result::Result<(), String> {
    super::timer::deadline(std::time::Duration::from_millis(milliseconds), async {
        loop {
            match WsProtocol::TransportWs.decode_client(socket.next().await?) {
                Ok(ClientFrame::Pong(payload)) if payload["gatewayDeliveryAck"] == nonce => {
                    return Ok(())
                }
                Ok(ClientFrame::Ping(payload)) => socket.send(&ws::pong(payload))?,
                _ => return Err("invalid delivery acknowledgement".into()),
            }
        }
    })
    .await
    .map_err(|_| "delivery acknowledgement timeout".to_owned())?
}

pub(super) async fn read_response(response: &mut Response, max: usize) -> Result<Vec<u8>> {
    match response.body() {
        worker::ResponseBody::Empty => return Ok(Vec::new()),
        worker::ResponseBody::Body(bytes) if bytes.len() <= max => return Ok(bytes.clone()),
        worker::ResponseBody::Body(_) => {
            return Err(worker::Error::RustError("response too large".into()))
        }
        _ => {}
    }
    let mut stream = response.stream()?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if bytes.len().saturating_add(chunk.len()) > max {
            return Err(worker::Error::RustError("response too large".into()));
        }
        bytes.extend(chunk);
    }
    Ok(bytes)
}
