use super::{proxy, RequestContext, WorkerOptions};
use crate::gateway::{delivery::*, DeliveryCapabilities, GatewayError};
use futures_util::{
    future::{LocalBoxFuture, Shared, WeakShared},
    FutureExt, StreamExt,
};
use std::{cell::RefCell, collections::BTreeMap, rc::Rc};
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
        let (groups, consumers) = self.flights.as_ref().map_or((0, 0), |flights| {
            let state = flights.state.borrow();
            (state.registry.len(), state.registry.consumers())
        });
        (
            self.cache.as_ref().map_or(0, |cache| cache.borrow().len()),
            groups,
            consumers,
        )
    }
    /// Active live groups and consumers, followed by source/reset/frame counters.
    pub fn live_counts(&self) -> (usize, usize, u64, u64, u64, u64, u64) {
        self.live
            .as_ref()
            .map_or((0, 0, 0, 0, 0, 0, 0), |live| live.counts())
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
            let value = first["payload"].clone();
            if first["type"] != "subscribe"
                || crate::gateway::graphql::admit_request(&value, capabilities).is_err()
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
        let init = value["extensions"]["gatewayDelivery"]
            .get("connectionInit")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let source = super::live_transport::source(origin.clone(), live_path, init);
        let admission = origin.validate(&value).await?;
        let lifetime = origin.options.limits.websocket_lifetime_ms;
        let ack_timeout = origin.options.limits.read_timeout_ms;
        match admission {
            Admitted::Eligible(admission) => {
                let live = self
                    .live
                    .as_ref()
                    .ok_or_else(|| worker::Error::RustError("live coordinator absent".into()))?;
                let mut lease = live
                    .join(admission, value, freshness, source)
                    .map_err(delivery_error)?;
                worker::wasm_bindgen_futures::spawn_local(async move {
                    let run = async {
                        loop {
                            enum Event {
                                Frame(
                                    std::result::Result<
                                        Option<Rc<super::live::WorkerFrame>>,
                                        &'static str,
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
                                Event::Client(Ok(value)) if value["type"] == "ping" => {
                                    let _=socket.send(&serde_json::json!({"type":"pong","payload":value["payload"]}));
                                    continue;
                                }
                                _ => return,
                            };
                            let frame = match frame {
                                Ok(Some(frame)) => frame,
                                Ok(None) => {
                                    let _ = socket.send(&serde_json::json!({"type":"complete"}));
                                    return;
                                }
                                Err(reason) => {
                                    let _ = socket.send(
                                        &serde_json::json!({"type":"error","payload":reason}),
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
                            if socket.send(&serde_json::json!({"type":"ping","payload":{"gatewayDeliveryAck":nonce}})).is_err(){return;}
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
                                let _ = socket.ws.close(Some(1013), Some("LIVE_RESET_REQUIRED"));
                                return;
                            }
                        }
                    };
                    let _ = super::timer::deadline(std::time::Duration::from_millis(lifetime), run)
                        .await;
                });
            }
            Admitted::Bypass => {
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
                                let _ = socket.ws.close(Some(1013), Some("LIVE_RESET_REQUIRED"));
                                return;
                            };
                            if socket
                                .send(&serde_json::json!({"type":"next","payload":frame}))
                                .is_err()
                            {
                                return;
                            }
                            let nonce = uuid::Uuid::now_v7().to_string();
                            if socket.send(&serde_json::json!({"type":"ping","payload":{"gatewayDeliveryAck":nonce}})).is_err()||wait_ack(&mut socket,&nonce,ack_timeout).await.is_err(){return;}
                        }
                        let _ = socket.send(&serde_json::json!({"type":"complete"}));
                    };
                    let _ = super::timer::deadline(std::time::Duration::from_millis(lifetime), run)
                        .await;
                });
            }
            Admitted::Error(mut response) => {
                let bytes = read_response(&mut response, 65536).await?;
                let payload = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_else(
                    |_| serde_json::json!({"errors":[{"message":"origin admission failed"}]}),
                );
                let _ = socket.send(&serde_json::json!({"type":"next","payload":payload}));
                let _ = socket.send(&serde_json::json!({"type":"complete"}));
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
            let lease = flights
                .join(key, move || {
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
                                    Response::error("origin unavailable", 502)
                                        .expect("fixed status"),
                                )
                            })
                    }
                    .boxed_local()
                })
                .map_err(delivery_error)?;
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
                    if current.identity != admission.identity || current.key != admission.key {
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
            &self.options,
            &self.context,
        )
        .await
    }
    pub(super) async fn validate(&self, value: &serde_json::Value) -> Result<Admitted> {
        let mut marked = value.clone();
        mark(&mut marked, "validate");
        let result = self.execute(marked).await?;
        let Captured::Bytes(snapshot) = capture(result, 65536).await? else {
            return Err(worker::Error::RustError(
                "oversized origin admission".into(),
            ));
        };
        if snapshot.status != 200 {
            return Ok(Admitted::Error(render(snapshot)?));
        }
        let parsed: serde_json::Value = serde_json::from_slice(&snapshot.body)?;
        if parsed
            .get("errors")
            .is_some_and(|v| v.as_array().is_none_or(|v| !v.is_empty()))
        {
            return Ok(Admitted::Error(render(snapshot)?));
        }
        let delivery = &parsed["extensions"]["gatewayDelivery"];
        if delivery["eligible"] != true
            || snapshot
                .headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("set-cookie"))
        {
            return Ok(Admitted::Bypass);
        }
        let admission: OriginAdmission = serde_json::from_value(delivery["admission"].clone())?;
        admission
            .bind(value, super::now())
            .map_err(delivery_error)?;
        Ok(Admitted::Eligible(admission))
    }
}
pub(super) enum Admitted {
    Eligible(OriginAdmission),
    Bypass,
    Error(Response),
}
pub(super) fn mark(value: &mut serde_json::Value, action: &str) {
    if !value["extensions"].is_object() {
        value["extensions"] = serde_json::json!({});
    }
    let init = value["extensions"]["gatewayDelivery"]
        .get("connectionInit")
        .cloned();
    value["extensions"]["gatewayDelivery"] = serde_json::json!({"action":action});
    if let Some(init) = init {
        value["extensions"]["gatewayDelivery"]["connectionInit"] = init;
    }
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
struct FlightState {
    registry: FlightRegistry,
    work: BTreeMap<u64, WeakShared<Work>>,
}
struct Flights {
    state: RefCell<FlightState>,
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
            let generation = ticket.generation();
            let mut state = self.owner.state.borrow_mut();
            if state.registry.leave(ticket) {
                state.work.remove(&generation);
            }
        }
    }
}
impl Flights {
    fn new(limits: FlightLimits) -> std::result::Result<Rc<Self>, GatewayError> {
        Ok(Rc::new(Self {
            state: RefCell::new(FlightState {
                registry: FlightRegistry::new(limits)
                    .map_err(|_| GatewayError("invalid flight limits"))?,
                work: BTreeMap::new(),
            }),
            limits,
        }))
    }
    fn join(
        self: &Rc<Self>,
        key: FlightKey,
        start: impl FnOnce() -> Work,
    ) -> std::result::Result<Lease, DeliveryError> {
        let mut state = self.state.borrow_mut();
        state.registry.expire(worker::Date::now().as_millis());
        let expired = state
            .work
            .keys()
            .filter(|generation| !state.registry.contains_generation(**generation))
            .copied()
            .collect::<Vec<_>>();
        for generation in expired {
            state.work.remove(&generation);
        }
        let (ticket, owner) = state.registry.join(key, worker::Date::now().as_millis())?;
        let generation = ticket.generation();
        let work = if owner {
            let work = start().shared();
            state.work.insert(
                generation,
                work.downgrade().ok_or(DeliveryError::Unavailable)?,
            );
            work
        } else {
            state
                .work
                .get(&generation)
                .and_then(WeakShared::upgrade)
                .ok_or(DeliveryError::Unavailable)?
        };
        Ok(Lease {
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
            let value = socket.next().await?;
            match value["type"].as_str() {
                Some("pong") if value["payload"]["gatewayDeliveryAck"] == nonce => return Ok(()),
                Some("ping") => {
                    socket.send(&serde_json::json!({"type":"pong","payload":value["payload"]}))?
                }
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
