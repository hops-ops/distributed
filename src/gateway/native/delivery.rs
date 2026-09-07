use super::{
    graphql::GraphqlBinding, response, Body, GatewayError, NativeInner, Request, RequestContext,
    Response, StatusCode,
};
use crate::gateway::{delivery::*, DeliveryCapabilities, GraphqlExecutor};
use axum::{
    body::Bytes,
    http::{header, request::Parts},
    response::IntoResponse,
};
use futures_util::StreamExt;
use std::sync::{Arc, Mutex};
use tokio::sync::OwnedSemaphorePermit;

/// Independently selected native delivery capabilities. None allocates nothing.
#[derive(Default)]
pub struct NativeDeliveryOptions {
    /// Optional complete snapshot storage.
    pub snapshots: Option<SnapshotLimits>,
    /// Optional concurrent query execution coordination.
    pub coalescing: Option<FlightLimits>,
    /// Optional shared live operation coordination.
    pub live: Option<LiveLimits>,
}
/// Bounded native delivery. Each consumer authenticates at the origin before
/// lookup/join; snapshot storage and shared query execution are independent.
pub struct NativeDelivery {
    snapshots: Option<Mutex<SnapshotCache>>,
    flights: Option<Arc<super::flight::NativeFlights>>,
    pub(super) live: Option<Arc<super::live::NativeLive>>,
    entry_bytes: usize,
}
struct Fill {
    binding: GraphqlBinding,
    inner: Arc<NativeInner>,
    executor: GraphqlExecutor,
    context: RequestContext,
    parts: Parts,
    value: serde_json::Value,
    admission: OriginAdmission,
    freshness: Option<FreshnessContext>,
    ticket: Option<FillTicket>,
}
impl NativeDelivery {
    /// Allocate only the explicitly selected capabilities and their bounds.
    pub fn new(options: NativeDeliveryOptions) -> Result<Self, GatewayError> {
        if options.snapshots.is_none() && options.coalescing.is_none() && options.live.is_none() {
            return Err(GatewayError("no delivery capability selected"));
        }
        let entry_bytes = options
            .coalescing
            .map(|limits| limits.response_bytes)
            .or(options.snapshots.map(|limits| limits.entry_bytes))
            .unwrap_or(1024 * 1024);
        Ok(Self {
            snapshots: options
                .snapshots
                .map(SnapshotCache::new)
                .transpose()
                .map_err(|_| GatewayError("invalid snapshot limits"))?
                .map(Mutex::new),
            flights: options
                .coalescing
                .map(super::flight::NativeFlights::new)
                .transpose()?,
            live: options.live.map(super::live::NativeLive::new).transpose()?,
            entry_bytes,
        })
    }
    /// Allocate a bounded origin-validated snapshot cache.
    pub fn snapshots(limits: SnapshotLimits) -> Result<Self, GatewayError> {
        Self::new(NativeDeliveryOptions {
            snapshots: Some(limits),
            coalescing: None,
            live: None,
        })
    }
    /// Allocate bounded query coalescing without snapshot storage.
    pub fn coalescing(limits: FlightLimits) -> Result<Self, GatewayError> {
        Self::new(NativeDeliveryOptions {
            snapshots: None,
            coalescing: Some(limits),
            live: None,
        })
    }
    /// Allocate shared live coordination without query caching/coalescing.
    pub fn live(limits: LiveLimits) -> Result<Self, GatewayError> {
        Self::new(NativeDeliveryOptions {
            live: Some(limits),
            ..Default::default()
        })
    }
    /// Active live groups/consumers and cumulative source attempts, resets,
    /// upstream frames, duplicate frames and safe consumer handoffs.
    pub fn live_counts(&self) -> (usize, usize, u64, u64, u64, u64, u64) {
        self.live
            .as_ref()
            .map_or((0, 0, 0, 0, 0, 0, 0), |live| live.counts())
    }
    pub(super) fn capabilities(&self) -> DeliveryCapabilities {
        DeliveryCapabilities {
            snapshots: self.snapshots.is_some(),
            coalescing: self.flights.is_some(),
            live_sharing: self.live.is_some(),
        }
    }
    /// Current active query groups and admitted consumers, without identifiers.
    pub fn flight_counts(&self) -> (usize, usize) {
        self.flights
            .as_ref()
            .map_or((0, 0), |flights| flights.counts())
    }
    /// Lost-feed/rebuild reset fences fills; primary hit validation still applies.
    pub fn invalidate_all(&self) {
        if let Some(cache) = &self.snapshots {
            if let Ok(mut cache) = cache.lock() {
                cache.invalidate_all();
            }
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute(
        self: &Arc<Self>,
        binding: &GraphqlBinding,
        inner: &Arc<NativeInner>,
        executor: &GraphqlExecutor,
        context: RequestContext,
        parts: Parts,
        value: serde_json::Value,
        permit: OwnedSemaphorePermit,
    ) -> Response {
        let freshness = match value["extensions"].get("gatewayFreshness") {
            Some(value) => match FreshnessContext::parse(value) {
                Ok(value) => Some(value),
                Err(_) => return response(StatusCode::BAD_REQUEST),
            },
            None => None,
        };
        let admission = match validate(binding, inner, executor, &context, &parts, &value).await {
            AdmissionResult::Eligible(admission) => admission,
            AdmissionResult::Bypass => {
                return binding
                    .execute_http(
                        inner,
                        executor,
                        context,
                        request(&parts, value),
                        Some(permit),
                    )
                    .await
            }
            AdmissionResult::Error(error) => return error,
        };
        let ticket = if let Some(cache) = &self.snapshots {
            let Ok(mut cache) = cache.lock() else {
                return response(StatusCode::SERVICE_UNAVAILABLE);
            };
            match cache.lookup(&admission, freshness.as_ref(), super::now()) {
                Ok(Some(hit)) => return cached_response(hit),
                Err(_) => return response(StatusCode::SERVICE_UNAVAILABLE),
                Ok(None) => {}
            }
            match cache.begin_fill(&admission, super::now()) {
                Ok(ticket) => Some(ticket),
                Err(_) => return response(StatusCode::SERVICE_UNAVAILABLE),
            }
        } else {
            None
        };
        let fill = Fill {
            binding: binding.clone(),
            inner: inner.clone(),
            executor: executor.clone(),
            context: context.clone(),
            parts: request(&parts, value.clone()).into_parts().0,
            value: value.clone(),
            admission: admission.clone(),
            freshness: freshness.clone(),
            ticket,
        };
        let result = if let Some(flights) = &self.flights {
            let key =
                match FlightKey::admitted(&admission, &value, freshness.as_ref(), super::now()) {
                    Ok(key) => key,
                    Err(_) => return response(StatusCode::BAD_REQUEST),
                };
            let owner = self.clone();
            match flights
                .execute(key, admission, freshness, move || async move {
                    owner.fill(fill).await
                })
                .await
            {
                Some(result) => result,
                None => {
                    return binding
                        .execute_http(
                            inner,
                            executor,
                            context,
                            request(&parts, value),
                            Some(permit),
                        )
                        .await
                }
            }
        } else {
            self.fill(fill).await
        };
        super::flight::with_permit(result, permit)
    }
    async fn fill(&self, fill: Fill) -> Response {
        let Fill {
            binding,
            inner,
            executor,
            context,
            parts,
            value,
            admission,
            freshness,
            ticket,
        } = fill;
        let mut execution = value.clone();
        mark(&mut execution, "snapshot");
        let result = binding
            .execute_http(
                &inner,
                &executor,
                context.clone(),
                request(&parts, execution),
                None,
            )
            .await;
        let (response_parts, body) = result.into_parts();
        let captured = match tokio::time::timeout(
            inner.options.limits.read_timeout,
            capture(body, self.entry_bytes),
        )
        .await
        {
            Ok(captured) => captured,
            Err(_) => return response(StatusCode::GATEWAY_TIMEOUT),
        };
        let body = match captured {
            Captured::Bytes(body) => body,
            Captured::Streaming(body) => return Response::from_parts(response_parts, body),
        };
        let headers = response_parts
            .headers
            .iter()
            .map(|(name, value)| {
                value
                    .to_str()
                    .map(|value| (name.to_string(), value.to_owned()))
            })
            .collect::<Result<Vec<_>, _>>();
        let Ok(headers) = headers else {
            return Response::from_parts(response_parts, Body::from(body));
        };
        let snapshot = SnapshotResponse {
            status: response_parts.status.as_u16(),
            headers,
            body: body.to_vec(),
        };
        if !snapshot.shareable(&admission, freshness.as_ref()) {
            return Response::from_parts(response_parts, Body::from(body));
        }
        // Scope/policy can change while result work is running. Authenticate
        // after the result too, including when only coalescing is selected.
        match validate(&binding, &inner, &executor, &context, &parts, &value).await {
            AdmissionResult::Eligible(current) => {
                if current.identity != admission.identity || current.key != admission.key {
                    return response(StatusCode::CONFLICT);
                }
                if let (Some(ticket), Some(cache)) = (ticket, &self.snapshots) {
                    let Ok(mut cache) = cache.lock() else {
                        return response(StatusCode::SERVICE_UNAVAILABLE);
                    };
                    if cache
                        .install(ticket, current, snapshot, super::now())
                        .is_err()
                    {
                        return response(StatusCode::SERVICE_UNAVAILABLE);
                    }
                }
            }
            AdmissionResult::Error(error) => return error,
            AdmissionResult::Bypass => {}
        }
        Response::from_parts(response_parts, Body::from(body))
    }
}

pub(super) enum AdmissionResult {
    Eligible(OriginAdmission),
    Bypass,
    Error(Response),
}
pub(super) async fn validate(
    binding: &GraphqlBinding,
    inner: &std::sync::Arc<NativeInner>,
    executor: &GraphqlExecutor,
    context: &RequestContext,
    parts: &Parts,
    value: &serde_json::Value,
) -> AdmissionResult {
    let mut validation = value.clone();
    mark(&mut validation, "validate");
    let pending = binding.execute_http(
        inner,
        executor,
        context.clone(),
        request(parts, validation),
        None,
    );
    let result =
        match tokio::time::timeout(inner.options.limits.response_header_timeout, pending).await {
            Ok(result) => result,
            Err(_) => return AdmissionResult::Error(response(StatusCode::GATEWAY_TIMEOUT)),
        };
    let (parts, body) = result.into_parts();
    let body = match tokio::time::timeout(
        inner.options.limits.read_timeout,
        axum::body::to_bytes(body, 65536),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(_)) => return AdmissionResult::Error(response(StatusCode::BAD_GATEWAY)),
        Err(_) => return AdmissionResult::Error(response(StatusCode::GATEWAY_TIMEOUT)),
    };
    if parts.status != StatusCode::OK {
        return AdmissionResult::Error(Response::from_parts(parts, Body::from(body)));
    }
    let parsed: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return AdmissionResult::Error(response(StatusCode::BAD_GATEWAY)),
    };
    if parsed
        .get("errors")
        .is_some_and(|e| e.as_array().is_none_or(|e| !e.is_empty()))
    {
        return AdmissionResult::Error(Response::from_parts(parts, Body::from(body)));
    }
    let delivery = &parsed["extensions"]["gatewayDelivery"];
    if delivery["eligible"] != true {
        return AdmissionResult::Bypass;
    }
    // Cookie-bearing validation is never a reusable authorization grant.
    if parts.headers.contains_key(header::SET_COOKIE) {
        return AdmissionResult::Bypass;
    }
    let admission: OriginAdmission = match serde_json::from_value(delivery["admission"].clone()) {
        Ok(value) => value,
        Err(_) => return AdmissionResult::Error(response(StatusCode::BAD_GATEWAY)),
    };
    if admission.bind(value, super::now()).is_err() {
        return AdmissionResult::Error(response(StatusCode::SERVICE_UNAVAILABLE));
    }
    AdmissionResult::Eligible(admission)
}
fn mark(value: &mut serde_json::Value, action: &str) {
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
pub(super) fn request(parts: &Parts, value: serde_json::Value) -> Request<Body> {
    let mut request = Request::new(Body::from(value.to_string()));
    *request.method_mut() = parts.method.clone();
    *request.uri_mut() = parts.uri.clone();
    *request.version_mut() = parts.version;
    *request.headers_mut() = parts.headers.clone();
    *request.extensions_mut() = parts.extensions.clone();
    request.headers_mut().remove(header::CONTENT_LENGTH);
    request.headers_mut().insert(
        header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    request
}
pub(super) fn cached_response(snapshot: SnapshotResponse) -> Response {
    let mut result = snapshot.body.into_response();
    *result.status_mut() = StatusCode::from_u16(snapshot.status).expect("validated status");
    result.headers_mut().clear();
    for (name, value) in snapshot.headers {
        if let (Ok(name), Ok(value)) = (
            name.parse::<header::HeaderName>(),
            value.parse::<axum::http::HeaderValue>(),
        ) {
            result.headers_mut().append(name, value);
        }
    }
    result
}
pub(super) enum Captured {
    Bytes(Bytes),
    Streaming(Body),
}
pub(super) async fn capture(body: Body, limit: usize) -> Captured {
    let mut stream = body.into_data_stream();
    let mut chunks = Vec::new();
    let mut bytes = 0;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(chunk) => {
                bytes += chunk.len();
                chunks.push(Ok(chunk));
            }
            Err(error) => {
                chunks.push(Err(error));
                return Captured::Streaming(Body::from_stream(
                    futures_util::stream::iter(chunks).chain(stream),
                ));
            }
        }
        if bytes > limit {
            return Captured::Streaming(Body::from_stream(
                futures_util::stream::iter(chunks).chain(stream),
            ));
        }
    }
    let mut result = Vec::with_capacity(bytes);
    for chunk in chunks {
        result.extend_from_slice(&chunk.expect("successful captured chunks"));
    }
    Captured::Bytes(result.into())
}
