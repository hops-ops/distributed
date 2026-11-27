use super::{RequestContext, WorkerOptions};
use crate::gateway::{append_hop, BackendCredential, Forwarding, HOPS_HEADER};
use futures_util::{
    future::{select, Either},
    StreamExt,
};
use std::{cell::Cell, future::Future, rc::Rc};
use worker::{
    AbortController, Fetch, Headers, Request, RequestInit, RequestRedirect, Response, Result,
    WebSocketPair,
};

pub(super) async fn timeout<T>(
    milliseconds: u64,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    match select(
        Box::pin(future),
        Box::pin(super::timer::Timer::new(milliseconds)),
    )
    .await
    {
        Either::Left((result, _)) => result,
        Either::Right(_) => Err(worker::Error::RustError("gateway deadline exceeded".into())),
    }
}
struct AbortOnDrop(Option<AbortController>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(controller) = self.0.take() {
            controller.abort();
        }
    }
}
pub(super) fn is_upgrade(request: &Request) -> Result<bool> {
    Ok(request
        .headers()
        .get("upgrade")?
        .is_some_and(|value| value.eq_ignore_ascii_case("websocket")))
}
pub(super) fn prepare_headers(
    headers: &Headers,
    forwarding: Forwarding,
    options: &WorkerOptions,
    context: &RequestContext,
    upgrade: bool,
) -> Result<()> {
    let nominated = headers.get("connection")?.unwrap_or_default();
    for name in nominated
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        headers.delete(name)?;
    }
    for name in headers.keys().collect::<Vec<_>>() {
        if !forwarding.forwards(&name, &options.strip_headers) {
            headers.delete(&name)?;
        }
    }
    if let (true, BackendCredential::Bearer(token)) = (
        forwarding.sends_backend_credential(),
        context.backend_credential(),
    ) {
        headers.set("authorization", &format!("Bearer {token}"))?;
    }
    let public = worker::Url::parse(&options.public_origin)?;
    let authority = &public[url::Position::BeforeHost..url::Position::AfterPort];
    headers.set("x-forwarded-host", authority)?;
    headers.set("x-forwarded-proto", public.scheme())?;
    // Workers require Host to agree with the fetched URL. Trusted public origin
    // is therefore carried in forwarded headers for delegated auth handlers.
    if upgrade {
        headers.set("upgrade", "websocket")?;
    }
    headers.set(HOPS_HEADER, &hop_chain(headers, options)?)?;
    Ok(())
}
/// The chain after this gateway; a loop or over-long chain fails.
pub(super) fn hop_chain(headers: &Headers, options: &WorkerOptions) -> Result<String> {
    append_hop(headers.get(HOPS_HEADER)?.as_deref(), &options.hop_id)
        .map_err(|error| worker::Error::RustError(error.0.into()))
}
pub(super) async fn read_request(request: &mut Request, max: usize) -> Result<Vec<u8>> {
    if request
        .headers()
        .get("content-length")?
        .and_then(|s| s.parse::<usize>().ok())
        .is_some_and(|n| n > max)
    {
        return Err(worker::Error::RustError("request too large".into()));
    }
    if request.inner().body().is_none() {
        return Ok(Vec::new());
    }
    let mut stream = request.stream()?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if bytes.len().saturating_add(chunk.len()) > max {
            return Err(worker::Error::RustError("request too large".into()));
        }
        bytes.extend(chunk);
    }
    Ok(bytes)
}
/// The request a local handler or asset binding receives: the client's
/// request under the same header rule as a proxied one.
pub(super) fn prepared(
    request: Request,
    forwarding: Forwarding,
    options: &WorkerOptions,
    context: &RequestContext,
) -> Result<Request> {
    let headers = copy_headers(request.headers())?;
    prepare_headers(&headers, forwarding, options, context, false)?;
    let init = worker::web_sys::RequestInit::new();
    init.set_headers(&headers.0);
    Ok(worker::web_sys::Request::new_with_request_and_init(request.inner(), &init)?.into())
}
pub(super) fn with_body(request: &Request, bytes: Vec<u8>) -> Result<Request> {
    let mut init = RequestInit::new();
    init.method = request.method();
    init.headers = copy_headers(request.headers())?;
    // The body may differ from the client's; the runtime states its length.
    init.headers.delete("content-length")?;
    init.redirect = RequestRedirect::Manual;
    init.body = Some(worker::js_sys::Uint8Array::from(bytes.as_slice()).into());
    super::cancellation::preserve_signal(
        Request::new_with_init(request.url()?.as_str(), &init)?,
        &request.inner().signal(),
    )
}
pub(super) async fn forward(
    mut request: Request,
    origin: &str,
    path: Option<&str>,
    allow_upgrade: bool,
    forwarding: Forwarding,
    options: &WorkerOptions,
    context: &RequestContext,
) -> Result<Response> {
    let upgrade = is_upgrade(&request)?;
    if upgrade && !allow_upgrade {
        return Response::error("upgrade disabled", 400);
    }
    if request
        .headers()
        .get("content-length")?
        .and_then(|s| s.parse::<usize>().ok())
        .is_some_and(|n| n > options.limits.request_bytes)
    {
        return Response::error("request too large", 413);
    }
    let source = request.url()?;
    // A GraphQL operation carries nothing in the query string, so nothing a
    // client writes there reaches the executor.
    let query = match forwarding {
        Forwarding::Graphql => None,
        Forwarding::Application | Forwarding::Zones => source.query(),
    };
    let url = format!(
        "{}{}{}",
        origin.trim_end_matches('/'),
        path.unwrap_or(source.path()),
        query.map(|q| format!("?{q}")).unwrap_or_default()
    );
    let headers = copy_headers(request.headers())?;
    if prepare_headers(&headers, forwarding, options, context, upgrade).is_err() {
        return Response::error("invalid proxy request", 400);
    }
    let mut init = RequestInit::new();
    init.method = request.method();
    init.headers = headers;
    init.redirect = RequestRedirect::Manual;
    init.cache = Some(worker::CacheMode::NoStore);
    let oversized = Rc::new(Cell::new(false));
    if request.inner().body().is_some() {
        let oversized = oversized.clone();
        let max = options.limits.request_bytes;
        let stream = request.stream()?;
        let bounded =
            futures_util::stream::try_unfold((stream, 0usize), move |(mut stream, total)| {
                let oversized = oversized.clone();
                async move {
                    match stream.next().await {
                        Some(chunk) => {
                            let chunk = chunk?;
                            let total = total.saturating_add(chunk.len());
                            if total > max {
                                oversized.set(true);
                                return Err(worker::Error::RustError("request too large".into()));
                            }
                            Ok(Some((chunk, (stream, total))))
                        }
                        None => Ok(None),
                    }
                }
            });
        let body: worker::web_sys::Response = Response::from_stream(bounded)?.into();
        init.body = body.body().map(Into::into);
    }
    let outbound = Request::new_with_init(&url, &init)?;
    let controller = AbortController::default();
    let signal = controller.signal();
    let guard = AbortOnDrop(Some(controller));
    let mut response = match timeout(
        options.limits.header_timeout_ms,
        Fetch::Request(outbound).send_with_signal(&signal),
    )
    .await
    {
        Ok(response) => response,
        Err(_) if oversized.get() => return Response::error("request too large", 413),
        Err(_) => return Response::error("origin unavailable", 502),
    };
    if oversized.get() {
        return Response::error("request too large", 413);
    }
    let mutable_headers = copy_headers(response.headers())?;
    response = response.with_headers(mutable_headers);
    rewrite_redirect(&mut response, origin, &options.public_origin)?;
    if response.status_code() == 101 {
        if !allow_upgrade {
            return Response::error("unexpected upgrade", 502);
        }
        let headers = copy_headers(response.headers())?;
        let Some(origin) = response.websocket() else {
            return Response::error("invalid upgrade", 502);
        };
        let pair = WebSocketPair::new()?;
        let client = pair.client;
        let lifetime = context
            .connection_lifetime(
                std::time::Duration::from_millis(options.limits.websocket_lifetime_ms),
                super::now(),
            )
            .as_millis() as u64;
        let max = options.limits.websocket_buffer_bytes;
        worker::wasm_bindgen_futures::spawn_local(async move {
            let _guard = guard;
            let _ = timeout(
                lifetime,
                super::raw_socket::bridge(pair.server, origin, max),
            )
            .await;
        });
        return Ok(Response::from_websocket(client)?.with_headers(headers));
    }
    if request.method() == worker::Method::Head || matches!(response.status_code(), 204 | 304) {
        return Ok(response);
    }
    let headers = copy_headers(response.headers())?;
    let status = response.status_code();
    let encode = *response.encode_body();
    if !matches!(response.body(), worker::ResponseBody::Stream(_)) {
        return Ok(response);
    }
    let stream = response.stream()?;
    let idle = options.limits.read_timeout_ms;
    let bounded =
        futures_util::stream::try_unfold((stream, guard), move |(mut stream, guard)| async move {
            let next = timeout(idle, async { stream.next().await.transpose() }).await?;
            Ok::<_, worker::Error>(next.map(|chunk| (chunk, (stream, guard))))
        });
    Ok(Response::from_stream(bounded)?
        .with_status(status)
        .with_headers(headers)
        .with_encode_body(encode))
}
fn rewrite_redirect(response: &mut Response, origin: &str, public: &str) -> Result<()> {
    if let Some(location) = response.headers().get("location")? {
        if let Ok(url) = worker::Url::parse(&location) {
            if url.origin() == worker::Url::parse(origin)?.origin() {
                response.headers_mut().set(
                    "location",
                    &format!(
                        "{}{}",
                        public.trim_end_matches('/'),
                        &url[url::Position::BeforePath..]
                    ),
                )?;
            }
        }
    }
    Ok(())
}
pub(super) fn copy_headers(headers: &Headers) -> Result<Headers> {
    Ok(Headers(worker::web_sys::Headers::new_with_headers(
        &headers.0,
    )?))
}
