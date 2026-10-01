use super::{
    response, BackendCredential, Body, HeaderMap, HeaderValue, NativeInner, Request,
    RequestContext, Response, StatusCode, Url,
};
use axum::http::header;
use futures_util::{Stream, StreamExt};
use std::{io, time::Duration};

pub(super) fn strip_hop_headers(headers: &mut HeaderMap) {
    let named: Vec<_> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|v| v.trim().to_owned())
        .collect();
    for name in named {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}

pub(super) fn prepare_headers(
    headers: &mut HeaderMap,
    inner: &NativeInner,
    context: &RequestContext,
    upgrade: bool,
) -> Result<(), ()> {
    strip_hop_headers(headers);
    let remove: Vec<_> = headers
        .keys()
        .filter(|name| {
            super::super::is_untrusted_identity_header(name.as_str())
                || inner
                    .options
                    .strip_headers
                    .iter()
                    .any(|s| s.eq_ignore_ascii_case(name.as_str()))
        })
        .cloned()
        .collect();
    for name in remove {
        headers.remove(name);
    }
    headers.remove(header::AUTHORIZATION);
    if let BackendCredential::Bearer(token) = context.backend_credential() {
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| ())?,
        );
    }
    let authority = &inner.origin[url::Position::BeforeHost..url::Position::AfterPort];
    let host = HeaderValue::from_str(authority).map_err(|_| ())?;
    headers.insert(header::HOST, host.clone());
    headers.insert("x-forwarded-host", host);
    headers.insert(
        "x-forwarded-proto",
        HeaderValue::from_str(inner.origin.scheme()).map_err(|_| ())?,
    );
    if upgrade {
        headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
        headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    }
    Ok(())
}

pub(super) async fn forward(
    inner: &NativeInner,
    origin: &str,
    allow_websocket: bool,
    context: RequestContext,
    request: Request<Body>,
) -> Response {
    let permit = match inner.permits.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return response(StatusCode::SERVICE_UNAVAILABLE),
    };
    forward_with_permit(
        inner,
        origin,
        allow_websocket,
        context,
        request,
        Some(permit),
    )
    .await
}

pub(super) async fn forward_with_permit(
    inner: &NativeInner,
    origin: &str,
    allow_websocket: bool,
    context: RequestContext,
    mut request: Request<Body>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Response {
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .is_some_and(|v| {
            v.to_str()
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .is_none_or(|n| n > inner.options.limits.request_body_bytes as u64)
        })
    {
        return response(StatusCode::PAYLOAD_TOO_LARGE);
    }
    let wants_upgrade = request.headers().contains_key(header::UPGRADE);
    if wants_upgrade
        && (!allow_websocket
            || request.method() != "GET"
            || !request.headers()[header::UPGRADE]
                .as_bytes()
                .eq_ignore_ascii_case(b"websocket"))
    {
        return response(StatusCode::BAD_REQUEST);
    }
    let on_upgrade = wants_upgrade.then(|| hyper::upgrade::on(&mut request));
    if prepare_headers(request.headers_mut(), inner, &context, wants_upgrade).is_err() {
        return response(StatusCode::BAD_REQUEST);
    }
    if add_hop(request.headers_mut(), inner).is_err() {
        return response(StatusCode::BAD_REQUEST);
    }
    let target = request.uri().path_and_query().map_or("/", |p| p.as_str());
    // Path ownership already rejected traversal/authority aliases. A configured
    // origin is concatenated with origin-form target, never joined to a URL.
    let url = format!("{}{target}", origin.trim_end_matches('/'));
    let (parts, body) = request.into_parts();
    let max = inner.options.limits.request_body_bytes;
    let mut read = 0usize;
    let body = body.into_data_stream().map(move |chunk| {
        let chunk = chunk.map_err(io::Error::other)?;
        read = read
            .checked_add(chunk.len())
            .ok_or_else(|| io::Error::other("request body limit"))?;
        if read > max {
            return Err(io::Error::other("request body limit"));
        }
        Ok(chunk)
    });
    let pending = inner
        .client
        .request(parts.method, url)
        .headers(parts.headers)
        .body(reqwest::Body::wrap_stream(body))
        .send();
    let upstream =
        match tokio::time::timeout(inner.options.limits.response_header_timeout, pending).await {
            Ok(Ok(upstream)) => upstream,
            Err(_) => return response(StatusCode::GATEWAY_TIMEOUT),
            _ => return response(StatusCode::BAD_GATEWAY),
        };
    let status = upstream.status();
    let mut headers = upstream.headers().clone();
    strip_hop_headers(&mut headers);
    if let Some(location) = headers.get(header::LOCATION).and_then(|v| v.to_str().ok()) {
        if let (Ok(location), Ok(upstream_origin)) = (
            Url::parse(origin).and_then(|origin| origin.join(location)),
            Url::parse(origin),
        ) {
            if location.origin() == upstream_origin.origin() {
                let suffix = &location[url::Position::BeforePath..];
                if let Ok(location) = HeaderValue::from_str(&format!(
                    "{}{suffix}",
                    inner.options.public_origin.trim_end_matches('/')
                )) {
                    headers.insert(header::LOCATION, location);
                }
            }
        }
    }
    if status == StatusCode::SWITCHING_PROTOCOLS {
        let Some(on_upgrade) = on_upgrade else {
            return response(StatusCode::BAD_GATEWAY);
        };
        if !upstream
            .headers()
            .get(header::UPGRADE)
            .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"websocket"))
        {
            return response(StatusCode::BAD_GATEWAY);
        }
        headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
        headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        let lifetime =
            context
                .identity()
                .map_or(inner.options.limits.upgrade_lifetime, |identity| {
                    inner
                        .options
                        .limits
                        .upgrade_lifetime
                        .min(Duration::from_secs(
                            identity.expires_at().saturating_sub(super::now()),
                        ))
                });
        tokio::spawn(async move {
            let _permit = permit;
            let _ = tokio::time::timeout(lifetime, async move {
                let (Ok(downstream), Ok(mut upstream)) =
                    tokio::join!(on_upgrade, upstream.upgrade())
                else {
                    return;
                };
                let mut downstream = hyper_util::rt::TokioIo::new(downstream);
                let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
            })
            .await;
        });
        let mut result = response(status);
        *result.headers_mut() = headers;
        return result;
    }
    // Dropping the downstream body drops both upstream body and capacity permit.
    // No detached response pump keeps reading after the client disconnects.
    let mut stream = Box::pin(upstream.bytes_stream());
    let body = futures_util::stream::poll_fn(move |cx| {
        let _permit = &permit;
        stream.as_mut().poll_next(cx)
    });
    let mut result = Response::new(Body::from_stream(body));
    *result.status_mut() = status;
    *result.headers_mut() = headers;
    result
}

pub(super) fn add_hop(headers: &mut HeaderMap, inner: &NativeInner) -> Result<(), ()> {
    let hops = headers
        .get("x-distributed-gateway-hops")
        .and_then(|v| v.to_str().ok())
        .map_or_else(
            || inner.hop_id.clone(),
            |previous| format!("{previous},{}", inner.hop_id),
        );
    if hops.len() > 512 {
        return Err(());
    }
    headers.insert(
        "x-distributed-gateway-hops",
        HeaderValue::from_str(&hops).map_err(|_| ())?,
    );
    Ok(())
}
