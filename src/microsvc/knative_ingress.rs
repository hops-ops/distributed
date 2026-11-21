//! Knative / CloudEvents HTTP ingress (microsvc side).
//!
//! Knative is *endpoint-driven*: the platform invokes an HTTP route, so this is
//! a separate ingress shape from the pull-based bus sources. The route parses a
//! CloudEvent (binary or structured HTTP mode), maps it to the canonical
//! [`Message`], and calls the same [`Service::dispatch_message`] boundary the
//! direct-transport runner uses.
//!
//! This lives on the **microsvc side**, not in the bus crate, because it is
//! intrinsically `Service`-coupled: it serializes the handler's returned `Value`
//! into the HTTP body and classifies a `HandlerError` to pick the status code —
//! neither of which the bus's `MessageRouter::dispatch` (unit-returning) exposes.
//!
//! A CloudEvent is an event unless it carries the `sourcedkind: command`
//! extension attribute `KnativeBus` stamps on the commands it sends. A typed
//! command delivered here runs through the command ledger as the transport
//! identity, with the CloudEvent `id` as the message id it deduplicates on.
//!
//! Acknowledgement is the HTTP response:
//!
//! - handler success → `200 OK` (the only ack);
//! - retryable failure → `503` so Knative redelivers per its Delivery config;
//! - permanent failure → `422` so Knative stops retrying / dead-letters.
//!
//! Retry, backoff, and dead-lettering are **platform-managed** by Knative
//! Eventing here — unlike direct transports, where the bus `FailurePolicy` owns
//! them. Requires the `http` feature.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use base64::Engine;
use serde_json::{json, Value};

use crate::bus::{validate_message_name, Message, MessageKind};
use crate::microsvc::{Service, MAX_HTTP_BODY_BYTES};
use crate::trace_context::{TraceContext, TRACEPARENT, TRACESTATE};

const STRUCTURED_CONTENT_TYPE: &str = "application/cloudevents+json";

/// Build an axum router exposing a CloudEvents ingress at `POST /` and the
/// per-type route `POST /cloudevent/{type}`.
///
/// Both dispatch by the parsed CloudEvent `type` (the path segment is for routing
/// alignment only), so a Knative Trigger can target either a single shared `ref`
/// (`/`) or the per-type subscriber URI `KnativeBus` emits (`/cloudevent/<type>`).
pub fn cloud_events_router(service: Arc<Service>) -> Router {
    let router = Router::new()
        .route("/", axum::routing::post(ingress_handler))
        .route("/cloudevent/{type}", axum::routing::post(ingress_handler));
    #[cfg(feature = "metrics")]
    let router = router.route("/metrics", axum::routing::get(metrics_handler));

    router
        // Pin the body limit explicitly rather than relying on axum's default,
        // since the handler buffers the whole body into memory.
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .with_state(service)
}

#[cfg(feature = "metrics")]
async fn metrics_handler(State(service): State<Arc<Service>>) -> impl IntoResponse {
    crate::metrics::prometheus_response(service.name())
}

async fn ingress_handler(
    State(service): State<Arc<Service>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let message = match parse_cloud_event(&headers, &body) {
        Ok(message) => message,
        Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
    };

    match service.dispatch_message(&message).await {
        Ok(value) => (StatusCode::OK, Json(value)).into_response(),
        Err(err) => {
            // Map the retryable/permanent classification onto HTTP so Knative's
            // platform-managed retry/DLQ does the right thing.
            let status = if err.transport_error_kind().is_retryable() {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::UNPROCESSABLE_ENTITY
            };
            // Mask internal faults (the same redaction the HTTP ingress applies):
            // `err.to_string()` can carry SQL/driver/path detail. Log the real
            // error server-side; return only a safe message on the wire.
            if err.status_code() >= 500 {
                eprintln!("knative ingress `{}` failed: {err}", message.name());
            }
            (
                status,
                Json(json!({ "error": err.client_facing_message() })),
            )
                .into_response()
        }
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

/// Parse a CloudEvent in binary or structured HTTP mode into a [`Message`].
fn parse_cloud_event(headers: &HeaderMap, body: &Bytes) -> Result<Message, String> {
    let content_type = header(headers, "content-type").unwrap_or("");
    let mut message = if content_type.starts_with(STRUCTURED_CONTENT_TYPE) {
        parse_structured(body)
    } else {
        parse_binary(headers, body)
    }?;
    // `KnativeBus` stamps the kind as the `sourcedkind` extension attribute.
    // Anything but `command` is an event, as on every other transport.
    if let Some(kind) = message.metadata("sourcedkind") {
        message.kind = MessageKind::from_str_lossy(kind);
    }
    inject_http_trace_context(headers, &mut message.metadata);
    Ok(message)
}

fn inject_http_trace_context(headers: &HeaderMap, metadata: &mut Vec<(String, String)>) {
    let context = TraceContext {
        traceparent: header(headers, TRACEPARENT).map(str::to_string),
        tracestate: header(headers, TRACESTATE).map(str::to_string),
    };
    // W3C tracestate is only meaningful alongside a traceparent, so HTTP
    // headers win only when they carry one. A tracestate-only header set must
    // not disturb trace context the message already carries (e.g. a
    // ce-traceparent extension stored as `traceparent` metadata).
    if context.traceparent.is_some() {
        context.inject_vec(metadata);
    }
}

/// Binary mode: attributes are `ce-*` headers, the body is the data.
fn parse_binary(headers: &HeaderMap, body: &Bytes) -> Result<Message, String> {
    let id = header(headers, "ce-id").ok_or("missing ce-id header")?;
    let name = header(headers, "ce-type").ok_or("missing ce-type header")?;
    // `ce-type` is attacker-controlled wire input that becomes Message.name and,
    // downstream, a broker routing identifier — validate before trusting it.
    validate_message_name(name).map_err(|e| format!("invalid ce-type: {e}"))?;
    let content_type = header(headers, "content-type")
        .unwrap_or("application/json")
        .to_string();

    let mut metadata = Vec::new();
    for (key, value) in headers.iter() {
        let key = key.as_str();
        if let Some(attr) = key.strip_prefix("ce-") {
            if attr == "id" || attr == "type" {
                continue;
            }
            if let Ok(value) = value.to_str() {
                metadata.push((attr.to_string(), value.to_string()));
            }
        }
    }

    Ok(Message {
        id: Some(id.to_string()),
        name: name.to_string(),
        kind: MessageKind::Event,
        payload: body.to_vec(),
        content_type,
        metadata,
    })
}

/// Structured mode: a single `application/cloudevents+json` document.
fn parse_structured(body: &Bytes) -> Result<Message, String> {
    let event: Value =
        serde_json::from_slice(body).map_err(|e| format!("invalid cloudevents+json: {e}"))?;
    let object = event
        .as_object()
        .ok_or("cloudevent must be a JSON object")?;

    let id = object
        .get("id")
        .and_then(Value::as_str)
        .ok_or("missing cloudevent id")?
        .to_string();
    let name = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or("missing cloudevent type")?
        .to_string();
    // Same wire-input check as binary mode: `type` becomes Message.name.
    validate_message_name(&name).map_err(|e| format!("invalid cloudevent type: {e}"))?;
    let content_type = object
        .get("datacontenttype")
        .and_then(Value::as_str)
        .unwrap_or("application/json")
        .to_string();

    // Data: `data` (any JSON; objects/arrays re-serialized to bytes) or
    // `data_base64` (binary).
    let payload = if let Some(data) = object.get("data") {
        match data {
            Value::String(s) => s.clone().into_bytes(),
            other => serde_json::to_vec(other).map_err(|e| format!("invalid data: {e}"))?,
        }
    } else if let Some(Value::String(b64)) = object.get("data_base64") {
        base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| format!("invalid data_base64: {e}"))?
    } else {
        Vec::new()
    };

    // Remaining attributes (source, subject, extensions) become metadata.
    let reserved = [
        "specversion",
        "id",
        "type",
        "datacontenttype",
        "data",
        "data_base64",
    ];
    let mut metadata = Vec::new();
    for (key, value) in object {
        if reserved.contains(&key.as_str()) {
            continue;
        }
        let value = match value {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        metadata.push((key.clone(), value));
    }

    Ok(Message {
        id: Some(id),
        name,
        kind: MessageKind::Event,
        payload,
        content_type,
        metadata,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            map.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        map
    }

    #[test]
    fn parses_binary_cloud_event() {
        let h = headers(&[
            ("ce-id", "evt-1"),
            ("ce-type", "order.created"),
            ("ce-source", "/orders"),
            ("content-type", "application/json"),
        ]);
        let body = Bytes::from_static(br#"{"order":"o1"}"#);
        let message = parse_cloud_event(&h, &body).unwrap();
        assert_eq!(message.id(), Some("evt-1"));
        assert_eq!(message.name(), "order.created");
        assert_eq!(message.payload(), br#"{"order":"o1"}"#);
        assert_eq!(message.metadata("source"), Some("/orders"));
    }

    #[test]
    fn binary_cloud_event_preserves_w3c_trace_headers() {
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let h = headers(&[
            ("ce-id", "evt-1"),
            ("ce-type", "order.created"),
            ("ce-traceparent", "old-extension-value"),
            ("traceparent", traceparent),
            ("tracestate", "vendor=value"),
            ("content-type", "application/json"),
        ]);
        let body = Bytes::from_static(br#"{"order":"o1"}"#);

        let message = parse_cloud_event(&h, &body).unwrap();

        assert_eq!(message.traceparent(), Some(traceparent));
        assert_eq!(message.tracestate(), Some("vendor=value"));
        assert_eq!(
            message
                .metadata
                .iter()
                .filter(|(key, _)| key.eq_ignore_ascii_case("traceparent"))
                .count(),
            1
        );
    }

    #[test]
    fn tracestate_only_headers_preserve_existing_trace_extensions() {
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let h = headers(&[
            ("ce-id", "evt-1"),
            ("ce-type", "order.created"),
            ("ce-traceparent", traceparent),
            ("tracestate", "vendor=value"),
            ("content-type", "application/json"),
        ]);
        let body = Bytes::from_static(br#"{"order":"o1"}"#);

        let message = parse_cloud_event(&h, &body).unwrap();

        // Without an HTTP traceparent, headers must not disturb the trace
        // context the event itself carries.
        assert_eq!(message.traceparent(), Some(traceparent));
    }

    #[test]
    fn parses_structured_cloud_event() {
        let h = headers(&[("content-type", "application/cloudevents+json")]);
        let body = Bytes::from(
            json!({
                "specversion": "1.0",
                "id": "evt-2",
                "type": "order.created",
                "source": "/orders",
                "datacontenttype": "application/json",
                "data": {"order": "o2"},
            })
            .to_string(),
        );
        let message = parse_cloud_event(&h, &body).unwrap();
        assert_eq!(message.id(), Some("evt-2"));
        assert_eq!(message.name(), "order.created");
        assert_eq!(message.payload(), br#"{"order":"o2"}"#);
        assert_eq!(message.metadata("source"), Some("/orders"));
    }

    #[test]
    fn structured_cloud_event_prefers_http_trace_headers_over_extensions() {
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let h = headers(&[
            ("content-type", "application/cloudevents+json"),
            ("traceparent", traceparent),
        ]);
        let body = Bytes::from(
            json!({
                "specversion": "1.0",
                "id": "evt-2",
                "type": "order.created",
                "traceparent": "old-extension-value",
                "data": {"order": "o2"},
            })
            .to_string(),
        );

        let message = parse_cloud_event(&h, &body).unwrap();

        assert_eq!(message.traceparent(), Some(traceparent));
    }

    #[test]
    fn structured_cloud_event_preserves_trace_extensions_without_http_headers() {
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let h = headers(&[("content-type", "application/cloudevents+json")]);
        let body = Bytes::from(
            json!({
                "specversion": "1.0",
                "id": "evt-2",
                "type": "order.created",
                "traceparent": traceparent,
                "tracestate": "vendor=value",
                "data": {"order": "o2"},
            })
            .to_string(),
        );

        let message = parse_cloud_event(&h, &body).unwrap();

        assert_eq!(message.traceparent(), Some(traceparent));
        assert_eq!(message.tracestate(), Some("vendor=value"));
    }

    #[test]
    fn missing_id_is_rejected() {
        let h = headers(&[("ce-type", "order.created")]);
        assert!(parse_cloud_event(&h, &Bytes::new()).is_err());
    }

    #[test]
    fn binary_wildcard_ce_type_is_rejected() {
        // A `ce-type` carrying a broker wildcard must not become a routing key.
        let h = headers(&[("ce-id", "evt-1"), ("ce-type", "order.*")]);
        let body = Bytes::from_static(br#"{}"#);
        let err = parse_cloud_event(&h, &body).unwrap_err();
        assert!(err.contains("invalid ce-type"), "got {err}");
    }

    #[test]
    fn structured_wildcard_type_is_rejected() {
        let h = headers(&[("content-type", "application/cloudevents+json")]);
        let body = Bytes::from(
            json!({
                "specversion": "1.0",
                "id": "evt-2",
                "type": "orders>",
                "source": "/orders",
            })
            .to_string(),
        );
        let err = parse_cloud_event(&h, &body).unwrap_err();
        assert!(err.contains("invalid cloudevent type"), "got {err}");
    }
}
