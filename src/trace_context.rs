//! W3C trace-context metadata helpers.
//!
//! Distributed treats trace context as ordinary message/event metadata in the
//! default build. These helpers provide canonical keys and case-insensitive
//! extraction/injection without depending on an OpenTelemetry SDK.

use std::collections::HashMap;

#[cfg(feature = "otel")]
use opentelemetry::propagation::{Extractor, TextMapPropagator};

/// W3C Trace Context parent header / metadata key.
pub const TRACEPARENT: &str = "traceparent";
/// W3C Trace Context vendor state header / metadata key.
pub const TRACESTATE: &str = "tracestate";
/// Application workflow correlation metadata key.
pub const CORRELATION_ID: &str = "correlation_id";
/// Immediate cause metadata key.
pub const CAUSATION_ID: &str = "causation_id";

/// W3C trace context carried through Distributed metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraceContext {
    /// W3C `traceparent` value.
    pub traceparent: Option<String>,
    /// W3C `tracestate` value.
    pub tracestate: Option<String>,
}

impl TraceContext {
    /// Extract trace context from key/value metadata using case-insensitive keys.
    ///
    /// The first match per key wins. Metadata does not dedup keys on insert, so
    /// this must agree with every other reader — `Message::traceparent()` and
    /// OTel span-parent extraction are also first-match — or the value a service
    /// reports for itself and the value it propagates downstream can diverge.
    pub fn from_metadata<I, K, V>(metadata: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut trace_context = Self::default();
        for (key, value) in metadata {
            if trace_context.traceparent.is_none() && key.as_ref().eq_ignore_ascii_case(TRACEPARENT)
            {
                trace_context.traceparent = Some(value.as_ref().to_string());
            } else if trace_context.tracestate.is_none()
                && key.as_ref().eq_ignore_ascii_case(TRACESTATE)
            {
                trace_context.tracestate = Some(value.as_ref().to_string());
            }
        }
        trace_context
    }

    /// Return true when both trace context fields are absent.
    pub fn is_empty(&self) -> bool {
        self.traceparent.is_none() && self.tracestate.is_none()
    }

    /// Replace existing trace keys in a vector carrier and insert canonical keys.
    pub fn inject_vec(&self, metadata: &mut Vec<(String, String)>) {
        replace_vec_key(metadata, TRACEPARENT, self.traceparent.as_deref());
        replace_vec_key(metadata, TRACESTATE, self.tracestate.as_deref());
    }

    /// Replace existing trace keys in a map carrier and insert canonical keys.
    pub fn inject_map(&self, metadata: &mut HashMap<String, String>) {
        replace_map_key(metadata, TRACEPARENT, self.traceparent.as_deref());
        replace_map_key(metadata, TRACESTATE, self.tracestate.as_deref());
    }
}

/// Parent `span` on the message's W3C trace context — unless a local span is
/// already active, in which case the ambient span wins by design and `span`
/// nests under it via normal `tracing` parenting.
///
/// This is the single parenting rule for every framework span (HTTP/Knative
/// dispatch, transport receive, outbox publish): a caller that opened a span
/// before reaching the framework (an ingress middleware layer, an
/// `#[instrument]`ed wrapper) owns the hierarchy, and is expected to have
/// installed any remote context on that ambient span itself. Messages
/// dispatched outside any span join their producer's trace directly.
#[cfg(feature = "otel")]
pub(crate) fn set_span_parent_from_metadata_if_no_current_span(
    span: &tracing::Span,
    metadata: &[(String, String)],
) {
    if tracing::Span::current().id().is_none() {
        set_span_parent_from_metadata(span, metadata);
    }
}

#[cfg(feature = "otel")]
fn set_span_parent_from_metadata(span: &tracing::Span, metadata: &[(String, String)]) {
    use opentelemetry::trace::TraceContextExt as _;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    let parent_context = extract_otel_context_from_metadata(metadata);
    if parent_context.span().span_context().is_valid() {
        let _ = span.set_parent(parent_context);
    }
}

#[cfg(feature = "otel")]
fn extract_otel_context_from_metadata(metadata: &[(String, String)]) -> opentelemetry::Context {
    opentelemetry_sdk::propagation::TraceContextPropagator::new()
        .extract(&MetadataExtractor { metadata })
}

#[cfg(feature = "otel")]
struct MetadataExtractor<'a> {
    metadata: &'a [(String, String)],
}

#[cfg(feature = "otel")]
impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.metadata
            .iter()
            .find(|(existing, _)| existing.eq_ignore_ascii_case(key))
            .map(|(_, value)| value.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.metadata.iter().map(|(key, _)| key.as_str()).collect()
    }
}

fn replace_vec_key(metadata: &mut Vec<(String, String)>, key: &'static str, value: Option<&str>) {
    metadata.retain(|(existing, _)| !existing.eq_ignore_ascii_case(key));
    if let Some(value) = value {
        metadata.push((key.to_string(), value.to_string()));
    }
}

fn replace_map_key(metadata: &mut HashMap<String, String>, key: &'static str, value: Option<&str>) {
    metadata.retain(|existing, _| !existing.eq_ignore_ascii_case(key));
    if let Some(value) = value {
        metadata.insert(key.to_string(), value.to_string());
    }
}

/// Return true when a `traceparent` value matches the W3C version-00 shape.
///
/// This is intentionally a light validation helper. Core propagation preserves
/// incoming values unchanged; callers can opt in at ingress boundaries that want
/// to reject obviously malformed trace context.
pub fn is_valid_traceparent(value: &str) -> bool {
    let mut parts = value.split('-');
    let Some(version) = parts.next() else {
        return false;
    };
    let Some(trace_id) = parts.next() else {
        return false;
    };
    let Some(parent_id) = parts.next() else {
        return false;
    };
    let Some(flags) = parts.next() else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }

    version == "00"
        && trace_id.len() == 32
        && parent_id.len() == 16
        && flags.len() == 2
        && is_lower_hex(version)
        && is_lower_hex(trace_id)
        && is_lower_hex(parent_id)
        && is_lower_hex(flags)
        && trace_id.bytes().any(|b| b != b'0')
        && parent_id.bytes().any(|b| b != b'0')
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRACEPARENT_VALUE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn trace_context_extracts_case_insensitive_metadata() {
        let context = TraceContext::from_metadata([
            ("TraceParent", TRACEPARENT_VALUE),
            ("TRACESTATE", "vendor=value"),
        ]);

        assert_eq!(
            context,
            TraceContext {
                traceparent: Some(TRACEPARENT_VALUE.to_string()),
                tracestate: Some("vendor=value".to_string()),
            }
        );
    }

    #[test]
    fn trace_context_first_match_wins_on_duplicate_keys() {
        let context = TraceContext::from_metadata([
            ("traceparent", TRACEPARENT_VALUE),
            ("Traceparent", "00-later-duplicate-must-lose-01"),
            ("tracestate", "vendor=first"),
            ("TRACESTATE", "vendor=second"),
        ]);

        assert_eq!(
            context,
            TraceContext {
                traceparent: Some(TRACEPARENT_VALUE.to_string()),
                tracestate: Some("vendor=first".to_string()),
            }
        );
    }

    #[test]
    fn inject_vec_replaces_existing_trace_keys_with_canonical_keys() {
        let mut metadata = vec![
            ("TraceParent".to_string(), "old".to_string()),
            ("tenant".to_string(), "acme".to_string()),
        ];
        TraceContext {
            traceparent: Some(TRACEPARENT_VALUE.to_string()),
            tracestate: Some("vendor=value".to_string()),
        }
        .inject_vec(&mut metadata);

        assert_eq!(metadata[0], ("tenant".to_string(), "acme".to_string()));
        assert_eq!(
            metadata[1],
            (TRACEPARENT.to_string(), TRACEPARENT_VALUE.to_string())
        );
        assert_eq!(
            metadata[2],
            (TRACESTATE.to_string(), "vendor=value".to_string())
        );
    }

    #[test]
    fn inject_map_replaces_case_insensitive_trace_keys() {
        let mut metadata = HashMap::from([
            ("TraceParent".to_string(), "old".to_string()),
            ("tenant".to_string(), "acme".to_string()),
        ]);
        TraceContext {
            traceparent: Some(TRACEPARENT_VALUE.to_string()),
            tracestate: None,
        }
        .inject_map(&mut metadata);

        assert_eq!(
            metadata.get(TRACEPARENT).map(String::as_str),
            Some(TRACEPARENT_VALUE)
        );
        assert!(!metadata.contains_key("TraceParent"));
        assert_eq!(metadata.get("tenant").map(String::as_str), Some("acme"));
    }

    #[test]
    fn empty_context_removes_existing_trace_keys_when_injected() {
        let mut metadata = vec![
            ("traceparent".to_string(), TRACEPARENT_VALUE.to_string()),
            ("tracestate".to_string(), "vendor=value".to_string()),
        ];
        TraceContext::default().inject_vec(&mut metadata);

        assert!(metadata.is_empty());
    }

    #[test]
    fn traceparent_validation_accepts_the_w3c_shape_and_rejects_zero_ids() {
        struct Case {
            name: &'static str,
            traceparent: &'static str,
            valid: bool,
        }
        const CASES: &[Case] = &[
            Case {
                name: "the W3C shape",
                traceparent: TRACEPARENT_VALUE,
                valid: true,
            },
            Case {
                name: "an all-zero trace id",
                traceparent: "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
                valid: false,
            },
            Case {
                name: "an all-zero parent id",
                traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
                valid: false,
            },
        ];
        for case in CASES {
            assert_eq!(
                is_valid_traceparent(case.traceparent),
                case.valid,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn traceparent_validation_rejects_unsupported_versions() {
        assert!(!is_valid_traceparent(
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        ));
    }

    #[test]
    fn traceparent_validation_rejects_uppercase_hex() {
        assert!(!is_valid_traceparent(
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01"
        ));
    }

    #[cfg(feature = "otel")]
    #[test]
    fn extracts_otel_remote_parent_context_from_metadata() {
        use opentelemetry::trace::TraceContextExt as _;

        let metadata = vec![
            ("traceparent".to_string(), TRACEPARENT_VALUE.to_string()),
            ("tracestate".to_string(), "vendor=value".to_string()),
        ];

        let context = extract_otel_context_from_metadata(&metadata);
        let span_context = context.span().span_context().clone();

        assert!(span_context.is_valid());
        assert!(span_context.is_remote());
        assert_eq!(
            span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(span_context.span_id().to_string(), "00f067aa0ba902b7");
        assert!(span_context.is_sampled());
    }
}
