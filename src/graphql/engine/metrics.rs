use super::*;

pub(crate) fn protocol_internal_error_response() -> Response {
    Response::from_errors(vec![ServerError::new(
        "internal protocol response error",
        None,
    )])
}

pub(crate) fn attach_protocol_response(
    mut response: Response,
    accumulator: Option<&ProtocolResponseAccumulator>,
) -> Response {
    let Some(accumulator) = accumulator else {
        return response;
    };
    if accumulator.attach(&mut response).is_ok() {
        return response;
    }

    // A resolver cannot shadow the framework-owned extension. Replace a
    // colliding response with a closed internal error and attach the one
    // authoritative envelope.
    let mut failure = protocol_internal_error_response();
    if accumulator.attach(&mut failure).is_ok() {
        failure
    } else {
        protocol_internal_error_response()
    }
}

/// Map GraphQL response errors to coarse metric `status` labels.
///
/// Privacy: only stable class names (`ok`, `timeout`, `bad_request`,
/// `forbidden`, `internal`, `error`) — never user/tenant/SQL text.
pub(crate) fn metrics_status_for_response(response: &Response) -> &'static str {
    if !response.is_err() {
        return "ok";
    }
    for err in &response.errors {
        if let Some(ext) = &err.extensions {
            if let Some(code) = ext.get("code") {
                let code = format!("{code:?}").to_ascii_uppercase();
                if code.contains("TIMEOUT") {
                    return "timeout";
                }
                if code.contains("BAD_REQUEST") {
                    return "bad_request";
                }
                if code.contains("FORBIDDEN") {
                    return "forbidden";
                }
                if code.contains("INTERNAL") {
                    return "internal";
                }
            }
        }
        let msg = err.message.to_ascii_lowercase();
        if msg.contains("timeout") {
            return "timeout";
        }
        if msg.contains("not configured") || msg.contains("forbidden") {
            return "forbidden";
        }
    }
    "error"
}

pub(crate) fn record_metrics(root_field: &str, status: &str, duration: Duration) {
    #[cfg(feature = "metrics")]
    crate::metrics::record_graphql_request(None, root_field, status, duration);
    #[cfg(not(feature = "metrics"))]
    let _ = (root_field, status, duration);
}

#[cfg(test)]
mod metrics_status_tests {
    use super::metrics_status_for_response;
    use async_graphql::{ErrorExtensionValues, Response, ServerError};

    fn response_with_code(code: &str, message: &str) -> Response {
        let mut err = ServerError::new(message, None);
        let mut ext = ErrorExtensionValues::default();
        ext.set("code", code);
        err.extensions = Some(ext);
        Response::from_errors(vec![err])
    }

    #[test]
    fn ok_when_no_errors() {
        let resp = Response::new(async_graphql::Value::Null);
        assert_eq!(metrics_status_for_response(&resp), "ok");
    }

    #[test]
    fn maps_extension_codes() {
        assert_eq!(
            metrics_status_for_response(&response_with_code("TIMEOUT", "statement timeout")),
            "timeout"
        );
        assert_eq!(
            metrics_status_for_response(&response_with_code("BAD_REQUEST", "bad request")),
            "bad_request"
        );
        assert_eq!(
            metrics_status_for_response(&response_with_code("INTERNAL", "internal error")),
            "internal"
        );
        assert_eq!(
            metrics_status_for_response(&response_with_code("FORBIDDEN", "nope")),
            "forbidden"
        );
    }

    #[test]
    fn maps_message_fallback_timeout() {
        let resp = Response::from_errors(vec![ServerError::new("statement timeout", None)]);
        assert_eq!(metrics_status_for_response(&resp), "timeout");
    }
}
