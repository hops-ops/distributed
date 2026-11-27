//! The gateway's side of the origin delivery control protocol: how it marks
//! requests it sends, and what an origin validation answer permits.
use super::{DeliveryError, OriginAdmission};
use serde_json::Value;

/// Largest origin validation answer read, in bytes.
pub const MAX_VALIDATION_BYTES: usize = 64 * 1024;

/// Write the gateway's control extension on a request it sends to the
/// origin. Only `connectionInit`, which gateway code itself recorded for a
/// WebSocket consumer, survives; clients never write this extension.
pub fn mark(request: &mut Value, action: &str) {
    let init = request["extensions"]["gatewayDelivery"]
        .get("connectionInit")
        .cloned();
    set(request, action, init);
}

/// Mark a WebSocket consumer's operation for execution under the consumer's
/// own `connection_init` payload.
pub fn mark_connection(request: &mut Value, init: Value) {
    set(request, "execute", Some(init));
}

fn set(request: &mut Value, action: &str, init: Option<Value>) {
    if !request["extensions"].is_object() {
        request["extensions"] = serde_json::json!({});
    }
    request["extensions"]["gatewayDelivery"] = serde_json::json!({ "action": action });
    if let Some(init) = init {
        request["extensions"]["gatewayDelivery"]["connectionInit"] = init;
    }
}

/// What an origin's answer to a `validate` request permits.
// Returned once per validation and matched at once; boxing buys nothing.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum OriginVerdict {
    /// Reuse is permitted under this fresh admission, bound to the request.
    Eligible(OriginAdmission),
    /// The origin does not offer reuse: execute this consumer on its own.
    Bypass,
    /// The origin refused the operation: relay its answer unchanged.
    Reject,
    /// The answer is not a usable validation (502).
    Malformed,
    /// The admission does not bind this request now (503).
    Unbound(DeliveryError),
}

/// Decide from a complete validation answer. A cookie-setting answer is
/// never a reusable authorization grant.
pub fn verdict(
    status: u16,
    sets_cookie: bool,
    body: &[u8],
    request: &Value,
    now: u64,
) -> OriginVerdict {
    if status != 200 {
        return OriginVerdict::Reject;
    }
    let Ok(parsed) = serde_json::from_slice::<Value>(body) else {
        return OriginVerdict::Malformed;
    };
    if parsed
        .get("errors")
        .is_some_and(|errors| errors.as_array().is_none_or(|errors| !errors.is_empty()))
    {
        return OriginVerdict::Reject;
    }
    let delivery = &parsed["extensions"]["gatewayDelivery"];
    if delivery["eligible"] != true || sets_cookie {
        return OriginVerdict::Bypass;
    }
    let Ok(admission) = serde_json::from_value::<OriginAdmission>(delivery["admission"].clone())
    else {
        return OriginVerdict::Malformed;
    };
    match admission.bind(request, now) {
        Ok(()) => OriginVerdict::Eligible(admission),
        Err(error) => OriginVerdict::Unbound(error),
    }
}

impl OriginAdmission {
    /// A fill's revalidation must answer for the same scope and operation
    /// as the admission it started under.
    pub fn same_scope(&self, current: &OriginAdmission) -> Result<(), DeliveryError> {
        if self.identity == current.identity && self.key == current.key {
            Ok(())
        } else {
            Err(DeliveryError::ScopeChanged)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn mark_keeps_only_the_gateways_own_connection_init() {
        let mut request = json!({"query":"{a}","extensions":{"gatewayDelivery":{"action":"execute","eligible":true}}});
        mark(&mut request, "validate");
        assert_eq!(
            request["extensions"]["gatewayDelivery"],
            json!({"action":"validate"})
        );
        mark_connection(&mut request, json!({"token":"t"}));
        mark(&mut request, "snapshot");
        assert_eq!(
            request["extensions"]["gatewayDelivery"],
            json!({"action":"snapshot","connectionInit":{"token":"t"}})
        );
        let mut bare = json!({"query":"{a}"});
        mark(&mut bare, "validate");
        assert_eq!(bare["extensions"]["gatewayDelivery"]["action"], "validate");
    }

    #[test]
    fn verdicts_fail_closed() {
        let request = json!({"query":"{a}"});
        let ok = |body: Value| serde_json::to_vec(&body).unwrap();
        let eligible = ok(
            json!({"data":{},"extensions":{"gatewayDelivery":{"eligible":true,"admission":{}}}}),
        );
        assert!(matches!(
            verdict(401, false, b"", &request, 0),
            OriginVerdict::Reject
        ));
        assert!(matches!(
            verdict(200, false, b"<html>", &request, 0),
            OriginVerdict::Malformed
        ));
        assert!(matches!(
            verdict(
                200,
                false,
                &ok(json!({"errors":[{"message":"no"}]})),
                &request,
                0
            ),
            OriginVerdict::Reject
        ));
        assert!(matches!(
            verdict(200, false, &ok(json!({"errors":"bad"})), &request, 0),
            OriginVerdict::Reject
        ));
        assert!(matches!(
            verdict(200, false, &ok(json!({"data":{}})), &request, 0),
            OriginVerdict::Bypass
        ));
        assert!(matches!(
            verdict(200, true, &eligible, &request, 0),
            OriginVerdict::Bypass
        ));
        assert!(matches!(
            verdict(200, false, &eligible, &request, 0),
            OriginVerdict::Malformed
        ));
    }

    #[test]
    fn an_admission_must_bind_this_request_now() {
        use super::super::{OperationKey, OriginIdentity, SnapshotPolicy};
        let request = json!({"query":"{a}"});
        let identity = OriginIdentity {
            application: "app".into(),
            endpoint: "origin".into(),
            schema_hash: "schema".into(),
            protocol_hash: "protocol".into(),
            authorization_generation: "policy".into(),
            cache_scope: "alice".into(),
        };
        let admission = OriginAdmission {
            key: OperationKey::from_origin(&identity, &request).unwrap(),
            identity,
            operation: "operation".into(),
            validator: "v1".into(),
            validated_at: 100,
            expires_at: 130,
            policy: SnapshotPolicy::Current,
        };
        let body = serde_json::to_vec(&json!({"data":{},"extensions":{"gatewayDelivery":{
            "eligible":true,"admission":admission}}}))
        .unwrap();
        let OriginVerdict::Eligible(admitted) = verdict(200, false, &body, &request, 100) else {
            panic!("a bound admission is eligible");
        };
        assert!(admission.same_scope(&admitted).is_ok());
        assert!(matches!(
            verdict(200, false, &body, &json!({"query":"{b}"}), 100),
            OriginVerdict::Unbound(DeliveryError::ScopeChanged)
        ));
        assert!(matches!(
            verdict(200, false, &body, &request, 130),
            OriginVerdict::Unbound(DeliveryError::Unavailable)
        ));
        let mut other = admitted.clone();
        other.identity.cache_scope = "bob".into();
        assert_eq!(
            admission.same_scope(&other),
            Err(DeliveryError::ScopeChanged)
        );
    }
}
