use super::*;
use crate::gateway::{
    delivery::{OperationKey, OriginAdmission},
    graphql::{operation_kind, OperationKind},
};
use crate::graphql::delivery::{self, GatewayCapture, PlanCapture};

pub(crate) fn request_value(request: &Request) -> serde_json::Value {
    serde_json::json!({"query": request.query, "operationName":request.operation_name, "variables":request.variables, "extensions":request.extensions})
}
pub(crate) fn action(request: &Request) -> Option<String> {
    request
        .extensions
        .get("gatewayDelivery")
        .and_then(|value| serde_json::to_value(value).ok())
        .and_then(|value| value["action"].as_str().map(str::to_owned))
}
fn unavailable() -> Response {
    let mut error = ServerError::new("origin delivery validation unavailable", None);
    let mut extensions = async_graphql::ErrorExtensionValues::default();
    extensions.set("code", "DELIVERY_UNAVAILABLE");
    error.extensions = Some(extensions);
    Response::from_errors(vec![error])
}
fn ineligible() -> Response {
    Response::new(Value::Null).extension(
        "gatewayDelivery",
        Value::from_json(serde_json::json!({"eligible":false})).expect("static JSON"),
    )
}
impl GraphqlEngine {
    /// Active origin live-query producers for delivery diagnostics.
    pub fn live_subscriber_count(&self) -> usize {
        self.inner.change_hub.subscriber_count()
    }

    pub(super) fn enable_delivery_capture(
        &self,
        session: &Session,
        request: &Request,
        accumulator: &ProtocolResponseAccumulator,
    ) -> Result<(), ()> {
        if action(request).as_deref() != Some("snapshot")
            || self
                .inner
                .gateway_versions
                .as_ref()
                .is_none_or(|store| !store.envelope_coverage())
        {
            return Ok(());
        }
        if operation_kind(&request.query, request.operation_name.as_deref())
            != Ok(OperationKind::Query)
        {
            return Err(());
        }
        let identity = self.delivery_identity(session, request).map_err(|_| ())?;
        let key = OperationKey::from_origin(&identity, &request_value(request)).map_err(|_| ())?;
        accumulator
            .enable_gateway(GatewayCapture {
                identity,
                key,
                validator: None,
            })
            .map_err(|_| ())
    }
    pub(super) async fn validate_delivery(&self, session: &Session, request: Request) -> Response {
        let live = match operation_kind(&request.query, request.operation_name.as_deref()) {
            Ok(OperationKind::Query) => false,
            Ok(OperationKind::Subscription) => true,
            _ => return ineligible(),
        };
        let Some(store) = &self.inner.gateway_versions else {
            return ineligible();
        };
        store.record_validation();
        let Some(runtime) = &self.inner.protocol else {
            return ineligible();
        };
        let identity = match self.delivery_identity(session, &request) {
            Ok(identity) => identity,
            Err(_) => return ineligible(),
        };
        let key = match OperationKey::from_origin(&identity, &request_value(&request)) {
            Ok(key) => key,
            Err(_) => return ineligible(),
        };
        if self.prepare_read(session, &request).is_err() {
            return unavailable();
        }
        let authority = match resolve_execution_authority(&self.inner, session, &request) {
            Ok(authority) => authority,
            Err(_) => return ineligible(),
        };
        let Some(schema) = self.inner.schemas.get(&authority.privilege_role) else {
            return ineligible();
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |value| value.as_secs());
        let expiry = request
            .data
            .get(&TypeId::of::<VerifiedPrincipal>())
            .and_then(|p| p.downcast_ref::<VerifiedPrincipal>())
            .and_then(VerifiedPrincipal::expires_at)
            .unwrap_or(now.saturating_add(if live { 3600 } else { 30 }));
        if expiry <= now {
            return unavailable();
        }
        let operation = operation_fingerprint(&request.query);
        let policy = store.policy(&request.query, request.operation_name.as_deref());
        let captured = PlanCapture::default();
        let request = request
            .data(session.clone())
            .data(authority)
            .data(Arc::clone(&self.inner))
            .data(captured.clone());
        let response = if live {
            schema
                .execute_stream(request)
                .next()
                .await
                .unwrap_or_else(ineligible)
        } else {
            schema.execute(request).await
        };
        // Schema validation and normal compiler authorization still run. Only
        // the private capture sentinel may replace SQL; unknown/custom fields,
        // cell reads and multi-root documents never acquire cache eligibility.
        if response.errors.len() != 1 || response.errors[0].message != delivery::CAPTURED {
            return ineligible();
        }
        let plan = match captured.0.lock() {
            Ok(mut plans) if plans.len() == 1 => plans.pop().expect("captured plan"),
            _ => return ineligible(),
        };
        if !store.covers(&plan.tables_touched) {
            return ineligible();
        }
        let versions = match store.current(&self.inner.pool, &plan.tables_touched).await {
            Ok(versions) => versions,
            Err(error)
                if error.starts_with("dependency ") || error.starts_with("query dependency ") =>
            {
                return ineligible()
            }
            Err(_) => return unavailable(),
        };
        let validator = match delivery::validator(&runtime.codec, &identity, &key, &versions) {
            Ok(validator) => validator,
            Err(_) => return unavailable(),
        };
        let admission = OriginAdmission {
            identity,
            key,
            operation,
            validator,
            validated_at: now,
            expires_at: expiry,
            policy,
        };
        Response::new(Value::Null).extension(
            "gatewayDelivery",
            Value::from_json(serde_json::json!({"eligible":true,"admission":admission}))
                .expect("serializable admission"),
        )
    }
}
