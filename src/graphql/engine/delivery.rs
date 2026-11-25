use super::*;
use crate::gateway::{
    delivery::{OperationKey, OriginAdmission},
    graphql::{operation_kind, OperationKind},
};
use crate::graphql::compile::SqlPlan;
use crate::graphql::delivery::{self, GatewayCapture, PlanRecorder, VersionError};
use async_graphql::Value;

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
        let request = request
            .data(session.clone())
            .data(authority)
            .data(Arc::clone(&self.inner));
        let Some(plan) = self.plan(schema, request, live).await else {
            return ineligible();
        };
        if !store.covers(&plan.tables_touched) {
            return ineligible();
        }
        let versions = match store.current(&self.inner.pool, &plan.tables_touched).await {
            Ok(versions) => versions,
            Err(VersionError::Uncovered(_)) => return ineligible(),
            Err(VersionError::Storage(_)) => return unavailable(),
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

    /// Compile the request's single root field without executing it.
    ///
    /// `request` already carries its session, execution authority and engine.
    /// Schema validation and the compiler's authorization run as for an
    /// execution; resolvers record their plan instead of running SQL. `None`
    /// when the operation is not exactly one root field compiling to one SQL
    /// statement (several roots, a cell read, a custom field, an invalid or
    /// unauthorized request).
    pub(crate) async fn plan(
        &self,
        schema: &async_graphql::dynamic::Schema,
        mut request: Request,
        live: bool,
    ) -> Option<Arc<SqlPlan>> {
        if !has_single_root_field(&mut request) {
            return None;
        }
        let recorder = PlanRecorder::default();
        let request = request.data(recorder.clone());
        if live {
            // The stream's first item is the root resolver's outcome.
            let _ = schema.execute_stream(request).next().await;
        } else {
            let _ = schema.execute(request).await;
        }
        recorder.single()
    }
}

/// The selected operation's root selection set is exactly one field, counting
/// through fragments. Anything unparsable, or nested past the request analysis
/// bounds, is not.
fn has_single_root_field(request: &mut Request) -> bool {
    use async_graphql::parser::types::{ExecutableDocument, Selection, SelectionSet};

    fn count(
        set: &SelectionSet,
        document: &ExecutableDocument,
        depth: usize,
        fields: &mut usize,
    ) -> Option<()> {
        if depth > REQUEST_ANALYSIS_MAX_DEPTH {
            return None;
        }
        for item in &set.items {
            match &item.node {
                Selection::Field(_) => *fields += 1,
                Selection::InlineFragment(fragment) => count(
                    &fragment.node.selection_set.node,
                    document,
                    depth + 1,
                    fields,
                )?,
                Selection::FragmentSpread(spread) => {
                    let fragment = document.fragments.get(&spread.node.fragment_name.node)?;
                    count(
                        &fragment.node.selection_set.node,
                        document,
                        depth + 1,
                        fields,
                    )?
                }
            }
            if *fields > 1 {
                return None;
            }
        }
        Some(())
    }

    let operation_name = request.operation_name.clone();
    let Ok(document) = request.parsed_query() else {
        return false;
    };
    let Some(operation) = selected_operation(document, operation_name.as_deref()) else {
        return false;
    };
    let mut fields = 0;
    count(&operation.selection_set.node, document, 0, &mut fields).is_some() && fields == 1
}
