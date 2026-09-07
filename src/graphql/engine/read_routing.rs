use super::*;
use crate::gateway::{
    delivery::*,
    graphql::{operation_kind, OperationKind},
};

/// Explicit stale-tolerant allowlist. No timeout/stickiness is treated as replay
/// proof. With any retained floor the initial router always uses primary.
#[derive(Clone)]
pub struct ReadRouting {
    pub(crate) replica: GraphqlPool,
    stale_tolerant: BTreeSet<(String, Option<String>)>,
}
impl ReadRouting {
    /// Bind an optional read replica; no query uses it until explicitly registered.
    pub fn new(replica: impl Into<GraphqlPool>) -> Self {
        Self {
            replica: replica.into(),
            stale_tolerant: BTreeSet::new(),
        }
    }
    /// Register one exact selected ordinary query as stale-tolerant.
    pub fn stale_tolerant(
        mut self,
        document: impl Into<String>,
        operation_name: Option<String>,
    ) -> Result<Self, GraphqlBuildError> {
        let document = document.into();
        if operation_kind(&document, operation_name.as_deref()) != Ok(OperationKind::Query)
            || self.stale_tolerant.len() >= 4096
        {
            return Err(GraphqlBuildError(
                "stale-tolerant routing requires a bounded ordinary query inventory".into(),
            ));
        }
        self.stale_tolerant.insert((document, operation_name));
        Ok(self)
    }
}

#[derive(Clone)]
pub(crate) struct ReadRequest {
    policy: ReadConsistency,
    pub(crate) freshness: Option<FreshnessContext>,
}
impl ReadRequest {
    pub(crate) fn pool<'a>(
        &self,
        inner: &'a EngineInner,
        plan: &SqlPlan,
    ) -> (&'a GraphqlPool, bool) {
        let Some(routing) = &inner.read_routing else {
            return (&inner.pool, true);
        };
        // Physical compiler footprint covers filters, empty membership, joins
        // and counts even when no result row contains a corresponding key.
        let mut dependencies = Dependencies {
            complete: true,
            ..Default::default()
        };
        for table in &plan.tables_touched {
            if let Some(model) = inner.by_table.get(table) {
                dependencies.models.insert(model.clone());
            } else {
                dependencies.complete = false;
            }
        }
        match read_target(self.policy, &dependencies, self.freshness.as_ref()) {
            ReadTarget::Primary => (&inner.pool, true),
            ReadTarget::Replica => (&routing.replica, false),
        }
    }
}

impl GraphqlEngine {
    /// Establish identity on the authenticated origin control path. Callers
    /// must supply the same verified principal and surface request as execution.
    /// The returned scope is an identifier, never authorization by itself.
    pub fn delivery_identity(
        &self,
        session: &Session,
        request: &Request,
    ) -> Result<OriginIdentity, DeliveryError> {
        let authority = resolve_execution_authority(&self.inner, session, request)
            .map_err(|_| DeliveryError::Ineligible)?;
        let runtime = self
            .inner
            .protocol
            .as_ref()
            .ok_or(DeliveryError::Ineligible)?;
        let (_, surface, _, _) =
            select_protocol_surface(runtime, &authority).map_err(|_| DeliveryError::Ineligible)?;
        let envelope = self
            .protocol_accumulator(&authority, session, request)
            .map_err(|_| DeliveryError::Ineligible)?
            .ok_or(DeliveryError::Ineligible)?
            .snapshot()
            .map_err(|_| DeliveryError::Ineligible)?;
        let identity = OriginIdentity {
            application: serde_json::to_string(&authority.surface)
                .map_err(|_| DeliveryError::Ineligible)?,
            endpoint: runtime.service_id.clone(),
            schema_hash: envelope.schema_hash,
            protocol_hash: surface.protocol_fingerprint.clone(),
            authorization_generation: envelope.authorization_generation,
            cache_scope: envelope.cache_scope.as_str().to_owned(),
        };
        identity.validate()?;
        Ok(identity)
    }
    pub(crate) fn prepare_read(
        &self,
        session: &Session,
        request: &Request,
    ) -> Result<ReadRequest, DeliveryError> {
        let kind = operation_kind(&request.query, request.operation_name.as_deref());
        let context = request.extensions.get("gatewayFreshness");
        let freshness = if let Some(context) = context {
            if !matches!(kind, Ok(OperationKind::Query | OperationKind::Subscription)) {
                return Err(DeliveryError::Ineligible);
            }
            let context =
                serde_json::to_value(context).map_err(|_| DeliveryError::InvalidContext)?;
            let context = FreshnessContext::parse(&context)?;
            context.bind(&self.delivery_identity(session, request)?)?;
            Some(context)
        } else {
            None
        };
        let policy = if kind == Ok(OperationKind::Query)
            && self.inner.read_routing.as_ref().is_some_and(|r| {
                r.stale_tolerant
                    .contains(&(request.query.clone(), request.operation_name.clone()))
            }) {
            ReadConsistency::StaleTolerant
        } else {
            ReadConsistency::Current
        };
        Ok(ReadRequest { policy, freshness })
    }
}

pub(crate) fn enforce_minimum(response: Response, read: &ReadRequest) -> Response {
    let Some(context) = &read.freshness else {
        return response;
    };
    if context.minimum.is_empty() || !response.errors.is_empty() {
        return response;
    }
    let value = serde_json::to_value(&response).unwrap_or_default();
    let snapshot = &value["extensions"]["distributed"]["snapshot"];
    let mut evidence = Vec::new();
    for record in snapshot["records"].as_array().into_iter().flatten() {
        let mut record = record.clone();
        if let Some(object) = record.as_object_mut() {
            object.remove("path");
            object.remove("tombstone");
            object.insert("kind".into(), "record".into());
        }
        if let Ok(minimum) = serde_json::from_value::<Minimum>(record) {
            if minimum.validate().is_ok() {
                evidence.push(minimum);
            }
        }
    }
    for index in snapshot["indexes"].as_array().into_iter().flatten() {
        let mut index = index.clone();
        if let Some(object) = index.as_object_mut() {
            object.remove("resume");
            object.insert("kind".into(), "index".into());
        }
        if let Ok(minimum) = serde_json::from_value::<Minimum>(index) {
            if minimum.validate().is_ok() {
                evidence.push(minimum);
            }
        }
    }
    if context.satisfied_by(&evidence) {
        response
    } else {
        delivery_error(DeliveryError::Pending)
    }
}
pub(crate) fn delivery_error(error: DeliveryError) -> Response {
    let mut result = ServerError::new(error.to_string(), None);
    let mut extensions = async_graphql::ErrorExtensionValues::default();
    extensions.set(
        "code",
        match error {
            DeliveryError::Pending => "FRESHNESS_PENDING",
            DeliveryError::ScopeChanged => "FRESHNESS_SCOPE_CHANGED",
            _ => "INVALID_FRESHNESS_CONTEXT",
        },
    );
    result.extensions = Some(extensions);
    Response::from_errors(vec![result])
}
