//! Origin-side delivery validation. SQL dependency versions are private;
//! gateway responses carry only scope-bound opaque validators.
mod versions;
pub(crate) use versions::*;
pub use versions::{GatewayOriginMetrics, GatewayVersionStore};

use crate::gateway::delivery::{OperationKey, OriginIdentity};
use crate::graphql::protocol::{ProtocolTokenCodec, ProtocolTokenPurpose};

#[derive(Clone, Debug)]
pub(crate) struct GatewayCapture {
    pub(crate) identity: OriginIdentity,
    pub(crate) key: OperationKey,
    pub(crate) validator: Option<String>,
}

pub(crate) fn validator(
    codec: &ProtocolTokenCodec,
    identity: &OriginIdentity,
    key: &OperationKey,
    versions: &VersionVector,
) -> Result<String, String> {
    codec
        .issue(
            ProtocolTokenPurpose::QuerySnapshot,
            &serde_json::json!({
                "domain":"distributed.gateway.snapshot-validator", "version":1,
                "identity":identity, "operation":key, "versions":versions
            }),
        )
        .map(|token| token.as_str().to_owned())
        .map_err(|_| "validator encoding failed".into())
}

/// Plan mode for one request: every root resolver records the plan it
/// compiled here and returns without executing it. Present in request data
/// only while [`GraphqlEngine::plan`](crate::graphql::GraphqlEngine) runs.
#[derive(Clone, Default)]
pub(crate) struct PlanRecorder(
    std::sync::Arc<std::sync::Mutex<Vec<Option<std::sync::Arc<crate::graphql::compile::SqlPlan>>>>>,
);

impl PlanRecorder {
    /// Record `plan` (`None` when no SQL plan can stand for the field, as for
    /// a cell-by-key read) and return the error the resolver returns instead
    /// of a value. The error carries no meaning: eligibility is read from the
    /// recorder, never from response errors.
    pub(crate) fn record(&self, plan: crate::graphql::compile::QueryPlan) -> async_graphql::Error {
        let plan = match plan {
            crate::graphql::compile::QueryPlan::Sql(plan) => Some(plan),
            crate::graphql::compile::QueryPlan::CellByKey { .. } => None,
        };
        if let Ok(mut plans) = self.0.lock() {
            plans.push(plan);
        }
        crate::graphql::schema::client_error("INTERNAL", "not executed")
    }

    /// The plan, when exactly one root resolver ran and it compiled to SQL.
    pub(crate) fn single(&self) -> Option<std::sync::Arc<crate::graphql::compile::SqlPlan>> {
        let mut plans = self.0.lock().ok()?;
        match plans.len() {
            1 => plans.pop().flatten(),
            _ => None,
        }
    }
}
