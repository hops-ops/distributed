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

#[derive(Clone, Default)]
pub(crate) struct PlanCapture(
    pub(crate) std::sync::Arc<std::sync::Mutex<Vec<crate::graphql::compile::SqlPlan>>>,
);
pub(crate) const CAPTURED: &str = "gateway query plan captured";
