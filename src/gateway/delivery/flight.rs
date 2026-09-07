use super::{canonical_json, DeliveryError, FreshnessContext, OriginAdmission};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Exact admitted operation, dependency version and freshness requirement.
/// Conservative equality prevents stronger late consumers joining older work.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FlightKey(String);
impl FlightKey {
    /// Authenticate at the origin before constructing a flight identity.
    pub fn admitted(
        admission: &OriginAdmission,
        request: &serde_json::Value,
        freshness: Option<&FreshnessContext>,
        now: u64,
    ) -> Result<Self, DeliveryError> {
        admission.bind(request, now)?;
        if crate::gateway::graphql::operation_kind(
            request["query"].as_str().ok_or(DeliveryError::Ineligible)?,
            request["operationName"].as_str(),
        ) != Ok(crate::gateway::graphql::OperationKind::Query)
        {
            return Err(DeliveryError::Ineligible);
        }
        if let Some(context) = freshness {
            context.bind(&admission.identity)?;
        }
        let bytes = canonical_json(&serde_json::json!([
            "query-flight-v1",
            admission.key,
            admission.validator,
            freshness
        ]))?;
        Ok(Self(format!("{:x}", Sha256::digest(bytes))))
    }
}
/// Explicit coordinator bounds, independent of snapshot cache activation.
#[derive(Clone, Copy, Debug)]
pub struct FlightLimits {
    /// Maximum simultaneously active exact-scope groups.
    pub groups: usize,
    /// Maximum admitted consumers in one group.
    pub consumers: usize,
    /// Maximum operation duration in milliseconds.
    pub deadline_ms: u64,
    /// Maximum complete response bytes retained by one group.
    pub response_bytes: usize,
}
impl Default for FlightLimits {
    fn default() -> Self {
        Self {
            groups: 256,
            consumers: 1024,
            deadline_ms: 30000,
            response_bytes: 1024 * 1024,
        }
    }
}
impl FlightLimits {
    /// Validate resource bounds before allocating runtime coordination.
    pub fn validate(&self) -> Result<(), DeliveryError> {
        if self.groups == 0
            || self.groups > 4096
            || self.consumers == 0
            || self.consumers > 65536
            || self.deadline_ms == 0
            || self.deadline_ms > 300000
            || self.response_bytes == 0
            || self.response_bytes > 16 * 1024 * 1024
        {
            Err(DeliveryError::InvalidContext)
        } else {
            Ok(())
        }
    }
}
/// Portable query registry using the common bounded coordinator.
pub type FlightRegistry = super::CoordinatorRegistry<FlightKey>;
/// One admitted query consumer's cancellation/generation ticket.
pub type FlightTicket = super::CoordinatorTicket<FlightKey>;
impl TryFrom<FlightLimits> for super::CoordinatorLimits {
    type Error = DeliveryError;
    fn try_from(limits: FlightLimits) -> Result<Self, Self::Error> {
        limits.validate()?;
        Ok(Self {
            groups: limits.groups,
            consumers: limits.consumers,
            deadline_ms: limits.deadline_ms,
        })
    }
}
