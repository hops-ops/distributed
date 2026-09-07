use super::{
    canonical_json, CoordinatorLimits, DeliveryError, FreshnessContext, OperationKey,
    OriginAdmission, SnapshotResponse,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Authenticated live operation identity and initial replay requirements.
/// HTTP query documents can never construct this key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LiveKey {
    base: String,
    initial: String,
    fork: u64,
}
impl LiveKey {
    /// Construct only after fresh origin admission for this exact consumer.
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
        ) != Ok(crate::gateway::graphql::OperationKind::Subscription)
        {
            return Err(DeliveryError::Ineligible);
        }
        if let Some(freshness) = freshness {
            freshness.bind(&admission.identity)?;
        }
        let mut base = request.clone();
        if let Some(extensions) = base
            .get_mut("extensions")
            .and_then(serde_json::Value::as_object_mut)
        {
            if let Some(distributed) = extensions
                .get_mut("distributed")
                .and_then(serde_json::Value::as_object_mut)
            {
                distributed.remove("resume");
                if distributed.is_empty() {
                    extensions.remove("distributed");
                }
            }
        }
        let base = OperationKey::from_origin(&admission.identity, &base)?
            .as_str()
            .to_owned();
        let initial = canonical_json(&serde_json::json!([
            "live-join-v1",
            admission.key,
            admission.validator,
            freshness
        ]))?;
        Ok(Self {
            base,
            initial: format!("{:x}", Sha256::digest(initial)),
            fork: 0,
        })
    }
    /// Same exact operation/scope may hand off only after comparable cursor
    /// equality. Different resume requests are not automatically initial joins.
    pub fn same_operation(&self, other: &Self) -> bool {
        self.base == other.base
    }
    /// Independent replay generation while waiting for a safe handoff.
    pub fn fork(&self, nonce: u64) -> Self {
        Self {
            fork: nonce,
            ..self.clone()
        }
    }
    /// Exact initial request compatibility, ignoring runtime generation.
    pub fn same_initial(&self, other: &Self) -> bool {
        self.base == other.base && self.initial == other.initial
    }
}
/// Bounded per-coordinator and per-consumer live resources.
#[derive(Clone, Copy, Debug)]
pub struct LiveLimits {
    /// Maximum active upstream groups including independent replays.
    pub groups: usize,
    /// Maximum logical consumers sharing one group.
    pub consumers: usize,
    /// Maximum pending full frames for each consumer.
    pub queue_frames: usize,
    /// Maximum bytes in one full data/protocol frame.
    pub frame_bytes: usize,
    /// Maximum retained history frames for exact initial replay.
    pub history_frames: usize,
    /// Maximum upstream group lifetime; reconnect reauthenticates.
    pub lifetime_ms: u64,
}
impl Default for LiveLimits {
    fn default() -> Self {
        Self {
            groups: 256,
            consumers: 1024,
            queue_frames: 16,
            frame_bytes: 1024 * 1024,
            history_frames: 8,
            lifetime_ms: 3600000,
        }
    }
}
impl LiveLimits {
    /// Validate all bounds before mounting live coordination.
    pub fn validate(&self) -> Result<(), DeliveryError> {
        if self.groups == 0
            || self.groups > 4096
            || self.consumers == 0
            || self.consumers > 65536
            || self.queue_frames == 0
            || self.queue_frames > 1024
            || self.frame_bytes == 0
            || self.frame_bytes > 16 * 1024 * 1024
            || self.history_frames == 0
            || self.history_frames > self.queue_frames
            || self.lifetime_ms == 0
            || self.lifetime_ms > 3600000
        {
            Err(DeliveryError::InvalidContext)
        } else {
            Ok(())
        }
    }
}
impl TryFrom<LiveLimits> for CoordinatorLimits {
    type Error = DeliveryError;
    fn try_from(limits: LiveLimits) -> Result<Self, Self::Error> {
        limits.validate()?;
        Ok(Self {
            groups: limits.groups,
            consumers: limits.consumers,
            deadline_ms: limits.lifetime_ms,
        })
    }
}
/// Live refcounts/deadlines use the same coordinator as query flights.
pub type LiveRegistry = super::CoordinatorRegistry<LiveKey>;
/// One admitted live consumer's generation ticket.
pub type LiveTicket = super::CoordinatorTicket<LiveKey>;

/// Full origin frame, including every causal observation and checkpoint.
#[derive(Clone, Debug)]
pub struct LiveFrame {
    payload: serde_json::Value,
    hash: [u8; 32],
    cursor: Option<Vec<u8>>,
    identity: super::OriginIdentity,
    operation: String,
    evidence: Vec<super::Minimum>,
}
impl LiveFrame {
    /// Validate exact origin authority and any supplied floors before fan-out.
    pub fn from_origin(
        admission: &OriginAdmission,
        payload: serde_json::Value,
        freshness: Option<&FreshnessContext>,
        max_bytes: usize,
    ) -> Result<Self, DeliveryError> {
        let bytes = serde_json::to_vec(&payload).map_err(|_| DeliveryError::Ineligible)?;
        if bytes.len() > max_bytes {
            return Err(DeliveryError::Unavailable);
        }
        let response = SnapshotResponse {
            status: 200,
            headers: Vec::new(),
            body: bytes.clone(),
        };
        if !response.live_shareable(admission, freshness) {
            return Err(DeliveryError::Ineligible);
        }
        let evidence = response
            .evidence(admission, false, true)
            .ok_or(DeliveryError::Ineligible)?;
        let canonical = canonical_json(&payload).unwrap_or(bytes);
        let protocol = &payload["extensions"]["distributed"];
        let cursors = &protocol["live"]["cursors"];
        let cursor = if protocol["live"]["supported"] == true
            && protocol["snapshot"]["indexesComparable"] == true
            && cursors.as_array().is_some_and(|cursors| {
                !cursors.is_empty()
                    && cursors.len() <= 256
                    && cursors.iter().all(|cursor| {
                        cursor["projection"]
                            .as_str()
                            .is_some_and(|value| !value.is_empty() && value.len() <= 1024)
                            && cursor["position"].as_str().is_some_and(|value| {
                                !value.is_empty()
                                    && value.bytes().all(|byte| byte.is_ascii_digit())
                                    && value.parse::<u64>().is_ok()
                            })
                            && cursor["token"]
                                .as_str()
                                .is_some_and(|value| !value.is_empty() && value.len() <= 1024)
                    })
            }) {
            Some(canonical_json(cursors)?)
        } else {
            None
        };
        Ok(Self {
            payload,
            hash: Sha256::digest(canonical).into(),
            cursor,
            identity: admission.identity.clone(),
            operation: admission.operation.clone(),
            evidence,
        })
    }
    /// Check each consumer's independent authority and retained minima.
    pub fn satisfies(
        &self,
        admission: &OriginAdmission,
        freshness: Option<&FreshnessContext>,
    ) -> bool {
        self.identity == admission.identity
            && self.operation == admission.operation
            && freshness.is_none_or(|context| {
                context.bind(&admission.identity).is_ok() && context.satisfied_by(&self.evidence)
            })
    }
    /// Full payload to serialize under each consumer's own transport ID.
    pub fn payload(&self) -> &serde_json::Value {
        &self.payload
    }
    /// Suppress only fully identical data plus protocol, never data alone.
    pub fn same_frame(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
    /// Independent replay can hand off only at an exact proven cursor vector.
    /// Adapters must also compare LiveKey::same_operation and preserve queued
    /// frames before moving the consumer to the target's future stream.
    pub fn same_cursor(&self, other: &Self) -> bool {
        self.cursor.is_some()
            && self.cursor == other.cursor
            && self.payload["data"] == other.payload["data"]
    }
}
