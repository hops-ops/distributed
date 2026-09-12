use super::{DeliveryError, FreshnessContext, Minimum, OperationKey, OriginIdentity};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Fresh authenticated origin control response. Deserialize only from the
/// configured origin, never from public request headers/body or a cached grant.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OriginAdmission {
    /// Effective origin identity including subject and policy generation.
    pub identity: OriginIdentity,
    /// Exact admitted operation and variable identity.
    pub key: OperationKey,
    /// Exact transport document fingerprint in the response envelope.
    pub operation: String,
    /// Opaque version vector computed on the primary at this validation.
    pub validator: String,
    /// Origin validation time, seconds since Unix epoch.
    pub validated_at: u64,
    /// Credential/admission expiry; never extended by copying an entry.
    pub expires_at: u64,
    /// Origin-approved policy; private/current is the default.
    pub policy: SnapshotPolicy,
}
impl OriginAdmission {
    /// Ensure a configured origin answered this exact consumer operation.
    pub fn bind(&self, request: &serde_json::Value, now: u64) -> Result<(), DeliveryError> {
        self.validate(now)?;
        if OperationKey::from_origin(&self.identity, request)? != self.key {
            return Err(DeliveryError::ScopeChanged);
        }
        Ok(())
    }
    /// Validate a fresh control response before lookup/join.
    pub fn validate(&self, now: u64) -> Result<(), DeliveryError> {
        self.identity.validate()?;
        if self.key.as_str().len() != 64
            || !self
                .key
                .as_str()
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(DeliveryError::InvalidContext);
        }
        if self.validator.is_empty()
            || self.validator.len() > 1024
            || self.operation.is_empty()
            || self.operation.len() > 256
            || self.validated_at > now.saturating_add(5)
            || self.expires_at <= now
            || self.expires_at <= self.validated_at
        {
            return Err(DeliveryError::Unavailable);
        }
        if let SnapshotPolicy::Public { max_age_seconds } = self.policy {
            if max_age_seconds == 0 || max_age_seconds > 86400 {
                return Err(DeliveryError::InvalidContext);
            }
        }
        Ok(())
    }
}

/// Data staleness policy never relaxes per-consumer origin authentication.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SnapshotPolicy {
    /// Require the primary's current version vector on every hit.
    #[default]
    Current,
    /// Explicit origin-approved public staleness, measured from validation.
    Public {
        /// Maximum age in seconds; copying cannot renew it.
        max_age_seconds: u64,
    },
}

/// Bounded storage limits. No cache exists unless explicitly constructed.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotLimits {
    /// Maximum resident entries.
    pub entries: usize,
    /// Maximum aggregate response bytes.
    pub bytes: usize,
    /// Maximum individual response size.
    pub entry_bytes: usize,
}
impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            entries: 1024,
            bytes: 16 * 1024 * 1024,
            entry_bytes: 1024 * 1024,
        }
    }
}

/// Complete HTTP envelope retained without reconstructing GraphQL data/proofs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotResponse {
    /// HTTP status; reusable responses must be successful.
    pub status: u16,
    /// End-to-end response headers, preserving duplicate values.
    pub headers: Vec<(String, String)>,
    /// Exact JSON response bytes from the executor.
    pub body: Vec<u8>,
}
impl SnapshotResponse {
    pub(super) fn evidence(
        &self,
        admission: &OriginAdmission,
        complete: bool,
        live: bool,
    ) -> Option<Vec<Minimum>> {
        if self.status != 200
            || self.headers.iter().any(|(name, value)| {
                name.eq_ignore_ascii_case("set-cookie")
                    || (name.eq_ignore_ascii_case("cache-control")
                        && value
                            .split(',')
                            .any(|directive| directive.trim().eq_ignore_ascii_case("no-store")))
                    || (name.eq_ignore_ascii_case("vary")
                        && value.split(',').any(|field| field.trim() == "*"))
            })
        {
            return None;
        }
        let value: serde_json::Value = serde_json::from_slice(&self.body).ok()?;
        if value.get("data").is_none()
            || value["data"].is_null()
            || value
                .get("errors")
                .is_some_and(|e| e.as_array().is_none_or(|e| !e.is_empty()))
        {
            return None;
        }
        let protocol = &value["extensions"]["distributed"];
        if protocol["protocolVersion"] != 1
            || protocol["schemaHash"] != admission.identity.schema_hash
            || protocol["authorizationGeneration"] != admission.identity.authorization_generation
            || protocol["cacheScope"] != admission.identity.cache_scope
            || protocol["operation"] != admission.operation
            || protocol.get("command").is_some()
            || protocol.get("receipt").is_some()
            || (!live && protocol.get("live").is_some())
        {
            return None;
        }
        let snapshot = &protocol["snapshot"];
        if complete
            && (snapshot["recordsComplete"] != true || snapshot["indexesComparable"] != true)
        {
            return None;
        }
        if complete
            && (snapshot["records"].as_array().is_none()
                || snapshot["indexes"].as_array().is_none())
        {
            return None;
        }
        let mut evidence = Vec::new();
        for record in snapshot["records"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or(&[])
        {
            let minimum = Minimum::Record {
                model: record["model"].as_str()?.into(),
                scope_token: record["scopeToken"].as_str()?.into(),
                incarnation: record["incarnation"].as_str()?.into(),
                revision: record["revision"].as_str()?.into(),
            };
            minimum.validate().ok()?;
            evidence.push(minimum);
        }
        for index in snapshot["indexes"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or(&[])
        {
            let minimum = Minimum::Index {
                projection: index["projection"].as_str()?.into(),
                scope_token: index["scopeToken"].as_str()?.into(),
                position: index["position"].as_str()?.into(),
            };
            minimum.validate().ok()?;
            evidence.push(minimum);
        }
        Some(evidence)
    }
    /// Complete admitted HTTP result may be shared in flight without requiring
    /// future cache eligibility. Any supplied minima still require actual proof.
    pub fn shareable(
        &self,
        admission: &OriginAdmission,
        freshness: Option<&FreshnessContext>,
    ) -> bool {
        self.evidence(admission, false, false)
            .is_some_and(|evidence| {
                freshness.is_none_or(|context| {
                    context.bind(&admission.identity).is_ok() && context.satisfied_by(&evidence)
                })
            })
    }
    /// Validate live fan-out while retaining the origin's live envelope.
    pub fn live_shareable(
        &self,
        admission: &OriginAdmission,
        freshness: Option<&FreshnessContext>,
    ) -> bool {
        self.evidence(admission, false, true)
            .is_some_and(|evidence| {
                freshness.is_none_or(|context| {
                    context.bind(&admission.identity).is_ok() && context.satisfied_by(&evidence)
                })
            })
    }
    /// Candidate proof covers every required floor in the admitted scope.
    pub fn satisfies(
        &self,
        admission: &OriginAdmission,
        freshness: Option<&FreshnessContext>,
    ) -> bool {
        self.evidence(admission, true, false)
            .is_some_and(|evidence| {
                freshness.is_none_or(|context| {
                    context.bind(&admission.identity).is_ok() && context.satisfied_by(&evidence)
                })
            })
    }
}

#[derive(Clone)]
struct Entry {
    admission: OriginAdmission,
    response: SnapshotResponse,
    bytes: usize,
    sequence: u64,
}
/// Opaque reservation fencing cache installation after invalidation/restart.
#[derive(Clone, Debug)]
pub struct FillTicket {
    key: OperationKey,
    generation: u64,
}

/// Portable bounded snapshot store. Runtime adapters provide current origin
/// admission for every consumer and coordinate calls; this owns no task/socket.
pub struct SnapshotCache {
    metrics: super::SnapshotMetrics,
    limits: SnapshotLimits,
    entries: BTreeMap<OperationKey, Entry>,
    bytes: usize,
    generation: u64,
    sequence: u64,
}
impl SnapshotCache {
    /// Allocate an empty cache with explicit resource bounds.
    pub fn new(limits: SnapshotLimits) -> Result<Self, DeliveryError> {
        if limits.entries == 0
            || limits.entries > 65536
            || limits.bytes == 0
            || limits.bytes > 1024 * 1024 * 1024
            || limits.entry_bytes == 0
            || limits.entry_bytes > limits.bytes
        {
            return Err(DeliveryError::InvalidContext);
        }
        Ok(Self {
            metrics: super::SnapshotMetrics::default(),
            limits,
            entries: BTreeMap::new(),
            bytes: 0,
            generation: 0,
            sequence: 0,
        })
    }
    /// Reserve a fill after authentication. Invalidations fence old tickets.
    pub fn begin_fill(
        &self,
        admission: &OriginAdmission,
        now: u64,
    ) -> Result<FillTicket, DeliveryError> {
        admission.validate(now)?;
        Ok(FillTicket {
            key: admission.key.clone(),
            generation: self.generation,
        })
    }
    /// Lookup against a fresh authenticated primary validation, never a stored
    /// lease alone. Public age is anchored to the original validation time.
    pub fn lookup(
        &mut self,
        admission: &OriginAdmission,
        freshness: Option<&FreshnessContext>,
        now: u64,
    ) -> Result<Option<SnapshotResponse>, DeliveryError> {
        admission.validate(now)?;
        if let Some(freshness) = freshness {
            freshness.bind(&admission.identity)?;
        }
        let Some(entry) = self.entries.get_mut(&admission.key) else {
            self.metrics.misses = self.metrics.misses.saturating_add(1);
            return Ok(None);
        };
        let current = entry.admission.validator == admission.validator;
        let public_age = match (entry.admission.policy, admission.policy) {
            (
                SnapshotPolicy::Public {
                    max_age_seconds: first,
                },
                SnapshotPolicy::Public {
                    max_age_seconds: next,
                },
            ) => now.saturating_sub(entry.admission.validated_at) <= first.min(next),
            _ => false,
        };
        if (!current && !public_age)
            || entry.admission.identity != admission.identity
            || !entry.response.satisfies(admission, freshness)
        {
            self.metrics.misses = self.metrics.misses.saturating_add(1);
            self.metrics.stale_rejections = self.metrics.stale_rejections.saturating_add(1);
            return Ok(None);
        }
        self.metrics.hits = self.metrics.hits.saturating_add(1);
        self.sequence = self.sequence.saturating_add(1);
        entry.sequence = self.sequence;
        Ok(Some(entry.response.clone()))
    }
    /// Install only after revalidating the response's own snapshot validator at
    /// the origin. A late old fill cannot acquire a newer result's validator.
    pub fn install(
        &mut self,
        ticket: FillTicket,
        admission: OriginAdmission,
        response: SnapshotResponse,
        now: u64,
    ) -> Result<bool, DeliveryError> {
        admission.validate(now)?;
        let bytes = response.body.len()
            + response
                .headers
                .iter()
                .map(|(a, b)| a.len() + b.len())
                .sum::<usize>();
        if bytes > self.limits.entry_bytes {
            self.metrics.fill_bypasses = self.metrics.fill_bypasses.saturating_add(1);
            return Ok(false);
        }
        if self.generation == u64::MAX
            || ticket.generation != self.generation
            || ticket.key != admission.key
            || !response.satisfies(&admission, None)
        {
            self.metrics.fill_bypasses = self.metrics.fill_bypasses.saturating_add(1);
            return Ok(false);
        }
        let value: serde_json::Value =
            serde_json::from_slice(&response.body).map_err(|_| DeliveryError::Ineligible)?;
        if value["extensions"]["gatewayDelivery"]["validator"] != admission.validator {
            self.metrics.fill_bypasses = self.metrics.fill_bypasses.saturating_add(1);
            return Ok(false);
        }
        if let Some(previous) = self.entries.remove(&ticket.key) {
            self.bytes -= previous.bytes;
        }
        while self.entries.len() >= self.limits.entries || self.bytes + bytes > self.limits.bytes {
            let Some(key) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.sequence)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            self.bytes -= self.entries.remove(&key).expect("resident key").bytes;
        }
        self.sequence = self.sequence.saturating_add(1);
        self.bytes += bytes;
        self.entries.insert(
            ticket.key,
            Entry {
                admission,
                response,
                bytes,
                sequence: self.sequence,
            },
        );
        Ok(true)
    }
    /// Lost feed, rebuild or coordinator reset discards data and fences fills.
    pub fn invalidate_all(&mut self) {
        self.metrics.invalidations = self.metrics.invalidations.saturating_add(1);
        self.entries.clear();
        self.bytes = 0;
        // Saturation cannot permit an old ticket to become current: at the
        // terminal counter value installation is permanently disabled.
        self.generation = self.generation.saturating_add(1);
    }
    /// Snapshot decisions without any identity-bearing labels.
    pub fn metrics(&self) -> super::SnapshotMetrics {
        self.metrics
    }
    /// Current resident entry count for diagnostics and boundedness checks.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
