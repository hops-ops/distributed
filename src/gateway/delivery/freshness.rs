use super::{DeliveryError, OriginIdentity};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Maximum combined number of pending dependencies and retained floors.
pub const MAX_FRESHNESS_ITEMS: usize = 256;
/// Maximum serialized causal context size.
pub const MAX_FRESHNESS_BYTES: usize = 64 * 1024;

/// Compiler-known dependency inventory. Unknown coverage overlaps everything
/// within the already authorized surface, including empty lists and counts.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Dependencies {
    /// Whether the compiler accounts for every dependency.
    pub complete: bool,
    /// Model names including filter and join dependencies.
    pub models: BTreeSet<String>,
    /// Relationship identities affecting membership.
    pub relationships: BTreeSet<String>,
}
impl Dependencies {
    /// Conservatively test dependency overlap without inspecting result rows.
    pub fn overlaps(&self, other: &Self) -> bool {
        !self.complete
            || !other.complete
            || !self.models.is_disjoint(&other.models)
            || !self.relationships.is_disjoint(&other.relationships)
    }
    /// Validate bounded contract values before accepting external data.
    pub fn validate(&self) -> Result<(), DeliveryError> {
        if self.models.len() + self.relationships.len() > MAX_FRESHNESS_ITEMS {
            return Err(DeliveryError::InvalidContext);
        }
        for name in self.models.iter().chain(&self.relationships) {
            bounded(name)?;
        }
        Ok(())
    }
}

/// Opaque scope tokens keep incomparable record/index obligations separate.
/// This carries existing evidence, never a new public/global partition clock.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Minimum {
    /// Comparable evidence for one record.
    Record {
        /// Model owning this record evidence.
        model: String,
        #[serde(rename = "scopeToken")]
        /// Opaque origin-issued comparison scope.
        scope_token: String,
        /// Decimal record incarnation.
        incarnation: String,
        /// Decimal revision within the incarnation.
        revision: String,
    },
    /// Comparable evidence for an index.
    Index {
        /// Projection owning this index evidence.
        projection: String,
        #[serde(rename = "scopeToken")]
        /// Opaque origin-issued comparison scope.
        scope_token: String,
        /// Decimal checkpoint within this opaque scope.
        position: String,
    },
}
impl Minimum {
    /// Validate bounded contract values before accepting external data.
    pub fn validate(&self) -> Result<(), DeliveryError> {
        match self {
            Self::Record {
                model,
                scope_token,
                incarnation,
                revision,
            } => {
                bounded(model)?;
                bounded(scope_token)?;
                decimal(incarnation)?;
                decimal(revision)?;
            }
            Self::Index {
                projection,
                scope_token,
                position,
            } => {
                bounded(projection)?;
                bounded(scope_token)?;
                decimal(position)?;
            }
        }
        Ok(())
    }
    /// Whether this same-scope evidence is at least the required revision.
    pub fn covers(&self, required: &Self) -> bool {
        match (self, required) {
            (
                Self::Record {
                    model: a,
                    scope_token: sa,
                    incarnation: ia,
                    revision: ra,
                },
                Self::Record {
                    model: b,
                    scope_token: sb,
                    incarnation: ib,
                    revision: rb,
                },
            ) => {
                a == b
                    && sa == sb
                    && (compare(ia, ib).is_gt() || (ia == ib && !compare(ra, rb).is_lt()))
            }
            (
                Self::Index {
                    projection: a,
                    scope_token: sa,
                    position: pa,
                },
                Self::Index {
                    projection: b,
                    scope_token: sb,
                    position: pb,
                },
            ) => a == b && sa == sb && !compare(pa, pb).is_lt(),
            _ => false,
        }
    }
}

/// An authenticated request binds these client hints to the origin identity.
/// Hints may force primary reads; they are never proof that a command committed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FreshnessContext {
    /// Wire contract version; must be one.
    pub version: u32,
    /// Exact schema generation.
    pub schema_hash: String,
    /// Exact protocol generation.
    pub protocol_hash: String,
    /// Origin policy generation.
    pub authorization_generation: String,
    /// Origin-issued subject and authorization scope.
    pub cache_scope: String,
    /// Unconfirmed effect dependencies, separate from minima.
    pub pending: Vec<Dependencies>,
    /// Retained confirmed evidence, including incomparable scopes.
    pub minimum: Vec<Minimum>,
}
impl FreshnessContext {
    /// Decode a bounded context and reject malformed or unknown fields.
    pub fn parse(value: &serde_json::Value) -> Result<Self, DeliveryError> {
        if serde_json::to_vec(value)
            .map_err(|_| DeliveryError::InvalidContext)?
            .len()
            > MAX_FRESHNESS_BYTES
        {
            return Err(DeliveryError::InvalidContext);
        }
        let context: Self =
            serde_json::from_value(value.clone()).map_err(|_| DeliveryError::InvalidContext)?;
        if context.version != 1
            || context.pending.len() + context.minimum.len() > MAX_FRESHNESS_ITEMS
        {
            return Err(DeliveryError::InvalidContext);
        }
        for part in [
            &context.schema_hash,
            &context.protocol_hash,
            &context.authorization_generation,
            &context.cache_scope,
        ] {
            bounded(part)?;
        }
        for dependencies in &context.pending {
            dependencies.validate()?;
        }
        for minimum in &context.minimum {
            minimum.validate()?;
        }
        Ok(context)
    }
    /// Compare client context with freshly authenticated origin authority.
    pub fn bind(&self, identity: &OriginIdentity) -> Result<(), DeliveryError> {
        identity.validate()?;
        if self.schema_hash != identity.schema_hash
            || self.protocol_hash != identity.protocol_hash
            || self.authorization_generation != identity.authorization_generation
            || self.cache_scope != identity.cache_scope
        {
            return Err(DeliveryError::ScopeChanged);
        }
        Ok(())
    }
    /// Whether any unconfirmed effect can affect these dependencies.
    pub fn pending_overlaps(&self, dependencies: &Dependencies) -> bool {
        self.pending
            .iter()
            .any(|pending| pending.overlaps(dependencies))
    }
    /// Require every retained floor to have comparable covering evidence.
    pub fn satisfied_by(&self, evidence: &[Minimum]) -> bool {
        self.minimum
            .iter()
            .all(|required| evidence.iter().any(|candidate| candidate.covers(required)))
    }
    /// Retain maximal comparable floors without discarding incomparable scopes.
    pub fn observe(
        &mut self,
        evidence: impl IntoIterator<Item = Minimum>,
    ) -> Result<(), DeliveryError> {
        let mut next = self.minimum.clone();
        for candidate in evidence {
            candidate.validate()?;
            if next.iter().any(|current| current.covers(&candidate)) {
                continue;
            }
            next.retain(|current| !candidate.covers(current));
            next.push(candidate);
            if next.len() + self.pending.len() > MAX_FRESHNESS_ITEMS {
                return Err(DeliveryError::InvalidContext);
            }
        }
        self.minimum = next;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// Origin-owned operation consistency policy.
pub enum ReadConsistency {
    #[default]
    /// Require an authoritative current read.
    Current,
    /// Explicit opt-in to stale reads subject to causal requirements.
    StaleTolerant,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Backend chosen without assuming replay freshness from a pool.
pub enum ReadTarget {
    /// Authoritative read-model backend.
    Primary,
    /// Explicitly configured stale-tolerant read backend.
    Replica,
}
/// No replica-proof adapter is installed by the initial framework router.
/// Even an unrelated retained floor conservatively uses primary; pending work
/// with known disjoint dependencies does not change an explicit stale policy.
pub fn read_target(
    policy: ReadConsistency,
    dependencies: &Dependencies,
    freshness: Option<&FreshnessContext>,
) -> ReadTarget {
    if policy == ReadConsistency::Current
        || freshness.is_some_and(|f| f.pending_overlaps(dependencies) || !f.minimum.is_empty())
    {
        ReadTarget::Primary
    } else {
        ReadTarget::Replica
    }
}
fn bounded(value: &str) -> Result<(), DeliveryError> {
    if value.is_empty() || value.len() > 1024 || value.chars().any(char::is_control) {
        Err(DeliveryError::InvalidContext)
    } else {
        Ok(())
    }
}
fn decimal(value: &str) -> Result<(), DeliveryError> {
    if value.is_empty()
        || value.len() > 20
        || !value.bytes().all(|b| b.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
        || value.parse::<u64>().is_err()
    {
        Err(DeliveryError::InvalidContext)
    } else {
        Ok(())
    }
}
fn compare(a: &str, b: &str) -> std::cmp::Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}
