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

/// Portable query-flight table: bounded groups plus one weak handle to each
/// group's running work. Holding only weak handles means the last consumer
/// dropping its strong handle cancels the work; adapters own the lock, the
/// clock and the work futures.
pub struct FlightTable<W> {
    registry: FlightRegistry,
    work: std::collections::BTreeMap<u64, W>,
}
impl<W> FlightTable<W> {
    /// Allocate empty bookkeeping with validated limits.
    pub fn new(limits: FlightLimits) -> Result<Self, DeliveryError> {
        Ok(Self {
            registry: FlightRegistry::new(limits)?,
            work: std::collections::BTreeMap::new(),
        })
    }
    /// Join or create the exact group. The owner calls `start`, which returns
    /// the strong handle it awaits and the weak handle the table keeps; a
    /// joiner gets a strong handle through `share`. A group whose work is
    /// already gone, or a full registry, refuses: the caller runs alone.
    pub fn join<S>(
        &mut self,
        key: FlightKey,
        now_ms: u64,
        start: impl FnOnce() -> (S, W),
        share: impl FnOnce(&W) -> Option<S>,
    ) -> Result<(FlightTicket, S), DeliveryError> {
        self.registry.expire(now_ms);
        let registry = &self.registry;
        self.work
            .retain(|generation, _| registry.contains_generation(*generation));
        let (ticket, owner) = self.registry.join(key, now_ms)?;
        let generation = ticket.generation();
        if owner {
            let (strong, weak) = start();
            self.work.insert(generation, weak);
            return Ok((ticket, strong));
        }
        match self.work.get(&generation).and_then(share) {
            Some(strong) => Ok((ticket, strong)),
            None => {
                self.registry.leave(ticket);
                Err(DeliveryError::Unavailable)
            }
        }
    }
    /// Release one consumer; the last one forgets the group's work.
    pub fn leave(&mut self, ticket: FlightTicket) {
        let generation = ticket.generation();
        if self.registry.leave(ticket) {
            self.work.remove(&generation);
        }
    }
    /// Active groups and admitted consumers, without identifiers.
    pub fn counts(&self) -> (usize, usize) {
        (self.registry.len(), self.registry.consumers())
    }
}

#[cfg(test)]
mod tests {
    use super::super::{OperationKey, OriginIdentity, SnapshotPolicy};
    use super::*;
    use std::rc::{Rc, Weak};

    fn key() -> FlightKey {
        let request = serde_json::json!({"query":"{ rows { title } }"});
        let identity = OriginIdentity {
            application: "test".into(),
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
        FlightKey::admitted(&admission, &request, None, 100).unwrap()
    }
    type Table = FlightTable<Weak<&'static str>>;
    fn start(work: &'static str) -> impl FnOnce() -> (Rc<&'static str>, Weak<&'static str>) {
        move || {
            let strong = Rc::new(work);
            let weak = Rc::downgrade(&strong);
            (strong, weak)
        }
    }

    #[test]
    fn joiners_share_the_owners_work_until_the_last_leaves() {
        let mut table = Table::new(FlightLimits::default()).unwrap();
        let (first, work) = table.join(key(), 0, start("owner"), Weak::upgrade).unwrap();
        let (second, shared) = table
            .join(
                key(),
                0,
                || panic!("a joiner starts no work"),
                Weak::upgrade,
            )
            .unwrap();
        assert!(Rc::ptr_eq(&work, &shared));
        assert_eq!(table.counts(), (1, 2));
        table.leave(first);
        assert_eq!(table.counts(), (1, 1));
        table.leave(second);
        assert_eq!(table.counts(), (0, 0));
        drop((work, shared));
        let (_, fresh) = table.join(key(), 0, start("next"), Weak::upgrade).unwrap();
        assert_eq!(*fresh, "next", "a finished group's work is never reused");
    }

    #[test]
    fn a_full_registry_or_vanished_work_refuses_without_a_leaked_slot() {
        let mut table = Table::new(FlightLimits {
            consumers: 1,
            ..Default::default()
        })
        .unwrap();
        let (owner, work) = table.join(key(), 0, start("owner"), Weak::upgrade).unwrap();
        assert_eq!(
            table.join(key(), 0, start("x"), Weak::upgrade).err(),
            Some(DeliveryError::Unavailable)
        );
        assert_eq!(table.counts(), (1, 1));
        table.leave(owner);

        let mut table = Table::new(FlightLimits::default()).unwrap();
        let (owner, work_2) = table.join(key(), 0, start("owner"), Weak::upgrade).unwrap();
        drop(work_2); // The owner's work ended while its ticket is still held.
        assert_eq!(
            table.join(key(), 0, start("x"), Weak::upgrade).err(),
            Some(DeliveryError::Unavailable)
        );
        assert_eq!(table.counts(), (1, 1));
        table.leave(owner);
        assert_eq!(table.counts(), (0, 0));
        drop(work);
    }

    #[test]
    fn the_deadline_expires_a_group_and_its_work() {
        let mut table = Table::new(FlightLimits {
            deadline_ms: 20,
            ..Default::default()
        })
        .unwrap();
        let (stale, _old) = table.join(key(), 0, start("old"), Weak::upgrade).unwrap();
        let (_, new) = table.join(key(), 20, start("new"), Weak::upgrade).unwrap();
        assert_eq!(*new, "new");
        assert_eq!(table.counts(), (1, 1));
        table.leave(stale);
        assert_eq!(
            table.counts(),
            (1, 1),
            "a stale ticket cannot release the newer group"
        );
    }
}
