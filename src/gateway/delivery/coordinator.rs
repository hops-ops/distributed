use super::DeliveryError;
use std::collections::BTreeMap;

/// Validated shared group bounds; construct through flight or live limits.
pub struct CoordinatorLimits {
    pub(crate) groups: usize,
    pub(crate) consumers: usize,
    pub(crate) deadline_ms: u64,
}

/// Adapter-owned consumer ticket. Release exactly once when that consumer
/// finishes/cancels. Stale tickets cannot release a newer same-key generation.
#[derive(Debug)]
pub struct CoordinatorTicket<K> {
    key: K,
    generation: u64,
}
impl<K> CoordinatorTicket<K> {
    /// Flight generation used to bind a runtime future to portable bookkeeping.
    pub fn generation(&self) -> u64 {
        self.generation
    }
}
struct Group {
    generation: u64,
    consumers: usize,
    deadline: u64,
}
/// Portable bounded refcount/deadline bookkeeping. Runtime futures, clocks,
/// sockets and cancellation guards belong to native/Worker adapters.
pub struct CoordinatorRegistry<K> {
    limits: CoordinatorLimits,
    groups: BTreeMap<K, Group>,
    next: u64,
}
impl<K: Ord + Clone> CoordinatorRegistry<K> {
    /// Allocate empty bookkeeping only when query coalescing is selected.
    pub fn new(
        limits: impl TryInto<CoordinatorLimits, Error = DeliveryError>,
    ) -> Result<Self, DeliveryError> {
        let limits = limits.try_into()?;
        Ok(Self {
            limits,
            groups: BTreeMap::new(),
            next: 0,
        })
    }
    /// Join/create; the boolean identifies the one upstream execution owner.
    /// `now_ms` is the adapter's monotonic clock, not a causal data clock.
    pub fn join(
        &mut self,
        key: K,
        now_ms: u64,
    ) -> Result<(CoordinatorTicket<K>, bool), DeliveryError> {
        self.expire(now_ms);
        if let Some(group) = self.groups.get_mut(&key) {
            if group.consumers >= self.limits.consumers {
                return Err(DeliveryError::Unavailable);
            }
            group.consumers += 1;
            return Ok((
                CoordinatorTicket {
                    key,
                    generation: group.generation,
                },
                false,
            ));
        }
        if self.groups.len() >= self.limits.groups {
            return Err(DeliveryError::Unavailable);
        }
        self.next = self.next.checked_add(1).ok_or(DeliveryError::Unavailable)?;
        self.groups.insert(
            key.clone(),
            Group {
                generation: self.next,
                consumers: 1,
                deadline: now_ms.saturating_add(self.limits.deadline_ms),
            },
        );
        Ok((
            CoordinatorTicket {
                key,
                generation: self.next,
            },
            true,
        ))
    }
    /// Release one consumer; true means the last consumer left that generation.
    pub fn leave(&mut self, ticket: CoordinatorTicket<K>) -> bool {
        let Some(group) = self.groups.get_mut(&ticket.key) else {
            return false;
        };
        if group.generation != ticket.generation {
            return false;
        }
        group.consumers -= 1;
        if group.consumers == 0 {
            self.groups.remove(&ticket.key);
            true
        } else {
            false
        }
    }
    /// Forget expired groups; adapters enforce matching upstream deadlines.
    pub fn expire(&mut self, now_ms: u64) {
        self.groups.retain(|_, group| group.deadline > now_ms);
    }
    /// Active group count.
    pub fn len(&self) -> usize {
        self.groups.len()
    }
    /// Whether no group has an admitted consumer.
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
    /// Total current admitted consumers.
    pub fn consumers(&self) -> usize {
        self.groups.values().map(|group| group.consumers).sum()
    }
    /// Check whether a runtime future still belongs to an active generation.
    pub fn contains_generation(&self, generation: u64) -> bool {
        self.groups
            .values()
            .any(|group| group.generation == generation)
    }
}
