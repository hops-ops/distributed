use std::fmt;

use crate::entity::{upcast_events_for_replay, Entity, EventRecord, EventUpcaster};
use crate::repository::RepositoryError;

/// Trait for domain aggregates that can be event-sourced.
pub trait Aggregate: Sized + Default {
    type ReplayError: fmt::Display;

    /// Stable aggregate type used by stream-aware persistence.
    ///
    /// The default is a development fallback based on Rust's type name so
    /// existing examples keep working. Production persistence should override
    /// this with an explicit, durable application name.
    fn aggregate_type() -> &'static str {
        std::any::type_name::<Self>()
    }

    fn new_empty() -> Self {
        Self::default()
    }
    fn entity(&self) -> &Entity;
    fn entity_mut(&mut self) -> &mut Entity;
    fn replay_event(&mut self, event: &EventRecord) -> Result<(), Self::ReplayError>;

    /// Override to register upcasters for this aggregate's events.
    /// Upcasters are configuration, not state — this is a static method.
    fn upcasters() -> &'static [EventUpcaster] {
        &[]
    }
}

/// Hydrate an aggregate from an entity by replaying its events.
pub fn hydrate<A: Aggregate>(entity: Entity) -> Result<A, RepositoryError> {
    let mut agg = A::new_empty();
    *agg.entity_mut() = entity;

    // Take the events out of the entity so we can iterate them while holding a
    // mutable borrow of `agg` during replay. `hydrate` is only ever called with
    // a full-history entity, so `prefix_version == 0` and restoring via
    // `load_from_history` reproduces the exact loaded version/committed_version.
    let history = agg.entity_mut().take_events();

    // Upcasters may rewrite events for replay, but the durable history is
    // unchanged: replay from the upcasted view (which clones only the events an
    // upcaster rewrites), then restore the originals. When no upcaster applies —
    // no upcasters registered, or every event already current — the borrowed
    // history is replayed directly with no clone.
    match upcast_events_for_replay(history.iter(), A::upcasters())
        .map_err(|err| RepositoryError::Replay(err.to_string()))?
    {
        Some(upcasted) => replay_into(&mut agg, upcasted.iter().map(|event| event.as_ref()))?,
        None => replay_into(&mut agg, history.iter())?,
    }

    agg.entity_mut().load_from_history(history);

    Ok(agg)
}

fn replay_into<'e, A: Aggregate>(
    agg: &mut A,
    events: impl Iterator<Item = &'e EventRecord>,
) -> Result<(), RepositoryError> {
    agg.entity_mut().set_replaying(true);
    for event in events {
        if let Err(err) = agg.replay_event(event) {
            agg.entity_mut().set_replaying(false);
            return Err(RepositoryError::Replay(err.to_string()));
        }
    }
    agg.entity_mut().set_replaying(false);
    Ok(())
}
