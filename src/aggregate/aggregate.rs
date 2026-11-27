use std::fmt;

use crate::entity::{upcast_events_for_replay, Entity, EventRecord, EventUpcaster};
use crate::repository::RepositoryError;

/// Trait for domain aggregates that can be event-sourced.
pub trait Aggregate: Sized + Default {
    type ReplayError: fmt::Display;

    /// Stable aggregate type used by stream-aware persistence.
    ///
    /// It is part of every stream's identity, so it must be a durable
    /// application name: renaming or moving the Rust type must not change it.
    /// `#[sourced(entity, aggregate_type = "...")]` emits it.
    fn aggregate_type() -> &'static str;

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

    // Take the events out of the entity so they can be iterated while `agg` is
    // borrowed mutably, then put them back. `hydrate` is only called with a
    // full-history entity, so `load_from_history` reproduces the loaded
    // version and committed version.
    let history = agg.entity_mut().take_events();
    let replayed = replay_into(&mut agg, history.iter());
    agg.entity_mut().load_from_history(history);
    replayed.map(|()| agg)
}

/// Replay `events` into `agg`: the one replay loop, used for a full history
/// and for the tail after a snapshot.
///
/// Upcasters may rewrite events for replay; the durable history is unchanged.
/// When no upcaster applies, the borrowed events are replayed with no clone.
/// Domain-event capture is suppressed while replaying.
pub(crate) fn replay_into<'e, A: Aggregate>(
    agg: &mut A,
    events: impl Iterator<Item = &'e EventRecord> + Clone,
) -> Result<(), RepositoryError> {
    let upcasted = upcast_events_for_replay(events.clone(), A::upcasters())
        .map_err(|err| RepositoryError::Replay(err.to_string()))?;
    agg.entity_mut().set_replaying(true);
    let replayed = match &upcasted {
        Some(upcasted) => apply_each(agg, upcasted.iter().map(|event| event.as_ref())),
        None => apply_each(agg, events),
    };
    agg.entity_mut().set_replaying(false);
    replayed
}

fn apply_each<'e, A: Aggregate>(
    agg: &mut A,
    events: impl Iterator<Item = &'e EventRecord>,
) -> Result<(), RepositoryError> {
    for event in events {
        agg.replay_event(event)
            .map_err(|err| RepositoryError::Replay(err.to_string()))?;
    }
    Ok(())
}
