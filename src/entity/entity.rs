use std::collections::HashMap;
use std::fmt;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::domain_event::{
    state_descriptor_matches, DomainDeletion, DomainEvent, DomainEventBodyKind,
    DomainEventCaptureError, DomainEventCaptureOutcome, DomainEventCapturePoison,
    DomainEventCommitGuardError, DomainEventDescriptor, DomainEventEnvelope, DomainEventOccurrence,
    DomainState,
};
use crate::trace_context::{TraceContext, CAUSATION_ID, CORRELATION_ID};
use crate::SourcedResult;

use super::{BitcodePayloadCodec, EventRecord, EventRecordError, PayloadCodec};

/// Position and time of an event recorded by the current unit of work
/// ([`Entity::last_recorded`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recorded {
    /// Stream sequence (1-based; equals the stream version after commit).
    pub sequence: u64,
    /// Event time.
    pub timestamp: SystemTime,
}

impl Recorded {
    /// Event time as Unix milliseconds (0 before the epoch).
    pub fn unix_ms(&self) -> u64 {
        self.timestamp
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Entity {
    id: String,
    version: u64,
    events: Vec<EventRecord>,
    /// Number of leading events NOT held in `events` — the prefix folded into a
    /// snapshot and skipped on load. Zero for a full-history entity. The true
    /// stream length is always `prefix_version + events.len()`, so event
    /// sequencing and `new_events` slicing stay correct when only a tail is in
    /// memory. Transient: not part of the durable entity representation.
    #[serde(skip, default)]
    prefix_version: u64,
    #[serde(skip, default)]
    replaying: bool,
    snapshot_version: u64,
    #[serde(skip, default)]
    committed_version: u64,
    timestamp: SystemTime,
    /// Metadata applied to every event produced by `digest`.
    /// Transient — not serialized with the entity. Set before calling
    /// command methods to attach correlation IDs, user context, etc.
    #[serde(skip, default)]
    metadata: HashMap<String, String>,
    #[serde(skip, default)]
    pending_domain_events: Vec<DomainEventOccurrence>,
    #[serde(skip, default)]
    domain_event_poison: Option<DomainEventCapturePoison>,
}

impl Default for Entity {
    fn default() -> Self {
        Entity {
            id: String::new(),
            version: 0,
            events: Vec::new(),
            prefix_version: 0,
            replaying: false,
            snapshot_version: 0,
            committed_version: 0,
            timestamp: crate::time::now(),
            metadata: HashMap::new(),
            pending_domain_events: Vec::new(),
            domain_event_poison: None,
        }
    }
}

impl fmt::Debug for Entity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entity")
            .field("id", &self.id)
            .field("version", &self.version)
            .field("events", &self.events)
            .field("prefix_version", &self.prefix_version)
            .field("replaying", &self.replaying)
            .field("snapshot_version", &self.snapshot_version)
            .field("committed_version", &self.committed_version)
            .field("timestamp", &self.timestamp)
            .field("metadata", &self.metadata)
            .field("pending_domain_events", &self.pending_domain_events.len())
            .field("domain_event_poison", &self.domain_event_poison)
            .finish()
    }
}

impl Clone for Entity {
    fn clone(&self) -> Self {
        Entity {
            id: self.id.clone(),
            version: self.version,
            events: self.events.clone(),
            prefix_version: self.prefix_version,
            replaying: self.replaying,
            snapshot_version: self.snapshot_version,
            committed_version: self.committed_version,
            timestamp: self.timestamp,
            metadata: self.metadata.clone(),
            pending_domain_events: self.pending_domain_events.clone(),
            domain_event_poison: self.domain_event_poison.clone(),
        }
    }
}

struct ReplayGuard<'a> {
    replaying: &'a mut bool,
}

impl<'a> ReplayGuard<'a> {
    fn new(replaying: &'a mut bool) -> Self {
        *replaying = true;
        ReplayGuard { replaying }
    }
}

impl Drop for ReplayGuard<'_> {
    fn drop(&mut self) {
        *self.replaying = false;
    }
}

impl Entity {
    pub fn new() -> Self {
        Entity::default()
    }

    pub fn with_id(id: impl Into<String>) -> Self {
        Entity {
            id: id.into(),
            ..Entity::default()
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn set_id(&mut self, id: impl Into<String>) {
        self.id = id.into();
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn snapshot_version(&self) -> u64 {
        self.snapshot_version
    }

    pub fn set_snapshot_version(&mut self, snapshot_version: u64) {
        self.snapshot_version = snapshot_version;
    }

    pub fn committed_version(&self) -> u64 {
        self.committed_version
    }

    pub fn events(&self) -> &[EventRecord] {
        &self.events
    }

    /// Return exact outward occurrences awaiting atomic persistence.
    pub fn pending_domain_events(&self) -> &[DomainEventOccurrence] {
        &self.pending_domain_events
    }

    /// Return the sticky first domain-event capture poison, if any.
    pub fn domain_event_poison(&self) -> Option<&DomainEventCapturePoison> {
        self.domain_event_poison.as_ref()
    }

    /// Reject manual commit preparation when transition-time capture failed.
    ///
    /// Task-owned repository integrations must call this before I/O. A failure
    /// is sticky so an aggregate replay event cannot commit without its intended
    /// outward occurrence.
    pub fn domain_event_commit_guard(&self) -> Result<(), DomainEventCommitGuardError> {
        match &self.domain_event_poison {
            Some(poison) => Err(DomainEventCommitGuardError::new(poison.clone())),
            None => Ok(()),
        }
    }

    /// Borrow the exact occurrence batch after applying the manual poison guard.
    ///
    /// # Errors
    ///
    /// Returns the sticky capture poison. Merely borrowing this batch never
    /// removes it, so a persistence failure retains identical bytes for retry.
    pub fn pending_domain_events_for_commit(
        &self,
    ) -> Result<&[DomainEventOccurrence], DomainEventCommitGuardError> {
        self.domain_event_commit_guard()?;
        Ok(&self.pending_domain_events)
    }

    /// Clear pending outward occurrences after their atomic persistence succeeds.
    ///
    /// # Errors
    ///
    /// Refuses to clear a poisoned buffer. Failed persistence must not call this
    /// method; leaving the entity untouched retains exact bytes for retry.
    pub fn mark_domain_events_committed(&mut self) -> Result<(), DomainEventCommitGuardError> {
        self.domain_event_commit_guard()?;
        self.pending_domain_events.clear();
        Ok(())
    }

    /// Capture a public post-transition state for one semantic event.
    ///
    /// # Errors
    ///
    /// Records a sticky typed poison when descriptor validation or canonical
    /// serialization fails.
    pub fn capture_domain_state<S: DomainState>(
        &mut self,
        aggregate_type: &str,
        descriptor: DomainEventDescriptor,
        state: &S,
    ) -> Result<DomainEventCaptureOutcome, DomainEventCaptureError> {
        if self.replaying {
            return Ok(DomainEventCaptureOutcome::SuppressedDuringReplay);
        }
        if !state_descriptor_matches(&descriptor, &S::DESCRIPTOR) {
            return self
                .capture_failed(descriptor, DomainEventCaptureError::StateDescriptorMismatch);
        }
        self.capture_typed(aggregate_type, descriptor, state)
    }

    /// Capture a sparse or explicitly adapted typed domain event.
    ///
    /// Aggregate replay payload bytes do not implement [`DomainEvent`], so they
    /// cannot become an implicit outward body:
    ///
    /// ```compile_fail
    /// use distributed::Entity;
    ///
    /// let mut entity = Entity::with_id("todo-1");
    /// let replay_payload = vec![0_u8; 4];
    /// entity.capture_domain_event("todo", &replay_payload);
    /// ```
    ///
    /// # Errors
    ///
    /// Records a sticky typed poison when descriptor validation or canonical
    /// serialization fails.
    pub fn capture_domain_event<E: DomainEvent>(
        &mut self,
        aggregate_type: &str,
        event: &E,
    ) -> Result<DomainEventCaptureOutcome, DomainEventCaptureError> {
        if self.replaying {
            return Ok(DomainEventCaptureOutcome::SuppressedDuringReplay);
        }
        let descriptor = E::DESCRIPTOR.clone();
        if descriptor.body.kind != DomainEventBodyKind::Event {
            return self.capture_failed(
                descriptor,
                DomainEventCaptureError::BodyKindMismatch {
                    expected: DomainEventBodyKind::Event,
                    actual: E::DESCRIPTOR.body.kind,
                },
            );
        }
        self.capture_typed(aggregate_type, descriptor, event)
    }

    /// Capture a typed deletion identity and incarnation.
    ///
    /// # Errors
    ///
    /// Records a sticky typed poison when descriptor validation or canonical
    /// serialization fails.
    pub fn capture_domain_deletion<K: Serialize>(
        &mut self,
        aggregate_type: &str,
        descriptor: DomainEventDescriptor,
        deletion: &DomainDeletion<K>,
    ) -> Result<DomainEventCaptureOutcome, DomainEventCaptureError> {
        if self.replaying {
            return Ok(DomainEventCaptureOutcome::SuppressedDuringReplay);
        }
        if descriptor.body.kind != DomainEventBodyKind::Deletion {
            let actual = descriptor.body.kind;
            return self.capture_failed(
                descriptor,
                DomainEventCaptureError::BodyKindMismatch {
                    expected: DomainEventBodyKind::Deletion,
                    actual,
                },
            );
        }
        self.capture_typed(aggregate_type, descriptor, deletion)
    }

    /// Take the in-memory events out of the entity, leaving it empty.
    ///
    /// Used by replay paths that need to iterate events while also holding a
    /// mutable borrow of the owning aggregate (the borrow checker forbids
    /// borrowing `events` immutably and the aggregate mutably at once). The
    /// caller is responsible for restoring history afterward — e.g. via
    /// [`load_from_history`](Self::load_from_history) — so the entity's
    /// `version`/`committed_version` invariants hold.
    pub fn take_events(&mut self) -> Vec<EventRecord> {
        std::mem::take(&mut self.events)
    }

    /// Put a previously [taken](Self::take_events) event history back without
    /// recomputing `version`/`prefix_version`/`committed_version`. The caller
    /// must restore the *same* events the entity already accounted for, so the
    /// existing invariants continue to hold (used by replay paths that only
    /// borrowed the events out to satisfy the borrow checker).
    pub fn restore_history(&mut self, events: Vec<EventRecord>) {
        self.events = events;
    }

    /// Returns events added since the entity was loaded (not yet persisted).
    ///
    /// Slices relative to `prefix_version` so it is correct whether the entity
    /// holds the full history (`prefix_version == 0`) or only a snapshot tail.
    pub fn new_events(&self) -> &[EventRecord] {
        // Invariant (established by `load_from_history`/`load_tail_from_history`
        // and preserved by `mark_committed`): the committed position never lies
        // inside the omitted prefix, so this subtraction cannot underflow. In
        // release builds a violation still fails loudly at the slice below.
        debug_assert!(
            self.committed_version >= self.prefix_version,
            "entity invariant violated: committed_version {} < prefix_version {}",
            self.committed_version,
            self.prefix_version
        );
        let start = (self.committed_version - self.prefix_version) as usize;
        &self.events[start..]
    }

    /// Where and when the latest event recorded by the current unit of work
    /// (not yet committed) lands in the stream: its sequence and timestamp,
    /// as they will be committed. `None` when nothing was recorded. Command
    /// results (receipts, leaderboard occurrences) read this instead of
    /// digging through [`events`](Self::events).
    pub fn last_recorded(&self) -> Option<Recorded> {
        self.new_events().last().map(|record| Recorded {
            sequence: record.sequence,
            timestamp: record.timestamp,
        })
    }

    /// Mark all current events as committed. Called by repository after successful commit.
    pub fn mark_committed(&mut self) {
        self.committed_version = self.version;
    }

    /// Set metadata that will be attached to every subsequent event.
    ///
    /// Call this before invoking command methods to propagate context
    /// (correlation IDs, user info, trace spans) into the event stream.
    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) {
        self.metadata = metadata;
    }

    /// Set a single metadata key-value pair.
    pub fn set_meta(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert(key.into(), value.into());
    }

    /// Set the correlation ID for subsequent events.
    pub fn set_correlation_id(&mut self, id: impl Into<String>) {
        self.set_meta(CORRELATION_ID, id);
    }

    /// Set the causation ID for subsequent events.
    pub fn set_causation_id(&mut self, id: impl Into<String>) {
        self.set_meta(CAUSATION_ID, id);
    }

    /// Overwrite causation on every event not yet committed.
    ///
    /// The command ledger chooses the authoritative causation ID only after a
    /// reservation succeeds. A handler may have emitted events before then or
    /// replaced entity metadata, so merely configuring metadata for subsequent
    /// events is insufficient. The causal batch constructor calls this once at
    /// the final pre-commit boundary.
    #[cfg_attr(not(feature = "graphql"), allow(dead_code))]
    pub(crate) fn overwrite_new_event_causation_id(&mut self, id: &str) {
        let start = (self.committed_version - self.prefix_version) as usize;
        for event in &mut self.events[start..] {
            event.overwrite_causation_id(id);
        }
        for occurrence in &mut self.pending_domain_events {
            occurrence.overwrite_causation_id(id);
        }
        self.metadata
            .retain(|key, _| !key.eq_ignore_ascii_case(CAUSATION_ID));
        self.metadata
            .insert(CAUSATION_ID.to_string(), id.to_string());
    }

    /// Set W3C trace context for subsequent events.
    pub fn set_trace_context(&mut self, context: &TraceContext) {
        context.inject_map(&mut self.metadata);
    }

    /// Get the current W3C trace context.
    pub fn trace_context(&self) -> TraceContext {
        TraceContext::from_metadata(self.metadata.iter())
    }

    /// Get the current metadata context.
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Clear all metadata.
    pub fn clear_metadata(&mut self) {
        self.metadata.clear();
    }

    /// Record an event with a serializable payload.
    ///
    /// If serialization fails, the entity is left unchanged.
    /// Any metadata set on the entity is attached to the event.
    pub fn digest<T: serde::Serialize>(
        &mut self,
        name: impl Into<String>,
        payload: &T,
    ) -> SourcedResult {
        if self.replaying {
            return Ok(());
        }

        let bytes = BitcodePayloadCodec::encode(payload).map_err(EventRecordError::encode)?;
        let sequence = self.next_sequence();
        let mut record = EventRecord::new(name, bytes, sequence);
        if !self.metadata.is_empty() {
            record.metadata = self.metadata.clone();
        }
        self.push_new_event(record);
        Ok(())
    }

    /// Record a versioned event.
    ///
    /// If serialization fails, the entity is left unchanged.
    pub fn digest_v<T: serde::Serialize>(
        &mut self,
        name: impl Into<String>,
        version: u64,
        payload: &T,
    ) -> SourcedResult {
        if self.replaying {
            return Ok(());
        }

        let bytes = BitcodePayloadCodec::encode(payload).map_err(EventRecordError::encode)?;
        let sequence = self.next_sequence();
        let mut record = EventRecord::new_versioned(name, bytes, sequence, version);
        if !self.metadata.is_empty() {
            record.metadata = self.metadata.clone();
        }
        self.push_new_event(record);
        Ok(())
    }

    /// Record an event with no payload.
    pub fn digest_empty(&mut self, name: impl Into<String>) -> SourcedResult {
        self.digest(name, &())
    }

    /// The sequence number the next appended event will carry — the true stream
    /// length plus one. Counts the omitted snapshot prefix as well as the
    /// in-memory events, so tail-only entities still produce contiguous,
    /// non-colliding sequences.
    fn next_sequence(&self) -> u64 {
        self.prefix_version + self.events.len() as u64 + 1
    }

    fn push_new_event(&mut self, record: EventRecord) {
        self.events.push(record);
        self.version = self.prefix_version + self.events.len() as u64;
        self.timestamp = crate::time::now();
    }

    pub fn load_from_history(&mut self, history: Vec<EventRecord>) {
        self.events = history;
        self.prefix_version = 0;
        self.version = self.events.len() as u64;
        self.committed_version = self.version;
    }

    /// Load only the *tail* of a stream — the events after a known prefix that
    /// is not held in memory (e.g. events already folded into a snapshot).
    ///
    /// Unlike [`load_from_history`](Self::load_from_history), the in-memory
    /// `events` here are a suffix of the durable stream, so `version` and
    /// `committed_version` must be supplied explicitly rather than derived from
    /// `events.len()`. They reflect the true persisted stream position:
    /// `committed_version` is the optimistic-concurrency `expected_version` for
    /// the next commit, and `new_events()` slices relative to it.
    ///
    /// # Invariant
    ///
    /// `prefix_version` is the count of events covered by the omitted prefix.
    /// Callers must guarantee that every event in `tail` has
    /// `sequence > prefix_version` and that sequences are contiguous, so the
    /// resulting `version == prefix_version + tail.len()` equals the true
    /// `max(sequence)`. Snapshots satisfy this: `prefix_version` is the snapshot
    /// version and the tail is exactly `sequence > snapshot.version`.
    pub fn load_tail_from_history(&mut self, tail: Vec<EventRecord>, prefix_version: u64) {
        self.events = tail;
        self.prefix_version = prefix_version;
        self.version = prefix_version + self.events.len() as u64;
        self.committed_version = self.version;
    }

    pub fn rehydrate<F, E>(&mut self, mut apply: F) -> Result<(), E>
    where
        F: FnMut(&EventRecord) -> Result<(), E>,
    {
        let _guard = ReplayGuard::new(&mut self.replaying);

        for event in &self.events {
            apply(event)?;
        }

        Ok(())
    }

    pub fn is_replaying(&self) -> bool {
        self.replaying
    }

    pub(crate) fn set_replaying(&mut self, replaying: bool) {
        self.replaying = replaying;
    }

    fn capture_typed(
        &mut self,
        aggregate_type: &str,
        descriptor: DomainEventDescriptor,
        body: &impl Serialize,
    ) -> Result<DomainEventCaptureOutcome, DomainEventCaptureError> {
        if self.domain_event_poison.is_some() {
            return Err(DomainEventCaptureError::EntityAlreadyPoisoned);
        }
        let Some(causing_event) = self.events.last() else {
            return self
                .capture_failed(descriptor, DomainEventCaptureError::NoPendingAggregateEvent);
        };
        if self.version == self.committed_version || causing_event.sequence != self.version {
            return self
                .capture_failed(descriptor, DomainEventCaptureError::NoPendingAggregateEvent);
        }
        let ordinal_count = self
            .pending_domain_events
            .iter()
            .filter(|occurrence| occurrence.aggregate_sequence() == self.version)
            .count();
        let Ok(ordinal) = ordinal_count.try_into() else {
            return self.capture_failed(
                descriptor,
                DomainEventCaptureError::PublicationOrdinalOverflow,
            );
        };
        let envelope = DomainEventEnvelope {
            aggregate_type: aggregate_type.to_string(),
            aggregate_id: self.id.clone(),
            aggregate_sequence: self.version,
            publication_ordinal: ordinal,
            occurred_at: causing_event.timestamp,
            metadata: causing_event
                .metadata
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        };
        match DomainEventOccurrence::capture(descriptor.clone(), envelope, body) {
            Ok(occurrence) => {
                let id = occurrence.id().to_string();
                self.pending_domain_events.push(occurrence);
                Ok(DomainEventCaptureOutcome::Captured { id })
            }
            Err(error) => self.capture_failed(descriptor, error),
        }
    }

    fn capture_failed<T>(
        &mut self,
        descriptor: DomainEventDescriptor,
        error: DomainEventCaptureError,
    ) -> Result<T, DomainEventCaptureError> {
        if self.domain_event_poison.is_none() {
            let publication_ordinal = self
                .pending_domain_events
                .iter()
                .filter(|occurrence| occurrence.aggregate_sequence() == self.version)
                .count()
                .try_into()
                .unwrap_or(u32::MAX);
            self.domain_event_poison = Some(DomainEventCapturePoison {
                descriptor,
                aggregate_sequence: self.version,
                publication_ordinal,
                error: error.clone(),
            });
        }
        Err(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::ser::Error as _;
    use serde_json;

    #[derive(Clone)]
    struct FailingSerialize;

    impl Serialize for FailingSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(S::Error::custom("intentional serialization failure"))
        }
    }

    #[test]
    fn new() {
        let entity = Entity::new();
        assert_eq!(entity.id(), "");
        assert_eq!(entity.version(), 0);
        assert!(entity.events().is_empty());
        assert!(!entity.is_replaying());
        assert_eq!(entity.snapshot_version(), 0);
        assert_eq!(entity.committed_version(), 0);
    }

    #[test]
    fn digest() {
        let mut entity = Entity::new();
        entity.digest("test_event", &("arg1", "arg2")).unwrap();

        assert_eq!(entity.version(), 1);
        assert_eq!(entity.events().len(), 1);
        assert_eq!(entity.events()[0].event_name, "test_event");
        let decoded: (String, String) = entity.events()[0].decode().unwrap();
        assert_eq!(decoded, ("arg1".to_string(), "arg2".to_string()));
        assert_eq!(entity.events()[0].sequence, 1);
    }

    #[test]
    fn digest_returns_serialization_errors_without_mutating_entity() {
        let mut entity = Entity::new();

        let err = entity.digest("bad_event", &FailingSerialize).unwrap_err();

        assert!(err.message.contains("intentional serialization failure"));
        assert_eq!(entity.version(), 0);
        assert!(entity.events().is_empty());
    }

    #[test]
    fn digest_v_returns_serialization_errors_without_mutating_entity() {
        let mut entity = Entity::new();

        let err = entity
            .digest_v("bad_event", 2, &FailingSerialize)
            .unwrap_err();

        assert!(err.message.contains("intentional serialization failure"));
        assert_eq!(entity.version(), 0);
        assert!(entity.events().is_empty());
    }

    #[test]
    fn rehydrate() {
        let mut entity = Entity::new();
        entity.digest("test_event1", &"arg1").unwrap();
        entity.digest("test_event2", &"arg2").unwrap();

        let mut replayed = Vec::new();
        let result = entity.rehydrate(|event| {
            replayed.push(event.event_name.clone());
            Ok::<(), ()>(())
        });

        assert!(result.is_ok());
        assert_eq!(replayed, vec!["test_event1", "test_event2"]);
        assert!(!entity.is_replaying());
    }

    #[test]
    fn clone() {
        let entity = Entity::new();
        let cloned_entity = entity.clone();

        assert_eq!(entity.id(), cloned_entity.id());
        assert_eq!(entity.version(), cloned_entity.version());
        assert_eq!(entity.events(), cloned_entity.events());
        assert_eq!(entity.snapshot_version(), cloned_entity.snapshot_version());
        assert_eq!(
            entity.committed_version(),
            cloned_entity.committed_version()
        );
    }

    #[test]
    fn debug() {
        let entity = Entity::new();
        let debug_str = format!("{:?}", entity);

        assert!(debug_str.contains("Entity"));
        assert!(debug_str.contains("id: \"\""));
        assert!(debug_str.contains("version: 0"));
    }

    #[test]
    fn serialize_deserialize() {
        let mut entity = Entity::new();
        entity.digest("test_event1", &"arg1").unwrap();

        let serialized: String = serde_json::to_string(&entity).unwrap();
        let deserialized: Entity = serde_json::from_str(&serialized).unwrap();

        assert_eq!(entity.id(), deserialized.id());
        assert_eq!(entity.version(), deserialized.version());
        assert_eq!(entity.events(), deserialized.events());
        assert_eq!(entity.snapshot_version(), deserialized.snapshot_version());
        assert_eq!(entity.timestamp, deserialized.timestamp);
        // committed_version is serde(skip) — not serialized, defaults to 0
        assert_eq!(deserialized.committed_version(), 0);
    }

    #[test]
    fn replaying_state_blocks_changes() {
        let mut entity = Entity::new();
        entity.replaying = true;

        entity.digest("test_event", &"arg1").unwrap();
        assert!(entity.events().is_empty());
    }

    #[test]
    fn load_from_history_sets_committed_version() {
        let mut entity = Entity::new();
        assert_eq!(entity.committed_version(), 0);

        let mut source = Entity::new();
        source.digest("e1", &"a").unwrap();
        source.digest("e2", &"b").unwrap();

        entity.load_from_history(source.events().to_vec());
        assert_eq!(entity.version(), 2);
        assert_eq!(entity.committed_version(), 2);
        assert_eq!(entity.snapshot_version(), 0);
    }

    #[test]
    fn new_events_on_fresh_entity() {
        let mut entity = Entity::new();
        assert!(entity.new_events().is_empty());

        entity.digest("e1", &"a").unwrap();
        entity.digest("e2", &"b").unwrap();
        assert_eq!(entity.new_events().len(), 2);
        assert_eq!(entity.new_events()[0].event_name, "e1");
        assert_eq!(entity.new_events()[1].event_name, "e2");
    }

    #[test]
    fn new_events_after_load_and_digest() {
        let mut source = Entity::new();
        source.digest("e1", &"a").unwrap();
        source.digest("e2", &"b").unwrap();

        let mut entity = Entity::new();
        entity.load_from_history(source.events().to_vec());
        assert!(entity.new_events().is_empty());

        entity.digest("e3", &"c").unwrap();
        assert_eq!(entity.new_events().len(), 1);
        assert_eq!(entity.new_events()[0].event_name, "e3");
    }

    #[test]
    fn mark_committed_resets_new_events() {
        let mut entity = Entity::new();
        entity.digest("e1", &"a").unwrap();
        entity.digest("e2", &"b").unwrap();
        assert_eq!(entity.new_events().len(), 2);

        entity.mark_committed();
        assert!(entity.new_events().is_empty());
        assert_eq!(entity.committed_version(), 2);
        assert_eq!(entity.snapshot_version(), 0);
        assert_eq!(entity.version(), 2);
        assert_eq!(entity.events().len(), 2);
    }

    #[test]
    fn digest_propagates_metadata_to_event_record() {
        let mut entity = Entity::new();
        entity.set_correlation_id("req-abc");
        entity.set_causation_id("cmd-xyz");
        entity.set_meta("user_id", "u-42");

        entity.digest("e1", &"payload").unwrap();

        let record = &entity.events()[0];
        assert_eq!(record.correlation_id(), Some("req-abc"));
        assert_eq!(record.causation_id(), Some("cmd-xyz"));
        assert_eq!(record.meta("user_id"), Some("u-42"));
    }

    #[test]
    fn digest_propagates_trace_context_to_event_record() {
        let mut entity = Entity::new();
        let context = TraceContext {
            traceparent: Some(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
            ),
            tracestate: Some("vendor=value".to_string()),
        };
        entity.set_trace_context(&context);

        entity.digest("e1", &"payload").unwrap();

        let record = &entity.events()[0];
        assert_eq!(record.trace_context(), context);
    }

    #[test]
    fn digest_without_metadata_leaves_event_record_empty() {
        let mut entity = Entity::new();
        entity.digest("e1", &"payload").unwrap();

        let record = &entity.events()[0];
        assert!(record.metadata.is_empty());
        assert_eq!(record.correlation_id(), None);
    }

    #[test]
    fn metadata_is_transient_not_serialized() {
        let mut entity = Entity::new();
        entity.set_correlation_id("req-abc");
        entity.digest("e1", &"payload").unwrap();

        let serialized = serde_json::to_string(&entity).unwrap();
        let deserialized: Entity = serde_json::from_str(&serialized).unwrap();

        // Entity metadata is transient (serde skip), should be empty after round-trip
        assert!(deserialized.metadata().is_empty());
        // But the event record metadata was persisted
        assert_eq!(deserialized.events()[0].correlation_id(), Some("req-abc"));
    }

    #[test]
    fn clear_metadata_stops_propagation() {
        let mut entity = Entity::new();
        entity.set_correlation_id("req-abc");
        entity.digest("e1", &"first").unwrap();

        entity.clear_metadata();
        entity.digest("e2", &"second").unwrap();

        assert_eq!(entity.events()[0].correlation_id(), Some("req-abc"));
        assert!(entity.events()[1].metadata.is_empty());
    }

    #[test]
    fn final_causal_stamp_overwrites_only_uncommitted_events() {
        let mut entity = Entity::with_id("aggregate-1");
        entity.set_causation_id("original-command");
        entity.digest_empty("Created").unwrap();
        entity.mark_committed();

        entity.set_causation_id("handler-supplied");
        entity.set_meta("Causation_Id", "forged-case-alias");
        entity.digest_empty("Changed").unwrap();
        entity.overwrite_new_event_causation_id("ledger-causation");

        assert_eq!(entity.events()[0].causation_id(), Some("original-command"));
        assert_eq!(entity.events()[1].causation_id(), Some("ledger-causation"));
        assert_eq!(
            entity.events()[1]
                .metadata
                .keys()
                .filter(|key| key.eq_ignore_ascii_case(CAUSATION_ID))
                .count(),
            1
        );
        assert_eq!(
            entity.metadata().get(CAUSATION_ID).map(String::as_str),
            Some("ledger-causation")
        );
        assert_eq!(
            entity
                .metadata()
                .keys()
                .filter(|key| key.eq_ignore_ascii_case(CAUSATION_ID))
                .count(),
            1
        );
    }

    #[test]
    fn load_tail_records_true_version_without_full_history() {
        // A tail of 2 events after a snapshot prefix of 3: true version is 5,
        // even though only 2 events are held in memory.
        let mut entity = Entity::with_id("agg");
        entity.load_tail_from_history(
            vec![
                EventRecord::new("e4", vec![], 4),
                EventRecord::new("e5", vec![], 5),
            ],
            3,
        );

        assert_eq!(entity.version(), 5);
        assert_eq!(entity.committed_version(), 5);
        assert_eq!(entity.events().len(), 2);
        assert!(entity.new_events().is_empty());
    }

    #[test]
    fn digest_after_tail_load_continues_sequence_from_true_version() {
        // After loading a tail (prefix 3 + 2 events = version 5), the next
        // digested event must be sequence 6 — not 3 (events.len() + 1).
        let mut entity = Entity::with_id("agg");
        entity.load_tail_from_history(
            vec![
                EventRecord::new("e4", vec![], 4),
                EventRecord::new("e5", vec![], 5),
            ],
            3,
        );

        entity.digest("e6", &"payload").unwrap();

        assert_eq!(entity.version(), 6);
        assert_eq!(entity.events().last().unwrap().sequence, 6);
        // Only the freshly digested event is new (uncommitted).
        assert_eq!(entity.new_events().len(), 1);
        assert_eq!(entity.new_events()[0].event_name, "e6");
        assert_eq!(entity.new_events()[0].sequence, 6);
    }

    #[test]
    fn digest_on_empty_tail_load_continues_from_prefix() {
        // A current snapshot leaves an empty tail at the snapshot version. The
        // next event must continue from there, not restart at sequence 1.
        let mut entity = Entity::with_id("agg");
        entity.load_tail_from_history(Vec::new(), 5);
        assert_eq!(entity.version(), 5);
        assert!(entity.new_events().is_empty());

        entity.digest("e6", &"payload").unwrap();
        assert_eq!(entity.version(), 6);
        assert_eq!(entity.events()[0].sequence, 6);
        assert_eq!(entity.new_events().len(), 1);
    }

    #[test]
    fn snapshot_version_not_affected_by_load_or_commit() {
        let mut entity = Entity::new();
        assert_eq!(entity.snapshot_version(), 0);

        // digest doesn't touch snapshot_version
        entity.digest("e1", &"a").unwrap();
        entity.digest("e2", &"b").unwrap();
        assert_eq!(entity.snapshot_version(), 0);

        // mark_committed doesn't touch snapshot_version
        entity.mark_committed();
        assert_eq!(entity.snapshot_version(), 0);

        // load_from_history doesn't touch snapshot_version
        let mut entity2 = Entity::new();
        entity2.load_from_history(entity.events().to_vec());
        assert_eq!(entity2.snapshot_version(), 0);
        assert_eq!(entity2.committed_version(), 2);
    }
}
