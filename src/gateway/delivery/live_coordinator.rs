//! Portable live sharing state machine. It owns groups, consumers, retained
//! history and safe handoff; adapters own the lock, clock, spawned upstream
//! driver and consumer queues, all reached through [`LiveRuntime`].
use super::{DeliveryError, FreshnessContext, LiveFrame, LiveKey, LiveLimits, LiveRegistry};
use super::{LiveTicket, OriginAdmission};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Why a live consumer stopped receiving frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LiveEnd {
    /// The upstream completed; frames already queued are still delivered.
    Complete,
    /// The consumer released its own lease.
    ConsumerLeft,
    /// The consumer's origin identity expired.
    AuthExpired,
    /// A frame could not prove this consumer's retained freshness minima.
    FreshnessPending,
    /// Slow consumer, rejected frame, upstream failure or deadline.
    ResetRequired,
}
impl LiveEnd {
    /// Wire code sent to the client for a terminal end.
    pub fn code(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::ConsumerLeft => "consumer_left",
            Self::AuthExpired => "AUTH_EXPIRED",
            Self::FreshnessPending => "FRESHNESS_PENDING",
            Self::ResetRequired => "LIVE_RESET_REQUIRED",
        }
    }
}

/// Runtime types an adapter supplies to [`LiveCoordinator`].
pub trait LiveRuntime {
    /// Shared handle to one validated frame (for example `Arc<LiveFrame>`).
    type Frame: Clone;
    /// One consumer's bounded queue plus its terminal signal.
    type Outlet: LiveOutlet<Self::Frame>;
    /// Per-consumer upstream factory; it carries that consumer's credentials.
    type Source: Clone;
    /// Running upstream driver. Dropping it must cancel the upstream.
    type Driver;
    /// The validated frame behind a handle.
    fn frame(frame: &Self::Frame) -> &LiveFrame;
}

/// A consumer's queue, written without waiting.
pub trait LiveOutlet<F> {
    /// Queue one frame; false when the queue is full or closed.
    fn offer(&mut self, frame: F) -> bool;
    /// Signal the terminal end; the outlet is consumed.
    fn close(self, end: LiveEnd);
}

/// One consumer's admitted request and the factory that can open its upstream.
#[derive(Clone)]
pub struct LiveInput<S> {
    /// Fresh origin admission for this consumer.
    pub admission: OriginAdmission,
    /// The consumer's subscription request.
    pub request: serde_json::Value,
    /// Retained client freshness minima, if any.
    pub freshness: Option<FreshnessContext>,
    /// Upstream factory under this consumer's credentials.
    pub source: S,
}

/// Result of a join. `start` means the caller must spawn the group's driver
/// and [`LiveCoordinator::attach`] it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiveJoin {
    /// Consumer id; pass to [`LiveCoordinator::leave`] when the lease ends.
    pub id: u64,
    /// Group generation the consumer joined.
    pub generation: u64,
    /// Whether this join created the group.
    pub start: bool,
}

/// Cumulative and current live sharing counters, without identifiers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LiveCounts {
    /// Active upstream groups.
    pub groups: usize,
    /// Active consumers.
    pub consumers: usize,
    /// Upstream source attempts.
    pub upstreams: u64,
    /// Consumers ended by anything but completion or their own leave.
    pub resets: u64,
    /// Accepted upstream frames.
    pub frames: u64,
    /// Exact duplicate frames suppressed.
    pub deduplicated: u64,
    /// Consumers moved from an independent replay onto an existing group.
    pub handoffs: u64,
}

struct Group<R: LiveRuntime> {
    key: LiveKey,
    initial: LiveKey,
    consumers: BTreeSet<u64>,
    history: VecDeque<R::Frame>,
    complete_history: bool,
    driver: Option<R::Driver>,
}
struct Consumer<R: LiveRuntime> {
    ticket: LiveTicket,
    input: LiveInput<R::Source>,
    outlet: R::Outlet,
}

/// Portable live group/consumer/history state machine. Every method is
/// synchronous and non-blocking; times are passed in by the adapter
/// (`now_ms` monotonic milliseconds, `now` Unix seconds).
pub struct LiveCoordinator<R: LiveRuntime> {
    limits: LiveLimits,
    registry: LiveRegistry,
    groups: BTreeMap<u64, Group<R>>,
    consumers: BTreeMap<u64, Consumer<R>>,
    next: u64,
    counts: LiveCounts,
}

impl<R: LiveRuntime> LiveCoordinator<R> {
    /// Allocate empty state with validated limits.
    pub fn new(limits: LiveLimits) -> Result<Self, DeliveryError> {
        Ok(Self {
            limits,
            registry: LiveRegistry::new(limits)?,
            groups: BTreeMap::new(),
            consumers: BTreeMap::new(),
            next: 0,
            counts: LiveCounts::default(),
        })
    }
    /// The validated limits.
    pub fn limits(&self) -> LiveLimits {
        self.limits
    }
    /// Current and cumulative counters.
    pub fn counts(&self) -> LiveCounts {
        LiveCounts {
            groups: self.groups.len(),
            consumers: self.consumers.len(),
            ..self.counts
        }
    }
    /// Admit a consumer: join an exact-initial group whose history is
    /// complete, or start an independent replay generation. Retained history
    /// is offered to the outlet first; a frame this consumer's minima reject
    /// fails the join with `Pending`.
    pub fn join(
        &mut self,
        input: LiveInput<R::Source>,
        mut outlet: R::Outlet,
        now_ms: u64,
        now: u64,
    ) -> Result<LiveJoin, DeliveryError> {
        let initial = LiveKey::admitted(
            &input.admission,
            &input.request,
            input.freshness.as_ref(),
            now,
        )?;
        self.expire(now_ms);
        let id = self.next.checked_add(1).ok_or(DeliveryError::Unavailable)?;
        self.next = id;
        let key = self
            .groups
            .values()
            .find(|group| {
                group.initial.same_initial(&initial)
                    && group.complete_history
                    && group.consumers.len() < self.limits.consumers
            })
            .map_or_else(|| initial.fork(id), |group| group.key.clone());
        let (ticket, start) = self.registry.join(key.clone(), now_ms)?;
        let generation = ticket.generation();
        if start {
            self.groups.insert(
                generation,
                Group {
                    key,
                    initial,
                    consumers: BTreeSet::new(),
                    history: VecDeque::new(),
                    complete_history: true,
                    driver: None,
                },
            );
        }
        let Some(group) = self.groups.get_mut(&generation) else {
            self.registry.leave(ticket);
            return Err(DeliveryError::Unavailable);
        };
        for frame in &group.history {
            let error = if !R::frame(frame).satisfies(&input.admission, input.freshness.as_ref()) {
                DeliveryError::Pending
            } else if !outlet.offer(frame.clone()) {
                DeliveryError::Unavailable
            } else {
                continue;
            };
            // A joiner of an existing group: others keep it alive.
            self.registry.leave(ticket);
            return Err(error);
        }
        group.consumers.insert(id);
        self.consumers.insert(
            id,
            Consumer {
                ticket,
                input,
                outlet,
            },
        );
        Ok(LiveJoin {
            id,
            generation,
            start,
        })
    }
    /// Hand the group its running driver. A group that already ended drops
    /// it at once, which cancels the upstream.
    pub fn attach(&mut self, generation: u64, driver: R::Driver) {
        if let Some(group) = self.groups.get_mut(&generation) {
            group.driver = Some(driver);
        }
    }
    /// The consumer released its lease.
    pub fn leave(&mut self, id: u64) {
        self.remove(id, LiveEnd::ConsumerLeft);
    }
    /// Input for (re)opening the group's upstream: the remaining unexpired
    /// consumer with the latest expiry, resuming at the last retained cursor.
    /// None means no consumer can authorize the upstream any more.
    pub fn upstream(&mut self, generation: u64, now: u64) -> Option<LiveInput<R::Source>> {
        let group = self.groups.get(&generation)?;
        let mut input = group
            .consumers
            .iter()
            .filter_map(|id| self.consumers.get(id))
            .filter(|consumer| consumer.input.admission.expires_at > now)
            .max_by_key(|consumer| consumer.input.admission.expires_at)?
            .input
            .clone();
        if let Some(frame) = group.history.back() {
            let cursors =
                R::frame(frame).payload()["extensions"]["distributed"]["live"]["cursors"].clone();
            if !input.request["extensions"].is_object() {
                input.request["extensions"] = serde_json::json!({});
            }
            if !input.request["extensions"]["distributed"].is_object() {
                input.request["extensions"]["distributed"] = serde_json::json!({});
            }
            input.request["extensions"]["distributed"]["resume"] =
                serde_json::json!({ "cursors": cursors });
        }
        self.counts.upstreams = self.counts.upstreams.saturating_add(1);
        Some(input)
    }
    /// Fan out one upstream frame. `None` is a frame the adapter rejected
    /// (unprovable, oversized or over budget) and ends the group. False means
    /// the group no longer exists and its driver must stop.
    pub fn emit(
        &mut self,
        generation: u64,
        frame: Option<R::Frame>,
        now_ms: u64,
        now: u64,
    ) -> bool {
        let Some(frame) = frame else {
            self.end(generation, LiveEnd::ResetRequired);
            return false;
        };
        self.counts.frames = self.counts.frames.saturating_add(1);
        let Some(group) = self.groups.get_mut(&generation) else {
            return false;
        };
        if group
            .history
            .back()
            .is_some_and(|last| R::frame(last).same_frame(R::frame(&frame)))
        {
            self.counts.deduplicated = self.counts.deduplicated.saturating_add(1);
            return true;
        }
        group.history.push_back(frame.clone());
        if group.history.len() > self.limits.history_frames {
            group.history.pop_front();
            group.complete_history = false;
        }
        for id in group.consumers.iter().copied().collect::<Vec<_>>() {
            let Some(consumer) = self.consumers.get_mut(&id) else {
                continue;
            };
            let end = if consumer.input.admission.expires_at <= now {
                LiveEnd::AuthExpired
            } else if !R::frame(&frame)
                .satisfies(&consumer.input.admission, consumer.input.freshness.as_ref())
            {
                LiveEnd::FreshnessPending
            } else if !consumer.outlet.offer(frame.clone()) {
                LiveEnd::ResetRequired
            } else {
                continue;
            };
            self.remove(id, end);
        }
        self.hand_off(generation, &frame, now_ms)
    }
    // Each replay's own frame is queued first. At an equal proven cursor and
    // data, future frames can come from an older group of the same operation
    // without a gap, so the replay's consumers move there.
    fn hand_off(&mut self, generation: u64, frame: &R::Frame, now_ms: u64) -> bool {
        let Some(group) = self.groups.get(&generation) else {
            return false;
        };
        let Some((target, key)) = self
            .groups
            .range(..generation)
            .find(|(_, target)| {
                target.key.same_operation(&group.key)
                    && target
                        .history
                        .back()
                        .is_some_and(|head| R::frame(head).same_cursor(R::frame(frame)))
            })
            .map(|(id, target)| (*id, target.key.clone()))
        else {
            return true;
        };
        for id in group.consumers.iter().copied().collect::<Vec<_>>() {
            let Ok((ticket, created)) = self.registry.join(key.clone(), now_ms) else {
                break;
            };
            if created {
                // The target expired meanwhile; nothing to move onto.
                self.registry.leave(ticket);
                break;
            }
            let Some(consumer) = self.consumers.get_mut(&id) else {
                self.registry.leave(ticket);
                continue;
            };
            let old = std::mem::replace(&mut consumer.ticket, ticket);
            self.registry.leave(old);
            if let Some(group) = self.groups.get_mut(&generation) {
                group.consumers.remove(&id);
            }
            if let Some(group) = self.groups.get_mut(&target) {
                group.consumers.insert(id);
            }
            self.counts.handoffs = self.counts.handoffs.saturating_add(1);
        }
        if self
            .groups
            .get(&generation)
            .is_some_and(|group| group.consumers.is_empty())
        {
            self.groups.remove(&generation);
            return false;
        }
        true
    }
    /// End every consumer of a group and drop it (cancelling its driver).
    pub fn end(&mut self, generation: u64, end: LiveEnd) {
        let ids = self
            .groups
            .get(&generation)
            .map(|group| group.consumers.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        for id in ids {
            self.remove(id, end);
        }
        self.groups.remove(&generation);
    }
    // Registry deadlines are the group lifetime; an expired group resets.
    fn expire(&mut self, now_ms: u64) {
        self.registry.expire(now_ms);
        let expired = self
            .groups
            .keys()
            .filter(|generation| !self.registry.contains_generation(**generation))
            .copied()
            .collect::<Vec<_>>();
        for generation in expired {
            self.end(generation, LiveEnd::ResetRequired);
        }
    }
    fn remove(&mut self, id: u64, end: LiveEnd) {
        let Some(consumer) = self.consumers.remove(&id) else {
            return;
        };
        let generation = consumer.ticket.generation();
        if !matches!(end, LiveEnd::ConsumerLeft | LiveEnd::Complete) {
            self.counts.resets = self.counts.resets.saturating_add(1);
        }
        consumer.outlet.close(end);
        let last = self.registry.leave(consumer.ticket);
        if let Some(group) = self.groups.get_mut(&generation) {
            group.consumers.remove(&id);
        }
        if last {
            self.groups.remove(&generation);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{OperationKey, OriginIdentity, SnapshotPolicy};
    use super::*;
    use std::{
        cell::{Cell, RefCell},
        collections::BTreeMap,
        rc::Rc,
    };

    const NOW: u64 = 1_000_000;

    /// Everything the state machine did, observed through its runtime seam.
    #[derive(Default)]
    struct Effects {
        queued: BTreeMap<&'static str, Vec<serde_json::Value>>,
        ended: BTreeMap<&'static str, LiveEnd>,
        cancelled: usize,
    }
    type Log = Rc<RefCell<Effects>>;
    struct Test;
    struct Outlet {
        name: &'static str,
        capacity: usize,
        log: Log,
    }
    impl LiveOutlet<Rc<LiveFrame>> for Outlet {
        fn offer(&mut self, frame: Rc<LiveFrame>) -> bool {
            let mut log = self.log.borrow_mut();
            let queue = log.queued.entry(self.name).or_default();
            if queue.len() >= self.capacity {
                return false;
            }
            queue.push(frame.payload().clone());
            true
        }
        fn close(self, end: LiveEnd) {
            self.log.borrow_mut().ended.insert(self.name, end);
        }
    }
    struct Driver(Log);
    impl Drop for Driver {
        fn drop(&mut self) {
            self.0.borrow_mut().cancelled += 1;
        }
    }
    impl LiveRuntime for Test {
        type Frame = Rc<LiveFrame>;
        type Outlet = Outlet;
        type Source = &'static str;
        type Driver = Driver;
        fn frame(frame: &Rc<LiveFrame>) -> &LiveFrame {
            frame
        }
    }

    fn request() -> serde_json::Value {
        serde_json::json!({"query":"subscription Watch { rows { title } }"})
    }
    fn resume(position: u64) -> serde_json::Value {
        let mut request = request();
        request["extensions"] = serde_json::json!({"distributed":{"resume":{"cursors":[
            {"projection":"rows","position":position.to_string(),"token":"old"}]}}});
        request
    }
    fn admission(request: &serde_json::Value, expires_at: u64) -> OriginAdmission {
        let identity = OriginIdentity {
            application: "app".into(),
            endpoint: "origin".into(),
            schema_hash: "schema".into(),
            protocol_hash: "protocol".into(),
            authorization_generation: "policy".into(),
            cache_scope: "alice".into(),
        };
        OriginAdmission {
            key: OperationKey::from_origin(&identity, request).unwrap(),
            identity,
            operation: "operation".into(),
            validator: "v1".into(),
            validated_at: NOW,
            expires_at,
            policy: SnapshotPolicy::Current,
        }
    }
    fn payload(position: u64, proof: &str) -> serde_json::Value {
        serde_json::json!({"data":{"rows":[{"title":"unchanged"}]},"extensions":{"distributed":{
            "protocolVersion":1,"schemaHash":"schema","authorizationGeneration":"policy","cacheScope":"alice","operation":"operation",
            "snapshot":{"recordsComplete":true,"indexesComparable":true,"records":[],"indexes":[{"projection":"rows","scopeToken":"scope","position":position.to_string()}],"observations":[proof]},
            "live":{"mode":"resumable","reset":false,"cursors":[{"projection":"rows","position":position.to_string(),"token":format!("token-{position}")}]}
        }}})
    }
    fn frame(position: u64, proof: &str) -> Option<Rc<LiveFrame>> {
        let request = request();
        Some(Rc::new(
            LiveFrame::from_origin(
                &admission(&request, NOW + 30),
                payload(position, proof),
                None,
                1 << 20,
            )
            .unwrap(),
        ))
    }

    struct Harness {
        live: LiveCoordinator<Test>,
        log: Log,
        now_ms: Cell<u64>,
    }
    impl Harness {
        fn new(limits: LiveLimits) -> Self {
            Self {
                live: LiveCoordinator::new(limits).unwrap(),
                log: Log::default(),
                now_ms: Cell::new(0),
            }
        }
        fn join_as(
            &mut self,
            name: &'static str,
            request: serde_json::Value,
            expires_at: u64,
            capacity: usize,
        ) -> Result<LiveJoin, DeliveryError> {
            let input = LiveInput {
                admission: admission(&request, expires_at),
                request,
                freshness: None,
                source: name,
            };
            let outlet = Outlet {
                name,
                capacity,
                log: self.log.clone(),
            };
            let joined = self.live.join(input, outlet, self.now_ms.get(), NOW)?;
            if joined.start {
                self.live
                    .attach(joined.generation, Driver(self.log.clone()));
            }
            Ok(joined)
        }
        fn join(&mut self, name: &'static str) -> LiveJoin {
            self.join_as(name, request(), NOW + 30, 16).unwrap()
        }
        fn emit(&mut self, generation: u64, frame: Option<Rc<LiveFrame>>) -> bool {
            self.live.emit(generation, frame, self.now_ms.get(), NOW)
        }
        fn queued(&self, name: &str) -> Vec<serde_json::Value> {
            self.log
                .borrow()
                .queued
                .get(name)
                .cloned()
                .unwrap_or_default()
        }
        fn ended(&self, name: &str) -> Option<LiveEnd> {
            self.log.borrow().ended.get(name).copied()
        }
        fn cancelled(&self) -> usize {
            self.log.borrow().cancelled
        }
    }

    #[test]
    fn many_consumers_share_one_upstream_and_only_exact_duplicates_are_suppressed() {
        let mut h = Harness::new(LiveLimits::default());
        let names: Vec<&'static str> = (0..100).map(|i| &*format!("c{i}").leak()).collect();
        let joins: Vec<_> = names.iter().map(|name| h.join(name)).collect();
        assert!(joins[0].start);
        assert!(joins[1..]
            .iter()
            .all(|j| !j.start && j.generation == joins[0].generation));
        assert_eq!(
            (h.live.counts().groups, h.live.counts().consumers),
            (1, 100)
        );
        let generation = joins[0].generation;
        assert!(h.emit(generation, frame(1, "first-proof")));
        assert!(h.emit(generation, frame(1, "first-proof")));
        assert!(h.emit(generation, frame(1, "new-confirmation")));
        for name in &names {
            assert_eq!(
                h.queued(name),
                vec![payload(1, "first-proof"), payload(1, "new-confirmation")]
            );
        }
        assert_eq!(
            h.live.counts().deduplicated,
            1,
            "a new proof is a new frame"
        );
        for join in &joins[1..] {
            h.live.leave(join.id);
        }
        assert_eq!((h.live.counts().groups, h.live.counts().consumers), (1, 1));
        assert_eq!(h.cancelled(), 0);
        h.live.leave(joins[0].id);
        assert_eq!((h.live.counts().groups, h.live.counts().consumers), (0, 0));
        assert_eq!(h.cancelled(), 1, "the last leave cancels the upstream");
        assert_eq!(h.live.counts().resets, 0);
    }

    #[test]
    fn a_late_joiner_replays_retained_history_until_it_is_incomplete() {
        let mut h = Harness::new(LiveLimits {
            history_frames: 2,
            ..Default::default()
        });
        let first = h.join("first");
        h.emit(first.generation, frame(1, "a"));
        h.emit(first.generation, frame(2, "b"));
        let late = h.join("late");
        assert_eq!(late.generation, first.generation);
        assert_eq!(h.queued("late"), vec![payload(1, "a"), payload(2, "b")]);
        h.emit(first.generation, frame(3, "c"));
        // History lost frame 1, so an exact initial join can no longer share it.
        let fresh = h.join("fresh");
        assert!(fresh.start);
        assert_ne!(fresh.generation, first.generation);
        assert!(h.queued("fresh").is_empty());
    }

    #[test]
    fn a_joiner_whose_queue_cannot_take_history_is_refused_without_a_leaked_slot() {
        let mut h = Harness::new(LiveLimits::default());
        let first = h.join("first");
        h.emit(first.generation, frame(1, "a"));
        h.emit(first.generation, frame(2, "b"));
        assert_eq!(
            h.join_as("tiny", request(), NOW + 30, 1),
            Err(DeliveryError::Unavailable)
        );
        assert_eq!(h.live.counts().consumers, 1);
        h.live.leave(first.id);
        assert_eq!(
            h.live.counts().groups,
            0,
            "the refused joiner held no ticket"
        );
        assert_eq!(h.cancelled(), 1);
    }

    #[test]
    fn an_independent_replay_hands_off_at_an_equal_cursor() {
        let mut h = Harness::new(LiveLimits::default());
        let existing = h.join("existing");
        h.emit(existing.generation, frame(1, "existing"));
        let replay = h.join_as("replay", resume(0), NOW + 30, 16).unwrap();
        assert!(
            replay.start,
            "a different resume cursor replays independently"
        );
        assert_eq!(h.live.counts().groups, 2);
        // The replay's own confirmation reaches it before the handoff.
        assert!(!h.emit(replay.generation, frame(1, "replayed-confirmation")));
        assert_eq!(
            h.queued("replay"),
            vec![payload(1, "replayed-confirmation")]
        );
        assert_eq!(h.live.counts().groups, 1);
        assert_eq!(h.live.counts().handoffs, 1);
        assert_eq!(h.cancelled(), 1, "the emptied replay's driver is cancelled");
        assert!(h.emit(existing.generation, frame(2, "next")));
        assert_eq!(h.queued("existing").last(), Some(&payload(2, "next")));
        assert_eq!(h.queued("replay").last(), Some(&payload(2, "next")));
        h.live.leave(existing.id);
        assert_eq!(
            h.cancelled(),
            1,
            "the moved consumer keeps the target alive"
        );
        h.live.leave(replay.id);
        assert_eq!(h.cancelled(), 2);
    }

    #[test]
    fn different_data_at_the_same_cursor_does_not_hand_off() {
        let mut h = Harness::new(LiveLimits::default());
        let existing = h.join("existing");
        h.emit(existing.generation, frame(1, "existing"));
        let replay = h.join_as("replay", resume(0), NOW + 30, 16).unwrap();
        let mut other = payload(1, "x");
        other["data"]["rows"][0]["title"] = "changed".into();
        let request = request();
        let other =
            LiveFrame::from_origin(&admission(&request, NOW + 30), other, None, 1 << 20).unwrap();
        assert!(h.emit(replay.generation, Some(Rc::new(other))));
        assert_eq!((h.live.counts().groups, h.live.counts().handoffs), (2, 0));
    }

    #[test]
    fn a_slow_consumer_is_reset_and_its_upstream_released() {
        let mut h = Harness::new(LiveLimits {
            queue_frames: 1,
            history_frames: 1,
            ..Default::default()
        });
        let slow = h.join_as("slow", request(), NOW + 30, 1).unwrap();
        assert!(h.emit(slow.generation, frame(1, "one")));
        assert!(!h.emit(slow.generation, frame(2, "two")));
        assert_eq!(h.ended("slow"), Some(LiveEnd::ResetRequired));
        assert_eq!((h.live.counts().groups, h.live.counts().resets), (0, 1));
        assert_eq!(h.cancelled(), 1);
    }

    #[test]
    fn expired_consumers_end_and_never_authorize_the_upstream() {
        let mut h = Harness::new(LiveLimits::default());
        let early = h.join_as("early", request(), NOW + 1, 16).unwrap();
        let later = h.join_as("later", request(), NOW + 30, 16).unwrap();
        assert_eq!(
            h.live.upstream(early.generation, NOW).unwrap().source,
            "later"
        );
        // At expiry the upstream reconnects under the remaining consumer.
        assert_eq!(
            h.live.upstream(early.generation, NOW + 1).unwrap().source,
            "later"
        );
        assert!(h.live.emit(early.generation, frame(2, "after"), 0, NOW + 1));
        assert_eq!(h.ended("early"), Some(LiveEnd::AuthExpired));
        assert_eq!(h.queued("later"), vec![payload(2, "after")]);
        let resumed = h.live.upstream(later.generation, NOW + 1).unwrap();
        assert_eq!(
            resumed.request["extensions"]["distributed"]["resume"]["cursors"],
            payload(2, "after")["extensions"]["distributed"]["live"]["cursors"],
            "a reconnect resumes at the last retained cursor"
        );
        assert!(h.live.upstream(later.generation, NOW + 30).is_none());
        assert_eq!(h.live.counts().upstreams, 3);
    }

    #[test]
    fn a_rejected_frame_resets_the_whole_group() {
        let mut h = Harness::new(LiveLimits::default());
        let a = h.join("a");
        h.join("b");
        assert!(!h.emit(a.generation, None));
        assert_eq!(h.ended("a"), Some(LiveEnd::ResetRequired));
        assert_eq!(h.ended("b"), Some(LiveEnd::ResetRequired));
        assert_eq!((h.live.counts().groups, h.live.counts().resets), (0, 2));
        assert_eq!(h.cancelled(), 1);
    }

    #[test]
    fn completion_ends_consumers_without_counting_a_reset() {
        let mut h = Harness::new(LiveLimits::default());
        let a = h.join("a");
        h.live.end(a.generation, LiveEnd::Complete);
        assert_eq!(h.ended("a"), Some(LiveEnd::Complete));
        assert_eq!(h.live.counts().resets, 0);
        assert_eq!(h.cancelled(), 1);
    }

    #[test]
    fn the_group_lifetime_resets_its_consumers_on_the_next_join() {
        let mut h = Harness::new(LiveLimits {
            lifetime_ms: 1000,
            ..Default::default()
        });
        let old = h.join("old");
        h.now_ms.set(1000);
        let new = h.join("new");
        assert_eq!(h.ended("old"), Some(LiveEnd::ResetRequired));
        assert!(new.start);
        assert_ne!(new.generation, old.generation);
        assert_eq!(h.cancelled(), 1);
    }

    #[test]
    fn a_full_registry_refuses_to_share() {
        let mut h = Harness::new(LiveLimits {
            groups: 1,
            ..Default::default()
        });
        h.join("shared");
        assert_eq!(
            h.join_as("replay", resume(0), NOW + 30, 16),
            Err(DeliveryError::Unavailable)
        );
        assert_eq!(h.live.counts().consumers, 1);
    }

    #[test]
    fn a_group_that_ended_before_its_driver_attached_cancels_it() {
        let mut h = Harness::new(LiveLimits::default());
        let a = h.join("a");
        h.live.end(a.generation, LiveEnd::ResetRequired);
        assert_eq!(h.cancelled(), 1);
        h.live.attach(a.generation, Driver(h.log.clone()));
        assert_eq!(h.cancelled(), 2);
    }
}
