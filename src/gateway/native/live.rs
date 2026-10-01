use crate::gateway::delivery::*;
use futures_util::{future::BoxFuture, stream::BoxStream, StreamExt};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

pub(super) type LiveSource = BoxStream<'static, Result<serde_json::Value, String>>;
pub(super) type LiveSourceFactory =
    Arc<dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<LiveSource, String>> + Send + Sync>;
#[derive(Clone)]
struct Input {
    admission: OriginAdmission,
    request: serde_json::Value,
    freshness: Option<FreshnessContext>,
    source: LiveSourceFactory,
}
struct Consumer {
    ticket: LiveTicket,
    input: Input,
    frames: mpsc::Sender<Arc<LiveFrame>>,
    reset: oneshot::Sender<&'static str>,
}
struct Group {
    key: LiveKey,
    initial: LiveKey,
    consumers: BTreeSet<u64>,
    history: VecDeque<Arc<LiveFrame>>,
    complete_history: bool,
    driver: Option<tokio::task::AbortHandle>,
}
impl Drop for Group {
    fn drop(&mut self) {
        if let Some(driver) = &self.driver {
            driver.abort();
        }
    }
}
struct State {
    registry: LiveRegistry,
    groups: BTreeMap<u64, Group>,
    consumers: BTreeMap<u64, Consumer>,
    next: u64,
    upstreams: u64,
    resets: u64,
    frames: u64,
    deduplicated: u64,
    handoffs: u64,
}
/// Native stream drivers are owned by these groups; removing the last consumer
/// aborts the upstream and releases its socket/stream, including on disconnect.
pub(super) struct NativeLive {
    state: Mutex<State>,
    limits: LiveLimits,
    started: Instant,
}
pub(super) struct LiveLease {
    id: u64,
    owner: Arc<NativeLive>,
    frames: mpsc::Receiver<Arc<LiveFrame>>,
    reset: oneshot::Receiver<&'static str>,
    expiry: u64,
    terminal: Option<&'static str>,
}
impl Drop for LiveLease {
    fn drop(&mut self) {
        self.owner.remove(self.id, "consumer_left");
    }
}
impl LiveLease {
    pub(super) async fn next(&mut self) -> Result<Option<Arc<LiveFrame>>, &'static str> {
        loop {
            let remaining = Duration::from_secs(self.expiry.saturating_sub(super::now()));
            if remaining.is_zero() {
                return Err("AUTH_EXPIRED");
            }
            if let Some(reason) = self.terminal {
                if reason != "complete" {
                    return Err(reason);
                }
                let frame = self.frames.recv().await;
                return if super::now() >= self.expiry {
                    Err("AUTH_EXPIRED")
                } else {
                    Ok(frame)
                };
            }
            tokio::select! { biased;
                reset = &mut self.reset => { self.terminal = Some(reset.unwrap_or("LIVE_RESET_REQUIRED")); },
                _ = tokio::time::sleep(remaining) => return Err("AUTH_EXPIRED"),
                frame = self.frames.recv() => return if super::now() >= self.expiry { Err("AUTH_EXPIRED") } else { Ok(frame) },
            }
        }
    }
    // A reset/expiry interrupts a blocked network queue. Normal completion
    // preserves all already-queued frames before the terminal complete packet.
    pub(super) async fn interrupted(&mut self) -> &'static str {
        let remaining = Duration::from_secs(self.expiry.saturating_sub(super::now()));
        if remaining.is_zero() {
            return "AUTH_EXPIRED";
        }
        if let Some(reason) = self.terminal {
            if reason != "complete" {
                return reason;
            }
            tokio::time::sleep(remaining).await;
            return "AUTH_EXPIRED";
        }
        tokio::select! {
            reset = &mut self.reset => {
                let reason = reset.unwrap_or("LIVE_RESET_REQUIRED"); self.terminal = Some(reason);
                if reason == "complete" { tokio::time::sleep(remaining).await; "AUTH_EXPIRED" } else { reason }
            },
            _ = tokio::time::sleep(remaining) => "AUTH_EXPIRED",
        }
    }
}

impl NativeLive {
    pub(super) fn new(limits: LiveLimits) -> Result<Arc<Self>, super::GatewayError> {
        let registry =
            LiveRegistry::new(limits).map_err(|_| super::GatewayError("invalid live limits"))?;
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                registry,
                groups: BTreeMap::new(),
                consumers: BTreeMap::new(),
                next: 0,
                upstreams: 0,
                resets: 0,
                frames: 0,
                deduplicated: 0,
                handoffs: 0,
            }),
            limits,
            started: Instant::now(),
        }))
    }
    pub(super) fn counts(&self) -> (usize, usize, u64, u64, u64, u64, u64) {
        self.state.lock().map_or((0, 0, 0, 0, 0, 0, 0), |s| {
            (
                s.groups.len(),
                s.consumers.len(),
                s.upstreams,
                s.resets,
                s.frames,
                s.deduplicated,
                s.handoffs,
            )
        })
    }
    pub(super) fn join(
        self: &Arc<Self>,
        admission: OriginAdmission,
        request: serde_json::Value,
        freshness: Option<FreshnessContext>,
        source: LiveSourceFactory,
    ) -> Result<LiveLease, DeliveryError> {
        let initial = LiveKey::admitted(&admission, &request, freshness.as_ref(), super::now())?;
        let input = Input {
            admission,
            request,
            freshness,
            source,
        };
        let (frames, receiver) = mpsc::channel(self.limits.queue_frames);
        let (reset, reset_receiver) = oneshot::channel();
        let mut state = self.state.lock().map_err(|_| DeliveryError::Unavailable)?;
        let now = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        state.registry.expire(now);
        let expired = state
            .groups
            .keys()
            .filter(|generation| !state.registry.contains_generation(**generation))
            .copied()
            .collect::<Vec<_>>();
        for generation in expired {
            end(&mut state, generation, "LIVE_RESET_REQUIRED");
        }
        state.next = state
            .next
            .checked_add(1)
            .ok_or(DeliveryError::Unavailable)?;
        let id = state.next;
        let existing = state
            .groups
            .iter()
            .find(|(_, group)| {
                group.initial.same_initial(&initial)
                    && group.complete_history
                    && group.consumers.len() < self.limits.consumers
            })
            .map(|(generation, group)| (*generation, group.key.clone()));
        let key = existing
            .map(|(_, key)| key)
            .unwrap_or_else(|| initial.fork(id));
        let (ticket, owner) = state.registry.join(key.clone(), now)?;
        let generation = ticket.generation();
        if owner {
            state.groups.insert(
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
        let group = state
            .groups
            .get_mut(&generation)
            .ok_or(DeliveryError::Unavailable)?;
        for frame in &group.history {
            if !frame.satisfies(&input.admission, input.freshness.as_ref()) {
                state.registry.leave(ticket);
                return Err(DeliveryError::Pending);
            }
            frames
                .try_send(frame.clone())
                .map_err(|_| DeliveryError::Unavailable)?;
        }
        group.consumers.insert(id);
        let expiry = input.admission.expires_at;
        state.consumers.insert(
            id,
            Consumer {
                ticket,
                input,
                frames,
                reset,
            },
        );
        drop(state);
        if owner {
            let weak = Arc::downgrade(self);
            let lifetime = self.limits.lifetime_ms;
            let driver = tokio::spawn(async move {
                let reason = tokio::time::timeout(
                    Duration::from_millis(lifetime),
                    drive(weak.clone(), generation),
                )
                .await
                .unwrap_or("LIVE_RESET_REQUIRED");
                if let Some(owner) = weak.upgrade() {
                    if let Ok(mut state) = owner.state.lock() {
                        end(&mut state, generation, reason);
                    }
                }
            });
            let handle = driver.abort_handle();
            // No detached owner: Group owns the abort handle, and the driver
            // retains only Weak so the coordinator can be dropped cleanly.
            if let Ok(mut state) = self.state.lock() {
                if let Some(group) = state.groups.get_mut(&generation) {
                    group.driver = Some(handle);
                } else {
                    handle.abort();
                }
            } else {
                handle.abort();
            }
        }
        Ok(LiveLease {
            id,
            owner: self.clone(),
            frames: receiver,
            reset: reset_receiver,
            expiry,
            terminal: None,
        })
    }
    fn remove(&self, id: u64, reason: &'static str) {
        if let Ok(mut state) = self.state.lock() {
            remove(&mut state, id, reason);
        }
    }
    fn input(&self, generation: u64) -> Option<Input> {
        let mut state = self.state.lock().ok()?;
        let group = state.groups.get(&generation)?;
        let mut input = group
            .consumers
            .iter()
            .filter_map(|id| state.consumers.get(id))
            .filter(|consumer| consumer.input.admission.expires_at > super::now())
            .max_by_key(|consumer| consumer.input.admission.expires_at)?
            .input
            .clone();
        // Reconnect under a remaining consumer's current credentials. The
        // origin handles replay/reset at the last observed proven cursor.
        if let Some(frame) = group.history.back() {
            if !input.request["extensions"].is_object() {
                input.request["extensions"] = serde_json::json!({});
            }
            if !input.request["extensions"]["distributed"].is_object() {
                input.request["extensions"]["distributed"] = serde_json::json!({});
            }
            input.request["extensions"]["distributed"]["resume"] = serde_json::json!({"cursors":frame.payload()["extensions"]["distributed"]["live"]["cursors"]});
        }
        state.upstreams = state.upstreams.saturating_add(1);
        Some(input)
    }
    fn emit(&self, generation: u64, input: &Input, payload: serde_json::Value) -> bool {
        let frame = match LiveFrame::from_origin(
            &input.admission,
            payload,
            None,
            self.limits.frame_bytes,
        ) {
            Ok(frame) => Arc::new(frame),
            Err(_) => {
                if let Ok(mut state) = self.state.lock() {
                    end(&mut state, generation, "LIVE_RESET_REQUIRED");
                }
                return false;
            }
        };
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        state.frames = state.frames.saturating_add(1);
        let Some(group) = state.groups.get_mut(&generation) else {
            return false;
        };
        if group
            .history
            .back()
            .is_some_and(|last| last.same_frame(&frame))
        {
            state.deduplicated = state.deduplicated.saturating_add(1);
            return true;
        }
        group.history.push_back(frame.clone());
        if group.history.len() > self.limits.history_frames {
            group.history.pop_front();
            group.complete_history = false;
        }
        let ids = group.consumers.iter().copied().collect::<Vec<_>>();
        for id in ids {
            let Some(consumer) = state.consumers.get(&id) else {
                continue;
            };
            let reason = if consumer.input.admission.expires_at <= super::now() {
                Some("AUTH_EXPIRED")
            } else if !frame.satisfies(&consumer.input.admission, consumer.input.freshness.as_ref())
            {
                Some("FRESHNESS_PENDING")
            } else if consumer.frames.try_send(frame.clone()).is_err() {
                Some("LIVE_RESET_REQUIRED")
            } else {
                None
            };
            if let Some(reason) = reason {
                remove(&mut state, id, reason);
            }
        }
        // Each replay's own frame is queued first. At equal proven cursor and
        // data, future frames can move to an existing operation without a gap.
        let Some(group) = state.groups.get(&generation) else {
            return false;
        };
        let target = state
            .groups
            .iter()
            .find(|(other, target)| {
                **other < generation
                    && target.key.same_operation(&group.key)
                    && target
                        .history
                        .back()
                        .is_some_and(|head| head.same_cursor(&frame))
            })
            .map(|(id, group)| (*id, group.key.clone()));
        if let Some((target, key)) = target {
            let ids = group.consumers.iter().copied().collect::<Vec<_>>();
            let now = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
            for id in ids {
                let Ok((ticket, new_owner)) = state.registry.join(key.clone(), now) else {
                    break;
                };
                if new_owner {
                    state.registry.leave(ticket);
                    break;
                }
                let Some(consumer) = state.consumers.get_mut(&id) else {
                    state.registry.leave(ticket);
                    continue;
                };
                let old = std::mem::replace(&mut consumer.ticket, ticket);
                state.registry.leave(old);
                if let Some(group) = state.groups.get_mut(&generation) {
                    group.consumers.remove(&id);
                }
                if let Some(group) = state.groups.get_mut(&target) {
                    group.consumers.insert(id);
                }
                state.handoffs = state.handoffs.saturating_add(1);
            }
            if state
                .groups
                .get(&generation)
                .is_some_and(|group| group.consumers.is_empty())
            {
                state.groups.remove(&generation);
                return false;
            }
        }
        true
    }
}
fn remove(state: &mut State, id: u64, reason: &'static str) {
    let Some(consumer) = state.consumers.remove(&id) else {
        return;
    };
    let generation = consumer.ticket.generation();
    if reason != "consumer_left" && reason != "complete" {
        state.resets = state.resets.saturating_add(1);
    }
    let _ = consumer.reset.send(reason);
    let last = state.registry.leave(consumer.ticket);
    if let Some(group) = state.groups.get_mut(&generation) {
        group.consumers.remove(&id);
    }
    if last {
        state.groups.remove(&generation);
    }
}
fn end(state: &mut State, generation: u64, reason: &'static str) {
    let ids = state
        .groups
        .get(&generation)
        .map(|group| group.consumers.iter().copied().collect::<Vec<_>>())
        .unwrap_or_default();
    for id in ids {
        remove(state, id, reason);
    }
    state.groups.remove(&generation);
}
async fn drive(owner: Weak<NativeLive>, generation: u64) -> &'static str {
    loop {
        let input = match owner.upgrade().and_then(|owner| owner.input(generation)) {
            Some(input) => input,
            None => return "AUTH_EXPIRED",
        };
        let expiry = Duration::from_secs(input.admission.expires_at.saturating_sub(super::now()));
        let run = async {
            let mut stream = (input.source)(input.request.clone()).await?;
            while let Some(frame) = stream.next().await {
                let frame = frame?;
                if !owner
                    .upgrade()
                    .is_some_and(|owner| owner.emit(generation, &input, frame))
                {
                    return Err("upstream no longer owned".into());
                }
            }
            Ok::<(), String>(())
        };
        match tokio::time::timeout(expiry, run).await {
            Ok(Ok(())) => return "complete",
            Ok(Err(_)) => return "LIVE_RESET_REQUIRED",
            Err(_) => continue, // Reauthenticate with a remaining unexpired consumer.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{FutureExt, StreamExt};
    use std::sync::atomic::{AtomicUsize, Ordering};
    fn request() -> serde_json::Value {
        serde_json::json!({"query":"subscription Watch { rows { title } }"})
    }
    fn admission(request: &serde_json::Value) -> OriginAdmission {
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
            validated_at: super::super::now(),
            expires_at: super::super::now() + 30,
            policy: SnapshotPolicy::Current,
        }
    }
    fn frame(position: u64, proof: &str) -> serde_json::Value {
        serde_json::json!({"data":{"rows":[{"title":"unchanged"}]},"extensions":{"distributed":{
            "protocolVersion":1,"schemaHash":"schema","authorizationGeneration":"policy","cacheScope":"alice","operation":"operation",
            "snapshot":{"recordsComplete":true,"indexesComparable":true,"records":[],"indexes":[{"projection":"rows","scopeToken":"scope","position":position.to_string()}],"observations":[proof]},
            "live":{"mode":"resumable","reset":false,"cursors":[{"projection":"rows","position":position.to_string(),"token":format!("token-{position}")}]}
        }}})
    }
    struct DropCount(Arc<AtomicUsize>);
    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn source() -> (
        LiveSourceFactory,
        mpsc::UnboundedSender<serde_json::Value>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let receiver = Arc::new(Mutex::new(Some(receiver)));
        let calls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let drop_count = dropped.clone();
        let source: LiveSourceFactory = Arc::new(move |_| {
            let receiver = receiver.lock().unwrap().take();
            let count = count.clone();
            let drop_count = drop_count.clone();
            async move {
                let receiver = receiver.ok_or_else(|| "fixture source consumed".to_owned())?;
                count.fetch_add(1, Ordering::SeqCst);
                let guard = DropCount(drop_count);
                Ok(futures_util::stream::unfold(
                    (receiver, guard),
                    |(mut receiver, guard)| async move {
                        receiver
                            .recv()
                            .await
                            .map(|frame| (Ok(frame), (receiver, guard)))
                    },
                )
                .boxed())
            }
            .boxed()
        });
        (source, sender, calls, dropped)
    }
    async fn wait(condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn hundred_live_leases_preserve_proof_and_last_leave() {
        let live = NativeLive::new(LiveLimits::default()).unwrap();
        let request = request();
        let admission = admission(&request);
        let (factory, sender, calls, dropped) = source();
        let mut leases = Vec::new();
        for _ in 0..100 {
            leases.push(
                live.join(admission.clone(), request.clone(), None, factory.clone())
                    .unwrap(),
            );
        }
        assert_eq!((live.counts().0, live.counts().1), (1, 100));
        wait(|| calls.load(Ordering::SeqCst) == 1).await;
        sender.send(frame(1, "first-proof")).unwrap();
        for lease in &mut leases {
            assert_eq!(
                lease.next().await.unwrap().unwrap().payload(),
                &frame(1, "first-proof")
            );
        }
        sender.send(frame(1, "first-proof")).unwrap();
        sender.send(frame(1, "new-confirmation")).unwrap();
        for lease in &mut leases {
            assert_eq!(
                lease.next().await.unwrap().unwrap().payload(),
                &frame(1, "new-confirmation")
            );
        }
        assert_eq!(
            live.counts().5,
            1,
            "only a full data-plus-proof duplicate is suppressed"
        );
        while leases.len() > 1 {
            leases.pop();
        }
        assert_eq!((live.counts().0, live.counts().1), (1, 1));
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        leases.clear();
        wait(|| dropped.load(Ordering::SeqCst) == 1).await;
        assert_eq!((live.counts().0, live.counts().1), (0, 0));
    }
    #[tokio::test]
    async fn independent_resume_handoff_keeps_prior_confirmation() {
        let live = NativeLive::new(LiveLimits::default()).unwrap();
        let request = request();
        let (first, first_sender, _, first_dropped) = source();
        let mut existing = live
            .join(admission(&request), request.clone(), None, first)
            .unwrap();
        first_sender.send(frame(1, "existing")).unwrap();
        existing.next().await.unwrap().unwrap();
        let mut replay = request.clone();
        replay["extensions"] = serde_json::json!({"distributed":{"resume":{"cursors":[{"projection":"rows","position":"0","token":"old"}]}}});
        let (second, second_sender, _, second_dropped) = source();
        let mut replaying = live.join(admission(&replay), replay, None, second).unwrap();
        assert_eq!(
            live.counts().0,
            2,
            "different resume cursor initially replays independently"
        );
        second_sender
            .send(frame(1, "replayed-command-confirmation"))
            .unwrap();
        assert_eq!(
            replaying.next().await.unwrap().unwrap().payload(),
            &frame(1, "replayed-command-confirmation")
        );
        wait(|| live.counts().0 == 1).await;
        assert_eq!(live.counts().6, 1);
        wait(|| second_dropped.load(Ordering::SeqCst) == 1).await;
        first_sender.send(frame(2, "next")).unwrap();
        assert_eq!(
            existing.next().await.unwrap().unwrap().payload(),
            &frame(2, "next")
        );
        assert_eq!(
            replaying.next().await.unwrap().unwrap().payload(),
            &frame(2, "next")
        );
        drop(existing);
        assert_eq!(live.counts().1, 1);
        assert_eq!(first_dropped.load(Ordering::SeqCst), 0);
        drop(replaying);
        wait(|| first_dropped.load(Ordering::SeqCst) == 1).await;
    }
    #[tokio::test]
    async fn slow_consumer_gets_explicit_reset_and_releases_origin() {
        let live = NativeLive::new(LiveLimits {
            queue_frames: 1,
            history_frames: 1,
            ..Default::default()
        })
        .unwrap();
        let request = request();
        let (factory, sender, _, dropped) = source();
        let mut lease = live
            .join(admission(&request), request, None, factory)
            .unwrap();
        sender.send(frame(1, "one")).unwrap();
        sender.send(frame(2, "two")).unwrap();
        wait(|| live.counts().0 == 0).await;
        assert_eq!(lease.next().await.unwrap_err(), "LIVE_RESET_REQUIRED");
        assert_eq!(live.counts().3, 1);
        wait(|| dropped.load(Ordering::SeqCst) == 1).await;
    }
    #[tokio::test]
    async fn expired_consumer_does_not_own_remaining_consumers_upstream() {
        let live = NativeLive::new(LiveLimits::default()).unwrap();
        let request = request();
        let mut early = admission(&request);
        early.expires_at = super::super::now() + 1;
        let (first, first_sender, first_calls, first_dropped) = source();
        let mut expires = live.join(early, request.clone(), None, first).unwrap();
        wait(|| first_calls.load(Ordering::SeqCst) == 1).await;
        first_sender.send(frame(1, "before-renewal")).unwrap();
        expires.next().await.unwrap();
        let (second, second_sender, second_calls, second_dropped) = source();
        let mut remaining = live
            .join(admission(&request), request, None, second)
            .unwrap();
        remaining.next().await.unwrap();
        assert_eq!(expires.next().await.unwrap_err(), "AUTH_EXPIRED");
        drop(expires);
        wait(|| second_calls.load(Ordering::SeqCst) == 1).await;
        wait(|| first_dropped.load(Ordering::SeqCst) == 1).await;
        second_sender.send(frame(2, "after-renewal")).unwrap();
        assert_eq!(
            remaining.next().await.unwrap().unwrap().payload(),
            &frame(2, "after-renewal")
        );
        assert_eq!((live.counts().0, live.counts().1), (1, 1));
        drop(remaining);
        wait(|| second_dropped.load(Ordering::SeqCst) == 1).await;
    }
}
