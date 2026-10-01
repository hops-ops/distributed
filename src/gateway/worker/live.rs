use crate::gateway::delivery::*;
use futures_channel::{mpsc, oneshot};
use futures_util::future::{select, AbortHandle, Abortable, Either};
use futures_util::{future::LocalBoxFuture, stream::LocalBoxStream, StreamExt};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet, VecDeque},
    rc::{Rc, Weak},
    time::Duration,
};

/// The charge follows the shared frame through queues, history and handoff.
pub(super) struct WorkerFrame {
    frame: LiveFrame,
    bytes: usize,
    used: Rc<Cell<usize>>,
}
impl std::ops::Deref for WorkerFrame {
    type Target = LiveFrame;
    fn deref(&self) -> &LiveFrame {
        &self.frame
    }
}
impl Drop for WorkerFrame {
    fn drop(&mut self) {
        self.used.set(self.used.get().saturating_sub(self.bytes));
    }
}
pub(super) type LiveSource = LocalBoxStream<'static, Result<serde_json::Value, String>>;
pub(super) type LiveSourceFactory =
    Rc<dyn Fn(serde_json::Value) -> LocalBoxFuture<'static, Result<LiveSource, String>>>;
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
    frames: mpsc::Sender<Rc<WorkerFrame>>,
    reset: oneshot::Sender<&'static str>,
}
struct Group {
    key: LiveKey,
    initial: LiveKey,
    consumers: BTreeSet<u64>,
    history: VecDeque<Rc<WorkerFrame>>,
    complete_history: bool,
    driver: Option<AbortHandle>,
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
/// Local stream drivers are owned by these groups; removing the last consumer
/// aborts the upstream and releases its socket/stream, including on disconnect.
pub(super) struct WorkerLive {
    state: RefCell<State>,
    limits: LiveLimits,
    payload_limit: usize,
    payload_used: Rc<Cell<usize>>,
    started: u64,
}
pub(super) struct LiveLease {
    id: u64,
    owner: Rc<WorkerLive>,
    frames: mpsc::Receiver<Rc<WorkerFrame>>,
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
    pub(super) async fn next(&mut self) -> Result<Option<Rc<WorkerFrame>>, &'static str> {
        loop {
            let remaining = Duration::from_secs(self.expiry.saturating_sub(super::now()));
            if remaining.is_zero() {
                return Err("AUTH_EXPIRED");
            }
            if let Some(reason) = self.terminal {
                if reason != "complete" {
                    return Err(reason);
                }
                let frame = self.frames.next().await;
                return if super::now() >= self.expiry {
                    Err("AUTH_EXPIRED")
                } else {
                    Ok(frame)
                };
            }
            enum Event {
                Reset(&'static str),
                Frame(Option<Rc<WorkerFrame>>),
            }
            let next = async {
                match select(Box::pin(&mut self.reset), Box::pin(self.frames.next())).await {
                    Either::Left((reason, _)) => {
                        Event::Reset(reason.unwrap_or("LIVE_RESET_REQUIRED"))
                    }
                    Either::Right((frame, _)) => Event::Frame(frame),
                }
            };
            match super::timer::deadline(remaining, next).await {
                Err(_) => return Err("AUTH_EXPIRED"),
                Ok(Event::Reset(reason)) => self.terminal = Some(reason),
                Ok(Event::Frame(frame)) => {
                    return if super::now() >= self.expiry {
                        Err("AUTH_EXPIRED")
                    } else {
                        Ok(frame)
                    }
                }
            }
        }
    }
    pub(super) async fn interrupted(&mut self) -> &'static str {
        let remaining = Duration::from_secs(self.expiry.saturating_sub(super::now()));
        if remaining.is_zero() {
            return "AUTH_EXPIRED";
        }
        if let Some(reason) = self.terminal {
            if reason != "complete" {
                return reason;
            }
            let _ = super::timer::Timer::new(remaining.as_millis() as u64).await;
            return "AUTH_EXPIRED";
        }
        match super::timer::deadline(remaining, &mut self.reset).await {
            Ok(reason) => {
                let reason = reason.unwrap_or("LIVE_RESET_REQUIRED");
                self.terminal = Some(reason);
                if reason == "complete" {
                    let _ = super::timer::Timer::new(remaining.as_millis() as u64).await;
                    "AUTH_EXPIRED"
                } else {
                    reason
                }
            }
            Err(_) => "AUTH_EXPIRED",
        }
    }
}

impl WorkerLive {
    pub(super) fn new(
        limits: LiveLimits,
        payload_limit: usize,
    ) -> Result<Rc<Self>, crate::gateway::GatewayError> {
        let registry = LiveRegistry::new(limits)
            .map_err(|_| crate::gateway::GatewayError("invalid live limits"))?;
        Ok(Rc::new(Self {
            state: RefCell::new(State {
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
            payload_limit,
            payload_used: Rc::new(Cell::new(0)),
            started: worker::Date::now().as_millis(),
        }))
    }
    pub(super) fn counts(&self) -> (usize, usize, u64, u64, u64, u64, u64) {
        self.state
            .try_borrow_mut()
            .map_or((0, 0, 0, 0, 0, 0, 0), |s| {
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
        self: &Rc<Self>,
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
        let (mut frames, receiver) = mpsc::channel(self.limits.queue_frames);
        let (reset, reset_receiver) = oneshot::channel();
        let mut state = self
            .state
            .try_borrow_mut()
            .map_err(|_| DeliveryError::Unavailable)?;
        let now = worker::Date::now().as_millis().saturating_sub(self.started);
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
            let weak = Rc::downgrade(self);
            let lifetime = self.limits.lifetime_ms;
            let (handle, registration) = AbortHandle::new_pair();
            worker::wasm_bindgen_futures::spawn_local(async move {
                let task = async move {
                    let reason = super::timer::deadline(
                        Duration::from_millis(lifetime),
                        drive(weak.clone(), generation),
                    )
                    .await
                    .unwrap_or("LIVE_RESET_REQUIRED");
                    if let Some(owner) = weak.upgrade() {
                        if let Ok(mut state) = owner.state.try_borrow_mut() {
                            end(&mut state, generation, reason);
                        }
                    }
                };
                let _ = Abortable::new(task, registration).await;
            });
            // No detached owner: Group owns the abort handle, and the driver
            // retains only Weak so the coordinator can be dropped cleanly.
            if let Ok(mut state) = self.state.try_borrow_mut() {
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
        if let Ok(mut state) = self.state.try_borrow_mut() {
            remove(&mut state, id, reason);
        }
    }
    fn input(&self, generation: u64) -> Option<Input> {
        let mut state = self.state.try_borrow_mut().ok()?;
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
            Ok(frame) => {
                let bytes =
                    serde_json::to_vec(frame.payload()).map_or(usize::MAX, |bytes| bytes.len());
                let used = self.payload_used.get().saturating_add(bytes);
                if used > self.payload_limit {
                    if let Ok(mut state) = self.state.try_borrow_mut() {
                        end(&mut state, generation, "LIVE_RESET_REQUIRED");
                    }
                    return false;
                }
                self.payload_used.set(used);
                Rc::new(WorkerFrame {
                    frame,
                    bytes,
                    used: self.payload_used.clone(),
                })
            }
            Err(_) => {
                if let Ok(mut state) = self.state.try_borrow_mut() {
                    end(&mut state, generation, "LIVE_RESET_REQUIRED");
                }
                return false;
            }
        };
        let Ok(mut state) = self.state.try_borrow_mut() else {
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
            let Some(consumer) = state.consumers.get_mut(&id) else {
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
            let now = worker::Date::now().as_millis().saturating_sub(self.started);
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
async fn drive(owner: Weak<WorkerLive>, generation: u64) -> &'static str {
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
        match super::timer::deadline(expiry, run).await {
            Ok(Ok(())) => return "complete",
            Ok(Err(_)) => return "LIVE_RESET_REQUIRED",
            Err(_) => continue, // Reauthenticate with a remaining unexpired consumer.
        }
    }
}
