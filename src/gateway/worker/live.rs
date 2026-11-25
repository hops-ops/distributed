use crate::gateway::delivery::*;
use futures_channel::{mpsc, oneshot};
use futures_util::future::{select, AbortHandle, Abortable, Either};
use futures_util::{future::LocalBoxFuture, stream::LocalBoxStream, StreamExt};
use std::{
    cell::{Cell, RefCell},
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

/// Local channels, charged Rc frames and an abortable local task for the
/// portable live state machine.
pub(super) struct Worker;
pub(super) struct Outlet {
    frames: mpsc::Sender<Rc<WorkerFrame>>,
    end: oneshot::Sender<LiveEnd>,
}
impl LiveOutlet<Rc<WorkerFrame>> for Outlet {
    fn offer(&mut self, frame: Rc<WorkerFrame>) -> bool {
        self.frames.try_send(frame).is_ok()
    }
    fn close(self, end: LiveEnd) {
        let _ = self.end.send(end);
    }
}
pub(super) struct Driver(AbortHandle);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl LiveRuntime for Worker {
    type Frame = Rc<WorkerFrame>;
    type Outlet = Outlet;
    type Source = LiveSourceFactory;
    type Driver = Driver;
    fn frame(frame: &Rc<WorkerFrame>) -> &LiveFrame {
        &frame.frame
    }
}

/// Local stream drivers are owned by their groups; removing the last consumer
/// aborts the upstream and releases its socket/stream, including on disconnect.
pub(super) struct WorkerLive {
    state: RefCell<LiveCoordinator<Worker>>,
    limits: LiveLimits,
    payload_limit: usize,
    payload_used: Rc<Cell<usize>>,
    started: u64,
}
pub(super) struct LiveLease {
    id: u64,
    owner: Rc<WorkerLive>,
    frames: mpsc::Receiver<Rc<WorkerFrame>>,
    end: oneshot::Receiver<LiveEnd>,
    expiry: u64,
    terminal: Option<LiveEnd>,
}
impl Drop for LiveLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.owner.state.try_borrow_mut() {
            state.leave(self.id);
        }
    }
}
impl LiveLease {
    pub(super) async fn next(&mut self) -> Result<Option<Rc<WorkerFrame>>, LiveEnd> {
        loop {
            let remaining = Duration::from_secs(self.expiry.saturating_sub(super::now()));
            if remaining.is_zero() {
                return Err(LiveEnd::AuthExpired);
            }
            if let Some(end) = self.terminal {
                if end != LiveEnd::Complete {
                    return Err(end);
                }
                let frame = self.frames.next().await;
                return self.unexpired(frame);
            }
            enum Event {
                End(LiveEnd),
                Frame(Option<Rc<WorkerFrame>>),
            }
            let next = async {
                match select(Box::pin(&mut self.end), Box::pin(self.frames.next())).await {
                    Either::Left((end, _)) => Event::End(end.unwrap_or(LiveEnd::ResetRequired)),
                    Either::Right((frame, _)) => Event::Frame(frame),
                }
            };
            match super::timer::deadline(remaining, next).await {
                Err(_) => return Err(LiveEnd::AuthExpired),
                Ok(Event::End(end)) => self.terminal = Some(end),
                Ok(Event::Frame(frame)) => return self.unexpired(frame),
            }
        }
    }
    fn unexpired<T>(&self, frame: T) -> Result<T, LiveEnd> {
        if super::now() >= self.expiry {
            Err(LiveEnd::AuthExpired)
        } else {
            Ok(frame)
        }
    }
    // A reset/expiry interrupts a blocked delivery acknowledgement. Normal
    // completion preserves already-queued frames before the complete packet.
    pub(super) async fn interrupted(&mut self) -> LiveEnd {
        let remaining = Duration::from_secs(self.expiry.saturating_sub(super::now()));
        if remaining.is_zero() {
            return LiveEnd::AuthExpired;
        }
        let end = match self.terminal {
            Some(end) => end,
            None => match super::timer::deadline(remaining, &mut self.end).await {
                Ok(end) => *self.terminal.insert(end.unwrap_or(LiveEnd::ResetRequired)),
                Err(_) => return LiveEnd::AuthExpired,
            },
        };
        if end != LiveEnd::Complete {
            return end;
        }
        let _ = super::timer::Timer::new(remaining.as_millis() as u64).await;
        LiveEnd::AuthExpired
    }
}

impl WorkerLive {
    pub(super) fn new(
        limits: LiveLimits,
        payload_limit: usize,
    ) -> Result<Rc<Self>, crate::gateway::GatewayError> {
        let state = LiveCoordinator::new(limits)
            .map_err(|_| crate::gateway::GatewayError("invalid live limits"))?;
        Ok(Rc::new(Self {
            state: RefCell::new(state),
            limits,
            payload_limit,
            payload_used: Rc::new(Cell::new(0)),
            started: worker::Date::now().as_millis(),
        }))
    }
    fn now_ms(&self) -> u64 {
        worker::Date::now().as_millis().saturating_sub(self.started)
    }
    pub(super) fn counts(&self) -> LiveCounts {
        self.state
            .try_borrow()
            .map_or_else(|_| LiveCounts::default(), |state| state.counts())
    }
    pub(super) fn join(
        self: &Rc<Self>,
        admission: OriginAdmission,
        request: serde_json::Value,
        freshness: Option<FreshnessContext>,
        source: LiveSourceFactory,
    ) -> Result<LiveLease, DeliveryError> {
        let expiry = admission.expires_at;
        let input = LiveInput {
            admission,
            request,
            freshness,
            source,
        };
        let (frames, receiver) = mpsc::channel(self.limits.queue_frames);
        let (end, end_receiver) = oneshot::channel();
        let joined = self
            .state
            .try_borrow_mut()
            .map_err(|_| DeliveryError::Unavailable)?
            .join(input, Outlet { frames, end }, self.now_ms(), super::now())?;
        if joined.start {
            let weak = Rc::downgrade(self);
            let generation = joined.generation;
            let lifetime = Duration::from_millis(self.limits.lifetime_ms);
            let (handle, registration) = AbortHandle::new_pair();
            // The group owns the abort handle; the driver holds only Weak so
            // the coordinator can be dropped cleanly.
            worker::wasm_bindgen_futures::spawn_local(async move {
                let task = async move {
                    let end = super::timer::deadline(lifetime, drive(weak.clone(), generation))
                        .await
                        .unwrap_or(LiveEnd::ResetRequired);
                    if let Some(owner) = weak.upgrade() {
                        if let Ok(mut state) = owner.state.try_borrow_mut() {
                            state.end(generation, end);
                        }
                    }
                };
                let _ = Abortable::new(task, registration).await;
            });
            let driver = Driver(handle);
            if let Ok(mut state) = self.state.try_borrow_mut() {
                state.attach(generation, driver);
            }
        }
        Ok(LiveLease {
            id: joined.id,
            owner: self.clone(),
            frames: receiver,
            end: end_receiver,
            expiry,
            terminal: None,
        })
    }
    fn upstream(&self, generation: u64) -> Option<LiveInput<LiveSourceFactory>> {
        self.state
            .try_borrow_mut()
            .ok()?
            .upstream(generation, super::now())
    }
    /// Charge a frame against the retained payload budget; None when over it.
    fn charge(&self, frame: LiveFrame) -> Option<Rc<WorkerFrame>> {
        let bytes = serde_json::to_vec(frame.payload()).map_or(usize::MAX, |bytes| bytes.len());
        let used = self.payload_used.get().saturating_add(bytes);
        if used > self.payload_limit {
            return None;
        }
        self.payload_used.set(used);
        Some(Rc::new(WorkerFrame {
            frame,
            bytes,
            used: self.payload_used.clone(),
        }))
    }
    fn emit(
        &self,
        generation: u64,
        input: &LiveInput<LiveSourceFactory>,
        payload: serde_json::Value,
    ) -> bool {
        let frame =
            LiveFrame::from_origin(&input.admission, payload, None, self.limits.frame_bytes)
                .ok()
                .and_then(|frame| self.charge(frame));
        let now_ms = self.now_ms();
        self.state
            .try_borrow_mut()
            .is_ok_and(|mut state| state.emit(generation, frame, now_ms, super::now()))
    }
}
async fn drive(owner: Weak<WorkerLive>, generation: u64) -> LiveEnd {
    loop {
        let Some(input) = owner.upgrade().and_then(|owner| owner.upstream(generation)) else {
            return LiveEnd::AuthExpired;
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
                    return Err("upstream no longer owned".to_owned());
                }
            }
            Ok::<(), String>(())
        };
        match super::timer::deadline(expiry, run).await {
            Ok(Ok(())) => return LiveEnd::Complete,
            Ok(Err(_)) => return LiveEnd::ResetRequired,
            Err(_) => continue, // Reauthenticate with a remaining unexpired consumer.
        }
    }
}
