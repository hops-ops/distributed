use crate::gateway::delivery::*;
use futures_util::{future::BoxFuture, stream::BoxStream, StreamExt};
use std::{
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

pub(super) type LiveSource = BoxStream<'static, Result<serde_json::Value, String>>;
pub(super) type LiveSourceFactory =
    Arc<dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<LiveSource, String>> + Send + Sync>;

/// Tokio channels, Arc frames and an aborting task handle for the portable
/// live state machine.
pub(super) struct Native;
pub(super) struct Outlet {
    frames: mpsc::Sender<Arc<LiveFrame>>,
    end: oneshot::Sender<LiveEnd>,
}
impl LiveOutlet<Arc<LiveFrame>> for Outlet {
    fn offer(&mut self, frame: Arc<LiveFrame>) -> bool {
        self.frames.try_send(frame).is_ok()
    }
    fn close(self, end: LiveEnd) {
        let _ = self.end.send(end);
    }
}
pub(super) struct Driver(tokio::task::AbortHandle);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl LiveRuntime for Native {
    type Frame = Arc<LiveFrame>;
    type Outlet = Outlet;
    type Source = LiveSourceFactory;
    type Driver = Driver;
    fn frame(frame: &Arc<LiveFrame>) -> &LiveFrame {
        frame
    }
}

/// Native stream drivers are owned by their groups; removing the last consumer
/// aborts the upstream and releases its socket/stream, including on disconnect.
pub(super) struct NativeLive {
    state: Mutex<LiveCoordinator<Native>>,
    limits: LiveLimits,
    started: Instant,
}
pub(super) struct LiveLease {
    id: u64,
    owner: Arc<NativeLive>,
    frames: mpsc::Receiver<Arc<LiveFrame>>,
    end: oneshot::Receiver<LiveEnd>,
    expiry: u64,
    terminal: Option<LiveEnd>,
}
impl Drop for LiveLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.owner.state.lock() {
            state.leave(self.id);
        }
    }
}
impl LiveLease {
    pub(super) async fn next(&mut self) -> Result<Option<Arc<LiveFrame>>, LiveEnd> {
        loop {
            let remaining = Duration::from_secs(self.expiry.saturating_sub(super::now()));
            if remaining.is_zero() {
                return Err(LiveEnd::AuthExpired);
            }
            if let Some(end) = self.terminal {
                if end != LiveEnd::Complete {
                    return Err(end);
                }
                let frame = self.frames.recv().await;
                return self.unexpired(frame);
            }
            tokio::select! { biased;
                end = &mut self.end => { self.terminal = Some(end.unwrap_or(LiveEnd::ResetRequired)); },
                _ = tokio::time::sleep(remaining) => return Err(LiveEnd::AuthExpired),
                frame = self.frames.recv() => return self.unexpired(frame),
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
    // A reset/expiry interrupts a blocked network queue. Normal completion
    // preserves all already-queued frames before the terminal complete packet.
    pub(super) async fn interrupted(&mut self) -> LiveEnd {
        let remaining = Duration::from_secs(self.expiry.saturating_sub(super::now()));
        if remaining.is_zero() {
            return LiveEnd::AuthExpired;
        }
        let end = match self.terminal {
            Some(end) => end,
            None => tokio::select! {
                end = &mut self.end => *self.terminal.insert(end.unwrap_or(LiveEnd::ResetRequired)),
                _ = tokio::time::sleep(remaining) => return LiveEnd::AuthExpired,
            },
        };
        if end != LiveEnd::Complete {
            return end;
        }
        tokio::time::sleep(remaining).await;
        LiveEnd::AuthExpired
    }
}

impl NativeLive {
    pub(super) fn new(limits: LiveLimits) -> Result<Arc<Self>, super::GatewayError> {
        let state =
            LiveCoordinator::new(limits).map_err(|_| super::GatewayError("invalid live limits"))?;
        Ok(Arc::new(Self {
            state: Mutex::new(state),
            limits,
            started: Instant::now(),
        }))
    }
    fn now_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
    pub(super) fn counts(&self) -> LiveCounts {
        self.state
            .lock()
            .map_or_else(|_| LiveCounts::default(), |state| state.counts())
    }
    pub(super) fn join(
        self: &Arc<Self>,
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
            .lock()
            .map_err(|_| DeliveryError::Unavailable)?
            .join(input, Outlet { frames, end }, self.now_ms(), super::now())?;
        if joined.start {
            let weak = Arc::downgrade(self);
            let generation = joined.generation;
            let lifetime = Duration::from_millis(self.limits.lifetime_ms);
            // The group owns the abort handle; the driver holds only Weak so
            // the coordinator can be dropped cleanly.
            let driver = tokio::spawn(async move {
                let end = tokio::time::timeout(lifetime, drive(weak.clone(), generation))
                    .await
                    .unwrap_or(LiveEnd::ResetRequired);
                if let Some(owner) = weak.upgrade() {
                    if let Ok(mut state) = owner.state.lock() {
                        state.end(generation, end);
                    }
                }
            });
            let driver = Driver(driver.abort_handle());
            if let Ok(mut state) = self.state.lock() {
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
        self.state.lock().ok()?.upstream(generation, super::now())
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
                .map(Arc::new);
        let now_ms = self.now_ms();
        self.state
            .lock()
            .is_ok_and(|mut state| state.emit(generation, frame, now_ms, super::now()))
    }
}
async fn drive(owner: Weak<NativeLive>, generation: u64) -> LiveEnd {
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
        match tokio::time::timeout(expiry, run).await {
            Ok(Ok(())) => return LiveEnd::Complete,
            Ok(Err(_)) => return LiveEnd::ResetRequired,
            Err(_) => continue, // Reauthenticate with a remaining unexpired consumer.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // The sharing rules are tested on the portable LiveCoordinator. This
    // covers only the adapter: spawned driver, Tokio queues and abort on leave.
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
    fn frame(position: u64) -> serde_json::Value {
        serde_json::json!({"data":{"rows":[{"title":"unchanged"}]},"extensions":{"distributed":{
            "protocolVersion":1,"schemaHash":"schema","authorizationGeneration":"policy","cacheScope":"alice","operation":"operation",
            "snapshot":{"recordsComplete":true,"indexesComparable":true,"records":[],"indexes":[{"projection":"rows","scopeToken":"scope","position":position.to_string()}],"observations":["proof"]},
            "live":{"mode":"resumable","reset":false,"cursors":[{"projection":"rows","position":position.to_string(),"token":"t"}]}
        }}})
    }
    struct DropCount(Arc<AtomicUsize>);
    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    async fn wait(condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn leases_receive_frames_and_the_last_leave_aborts_the_upstream() {
        let live = NativeLive::new(LiveLimits::default()).unwrap();
        let (sender, receiver) = mpsc::unbounded_channel();
        let receiver = Arc::new(Mutex::new(Some(receiver)));
        let dropped = Arc::new(AtomicUsize::new(0));
        let guard = dropped.clone();
        let factory: LiveSourceFactory = Arc::new(move |_| {
            let receiver = receiver.lock().unwrap().take();
            let guard = DropCount(guard.clone());
            async move {
                let receiver = receiver.ok_or_else(|| "source opened twice".to_owned())?;
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
        let request = request();
        let mut first = live
            .join(admission(&request), request.clone(), None, factory.clone())
            .unwrap();
        let mut second = live
            .join(admission(&request), request, None, factory)
            .unwrap();
        assert_eq!((live.counts().groups, live.counts().consumers), (1, 2));
        sender.send(frame(1)).unwrap();
        assert_eq!(first.next().await.unwrap().unwrap().payload(), &frame(1));
        assert_eq!(second.next().await.unwrap().unwrap().payload(), &frame(1));
        drop(first);
        assert_eq!(live.counts().consumers, 1);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(second);
        wait(|| dropped.load(Ordering::SeqCst) == 1).await;
        assert_eq!((live.counts().groups, live.counts().consumers), (0, 0));
    }

    #[tokio::test]
    async fn a_reset_reaches_a_lease_blocked_on_its_queue() {
        let live = NativeLive::new(LiveLimits {
            queue_frames: 1,
            history_frames: 1,
            ..Default::default()
        })
        .unwrap();
        let factory: LiveSourceFactory = Arc::new(|_| {
            async { Ok(futures_util::stream::iter([Ok(frame(1)), Ok(frame(2))]).boxed()) }.boxed()
        });
        let request = request();
        let mut lease = live
            .join(admission(&request), request, None, factory)
            .unwrap();
        wait(|| live.counts().groups == 0).await;
        assert_eq!(lease.interrupted().await, LiveEnd::ResetRequired);
        assert_eq!(lease.next().await.unwrap_err(), LiveEnd::ResetRequired);
        assert_eq!(live.counts().resets, 1);
    }
}
