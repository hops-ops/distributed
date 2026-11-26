//! Live queries: one shared producer per distinct read, fanned out.
//!
//! Every live root — a catalog plan or an external read — is a `LiveRead`.
//! Subscribers whose reads have the same key share one entry of a
//! `LiveRegistry` and its one producer loop:
//!
//! 1. The first subscriber creates the entry; the producer's first execution
//!    is that subscriber's first frame. A later subscriber opens with its own
//!    execution (it may resume from its own cursors), then follows the entry.
//! 2. The producer listens on [`ChangeHub`]. On a change that touches its last
//!    result it debounces, executes once, and publishes the frame to every
//!    subscriber.
//! 3. Each subscriber keeps its own hash gate and protocol envelope. A frame
//!    whose replay starts where the subscriber's last frame ended is taken as
//!    is; otherwise (the subscriber joined between frames, or skipped one
//!    while it was slow) the subscriber executes once from its own cursors.
//!
//! Publishing never waits on a subscriber: the entry holds only the latest
//! frame, so a slow subscriber skips frames instead of stalling the others.
//! The producer's executions are exclusive on the entry and a subscriber's own
//! are shared, so a published frame is always newer than any own execution a
//! subscriber delivered before it. At most `live_concurrency` live executions
//! (first reads, refreshes and catch-ups) run at once per engine.
//!
//! A failed execution is terminal (`docs/live-query-delivery.md`): a failed
//! shared execution ends every subscriber of the entry with that one error,
//! and the entry is gone before anyone else can join it. Protocol frame
//! metadata is a FIFO consumed by each emitted response, so it is recorded
//! only at the moment its value is yielded. When the last subscriber leaves,
//! the producer stops and the entry is removed.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use async_graphql::Value;
use futures_util::stream::BoxStream;
use futures_util::Stream;
use tokio::sync::{broadcast, watch, RwLock, Semaphore};

use crate::read_model::ReadModelChange;

use super::compile::{BindValue, QueryPlan, SqlPlan};
use super::engine::EngineInner;
use super::execute::{execute_plan, ProtocolEvidence, ProtocolRead};
use super::external_roots::{ExternalKey, ExternalRead};
use super::protocol::{ProtocolResponseAccumulator, RequestedLiveResume};
use super::schema::client_error;
use super::surface::Surface;

/// Live executions (first reads, refreshes, catch-ups) one engine runs at
/// once unless [`GraphqlEngineBuilder::live_concurrency`] says otherwise.
///
/// [`GraphqlEngineBuilder::live_concurrency`]: super::GraphqlEngineBuilder::live_concurrency
pub const DEFAULT_LIVE_CONCURRENCY: usize = 8;

/// How long a producer waits after a change for more to coalesce.
const DEBOUNCE: Duration = Duration::from_millis(100);

/// Fan-out hub for read-model change notifications.
#[derive(Clone, Debug)]
pub struct ChangeHub {
    tx: broadcast::Sender<ReadModelChange>,
}

impl ChangeHub {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self { tx }
    }

    /// Active live producers (one per shared live read).
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ReadModelChange> {
        self.tx.subscribe()
    }

    /// Publish a change. An empty table set means "everything changed" (the
    /// lag signal the forwarder sends).
    pub fn publish(&self, change: ReadModelChange) {
        let _ = self.tx.send(change);
    }

    /// Forward an external receiver into this hub until the source closes.
    pub fn spawn_forward_from(&self, mut rx: broadcast::Receiver<ReadModelChange>) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(change) => {
                        let _ = tx.send(change);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Empty tables = all-dirty for subscribers.
                        let _ = tx.send(ReadModelChange {
                            tables: BTreeSet::new(),
                        });
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
}

impl Default for ChangeHub {
    fn default() -> Self {
        Self::new()
    }
}

// Yield `Value` (not FieldValue<'static>) so HRTB `Into<FieldValue<'a>>` holds for any 'a.
type LiveItem = Result<Value, async_graphql::Error>;

/// Stream of GraphQL field values for one live subscription.
pub struct LiveQueryStream {
    inner: BoxStream<'static, LiveItem>,
}

impl Stream for LiveQueryStream {
    type Item = LiveItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// One live execution's result, as every subscriber of its read sees it.
pub(crate) struct LiveFrame<M> {
    pub(crate) value: Value,
    /// The hash gate's key: a frame equal to the last one is not pushed.
    pub(crate) hash: u64,
    /// Where the next execution resumes from.
    pub(crate) next: RequestedLiveResume,
    /// What each subscriber records with the value (protocol evidence).
    pub(crate) meta: M,
}

pub(crate) type LiveResult<M> = Result<LiveFrame<M>, async_graphql::Error>;

/// A read a live producer re-executes, independent of who subscribed to it.
pub(crate) trait LiveRead: Send + Sync + 'static {
    type Meta: Send + Sync + 'static;

    /// Execute once, replaying from `resume`.
    fn execute(
        &self,
        resume: RequestedLiveResume,
    ) -> impl Future<Output = LiveResult<Self::Meta>> + Send + '_;

    /// Whether `change` can alter the read whose last frame is `last`.
    fn touched_by(&self, last: &LiveFrame<Self::Meta>, change: &ReadModelChange) -> bool;
}

/// Records a frame's metadata into one subscriber's response envelope.
pub(crate) type Record<M> = Box<dyn FnMut(&M) -> Result<(), async_graphql::Error> + Send>;

/// The live registries of one engine, sharing one execution bound.
pub(crate) struct LiveRegistries {
    pub(crate) plans: LiveRegistry<PlanKey, PlanRead>,
    pub(crate) external: LiveRegistry<ExternalKey, ExternalRead>,
}

impl LiveRegistries {
    pub(crate) fn new(concurrency: usize) -> Self {
        let permits = Arc::new(Semaphore::new(concurrency));
        Self {
            plans: LiveRegistry::new(Arc::clone(&permits)),
            external: LiveRegistry::new(permits),
        }
    }

    /// Live subscription streams across every shared read.
    pub(crate) fn subscriber_count(&self) -> usize {
        self.plans.subscriber_count() + self.external.subscriber_count()
    }
}

/// Shared live reads by key.
pub(crate) struct LiveRegistry<K, R: LiveRead> {
    entries: Arc<Mutex<HashMap<K, Entry<R>>>>,
    permits: Arc<Semaphore>,
}

struct Entry<R: LiveRead> {
    frames: Weak<watch::Sender<Slot<R::Meta>>>,
    shared: Weak<Shared<R>>,
}

/// The latest published frame, if any.
type Slot<M> = Option<Arc<Published<M>>>;

struct Published<M> {
    /// Counts the entry's executions; a subscriber takes only newer frames.
    version: u64,
    /// Where this execution replayed from.
    resumed: RequestedLiveResume,
    result: LiveResult<M>,
}

/// What a producer and its subscribers share.
struct Shared<R> {
    read: R,
    /// The producer's executions take it exclusively, a subscriber's own
    /// executions shared, so a frame published after a subscriber's own
    /// execution was read from a newer snapshot.
    order: RwLock<()>,
    permits: Arc<Semaphore>,
}

impl<K, R> LiveRegistry<K, R>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    R: LiveRead,
{
    fn new(permits: Arc<Semaphore>) -> Self {
        Self {
            entries: Arc::default(),
            permits,
        }
    }

    /// Follow the live read `key`, creating it from `read` when nobody does
    /// yet. `resume` is where this subscriber's first frame replays from.
    pub(crate) fn subscribe(
        &self,
        hub: &ChangeHub,
        key: K,
        resume: RequestedLiveResume,
        read: impl FnOnce() -> R,
        record: Record<R::Meta>,
    ) -> LiveQueryStream {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let existing = entries
            .get(&key)
            .and_then(|entry| Some((entry.frames.upgrade()?, entry.shared.upgrade()?)));
        let (frames, shared, joined) = match existing {
            Some((sender, shared)) => (sender.subscribe(), shared, true),
            None => {
                let (sender, frames) = watch::channel(None);
                let sender = Arc::new(sender);
                let shared = Arc::new(Shared {
                    read: read(),
                    order: RwLock::new(()),
                    permits: Arc::clone(&self.permits),
                });
                entries.insert(
                    key.clone(),
                    Entry {
                        frames: Arc::downgrade(&sender),
                        shared: Arc::downgrade(&shared),
                    },
                );
                let producer = Producer {
                    key,
                    entries: Arc::clone(&self.entries),
                    frames: sender,
                    shared: Arc::clone(&shared),
                };
                // Subscribe before the first read so no change slips between.
                tokio::spawn(producer.run(hub.subscribe(), resume.clone()));
                (frames, shared, false)
            }
        };
        let seen = frames.borrow().as_ref().map_or(0, |frame| frame.version);
        drop(entries);
        let subscriber = Subscriber {
            shared,
            frames,
            seen,
            resume,
            own_first: joined,
            last_hash: None,
            record,
            done: false,
        };
        LiveQueryStream {
            inner: Box::pin(futures_util::stream::unfold(
                subscriber,
                |mut subscriber| async move {
                    let item = subscriber.next().await?;
                    Some((item, subscriber))
                },
            )),
        }
    }

    /// Subscription streams following this registry's reads (each holds one
    /// receiver of its read's frame channel).
    fn subscriber_count(&self) -> usize {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries
            .values()
            .filter_map(|entry| entry.frames.upgrade())
            .map(|frames| frames.receiver_count())
            .sum()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

struct Producer<K, R: LiveRead> {
    key: K,
    entries: Arc<Mutex<HashMap<K, Entry<R>>>>,
    frames: Arc<watch::Sender<Slot<R::Meta>>>,
    shared: Arc<Shared<R>>,
}

impl<K, R> Producer<K, R>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    R: LiveRead,
{
    async fn run(
        self,
        mut changes: broadcast::Receiver<ReadModelChange>,
        first: RequestedLiveResume,
    ) {
        let shared = Arc::clone(&self.shared);
        let mut resume = first;
        let mut version = 0;
        let mut last: Option<Arc<Published<R::Meta>>> = None;
        loop {
            if let Some(Ok(frame)) = last.as_ref().map(|published| &published.result) {
                // Wait for a change to what the last frame read.
                loop {
                    let Some(change) = self.while_watched(changes.recv()).await else {
                        return;
                    };
                    match change {
                        Ok(change) if shared.read.touched_by(frame, &change) => break,
                        Ok(_) => continue,
                        Err(broadcast::error::RecvError::Lagged(_)) => break,
                        Err(broadcast::error::RecvError::Closed) => return self.retire(None),
                    }
                }
                if self
                    .while_watched(tokio::time::sleep(DEBOUNCE))
                    .await
                    .is_none()
                {
                    return;
                }
                loop {
                    match changes.try_recv() {
                        Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) => {}
                        Err(broadcast::error::TryRecvError::Empty) => break,
                        Err(broadcast::error::TryRecvError::Closed) => return self.retire(None),
                    }
                }
            }
            // Lock, then permit: whoever holds a permit is executing, never
            // waiting, so the two cannot deadlock.
            let Some(_order) = self.while_watched(shared.order.write()).await else {
                return;
            };
            let Some(Ok(_permit)) = self.while_watched(shared.permits.acquire()).await else {
                return;
            };
            let Some(result) = self
                .while_watched(shared.read.execute(resume.clone()))
                .await
            else {
                return;
            };
            version += 1;
            let published = Arc::new(Published {
                version,
                resumed: std::mem::take(&mut resume),
                result,
            });
            let Ok(frame) = &published.result else {
                return self.retire(Some(published));
            };
            resume = frame.next.clone();
            // Published while the order lock is held.
            self.frames.send_replace(Some(Arc::clone(&published)));
            last = Some(published);
        }
    }

    /// Run `work` unless every subscriber leaves first (`None`: stop).
    async fn while_watched<T>(&self, work: impl Future<Output = T>) -> Option<T> {
        tokio::pin!(work);
        loop {
            tokio::select! {
                output = &mut work => return Some(output),
                _ = self.frames.closed() => {
                    let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
                    // A subscriber may have joined since: joining holds this lock.
                    if self.frames.receiver_count() == 0 {
                        self.remove(&mut entries);
                        return None;
                    }
                }
            }
        }
    }

    /// Stop for good, publishing `last` (a failure) to every subscriber first.
    /// Nobody can join between the two: joining holds the registry lock.
    fn retire(self, last: Option<Arc<Published<R::Meta>>>) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(last) = last {
            self.frames.send_replace(Some(last));
        }
        self.remove(&mut entries);
        // Dropping the sender ends every subscriber that has nothing left.
    }

    fn remove(&self, entries: &mut HashMap<K, Entry<R>>) {
        let ours = entries
            .get(&self.key)
            .is_some_and(|entry| std::ptr::eq(entry.frames.as_ptr(), Arc::as_ptr(&self.frames)));
        if ours {
            entries.remove(&self.key);
        }
    }
}

struct Subscriber<R: LiveRead> {
    shared: Arc<Shared<R>>,
    frames: watch::Receiver<Slot<R::Meta>>,
    /// The newest frame version this subscriber has taken or outrun.
    seen: u64,
    /// Where this subscriber's next frame must replay from.
    resume: RequestedLiveResume,
    /// Joined an existing entry: the first frame is its own execution.
    own_first: bool,
    last_hash: Option<u64>,
    record: Record<R::Meta>,
    done: bool,
}

impl<R: LiveRead> Subscriber<R> {
    async fn next(&mut self) -> Option<LiveItem> {
        if self.done {
            return None;
        }
        loop {
            let own;
            let published;
            let result = if std::mem::take(&mut self.own_first) {
                own = self.execute_own().await;
                &own
            } else {
                // The producer is gone with nothing new: the stream ends.
                self.frames.changed().await.ok()?;
                let Some(latest) = self.frames.borrow_and_update().clone() else {
                    continue;
                };
                if latest.version <= self.seen {
                    continue;
                }
                self.seen = latest.version;
                published = latest;
                if published.result.is_ok() && published.resumed != self.resume {
                    // Joined between frames or skipped one: catch up exactly.
                    own = self.execute_own().await;
                    &own
                } else {
                    &published.result
                }
            };
            let frame = match result {
                Ok(frame) => frame,
                Err(error) => {
                    self.done = true;
                    return Some(Err(error.clone()));
                }
            };
            // Advance the replay cursor even when the frame is hash-gated.
            self.resume = frame.next.clone();
            if self.last_hash == Some(frame.hash) {
                continue;
            }
            if let Err(error) = (self.record)(&frame.meta) {
                self.done = true;
                return Some(Err(error));
            }
            self.last_hash = Some(frame.hash);
            return Some(Ok(frame.value.clone()));
        }
    }

    /// Execute from this subscriber's own cursors, ordered against the
    /// producer: every frame it publishes afterwards is newer.
    async fn execute_own(&mut self) -> LiveResult<R::Meta> {
        let shared = Arc::clone(&self.shared);
        let _order = shared.order.read().await;
        let _permit = shared.permits.acquire().await;
        self.seen = self
            .frames
            .borrow()
            .as_ref()
            .map_or(0, |frame| frame.version);
        shared.read.execute(self.resume.clone()).await
    }
}

/// What makes two catalog live subscriptions one read.
///
/// The plan's SQL and binds carry every row policy and claim value; the
/// role selects the surface its evidence is read under; protocol tokens are
/// MACs over the cache scope (privilege, asserted roles, principal partition
/// and every claim a policy reads) and schema hash. Live subscriptions never
/// carry a gateway capture (`enable_delivery_capture` admits queries only).
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct PlanKey {
    role: String,
    sql: Arc<str>,
    binds: Vec<BindValue>,
    tables: Vec<String>,
    bytes_hex_paths: Vec<String>,
    protocol: Option<(String, String)>,
}

/// A compiled catalog plan, re-executed live.
pub(crate) struct PlanRead {
    inner: Arc<EngineInner>,
    plan: Arc<SqlPlan>,
    footprint: BTreeSet<String>,
    /// The role surface and a token issuer, under the causal protocol.
    protocol: Option<(Arc<Surface>, ProtocolResponseAccumulator)>,
}

impl LiveRead for PlanRead {
    type Meta = Option<ProtocolEvidence>;

    async fn execute(&self, resume: RequestedLiveResume) -> LiveResult<Self::Meta> {
        let read = self
            .protocol
            .as_ref()
            .map(|(role_surface, issuer)| ProtocolRead {
                role_surface: Arc::clone(role_surface),
                accumulator: issuer.clone(),
                live_resume: Some(resume),
            });
        let executed = execute_plan(&self.inner, &self.inner.pool, true, &self.plan, read)
            .await
            .map_err(|error| error.client_error())?;
        let Some(evidence) = executed.protocol else {
            return Ok(LiveFrame {
                hash: response_hash(&executed.value),
                value: executed.value,
                next: RequestedLiveResume::Absent,
                meta: None,
            });
        };
        let hash = hash_json(&(&executed.value, &evidence.snapshot, &evidence.live));
        let next = evidence
            .live
            .as_ref()
            .filter(|live| live.mode == super::protocol::DistributedLiveMode::Resumable)
            .map(|live| RequestedLiveResume::Cursors(live.cursors.clone()))
            .unwrap_or(RequestedLiveResume::Absent);
        Ok(LiveFrame {
            value: executed.value,
            hash,
            next,
            meta: Some(evidence),
        })
    }

    fn touched_by(&self, _last: &LiveFrame<Self::Meta>, change: &ReadModelChange) -> bool {
        footprint_hits(&self.footprint, change)
    }
}

/// Build a live-query stream for a subscription root field's compiled plan.
pub(crate) fn live_query_stream(
    inner: &Arc<EngineInner>,
    role: String,
    plan: QueryPlan,
    protocol: Option<ProtocolResponseAccumulator>,
) -> Result<LiveQueryStream, async_graphql::Error> {
    let plan: Arc<SqlPlan> = match plan {
        QueryPlan::Sql(plan) => plan,
        QueryPlan::CellByKey { .. } => {
            return Err(client_error("BAD_REQUEST", "unsupported on cell store"));
        }
    };
    let resume = protocol
        .as_ref()
        .map(ProtocolResponseAccumulator::requested_live_resume)
        .transpose()
        .map_err(|_| client_error("BAD_REQUEST", "invalid live resume"))?
        .unwrap_or(RequestedLiveResume::Absent);
    let shared_protocol = match &protocol {
        None => None,
        Some(accumulator) => {
            // Matching compiler plans share one comparable snapshot scope, so
            // a lagging refresh is never authoritative over a newer frame.
            accumulator
                .bind_query_snapshot_scope(&plan.snapshot_scope())
                .map_err(|_| evidence_failed())?;
            let role_surface = inner.role_surfaces.get(&role).cloned().ok_or_else(|| {
                client_error("INTERNAL", "authorized GraphQL role surface is unavailable")
            })?;
            let scope = accumulator.token_scope().map_err(|_| evidence_failed())?;
            let issuer = accumulator.issuer().map_err(|_| evidence_failed())?;
            Some((scope, role_surface, issuer))
        }
    };
    let key = PlanKey {
        role,
        sql: Arc::clone(&plan.sql),
        binds: plan.binds.clone(),
        tables: plan.tables_touched.clone(),
        bytes_hex_paths: plan.bytes_hex_paths.clone(),
        protocol: shared_protocol.as_ref().map(|(scope, ..)| scope.clone()),
    };
    let record: Record<Option<ProtocolEvidence>> = Box::new(move |meta| {
        let Some(protocol) = &protocol else {
            return Ok(());
        };
        let evidence = meta.as_ref().ok_or_else(evidence_failed)?;
        protocol
            .record_query_metadata(evidence.snapshot.clone(), evidence.live.clone())
            .map_err(|_| evidence_failed())
    });
    let read = || PlanRead {
        inner: Arc::clone(inner),
        footprint: footprint_from_tables(&plan.tables_touched),
        plan: Arc::clone(&plan),
        protocol: shared_protocol.map(|(_, role_surface, issuer)| (role_surface, issuer)),
    };
    Ok(inner
        .live
        .plans
        .subscribe(&inner.change_hub, key, resume, read, record))
}

fn evidence_failed() -> async_graphql::Error {
    client_error("INTERNAL", "query evidence encoding failed")
}

fn footprint_hits(footprint: &BTreeSet<String>, change: &ReadModelChange) -> bool {
    // Empty tables = all-dirty (lag signal from forwarder).
    if change.tables.is_empty() {
        return true;
    }
    change.tables.iter().any(|t| footprint.contains(t))
}

/// Compute the table footprint of a compiled plan (for dirty matching).
pub fn footprint_from_tables(tables: &[String]) -> BTreeSet<String> {
    tables.iter().cloned().collect()
}

/// Hash a GraphQL JSON payload for hash-gated push (no push on no-change).
pub fn response_hash(value: &Value) -> u64 {
    hash_json(value)
}

/// Hash the JSON encoding of `value` as it is produced, without building the
/// string.
fn hash_json(value: &impl serde::Serialize) -> u64 {
    use std::hash::Hasher;

    struct HashWriter(std::collections::hash_map::DefaultHasher);
    impl std::io::Write for HashWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.write(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut writer = HashWriter(Default::default());
    // Encoding a GraphQL value cannot fail; if it ever did, the partial
    // encoding still hashes deterministically.
    let _ = serde_json::to_writer(&mut writer, value);
    writer.0.finish()
}

/// Forward an external change receiver into the engine hub.
pub fn spawn_change_forwarder(hub: ChangeHub, rx: broadcast::Receiver<ReadModelChange>) {
    hub.spawn_forward_from(rx);
}

#[cfg(test)]
mod tests;
