//! Non-blocking durable facts.
//!
//! A *fact* is a meaningful, durable outcome of simulation ("enemy killed,
//! credit XP to player P"). Zones emit facts during a tick through
//! [`Ctx::emit_fact`](crate::Ctx::emit_fact); the call only enqueues. A
//! [`FactSink`] commits them asynchronously on a worker ([`FactFeed::run`] or
//! `spawn_fact_worker` with the `runtime` feature) with bounded retries and
//! exponential backoff; the resulting [`FactSettlement`] is delivered back to
//! the zone (`Zone::on_fact_settled`) at the start of a later tick.
//! **The tick never awaits storage.**
//!
//! Guarantees:
//! - Bounded: at most `capacity` facts may be unsettled; beyond that
//!   `emit_fact` returns [`EmitError::Backpressure`] and the game decides
//!   (retry next tick, freeze, drop a cosmetic fact).
//! - Idempotent: each fact has a caller-chosen idempotency key. The queue
//!   rejects a key already in flight; sinks must treat a repeated key as a
//!   replay (the cell adapter maps it onto `dispatch_idempotent`).
//! - Trusted identity: the principal is bound by the host from the player's
//!   verified ticket — zones and clients cannot set it.
//!
//! Ordering: by default (`max_concurrent_commits = 1`) one worker commits
//! facts strictly in emission order, one at a time, so a slow commit delays
//! every later fact. With [`FactFeed::with_max_concurrent_commits`]`(n > 1)`
//! up to `n` commits run at once, in **lanes by principal**: facts of the
//! same principal (and, separately, all zone/system facts without one) are
//! still committed one at a time in emission order, while facts of
//! different principals overlap. A key is never in flight twice (the queue
//! refuses duplicates), so per-key order always holds. Settlements arrive
//! in completion order.

use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use serde_json::Value;

use crate::stamp::FactStamp;

/// Boxed `Send` future used by [`FactSink`].
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Restart-safe idempotency key: `"{prefix}:{zone}:{epoch}:{part}:…"`.
///
/// `epoch` must change between activations and grow across restarts
/// ([`Ctx::boot_epoch`](crate::Ctx::boot_epoch)). Keys derived only from the
/// tick or an entity generation **collide after a restart** — both counters
/// start again from zero — and an idempotent sink then treats the new
/// outcome as a replay of the old one (silently dropping it).
///
/// ```
/// use distributed_game::facts::fact_key;
/// assert_eq!(fact_key("kill", "pit-1", 7, &[&"mob-3", &42]), "kill:pit-1:7:mob-3:42");
/// ```
pub fn fact_key(prefix: &str, zone: &str, epoch: u64, parts: &[&dyn std::fmt::Display]) -> String {
    use std::fmt::Write as _;
    let mut key = format!("{prefix}:{zone}:{epoch}");
    for part in parts {
        let _ = write!(key, ":{part}");
    }
    key
}

/// A cell command input a zone may emit as a typed fact
/// (`#[derive(FactCommand)]`, or implement by hand).
///
/// `COMMAND` names the durable command the input belongs to: point it at
/// the same constant the cell mounts it under (`typed_command::<I, O>(NAME)`
/// or `#[command]`'s generated `<HANDLER>_COMMAND_ID`), so the zone and the
/// handler cannot disagree. [`Ctx::emit`](crate::Ctx::emit) then builds the
/// restart-safe key (`"{COMMAND}:{zone}:{boot_epoch}:{parts…}"`) from
/// [`dedupe_parts`](Self::dedupe_parts), or from the tick and emission order
/// when it returns none.
///
/// Trust rules (enforced by the host, not by this trait): the principal is
/// bound from the player's verified ticket, never from the input; the input
/// reports *what happened* (which mob died) and the command handler decides
/// any reward or economy amount. The derive rejects identity-, reward- and
/// economy-looking field names (`user_id`, `xp`, `amount`, `gold`, …).
pub trait FactCommand: serde::Serialize {
    /// Durable command name.
    const COMMAND: &'static str;

    /// Parts that identify this logical outcome within one zone activation
    /// (e.g. mob id + generation). Empty = use the tick and emission order.
    fn dedupe_parts(&self) -> Vec<String> {
        Vec::new()
    }
}

/// A durable outcome to commit.
#[derive(Debug, Clone, PartialEq)]
pub struct Fact {
    key: String,
    command: String,
    input: Value,
    principal: Option<String>,
    stamp: FactStamp,
}

impl Fact {
    /// `key` must be unique per logical outcome **across restarts**: build it
    /// with [`Ctx::fact_key`](crate::Ctx::fact_key) / [`fact_key`] (zone +
    /// boot epoch + your parts), never from the tick alone;
    /// `command` names the durable command; `input` is its payload.
    pub fn new(key: impl Into<String>, command: impl Into<String>, input: Value) -> Self {
        Self {
            key: key.into(),
            command: command.into(),
            input,
            principal: None,
            stamp: FactStamp::default(),
        }
    }

    /// Typed fact: `C::COMMAND` with `input` serialized as its payload.
    pub fn typed<C: FactCommand>(
        key: impl Into<String>,
        input: &C,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self::new(key, C::COMMAND, serde_json::to_value(input)?))
    }

    /// True when this fact is a `C` command.
    pub fn is<C: FactCommand>(&self) -> bool {
        self.command == C::COMMAND
    }

    /// Idempotency key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Command name.
    pub fn command(&self) -> &str {
        &self.command
    }

    /// Command input.
    pub fn input(&self) -> &Value {
        &self.input
    }

    /// Verified principal (from the player's ticket), or None for zone/system facts.
    pub fn principal(&self) -> Option<&str> {
        self.principal.as_deref()
    }

    /// Provenance stamp.
    pub fn stamp(&self) -> &FactStamp {
        &self.stamp
    }

    pub(crate) fn bind(mut self, principal: Option<String>, stamp: FactStamp) -> Self {
        self.principal = principal;
        self.stamp = stamp;
        self
    }

    /// Build a fact with an explicit principal/stamp. Only for sinks' own
    /// tests and tooling; zones go through `Ctx::emit_fact_for`.
    pub fn trusted(
        key: impl Into<String>,
        command: impl Into<String>,
        input: Value,
        principal: Option<String>,
        stamp: FactStamp,
    ) -> Self {
        Self::new(key, command, input).bind(principal, stamp)
    }
}

/// Sink failure classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FactError {
    /// Transient; retried with backoff.
    Retryable(String),
    /// Will never succeed; settles as failed immediately.
    Permanent(String),
}

/// Successful commit.
#[derive(Debug, Clone, PartialEq)]
pub struct FactReceipt {
    /// Sink-specific result payload.
    pub payload: Value,
    /// True when the sink recognised the key as already committed.
    pub replayed: bool,
}

impl FactReceipt {
    /// The sink's result payload as a typed value (e.g. the command output).
    pub fn payload_as<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        T::deserialize(&self.payload)
    }
}

/// Final outcome of one fact.
#[derive(Debug, Clone, PartialEq)]
pub enum FactOutcome {
    /// Durably committed.
    Committed(FactReceipt),
    /// Gave up.
    Failed {
        /// Last error.
        error: FactError,
        /// Attempts made.
        attempts: u32,
    },
}

/// Delivered to the zone on a later tick.
#[derive(Debug, Clone, PartialEq)]
pub struct FactSettlement {
    /// Idempotency key.
    pub key: String,
    /// Command name.
    pub command: String,
    /// Outcome.
    pub outcome: FactOutcome,
}

impl FactSettlement {
    /// True when the settled fact was a `C` command.
    pub fn is<C: FactCommand>(&self) -> bool {
        self.command == C::COMMAND
    }
}

/// Durable commit target.
pub trait FactSink: Send + Sync + 'static {
    /// Commit one fact. Must be idempotent per [`Fact::key`].
    fn commit<'a>(&'a self, fact: &'a Fact) -> BoxFuture<'a, Result<FactReceipt, FactError>>;
}

/// Retry/backoff policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts including the first (>= 1).
    pub max_attempts: u32,
    /// Delay after the first retryable failure; doubles each time.
    pub base_backoff: Duration,
    /// Backoff ceiling.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(5),
        }
    }
}

impl RetryPolicy {
    /// Retry retryable failures until the fact settles (backing off up to
    /// `max_backoff`): a network sink whose target is down keeps the fact
    /// pending in the queue, so the zone sees backpressure at capacity
    /// instead of losing durable outcomes. Permanent failures still settle
    /// immediately.
    pub fn until_settled(base_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            max_attempts: u32::MAX,
            base_backoff,
            max_backoff,
        }
    }

    /// Backoff before attempt `attempt + 1` (attempt >= 1).
    pub fn backoff(&self, attempt: u32) -> Duration {
        let factor = 1u32 << attempt.saturating_sub(1).min(20);
        self.base_backoff
            .saturating_mul(factor)
            .min(self.max_backoff)
    }
}

/// Why `emit_fact` refused a fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitError {
    /// `capacity` facts are unsettled.
    Backpressure,
    /// A fact with this key is already in flight.
    DuplicateKey,
    /// The host has no fact queue attached.
    NoSink,
    /// The player is unknown (cannot bind a principal).
    UnknownPlayer,
    /// A typed fact input did not serialize.
    Encode(String),
}

#[derive(Debug, Default)]
struct Shared {
    queue: Mutex<VecDeque<Fact>>,
    waker: Mutex<Option<Waker>>,
    closed: AtomicBool,
}

impl Shared {
    fn wake(&self) {
        if let Some(waker) = self.waker.lock().expect("waker lock").take() {
            waker.wake();
        }
    }
}

/// Zone-side handle: enqueue facts, collect settlements. Never blocks.
#[derive(Debug)]
pub struct FactQueue {
    shared: Arc<Shared>,
    settlements: mpsc::Receiver<FactSettlement>,
    in_flight: HashSet<String>,
    capacity: usize,
    script: Option<SettlementScript>,
}

/// Worker-side handle consumed by [`FactFeed::run`].
#[derive(Debug)]
pub struct FactFeed {
    shared: Arc<Shared>,
    settlements: mpsc::Sender<FactSettlement>,
    policy: RetryPolicy,
    max_concurrent: usize,
}

/// Create a queue allowing `capacity` unsettled facts.
pub fn fact_channel(capacity: usize, policy: RetryPolicy) -> (FactQueue, FactFeed) {
    let shared = Arc::new(Shared::default());
    let (tx, rx) = mpsc::channel();
    (
        FactQueue {
            shared: shared.clone(),
            settlements: rx,
            in_flight: HashSet::new(),
            capacity: capacity.max(1),
            script: None,
        },
        FactFeed {
            shared,
            settlements: tx,
            policy,
            max_concurrent: 1,
        },
    )
}

impl FactQueue {
    /// Enqueue without blocking.
    pub fn try_send(&mut self, fact: Fact) -> Result<(), EmitError> {
        if self.in_flight.contains(fact.key()) {
            return Err(EmitError::DuplicateKey);
        }
        if self.in_flight.len() >= self.capacity {
            return Err(EmitError::Backpressure);
        }
        self.in_flight.insert(fact.key.clone());
        if self.script.is_some() {
            return Ok(());
        }
        self.shared
            .queue
            .lock()
            .expect("fact queue lock")
            .push_back(fact);
        self.shared.wake();
        Ok(())
    }

    /// Settlements that arrived since the last call.
    pub fn drain(&mut self) -> Vec<FactSettlement> {
        let settled: Vec<_> = self.settlements.try_iter().collect();
        for s in &settled {
            self.in_flight.remove(&s.key);
        }
        settled
    }

    /// Unsettled facts.
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// Queue whose settlements come from a script instead of a worker
    /// (record/replay). Emitted facts are tracked for backpressure and
    /// duplicate keys exactly like a live queue but never committed.
    pub fn scripted(capacity: usize) -> (Self, SettlementScript) {
        let (mut queue, _feed) = fact_channel(capacity, RetryPolicy::default());
        let script = SettlementScript::default();
        queue.script = Some(script.clone());
        (queue, script)
    }

    /// Settlements due at `tick`: scripted ones scheduled at or before it,
    /// otherwise whatever the worker delivered.
    pub(crate) fn drain_at(&mut self, tick: u64) -> Vec<FactSettlement> {
        let Some(script) = &self.script else {
            return self.drain();
        };
        let mut due = Vec::new();
        {
            let mut queue = script.0.lock().expect("script lock");
            while queue.front().is_some_and(|(at, _)| *at <= tick) {
                due.push(queue.pop_front().expect("front").1);
            }
        }
        for s in &due {
            self.in_flight.remove(&s.key);
        }
        due
    }
}

/// Feeds scripted settlements into a [`FactQueue::scripted`] queue.
#[derive(Debug, Clone, Default)]
pub struct SettlementScript(Arc<Mutex<VecDeque<(u64, FactSettlement)>>>);

impl SettlementScript {
    /// Deliver `settlement` on the first settlement point at or after `tick`.
    pub fn push(&self, tick: u64, settlement: FactSettlement) {
        self.0
            .lock()
            .expect("script lock")
            .push_back((tick, settlement));
    }

    /// Scheduled but not yet delivered.
    pub fn len(&self) -> usize {
        self.0.lock().expect("script lock").len()
    }

    /// True when nothing is scheduled.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for FactQueue {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::SeqCst);
        self.shared.wake();
    }
}

struct NextFact<'a>(&'a Shared);

impl Future for NextFact<'_> {
    type Output = Option<Fact>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let shared = self.0;
        // Register before checking so a concurrent push cannot be missed.
        *shared.waker.lock().expect("waker lock") = Some(cx.waker().clone());
        if let Some(fact) = shared.queue.lock().expect("fact queue lock").pop_front() {
            return Poll::Ready(Some(fact));
        }
        if shared.closed.load(Ordering::SeqCst) {
            return Poll::Ready(None);
        }
        Poll::Pending
    }
}

impl FactFeed {
    /// Allow up to `n` commits at once (default 1: strict emission order).
    /// Facts of one principal stay sequential; see the module docs.
    pub fn with_max_concurrent_commits(mut self, n: usize) -> Self {
        self.max_concurrent = n.max(1);
        self
    }

    /// Commits allowed at once.
    pub fn max_concurrent_commits(&self) -> usize {
        self.max_concurrent
    }

    /// Commit facts until the queue is dropped and drained: in emission
    /// order, or with bounded concurrency in per-principal lanes
    /// ([`with_max_concurrent_commits`](Self::with_max_concurrent_commits)).
    /// `sleep` provides the runtime's timer for backoff.
    pub async fn run<S, F, Fut>(self, sink: S, sleep: F)
    where
        S: FactSink,
        F: Fn(Duration) -> Fut,
        Fut: Future<Output = ()>,
    {
        let policy = self.policy;
        if self.max_concurrent <= 1 {
            while let Some(fact) = NextFact(&self.shared).await {
                let settlement = commit_with_retries(&sink, fact, policy, &sleep).await;
                let _ = self.settlements.send(settlement);
            }
            return;
        }
        let start = |fact: Fact| Box::pin(commit_with_retries(&sink, fact, policy, &sleep));
        Pump {
            shared: &self.shared,
            settlements: &self.settlements,
            limit: self.max_concurrent,
            start,
            waiting: VecDeque::new(),
            active: Vec::new(),
        }
        .await;
    }
}

async fn commit_with_retries<S, F, Fut>(
    sink: &S,
    fact: Fact,
    policy: RetryPolicy,
    sleep: &F,
) -> FactSettlement
where
    S: FactSink,
    F: Fn(Duration) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut attempts = 0;
    let outcome = loop {
        attempts += 1;
        match sink.commit(&fact).await {
            Ok(receipt) => break FactOutcome::Committed(receipt),
            Err(FactError::Retryable(_)) if attempts < policy.max_attempts.max(1) => {
                sleep(policy.backoff(attempts)).await;
            }
            Err(error) => break FactOutcome::Failed { error, attempts },
        }
    };
    FactSettlement {
        key: fact.key,
        command: fact.command,
        outcome,
    }
}

/// Concurrent commit driver: at most `limit` commits in flight, one per
/// principal lane, admitted in emission order.
struct Pump<'a, T, M> {
    shared: &'a Shared,
    settlements: &'a mpsc::Sender<FactSettlement>,
    limit: usize,
    start: M,
    waiting: VecDeque<Fact>,
    active: Vec<(Option<String>, Pin<Box<T>>)>,
}

impl<T, M> Future for Pump<'_, T, M>
where
    T: Future<Output = FactSettlement>,
    M: FnMut(Fact) -> Pin<Box<T>> + Unpin,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        // Register before checking so a concurrent push cannot be missed.
        *this.shared.waker.lock().expect("waker lock") = Some(cx.waker().clone());
        loop {
            this.waiting
                .extend(this.shared.queue.lock().expect("fact queue lock").drain(..));
            // Admit in order; a lane is blocked by an active commit or by an
            // earlier waiting fact of the same lane.
            let mut blocked: HashSet<Option<String>> =
                this.active.iter().map(|(lane, _)| lane.clone()).collect();
            let mut i = 0;
            while i < this.waiting.len() && this.active.len() < this.limit {
                let lane = this.waiting[i].principal.clone();
                if blocked.contains(&lane) {
                    i += 1;
                    continue;
                }
                let fact = this.waiting.remove(i).expect("index in range");
                blocked.insert(lane.clone());
                this.active.push((lane, (this.start)(fact)));
            }
            let mut progressed = false;
            let mut j = 0;
            while j < this.active.len() {
                match this.active[j].1.as_mut().poll(cx) {
                    Poll::Ready(settlement) => {
                        let _ = this.settlements.send(settlement);
                        this.active.remove(j);
                        progressed = true;
                    }
                    Poll::Pending => j += 1,
                }
            }
            if !progressed {
                break;
            }
        }
        let drained = this.active.is_empty()
            && this.waiting.is_empty()
            && this
                .shared
                .queue
                .lock()
                .expect("fact queue lock")
                .is_empty();
        if drained && this.shared.closed.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

/// Run a [`FactFeed`] on the current Tokio runtime.
#[cfg(feature = "runtime")]
pub fn spawn_fact_worker<S: FactSink>(feed: FactFeed, sink: S) -> tokio::task::JoinHandle<()> {
    tokio::spawn(feed.run(sink, tokio::time::sleep))
}

/// In-memory sink that records commits and replays repeated keys.
/// Useful for tests and local development.
#[derive(Debug, Default, Clone)]
pub struct MemoryFactSink {
    committed: Arc<Mutex<Vec<Fact>>>,
}

impl MemoryFactSink {
    /// Empty sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// Facts committed so far (one per key).
    pub fn committed(&self) -> Vec<Fact> {
        self.committed.lock().expect("sink lock").clone()
    }
}

impl FactSink for MemoryFactSink {
    fn commit<'a>(&'a self, fact: &'a Fact) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
        Box::pin(async move {
            let mut committed = self.committed.lock().expect("sink lock");
            let replayed = committed.iter().any(|f| f.key == fact.key);
            if !replayed {
                committed.push(fact.clone());
            }
            Ok(FactReceipt {
                payload: Value::Null,
                replayed,
            })
        })
    }
}

impl<S: FactSink> FactSink for Arc<S> {
    fn commit<'a>(&'a self, fact: &'a Fact) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
        (**self).commit(fact)
    }
}
