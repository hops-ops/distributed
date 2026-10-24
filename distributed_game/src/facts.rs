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
//!
//! Batching: the feed hands a sink up to
//! [`max_batch`](FactFeed::with_max_batch) (default
//! [`DEFAULT_MAX_BATCH`]) consecutive facts of one lane at once
//! ([`FactSink::commit_batch`]). A sink that can settle many facts in
//! one round trip (the celld cell hosts: one `__batch` request per cell)
//! overrides it; the default commits them one by one. A batch commits in
//! order and stops at its first retryable failure, which is retried from
//! there, so order and exactly-once hold as for single facts.

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

    /// Commit `facts` in order: one result per fact attempted. The
    /// results end at the first [`FactError::Retryable`] (included), or
    /// earlier; facts past the end were not attempted and are retried.
    /// The default commits them one by one.
    fn commit_batch<'a>(
        &'a self,
        facts: &'a [Fact],
    ) -> BoxFuture<'a, Vec<Result<FactReceipt, FactError>>> {
        Box::pin(async move {
            let mut results = Vec::with_capacity(facts.len());
            for fact in facts {
                let result = self.commit(fact).await;
                let stop = matches!(result, Err(FactError::Retryable(_)));
                results.push(result);
                if stop {
                    break;
                }
            }
            results
        })
    }
}

/// Facts per [`FactSink::commit_batch`] unless the feed says otherwise.
pub const DEFAULT_MAX_BATCH: usize = 16;

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
    journal: Option<Vec<Fact>>,
}

/// Worker-side handle consumed by [`FactFeed::run`].
#[derive(Debug)]
pub struct FactFeed {
    shared: Arc<Shared>,
    settlements: mpsc::Sender<FactSettlement>,
    policy: RetryPolicy,
    max_concurrent: usize,
    max_batch: usize,
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
            journal: None,
        },
        FactFeed {
            shared,
            settlements: tx,
            policy,
            max_concurrent: 1,
            max_batch: DEFAULT_MAX_BATCH,
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
        if let Some(journal) = self.journal.as_mut() {
            journal.push(fact.clone());
        }
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

    /// Keep a copy of every accepted fact until [`take_journal`](Self::take_journal):
    /// a host that keeps its own durable outbox writes them there right
    /// after the tick that emitted them (the celld zone host does).
    pub fn with_journal(mut self) -> Self {
        self.journal = Some(Vec::new());
        self
    }

    /// Facts accepted since the last call (empty without a journal).
    pub fn take_journal(&mut self) -> Vec<Fact> {
        self.journal
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
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

/// The next facts in emission order (at most `.1`).
struct NextFact<'a>(&'a Shared, usize);

impl Future for NextFact<'_> {
    type Output = Option<Vec<Fact>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let shared = self.0;
        // Register before checking so a concurrent push cannot be missed.
        *shared.waker.lock().expect("waker lock") = Some(cx.waker().clone());
        let mut queue = shared.queue.lock().expect("fact queue lock");
        if !queue.is_empty() {
            let n = queue.len().min(self.1);
            return Poll::Ready(Some(queue.drain(..n).collect()));
        }
        drop(queue);
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

    /// Hand the sink up to `n` facts of a lane at once (default
    /// [`DEFAULT_MAX_BATCH`]; 1 commits fact by fact).
    pub fn with_max_batch(mut self, n: usize) -> Self {
        self.max_batch = n.max(1);
        self
    }

    /// Facts per batch at most.
    pub fn max_batch(&self) -> usize {
        self.max_batch
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
            while let Some(facts) = NextFact(&self.shared, self.max_batch).await {
                for settlement in commit_with_retries(&sink, facts, policy, &sleep).await {
                    let _ = self.settlements.send(settlement);
                }
            }
            return;
        }
        let start = |facts: Vec<Fact>| Box::pin(commit_with_retries(&sink, facts, policy, &sleep));
        Pump {
            shared: &self.shared,
            settlements: &self.settlements,
            limit: self.max_concurrent,
            max_batch: self.max_batch,
            start,
            waiting: VecDeque::new(),
            active: Vec::new(),
        }
        .await;
    }
}

/// Commit `facts` in order through [`FactSink::commit_batch`], retrying
/// from the first unsettled fact; `policy` bounds the attempts at each
/// fact as if it were committed alone.
async fn commit_with_retries<S, F, Fut>(
    sink: &S,
    facts: Vec<Fact>,
    policy: RetryPolicy,
    sleep: &F,
) -> Vec<FactSettlement>
where
    S: FactSink,
    F: Fn(Duration) -> Fut,
    Fut: Future<Output = ()>,
{
    let max_attempts = policy.max_attempts.max(1);
    let mut settled = Vec::with_capacity(facts.len());
    let mut pending: VecDeque<Fact> = facts.into();
    // Attempts at the head of `pending`.
    let mut attempts = 0;
    while !pending.is_empty() {
        attempts += 1;
        let results = sink.commit_batch(pending.make_contiguous()).await;
        let answered = results.len().min(pending.len());
        // A sink that answered for nothing is retried like a retryable head.
        let mut retry = answered == 0;
        for result in results.into_iter().take(answered) {
            let outcome = match result {
                Ok(receipt) => FactOutcome::Committed(receipt),
                Err(FactError::Retryable(_)) if attempts < max_attempts => {
                    retry = true;
                    break;
                }
                Err(error) => FactOutcome::Failed { error, attempts },
            };
            let fact = pending.pop_front().expect("a result per attempted fact");
            settled.push(FactSettlement {
                key: fact.key,
                command: fact.command,
                outcome,
            });
            // The next result is the next fact's first attempt.
            attempts = 1;
        }
        if !retry {
            // The facts left (if any) were not attempted yet.
            attempts = 0;
        } else if answered == 0 && attempts >= max_attempts {
            let fact = pending.pop_front().expect("a pending fact");
            settled.push(FactSettlement {
                key: fact.key,
                command: fact.command,
                outcome: FactOutcome::Failed {
                    error: FactError::Retryable("the sink committed nothing".into()),
                    attempts,
                },
            });
            attempts = 0;
        } else {
            sleep(policy.backoff(attempts)).await;
        }
    }
    settled
}

/// Concurrent commit driver: at most `limit` commits in flight, one per
/// principal lane, admitted in emission order.
struct Pump<'a, T, M> {
    shared: &'a Shared,
    settlements: &'a mpsc::Sender<FactSettlement>,
    limit: usize,
    max_batch: usize,
    start: M,
    waiting: VecDeque<Fact>,
    active: Vec<(Option<String>, Pin<Box<T>>)>,
}

impl<T, M> Future for Pump<'_, T, M>
where
    T: Future<Output = Vec<FactSettlement>>,
    M: FnMut(Vec<Fact>) -> Pin<Box<T>> + Unpin,
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
                // The lane's waiting facts, in order, up to a batch.
                let mut batch = vec![this.waiting.remove(i).expect("index in range")];
                let mut k = i;
                while k < this.waiting.len() && batch.len() < this.max_batch {
                    if this.waiting[k].principal == lane {
                        batch.push(this.waiting.remove(k).expect("index in range"));
                    } else {
                        k += 1;
                    }
                }
                blocked.insert(lane.clone());
                this.active.push((lane, (this.start)(batch)));
            }
            let mut progressed = false;
            let mut j = 0;
            while j < this.active.len() {
                match this.active[j].1.as_mut().poll(cx) {
                    Poll::Ready(settlements) => {
                        for settlement in settlements {
                            let _ = this.settlements.send(settlement);
                        }
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

    fn commit_batch<'a>(
        &'a self,
        facts: &'a [Fact],
    ) -> BoxFuture<'a, Vec<Result<FactReceipt, FactError>>> {
        (**self).commit_batch(facts)
    }
}

#[cfg(test)]
mod batch_tests {
    use super::*;

    /// Commits whole batches, recording each; `flaky` answers retryable
    /// `failures` times when it reaches that key.
    #[derive(Default)]
    struct BatchSink {
        batches: Mutex<Vec<Vec<String>>>,
        committed: Mutex<Vec<String>>,
        flaky: Option<(String, Mutex<u32>)>,
    }

    impl FactSink for BatchSink {
        fn commit<'a>(&'a self, _: &'a Fact) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
            unreachable!("batches only")
        }

        fn commit_batch<'a>(
            &'a self,
            facts: &'a [Fact],
        ) -> BoxFuture<'a, Vec<Result<FactReceipt, FactError>>> {
            Box::pin(async move {
                let keys = facts.iter().map(|f| f.key().to_string()).collect();
                self.batches.lock().unwrap().push(keys);
                let mut results = Vec::new();
                for fact in facts {
                    if let Some((key, failures)) = &self.flaky {
                        let mut failures = failures.lock().unwrap();
                        if fact.key() == key && *failures > 0 {
                            *failures -= 1;
                            results.push(Err(FactError::Retryable("busy".into())));
                            break;
                        }
                    }
                    let mut committed = self.committed.lock().unwrap();
                    let replayed = committed.iter().any(|k| k == fact.key());
                    if !replayed {
                        committed.push(fact.key().to_string());
                    }
                    results.push(Ok(FactReceipt {
                        payload: Value::Null,
                        replayed,
                    }));
                }
                results
            })
        }
    }

    fn fact(key: &str, principal: Option<&str>) -> Fact {
        Fact::new(key, "credit", Value::Null)
            .bind(principal.map(str::to_string), FactStamp::default())
    }

    async fn settle(queue: &mut FactQueue, n: usize) -> Vec<FactSettlement> {
        let mut settled = Vec::new();
        while settled.len() < n {
            settled.extend(queue.drain());
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        settled
    }

    fn keys(settled: &[FactSettlement]) -> Vec<&str> {
        settled.iter().map(|s| s.key.as_str()).collect()
    }

    fn policy(max_attempts: u32) -> RetryPolicy {
        RetryPolicy {
            max_attempts,
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        }
    }

    #[tokio::test]
    async fn a_lane_settles_in_batches_in_order() {
        let (mut queue, feed) = fact_channel(64, policy(3));
        for i in 0..5 {
            queue
                .try_send(fact(&format!("a{i}"), Some("alice")))
                .unwrap();
        }
        for i in 0..3 {
            queue.try_send(fact(&format!("b{i}"), Some("bob"))).unwrap();
        }
        let sink = Arc::new(BatchSink::default());
        let feed = feed.with_max_concurrent_commits(4).with_max_batch(4);
        let worker = tokio::spawn(feed.run(sink.clone(), tokio::time::sleep));
        let settled = settle(&mut queue, 8).await;
        drop(queue);
        worker.await.unwrap();

        let batches = sink.batches.lock().unwrap().clone();
        assert!(batches.contains(&vec!["a0".into(), "a1".into(), "a2".into(), "a3".into()]));
        assert!(batches.contains(&vec!["b0".into(), "b1".into(), "b2".into()]));
        assert!(batches.contains(&vec!["a4".into()]));
        assert_eq!(batches.len(), 3);
        let lane = |p: char| -> Vec<&str> {
            keys(&settled)
                .into_iter()
                .filter(|k| k.starts_with(p))
                .collect()
        };
        assert_eq!(lane('a'), ["a0", "a1", "a2", "a3", "a4"]);
        assert_eq!(lane('b'), ["b0", "b1", "b2"]);
        assert!(settled
            .iter()
            .all(|s| matches!(s.outcome, FactOutcome::Committed(_))));
    }

    #[tokio::test]
    async fn a_batch_retries_from_its_first_retryable_fact() {
        let (mut queue, feed) = fact_channel(64, policy(3));
        for i in 0..5 {
            queue.try_send(fact(&format!("f{i}"), None)).unwrap();
        }
        let sink = Arc::new(BatchSink {
            flaky: Some(("f2".into(), Mutex::new(1))),
            ..BatchSink::default()
        });
        let worker = tokio::spawn(feed.with_max_batch(8).run(sink.clone(), tokio::time::sleep));
        let settled = settle(&mut queue, 5).await;
        drop(queue);
        worker.await.unwrap();

        assert_eq!(keys(&settled), ["f0", "f1", "f2", "f3", "f4"]);
        assert!(settled
            .iter()
            .all(|s| matches!(s.outcome, FactOutcome::Committed(_))));
        // Exactly once each, in order; the retry resent only f2 onward.
        assert_eq!(
            *sink.committed.lock().unwrap(),
            ["f0", "f1", "f2", "f3", "f4"]
        );
        assert_eq!(
            *sink.batches.lock().unwrap(),
            [vec!["f0", "f1", "f2", "f3", "f4"], vec!["f2", "f3", "f4"]]
        );
    }

    #[tokio::test]
    async fn attempts_count_per_fact_in_a_batch() {
        let (mut queue, feed) = fact_channel(64, policy(2));
        for i in 0..3 {
            queue.try_send(fact(&format!("f{i}"), None)).unwrap();
        }
        let sink = Arc::new(BatchSink {
            flaky: Some(("f1".into(), Mutex::new(u32::MAX))),
            ..BatchSink::default()
        });
        let worker = tokio::spawn(feed.with_max_batch(8).run(sink.clone(), tokio::time::sleep));
        let settled = settle(&mut queue, 3).await;
        drop(queue);
        worker.await.unwrap();

        assert_eq!(keys(&settled), ["f0", "f1", "f2"]);
        assert!(matches!(settled[0].outcome, FactOutcome::Committed(_)));
        assert!(matches!(
            settled[1].outcome,
            FactOutcome::Failed {
                error: FactError::Retryable(_),
                attempts: 2
            }
        ));
        assert!(matches!(settled[2].outcome, FactOutcome::Committed(_)));
        assert_eq!(*sink.committed.lock().unwrap(), ["f0", "f2"]);
    }
}
