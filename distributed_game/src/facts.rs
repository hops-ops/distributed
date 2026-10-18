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
    /// `key` must be unique per logical outcome (e.g. `"kill:{zone}:{tick}:{enemy}"`);
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
}

/// Worker-side handle consumed by [`FactFeed::run`].
#[derive(Debug)]
pub struct FactFeed {
    shared: Arc<Shared>,
    settlements: mpsc::Sender<FactSettlement>,
    policy: RetryPolicy,
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
        },
        FactFeed {
            shared,
            settlements: tx,
            policy,
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
    /// Commit facts in emission order until the queue is dropped and drained.
    /// `sleep` provides the runtime's timer for backoff.
    pub async fn run<S, F, Fut>(self, sink: S, sleep: F)
    where
        S: FactSink,
        F: Fn(Duration) -> Fut,
        Fut: Future<Output = ()>,
    {
        while let Some(fact) = NextFact(&self.shared).await {
            let mut attempts = 0;
            let outcome = loop {
                attempts += 1;
                match sink.commit(&fact).await {
                    Ok(receipt) => break FactOutcome::Committed(receipt),
                    Err(FactError::Retryable(_)) if attempts < self.policy.max_attempts.max(1) => {
                        sleep(self.policy.backoff(attempts)).await;
                    }
                    Err(error) => break FactOutcome::Failed { error, attempts },
                }
            };
            let _ = self.settlements.send(FactSettlement {
                key: fact.key,
                command: fact.command,
                outcome,
            });
        }
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
