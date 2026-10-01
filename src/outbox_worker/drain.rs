//! Cancellable background drain over [`OutboxDispatcher::dispatch_batch`].
//!
//! After-commit publish is a hint onto this same loop (`dispatch_ids`), not a
//! spawn per command. Polling `dispatch_batch` is the crash/overflow net for
//! pending rows. It does not resurrect the old in-memory `OutboxWorker`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

use crate::bus::{MessagePublisher, TransportError};

use super::{OutboxDispatcher, OutboxStore};

/// Default time between empty drain passes.
pub const DEFAULT_DRAIN_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Default claim batch for one drain pass.
pub const DEFAULT_DRAIN_BATCH_SIZE: usize = 32;

/// Default lease for drain claims. Longer than the immediate hook's 5s lease
/// so a pass can publish a real backlog without the row becoming claimable
/// by a second drainer.
pub const DEFAULT_DRAIN_LEASE: Duration = Duration::from_secs(30);

/// Default first backoff after a store error. Doubles up to
/// [`DEFAULT_DRAIN_MAX_ERROR_BACKOFF`].
pub const DEFAULT_DRAIN_ERROR_BACKOFF: Duration = Duration::from_secs(1);

/// Ceiling for store-error backoff.
pub const DEFAULT_DRAIN_MAX_ERROR_BACKOFF: Duration = Duration::from_secs(30);

/// Worker-id prefix that distinguishes drain claims from
/// `microsvc-immediate:<pid>`.
pub fn drain_worker_id() -> String {
    format!("drain:{}", std::process::id())
}

/// How many commit-id batches the after-commit mailbox will hold before
/// overflowing to a wake + `dispatch_batch` of pending rows.
pub const DEFAULT_OUTBOX_HINT_CAPACITY: usize = 256;

/// Bounded after-commit mailbox: `try_send` ids, or `notify_one` on overflow
/// so the same worker claims pending rows instead of spawning a task.
#[derive(Clone)]
pub struct OutboxPublishMailbox {
    tx: mpsc::Sender<Vec<String>>,
    wake: Arc<Notify>,
}

impl OutboxPublishMailbox {
    /// Create a mailbox, the worker's hint receiver, and the shared wake.
    pub fn channel(capacity: usize) -> (Self, mpsc::Receiver<Vec<String>>, Arc<Notify>) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let wake = Arc::new(Notify::new());
        (
            Self {
                tx,
                wake: Arc::clone(&wake),
            },
            rx,
            wake,
        )
    }

    /// Enqueue ids for `dispatch_ids`. If the channel is full or closed, wake
    /// the worker so `dispatch_batch` can claim the pending rows. Never waits.
    pub fn try_submit(&self, ids: Vec<String>) {
        if ids.is_empty() {
            return;
        }
        if self.tx.try_send(ids).is_err() {
            self.wake.notify_one();
        }
    }
}

/// Repeatedly [`OutboxDispatcher::dispatch_batch`] until cancelled.
pub struct OutboxDrainRunner<S, P> {
    dispatcher: OutboxDispatcher<S, P>,
    batch_size: usize,
    poll_interval: Duration,
    error_backoff: Duration,
    max_error_backoff: Duration,
    hint_rx: Option<mpsc::Receiver<Vec<String>>>,
    wake: Option<Arc<Notify>>,
}

impl<S, P> OutboxDrainRunner<S, P>
where
    S: OutboxStore,
    P: MessagePublisher,
{
    /// Wrap an existing dispatcher. Reuse the dispatcher's lease / attempts /
    /// concurrency rather than duplicating them here.
    pub fn new(dispatcher: OutboxDispatcher<S, P>) -> Self {
        Self {
            dispatcher,
            batch_size: DEFAULT_DRAIN_BATCH_SIZE,
            poll_interval: DEFAULT_DRAIN_POLL_INTERVAL,
            error_backoff: DEFAULT_DRAIN_ERROR_BACKOFF,
            max_error_backoff: DEFAULT_DRAIN_MAX_ERROR_BACKOFF,
            hint_rx: None,
            wake: None,
        }
    }

    /// Build a dispatcher + runner with the drain worker id and default lease.
    pub fn for_store(store: S, publisher: P, max_attempts: u32) -> Self {
        Self::new(OutboxDispatcher::new(
            store,
            publisher,
            drain_worker_id(),
            DEFAULT_DRAIN_LEASE,
            max_attempts,
        ))
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub fn with_error_backoff(mut self, error_backoff: Duration) -> Self {
        self.error_backoff = error_backoff;
        self
    }

    pub fn with_max_error_backoff(mut self, max_error_backoff: Duration) -> Self {
        self.max_error_backoff = max_error_backoff;
        self
    }

    /// Receive after-commit id hints and overflow wakes on this loop.
    pub fn with_hints(mut self, hint_rx: mpsc::Receiver<Vec<String>>, wake: Arc<Notify>) -> Self {
        self.hint_rx = Some(hint_rx);
        self.wake = Some(wake);
        self
    }

    /// The dispatcher this runner drives.
    pub fn dispatcher(&self) -> &OutboxDispatcher<S, P> {
        &self.dispatcher
    }

    /// Drain until `shutdown` resolves. Store errors back off; they do not
    /// terminate the loop. Empty passes sleep `poll_interval`. A full batch
    /// is followed immediately by another pass. After-commit hints run
    /// `dispatch_ids` on this same worker.
    pub async fn run(self, shutdown: impl Future<Output = ()>) -> Result<(), TransportError> {
        tokio::pin!(shutdown);
        let mut backoff = self.error_backoff;
        let mut hint_rx = self.hint_rx;
        let wake = self.wake;
        let mut sleep_for = Duration::ZERO;
        loop {
            // Hints must beat sleep(0) / poll. Unbiased select can run
            // dispatch_batch on scrape backlog while a command's Eventual
            // `projected` waits for its id still sitting in the mailbox.
            tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(()),
                hint = next_hint(&mut hint_rx) => {
                    if let Some(ids) = hint {
                        let ids = coalesce_hints(&mut hint_rx, ids, self.batch_size);
                        match self.dispatcher.dispatch_ids(&ids).await {
                            Ok(_) => backoff = self.error_backoff,
                            Err(error) => {
                                eprintln!("outbox drain: {error}");
                                backoff = backoff.saturating_mul(2).min(self.max_error_backoff);
                                sleep_for = backoff;
                            }
                        }
                    }
                    continue;
                }
                _ = wake_notified(&wake) => {}
                _ = tokio::time::sleep(sleep_for) => {}
            }
            match self.dispatcher.dispatch_batch(self.batch_size).await {
                Ok(outcome) => {
                    backoff = self.error_backoff;
                    sleep_for = if outcome.claimed >= self.batch_size {
                        Duration::ZERO
                    } else {
                        self.poll_interval
                    };
                }
                Err(error) => {
                    eprintln!("outbox drain: {error}");
                    sleep_for = backoff;
                    backoff = backoff.saturating_mul(2).min(self.max_error_backoff);
                }
            }
        }
    }

    /// Spawn on the current tokio runtime. Dropping the handle does **not**
    /// stop the task (call [`OutboxDrainHandle::stop`]).
    pub fn spawn(self) -> OutboxDrainHandle
    where
        S: 'static,
        P: 'static,
    {
        let join = tokio::spawn(async move { self.run(std::future::pending()).await });
        OutboxDrainHandle { join }
    }
}

async fn next_hint(hint_rx: &mut Option<mpsc::Receiver<Vec<String>>>) -> Option<Vec<String>> {
    loop {
        match hint_rx.as_mut() {
            None => std::future::pending::<()>().await,
            Some(rx) => match rx.recv().await {
                Some(ids) => return Some(ids),
                None => {
                    *hint_rx = None;
                }
            },
        }
    }
}

fn coalesce_hints(
    hint_rx: &mut Option<mpsc::Receiver<Vec<String>>>,
    ids: Vec<String>,
    limit: usize,
) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut bounded = Vec::with_capacity(limit.min(ids.len()));
    for id in ids {
        if bounded.len() == limit {
            break;
        }
        if seen.insert(id.clone()) {
            bounded.push(id);
        }
    }
    if let Some(rx) = hint_rx.as_mut() {
        while bounded.len() < limit {
            match rx.try_recv() {
                Ok(more) => {
                    for id in more {
                        if bounded.len() == limit {
                            break;
                        }
                        if seen.insert(id.clone()) {
                            bounded.push(id);
                        }
                    }
                }
                Err(_) => break,
            }
        }
    }
    bounded
}

async fn wake_notified(wake: &Option<Arc<Notify>>) {
    match wake {
        Some(wake) => wake.notified().await,
        None => std::future::pending().await,
    }
}

/// Handle for a spawned [`OutboxDrainRunner`]. Abort-only on [`stop`]; drop
/// leaves the task running so fire-and-forget hosts stay drained.
pub struct OutboxDrainHandle {
    join: JoinHandle<Result<(), TransportError>>,
}

impl OutboxDrainHandle {
    /// Abort the loop and wait for it to unwind.
    pub async fn stop(self) -> Result<(), TransportError> {
        self.join.abort();
        match self.join.await {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(TransportError::retryable(format!(
                "outbox drain task: {error}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::Message;
    use crate::outbox_worker::{ClaimOutboxMessages, OutboxClaimRef, OutboxStore};
    use crate::repository::RepositoryError;

    #[test]
    fn hint_coalescing_deduplicates_and_never_exceeds_batch_limit() {
        let (tx, rx) = mpsc::channel(4);
        tx.try_send(vec!["b".into(), "c".into(), "d".into()])
            .unwrap();
        tx.try_send(vec!["e".into(), "f".into()]).unwrap();
        let mut rx = Some(rx);
        let ids = coalesce_hints(&mut rx, vec!["a".into(), "a".into(), "b".into()], 4);
        assert_eq!(ids, vec!["a", "b", "c", "d"]);
    }
    use crate::{
        CommitBatch, InMemoryOutboxStore, InMemoryRepository, OutboxMessage, OutboxMessageStatus,
        TransactionalCommit,
    };
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    struct RecordingPublisher {
        published: Mutex<Vec<String>>,
    }

    impl RecordingPublisher {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                published: Mutex::new(Vec::new()),
            })
        }
        fn ids(&self) -> Vec<String> {
            self.published.lock().unwrap().clone()
        }
    }

    impl MessagePublisher for Arc<RecordingPublisher> {
        async fn publish(&self, message: Message) -> Result<(), TransportError> {
            self.published
                .lock()
                .unwrap()
                .push(message.id().unwrap_or_default().to_string());
            Ok(())
        }
    }

    fn outbox(id: &str) -> OutboxMessage {
        OutboxMessage::create(id, "OrderCreated", b"{}".to_vec()).unwrap()
    }

    fn store_message(repo: &InMemoryRepository, message: OutboxMessage) -> String {
        let id = message.id().to_string();
        let mut batch = CommitBatch::empty();
        batch.outbox_messages.push(message);
        futures_executor_block(repo.commit_batch(batch)).unwrap();
        id
    }

    fn futures_executor_block<F: std::future::Future>(future: F) -> F::Output {
        // commit_batch is runtime-free; keep that path off the tokio worker.
        crate::outbox_worker::testing::block_on(future)
    }

    #[tokio::test]
    async fn immediate_publish_leaves_nothing_for_the_drainer() {
        let repo = InMemoryRepository::new();
        let id = store_message(&repo, outbox("evt-1"));
        let publisher = RecordingPublisher::new();
        let immediate = OutboxDispatcher::new(
            repo.outbox_store(),
            publisher.clone(),
            "immediate:test",
            Duration::from_secs(60),
            3,
        );
        let outcome = immediate
            .dispatch_ids(std::slice::from_ref(&id))
            .await
            .unwrap();
        assert_eq!(outcome.published, 1);
        assert!(!repo.outbox_storage().read().unwrap().contains_key(&id));

        let drain = OutboxDrainRunner::new(OutboxDispatcher::new(
            repo.outbox_store(),
            publisher.clone(),
            drain_worker_id(),
            Duration::from_secs(60),
            3,
        ))
        .with_batch_size(8)
        .with_poll_interval(Duration::from_millis(5));
        let handle = drain.spawn();
        tokio::time::sleep(Duration::from_millis(30)).await;
        handle.stop().await.unwrap();

        assert_eq!(publisher.ids(), vec!["evt-1".to_string()]);
        assert!(repo.outbox_store().pending(8).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn drain_publishes_rows_the_immediate_path_never_attempted() {
        let repo = InMemoryRepository::new();
        store_message(&repo, outbox("evt-crash"));
        let publisher = RecordingPublisher::new();
        let drain = OutboxDrainRunner::for_store(repo.outbox_store(), publisher.clone(), 3)
            .with_batch_size(8)
            .with_poll_interval(Duration::from_millis(5));
        let handle = drain.spawn();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if publisher.ids() == ["evt-crash"] {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("drain should publish the unclaimed row");
        handle.stop().await.unwrap();
        assert!(!repo
            .outbox_storage()
            .read()
            .unwrap()
            .contains_key("evt-crash"));
    }

    #[tokio::test]
    async fn store_error_is_retried_and_the_task_survives() {
        let repo = InMemoryRepository::new();
        store_message(&repo, outbox("evt-retry"));
        let publisher = RecordingPublisher::new();
        let store = FlakyClaimStore {
            inner: repo.outbox_store(),
            fail_remaining: AtomicU32::new(2),
            claims: AtomicU32::new(0),
        };
        let drain = OutboxDrainRunner::for_store(store, publisher.clone(), 3)
            .with_batch_size(8)
            .with_poll_interval(Duration::from_millis(5))
            .with_error_backoff(Duration::from_millis(5))
            .with_max_error_backoff(Duration::from_millis(20));
        let handle = drain.spawn();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if publisher.ids() == ["evt-retry"] {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("drain should recover after store errors");
        handle.stop().await.unwrap();
    }

    #[tokio::test]
    async fn stop_returns_promptly() {
        let repo = InMemoryRepository::new();
        let publisher = RecordingPublisher::new();
        let drain = OutboxDrainRunner::for_store(repo.outbox_store(), publisher, 3)
            .with_poll_interval(Duration::from_secs(30));
        let handle = drain.spawn();
        let started = std::time::Instant::now();
        handle.stop().await.unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "stop should abort the idle sleep"
        );
    }

    #[tokio::test]
    async fn hint_publishes_without_waiting_for_poll_interval() {
        let repo = InMemoryRepository::new();
        let id = store_message(&repo, outbox("evt-hint"));
        let publisher = RecordingPublisher::new();
        let (mailbox, rx, wake) = OutboxPublishMailbox::channel(8);
        let handle = OutboxDrainRunner::new(OutboxDispatcher::new(
            repo.outbox_store(),
            publisher.clone(),
            "immediate:test",
            Duration::from_secs(30),
            3,
        ))
        .with_poll_interval(Duration::from_secs(30))
        .with_hints(rx, wake)
        .spawn();

        mailbox.try_submit(vec![id]);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if publisher.ids() == ["evt-hint"] {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("hint should publish without waiting for the 30s poll");
        handle.stop().await.unwrap();
        assert!(repo.outbox_store().pending(8).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn overflow_wake_publishes_pending_rows() {
        let repo = InMemoryRepository::new();
        store_message(&repo, outbox("evt-a"));
        store_message(&repo, outbox("evt-b"));
        let publisher = RecordingPublisher::new();
        let (mailbox, rx, wake) = OutboxPublishMailbox::channel(1);
        mailbox.try_submit(vec!["evt-a".to_string()]);
        mailbox.try_submit(vec!["evt-b".to_string()]);
        let handle = OutboxDrainRunner::new(OutboxDispatcher::new(
            repo.outbox_store(),
            publisher.clone(),
            "immediate:test",
            Duration::from_secs(30),
            3,
        ))
        .with_poll_interval(Duration::from_secs(30))
        .with_hints(rx, wake)
        .spawn();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let mut ids = publisher.ids();
                ids.sort();
                if ids == ["evt-a", "evt-b"] {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("overflow wake should drain the pending row the mailbox could not hold");
        handle.stop().await.unwrap();
    }

    struct FlakyClaimStore {
        inner: InMemoryOutboxStore,
        fail_remaining: AtomicU32,
        claims: AtomicU32,
    }

    impl OutboxStore for FlakyClaimStore {
        fn messages_by_status(
            &self,
            status: OutboxMessageStatus,
            limit: usize,
        ) -> impl Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + '_ {
            self.inner.messages_by_status(status, limit)
        }

        fn claim<'a>(
            &'a self,
            request: ClaimOutboxMessages,
        ) -> impl Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + 'a {
            async move {
                if self.fail_remaining.load(Ordering::SeqCst) > 0 {
                    self.fail_remaining.fetch_sub(1, Ordering::SeqCst);
                    return Err(RepositoryError::Storage {
                        operation: "injected claim failure".into(),
                        retryable: true,
                        source: None,
                    });
                }
                self.claims.fetch_add(1, Ordering::SeqCst);
                self.inner.claim(request).await
            }
        }

        fn complete<'a>(
            &'a self,
            claim: &'a OutboxClaimRef,
        ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
            self.inner.complete(claim)
        }

        fn release<'a>(
            &'a self,
            claim: &'a OutboxClaimRef,
            error: &'a str,
        ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
            self.inner.release(claim, error)
        }

        fn fail<'a>(
            &'a self,
            claim: &'a OutboxClaimRef,
            error: &'a str,
        ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
            self.inner.fail(claim, error)
        }
    }
}
