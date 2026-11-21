//! Fire-and-forget leaderboard updates on a background task.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::DomainEventOccurrence;

use super::{LeaderboardErrorKind, LeaderboardProjection, LeaderboardProjector, LeaderboardStore};

/// Tuning for [`BackgroundLeaderboard`].
#[derive(Debug, Clone)]
pub struct BackgroundLeaderboardOptions {
    /// Queue capacity; [`BackgroundLeaderboard::submit`] drops when full.
    pub capacity: usize,
    /// Attempts per occurrence while the store is unavailable (>= 1).
    pub max_attempts: u32,
    /// Delay before the first retry; doubles per attempt.
    pub retry_backoff: Duration,
}

impl Default for BackgroundLeaderboardOptions {
    fn default() -> Self {
        Self {
            capacity: 4_096,
            max_attempts: 3,
            retry_backoff: Duration::from_millis(50),
        }
    }
}

/// Counters exposed by [`BackgroundLeaderboard::stats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackgroundStats {
    /// Occurrences accepted into the queue.
    pub enqueued: u64,
    /// Occurrences rejected because the queue was full or closed.
    pub dropped: u64,
    /// Occurrences fully applied (including duplicates).
    pub applied: u64,
    /// Occurrences abandoned after an invalid change or exhausted retries.
    pub failed: u64,
}

#[derive(Default)]
struct Counters {
    enqueued: AtomicU64,
    dropped: AtomicU64,
    applied: AtomicU64,
    failed: AtomicU64,
}

/// A bounded in-process queue in front of a [`LeaderboardProjector`].
///
/// [`submit`](Self::submit) never awaits and never fails the caller: it is safe
/// to call from a gameplay command path or zone tick. A background task applies
/// occurrences in order, retrying while the store is unavailable; when the
/// queue is full or retries are exhausted the update is dropped and counted.
/// Because applies are idempotent, any loss is repaired by replaying the event
/// stream or rebuilding the board — for guaranteed delivery feed the projector
/// from the outbox instead (see the [module docs](super)).
pub struct BackgroundLeaderboard {
    tx: mpsc::Sender<DomainEventOccurrence>,
    counters: Arc<Counters>,
}

impl BackgroundLeaderboard {
    /// Spawn the worker on the current Tokio runtime. The worker stops once
    /// every handle is dropped and the queue drains.
    pub fn spawn<S, P>(
        projector: LeaderboardProjector<S, P>,
        options: BackgroundLeaderboardOptions,
    ) -> Self
    where
        S: LeaderboardStore + ?Sized + 'static,
        P: LeaderboardProjection + ?Sized + 'static,
    {
        let (tx, mut rx) = mpsc::channel::<DomainEventOccurrence>(options.capacity.max(1));
        let counters = Arc::new(Counters::default());
        let worker = Arc::clone(&counters);
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let mut backoff = options.retry_backoff;
                let mut attempt = 1;
                loop {
                    match projector.apply_event(&event).await {
                        Ok(_) => {
                            worker.applied.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(err)
                            if err.kind() == LeaderboardErrorKind::Unavailable
                                && attempt < options.max_attempts.max(1) =>
                        {
                            tokio::time::sleep(backoff).await;
                            backoff = backoff.saturating_mul(2);
                            attempt += 1;
                        }
                        Err(_) => {
                            worker.failed.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }
        });
        Self { tx, counters }
    }

    /// Enqueue an occurrence without waiting; returns `false` (and counts a
    /// drop) when the queue is full or the worker has stopped.
    pub fn submit(&self, event: DomainEventOccurrence) -> bool {
        match self.tx.try_send(event) {
            Ok(()) => {
                self.counters.enqueued.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(_) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// A snapshot of the counters.
    pub fn stats(&self) -> BackgroundStats {
        BackgroundStats {
            enqueued: self.counters.enqueued.load(Ordering::Relaxed),
            dropped: self.counters.dropped.load(Ordering::Relaxed),
            applied: self.counters.applied.load(Ordering::Relaxed),
            failed: self.counters.failed.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leaderboard::{
        ApplyKey, ApplyOutcome, Board, InMemoryLeaderboard, LeaderboardChange, LeaderboardEntry,
        LeaderboardError, ScorePolicy, ScoreSubmission,
    };
    use async_trait::async_trait;
    use std::sync::atomic::AtomicU32;
    use std::time::UNIX_EPOCH;

    #[derive(serde::Serialize, serde::Deserialize, crate::DomainEvent)]
    #[domain_event(name = "test.points_won", version = 1)]
    struct PointsWon {
        points: u32,
    }

    fn event(position: u64) -> DomainEventOccurrence {
        DomainEventOccurrence::capture_external(
            crate::ExternalEventSource {
                producer: "zone.sim".into(),
                stream: "zone-1".into(),
                position,
                key: "points".into(),
            },
            UNIX_EPOCH,
            Default::default(),
            &PointsWon { points: 1 },
        )
        .unwrap()
    }

    fn board() -> Board {
        Board::high_first("points").unwrap()
    }

    fn projection(_: &DomainEventOccurrence) -> Result<Vec<LeaderboardChange>, LeaderboardError> {
        Ok(vec![LeaderboardChange::new(
            board(),
            ScoreSubmission::new("p", 1.0, ScorePolicy::Increment),
        )])
    }

    /// Fails the first `failures` applies as unavailable, then delegates.
    struct Flaky {
        inner: InMemoryLeaderboard,
        failures: AtomicU32,
    }

    #[async_trait]
    impl LeaderboardStore for Flaky {
        async fn submit(&self, b: &Board, s: &ScoreSubmission) -> Result<f64, LeaderboardError> {
            self.inner.submit(b, s).await
        }
        async fn apply(
            &self,
            b: &Board,
            s: &ScoreSubmission,
            k: &ApplyKey,
        ) -> Result<ApplyOutcome, LeaderboardError> {
            // Only the single background worker calls `apply`, so load-then-store is race-free.
            let remaining = self.failures.load(Ordering::SeqCst);
            if remaining > 0 {
                self.failures.store(remaining - 1, Ordering::SeqCst);
                return Err(LeaderboardError::unavailable("blip"));
            }
            self.inner.apply(b, s, k).await
        }
        async fn score(&self, b: &Board, m: &str) -> Result<Option<f64>, LeaderboardError> {
            self.inner.score(b, m).await
        }
        async fn rank(&self, b: &Board, m: &str) -> Result<Option<u64>, LeaderboardError> {
            self.inner.rank(b, m).await
        }
        async fn top(
            &self,
            b: &Board,
            n: usize,
        ) -> Result<Vec<LeaderboardEntry>, LeaderboardError> {
            self.inner.top(b, n).await
        }
        async fn around(
            &self,
            b: &Board,
            m: &str,
            r: usize,
        ) -> Result<Vec<LeaderboardEntry>, LeaderboardError> {
            self.inner.around(b, m, r).await
        }
        async fn len(&self, b: &Board) -> Result<u64, LeaderboardError> {
            self.inner.len(b).await
        }
        async fn remove(&self, b: &Board, m: &str) -> Result<bool, LeaderboardError> {
            self.inner.remove(b, m).await
        }
        async fn reset(&self, b: &Board) -> Result<(), LeaderboardError> {
            self.inner.reset(b).await
        }
        async fn promote(&self, b: &Board) -> Result<(), LeaderboardError> {
            self.inner.promote(b).await
        }
        async fn prune_applied(&self, b: &Board, t: u64) -> Result<u64, LeaderboardError> {
            self.inner.prune_applied(b, t).await
        }
    }

    async fn settle(handle: &BackgroundLeaderboard, done: u64) {
        for _ in 0..200 {
            let stats = handle.stats();
            if stats.applied + stats.failed >= done {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("background worker did not settle: {:?}", handle.stats());
    }

    #[tokio::test]
    async fn retries_through_a_blip_then_drops_when_exhausted() {
        let store = Arc::new(Flaky {
            inner: InMemoryLeaderboard::new(),
            failures: AtomicU32::new(2),
        });
        let projector = LeaderboardProjector::new(Arc::clone(&store), Arc::new(projection));
        let options = BackgroundLeaderboardOptions {
            capacity: 8,
            max_attempts: 3,
            retry_backoff: Duration::from_millis(1),
        };
        let handle = BackgroundLeaderboard::spawn(projector, options);
        assert!(handle.submit(event(1)));
        settle(&handle, 1).await;
        assert_eq!(store.score(&board(), "p").await.unwrap(), Some(1.0));

        store.failures.store(10, Ordering::SeqCst);
        assert!(handle.submit(event(2)));
        settle(&handle, 2).await;
        let stats = handle.stats();
        assert_eq!((stats.applied, stats.failed), (1, 1));
        assert_eq!(store.score(&board(), "p").await.unwrap(), Some(1.0));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn submit_never_blocks_and_counts_drops_when_full() {
        let projector =
            LeaderboardProjector::new(Arc::new(InMemoryLeaderboard::new()), Arc::new(projection));
        let options = BackgroundLeaderboardOptions {
            capacity: 2,
            ..BackgroundLeaderboardOptions::default()
        };
        let handle = BackgroundLeaderboard::spawn(projector, options);
        // The current-thread worker cannot run until we yield, so the third
        // submit finds the queue full.
        assert!(handle.submit(event(1)));
        assert!(handle.submit(event(2)));
        assert!(!handle.submit(event(3)));
        assert_eq!(handle.stats().dropped, 1);
        settle(&handle, 2).await;
        assert_eq!(handle.stats().applied, 2);
    }
}
