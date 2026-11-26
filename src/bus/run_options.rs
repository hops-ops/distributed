//! How a consumer run behaves: when it ends and what it does with failures.
//!
//! These decisions are made by the runner, above the transport. An adapter
//! only receives, acknowledges, and redelivers after a delay.

use std::sync::Arc;
use std::time::Duration;

use super::{DeadLetterSink, FailurePolicy, Shutdown};

/// Default first redelivery delay; it doubles with each delivery.
pub const DEFAULT_RETRY_BACKOFF_BASE: Duration = Duration::from_millis(50);
/// Default ceiling for a redelivery delay.
pub const DEFAULT_RETRY_BACKOFF_MAX: Duration = Duration::from_secs(5);
/// Default bound on unsettled deliveries for a multi-lane run.
pub const DEFAULT_LANE_WINDOW: usize = 16;

/// The delay before a failed delivery is redelivered:
/// `base * 2^(attempt - 1)`, capped at `max`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryBackoff {
    base: Duration,
    max: Duration,
}

impl RetryBackoff {
    /// Backoff starting at `base` and capped at `max` (at least `base`).
    pub const fn new(base: Duration, max: Duration) -> Self {
        let max = if max.as_nanos() < base.as_nanos() {
            base
        } else {
            max
        };
        Self { base, max }
    }

    /// Redeliver at once. A failing dependency then gets a tight retry loop;
    /// use it only in tests.
    pub fn immediate() -> Self {
        Self::new(Duration::ZERO, Duration::ZERO)
    }

    /// The delay after the `attempt`th delivery (1-based) failed.
    pub fn delay(&self, attempt: u32) -> Duration {
        let doublings = attempt.saturating_sub(1).min(30);
        self.base
            .checked_mul(1u32 << doublings)
            .unwrap_or(self.max)
            .min(self.max)
    }
}

impl Default for RetryBackoff {
    fn default() -> Self {
        Self::new(DEFAULT_RETRY_BACKOFF_BASE, DEFAULT_RETRY_BACKOFF_MAX)
    }
}

/// When a consumer run ends without an error.
#[derive(Clone, Debug)]
pub enum RunUntil {
    /// Keep consuming until the signal is triggered. A quiet source is not a
    /// reason to stop.
    Shutdown(Shutdown),
    /// Return as soon as the source has nothing to deliver. For tests and
    /// one-shot drains.
    Idle,
}

/// Policy for one consumer run.
#[derive(Clone)]
pub struct RunOptions {
    /// When the run ends.
    pub until: RunUntil,
    /// What happens to a permanent failure.
    pub failure_policy: FailurePolicy,
    /// Spacing of redeliveries.
    pub backoff: RetryBackoff,
    /// Deliveries after which a still-failing message is dead-lettered.
    /// `None` retries without limit.
    pub max_attempts: Option<u32>,
    /// Where dead letters are stored. A bus fills this with its own sink when
    /// it is `None`; a bare [`run_source`](super::run_source) with no sink ends
    /// the run instead of dead-lettering.
    pub dead_letters: Option<Arc<dyn DeadLetterSink>>,
    /// Maximum received-but-unsettled deliveries when a router runs several
    /// delivery lanes concurrently. Ignored for single-lane routers.
    pub lane_window: usize,
}

impl Default for RunOptions {
    /// Run until the process ends, retry transient failures without limit with
    /// the default backoff, and dead-letter permanent failures.
    fn default() -> Self {
        Self {
            until: RunUntil::Shutdown(Shutdown::new()),
            failure_policy: FailurePolicy::default(),
            backoff: RetryBackoff::default(),
            max_attempts: None,
            dead_letters: None,
            lane_window: DEFAULT_LANE_WINDOW,
        }
    }
}

impl RunOptions {
    /// Run until `shutdown` is triggered.
    pub fn until_shutdown(shutdown: Shutdown) -> Self {
        Self {
            until: RunUntil::Shutdown(shutdown),
            ..Self::default()
        }
    }

    /// Return as soon as the source has nothing to deliver, and redeliver a
    /// failed message at once. For tests and one-shot drains.
    ///
    /// "Nothing to deliver" is the source's to say: a queue or table that is
    /// empty, a log read to its end. A Kafka consumer is idle once it has
    /// joined its group and read every partition assigned to it to its end.
    pub fn until_idle() -> Self {
        Self {
            until: RunUntil::Idle,
            backoff: RetryBackoff::immediate(),
            ..Self::default()
        }
    }

    /// Override the failure policy.
    pub fn with_failure_policy(mut self, policy: FailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    /// Override the redelivery spacing.
    pub fn with_backoff(mut self, backoff: RetryBackoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Dead-letter a message once it has been delivered `max_attempts` times
    /// and still fails. Values below 1 are raised to 1.
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = Some(max_attempts.max(1));
        self
    }

    /// Store dead letters in `sink`.
    pub fn with_dead_letters(mut self, sink: Arc<dyn DeadLetterSink>) -> Self {
        self.dead_letters = Some(sink);
        self
    }

    /// Store dead letters in `sink()` unless a sink is already set.
    pub fn or_dead_letters(mut self, sink: impl FnOnce() -> Arc<dyn DeadLetterSink>) -> Self {
        if self.dead_letters.is_none() {
            self.dead_letters = Some(sink());
        }
        self
    }

    /// Bound the received-but-unsettled deliveries of a multi-lane run.
    /// Values below 1 are raised to 1.
    pub fn with_lane_window(mut self, window: usize) -> Self {
        self.lane_window = window.max(1);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_from_base_and_stops_at_max() {
        let backoff = RetryBackoff::new(Duration::from_millis(50), Duration::from_millis(300));
        let delays = (1..=5).map(|attempt| backoff.delay(attempt).as_millis());
        assert_eq!(delays.collect::<Vec<_>>(), [50, 100, 200, 300, 300]);
        assert_eq!(backoff.delay(0), Duration::from_millis(50));
        assert_eq!(backoff.delay(u32::MAX), Duration::from_millis(300));
        assert_eq!(RetryBackoff::immediate().delay(9), Duration::ZERO);
        // A max below base is raised to base rather than inverting the range.
        let inverted = RetryBackoff::new(Duration::from_secs(2), Duration::from_secs(1));
        assert_eq!(inverted.delay(4), Duration::from_secs(2));
    }

    #[test]
    fn the_default_run_never_stops_on_its_own_and_never_gives_up_on_a_retry() {
        let options = RunOptions::default();
        assert!(matches!(&options.until, RunUntil::Shutdown(s) if !s.is_triggered()));
        assert_eq!(options.max_attempts, None);
        assert_eq!(options.failure_policy, FailurePolicy::DeadLetter);
        assert!(options.dead_letters.is_none());
    }

    #[test]
    fn builders_set_what_they_name() {
        let shutdown = Shutdown::new();
        let options = RunOptions::until_shutdown(shutdown.clone())
            .with_max_attempts(0)
            .with_lane_window(0)
            .with_failure_policy(FailurePolicy::Stop);
        shutdown.trigger();
        assert!(matches!(&options.until, RunUntil::Shutdown(s) if s.is_triggered()));
        assert_eq!(options.max_attempts, Some(1));
        assert_eq!(options.lane_window, 1);
        assert_eq!(options.failure_policy, FailurePolicy::Stop);

        let idle = RunOptions::until_idle();
        assert!(matches!(idle.until, RunUntil::Idle));
        assert_eq!(idle.backoff, RetryBackoff::immediate());

        let first = Arc::new(crate::bus::InMemoryDeadLetters::new());
        let kept = RunOptions::default()
            .with_dead_letters(first.clone())
            .or_dead_letters(|| unreachable!("a sink is already set"));
        assert!(kept.dead_letters.is_some());
        assert!(RunOptions::default()
            .or_dead_letters(|| first)
            .dead_letters
            .is_some());
    }
}
