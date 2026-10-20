//! Injectable wall clock for coordination primitives.
//!
//! Sagas compute step deadlines and the in-memory [`lease`](crate::lease)
//! store evaluates TTLs against a [`Clock`] so tests can drive time
//! deterministically with a [`ManualClock`] instead of sleeping. Production
//! code uses [`SystemClock`]. (The SQL lease stores deliberately use the
//! *database* clock instead, so every process agrees on one authoritative
//! time source.)

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// A source of "now".
pub trait Clock: Send + Sync {
    /// The current wall-clock time.
    fn now(&self) -> SystemTime;
}

/// The process wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        crate::time::now()
    }
}

/// A clock that only moves when told to. Clones share the same time, so a
/// test can hand one clone to the code under test and advance another.
#[derive(Debug, Clone)]
pub struct ManualClock {
    now: Arc<Mutex<SystemTime>>,
}

impl ManualClock {
    /// A manual clock frozen at `start`.
    pub fn new(start: SystemTime) -> Self {
        Self {
            now: Arc::new(Mutex::new(start)),
        }
    }

    /// A manual clock frozen at the current wall-clock time.
    pub fn starting_now() -> Self {
        Self::new(crate::time::now())
    }

    /// Move the clock forward by `by`.
    pub fn advance(&self, by: Duration) {
        let mut now = self.now.lock().unwrap_or_else(|p| p.into_inner());
        *now = now.checked_add(by).unwrap_or(*now);
    }

    /// Jump the clock to `at` (may move backwards; leases and sagas must
    /// tolerate that, since real clocks step too).
    pub fn set(&self, at: SystemTime) {
        *self.now.lock().unwrap_or_else(|p| p.into_inner()) = at;
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::starting_now()
    }
}

impl Clock for ManualClock {
    fn now(&self) -> SystemTime {
        *self.now.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl<C: Clock + ?Sized> Clock for Arc<C> {
    fn now(&self) -> SystemTime {
        (**self).now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_clones_share_time() {
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let clock = ManualClock::new(start);
        let other = clock.clone();
        other.advance(Duration::from_secs(5));
        assert_eq!(clock.now(), start + Duration::from_secs(5));
    }
}
