//! Injectable monotonic clocks.
//!
//! The runtime never reads wall time directly. Every [`ZoneHost`](crate::ZoneHost)
//! is built with an `Arc<dyn Clock>`; production uses [`SystemClock`] and tests
//! use [`ManualClock`] to drive time explicitly, which (together with the seeded
//! [`ZoneRng`](crate::ZoneRng)) makes simulation runs reproducible.
//!
//! A clock reports elapsed time since its own origin as a [`Duration`]. Only
//! differences matter to the runtime; values are also sent to clients as
//! "server time" in milliseconds for clock synchronization.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Monotonic time source.
pub trait Clock: Send + Sync + 'static {
    /// Time elapsed since this clock's origin. Must never go backwards.
    fn now(&self) -> Duration;
}

/// Real monotonic clock backed by [`Instant`].
#[derive(Debug, Clone)]
pub struct SystemClock {
    origin: Instant,
}

impl SystemClock {
    /// Clock whose origin is "now".
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

/// Virtual clock advanced explicitly; clones share the same time.
#[derive(Debug, Clone, Default)]
pub struct ManualClock {
    nanos: Arc<AtomicU64>,
}

impl ManualClock {
    /// Clock starting at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Move time forward by `by`.
    pub fn advance(&self, by: Duration) {
        self.nanos.fetch_add(by.as_nanos() as u64, Ordering::SeqCst);
    }

    /// Jump to an absolute time. Ignored if it would move time backwards.
    pub fn set(&self, to: Duration) {
        self.nanos.fetch_max(to.as_nanos() as u64, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        Duration::from_nanos(self.nanos.load(Ordering::SeqCst))
    }
}
