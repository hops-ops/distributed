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
//!
//! [`Clock::unix_ms`] is the clock's wall time (Unix milliseconds), used to
//! stamp facts ([`FactStamp::emitted_at_ms`](crate::FactStamp::emitted_at_ms))
//! so facts from different zones and hosts can be ordered. It advances with
//! [`now`](Clock::now) and never goes backwards either.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Monotonic time source.
pub trait Clock: Send + Sync + 'static {
    /// Time elapsed since this clock's origin. Must never go backwards.
    fn now(&self) -> Duration;

    /// Wall time in Unix milliseconds. Must never go backwards.
    fn unix_ms(&self) -> u64;
}

/// Real monotonic clock backed by [`Instant`]. Its wall time is the system
/// time read once at creation plus the monotonic elapsed time, so a system
/// clock step (NTP) never moves fact stamps backwards.
#[derive(Debug, Clone)]
pub struct SystemClock {
    origin: Instant,
    origin_unix: Duration,
}

impl SystemClock {
    /// Clock whose origin is "now".
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
            origin_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default(),
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

    fn unix_ms(&self) -> u64 {
        (self.origin_unix + self.origin.elapsed()).as_millis() as u64
    }
}

/// Virtual clock advanced explicitly; clones share the same time. Its wall
/// time is a fixed Unix origin (0 unless set with
/// [`with_unix_origin_ms`](Self::with_unix_origin_ms)) plus the virtual time.
#[derive(Debug, Clone, Default)]
pub struct ManualClock {
    nanos: Arc<AtomicU64>,
    unix_origin_ms: u64,
}

impl ManualClock {
    /// Clock starting at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// The same clock reporting wall time `origin_ms + now()` (e.g. two
    /// hosts whose wall clocks disagree). Clones made before this call keep
    /// their own origin but share the virtual time.
    pub fn with_unix_origin_ms(mut self, origin_ms: u64) -> Self {
        self.unix_origin_ms = origin_ms;
        self
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

    fn unix_ms(&self) -> u64 {
        self.unix_origin_ms + self.now().as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_wall_time_follows_virtual_time() {
        let clock = ManualClock::new().with_unix_origin_ms(1_000);
        let shared = clock.clone();
        assert_eq!(clock.unix_ms(), 1_000);
        shared.advance(Duration::from_millis(250));
        assert_eq!(clock.unix_ms(), 1_250);
        let skewed = shared.clone().with_unix_origin_ms(500);
        assert_eq!(skewed.unix_ms(), 750);
    }

    #[test]
    fn system_wall_time_is_close_to_system_time() {
        let clock = SystemClock::new();
        let system = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!(clock.unix_ms().abs_diff(system) < 1_000);
        assert!(clock.unix_ms() <= clock.unix_ms());
    }
}
