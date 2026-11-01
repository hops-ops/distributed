//! Fixed timestep with an accumulator and a catch-up cap.
//!
//! Real elapsed time is added to an accumulator; whole steps are consumed
//! from it. When a host falls behind (GC pause, overloaded core, debugger) at
//! most `max_catch_up` steps run per advance and the remaining backlog is
//! **dropped** rather than carried forward, so a slow tick can never cause an
//! ever-growing backlog (the "spiral of death"). Arithmetic is in integer
//! nanoseconds so step counts are exact and reproducible.

use std::time::Duration;

/// Result of feeding elapsed time into a [`FixedTimestep`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Steps {
    /// Whole steps to simulate now.
    pub steps: u32,
    /// Backlog discarded because the catch-up cap was hit.
    pub dropped: Duration,
}

/// Accumulator for one fixed step length.
#[derive(Debug, Clone)]
pub struct FixedTimestep {
    step_nanos: u64,
    accumulator: u64,
    max_catch_up: u32,
}

impl FixedTimestep {
    /// Steps of `step` length, running at most `max_catch_up` (>= 1) per call.
    pub fn new(step: Duration, max_catch_up: u32) -> Self {
        Self {
            step_nanos: (step.as_nanos() as u64).max(1),
            accumulator: 0,
            max_catch_up: max_catch_up.max(1),
        }
    }

    /// Step for a rate in Hz.
    pub fn from_rate(hz: u32, max_catch_up: u32) -> Self {
        Self::new(rate_to_step(hz), max_catch_up)
    }

    /// Length of one step.
    pub fn step(&self) -> Duration {
        Duration::from_nanos(self.step_nanos)
    }

    /// Time accumulated toward the next step.
    pub fn pending(&self) -> Duration {
        Duration::from_nanos(self.accumulator)
    }

    /// Time until the next whole step is available.
    pub fn until_next(&self) -> Duration {
        Duration::from_nanos(self.step_nanos.saturating_sub(self.accumulator))
    }

    /// Fraction of a step accumulated (render interpolation alpha).
    pub fn alpha(&self) -> f32 {
        self.accumulator as f32 / self.step_nanos as f32
    }

    /// Discard accumulated time (used on activity-tier changes).
    pub fn reset(&mut self) {
        self.accumulator = 0;
    }

    /// Feed real elapsed time and take whole steps.
    pub fn advance(&mut self, elapsed: Duration) -> Steps {
        self.accumulator = self.accumulator.saturating_add(elapsed.as_nanos() as u64);
        let available = self.accumulator / self.step_nanos;
        let steps = available.min(u64::from(self.max_catch_up));
        self.accumulator -= steps * self.step_nanos;
        let mut dropped = 0;
        if available > steps {
            // Keep only the sub-step remainder; drop the rest of the backlog.
            dropped = self.accumulator - self.accumulator % self.step_nanos;
            self.accumulator %= self.step_nanos;
        }
        Steps {
            steps: steps as u32,
            dropped: Duration::from_nanos(dropped),
        }
    }
}

/// `1 / hz` seconds (zero Hz is treated as 1 Hz).
pub fn rate_to_step(hz: u32) -> Duration {
    Duration::from_nanos(1_000_000_000 / u64::from(hz.max(1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_partial_steps() {
        let mut ts = FixedTimestep::new(Duration::from_millis(10), 4);
        assert_eq!(ts.advance(Duration::from_millis(7)).steps, 0);
        assert_eq!(ts.advance(Duration::from_millis(7)).steps, 1);
        assert_eq!(ts.pending(), Duration::from_millis(4));
    }

    #[test]
    fn catch_up_is_capped_and_backlog_dropped() {
        let mut ts = FixedTimestep::new(Duration::from_millis(10), 3);
        let s = ts.advance(Duration::from_millis(1_005));
        assert_eq!(s.steps, 3);
        assert_eq!(s.dropped, Duration::from_millis(970));
        assert_eq!(ts.pending(), Duration::from_millis(5));
        // Next normal frame is unaffected by the old backlog.
        assert_eq!(ts.advance(Duration::from_millis(10)).steps, 1);
    }
}
