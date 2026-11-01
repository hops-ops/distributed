//! Per-zone timers measured in simulation time.
//!
//! Timers are owned by the zone's host, fire only during ticks (in schedule
//! order, before `on_tick`), and are all cleared when the zone is disposed.
//! Because they run on simulation time they are deterministic and they pause
//! while a zone is Dormant (simulation time does not advance without ticks).

use std::time::Duration;

/// Handle for cancelling a timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimerId(pub u64);

#[derive(Debug)]
struct Entry<T> {
    id: TimerId,
    due: Duration,
    every: Option<Duration>,
    payload: T,
}

/// Timer set holding game-defined payloads `T`.
#[derive(Debug)]
pub struct Timers<T> {
    next_id: u64,
    entries: Vec<Entry<T>>,
}

impl<T> Default for Timers<T> {
    fn default() -> Self {
        Self {
            next_id: 1,
            entries: Vec::new(),
        }
    }
}

impl<T: Clone> Timers<T> {
    /// Fire once at `now + after`.
    pub fn once(&mut self, now: Duration, after: Duration, payload: T) -> TimerId {
        self.insert(now + after, None, payload)
    }

    /// Fire every `every` (minimum 1ns), first at `now + every`.
    pub fn interval(&mut self, now: Duration, every: Duration, payload: T) -> TimerId {
        let every = every.max(Duration::from_nanos(1));
        self.insert(now + every, Some(every), payload)
    }

    fn insert(&mut self, due: Duration, every: Option<Duration>, payload: T) -> TimerId {
        let id = TimerId(self.next_id);
        self.next_id += 1;
        self.entries.push(Entry {
            id,
            due,
            every,
            payload,
        });
        id
    }

    /// Cancel; returns whether the timer existed.
    pub fn cancel(&mut self, id: TimerId) -> bool {
        let before = self.entries.len();
        self.entries.retain(|entry| entry.id != id);
        before != self.entries.len()
    }

    /// Number of live timers.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when no timers are scheduled.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove every timer.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Collect timers due at `now`, ordered by (due, id). Interval timers are
    /// rescheduled; an interval fires at most once per call.
    pub fn due(&mut self, now: Duration) -> Vec<(TimerId, T)> {
        let mut fired: Vec<(Duration, TimerId, T)> = Vec::new();
        let mut index = 0;
        while index < self.entries.len() {
            if self.entries[index].due <= now {
                let entry = &mut self.entries[index];
                fired.push((entry.due, entry.id, entry.payload.clone()));
                if let Some(every) = entry.every {
                    while entry.due <= now {
                        entry.due += every;
                    }
                    index += 1;
                } else {
                    self.entries.swap_remove(index);
                }
            } else {
                index += 1;
            }
        }
        fired.sort_by_key(|(due, id, _)| (*due, *id));
        fired.into_iter().map(|(_, id, p)| (id, p)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn once_and_interval_fire_in_order() {
        let mut timers = Timers::default();
        let ms = Duration::from_millis;
        let a = timers.once(ms(0), ms(30), "a");
        let b = timers.interval(ms(0), ms(10), "b");
        assert!(timers.due(ms(5)).is_empty());
        assert_eq!(timers.due(ms(10)), vec![(b, "b")]);
        assert_eq!(timers.due(ms(30)), vec![(b, "b"), (a, "a")]);
        assert!(timers.cancel(b));
        assert!(timers.due(ms(100)).is_empty());
        assert!(timers.is_empty());
    }
}
