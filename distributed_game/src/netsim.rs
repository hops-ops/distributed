//! Development network simulation (latency, jitter, loss), portable: the
//! native WebSocket server ([`crate::net`]) and the celld zone host
//! (`celld_zone`, wasm32) both apply it per connection and direction.
//!
//! [`NetSim`] is the setting; [`DelayLine`] is the order-preserving queue a
//! host puts one direction of one connection through, over plain
//! millisecond timestamps (so it runs on Tokio's clock and on `Date.now()`).

use std::collections::VecDeque;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::rng::ZoneRng;

/// Cell kind of a celld worker's dev settings object (binding `ZONE_DEV`,
/// `celld_zone::ZoneDevSettings`), where celld zone objects read the
/// network simulation.
pub const ZONE_DEV_KIND: &str = "zone-dev";
/// Shard of the dev settings holding the network simulation ([`NetSimJson`]).
pub const NETSIM_SHARD: &str = "netsim";

/// Development network simulation, applied independently to each direction
/// of every connection.
///
/// Semantics over WebSocket (a reliable, ordered stream): a message is never
/// dropped or reordered, because Deltas are encoded against the previous
/// one. `loss` models what TCP does with a lost segment instead: with
/// probability `loss` a message is delayed by an extra `retransmit` (repeated
/// with the same probability, at most 3 times), and since order is preserved
/// every later message queues behind it (head-of-line blocking). Total
/// one-way delay = `latency + U[0, jitter] + k·retransmit`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct NetSim {
    /// Fixed one-way delay.
    pub latency: Duration,
    /// Extra uniform random delay in `[0, jitter]` (order is preserved).
    pub jitter: Duration,
    /// Probability in `[0, 1]` that a message needs a retransmission.
    pub loss: f32,
    /// Delay added per simulated retransmission (TCP minimum RTO ≈ 200 ms).
    pub retransmit: Duration,
}

impl NetSim {
    /// Latency + jitter, no loss.
    pub fn new(latency: Duration, jitter: Duration) -> Self {
        Self {
            latency,
            jitter,
            loss: 0.0,
            retransmit: Duration::from_millis(200),
        }
    }

    /// Add simulated loss (see the type docs for WebSocket semantics).
    pub fn with_loss(mut self, loss: f32, retransmit: Duration) -> Self {
        self.loss = loss.clamp(0.0, 1.0);
        self.retransmit = retransmit;
        self
    }

    /// Read `DG_NET_LATENCY_MS`, `DG_NET_JITTER_MS`, `DG_NET_LOSS` (fraction,
    /// e.g. `0.02`) and `DG_NET_RETRANSMIT_MS` (default 200). `None` when
    /// none of the first three is set or all are zero.
    pub fn from_env() -> Option<Self> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// [`from_env`](Self::from_env) over any key lookup.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Option<Self> {
        let ms = |key: &str| {
            lookup(key)
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map(Duration::from_millis)
        };
        let latency = ms("DG_NET_LATENCY_MS").unwrap_or_default();
        let jitter = ms("DG_NET_JITTER_MS").unwrap_or_default();
        let loss = lookup("DG_NET_LOSS")
            .and_then(|v| v.trim().parse::<f32>().ok())
            .unwrap_or(0.0);
        let retransmit = ms("DG_NET_RETRANSMIT_MS").unwrap_or(Duration::from_millis(200));
        (!latency.is_zero() || !jitter.is_zero() || loss > 0.0)
            .then(|| Self::new(latency, jitter).with_loss(loss, retransmit))
    }

    /// One-way delay for one message.
    pub fn delay(&self, rng: &mut ZoneRng) -> Duration {
        let mut delay = self.latency + self.jitter.mul_f32(rng.next_f32());
        for _ in 0..3 {
            if self.loss <= 0.0 || !rng.chance(self.loss) {
                break;
            }
            delay += self.retransmit;
        }
        delay
    }

    /// The JSON shape of dev controls (`{latencyMs, jitterMs, loss}`); `None`
    /// reads as all zero.
    pub fn to_json(sim: Option<NetSim>) -> NetSimJson {
        let sim = sim.unwrap_or_default();
        NetSimJson {
            latency_ms: sim.latency.as_millis() as u64,
            jitter_ms: sim.jitter.as_millis() as u64,
            loss: sim.loss,
        }
    }
}

/// A [`NetSim`] as dev routes exchange it: milliseconds, loss as a
/// fraction (retransmit is the default 200 ms).
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetSimJson {
    /// One-way latency (ms).
    #[serde(default)]
    pub latency_ms: u64,
    /// Jitter (ms).
    #[serde(default)]
    pub jitter_ms: u64,
    /// Loss (fraction, `0..=0.5`).
    #[serde(default)]
    pub loss: f32,
}

impl NetSimJson {
    /// The simulation (bounded for dev use: ≤ 2 s latency and jitter, ≤ 50 %
    /// loss); `None` when everything is zero.
    pub fn to_sim(self) -> Option<NetSim> {
        let latency = Duration::from_millis(self.latency_ms.min(2_000));
        let jitter = Duration::from_millis(self.jitter_ms.min(2_000));
        let loss = if self.loss.is_finite() {
            self.loss.clamp(0.0, 0.5)
        } else {
            0.0
        };
        (!latency.is_zero() || !jitter.is_zero() || loss > 0.0)
            .then(|| NetSim::new(latency, jitter).with_loss(loss, Duration::from_millis(200)))
    }
}

/// Order-preserving delay line for one direction of one connection, over
/// millisecond timestamps.
#[derive(Debug)]
pub struct DelayLine<T> {
    rng: ZoneRng,
    queue: VecDeque<(f64, T)>,
    last: f64,
}

impl<T> DelayLine<T> {
    /// An empty line (`seed` drives jitter and loss).
    pub fn new(seed: u64) -> Self {
        Self {
            rng: ZoneRng::from_seed(seed),
            queue: VecDeque::new(),
            last: f64::MIN,
        }
    }

    /// Queue `item` sent at `now_ms` under `sim`. Returns it back when it is
    /// due immediately (no simulation and nothing still queued: order is
    /// preserved when the simulation is switched off mid-flight).
    pub fn push(&mut self, sim: Option<NetSim>, now_ms: f64, item: T) -> Option<T> {
        let delay = match sim {
            Some(sim) => sim.delay(&mut self.rng).as_secs_f64() * 1000.0,
            None if self.queue.is_empty() => return Some(item),
            None => 0.0,
        };
        let at = (now_ms + delay).max(self.last);
        self.last = at;
        self.queue.push_back((at, item));
        None
    }

    /// When the next item is due (ms), if any is queued.
    pub fn next_due(&self) -> Option<f64> {
        self.queue.front().map(|(at, _)| *at)
    }

    /// The next item if it is due at `now_ms`.
    pub fn pop_due(&mut self, now_ms: f64) -> Option<T> {
        if self.queue.front()?.0 <= now_ms {
            self.queue.pop_front().map(|(_, item)| item)
        } else {
            None
        }
    }

    /// Items queued.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Nothing queued.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Drop everything queued.
    pub fn clear(&mut self) {
        self.queue.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_env_and_loss_semantics() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(key, _)| *key == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        assert_eq!(NetSim::from_lookup(env(&[])), None);
        let sim = NetSim::from_lookup(env(&[
            ("DG_NET_LATENCY_MS", "40"),
            ("DG_NET_JITTER_MS", "10"),
            ("DG_NET_LOSS", "0.5"),
        ]))
        .unwrap();
        assert_eq!(sim.latency, Duration::from_millis(40));
        assert_eq!(sim.jitter, Duration::from_millis(10));
        assert_eq!(sim.retransmit, Duration::from_millis(200));
        let mut rng = ZoneRng::from_seed(7);
        let delays: Vec<Duration> = (0..2000).map(|_| sim.delay(&mut rng)).collect();
        let base_max = Duration::from_millis(50);
        let retransmitted = delays.iter().filter(|d| **d > base_max).count();
        // P(at least one retransmit) = 0.5.
        assert!((850..1150).contains(&retransmitted), "{retransmitted}");
        assert!(delays.iter().all(|d| *d >= sim.latency));
        assert!(delays.iter().all(|d| *d <= base_max + sim.retransmit * 3));
        // Without loss the delay is bounded by latency + jitter.
        let clean = NetSim::new(Duration::from_millis(40), Duration::from_millis(10));
        assert!((0..500).all(|_| clean.delay(&mut rng) <= base_max));
    }

    #[test]
    fn switching_the_simulation_preserves_order() {
        let mut line = DelayLine::new(1);
        let on = Some(NetSim::new(Duration::from_millis(200), Duration::ZERO));
        assert_eq!(line.push(None, 0.0, 1), Some(1), "no simulation: immediate");
        assert_eq!(
            line.push(on, 0.0, 2),
            None,
            "simulation switched on: delayed"
        );
        assert_eq!(line.next_due(), Some(200.0));
        assert_eq!(
            line.push(None, 10.0, 3),
            None,
            "switched off: queues behind 2"
        );
        assert_eq!(line.pop_due(150.0), None);
        assert_eq!(line.pop_due(200.0), Some(2));
        assert_eq!(line.pop_due(200.0), Some(3));
        assert_eq!(
            line.push(None, 300.0, 4),
            Some(4),
            "drained: immediate again"
        );
    }

    #[test]
    fn json_round_trip_is_bounded() {
        let json = NetSimJson {
            latency_ms: 80,
            jitter_ms: 20,
            loss: 0.05,
        };
        let sim = json.to_sim().expect("on");
        assert_eq!(NetSim::to_json(Some(sim)), json);
        assert_eq!(NetSimJson::default().to_sim(), None);
        let wild = NetSimJson {
            latency_ms: 99_999,
            jitter_ms: 0,
            loss: f32::NAN,
        }
        .to_sim()
        .expect("on");
        assert_eq!(wild.latency, Duration::from_millis(2_000));
        assert_eq!(wild.loss, 0.0);
    }
}
