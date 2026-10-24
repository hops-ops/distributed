//! Per-zone Prometheus metrics (`metrics` feature, no dependencies).
//!
//! Why not the core `distributed::metrics` registry: that registry is a
//! deliberately *fixed* set of framework families with typed keys behind a
//! process-global `OnceLock`, and `distributed_game` does not depend on
//! `distributed` at all outside the `cell` feature. Game metrics need an
//! instance-scoped registry (one per [`GameServer`](crate::net::GameServer),
//! parallel test servers) and a zone label, so this module renders its own
//! Prometheus text (exposition format 0.0.4). Both expositions can be served
//! side by side; family names are prefixed `distributed_game_`.
//!
//! **Cardinality guard.** Zone ids are unbounded, so only the first
//! [`GameMetrics::max_zone_labels`] live zones get their own `zone` label;
//! every other zone is folded into `zone="_other"` (counters and histograms
//! accumulate there, gauges are summed at render time). When a zone stops
//! ([`GameMetrics::forget_zone`]) its series are dropped and its label slot
//! is freed. Series count is therefore bounded by
//! `(max_zone_labels + 1) × families × fixed label values`. Other labels
//! (`tier`, `outcome`) have fixed value sets. Principals and player ids are
//! never labels.
//!
//! Families:
//!
//! | Name | Type | Labels |
//! |---|---|---|
//! | `distributed_game_zones` | gauge | – |
//! | `distributed_game_zone_tick_duration_seconds` | histogram | zone |
//! | `distributed_game_zone_ticks_total` | counter | zone |
//! | `distributed_game_zone_ticks_dropped_total` | counter | zone |
//! | `distributed_game_zone_tier` | gauge (one-hot) | zone, tier |
//! | `distributed_game_zone_players` / `_observers` / `_entities` | gauge | zone |
//! | `distributed_game_zone_inputs_total` | counter | zone, outcome |
//! | `distributed_game_zone_patches_total` | counter | zone |
//! | `distributed_game_zone_delta_bytes` | histogram (per Delta frame) | zone |
//! | `distributed_game_zone_delta_bytes_total` | counter | zone |
//! | `distributed_game_zone_delta_deferred_total` | counter | zone |
//! | `distributed_game_zone_facts_pending` | gauge | zone |
//! | `distributed_game_zone_facts_settled_total` | counter | zone, outcome |
//! | `distributed_game_hibernation_saves_total` | counter | outcome (`saved`, `stale`, `error`) |
//! | `distributed_game_activations_refused_total` | counter | reason (`not_found`, `unavailable`) |

use crate::facts::FactOutcome;
use crate::host::{AdvanceReport, ZoneHost};
use crate::input::PushOutcome;
use crate::zone::{Tier, Zone};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;

/// Label for zones beyond the cardinality budget.
pub const OTHER_ZONE: &str = "_other";

const TICK_BUCKETS: [f64; 12] = [
    0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.016, 0.033, 0.05, 0.1,
];
const BYTE_BUCKETS: [f64; 10] = [
    16.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 16384.0, 65536.0,
];

/// Input outcome label values (fixed set).
pub const INPUT_OUTCOMES: [&str; 8] = [
    "accepted",
    "dropped_oldest",
    "duplicate",
    "stale",
    "rejected",
    "unknown_client",
    "rate_limited",
    "frozen",
];

fn input_label(outcome: PushOutcome) -> &'static str {
    match outcome {
        PushOutcome::Accepted => "accepted",
        PushOutcome::AcceptedDroppedOldest => "dropped_oldest",
        PushOutcome::Duplicate => "duplicate",
        PushOutcome::Stale => "stale",
        PushOutcome::Rejected => "rejected",
        PushOutcome::UnknownClient => "unknown_client",
    }
}

#[derive(Debug, Clone)]
struct Histogram {
    bounds: &'static [f64],
    counts: Vec<u64>,
    sum: f64,
    count: u64,
}

impl Histogram {
    fn new(bounds: &'static [f64]) -> Self {
        Self {
            bounds,
            counts: vec![0; bounds.len()],
            sum: 0.0,
            count: 0,
        }
    }

    fn observe(&mut self, v: f64) {
        for (bound, count) in self.bounds.iter().zip(self.counts.iter_mut()) {
            if v <= *bound {
                *count += 1;
            }
        }
        self.sum += v;
        self.count += 1;
    }
}

/// Monotonic per-zone series.
#[derive(Debug, Clone)]
struct Counters {
    tick_duration: Histogram,
    ticks: u64,
    ticks_dropped: u64,
    inputs: BTreeMap<&'static str, u64>,
    patches: u64,
    delta_bytes: Histogram,
    delta_bytes_total: u64,
    delta_deferred: u64,
    facts_committed: u64,
    facts_failed: u64,
}

impl Default for Counters {
    fn default() -> Self {
        Self {
            tick_duration: Histogram::new(&TICK_BUCKETS),
            ticks: 0,
            ticks_dropped: 0,
            inputs: BTreeMap::new(),
            patches: 0,
            delta_bytes: Histogram::new(&BYTE_BUCKETS),
            delta_bytes_total: 0,
            delta_deferred: 0,
            facts_committed: 0,
            facts_failed: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Gauges {
    tier: Option<Tier>,
    players: usize,
    observers: usize,
    entities: usize,
    facts_pending: usize,
}

#[derive(Debug, Default)]
struct ZoneSeries {
    labeled: bool,
    counters: Counters,
    gauges: Gauges,
}

#[derive(Debug, Default)]
struct State {
    zones: BTreeMap<String, ZoneSeries>,
    labeled: usize,
    other: Counters,
    /// Server-wide (zones are forgotten when they hibernate).
    hibernation_saves: BTreeMap<&'static str, u64>,
    /// Server-wide activations refused before the zone ran.
    activations_refused: BTreeMap<&'static str, u64>,
}

/// Hibernation snapshot save outcome label values (fixed set).
pub const HIBERNATION_OUTCOMES: [&str; 3] = ["saved", "stale", "error"];

/// Refused activation reason label values (fixed set).
pub const ACTIVATION_REFUSALS: [&str; 2] = ["not_found", "unavailable"];

impl State {
    fn zone(&mut self, zone: &str, max: usize) -> &mut ZoneSeries {
        if !self.zones.contains_key(zone) {
            let labeled = self.labeled < max;
            if labeled {
                self.labeled += 1;
            }
            self.zones.insert(
                zone.to_string(),
                ZoneSeries {
                    labeled,
                    ..ZoneSeries::default()
                },
            );
        }
        self.zones.get_mut(zone).expect("inserted")
    }

    fn counters(&mut self, zone: &str, max: usize) -> &mut Counters {
        if self.zone(zone, max).labeled {
            &mut self.zones.get_mut(zone).expect("present").counters
        } else {
            &mut self.other
        }
    }
}

/// Instance-scoped game metrics registry.
#[derive(Debug)]
pub struct GameMetrics {
    max_zone_labels: usize,
    state: Mutex<State>,
}

impl Default for GameMetrics {
    fn default() -> Self {
        Self::new(64)
    }
}

impl GameMetrics {
    /// Registry giving at most `max_zone_labels` live zones their own label.
    pub fn new(max_zone_labels: usize) -> Self {
        Self {
            max_zone_labels,
            state: Mutex::new(State::default()),
        }
    }

    /// Cardinality budget for the `zone` label.
    pub fn max_zone_labels(&self) -> usize {
        self.max_zone_labels
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record one [`ZoneHost::advance`] and the host's current gauges.
    pub fn record_advance<Z: Zone>(&self, host: &ZoneHost<Z>, report: &AdvanceReport) {
        let max = self.max_zone_labels;
        let mut state = self.lock();
        let series = state.zone(host.zone_id(), max);
        series.gauges = Gauges {
            tier: Some(host.tier()),
            players: host.player_count(),
            observers: host.observer_count(),
            entities: report.entities.unwrap_or(series.gauges.entities),
            facts_pending: host.facts_in_flight(),
        };
        let c = state.counters(host.zone_id(), max);
        for d in &report.tick_durations {
            c.tick_duration.observe(d.as_secs_f64());
        }
        c.ticks += u64::from(report.ticks);
        c.ticks_dropped += u64::from(report.dropped_ticks);
        if report.patched {
            c.patches += 1;
        }
        for frame in &report.frames {
            c.delta_bytes.observe(frame.bytes as f64);
            c.delta_bytes_total += frame.bytes as u64;
            c.delta_deferred += frame.stats.deferred as u64;
        }
        for (_, s) in &report.settled {
            match s.outcome {
                FactOutcome::Committed(_) => c.facts_committed += 1,
                FactOutcome::Failed { .. } => c.facts_failed += 1,
            }
        }
    }

    /// Record an input offer outcome.
    pub fn record_input(&self, zone: &str, outcome: PushOutcome) {
        self.record_input_label(zone, input_label(outcome));
    }

    /// Record an input dropped by the transport's rate limiter.
    pub fn record_rate_limited(&self, zone: &str) {
        self.record_input_label(zone, "rate_limited");
    }

    /// Record an input dropped because the zone was frozen (world barrier).
    pub fn record_frozen_input(&self, zone: &str) {
        self.record_input_label(zone, "frozen");
    }

    fn record_input_label(&self, zone: &str, label: &'static str) {
        let max = self.max_zone_labels;
        *self
            .lock()
            .counters(zone, max)
            .inputs
            .entry(label)
            .or_insert(0) += 1;
    }

    /// Record a hibernation snapshot save: `"saved"`, `"stale"` (rejected by
    /// the fenced store: a newer owner saved, or tokens went backwards) or
    /// `"error"` (backend failure). Other labels are ignored.
    pub fn record_hibernation_save(&self, outcome: &str) {
        if let Some(label) = HIBERNATION_OUTCOMES.iter().find(|l| **l == outcome) {
            *self.lock().hibernation_saves.entry(label).or_insert(0) += 1;
        }
    }

    /// A zone activation refused before the zone ran, by protocol error
    /// code (`NOT_FOUND` → `not_found`, anything else → `unavailable`).
    pub fn record_activation_refused(&self, code: u32) {
        let label = if code == crate::protocol::error::NOT_FOUND {
            "not_found"
        } else {
            "unavailable"
        };
        *self.lock().activations_refused.entry(label).or_insert(0) += 1;
    }

    /// Refused activations recorded with `reason`.
    pub fn activations_refused(&self, reason: &str) -> u64 {
        self.lock()
            .activations_refused
            .get(reason)
            .copied()
            .unwrap_or(0)
    }

    /// Hibernation saves recorded with `outcome`.
    pub fn hibernation_saves(&self, outcome: &str) -> u64 {
        self.lock()
            .hibernation_saves
            .get(outcome)
            .copied()
            .unwrap_or(0)
    }

    /// A zone stopped: drop its series and free its label slot.
    pub fn forget_zone(&self, zone: &str) {
        let mut state = self.lock();
        if let Some(series) = state.zones.remove(zone) {
            if series.labeled {
                state.labeled -= 1;
            }
        }
    }

    /// Zones currently tracked.
    pub fn zone_count(&self) -> usize {
        self.lock().zones.len()
    }

    /// Prometheus text exposition (format 0.0.4).
    pub fn render(&self) -> String {
        let state = self.lock();
        let mut out = String::new();
        let labeled: Vec<(&str, &ZoneSeries)> = state
            .zones
            .iter()
            .filter(|(_, s)| s.labeled)
            .map(|(z, s)| (z.as_str(), s))
            .collect();
        let overflow: Vec<&ZoneSeries> = state.zones.values().filter(|s| !s.labeled).collect();
        let mut counter_rows: Vec<(&str, &Counters)> =
            labeled.iter().map(|(z, s)| (*z, &s.counters)).collect();
        if !overflow.is_empty()
            || state.other.ticks > 0
            || !state.other.inputs.is_empty()
            || state.other.patches > 0
        {
            counter_rows.push((OTHER_ZONE, &state.other));
        }
        let mut gauge_rows: Vec<(&str, Gauges, [u64; 3])> = labeled
            .iter()
            .map(|(z, s)| (*z, s.gauges, tier_hot(s.gauges.tier)))
            .collect();
        if !overflow.is_empty() {
            let mut sum = Gauges::default();
            let mut tiers = [0u64; 3];
            for s in &overflow {
                sum.players += s.gauges.players;
                sum.observers += s.gauges.observers;
                sum.entities += s.gauges.entities;
                sum.facts_pending += s.gauges.facts_pending;
                for (t, v) in tiers.iter_mut().zip(tier_hot(s.gauges.tier)) {
                    *t += v;
                }
            }
            gauge_rows.push((OTHER_ZONE, sum, tiers));
        }

        header(&mut out, "zones", "gauge", "Zones currently hosted.");
        let _ = writeln!(out, "distributed_game_zones {}", state.zones.len());

        header(
            &mut out,
            "zone_tick_duration_seconds",
            "histogram",
            "Wall time of one fixed simulation step.",
        );
        for (zone, c) in &counter_rows {
            histogram(
                &mut out,
                "zone_tick_duration_seconds",
                zone,
                &c.tick_duration,
            );
        }
        counter(
            &mut out,
            "zone_ticks_total",
            "Fixed steps simulated.",
            &counter_rows,
            |c| c.ticks,
        );
        counter(
            &mut out,
            "zone_ticks_dropped_total",
            "Fixed steps dropped by the catch-up cap.",
            &counter_rows,
            |c| c.ticks_dropped,
        );

        header(
            &mut out,
            "zone_tier",
            "gauge",
            "Activity tier (one-hot; summed for _other).",
        );
        for (zone, _, tiers) in &gauge_rows {
            for (name, v) in ["hot", "idle", "dormant"].iter().zip(tiers) {
                let _ = writeln!(
                    out,
                    "distributed_game_zone_tier{{zone=\"{}\",tier=\"{name}\"}} {v}",
                    escape(zone)
                );
            }
        }
        gauge(
            &mut out,
            "zone_players",
            "Players (incl. reconnect grace).",
            &gauge_rows,
            |g| g.players,
        );
        gauge(&mut out, "zone_observers", "Observers.", &gauge_rows, |g| {
            g.observers
        });
        gauge(
            &mut out,
            "zone_entities",
            "Replicated entities at the last patch.",
            &gauge_rows,
            |g| g.entities,
        );
        gauge(
            &mut out,
            "zone_facts_pending",
            "Unsettled facts.",
            &gauge_rows,
            |g| g.facts_pending,
        );

        header(
            &mut out,
            "zone_inputs_total",
            "counter",
            "Input offers by outcome.",
        );
        for (zone, c) in &counter_rows {
            for (label, v) in &c.inputs {
                let _ = writeln!(
                    out,
                    "distributed_game_zone_inputs_total{{zone=\"{}\",outcome=\"{label}\"}} {v}",
                    escape(zone)
                );
            }
        }
        counter(
            &mut out,
            "zone_patches_total",
            "Patches (Delta rounds).",
            &counter_rows,
            |c| c.patches,
        );
        header(
            &mut out,
            "zone_delta_bytes",
            "histogram",
            "Encoded size of one Delta frame.",
        );
        for (zone, c) in &counter_rows {
            histogram(&mut out, "zone_delta_bytes", zone, &c.delta_bytes);
        }
        counter(
            &mut out,
            "zone_delta_bytes_total",
            "Delta bytes produced.",
            &counter_rows,
            |c| c.delta_bytes_total,
        );
        counter(
            &mut out,
            "zone_delta_deferred_total",
            "Entity changes deferred by the byte budget.",
            &counter_rows,
            |c| c.delta_deferred,
        );
        header(
            &mut out,
            "zone_facts_settled_total",
            "counter",
            "Fact settlements delivered to zones by outcome.",
        );
        for (zone, c) in &counter_rows {
            for (label, v) in [("committed", c.facts_committed), ("failed", c.facts_failed)] {
                let _ = writeln!(
                    out,
                    "distributed_game_zone_facts_settled_total{{zone=\"{}\",outcome=\"{label}\"}} {v}",
                    escape(zone)
                );
            }
        }
        header(
            &mut out,
            "hibernation_saves_total",
            "counter",
            "Hibernation snapshot saves by outcome (stale = rejected by the fenced store).",
        );
        for label in HIBERNATION_OUTCOMES {
            let v = state.hibernation_saves.get(label).copied().unwrap_or(0);
            let _ = writeln!(
                out,
                "distributed_game_hibernation_saves_total{{outcome=\"{label}\"}} {v}"
            );
        }
        header(
            &mut out,
            "activations_refused_total",
            "counter",
            "Zone activations refused before the zone ran (unknown zone, factory or allocation failure).",
        );
        for label in ACTIVATION_REFUSALS {
            let v = state.activations_refused.get(label).copied().unwrap_or(0);
            let _ = writeln!(
                out,
                "distributed_game_activations_refused_total{{reason=\"{label}\"}} {v}"
            );
        }
        out
    }
}

fn tier_hot(tier: Option<Tier>) -> [u64; 3] {
    match tier {
        Some(Tier::Hot) => [1, 0, 0],
        Some(Tier::Idle) => [0, 1, 0],
        Some(Tier::Dormant) => [0, 0, 1],
        None => [0, 0, 0],
    }
}

fn escape(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP distributed_game_{name} {help}");
    let _ = writeln!(out, "# TYPE distributed_game_{name} {kind}");
}

fn counter(
    out: &mut String,
    name: &str,
    help: &str,
    rows: &[(&str, &Counters)],
    value: impl Fn(&Counters) -> u64,
) {
    header(out, name, "counter", help);
    for (zone, c) in rows {
        let _ = writeln!(
            out,
            "distributed_game_{name}{{zone=\"{}\"}} {}",
            escape(zone),
            value(c)
        );
    }
}

fn gauge(
    out: &mut String,
    name: &str,
    help: &str,
    rows: &[(&str, Gauges, [u64; 3])],
    value: impl Fn(&Gauges) -> usize,
) {
    header(out, name, "gauge", help);
    for (zone, g, _) in rows {
        let _ = writeln!(
            out,
            "distributed_game_{name}{{zone=\"{}\"}} {}",
            escape(zone),
            value(g)
        );
    }
}

fn histogram(out: &mut String, name: &str, zone: &str, h: &Histogram) {
    let zone = escape(zone);
    for (bound, count) in h.bounds.iter().zip(&h.counts) {
        let _ = writeln!(
            out,
            "distributed_game_{name}_bucket{{zone=\"{zone}\",le=\"{bound}\"}} {count}"
        );
    }
    let _ = writeln!(
        out,
        "distributed_game_{name}_bucket{{zone=\"{zone}\",le=\"+Inf\"}} {}",
        h.count
    );
    let _ = writeln!(
        out,
        "distributed_game_{name}_sum{{zone=\"{zone}\"}} {}",
        h.sum
    );
    let _ = writeln!(
        out,
        "distributed_game_{name}_count{{zone=\"{zone}\"}} {}",
        h.count
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClientMirror, EntitySchema, EntityState, FieldType, FieldValue, KindDef, ManualClock, Role,
        TicketClaims, WorldView, Zone, ZoneHost,
    };
    use std::sync::Arc;
    use std::time::Duration;

    struct Dots;

    impl Zone for Dots {
        type Config = ();
        type Input = u8;
        type Timer = ();
        type Message = distributed_game::NoMessages;

        fn schema() -> EntitySchema {
            EntitySchema::new().kind(KindDef::new(1, "dot").field("v", FieldType::U8))
        }

        fn on_create(_ctx: &mut crate::Ctx<'_, Self>, _: ()) -> Self {
            Dots
        }

        fn decode_input(&self, _p: crate::PlayerId, b: &[u8]) -> Option<u8> {
            b.first().copied()
        }

        fn on_tick(&mut self, _ctx: &mut crate::Ctx<'_, Self>) {}

        fn replicate(&self, out: &mut WorldView) {
            for i in 0..3 {
                out.push(EntityState::new(i, 1, 0.0, 0.0, vec![FieldValue::U(i)]));
            }
        }
    }

    fn host(zone: &str, clock: &ManualClock) -> ZoneHost<Dots> {
        ZoneHost::new(zone, Default::default(), Arc::new(clock.clone()), ())
    }

    #[test]
    fn records_zone_metrics_and_renders_prometheus() {
        let metrics = GameMetrics::new(8);
        let clock = ManualClock::new();
        let mut h = host("z1", &clock);
        let claims = TicketClaims {
            principal: "a".into(),
            zone: "z1".into(),
            expires_at: u64::MAX,
            nonce: "n".into(),
            role: Role::Player,
            name: None,
            extensions: Default::default(),
            wake: false,
        };
        let joined = h.join(&claims, 0, "").unwrap();
        metrics.record_input("z1", h.push_input(joined.client, 1, 0, &[1]));
        metrics.record_input("z1", h.push_input(joined.client, 1, 0, &[1]));
        metrics.record_rate_limited("z1");
        clock.advance(Duration::from_millis(100));
        let report = h.advance();
        assert_eq!(report.frames.len(), 1);
        let mut mirror = ClientMirror::new();
        for o in h.drain_outbound() {
            if let crate::Outbound::Frame { frame, .. } = o {
                mirror.apply(&frame).unwrap();
            }
        }
        assert_eq!(mirror.entities.len(), 3);
        metrics.record_advance(&h, &report);
        let text = metrics.render();
        assert!(text.contains("distributed_game_zones 1"), "{text}");
        assert!(
            text.contains("distributed_game_zone_ticks_total{zone=\"z1\"} 3"),
            "{text}"
        );
        assert!(text.contains("distributed_game_zone_tier{zone=\"z1\",tier=\"hot\"} 1"));
        assert!(text.contains("distributed_game_zone_players{zone=\"z1\"} 1"));
        assert!(text.contains("distributed_game_zone_entities{zone=\"z1\"} 3"));
        assert!(
            text.contains("distributed_game_zone_inputs_total{zone=\"z1\",outcome=\"accepted\"} 1")
        );
        assert!(text
            .contains("distributed_game_zone_inputs_total{zone=\"z1\",outcome=\"duplicate\"} 1"));
        assert!(text.contains(
            "distributed_game_zone_inputs_total{zone=\"z1\",outcome=\"rate_limited\"} 1"
        ));
        assert!(text.contains("distributed_game_zone_delta_bytes_count{zone=\"z1\"} 1"));
        assert!(text.contains("distributed_game_zone_tick_duration_seconds_count{zone=\"z1\"} 3"));
        assert!(text.contains("# TYPE distributed_game_zone_delta_bytes histogram"));
    }

    #[test]
    fn zone_label_cardinality_is_bounded() {
        let metrics = GameMetrics::new(2);
        let clock = ManualClock::new();
        let mut hosts: Vec<_> = (0..5).map(|i| host(&format!("zone-{i}"), &clock)).collect();
        clock.advance(Duration::from_millis(100));
        for h in &mut hosts {
            let r = h.advance();
            metrics.record_advance(h, &r);
        }
        let text = metrics.render();
        let zones: std::collections::BTreeSet<&str> = text
            .lines()
            .filter(|l| l.starts_with("distributed_game_zone_ticks_total{"))
            .map(|l| l.split('"').nth(1).unwrap())
            .collect();
        assert_eq!(
            zones.into_iter().collect::<Vec<_>>(),
            vec!["_other", "zone-0", "zone-1"]
        );
        assert!(
            text.contains("distributed_game_zone_ticks_total{zone=\"_other\"} 0"),
            "{text}"
        );
        assert!(text.contains("distributed_game_zone_tier{zone=\"_other\",tier=\"hot\"} 0"));
        assert!(text.contains("distributed_game_zone_tier{zone=\"_other\",tier=\"idle\"} 3"));
        // A labeled zone stopping frees its slot for the next new zone.
        metrics.forget_zone("zone-0");
        let mut late = host("late", &clock);
        clock.advance(Duration::from_millis(100));
        let r = late.advance();
        metrics.record_advance(&late, &r);
        assert!(metrics.render().contains("zone=\"late\""));
        assert_eq!(metrics.zone_count(), 5);
    }
}
