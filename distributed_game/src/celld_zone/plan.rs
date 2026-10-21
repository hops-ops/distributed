//! The runtime-free parts of the celld zone host: options, the room's
//! lifecycle states, the self-correcting tick schedule, the durable fact
//! outbox encoding and the cell wait-path answer classification. Compiled
//! on every target so they are unit-tested natively; the Durable Object glue
//! ([`super::ZoneObject`], wasm32) drives them.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::facts::{Fact, FactError, FactReceipt};
use crate::stamp::FactStamp;

/// Env var: HS256 secret admission (and transfer) tickets are signed with.
pub const TICKET_SECRET_VAR: &str = "GAME_TICKET_SECRET";
/// Env var: tick rate (Hz, default 30).
pub const TICK_HZ_VAR: &str = "ZONE_TICK_HZ";
/// Env var: patch rate (Hz, default 20, at most the tick rate).
pub const PATCH_HZ_VAR: &str = "ZONE_PATCH_HZ";
/// Env var: snapshot interval while occupied (ms, default 5000).
pub const PERSIST_MS_VAR: &str = "ZONE_PERSIST_MS";
/// Env var: how long an empty room stays dormant before it hibernates (ms,
/// default 10000).
pub const HIBERNATE_AFTER_MS_VAR: &str = "ZONE_HIBERNATE_AFTER_MS";
/// Env var: reconnect grace (ms, default 5000).
pub const RECONNECT_GRACE_MS_VAR: &str = "ZONE_RECONNECT_GRACE_MS";
/// Env var: command-ledger service id of the zone's facts (default
/// `celld-zone`).
pub const FACT_SERVICE_VAR: &str = "ZONE_FACT_SERVICE";
/// Env var (debug only, off by default): `name=host:port,…` celld operator
/// listeners asked which node holds the room (celld does not tell
/// application code its node id).
pub const NODES_VAR: &str = "CELLD_ZONE_NODES";
/// Env var (debug only): a fixed node name (e.g. `dev` for `celld dev`).
pub const NODE_VAR: &str = "CELLD_ZONE_NODE";

/// Env var (dev only): `1` makes zone objects read the worker's dev
/// settings (the network simulation, [`NETSIM_SHARD`]).
pub const DEV_SETTINGS_VAR: &str = "ZONE_DEV_SETTINGS";
pub use crate::netsim::{NETSIM_SHARD, ZONE_DEV_KIND};

/// Request header naming the zone (set by [`super::forward_to_zone`]; the
/// object's id has no name on some celld paths).
pub const ZONE_HEADER: &str = "x-celld-zone";

/// Close code for sockets of an earlier activation (the room restarted or
/// moved): the client rejoins with a fresh ticket.
pub const CLOSE_RESTARTED: u16 = 1012;

/// Object settings, from the worker's vars and secrets.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectOptions {
    /// Ticket key secret.
    pub ticket_secret: String,
    /// Internal secret for cell calls (`DISTRIBUTED_INTERNAL_SECRET`).
    pub internal_secret: Option<String>,
    /// Hot tick rate.
    pub tick_hz: u32,
    /// Patch rate.
    pub patch_hz: u32,
    /// Snapshot interval while occupied.
    pub persist_every: Duration,
    /// Dormant → hibernated.
    pub hibernate_after: Duration,
    /// Reconnect grace window.
    pub reconnect_grace: Duration,
    /// Fact command-ledger service id.
    pub fact_service: String,
    /// Debug node lookup: `(name, operator host:port)`.
    pub nodes: Vec<(String, String)>,
    /// Debug fixed node name.
    pub node: Option<String>,
    /// Dev only: read the worker's dev settings (network simulation).
    pub dev_settings: bool,
}

fn parse<T: std::str::FromStr>(
    get: &impl Fn(&str) -> Option<String>,
    name: &str,
    default: T,
) -> Result<T, String> {
    match get(name).filter(|value| !value.trim().is_empty()) {
        None => Ok(default),
        Some(value) => value
            .trim()
            .parse()
            .map_err(|_| format!("{name}: not a number: {value}")),
    }
}

impl ObjectOptions {
    /// Read the options through `get` (vars, then secrets).
    pub fn from_vars(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let ticket_secret = get(TICKET_SECRET_VAR)
            .filter(|secret| secret.len() >= 16)
            .ok_or_else(|| format!("{TICKET_SECRET_VAR} (>= 16 bytes) is required"))?;
        let tick_hz: u32 = parse(&get, TICK_HZ_VAR, 30)?;
        if !(1..=120).contains(&tick_hz) {
            return Err(format!("{TICK_HZ_VAR} must be within 1..=120"));
        }
        let patch_hz: u32 = parse(&get, PATCH_HZ_VAR, 20)?;
        let nodes = get(NODES_VAR)
            .unwrap_or_default()
            .split(',')
            .filter_map(|entry| {
                let (name, addr) = entry.trim().split_once('=')?;
                (!name.is_empty() && !addr.is_empty()).then(|| (name.into(), addr.into()))
            })
            .collect();
        Ok(Self {
            ticket_secret,
            internal_secret: get(distributed::cell_host::CELL_INTERNAL_SECRET_ENV)
                .filter(|secret| !secret.is_empty()),
            tick_hz,
            patch_hz: patch_hz.clamp(1, tick_hz),
            persist_every: Duration::from_millis(parse(&get, PERSIST_MS_VAR, 5_000)?),
            hibernate_after: Duration::from_millis(parse(&get, HIBERNATE_AFTER_MS_VAR, 10_000)?),
            reconnect_grace: Duration::from_millis(parse(&get, RECONNECT_GRACE_MS_VAR, 5_000)?),
            fact_service: get(FACT_SERVICE_VAR)
                .filter(|service| !service.trim().is_empty())
                .unwrap_or_else(|| "celld-zone".into()),
            nodes,
            node: get(NODE_VAR).filter(|node| !node.is_empty()),
            dev_settings: get(DEV_SETTINGS_VAR)
                .is_some_and(|v| matches!(v.trim(), "1" | "true" | "yes")),
        })
    }
}

/// Where a room is in its life (per object activation).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RoomState {
    /// No zone in memory (fresh activation, or hibernated and dropped).
    Cold,
    /// Zone in memory with players (or just woke up).
    Active,
    /// Empty since `since_ms` (Unix ms); snapshot written.
    Dormant {
        /// When the last player left.
        since_ms: f64,
    },
}

impl RoomState {
    /// True when an empty room should hibernate at `now_ms`.
    pub fn hibernate_due(&self, now_ms: f64, after: Duration) -> bool {
        match self {
            Self::Dormant { since_ms } => now_ms + 1.0 >= since_ms + after.as_millis() as f64,
            _ => false,
        }
    }
}

/// A self-correcting fixed-rate schedule over absolute deadlines
/// (`setInterval` drifts: 33.3 ms gave 35 ms on celld). Each wake-up aims at
/// `start + k × step`; deadlines already missed are skipped (the fixed
/// timestep catches up inside one `advance`).
#[derive(Debug, Clone)]
pub struct TickSchedule {
    start_ms: f64,
    step_ms: f64,
    k: u64,
}

impl TickSchedule {
    /// A schedule at `hz` from `now_ms`.
    pub fn new(now_ms: f64, hz: u32) -> Self {
        Self {
            start_ms: now_ms,
            step_ms: 1000.0 / f64::from(hz.max(1)),
            k: 1,
        }
    }

    /// The next deadline.
    pub fn deadline(&self) -> f64 {
        self.start_ms + self.k as f64 * self.step_ms
    }

    /// How long to wait from `now_ms` (never negative).
    pub fn wait_ms(&self, now_ms: f64) -> f64 {
        (self.deadline() - now_ms).max(0.0)
    }

    /// Record a wake-up at `now_ms`: returns how late it was, and moves to
    /// the first deadline after `now_ms`.
    pub fn woke(&mut self, now_ms: f64) -> f64 {
        let late = (now_ms - self.deadline()).max(0.0);
        let next = ((now_ms - self.start_ms) / self.step_ms).floor() as u64 + 1;
        self.k = next.max(self.k + 1);
        late
    }
}

/// A fact in the object's durable outbox (written before it is sent,
/// deleted once settled, resent by the next activation).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredFact {
    /// Idempotency key.
    pub key: String,
    /// Command.
    pub command: String,
    /// Input.
    pub input: Value,
    /// Bound principal.
    pub principal: Option<String>,
    /// Stamp as its `x-game-*` session variables.
    pub stamp: HashMap<String, String>,
}

impl StoredFact {
    /// Storage key of a fact.
    pub fn storage_key(key: &str) -> String {
        format!("{OUTBOX_PREFIX}{key}")
    }

    /// Encode `fact`.
    pub fn of(fact: &Fact) -> Self {
        Self {
            key: fact.key().to_string(),
            command: fact.command().to_string(),
            input: fact.input().clone(),
            principal: fact.principal().map(str::to_string),
            stamp: fact.stamp().session_vars().into_iter().collect(),
        }
    }

    /// Decode (None for a malformed stamp).
    pub fn into_fact(self) -> Option<Fact> {
        let stamp = FactStamp::from_session_vars(&self.stamp)?;
        Some(Fact::trusted(
            self.key,
            self.command,
            self.input,
            self.principal,
            stamp,
        ))
    }
}

/// Storage key prefix of the durable fact outbox.
pub const OUTBOX_PREFIX: &str = "fact:";

/// Durable Object binding of a cell kind: upper case, `-` → `_`
/// (`zone-activity` → `ZONE_ACTIVITY`, `crawler` → `CRAWLER`).
pub fn cell_binding(kind: &str) -> String {
    kind.chars()
        .map(|c| {
            if c == '-' {
                '_'
            } else {
                c.to_ascii_uppercase()
            }
        })
        .collect()
}

/// Classify a cell wait-path answer (`status`, JSON `body`).
pub fn classify_wait_path(status: u16, body: &Value) -> Result<FactReceipt, FactError> {
    if (200..300).contains(&status) {
        return Ok(FactReceipt {
            payload: body.get("payload").cloned().unwrap_or(Value::Null),
            replayed: body
                .pointer("/receipt/replayed")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        });
    }
    let code = body.get("code").and_then(Value::as_str).unwrap_or("");
    let message = body
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("rejected");
    let detail = format!("HTTP {status} {code}: {message}");
    if status >= 500 || matches!(status, 408 | 429) || code == "COMMAND_IN_PROGRESS" {
        Err(FactError::Retryable(detail))
    } else {
        Err(FactError::Permanent(detail))
    }
}

/// `"{prefix}{zone}[/rest]"` → `(zone, rest)` (percent-decoded zone; None
/// when the path does not match or the zone is empty / not printable).
pub fn zone_from_path<'a>(prefix: &str, path: &'a str) -> Option<(String, &'a str)> {
    let rest = path.strip_prefix(prefix)?;
    let (raw, tail) = rest.split_once('/').unwrap_or((rest, ""));
    let zone = percent_decode(raw)?;
    (!zone.is_empty()
        && zone.len() <= 128
        && zone.bytes().all(|byte| (0x21..=0x7e).contains(&byte)))
    .then_some((zone, tail))
}

fn percent_decode(segment: &str) -> Option<String> {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            out.push(u8::from_str_radix(segment.get(i + 1..i + 3)?, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Percentiles of the last values (tick timing in the debug info).
#[derive(Debug, Clone, Default)]
pub struct Window {
    values: std::collections::VecDeque<f64>,
}

impl Window {
    const MAX: usize = 900;

    /// Add a value.
    pub fn push(&mut self, value: f64) {
        if self.values.len() == Self::MAX {
            self.values.pop_front();
        }
        self.values.push_back(value);
    }

    /// The `p` percentile (0 when empty).
    pub fn percentile(&self, p: f64) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }
        let mut sorted: Vec<f64> = self.values.iter().copied().collect();
        sorted.sort_by(f64::total_cmp);
        sorted[((sorted.len() - 1) as f64 * p.clamp(0.0, 1.0)).round() as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn options_need_a_ticket_secret_and_default_the_rest() {
        assert!(ObjectOptions::from_vars(vars(&[])).is_err());
        assert!(ObjectOptions::from_vars(vars(&[(TICKET_SECRET_VAR, "short")])).is_err());
        let options =
            ObjectOptions::from_vars(vars(&[(TICKET_SECRET_VAR, "0123456789abcdef")])).unwrap();
        assert_eq!((options.tick_hz, options.patch_hz), (30, 20));
        assert_eq!(options.hibernate_after, Duration::from_secs(10));
        assert!(options.nodes.is_empty(), "node lookup is off by default");
        assert_eq!(options.fact_service, "celld-zone");
        let options = ObjectOptions::from_vars(vars(&[
            (TICKET_SECRET_VAR, "0123456789abcdef"),
            (TICK_HZ_VAR, "10"),
            (PATCH_HZ_VAR, "20"),
            (NODES_VAR, "node-a=127.0.0.1:1, bad ,node-b=127.0.0.1:2"),
            ("DISTRIBUTED_INTERNAL_SECRET", "s"),
        ]))
        .unwrap();
        assert_eq!(options.patch_hz, 10, "patches never outpace ticks");
        assert_eq!(options.nodes.len(), 2);
        assert_eq!(options.internal_secret.as_deref(), Some("s"));
        assert!(ObjectOptions::from_vars(vars(&[
            (TICKET_SECRET_VAR, "0123456789abcdef"),
            (TICK_HZ_VAR, "0"),
        ]))
        .is_err());
    }

    #[test]
    fn the_tick_schedule_corrects_itself_and_skips_missed_deadlines() {
        let mut schedule = TickSchedule::new(1000.0, 20);
        assert_eq!(schedule.deadline(), 1050.0);
        assert_eq!(schedule.wait_ms(1010.0), 40.0);
        // Woke 3 ms late: the next deadline stays on the grid (no drift).
        assert_eq!(schedule.woke(1053.0), 3.0);
        assert_eq!(schedule.deadline(), 1100.0);
        // A 130 ms stall skips the missed deadlines.
        assert_eq!(schedule.woke(1230.0), 130.0);
        assert_eq!(schedule.deadline(), 1250.0);
        assert_eq!(schedule.wait_ms(1300.0), 0.0);
        // Early wake-ups still advance by one step.
        let before = schedule.deadline();
        schedule.woke(before - 5.0);
        assert_eq!(schedule.deadline(), before + 50.0);
    }

    #[test]
    fn dormant_rooms_hibernate_after_the_delay() {
        let after = Duration::from_millis(500);
        assert!(!RoomState::Active.hibernate_due(10_000.0, after));
        assert!(!RoomState::Cold.hibernate_due(10_000.0, after));
        let dormant = RoomState::Dormant { since_ms: 1_000.0 };
        assert!(!dormant.hibernate_due(1_400.0, after));
        assert!(dormant.hibernate_due(1_500.0, after));
    }

    #[test]
    fn stored_facts_round_trip() {
        let fact = Fact::trusted(
            "kill:pit:1:m1:0",
            "crawler.record_kill",
            json!({ "mob": 1 }),
            Some("alice".into()),
            FactStamp {
                zone_id: "pit.0.0".into(),
                tick: 7,
                activation: 9,
                seq: 2,
                emitted_at_ms: 1_700_000_000_000,
                ..FactStamp::default()
            },
        );
        let stored = StoredFact::of(&fact);
        let text = serde_json::to_string(&stored).unwrap();
        let back: StoredFact = serde_json::from_str(&text).unwrap();
        assert_eq!(back.into_fact().unwrap(), fact);
        assert_eq!(StoredFact::storage_key("k"), format!("{OUTBOX_PREFIX}k"));
    }

    #[test]
    fn wait_path_answers_are_classified() {
        let ok = classify_wait_path(
            200,
            &json!({ "payload": { "xp": 3 }, "receipt": { "replayed": true } }),
        )
        .unwrap();
        assert_eq!(ok.payload, json!({ "xp": 3 }));
        assert!(ok.replayed);
        let retry = |status, code: &str| {
            matches!(
                classify_wait_path(status, &json!({ "code": code })),
                Err(FactError::Retryable(_))
            )
        };
        assert!(retry(503, "INTERNAL"));
        assert!(retry(409, "COMMAND_IN_PROGRESS"));
        assert!(retry(429, ""));
        assert!(!retry(401, "UNAUTHORIZED"));
        assert!(!retry(422, "REJECTED"));
    }

    #[test]
    fn bindings_and_paths() {
        assert_eq!(cell_binding("zone-activity"), "ZONE_ACTIVITY");
        assert_eq!(cell_binding("crawler"), "CRAWLER");
        assert_eq!(
            zone_from_path("/rooms/", "/rooms/pit.1.-2/info"),
            Some(("pit.1.-2".to_string(), "info"))
        );
        assert_eq!(
            zone_from_path("/rooms/", "/rooms/a%20b"),
            None,
            "zones are printable ASCII without spaces"
        );
        assert_eq!(zone_from_path("/rooms/", "/rooms/"), None);
        assert_eq!(zone_from_path("/rooms/", "/other/x"), None);
    }

    #[test]
    fn windows_report_percentiles() {
        let mut window = Window::default();
        assert_eq!(window.percentile(0.5), 0.0);
        for v in 1..=100 {
            window.push(f64::from(v));
        }
        assert_eq!(window.percentile(0.0), 1.0);
        assert_eq!(window.percentile(1.0), 100.0);
        assert!((window.percentile(0.99) - 99.0).abs() <= 1.0);
    }
}
