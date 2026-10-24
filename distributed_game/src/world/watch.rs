//! Watched world views: zones keep pushed read models of the world cells
//! they care about instead of polling them.
//!
//! A world cell (a hood, the content rollout, …) is the write model. A zone
//! that cares about it **watches** it: the zone's read of the cell (`GET
//! /{kind}/{shard}?watch=<zone>&lease=<ms>`) returns the cell's view with
//! its `version` and leaves a lease, and after every commit that moves the
//! version the cell pushes [`WatchUpdate`] `{topic, version, view}` to the
//! zones whose lease is live. The zone keeps the highest version per topic
//! ([`TopicVersions`]), so a late push or read never rolls it back, and
//! re-reads each topic every [`RECONCILE_MS`] ± [`RECONCILE_JITTER_MS`]
//! (renewing its lease; repairing a lost push). Eventually consistent by
//! design: for display and world flavour, never for settling rewards.
//!
//! Cost: O(changes × watchers + zones / [`RECONCILE_MS`]) instead of every
//! zone reading every cell every second or two.
//!
//! Leases live in the cell's memory only ([`Watchers`]): a watch writes
//! nothing, and a cell that restarted relearns its watchers within one
//! reconcile.
//!
//! A zone declares what it watches with [`Watches`] (typed: each view
//! decodes into a plain serde struct and becomes one of the zone's
//! messages); the celld zone host does the reads, leases and pushes.

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// How long a watch lasts without a renewing read (ms).
pub const WATCH_LEASE_MS: u64 = 120_000;
/// How often a zone re-reads each watched topic (ms), on average.
pub const RECONCILE_MS: u64 = 45_000;
/// Spread of [`RECONCILE_MS`] (± ms), so zones that woke together do not
/// read together.
pub const RECONCILE_JITTER_MS: u64 = 15_000;
/// How often a zone that takes no pushes (no class binding, or
/// `ZONE_WORLD_PUSH=0`) polls each watched topic (ms).
pub const POLL_MS: u64 = 1_000;
/// Watchers pushed to at once by one cell.
pub const PUSH_CONCURRENCY: usize = 16;
/// Waits (ms) before each retry of a push that failed in transport or was
/// refused for load (5xx, 429): a busy zone still gets a rollout's freeze
/// within seconds instead of at its next reconcile.
pub const PUSH_RETRY_MS: [u64; 3] = [250, 1_000, 3_000];

/// Whether a push answered with `status` is worth retrying. A zone takes
/// every push it can parse with 204, asleep or not; other 4xx stay wrong.
pub fn push_retryable(status: u16) -> bool {
    status == 429 || status >= 500
}
/// Query parameter naming the watcher (`<binding>/<zone>`).
pub const WATCH_PARAM: &str = "watch";
/// Query parameter with the lease (ms).
pub const LEASE_PARAM: &str = "lease";
/// Path segment of the zone route that takes pushes (`POST …/__world`).
pub const PUSH_ROUTE: &str = "__world";
/// Path segment of a watched cell's counters (`GET /{kind}/{shard}/__watchers`,
/// internal secret): its live watchers and [`WatcherStats`].
pub const STATS_ROUTE: &str = "__watchers";

/// The topic of cell `kind/shard`.
pub fn topic(kind: &str, shard: &str) -> String {
    format!("{kind}/{shard}")
}

/// A watcher's address: the Durable Object namespace binding of its class
/// and its name (`PIT_ROOM/pit.0.0`).
pub fn watcher_address(binding: &str, name: &str) -> String {
    format!("{binding}/{name}")
}

/// `(binding, name)` of a [`watcher_address`].
pub fn parse_watcher_address(address: &str) -> Option<(&str, &str)> {
    address
        .split_once('/')
        .filter(|(binding, name)| !binding.is_empty() && !name.is_empty())
}

/// The class binding of a Durable Object class name, by convention
/// (`PitRoom` → `PIT_ROOM`).
pub fn class_binding(class: &str) -> String {
    let mut binding = String::with_capacity(class.len() + 4);
    for (i, c) in class.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            binding.push('_');
        }
        binding.push(c.to_ascii_uppercase());
    }
    binding
}

/// What a cell pushes to its watchers (`POST …/__world`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WatchUpdate {
    /// [`topic`] of the cell.
    pub topic: String,
    /// The cell's version (its aggregate version).
    pub version: u64,
    /// The cell's view (what its read route returns).
    pub view: Value,
}

/// A cell's watchers: leases by [`watcher_address`] (memory only).
#[derive(Debug, Default)]
pub struct Watchers {
    leases: BTreeMap<String, u64>,
    published: Option<u64>,
    last_view: Option<String>,
    stats: WatcherStats,
}

/// What a watched cell did for its watchers since it started (camelCase
/// JSON; the cell's `GET …/__watchers`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct WatcherStats {
    /// Watching reads (each leaves or renews a lease).
    pub reads: u64,
    /// Views published to the live watchers.
    pub published: u64,
    /// Views not published: unchanged apart from `version`, or not newer.
    pub skipped: u64,
    /// Pushes sent (one per live watcher of each published view).
    pub pushes: u64,
}

impl Watchers {
    /// No watchers.
    pub fn new() -> Self {
        Self::default()
    }

    /// `watcher` watches until `now_ms + lease_ms` (capped at
    /// [`WATCH_LEASE_MS`]); a zero lease ends its watch.
    pub fn watch(&mut self, watcher: &str, now_ms: u64, lease_ms: u64) {
        self.stats.reads += 1;
        if lease_ms == 0 {
            self.leases.remove(watcher);
            return;
        }
        let until = now_ms.saturating_add(lease_ms.min(WATCH_LEASE_MS));
        self.leases.insert(watcher.to_string(), until);
    }

    /// The watchers with a live lease at `now_ms` (expired ones dropped).
    pub fn live(&mut self, now_ms: u64) -> Vec<String> {
        self.leases.retain(|_, until| *until > now_ms);
        self.leases.keys().cloned().collect()
    }

    /// Whether `view` at `version` is news for the watchers: newer than
    /// the last one published and different apart from its `version`
    /// (a command that moves the aggregate but not its view, like a
    /// heartbeat, pushes nothing). Records it when it is.
    pub fn publish(&mut self, version: u64, view: &Value) -> bool {
        let news = self.news(version, view);
        if news {
            self.stats.published += 1;
        } else {
            self.stats.skipped += 1;
        }
        news
    }

    fn news(&mut self, version: u64, view: &Value) -> bool {
        if self.published.is_some_and(|last| version <= last) {
            return false;
        }
        self.published = Some(version);
        let content = match view {
            Value::Object(fields) => {
                let mut fields = fields.clone();
                fields.remove("version");
                Value::Object(fields).to_string()
            }
            other => other.to_string(),
        };
        if self.last_view.as_ref() == Some(&content) {
            return false;
        }
        self.last_view = Some(content);
        true
    }

    /// `n` pushes were sent.
    pub fn pushed(&mut self, n: usize) {
        self.stats.pushes += n as u64;
    }

    /// Counts so far.
    pub fn stats(&self) -> WatcherStats {
        self.stats
    }
}

/// `(watcher, lease ms)` from a read's query string, if it watches.
pub fn watch_query(query: &str) -> Option<(String, u64)> {
    let mut watcher = None;
    let mut lease = WATCH_LEASE_MS;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match key {
            WATCH_PARAM => watcher = Some(decode_component(value)),
            LEASE_PARAM => lease = value.parse().unwrap_or(WATCH_LEASE_MS),
            _ => {}
        }
    }
    watcher
        .filter(|w| parse_watcher_address(w).is_some())
        .map(|w| (w, lease))
}

/// The query string a watching read sends.
pub fn watch_query_string(watcher: &str, lease_ms: u64) -> String {
    format!(
        "{WATCH_PARAM}={}&{LEASE_PARAM}={lease_ms}",
        encode_component(watcher)
    )
}

/// The view's version: its `version` field (0 when the cell does not
/// version its view: every read then applies).
pub fn view_version(view: &Value) -> u64 {
    view.get("version").and_then(Value::as_u64).unwrap_or(0)
}

/// A zone's highest version seen per topic.
#[derive(Debug, Default, Clone)]
pub struct TopicVersions {
    seen: BTreeMap<String, u64>,
}

impl TopicVersions {
    /// Nothing seen.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `version` of `topic` should be applied: newer than any
    /// seen, or unversioned (0) views, which always apply.
    pub fn accept(&mut self, topic: &str, version: u64) -> bool {
        if version == 0 {
            return true;
        }
        match self.seen.get(topic) {
            Some(&seen) if version <= seen => false,
            _ => {
                self.seen.insert(topic.to_string(), version);
                true
            }
        }
    }

    /// The highest version seen of `topic`.
    pub fn get(&self, topic: &str) -> Option<u64> {
        self.seen.get(topic).copied()
    }

    /// Forget everything (a new activation re-hydrates).
    pub fn clear(&mut self) {
        self.seen.clear();
    }
}

/// A zone's side of its watches: the highest version applied per topic
/// ([`TopicVersions`]) and when each topic is read next. A zone that takes
/// pushes reads each topic on wake and then every [`RECONCILE_MS`] ±
/// [`RECONCILE_JITTER_MS`] (renewing its lease, repairing a lost push);
/// one that does not polls every [`POLL_MS`].
#[derive(Debug, Default)]
pub struct WorldSync {
    versions: TopicVersions,
    due: BTreeMap<String, u64>,
}

impl WorldSync {
    /// Nothing applied, every topic due.
    pub fn new() -> Self {
        Self::default()
    }

    /// The `topics` to read at `now_ms` (every one with `all`, on wake),
    /// each scheduled for its next read: a reconcile when the zone takes
    /// `pushed` updates (`unit()` in `[0, 1)` choosing where in the
    /// spread), else a poll.
    pub fn due(
        &mut self,
        topics: impl IntoIterator<Item = String>,
        now_ms: u64,
        all: bool,
        pushed: bool,
        mut unit: impl FnMut() -> f64,
    ) -> Vec<String> {
        let mut reads = Vec::new();
        for topic in topics {
            let next = self.due.get(&topic).copied().unwrap_or(0);
            if !all && now_ms < next {
                continue;
            }
            let after = if pushed {
                next_reconcile_ms(now_ms, unit())
            } else {
                now_ms + POLL_MS
            };
            self.due.insert(topic.clone(), after);
            reads.push(topic);
        }
        reads
    }

    /// Whether `version` of `topic` (read or pushed) should be applied
    /// ([`TopicVersions::accept`]).
    pub fn accept(&mut self, topic: &str, version: u64) -> bool {
        self.versions.accept(topic, version)
    }

    /// The highest version applied of `topic`.
    pub fn version(&self, topic: &str) -> Option<u64> {
        self.versions.get(topic)
    }

    /// Forget everything (a new activation re-hydrates).
    pub fn clear(&mut self) {
        self.versions.clear();
        self.due.clear();
    }
}

/// The next reconcile of a topic: [`RECONCILE_MS`] ± [`RECONCILE_JITTER_MS`]
/// after `now_ms`, `unit` in `[0, 1)` choosing where in the spread.
pub fn next_reconcile_ms(now_ms: u64, unit: f64) -> u64 {
    let spread = (unit.clamp(0.0, 1.0) * 2.0 - 1.0) * RECONCILE_JITTER_MS as f64;
    now_ms + (RECONCILE_MS as f64 + spread).max(1_000.0) as u64
}

type Decode<M> = Box<dyn Fn(Value) -> Option<M>>;

/// One watched cell: its topic and how its view becomes a message.
pub struct Watch<M> {
    /// Cell kind.
    pub kind: String,
    /// Cell shard.
    pub shard: String,
    decode: Decode<M>,
}

impl<M> Watch<M> {
    /// [`topic`] of the cell.
    pub fn topic(&self) -> String {
        topic(&self.kind, &self.shard)
    }

    /// The message for `view` (`None`: nothing to deliver, e.g. the cell
    /// has no state yet or the view does not decode).
    pub fn message(&self, view: Value) -> Option<M> {
        (self.decode)(view)
    }
}

/// What a zone watches, each cell's view decoding into a plain struct that
/// becomes a zone message:
///
/// ```
/// # use distributed_game::world::watch::Watches;
/// # #[derive(serde::Deserialize)] struct HoodRead { state: Option<u32> }
/// # enum Msg { Hood(u32) }
/// let watches: Watches<Msg> = Watches::new()
///     .cell("hood", "0.0", |read: HoodRead| read.state.map(Msg::Hood));
/// assert_eq!(watches.len(), 1);
/// ```
pub struct Watches<M> {
    entries: Vec<Watch<M>>,
}

impl<M> Default for Watches<M> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<M: 'static> Watches<M> {
    /// Nothing watched.
    pub fn new() -> Self {
        Self::default()
    }

    /// Watch cell `kind/shard`; its view decodes into `V` and `to` makes
    /// the zone's message of it.
    pub fn cell<V, F>(mut self, kind: &str, shard: &str, to: F) -> Self
    where
        V: DeserializeOwned,
        F: Fn(V) -> Option<M> + 'static,
    {
        self.entries.push(Watch {
            kind: kind.to_string(),
            shard: shard.to_string(),
            decode: Box::new(move |view| serde_json::from_value(view).ok().and_then(&to)),
        });
        self
    }

    /// Watched cells.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Nothing watched.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The watched cells.
    pub fn iter(&self) -> impl Iterator<Item = &Watch<M>> {
        self.entries.iter()
    }

    /// The watch of `topic`.
    pub fn get(&self, topic: &str) -> Option<&Watch<M>> {
        self.entries.iter().find(|w| w.topic() == topic)
    }
}

fn encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn decode_component(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => match u8::from_str_radix(&value[i + 1..i + 3], 16) {
                Ok(byte) => {
                    out.push(byte);
                    i += 3;
                }
                Err(_) => {
                    out.push(b'%');
                    i += 1;
                }
            },
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn leases_expire_and_renew() {
        let mut watchers = Watchers::new();
        watchers.watch("PIT_ROOM/pit.0.0", 1_000, 10_000);
        watchers.watch("PIT_ROOM/pit.1.0", 1_000, 60_000);
        assert_eq!(watchers.live(5_000).len(), 2);
        assert_eq!(watchers.live(12_000), vec!["PIT_ROOM/pit.1.0".to_string()]);
        watchers.watch("PIT_ROOM/pit.1.0", 12_000, 0);
        assert!(watchers.live(12_000).is_empty());
        // Leases are capped.
        watchers.watch("PIT_ROOM/pit.2.0", 0, u64::MAX);
        assert!(watchers.live(WATCH_LEASE_MS).is_empty());
    }

    #[test]
    fn a_version_is_published_once() {
        let mut watchers = Watchers::new();
        let view = |n: u64| json!({ "version": n, "tier": n });
        assert!(watchers.publish(3, &view(3)));
        assert!(!watchers.publish(3, &view(3)));
        assert!(!watchers.publish(2, &view(2)));
        assert!(watchers.publish(4, &view(4)));
    }

    #[test]
    fn an_unchanged_view_is_not_pushed() {
        let mut watchers = Watchers::new();
        assert!(watchers.publish(1, &json!({ "version": 1, "tier": 2 })));
        // The aggregate moved (a heartbeat) but its view did not.
        assert!(!watchers.publish(2, &json!({ "version": 2, "tier": 2 })));
        assert!(watchers.publish(3, &json!({ "version": 3, "tier": 3 })));
    }

    #[test]
    fn watchers_count_their_work() {
        let mut watchers = Watchers::new();
        watchers.watch("PIT_ROOM/pit.0.0", 0, 1_000);
        watchers.watch("PIT_ROOM/pit.0.0", 500, 1_000);
        assert!(watchers.publish(1, &json!({ "version": 1, "tier": 1 })));
        let live = watchers.live(600).len();
        watchers.pushed(live);
        assert!(!watchers.publish(2, &json!({ "version": 2, "tier": 1 })));
        assert_eq!(
            watchers.stats(),
            WatcherStats {
                reads: 2,
                published: 1,
                skipped: 1,
                pushes: 1,
            }
        );
    }

    /// A deterministic `[0, 1)` sequence (xorshift).
    fn units(seed: u64) -> impl FnMut() -> f64 {
        let mut x = seed.max(1);
        move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 11) as f64 / (1u64 << 53) as f64
        }
    }

    /// One zone watching one cell on a fake clock: the cell changes now and
    /// then, and pushes are lost with `loss` (0..=1). Returns the longest
    /// time (ms) the zone was behind the cell.
    fn longest_lag(seed: u64, loss: f64) -> u64 {
        const ZONE: &str = "PIT_ROOM/pit.0.0";
        const STEP_MS: u64 = 100;
        let hood = topic("hood", "0.0");
        let view = |v: u64| json!({ "version": v, "tier": v });
        let mut cell = Watchers::new();
        let mut cell_version = 1u64;
        let mut zone = WorldSync::new();
        let mut unit = units(seed);
        let mut chance = units(seed ^ 0xfeed);
        let (mut behind_since, mut longest) = (None::<u64>, 0u64);
        let mut now = 0u64;
        let mut all = true;
        while now < 30 * 60_000 {
            // The cell changes about every 20 s; its push may be lost.
            if now > 0 && chance() < STEP_MS as f64 / 20_000.0 {
                cell_version += 1;
                if cell.publish(cell_version, &view(cell_version)) {
                    let live = cell.live(now);
                    cell.pushed(live.len());
                    if live.iter().any(|w| w == ZONE) && chance() >= loss {
                        zone.accept(&hood, cell_version);
                    }
                }
            }
            // The zone's tick: due reads renew its lease and apply the view.
            for topic in zone.due([hood.clone()], now, all, true, &mut unit) {
                cell.watch(ZONE, now, WATCH_LEASE_MS);
                zone.accept(&topic, cell_version);
            }
            all = false;
            if zone.version(&hood) == Some(cell_version) {
                behind_since = None;
            } else {
                let since = *behind_since.get_or_insert(now);
                longest = longest.max(now - since);
            }
            now += STEP_MS;
        }
        assert!(cell.live(now).contains(&ZONE.to_string()), "lease renewed");
        longest
    }

    #[test]
    fn a_reconcile_repairs_a_missed_push() {
        let period = RECONCILE_MS + RECONCILE_JITTER_MS;
        for seed in 1..=8 {
            // Every push lost: the next reconcile catches the zone up.
            let lag = longest_lag(seed, 1.0);
            assert!(lag > 0 && lag <= period, "seed {seed}: behind for {lag} ms");
            // Some pushes lost.
            let lag = longest_lag(seed, 0.3);
            assert!(lag <= period, "seed {seed}: behind for {lag} ms");
        }
        // No push lost: the zone is never behind.
        assert_eq!(longest_lag(3, 0.0), 0);
    }

    #[test]
    fn a_zone_without_pushes_polls() {
        let mut zone = WorldSync::new();
        let topics = || [topic("hood", "0.0"), topic("city", "city")];
        assert_eq!(zone.due(topics(), 0, true, false, || 0.5).len(), 2);
        assert!(zone
            .due(topics(), POLL_MS - 1, false, false, || 0.5)
            .is_empty());
        assert_eq!(zone.due(topics(), POLL_MS, false, false, || 0.5).len(), 2);
        // Pushed: the next read is a reconcile period away.
        assert_eq!(
            zone.due(topics(), 2 * POLL_MS, false, true, || 0.5).len(),
            2
        );
        assert!(zone
            .due(
                topics(),
                2 * POLL_MS + RECONCILE_MS - 1,
                false,
                true,
                || 0.5
            )
            .is_empty());
    }

    #[test]
    fn zones_never_roll_back() {
        let mut seen = TopicVersions::new();
        assert!(seen.accept("hood/0.0", 5));
        assert!(!seen.accept("hood/0.0", 5));
        assert!(!seen.accept("hood/0.0", 4), "a late push");
        assert!(seen.accept("hood/0.0", 6));
        assert!(seen.accept("borough/0.0", 1), "topics are independent");
        assert!(seen.accept("zone-dev/netsim", 0) && seen.accept("zone-dev/netsim", 0));
        seen.clear();
        assert!(seen.accept("hood/0.0", 2));
    }

    #[test]
    fn watch_queries_round_trip() {
        let query = watch_query_string("PIT_ROOM/pit.-1.2", 90_000);
        assert_eq!(query, "watch=PIT_ROOM%2Fpit.-1.2&lease=90000");
        assert_eq!(
            watch_query(&query),
            Some(("PIT_ROOM/pit.-1.2".to_string(), 90_000))
        );
        assert_eq!(
            watch_query("watch=PIT_ROOM%2Fpit.0.0"),
            Some(("PIT_ROOM/pit.0.0".into(), WATCH_LEASE_MS))
        );
        assert_eq!(
            watch_query("watch=A%2Fb%"),
            Some(("A/b%".into(), WATCH_LEASE_MS))
        );
        assert_eq!(watch_query(""), None);
        assert_eq!(watch_query("watch=nobinding"), None);
        assert_eq!(watch_query("lease=5"), None);
    }

    #[test]
    fn class_bindings_follow_the_convention() {
        assert_eq!(class_binding("PitRoom"), "PIT_ROOM");
        assert_eq!(class_binding("Room"), "ROOM");
        assert_eq!(
            parse_watcher_address("PIT_ROOM/pit.0.0"),
            Some(("PIT_ROOM", "pit.0.0"))
        );
        assert_eq!(parse_watcher_address("/x"), None);
    }

    #[test]
    fn reconciles_spread_around_the_period() {
        let low = next_reconcile_ms(0, 0.0);
        let mid = next_reconcile_ms(0, 0.5);
        let high = next_reconcile_ms(0, 0.999_999);
        assert_eq!(low, RECONCILE_MS - RECONCILE_JITTER_MS);
        assert_eq!(mid, RECONCILE_MS);
        assert!(high <= RECONCILE_MS + RECONCILE_JITTER_MS && high > mid);
        assert!(high < WATCH_LEASE_MS, "a zone renews before its lease ends");
    }

    #[test]
    fn watches_decode_views_into_messages() {
        #[derive(Deserialize)]
        struct HoodRead {
            state: Option<Hood>,
        }
        #[derive(Deserialize, Debug, PartialEq)]
        struct Hood {
            tier: u32,
        }
        #[derive(Debug, PartialEq)]
        enum Msg {
            Hood(u32),
        }
        let watches: Watches<Msg> = Watches::new().cell("hood", "1.2", |read: HoodRead| {
            read.state.map(|h| Msg::Hood(h.tier))
        });
        let watch = watches.get("hood/1.2").expect("watched");
        assert_eq!(
            watch.message(json!({"state": {"tier": 3}, "version": 7})),
            Some(Msg::Hood(3))
        );
        assert_eq!(watch.message(json!({"state": null})), None);
        assert_eq!(watch.message(json!("garbage")), None);
        assert_eq!(view_version(&json!({"version": 7})), 7);
        assert_eq!(view_version(&json!({})), 0);
    }

    #[test]
    fn a_push_is_retried_only_when_the_zone_could_not_take_it() {
        for status in [429, 500, 502, 503] {
            assert!(push_retryable(status), "{status}");
        }
        for status in [200, 204, 400, 401, 404] {
            assert!(!push_retryable(status), "{status}");
        }
    }
}
