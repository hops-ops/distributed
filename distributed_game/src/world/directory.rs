//! Zone placement: which host owns a zone.
//!
//! A [`ZoneDirectory`] answers `locate(zone_id)` with the owning
//! [`HostAddr`]. It is advisory routing: actual ownership is enforced by a
//! fenced claim ([`ZoneOwnership`](super::ZoneOwnership)) when the zone is
//! activated, so a stale directory can at worst send a client to the wrong
//! host, which redirects it or refuses — it can never make two hosts run the
//! same zone.

use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

use crate::rng::Fnv64;

/// A host in the world.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HostAddr {
    /// Stable host id (also the ownership holder prefix).
    pub id: String,
    /// WebSocket base URL clients are redirected to, e.g.
    /// `ws://10.0.0.2:2567` or, behind a proxy, `wss://game.example/h/a`
    /// (clients connect to `{ws_url}/zones/{zone_id}`). Peers never use it.
    pub ws_url: String,
    /// Admin HTTP base URL, e.g. `http://10.0.0.2:2567` (peer calls).
    pub admin_url: String,
}

impl HostAddr {
    /// Host listening on `addr` (`host:port`) for both WebSocket and admin.
    pub fn new(id: impl Into<String>, addr: impl AsRef<str>) -> Self {
        let addr = addr.as_ref();
        Self {
            id: id.into(),
            ws_url: format!("ws://{addr}"),
            admin_url: format!("http://{addr}"),
        }
    }

    /// Advertise `ws_url` (e.g. the proxied public URL) instead of the raw
    /// `ws://{addr}`; admin/peer calls keep using `admin_url`.
    pub fn with_ws_url(mut self, ws_url: impl Into<String>) -> Self {
        self.ws_url = ws_url.into().trim_end_matches('/').to_string();
        self
    }

    /// Client URL of `zone_id` on this host.
    pub fn zone_url(&self, zone_id: &str) -> String {
        format!("{}/zones/{zone_id}", self.ws_url)
    }
}

/// Zone → owning host lookup.
pub trait ZoneDirectory: Send + Sync + 'static {
    /// Host that should own `zone_id` (None = unknown / not served).
    fn locate(&self, zone_id: &str) -> Option<HostAddr>;

    /// Host by id.
    fn host(&self, host_id: &str) -> Option<HostAddr>;

    /// Every known host.
    fn hosts(&self) -> Vec<HostAddr>;
}

/// Explicit zone → host assignments with an optional fallback host.
#[derive(Debug, Default)]
pub struct StaticDirectory {
    inner: RwLock<StaticInner>,
}

#[derive(Debug, Default)]
struct StaticInner {
    hosts: BTreeMap<String, HostAddr>,
    zones: HashMap<String, String>,
    fallback: Option<String>,
}

impl StaticDirectory {
    /// Empty directory.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a host.
    pub fn add_host(&self, host: HostAddr) -> &Self {
        let mut inner = self.inner.write().expect("directory");
        inner.hosts.insert(host.id.clone(), host);
        self
    }

    /// Assign `zone_id` to `host_id` (also used to move a zone).
    pub fn assign(&self, zone_id: impl Into<String>, host_id: impl Into<String>) -> &Self {
        let mut inner = self.inner.write().expect("directory");
        inner.zones.insert(zone_id.into(), host_id.into());
        self
    }

    /// Host for zones without an explicit assignment.
    pub fn fallback(&self, host_id: impl Into<String>) -> &Self {
        self.inner.write().expect("directory").fallback = Some(host_id.into());
        self
    }
}

impl ZoneDirectory for StaticDirectory {
    fn locate(&self, zone_id: &str) -> Option<HostAddr> {
        let inner = self.inner.read().expect("directory");
        let id = inner.zones.get(zone_id).or(inner.fallback.as_ref())?;
        inner.hosts.get(id).cloned()
    }

    fn host(&self, host_id: &str) -> Option<HostAddr> {
        self.inner
            .read()
            .expect("directory")
            .hosts
            .get(host_id)
            .cloned()
    }

    fn hosts(&self) -> Vec<HostAddr> {
        self.inner
            .read()
            .expect("directory")
            .hosts
            .values()
            .cloned()
            .collect()
    }
}

/// Consistent-hash ring over hosts with virtual nodes.
///
/// Each host owns `vnodes` points on a 64-bit ring; a zone belongs to the
/// first point clockwise from its hash. Adding or removing one of N hosts
/// moves only ~1/N of the zones (those whose nearest point changed); every
/// other zone keeps its owner.
#[derive(Debug)]
pub struct HashRing {
    vnodes: u32,
    inner: RwLock<RingInner>,
}

#[derive(Debug, Default)]
struct RingInner {
    hosts: BTreeMap<String, HostAddr>,
    ring: BTreeMap<u64, String>,
}

fn ring_hash(bytes: &[u8]) -> u64 {
    // FNV-1a then a SplitMix64 finalizer for avalanche.
    let mut z = Fnv64::new().write(bytes).finish();
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

impl HashRing {
    /// Empty ring with `vnodes` virtual nodes per host (>= 1; 64–256 typical).
    pub fn new(vnodes: u32) -> Self {
        Self {
            vnodes: vnodes.max(1),
            inner: RwLock::default(),
        }
    }

    /// Ring over `hosts`.
    pub fn with_hosts(vnodes: u32, hosts: impl IntoIterator<Item = HostAddr>) -> Self {
        let ring = Self::new(vnodes);
        for host in hosts {
            ring.add_host(host);
        }
        ring
    }

    /// Add (or replace) a host.
    pub fn add_host(&self, host: HostAddr) {
        let mut inner = self.inner.write().expect("ring");
        for v in 0..self.vnodes {
            let point = ring_hash(format!("{}#{v}", host.id).as_bytes());
            inner.ring.insert(point, host.id.clone());
        }
        inner.hosts.insert(host.id.clone(), host);
    }

    /// Remove a host; its zones move to their next points.
    pub fn remove_host(&self, host_id: &str) {
        let mut inner = self.inner.write().expect("ring");
        inner.hosts.remove(host_id);
        inner.ring.retain(|_, id| id != host_id);
    }

    /// Owning host id of `zone_id`.
    pub fn owner_id(&self, zone_id: &str) -> Option<String> {
        let inner = self.inner.read().expect("ring");
        let h = ring_hash(zone_id.as_bytes());
        inner
            .ring
            .range(h..)
            .next()
            .or_else(|| inner.ring.iter().next())
            .map(|(_, id)| id.clone())
    }
}

impl ZoneDirectory for HashRing {
    fn locate(&self, zone_id: &str) -> Option<HostAddr> {
        let id = self.owner_id(zone_id)?;
        self.host(&id)
    }

    fn host(&self, host_id: &str) -> Option<HostAddr> {
        self.inner.read().expect("ring").hosts.get(host_id).cloned()
    }

    fn hosts(&self) -> Vec<HostAddr> {
        self.inner
            .read()
            .expect("ring")
            .hosts
            .values()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host(i: usize) -> HostAddr {
        HostAddr::new(format!("h{i}"), format!("10.0.0.{i}:2567"))
    }

    fn owners(ring: &HashRing, zones: &[String]) -> Vec<String> {
        zones.iter().map(|z| ring.owner_id(z).unwrap()).collect()
    }

    #[test]
    fn ring_is_balanced_and_stable_under_add_and_remove() {
        let ring = HashRing::with_hosts(128, (0..4).map(host));
        let zones: Vec<String> = (0..4000).map(|i| format!("zone-{i}")).collect();
        let before = owners(&ring, &zones);
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for o in &before {
            *counts.entry(o.as_str()).or_default() += 1;
        }
        for (_, n) in counts {
            assert!((600..=1400).contains(&n), "unbalanced: {n}");
        }

        ring.add_host(host(4));
        let after = owners(&ring, &zones);
        let moved = before.iter().zip(&after).filter(|(a, b)| a != b).count();
        // Ideal is 1/5 = 800; every moved zone moved to the new host.
        assert!(moved > 400 && moved < 1300, "moved {moved}");
        assert!(before.iter().zip(&after).all(|(a, b)| a == b || b == "h4"));

        ring.remove_host("h4");
        assert_eq!(owners(&ring, &zones), before);

        ring.remove_host("h1");
        let without = owners(&ring, &zones);
        assert!(before
            .iter()
            .zip(&without)
            .all(|(a, b)| a == b || a == "h1"));
        assert!(ring.locate("zone-1").is_some());
        assert_eq!(ring.hosts().len(), 3);
    }

    #[test]
    fn static_directory_assigns_with_fallback() {
        let dir = StaticDirectory::new();
        dir.add_host(host(1)).add_host(host(2));
        dir.assign("realm.west", "h2").fallback("h1");
        assert_eq!(dir.locate("realm.west").unwrap().id, "h2");
        assert_eq!(dir.locate("anything").unwrap().id, "h1");
        assert_eq!(
            dir.locate("realm.west").unwrap().zone_url("realm.west"),
            "ws://10.0.0.2:2567/zones/realm.west"
        );
    }
}
