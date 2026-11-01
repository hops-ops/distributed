//! Fenced zone ownership: at most one host runs a zone.
//!
//! A host activating a zone first acquires a claim on it
//! ([`ZoneOwnership::acquire`]) and keeps renewing it while the zone runs.
//! Every acquisition carries a fencing token strictly greater than any
//! earlier one for that zone, so writes made under an old claim (e.g. a
//! hibernation snapshot from a paused host) are rejected by fenced stores
//! ([`ZoneStateStore::save`](super::ZoneStateStore::save)). A host whose
//! renewal fails ([`OwnershipError::Lost`]) must stop the zone immediately
//! without persisting anything.
//!
//! Holders are `"{host_id}#{activation}"`, so a re-activation on the same
//! host (after hibernation) waits for the previous activation to release.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use crate::facts::BoxFuture;

/// A held claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneClaim {
    /// Zone id.
    pub zone_id: String,
    /// Holder (`host#activation`).
    pub holder: String,
    /// Monotonic fencing token.
    pub token: u64,
    /// Expiry as seen by the ownership store (`UNIX_EPOCH` = no TTL).
    pub expires_at: SystemTime,
}

/// Ownership failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipError {
    /// Someone else holds a live claim.
    Held {
        /// Current holder.
        holder: String,
    },
    /// The claim expired or was superseded: stop the zone.
    Lost,
    /// Transient backend failure.
    Backend(String),
}

/// Store of fenced zone claims.
pub trait ZoneOwnership: Send + Sync + 'static {
    /// Acquire `zone_id` for `holder` for `ttl`.
    fn acquire<'a>(
        &'a self,
        zone_id: &'a str,
        holder: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<ZoneClaim, OwnershipError>>;

    /// Extend a live claim (keeps its token).
    fn renew<'a>(
        &'a self,
        claim: &'a ZoneClaim,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<ZoneClaim, OwnershipError>>;

    /// Release a claim early.
    fn release<'a>(&'a self, claim: &'a ZoneClaim) -> BoxFuture<'a, ()>;
}

/// Process-local ownership without TTLs (single host / tests). Tokens are
/// monotonic per zone, survive release and **keep increasing across process
/// restarts**: the first token of every zone is the instance's epoch (Unix
/// time in microseconds at creation), so a restarted host's claims outrank
/// snapshots its previous run saved in a durable
/// [`ZoneStateStore`](super::ZoneStateStore).
#[derive(Debug)]
pub struct LocalOwnership {
    epoch: u64,
    claims: Mutex<HashMap<String, (Option<String>, u64)>>,
}

impl Default for LocalOwnership {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalOwnership {
    /// Empty, epoch = now (Unix microseconds).
    pub fn new() -> Self {
        let micros = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(1, |d| d.as_micros() as u64);
        Self::with_epoch(micros)
    }

    /// Empty with an explicit epoch (first token of every zone; must exceed
    /// every token a previous run used with the same state store).
    pub fn with_epoch(epoch: u64) -> Self {
        Self {
            epoch: epoch.max(1),
            claims: Mutex::new(HashMap::new()),
        }
    }

    /// First token handed out for each zone.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl ZoneOwnership for LocalOwnership {
    fn acquire<'a>(
        &'a self,
        zone_id: &'a str,
        holder: &'a str,
        _ttl: Duration,
    ) -> BoxFuture<'a, Result<ZoneClaim, OwnershipError>> {
        Box::pin(async move {
            let mut claims = self.claims.lock().expect("claims");
            let entry = claims.entry(zone_id.to_string()).or_insert((None, 0));
            match &entry.0 {
                Some(current) if current != holder => Err(OwnershipError::Held {
                    holder: current.clone(),
                }),
                Some(_) => Ok(entry.1),
                None => {
                    entry.0 = Some(holder.to_string());
                    entry.1 = (entry.1 + 1).max(self.epoch);
                    Ok(entry.1)
                }
            }
            .map(|token| ZoneClaim {
                zone_id: zone_id.to_string(),
                holder: holder.to_string(),
                token,
                expires_at: SystemTime::UNIX_EPOCH,
            })
        })
    }

    fn renew<'a>(
        &'a self,
        claim: &'a ZoneClaim,
        _ttl: Duration,
    ) -> BoxFuture<'a, Result<ZoneClaim, OwnershipError>> {
        Box::pin(async move {
            let claims = self.claims.lock().expect("claims");
            match claims.get(&claim.zone_id) {
                Some((Some(holder), token)) if *holder == claim.holder && *token == claim.token => {
                    Ok(claim.clone())
                }
                _ => Err(OwnershipError::Lost),
            }
        })
    }

    fn release<'a>(&'a self, claim: &'a ZoneClaim) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut claims = self.claims.lock().expect("claims");
            if let Some(entry) = claims.get_mut(&claim.zone_id) {
                if entry.0.as_deref() == Some(claim.holder.as_str()) && entry.1 == claim.token {
                    entry.0 = None;
                }
            }
        })
    }
}

/// Zone ownership over any `distributed::lease::LeaseStore` (in-memory,
/// SQLite or Postgres): key `zone:{zone_id}`, the lease's fencing token is
/// the claim token.
#[cfg(feature = "lease")]
#[derive(Debug, Clone)]
pub struct FencedLeaseOwnership<S> {
    store: S,
}

#[cfg(feature = "lease")]
impl<S> FencedLeaseOwnership<S> {
    /// Wrap a lease store.
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// The lease store.
    pub fn store(&self) -> &S {
        &self.store
    }
}

#[cfg(feature = "lease")]
fn lease_key(zone_id: &str) -> String {
    format!("zone:{zone_id}")
}

#[cfg(feature = "lease")]
fn map_lease_error(error: distributed::lease::LeaseError) -> OwnershipError {
    use distributed::lease::LeaseError;
    match error {
        LeaseError::Held { holder, .. } => OwnershipError::Held { holder },
        LeaseError::Lost { .. } | LeaseError::StaleFence { .. } => OwnershipError::Lost,
        other => OwnershipError::Backend(other.to_string()),
    }
}

#[cfg(feature = "lease")]
fn claim_of(zone_id: &str, lease: distributed::lease::Lease) -> ZoneClaim {
    ZoneClaim {
        zone_id: zone_id.to_string(),
        holder: lease.holder,
        token: lease.token.get(),
        expires_at: lease.expires_at,
    }
}

#[cfg(feature = "lease")]
impl<S> ZoneOwnership for FencedLeaseOwnership<S>
where
    S: distributed::lease::LeaseStore + 'static,
{
    fn acquire<'a>(
        &'a self,
        zone_id: &'a str,
        holder: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<ZoneClaim, OwnershipError>> {
        Box::pin(async move {
            let key = lease_key(zone_id);
            self.store
                .acquire(&key, holder, ttl)
                .await
                .map(|lease| claim_of(zone_id, lease))
                .map_err(map_lease_error)
        })
    }

    fn renew<'a>(
        &'a self,
        claim: &'a ZoneClaim,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<ZoneClaim, OwnershipError>> {
        Box::pin(async move {
            let lease = distributed::lease::Lease {
                key: lease_key(&claim.zone_id),
                holder: claim.holder.clone(),
                token: distributed::lease::FencingToken::new(claim.token),
                expires_at: claim.expires_at,
            };
            self.store
                .renew(&lease, ttl)
                .await
                .map(|lease| claim_of(&claim.zone_id, lease))
                .map_err(map_lease_error)
        })
    }

    fn release<'a>(&'a self, claim: &'a ZoneClaim) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let lease = distributed::lease::Lease {
                key: lease_key(&claim.zone_id),
                holder: claim.holder.clone(),
                token: distributed::lease::FencingToken::new(claim.token),
                expires_at: claim.expires_at,
            };
            let _ = self.store.release(&lease).await;
        })
    }
}

/// Behaviour every [`ZoneOwnership`] must show (exclusive claims, `Held`,
/// idempotent re-acquire, `Lost` after release, strictly increasing
/// fencing tokens). `zone_id` must be unused. Panics on a violation.
#[doc(hidden)]
pub async fn ownership_conformance(ownership: &dyn ZoneOwnership, zone_id: &str) {
    let ttl = Duration::from_secs(30);
    let first = ownership
        .acquire(zone_id, "h1#1", ttl)
        .await
        .expect("first acquire");
    assert_eq!(first.zone_id, zone_id);
    assert_eq!(first.holder, "h1#1");
    assert_eq!(
        ownership.acquire(zone_id, "h2#1", ttl).await,
        Err(OwnershipError::Held {
            holder: "h1#1".into()
        }),
        "a live claim is exclusive"
    );
    let again = ownership
        .acquire(zone_id, "h1#1", ttl)
        .await
        .expect("re-acquire");
    assert_eq!(
        again.token, first.token,
        "the holder gets its own claim back"
    );
    let renewed = ownership.renew(&first, ttl).await.expect("renew");
    assert_eq!(renewed.token, first.token, "renewal keeps the token");
    ownership.release(&first).await;
    assert_eq!(
        ownership.renew(&first, ttl).await,
        Err(OwnershipError::Lost),
        "a released claim is lost"
    );
    let second = ownership
        .acquire(zone_id, "h2#1", ttl)
        .await
        .expect("second acquire");
    assert!(second.token > first.token, "tokens strictly increase");
    assert_eq!(
        ownership.renew(&first, ttl).await,
        Err(OwnershipError::Lost),
        "an old claim stays lost"
    );
    ownership.release(&second).await;
    let third = ownership
        .acquire(zone_id, "h1#2", ttl)
        .await
        .expect("third acquire");
    assert!(third.token > second.token, "tokens survive release");
    ownership.release(&third).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block<T>(f: impl std::future::Future<Output = T>) -> T {
        // The local store never awaits anything; poll once.
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut f = std::pin::pin!(f);
        match f.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(v) => v,
            std::task::Poll::Pending => panic!("local ownership must not pend"),
        }
    }

    #[test]
    fn local_claims_are_exclusive_and_tokens_monotonic() {
        let own = LocalOwnership::new();
        let ttl = Duration::from_secs(1);
        let a = block(own.acquire("z", "h1#1", ttl)).unwrap();
        assert_eq!(
            block(own.acquire("z", "h2#1", ttl)),
            Err(OwnershipError::Held {
                holder: "h1#1".into()
            })
        );
        assert_eq!(block(own.acquire("z", "h1#1", ttl)).unwrap(), a);
        block(own.release(&a));
        assert_eq!(block(own.renew(&a, ttl)), Err(OwnershipError::Lost));
        let b = block(own.acquire("z", "h2#1", ttl)).unwrap();
        assert!(b.token > a.token);
    }

    #[test]
    fn local_ownership_passes_the_conformance_suite() {
        block(ownership_conformance(&LocalOwnership::new(), "conformance"));
    }

    #[test]
    fn local_tokens_outrank_a_previous_process() {
        let ttl = Duration::from_secs(1);
        let before = LocalOwnership::with_epoch(100);
        let old = block(before.acquire("z", "h#1", ttl)).unwrap();
        assert_eq!(old.token, 100);
        // A restarted process starts from a later epoch.
        let after = LocalOwnership::new();
        let new = block(after.acquire("z", "h#1", ttl)).unwrap();
        assert!(new.token > old.token);
        assert_eq!(new.token, after.epoch());
    }
}
