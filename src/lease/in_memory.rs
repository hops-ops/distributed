#![expect(
    clippy::manual_async_fn,
    reason = "trait impls return impl Future + Send to preserve public Send bounds"
)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use crate::clock::{Clock, SystemClock};

use super::{validate_request, FencingToken, Lease, LeaseError, LeaseStore};

#[derive(Debug, Clone)]
struct Entry {
    holder: String,
    token: u64,
    /// `UNIX_EPOCH` once released (the entry, and its token, are retained).
    expires_at: SystemTime,
}

impl Entry {
    fn lease(&self, key: &str) -> Lease {
        Lease {
            key: key.to_string(),
            holder: self.holder.clone(),
            token: FencingToken::new(self.token),
            expires_at: self.expires_at,
        }
    }

    fn is_live(&self, now: SystemTime) -> bool {
        self.expires_at > now
    }

    fn held_by(&self, lease: &Lease, now: SystemTime) -> bool {
        self.is_live(now) && self.holder == lease.holder && self.token == lease.token.get()
    }
}

/// Process-local [`LeaseStore`]. Clones share state. Time comes from the
/// injected [`Clock`] — pass a [`ManualClock`](crate::clock::ManualClock) to
/// test expiry without sleeping.
#[derive(Clone)]
pub struct InMemoryLeaseStore<C = SystemClock> {
    clock: C,
    entries: Arc<Mutex<HashMap<String, Entry>>>,
}

impl InMemoryLeaseStore<SystemClock> {
    /// A store on the system clock.
    pub fn new() -> Self {
        Self::with_clock(SystemClock)
    }
}

impl Default for InMemoryLeaseStore<SystemClock> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: Clock> InMemoryLeaseStore<C> {
    /// A store on an explicit clock.
    pub fn with_clock(clock: C) -> Self {
        Self {
            clock,
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        // Mutations never run user code while holding the guard, so a poison
        // cannot leave an entry half-written; recover rather than fail.
        self.entries.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn deadline(now: SystemTime, ttl: Duration) -> Result<SystemTime, LeaseError> {
        now.checked_add(ttl)
            .ok_or_else(|| LeaseError::Invalid("lease ttl overflows the clock".into()))
    }
}

impl<C: Clock> LeaseStore for InMemoryLeaseStore<C> {
    fn acquire<'a>(
        &'a self,
        key: &'a str,
        holder: &'a str,
        ttl: Duration,
    ) -> impl Future<Output = Result<Lease, LeaseError>> + Send + 'a {
        async move {
            validate_request(key, holder, ttl)?;
            let now = self.clock.now();
            let expires_at = Self::deadline(now, ttl)?;
            let mut entries = self.entries();
            match entries.get_mut(key) {
                Some(entry) if entry.is_live(now) => {
                    if entry.holder == holder {
                        Ok(entry.lease(key))
                    } else {
                        Err(LeaseError::Held {
                            key: key.to_string(),
                            holder: entry.holder.clone(),
                            expires_at: entry.expires_at,
                        })
                    }
                }
                Some(entry) => {
                    entry.holder = holder.to_string();
                    entry.token += 1;
                    entry.expires_at = expires_at;
                    Ok(entry.lease(key))
                }
                None => {
                    let entry = Entry {
                        holder: holder.to_string(),
                        token: 1,
                        expires_at,
                    };
                    let lease = entry.lease(key);
                    entries.insert(key.to_string(), entry);
                    Ok(lease)
                }
            }
        }
    }

    fn renew<'a>(
        &'a self,
        lease: &'a Lease,
        ttl: Duration,
    ) -> impl Future<Output = Result<Lease, LeaseError>> + Send + 'a {
        async move {
            validate_request(&lease.key, &lease.holder, ttl)?;
            let now = self.clock.now();
            let expires_at = Self::deadline(now, ttl)?;
            let mut entries = self.entries();
            match entries.get_mut(&lease.key) {
                Some(entry) if entry.held_by(lease, now) => {
                    entry.expires_at = expires_at;
                    Ok(entry.lease(&lease.key))
                }
                _ => Err(LeaseError::Lost {
                    key: lease.key.clone(),
                }),
            }
        }
    }

    fn release<'a>(
        &'a self,
        lease: &'a Lease,
    ) -> impl Future<Output = Result<bool, LeaseError>> + Send + 'a {
        async move {
            let now = self.clock.now();
            let mut entries = self.entries();
            match entries.get_mut(&lease.key) {
                Some(entry) if entry.held_by(lease, now) => {
                    entry.expires_at = SystemTime::UNIX_EPOCH;
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
    }

    fn current<'a>(
        &'a self,
        key: &'a str,
    ) -> impl Future<Output = Result<Option<Lease>, LeaseError>> + Send + 'a {
        async move {
            let now = self.clock.now();
            Ok(self
                .entries()
                .get(key)
                .filter(|entry| entry.is_live(now))
                .map(|entry| entry.lease(key)))
        }
    }
}
