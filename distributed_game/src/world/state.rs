//! Durable zone state for hibernation.
//!
//! When a zone hibernates, the host asks it for a snapshot
//! ([`Zone::hibernate`](crate::Zone::hibernate)) and stores it here under
//! the zone's ownership token; the next activation (on any host) loads it
//! and hands it to [`Zone::on_restore`](crate::Zone::on_restore).
//!
//! Writes are **fenced**: `save` (and `delete`) with a token older than the
//! stored one fails with [`StateError::Stale`], so a host that lost
//! ownership while paused can never overwrite the state written by its
//! successor. An equal token is accepted (the same owner saving again).
//!
//! Durable snapshots live in celld: one `ZoneState:{zone_id}` cell per zone
//! ([`super::zone_state`], `celld::CellZoneStateStore`), so a sleeping zone
//! is a sleeping Durable Object. [`MemoryZoneStateStore`] is the in-process
//! store for unit tests. There is no native file store.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::facts::BoxFuture;

/// A stored snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredZoneState {
    /// Ownership token it was written under.
    pub token: u64,
    /// Content version the zone was running.
    pub content_version: String,
    /// Opaque zone bytes.
    pub state: Vec<u8>,
}

/// Store failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateError {
    /// Write fenced off: a newer owner already saved.
    Stale {
        /// Presented token.
        presented: u64,
        /// Stored token.
        current: u64,
    },
    /// Backend failure.
    Backend(String),
}

impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stale { presented, current } => {
                write!(f, "stale zone-state token {presented} (current {current})")
            }
            Self::Backend(e) => write!(f, "zone-state backend: {e}"),
        }
    }
}

impl std::error::Error for StateError {}

/// Fenced snapshot store.
pub trait ZoneStateStore: Send + Sync + 'static {
    /// Latest snapshot of `zone_id` (`None` when none, or deleted).
    fn load<'a>(
        &'a self,
        zone_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredZoneState>, StateError>>;

    /// Store a snapshot if `state.token` >= the stored token.
    fn save<'a>(
        &'a self,
        zone_id: &'a str,
        state: &'a StoredZoneState,
    ) -> BoxFuture<'a, Result<(), StateError>>;

    /// Drop the snapshot if `token` >= the stored token. The token stays
    /// (a later save under an older token is still stale).
    fn delete<'a>(&'a self, zone_id: &'a str, token: u64) -> BoxFuture<'a, Result<(), StateError>>;
}

/// In-memory store (unit tests / single process).
#[derive(Debug, Default)]
pub struct MemoryZoneStateStore {
    /// zone -> (highest token seen, snapshot).
    states: Mutex<HashMap<String, (u64, Option<StoredZoneState>)>>,
}

impl MemoryZoneStateStore {
    /// Empty.
    pub fn new() -> Self {
        Self::default()
    }

    fn fenced(
        &self,
        zone_id: &str,
        token: u64,
        value: Option<StoredZoneState>,
    ) -> Result<(), StateError> {
        let mut states = self.states.lock().expect("states");
        let entry = states.entry(zone_id.to_string()).or_insert((0, None));
        if entry.0 > token {
            return Err(StateError::Stale {
                presented: token,
                current: entry.0,
            });
        }
        *entry = (token, value);
        Ok(())
    }
}

impl ZoneStateStore for MemoryZoneStateStore {
    fn load<'a>(
        &'a self,
        zone_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredZoneState>, StateError>> {
        let state = self
            .states
            .lock()
            .expect("states")
            .get(zone_id)
            .and_then(|(_, state)| state.clone());
        Box::pin(async move { Ok(state) })
    }

    fn save<'a>(
        &'a self,
        zone_id: &'a str,
        state: &'a StoredZoneState,
    ) -> BoxFuture<'a, Result<(), StateError>> {
        let result = self.fenced(zone_id, state.token, Some(state.clone()));
        Box::pin(async move { result })
    }

    fn delete<'a>(&'a self, zone_id: &'a str, token: u64) -> BoxFuture<'a, Result<(), StateError>> {
        let result = self.fenced(zone_id, token, None);
        Box::pin(async move { result })
    }
}

/// The fencing contract every [`ZoneStateStore`] must meet (memory, celld).
#[doc(hidden)]
pub async fn state_store_conformance(store: &dyn ZoneStateStore, zone: &str, payload: Vec<u8>) {
    let snap = |token: u64, bytes: Vec<u8>| StoredZoneState {
        token,
        content_version: "c1".into(),
        state: bytes,
    };
    assert_eq!(store.load(zone).await.unwrap(), None);
    store.save(zone, &snap(2, b"a".to_vec())).await.unwrap();
    store.save(zone, &snap(2, payload.clone())).await.unwrap();
    assert_eq!(
        store.load(zone).await.unwrap(),
        Some(snap(2, payload.clone()))
    );
    assert_eq!(
        store.save(zone, &snap(1, b"old".to_vec())).await,
        Err(StateError::Stale {
            presented: 1,
            current: 2
        })
    );
    assert_eq!(
        store.delete(zone, 1).await,
        Err(StateError::Stale {
            presented: 1,
            current: 2
        })
    );
    store.save(zone, &snap(3, b"c".to_vec())).await.unwrap();
    assert_eq!(
        store.load(zone).await.unwrap(),
        Some(snap(3, b"c".to_vec()))
    );
    store.save(zone, &snap(4, payload.clone())).await.unwrap();
    store.delete(zone, 4).await.unwrap();
    assert_eq!(store.load(zone).await.unwrap(), None);
    assert_eq!(
        store.save(zone, &snap(3, b"late".to_vec())).await,
        Err(StateError::Stale {
            presented: 3,
            current: 4
        }),
        "the token survives a delete"
    );
    store.save(zone, &snap(5, payload.clone())).await.unwrap();
    assert_eq!(store.load(zone).await.unwrap(), Some(snap(5, payload)));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        use std::task::{Context, Poll, Waker};
        let mut future = std::pin::pin!(future);
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("memory store futures are ready"),
        }
    }

    #[test]
    fn memory_store_is_fenced() {
        let store = MemoryZoneStateStore::new();
        block_on(state_store_conformance(&store, "z", vec![7; 300_000]));
    }
}
