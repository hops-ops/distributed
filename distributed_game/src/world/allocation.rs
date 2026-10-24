//! Zone allocation: what a zone is for, decided before it starts.
//!
//! A service that allocates a zone (game-services `Match`, a party
//! finder, an instance spawner) writes a [`ZoneAllocation`] record for the
//! zone id: roster, mode, config version, seed. When the zone activates,
//! the server loads the record and hands it to the async zone factory in a
//! [`ZoneActivation`] together with the activation's fencing token, so the
//! zone's `on_create` sees it before the first player joins. Joins that
//! arrive while the zone is being created wait; the tick never awaits.

use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::facts::BoxFuture;

/// The record an allocating service writes for a zone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneAllocation {
    /// Who allocated it (service name, e.g. `"game-services.match"`).
    pub allocated_by: String,
    /// Monotonic revision per zone id (a re-allocation bumps it).
    pub revision: u64,
    /// Game-defined payload (roster, mode, config version…).
    pub payload: Value,
}

impl ZoneAllocation {
    /// Record with a typed payload.
    pub fn new(
        allocated_by: impl Into<String>,
        revision: u64,
        payload: &impl Serialize,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self {
            allocated_by: allocated_by.into(),
            revision,
            payload: serde_json::to_value(payload)?,
        })
    }

    /// Decode the payload.
    pub fn payload_as<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.payload.clone())
    }
}

/// Why an allocation store failed (the activation is refused with
/// `ZONE_UNAVAILABLE`, never admitted half-configured).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationError(pub String);

impl fmt::Display for AllocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "zone allocation store: {}", self.0)
    }
}

impl std::error::Error for AllocationError {}

/// Where allocation records live. Async: typically a service's database or
/// read model, read once per activation.
pub trait ZoneAllocationStore: Send + Sync + 'static {
    /// The record for `zone_id`, if any.
    fn load<'a>(
        &'a self,
        zone_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<ZoneAllocation>, AllocationError>>;

    /// Write a record. Refused when a record with a higher revision exists.
    fn put<'a>(
        &'a self,
        zone_id: &'a str,
        allocation: ZoneAllocation,
    ) -> BoxFuture<'a, Result<(), AllocationError>>;

    /// Drop the record (the zone was released).
    fn remove<'a>(&'a self, zone_id: &'a str) -> BoxFuture<'a, Result<(), AllocationError>>;
}

/// In-memory allocation store (tests, single process).
#[derive(Debug, Default)]
pub struct MemoryZoneAllocationStore {
    records: Mutex<HashMap<String, ZoneAllocation>>,
}

impl MemoryZoneAllocationStore {
    /// Empty.
    pub fn new() -> Self {
        Self::default()
    }
}

impl ZoneAllocationStore for MemoryZoneAllocationStore {
    fn load<'a>(
        &'a self,
        zone_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<ZoneAllocation>, AllocationError>> {
        let found = self.records.lock().expect("records").get(zone_id).cloned();
        Box::pin(async move { Ok(found) })
    }

    fn put<'a>(
        &'a self,
        zone_id: &'a str,
        allocation: ZoneAllocation,
    ) -> BoxFuture<'a, Result<(), AllocationError>> {
        let mut records = self.records.lock().expect("records");
        let result = match records.get(zone_id) {
            Some(current) if current.revision > allocation.revision => {
                Err(AllocationError(format!(
                    "revision {} is older than the stored {}",
                    allocation.revision, current.revision
                )))
            }
            _ => {
                records.insert(zone_id.to_string(), allocation);
                Ok(())
            }
        };
        Box::pin(async move { result })
    }

    fn remove<'a>(&'a self, zone_id: &'a str) -> BoxFuture<'a, Result<(), AllocationError>> {
        self.records.lock().expect("records").remove(zone_id);
        Box::pin(async { Ok(()) })
    }
}

/// What the zone factory is told about the activation it configures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneActivation {
    /// Zone id.
    pub zone_id: String,
    /// Activation epoch: the ownership fencing token, also the host's
    /// `boot_epoch` and every fact stamp's `activation`.
    pub activation: u64,
    /// The allocation record, when an allocation store is configured and
    /// holds one for this zone.
    pub allocation: Option<ZoneAllocation>,
}

/// The factory refused to create the zone because something it needs
/// failed (storage down, invalid allocation). Clients waiting to join get
/// `Error(ZONE_UNAVAILABLE)` with the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneCreateError(pub String);

impl ZoneCreateError {
    /// Error with a message for the joining clients.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ZoneCreateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ZoneCreateError {}

impl From<AllocationError> for ZoneCreateError {
    fn from(error: AllocationError) -> Self {
        Self(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block<T>(f: impl std::future::Future<Output = T>) -> T {
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut f = std::pin::pin!(f);
        loop {
            if let std::task::Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }

    #[test]
    fn memory_store_keeps_the_newest_revision() {
        let store = MemoryZoneAllocationStore::new();
        let record = |revision| {
            ZoneAllocation::new("match", revision, &serde_json::json!({"mode": "duel"})).unwrap()
        };
        block(store.put("m1", record(2))).unwrap();
        assert!(block(store.put("m1", record(1))).is_err());
        let loaded = block(store.load("m1")).unwrap().unwrap();
        assert_eq!(loaded.revision, 2);
        let payload: serde_json::Value = loaded.payload_as().unwrap();
        assert_eq!(payload["mode"], "duel");
        block(store.remove("m1")).unwrap();
        assert_eq!(block(store.load("m1")).unwrap(), None);
    }
}
