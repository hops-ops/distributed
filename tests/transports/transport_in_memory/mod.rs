//! The bus contract over `InMemoryBus`, with no external service.

use common::transport::bus::{BusCapabilities, BusHarness, KindCapabilities};
use distributed::bus::InMemoryBus;

/// One queue for commands and one log for events, both in publish order. A
/// nacked command goes back to the head of the queue; the queue keeps every
/// send, the log stores a stable id once.
pub(crate) const IN_MEMORY: BusCapabilities = BusCapabilities {
    commands: KindCapabilities {
        dedupes_ids: false,
        ..KindCapabilities::LOG
    },
    events: KindCapabilities::LOG,
    rebalance_may_redeliver: false,
    added_metadata: &[],
};

/// Every bus is a clone of one in-memory bus.
struct InMemory(InMemoryBus);

impl BusHarness for InMemory {
    type Bus = InMemoryBus;

    const CAPABILITIES: BusCapabilities = IN_MEMORY;

    async fn start() -> Option<Self> {
        Some(Self(InMemoryBus::new()))
    }

    async fn bus(&self) -> InMemoryBus {
        self.0.clone()
    }
}

common::bus_conformance_tests!(InMemory);
