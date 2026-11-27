//! Transport conformance: the bus contract every `Bus + BusConsumer` runs.
//!
//! The runner and outbox dispatcher contracts that need no transport are unit
//! tests next to their code (`src/bus/runner`, `src/outbox_worker`).

pub mod bus;
