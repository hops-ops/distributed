//! World structure: where zones live, how they nest, how their durable state
//! survives hibernation and how ownership is fenced.
//!
//! - [`directory`]: [`ZoneDirectory`] maps a zone id to its owning host
//!   ([`StaticDirectory`], consistent-hash [`HashRing`]).
//! - [`ownership`]: [`ZoneOwnership`] fenced claims so two hosts never run
//!   the same zone ([`LocalOwnership`]; `FencedLeaseOwnership` over
//!   `distributed::lease` with the `lease` feature).
//! - [`state`]: [`ZoneStateStore`] for hibernated zone state
//!   ([`MemoryZoneStateStore`]; `SqliteZoneStateStore` with `sqlite`).
//! - [`topology`]: [`ZoneTopology`] parent links (province > city > borough
//!   > neighborhood) and [`ChildEvent`] tier-up propagation.
//! - [`barrier`]: content-version policy types shared by the host and the
//!   world barrier ([`StalePolicy`], [`BarrierMode`]).

pub mod barrier;
pub mod directory;
pub mod ownership;
pub mod state;
pub mod topology;

pub use barrier::{BarrierMode, FreezeState, StalePolicy};
pub use directory::{HashRing, HostAddr, StaticDirectory, ZoneDirectory};
#[cfg(feature = "lease")]
pub use ownership::FencedLeaseOwnership;
pub use ownership::{LocalOwnership, OwnershipError, ZoneClaim, ZoneOwnership};
#[cfg(feature = "sqlite")]
pub use state::SqliteZoneStateStore;
pub use state::{MemoryZoneStateStore, StateError, StoredZoneState, ZoneStateStore};
pub use topology::{ChildEvent, DottedTopology, FlatTopology, ZoneTopology};
