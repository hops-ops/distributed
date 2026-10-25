//! World structure: where zones live, how they nest, how their durable state
//! survives hibernation and how ownership is fenced.
//!
//! - [`directory`]: [`ZoneDirectory`] maps a zone id to its owning host
//!   ([`StaticDirectory`], consistent-hash [`HashRing`]).
//! - [`ownership`]: [`ZoneOwnership`] fenced claims so two hosts never run
//!   the same zone ([`LocalOwnership`]; `FencedLeaseOwnership` over
//!   `distributed::lease` with the `lease` feature).
//! - [`state`]: [`ZoneStateStore`] for hibernated zone state
//!   ([`MemoryZoneStateStore`] for unit tests; on celld,
//!   `celld::CellZoneStateStore` over [`zone_state`] cells).
//! - [`topology`]: [`ZoneTopology`] parent links (province > city > borough
//!   > neighborhood) and [`ChildEvent`] tier-up propagation.
//! - [`allocation`]: [`ZoneAllocation`] records written by the service that
//!   allocates a zone, read before the zone is created
//!   ([`ZoneActivation`], [`ZoneAllocationStore`]).
//! - `zone_activity` (feature `cell`): a celld-hosted zone's own lifecycle
//!   reports (active / dormant / hibernated) as a cell.
//! - `zone_status` (feature `cell`): the read model of every zone's lease,
//!   hibernation slot and activity, projected from the `zone_lease` /
//!   `zone_state` / `zone_activity` cells' outward events (active / dormant
//!   / hibernated / cold without opening a cell).
//! - `content_rollout` (feature `cell`): a world's live content rollout
//!   (freeze → apply → resume across every celld-hosted zone) as a cell.
//! - [`watch`]: watched world views — a zone keeps pushed read models of
//!   the world cells it cares about ([`watch::Watches`]); cells keep
//!   in-memory watcher leases ([`watch::Watchers`]) and push each new
//!   version; a slow reconcile read repairs lost pushes.
//! - [`barrier`]: content-version policy types shared by the host and the
//!   world barrier ([`StalePolicy`], [`BarrierMode`]).

pub mod allocation;
pub mod barrier;
#[cfg(feature = "cell")]
#[allow(missing_docs)]
pub mod content_rollout;
pub mod directory;
pub mod ownership;
pub mod state;
pub mod topology;
pub mod watch;
#[cfg(feature = "cell")]
#[allow(missing_docs)]
pub mod zone_activity;
#[cfg(feature = "cell")]
// The `sourced` and `Snapshot` derives generate undocumented public items.
#[allow(missing_docs)]
pub mod zone_lease;
#[cfg(feature = "cell")]
#[allow(missing_docs)]
pub mod zone_state;
#[cfg(feature = "cell")]
#[allow(missing_docs)]
pub mod zone_status;

pub use crate::facts::BoxFuture;
pub use allocation::{
    AllocationError, MemoryZoneAllocationStore, ZoneActivation, ZoneAllocation,
    ZoneAllocationStore, ZoneCreateError,
};
pub use barrier::{BarrierMode, FreezeState, StalePolicy};
pub use directory::{HashRing, HostAddr, StaticDirectory, ZoneDirectory};
#[doc(hidden)]
pub use ownership::ownership_conformance;
#[cfg(feature = "lease")]
pub use ownership::FencedLeaseOwnership;
pub use ownership::{LocalOwnership, OwnershipError, ZoneClaim, ZoneOwnership};
#[doc(hidden)]
pub use state::state_store_conformance;
pub use state::{MemoryZoneStateStore, StateError, StoredZoneState, ZoneStateStore};
pub use topology::{ChildEvent, DottedTopology, FlatTopology, ZoneTopology};
