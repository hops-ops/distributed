//! Zone status read model: which zones are active (and on which holder),
//! which are asleep with a hibernation snapshot, and which never ran —
//! answered from SQL rows instead of opening one cell per zone.
//!
//! Projected from the outward events of the per-zone cells:
//!
//! | Source cell | Events | Row |
//! |---|---|---|
//! | [`ZoneLease`](super::zone_lease::ZoneLease) | `zone.activated`, `zone.released`, `zone.lease_expired` | [`ZoneLeases`] (`zone_leases`) |
//! | [`ZoneState`](super::zone_state::ZoneState) | `zone_state.saved`, `zone_state.deleted` | [`ZoneSnapshots`] (`zone_snapshots`) |
//!
//! Each event carries its cell's post-transition state, so a row is an
//! upsert of the body ([`ZONE_LEASES`], [`ZONE_SNAPSHOTS`]); the projector
//! runtime orders and deduplicates per stream. [`ZoneStatus::of`] combines
//! a zone's two rows. Mount [`projectors`] on a consumer service (any read
//! model store: SQLite, Postgres) fed by the bus the cells' outbox reaches.
//!
//! The read model is eventually consistent: it is for views (world maps,
//! minimaps, dashboards), never for decisions a zone server must fence —
//! ownership and restore always go to the cells.

use distributed::graphql::SurfaceProjector;
use distributed::projection::lower::{EventualOnly, ProjectionDescriptor};
use distributed::{
    mutation_file, LocalProjectionMountsBuilder, Mutation, ReadModel, ReadModelCatalog,
    RelationalReadModel,
};
use serde::{Deserialize, Serialize};

use super::zone_lease::{
    ZoneLeaseActivatedDomainEvent, ZoneLeaseLeaseExpiredDomainEvent, ZoneLeaseReleasedDomainEvent,
    STATUS_ACTIVE,
};
use super::zone_state::{ZoneStateDeletedDomainEvent, ZoneStateSavedDomainEvent};

/// Application of the read model (catalog and projector owner; the
/// consumer service is named after it).
pub const APPLICATION: &str = "zone-status";
/// Projector of [`ZoneLeases`].
pub const LEASES_PROJECTOR: &str = "project_zone_leases";
/// Projector of [`ZoneSnapshots`].
pub const SNAPSHOTS_PROJECTOR: &str = "project_zone_snapshots";
/// Epoch of [`ZONE_LEASES`] (bump to rebuild from the bus log).
pub const LEASES_EPOCH: &str = "zone-leases-v1";
/// Epoch of [`ZONE_SNAPSHOTS`].
pub const SNAPSHOTS_EPOCH: &str = "zone-snapshots-v1";

/// One `zone_leases` row: the last lease transition of a zone
/// ([`ZoneLeaseStatus`](super::zone_lease::ZoneLeaseStatus); the type is
/// named after its table, as `mutation_file!` requires).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ReadModel)]
#[table("zone_leases")]
pub struct ZoneLeases {
    /// Zone id.
    #[id("zone_id")]
    pub zone_id: String,
    /// Holder of the current (or last) claim.
    pub holder: String,
    /// Its fencing token.
    pub token: i64,
    /// `active`, `released` or `expired`.
    pub status: String,
}

/// One `zone_snapshots` row: the hibernation slot of a zone
/// ([`ZoneSnapshotStatus`](super::zone_state::ZoneSnapshotStatus)).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ReadModel)]
#[table("zone_snapshots")]
pub struct ZoneSnapshots {
    /// Zone id.
    #[id("zone_id")]
    pub zone_id: String,
    /// Highest token seen.
    pub token: i64,
    /// A snapshot is stored.
    pub stored: bool,
    /// Content version of the stored snapshot.
    pub content_version: String,
    /// Snapshot size in bytes.
    pub bytes: i64,
}

/// Upsert a [`ZoneLeases`] from a lease event's state body.
#[allow(non_snake_case)]
pub fn SaveZoneLease() -> Mutation<()> {
    mutation_file!("src/world/mutations/save_zone_lease.mutation.graphql")
}

/// Upsert a [`ZoneSnapshots`] from a zone-state event's state body.
#[allow(non_snake_case)]
pub fn SaveZoneSnapshot() -> Mutation<()> {
    mutation_file!("src/world/mutations/save_zone_snapshot.mutation.graphql")
}

distributed::projection! {
    pub const ZONE_LEASES: ProjectionDescriptor<EventualOnly> = {
        name: "project_zone_leases",
        version: 1,
        epoch: "zone-leases-v1",
        model: ZoneLeases,
        on {
            events: [
                ZoneLeaseActivatedDomainEvent,
                ZoneLeaseReleasedDomainEvent,
                ZoneLeaseLeaseExpiredDomainEvent,
            ],
            mutation: SaveZoneLease,
            input: { lease: body },
        },
    };
}

distributed::projection! {
    pub const ZONE_SNAPSHOTS: ProjectionDescriptor<EventualOnly> = {
        name: "project_zone_snapshots",
        version: 1,
        epoch: "zone-snapshots-v1",
        model: ZoneSnapshots,
        on {
            events: [ZoneStateSavedDomainEvent, ZoneStateDeletedDomainEvent],
            mutation: SaveZoneSnapshot,
            input: { snapshot: body },
        },
    };
}

/// Physical read-model catalog (`zone_leases`, `zone_snapshots`).
pub fn read_model_catalog() -> ReadModelCatalog {
    ReadModelCatalog::new(APPLICATION)
        .table_schema(ZoneLeases::schema().clone())
        .table_schema(ZoneSnapshots::schema().clone())
}

/// The modeled projector mounts `(leases, snapshots)`: mount both on one
/// consumer service named [`APPLICATION`], applying [`ZONE_LEASES`] and
/// [`ZONE_SNAPSHOTS`] respectively.
pub fn projectors() -> (SurfaceProjector, SurfaceProjector) {
    let mounts = LocalProjectionMountsBuilder::new(APPLICATION, "ordered-domain-events")
        .expect("projection source")
        .eventual_model::<ZoneLeases, _>(LEASES_PROJECTOR, ZONE_LEASES, LEASES_EPOCH)
        .expect("zone leases mount")
        .eventual_model::<ZoneSnapshots, _>(SNAPSHOTS_PROJECTOR, ZONE_SNAPSHOTS, SNAPSHOTS_EPOCH)
        .expect("zone snapshots mount")
        .build()
        .expect("projection catalog");
    (
        mounts
            .projector(LEASES_PROJECTOR)
            .expect("zone leases projector"),
        mounts
            .projector(SNAPSHOTS_PROJECTOR)
            .expect("zone snapshots projector"),
    )
}

/// What the read model knows about a zone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum ZoneStatus {
    /// A holder has the lease.
    Active {
        /// The lease holder.
        holder: String,
    },
    /// No holder; a hibernation snapshot is stored.
    Hibernated {
        /// Snapshot size in bytes.
        bytes: u64,
    },
    /// No holder and no snapshot: never ran, or ran without hibernating.
    Cold,
}

impl ZoneStatus {
    /// Combine a zone's rows (either may be missing).
    pub fn of(lease: Option<&ZoneLeases>, snapshot: Option<&ZoneSnapshots>) -> Self {
        if let Some(lease) = lease.filter(|lease| lease.status == STATUS_ACTIVE) {
            return Self::Active {
                holder: lease.holder.clone(),
            };
        }
        match snapshot.filter(|snapshot| snapshot.stored) {
            Some(snapshot) => Self::Hibernated {
                bytes: u64::try_from(snapshot.bytes).unwrap_or(0),
            },
            None => Self::Cold,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_prefers_an_active_lease_then_a_stored_snapshot() {
        let lease = |status: &str| ZoneLeases {
            zone_id: "z".into(),
            holder: "A#1".into(),
            token: 3,
            status: status.into(),
        };
        let snapshot = |stored| ZoneSnapshots {
            zone_id: "z".into(),
            token: 3,
            stored,
            content_version: "v".into(),
            bytes: if stored { 42 } else { 0 },
        };
        assert_eq!(
            ZoneStatus::of(Some(&lease("active")), Some(&snapshot(true))),
            ZoneStatus::Active {
                holder: "A#1".into()
            }
        );
        assert_eq!(
            ZoneStatus::of(Some(&lease("released")), Some(&snapshot(true))),
            ZoneStatus::Hibernated { bytes: 42 }
        );
        assert_eq!(
            ZoneStatus::of(Some(&lease("expired")), Some(&snapshot(false))),
            ZoneStatus::Cold
        );
        assert_eq!(ZoneStatus::of(None, None), ZoneStatus::Cold);
        // The projections compile against the cells' outward events.
        let (leases, snapshots) = projectors();
        drop((leases, snapshots));
        assert_eq!(read_model_catalog().tables.len(), 2);
    }
}
