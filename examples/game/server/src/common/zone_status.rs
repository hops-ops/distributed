//! A host's view of `distributed_game::world::zone_status`: the zone lease
//! and zone-state cells' outward events (cell outbox → celld Queue →
//! [`super::relay`] → projection bus) projected into a SQLite read model of
//! this host, under a bus consumer group of its own.
//!
//! This is the consumer of `zone.activated` / `zone.released` /
//! `zone.lease_expired` and `zone_state.saved` / `zone_state.deleted`; the
//! grid's world map reads it to tell active, hibernated and cold zones apart
//! without opening one cell per zone (a read of a never-used cell would
//! create it). Eventually consistent: for views only.

use std::collections::HashMap;

use distributed::bus::SqliteBus;
use distributed::microsvc::{
    spawn_service_consumer_loop, CausalProjectorContext, HandlerError, ModeledProjection, Routes,
    Service, CONSUMER_IDLE_POLL,
};
use distributed::SqliteRepository;
use distributed_game::world::zone_status::{
    projectors, read_model_catalog, ZoneLeases, ZoneSnapshots, ZoneStatus, APPLICATION,
    ZONE_LEASES, ZONE_SNAPSHOTS,
};

/// See the module docs.
#[derive(Clone)]
pub struct ZoneStatusView {
    repo: SqliteRepository,
}

async fn project_leases(
    context: CausalProjectorContext,
    projection: ModeledProjection,
) -> Result<(), HandlerError> {
    projection.apply(ZONE_LEASES, &context).await
}

async fn project_snapshots(
    context: CausalProjectorContext,
    projection: ModeledProjection,
) -> Result<(), HandlerError> {
    projection.apply(ZONE_SNAPSHOTS, &context).await
}

/// `{select} WHERE zone_id IN (?, ?, …)` over `zones`.
fn by_zone(select: &str, zones: &[String]) -> sqlx::QueryBuilder<sqlx::Sqlite> {
    let mut query = sqlx::QueryBuilder::new(select);
    query.push(" WHERE zone_id IN (");
    let mut separated = query.separated(", ");
    for zone in zones {
        separated.push_bind(zone);
    }
    query.push(")");
    query
}

impl ZoneStatusView {
    /// Open (creating) the read model at `url` and project `bus` into it
    /// forever as consumer group `group` (the cursor lives in the read
    /// model, so a restart resumes).
    pub async fn open(url: &str, bus: sqlx::SqlitePool, group: &str) -> Result<Self, String> {
        let repo = SqliteRepository::connect_and_migrate(url)
            .await
            .map_err(|e| format!("zone status {url}: {e}"))?;
        let registry = read_model_catalog()
            .table_registry()
            .map_err(|e| format!("zone status tables: {e}"))?;
        repo.bootstrap_table_schema_for_dev(&registry)
            .await
            .map_err(|e| format!("zone status tables: {e}"))?;
        let read_models = repo.clone();
        let group = group.to_string();
        spawn_service_consumer_loop(move || {
            let (leases, snapshots) = projectors();
            Service::new()
                .named(APPLICATION)
                .routes(
                    Routes::new()
                        .with_read_model_store(read_models.clone())
                        .modeled_projector(leases)
                        .handle(project_leases)
                        .modeled_projector(snapshots)
                        .handle(project_snapshots),
                )
                .with_bus(
                    SqliteBus::new_with_group(bus.clone(), group.clone())
                        .with_idle_poll(CONSUMER_IDLE_POLL),
                )
        });
        Ok(Self { repo })
    }

    /// The status of each of `zones` (`Cold` when the read model has no
    /// row).
    pub async fn statuses(&self, zones: &[String]) -> Result<HashMap<String, ZoneStatus>, String> {
        let mut leases: HashMap<String, ZoneLeases> = HashMap::new();
        let mut snapshots: HashMap<String, ZoneSnapshots> = HashMap::new();
        for chunk in zones.chunks(256) {
            let mut query = by_zone(
                "SELECT zone_id, holder, token, status FROM zone_leases",
                chunk,
            );
            for (zone_id, holder, token, status) in query
                .build_query_as::<(String, String, i64, String)>()
                .fetch_all(self.repo.pool())
                .await
                .map_err(|e| format!("zone status: {e}"))?
            {
                leases.insert(
                    zone_id.clone(),
                    ZoneLeases {
                        zone_id,
                        holder,
                        token,
                        status,
                    },
                );
            }
            let mut query = by_zone(
                "SELECT zone_id, token, stored, content_version, bytes FROM zone_snapshots",
                chunk,
            );
            for (zone_id, token, stored, content_version, bytes) in query
                .build_query_as::<(String, i64, bool, String, i64)>()
                .fetch_all(self.repo.pool())
                .await
                .map_err(|e| format!("zone status: {e}"))?
            {
                snapshots.insert(
                    zone_id.clone(),
                    ZoneSnapshots {
                        zone_id,
                        token,
                        stored,
                        content_version,
                        bytes,
                    },
                );
            }
        }
        Ok(zones
            .iter()
            .map(|zone| {
                (
                    zone.clone(),
                    ZoneStatus::of(leases.get(zone), snapshots.get(zone)),
                )
            })
            .collect())
    }
}
