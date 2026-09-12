//! Explicit, bounded maintenance for full-state snapshot projections.
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::graphql::SurfaceProjector;
use crate::projection::lower::ProjectionServerExecutorDescriptor;
use crate::projection_protocol::*;
use crate::table::{TableMutation, TableWritePlan};
use crate::DomainEventOccurrence;

pub(crate) const MAX_REBUILD_RECORDS: usize = 10_000;
const MAX_HISTORY_EVENTS: usize = 100_000;
const MAX_HISTORY_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn invalid(detail: impl ToString) -> ProjectionProtocolError {
    ProjectionProtocolError::InvalidBatch(detail.to_string())
}

/// Captured maintenance boundary for one registered snapshot projector.
///
/// Stop producers and drain their outboxes before beginning. Read the complete,
/// retained canonical history *after* beginning, then call
/// `from_complete_history`. A broker retention boundary is not proof of
/// aggregate-history completeness. This API never runs event handlers, changes
/// broker checkpoints, or republishes messages.
///
/// Concurrent row changes (including new records) invalidate the captured
/// boundary. Applying a plan is one adapter transaction; failures leave the
/// projection untouched. This bounded API is not an online shadow rebuild or
/// a schema/topology migration.
pub struct SnapshotProjectionRebuild {
    pub(crate) context: RebuildContext,
    executor: ProjectionServerExecutorDescriptor,
    pub(crate) expected: Vec<ProjectionRecordMetadata>,
}

#[derive(Clone, Debug)]
pub(crate) struct RebuildContext {
    pub(crate) compiled: CompiledProjectionTopology,
    pub(crate) epoch: ProjectionEpoch,
}

impl RebuildContext {
    pub(crate) fn partition(&self) -> Result<ProjectionPartition, ProjectionProtocolError> {
        self.compiled
            .codec()
            .encode_partition(None)
            .map_err(invalid)
    }
}

/// Opaque, validated replacement plan. It contains no transport input authority.
pub struct SnapshotProjectionRebuildPlan {
    pub(crate) context: RebuildContext,
    pub(crate) expected: Vec<ProjectionRecordMetadata>,
    pub(crate) rows: Vec<RebuildRow>,
}

#[derive(Clone, Debug)]
pub(crate) struct RebuildRow {
    pub(crate) scope: ProjectionRecordScope,
    pub(crate) mutation: TableMutation,
    pub(crate) source: SourceSnapshotVersion,
}

impl RebuildRow {
    pub(crate) fn transition(
        &self,
        current: Option<&ProjectionRecordMetadata>,
    ) -> (ProjectionMutationKind, ProjectionRecordExpectation) {
        let kind = match (&self.mutation, current) {
            (TableMutation::DeleteRow(_), _) => ProjectionMutationKind::Delete,
            (_, Some(record)) if record.tombstone => ProjectionMutationKind::Recreate,
            _ => ProjectionMutationKind::Upsert,
        };
        let expectation = current
            .map(|r| ProjectionRecordExpectation::Exact(r.revision.clone()))
            .unwrap_or(ProjectionRecordExpectation::Missing);
        (kind, expectation)
    }

    pub(crate) fn verify_physical(
        &self,
        current: Option<&ProjectionRecordMetadata>,
        exists: bool,
    ) -> Result<(), ProjectionProtocolError> {
        if exists != current.is_some_and(|r| !r.tombstone) {
            return Err(invalid(
                "snapshot rebuild found inconsistent row/protocol metadata",
            ));
        }
        Ok(())
    }
}

#[allow(private_bounds)]
impl SnapshotProjectionRebuild {
    /// Capture the current record inventory before reading the source archive.
    pub async fn begin(
        store: &impl ProjectionProtocolStore,
        projector: &SurfaceProjector,
    ) -> Result<Self, ProjectionProtocolError> {
        let [projection] = projector.modeled.as_slice() else {
            return Err(invalid(
                "snapshot rebuild requires exactly one modeled binding",
            ));
        };
        if !projection.is_causally_eligible()
            || !matches!(
                projection.route(),
                crate::projection::placement::ProjectionExecutorRoute::Local { .. }
            )
        {
            return Err(invalid(
                "snapshot rebuild requires an active local eventual binding",
            ));
        }
        let (program, binding) = projection
            .raw()
            .ok_or_else(|| invalid("snapshot rebuild requires a generated binding"))?;
        if !program.source_snapshots() {
            return Err(invalid(
                "snapshot rebuild requires source: aggregate_snapshot",
            ));
        }
        let physical = binding
            .physical_topology()
            .ok_or_else(|| invalid("snapshot rebuild requires a physical topology"))?;
        let topology =
            ProjectorTopologyId::new(physical.version(), physical.name(), physical.digest())?;
        let compiled = CompiledProjectionTopology::from_modeled_binding(
            topology,
            binding
                .outputs()
                .iter()
                .map(|o| (o.model(), o.storage(), o.schema())),
        )?;
        let executor = projection
            .server_executor()
            .cloned()
            .ok_or_else(|| invalid("snapshot rebuild requires a generated executor"))?;
        let context = RebuildContext {
            compiled,
            epoch: ProjectionEpoch::new(projection.epoch().as_str())?,
        };
        let expected = store.projection_rebuild_records(&context).await?;
        Ok(Self {
            context,
            executor,
            expected,
        })
    }

    /// Resolve retained canonical history through the original typed projection.
    ///
    /// The caller must supply the entire publication history through the
    /// quiescent source head, not a filtered consumer window. This method checks
    /// covered record identities, sequence prefixes and conflicting duplicates;
    /// it cannot discover unpublished or externally deleted source history.
    pub fn from_complete_history(
        self,
        history: &[DomainEventOccurrence],
    ) -> Result<SnapshotProjectionRebuildPlan, ProjectionProtocolError> {
        if history.len() > MAX_HISTORY_EVENTS {
            return Err(invalid(
                "snapshot rebuild history exceeds 100000 occurrences",
            ));
        }
        let mut bytes = 0usize;
        let mut identities = BTreeMap::new();
        let mut sequences: BTreeMap<(String, String), BTreeSet<u64>> = BTreeMap::new();
        let mut rows: HashMap<ProjectionRecordScope, RebuildRow> = HashMap::new();
        let mut relevant = BTreeSet::new();
        let mut versions: HashMap<ProjectionRecordScope, Vec<SourceSnapshotVersion>> =
            HashMap::new();
        for event in history {
            let canonical = event.canonical_bytes().map_err(invalid)?;
            bytes = bytes
                .checked_add(canonical.len())
                .ok_or_else(|| invalid("history size overflow"))?;
            if bytes > MAX_HISTORY_BYTES {
                return Err(invalid("snapshot rebuild history exceeds 64 MiB"));
            }
            let stream = (
                event.aggregate_type().to_owned(),
                event.aggregate_id().to_owned(),
            );
            sequences
                .entry(stream.clone())
                .or_default()
                .insert(event.aggregate_sequence());
            let identity = (
                stream.clone(),
                event.aggregate_sequence(),
                event.publication_ordinal(),
            );
            if let Some(previous) = identities.insert(identity, canonical.clone()) {
                if previous != canonical {
                    return Err(invalid(
                        "snapshot rebuild history contains conflicting occurrences",
                    ));
                }
                continue;
            }
            if !self.executor.matches(event) {
                continue;
            }
            relevant.insert(stream);
            let lowered = self.executor.plan(event).map_err(invalid)?;
            if !lowered.resolved.source_snapshots() {
                return Err(invalid("snapshot rebuild resolved a non-snapshot program"));
            }
            lowered.write_plan.validate()?;
            let source = SourceSnapshotVersion::from_occurrence(event)?;
            for mut mutation in lowered.write_plan.mutations {
                // Protocol inventory CAS, not a domain version column, fences maintenance.
                match &mut mutation {
                    TableMutation::UpsertRow(row) => {
                        row.expected_version = crate::table::ExpectedVersion::Any;
                        row.mode = crate::table::RowWriteMode::Upsert;
                    }
                    TableMutation::DeleteRow(row) => {
                        row.expected_version = crate::table::ExpectedVersion::Any
                    }
                    _ => {}
                }
                let (schema, key) = match &mutation {
                    TableMutation::UpsertRow(row) => (row.schema, &row.key),
                    TableMutation::DeleteRow(row) => (row.schema, &row.key),
                    TableMutation::PatchRow(_) => {
                        return Err(invalid("snapshot rebuild cannot apply patches"))
                    }
                };
                let scope = self
                    .context
                    .compiled
                    .codec()
                    .encode_row_scope_in_partition(
                        &schema.model_name,
                        self.context.partition()?,
                        key,
                    )
                    .map_err(invalid)?;
                versions
                    .entry(scope.clone())
                    .or_default()
                    .push(source.clone());
                if let Some(previous) = rows.get(&scope) {
                    if !source.advances(&previous.source)? {
                        continue;
                    }
                }
                rows.insert(
                    scope.clone(),
                    RebuildRow {
                        scope,
                        mutation,
                        source: source.clone(),
                    },
                );
                if rows.len() > MAX_REBUILD_RECORDS {
                    return Err(invalid("snapshot rebuild exceeds 10000 records"));
                }
            }
        }
        for stream in relevant {
            let sequence = &sequences[&stream];
            if sequence
                .iter()
                .copied()
                .ne(1..=sequence.last().copied().unwrap_or(0))
            {
                return Err(invalid(
                    "snapshot rebuild requires a complete aggregate sequence prefix",
                ));
            }
        }
        for current in &self.expected {
            let scope = current.revision.scope();
            let row = rows.get(scope).ok_or_else(|| {
                invalid(format!(
                    "snapshot rebuild history does not cover an existing {} record",
                    scope.model(),
                ))
            })?;
            if let Some(source) = &current.source_snapshot {
                if !versions[scope].contains(source) {
                    return Err(invalid(
                        "snapshot rebuild history omits the stored source occurrence",
                    ));
                }
                // Also validates same-stream ownership and equal-version conflicts.
                if source.advances(&row.source)? {
                    return Err(invalid(
                        "snapshot rebuild history ends before the stored source version",
                    ));
                }
            }
        }
        let mut rows: Vec<_> = rows.into_values().collect();
        rows.sort_by(|a, b| {
            (a.scope.model(), a.scope.canonical_key_bytes())
                .cmp(&(b.scope.model(), b.scope.canonical_key_bytes()))
        });
        Ok(SnapshotProjectionRebuildPlan {
            context: self.context,
            expected: self.expected,
            rows,
        })
    }
}

#[allow(private_bounds)]
impl SnapshotProjectionRebuildPlan {
    /// Number of complete rows/tombstones represented by this plan.
    pub fn record_count(&self) -> usize {
        self.rows.len()
    }

    /// Atomically apply if the captured inventory has not changed.
    pub async fn apply(
        self,
        store: &impl ProjectionProtocolStore,
    ) -> Result<usize, ProjectionProtocolError> {
        store.commit_projection_rebuild(self).await
    }

    pub(crate) fn verify_inventory(
        &self,
        current: &[ProjectionRecordMetadata],
    ) -> Result<(), ProjectionProtocolError> {
        let map = |rows: &[ProjectionRecordMetadata]| {
            rows.iter()
                .map(|r| (r.revision.scope().clone(), r.clone()))
                .collect::<HashMap<_, _>>()
        };
        if map(current) != map(&self.expected) {
            return Err(invalid(
                "projection changed during rebuild; begin again with fresh history",
            ));
        }
        Ok(())
    }

    pub(crate) fn write_plan(&self) -> TableWritePlan {
        TableWritePlan::new(self.rows.iter().map(|row| row.mutation.clone()).collect())
    }
}
