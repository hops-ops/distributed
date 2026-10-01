//! Explicit, bounded maintenance for full-state snapshot projections.
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::graphql::SurfaceProjector;
use crate::projection::lower::ProjectionServerExecutorDescriptor;
use crate::projection_protocol::*;
use crate::table::{TableMutation, TableWritePlan};
use crate::DomainEventOccurrence;
use crate::{repository::StreamIdentity, EventRecord};
use sha2::{Digest, Sha256};

pub(crate) const MAX_REBUILD_RECORDS: usize = 10_000;
const MAX_HISTORY_EVENTS: usize = 100_000;
const MAX_HISTORY_BYTES: usize = 64 * 1024 * 1024;

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum HistoryIdentity {
    Aggregate(String, String, u64, u32),
    External(String, String, u64, String),
    Derived(String),
}

fn history_identity(event: &DomainEventOccurrence) -> HistoryIdentity {
    if event.derivation().is_some() {
        HistoryIdentity::Derived(event.id().to_owned())
    } else if let Some(source) = event.external_source() {
        HistoryIdentity::External(
            source.producer.clone(),
            source.stream.clone(),
            source.position,
            source.key.clone(),
        )
    } else {
        HistoryIdentity::Aggregate(
            event.aggregate_type().into(),
            event.aggregate_id().into(),
            event.aggregate_sequence(),
            event.publication_ordinal(),
        )
    }
}

/// Offline evidence from one original, quiescent aggregate event store.
///
/// This does not authenticate arbitrary supplied data. The operator/source
/// adapter must retain and review the original store export and its digest.
/// Private event records are never converted into public occurrences. Explicit
/// private contracts must come from the authored aggregate, not archive absence.
/// Public JSON bodies remain authenticated retained publications, not a claimed
/// reconstruction from private payload bytes.
#[derive(Clone)]
pub struct AggregateRebuildCoverage {
    stream: (String, String),
    head: u64,
    public: BTreeMap<String, [u8; 32]>,
    source_digest: [u8; 32],
}

impl AggregateRebuildCoverage {
    /// Validate a complete original stream against its independently captured
    /// head and retained public publications. The trusted offline adapter must
    /// bind `identity`, `head` and `records` to the same quiescent source store.
    /// `private_contracts` is an explicit authored contract inventory, never a
    /// list inferred from missing broker messages.
    /// This bounded adapter supports only authored streams that emit exactly
    /// one public occurrence (ordinal zero) per non-private source record. The
    /// caller must verify that contract; arbitrary one-to-many emitters require
    /// an independently complete publication manifest and are not supported.
    pub fn from_retained_stream(
        identity: &StreamIdentity,
        head: u64,
        records: &[EventRecord],
        private_contracts: &[(&str, u64)],
        public_history: &[DomainEventOccurrence],
    ) -> Result<Self, ProjectionProtocolError> {
        if records.is_empty()
            || head != records.len() as u64
            || records.len() > MAX_HISTORY_EVENTS
            || public_history.len() > MAX_HISTORY_EVENTS
        {
            return Err(invalid(
                "aggregate coverage requires a bounded nonempty original stream",
            ));
        }
        let stream = (
            identity.aggregate_type().to_owned(),
            identity.aggregate_id().to_owned(),
        );
        let mut public = BTreeMap::new();
        let mut public_bytes = 0usize;
        let mut private = BTreeSet::new();
        for contract in private_contracts {
            if contract.0.is_empty() || contract.1 == 0 || !private.insert(*contract) {
                return Err(invalid("invalid or duplicate private contract inventory"));
            }
        }
        let mut positions: BTreeMap<u64, Vec<&DomainEventOccurrence>> = BTreeMap::new();
        for event in public_history.iter().filter(|event| {
            event.external_source().is_none()
                && event.derivation().is_none()
                && event.aggregate_type() == stream.0
                && event.aggregate_id() == stream.1
        }) {
            let bytes = event.canonical_bytes().map_err(invalid)?;
            public_bytes = public_bytes
                .checked_add(bytes.len())
                .ok_or_else(|| invalid("coverage public size overflow"))?;
            if public_bytes > MAX_HISTORY_BYTES {
                return Err(invalid("aggregate coverage public history exceeds 64 MiB"));
            }
            let fingerprint: [u8; 32] = Sha256::digest(bytes).into();
            if let Some(previous) = public.insert(event.id().into(), fingerprint) {
                if previous != fingerprint {
                    return Err(invalid("aggregate coverage public identity conflict"));
                }
                continue;
            }
            positions
                .entry(event.aggregate_sequence())
                .or_default()
                .push(event);
        }
        let mut digest = Sha256::new();
        let mut total = 0usize;
        for (offset, record) in records.iter().enumerate() {
            if record.sequence != offset as u64 + 1
                || record.event_version == 0
                || record.event_name.is_empty()
                || record.payload_codec.is_empty()
                || record.payload_codec_version == 0
            {
                return Err(invalid(
                    "aggregate coverage has a missing, reordered or invalid original record",
                ));
            }
            let bytes = crate::domain_event::canonical_json_bytes(record).map_err(invalid)?;
            total = total
                .checked_add(bytes.len())
                .ok_or_else(|| invalid("coverage size overflow"))?;
            if total > MAX_HISTORY_BYTES {
                return Err(invalid("aggregate coverage exceeds 64 MiB"));
            }
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(bytes);
            let declared_private =
                private.contains(&(record.event_name.as_str(), record.event_version));
            let occurrences = positions.remove(&record.sequence).unwrap_or_default();
            if declared_private {
                if !occurrences.is_empty() {
                    return Err(invalid(
                        "private source contract conflicts with a public occurrence",
                    ));
                }
            } else {
                if occurrences.is_empty() {
                    return Err(invalid(
                        "aggregate coverage omits a required public occurrence",
                    ));
                }
                if occurrences.len() != 1 || occurrences[0].publication_ordinal() != 0 {
                    return Err(invalid(
                        "aggregate coverage requires an authored single-publication stream",
                    ));
                }
                for occurrence in occurrences {
                    if occurrence.descriptor().name != record.event_name
                        || occurrence.descriptor().version != record.event_version
                        || record.metadata.get("causation_id").map(String::as_str)
                            != occurrence.causation_id()
                    {
                        return Err(invalid("aggregate coverage public occurrence differs from its original source record"));
                    }
                }
            }
        }
        if !positions.is_empty() {
            return Err(invalid(
                "aggregate coverage ends before a public occurrence",
            ));
        }
        Ok(Self {
            stream,
            head,
            public,
            source_digest: digest.finalize().into(),
        })
    }

    /// Content identity of the validated original private record sequence.
    pub fn source_digest(&self) -> [u8; 32] {
        self.source_digest
    }
}

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
    /// covered record identities, aggregate sequence prefixes and conflicting duplicates;
    /// it cannot discover unpublished or externally deleted source history.
    /// External source positions can legitimately be sparse (including between
    /// members of one source transaction). Their completeness must be certified
    /// by the source adapter/archive; no aggregate prefix is invented for them.
    /// Stored external row versions must still occur in the supplied history.
    pub fn from_complete_history(
        self,
        history: &[DomainEventOccurrence],
    ) -> Result<SnapshotProjectionRebuildPlan, ProjectionProtocolError> {
        self.from_complete_history_with_coverage(history, &[])
    }

    /// Rebuild with independently retained original aggregate-log evidence.
    /// The original public-archive-only method remains strict when no witness
    /// is supplied. Every public identity captured by a witness must be present
    /// with the same canonical bytes in this history.
    pub fn from_complete_history_with_coverage(
        self,
        history: &[DomainEventOccurrence],
        coverage: &[AggregateRebuildCoverage],
    ) -> Result<SnapshotProjectionRebuildPlan, ProjectionProtocolError> {
        if history.len() > MAX_HISTORY_EVENTS {
            return Err(invalid(
                "snapshot rebuild history exceeds 100000 occurrences",
            ));
        }
        if coverage.len() > MAX_REBUILD_RECORDS {
            return Err(invalid("aggregate coverage exceeds 10000 streams"));
        }
        let mut bytes = 0usize;
        let mut identities = BTreeMap::new();
        let mut logical_ids = BTreeMap::new();
        let mut sequences: BTreeMap<(String, String), BTreeSet<u64>> = BTreeMap::new();
        let mut rows: HashMap<ProjectionRecordScope, RebuildRow> = HashMap::new();
        let mut relevant = BTreeSet::new();
        let mut versions: HashMap<ProjectionRecordScope, Vec<SourceSnapshotVersion>> =
            HashMap::new();
        let mut covered = BTreeMap::new();
        for witness in coverage {
            if covered.insert(witness.stream.clone(), witness).is_some() {
                return Err(invalid("duplicate aggregate coverage stream"));
            }
        }
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
            if event.external_source().is_none() && event.derivation().is_none() {
                sequences
                    .entry(stream.clone())
                    .or_default()
                    .insert(event.aggregate_sequence());
            }
            if let Some(previous) = logical_ids.insert(event.id(), canonical.clone()) {
                if previous != canonical {
                    return Err(invalid(
                        "snapshot rebuild history contains conflicting logical identities",
                    ));
                }
            }
            let identity = history_identity(event);
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
            if event.external_source().is_none() && event.derivation().is_none() {
                relevant.insert(stream);
            }
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
        for witness in covered.values() {
            for (id, expected) in &witness.public {
                let bytes = logical_ids
                    .get(id.as_str())
                    .ok_or_else(|| invalid("rebuild history omits witnessed public occurrence"))?;
                if <[u8; 32]>::from(Sha256::digest(bytes)) != *expected {
                    return Err(invalid(
                        "rebuild history differs from witnessed public content",
                    ));
                }
            }
        }
        for stream in relevant {
            let sequence = &sequences[&stream];
            if let Some(witness) = covered.get(&stream) {
                if sequence.last().is_some_and(|last| *last > witness.head) {
                    return Err(invalid(
                        "public history exceeds its original source coverage",
                    ));
                }
                // Every public occurrence for a covered stream must be in the
                // exact reviewed inventory, including ones irrelevant to this
                // particular projection.
                for event in history.iter().filter(|event| {
                    event.external_source().is_none()
                        && event.derivation().is_none()
                        && event.aggregate_type() == stream.0
                        && event.aggregate_id() == stream.1
                }) {
                    if !witness.public.contains_key(event.id()) {
                        return Err(invalid("unwitnessed public occurrence in covered stream"));
                    }
                }
                continue;
            }
            if let Some((expected, found)) = (1..)
                .zip(sequence.iter().copied())
                .find(|(expected, found)| expected != found)
            {
                return Err(invalid(format!(
                    "snapshot rebuild requires a complete aggregate sequence prefix for {}/{}: expected {}, found {}",
                    stream.0, stream.1, expected, found,
                )));
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
