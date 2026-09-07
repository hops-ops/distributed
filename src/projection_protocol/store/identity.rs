use super::*;

pub(super) const INPUT_FINGERPRINT_DOMAIN: &[u8] = b"distributed.projection.input.v1\0";
pub(super) const FAILURE_FINGERPRINT_DOMAIN: &[u8] = b"distributed.projection.failure.v1\0";
pub(super) const MAX_MESSAGE_ID_BYTES: usize = 255;
pub(super) const MAX_CAUSATION_ID_BYTES: usize = 128;
pub(super) const MAX_FAILURE_ID_BYTES: usize = 255;
pub(super) const MAX_FAILURE_CODE_BYTES: usize = 255;
pub(super) const MAX_FAILURE_DETAIL_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_PROJECTION_QUERY_CHECKPOINT_PROBES: usize = 128;
pub(crate) const MAX_PROJECTION_QUERY_BATCH_ROWS: usize = 4_096;
pub(crate) const MAX_PROJECTION_QUERY_BATCH_CHECKPOINT_PROBES: usize = 4_096;
pub(crate) const MAX_PROJECTION_EVIDENCE_BATCH_ITEMS: usize = 128;

/// Default maximum number of durable changes retained per projector partition.
pub const DEFAULT_MAX_RETAINED_PROJECTION_CHANGES: u64 = 4_096;

/// Automatic per-partition change-log retention bound.
///
/// Every change-producing transaction retains at most this many newest entries
/// and advances the durable compacted-through watermark only after the older
/// contiguous prefix is actually removed. The watermark remains authoritative
/// if configuration is later lengthened; compacted entries are never inferred
/// or advertised as restored.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProjectionChangeRetention(NonZeroU64);

impl ProjectionChangeRetention {
    pub fn new(max_retained_changes: u64) -> Result<Self, ProjectionProtocolValidationError> {
        if max_retained_changes > MAX_PROJECTION_POSITION {
            return Err(ProjectionProtocolValidationError::TooLarge {
                field: "projection retained change count",
                value: max_retained_changes,
                max: MAX_PROJECTION_POSITION,
            });
        }
        NonZeroU64::new(max_retained_changes).map(Self).ok_or(
            ProjectionProtocolValidationError::Zero {
                field: "projection retained change count",
            },
        )
    }

    pub fn max_retained_changes(self) -> u64 {
        self.0.get()
    }
}

impl Default for ProjectionChangeRetention {
    fn default() -> Self {
        Self(
            NonZeroU64::new(DEFAULT_MAX_RETAINED_PROJECTION_CHANGES)
                .expect("the default projection change retention is nonzero"),
        )
    }
}

/// Explicit repair generation for one projector partition.
///
/// Generation one is the initial run. A terminal failure can only be retried
/// after an operator creates a later generation linked to the failed one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProjectionGeneration(NonZeroU64);

impl ProjectionGeneration {
    pub fn new(value: u64) -> Result<Self, ProjectionProtocolValidationError> {
        if value > MAX_PROJECTION_POSITION {
            return Err(ProjectionProtocolValidationError::TooLarge {
                field: "projection generation",
                value,
                max: MAX_PROJECTION_POSITION,
            });
        }
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ProjectionProtocolValidationError::Zero {
                field: "projection generation",
            })
    }

    pub fn initial() -> Self {
        Self(NonZeroU64::MIN)
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }

    pub fn checked_next(self) -> Result<Self, ProjectionProtocolError> {
        let next = self
            .get()
            .checked_add(1)
            .ok_or(ProjectionProtocolError::PositionOverflow {
                domain: "projection generation",
            })?;
        Self::new(next).map_err(|_| ProjectionProtocolError::PositionOverflow {
            domain: "projection generation",
        })
    }
}

/// SHA-256 identity of the exact canonical projector input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProjectionInputFingerprint([u8; 32]);

impl ProjectionInputFingerprint {
    /// Fingerprint bytes emitted by a registered source adapter after it has
    /// canonicalized every semantic part of the input envelope.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Self {
        Self(domain_separated_digest(INPUT_FINGERPRINT_DOMAIN, bytes))
    }

    pub(crate) fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    pub fn digest(self) -> [u8; 32] {
        self.0
    }
}

/// Framework-authenticated input used by the sealed asynchronous commit path.
///
/// Its constructor is crate-private by design. A public `Message` and its
/// metadata cannot mint ordering evidence; only a registered transport/source
/// adapter may do so after authenticating its cursor scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TrustedProjectionInput {
    pub(crate) cursor: ProjectionInputCursor,
    pub(crate) fingerprint: ProjectionInputFingerprint,
    pub(crate) message_id: String,
    pub(crate) causation_id: String,
    pub(crate) generation: ProjectionGeneration,
    pub(crate) gap_free: bool,
}

impl TrustedProjectionInput {
    pub(crate) fn mint(
        cursor: ProjectionInputCursor,
        fingerprint: ProjectionInputFingerprint,
        message_id: impl Into<String>,
        causation_id: impl Into<String>,
        generation: ProjectionGeneration,
        gap_free: bool,
    ) -> Result<Self, ProjectionProtocolError> {
        let message_id = bounded_opaque("projection message ID", message_id, MAX_MESSAGE_ID_BYTES)?;
        let causation_id = bounded_opaque(
            "projection causation ID",
            causation_id,
            MAX_CAUSATION_ID_BYTES,
        )?;
        Ok(Self {
            cursor,
            fingerprint,
            message_id,
            causation_id,
            generation,
            gap_free,
        })
    }

    pub(crate) fn inbox_receipt(&self) -> InboxReceipt {
        InboxReceipt::new(self.consumer_name(), self.message_id.clone())
    }

    pub(super) fn consumer_name(&self) -> String {
        format!(
            "projection:v1:{}:{}:{}",
            digest_hex(&self.cursor.topology().digest()),
            digest_hex(&self.cursor.projection_partition().digest()),
            self.generation.get(),
        )
    }
}

/// Read-only disposition of one exact framework-authenticated input.
///
/// Projector runtimes use this before typed decoding and handler invocation so
/// fan-out redelivery of an already committed sibling cannot rerun application
/// projection logic. `Pending` is only returned after exact immutable identity,
/// active generation, stop, source-capability, and repair fences pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionInputDisposition {
    Pending,
    Duplicate(ProjectionCheckpoint),
    Stale(ProjectionCheckpoint),
}

/// Fail-closed ownership registration for one projection model/table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionModelOwnership {
    pub(crate) model: String,
    pub(crate) table: String,
}

impl ProjectionModelOwnership {
    pub(crate) fn new(
        model: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<Self, ProjectionProtocolError> {
        Ok(Self {
            model: bounded_name("projection model", model, 255)?,
            table: bounded_name("projection table", table, 255)?,
        })
    }
}

/// Record state required before a staged mutation may apply.
#[derive(Clone, Debug, PartialEq, Eq)]
#[expect(
    clippy::large_enum_variant,
    reason = "bounded staged mutations carry the exact canonical revision inline for atomic validation"
)]
pub(crate) enum ProjectionRecordExpectation {
    Missing,
    Exact(RecordRevision),
}

/// Protocol meaning of a staged table mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionMutationKind {
    Upsert,
    Delete,
    Recreate,
}

/// One scope-checked record mutation in a sealed projection batch.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectionRecordMutation {
    pub(crate) scope: ProjectionRecordScope,
    pub(crate) mutation: TableMutation,
    pub(crate) expectation: ProjectionRecordExpectation,
    pub(crate) kind: ProjectionMutationKind,
    pub(crate) source_snapshot: Option<super::super::SourceSnapshotVersion>,
}

impl ProjectionRecordMutation {
    pub(crate) fn new(
        scope: ProjectionRecordScope,
        mutation: TableMutation,
        expectation: ProjectionRecordExpectation,
        kind: ProjectionMutationKind,
    ) -> Result<Self, ProjectionProtocolError> {
        Self::with_source_snapshot(scope, mutation, expectation, kind, None)
    }

    pub(crate) fn with_source_snapshot(
        scope: ProjectionRecordScope,
        mutation: TableMutation,
        expectation: ProjectionRecordExpectation,
        kind: ProjectionMutationKind,
        source_snapshot: Option<super::super::SourceSnapshotVersion>,
    ) -> Result<Self, ProjectionProtocolError> {
        if let ProjectionRecordExpectation::Exact(revision) = &expectation {
            if revision.scope() != &scope {
                return Err(ProjectionProtocolError::ScopeMismatch {
                    field: "projection record expectation",
                });
            }
        }
        let is_delete = matches!(mutation, TableMutation::DeleteRow(_));
        if is_delete != matches!(kind, ProjectionMutationKind::Delete) {
            return Err(ProjectionProtocolError::InvalidBatch(
                "projection delete kind and table mutation disagree".into(),
            ));
        }
        if (kind == ProjectionMutationKind::Recreate
            || (kind == ProjectionMutationKind::Delete && source_snapshot.is_none()))
            && !matches!(expectation, ProjectionRecordExpectation::Exact(_))
        {
            return Err(ProjectionProtocolError::InvalidBatch(
                "projection delete/recreate requires an exact record revision".into(),
            ));
        }
        Ok(Self {
            scope,
            mutation,
            expectation,
            kind,
            source_snapshot,
        })
    }
}

/// Whether an observation confirms a concrete row or a dependency scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProjectionObservationKind {
    Record,
    Dependency,
}

impl ProjectionObservationKind {
    pub(crate) fn as_storage_str(self) -> &'static str {
        match self {
            Self::Record => "record",
            Self::Dependency => "dependency",
        }
    }
}

/// Revision fence for one causation observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionObservationTarget {
    /// Observe the revision allocated to a mutation of this scope in the batch.
    StagedRecord(ProjectionRecordScope),
    /// Observe an already persisted exact revision without mutating the row.
    ExistingRecord(RecordRevision),
    /// Observe an embedded/dependency scope that has no independent row or
    /// record revision. Clients revalidate its owning dependency when seen.
    Dependency(ProjectionRecordScope),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionObservationRequest {
    pub(crate) kind: ProjectionObservationKind,
    pub(crate) target: ProjectionObservationTarget,
}

impl ProjectionObservationRequest {
    pub(crate) fn scope(&self) -> &ProjectionRecordScope {
        match &self.target {
            ProjectionObservationTarget::StagedRecord(scope)
            | ProjectionObservationTarget::Dependency(scope) => scope,
            ProjectionObservationTarget::ExistingRecord(revision) => revision.scope(),
        }
    }
}
