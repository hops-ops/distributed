use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct PartitionKey {
    pub(super) topology: ProjectorTopologyId,
    pub(super) partition: PartitionId,
}

impl PartitionKey {
    pub(super) fn new(topology: &ProjectorTopologyId, partition: &PartitionId) -> Self {
        Self {
            topology: topology.clone(),
            partition: partition.clone(),
        }
    }

    pub(super) fn from_input(input: &ProjectionInputCursor) -> Self {
        Self::new(input.topology(), input.projection_partition())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct InputKey {
    pub(super) partition: PartitionKey,
    pub(super) source: ProjectionSource,
    pub(super) generation: ProjectionGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct SourceCapabilityKey {
    pub(super) partition: PartitionKey,
    pub(super) source: ProjectionSource,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct CursorReceiptKey {
    pub(super) partition: PartitionKey,
    pub(super) source: ProjectionSource,
    pub(super) source_epoch: ProjectionEpoch,
    pub(super) source_position: u64,
    pub(super) generation: ProjectionGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct CursorIdentityKey {
    pub(super) partition: PartitionKey,
    pub(super) source: ProjectionSource,
    pub(super) source_epoch: ProjectionEpoch,
    pub(super) source_position: u64,
}

impl CursorReceiptKey {
    pub(super) fn new(cursor: &ProjectionInputCursor, generation: ProjectionGeneration) -> Self {
        Self {
            partition: PartitionKey::from_input(cursor),
            source: cursor.source().clone(),
            source_epoch: cursor.epoch().clone(),
            source_position: cursor.position(),
            generation,
        }
    }
}

impl CursorIdentityKey {
    pub(super) fn new(cursor: &ProjectionInputCursor) -> Self {
        Self {
            partition: PartitionKey::from_input(cursor),
            source: cursor.source().clone(),
            source_epoch: cursor.epoch().clone(),
            source_position: cursor.position(),
        }
    }
}

impl SourceCapabilityKey {
    pub(super) fn new(cursor: &ProjectionInputCursor) -> Self {
        Self {
            partition: PartitionKey::from_input(cursor),
            source: cursor.source().clone(),
        }
    }
}

impl InputKey {
    pub(super) fn new(cursor: &ProjectionInputCursor, generation: ProjectionGeneration) -> Self {
        Self {
            partition: PartitionKey::from_input(cursor),
            source: cursor.source().clone(),
            generation,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct MessageKey {
    pub(super) topology: ProjectorTopologyId,
    pub(super) message_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct GenerationKey {
    pub(super) partition: PartitionKey,
    pub(super) generation: ProjectionGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct OwnershipKey {
    pub(super) partition: PartitionKey,
    pub(super) model: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct RegisteredModelKey {
    pub(super) topology: ProjectorTopologyId,
    pub(super) model: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct ObservationKey {
    pub(super) causation_id: String,
    pub(super) scope: ProjectionRecordScope,
    pub(super) kind: ProjectionObservationKind,
}

/// An input a generation applied, with the change that applying it produced.
pub(super) type AppliedInput = (TrustedProjectionInput, ProjectionChangeCursor);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct GenerationLineage {
    pub(super) retry_of_generation: Option<ProjectionGeneration>,
    pub(super) retry_of_failure_id: Option<String>,
}

impl GenerationLineage {
    pub(super) fn initial() -> Self {
        Self {
            retry_of_generation: None,
            retry_of_failure_id: None,
        }
    }

    pub(super) fn retry_of(generation: ProjectionGeneration, failure_id: String) -> Self {
        Self {
            retry_of_generation: Some(generation),
            retry_of_failure_id: Some(failure_id),
        }
    }
}

#[derive(Clone)]
pub(super) struct PartitionState {
    pub(super) active_generation: ProjectionGeneration,
    pub(super) change_epoch: ProjectionEpoch,
    pub(super) change_head: u64,
    pub(super) compacted_through: u64,
    pub(super) stopped_failure_id: Option<String>,
    pub(super) pending_retry_failure_id: Option<String>,
    pub(super) changes: BTreeMap<u64, ProjectionChange>,
}

pub(super) struct PendingChange {
    pub(super) kind: ProjectionChangeKind,
    pub(super) causation_id: String,
    pub(super) observation_kind: Option<ProjectionObservationKind>,
    pub(super) scope: Option<ProjectionRecordScope>,
    pub(super) revision: Option<RecordRevision>,
    pub(super) failure_id: Option<String>,
    pub(super) program_id: Option<crate::ProjectionProgramId>,
}

impl PartitionState {
    pub(super) fn new(change_epoch: ProjectionEpoch) -> Self {
        Self {
            active_generation: ProjectionGeneration::initial(),
            change_epoch,
            change_head: 0,
            compacted_through: 0,
            stopped_failure_id: None,
            pending_retry_failure_id: None,
            changes: BTreeMap::new(),
        }
    }
}

/// Dev-only in-memory representation of the durable projection protocol.
///
/// The state is cloned before every transaction. This intentionally favors
/// simple, auditable atomicity over throughput: the production SQL adapters use
/// database transactions, while this adapter is primarily for tests and local
/// development.
#[derive(Clone, Default)]
pub(in crate::in_memory_repo) struct InMemoryProjectionProtocolState {
    pub(super) partitions: HashMap<PartitionKey, PartitionState>,
    pub(super) generations: HashMap<GenerationKey, GenerationLineage>,
    /// Newest applied input per source and generation.
    pub(super) inputs: HashMap<InputKey, AppliedInput>,
    /// The input first recorded at each cursor, in any generation.
    pub(super) input_identities: HashMap<CursorIdentityKey, TrustedProjectionInput>,
    /// The input first recorded under each message ID, in any generation.
    pub(super) messages: HashMap<MessageKey, TrustedProjectionInput>,
    pub(super) applied_receipts: HashMap<CursorReceiptKey, AppliedInput>,
    pub(super) registered_topologies: HashSet<ProjectorTopologyId>,
    pub(super) registered_models: HashMap<RegisteredModelKey, String>,
    pub(super) authoritative_table_owners: HashMap<String, RegisteredModelKey>,
    pub(super) ownership: HashMap<OwnershipKey, String>,
    pub(super) gap_free_capabilities: HashMap<SourceCapabilityKey, bool>,
    pub(super) records: HashMap<ProjectionRecordScope, ProjectionRecordMetadata>,
    pub(super) observations: HashMap<ObservationKey, ProjectionObservation>,
    pub(super) failures: HashMap<String, ProjectionFailure>,
}
