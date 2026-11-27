use super::*;

/// The registrations a store has read back from its database.
///
/// `projection_registered_models` is only ever inserted into, and its primary
/// key is `(topology_hash, model_name)`: once a store has read a topology's
/// canonical bytes or a model's table, no later registration by this or any
/// other process can change what it read. A commit therefore verifies each
/// once per store, not once per transaction. A refusal is never remembered,
/// so a registration that arrives later is seen by the next commit. Entries
/// are bounded by the registered rows.
#[derive(Clone, Default)]
pub(crate) struct VerifiedRegistrations {
    /// `None` is the topology itself; `Some` is one of its `(model, table)`.
    verified: std::sync::Arc<
        std::sync::Mutex<
            std::collections::HashSet<(ProjectorTopologyId, Option<(String, String)>)>,
        >,
    >,
}

impl VerifiedRegistrations {
    pub(super) fn contains(
        &self,
        topology: &ProjectorTopologyId,
        model: Option<&ProjectionModelOwnership>,
    ) -> bool {
        self.verified
            .lock()
            .expect("verified registrations lock")
            .contains(&Self::key(topology, model))
    }

    pub(super) fn insert(
        &self,
        topology: &ProjectorTopologyId,
        model: Option<&ProjectionModelOwnership>,
    ) {
        self.verified
            .lock()
            .expect("verified registrations lock")
            .insert(Self::key(topology, model));
    }

    fn key(
        topology: &ProjectorTopologyId,
        model: Option<&ProjectionModelOwnership>,
    ) -> (ProjectorTopologyId, Option<(String, String)>) {
        (
            topology.clone(),
            model.map(|owner| (owner.model.clone(), owner.table.clone())),
        )
    }
}

#[derive(Clone, Debug)]
pub(super) struct PartitionState {
    pub(super) active_generation: ProjectionGeneration,
    pub(super) change_epoch: ProjectionEpoch,
    pub(super) change_head: u64,
    pub(super) compacted_through: u64,
    pub(super) pending_retry_failure_id: Option<String>,
    pub(super) stopped_failure_id: Option<String>,
}

#[derive(Clone, Debug)]
pub(super) struct StoredCursor {
    pub(super) source_epoch: ProjectionEpoch,
    pub(super) source_position: u64,
    pub(super) input_fingerprint: ProjectionInputFingerprint,
    pub(super) message_id: String,
    pub(super) causation_id: String,
    pub(super) gap_free: bool,
    pub(super) change: ProjectionChangeCursor,
}

#[derive(Clone, Debug)]
pub(super) struct StoredReceipt {
    pub(super) source_bytes: Vec<u8>,
    pub(super) source_hash: Vec<u8>,
    pub(super) source_partition_bytes: Vec<u8>,
    pub(super) source_partition_hash: Vec<u8>,
    pub(super) source_epoch: ProjectionEpoch,
    pub(super) source_position: u64,
    pub(super) input_fingerprint: ProjectionInputFingerprint,
    pub(super) message_id: String,
    pub(super) causation_id: String,
    pub(super) gap_free: bool,
    pub(super) outcome_kind: String,
    pub(super) change: ProjectionChangeCursor,
}

#[derive(Clone, Debug)]
pub(super) struct StoredRecord {
    pub(super) metadata: ProjectionRecordMetadata,
}

pub(super) struct StoredFailure {
    pub(super) failure: ProjectionFailure,
}
