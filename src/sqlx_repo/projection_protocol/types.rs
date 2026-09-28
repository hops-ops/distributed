use super::*;

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
pub(super) struct StoredInputIdentity {
    pub(super) partition_bytes: Vec<u8>,
    pub(super) partition_hash: Vec<u8>,
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
}

#[derive(Clone, Debug)]
pub(super) struct StoredRecord {
    pub(super) metadata: ProjectionRecordMetadata,
}

pub(super) struct StoredFailure {
    pub(super) failure: ProjectionFailure,
}

pub(super) enum InputDisposition {
    New,
    Redelivery,
    Duplicate(ProjectionCheckpoint),
    Stale(ProjectionCheckpoint),
}
