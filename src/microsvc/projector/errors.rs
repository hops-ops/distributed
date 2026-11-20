use sha2::{Digest, Sha256};

use crate::bus::{Message, OrderedDelivery};
use crate::projection_protocol::{
    CompiledProjectionTopology, PartitionId, ProjectionEpoch, ProjectionFailureBatch,
    ProjectionGeneration, ProjectionInputCursor, ProjectionInputFingerprint,
    ProjectionPartitionRuntimeState, ProjectionProtocolError, ProjectionProtocolStore,
    TrustedProjectionInput,
};

use super::super::HandlerError;
use super::handle::ProjectionRepairHandle;

#[expect(
    clippy::too_many_arguments,
    reason = "failure persistence requires the full trusted ingress identity and ordering tuple"
)]
pub(super) async fn record_ingress_failure<S>(
    store: &S,
    compiled: &CompiledProjectionTopology,
    change_epoch: ProjectionEpoch,
    message: &Message,
    ordered: &OrderedDelivery,
    message_id: &str,
    causation_id: &str,
    error: HandlerError,
) -> Result<(), HandlerError>
where
    S: ProjectionProtocolStore,
{
    let partition =
        PartitionId::new(b"distributed.projection.ingress-failure-partition.v1\0".to_vec())
            .map_err(ProjectionProtocolError::from)?;
    let cursor = ProjectionInputCursor::new(
        compiled.topology().clone(),
        partition.clone(),
        ordered.source().clone(),
        ordered.epoch().clone(),
        ordered.position(),
    )
    .map_err(ProjectionProtocolError::from)?;
    let fingerprint = canonical_message_fingerprint(message);
    store
        .register_projection_models(compiled.topology(), compiled.ownership())
        .await?;
    let runtime_state = store
        .projection_partition_runtime_state(compiled.topology(), &partition)
        .await?;
    let generation = preflight_runtime_state(
        runtime_state.as_ref(),
        &cursor,
        fingerprint,
        message_id,
        causation_id,
        false,
    )?;
    let input = TrustedProjectionInput::mint(
        cursor,
        fingerprint,
        message_id,
        causation_id,
        generation,
        false,
    )?;
    handle_projector_error(store, input, change_epoch, "ingress_partition", error).await
}

pub(super) fn preflight_runtime_state(
    state: Option<&ProjectionPartitionRuntimeState>,
    cursor: &ProjectionInputCursor,
    fingerprint: ProjectionInputFingerprint,
    message_id: &str,
    causation_id: &str,
    gap_free: bool,
) -> Result<ProjectionGeneration, HandlerError> {
    let Some(state) = state else {
        return Ok(ProjectionGeneration::initial());
    };
    if let Some(failure_id) = &state.stopped_failure_id {
        return Err(terminal_recorded(failure_id.clone()));
    }
    if let Some(pending) = &state.pending_retry {
        if &pending.input != cursor {
            return Err(terminal_recorded(pending.failure_id.clone()));
        }
        if pending.input_fingerprint != fingerprint
            || pending.message_id != message_id
            || pending.causation_id != causation_id
            || pending.gap_free != gap_free
        {
            return Err(terminal_recorded(pending.failure_id.clone()));
        }
    }
    Ok(state.active_generation)
}

pub(super) async fn handle_projector_error<S>(
    store: &S,
    input: TrustedProjectionInput,
    change_epoch: ProjectionEpoch,
    code: &'static str,
    error: HandlerError,
) -> Result<(), HandlerError>
where
    S: ProjectionProtocolStore,
{
    if error.is_projection_retryable() {
        return Err(error);
    }
    let detail = error.to_string();
    let failure_id =
        record_terminal_failure(store, input, change_epoch, code, detail.as_bytes()).await?;
    Err(terminal_recorded(failure_id))
}

pub(super) async fn record_terminal_protocol_failure<S>(
    store: &S,
    input: TrustedProjectionInput,
    change_epoch: ProjectionEpoch,
    code: &'static str,
    error: ProjectionProtocolError,
) -> Result<(), HandlerError>
where
    S: ProjectionProtocolStore,
{
    if let ProjectionProtocolError::PartitionStopped { failure_id } = error {
        return Err(terminal_recorded(failure_id));
    }
    if projection_error_is_retryable(&error) {
        return Err(error.into());
    }
    let detail = error.to_string();
    let failure_id =
        record_terminal_failure(store, input, change_epoch, code, detail.as_bytes()).await?;
    Err(terminal_recorded(failure_id))
}

async fn record_terminal_failure<S>(
    store: &S,
    input: TrustedProjectionInput,
    change_epoch: ProjectionEpoch,
    code: &'static str,
    detail: &[u8],
) -> Result<String, HandlerError>
where
    S: ProjectionProtocolStore,
{
    const MAX_FAILURE_DETAIL_BYTES: usize = 1024 * 1024;

    let failure_id = deterministic_failure_id(&input);
    let detail = if detail.len() > MAX_FAILURE_DETAIL_BYTES {
        &detail[..MAX_FAILURE_DETAIL_BYTES]
    } else {
        detail
    };
    let batch = ProjectionFailureBatch::new(
        input,
        change_epoch,
        failure_id.clone(),
        code,
        detail.to_vec(),
    )?;
    match store.record_projection_failure(batch).await {
        Ok(_) => Ok(failure_id),
        // Another worker can durably stop this partition between our preflight
        // and failure write. Retaining the current exact source position and
        // stopping is the same required outcome as observing it in preflight.
        Err(ProjectionProtocolError::PartitionStopped { failure_id }) => {
            Err(terminal_recorded(failure_id))
        }
        Err(error) => Err(error.into()),
    }
}

pub(super) fn terminal_recorded(failure_id: String) -> HandlerError {
    HandlerError::ProjectionTerminalRecorded {
        repair: ProjectionRepairHandle::for_failure(failure_id),
    }
}

fn deterministic_failure_id(input: &TrustedProjectionInput) -> String {
    let mut digest = Sha256::new();
    digest.update(b"distributed.causal-projector.failure-id.v1\0");
    digest.update(input.cursor.topology().canonical_bytes());
    digest.update(input.cursor.projection_partition().canonical_bytes());
    digest.update(input.cursor.source().name().as_bytes());
    digest.update(input.cursor.source().canonical_partition_bytes());
    digest.update(input.cursor.epoch().as_str().as_bytes());
    digest.update(input.cursor.position().to_be_bytes());
    digest.update(input.generation.get().to_be_bytes());
    format!("{:x}", digest.finalize())
}

pub(super) fn canonical_message_fingerprint(message: &Message) -> ProjectionInputFingerprint {
    let canonical = serde_json::json!({
        "version": 1,
        "id": message.id(),
        "name": message.name(),
        "kind": message.kind.as_str(),
        "content_type": message.content_type,
        "payload": message.payload,
        "causation_id": message.causation_id(),
    });
    ProjectionInputFingerprint::from_canonical_bytes(
        &serde_json::to_vec(&canonical)
            .expect("a canonical transport message contains only serializable primitives"),
    )
}

pub(in crate::microsvc) fn projection_error_is_retryable(error: &ProjectionProtocolError) -> bool {
    use crate::lock::RetryClass;
    use crate::table::TableStoreError;

    match error {
        ProjectionProtocolError::Repository(error) => error.is_retryable(),
        ProjectionProtocolError::Table(TableStoreError::ConcurrencyConflict { .. })
        | ProjectionProtocolError::Table(TableStoreError::NotFound { .. })
        | ProjectionProtocolError::RecordRevisionConflict { .. }
        | ProjectionProtocolError::GenerationFenced { .. } => true,
        ProjectionProtocolError::Table(TableStoreError::BackendStorage { retryable, .. }) => {
            *retryable
        }
        ProjectionProtocolError::Table(TableStoreError::Lock(error)) => {
            error.kind() == RetryClass::Retryable
        }
        ProjectionProtocolError::Validation(_)
        | ProjectionProtocolError::Table(_)
        | ProjectionProtocolError::InvalidBatch(_)
        | ProjectionProtocolError::ScopeMismatch { .. }
        | ProjectionProtocolError::IncomparableInput
        | ProjectionProtocolError::InputCorruption
        | ProjectionProtocolError::MessageIdReuse { .. }
        | ProjectionProtocolError::PartitionStopped { .. }
        | ProjectionProtocolError::RecordMissing { .. }
        | ProjectionProtocolError::RecordAlreadyExists { .. }
        | ProjectionProtocolError::RecordTombstoned { .. }
        | ProjectionProtocolError::RecreateRequiresTombstone { .. }
        | ProjectionProtocolError::CausalWriteRequired { .. }
        | ProjectionProtocolError::PositionOverflow { .. } => false,
    }
}
