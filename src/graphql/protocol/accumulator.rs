use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};

use async_graphql::{Response, Value};
use serde::Serialize;

use super::{
    DistributedCommandConsistency, DistributedCommandMetadata, DistributedCommandState,
    DistributedEnvelopeV1, DistributedLiveCursor, DistributedLiveMetadata,
    DistributedProjectionDisposition, DistributedProjectionExpectation,
    DistributedProjectionObservation, DistributedQuerySnapshot, DistributedRecordRevision,
    OpaqueProtocolToken, ProtocolTokenCodec, ProtocolTokenError, ProtocolTokenPurpose,
    RequestedLiveResume,
};
use crate::command::CommandConsistency;
use crate::command_ledger::CommandLedgerState;
use crate::graphql::projection_delta::runtime::ModeledProjectionStatusDisposition;
use crate::microsvc::{
    CausalCommandProjectionObligation, CausalCommandPublicState, CausalCommandPublicStatus,
    CausalCommandReceiptSource, CausalProjectionEvidenceState,
};

#[derive(Serialize)]
struct ProjectionObligationTokenMaterial<'a> {
    domain: &'static str,
    version: u32,
    cache_scope: &'a str,
    schema_hash: &'a str,
    causation_id: &'a str,
    projector: &'a str,
    model: &'a str,
    observation_kind: &'static str,
    scope: &'a crate::projection_protocol::ProjectionRecordScope,
}

#[expect(
    clippy::too_many_arguments,
    reason = "each argument is authenticated token material and keeping it explicit prevents accidental omission"
)]
pub(crate) fn issue_projection_obligation_token(
    codec: &ProtocolTokenCodec,
    cache_scope: &str,
    schema_hash: &str,
    causation_id: &str,
    projector: &str,
    model: &str,
    observation_kind: crate::projection_protocol::ProjectionObservationKind,
    scope: &crate::projection_protocol::ProjectionRecordScope,
) -> Result<OpaqueProtocolToken, ProtocolTokenError> {
    codec.issue(
        ProtocolTokenPurpose::ProjectionObligation,
        &ProjectionObligationTokenMaterial {
            domain: "distributed.graphql.projection-obligation",
            version: 1,
            cache_scope,
            schema_hash,
            causation_id,
            projector,
            model,
            observation_kind: observation_kind.as_storage_str(),
            scope,
        },
    )
}

#[derive(Serialize)]
struct LiveResumeTokenMaterial<'a> {
    domain: &'static str,
    version: u32,
    cache_scope: &'a str,
    schema_hash: &'a str,
    snapshot_scope: &'a str,
    projection: &'a str,
    topology: &'a crate::projection_protocol::ProjectorTopologyId,
    partition: &'a crate::projection_protocol::ProjectionPartition,
    epoch: &'a str,
    position: String,
}

#[derive(Clone, Debug)]
pub struct ProtocolResponseAccumulator {
    inner: Arc<ProtocolResponseState>,
}

#[derive(Debug)]
struct ProtocolResponseState {
    envelope: Mutex<DistributedEnvelopeV1>,
    projection_request:
        Mutex<Option<crate::graphql::projection_delta::runtime::ProtocolProjectionRequestSeed>>,
    query_snapshot_scope: Mutex<Option<OpaqueProtocolToken>>,
    requested_live_resume: Mutex<RequestedLiveResume>,
    /// `None` is one-shot HTTP execution. `Some` is a stream FIFO containing
    /// one immutable envelope per yielded GraphQL response.
    stream_frames: Mutex<Option<VecDeque<DistributedEnvelopeV1>>>,
    dispatch_claimed: Mutex<bool>,
    codec: ProtocolTokenCodec,
}

impl ProtocolResponseAccumulator {
    pub(crate) fn new(envelope: DistributedEnvelopeV1, codec: ProtocolTokenCodec) -> Self {
        Self {
            inner: Arc::new(ProtocolResponseState {
                envelope: Mutex::new(envelope),
                projection_request: Mutex::new(None),
                query_snapshot_scope: Mutex::new(None),
                requested_live_resume: Mutex::new(RequestedLiveResume::Absent),
                stream_frames: Mutex::new(None),
                dispatch_claimed: Mutex::new(false),
                codec,
            }),
        }
    }

    pub(crate) fn bind_projection_request(
        &self,
        request: crate::graphql::projection_delta::runtime::ProtocolProjectionRequestSeed,
    ) -> Result<(), ProtocolAccumulatorError> {
        let mut existing = self
            .inner
            .projection_request
            .lock()
            .map_err(|_| ProtocolAccumulatorError::Poisoned)?;
        if existing.is_some() {
            return Err(ProtocolAccumulatorError::IncomparableProjectionRequest);
        }
        *existing = Some(request);
        Ok(())
    }

    pub(crate) fn projection_metadata_for_actual(
        &self,
        causation_id: crate::command_ledger::CausationId,
        replay_retention: std::time::Duration,
        occurrences: &[crate::DomainEventOccurrence],
        sealed_events: &[crate::ProjectionEventSelector],
    ) -> Result<super::CommandProjectionMetadataV1, ProtocolAccumulatorError> {
        let request = self
            .inner
            .projection_request
            .lock()
            .map_err(|_| ProtocolAccumulatorError::Poisoned)?
            .clone()
            .ok_or(ProtocolAccumulatorError::MissingProjectionRequest)?;
        let cache_scope = self.with_envelope(|envelope| envelope.cache_scope.clone())?;
        request
            .metadata_for_actual(
                self.inner.codec.clone(),
                &cache_scope,
                causation_id,
                replay_retention,
                occurrences,
                sealed_events,
            )
            .map_err(|_| ProtocolAccumulatorError::ProjectionAuthority)
    }

    pub(crate) fn modeled_projection_evidence(
        &self,
        command_name: &str,
        causation_id: &str,
        metadata: &super::CommandProjectionMetadataV1,
        batch: &crate::projection_protocol::ProjectionCausationEvidenceBatch,
        disposition: crate::graphql::projection_delta::runtime::ModeledProjectionStatusDisposition,
    ) -> Result<
        Vec<crate::graphql::projection_delta::runtime::ModeledProjectionEvidence>,
        ProtocolAccumulatorError,
    > {
        let request = self
            .inner
            .projection_request
            .lock()
            .map_err(|_| ProtocolAccumulatorError::Poisoned)?
            .clone()
            .ok_or(ProtocolAccumulatorError::MissingProjectionRequest)?;
        let cache_scope = self.with_envelope(|envelope| envelope.cache_scope.clone())?;
        request
            .modeled_evidence(
                &self.inner.codec,
                &cache_scope,
                command_name,
                causation_id,
                metadata,
                batch,
                disposition,
            )
            .map_err(|_| ProtocolAccumulatorError::ProjectionAuthority)
    }

    pub(crate) fn modeled_projection_evidence_topologies(
        &self,
        command_name: &str,
        causation_id: &str,
        metadata: &super::CommandProjectionMetadataV1,
    ) -> Result<
        crate::graphql::projection_delta::runtime::ModeledProjectionStatusPlan,
        ProtocolAccumulatorError,
    > {
        let request = self
            .inner
            .projection_request
            .lock()
            .map_err(|_| ProtocolAccumulatorError::Poisoned)?
            .clone()
            .ok_or(ProtocolAccumulatorError::MissingProjectionRequest)?;
        let cache_scope = self.with_envelope(|envelope| envelope.cache_scope.clone())?;
        request
            .modeled_evidence_topologies(
                &self.inner.codec,
                &cache_scope,
                command_name,
                causation_id,
                metadata,
            )
            .map_err(|_| ProtocolAccumulatorError::ProjectionAuthority)
    }

    fn validate_modeled_projection_metadata(
        &self,
        command_name: &str,
        causation_id: &str,
        metadata: &super::CommandProjectionMetadataV1,
    ) -> Result<(), ProtocolAccumulatorError> {
        let request = self
            .inner
            .projection_request
            .lock()
            .map_err(|_| ProtocolAccumulatorError::Poisoned)?
            .clone()
            .ok_or(ProtocolAccumulatorError::MissingProjectionRequest)?;
        let cache_scope = self.with_envelope(|envelope| envelope.cache_scope.clone())?;
        request
            .validate_modeled_metadata(
                &self.inner.codec,
                &cache_scope,
                command_name,
                causation_id,
                metadata,
            )
            .map_err(|_| ProtocolAccumulatorError::ProjectionAuthority)
    }

    fn validate_modeled_projection_lifecycle_metadata(
        &self,
        command_name: &str,
        causation_id: &str,
        metadata: &super::CommandProjectionMetadataV1,
    ) -> Result<(), ProtocolAccumulatorError> {
        let request = self
            .inner
            .projection_request
            .lock()
            .map_err(|_| ProtocolAccumulatorError::Poisoned)?
            .clone()
            .ok_or(ProtocolAccumulatorError::MissingProjectionRequest)?;
        let cache_scope = self.with_envelope(|envelope| envelope.cache_scope.clone())?;
        request
            .validate_modeled_lifecycle_metadata(
                &self.inner.codec,
                &cache_scope,
                command_name,
                causation_id,
                metadata,
            )
            .map_err(|_| ProtocolAccumulatorError::ProjectionAuthority)
    }

    pub(crate) fn set_requested_live_resume(
        &self,
        requested: RequestedLiveResume,
    ) -> Result<(), ProtocolAccumulatorError> {
        *self
            .inner
            .requested_live_resume
            .lock()
            .map_err(|_| ProtocolAccumulatorError::Poisoned)? = requested;
        Ok(())
    }

    pub(crate) fn requested_live_resume(
        &self,
    ) -> Result<RequestedLiveResume, ProtocolAccumulatorError> {
        self.inner
            .requested_live_resume
            .lock()
            .map(|requested| requested.clone())
            .map_err(|_| ProtocolAccumulatorError::Poisoned)
    }

    /// Switch this request accumulator to stream mode before injecting it into
    /// async-graphql. Producers then enqueue immutable frame envelopes; the
    /// transport pops them in order and cannot race a later frame mutation.
    pub(crate) fn begin_stream(&self) -> Result<(), ProtocolAccumulatorError> {
        let mut frames = self
            .inner
            .stream_frames
            .lock()
            .map_err(|_| ProtocolAccumulatorError::Poisoned)?;
        if frames.is_none() {
            *frames = Some(VecDeque::new());
        }
        Ok(())
    }

    /// Stable operation-instance scope used by query/index evidence.
    ///
    /// Callers pass only canonical, bounded material (normally the generated
    /// operation identity plus canonical variables/window). Authorization and
    /// schema generation are always injected here and cannot be forgotten.
    pub(crate) fn issue_query_snapshot_scope<T: Serialize>(
        &self,
        operation_instance: &T,
    ) -> Result<OpaqueProtocolToken, ProtocolAccumulatorError> {
        #[derive(Serialize)]
        struct Material<'a, T> {
            domain: &'static str,
            version: u32,
            cache_scope: &'a str,
            schema_hash: &'a str,
            operation_instance: &'a T,
        }

        self.with_envelope(|envelope| {
            self.inner
                .codec
                .issue(
                    ProtocolTokenPurpose::QuerySnapshot,
                    &Material {
                        domain: "distributed.graphql.query-snapshot",
                        version: 1,
                        cache_scope: envelope.cache_scope.as_str(),
                        schema_hash: &envelope.schema_hash,
                        operation_instance,
                    },
                )
                .map_err(|_| ProtocolAccumulatorError::Encoding)
        })?
    }

    pub(crate) fn bind_query_snapshot_scope<T: Serialize>(
        &self,
        operation_instance: &T,
    ) -> Result<OpaqueProtocolToken, ProtocolAccumulatorError> {
        let token = self.issue_query_snapshot_scope(operation_instance)?;
        let mut existing = self
            .inner
            .query_snapshot_scope
            .lock()
            .map_err(|_| ProtocolAccumulatorError::Poisoned)?;
        match existing.as_ref() {
            None => *existing = Some(token.clone()),
            Some(existing) if existing == &token => {}
            Some(_) => return Err(ProtocolAccumulatorError::IncomparableSnapshot),
        }
        Ok(token)
    }

    pub(crate) fn query_snapshot_scope(
        &self,
    ) -> Result<OpaqueProtocolToken, ProtocolAccumulatorError> {
        self.inner
            .query_snapshot_scope
            .lock()
            .map_err(|_| ProtocolAccumulatorError::Poisoned)?
            .clone()
            .ok_or(ProtocolAccumulatorError::MissingSnapshotScope)
    }

    /// Stable record identity token. Revision numbers and operation identity
    /// are deliberately excluded so values from separate queries remain
    /// comparable only after this token matches.
    pub(crate) fn issue_record_scope(
        &self,
        scope: &crate::projection_protocol::ProjectionRecordScope,
    ) -> Result<OpaqueProtocolToken, ProtocolAccumulatorError> {
        #[derive(Serialize)]
        struct Material<'a> {
            domain: &'static str,
            version: u32,
            cache_scope: &'a str,
            schema_hash: &'a str,
            scope: &'a crate::projection_protocol::ProjectionRecordScope,
        }

        self.with_envelope(|envelope| {
            self.inner
                .codec
                .issue(
                    ProtocolTokenPurpose::RecordRevision,
                    &Material {
                        domain: "distributed.graphql.record-scope",
                        version: 1,
                        cache_scope: envelope.cache_scope.as_str(),
                        schema_hash: &envelope.schema_hash,
                        scope,
                    },
                )
                .map_err(|_| ProtocolAccumulatorError::Encoding)
        })?
    }

    /// Stable exact-index scope. The current head is excluded so later
    /// positions within the same projector/partition/query window compare.
    #[allow(dead_code)]
    pub(crate) fn issue_index_scope(
        &self,
        snapshot_scope: &OpaqueProtocolToken,
        cursor: &crate::projection_protocol::ProjectionChangeCursor,
    ) -> Result<OpaqueProtocolToken, ProtocolAccumulatorError> {
        self.issue_index_scope_parts(
            snapshot_scope,
            cursor.topology(),
            cursor.projection_partition(),
            cursor.epoch(),
        )
    }

    pub(crate) fn issue_index_scope_parts(
        &self,
        snapshot_scope: &OpaqueProtocolToken,
        topology: &crate::projection_protocol::ProjectorTopologyId,
        partition: &crate::projection_protocol::ProjectionPartition,
        epoch: &crate::projection_protocol::ProjectionEpoch,
    ) -> Result<OpaqueProtocolToken, ProtocolAccumulatorError> {
        #[derive(Serialize)]
        struct Material<'a> {
            domain: &'static str,
            version: u32,
            cache_scope: &'a str,
            schema_hash: &'a str,
            snapshot_scope: &'a str,
            topology: &'a crate::projection_protocol::ProjectorTopologyId,
            partition: &'a crate::projection_protocol::ProjectionPartition,
            epoch: &'a str,
        }

        self.with_envelope(|envelope| {
            self.inner
                .codec
                .issue(
                    ProtocolTokenPurpose::QueryIndex,
                    &Material {
                        domain: "distributed.graphql.query-index",
                        version: 1,
                        cache_scope: envelope.cache_scope.as_str(),
                        schema_hash: &envelope.schema_hash,
                        snapshot_scope: snapshot_scope.as_str(),
                        topology,
                        partition,
                        epoch: epoch.as_str(),
                    },
                )
                .map_err(|_| ProtocolAccumulatorError::Encoding)
        })?
    }

    #[allow(dead_code)]
    pub(crate) fn issue_live_resume(
        &self,
        projection: &str,
        snapshot_scope: &OpaqueProtocolToken,
        cursor: &crate::projection_protocol::ProjectionChangeCursor,
    ) -> Result<DistributedLiveCursor, ProtocolAccumulatorError> {
        self.issue_live_resume_position(
            projection,
            snapshot_scope,
            cursor.topology(),
            cursor.projection_partition(),
            cursor.epoch(),
            cursor.position(),
        )
    }

    pub(crate) fn issue_live_resume_position(
        &self,
        projection: &str,
        snapshot_scope: &OpaqueProtocolToken,
        topology: &crate::projection_protocol::ProjectorTopologyId,
        partition: &crate::projection_protocol::ProjectionPartition,
        epoch: &crate::projection_protocol::ProjectionEpoch,
        position: u64,
    ) -> Result<DistributedLiveCursor, ProtocolAccumulatorError> {
        let token = self.live_resume_token(
            projection,
            snapshot_scope,
            topology,
            partition,
            epoch,
            position,
        )?;
        Ok(DistributedLiveCursor {
            projection: projection.to_string(),
            position: position.to_string(),
            token,
        })
    }

    /// Verify a client-returned cursor against one server-derived static
    /// projector scope. The public projection/position fields select the
    /// finite candidate; hidden topology/partition/epoch remain MAC-bound.
    #[allow(dead_code)]
    pub(crate) fn verify_live_resume(
        &self,
        supplied: &DistributedLiveCursor,
        snapshot_scope: &OpaqueProtocolToken,
        expected: &crate::projection_protocol::ProjectionChangeCursor,
    ) -> Result<(), ProtocolTokenError> {
        if supplied.position != expected.position().to_string() {
            return Err(ProtocolTokenError::Mismatch);
        }
        self.verify_live_resume_position(
            supplied,
            snapshot_scope,
            &supplied.projection,
            expected.topology(),
            expected.projection_partition(),
            expected.epoch(),
            expected.position(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn verify_live_resume_position(
        &self,
        supplied: &DistributedLiveCursor,
        snapshot_scope: &OpaqueProtocolToken,
        projection: &str,
        topology: &crate::projection_protocol::ProjectorTopologyId,
        partition: &crate::projection_protocol::ProjectionPartition,
        epoch: &crate::projection_protocol::ProjectionEpoch,
        position: u64,
    ) -> Result<(), ProtocolTokenError> {
        if supplied.projection != projection || supplied.position != position.to_string() {
            return Err(ProtocolTokenError::Mismatch);
        }
        self.with_envelope(|envelope| {
            self.inner.codec.verify(
                &supplied.token,
                ProtocolTokenPurpose::LiveResume,
                &LiveResumeTokenMaterial {
                    domain: "distributed.graphql.live-resume",
                    version: 1,
                    cache_scope: envelope.cache_scope.as_str(),
                    schema_hash: &envelope.schema_hash,
                    snapshot_scope: snapshot_scope.as_str(),
                    projection,
                    topology,
                    partition,
                    epoch: epoch.as_str(),
                    position: position.to_string(),
                },
            )
        })
        .map_err(|_| ProtocolTokenError::InvalidMaterial)?
    }

    fn live_resume_token(
        &self,
        projection: &str,
        snapshot_scope: &OpaqueProtocolToken,
        topology: &crate::projection_protocol::ProjectorTopologyId,
        partition: &crate::projection_protocol::ProjectionPartition,
        epoch: &crate::projection_protocol::ProjectionEpoch,
        position: u64,
    ) -> Result<OpaqueProtocolToken, ProtocolAccumulatorError> {
        self.with_envelope(|envelope| {
            self.inner
                .codec
                .issue(
                    ProtocolTokenPurpose::LiveResume,
                    &LiveResumeTokenMaterial {
                        domain: "distributed.graphql.live-resume",
                        version: 1,
                        cache_scope: envelope.cache_scope.as_str(),
                        schema_hash: &envelope.schema_hash,
                        snapshot_scope: snapshot_scope.as_str(),
                        projection,
                        topology,
                        partition,
                        epoch: epoch.as_str(),
                        position: position.to_string(),
                    },
                )
                .map_err(|_| ProtocolAccumulatorError::Encoding)
        })?
    }

    /// Issue the exact token used by both receipt expectations and query/live
    /// observations. Equality is therefore meaningful without exposing or
    /// reconstructing canonical partition/key bytes in JavaScript.
    pub(crate) fn issue_projection_obligation_scope(
        &self,
        causation_id: &str,
        projector: &str,
        model: &str,
        observation_kind: crate::projection_protocol::ProjectionObservationKind,
        scope: &crate::projection_protocol::ProjectionRecordScope,
    ) -> Result<OpaqueProtocolToken, ProtocolAccumulatorError> {
        self.with_envelope(|envelope| {
            issue_projection_obligation_token(
                &self.inner.codec,
                envelope.cache_scope.as_str(),
                &envelope.schema_hash,
                causation_id,
                projector,
                model,
                observation_kind,
                scope,
            )
            .map_err(|_| ProtocolAccumulatorError::Encoding)
        })?
    }

    fn with_envelope<T>(
        &self,
        operation: impl FnOnce(&DistributedEnvelopeV1) -> T,
    ) -> Result<T, ProtocolAccumulatorError> {
        self.inner
            .envelope
            .lock()
            .map(|envelope| operation(&envelope))
            .map_err(|_| ProtocolAccumulatorError::Poisoned)
    }

    /// Reserve this operation's single causal-command dispatch slot before
    /// invoking application code. This prevents a multi-field mutation from
    /// committing a second causal command and only then discovering that one
    /// response envelope cannot represent both receipts.
    pub(crate) fn claim_dispatch(&self) -> Result<(), ProtocolAccumulatorError> {
        let mut claimed = self
            .inner
            .dispatch_claimed
            .lock()
            .map_err(|_| ProtocolAccumulatorError::Poisoned)?;
        if *claimed {
            return Err(ProtocolAccumulatorError::MultipleCommands);
        }
        *claimed = true;
        Ok(())
    }

    /// Convert exact durable replay material into the public wire receipt.
    ///
    /// The command payload is deliberately not consumed here. Canonical
    /// projector partition/key material is fed only into HMAC token issuance
    /// and can never appear in the serialized envelope.
    pub(crate) fn record_receipt(
        &self,
        receipt: &CausalCommandReceiptSource,
    ) -> Result<(), ProtocolAccumulatorError> {
        let projection_disposition = receipt
            .projection_metadata
            .as_ref()
            .map(|metadata| {
                self.modeled_projection_evidence_topologies(
                    &receipt.command_name,
                    &receipt.causation_id,
                    metadata,
                )
                .map(|plan| match plan.disposition {
                    ModeledProjectionStatusDisposition::Applicable => None,
                    ModeledProjectionStatusDisposition::Revalidate => {
                        Some(DistributedProjectionDisposition::Revalidate)
                    }
                })
            })
            .transpose()?
            .flatten();
        let obligation_count = receipt
            .projection_metadata
            .as_ref()
            .map_or(receipt.obligations.len(), |metadata| {
                metadata.obligations.len()
            });
        let observed = if receipt.state == CommandLedgerState::Atomic {
            (0..obligation_count).collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        self.record_command(self.metadata(
            &receipt.command_id,
            &receipt.command_name,
            &receipt.causation_id,
            command_state(receipt.state),
            receipt.consistency,
            &receipt.obligations,
            receipt.projection_metadata.as_ref(),
            receipt.direct_projection.as_ref(),
            &observed,
            projection_disposition,
        )?)
    }

    /// Record the authorized public status when it contains a complete receipt.
    ///
    /// `unknown` and compact `expired` status intentionally omit command
    /// metadata: their GraphQL data state remains useful without fabricating or
    /// disclosing causation. `in_progress` is complete because the durable
    /// reservation already has a causation ID and consistency contract.
    pub(crate) fn record_status(
        &self,
        status: &CausalCommandPublicStatus,
    ) -> Result<(), ProtocolAccumulatorError> {
        let (Some(causation_id), Some(consistency)) =
            (status.causation_id.as_deref(), status.consistency)
        else {
            return Ok(());
        };
        let observed = status
            .evidence
            .iter()
            .filter(|evidence| evidence.state == CausalProjectionEvidenceState::Observed)
            .map(|evidence| evidence.obligation_index)
            .collect::<Vec<_>>();
        self.record_command(
            self.metadata(
                &status.command_id,
                status.command_name.as_deref().unwrap_or_default(),
                causation_id,
                public_command_state(status.state),
                consistency,
                &status.obligations,
                status.projection_metadata.as_ref(),
                status.direct_projection.as_ref(),
                &observed,
                status
                    .projection_revalidate
                    .then_some(DistributedProjectionDisposition::Revalidate),
            )?,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "wire command metadata is assembled from distinct authenticated receipt fields"
    )]
    fn metadata(
        &self,
        command_id: &str,
        command_name: &str,
        causation_id: &str,
        state: DistributedCommandState,
        consistency: CommandConsistency,
        obligations: &[CausalCommandProjectionObligation],
        projection_metadata: Option<&super::CommandProjectionMetadataV1>,
        direct_projection: Option<&crate::projection_protocol::SameTransactionProjectionEvidence>,
        observed_obligation_indices: &[usize],
        projection_disposition: Option<DistributedProjectionDisposition>,
    ) -> Result<DistributedCommandMetadata, ProtocolAccumulatorError> {
        if projection_metadata.is_some() && !obligations.is_empty() {
            return Err(ProtocolAccumulatorError::Encoding);
        }
        if let Some(metadata) = projection_metadata {
            // Modeled metadata is a request-authorized capability even when it
            // carries no finite obligations and the ledger state is already
            // Succeeded. Validate every receipt/status emission so replay can
            // never expose a stale surface, authorization generation, cache
            // scope, causation, or expired delta.
            match projection_disposition {
                Some(DistributedProjectionDisposition::Revalidate) => {
                    self.validate_modeled_projection_lifecycle_metadata(
                        command_name,
                        causation_id,
                        metadata,
                    )?;
                }
                None => {
                    self.validate_modeled_projection_metadata(
                        command_name,
                        causation_id,
                        metadata,
                    )?;
                }
            }
        }
        if projection_disposition.is_some() && projection_metadata.is_none() {
            return Err(ProtocolAccumulatorError::Encoding);
        }
        if projection_disposition.is_some()
            && (!obligations.is_empty() || direct_projection.is_some())
        {
            return Err(ProtocolAccumulatorError::Encoding);
        }
        if projection_disposition.is_some() {
            return Ok(DistributedCommandMetadata {
                command_id: command_id.to_string(),
                causation_id: causation_id.to_string(),
                state,
                consistency: command_consistency(consistency),
                projection_disposition,
                expects: Vec::new(),
                projection: None,
                observations: Vec::new(),
                records: Vec::new(),
            });
        }
        let mut expects = obligations
            .iter()
            .map(|obligation| {
                self.issue_projection_obligation_scope(
                    causation_id,
                    &obligation.projector,
                    &obligation.model,
                    obligation.observation_kind,
                    &obligation.scope,
                )
                .map(|scope_token| DistributedProjectionExpectation {
                    projection: obligation.projector.clone(),
                    model: obligation.model.clone(),
                    scope_token,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(metadata) = projection_metadata {
            expects.extend(
                metadata
                    .obligations
                    .iter()
                    .map(|obligation| {
                        modeled_obligation_label(metadata, obligation).map(|(projection, model)| {
                            DistributedProjectionExpectation {
                                projection,
                                model,
                                scope_token: obligation.scope_token.clone(),
                            }
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        let mut seen_observations = std::collections::BTreeSet::new();
        let observations = observed_obligation_indices
            .iter()
            .map(|index| {
                if !seen_observations.insert(*index) {
                    return Err(ProtocolAccumulatorError::Encoding);
                }
                match projection_metadata {
                    Some(metadata) => {
                        let obligation = metadata
                            .obligations
                            .get(*index)
                            .ok_or(ProtocolAccumulatorError::Encoding)?;
                        let (projection, model) = modeled_obligation_label(metadata, obligation)?;
                        Ok(DistributedProjectionObservation {
                            causation_id: causation_id.to_string(),
                            projection,
                            model,
                            scope_token: obligation.scope_token.clone(),
                        })
                    }
                    None => {
                        let obligation = obligations
                            .get(*index)
                            .ok_or(ProtocolAccumulatorError::Encoding)?;
                        Ok(DistributedProjectionObservation {
                            causation_id: causation_id.to_string(),
                            projection: obligation.projector.clone(),
                            model: obligation.model.clone(),
                            scope_token: self.issue_projection_obligation_scope(
                                causation_id,
                                &obligation.projector,
                                &obligation.model,
                                obligation.observation_kind,
                                &obligation.scope,
                            )?,
                        })
                    }
                }
            })
            .collect::<Result<Vec<_>, ProtocolAccumulatorError>>()?;
        let records = direct_projection
            .into_iter()
            .flat_map(|evidence| evidence.records.iter())
            .map(|record| {
                Ok(DistributedRecordRevision {
                    path: None,
                    model: record.revision.scope().model().to_string(),
                    scope_token: self.issue_record_scope(record.revision.scope())?,
                    incarnation: record.revision.incarnation().to_string(),
                    revision: record.revision.revision().to_string(),
                    tombstone: record.tombstone,
                })
            })
            .collect::<Result<Vec<_>, ProtocolAccumulatorError>>()?;

        Ok(DistributedCommandMetadata {
            command_id: command_id.to_string(),
            causation_id: causation_id.to_string(),
            state,
            consistency: command_consistency(consistency),
            projection_disposition,
            expects,
            projection: projection_metadata.cloned(),
            observations,
            records,
        })
    }

    /// Record the one generated causal command represented by this operation.
    /// Re-recording identical metadata is idempotent; a second distinct command
    /// fails closed rather than attaching an ambiguous receipt.
    pub(crate) fn record_command(
        &self,
        command: DistributedCommandMetadata,
    ) -> Result<(), ProtocolAccumulatorError> {
        let mut envelope = self
            .inner
            .envelope
            .lock()
            .map_err(|_| ProtocolAccumulatorError::Poisoned)?;
        match &envelope.command {
            None => envelope.command = Some(command),
            Some(existing) if existing == &command => {}
            Some(_) => return Err(ProtocolAccumulatorError::MultipleCommands),
        }
        Ok(())
    }

    /// Attach query/live evidence to this HTTP response or enqueue one
    /// immutable GraphQL-WS frame envelope.
    pub(crate) fn record_query_metadata(
        &self,
        mut snapshot: DistributedQuerySnapshot,
        live: Option<DistributedLiveMetadata>,
    ) -> Result<(), ProtocolAccumulatorError> {
        snapshot.discard_incomparable_index_evidence();
        let mut envelope = self
            .inner
            .envelope
            .lock()
            .map_err(|_| ProtocolAccumulatorError::Poisoned)?;
        let mut frames = self
            .inner
            .stream_frames
            .lock()
            .map_err(|_| ProtocolAccumulatorError::Poisoned)?;

        if let Some(frames) = frames.as_mut() {
            let mut frame = envelope.clone();
            frame.snapshot = Some(snapshot);
            frame.live = live;
            frames.push_back(frame);
            return Ok(());
        }

        match &mut envelope.snapshot {
            None => envelope.snapshot = Some(snapshot),
            Some(existing) if existing.scope_token == snapshot.scope_token => {
                existing.records_complete &= snapshot.records_complete;
                existing.indexes_comparable &= snapshot.indexes_comparable;
                existing.records.extend(snapshot.records);
                existing.indexes.extend(snapshot.indexes);
                existing.observations.extend(snapshot.observations);
                existing.discard_incomparable_index_evidence();
            }
            Some(_) => return Err(ProtocolAccumulatorError::IncomparableSnapshot),
        }
        match (&envelope.live, live) {
            (None, Some(live)) => envelope.live = Some(live),
            (Some(existing), Some(live)) if existing == &live => {}
            (Some(_), Some(_)) => return Err(ProtocolAccumulatorError::IncomparableSnapshot),
            (_, None) => {}
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn snapshot(&self) -> Result<DistributedEnvelopeV1, ProtocolAccumulatorError> {
        self.inner
            .envelope
            .lock()
            .map(|envelope| envelope.clone())
            .map_err(|_| ProtocolAccumulatorError::Poisoned)
    }

    pub(crate) fn attach(&self, response: &mut Response) -> Result<(), ProtocolAccumulatorError> {
        if response.extensions.contains_key("distributed") {
            return Err(ProtocolAccumulatorError::ExtensionCollision);
        }
        let envelope = {
            let envelope = self
                .inner
                .envelope
                .lock()
                .map_err(|_| ProtocolAccumulatorError::Poisoned)?;
            let mut frames = self
                .inner
                .stream_frames
                .lock()
                .map_err(|_| ProtocolAccumulatorError::Poisoned)?;
            match frames.as_mut() {
                Some(frames) => frames.pop_front().unwrap_or_else(|| envelope.clone()),
                None => envelope.clone(),
            }
        };
        let json =
            serde_json::to_value(envelope).map_err(|_| ProtocolAccumulatorError::Encoding)?;
        let value = Value::from_json(json).map_err(|_| ProtocolAccumulatorError::Encoding)?;
        response.extensions.insert("distributed".into(), value);
        Ok(())
    }
}

fn modeled_obligation_label(
    metadata: &super::CommandProjectionMetadataV1,
    obligation: &super::CommandProjectionObligationV1,
) -> Result<(String, String), ProtocolAccumulatorError> {
    let projection = metadata
        .delta
        .projections
        .get(obligation.projection_ref as usize)
        .ok_or(ProtocolAccumulatorError::Encoding)?;
    Ok((projection.program_id.clone(), obligation.model.clone()))
}

fn command_consistency(value: CommandConsistency) -> DistributedCommandConsistency {
    match value {
        CommandConsistency::Succeeded => DistributedCommandConsistency::Succeeded,
        CommandConsistency::Eventual => DistributedCommandConsistency::Eventual,
        CommandConsistency::Atomic => DistributedCommandConsistency::Atomic,
    }
}

fn command_state(value: CommandLedgerState) -> DistributedCommandState {
    match value {
        CommandLedgerState::InProgress | CommandLedgerState::RetryableUnknown => {
            DistributedCommandState::InProgress
        }
        CommandLedgerState::Succeeded => DistributedCommandState::Succeeded,
        CommandLedgerState::SucceededPendingProjection => {
            DistributedCommandState::SucceededPendingProjection
        }
        CommandLedgerState::Atomic => DistributedCommandState::Atomic,
        CommandLedgerState::Rejected => DistributedCommandState::Rejected,
        CommandLedgerState::ProjectionFailed => DistributedCommandState::ProjectionFailed,
        CommandLedgerState::Expired => DistributedCommandState::Expired,
    }
}

fn public_command_state(value: CausalCommandPublicState) -> DistributedCommandState {
    match value {
        CausalCommandPublicState::InProgress => DistributedCommandState::InProgress,
        CausalCommandPublicState::Succeeded => DistributedCommandState::Succeeded,
        CausalCommandPublicState::SucceededPendingProjection => {
            DistributedCommandState::SucceededPendingProjection
        }
        CausalCommandPublicState::Atomic => DistributedCommandState::Atomic,
        CausalCommandPublicState::Rejected => DistributedCommandState::Rejected,
        CausalCommandPublicState::ProjectionFailed => DistributedCommandState::ProjectionFailed,
        CausalCommandPublicState::Expired => DistributedCommandState::Expired,
        CausalCommandPublicState::Unknown => DistributedCommandState::Unknown,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProtocolAccumulatorError {
    Poisoned,
    MultipleCommands,
    ExtensionCollision,
    IncomparableSnapshot,
    IncomparableProjectionRequest,
    MissingSnapshotScope,
    MissingProjectionRequest,
    ProjectionAuthority,
    Encoding,
}

impl fmt::Display for ProtocolAccumulatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Poisoned => "protocol response state is unavailable",
            Self::MultipleCommands => {
                "one GraphQL operation cannot carry multiple causal command receipts"
            }
            Self::ExtensionCollision => "GraphQL response already defines extensions.distributed",
            Self::IncomparableSnapshot => {
                "GraphQL operation produced incomparable query snapshot metadata"
            }
            Self::IncomparableProjectionRequest => {
                "GraphQL operation produced incomparable projection request authority"
            }
            Self::MissingSnapshotScope => "GraphQL query snapshot scope was not initialized",
            Self::MissingProjectionRequest => {
                "GraphQL projection request authority was not initialized"
            }
            Self::ProjectionAuthority => "GraphQL projection request validation failed",
            Self::Encoding => "protocol response encoding failed",
        })
    }
}

impl std::error::Error for ProtocolAccumulatorError {}
