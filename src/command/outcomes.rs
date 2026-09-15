use std::any::TypeId;
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use super::direct_projection::ResolvedDirectProjectionTarget;
use super::projection_proof::{
    validate_resolved_direct_plan, CommandCommitProofError, ProjectionCommitProof,
};
use super::typed_command::TypedCommandContract;
use super::types::{read_model_command_type, CommandOutputType, CommandTypeDef};
use crate::outbox::OutboxMessage;
use crate::projection::lower::LoweredProjectionPlan;
use crate::projection_protocol::SameTransactionProjectionBatch;
use crate::read_model::RelationalReadModel;
use crate::table::{TableSchema, TableWritePlan};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandConsistency {
    /// The command transaction succeeded. With no confirmation plan this is
    /// terminal; with an explicit finite plan it is pending projection.
    Succeeded,
    /// Aggregate committed; read models update **eventually** (projectors /
    /// event handlers after the command transaction).
    Eventual,
    /// Aggregate + read-model row in the **same** command transaction.
    Atomic,
}

mod sealed {
    pub trait Outcome {}
    pub trait PreparableOutcome {}
}

/// A successfully committed command result.
///
/// There is intentionally no public constructor. The durable command
/// committer is the only framework component allowed to create this wrapper.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Succeeded<T> {
    payload: T,
}

/// Eventual aggregate + read-model update: domain events committed; projectors
/// apply later. Client may use `.applies` previews until obligations complete.
///
/// There is intentionally no public constructor. The durable command
/// committer is the only framework component allowed to create this wrapper.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Eventual<T> {
    payload: T,
}

/// Atomic aggregate + read-model update: exact row staged and returned in the
/// command transaction (`readmodel(row).…commit()?.atomic()`).
///
/// There is intentionally no public constructor. The durable command
/// committer is the only framework component allowed to create this wrapper.
/// `T` must be a relational read model, and preparation is available only
/// through the framework-owned workspace that stages the exact row upsert.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Atomic<T> {
    payload: T,
}

macro_rules! committed_outcome {
    ($wrapper:ident, $kind:expr) => {
        impl<T> sealed::Outcome for $wrapper<T> {}

        impl<T> CommandOutcome for $wrapper<T>
        where
            T: CommandOutputType + Serialize + Send + Sync + 'static,
        {
            type Payload = T;
            const CONSISTENCY: CommandConsistency = $kind;

            fn payload(&self) -> &T {
                &self.payload
            }

            fn __finalize_committed(payload: T) -> Self {
                Self::from_committed_payload(payload)
            }

            fn __command_output_type() -> CommandTypeDef {
                T::command_type()
            }
        }
    };
}

committed_outcome!(Succeeded, CommandConsistency::Succeeded);
committed_outcome!(Eventual, CommandConsistency::Eventual);

impl<T> sealed::Outcome for Atomic<T> where T: RelationalReadModel {}

impl<T> CommandOutcome for Atomic<T>
where
    T: RelationalReadModel + Serialize + Send + Sync + 'static,
{
    type Payload = T;
    const CONSISTENCY: CommandConsistency = CommandConsistency::Atomic;

    fn payload(&self) -> &T {
        &self.payload
    }

    fn __finalize_committed(payload: T) -> Self {
        Self::from_committed_payload(payload)
    }

    fn __command_output_type() -> CommandTypeDef {
        read_model_command_type::<T>()
    }

    fn __projected_model() -> Option<(TypeId, &'static TableSchema)> {
        Some((TypeId::of::<T>(), T::schema()))
    }

    fn __projected_payload_from_row(
        row: crate::table::RowValues,
    ) -> Result<Option<T>, crate::table::TableStoreError> {
        T::from_row(row).map(Some)
    }

    fn __projected_row_for_payload(
        payload: &T,
    ) -> Result<
        Option<(crate::table::RowKey, crate::table::RowValues)>,
        crate::table::TableStoreError,
    > {
        Ok(Some((payload.primary_key()?, payload.to_row()?)))
    }
}

macro_rules! crate_committed_constructor {
    ($wrapper:ident) => {
        impl<T> $wrapper<T> {
            /// The ledger-aware committer is the only intended caller.
            pub(crate) fn from_committed_payload(payload: T) -> Self {
                Self { payload }
            }
        }
    };
}

crate_committed_constructor!(Succeeded);
crate_committed_constructor!(Eventual);

impl<T> Atomic<T>
where
    T: RelationalReadModel,
{
    /// Created only after a proof-bearing staged projection commits.
    fn from_committed_payload(payload: T) -> Self {
        Self { payload }
    }
}

impl<T> sealed::PreparableOutcome for Succeeded<T> {}
impl<T> sealed::PreparableOutcome for Eventual<T> {}

/// Sealed type-level contract implemented by committed command outcomes.
pub trait CommandOutcome: sealed::Outcome + Send + Sync + 'static {
    type Payload: Serialize + Send + Sync + 'static;
    const CONSISTENCY: CommandConsistency;

    fn payload(&self) -> &Self::Payload;

    #[doc(hidden)]
    fn __finalize_committed(payload: Self::Payload) -> Self;

    #[doc(hidden)]
    fn __command_output_type() -> CommandTypeDef;

    /// Compiler-only model identity retained by an ordinary
    /// `typed_command::<I, Atomic<M>>` declaration. The sealed default keeps
    /// succeeded/eventual outcomes unbound while `Atomic<M>` supplies its exact
    /// relational schema without an application-facing projection target API.
    #[doc(hidden)]
    fn __projected_model() -> Option<(TypeId, &'static TableSchema)> {
        None
    }

    #[doc(hidden)]
    fn __projected_payload_from_row(
        _row: crate::table::RowValues,
    ) -> Result<Option<Self::Payload>, crate::table::TableStoreError> {
        Ok(None)
    }

    #[doc(hidden)]
    fn __projected_row_for_payload(
        _payload: &Self::Payload,
    ) -> Result<
        Option<(crate::table::RowKey, crate::table::RowValues)>,
        crate::table::TableStoreError,
    > {
        Ok(None)
    }
}

/// Error produced while serializing a completion before commit I/O.
#[derive(Debug)]
pub enum PrepareCommandError {
    Serialize(serde_json::Error),
}

impl std::fmt::Display for PrepareCommandError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialize(error) => {
                write!(formatter, "command payload serialization failed: {error}")
            }
        }
    }
}

impl std::error::Error for PrepareCommandError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialize(error) => Some(error),
        }
    }
}

impl From<serde_json::Error> for PrepareCommandError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialize(error)
    }
}

/// A serialized command completion waiting for the atomic command committer.
///
/// Preparing is deliberately separate from returning a committed outcome: it
/// proves serialization before transaction I/O while keeping both the durable
/// outcome and the declaration-owned confirmation plan outside application
/// handler control.
pub struct PreparedCommand<K: CommandOutcome> {
    payload: Option<K::Payload>,
    serialized_payload: Option<serde_json::Value>,
    projection_proof: Option<ProjectionCommitProof>,
    modeled_projection_payload_pending: bool,
    _outcome: PhantomData<fn() -> K>,
}

impl<K: CommandOutcome> PreparedCommand<K> {
    fn prepare_payload(payload: K::Payload) -> Result<Self, PrepareCommandError> {
        let serialized_payload = serde_json::to_value(&payload)?;
        Ok(Self {
            payload: Some(payload),
            serialized_payload: Some(serialized_payload),
            projection_proof: None,
            modeled_projection_payload_pending: false,
            _outcome: PhantomData,
        })
    }

    pub fn consistency(&self) -> CommandConsistency {
        K::CONSISTENCY
    }

    pub fn serialized_payload(&self) -> &serde_json::Value {
        self.serialized_payload
            .as_ref()
            .expect("prepared command payload is materialized before commit")
    }

    /// Validate declaration-owned completion obligations against exactly what
    /// the handler staged. This runs before any commit I/O.
    pub(crate) fn validate_commit_evidence(
        &self,
        contract: &TypedCommandContract,
        has_staged_aggregate_events: bool,
        outbox_messages: &[OutboxMessage],
        read_model_plans: &[TableWritePlan],
        modeled_direct_plan: Option<&LoweredProjectionPlan>,
    ) -> Result<(), CommandCommitProofError> {
        if contract.consistency != K::CONSISTENCY {
            return Err(CommandCommitProofError::ConsistencyMismatch {
                declared: contract.consistency,
                prepared: K::CONSISTENCY,
            });
        }
        if contract.output_type_id != TypeId::of::<K::Payload>() {
            return Err(CommandCommitProofError::OutputTypeMismatch);
        }

        match K::CONSISTENCY {
            CommandConsistency::Succeeded | CommandConsistency::Eventual => {
                if self.projection_proof.is_some() || modeled_direct_plan.is_some() {
                    return Err(CommandCommitProofError::UnexpectedProjectionProof);
                }
                contract.validate_outbox_fact_coverage(outbox_messages)
            }
            CommandConsistency::Atomic => {
                if !has_staged_aggregate_events && outbox_messages.is_empty() {
                    return Err(CommandCommitProofError::DurableEventMissing);
                }
                if !contract.confirmations.is_empty() {
                    return Err(CommandCommitProofError::ProjectedHasConfirmations);
                }
                let proof = self
                    .projection_proof
                    .as_ref()
                    .ok_or(CommandCommitProofError::MissingProjectionProof)?;
                if let Some(modeled) = modeled_direct_plan {
                    if !read_model_plans.is_empty() {
                        return Err(CommandCommitProofError::DirectProjection(
                            "modeled direct projection cannot be mixed with separate read-model mutations"
                                .into(),
                        ));
                    }
                    validate_resolved_direct_plan(modeled)?;
                    proof.validate(
                        contract.output_type_id,
                        std::iter::once(&modeled.write_plan),
                    )
                } else {
                    proof.validate(contract.output_type_id, read_model_plans.iter())
                }
            }
        }
    }

    /// The durable committer is the sole intended consumer.
    pub(crate) fn finalize_after_commit(self) -> (K, serde_json::Value) {
        let payload = self
            .payload
            .expect("prepared command payload is materialized before commit");
        let serialized_payload = self
            .serialized_payload
            .expect("prepared command payload is serialized before commit");
        (K::__finalize_committed(payload), serialized_payload)
    }

    /// Materialize the typed result from the exact modeled full-row upsert.
    ///
    /// The handler cannot provide a competing value: the authoritative
    /// occurrence is resolved first, then the read-model derive converts that
    /// same row into the command payload and proof.
    pub(crate) fn materialize_modeled_projection(
        &mut self,
        modeled: Option<&LoweredProjectionPlan>,
    ) -> Result<(), CommandCommitProofError> {
        if !self.modeled_projection_payload_pending {
            return Ok(());
        }
        let modeled = modeled.ok_or_else(|| {
            CommandCommitProofError::DirectProjection(
                "modeled projected result has no resolved projection plan".into(),
            )
        })?;
        validate_resolved_direct_plan(modeled)?;
        let [crate::table::TableMutation::UpsertRow(row)] = modeled.write_plan.mutations.as_slice()
        else {
            return Err(CommandCommitProofError::DirectProjection(
                "modeled projected result must contain one complete row".into(),
            ));
        };
        let payload = K::__projected_payload_from_row(row.values.clone())
            .map_err(|error| CommandCommitProofError::DirectProjection(error.to_string()))?
            .ok_or_else(|| {
                CommandCommitProofError::DirectProjection(
                    "command outcome cannot materialize a modeled projected row".into(),
                )
            })?;
        let (model_type_id, schema) = K::__projected_model().ok_or_else(|| {
            CommandCommitProofError::DirectProjection(
                "command outcome has no projected read-model identity".into(),
            )
        })?;
        let (key, projected_row) = K::__projected_row_for_payload(&payload)
            .map_err(|error| CommandCommitProofError::DirectProjection(error.to_string()))?
            .ok_or(CommandCommitProofError::MissingProjectionProof)?;
        let proof =
            ProjectionCommitProof::for_materialized(model_type_id, schema, &key, &projected_row)
                .map_err(|error| CommandCommitProofError::DirectProjection(error.to_string()))?;
        let serialized_payload = serde_json::to_value(&payload).map_err(|error| {
            CommandCommitProofError::DirectProjection(format!(
                "modeled projected result serialization failed: {error}"
            ))
        })?;
        self.payload = Some(payload);
        self.serialized_payload = Some(serialized_payload);
        self.projection_proof = Some(proof);
        self.modeled_projection_payload_pending = false;
        Ok(())
    }

    /// Remove the proof-matched projected upsert from ordinary table plans and
    /// seal it as the repository's causal direct-projection participant.
    ///
    /// Succeeded and Eventual commands return no participant. An Atomic command
    /// must have exactly one resolved declaration-owned target; the extracted
    /// mutation is never also submitted through the legacy/raw plan path.
    pub(crate) fn seal_direct_projection(
        &self,
        target: Option<ResolvedDirectProjectionTarget>,
        read_model_plans: &mut Vec<TableWritePlan>,
        modeled_direct_plan: Option<LoweredProjectionPlan>,
        causation_id: &str,
    ) -> Result<Option<SameTransactionProjectionBatch>, CommandCommitProofError> {
        match K::CONSISTENCY {
            CommandConsistency::Succeeded | CommandConsistency::Eventual => {
                if target.is_some() || modeled_direct_plan.is_some() {
                    return Err(CommandCommitProofError::UnexpectedDirectProjectionTarget);
                }
                Ok(None)
            }
            CommandConsistency::Atomic => {
                let target =
                    target.ok_or(CommandCommitProofError::MissingDirectProjectionTarget)?;
                let proof = self
                    .projection_proof
                    .as_ref()
                    .ok_or(CommandCommitProofError::MissingProjectionProof)?;
                let sealed = if let Some(modeled) = modeled_direct_plan {
                    if !read_model_plans.is_empty() {
                        return Err(CommandCommitProofError::DirectProjection(
                            "modeled direct projection cannot be mixed with separate read-model mutations"
                                .into(),
                        ));
                    }
                    validate_resolved_direct_plan(&modeled)?;
                    proof.validate(
                        TypeId::of::<K::Payload>(),
                        std::iter::once(&modeled.write_plan),
                    )?;
                    let LoweredProjectionPlan {
                        mut write_plan,
                        resolved,
                    } = modeled;
                    let mutation = write_plan
                        .mutations
                        .pop()
                        .expect("validated modeled direct plan has one physical mutation");
                    target.seal_resolved(&resolved, mutation, causation_id)
                } else {
                    let mutation =
                        proof.extract_exact_upsert(TypeId::of::<K::Payload>(), read_model_plans)?;
                    target.seal(mutation, causation_id)
                };
                sealed
                    .map(Some)
                    .map_err(|error| CommandCommitProofError::DirectProjection(error.to_string()))
            }
        }
    }
}

impl<M> PreparedCommand<Atomic<M>>
where
    M: RelationalReadModel + Serialize + Send + Sync + 'static,
{
    /// Build a projected completion from the exact model value whose full-row
    /// upsert was staged by the framework-owned causal workspace.
    pub(crate) fn prepare_atomic(
        payload: M,
        proof: ProjectionCommitProof,
    ) -> Result<Self, PrepareCommandError> {
        let serialized_payload = serde_json::to_value(&payload)?;
        Ok(Self {
            payload: Some(payload),
            serialized_payload: Some(serialized_payload),
            projection_proof: Some(proof),
            modeled_projection_payload_pending: false,
            _outcome: PhantomData,
        })
    }

    pub(crate) fn prepare_modeled_atomic() -> Self {
        Self {
            payload: None,
            serialized_payload: None,
            projection_proof: None,
            modeled_projection_payload_pending: true,
            _outcome: PhantomData,
        }
    }
}

impl<K> PreparedCommand<K>
where
    K: CommandOutcome + sealed::PreparableOutcome,
{
    /// Prepare a succeeded or causal payload for the durable committer.
    /// Atomic results require a staged transactional proof and do
    /// not implement the private preparation capability.
    pub fn prepare(payload: K::Payload) -> Result<Self, PrepareCommandError> {
        Self::prepare_payload(payload)
    }
}
