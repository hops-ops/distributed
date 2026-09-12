use std::any::TypeId;

use sha2::{Digest, Sha256};

use super::outcomes::CommandConsistency;
use crate::projection::lower::LoweredProjectionPlan;
use crate::read_model::RelationalReadModel;
use crate::table::{
    ExpectedVersion, RowKey, RowValue, RowValues, RowWriteMode, TableMutation, TableStoreError,
    TableWritePlan,
};
use crate::{ProjectionMutationKind, ResolvedProjectionValue};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CommandCommitProofError {
    ConsistencyMismatch {
        declared: CommandConsistency,
        prepared: CommandConsistency,
    },
    OutputTypeMismatch,
    DurableEventMissing,
    CausalHasNoConfirmations,
    UnreachableConfirmation {
        projector: String,
        expected_facts: Vec<String>,
        staged_facts: Vec<String>,
    },
    UnexpectedProjectionProof,
    MissingProjectionProof,
    ProjectedHasConfirmations,
    ProjectionOutputTypeMismatch,
    ProjectionWriteMissing {
        model: String,
    },
    ProjectionWriteConflict {
        model: String,
    },
    ProjectionWriteMismatch {
        model: String,
    },
    MissingDirectProjectionTarget,
    UnexpectedDirectProjectionTarget,
    DirectProjection(String),
}

impl std::fmt::Display for CommandCommitProofError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConsistencyMismatch { declared, prepared } => write!(
                formatter,
                "prepared command consistency {prepared:?} does not match declaration {declared:?}"
            ),
            Self::OutputTypeMismatch => {
                formatter.write_str("prepared command output type does not match its declaration")
            }
            Self::DurableEventMissing => formatter.write_str(
                "causal and projected commands require a staged aggregate event or domain event",
            ),
            Self::CausalHasNoConfirmations => formatter.write_str(
                "a causal command requires at least one finite projector confirmation",
            ),
            Self::UnreachableConfirmation {
                projector,
                expected_facts,
                staged_facts,
            } => write!(
                formatter,
                "projector `{projector}` cannot be reached: expected one of {expected_facts:?}, staged outbox facts were {staged_facts:?}"
            ),
            Self::UnexpectedProjectionProof => formatter.write_str(
                "succeeded and causal commands cannot carry a same-transaction projection proof",
            ),
            Self::MissingProjectionProof => formatter.write_str(
                "projected command did not stage its returned read model as an exact upsert",
            ),
            Self::ProjectedHasConfirmations => formatter.write_str(
                "projected command cannot declare asynchronous projector confirmations",
            ),
            Self::ProjectionOutputTypeMismatch => formatter.write_str(
                "projected command proof is for a different Rust read-model type",
            ),
            Self::ProjectionWriteMissing { model } => write!(
                formatter,
                "projected command did not stage an upsert for returned model `{model}`"
            ),
            Self::ProjectionWriteConflict { model } => write!(
                formatter,
                "projected command staged more than one mutation for returned model `{model}` and key"
            ),
            Self::ProjectionWriteMismatch { model } => write!(
                formatter,
                "projected command staged a row that differs from returned model `{model}`"
            ),
            Self::MissingDirectProjectionTarget => formatter.write_str(
                "projected command has no declaration-owned direct projection target",
            ),
            Self::UnexpectedDirectProjectionTarget => formatter.write_str(
                "succeeded and causal commands cannot carry a direct projection target",
            ),
            Self::DirectProjection(error) => {
                write!(formatter, "direct projection target could not be sealed: {error}")
            }
        }
    }
}

impl std::error::Error for CommandCommitProofError {}

/// Private evidence tying a `Atomic<M>` payload to one exact full-row
/// upsert. Application handlers can obtain this only through the causal
/// workspace's stage-and-prepare operation.
pub(crate) struct ProjectionCommitProof {
    model_type_id: TypeId,
    model_name: String,
    table_name: String,
    key_fingerprint: String,
    row_fingerprint: String,
}

impl ProjectionCommitProof {
    pub(crate) fn for_model<M>(model: &M) -> Result<Self, TableStoreError>
    where
        M: RelationalReadModel + 'static,
    {
        let schema = M::schema();
        schema.validate()?;
        let key = model.primary_key()?;
        let row = model.to_row()?;
        Ok(Self {
            model_type_id: TypeId::of::<M>(),
            model_name: schema.model_name.clone(),
            table_name: schema.table_name.clone(),
            key_fingerprint: fingerprint_key(&key),
            row_fingerprint: fingerprint_row(&row),
        })
    }

    pub(crate) fn for_materialized(
        model_type_id: TypeId,
        schema: &'static crate::table::TableSchema,
        key: &RowKey,
        row: &RowValues,
    ) -> Result<Self, TableStoreError> {
        schema.validate()?;
        Ok(Self {
            model_type_id,
            model_name: schema.model_name.clone(),
            table_name: schema.table_name.clone(),
            key_fingerprint: fingerprint_key(key),
            row_fingerprint: fingerprint_row(row),
        })
    }

    pub(super) fn validate<'a>(
        &self,
        output_type_id: TypeId,
        plans: impl IntoIterator<Item = &'a TableWritePlan>,
    ) -> Result<(), CommandCommitProofError>
    where
        TableWritePlan: 'a,
    {
        if self.model_type_id != output_type_id {
            return Err(CommandCommitProofError::ProjectionOutputTypeMismatch);
        }

        let mut target_count = 0usize;
        let mut exact_match = false;
        for mutation in plans.into_iter().flat_map(|plan| plan.mutations.iter()) {
            let (schema, key) = match mutation {
                TableMutation::UpsertRow(mutation) => (mutation.schema, &mutation.key),
                TableMutation::PatchRow(mutation) => (mutation.schema, &mutation.key),
                TableMutation::DeleteRow(mutation) => (mutation.schema, &mutation.key),
            };
            if schema.table_name != self.table_name || fingerprint_key(key) != self.key_fingerprint
            {
                continue;
            }

            target_count += 1;
            if let TableMutation::UpsertRow(mutation) = mutation {
                exact_match = mutation.mode == RowWriteMode::Upsert
                    && mutation.schema.model_name == self.model_name
                    && fingerprint_row(&mutation.values) == self.row_fingerprint;
            }
        }

        match target_count {
            0 => Err(CommandCommitProofError::ProjectionWriteMissing {
                model: self.model_name.clone(),
            }),
            1 if exact_match => Ok(()),
            1 => Err(CommandCommitProofError::ProjectionWriteMismatch {
                model: self.model_name.clone(),
            }),
            _ => Err(CommandCommitProofError::ProjectionWriteConflict {
                model: self.model_name.clone(),
            }),
        }
    }

    pub(super) fn extract_exact_upsert(
        &self,
        output_type_id: TypeId,
        plans: &mut Vec<TableWritePlan>,
    ) -> Result<TableMutation, CommandCommitProofError> {
        self.validate(output_type_id, plans.iter())?;

        let mut found = None;
        for (plan_index, plan) in plans.iter().enumerate() {
            for (mutation_index, mutation) in plan.mutations.iter().enumerate() {
                let TableMutation::UpsertRow(row) = mutation else {
                    continue;
                };
                if row.schema.table_name == self.table_name
                    && row.schema.model_name == self.model_name
                    && fingerprint_key(&row.key) == self.key_fingerprint
                    && row.mode == RowWriteMode::Upsert
                    && fingerprint_row(&row.values) == self.row_fingerprint
                {
                    found = Some((plan_index, mutation_index));
                }
            }
        }
        let (plan_index, mutation_index) =
            found.ok_or_else(|| CommandCommitProofError::ProjectionWriteMissing {
                model: self.model_name.clone(),
            })?;
        let mutation = plans[plan_index].mutations.remove(mutation_index);
        if plans[plan_index].mutations.is_empty() {
            plans.remove(plan_index);
        } else {
            plans[plan_index].validate().map_err(|_| {
                CommandCommitProofError::ProjectionWriteConflict {
                    model: self.model_name.clone(),
                }
            })?;
        }
        Ok(mutation)
    }
}

/// Revalidate the one shape supported by the current direct evidence protocol.
///
/// The generated direct-candidate marker is intentionally insufficient by
/// itself: this checks the actual selected arm, resolved values, relationship
/// consequences, and authoritative ORM lowering before any table mutation can
/// reach an adapter.
pub(crate) fn validate_resolved_direct_plan(
    lowered: &LoweredProjectionPlan,
) -> Result<(), CommandCommitProofError> {
    lowered
        .write_plan
        .validate()
        .map_err(|error| direct_plan_error(error.to_string()))?;

    let [resolved] = lowered.resolved.mutations() else {
        return Err(direct_plan_error(
            "modeled direct projection must resolve exactly one record mutation",
        ));
    };
    if resolved.kind() != ProjectionMutationKind::Upsert {
        return Err(direct_plan_error(
            "modeled direct projection must resolve one complete upsert",
        ));
    }
    if resolved.target().model() != resolved.scope().model()
        || resolved.target().storage() != resolved.scope().storage()
        || resolved.scope().partition() != lowered.resolved.partition()
    {
        return Err(direct_plan_error(
            "modeled direct projection target, storage, or partition scope is inconsistent",
        ));
    }
    if !resolved.provenance().relationship_effects().is_empty()
        || !resolved.provenance().invalidations().is_empty()
    {
        return Err(direct_plan_error(
            "modeled direct projection cannot carry relationship effects or invalidations",
        ));
    }
    if resolved
        .fields()
        .iter()
        .any(|field| !matches!(field.value(), ResolvedProjectionValue::Value(_)))
    {
        return Err(direct_plan_error(
            "modeled direct projection upsert must resolve every row field",
        ));
    }
    if resolved.provenance().program_id() != lowered.resolved.program_id()
        || resolved.provenance().occurrence().occurrence_id() != lowered.resolved.occurrence().id()
    {
        return Err(direct_plan_error(
            "modeled direct projection provenance does not match its resolved plan",
        ));
    }

    let [TableMutation::UpsertRow(row)] = lowered.write_plan.mutations.as_slice() else {
        return Err(direct_plan_error(
            "modeled direct projection must lower to exactly one full-row upsert",
        ));
    };
    if row.mode != RowWriteMode::Upsert || row.expected_version != ExpectedVersion::Any {
        return Err(direct_plan_error(
            "modeled direct projection requires an unfenced full-row upsert",
        ));
    }
    if row.schema.model_name != resolved.target().model()
        || row.schema.table_name != resolved.target().storage()
    {
        return Err(direct_plan_error(
            "modeled direct projection logical and physical targets differ",
        ));
    }
    Ok(())
}

fn direct_plan_error(error: impl Into<String>) -> CommandCommitProofError {
    CommandCommitProofError::DirectProjection(error.into())
}

pub(super) fn fingerprint_key(key: &RowKey) -> String {
    fingerprint_values("distributed.command-projection-key.v1", key.iter())
}

fn fingerprint_row(row: &RowValues) -> String {
    fingerprint_values("distributed.command-projection-row.v1", row.iter())
}

fn fingerprint_values<'a>(
    domain: &str,
    values: impl Iterator<Item = (&'a str, &'a RowValue)>,
) -> String {
    let canonical = values
        .map(|(column, value)| serde_json::json!([column, canonical_row_value(value)]))
        .collect::<Vec<_>>();
    let mut digest = Sha256::new();
    digest.update(domain.as_bytes());
    digest.update([0]);
    digest.update(
        serde_json::to_vec(&canonical)
            .expect("canonical row projection fingerprint serialization cannot fail"),
    );
    format!("sha256:{:x}", digest.finalize())
}

fn canonical_row_value(value: &RowValue) -> serde_json::Value {
    match value {
        RowValue::Null => serde_json::json!(["null"]),
        RowValue::Bool(value) => serde_json::json!(["bool", value]),
        RowValue::I64(value) => serde_json::json!(["i64", value.to_string()]),
        RowValue::U64(value) => serde_json::json!(["u64", value.to_string()]),
        RowValue::F64(value) => serde_json::json!(["f64_bits", value.to_bits().to_string()]),
        RowValue::String(value) => serde_json::json!(["string", value]),
        RowValue::Bytes(value) => serde_json::json!(["bytes", value]),
        RowValue::Json(value) => serde_json::json!(["json", canonical_json(value)]),
    }
}

pub(super) fn canonical_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical_json).collect())
        }
        serde_json::Value::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.clone(), canonical_json(value)))
                    .collect(),
            )
        }
        scalar => scalar.clone(),
    }
}
