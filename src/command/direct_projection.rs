use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;

use super::effect_wire::{EffectWireCompatible, EffectWireString, TypedEffectExpression};
use super::effects::EffectExpression;
use super::projection_obligations::ProjectorTopologyIdentity;
use super::projection_proof::canonical_json;
use crate::microsvc::Session;
use crate::projection::executor::resolved_partition_json;
use crate::projection_protocol::{
    ProjectionEpoch, ProjectionModelOwnership, ProjectionPartition, ProjectionPartitionSpec,
    ProjectionProtocolError, ProjectionScopeCodec, ProjectorTopologyId,
    SameTransactionProjectionBatch,
};
use crate::read_model::RelationalReadModel;
use crate::table::{TableMutation, TableSchema};
use crate::{ProjectionProgramId, ResolvedProjectionPlan};

/// Compiler-retained relational identity for one ordinary `Atomic<M>`
/// declaration before the GraphQL Surface resolves its unique physical owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommandProjectedModel {
    pub(crate) output_type_id: TypeId,
    pub(crate) model: String,
    pub(crate) table: String,
    pub(crate) schema: &'static TableSchema,
    pub(crate) partition: Option<EffectExpression>,
}

impl CommandProjectedModel {
    pub(super) fn new(output_type_id: TypeId, schema: &'static TableSchema) -> Self {
        Self {
            output_type_id,
            model: schema.model_name.clone(),
            table: schema.table_name.clone(),
            schema,
            partition: None,
        }
    }

    pub(super) fn canonical_value(&self) -> serde_json::Value {
        canonical_json(&serde_json::json!({
            "model": self.model,
            "table": self.table,
            "partition": self.partition,
        }))
    }

    pub(crate) fn partition_matches(&self, partition: &ProjectionPartitionSpec) -> bool {
        match partition {
            ProjectionPartitionSpec::Unit => self.partition.is_none(),
            ProjectionPartitionSpec::Constant { value } => {
                self.partition
                    == Some(EffectExpression::Constant {
                        value: value.clone(),
                    })
            }
            ProjectionPartitionSpec::InputPath { .. } => self.partition.is_some(),
            ProjectionPartitionSpec::ModeledExpression { .. }
            | ProjectionPartitionSpec::ModeledInactive => false,
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the private binding freezes independent topology and modeled-program pins"
    )]
    pub(crate) fn bind(
        &self,
        projector: &str,
        facts: &[String],
        models: &[String],
        projector_partition: &ProjectionPartitionSpec,
        change_epoch: Option<&str>,
        mut ownership: Vec<ProjectionModelOwnership>,
        protocol_topology: Option<ProjectorTopologyId>,
        modeled_program_id: Option<ProjectionProgramId>,
    ) -> CommandDirectProjectionTarget {
        ownership.sort_by(|left, right| {
            (left.model.as_str(), left.table.as_str())
                .cmp(&(right.model.as_str(), right.table.as_str()))
        });
        CommandDirectProjectionTarget {
            projector: projector.to_string(),
            model: self.model.clone(),
            table: self.table.clone(),
            output_type_id: self.output_type_id,
            projector_topology: ProjectorTopologyIdentity::new(
                projector,
                facts,
                models,
                projector_partition,
            ),
            protocol_topology,
            partition: self.partition.clone(),
            change_epoch: change_epoch.map(str::to_string),
            schema: self.schema,
            ownership,
            modeled_program_id,
        }
    }
}

/// Compiler-owned direct target for one `Atomic<M>` command.
///
/// This metadata is deliberately hidden from ordinary handler code. Generated
/// declarations bind it once; application handlers select the narrow
/// `direct_read_model::<M>()` proof on their fluent causal commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommandDirectProjectionTarget {
    pub(crate) projector: String,
    pub(crate) model: String,
    pub(crate) table: String,
    pub(crate) output_type_id: TypeId,
    projector_topology: ProjectorTopologyIdentity,
    /// Exact post-bind protocol identity compiled from accepted facts, the
    /// versioned scope codec, and every complete owned table schema. The
    /// pre-bind typed declaration deliberately carries `None` and cannot
    /// resolve into a direct projection participant.
    protocol_topology: Option<ProjectorTopologyId>,
    pub(crate) partition: Option<EffectExpression>,
    pub(crate) change_epoch: Option<String>,
    pub(crate) schema: &'static TableSchema,
    /// Complete frozen model → physical-table inventory owned by the
    /// projector topology. Bootstrap claims this entire set atomically even
    /// though the direct command mutates exactly one output model.
    pub(crate) ownership: Vec<ProjectionModelOwnership>,
    /// Exact active modeled program accepted by this compiler-bound owner.
    ///
    /// Legacy direct owners retain `None`; the modeled handler path requires
    /// `Some` and compares it with the descriptor supplied by application
    /// code before the direct participant can be sealed.
    modeled_program_id: Option<ProjectionProgramId>,
}

impl CommandDirectProjectionTarget {
    pub(crate) fn canonical_value(&self) -> serde_json::Value {
        canonical_json(&serde_json::json!({
            "projector": self.projector,
            "projector_topology": self.projector_topology.canonical_value(),
            "protocol_topology": self.protocol_topology.as_ref().map(|topology| serde_json::json!({
                "version": topology.version(),
                "name": topology.name(),
                "digest": topology.digest(),
            })),
            "model": self.model,
            "table": self.table,
            "partition": self.partition,
            "change_epoch": self.change_epoch,
            "ownership": self.ownership.iter().map(|owner| serde_json::json!({
                "model": owner.model,
                "table": owner.table,
            })).collect::<Vec<_>>(),
            "modeled_program_id": self.modeled_program_id.map(|program| program.to_string()),
        }))
    }

    pub(crate) fn topology_matches(
        &self,
        name: &str,
        facts: &[String],
        models: &[String],
        projector_partition: &ProjectionPartitionSpec,
        change_epoch: Option<&str>,
    ) -> bool {
        self.projector_topology
            == ProjectorTopologyIdentity::new(name, facts, models, projector_partition)
            && self.change_epoch.as_deref() == change_epoch
    }

    pub(crate) fn protocol_topology_matches(&self, topology: &ProjectorTopologyId) -> bool {
        self.protocol_topology.as_ref() == Some(topology)
    }

    /// Exact compiled protocol topology retained by the declaration binder.
    ///
    /// Client-manifest export exposes only this opaque identity. The full
    /// projector facts, schemas, tables, and ownership inventory remain
    /// server-private.
    pub(crate) fn protocol_topology(&self) -> Option<&ProjectorTopologyId> {
        self.protocol_topology.as_ref()
    }

    pub(crate) fn resolve(
        &self,
        canonical_wire_input: &serde_json::Value,
        session: Option<&Session>,
    ) -> Result<ResolvedDirectProjectionTarget, DirectProjectionTargetResolutionError> {
        let change_epoch = self.change_epoch.as_ref().ok_or_else(|| {
            DirectProjectionTargetResolutionError::InvalidTarget {
                projector: self.projector.clone(),
                model: self.model.clone(),
                reason: "registered projector has no change-log epoch".into(),
            }
        })?;
        let change_epoch = ProjectionEpoch::new(change_epoch.clone()).map_err(|error| {
            DirectProjectionTargetResolutionError::InvalidTarget {
                projector: self.projector.clone(),
                model: self.model.clone(),
                reason: error.to_string(),
            }
        })?;
        let topology = self.protocol_topology.clone().ok_or_else(|| {
            DirectProjectionTargetResolutionError::InvalidTarget {
                projector: self.projector.clone(),
                model: self.model.clone(),
                reason: "direct projection target was not bound to its complete compiled topology"
                    .into(),
            }
        })?;
        let codec = ProjectionScopeCodec::with_models(
            topology,
            [(self.schema.model_name.as_str(), self.schema)],
        )
        .map_err(
            |error| DirectProjectionTargetResolutionError::InvalidTarget {
                projector: self.projector.clone(),
                model: self.model.clone(),
                reason: error.to_string(),
            },
        )?;
        let partition_value = self
            .partition
            .as_ref()
            .map(|expression| {
                resolve_direct_projection_expression(
                    canonical_wire_input,
                    self,
                    "partition",
                    expression,
                    session,
                    Some("String"),
                )
            })
            .transpose()?;
        let partition = codec
            .encode_partition(partition_value.as_ref())
            .map_err(
                |error| DirectProjectionTargetResolutionError::InvalidTarget {
                    projector: self.projector.clone(),
                    model: self.model.clone(),
                    reason: error.to_string(),
                },
            )?;
        Ok(ResolvedDirectProjectionTarget {
            codec: Arc::new(codec),
            partition_value,
            partition,
            change_epoch,
            model: self.model.clone(),
            table: self.table.clone(),
            schema: self.schema,
            ownership: self.ownership.clone(),
            modeled_program_id: self.modeled_program_id,
        })
    }
}

/// Opaque compiler product attached to a typed projected command.
#[doc(hidden)]
pub struct CompiledDirectProjectionTarget<I, M>(
    pub(super) CommandDirectProjectionTarget,
    PhantomData<fn(I) -> M>,
);

impl<I, M> CompiledDirectProjectionTarget<I, M> {
    /// Generated declarations may resolve the registered projection partition
    /// from one typed canonical input expression.
    #[doc(hidden)]
    pub fn partition<Wire>(mut self, partition: TypedEffectExpression<String, Wire>) -> Self
    where
        Wire: EffectWireCompatible<EffectWireString>,
    {
        self.0.partition = Some(partition.__into_ir());
        self
    }
}

pub(crate) fn compiled_direct_projection_target<I, M>(
    projector: &str,
    facts: &[String],
    models: &[String],
    projector_partition: &ProjectionPartitionSpec,
    change_epoch: Option<&str>,
) -> CompiledDirectProjectionTarget<I, M>
where
    M: RelationalReadModel + 'static,
{
    let schema = M::schema();
    let mut projected = CommandProjectedModel::new(TypeId::of::<M>(), schema);
    if let ProjectionPartitionSpec::Constant { value } = projector_partition {
        projected.partition = Some(EffectExpression::Constant {
            value: value.clone(),
        });
    }
    CompiledDirectProjectionTarget(
        projected.bind(
            projector,
            facts,
            models,
            projector_partition,
            change_epoch,
            vec![
                ProjectionModelOwnership::new(&schema.model_name, &schema.table_name)
                    .expect("validated relational schema has bounded model/table names"),
            ],
            None,
            None,
        ),
        PhantomData,
    )
}

pub(crate) struct ResolvedDirectProjectionTarget {
    codec: Arc<ProjectionScopeCodec>,
    partition_value: Option<serde_json::Value>,
    partition: ProjectionPartition,
    change_epoch: ProjectionEpoch,
    model: String,
    table: String,
    schema: &'static TableSchema,
    ownership: Vec<ProjectionModelOwnership>,
    modeled_program_id: Option<ProjectionProgramId>,
}

impl ResolvedDirectProjectionTarget {
    pub(crate) fn registration(&self) -> (&ProjectorTopologyId, &[ProjectionModelOwnership]) {
        (self.codec.topology(), &self.ownership)
    }

    pub(crate) fn validate_modeled_owner(
        &self,
        projection_name: &str,
        projection_epoch: &str,
        program_id: ProjectionProgramId,
    ) -> Result<(), ProjectionProtocolError> {
        if self.codec.topology().name() != projection_name {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "modeled direct projection `{projection_name}` does not match compiled owner `{}`",
                self.codec.topology().name()
            )));
        }
        if self.change_epoch.as_str() != projection_epoch {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "modeled direct projection epoch `{projection_epoch}` does not match compiled owner epoch `{}`",
                self.change_epoch.as_str()
            )));
        }
        match self.modeled_program_id {
            Some(expected) if expected == program_id => {}
            Some(expected) => {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "modeled direct projection program `{program_id}` does not match compiled owner program `{expected}`"
                )));
            }
            None => {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "modeled direct projection requires an exact active program binding".into(),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn seal(
        self,
        mutation: TableMutation,
        causation_id: &str,
    ) -> Result<SameTransactionProjectionBatch, ProjectionProtocolError> {
        let TableMutation::UpsertRow(row) = &mutation else {
            return Err(ProjectionProtocolError::InvalidBatch(
                "direct projection proof did not extract a full-row upsert".into(),
            ));
        };
        if row.schema != self.schema
            || row.schema.model_name != self.model
            || row.schema.table_name != self.table
        {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "direct projection target `{}`/`{}` does not match the staged row",
                self.model, self.table
            )));
        }
        let scope = self
            .codec
            .encode_row_scope(
                self.codec.topology().name(),
                &self.model,
                self.partition_value.as_ref(),
                &row.key,
            )
            .map_err(|error| ProjectionProtocolError::InvalidBatch(error.to_string()))?;
        let ownership = ProjectionModelOwnership::new(self.model, self.table)?;
        SameTransactionProjectionBatch::single_upsert(
            self.codec.topology().clone(),
            self.partition,
            self.change_epoch,
            ownership,
            scope,
            mutation,
            causation_id,
        )
    }

    /// Seal one already validated modeled projection through the existing
    /// single-row direct participant.
    ///
    /// The logical plan is retained until this boundary so the resolved
    /// partition, model/storage target, occurrence causation, and compiled
    /// direct owner can be checked against the exact physical row. The
    /// resulting batch is still the original one-upsert/one-observation proof.
    pub(super) fn seal_resolved(
        self,
        plan: &ResolvedProjectionPlan,
        mutation: TableMutation,
        causation_id: &str,
    ) -> Result<SameTransactionProjectionBatch, ProjectionProtocolError> {
        let [resolved] = plan.mutations() else {
            return Err(ProjectionProtocolError::InvalidBatch(
                "modeled direct projection must contain exactly one resolved record".into(),
            ));
        };
        if resolved.target().model() != self.model
            || resolved.target().storage() != self.table
            || resolved.scope().model() != self.model
            || resolved.scope().storage() != self.table
        {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "modeled direct projection target `{}/{}` does not match compiled owner `{}/{}`",
                resolved.target().model(),
                resolved.target().storage(),
                self.model,
                self.table
            )));
        }
        let owner_matches = self
            .ownership
            .iter()
            .filter(|owner| owner.model == self.model && owner.table == self.table)
            .count();
        if owner_matches != 1 {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "modeled direct projection `{}/{}` must have one exact compiled owner",
                self.model, self.table
            )));
        }
        let logical_partition = resolved_partition_json(plan.partition())?;
        let encoded_partition = self
            .codec
            .encode_partition(logical_partition.as_ref())
            .map_err(|error| ProjectionProtocolError::InvalidBatch(error.to_string()))?;
        if encoded_partition != self.partition || logical_partition != self.partition_value {
            return Err(ProjectionProtocolError::ScopeMismatch {
                field: "modeled direct projection partition",
            });
        }
        if plan
            .occurrence()
            .metadata()
            .get(crate::CAUSATION_ID)
            .map(String::as_str)
            != Some(causation_id)
        {
            return Err(ProjectionProtocolError::InvalidBatch(
                "modeled direct projection occurrence has different command causation".into(),
            ));
        }
        self.seal(mutation, causation_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DirectProjectionTargetResolutionError {
    MissingInputPath {
        projector: String,
        model: String,
        target: String,
        path: Vec<String>,
    },
    TrustedPresetUnavailable {
        projector: String,
        model: String,
        target: String,
        preset: String,
    },
    InvalidConstant {
        projector: String,
        model: String,
        target: String,
        error: String,
    },
    InvalidTarget {
        projector: String,
        model: String,
        reason: String,
    },
}

impl std::fmt::Display for DirectProjectionTargetResolutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingInputPath {
                projector,
                model,
                target,
                path,
            } => write!(
                formatter,
                "direct projection `{projector}`/`{model}` {target} references absent canonical input path `{}`",
                path.join("."),
            ),
            Self::TrustedPresetUnavailable {
                projector,
                model,
                target,
                preset,
            } => write!(
                formatter,
                "direct projection `{projector}`/`{model}` {target} uses unavailable trusted preset `{preset}`",
            ),
            Self::InvalidConstant {
                projector,
                model,
                target,
                error,
            } => write!(
                formatter,
                "direct projection `{projector}`/`{model}` {target} contains an invalid constant: {error}",
            ),
            Self::InvalidTarget {
                projector,
                model,
                reason,
            } => write!(
                formatter,
                "direct projection `{projector}`/`{model}` is invalid: {reason}"
            ),
        }
    }
}

impl std::error::Error for DirectProjectionTargetResolutionError {}

pub(super) fn resolve_trusted_preset(
    session: Option<&Session>,
    name: &str,
    scalar: &str,
) -> Option<serde_json::Value> {
    use base64::Engine as _;

    let raw = session?.get(name)?;
    match scalar {
        "ID" | "String" | "Timestamptz" => Some(serde_json::Value::String(raw.to_string())),
        "Bytea" => {
            let decoded = base64::engine::general_purpose::STANDARD.decode(raw).ok()?;
            (base64::engine::general_purpose::STANDARD.encode(decoded) == raw)
                .then(|| serde_json::Value::String(raw.to_string()))
        }
        "Boolean" => match raw {
            "true" => Some(serde_json::Value::Bool(true)),
            "false" => Some(serde_json::Value::Bool(false)),
            _ => None,
        },
        "Int" => raw
            .parse::<i32>()
            .ok()
            .filter(|value| value.to_string() == raw)
            .map(|value| serde_json::json!(value)),
        "BigInt" => raw
            .parse::<i64>()
            .ok()
            .filter(|value| (-9_007_199_254_740_991..=9_007_199_254_740_991).contains(value))
            .filter(|value| value.to_string() == raw)
            .map(|value| serde_json::json!(value)),
        "Float" => raw
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number),
        "JSON" => serde_json::from_str(raw).ok(),
        _ => None,
    }
}

fn resolve_direct_projection_expression(
    canonical_wire_input: &serde_json::Value,
    target: &CommandDirectProjectionTarget,
    field: &str,
    expression: &EffectExpression,
    session: Option<&Session>,
    expected_scalar: Option<&str>,
) -> Result<serde_json::Value, DirectProjectionTargetResolutionError> {
    match expression {
        EffectExpression::Input { path } => {
            let mut value = canonical_wire_input;
            if path.is_empty() {
                return Err(DirectProjectionTargetResolutionError::MissingInputPath {
                    projector: target.projector.clone(),
                    model: target.model.clone(),
                    target: field.to_string(),
                    path: path.clone(),
                });
            }
            for segment in path {
                let Some(next) = value.as_object().and_then(|object| object.get(segment)) else {
                    return Err(DirectProjectionTargetResolutionError::MissingInputPath {
                        projector: target.projector.clone(),
                        model: target.model.clone(),
                        target: field.to_string(),
                        path: path.clone(),
                    });
                };
                value = next;
            }
            Ok(value.clone())
        }
        EffectExpression::Constant { value } => Ok(value.clone()),
        EffectExpression::Null => Ok(serde_json::Value::Null),
        EffectExpression::TrustedPreset { name } => {
            resolve_trusted_preset(session, name, expected_scalar.unwrap_or("String")).ok_or_else(
                || DirectProjectionTargetResolutionError::TrustedPresetUnavailable {
                    projector: target.projector.clone(),
                    model: target.model.clone(),
                    target: field.to_string(),
                    preset: name.clone(),
                },
            )
        }
        EffectExpression::InvalidConstant { error } => {
            Err(DirectProjectionTargetResolutionError::InvalidConstant {
                projector: target.projector.clone(),
                model: target.model.clone(),
                target: field.to_string(),
                error: error.clone(),
            })
        }
    }
}
