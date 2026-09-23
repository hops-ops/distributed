use serde::{Deserialize, Serialize};

use super::effects::{EffectExpression, EffectKey};
use super::projection_proof::canonical_json;
use crate::projection_protocol::{ProjectionPartitionSpec, ProjectorTopologyId};
use crate::table::TableSchema;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InputDefaultGenerator {
    UuidV7,
    Ulid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CommandInputDefault {
    pub path: Vec<String>,
    pub generator: InputDefaultGenerator,
}

/// One declaration-owned projector/model/key confirmation target.
///
/// The dispatcher resolves these expressions from the retained canonical
/// GraphQL wire input before commit I/O, then commits the finite obligations
/// atomically with the command ledger/fact. Handlers cannot add, remove, or
/// rewrite targets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct CommandProjectionConfirmation {
    pub projector: String,
    pub model: String,
    pub key: EffectKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<EffectExpression>,
    /// Frozen declaration identity used for server-side topology validation
    /// and typed service binding. It is intentionally absent from role client
    /// manifests, whose projector catalog already carries authorized topology.
    #[serde(skip_serializing)]
    pub(super) projector_topology: ProjectorTopologyIdentity,
    /// Exact server-side topology identity compiled from accepted facts, the
    /// versioned scope codec, and every complete owned table schema. Typed
    /// declarations start unbound; Surface/engine compilation must attach this
    /// before an obligation can be lowered or committed.
    #[serde(skip_serializing)]
    pub(super) protocol_topology: Option<ProjectorTopologyId>,
    #[serde(skip_serializing)]
    pub(super) schema: Option<&'static TableSchema>,
}

/// Why a declaration-owned projection obligation could not be resolved before
/// commit I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionObligationResolutionError {
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
    InvalidBinding {
        projector: String,
        model: String,
        reason: String,
    },
}

impl std::fmt::Display for ProjectionObligationResolutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingInputPath {
                projector,
                model,
                target,
                path,
            } => write!(
                formatter,
                "projection obligation `{projector}`/`{model}` {target} references absent canonical input path `{}`",
                path.join("."),
            ),
            Self::TrustedPresetUnavailable {
                projector,
                model,
                target,
                preset,
            } => write!(
                formatter,
                "projection obligation `{projector}`/`{model}` {target} uses unavailable trusted preset `{preset}`",
            ),
            Self::InvalidConstant {
                projector,
                model,
                target,
                error,
            } => write!(
                formatter,
                "projection obligation `{projector}`/`{model}` {target} contains an invalid constant: {error}",
            ),
            Self::InvalidBinding {
                projector,
                model,
                reason,
            } => write!(
                formatter,
                "projection obligation `{projector}`/`{model}` is not bound to an exact topology: {reason}",
            ),
        }
    }
}

impl std::error::Error for ProjectionObligationResolutionError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectorTopologyIdentity {
    name: String,
    pub(super) facts: Vec<String>,
    models: Vec<String>,
    partition: ProjectionPartitionSpec,
}

impl ProjectorTopologyIdentity {
    pub(super) fn new(
        name: &str,
        facts: &[String],
        models: &[String],
        partition: &ProjectionPartitionSpec,
    ) -> Self {
        let mut facts = facts.to_vec();
        facts.sort();
        facts.dedup();
        let mut models = models.to_vec();
        models.sort();
        models.dedup();
        Self {
            name: name.to_string(),
            facts,
            models,
            partition: partition.clone(),
        }
    }

    pub(super) fn canonical_value(&self) -> serde_json::Value {
        canonical_json(&serde_json::json!({
            "name": self.name,
            "facts": self.facts,
            "models": self.models,
            "partition": self.partition,
        }))
    }
}

impl CommandProjectionConfirmation {
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
            "key": self.key,
            "partition": self.partition,
        }))
    }

    pub(crate) fn topology_matches(
        &self,
        name: &str,
        facts: &[String],
        models: &[String],
        partition: &ProjectionPartitionSpec,
    ) -> bool {
        self.projector_topology == ProjectorTopologyIdentity::new(name, facts, models, partition)
    }

    pub(crate) fn bind_protocol_topology(&mut self, topology: ProjectorTopologyId) {
        self.protocol_topology = Some(topology);
    }

    pub(crate) fn protocol_topology(&self) -> Option<&ProjectorTopologyId> {
        self.protocol_topology.as_ref()
    }

    pub(crate) fn clear_protocol_topology(&mut self) {
        self.protocol_topology = None;
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
}

pub(crate) fn validate_projection_confirmation_count(
    command_name: &str,
    count: usize,
) -> Result<(), String> {
    if count > crate::projection_protocol::MAX_PROJECTION_EVIDENCE_BATCH_ITEMS {
        return Err(format!(
            "typed command `{command_name}` declares {count} projector confirmations; maximum is {}",
            crate::projection_protocol::MAX_PROJECTION_EVIDENCE_BATCH_ITEMS
        ));
    }
    Ok(())
}
