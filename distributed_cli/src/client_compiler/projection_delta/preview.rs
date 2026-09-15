use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use super::super::manifest::*;
use super::super::ClientCompileError;
use super::PROJECTION_DELTA_WIRE_VERSION;

const PREVIEW_PLAN_VERSION: u16 = 1;
const MAX_PREVIEW_ITEMS: usize = 128;
const MAX_PROJECTION_ARTIFACT_BYTES: usize = 1024 * 1024;

/// Compiler knowledge is deliberately richer than JavaScript values.
///
/// `Known(Null)` is a value. `Absent` is missing source data; `Unset` is
/// reserved for explicit clearing intent. `Denied` and `CacheUnowned` are
/// authorization/coverage states and never become artifact paths or values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Knowledge {
    Known(PreviewExpression),
    Unknown,
    Absent,
    #[allow(
        dead_code,
        reason = "reserved for explicit projection-assignment clearing, never missing source data"
    )]
    Unset,
    Denied,
    #[allow(
        dead_code,
        reason = "reserved lattice state supplied by the Task 16 replica capability check"
    )]
    CacheUnowned,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum PreviewExpression {
    Input {
        path: Vec<String>,
    },
    GeneratedDefault {
        path: Vec<String>,
    },
    TrustedPreset {
        name: String,
        codec: String,
    },
    Constant {
        value: ManifestProjectionValue,
    },
    Null,
    List {
        values: Vec<PreviewExpression>,
    },
    Object {
        fields: Vec<PreviewObjectField>,
    },
    Transform {
        transform: ManifestProjectionScalarTransform,
        arguments: Vec<PreviewExpression>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct PreviewObjectField {
    name: String,
    value: PreviewExpression,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompiledCommandProjection {
    version: u32,
    delta_wire_version: u16,
    projection_program_version: u32,
    operation_semantics_version: u16,
    projections: Vec<PreviewProjectionIdentity>,
    event_set: Vec<ManifestProjectionEventRef>,
    capabilities: ProjectionCapabilities,
    preview: PreviewPlan,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pure_reduces: Vec<CompiledPureReduce>,
    fallback: ManifestProjectionFallback,
    #[serde(skip)]
    selected_models: BTreeSet<String>,
}

/// Client pure-reduce IR (`pureReduces` on the projection artifact).
///
/// Field names match the rest of the projection preview wire (`occurrence_ordinal`,
/// `projection_refs` snake_case). `client_module` / `client_export` are gen-time
/// only (drive `pures.ts`); inventory is taken from the manifest, not this body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct CompiledPureReduce {
    /// Stable pure id used as pureFunctions key.
    #[serde(rename = "fn")]
    pure_fn: String,
    /// App `$lib`-relative module without extension (for generated pures.ts).
    #[serde(skip)]
    client_module: String,
    /// Named export in that module.
    #[serde(skip)]
    client_export: String,
    scope: PreviewScope,
    args: Vec<CompiledPureArg>,
    assign: Vec<String>,
    occurrence_ordinal: u32,
    projection_refs: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct CompiledPureArg {
    name: String,
    value: PreviewExpression,
}

impl CompiledCommandProjection {
    /// Only emitted mutations consume command presets. Authored occurrences
    /// can lower to recovery without retaining any of their input expressions.
    pub(crate) fn trusted_preset_names(&self) -> BTreeSet<String> {
        let mut names = BTreeSet::new();
        for operation in &self.preview.operations {
            operation.mutation.collect_trusted_presets(&mut names);
        }
        names
    }

    pub(crate) fn affected_models(&self) -> BTreeSet<String> {
        let mut models = BTreeSet::new();
        for operation in &self.preview.operations {
            operation.mutation.collect_models(&mut models);
        }
        for reduce in &self.pure_reduces {
            models.insert(reduce.scope.model.clone());
        }
        for recovery in &self.preview.recoveries {
            recovery.target.collect_models(&mut models);
        }
        models
    }

    pub(crate) fn affected_relationships(
        &self,
        manifest: &ClientManifest,
    ) -> BTreeSet<(String, String, String)> {
        let mut relationships = BTreeSet::new();
        for operation in &self.preview.operations {
            operation.mutation.collect_relationships(&mut relationships);
        }
        for recovery in &self.preview.recoveries {
            if let PreviewRecoveryTarget::Relationship {
                relationship,
                source,
            } = &recovery.target
            {
                relationships.insert((source.model.clone(), relationship.clone(), String::new()));
            }
        }
        relationships
            .into_iter()
            .filter_map(|(source, relationship, target)| {
                if !target.is_empty() {
                    return Some((source, relationship, target));
                }
                let target = manifest
                    .models
                    .get(&source)?
                    .relationship(&relationship)?
                    .target_model
                    .clone();
                Some((source, relationship, target))
            })
            .collect()
    }

    pub(crate) fn requires_revalidation(&self) -> bool {
        self.preview
            .recoveries
            .iter()
            .any(|recovery| recovery.condition == PreviewRecoveryCondition::Always)
    }

    pub(crate) fn selected_models(&self) -> &BTreeSet<String> {
        &self.selected_models
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "camelCase")]
struct PreviewProjectionIdentity {
    program_id: String,
    binding_id: String,
    epoch: String,
    program_ir_version: u16,
    operation_semantics_version: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ProjectionCapabilities {
    version: u16,
    arms: Vec<ProjectionCapabilityArm>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ProjectionCapabilityArm {
    event: ManifestProjectionEventRef,
    projection_ref: u32,
    arm: String,
    partition: ProjectionCapabilityPartition,
    mutations: Vec<ProjectionCapabilityMutation>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ProjectionCapabilityPartition {
    Unit,
    Opaque { expression_fingerprint: String },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ProjectionCapabilityMutation {
    Record {
        model: String,
        key: Vec<String>,
        fields: Vec<String>,
        replace: Vec<String>,
        upsert: bool,
        patch: bool,
        delete: bool,
    },
    Relationship {
        relationship: String,
        source_model: String,
        source_key: Vec<String>,
        target_model: String,
        target_key: Vec<String>,
        link: bool,
        unlink: bool,
    },
    Model {
        model: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PreviewPlan {
    version: u16,
    occurrences: Vec<PreviewOccurrence>,
    operations: Vec<PreviewOperation>,
    recoveries: Vec<PreviewRecovery>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PreviewOccurrence {
    ordinal: u32,
    event: ManifestProjectionEventRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PreviewOperation {
    occurrence_ordinal: u32,
    projection_refs: Vec<u32>,
    mutation: PreviewMutation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct PreviewScope {
    partition: PreviewPartition,
    model: String,
    key: Vec<PreviewKeyField>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PreviewPartition {
    Unit,
    Expression {
        expression: PreviewExpression,
        requires: PreviewPartitionRequirement,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PreviewPartitionRequirement {
    CurrentCachePartition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PreviewKeyField {
    ordinal: u32,
    field: String,
    value: PreviewExpression,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PreviewField {
    field: String,
    value: PreviewExpression,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum PreviewMutation {
    Upsert {
        scope: PreviewScope,
        fields: Vec<PreviewField>,
        replace: Vec<String>,
    },
    Patch {
        scope: PreviewScope,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        set: Vec<PreviewField>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        unset: Vec<String>,
        if_present: bool,
    },
    Delete {
        scope: PreviewScope,
    },
    Link {
        relationship: String,
        source: PreviewScope,
        target: PreviewScope,
    },
    Unlink {
        relationship: String,
        source: PreviewScope,
        target: PreviewScope,
    },
    InvalidateModel {
        #[serde(skip_serializing_if = "Option::is_none")]
        partition: Option<PreviewPartition>,
        model: String,
    },
    InvalidateRelationship {
        relationship: String,
        source: PreviewScope,
    },
}

impl PreviewMutation {
    fn collect_trusted_presets(&self, names: &mut BTreeSet<String>) {
        match self {
            Self::Upsert { scope, fields, .. }
            | Self::Patch {
                scope, set: fields, ..
            } => {
                scope.collect_trusted_presets(names);
                for field in fields {
                    field.value.collect_trusted_presets(names);
                }
            }
            Self::Delete { scope } => scope.collect_trusted_presets(names),
            Self::Link { source, target, .. } | Self::Unlink { source, target, .. } => {
                source.collect_trusted_presets(names);
                target.collect_trusted_presets(names);
            }
            Self::InvalidateRelationship { source, .. } => source.collect_trusted_presets(names),
            Self::InvalidateModel { partition, .. } => {
                if let Some(partition) = partition {
                    partition.collect_trusted_presets(names);
                }
            }
        }
    }

    fn canonical_scope(&self) -> Result<PreviewOperationScope, ClientCompileError> {
        let scope_json = |scope: &PreviewScope| {
            serde_json::to_string(scope).map_err(|error| {
                ClientCompileError::manifest(
                    "client.projection_preview.canonical",
                    format!("cannot canonicalize preview scope: {error}"),
                )
            })
        };
        Ok(match self {
            Self::Upsert { scope, .. } | Self::Patch { scope, .. } | Self::Delete { scope } => {
                PreviewOperationScope::Record(scope_json(scope)?)
            }
            Self::Link {
                relationship,
                source,
                target,
            }
            | Self::Unlink {
                relationship,
                source,
                target,
            } => PreviewOperationScope::Edge {
                relationship: relationship.clone(),
                source: scope_json(source)?,
                target: scope_json(target)?,
            },
            Self::InvalidateModel { partition, model } => PreviewOperationScope::Model {
                partition: partition
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(|error| {
                        ClientCompileError::manifest(
                            "client.projection_preview.canonical",
                            format!("cannot canonicalize preview partition: {error}"),
                        )
                    })?,
                model: model.clone(),
            },
            Self::InvalidateRelationship {
                relationship,
                source,
            } => PreviewOperationScope::Relationship {
                relationship: relationship.clone(),
                source: scope_json(source)?,
            },
        })
    }

    fn collect_models(&self, models: &mut BTreeSet<String>) {
        match self {
            Self::Upsert { scope, .. } | Self::Patch { scope, .. } | Self::Delete { scope } => {
                models.insert(scope.model.clone());
            }
            Self::Link { source, target, .. } | Self::Unlink { source, target, .. } => {
                models.insert(source.model.clone());
                models.insert(target.model.clone());
            }
            Self::InvalidateModel { model, .. } => {
                models.insert(model.clone());
            }
            Self::InvalidateRelationship { source, .. } => {
                models.insert(source.model.clone());
            }
        }
    }

    fn collect_relationships(&self, relationships: &mut BTreeSet<(String, String, String)>) {
        match self {
            Self::Link {
                relationship,
                source,
                target,
            }
            | Self::Unlink {
                relationship,
                source,
                target,
            } => {
                relationships.insert((
                    source.model.clone(),
                    relationship.clone(),
                    target.model.clone(),
                ));
            }
            Self::InvalidateRelationship {
                relationship,
                source,
            } => {
                relationships.insert((source.model.clone(), relationship.clone(), String::new()));
            }
            _ => {}
        }
    }
}

impl PreviewScope {
    fn collect_trusted_presets(&self, names: &mut BTreeSet<String>) {
        self.partition.collect_trusted_presets(names);
        for field in &self.key {
            field.value.collect_trusted_presets(names);
        }
    }
}

impl PreviewPartition {
    fn collect_trusted_presets(&self, names: &mut BTreeSet<String>) {
        if let Self::Expression { expression, .. } = self {
            expression.collect_trusted_presets(names);
        }
    }
}

impl PreviewExpression {
    fn collect_trusted_presets(&self, names: &mut BTreeSet<String>) {
        match self {
            Self::TrustedPreset { name, .. } => {
                names.insert(name.clone());
            }
            Self::List { values }
            | Self::Transform {
                arguments: values, ..
            } => {
                for value in values {
                    value.collect_trusted_presets(names);
                }
            }
            Self::Object { fields } => {
                for field in fields {
                    field.value.collect_trusted_presets(names);
                }
            }
            Self::Input { .. }
            | Self::GeneratedDefault { .. }
            | Self::Constant { .. }
            | Self::Null => {}
        }
    }
}

/// The variant declaration order is the canonical operation-kind order shared
/// with the authoritative ProjectionDelta wire.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum PreviewOperationScope {
    Record(String),
    Edge {
        relationship: String,
        source: String,
        target: String,
    },
    Model {
        partition: Option<String>,
        model: String,
    },
    Relationship {
        relationship: String,
        source: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PreviewRecovery {
    occurrence_ordinal: u32,
    projection_refs: Vec<u32>,
    condition: PreviewRecoveryCondition,
    target: PreviewRecoveryTarget,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
enum PreviewRecoveryCondition {
    Always,
    IfRecordMissing,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PreviewRecoveryTarget {
    Record {
        scope: PreviewScope,
    },
    Relationship {
        relationship: String,
        source: PreviewScope,
    },
    Model {
        #[serde(skip_serializing_if = "Option::is_none")]
        partition: Option<PreviewPartition>,
        model: String,
    },
}

impl PreviewRecoveryTarget {
    fn canonical_key(&self) -> Result<PreviewRecoveryTargetKey, ClientCompileError> {
        let scope_json = |scope: &PreviewScope| {
            serde_json::to_string(scope).map_err(|error| {
                ClientCompileError::manifest(
                    "client.projection_preview.canonical",
                    format!("cannot canonicalize preview recovery scope: {error}"),
                )
            })
        };
        Ok(match self {
            Self::Record { scope } => PreviewRecoveryTargetKey::Record(scope_json(scope)?),
            Self::Relationship {
                relationship,
                source,
            } => PreviewRecoveryTargetKey::Relationship {
                relationship: relationship.clone(),
                source: scope_json(source)?,
            },
            Self::Model { partition, model } => PreviewRecoveryTargetKey::Model {
                partition: partition
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(|error| {
                        ClientCompileError::manifest(
                            "client.projection_preview.canonical",
                            format!("cannot canonicalize preview recovery partition: {error}"),
                        )
                    })?,
                model: model.clone(),
            },
        })
    }

    fn collect_models(&self, models: &mut BTreeSet<String>) {
        match self {
            Self::Record { scope } | Self::Relationship { source: scope, .. } => {
                models.insert(scope.model.clone());
            }
            Self::Model { model, .. } => {
                models.insert(model.clone());
            }
        }
    }
}

/// The variant declaration order is the canonical recovery-kind order shared
/// with the authoritative ProjectionDelta wire.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum PreviewRecoveryTargetKey {
    Record(String),
    Relationship {
        relationship: String,
        source: String,
    },
    Model {
        partition: Option<String>,
        model: String,
    },
}

pub(crate) fn compile_command_preview(
    command: &ManifestCommand,
    manifest: &ClientManifest,
) -> Result<Option<CompiledCommandProjection>, ClientCompileError> {
    let Some(extension) = &command.extensions.projection else {
        return Ok(None);
    };
    let programs = manifest
        .projection_programs
        .iter()
        .map(|program| (program.program_id.as_str(), program))
        .collect::<BTreeMap<_, _>>();
    let selected_program_ids = extension
        .program_arms
        .iter()
        .map(|selected| selected.program_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut identities = Vec::new();
    for program_id in selected_program_ids {
        let program = programs
            .get(program_id)
            .expect("manifest validation proved selected program");
        let binding = manifest
            .projection_bindings
            .iter()
            .find(|binding| {
                binding.program_id == program_id
                    && binding.state == ManifestProjectionBindingState::Active
                    && binding.execution_class == ManifestProjectionExecutionClass::Causal
                    && matches!(
                        binding.placement,
                        ManifestProjectionPlacement::Eventual | ManifestProjectionPlacement::Direct
                    )
            })
            .expect("manifest validation proved one eligible binding");
        identities.push(PreviewProjectionIdentity {
            program_id: program.program_id.clone(),
            binding_id: binding.binding_id.clone(),
            epoch: binding.epoch.clone(),
            program_ir_version: program.ir_version,
            operation_semantics_version: program.operation_semantics_version,
        });
    }
    identities.sort();
    let projection_refs = identities
        .iter()
        .enumerate()
        .map(|(index, identity)| (identity.program_id.as_str(), index as u32))
        .collect::<BTreeMap<_, _>>();
    let mut selected_models = BTreeSet::new();
    for selected in &extension.program_arms {
        let arm = programs[selected.program_id.as_str()]
            .arms
            .iter()
            .find(|arm| arm.arm == selected.arm && arm.event == selected.event)
            .expect("manifest validation proved selected arm");
        collect_selected_models(arm, &mut selected_models);
    }
    let capabilities = extension
        .program_arms
        .iter()
        .map(|selected| {
            let program = programs[selected.program_id.as_str()];
            let arm = program
                .arms
                .iter()
                .find(|arm| arm.arm == selected.arm && arm.event == selected.event)
                .expect("manifest validation proved selected arm");
            compile_capability_arm(
                selected.event.clone(),
                projection_refs[selected.program_id.as_str()],
                arm,
                manifest,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut occurrences = Vec::new();
    let mut operations = Vec::new();
    let mut recoveries = Vec::new();
    for occurrence in &extension.preview_occurrences {
        occurrences.push(PreviewOccurrence {
            ordinal: occurrence.ordinal,
            event: occurrence.event.clone(),
        });
        let slots = occurrence
            .values
            .iter()
            .map(|value| (value.slot.as_str(), source_knowledge(&value.source)))
            .collect::<BTreeMap<_, _>>();
        for selected in extension
            .program_arms
            .iter()
            .filter(|selected| selected.event == occurrence.event)
        {
            let program = programs[selected.program_id.as_str()];
            let arm = program
                .arms
                .iter()
                .find(|arm| arm.arm == selected.arm && arm.event == selected.event)
                .expect("manifest validation proved selected arm");
            let projection_ref = projection_refs[selected.program_id.as_str()];
            lower_arm(
                occurrence.ordinal,
                projection_ref,
                &occurrence.event,
                arm,
                &slots,
                command,
                manifest,
                &mut operations,
                &mut recoveries,
            )?;
        }
    }
    validate_preview_inventory(&operations, &recoveries)?;
    let operations = canonicalize_operations(operations)?;
    let recoveries = canonicalize_recoveries(recoveries, &operations)?;
    let pure_reduces = compile_pure_reduces(extension, &projection_refs, &occurrences)?;
    let compiled = CompiledCommandProjection {
        version: extension.version,
        delta_wire_version: PROJECTION_DELTA_WIRE_VERSION,
        projection_program_version: CLIENT_PROJECTION_PROGRAM_VERSION,
        operation_semantics_version: PROJECTION_OPERATION_SEMANTICS_VERSION,
        projections: identities,
        event_set: extension.event_set.clone(),
        capabilities: ProjectionCapabilities {
            version: 1,
            arms: capabilities,
        },
        preview: PreviewPlan {
            version: PREVIEW_PLAN_VERSION,
            occurrences,
            operations,
            recoveries,
        },
        pure_reduces,
        fallback: extension.fallback,
        selected_models,
    };
    let encoded = serde_json::to_vec(&compiled).map_err(|error| {
        ClientCompileError::manifest(
            "client.projection_artifact.body",
            format!("cannot encode generated projection artifact: {error}"),
        )
    })?;
    if encoded.len() > MAX_PROJECTION_ARTIFACT_BYTES {
        return Err(ClientCompileError::manifest(
            "client.projection_artifact.body",
            format!("generated projection artifact exceeds {MAX_PROJECTION_ARTIFACT_BYTES} bytes"),
        ));
    }
    Ok(Some(compiled))
}

fn compile_capability_arm(
    event: ManifestProjectionEventRef,
    projection_ref: u32,
    arm: &ManifestProjectionArm,
    manifest: &ClientManifest,
) -> Result<ProjectionCapabilityArm, ClientCompileError> {
    let partition = match &arm.partition {
        ManifestProjectionPartition::Unit => ProjectionCapabilityPartition::Unit,
        ManifestProjectionPartition::Expression { expression } => {
            let encoded = serde_json::to_vec(expression).map_err(|error| {
                ClientCompileError::manifest(
                    "client.projection_capability.partition",
                    format!("cannot fingerprint projection partition expression: {error}"),
                )
            })?;
            ProjectionCapabilityPartition::Opaque {
                expression_fingerprint: hash_bytes(&encoded),
            }
        }
    };
    let mut mutations = Vec::new();
    for operation in &arm.operations {
        let model = manifest
            .models
            .get(&operation.model)
            .expect("manifest validation proved operation model");
        if !matches!(
            operation.kind,
            ManifestProjectionMutationKind::InvalidateModel
                | ManifestProjectionMutationKind::InvalidateRelationship
        ) {
            let identity = model
                .identity()
                .expect("projection model has normalized identity");
            let identity_names = identity
                .iter()
                .map(|field| field.name.as_str())
                .collect::<BTreeSet<_>>();
            let mut replace = model
                .fields
                .iter()
                .filter(|field| !identity_names.contains(field.name.as_str()))
                .map(|field| field.name.clone())
                .collect::<Vec<_>>();
            replace.sort();
            let replacement = replace.iter().map(String::as_str).collect::<BTreeSet<_>>();
            let mut fields = operation
                .fields
                .iter()
                .map(|field| field.name.clone())
                .filter(|field| replacement.contains(field.as_str()))
                .collect::<Vec<_>>();
            fields.sort();
            fields.dedup();
            let complete = matches!(
                operation.kind,
                ManifestProjectionMutationKind::Insert
                    | ManifestProjectionMutationKind::Upsert
                    | ManifestProjectionMutationKind::Recreate
                    | ManifestProjectionMutationKind::InsertRelated
                    | ManifestProjectionMutationKind::UpsertRelated
            );
            mutations.push(ProjectionCapabilityMutation::Record {
                model: operation.model.clone(),
                key: operation
                    .key
                    .iter()
                    .map(|field| field.name.clone())
                    .collect(),
                fields: fields.clone(),
                replace: replace.clone(),
                upsert: complete,
                patch: !fields.is_empty(),
                delete: operation.kind == ManifestProjectionMutationKind::Delete,
            });
            mutations.push(ProjectionCapabilityMutation::Model {
                model: operation.model.clone(),
            });
        }
        if operation.kind == ManifestProjectionMutationKind::InvalidateModel {
            mutations.push(ProjectionCapabilityMutation::Model {
                model: operation.model.clone(),
            });
        }
        for effect in &operation.relationships {
            mutations.push(ProjectionCapabilityMutation::Relationship {
                relationship: effect.relationship.clone(),
                source_model: effect.source_model.clone(),
                source_key: effect
                    .source_key
                    .iter()
                    .map(|field| field.name.clone())
                    .collect(),
                target_model: effect.target_model.clone(),
                target_key: effect
                    .target_key
                    .iter()
                    .map(|field| field.name.clone())
                    .collect(),
                link: effect.kind == ManifestProjectionRelationshipEffectKind::Link,
                unlink: effect.kind == ManifestProjectionRelationshipEffectKind::Unlink,
            });
            mutations.push(ProjectionCapabilityMutation::Model {
                model: effect.source_model.clone(),
            });
        }
        for invalidation in &operation.invalidations {
            match invalidation {
                ManifestProjectionInvalidation::Model { model } => {
                    mutations.push(ProjectionCapabilityMutation::Model {
                        model: model.clone(),
                    });
                }
                ManifestProjectionInvalidation::Relationship {
                    source_model,
                    relationship,
                    target_model,
                } => {
                    let keyed = operation.relationships.iter().any(|effect| {
                        effect.kind == ManifestProjectionRelationshipEffectKind::Invalidate
                            && effect.source_model == *source_model
                            && effect.relationship == *relationship
                            && effect.target_model == *target_model
                    });
                    if !keyed {
                        mutations.push(ProjectionCapabilityMutation::Model {
                            model: source_model.clone(),
                        });
                    }
                }
            }
        }
    }
    mutations.sort();
    mutations.dedup();
    Ok(ProjectionCapabilityArm {
        event,
        projection_ref,
        arm: arm.arm.clone(),
        partition,
        mutations,
    })
}

#[allow(clippy::too_many_arguments)]
fn lower_arm(
    occurrence_ordinal: u32,
    projection_ref: u32,
    event: &ManifestProjectionEventRef,
    arm: &ManifestProjectionArm,
    slots: &BTreeMap<&str, Knowledge>,
    command: &ManifestCommand,
    manifest: &ClientManifest,
    operations: &mut Vec<PreviewOperation>,
    recoveries: &mut Vec<PreviewRecovery>,
) -> Result<(), ClientCompileError> {
    let partition = evaluate_partition(&arm.partition, slots, event);
    for operation in &arm.operations {
        let model = manifest
            .models
            .get(&operation.model)
            .expect("manifest validation proved operation model");
        lower_record(
            occurrence_ordinal,
            projection_ref,
            event,
            operation,
            &partition,
            slots,
            command,
            model,
            operations,
            recoveries,
        )?;
        validate_preview_inventory(operations, recoveries)?;
        lower_relationships(
            occurrence_ordinal,
            projection_ref,
            event,
            operation,
            &partition,
            slots,
            command,
            operations,
            recoveries,
        )?;
        validate_preview_inventory(operations, recoveries)?;
        lower_invalidations(
            occurrence_ordinal,
            projection_ref,
            operation,
            &partition,
            operations,
            recoveries,
        )?;
        validate_preview_inventory(operations, recoveries)?;
    }
    Ok(())
}

fn validate_preview_inventory(
    operations: &[PreviewOperation],
    recoveries: &[PreviewRecovery],
) -> Result<(), ClientCompileError> {
    if operations.len() > MAX_PREVIEW_ITEMS || recoveries.len() > MAX_PREVIEW_ITEMS {
        return Err(ClientCompileError::manifest(
            "client.manifest.command_projection_preview_inventory",
            format!(
                "expanded projection preview operations and recoveries cannot exceed \
                 {MAX_PREVIEW_ITEMS} entries"
            ),
        ));
    }
    Ok(())
}

fn push_preview_item<T>(items: &mut Vec<T>, item: T) -> Result<(), ClientCompileError> {
    if items.len() >= MAX_PREVIEW_ITEMS {
        return Err(ClientCompileError::manifest(
            "client.manifest.command_projection_preview_inventory",
            format!(
                "expanded projection preview operations and recoveries cannot exceed \
                 {MAX_PREVIEW_ITEMS} entries"
            ),
        ));
    }
    items.push(item);
    Ok(())
}

fn collect_selected_models(arm: &ManifestProjectionArm, models: &mut BTreeSet<String>) {
    for operation in &arm.operations {
        models.insert(operation.model.clone());
        for relationship in &operation.relationships {
            models.insert(relationship.source_model.clone());
            models.insert(relationship.target_model.clone());
        }
        for invalidation in &operation.invalidations {
            match invalidation {
                ManifestProjectionInvalidation::Model { model } => {
                    models.insert(model.clone());
                }
                ManifestProjectionInvalidation::Relationship {
                    source_model,
                    target_model,
                    ..
                } => {
                    models.insert(source_model.clone());
                    models.insert(target_model.clone());
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn lower_record(
    occurrence_ordinal: u32,
    projection_ref: u32,
    event: &ManifestProjectionEventRef,
    operation: &ManifestProjectionOperation,
    partition: &Option<PreviewPartition>,
    slots: &BTreeMap<&str, Knowledge>,
    command: &ManifestCommand,
    model: &ManifestModel,
    operations: &mut Vec<PreviewOperation>,
    recoveries: &mut Vec<PreviewRecovery>,
) -> Result<(), ClientCompileError> {
    if matches!(
        operation.kind,
        ManifestProjectionMutationKind::InvalidateModel
            | ManifestProjectionMutationKind::InvalidateRelationship
    ) {
        return Ok(());
    }
    let partition = partition.clone();
    let key = evaluate_key(&operation.key, slots, event, command);
    let Some(scope) = partition
        .clone()
        .zip(key)
        .map(|(partition, key)| PreviewScope {
            partition,
            model: operation.model.clone(),
            key,
        })
    else {
        push_preview_item(
            recoveries,
            model_recovery(
                occurrence_ordinal,
                projection_ref,
                operation.model.clone(),
                partition,
            ),
        )?;
        return Ok(());
    };
    let replacement_fields = model
        .fields
        .iter()
        .filter(|field| {
            model
                .identity()
                .is_none_or(|identity| identity.iter().all(|identity| identity.name != field.name))
        })
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    let (set, unset, uncertain) = evaluate_fields(&operation.fields, slots, event);
    match operation.kind {
        ManifestProjectionMutationKind::Insert
        | ManifestProjectionMutationKind::Upsert
        | ManifestProjectionMutationKind::Recreate
        | ManifestProjectionMutationKind::InsertRelated
        | ManifestProjectionMutationKind::UpsertRelated => {
            let mapped = set
                .iter()
                .map(|field| field.field.as_str())
                .collect::<BTreeSet<_>>();
            let complete = !uncertain
                && unset.is_empty()
                && replacement_fields
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>()
                    == mapped;
            if complete {
                push_preview_item(
                    operations,
                    PreviewOperation {
                        occurrence_ordinal,
                        projection_refs: vec![projection_ref],
                        mutation: PreviewMutation::Upsert {
                            scope,
                            fields: set,
                            replace: replacement_fields,
                        },
                    },
                )?;
            } else if !set.is_empty() || !unset.is_empty() {
                push_preview_item(
                    operations,
                    PreviewOperation {
                        occurrence_ordinal,
                        projection_refs: vec![projection_ref],
                        mutation: PreviewMutation::Patch {
                            scope: scope.clone(),
                            set,
                            unset,
                            if_present: true,
                        },
                    },
                )?;
                push_preview_item(
                    recoveries,
                    record_recovery(
                        occurrence_ordinal,
                        projection_ref,
                        scope,
                        PreviewRecoveryCondition::IfRecordMissing,
                    ),
                )?;
            } else {
                push_preview_item(
                    recoveries,
                    record_recovery(
                        occurrence_ordinal,
                        projection_ref,
                        scope,
                        PreviewRecoveryCondition::Always,
                    ),
                )?;
            }
        }
        ManifestProjectionMutationKind::Patch | ManifestProjectionMutationKind::UpsertPatch => {
            if !set.is_empty() || !unset.is_empty() {
                push_preview_item(
                    operations,
                    PreviewOperation {
                        occurrence_ordinal,
                        projection_refs: vec![projection_ref],
                        mutation: PreviewMutation::Patch {
                            scope: scope.clone(),
                            set,
                            unset,
                            if_present: true,
                        },
                    },
                )?;
                push_preview_item(
                    recoveries,
                    record_recovery(
                        occurrence_ordinal,
                        projection_ref,
                        scope,
                        PreviewRecoveryCondition::IfRecordMissing,
                    ),
                )?;
            } else {
                push_preview_item(
                    recoveries,
                    record_recovery(
                        occurrence_ordinal,
                        projection_ref,
                        scope,
                        PreviewRecoveryCondition::Always,
                    ),
                )?;
            }
        }
        ManifestProjectionMutationKind::Delete => push_preview_item(
            operations,
            PreviewOperation {
                occurrence_ordinal,
                projection_refs: vec![projection_ref],
                mutation: PreviewMutation::Delete { scope },
            },
        )?,
        ManifestProjectionMutationKind::InvalidateModel
        | ManifestProjectionMutationKind::InvalidateRelationship => unreachable!(),
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn lower_relationships(
    occurrence_ordinal: u32,
    projection_ref: u32,
    event: &ManifestProjectionEventRef,
    operation: &ManifestProjectionOperation,
    partition: &Option<PreviewPartition>,
    slots: &BTreeMap<&str, Knowledge>,
    command: &ManifestCommand,
    operations: &mut Vec<PreviewOperation>,
    recoveries: &mut Vec<PreviewRecovery>,
) -> Result<(), ClientCompileError> {
    let partition = partition.clone();
    for effect in &operation.relationships {
        let source = partition
            .clone()
            .zip(evaluate_key(&effect.source_key, slots, event, command))
            .map(|(partition, key)| PreviewScope {
                partition,
                model: effect.source_model.clone(),
                key,
            });
        let target = partition
            .clone()
            .zip(evaluate_key(&effect.target_key, slots, event, command))
            .map(|(partition, key)| PreviewScope {
                partition,
                model: effect.target_model.clone(),
                key,
            });
        match (effect.kind, source, target) {
            (ManifestProjectionRelationshipEffectKind::Link, Some(source), Some(target)) => {
                push_preview_item(
                    operations,
                    PreviewOperation {
                        occurrence_ordinal,
                        projection_refs: vec![projection_ref],
                        mutation: PreviewMutation::Link {
                            relationship: effect.relationship.clone(),
                            source,
                            target,
                        },
                    },
                )?;
            }
            (ManifestProjectionRelationshipEffectKind::Unlink, Some(source), Some(target)) => {
                push_preview_item(
                    operations,
                    PreviewOperation {
                        occurrence_ordinal,
                        projection_refs: vec![projection_ref],
                        mutation: PreviewMutation::Unlink {
                            relationship: effect.relationship.clone(),
                            source,
                            target,
                        },
                    },
                )?;
            }
            (_, Some(source), _) => {
                push_preview_item(
                    operations,
                    PreviewOperation {
                        occurrence_ordinal,
                        projection_refs: vec![projection_ref],
                        mutation: PreviewMutation::InvalidateRelationship {
                            relationship: effect.relationship.clone(),
                            source: source.clone(),
                        },
                    },
                )?;
                push_preview_item(
                    recoveries,
                    relationship_recovery(
                        occurrence_ordinal,
                        projection_ref,
                        effect.relationship.clone(),
                        source,
                    ),
                )?;
            }
            (_, None, _) => push_preview_item(
                recoveries,
                model_recovery(
                    occurrence_ordinal,
                    projection_ref,
                    effect.source_model.clone(),
                    partition.clone(),
                ),
            )?,
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn lower_invalidations(
    occurrence_ordinal: u32,
    projection_ref: u32,
    operation: &ManifestProjectionOperation,
    partition: &Option<PreviewPartition>,
    operations: &mut Vec<PreviewOperation>,
    recoveries: &mut Vec<PreviewRecovery>,
) -> Result<(), ClientCompileError> {
    let partition = partition.clone();
    if operation.kind == ManifestProjectionMutationKind::InvalidateModel {
        push_preview_item(
            operations,
            PreviewOperation {
                occurrence_ordinal,
                projection_refs: vec![projection_ref],
                mutation: PreviewMutation::InvalidateModel {
                    partition: partition.clone(),
                    model: operation.model.clone(),
                },
            },
        )?;
        push_preview_item(
            recoveries,
            model_recovery(
                occurrence_ordinal,
                projection_ref,
                operation.model.clone(),
                partition.clone(),
            ),
        )?;
    }
    for invalidation in &operation.invalidations {
        match invalidation {
            ManifestProjectionInvalidation::Model { model } => {
                push_preview_item(
                    operations,
                    PreviewOperation {
                        occurrence_ordinal,
                        projection_refs: vec![projection_ref],
                        mutation: PreviewMutation::InvalidateModel {
                            partition: partition.clone(),
                            model: model.clone(),
                        },
                    },
                )?;
                push_preview_item(
                    recoveries,
                    model_recovery(
                        occurrence_ordinal,
                        projection_ref,
                        model.clone(),
                        partition.clone(),
                    ),
                )?;
            }
            ManifestProjectionInvalidation::Relationship {
                source_model,
                relationship,
                target_model,
            } => {
                let has_keyed_effect = operation.relationships.iter().any(|effect| {
                    effect.kind == ManifestProjectionRelationshipEffectKind::Invalidate
                        && effect.source_model == *source_model
                        && effect.relationship == *relationship
                        && effect.target_model == *target_model
                });
                if has_keyed_effect {
                    continue;
                }
                // A provenance-only relationship invalidation has no source
                // key of its own. The record operation key may belong to a
                // different model and is never authority for an edge scope.
                push_preview_item(
                    operations,
                    PreviewOperation {
                        occurrence_ordinal,
                        projection_refs: vec![projection_ref],
                        mutation: PreviewMutation::InvalidateModel {
                            partition: partition.clone(),
                            model: source_model.clone(),
                        },
                    },
                )?;
                push_preview_item(
                    recoveries,
                    model_recovery(
                        occurrence_ordinal,
                        projection_ref,
                        source_model.clone(),
                        partition.clone(),
                    ),
                )?;
            }
        }
    }
    Ok(())
}

fn evaluate_partition(
    partition: &ManifestProjectionPartition,
    slots: &BTreeMap<&str, Knowledge>,
    event: &ManifestProjectionEventRef,
) -> Option<PreviewPartition> {
    match partition {
        ManifestProjectionPartition::Unit => Some(PreviewPartition::Unit),
        ManifestProjectionPartition::Expression { expression } => {
            match evaluate_expression(expression, slots, event) {
                // This is an executable logical expression, never a server
                // projection-partition token. The runtime must prove it maps
                // to the replica's already-authorized current cache partition
                // before applying any row or edge mutation.
                Knowledge::Known(expression) => Some(PreviewPartition::Expression {
                    expression,
                    requires: PreviewPartitionRequirement::CurrentCachePartition,
                }),
                Knowledge::Unknown
                | Knowledge::Absent
                | Knowledge::Unset
                | Knowledge::Denied
                | Knowledge::CacheUnowned => None,
            }
        }
    }
}

fn evaluate_key(
    key: &[ManifestProjectionKeyField],
    slots: &BTreeMap<&str, Knowledge>,
    event: &ManifestProjectionEventRef,
    command: &ManifestCommand,
) -> Option<Vec<PreviewKeyField>> {
    let mut result = Vec::with_capacity(key.len());
    for field in key {
        let Knowledge::Known(value) = evaluate_expression(&field.expression, slots, event) else {
            return None;
        };
        if !preview_key_value_is_provably_non_null_scalar(&value, command) {
            return None;
        }
        result.push(PreviewKeyField {
            ordinal: field.ordinal,
            field: field.name.clone(),
            value,
        });
    }
    Some(result)
}

fn preview_key_value_is_provably_non_null_scalar(
    value: &PreviewExpression,
    command: &ManifestCommand,
) -> bool {
    match value {
        PreviewExpression::Input { path } => {
            command_input_field(&command.input, path).is_some_and(|field| {
                !field.nullable
                    && !field.list
                    && field.nested.is_none()
                    && field.codec.as_deref() != Some("json")
            })
        }
        PreviewExpression::GeneratedDefault { path } => command
            .extensions
            .input_defaults
            .as_ref()
            .is_some_and(|defaults| {
                defaults
                    .defaults
                    .iter()
                    .any(|default| default.path == *path)
            }),
        PreviewExpression::TrustedPreset { codec, .. } => codec != "json",
        PreviewExpression::Constant { value } => matches!(
            value,
            ManifestProjectionValue::Boolean(_)
                | ManifestProjectionValue::I64(_)
                | ManifestProjectionValue::U64(_)
                | ManifestProjectionValue::F64(_)
                | ManifestProjectionValue::String(_)
                | ManifestProjectionValue::Enum { .. }
        ),
        PreviewExpression::Null
        | PreviewExpression::List { .. }
        | PreviewExpression::Object { .. } => false,
        PreviewExpression::Transform {
            transform,
            arguments,
        } => match transform {
            ManifestProjectionScalarTransform::StringConcat => arguments
                .iter()
                .all(|argument| preview_key_value_is_provably_non_null_scalar(argument, command)),
            ManifestProjectionScalarTransform::FirstPresent => {
                arguments.first().is_some_and(|argument| {
                    preview_key_value_is_provably_non_null_scalar(argument, command)
                })
            }
        },
    }
}

fn command_input_field<'a>(
    shape: &'a ManifestCommandShape,
    path: &[String],
) -> Option<&'a ManifestTypeField> {
    let ManifestCommandShape::Object { definition } = shape else {
        return None;
    };
    let mut current = definition;
    for (index, segment) in path.iter().enumerate() {
        let field = current.fields.iter().find(|field| field.name == *segment)?;
        if index + 1 == path.len() {
            return Some(field);
        }
        if field.nullable || field.list {
            return None;
        }
        current = field.nested.as_deref()?;
    }
    None
}

fn evaluate_fields(
    fields: &[ManifestProjectionField],
    slots: &BTreeMap<&str, Knowledge>,
    event: &ManifestProjectionEventRef,
) -> (Vec<PreviewField>, Vec<String>, bool) {
    let mut set = Vec::new();
    let mut unset = Vec::new();
    let mut uncertain = false;
    for field in fields {
        match &field.assignment {
            ManifestProjectionAssignment::Unset => unset.push(field.name.clone()),
            ManifestProjectionAssignment::Set { expression } => {
                match evaluate_expression(expression, slots, event) {
                    Knowledge::Known(value) => set.push(PreviewField {
                        field: field.name.clone(),
                        value,
                    }),
                    Knowledge::Unknown
                    | Knowledge::Absent
                    | Knowledge::Unset
                    | Knowledge::Denied
                    | Knowledge::CacheUnowned => uncertain = true,
                }
            }
        }
    }
    set.sort_by(|left, right| left.field.cmp(&right.field));
    unset.sort();
    (set, unset, uncertain)
}

fn source_knowledge(source: &ManifestProjectionPreviewSource) -> Knowledge {
    match source {
        ManifestProjectionPreviewSource::Input { path } => {
            Knowledge::Known(PreviewExpression::Input { path: path.clone() })
        }
        ManifestProjectionPreviewSource::GeneratedDefault { path } => {
            Knowledge::Known(PreviewExpression::GeneratedDefault { path: path.clone() })
        }
        ManifestProjectionPreviewSource::TrustedPreset { name, codec } => {
            Knowledge::Known(PreviewExpression::TrustedPreset {
                name: name.clone(),
                codec: codec.clone(),
            })
        }
        ManifestProjectionPreviewSource::Constant { value } => {
            Knowledge::Known(PreviewExpression::Constant {
                value: value.clone(),
            })
        }
        ManifestProjectionPreviewSource::Null => Knowledge::Known(PreviewExpression::Null),
        ManifestProjectionPreviewSource::Absent => Knowledge::Absent,
        ManifestProjectionPreviewSource::Unknown => Knowledge::Unknown,
    }
}

fn compile_pure_reduces(
    extension: &super::super::manifest::ManifestCommandProjection,
    projection_refs: &BTreeMap<&str, u32>,
    occurrences: &[PreviewOccurrence],
) -> Result<Vec<CompiledPureReduce>, ClientCompileError> {
    if extension.pure_reduces.is_empty() {
        return Ok(Vec::new());
    }
    // Pure reduce is bound to the first preview occurrence when present so the
    // JS validator's occurrence_ordinal < occurrenceCount check passes. Pure
    // still needs at least one selected program arm (projection_refs).
    if projection_refs.is_empty() {
        return Err(ClientCompileError::manifest(
            "client.projection_pure_reduce",
            "pure reduce requires at least one selected projection program",
        ));
    }
    if occurrences.is_empty() {
        return Err(ClientCompileError::manifest(
            "client.projection_pure_reduce",
            "pure reduce requires at least one preview occurrence so auto-optimism can order the overlay",
        ));
    }
    let occurrence_ordinal = 0u32;
    let mut refs: Vec<u32> = projection_refs.values().copied().collect();
    refs.sort_unstable();
    refs.dedup();
    let mut compiled = Vec::with_capacity(extension.pure_reduces.len());
    for reduce in &extension.pure_reduces {
        if reduce.key.is_empty() {
            return Err(ClientCompileError::manifest(
                "client.projection_pure_reduce",
                format!(
                    "pure reduce `{}` requires at least one key field",
                    reduce.fn_name
                ),
            ));
        }
        if reduce.assign.is_empty() {
            return Err(ClientCompileError::manifest(
                "client.projection_pure_reduce",
                format!(
                    "pure reduce `{}` requires at least one assign field",
                    reduce.fn_name
                ),
            ));
        }
        compiled.push(compile_one_pure_reduce(
            reduce,
            occurrence_ordinal,
            refs.clone(),
        )?);
    }
    Ok(compiled)
}

fn compile_one_pure_reduce(
    reduce: &super::super::manifest::ManifestCommandPureReduce,
    occurrence_ordinal: u32,
    projection_refs: Vec<u32>,
) -> Result<CompiledPureReduce, ClientCompileError> {
    let mut key = Vec::with_capacity(reduce.key.len());
    for (ordinal, field) in reduce.key.iter().enumerate() {
        let Knowledge::Known(value) = source_knowledge(&field.source) else {
            return Err(ClientCompileError::manifest(
                "client.projection_pure_reduce",
                format!(
                    "pure reduce `{}` key `{}` must resolve from input, default, or trusted preset",
                    reduce.fn_name, field.name
                ),
            ));
        };
        key.push(PreviewKeyField {
            ordinal: ordinal as u32,
            field: field.name.clone(),
            value,
        });
    }
    let mut args = Vec::with_capacity(reduce.args.len());
    for arg in &reduce.args {
        let Knowledge::Known(value) = source_knowledge(&arg.source) else {
            return Err(ClientCompileError::manifest(
                "client.projection_pure_reduce",
                format!(
                    "pure reduce `{}` arg `{}` must resolve from input, default, or trusted preset",
                    reduce.fn_name, arg.name
                ),
            ));
        };
        args.push(CompiledPureArg {
            name: arg.name.clone(),
            value,
        });
    }
    Ok(CompiledPureReduce {
        pure_fn: reduce.fn_name.clone(),
        client_module: reduce.client_module.clone(),
        client_export: reduce.client_export.clone(),
        scope: PreviewScope {
            partition: PreviewPartition::Unit,
            model: reduce.model.clone(),
            key,
        },
        args,
        assign: reduce.assign.clone(),
        occurrence_ordinal,
        projection_refs,
    })
}

fn evaluate_expression(
    expression: &ManifestProjectionExpression,
    slots: &BTreeMap<&str, Knowledge>,
    event: &ManifestProjectionEventRef,
) -> Knowledge {
    match expression {
        ManifestProjectionExpression::Slot { slot, .. } => slots
            .get(slot.as_str())
            .cloned()
            .unwrap_or(Knowledge::Denied),
        ManifestProjectionExpression::Envelope { field } => {
            let value = match field {
                ManifestProjectionEnvelopeField::OccurrenceVersion => {
                    ManifestProjectionValue::U64("1".into())
                }
                ManifestProjectionEnvelopeField::EventName => {
                    ManifestProjectionValue::String(event.name.clone())
                }
                ManifestProjectionEnvelopeField::EventVersion => {
                    ManifestProjectionValue::U64(event.version.to_string())
                }
            };
            Knowledge::Known(PreviewExpression::Constant { value })
        }
        ManifestProjectionExpression::Constant { value } => {
            Knowledge::Known(PreviewExpression::Constant {
                value: value.clone(),
            })
        }
        ManifestProjectionExpression::Enum { enum_type, variant } => {
            Knowledge::Known(PreviewExpression::Constant {
                value: ManifestProjectionValue::Enum {
                    enum_type: enum_type.clone(),
                    variant: variant.clone(),
                },
            })
        }
        ManifestProjectionExpression::List { values } => {
            let mut resolved = Vec::with_capacity(values.len());
            for value in values {
                match evaluate_expression(value, slots, event) {
                    Knowledge::Known(value) => resolved.push(value),
                    other => return other,
                }
            }
            Knowledge::Known(PreviewExpression::List { values: resolved })
        }
        ManifestProjectionExpression::Object { fields } => {
            let mut resolved = Vec::with_capacity(fields.len());
            for field in fields {
                match evaluate_expression(&field.value, slots, event) {
                    Knowledge::Known(value) => resolved.push(PreviewObjectField {
                        name: field.name.clone(),
                        value,
                    }),
                    // Frozen server semantics reject Unset inside a composite.
                    // The client cannot reproduce that command-side error, so
                    // preserve cache state and recover instead.
                    Knowledge::Unset => return Knowledge::Unknown,
                    other => return other,
                }
            }
            Knowledge::Known(PreviewExpression::Object { fields: resolved })
        }
        ManifestProjectionExpression::Transform {
            transform,
            arguments,
        } => {
            let mut resolved = Vec::new();
            for argument in arguments {
                match evaluate_expression(argument, slots, event) {
                    Knowledge::Known(value) => resolved.push(value),
                    Knowledge::Absent
                        if *transform == ManifestProjectionScalarTransform::FirstPresent => {}
                    Knowledge::Unset
                        if *transform == ManifestProjectionScalarTransform::FirstPresent =>
                    {
                        return Knowledge::Unknown;
                    }
                    other => return other,
                }
            }
            if resolved.is_empty() {
                Knowledge::Absent
            } else {
                Knowledge::Known(PreviewExpression::Transform {
                    transform: *transform,
                    arguments: resolved,
                })
            }
        }
    }
}

fn canonicalize_operations(
    operations: Vec<PreviewOperation>,
) -> Result<Vec<PreviewOperation>, ClientCompileError> {
    let mut by_scope = BTreeMap::<PreviewOperationScope, PreviewOperation>::new();
    for operation in operations {
        let scope = operation.mutation.canonical_scope()?;
        if let Some(existing) = by_scope.get_mut(&scope) {
            merge_operation(existing, operation)?;
        } else {
            by_scope.insert(scope, operation);
        }
    }
    Ok(by_scope.into_values().collect())
}

fn canonicalize_recoveries(
    recoveries: Vec<PreviewRecovery>,
    operations: &[PreviewOperation],
) -> Result<Vec<PreviewRecovery>, ClientCompileError> {
    let mut by_target = BTreeMap::<PreviewRecoveryTargetKey, PreviewRecovery>::new();
    for recovery in recoveries {
        let target = recovery.target.canonical_key()?;
        by_target
            .entry(target)
            .and_modify(|existing| {
                existing.occurrence_ordinal =
                    existing.occurrence_ordinal.max(recovery.occurrence_ordinal);
                existing.projection_refs.extend(&recovery.projection_refs);
                existing.projection_refs.sort();
                existing.projection_refs.dedup();
                if recovery.condition == PreviewRecoveryCondition::Always {
                    existing.condition = PreviewRecoveryCondition::Always;
                }
            })
            .or_insert(recovery);
    }
    Ok(by_target
        .into_values()
        .filter(|recovery| {
            if recovery.condition == PreviewRecoveryCondition::Always {
                return true;
            }
            let PreviewRecoveryTarget::Record { scope } = &recovery.target else {
                return false;
            };
            operations.iter().any(|operation| {
                matches!(
                    &operation.mutation,
                    PreviewMutation::Patch {
                        scope: patch_scope,
                        if_present: true,
                        ..
                    } if patch_scope == scope
                )
            })
        })
        .collect())
}

fn merge_operation(
    existing: &mut PreviewOperation,
    incoming: PreviewOperation,
) -> Result<(), ClientCompileError> {
    if existing.occurrence_ordinal == incoming.occurrence_ordinal {
        if existing.mutation != incoming.mutation {
            return Err(incompatible_final_mutations(
                "same occurrence cannot contribute different mutations to one scope",
            ));
        }
        merge_refs(&mut existing.projection_refs, incoming.projection_refs);
        return Ok(());
    }
    let (earlier, later) = if existing.occurrence_ordinal < incoming.occurrence_ordinal {
        (existing.mutation.clone(), incoming.mutation)
    } else {
        (incoming.mutation, existing.mutation.clone())
    };
    existing.mutation = merge_mutation(earlier, later)?;
    existing.occurrence_ordinal = existing.occurrence_ordinal.max(incoming.occurrence_ordinal);
    merge_refs(&mut existing.projection_refs, incoming.projection_refs);
    Ok(())
}

fn merge_mutation(
    existing: PreviewMutation,
    incoming: PreviewMutation,
) -> Result<PreviewMutation, ClientCompileError> {
    if existing.canonical_scope()? != incoming.canonical_scope()? {
        return Err(incompatible_final_mutations(
            "same scope resolves to incompatible final mutations",
        ));
    }
    match (existing, incoming) {
        (
            PreviewMutation::Patch {
                scope,
                set,
                unset,
                if_present,
            },
            PreviewMutation::Patch {
                set: incoming_set,
                unset: incoming_unset,
                if_present: incoming_present,
                ..
            },
        ) => {
            let mut values = set
                .into_iter()
                .map(|field| (field.field.clone(), Some(field)))
                .collect::<BTreeMap<_, _>>();
            for field in unset {
                values.insert(field, None);
            }
            for field in incoming_set {
                values.insert(field.field.clone(), Some(field));
            }
            for field in incoming_unset {
                values.insert(field, None);
            }
            let set = values.values().filter_map(Clone::clone).collect();
            let unset = values
                .into_iter()
                .filter_map(|(field, value)| value.is_none().then_some(field))
                .collect();
            Ok(PreviewMutation::Patch {
                scope,
                set,
                unset,
                if_present: if_present && incoming_present,
            })
        }
        (
            PreviewMutation::Upsert {
                scope,
                fields,
                replace,
            },
            PreviewMutation::Patch {
                set,
                unset,
                if_present: true,
                ..
            },
        ) => {
            let mut values = fields
                .into_iter()
                .map(|field| (field.field.clone(), field))
                .collect::<BTreeMap<_, _>>();
            let replace = replace.into_iter().collect::<BTreeSet<_>>();
            if set
                .iter()
                .map(|field| &field.field)
                .chain(unset.iter())
                .any(|field| !replace.contains(field))
            {
                return Err(incompatible_final_mutations(
                    "patch fields must belong to the upsert replacement mask",
                ));
            }
            for field in set {
                values.insert(field.field.clone(), field);
            }
            for field in unset {
                values.remove(&field);
            }
            Ok(PreviewMutation::Upsert {
                scope,
                fields: values.into_values().collect(),
                replace: replace.into_iter().collect(),
            })
        }
        (PreviewMutation::Patch { .. }, incoming @ PreviewMutation::Upsert { .. })
        | (PreviewMutation::Patch { .. }, incoming @ PreviewMutation::Delete { .. })
        | (PreviewMutation::Upsert { .. }, incoming @ PreviewMutation::Upsert { .. })
        | (PreviewMutation::Upsert { .. }, incoming @ PreviewMutation::Delete { .. })
        | (PreviewMutation::Delete { .. }, incoming @ PreviewMutation::Upsert { .. })
        | (PreviewMutation::Delete { .. }, incoming @ PreviewMutation::Delete { .. }) => {
            Ok(incoming)
        }
        (existing @ PreviewMutation::Delete { .. }, PreviewMutation::Patch { .. }) => Ok(existing),
        (
            PreviewMutation::Link { .. } | PreviewMutation::Unlink { .. },
            incoming @ (PreviewMutation::Link { .. } | PreviewMutation::Unlink { .. }),
        )
        | (
            PreviewMutation::InvalidateModel { .. },
            incoming @ PreviewMutation::InvalidateModel { .. },
        )
        | (
            PreviewMutation::InvalidateRelationship { .. },
            incoming @ PreviewMutation::InvalidateRelationship { .. },
        ) => Ok(incoming),
        _ => Err(incompatible_final_mutations(
            "same scope resolves to incompatible final mutations",
        )),
    }
}

fn merge_refs(existing: &mut Vec<u32>, incoming: Vec<u32>) {
    let mut refs = existing.iter().copied().collect::<BTreeSet<_>>();
    refs.extend(incoming);
    *existing = refs.into_iter().collect();
}

fn incompatible_final_mutations(message: &'static str) -> ClientCompileError {
    ClientCompileError::manifest("client.projection_preview.ambiguous_scope", message)
}

fn record_recovery(
    occurrence_ordinal: u32,
    projection_ref: u32,
    scope: PreviewScope,
    condition: PreviewRecoveryCondition,
) -> PreviewRecovery {
    PreviewRecovery {
        occurrence_ordinal,
        projection_refs: vec![projection_ref],
        condition,
        target: PreviewRecoveryTarget::Record { scope },
    }
}

fn relationship_recovery(
    occurrence_ordinal: u32,
    projection_ref: u32,
    relationship: String,
    source: PreviewScope,
) -> PreviewRecovery {
    PreviewRecovery {
        occurrence_ordinal,
        projection_refs: vec![projection_ref],
        condition: PreviewRecoveryCondition::Always,
        target: PreviewRecoveryTarget::Relationship {
            relationship,
            source,
        },
    }
}

fn model_recovery(
    occurrence_ordinal: u32,
    projection_ref: u32,
    model: String,
    partition: Option<PreviewPartition>,
) -> PreviewRecovery {
    PreviewRecovery {
        occurrence_ordinal,
        projection_refs: vec![projection_ref],
        condition: PreviewRecoveryCondition::Always,
        target: PreviewRecoveryTarget::Model { partition, model },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emitted_mutation_presets_include_nested_values_keys_and_partitions_only() {
        let preset = |name: &str| PreviewExpression::TrustedPreset {
            name: name.into(),
            codec: "string".into(),
        };
        let scope = PreviewScope {
            model: "Todo".into(),
            partition: PreviewPartition::Expression {
                expression: preset("tenant"),
                requires: PreviewPartitionRequirement::CurrentCachePartition,
            },
            key: vec![PreviewKeyField {
                ordinal: 0,
                field: "id".into(),
                value: preset("actor"),
            }],
        };
        let mutation = PreviewMutation::Upsert {
            scope: scope.clone(),
            fields: vec![PreviewField {
                field: "title".into(),
                value: PreviewExpression::Object {
                    fields: vec![PreviewObjectField {
                        name: "nested".into(),
                        value: PreviewExpression::List {
                            values: vec![
                                preset("label"),
                                preset("actor"),
                                PreviewExpression::Constant {
                                    value: ManifestProjectionValue::String("trusted_preset".into()),
                                },
                            ],
                        },
                    }],
                },
            }],
            replace: vec![],
        };
        let mut names = BTreeSet::new();
        mutation.collect_trusted_presets(&mut names);
        assert_eq!(names, ["actor", "label", "tenant"].map(String::from).into());
        names.clear();
        PreviewMutation::Delete { scope }.collect_trusted_presets(&mut names);
        assert_eq!(names, ["actor", "tenant"].map(String::from).into());
        names.clear();
        PreviewMutation::InvalidateModel {
            model: "Todo".into(),
            partition: Some(PreviewPartition::Unit),
        }
        .collect_trusted_presets(&mut names);
        assert!(names.is_empty());
    }

    fn event() -> ManifestProjectionEventRef {
        ManifestProjectionEventRef {
            id: "event:test:v1".into(),
            name: "test.changed".into(),
            version: 1,
        }
    }

    fn slot() -> ManifestProjectionExpression {
        ManifestProjectionExpression::Slot {
            slot: "value".into(),
            value_type: ManifestProjectionValueType::String,
        }
    }

    fn key_command() -> ManifestCommand {
        ManifestCommand {
            version: 1,
            name: "test".into(),
            mutation_field: "test".into(),
            grants: vec!["user".into()],
            input: ManifestCommandShape::Object {
                definition: ManifestTypeDef {
                    name: "TestInput".into(),
                    fields: ["source", "target"]
                        .into_iter()
                        .map(|name| ManifestTypeField {
                            name: name.into(),
                            type_name: "ID".into(),
                            nullable: false,
                            list: false,
                            item_nullable: false,
                            codec: Some("string".into()),
                            nested: None,
                        })
                        .collect(),
                },
            },
            output: ManifestCommandShape::None,
            operation: "mutation Test { test }".into(),
            operation_hash:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000".into(),
            extensions: ManifestCommandExtensions {
                version: 2,
                consistency: ManifestCommandConsistency {
                    version: 1,
                    kind: ManifestConsistencyKind::Eventual,
                },
                direct_projection: None,
                input_defaults: None,
                effects: None,
                confirmations: None,
                projection: None,
                trusted_presets: Vec::new(),
            },
        }
    }

    fn projection_model() -> ManifestModel {
        ManifestModel {
            id: "Todo".into(),
            typename: "Todo".into(),
            source_table: "todos".into(),
            dependencies: vec!["todos".into()],
            normalization: ManifestNormalization::Normalized {
                fields: vec![ManifestKeyField {
                    name: "id".into(),
                    codec: "string".into(),
                }],
                encoding: "tuple_v1".into(),
            },
            fields: vec![
                ManifestField {
                    name: "id".into(),
                    scalar: "ID".into(),
                    codec: "string".into(),
                    nullable: false,
                },
                ManifestField {
                    name: "metadata".into(),
                    scalar: "JSON".into(),
                    codec: "json".into(),
                    nullable: true,
                },
            ],
            relationships: Vec::new(),
            filter_input: ManifestFilterInput {
                type_name: "TodoBoolExp".into(),
                fields: Vec::new(),
                relationships: Vec::new(),
            },
            row_policy: ManifestRowPolicy::Unrestricted,
            record_revisions: false,
            tombstones: false,
        }
    }

    fn preview_scope(model: &str) -> PreviewScope {
        PreviewScope {
            partition: PreviewPartition::Unit,
            model: model.into(),
            key: vec![PreviewKeyField {
                ordinal: 0,
                field: "id".into(),
                value: PreviewExpression::Input {
                    path: vec!["id".into()],
                },
            }],
        }
    }

    fn preview_field(name: &str, path: &str) -> PreviewField {
        PreviewField {
            field: name.into(),
            value: PreviewExpression::Input {
                path: vec![path.into()],
            },
        }
    }

    fn operation(occurrence_ordinal: u32, mutation: PreviewMutation) -> PreviewOperation {
        PreviewOperation {
            occurrence_ordinal,
            projection_refs: vec![0],
            mutation,
        }
    }

    fn relationship_operation() -> ManifestProjectionOperation {
        let key = |slot: &str| {
            vec![ManifestProjectionKeyField {
                ordinal: 0,
                name: "id".into(),
                expression: ManifestProjectionExpression::Slot {
                    slot: slot.into(),
                    value_type: ManifestProjectionValueType::String,
                },
            }]
        };
        ManifestProjectionOperation {
            operation: "link-owner".into(),
            ordinal: 0,
            kind: ManifestProjectionMutationKind::Patch,
            model: "Todo".into(),
            key: key("source"),
            fields: Vec::new(),
            relationships: vec![ManifestProjectionRelationshipEffect {
                ordinal: 0,
                kind: ManifestProjectionRelationshipEffectKind::Link,
                source_model: "Todo".into(),
                relationship: "owner".into(),
                target_model: "User".into(),
                source_key: key("source"),
                target_key: key("target"),
            }],
            invalidations: Vec::new(),
        }
    }

    #[test]
    fn knowledge_lattice_preserves_null_unset_unknown_denied_and_cache_unowned() {
        let event = event();
        let cases = [
            Knowledge::Known(PreviewExpression::Null),
            Knowledge::Absent,
            Knowledge::Unset,
            Knowledge::Unknown,
            Knowledge::Denied,
            Knowledge::CacheUnowned,
        ];
        for expected in cases {
            let slots = BTreeMap::from([("value", expected.clone())]);
            assert_eq!(evaluate_expression(&slot(), &slots, &event), expected);
        }
    }

    #[test]
    fn denied_or_cache_unowned_nested_dependencies_never_become_values() {
        let expression = ManifestProjectionExpression::Object {
            fields: vec![ManifestProjectionObjectField {
                name: "secret".into(),
                value: slot(),
            }],
        };
        for expected in [Knowledge::Denied, Knowledge::CacheUnowned] {
            let slots = BTreeMap::from([("value", expected.clone())]);
            assert_eq!(evaluate_expression(&expression, &slots, &event()), expected);
        }
    }

    #[test]
    fn known_partition_expression_requires_current_cache_proof() {
        let expression = ManifestProjectionPartition::Expression { expression: slot() };
        let known = BTreeMap::from([(
            "value",
            Knowledge::Known(PreviewExpression::Input {
                path: vec!["tenant_id".into()],
            }),
        )]);
        let partition = evaluate_partition(&expression, &known, &event()).unwrap();
        let rendered = serde_json::to_value(partition).unwrap();
        assert_eq!(rendered["kind"], "expression");
        assert_eq!(rendered["requires"], "current_cache_partition");
        assert_eq!(
            rendered["expression"],
            serde_json::json!({"kind": "input", "path": ["tenant_id"]})
        );
        assert!(rendered.get("token").is_none());
    }

    #[test]
    fn uncertain_or_unowned_partition_never_addresses_a_row() {
        let expression = ManifestProjectionPartition::Expression { expression: slot() };
        for knowledge in [
            Knowledge::Unknown,
            Knowledge::Absent,
            Knowledge::Unset,
            Knowledge::Denied,
            Knowledge::CacheUnowned,
        ] {
            let slots = BTreeMap::from([("value", knowledge)]);
            assert!(evaluate_partition(&expression, &slots, &event()).is_none());
        }
    }

    #[test]
    fn object_expression_is_absent_when_any_member_is_absent() {
        let slots = BTreeMap::from([
            ("absent", Knowledge::Absent),
            (
                "known",
                Knowledge::Known(PreviewExpression::Input {
                    path: vec!["known".into()],
                }),
            ),
        ]);
        let slot = |name: &str| ManifestProjectionExpression::Slot {
            slot: name.into(),
            value_type: ManifestProjectionValueType::String,
        };
        let object = ManifestProjectionExpression::Object {
            fields: vec![
                ManifestProjectionObjectField {
                    name: "missing".into(),
                    value: slot("absent"),
                },
                ManifestProjectionObjectField {
                    name: "present".into(),
                    value: slot("known"),
                },
            ],
        };
        assert_eq!(
            evaluate_expression(&object, &slots, &event()),
            Knowledge::Absent
        );
    }

    #[test]
    fn first_present_skips_absent_arguments_without_inventing_an_unset() {
        let slots = BTreeMap::from([
            ("absent", Knowledge::Absent),
            (
                "known",
                Knowledge::Known(PreviewExpression::Input {
                    path: vec!["known".into()],
                }),
            ),
        ]);
        let slot = |name: &str| ManifestProjectionExpression::Slot {
            slot: name.into(),
            value_type: ManifestProjectionValueType::String,
        };
        let first_present = ManifestProjectionExpression::Transform {
            transform: ManifestProjectionScalarTransform::FirstPresent,
            arguments: vec![slot("absent"), slot("known")],
        };
        assert!(matches!(
            evaluate_expression(&first_present, &slots, &event()),
            Knowledge::Known(PreviewExpression::Transform { arguments, .. })
                if arguments.len() == 1
        ));
        let all_absent = ManifestProjectionExpression::Transform {
            transform: ManifestProjectionScalarTransform::FirstPresent,
            arguments: vec![slot("absent")],
        };
        assert_eq!(
            evaluate_expression(&all_absent, &slots, &event()),
            Knowledge::Absent
        );
    }

    #[test]
    fn first_present_fails_closed_instead_of_skipping_an_unset_argument() {
        let expression = ManifestProjectionExpression::Transform {
            transform: ManifestProjectionScalarTransform::FirstPresent,
            arguments: vec![
                ManifestProjectionExpression::Slot {
                    slot: "unset".into(),
                    value_type: ManifestProjectionValueType::String,
                },
                ManifestProjectionExpression::Slot {
                    slot: "known".into(),
                    value_type: ManifestProjectionValueType::String,
                },
            ],
        };
        let slots = BTreeMap::from([
            ("unset", Knowledge::Unset),
            (
                "known",
                Knowledge::Known(PreviewExpression::Input {
                    path: vec!["target".into()],
                }),
            ),
        ]);
        assert_eq!(
            evaluate_expression(&expression, &slots, &event()),
            Knowledge::Unknown
        );
    }

    #[test]
    fn first_present_unset_lowers_to_record_recovery_without_a_write_or_clear() {
        let slot = |name: &str| ManifestProjectionExpression::Slot {
            slot: name.into(),
            value_type: ManifestProjectionValueType::String,
        };
        let operation = ManifestProjectionOperation {
            operation: "upsert-todo".into(),
            ordinal: 0,
            kind: ManifestProjectionMutationKind::Upsert,
            model: "Todo".into(),
            key: vec![ManifestProjectionKeyField {
                ordinal: 0,
                name: "id".into(),
                expression: slot("source"),
            }],
            fields: vec![ManifestProjectionField {
                ordinal: 0,
                name: "metadata".into(),
                assignment: ManifestProjectionAssignment::Set {
                    expression: ManifestProjectionExpression::Transform {
                        transform: ManifestProjectionScalarTransform::FirstPresent,
                        arguments: vec![slot("unset"), slot("known")],
                    },
                },
            }],
            relationships: Vec::new(),
            invalidations: Vec::new(),
        };
        let slots = BTreeMap::from([
            (
                "source",
                Knowledge::Known(PreviewExpression::Input {
                    path: vec!["source".into()],
                }),
            ),
            ("unset", Knowledge::Unset),
            (
                "known",
                Knowledge::Known(PreviewExpression::Input {
                    path: vec!["target".into()],
                }),
            ),
        ]);
        let mut operations = Vec::new();
        let mut recoveries = Vec::new();
        lower_record(
            0,
            0,
            &event(),
            &operation,
            &Some(PreviewPartition::Unit),
            &slots,
            &key_command(),
            &projection_model(),
            &mut operations,
            &mut recoveries,
        )
        .unwrap();

        assert_eq!(
            (
                operations.len(),
                recoveries.len(),
                matches!(
                    recoveries.first(),
                    Some(PreviewRecovery {
                        condition: PreviewRecoveryCondition::Always,
                        target: PreviewRecoveryTarget::Record { .. },
                        ..
                    })
                ),
            ),
            (0, 1, true)
        );
    }

    #[test]
    fn json_input_slots_compile_into_their_exact_optimistic_patch_fields() {
        let slot = |name: &str, value_type| ManifestProjectionExpression::Slot {
            slot: name.into(),
            value_type,
        };
        let operation = ManifestProjectionOperation {
            operation: "patch-json".into(),
            ordinal: 0,
            kind: ManifestProjectionMutationKind::Patch,
            model: "Todo".into(),
            key: vec![ManifestProjectionKeyField {
                ordinal: 0,
                name: "id".into(),
                expression: slot("source", ManifestProjectionValueType::String),
            }],
            fields: ["details", "tags"]
                .into_iter()
                .enumerate()
                .map(|(ordinal, name)| ManifestProjectionField {
                    ordinal: ordinal as u32,
                    name: name.into(),
                    assignment: ManifestProjectionAssignment::Set {
                        expression: slot(name, ManifestProjectionValueType::Json),
                    },
                })
                .collect(),
            relationships: Vec::new(),
            invalidations: Vec::new(),
        };
        let slots = BTreeMap::from([
            (
                "source",
                Knowledge::Known(PreviewExpression::Input {
                    path: vec!["source".into()],
                }),
            ),
            (
                "details",
                Knowledge::Known(PreviewExpression::Input {
                    path: vec!["details".into()],
                }),
            ),
            (
                "tags",
                Knowledge::Known(PreviewExpression::Input {
                    path: vec!["tags".into()],
                }),
            ),
        ]);
        let mut model = projection_model();
        model
            .fields
            .extend(["details", "tags"].into_iter().map(|name| ManifestField {
                name: name.into(),
                scalar: "JSON".into(),
                codec: "json".into(),
                nullable: false,
            }));
        let mut operations = Vec::new();
        let mut recoveries = Vec::new();

        lower_record(
            0,
            0,
            &event(),
            &operation,
            &Some(PreviewPartition::Unit),
            &slots,
            &key_command(),
            &model,
            &mut operations,
            &mut recoveries,
        )
        .unwrap();

        let PreviewMutation::Patch { set, .. } = &operations[0].mutation else {
            panic!("known JSON inputs must compile to an optimistic patch");
        };
        for field in ["details", "tags"] {
            assert!(set.iter().any(|compiled| {
                compiled.field == field
                    && compiled.value
                        == (PreviewExpression::Input {
                            path: vec![field.into()],
                        })
            }));
        }
        assert!(matches!(
            recoveries.as_slice(),
            [PreviewRecovery {
                condition: PreviewRecoveryCondition::IfRecordMissing,
                ..
            }]
        ));
    }

    #[test]
    fn object_expression_fails_closed_when_a_member_is_unset() {
        let expression = ManifestProjectionExpression::Object {
            fields: vec![ManifestProjectionObjectField {
                name: "cleared".into(),
                value: slot(),
            }],
        };
        let slots = BTreeMap::from([("value", Knowledge::Unset)]);
        assert_eq!(
            evaluate_expression(&expression, &slots, &event()),
            Knowledge::Unknown
        );
    }

    #[test]
    fn unset_object_member_lowers_to_uncertainty_without_a_write_or_clear() {
        let expression = ManifestProjectionExpression::Object {
            fields: vec![ManifestProjectionObjectField {
                name: "cleared".into(),
                value: slot(),
            }],
        };
        let slots = BTreeMap::from([("value", Knowledge::Unset)]);
        let fields = vec![ManifestProjectionField {
            ordinal: 0,
            name: "metadata".into(),
            assignment: ManifestProjectionAssignment::Set { expression },
        }];
        assert_eq!(
            evaluate_fields(&fields, &slots, &event()),
            (Vec::new(), Vec::new(), true)
        );
    }

    #[test]
    fn projection_keys_require_provable_non_null_scalars() {
        let command = key_command();
        for value in [
            ManifestProjectionValue::Boolean(true),
            ManifestProjectionValue::I64("-1".into()),
            ManifestProjectionValue::U64("1".into()),
            ManifestProjectionValue::F64("1.5".into()),
            ManifestProjectionValue::String("id".into()),
            ManifestProjectionValue::Enum {
                enum_type: "Status".into(),
                variant: "OPEN".into(),
            },
        ] {
            assert!(preview_key_value_is_provably_non_null_scalar(
                &PreviewExpression::Constant { value },
                &command,
            ));
        }
        for value in [
            ManifestProjectionValue::Null,
            ManifestProjectionValue::List(vec![ManifestProjectionValue::String("id".into())]),
            ManifestProjectionValue::Object(vec![ManifestProjectionValueField {
                name: "id".into(),
                value: ManifestProjectionValue::String("id".into()),
            }]),
        ] {
            assert!(!preview_key_value_is_provably_non_null_scalar(
                &PreviewExpression::Constant { value },
                &command,
            ));
        }

        assert!(preview_key_value_is_provably_non_null_scalar(
            &PreviewExpression::Input {
                path: vec!["source".into()],
            },
            &command,
        ));
        let mut nullable = command;
        let ManifestCommandShape::Object { definition } = &mut nullable.input else {
            unreachable!();
        };
        definition.fields[0].nullable = true;
        assert!(!preview_key_value_is_provably_non_null_scalar(
            &PreviewExpression::Input {
                path: vec!["source".into()],
            },
            &nullable,
        ));
    }

    #[test]
    fn canonical_merge_is_occurrence_ordered_and_input_permutation_independent() {
        let scope = preview_scope("Todo");
        let upsert = operation(
            0,
            PreviewMutation::Upsert {
                scope: scope.clone(),
                fields: vec![
                    preview_field("status", "created_status"),
                    preview_field("title", "title"),
                ],
                replace: vec!["status".into(), "title".into()],
            },
        );
        let patch = operation(
            1,
            PreviewMutation::Patch {
                scope: scope.clone(),
                set: vec![preview_field("status", "completed_status")],
                unset: Vec::new(),
                if_present: true,
            },
        );

        let forward = canonicalize_operations(vec![upsert.clone(), patch.clone()]).unwrap();
        let reversed = canonicalize_operations(vec![patch, upsert]).unwrap();
        assert_eq!(forward, reversed);
        assert_eq!(forward[0].occurrence_ordinal, 1);
        let PreviewMutation::Upsert {
            fields, replace, ..
        } = &forward[0].mutation
        else {
            panic!("upsert followed by a patch remains an upsert");
        };
        assert_eq!(replace, &vec!["status".to_string(), "title".to_string()]);
        assert_eq!(
            fields
                .iter()
                .find(|field| field.field == "status")
                .unwrap()
                .value,
            PreviewExpression::Input {
                path: vec!["completed_status".into()]
            }
        );
    }

    #[test]
    fn canonical_merge_preserves_delete_precedence_and_structured_kind_order() {
        let record = preview_scope("Todo");
        let target = preview_scope("User");
        let upsert = operation(
            0,
            PreviewMutation::Upsert {
                scope: record.clone(),
                fields: vec![preview_field("title", "title")],
                replace: vec!["title".into()],
            },
        );
        let delete = operation(
            2,
            PreviewMutation::Delete {
                scope: record.clone(),
            },
        );
        let link = operation(
            0,
            PreviewMutation::Link {
                relationship: "owner".into(),
                source: record.clone(),
                target,
            },
        );
        let model = operation(
            0,
            PreviewMutation::InvalidateModel {
                partition: Some(PreviewPartition::Unit),
                model: "Todo".into(),
            },
        );
        let relationship = operation(
            0,
            PreviewMutation::InvalidateRelationship {
                relationship: "owner".into(),
                source: record,
            },
        );
        let canonical =
            canonicalize_operations(vec![relationship, model, link, delete, upsert]).unwrap();
        assert!(matches!(
            canonical[0].mutation,
            PreviewMutation::Delete { .. }
        ));
        assert!(matches!(
            canonical[1].mutation,
            PreviewMutation::Link { .. }
        ));
        assert!(matches!(
            canonical[2].mutation,
            PreviewMutation::InvalidateModel { .. }
        ));
        assert!(matches!(
            canonical[3].mutation,
            PreviewMutation::InvalidateRelationship { .. }
        ));
    }

    #[test]
    fn conditional_recovery_is_removed_when_final_operation_is_not_a_patch() {
        let scope = preview_scope("Todo");
        let operations = vec![operation(
            1,
            PreviewMutation::Delete {
                scope: scope.clone(),
            },
        )];
        let recovery = record_recovery(0, 0, scope, PreviewRecoveryCondition::IfRecordMissing);
        assert!(canonicalize_recoveries(vec![recovery], &operations)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn expanded_preview_inventory_accepts_128_and_rejects_129() {
        let scope = preview_scope("Todo");
        let one_operation = operation(0, PreviewMutation::Delete { scope });
        let mut operations = Vec::new();
        for _ in 0..MAX_PREVIEW_ITEMS {
            push_preview_item(&mut operations, one_operation.clone()).unwrap();
        }
        assert!(validate_preview_inventory(&operations, &[]).is_ok());
        assert_eq!(
            push_preview_item(&mut operations, one_operation)
                .unwrap_err()
                .code,
            "client.manifest.command_projection_preview_inventory"
        );
        assert_eq!(operations.len(), MAX_PREVIEW_ITEMS);

        let one_recovery = model_recovery(0, 0, "Todo".into(), Some(PreviewPartition::Unit));
        let mut recoveries = Vec::new();
        for _ in 0..MAX_PREVIEW_ITEMS {
            push_preview_item(&mut recoveries, one_recovery.clone()).unwrap();
        }
        assert!(validate_preview_inventory(&[], &recoveries).is_ok());
        assert_eq!(
            push_preview_item(&mut recoveries, one_recovery)
                .unwrap_err()
                .code,
            "client.manifest.command_projection_preview_inventory"
        );
        assert_eq!(recoveries.len(), MAX_PREVIEW_ITEMS);
    }

    #[test]
    fn relationship_preview_never_guesses_an_uncertain_edge() {
        let command = key_command();
        let known = |path: &str| {
            Knowledge::Known(PreviewExpression::Input {
                path: vec![path.into()],
            })
        };
        let operation = relationship_operation();
        let partition = Some(PreviewPartition::Unit);

        let known_slots =
            BTreeMap::from([("source", known("source")), ("target", known("target"))]);
        let mut operations = Vec::new();
        let mut recoveries = Vec::new();
        lower_relationships(
            0,
            0,
            &event(),
            &operation,
            &partition,
            &known_slots,
            &command,
            &mut operations,
            &mut recoveries,
        )
        .unwrap();
        assert!(matches!(
            operations[0].mutation,
            PreviewMutation::Link { .. }
        ));
        assert!(recoveries.is_empty());

        let uncertain_target =
            BTreeMap::from([("source", known("source")), ("target", Knowledge::Unknown)]);
        operations.clear();
        lower_relationships(
            0,
            0,
            &event(),
            &operation,
            &partition,
            &uncertain_target,
            &command,
            &mut operations,
            &mut recoveries,
        )
        .unwrap();
        assert!(matches!(
            operations[0].mutation,
            PreviewMutation::InvalidateRelationship { .. }
        ));
        assert!(matches!(
            recoveries.last().unwrap().target,
            PreviewRecoveryTarget::Relationship { .. }
        ));

        let uncertain_source =
            BTreeMap::from([("source", Knowledge::Denied), ("target", known("target"))]);
        operations.clear();
        recoveries.clear();
        lower_relationships(
            0,
            0,
            &event(),
            &operation,
            &partition,
            &uncertain_source,
            &command,
            &mut operations,
            &mut recoveries,
        )
        .unwrap();
        assert!(operations.is_empty());
        assert!(matches!(
            recoveries[0].target,
            PreviewRecoveryTarget::Model { .. }
        ));
    }

    #[test]
    fn relationship_invalidation_uses_only_its_explicit_keyed_effect() {
        let command = key_command();
        let known = |path: &str| {
            Knowledge::Known(PreviewExpression::Input {
                path: vec![path.into()],
            })
        };
        let partition = Some(PreviewPartition::Unit);
        let slots = BTreeMap::from([("source", known("source")), ("target", known("target"))]);
        let mut operation = relationship_operation();
        operation.relationships[0].kind = ManifestProjectionRelationshipEffectKind::Invalidate;
        operation.invalidations = vec![ManifestProjectionInvalidation::Relationship {
            source_model: "Todo".into(),
            relationship: "owner".into(),
            target_model: "User".into(),
        }];
        let mut operations = Vec::new();
        let mut recoveries = Vec::new();
        lower_relationships(
            0,
            0,
            &event(),
            &operation,
            &partition,
            &slots,
            &command,
            &mut operations,
            &mut recoveries,
        )
        .unwrap();
        lower_invalidations(
            0,
            0,
            &operation,
            &partition,
            &mut operations,
            &mut recoveries,
        )
        .unwrap();
        assert_eq!(operations.len(), 1);
        assert_eq!(recoveries.len(), 1);
        assert!(matches!(
            operations[0].mutation,
            PreviewMutation::InvalidateRelationship { .. }
        ));
        assert!(matches!(
            recoveries[0].target,
            PreviewRecoveryTarget::Relationship { .. }
        ));

        operation.relationships.clear();
        operations.clear();
        recoveries.clear();
        lower_invalidations(
            0,
            0,
            &operation,
            &partition,
            &mut operations,
            &mut recoveries,
        )
        .unwrap();
        assert_eq!(operations.len(), 1);
        assert_eq!(recoveries.len(), 1);
        assert!(matches!(
            &operations[0].mutation,
            PreviewMutation::InvalidateModel { model, .. } if model == "Todo"
        ));
        assert!(matches!(
            &recoveries[0].target,
            PreviewRecoveryTarget::Model { model, .. } if model == "Todo"
        ));
    }

    #[test]
    fn model_invalidation_and_recovery_compile_without_a_record_key() {
        let mut operation = relationship_operation();
        operation.kind = ManifestProjectionMutationKind::InvalidateModel;
        operation.relationships.clear();
        operation.invalidations = vec![ManifestProjectionInvalidation::Model {
            model: "Todo".into(),
        }];
        let mut operations = Vec::new();
        let mut recoveries = Vec::new();

        lower_invalidations(
            0,
            0,
            &operation,
            &Some(PreviewPartition::Unit),
            &mut operations,
            &mut recoveries,
        )
        .unwrap();
        let operations = canonicalize_operations(operations).unwrap();
        let recoveries = canonicalize_recoveries(recoveries, &operations).unwrap();

        assert!(matches!(
            operations.as_slice(),
            [PreviewOperation {
                mutation: PreviewMutation::InvalidateModel { model, .. },
                ..
            }] if model == "Todo"
        ));
        assert!(matches!(
            recoveries.as_slice(),
            [PreviewRecovery {
                condition: PreviewRecoveryCondition::Always,
                target: PreviewRecoveryTarget::Model { model, .. },
                ..
            }] if model == "Todo"
        ));
    }
}
