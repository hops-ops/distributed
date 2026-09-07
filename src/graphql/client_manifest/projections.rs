use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::*;
use crate::command::CommandProjectionPreviewSource;
use crate::graphql::surface::{
    SurfaceProjectionArm, SurfaceProjectionOperation, SurfaceSelectedProjectionProgram,
};
use crate::projection::placement::{
    ProjectionBindingState, ProjectionExecutionClass, ProjectionPlacement,
};
use crate::projection::{
    ProjectionAssignmentRef, ProjectionEnvelopeField, ProjectionExpression,
    ProjectionExpressionRef, ProjectionInvalidation, ProjectionMutationKind, ProjectionPartition,
    ProjectionRelationshipEffectKind, ProjectionScalarTransform, ProjectionValue,
    ProjectionValueRef, ProjectionValueType,
};

pub(super) const CLIENT_PROJECTION_PROGRAM_VERSION: u32 = 2;
pub(super) const CLIENT_PROJECTION_BINDING_VERSION: u32 = 1;
pub(super) const CLIENT_PROJECTION_OPERATION_SEMANTICS_VERSION: u32 = 1;
pub(super) const COMMAND_PROJECTION_EXTENSION_VERSION: u32 = 2;

#[derive(Clone)]
struct SlotOrigin {
    event: crate::ProjectionEventSelector,
    slot: String,
    target: SlotTarget,
    value_type: ProjectionValueType,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SlotTarget {
    BodyPath { path: Vec<String> },
    Envelope { field: ProjectionEnvelopeField },
}

pub(super) fn projection_manifest(
    surface: &Surface,
) -> Result<(Vec<ClientProjectionProgram>, Vec<ClientProjectionBinding>), ClientManifestError> {
    let mut programs = BTreeMap::new();
    let mut bindings = Vec::new();
    for owner in &surface.projectors {
        for modeled in &owner.modeled {
            bindings.push(ClientProjectionBinding {
                version: CLIENT_PROJECTION_BINDING_VERSION,
                binding_id: modeled.binding_id().to_string(),
                program_id: modeled.program_id().to_string(),
                epoch: modeled.epoch().as_str().to_owned(),
                state: match modeled.state() {
                    ProjectionBindingState::Active => ClientProjectionBindingState::Active,
                    ProjectionBindingState::Draining => ClientProjectionBindingState::Draining,
                },
                placement: match modeled.placement() {
                    ProjectionPlacement::Eventual => ClientProjectionPlacement::Eventual,
                    ProjectionPlacement::Direct => ClientProjectionPlacement::Direct,
                },
                execution_class: match modeled.execution_class() {
                    ProjectionExecutionClass::Causal => ClientProjectionExecutionClass::Causal,
                    ProjectionExecutionClass::Background => {
                        ClientProjectionExecutionClass::Background
                    }
                },
            });
            let Some(selected) = modeled.selected_program() else {
                continue;
            };
            let mut slots = Vec::new();
            let program = lower_program(modeled.program_id(), selected, surface, &mut slots)?;
            match programs.entry(modeled.program_id()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(program);
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() != &program => {
                    return Err(ClientManifestError(format!(
                        "selected projection program `{}` has divergent authorized descriptors",
                        modeled.program_id()
                    )));
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
        }
    }
    bindings.sort_by(|left, right| {
        left.program_id
            .cmp(&right.program_id)
            .then_with(|| left.binding_id.cmp(&right.binding_id))
            .then_with(|| left.epoch.cmp(&right.epoch))
    });
    Ok((programs.into_values().collect(), bindings))
}

pub(super) fn command_projection_extension(
    command: &SurfaceCommand,
    surface: &Surface,
    _trusted_presets: &[ClientTrustedPresetDescriptor],
) -> Result<Option<CommandProjectionExtension>, ClientManifestError> {
    if command.projections.selectors.is_empty() {
        return Ok(None);
    }
    let emitted = &command.projections.selectors;
    let mut program_arms = Vec::new();
    let mut slot_origins = Vec::new();
    for owner in &surface.projectors {
        for modeled in &owner.modeled {
            // Preview composition uses portable mutation IR for both Eventual
            // and Direct placements. Async causal obligations still use
            // `is_causally_eligible` (Eventual-only) elsewhere.
            if !modeled.is_preview_eligible() {
                continue;
            }
            let Some(program) = modeled.selected_program() else {
                continue;
            };
            for arm in &program.arms {
                if !emitted.contains(&arm.selector) {
                    continue;
                }
                let arm_ref = arm_ref(modeled.program_id(), arm);
                let mut arm_slots = Vec::new();
                let _ = lower_arm(
                    modeled.program_id(),
                    arm,
                    &program.partition,
                    surface,
                    &arm_ref,
                    &mut arm_slots,
                )?;
                slot_origins.extend(arm_slots);
                program_arms.push(CommandProjectionArmRef {
                    event: event_ref(&arm.selector),
                    program_id: modeled.program_id().to_string(),
                    arm: arm_ref,
                });
            }
        }
    }
    program_arms.sort_by(|left, right| {
        left.event
            .cmp(&right.event)
            .then_with(|| left.program_id.cmp(&right.program_id))
            .then_with(|| left.arm.cmp(&right.arm))
    });
    program_arms.dedup();
    if program_arms.is_empty() {
        return Ok(None);
    }
    let event_set = program_arms
        .iter()
        .map(|arm| arm.event.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let mut preview_occurrences = Vec::new();
    if command.projections.previews.is_empty() {
        // Automatic optimism: input + defaults + row-policy claims + emits +
        // projection arms. No third mapping document is required. Unresolved
        // slots stay Unknown and fall back to revalidation.
        let claim_presets = surface_row_policy_claim_presets(surface);
        let mut seen_event_ids = BTreeSet::new();
        for selector in emitted {
            let preview_event = event_ref(selector);
            if !seen_event_ids.insert(preview_event.id.clone()) {
                continue;
            }
            if program_arms.iter().all(|arm| arm.event != preview_event) {
                continue;
            }
            let inferred = command
                .projections
                .inferred_values
                .iter()
                .find(|values| values.selector == *selector);
            let mut values = slot_origins
                .iter()
                .filter(|origin| origin.event == *selector)
                .map(|origin| {
                    let inferred_source = inferred
                        .and_then(|values| {
                            values.preview.fields.iter().find(|field| {
                                let target = match field.envelope {
                                    Some(envelope) => SlotTarget::Envelope { field: envelope },
                                    None => SlotTarget::BodyPath {
                                        path: field.body_path.clone(),
                                    },
                                };
                                target == origin.target
                            })
                        })
                        .map(|field| {
                            client_preview_source(
                                &field.source,
                                matches!(origin.target, SlotTarget::BodyPath { .. }),
                                match origin.target {
                                    SlotTarget::Envelope { field } => Some(field),
                                    SlotTarget::BodyPath { .. } => None,
                                },
                                field.body_type,
                                field.body_rust_type,
                                field.body_nullable,
                                field.body_always_present,
                                &origin.value_type,
                                command,
                            )
                        })
                        .transpose()?;
                    Ok(CommandProjectionPreviewValue {
                        slot: origin.slot.clone(),
                        source: inferred_source.unwrap_or_else(|| {
                            auto_preview_source(origin, command, &claim_presets)
                        }),
                    })
                })
                .collect::<Result<Vec<_>, ClientManifestError>>()?;
            values.sort_by(|left, right| left.slot.cmp(&right.slot));
            values.dedup_by(|left, right| left.slot == right.slot);
            let ordinal = u32::try_from(preview_occurrences.len()).map_err(|_| {
                ClientManifestError(format!(
                    "command `{}` declares too many projection preview occurrences",
                    command.command_name
                ))
            })?;
            preview_occurrences.push(CommandProjectionPreviewOccurrence {
                ordinal,
                event: preview_event,
                values,
            });
        }
    } else {
        for preview in &command.projections.previews {
            let preview_event = event_ref(&preview.selector);
            if program_arms.iter().all(|arm| arm.event != preview_event) {
                continue;
            }
            let occurrence_origins = slot_origins
                .iter()
                .filter(|origin| origin.event == preview.selector)
                .collect::<Vec<_>>();
            let mut values = occurrence_origins
                .into_iter()
                .map(|origin| {
                    let source = preview
                        .preview
                        .fields
                        .iter()
                        .find(|field| {
                            let target = match field.envelope {
                                Some(envelope) => SlotTarget::Envelope { field: envelope },
                                None => SlotTarget::BodyPath {
                                    path: field.body_path.clone(),
                                },
                            };
                            target == origin.target
                        })
                        .map(|field| {
                            client_preview_source(
                                &field.source,
                                matches!(origin.target, SlotTarget::BodyPath { .. }),
                                match origin.target {
                                    SlotTarget::Envelope { field } => Some(field),
                                    SlotTarget::BodyPath { .. } => None,
                                },
                                field.body_type,
                                field.body_rust_type,
                                field.body_nullable,
                                field.body_always_present,
                                &origin.value_type,
                                command,
                            )
                        })
                        .transpose()?
                        .unwrap_or(ClientProjectionPreviewSource::Unknown);
                    Ok(CommandProjectionPreviewValue {
                        slot: origin.slot.clone(),
                        source,
                    })
                })
                .collect::<Result<Vec<_>, ClientManifestError>>()?;
            values.sort_by(|left, right| left.slot.cmp(&right.slot));
            values.dedup_by(|left, right| left.slot == right.slot);
            let ordinal = u32::try_from(preview_occurrences.len()).map_err(|_| {
                ClientManifestError(format!(
                    "command `{}` declares too many projection preview occurrences",
                    command.command_name
                ))
            })?;
            preview_occurrences.push(CommandProjectionPreviewOccurrence {
                ordinal,
                event: preview_event,
                values,
            });
        }
    }

    let pure_reduces = command
        .projections
        .pure_reduces
        .iter()
        .map(|reduce| {
            use crate::command::CommandProjectionPreviewSource as ServerSource;
            use crate::graphql::client_manifest::ClientProjectionPreviewSource as ClientSource;
            let map_source = |source: &ServerSource| -> Result<ClientSource, ClientManifestError> {
                Ok(match source {
                    ServerSource::InputPath { path } => ClientSource::Input { path: path.clone() },
                    ServerSource::GeneratedDefaultPath { path } => {
                        ClientSource::GeneratedDefault { path: path.clone() }
                    }
                    ServerSource::TrustedPreset { name, codec } => ClientSource::TrustedPreset {
                        name: name.clone(),
                        codec: codec.clone(),
                    },
                    other => {
                        return Err(ClientManifestError(format!(
                            "command `{}` pure reduce uses unsupported source {other:?}",
                            command.command_name
                        )));
                    }
                })
            };
            Ok(crate::graphql::client_manifest::ClientCommandPureReduce {
                fn_name: reduce.fn_name.clone(),
                client_module: reduce.client_module.clone(),
                client_export: reduce.client_export.clone(),
                wasm_package: reduce.wasm_package.clone(),
                wasm_export: reduce.wasm_export.clone(),
                model: reduce.model.clone(),
                key: reduce
                    .key
                    .iter()
                    .map(|arg| {
                        Ok(crate::graphql::client_manifest::ClientCommandPureArg {
                            name: arg.name.clone(),
                            source: map_source(&arg.source)?,
                        })
                    })
                    .collect::<Result<Vec<_>, ClientManifestError>>()?,
                args: reduce
                    .args
                    .iter()
                    .map(|arg| {
                        Ok(crate::graphql::client_manifest::ClientCommandPureArg {
                            name: arg.name.clone(),
                            source: map_source(&arg.source)?,
                        })
                    })
                    .collect::<Result<Vec<_>, ClientManifestError>>()?,
                assign: reduce.assign.clone(),
            })
        })
        .collect::<Result<Vec<_>, ClientManifestError>>()?;

    Ok(Some(CommandProjectionExtension {
        version: COMMAND_PROJECTION_EXTENSION_VERSION,
        event_set,
        program_arms,
        preview_occurrences,
        pure_reduces,
        fallback: ClientProjectionFallback::Revalidate,
    }))
}

/// Map row-policy `column == claim(header)` bindings to trusted-preset sources.
///
/// Keyed by the model field / body-path leaf name so automatic optimism can
/// fill owner-like slots without a third mapping API.
fn surface_row_policy_claim_presets(surface: &Surface) -> BTreeMap<String, (String, String)> {
    let mut presets = BTreeMap::new();
    for model in surface.models.values() {
        let SurfaceRowPolicy::Predicate(expression) = &model.row_policy else {
            continue;
        };
        collect_row_policy_claim_presets(expression, model, surface, &mut presets);
    }
    presets
}

fn collect_row_policy_claim_presets(
    expression: &FilterExpr,
    model: &crate::graphql::surface::SurfaceModel,
    surface: &Surface,
    presets: &mut BTreeMap<String, (String, String)>,
) {
    match expression {
        FilterExpr::And(expressions) | FilterExpr::Or(expressions) => {
            for expression in expressions {
                collect_row_policy_claim_presets(expression, model, surface, presets);
            }
        }
        FilterExpr::Not(expression) => {
            collect_row_policy_claim_presets(expression, model, surface, presets);
        }
        FilterExpr::Cmp {
            column,
            rhs: Operand::Claim(claim),
            ..
        } => {
            insert_row_policy_claim_preset(model, column, &claim.header, presets);
        }
        FilterExpr::In { column, values, .. } => {
            for value in values {
                if let Operand::Claim(claim) = value {
                    insert_row_policy_claim_preset(model, column, &claim.header, presets);
                }
            }
        }
        FilterExpr::Rel { field, predicate } => {
            let Some(relationship) = model
                .relationships
                .iter()
                .find(|relationship| relationship.name == *field)
            else {
                return;
            };
            let Some(target) = surface.models.get(&relationship.target_model) else {
                return;
            };
            collect_row_policy_claim_presets(predicate, target, surface, presets);
        }
        FilterExpr::Cmp { .. } | FilterExpr::IsNull { .. } => {}
    }
}

fn insert_row_policy_claim_preset(
    model: &crate::graphql::surface::SurfaceModel,
    column: &str,
    claim: &str,
    presets: &mut BTreeMap<String, (String, String)>,
) {
    let codec = resolve_row_policy_column_codec(model, column);
    let Some(codec) = codec else {
        return;
    };
    if matches!(codec, "base64" | "json") {
        return;
    }
    // Index by the policy column text and every alias (logical field name,
    // physical column name) so body-path leaves and GraphQL field names both
    // resolve when they describe the same column.
    let mut keys = BTreeSet::from([column.to_owned()]);
    if let Some(schema_column) = model.schema.columns.iter().find(|candidate| {
        !candidate.skipped && (candidate.field_name == column || candidate.column_name == column)
    }) {
        keys.insert(schema_column.field_name.clone());
        keys.insert(schema_column.column_name.clone());
    }
    for field in &model.columns {
        if field.name == column || keys.contains(&field.name) {
            keys.insert(field.name.clone());
        }
    }
    for key in keys {
        match presets.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert((claim.to_owned(), codec.to_owned()));
            }
            std::collections::btree_map::Entry::Occupied(entry)
                if entry.get().0 == claim && entry.get().1 == codec => {}
            // Conflicting claim/codec for the same column is not portable; leave
            // the first binding and let remaining slots fall through to Unknown.
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
}

fn resolve_row_policy_column_codec(
    model: &crate::graphql::surface::SurfaceModel,
    column: &str,
) -> Option<&'static str> {
    if let Some(field) = model.columns.iter().find(|field| field.name == column) {
        return super::scalar_codec(&field.scalar);
    }
    let schema_column = model.schema.columns.iter().find(|candidate| {
        !candidate.skipped && (candidate.field_name == column || candidate.column_name == column)
    })?;
    if let Some(field) = model
        .columns
        .iter()
        .find(|field| field.name == schema_column.column_name)
    {
        return super::scalar_codec(&field.scalar);
    }
    match schema_column.column_type {
        crate::table::ColumnType::Text | crate::table::ColumnType::Timestamp => Some("string"),
        crate::table::ColumnType::Boolean => Some("boolean"),
        crate::table::ColumnType::Integer => Some("int32"),
        crate::table::ColumnType::UnsignedInteger => Some("json_number_precision_limited"),
        crate::table::ColumnType::Float => Some("float64"),
        crate::table::ColumnType::Json => Some("json"),
        crate::table::ColumnType::Bytes => Some("base64"),
        crate::table::ColumnType::Unsupported(_) => None,
    }
}

/// Derive one client preview source from command input / defaults / claims.
fn auto_preview_source(
    origin: &SlotOrigin,
    command: &SurfaceCommand,
    claim_presets: &BTreeMap<String, (String, String)>,
) -> ClientProjectionPreviewSource {
    match &origin.target {
        SlotTarget::Envelope {
            field: ProjectionEnvelopeField::AggregateId,
        } => {
            if let Some(path) = auto_aggregate_id_path(command, &origin.value_type) {
                if command
                    .input_defaults
                    .iter()
                    .any(|default| default.path == path)
                {
                    ClientProjectionPreviewSource::GeneratedDefault { path }
                } else {
                    ClientProjectionPreviewSource::Input { path }
                }
            } else {
                ClientProjectionPreviewSource::Unknown
            }
        }
        SlotTarget::Envelope { .. } => ClientProjectionPreviewSource::Unknown,
        SlotTarget::BodyPath { path } => {
            if command_input_field(&command.input, path)
                .is_some_and(|field| input_field_compatible(field, &origin.value_type, None, None))
            {
                if command
                    .input_defaults
                    .iter()
                    .any(|default| default.path == *path)
                {
                    return ClientProjectionPreviewSource::GeneratedDefault { path: path.clone() };
                }
                return ClientProjectionPreviewSource::Input { path: path.clone() };
            }
            if let Some(leaf) = path.last() {
                if let Some((name, codec)) = claim_presets.get(leaf) {
                    if codec_compatible(codec, &origin.value_type) {
                        return ClientProjectionPreviewSource::TrustedPreset {
                            name: name.clone(),
                            codec: codec.clone(),
                        };
                    }
                }
            }
            ClientProjectionPreviewSource::Unknown
        }
    }
}

/// Pick the best aggregate-id input path: generated id-like fields first, then
/// explicit id-like fields (`id`, `*_id`, or GraphQL `ID`).
fn auto_aggregate_id_path(
    command: &SurfaceCommand,
    expected: &ProjectionValueType,
) -> Option<Vec<String>> {
    let SurfaceCommandShape::Typed(definition) = &command.input else {
        return None;
    };
    let mut generated = Vec::new();
    let mut explicit = Vec::new();
    for field in &definition.fields {
        if field.list || !input_field_compatible(field, expected, None, None) {
            continue;
        }
        let id_like = field.type_name == "ID" || field.name == "id" || field.name.ends_with("_id");
        if !id_like {
            continue;
        }
        let path = vec![field.name.clone()];
        if command
            .input_defaults
            .iter()
            .any(|default| default.path == path)
        {
            generated.push(path);
        } else {
            explicit.push(path);
        }
    }
    generated.into_iter().chain(explicit).next()
}

fn lower_program(
    program_id: crate::ProjectionProgramId,
    program: &SurfaceSelectedProjectionProgram,
    surface: &Surface,
    slots: &mut Vec<SlotOrigin>,
) -> Result<ClientProjectionProgram, ClientManifestError> {
    let mut arms = Vec::with_capacity(program.arms.len());
    for arm in &program.arms {
        let arm_ref = arm_ref(program_id, arm);
        arms.push(lower_arm(
            program_id,
            arm,
            &program.partition,
            surface,
            &arm_ref,
            slots,
        )?);
    }
    arms.sort_by(|left, right| {
        left.event
            .cmp(&right.event)
            .then_with(|| left.arm.cmp(&right.arm))
    });
    Ok(ClientProjectionProgram {
        version: CLIENT_PROJECTION_PROGRAM_VERSION,
        program_id: program_id.to_string(),
        name: program.name.clone(),
        program_version: program.version,
        ir_version: program.ir_version,
        operation_semantics_version: program.operation_semantics_version,
        arms,
    })
}

#[cfg(test)]
pub(super) fn lower_program_for_test(
    program_id: crate::ProjectionProgramId,
    program: &SurfaceSelectedProjectionProgram,
    surface: &Surface,
) -> Result<ClientProjectionProgram, ClientManifestError> {
    lower_program(program_id, program, surface, &mut Vec::new())
}

fn lower_arm(
    program_id: crate::ProjectionProgramId,
    arm: &SurfaceProjectionArm,
    partition: &ProjectionPartition,
    surface: &Surface,
    arm_ref: &str,
    slots: &mut Vec<SlotOrigin>,
) -> Result<ClientProjectionArm, ClientManifestError> {
    let partition = match partition {
        ProjectionPartition::Unit => ClientProjectionPartition::Unit,
        ProjectionPartition::Expression(expression) => ClientProjectionPartition::Expression {
            expression: lower_expression(program_id, arm, arm_ref, "partition", expression, slots)?,
        },
    };
    let operations = arm
        .operations
        .iter()
        .map(|operation| lower_operation(program_id, arm, arm_ref, operation, surface, slots))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ClientProjectionArm {
        arm: arm_ref.to_owned(),
        event: event_ref(&arm.selector),
        partition,
        operations,
    })
}

fn lower_operation(
    program_id: crate::ProjectionProgramId,
    arm: &SurfaceProjectionArm,
    arm_ref: &str,
    operation: &SurfaceProjectionOperation,
    surface: &Surface,
    slots: &mut Vec<SlotOrigin>,
) -> Result<ClientProjectionOperation, ClientManifestError> {
    let model = surface.models.get(&operation.model).ok_or_else(|| {
        ClientManifestError(format!(
            "selected projection references hidden model `{}`",
            operation.model
        ))
    })?;
    let operation_ref = opaque_ref(
        b"distributed.client-projection-operation.v1",
        &(
            program_id.to_string(),
            arm_ref,
            operation.staging_ordinal,
            &operation.operation_id,
        ),
        "po1",
    )?;
    let key = operation
        .key
        .iter()
        .map(|field| {
            let name = physical_field(model, field.name())?;
            let position = format!(
                "operation/{}/key/{}",
                operation.staging_ordinal,
                field.ordinal()
            );
            Ok(ClientProjectionKeyField {
                ordinal: field.ordinal(),
                name,
                expression: lower_expression(
                    program_id,
                    arm,
                    arm_ref,
                    &position,
                    field.expression(),
                    slots,
                )?,
            })
        })
        .collect::<Result<Vec<_>, ClientManifestError>>()?;
    let key_fields = operation
        .key
        .iter()
        .map(|field| field.name())
        .collect::<BTreeSet<_>>();
    let fields = operation
        .fields
        .iter()
        .filter(|field| !key_fields.contains(field.name()))
        .enumerate()
        .map(|(ordinal, field)| {
            let name = physical_field(model, field.name())?;
            let position = format!(
                "operation/{}/field/{}",
                operation.staging_ordinal,
                field.ordinal()
            );
            let assignment = match field.assignment().as_ref() {
                ProjectionAssignmentRef::Set(expression) => ClientProjectionAssignment::Set {
                    expression: lower_expression(
                        program_id, arm, arm_ref, &position, expression, slots,
                    )?,
                },
                ProjectionAssignmentRef::Unset => ClientProjectionAssignment::Unset,
            };
            Ok(ClientProjectionField {
                ordinal: ordinal as u32,
                name,
                assignment,
            })
        })
        .collect::<Result<Vec<_>, ClientManifestError>>()?;
    let relationships = operation
        .relationship_effects
        .iter()
        .map(|effect| {
            let relationship = effect.relationship();
            let source = surface
                .models
                .get(relationship.source_model())
                .ok_or_else(|| ClientManifestError("hidden relationship source model".into()))?;
            let target = surface
                .models
                .get(relationship.target_model())
                .ok_or_else(|| ClientManifestError("hidden relationship target model".into()))?;
            let source_key = effect
                .source_key()
                .iter()
                .map(|field| {
                    Ok(ClientProjectionKeyField {
                        ordinal: field.ordinal(),
                        name: physical_field(source, field.name())?,
                        expression: lower_expression(
                            program_id,
                            arm,
                            arm_ref,
                            &format!(
                                "operation/{}/relationship/{}/source/{}",
                                operation.staging_ordinal,
                                effect.ordinal(),
                                field.ordinal()
                            ),
                            field.expression(),
                            slots,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>, ClientManifestError>>()?;
            let target_key = effect
                .target_key()
                .iter()
                .map(|field| {
                    Ok(ClientProjectionKeyField {
                        ordinal: field.ordinal(),
                        name: physical_field(target, field.name())?,
                        expression: lower_expression(
                            program_id,
                            arm,
                            arm_ref,
                            &format!(
                                "operation/{}/relationship/{}/target/{}",
                                operation.staging_ordinal,
                                effect.ordinal(),
                                field.ordinal()
                            ),
                            field.expression(),
                            slots,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>, ClientManifestError>>()?;
            Ok(ClientProjectionRelationshipEffect {
                ordinal: effect.ordinal(),
                kind: match effect.kind() {
                    ProjectionRelationshipEffectKind::Link => {
                        ClientProjectionRelationshipEffectKind::Link
                    }
                    ProjectionRelationshipEffectKind::Unlink => {
                        ClientProjectionRelationshipEffectKind::Unlink
                    }
                    ProjectionRelationshipEffectKind::Invalidate => {
                        ClientProjectionRelationshipEffectKind::Invalidate
                    }
                },
                source_model: relationship.source_model().to_owned(),
                relationship: relationship.relationship().to_owned(),
                target_model: relationship.target_model().to_owned(),
                source_key,
                target_key,
            })
        })
        .collect::<Result<Vec<_>, ClientManifestError>>()?;
    let invalidations = operation
        .invalidations
        .iter()
        .map(|invalidation| match invalidation {
            ProjectionInvalidation::Model { model } => ClientProjectionInvalidation::Model {
                model: model.clone(),
            },
            ProjectionInvalidation::Relationship {
                source_model,
                relationship,
                target_model,
            } => ClientProjectionInvalidation::Relationship {
                source_model: source_model.clone(),
                relationship: relationship.clone(),
                target_model: target_model.clone(),
            },
        })
        .collect();
    Ok(ClientProjectionOperation {
        operation: operation_ref,
        ordinal: operation.staging_ordinal,
        kind: if operation.force_revalidate {
            ClientProjectionMutationKind::InvalidateModel
        } else {
            mutation_kind(operation.kind)
        },
        model: operation.model.clone(),
        key,
        fields,
        relationships,
        invalidations,
    })
}

fn lower_expression(
    program_id: crate::ProjectionProgramId,
    arm: &SurfaceProjectionArm,
    arm_ref: &str,
    position: &str,
    expression: &ProjectionExpression,
    slots: &mut Vec<SlotOrigin>,
) -> Result<ClientProjectionExpression, ClientManifestError> {
    lower_expression_at(program_id, arm, arm_ref, position, expression, slots)
}

fn lower_expression_at(
    program_id: crate::ProjectionProgramId,
    arm: &SurfaceProjectionArm,
    arm_ref: &str,
    position: &str,
    expression: &ProjectionExpression,
    slots: &mut Vec<SlotOrigin>,
) -> Result<ClientProjectionExpression, ClientManifestError> {
    Ok(match expression.as_ref() {
        ProjectionExpressionRef::BodyPath { path, value_type } => {
            let target = SlotTarget::BodyPath {
                path: path.to_vec(),
            };
            let slot = slot_id(program_id, arm, arm_ref, position, &target, value_type)?;
            slots.push(SlotOrigin {
                event: arm.selector.clone(),
                slot: slot.clone(),
                target,
                value_type: value_type.clone(),
            });
            ClientProjectionExpression::Slot {
                slot,
                value_type: client_value_type(value_type),
            }
        }
        ProjectionExpressionRef::Envelope { field } if intrinsic_envelope(field) => {
            ClientProjectionExpression::Envelope {
                field: client_envelope(field),
            }
        }
        ProjectionExpressionRef::Envelope { field } => {
            let value_type = envelope_value_type(field);
            let target = SlotTarget::Envelope { field };
            let slot = slot_id(program_id, arm, arm_ref, position, &target, &value_type)?;
            slots.push(SlotOrigin {
                event: arm.selector.clone(),
                slot: slot.clone(),
                target,
                value_type: value_type.clone(),
            });
            ClientProjectionExpression::Slot {
                slot,
                value_type: client_value_type(&value_type),
            }
        }
        ProjectionExpressionRef::Constant { value } => ClientProjectionExpression::Constant {
            value: client_value(value),
        },
        ProjectionExpressionRef::Enum { enum_type, variant } => ClientProjectionExpression::Enum {
            enum_type: enum_type.to_owned(),
            variant: variant.to_owned(),
        },
        ProjectionExpressionRef::List { values } => ClientProjectionExpression::List {
            values: values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    lower_expression_at(
                        program_id,
                        arm,
                        arm_ref,
                        &format!("{position}/list/{index}"),
                        value,
                        slots,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        },
        ProjectionExpressionRef::Object { fields } => ClientProjectionExpression::Object {
            fields: fields
                .iter()
                .map(|field| {
                    Ok(ClientProjectionObjectField {
                        name: field.name().to_owned(),
                        value: lower_expression_at(
                            program_id,
                            arm,
                            arm_ref,
                            &format!("{position}/object/{}", field.name()),
                            field.value(),
                            slots,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>, ClientManifestError>>()?,
        },
        ProjectionExpressionRef::Transform {
            transform,
            arguments,
        } => ClientProjectionExpression::Transform {
            transform: match transform {
                ProjectionScalarTransform::StringConcat => {
                    ClientProjectionScalarTransform::StringConcat
                }
                ProjectionScalarTransform::FirstPresent => {
                    ClientProjectionScalarTransform::FirstPresent
                }
            },
            arguments: arguments
                .iter()
                .enumerate()
                .map(|(index, argument)| {
                    lower_expression_at(
                        program_id,
                        arm,
                        arm_ref,
                        &format!("{position}/transform/{index}"),
                        argument,
                        slots,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        },
    })
}

fn slot_id(
    program_id: crate::ProjectionProgramId,
    arm: &SurfaceProjectionArm,
    arm_ref: &str,
    position: &str,
    target: &SlotTarget,
    value_type: &ProjectionValueType,
) -> Result<String, ClientManifestError> {
    opaque_ref(
        b"distributed.client-projection-slot.v1",
        &(
            program_id.to_string(),
            event_ref(&arm.selector).id,
            arm_ref,
            position,
            target,
            value_type,
        ),
        "ps1",
    )
}

fn arm_ref(program_id: crate::ProjectionProgramId, arm: &SurfaceProjectionArm) -> String {
    opaque_ref(
        b"distributed.client-projection-arm.v1",
        &(
            program_id.to_string(),
            event_ref(&arm.selector).id,
            &arm.arm_id,
        ),
        "pa1",
    )
    .expect("canonical projection arm identity serialization cannot fail")
}

fn event_ref(selector: &crate::ProjectionEventSelector) -> ClientProjectionEventRef {
    ClientProjectionEventRef {
        id: opaque_ref(b"distributed.client-projection-event.v1", selector, "pe1")
            .expect("canonical event selector serialization cannot fail"),
        name: selector.event_name().to_owned(),
        version: selector.event_version(),
    }
}

fn opaque_ref(
    domain: &[u8],
    value: &impl Serialize,
    prefix: &str,
) -> Result<String, ClientManifestError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| ClientManifestError(format!("projection identity: {error}")))?;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update([0]);
    digest.update(bytes);
    Ok(format!("{prefix}:sha256:{:x}", digest.finalize()))
}

fn physical_field(
    model: &crate::graphql::surface::SurfaceModel,
    logical: &str,
) -> Result<String, ClientManifestError> {
    let matches = model
        .schema
        .columns
        .iter()
        .filter(|column| !column.skipped && column.field_name == logical)
        .collect::<Vec<_>>();
    let [column] = matches.as_slice() else {
        return Err(ClientManifestError(format!(
            "selected projection logical field `{logical}` does not map to one authorized physical column on `{}`",
            model.model_name
        )));
    };
    if !model
        .columns
        .iter()
        .any(|selected| selected.name == column.column_name)
    {
        return Err(ClientManifestError(format!(
            "selected projection field `{logical}` maps to a hidden physical column"
        )));
    }
    Ok(column.column_name.clone())
}

#[expect(
    clippy::too_many_arguments,
    reason = "the arguments are the independent schema facts needed to prove one preview source safe"
)]
fn client_preview_source(
    source: &CommandProjectionPreviewSource,
    body_slot: bool,
    envelope: Option<ProjectionEnvelopeField>,
    body_type: Option<crate::projection::lower::ProjectionPortableType>,
    body_rust_type: Option<&str>,
    body_nullable: Option<bool>,
    body_always_present: Option<bool>,
    expected: &ProjectionValueType,
    command: &SurfaceCommand,
) -> Result<ClientProjectionPreviewSource, ClientManifestError> {
    use crate::projection::lower::ProjectionPortableType;

    if body_slot && body_type.is_none() {
        return Ok(ClientProjectionPreviewSource::Unknown);
    }
    if envelope.is_some_and(|field| field != ProjectionEnvelopeField::AggregateId) {
        return Ok(ClientProjectionPreviewSource::Unknown);
    }
    let body_compatible = match body_type {
        None => true,
        Some(ProjectionPortableType::Boolean) => matches!(expected, ProjectionValueType::Boolean),
        Some(ProjectionPortableType::I64) => matches!(expected, ProjectionValueType::I64),
        Some(ProjectionPortableType::U64) => matches!(expected, ProjectionValueType::U64),
        Some(ProjectionPortableType::F64) => matches!(expected, ProjectionValueType::F64),
        Some(ProjectionPortableType::String) => matches!(expected, ProjectionValueType::String),
        Some(ProjectionPortableType::Json) => matches!(expected, ProjectionValueType::Json),
        Some(ProjectionPortableType::Custom) if matches!(expected, ProjectionValueType::Json) => {
            true
        }
        Some(ProjectionPortableType::Custom) => match expected {
            ProjectionValueType::Enum(enum_type) => body_rust_type
                .is_some_and(|rust_type| rust_type.rsplit("::").next() == Some(enum_type)),
            _ => false,
        },
        Some(ProjectionPortableType::Bytes) => false,
    };
    if !body_compatible {
        return Ok(ClientProjectionPreviewSource::Unknown);
    }
    Ok(match source {
        CommandProjectionPreviewSource::InputPath { path } => {
            if !command_input_field(&command.input, path).is_some_and(|field| {
                input_field_compatible(field, expected, body_type, body_rust_type)
            }) {
                ClientProjectionPreviewSource::Unknown
            } else {
                ClientProjectionPreviewSource::Input { path: path.clone() }
            }
        }
        CommandProjectionPreviewSource::GeneratedDefaultPath { path } => {
            if command_input_field(&command.input, path).is_some_and(|field| {
                input_field_compatible(field, expected, body_type, body_rust_type)
            }) && command
                .input_defaults
                .iter()
                .any(|default| default.path == *path)
            {
                ClientProjectionPreviewSource::GeneratedDefault { path: path.clone() }
            } else {
                ClientProjectionPreviewSource::Unknown
            }
        }
        CommandProjectionPreviewSource::TrustedPreset { name, codec } => {
            if codec_compatible(codec, expected) {
                ClientProjectionPreviewSource::TrustedPreset {
                    name: name.clone(),
                    codec: codec.clone(),
                }
            } else {
                ClientProjectionPreviewSource::Unknown
            }
        }
        CommandProjectionPreviewSource::Constant { value } => {
            if constant_compatible(value, expected) {
                ClientProjectionPreviewSource::Constant {
                    value: client_value(value),
                }
            } else {
                ClientProjectionPreviewSource::Unknown
            }
        }
        CommandProjectionPreviewSource::Null if body_nullable == Some(true) => {
            ClientProjectionPreviewSource::Null
        }
        CommandProjectionPreviewSource::Absent if body_always_present == Some(false) => {
            ClientProjectionPreviewSource::Absent
        }
        CommandProjectionPreviewSource::Null | CommandProjectionPreviewSource::Absent => {
            ClientProjectionPreviewSource::Unknown
        }
        CommandProjectionPreviewSource::Unknown => ClientProjectionPreviewSource::Unknown,
        CommandProjectionPreviewSource::ServerOnly => {
            return Err(ClientManifestError(
                "server-only projection preview source reached client manifest lowering".into(),
            ));
        }
    })
}

fn command_input_field<'a>(
    shape: &'a SurfaceCommandShape,
    path: &[String],
) -> Option<&'a crate::graphql::surface::SurfaceTypeField> {
    let SurfaceCommandShape::Typed(definition) = shape else {
        return None;
    };
    let mut fields = &definition.fields;
    for (index, segment) in path.iter().enumerate() {
        let field = fields.iter().find(|field| field.name == *segment)?;
        if index + 1 == path.len() {
            return (!field.list).then_some(field);
        }
        let nested = field.nested.as_deref()?;
        fields = &nested.fields;
    }
    None
}

fn input_field_compatible(
    field: &crate::graphql::surface::SurfaceTypeField,
    expected: &ProjectionValueType,
    body_type: Option<crate::projection::lower::ProjectionPortableType>,
    body_rust_type: Option<&str>,
) -> bool {
    match expected {
        ProjectionValueType::Boolean => field.type_name == "Boolean",
        ProjectionValueType::I64 | ProjectionValueType::U64 => {
            matches!(field.type_name.as_str(), "BigInt" | "Int")
        }
        ProjectionValueType::F64 => field.type_name == "Float",
        ProjectionValueType::String => {
            matches!(field.type_name.as_str(), "ID" | "String" | "Timestamptz")
        }
        ProjectionValueType::Enum(enum_type) => field.type_name == *enum_type,
        ProjectionValueType::Json
            if body_type == Some(crate::projection::lower::ProjectionPortableType::Custom) =>
        {
            body_rust_type.is_some_and(|rust_type| {
                rust_type.rsplit("::").next() == Some(field.type_name.as_str())
            })
        }
        ProjectionValueType::Json => field.type_name == "JSON",
    }
}

fn codec_compatible(codec: &str, expected: &ProjectionValueType) -> bool {
    match expected {
        ProjectionValueType::Boolean => codec == "boolean",
        ProjectionValueType::I64 | ProjectionValueType::U64 => {
            codec == "json_number_precision_limited"
        }
        ProjectionValueType::F64 => codec == "float64",
        ProjectionValueType::String | ProjectionValueType::Enum(_) => codec == "string",
        ProjectionValueType::Json => codec == "json",
    }
}

fn constant_compatible(value: &ProjectionValue, expected: &ProjectionValueType) -> bool {
    match (value.as_ref(), expected) {
        (ProjectionValueRef::Boolean(_), ProjectionValueType::Boolean)
        | (ProjectionValueRef::I64(_), ProjectionValueType::I64)
        | (ProjectionValueRef::U64(_), ProjectionValueType::U64)
        | (ProjectionValueRef::F64(_), ProjectionValueType::F64)
        | (ProjectionValueRef::String(_), ProjectionValueType::String) => true,
        (ProjectionValueRef::Enum { enum_type, .. }, ProjectionValueType::Enum(expected)) => {
            enum_type == expected
        }
        (_, ProjectionValueType::Json) => true,
        _ => false,
    }
}

fn intrinsic_envelope(field: ProjectionEnvelopeField) -> bool {
    matches!(
        field,
        ProjectionEnvelopeField::OccurrenceVersion
            | ProjectionEnvelopeField::EventName
            | ProjectionEnvelopeField::EventVersion
    )
}

fn envelope_value_type(field: ProjectionEnvelopeField) -> ProjectionValueType {
    match field {
        ProjectionEnvelopeField::AggregateSequence
        | ProjectionEnvelopeField::PublicationOrdinal
        | ProjectionEnvelopeField::BodyVersion
        | ProjectionEnvelopeField::BodyCodecVersion => ProjectionValueType::U64,
        ProjectionEnvelopeField::OccurrenceId
        | ProjectionEnvelopeField::AggregateType
        | ProjectionEnvelopeField::AggregateId
        | ProjectionEnvelopeField::BodyFingerprint
        | ProjectionEnvelopeField::BodyKind
        | ProjectionEnvelopeField::BodyTypeName
        | ProjectionEnvelopeField::BodySchema
        | ProjectionEnvelopeField::BodyCodec => ProjectionValueType::String,
        _ => unreachable!("intrinsic envelope fields are not lowered to slots"),
    }
}

fn client_value(value: &ProjectionValue) -> ClientProjectionValue {
    match value.as_ref() {
        ProjectionValueRef::Null => ClientProjectionValue::Null,
        ProjectionValueRef::Boolean(value) => ClientProjectionValue::Boolean(value),
        ProjectionValueRef::I64(value) => ClientProjectionValue::I64(value.to_owned()),
        ProjectionValueRef::U64(value) => ClientProjectionValue::U64(value.to_owned()),
        ProjectionValueRef::F64(value) => ClientProjectionValue::F64(value.to_owned()),
        ProjectionValueRef::String(value) => ClientProjectionValue::String(value.to_owned()),
        ProjectionValueRef::Enum { enum_type, variant } => ClientProjectionValue::Enum {
            enum_type: enum_type.to_owned(),
            variant: variant.to_owned(),
        },
        ProjectionValueRef::List(values) => {
            ClientProjectionValue::List(values.iter().map(client_value).collect())
        }
        ProjectionValueRef::Object(fields) => ClientProjectionValue::Object(
            fields
                .iter()
                .map(|field| ClientProjectionValueField {
                    name: field.name().to_owned(),
                    value: client_value(field.value()),
                })
                .collect(),
        ),
    }
}

fn client_value_type(value_type: &ProjectionValueType) -> ClientProjectionValueType {
    match value_type {
        ProjectionValueType::Boolean => ClientProjectionValueType::Boolean,
        ProjectionValueType::I64 => ClientProjectionValueType::I64,
        ProjectionValueType::U64 => ClientProjectionValueType::U64,
        ProjectionValueType::F64 => ClientProjectionValueType::F64,
        ProjectionValueType::String => ClientProjectionValueType::String,
        ProjectionValueType::Enum(name) => ClientProjectionValueType::Enum(name.clone()),
        ProjectionValueType::Json => ClientProjectionValueType::Json,
    }
}

fn client_envelope(field: ProjectionEnvelopeField) -> ClientProjectionEnvelopeField {
    match field {
        ProjectionEnvelopeField::OccurrenceVersion => {
            ClientProjectionEnvelopeField::OccurrenceVersion
        }
        ProjectionEnvelopeField::OccurrenceId
        | ProjectionEnvelopeField::AggregateType
        | ProjectionEnvelopeField::AggregateId
        | ProjectionEnvelopeField::AggregateSequence
        | ProjectionEnvelopeField::PublicationOrdinal
        | ProjectionEnvelopeField::BodyFingerprint
        | ProjectionEnvelopeField::BodyKind
        | ProjectionEnvelopeField::BodyTypeName
        | ProjectionEnvelopeField::BodyVersion
        | ProjectionEnvelopeField::BodySchema
        | ProjectionEnvelopeField::BodyCodec
        | ProjectionEnvelopeField::BodyCodecVersion => {
            unreachable!("non-intrinsic envelope fields are lowered to slots")
        }
        ProjectionEnvelopeField::EventName => ClientProjectionEnvelopeField::EventName,
        ProjectionEnvelopeField::EventVersion => ClientProjectionEnvelopeField::EventVersion,
    }
}

fn mutation_kind(kind: ProjectionMutationKind) -> ClientProjectionMutationKind {
    match kind {
        ProjectionMutationKind::Insert => ClientProjectionMutationKind::Insert,
        ProjectionMutationKind::Upsert => ClientProjectionMutationKind::Upsert,
        ProjectionMutationKind::Patch => ClientProjectionMutationKind::Patch,
        ProjectionMutationKind::UpsertPatch => ClientProjectionMutationKind::UpsertPatch,
        ProjectionMutationKind::Delete => ClientProjectionMutationKind::Delete,
        ProjectionMutationKind::Recreate => ClientProjectionMutationKind::Recreate,
        ProjectionMutationKind::InsertRelated => ClientProjectionMutationKind::InsertRelated,
        ProjectionMutationKind::UpsertRelated => ClientProjectionMutationKind::UpsertRelated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{CommandProjectionPreview, CommandProjectionPreviewSource};
    use crate::graphql::{
        build_surface, SurfaceOptions, SurfaceProjector, SurfaceTypeDef, SurfaceTypeField,
    };
    use crate::projection::lower::ProjectionPortableType;
    use crate::projection::placement::{
        ProjectionBindingId, ProjectionBindingState, ProjectionExecutionClass, ProjectionPlacement,
    };
    use crate::table::{ColumnType, PrimaryKey, TableColumn, TableKind, TableSchema};
    use crate::{
        DomainEvent, DomainEventBodyKind, ProjectionEnvelopeField, ProjectionExpression,
        ProjectionKeyField, ProjectionValue,
    };

    #[derive(Serialize, crate::DomainEvent)]
    #[domain_event(name = "todo.preview-a", version = 1)]
    struct PreviewA {
        value: String,
    }

    #[derive(Serialize, crate::DomainEvent)]
    #[domain_event(name = "todo.preview-b", version = 1)]
    struct PreviewB {
        value: String,
    }

    fn todo_surface() -> Surface {
        build_surface(
            &[TableSchema {
                model_name: "TodoView".into(),
                table_name: "todos".into(),
                columns: vec![
                    TableColumn {
                        primary_key: true,
                        ..TableColumn::new("todo_id", "todo_pk", ColumnType::Text)
                    },
                    TableColumn::new("owner_id", "owner_fk", ColumnType::Text),
                    TableColumn::new("title", "todo_title", ColumnType::Text),
                ],
                primary_key: PrimaryKey::new(["todo_pk"]),
                version_column: None,
                foreign_keys: Vec::new(),
                indexes: Vec::new(),
                relationships: Vec::new(),
                kind: TableKind::ReadModel,
            }],
            &SurfaceOptions::sqlite(),
        )
        .unwrap()
    }

    fn selector() -> crate::ProjectionEventSelector {
        crate::ProjectionEventSelector::try_new(
            crate::DOMAIN_EVENT_OCCURRENCE_VERSION,
            "todo.purged",
            1,
            DomainEventBodyKind::Deletion,
            "DomainDeletion<TodoDomainIdentity>",
            1,
            "private-delete-schema",
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            crate::DOMAIN_EVENT_BODY_CODEC,
            crate::DOMAIN_EVENT_BODY_CODEC_VERSION,
        )
        .unwrap()
    }

    fn command(input_type: &str) -> SurfaceCommand {
        SurfaceCommand {
            command_name: "todo.preview".into(),
            field_name: "todo_preview".into(),
            roles: vec!["user".into()],
            input: SurfaceCommandShape::Typed(SurfaceTypeDef {
                name: "TodoPreviewInput".into(),
                fields: vec![SurfaceTypeField {
                    name: "value".into(),
                    type_name: input_type.into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                }],
            }),
            output: SurfaceCommandShape::Typed(SurfaceTypeDef {
                name: "TodoPreviewPayload".into(),
                fields: Vec::new(),
            }),
            consistency: CommandConsistency::Succeeded,
            input_defaults: Vec::new(),
            effects: None,
            confirmations: Vec::new(),
            projected_model: None,
            direct_projection: None,
            projections: Default::default(),
            confirmation_unavailable: false,
        }
    }

    fn typed_selector<E: DomainEvent>() -> crate::ProjectionEventSelector {
        crate::ProjectionEventSelector::try_from_descriptor(&E::DESCRIPTOR).unwrap()
    }

    fn selected_program(
        name: &str,
        selectors: impl IntoIterator<Item = crate::ProjectionEventSelector>,
    ) -> SurfaceSelectedProjectionProgram {
        selected_program_with_partition(name, selectors, ProjectionPartition::Unit)
    }

    fn selected_program_with_partition(
        name: &str,
        selectors: impl IntoIterator<Item = crate::ProjectionEventSelector>,
        partition: ProjectionPartition,
    ) -> SurfaceSelectedProjectionProgram {
        let operations = |ordinal| {
            vec![SurfaceProjectionOperation {
                operation_id: format!("{name}-delete-{ordinal}"),
                staging_ordinal: ordinal,
                kind: ProjectionMutationKind::Delete,
                model: "TodoView".into(),
                storage: "todos".into(),
                key: vec![ProjectionKeyField::try_new(
                    0,
                    "todo_id",
                    ProjectionExpression::envelope(ProjectionEnvelopeField::AggregateId),
                )
                .unwrap()],
                fields: Vec::new(),
                relationship_effects: Vec::new(),
                invalidations: Vec::new(),
                force_revalidate: false,
            }]
        };
        SurfaceSelectedProjectionProgram {
            name: name.into(),
            version: 1,
            ir_version: crate::projection::PROJECTION_PROGRAM_IR_VERSION,
            operation_semantics_version: crate::projection::PROJECTION_OPERATION_SEMANTICS_VERSION,
            partition,
            arms: selectors
                .into_iter()
                .enumerate()
                .map(|(ordinal, selector)| SurfaceProjectionArm {
                    arm_id: format!("{name}-arm-{ordinal}"),
                    selector,
                    operations: operations(u32::try_from(ordinal).unwrap()),
                })
                .collect(),
        }
    }

    fn selected_constant_program(
        name: &str,
        selector: crate::ProjectionEventSelector,
    ) -> SurfaceSelectedProjectionProgram {
        SurfaceSelectedProjectionProgram {
            name: name.into(),
            version: 1,
            ir_version: crate::projection::PROJECTION_PROGRAM_IR_VERSION,
            operation_semantics_version: crate::projection::PROJECTION_OPERATION_SEMANTICS_VERSION,
            partition: ProjectionPartition::Unit,
            arms: vec![SurfaceProjectionArm {
                arm_id: format!("{name}-arm"),
                selector,
                operations: vec![SurfaceProjectionOperation {
                    operation_id: format!("{name}-delete"),
                    staging_ordinal: 0,
                    kind: ProjectionMutationKind::Delete,
                    model: "TodoView".into(),
                    storage: "todos".into(),
                    key: vec![ProjectionKeyField::try_new(
                        0,
                        "todo_id",
                        ProjectionExpression::constant(ProjectionValue::string("constant-todo")),
                    )
                    .unwrap()],
                    fields: Vec::new(),
                    relationship_effects: Vec::new(),
                    invalidations: Vec::new(),
                    force_revalidate: false,
                }],
            }],
        }
    }

    fn selected_value_patch_program(
        name: &str,
        selector: crate::ProjectionEventSelector,
    ) -> SurfaceSelectedProjectionProgram {
        SurfaceSelectedProjectionProgram {
            name: name.into(),
            version: 1,
            ir_version: crate::projection::PROJECTION_PROGRAM_IR_VERSION,
            operation_semantics_version: crate::projection::PROJECTION_OPERATION_SEMANTICS_VERSION,
            partition: ProjectionPartition::Unit,
            arms: vec![SurfaceProjectionArm {
                arm_id: format!("{name}-arm"),
                selector,
                operations: vec![SurfaceProjectionOperation {
                    operation_id: format!("{name}-patch"),
                    staging_ordinal: 0,
                    kind: ProjectionMutationKind::Patch,
                    model: "TodoView".into(),
                    storage: "todos".into(),
                    key: vec![ProjectionKeyField::try_new(
                        0,
                        "todo_id",
                        ProjectionExpression::constant(ProjectionValue::string("todo-1")),
                    )
                    .unwrap()],
                    fields: vec![crate::projection::ProjectionField::try_new(
                        0,
                        "title",
                        crate::projection::ProjectionAssignment::Set(
                            ProjectionExpression::body_path(ProjectionValueType::String, ["value"])
                                .unwrap(),
                        ),
                    )
                    .unwrap()],
                    relationship_effects: Vec::new(),
                    invalidations: Vec::new(),
                    force_revalidate: false,
                }],
            }],
        }
    }

    fn typed_body_preview(source: CommandProjectionPreviewSource) -> CommandProjectionPreview {
        let mut preview = CommandProjectionPreview::new()
            .events(crate::events![PreviewA])
            .field(["private_partition_path"], source);
        let field = &mut preview.fields[0];
        field.body_type = Some(ProjectionPortableType::String);
        field.body_rust_type = Some("String");
        field.body_nullable = Some(false);
        field.body_always_present = Some(true);
        preview
    }

    fn modeled(
        seed: u8,
        placement: ProjectionPlacement,
        selected: Option<SurfaceSelectedProjectionProgram>,
    ) -> crate::graphql::SurfaceModeledProjection {
        crate::graphql::SurfaceModeledProjection::selected_for_client_manifest_test(
            crate::ProjectionProgramId::parse(&format!(
                "pp1:sha256:{}",
                format!("{seed:02x}").repeat(32)
            ))
            .unwrap(),
            ProjectionBindingId::parse(&format!(
                "pb1:sha256:{}",
                format!("{:02x}", seed.wrapping_add(64)).repeat(32)
            ))
            .unwrap(),
            placement,
            ProjectionExecutionClass::Causal,
            ProjectionBindingState::Active,
            vec!["TodoView".into()],
            selected,
        )
    }

    fn surface_with_modeled(
        modeled: impl IntoIterator<Item = crate::graphql::SurfaceModeledProjection>,
    ) -> Surface {
        let mut surface = todo_surface();
        let projector = modeled.into_iter().fold(
            SurfaceProjector::new("client-manifest-test"),
            |owner, modeled| owner.modeled(modeled),
        );
        surface.projectors = vec![projector.into()];
        surface.projectors_attached = true;
        surface
    }

    #[test]
    fn allowed_event_arms_auto_derive_optimistic_occurrences_when_previews_absent() {
        let selector_a = typed_selector::<PreviewA>();
        let selector_b = typed_selector::<PreviewB>();
        let surface = surface_with_modeled([modeled(
            1,
            ProjectionPlacement::Eventual,
            Some(selected_program(
                "allowed-without-preview",
                [selector_a, selector_b],
            )),
        )]);
        let mut command = command("String");
        command
            .projections
            .add_event_set(crate::events![PreviewA, PreviewB]);

        let projection = command_projection_extension(&command, &surface, &[])
            .unwrap()
            .expect("eligible allowed arms remain visible");
        assert_eq!(projection.event_set.len(), 2);
        assert_eq!(projection.program_arms.len(), 2);
        assert_eq!(
            projection.preview_occurrences.len(),
            2,
            "emits + projection arms auto-derive one occurrence per event when .applies is absent"
        );
        // AggregateId envelope has no id-like input field named value → Unknown.
        assert!(projection
            .preview_occurrences
            .iter()
            .all(|occurrence| occurrence
                .values
                .iter()
                .all(|value| { matches!(value.source, ClientProjectionPreviewSource::Unknown) })));
    }

    #[test]
    fn auto_optimism_maps_input_defaults_and_row_policy_claims() {
        use crate::command::{CommandInputDefault, InputDefaultGenerator};

        use crate::graphql::{claim, col};

        let selector = typed_selector::<PreviewA>();
        let selected = SurfaceSelectedProjectionProgram {
            name: "auto-optimism".into(),
            version: 1,
            ir_version: crate::projection::PROJECTION_PROGRAM_IR_VERSION,
            operation_semantics_version: crate::projection::PROJECTION_OPERATION_SEMANTICS_VERSION,
            partition: ProjectionPartition::Unit,
            arms: vec![SurfaceProjectionArm {
                arm_id: "auto-arm".into(),
                selector,
                operations: vec![SurfaceProjectionOperation {
                    operation_id: "auto-upsert".into(),
                    staging_ordinal: 0,
                    kind: ProjectionMutationKind::Upsert,
                    model: "TodoView".into(),
                    storage: "todos".into(),
                    key: vec![ProjectionKeyField::try_new(
                        0,
                        "todo_id",
                        ProjectionExpression::body_path(ProjectionValueType::String, ["todo_id"])
                            .unwrap(),
                    )
                    .unwrap()],
                    fields: vec![
                        crate::projection::ProjectionField::try_new(
                            0,
                            "owner_id",
                            crate::projection::ProjectionAssignment::Set(
                                ProjectionExpression::body_path(
                                    ProjectionValueType::String,
                                    ["owner_id"],
                                )
                                .unwrap(),
                            ),
                        )
                        .unwrap(),
                        crate::projection::ProjectionField::try_new(
                            1,
                            "title",
                            crate::projection::ProjectionAssignment::Set(
                                ProjectionExpression::body_path(
                                    ProjectionValueType::String,
                                    ["title"],
                                )
                                .unwrap(),
                            ),
                        )
                        .unwrap(),
                    ],
                    relationship_effects: Vec::new(),
                    invalidations: Vec::new(),
                    force_revalidate: false,
                }],
            }],
        };
        let mut surface =
            surface_with_modeled([modeled(30, ProjectionPlacement::Eventual, Some(selected))]);
        surface.models.get_mut("TodoView").unwrap().row_policy =
            crate::graphql::surface::SurfaceRowPolicy::Predicate(
                col("owner_id").eq(claim("x-user-id")),
            );

        let mut command = SurfaceCommand {
            command_name: "todo.create".into(),
            field_name: "todos_create".into(),
            roles: vec!["user".into()],
            input: SurfaceCommandShape::Typed(SurfaceTypeDef {
                name: "TodoCreateInput".into(),
                fields: vec![
                    SurfaceTypeField {
                        name: "todo_id".into(),
                        type_name: "ID".into(),
                        nullable: false,
                        list: false,
                        item_nullable: false,
                        nested: None,
                    },
                    SurfaceTypeField {
                        name: "title".into(),
                        type_name: "String".into(),
                        nullable: false,
                        list: false,
                        item_nullable: false,
                        nested: None,
                    },
                ],
            }),
            output: SurfaceCommandShape::None,
            consistency: CommandConsistency::Succeeded,
            input_defaults: vec![CommandInputDefault {
                path: vec!["todo_id".into()],
                generator: InputDefaultGenerator::UuidV7,
            }],
            effects: None,
            confirmations: Vec::new(),
            projected_model: None,
            direct_projection: None,
            projections: Default::default(),
            confirmation_unavailable: false,
        };
        command.projections.add_event_set(crate::events![PreviewA]);

        let projection = command_projection_extension(&command, &surface, &[])
            .unwrap()
            .expect("auto optimism extension");
        assert_eq!(projection.preview_occurrences.len(), 1);
        let sources: BTreeMap<_, _> = projection.preview_occurrences[0]
            .values
            .iter()
            .map(|value| (value.slot.clone(), value.source.clone()))
            .collect();
        // Resolve slots via lowered program so we match opaque ids.
        let (programs, _) = projection_manifest(&surface).unwrap();
        let arm = &programs[0].arms[0];
        let todo_slot = match &arm.operations[0].key[0].expression {
            ClientProjectionExpression::Slot { slot, .. } => slot.clone(),
            other => panic!("expected key slot, got {other:?}"),
        };
        let owner_slot = match &arm.operations[0]
            .fields
            .iter()
            .find(|f| f.name == "owner_fk")
        {
            Some(field) => match &field.assignment {
                ClientProjectionAssignment::Set {
                    expression: ClientProjectionExpression::Slot { slot, .. },
                } => slot.clone(),
                other => panic!("expected owner slot set, got {other:?}"),
            },
            None => panic!("owner field missing"),
        };
        let title_slot = match &arm.operations[0]
            .fields
            .iter()
            .find(|f| f.name == "todo_title")
        {
            Some(field) => match &field.assignment {
                ClientProjectionAssignment::Set {
                    expression: ClientProjectionExpression::Slot { slot, .. },
                } => slot.clone(),
                other => panic!("expected title slot set, got {other:?}"),
            },
            None => panic!("title field missing"),
        };
        assert!(matches!(
            sources.get(&todo_slot),
            Some(ClientProjectionPreviewSource::GeneratedDefault { path })
                if path == &["todo_id"]
        ));
        assert!(matches!(
            sources.get(&owner_slot),
            Some(ClientProjectionPreviewSource::TrustedPreset { name, codec })
                if name == "x-user-id" && codec == "string"
        ));
        assert!(matches!(
            sources.get(&title_slot),
            Some(ClientProjectionPreviewSource::Input { path }) if path == &["title"]
        ));
    }

    #[test]
    fn inferred_transition_values_feed_eventual_and_atomic_projection_strategies() {
        for (seed, placement, consistency) in [
            (
                31,
                ProjectionPlacement::Eventual,
                CommandConsistency::Eventual,
            ),
            (32, ProjectionPlacement::Direct, CommandConsistency::Atomic),
        ] {
            let selector = typed_selector::<PreviewA>();
            let surface = surface_with_modeled([modeled(
                seed,
                placement,
                Some(selected_value_patch_program(
                    "inferred-transition-value",
                    selector,
                )),
            )]);
            let mut command = command("String");
            command.consistency = consistency;
            command.projections.add_event_set(crate::events![PreviewA]);
            command.projections.add_inferred_values(
                crate::command::__command_projection_event_preview::<PreviewA, PreviewA>(vec![(
                    "value",
                    CommandProjectionPreviewSource::constant(ProjectionValue::string(
                        "transition-value",
                    )),
                )]),
            );

            let projection = command_projection_extension(&command, &surface, &[])
                .unwrap()
                .expect("projection strategy");
            assert!(command.projections.previews.is_empty());
            assert_eq!(projection.preview_occurrences.len(), 1);
            assert!(projection.preview_occurrences[0]
                .values
                .iter()
                .any(|value| matches!(
                    &value.source,
                    ClientProjectionPreviewSource::Constant {
                        value: ClientProjectionValue::String(value),
                    } if value == "transition-value"
                )));
        }
    }

    #[test]
    fn unit_partition_is_explicit_on_every_client_arm() {
        let selector = typed_selector::<PreviewA>();
        let surface = surface_with_modeled([modeled(
            20,
            ProjectionPlacement::Eventual,
            Some(selected_program("unit-partition", [selector])),
        )]);

        let (programs, _) = projection_manifest(&surface).unwrap();

        assert!(matches!(
            programs[0].arms[0].partition,
            ClientProjectionPartition::Unit
        ));
    }

    #[test]
    fn non_unit_partition_slot_uses_known_input_source_and_is_deterministic() {
        let selector = typed_selector::<PreviewA>();
        let selected = selected_program_with_partition(
            "input-partition",
            [selector],
            ProjectionPartition::Expression(
                ProjectionExpression::body_path(
                    ProjectionValueType::String,
                    ["private_partition_path"],
                )
                .unwrap(),
            ),
        );
        let surface =
            surface_with_modeled([modeled(21, ProjectionPlacement::Eventual, Some(selected))]);
        let mut command = command("String");
        command.projections.add_event_set(crate::events![PreviewA]);
        command
            .projections
            .add_preview(typed_body_preview(CommandProjectionPreviewSource::input([
                "value",
            ])));

        let first = command_projection_extension(&command, &surface, &[])
            .unwrap()
            .unwrap();
        let second = command_projection_extension(&command, &surface, &[])
            .unwrap()
            .unwrap();
        let (programs, _) = projection_manifest(&surface).unwrap();
        let ClientProjectionPartition::Expression {
            expression: ClientProjectionExpression::Slot { slot, .. },
        } = &programs[0].arms[0].partition
        else {
            panic!("non-unit partition must lower to an opaque typed slot");
        };
        let source = &first.preview_occurrences[0]
            .values
            .iter()
            .find(|value| value.slot == *slot)
            .unwrap()
            .source;

        assert!(matches!(
            source,
            ClientProjectionPreviewSource::Input { path } if path == &["value"]
        ));
        assert_eq!(
            serde_json::to_vec(&first).unwrap(),
            serde_json::to_vec(&second).unwrap()
        );
        let wire = serde_json::to_string(&(programs, first)).unwrap();
        assert!(!wire.contains("private_partition_path"));
    }

    #[test]
    fn unknown_non_unit_partition_source_remains_an_explicit_unknown_slot() {
        let selector = typed_selector::<PreviewA>();
        let selected = selected_program_with_partition(
            "unknown-partition",
            [selector],
            ProjectionPartition::Expression(
                ProjectionExpression::body_path(
                    ProjectionValueType::String,
                    ["private_partition_path"],
                )
                .unwrap(),
            ),
        );
        let surface =
            surface_with_modeled([modeled(22, ProjectionPlacement::Eventual, Some(selected))]);
        let mut command = command("String");
        command.projections.add_event_set(crate::events![PreviewA]);
        command
            .projections
            .add_preview(typed_body_preview(CommandProjectionPreviewSource::Unknown));

        let projection = command_projection_extension(&command, &surface, &[])
            .unwrap()
            .unwrap();
        let (programs, _) = projection_manifest(&surface).unwrap();
        let ClientProjectionPartition::Expression {
            expression: ClientProjectionExpression::Slot { slot, .. },
        } = &programs[0].arms[0].partition
        else {
            panic!("non-unit partition must lower to an opaque typed slot");
        };

        assert!(projection.preview_occurrences[0]
            .values
            .iter()
            .any(|value| {
                value.slot == *slot && value.source == ClientProjectionPreviewSource::Unknown
            }));
    }

    #[test]
    fn selected_constant_only_preview_remains_ordered_with_an_empty_value_list() {
        let selector = typed_selector::<PreviewA>();
        let surface = surface_with_modeled([modeled(
            23,
            ProjectionPlacement::Eventual,
            Some(selected_constant_program("constant-only", selector)),
        )]);
        let mut command = command("String");
        command
            .projections
            .add_event_set(crate::events![PreviewA, PreviewB]);
        command
            .projections
            .add_preview(CommandProjectionPreview::new().events(crate::events![PreviewB]));
        command
            .projections
            .add_preview(CommandProjectionPreview::new().events(crate::events![PreviewA]));

        let projection = command_projection_extension(&command, &surface, &[])
            .unwrap()
            .unwrap();

        assert_eq!(projection.preview_occurrences.len(), 1);
        assert_eq!(projection.preview_occurrences[0].ordinal, 0);
        assert!(projection.preview_occurrences[0].values.is_empty());
        assert_eq!(
            projection.preview_occurrences[0].event,
            projection.program_arms[0].event
        );
    }

    #[test]
    fn partition_semantics_change_client_program_bytes_and_fingerprint() {
        let selector = typed_selector::<PreviewA>();
        let unit = selected_program("partition-fingerprint", [selector.clone()]);
        let expression = selected_program_with_partition(
            "partition-fingerprint",
            [selector],
            ProjectionPartition::Expression(ProjectionExpression::constant(
                ProjectionValue::string("tenant-a"),
            )),
        );
        let program_id =
            crate::ProjectionProgramId::parse(&format!("pp1:sha256:{}", "22".repeat(32))).unwrap();
        let mut unit_slots = Vec::new();
        let unit = lower_program(program_id, &unit, &todo_surface(), &mut unit_slots).unwrap();
        let mut expression_slots = Vec::new();
        let expression = lower_program(
            program_id,
            &expression,
            &todo_surface(),
            &mut expression_slots,
        )
        .unwrap();
        let unit_bytes = serde_json::to_vec(&unit).unwrap();
        let expression_bytes = serde_json::to_vec(&expression).unwrap();

        assert_ne!(unit_bytes, expression_bytes);
        assert_ne!(hash_bytes(&unit_bytes), hash_bytes(&expression_bytes));
    }

    #[test]
    fn one_preview_occurrence_fans_out_to_every_selected_program_arm() {
        let selector = typed_selector::<PreviewA>();
        let surface = surface_with_modeled([
            modeled(
                2,
                ProjectionPlacement::Eventual,
                Some(selected_program("fanout-a", [selector.clone()])),
            ),
            modeled(
                3,
                ProjectionPlacement::Eventual,
                Some(selected_program("fanout-b", [selector])),
            ),
        ]);
        let mut command = command("String");
        command.projections.add_event_set(crate::events![PreviewA]);
        command.projections.add_preview(
            CommandProjectionPreview::new()
                .events(crate::events![PreviewA])
                .envelope(
                    ProjectionEnvelopeField::AggregateId,
                    CommandProjectionPreviewSource::input(["value"]),
                ),
        );

        let projection = command_projection_extension(&command, &surface, &[])
            .unwrap()
            .expect("fanout command projection");
        assert_eq!(projection.program_arms.len(), 2);
        assert_eq!(projection.preview_occurrences.len(), 1);
        let occurrence = &projection.preview_occurrences[0];
        assert_eq!(occurrence.ordinal, 0);
        assert_eq!(occurrence.values.len(), 2);
        assert_ne!(occurrence.values[0].slot, occurrence.values[1].slot);
        assert!(occurrence
            .values
            .iter()
            .all(|value| matches!(value.source, ClientProjectionPreviewSource::Input { .. })));
    }

    #[test]
    fn repeated_preview_events_remain_distinct_and_ordered() {
        let selector = typed_selector::<PreviewA>();
        let surface = surface_with_modeled([modeled(
            4,
            ProjectionPlacement::Eventual,
            Some(selected_program("ordered-preview", [selector])),
        )]);
        let mut command = command("String");
        command.projections.add_event_set(crate::events![PreviewA]);
        command.projections.add_preview(
            CommandProjectionPreview::new()
                .events(crate::events![PreviewA])
                .envelope(
                    ProjectionEnvelopeField::AggregateId,
                    CommandProjectionPreviewSource::constant(ProjectionValue::string("first")),
                ),
        );
        command.projections.add_preview(
            CommandProjectionPreview::new()
                .events(crate::events![PreviewA])
                .envelope(
                    ProjectionEnvelopeField::AggregateId,
                    CommandProjectionPreviewSource::input(["value"]),
                ),
        );

        let projection = command_projection_extension(&command, &surface, &[])
            .unwrap()
            .expect("ordered command projection");
        assert_eq!(
            projection
                .preview_occurrences
                .iter()
                .map(|occurrence| occurrence.ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert!(matches!(
            projection.preview_occurrences[0].values[0].source,
            ClientProjectionPreviewSource::Constant { .. }
        ));
        assert!(matches!(
            projection.preview_occurrences[1].values[0].source,
            ClientProjectionPreviewSource::Input { .. }
        ));
    }

    #[test]
    fn filtered_preview_occurrences_leave_no_ordinal_or_identity_gap() {
        let selector_a = typed_selector::<PreviewA>();
        let surface = surface_with_modeled([modeled(
            5,
            ProjectionPlacement::Eventual,
            Some(selected_program("selected-a-only", [selector_a])),
        )]);
        let mut command = command("String");
        command
            .projections
            .add_event_set(crate::events![PreviewA, PreviewB]);
        command.projections.add_preview(
            CommandProjectionPreview::new()
                .events(crate::events![PreviewB])
                .envelope(
                    ProjectionEnvelopeField::AggregateId,
                    CommandProjectionPreviewSource::constant(ProjectionValue::string(
                        "hidden-first",
                    )),
                ),
        );
        command.projections.add_preview(
            CommandProjectionPreview::new()
                .events(crate::events![PreviewA])
                .envelope(
                    ProjectionEnvelopeField::AggregateId,
                    CommandProjectionPreviewSource::input(["value"]),
                ),
        );

        let projection = command_projection_extension(&command, &surface, &[])
            .unwrap()
            .expect("selected command projection");
        assert_eq!(projection.event_set.len(), 1);
        assert_eq!(projection.preview_occurrences.len(), 1);
        assert_eq!(projection.preview_occurrences[0].ordinal, 0);
        assert_eq!(
            projection.preview_occurrences[0].event,
            projection.event_set[0]
        );
        let wire = serde_json::to_string(&projection).unwrap();
        assert!(!wire.contains("todo.preview-b"));
        assert!(!wire.contains("hidden-first"));
    }

    #[test]
    fn direct_bindings_export_program_and_command_previews() {
        // Same mutation IR as eventual; server apply site is the command handler
        // (Projected). Client still composes .applies previews from the program.
        let selector = typed_selector::<PreviewA>();
        let surface = surface_with_modeled([modeled(
            6,
            ProjectionPlacement::Direct,
            Some(selected_program("direct-preview", [selector])),
        )]);
        let (programs, bindings) = projection_manifest(&surface).unwrap();
        assert_eq!(programs.len(), 1);
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].placement, ClientProjectionPlacement::Direct);

        let mut command = command("String");
        command.projections.add_event_set(crate::events![PreviewA]);
        command.projections.add_preview(
            CommandProjectionPreview::new()
                .events(crate::events![PreviewA])
                .envelope(
                    ProjectionEnvelopeField::AggregateId,
                    CommandProjectionPreviewSource::input(["value"]),
                ),
        );
        let projection = command_projection_extension(&command, &surface, &[])
            .unwrap()
            .expect("direct placement still exports applies previews");
        assert_eq!(projection.event_set.len(), 1);
        assert_eq!(projection.preview_occurrences.len(), 1);
        assert_eq!(projection.program_arms.len(), 1);
    }

    #[test]
    fn direct_binding_without_selected_program_exports_inventory_only() {
        let surface = surface_with_modeled([modeled(7, ProjectionPlacement::Direct, None)]);
        let (programs, bindings) = projection_manifest(&surface).unwrap();
        assert!(programs.is_empty());
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].placement, ClientProjectionPlacement::Direct);
    }

    #[test]
    fn non_intrinsic_envelope_values_are_opaque_typed_slots() {
        let arm = SurfaceProjectionArm {
            arm_id: "purged".into(),
            selector: selector(),
            operations: vec![SurfaceProjectionOperation {
                operation_id: "delete-todo".into(),
                staging_ordinal: 0,
                kind: ProjectionMutationKind::Delete,
                model: "TodoView".into(),
                storage: "todos".into(),
                key: vec![
                    ProjectionKeyField::try_new(
                        0,
                        "todo_id",
                        ProjectionExpression::envelope(ProjectionEnvelopeField::AggregateId),
                    )
                    .unwrap(),
                    ProjectionKeyField::try_new(
                        1,
                        "owner_id",
                        ProjectionExpression::envelope(ProjectionEnvelopeField::BodySchema),
                    )
                    .unwrap(),
                    ProjectionKeyField::try_new(
                        2,
                        "title",
                        ProjectionExpression::envelope(ProjectionEnvelopeField::EventName),
                    )
                    .unwrap(),
                ],
                fields: Vec::new(),
                relationship_effects: Vec::new(),
                invalidations: Vec::new(),
                force_revalidate: false,
            }],
        };
        let selected = SurfaceSelectedProjectionProgram {
            name: "delete-todo".into(),
            version: 1,
            ir_version: 1,
            operation_semantics_version: 1,
            partition: ProjectionPartition::Unit,
            arms: vec![arm],
        };
        let program_id =
            crate::ProjectionProgramId::parse(&format!("pp1:sha256:{}", "11".repeat(32))).unwrap();
        let mut slots = Vec::new();
        let lowered = lower_program(program_id, &selected, &todo_surface(), &mut slots).unwrap();
        let key = &lowered.arms[0].operations[0].key;
        assert!(matches!(
            key[0].expression,
            ClientProjectionExpression::Slot {
                value_type: ClientProjectionValueType::String,
                ..
            }
        ));
        assert!(matches!(
            key[1].expression,
            ClientProjectionExpression::Slot {
                value_type: ClientProjectionValueType::String,
                ..
            }
        ));
        assert!(matches!(
            key[2].expression,
            ClientProjectionExpression::Envelope {
                field: ClientProjectionEnvelopeField::EventName
            }
        ));
        assert_eq!(slots.len(), 2);
        let wire = serde_json::to_string(&lowered).unwrap();
        assert!(!wire.contains("private-delete-schema"));
        assert!(!wire.contains("aggregate_id"));
        assert!(!wire.contains("body_schema"));
    }

    #[test]
    fn preview_sources_require_exact_typed_and_presence_proof() {
        let string_command = command("String");
        let input = CommandProjectionPreviewSource::input(["value"]);

        assert_eq!(
            client_preview_source(
                &input,
                true,
                None,
                None,
                None,
                None,
                None,
                &ProjectionValueType::String,
                &string_command,
            )
            .unwrap(),
            ClientProjectionPreviewSource::Unknown,
            "raw body paths cannot become client authority"
        );
        assert!(matches!(
            client_preview_source(
                &input,
                false,
                Some(ProjectionEnvelopeField::AggregateId),
                None,
                None,
                None,
                None,
                &ProjectionValueType::String,
                &string_command,
            )
            .unwrap(),
            ClientProjectionPreviewSource::Input { .. }
        ));
        let private_schema = CommandProjectionPreviewSource::constant(ProjectionValue::string(
            "private-body-schema",
        ));
        let rejected_schema = client_preview_source(
            &private_schema,
            false,
            Some(ProjectionEnvelopeField::BodySchema),
            None,
            None,
            None,
            None,
            &ProjectionValueType::String,
            &string_command,
        )
        .unwrap();
        assert_eq!(rejected_schema, ClientProjectionPreviewSource::Unknown);
        assert!(!serde_json::to_string(&rejected_schema)
            .unwrap()
            .contains("private-body-schema"));

        let enum_command = command("TodoStatus");
        assert!(matches!(
            client_preview_source(
                &input,
                true,
                None,
                Some(ProjectionPortableType::Custom),
                Some("app::TodoStatus"),
                Some(false),
                Some(true),
                &ProjectionValueType::Json,
                &enum_command,
            )
            .unwrap(),
            ClientProjectionPreviewSource::Input { .. }
        ));

        assert_eq!(
            client_preview_source(
                &CommandProjectionPreviewSource::constant(ProjectionValue::unsigned(1)),
                true,
                None,
                Some(ProjectionPortableType::I64),
                Some("i64"),
                Some(false),
                Some(true),
                &ProjectionValueType::I64,
                &string_command,
            )
            .unwrap(),
            ClientProjectionPreviewSource::Unknown,
            "a u64-tagged constant cannot fill an i64 slot"
        );
        assert!(matches!(
            client_preview_source(
                &CommandProjectionPreviewSource::constant(ProjectionValue::signed(-1)),
                true,
                None,
                Some(ProjectionPortableType::I64),
                Some("i64"),
                Some(false),
                Some(true),
                &ProjectionValueType::I64,
                &string_command,
            )
            .unwrap(),
            ClientProjectionPreviewSource::Constant { .. }
        ));

        for (source, nullable, always_present, expected) in [
            (
                CommandProjectionPreviewSource::Null,
                false,
                true,
                ClientProjectionPreviewSource::Unknown,
            ),
            (
                CommandProjectionPreviewSource::Null,
                true,
                true,
                ClientProjectionPreviewSource::Null,
            ),
            (
                CommandProjectionPreviewSource::Absent,
                true,
                true,
                ClientProjectionPreviewSource::Unknown,
            ),
            (
                CommandProjectionPreviewSource::Absent,
                true,
                false,
                ClientProjectionPreviewSource::Absent,
            ),
        ] {
            assert_eq!(
                client_preview_source(
                    &source,
                    true,
                    None,
                    Some(ProjectionPortableType::String),
                    Some("String"),
                    Some(nullable),
                    Some(always_present),
                    &ProjectionValueType::String,
                    &string_command,
                )
                .unwrap(),
                expected
            );
        }
    }
}
