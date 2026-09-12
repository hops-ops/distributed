use std::collections::{BTreeMap, BTreeSet};

use super::*;

const MAX_PROJECTION_ITEMS: usize = 128;
// Frozen with `distributed::MAX_PROJECTION_EXPRESSION_DEPTH`. The CLI crate is
// intentionally dependency-free from the runtime crate, so keep a boundary
// test below to prevent this executable-contract limit from drifting.
const MAX_EXPRESSION_DEPTH: usize = 64;

pub(crate) fn validate_projection_manifest(
    mut programs: Vec<ManifestProjectionProgram>,
    mut bindings: Vec<ManifestProjectionBinding>,
    models: &BTreeMap<String, ManifestModel>,
) -> Result<
    (
        Vec<ManifestProjectionProgram>,
        Vec<ManifestProjectionBinding>,
    ),
    ClientCompileError,
> {
    if programs.len() > MAX_PROJECTION_ITEMS || bindings.len() > MAX_PROJECTION_ITEMS {
        return Err(projection_error(
            "client.manifest.projection_inventory",
            "projection program/binding inventory exceeds 128 entries",
        ));
    }
    programs.sort_by(|left, right| left.program_id.cmp(&right.program_id));
    bindings.sort_by(|left, right| {
        (
            &left.program_id,
            &left.binding_id,
            &left.epoch,
            left.placement as u8,
            left.execution_class as u8,
            left.state as u8,
        )
            .cmp(&(
                &right.program_id,
                &right.binding_id,
                &right.epoch,
                right.placement as u8,
                right.execution_class as u8,
                right.state as u8,
            ))
    });

    let mut program_ids = BTreeSet::new();
    let mut event_contracts = BTreeMap::new();
    for program in &programs {
        validate_program(program, models)?;
        if !program_ids.insert(program.program_id.as_str()) {
            return Err(projection_error(
                "client.manifest.projection_program_id",
                format!("duplicate projection program `{}`", program.program_id),
            ));
        }
        for arm in &program.arms {
            let contract = (arm.event.name.as_str(), arm.event.version);
            if event_contracts
                .insert(arm.event.id.as_str(), contract)
                .is_some_and(|existing| existing != contract)
            {
                return Err(projection_error(
                    "client.manifest.projection_event_identity",
                    format!(
                        "projection event id `{}` maps to conflicting name/version contracts",
                        arm.event.id
                    ),
                ));
            }
        }
    }

    let mut binding_ids = BTreeSet::new();
    let mut active_programs = BTreeSet::new();
    for binding in &bindings {
        validate_binding(binding)?;
        if !binding_ids.insert(binding.binding_id.as_str()) {
            return Err(projection_error(
                "client.manifest.projection_binding_id",
                format!("duplicate projection binding `{}`", binding.binding_id),
            ));
        }
        if binding.state == ManifestProjectionBindingState::Active
            && !active_programs.insert(binding.program_id.as_str())
        {
            return Err(projection_error(
                "client.manifest.projection_placement",
                format!(
                    "projection program `{}` has more than one active placement",
                    binding.program_id
                ),
            ));
        }
        if binding.placement == ManifestProjectionPlacement::Eventual
            && !program_ids.contains(binding.program_id.as_str())
        {
            return Err(projection_error(
                "client.manifest.projection_binding_program",
                format!(
                    "eventual projection binding `{}` references missing portable program `{}`",
                    binding.binding_id, binding.program_id
                ),
            ));
        }
        // Direct may export the same portable program for `.applies` previews.
        // Server apply site is still the command handler (Atomic response);
        // client never runs this as an async eventual obligation.
        if binding.placement == ManifestProjectionPlacement::Direct
            && program_ids.contains(binding.program_id.as_str())
            && binding.execution_class != ManifestProjectionExecutionClass::Causal
        {
            return Err(projection_error(
                "client.manifest.projection_placement",
                format!(
                    "direct projection binding `{}` may only expose a program when causal (preview IR)",
                    binding.binding_id
                ),
            ));
        }
    }
    Ok((programs, bindings))
}

/// Active causal binding that may contribute client preview composition.
///
/// Eventual: async projector path + local `.applies` previews.
/// Direct: same mutation IR; handler-owned Atomic apply; previews optional.
fn is_preview_eligible_binding(binding: &ManifestProjectionBinding) -> bool {
    binding.state == ManifestProjectionBindingState::Active
        && binding.execution_class == ManifestProjectionExecutionClass::Causal
        && matches!(
            binding.placement,
            ManifestProjectionPlacement::Eventual | ManifestProjectionPlacement::Direct
        )
}

pub(crate) fn validate_command_projections(
    commands: &[ManifestCommand],
    programs: &[ManifestProjectionProgram],
    bindings: &[ManifestProjectionBinding],
    models: &BTreeMap<String, ManifestModel>,
) -> Result<BTreeSet<String>, ClientCompileError> {
    let programs = programs
        .iter()
        .map(|program| (program.program_id.as_str(), program))
        .collect::<BTreeMap<_, _>>();
    let mut requiring_revalidation = BTreeSet::new();
    for command in commands {
        let Some(projection) = &command.extensions.projection else {
            requiring_revalidation.insert(command.name.clone());
            continue;
        };
        if projection.version != COMMAND_PROJECTION_EXTENSION_VERSION {
            return Err(command_projection_error(
                command,
                "client.manifest.command_projection_version",
                format!("projection version must be {COMMAND_PROJECTION_EXTENSION_VERSION}"),
            ));
        }
        if projection.event_set.is_empty() || projection.program_arms.is_empty() {
            return Err(command_projection_error(
                command,
                "client.manifest.command_projection_inventory",
                "projection event_set and program_arms must be non-empty",
            ));
        }
        if projection.event_set.len() > MAX_PROJECTION_ITEMS
            || projection.program_arms.len() > MAX_PROJECTION_ITEMS
            || projection.preview_occurrences.len() > MAX_PROJECTION_ITEMS
        {
            return Err(command_projection_error(
                command,
                "client.manifest.command_projection_inventory",
                format!(
                    "projection event_set, program_arms, and preview occurrences cannot exceed \
                     {MAX_PROJECTION_ITEMS} entries"
                ),
            ));
        }
        if projection
            .event_set
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(command_projection_error(
                command,
                "client.manifest.command_projection_event_set",
                "projection event_set must be sorted and unique",
            ));
        }
        if projection.program_arms.windows(2).any(|pair| {
            (&pair[0].event, &pair[0].program_id, &pair[0].arm)
                >= (&pair[1].event, &pair[1].program_id, &pair[1].arm)
        }) {
            return Err(command_projection_error(
                command,
                "client.manifest.command_projection_arm_refs",
                "projection program_arms must be sorted and unique",
            ));
        }
        let arm_events = projection
            .program_arms
            .iter()
            .map(|arm| arm.event.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if arm_events != projection.event_set {
            return Err(command_projection_error(
                command,
                "client.manifest.command_projection_event_set",
                "projection event_set must exactly match selected program arms",
            ));
        }

        let mut selected_slots = BTreeMap::<
            ManifestProjectionEventRef,
            BTreeMap<String, ManifestProjectionValueType>,
        >::new();
        let mut selected_programs = BTreeSet::new();
        for selected in &projection.program_arms {
            validate_event_ref(&selected.event)?;
            validate_program_id(&selected.program_id)?;
            let program = programs.get(selected.program_id.as_str()).ok_or_else(|| {
                command_projection_error(
                    command,
                    "client.manifest.command_projection_program",
                    format!(
                        "selected projection program `{}` is absent",
                        selected.program_id
                    ),
                )
            })?;
            let arm = program
                .arms
                .iter()
                .find(|arm| arm.arm == selected.arm && arm.event == selected.event)
                .ok_or_else(|| {
                    command_projection_error(
                        command,
                        "client.manifest.command_projection_arm",
                        format!(
                            "selected arm `{}` does not exactly match event `{}` in program `{}`",
                            selected.arm, selected.event.id, selected.program_id
                        ),
                    )
                })?;
            let eligible = bindings.iter().filter(|binding| {
                binding.program_id == selected.program_id && is_preview_eligible_binding(binding)
            });
            if eligible.count() != 1 {
                return Err(command_projection_error(
                    command,
                    "client.manifest.command_projection_eligibility",
                    format!(
                        "selected program `{}` requires exactly one active causal binding (eventual or direct)",
                        selected.program_id
                    ),
                ));
            }
            selected_programs.insert(selected.program_id.as_str());
            collect_arm_slots(
                arm,
                selected_slots.entry(selected.event.clone()).or_default(),
            )?;
        }

        if projection.preview_occurrences.is_empty() {
            requiring_revalidation.insert(command.name.clone());
        }
        for (index, occurrence) in projection.preview_occurrences.iter().enumerate() {
            if occurrence.values.len() > MAX_PROJECTION_ITEMS {
                return Err(command_projection_error(
                    command,
                    "client.manifest.command_projection_preview_inventory",
                    format!(
                        "one projection preview occurrence cannot exceed \
                         {MAX_PROJECTION_ITEMS} values"
                    ),
                ));
            }
            if occurrence.ordinal as usize != index {
                return Err(command_projection_error(
                    command,
                    "client.manifest.command_projection_preview_order",
                    "preview occurrence ordinals must be dense and zero-based",
                ));
            }
            let expected_slots = selected_slots.get(&occurrence.event).ok_or_else(|| {
                command_projection_error(
                    command,
                    "client.manifest.command_projection_preview_event",
                    "preview occurrence event has no selected projection arm",
                )
            })?;
            let actual_slots = occurrence
                .values
                .iter()
                .map(|value| value.slot.clone())
                .collect::<BTreeSet<_>>();
            let expected_slot_names = expected_slots.keys().cloned().collect::<BTreeSet<_>>();
            if occurrence
                .values
                .windows(2)
                .any(|pair| pair[0].slot >= pair[1].slot)
                || actual_slots.len() != occurrence.values.len()
                || actual_slots != expected_slot_names
            {
                return Err(command_projection_error(
                    command,
                    "client.manifest.command_projection_preview_slots",
                    "preview values must exactly cover the selected arm slots in canonical order",
                ));
            }
            for value in &occurrence.values {
                let expected = expected_slots
                    .get(&value.slot)
                    .expect("exact slot coverage was validated");
                validate_preview_source(command, &value.source, expected)?;
            }
        }
        if selected_programs.is_empty() {
            requiring_revalidation.insert(command.name.clone());
        }
        let _ = models;
    }
    Ok(requiring_revalidation)
}

fn validate_program(
    program: &ManifestProjectionProgram,
    models: &BTreeMap<String, ManifestModel>,
) -> Result<(), ClientCompileError> {
    if program.version != CLIENT_PROJECTION_PROGRAM_VERSION
        || program.ir_version != PROJECTION_PROGRAM_IR_VERSION
        || program.operation_semantics_version != PROJECTION_OPERATION_SEMANTICS_VERSION
        || program.program_version == 0
    {
        return Err(projection_error(
            "client.manifest.projection_program_version",
            format!(
                "projection program `{}` has an unsupported executable version",
                program.program_id
            ),
        ));
    }
    validate_program_id(&program.program_id)?;
    validate_nonempty(&program.name, "projection program name")?;
    if program.arms.is_empty() || program.arms.len() > MAX_PROJECTION_ITEMS {
        return Err(projection_error(
            "client.manifest.projection_program_arms",
            format!(
                "projection program `{}` must contain 1..={MAX_PROJECTION_ITEMS} arms",
                program.program_id
            ),
        ));
    }
    let mut arm_ids = BTreeSet::new();
    let mut operation_ids = BTreeSet::new();
    let mut previous = None;
    for arm in &program.arms {
        validate_nonempty(&arm.arm, "projection arm id")?;
        if !arm_ids.insert(arm.arm.as_str()) {
            return Err(projection_error(
                "client.manifest.projection_arm_id",
                format!(
                    "projection program `{}` repeats arm `{}`",
                    program.program_id, arm.arm
                ),
            ));
        }
        let order = (&arm.event, arm.arm.as_str());
        if previous.is_some_and(|previous| previous >= order) {
            return Err(projection_error(
                "client.manifest.projection_arm_order",
                format!(
                    "projection program `{}` arms must use canonical event/arm order",
                    program.program_id
                ),
            ));
        }
        previous = Some(order);
        validate_event_ref(&arm.event)?;
        validate_partition(&arm.partition)?;
        if arm.operations.is_empty() || arm.operations.len() > MAX_PROJECTION_ITEMS {
            return Err(projection_error(
                "client.manifest.projection_operations",
                format!(
                    "projection arm `{}` has an invalid operation count",
                    arm.arm
                ),
            ));
        }
        for (index, operation) in arm.operations.iter().enumerate() {
            if !operation_ids.insert(operation.operation.as_str()) {
                return Err(projection_error(
                    "client.manifest.projection_operation_id",
                    format!(
                        "projection program `{}` repeats operation id `{}`",
                        program.program_id, operation.operation
                    ),
                ));
            }
            if operation.ordinal as usize != index {
                return Err(projection_error(
                    "client.manifest.projection_operation_order",
                    format!(
                        "projection arm `{}` operation ordinals must be dense and zero-based",
                        arm.arm
                    ),
                ));
            }
            validate_operation(program, arm, operation, models)?;
        }
    }
    Ok(())
}

fn validate_binding(binding: &ManifestProjectionBinding) -> Result<(), ClientCompileError> {
    if binding.version != CLIENT_PROJECTION_BINDING_VERSION {
        return Err(projection_error(
            "client.manifest.projection_binding_version",
            format!(
                "projection binding `{}` must use version {CLIENT_PROJECTION_BINDING_VERSION}",
                binding.binding_id
            ),
        ));
    }
    validate_binding_id(&binding.binding_id)?;
    validate_program_id(&binding.program_id)?;
    validate_projection_epoch(&binding.epoch, "projection binding epoch")
}

fn validate_operation(
    program: &ManifestProjectionProgram,
    arm: &ManifestProjectionArm,
    operation: &ManifestProjectionOperation,
    models: &BTreeMap<String, ManifestModel>,
) -> Result<(), ClientCompileError> {
    validate_nonempty(&operation.operation, "projection operation id")?;
    let model = models.get(&operation.model).ok_or_else(|| {
        projection_error(
            "client.manifest.projection_model",
            format!(
                "projection program `{}` arm `{}` references absent model `{}`",
                program.program_id, arm.arm, operation.model
            ),
        )
    })?;
    let Some(identity) = model.identity() else {
        if operation.kind != ManifestProjectionMutationKind::InvalidateModel {
            return Err(projection_error(
                "client.manifest.projection_model",
                format!(
                    "projection operation `{}` targets non-normalized model `{}`",
                    operation.operation, operation.model
                ),
            ));
        }
        if !operation.key.is_empty()
            || !operation.fields.is_empty()
            || !operation.relationships.is_empty()
        {
            return Err(projection_error(
                "client.manifest.projection_invalidation",
                format!(
                    "embedded model invalidation `{}` cannot carry record keys, fields, or \
                     relationship effects",
                    operation.operation
                ),
            ));
        }
        validate_invalidations(operation, models)?;
        if !matches!(
            operation.invalidations.as_slice(),
            [ManifestProjectionInvalidation::Model { model }] if model == &operation.model
        ) {
            return Err(projection_error(
                "client.manifest.projection_invalidation",
                format!(
                    "embedded model invalidation `{}` must carry exactly one model invalidation \
                     for `{}`",
                    operation.operation, operation.model
                ),
            ));
        }
        return Ok(());
    };
    validate_key(&operation.key, identity, "projection operation key")?;
    validate_projection_fields(operation, model)?;

    match operation.kind {
        ManifestProjectionMutationKind::Insert
        | ManifestProjectionMutationKind::Upsert
        | ManifestProjectionMutationKind::Recreate
        | ManifestProjectionMutationKind::InsertRelated
        | ManifestProjectionMutationKind::UpsertRelated => {
            if operation.fields.is_empty() {
                return Err(projection_error(
                    "client.manifest.projection_field_mask",
                    format!(
                        "complete projection operation `{}` must expose a field mapping",
                        operation.operation
                    ),
                ));
            }
        }
        ManifestProjectionMutationKind::Patch | ManifestProjectionMutationKind::UpsertPatch => {
            if operation.fields.is_empty() {
                return Err(projection_error(
                    "client.manifest.projection_field_mask",
                    format!(
                        "patch projection operation `{}` must expose a field mapping",
                        operation.operation
                    ),
                ));
            }
        }
        ManifestProjectionMutationKind::Delete => {
            if !operation.fields.is_empty() {
                return Err(projection_error(
                    "client.manifest.projection_field_mask",
                    "delete projection operations cannot carry fields",
                ));
            }
        }
        ManifestProjectionMutationKind::InvalidateModel => {}
        ManifestProjectionMutationKind::InvalidateRelationship => {
            return Err(projection_error(
                "client.manifest.projection_invalidation",
                "top-level relationship invalidation has no source-key provenance; use an \
                 explicit relationship effect with kind `invalidate`",
            ));
        }
    }
    validate_relationships(operation, models)?;
    validate_invalidations(operation, models)
}

fn validate_projection_fields(
    operation: &ManifestProjectionOperation,
    model: &ManifestModel,
) -> Result<(), ClientCompileError> {
    let identity = model
        .identity()
        .expect("projection operation model was normalized");
    let identity_names = identity
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    let mut names = BTreeSet::new();
    for (index, field) in operation.fields.iter().enumerate() {
        if field.ordinal as usize != index
            || !names.insert(field.name.as_str())
            || identity_names.contains(field.name.as_str())
        {
            return Err(projection_error(
                "client.manifest.projection_field_mask",
                format!(
                    "projection operation `{}` has a noncanonical, duplicate, or key field assignment",
                    operation.operation
                ),
            ));
        }
        if model.field(&field.name).is_none() {
            return Err(projection_error(
                "client.manifest.projection_field_mask",
                format!(
                    "projection operation `{}` references absent field `{}`",
                    operation.operation, field.name
                ),
            ));
        }
        if let ManifestProjectionAssignment::Set { expression } = &field.assignment {
            validate_expression(expression, 1)?;
        }
    }
    Ok(())
}

fn validate_relationships(
    operation: &ManifestProjectionOperation,
    models: &BTreeMap<String, ManifestModel>,
) -> Result<(), ClientCompileError> {
    let mut identities = BTreeSet::new();
    for (index, effect) in operation.relationships.iter().enumerate() {
        if effect.ordinal as usize != index {
            return Err(projection_error(
                "client.manifest.projection_relationship_order",
                "projection relationship ordinals must be dense and zero-based",
            ));
        }
        let source = models.get(&effect.source_model).ok_or_else(|| {
            projection_error(
                "client.manifest.projection_relationship",
                "projection relationship source model is absent",
            )
        })?;
        let target = models.get(&effect.target_model).ok_or_else(|| {
            projection_error(
                "client.manifest.projection_relationship",
                "projection relationship target model is absent",
            )
        })?;
        let relationship = source.relationship(&effect.relationship).ok_or_else(|| {
            projection_error(
                "client.manifest.projection_relationship",
                format!(
                    "model `{}` has no relationship `{}`",
                    effect.source_model, effect.relationship
                ),
            )
        })?;
        if relationship.target_model != effect.target_model {
            return Err(projection_error(
                "client.manifest.projection_relationship",
                "projection relationship target does not match selected Surface metadata",
            ));
        }
        let source_identity = source.identity().ok_or_else(|| {
            projection_error(
                "client.manifest.projection_relationship",
                "projection relationship source is not normalized",
            )
        })?;
        let target_identity = target.identity().ok_or_else(|| {
            projection_error(
                "client.manifest.projection_relationship",
                "projection relationship target is not normalized",
            )
        })?;
        validate_key(
            &effect.source_key,
            source_identity,
            "projection relationship source key",
        )?;
        validate_key(
            &effect.target_key,
            target_identity,
            "projection relationship target key",
        )?;
        let identity = (
            effect.source_model.as_str(),
            effect.relationship.as_str(),
            effect.target_model.as_str(),
            effect.kind as u8,
        );
        if !identities.insert(identity) {
            return Err(projection_error(
                "client.manifest.projection_relationship",
                "projection operation repeats a relationship consequence",
            ));
        }
    }
    Ok(())
}

fn validate_invalidations(
    operation: &ManifestProjectionOperation,
    models: &BTreeMap<String, ManifestModel>,
) -> Result<(), ClientCompileError> {
    let mut identities = BTreeSet::new();
    for invalidation in &operation.invalidations {
        let identity = match invalidation {
            ManifestProjectionInvalidation::Model { model } => {
                if !models.contains_key(model) {
                    return Err(projection_error(
                        "client.manifest.projection_invalidation",
                        format!("projection invalidation model `{model}` is absent"),
                    ));
                }
                format!("model:{model}")
            }
            ManifestProjectionInvalidation::Relationship {
                source_model,
                relationship,
                target_model,
            } => {
                let source = models.get(source_model).ok_or_else(|| {
                    projection_error(
                        "client.manifest.projection_invalidation",
                        "relationship invalidation source is absent",
                    )
                })?;
                if source
                    .relationship(relationship)
                    .is_none_or(|candidate| candidate.target_model != *target_model)
                {
                    return Err(projection_error(
                        "client.manifest.projection_invalidation",
                        "relationship invalidation does not match selected Surface metadata",
                    ));
                }
                format!("relationship:{source_model}:{relationship}:{target_model}")
            }
        };
        if !identities.insert(identity) {
            return Err(projection_error(
                "client.manifest.projection_invalidation",
                "projection operation repeats an invalidation scope",
            ));
        }
    }
    Ok(())
}

fn validate_key(
    key: &[ManifestProjectionKeyField],
    identity: &[ManifestKeyField],
    label: &str,
) -> Result<(), ClientCompileError> {
    if key.len() != identity.len() {
        return Err(projection_error(
            "client.manifest.projection_key",
            format!("{label} must exactly cover the normalized identity"),
        ));
    }
    for (index, (field, expected)) in key.iter().zip(identity).enumerate() {
        if field.ordinal as usize != index || field.name != expected.name {
            return Err(projection_error(
                "client.manifest.projection_key",
                format!("{label} must use declared primary-key order"),
            ));
        }
        validate_expression(&field.expression, 1)?;
    }
    Ok(())
}

fn validate_partition(partition: &ManifestProjectionPartition) -> Result<(), ClientCompileError> {
    if let ManifestProjectionPartition::Expression { expression } = partition {
        validate_expression(expression, 1)?;
    }
    Ok(())
}

fn validate_expression(
    expression: &ManifestProjectionExpression,
    depth: usize,
) -> Result<(), ClientCompileError> {
    if depth > MAX_EXPRESSION_DEPTH {
        return Err(projection_error(
            "client.manifest.projection_expression_depth",
            "projection expression exceeds maximum depth 64",
        ));
    }
    match expression {
        ManifestProjectionExpression::Slot {
            slot,
            value_type: _,
        } => validate_nonempty(slot, "projection expression slot"),
        ManifestProjectionExpression::Envelope { field: _ } => Ok(()),
        ManifestProjectionExpression::Constant { value } => validate_value(value, depth),
        ManifestProjectionExpression::Enum { enum_type, variant } => {
            validate_nonempty(enum_type, "projection enum type")?;
            validate_nonempty(variant, "projection enum variant")
        }
        ManifestProjectionExpression::List { values } => {
            for value in values {
                validate_expression(value, depth + 1)?;
            }
            Ok(())
        }
        ManifestProjectionExpression::Object { fields } => {
            if fields.windows(2).any(|pair| pair[0].name >= pair[1].name) {
                return Err(projection_error(
                    "client.manifest.projection_expression_object",
                    "projection object expression fields must be sorted and unique",
                ));
            }
            for field in fields {
                validate_nonempty(&field.name, "projection object field")?;
                validate_expression(&field.value, depth + 1)?;
            }
            Ok(())
        }
        ManifestProjectionExpression::Transform {
            transform: _,
            arguments,
        } => {
            if arguments.is_empty() {
                return Err(projection_error(
                    "client.manifest.projection_transform",
                    "projection transform requires arguments",
                ));
            }
            for argument in arguments {
                validate_expression(argument, depth + 1)?;
            }
            Ok(())
        }
    }
}

fn validate_value(value: &ManifestProjectionValue, depth: usize) -> Result<(), ClientCompileError> {
    if depth > MAX_EXPRESSION_DEPTH {
        return Err(projection_error(
            "client.manifest.projection_value_depth",
            "projection value exceeds maximum depth 64",
        ));
    }
    match value {
        ManifestProjectionValue::Null | ManifestProjectionValue::Boolean(_) => Ok(()),
        ManifestProjectionValue::I64(value) => validate_canonical_i64(value),
        ManifestProjectionValue::U64(value) => validate_canonical_u64(value),
        ManifestProjectionValue::F64(value) => validate_canonical_f64(value),
        ManifestProjectionValue::String(value) => {
            if value.chars().any(char::is_control) {
                return Err(projection_error(
                    "client.manifest.projection_value",
                    "projection string values cannot contain control characters",
                ));
            }
            Ok(())
        }
        ManifestProjectionValue::Enum { enum_type, variant } => {
            validate_nonempty(enum_type, "projection enum type")?;
            validate_nonempty(variant, "projection enum variant")
        }
        ManifestProjectionValue::List(values) => {
            for value in values {
                validate_value(value, depth + 1)?;
            }
            Ok(())
        }
        ManifestProjectionValue::Object(fields) => {
            if fields.windows(2).any(|pair| pair[0].name >= pair[1].name) {
                return Err(projection_error(
                    "client.manifest.projection_value_object",
                    "projection object value fields must be sorted and unique",
                ));
            }
            for field in fields {
                validate_value(&field.value, depth + 1)?;
            }
            Ok(())
        }
    }
}

fn validate_preview_source(
    command: &ManifestCommand,
    source: &ManifestProjectionPreviewSource,
    expected: &ManifestProjectionValueType,
) -> Result<(), ClientCompileError> {
    match source {
        ManifestProjectionPreviewSource::Input { path } => {
            let field = validate_shape_path(&command.input, path, "input")?;
            validate_input_source_type(command, field, expected, "input")
        }
        ManifestProjectionPreviewSource::GeneratedDefault { path } => {
            let field = validate_shape_path(&command.input, path, "generated default")?;
            let declared = command
                .extensions
                .input_defaults
                .as_ref()
                .is_some_and(|defaults| {
                    defaults
                        .defaults
                        .iter()
                        .any(|default| default.path == *path)
                });
            if !declared {
                return Err(command_projection_error(
                    command,
                    "client.manifest.command_projection_default",
                    "projection preview references a path not owned by input_defaults",
                ));
            }
            validate_input_source_type(command, field, expected, "generated default")
        }
        ManifestProjectionPreviewSource::TrustedPreset { name, codec } => {
            let declared = command
                .extensions
                .trusted_presets
                .iter()
                .any(|preset| preset.name == *name && preset.codec == *codec);
            if !declared {
                return Err(command_projection_error(
                    command,
                    "client.manifest.command_projection_preset",
                    "projection preview references an undeclared trusted scoped preset",
                ));
            }
            if !codec_compatible(codec, expected) {
                return Err(command_projection_error(
                    command,
                    "client.manifest.command_projection_source_type",
                    "projection preview trusted preset codec does not match the selected slot type",
                ));
            }
            Ok(())
        }
        ManifestProjectionPreviewSource::Constant { value } => {
            validate_value(value, 1)?;
            if !constant_compatible(value, expected) {
                return Err(command_projection_error(
                    command,
                    "client.manifest.command_projection_source_type",
                    "projection preview constant tag does not match the selected slot type",
                ));
            }
            Ok(())
        }
        ManifestProjectionPreviewSource::Null
        | ManifestProjectionPreviewSource::Absent
        | ManifestProjectionPreviewSource::Unknown => Ok(()),
    }
}

fn validate_shape_path<'a>(
    shape: &'a ManifestCommandShape,
    path: &[String],
    label: &str,
) -> Result<&'a ManifestTypeField, ClientCompileError> {
    let ManifestCommandShape::Object { definition } = shape else {
        return Err(projection_error(
            "client.manifest.command_projection_path",
            format!("{label} path cannot target a command without input"),
        ));
    };
    if path.is_empty() {
        return Err(projection_error(
            "client.manifest.command_projection_path",
            format!("{label} path must not be empty"),
        ));
    }
    let mut current = definition;
    for (index, segment) in path.iter().enumerate() {
        let field = current
            .fields
            .iter()
            .find(|field| field.name == *segment)
            .ok_or_else(|| {
                projection_error(
                    "client.manifest.command_projection_path",
                    format!("{label} path references an absent command input field"),
                )
            })?;
        if index + 1 == path.len() {
            return Ok(field);
        }
        current = field.nested.as_deref().ok_or_else(|| {
            projection_error(
                "client.manifest.command_projection_path",
                format!("{label} path traverses a scalar command input field"),
            )
        })?;
    }
    unreachable!("a non-empty shape path returns from its final segment")
}

fn validate_input_source_type(
    command: &ManifestCommand,
    field: &ManifestTypeField,
    expected: &ManifestProjectionValueType,
    label: &str,
) -> Result<(), ClientCompileError> {
    let compatible = !field.list
        && field.nested.is_none()
        && field
            .codec
            .as_deref()
            .is_some_and(|codec| input_codec_compatible(&field.type_name, codec, expected));
    if !compatible {
        return Err(command_projection_error(
            command,
            "client.manifest.command_projection_source_type",
            format!("projection preview {label} must resolve to a compatible non-list scalar leaf"),
        ));
    }
    Ok(())
}

fn input_codec_compatible(
    type_name: &str,
    codec: &str,
    expected: &ManifestProjectionValueType,
) -> bool {
    match expected {
        ManifestProjectionValueType::Boolean => type_name == "Boolean" && codec == "boolean",
        ManifestProjectionValueType::I64 => matches!(
            (type_name, codec),
            ("Int", "int32") | ("BigInt", "json_number_precision_limited")
        ),
        ManifestProjectionValueType::U64 => type_name == "BigInt" && unsigned_codec(codec),
        ManifestProjectionValueType::F64 => type_name == "Float" && codec == "float64",
        ManifestProjectionValueType::String => matches!(
            (type_name, codec),
            ("ID" | "String", "string") | ("Timestamptz", "string_unvalidated_timestamp")
        ),
        ManifestProjectionValueType::Enum(enum_type) => type_name == enum_type && codec == "string",
        ManifestProjectionValueType::Json => type_name == "JSON" && codec == "json",
    }
}

fn codec_compatible(codec: &str, expected: &ManifestProjectionValueType) -> bool {
    match expected {
        ManifestProjectionValueType::Boolean => codec == "boolean",
        ManifestProjectionValueType::I64 => {
            matches!(codec, "int32" | "json_number_precision_limited")
        }
        ManifestProjectionValueType::U64 => unsigned_codec(codec),
        ManifestProjectionValueType::F64 => codec == "float64",
        ManifestProjectionValueType::String | ManifestProjectionValueType::Enum(_) => {
            codec == "string"
        }
        ManifestProjectionValueType::Json => codec == "json",
    }
}

fn unsigned_codec(codec: &str) -> bool {
    matches!(codec, "uint8" | "uint16" | "uint32" | "uint64_safe_integer")
}

fn constant_compatible(
    value: &ManifestProjectionValue,
    expected: &ManifestProjectionValueType,
) -> bool {
    match (value, expected) {
        (ManifestProjectionValue::Boolean(_), ManifestProjectionValueType::Boolean)
        | (ManifestProjectionValue::I64(_), ManifestProjectionValueType::I64)
        | (ManifestProjectionValue::U64(_), ManifestProjectionValueType::U64)
        | (ManifestProjectionValue::F64(_), ManifestProjectionValueType::F64)
        | (ManifestProjectionValue::String(_), ManifestProjectionValueType::String) => true,
        (
            ManifestProjectionValue::Enum { enum_type, .. },
            ManifestProjectionValueType::Enum(expected),
        ) => enum_type == expected,
        (_, ManifestProjectionValueType::Json) => true,
        _ => false,
    }
}

fn collect_arm_slots(
    arm: &ManifestProjectionArm,
    slots: &mut BTreeMap<String, ManifestProjectionValueType>,
) -> Result<(), ClientCompileError> {
    if let ManifestProjectionPartition::Expression { expression } = &arm.partition {
        collect_expression_slots(expression, slots)?;
    }
    for operation in &arm.operations {
        for field in &operation.key {
            collect_expression_slots(&field.expression, slots)?;
        }
        for field in &operation.fields {
            if let ManifestProjectionAssignment::Set { expression } = &field.assignment {
                collect_expression_slots(expression, slots)?;
            }
        }
        for relationship in &operation.relationships {
            for field in relationship
                .source_key
                .iter()
                .chain(&relationship.target_key)
            {
                collect_expression_slots(&field.expression, slots)?;
            }
        }
    }
    Ok(())
}

fn collect_expression_slots(
    expression: &ManifestProjectionExpression,
    slots: &mut BTreeMap<String, ManifestProjectionValueType>,
) -> Result<(), ClientCompileError> {
    match expression {
        ManifestProjectionExpression::Slot { slot, value_type } => {
            if slots
                .insert(slot.clone(), value_type.clone())
                .is_some_and(|existing| existing != *value_type)
            {
                return Err(projection_error(
                    "client.manifest.command_projection_slot_type",
                    format!("projection slot `{slot}` has conflicting value types"),
                ));
            }
        }
        ManifestProjectionExpression::List { values } => {
            for value in values {
                collect_expression_slots(value, slots)?;
            }
        }
        ManifestProjectionExpression::Object { fields } => {
            for field in fields {
                collect_expression_slots(&field.value, slots)?;
            }
        }
        ManifestProjectionExpression::Transform { arguments, .. } => {
            for argument in arguments {
                collect_expression_slots(argument, slots)?;
            }
        }
        ManifestProjectionExpression::Envelope { .. }
        | ManifestProjectionExpression::Constant { .. }
        | ManifestProjectionExpression::Enum { .. } => {}
    }
    Ok(())
}

fn validate_event_ref(event: &ManifestProjectionEventRef) -> Result<(), ClientCompileError> {
    validate_nonempty(&event.id, "projection event id")?;
    validate_nonempty(&event.name, "projection event name")?;
    if event.version == 0 {
        return Err(projection_error(
            "client.manifest.projection_event_version",
            "projection event version must be non-zero",
        ));
    }
    Ok(())
}

fn validate_program_id(value: &str) -> Result<(), ClientCompileError> {
    validate_prefixed_hash(value, "pp1:", "projection program id")
}

fn validate_binding_id(value: &str) -> Result<(), ClientCompileError> {
    validate_prefixed_hash(value, "pb1:", "projection binding id")
}

fn validate_prefixed_hash(
    value: &str,
    prefix: &str,
    label: &str,
) -> Result<(), ClientCompileError> {
    let Some(hash) = value.strip_prefix(prefix) else {
        return Err(projection_error(
            "client.manifest.projection_identity",
            format!("{label} must use the `{prefix}sha256:` format"),
        ));
    };
    validate_hash(hash, label)
}

fn validate_canonical_i64(value: &str) -> Result<(), ClientCompileError> {
    value
        .parse::<i64>()
        .ok()
        .filter(|parsed| parsed.to_string() == value)
        .map(|_| ())
        .ok_or_else(|| {
            projection_error(
                "client.manifest.projection_number",
                "projection i64 must be canonical and in range",
            )
        })
}

fn validate_canonical_u64(value: &str) -> Result<(), ClientCompileError> {
    value
        .parse::<u64>()
        .ok()
        .filter(|parsed| parsed.to_string() == value)
        .map(|_| ())
        .ok_or_else(|| {
            projection_error(
                "client.manifest.projection_number",
                "projection u64 must be canonical and in range",
            )
        })
}

fn validate_canonical_f64(value: &str) -> Result<(), ClientCompileError> {
    let Some(parsed) = value.parse::<f64>().ok().filter(|value| value.is_finite()) else {
        return Err(projection_error(
            "client.manifest.projection_number",
            "projection f64 must be finite and canonical",
        ));
    };
    let canonical = serde_json::Number::from_f64(if parsed == 0.0 { 0.0 } else { parsed })
        .expect("finite float")
        .to_string();
    if canonical != value {
        return Err(projection_error(
            "client.manifest.projection_number",
            "projection f64 must be finite and canonical",
        ));
    }
    Ok(())
}

fn projection_error(code: &'static str, message: impl Into<String>) -> ClientCompileError {
    ClientCompileError::manifest(code, message)
}

fn command_projection_error(
    command: &ManifestCommand,
    code: &'static str,
    message: impl Into<String>,
) -> ClientCompileError {
    projection_error(
        code,
        format!("manifest command `{}` {}", command.name, message.into()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested_expression(depth: usize) -> ManifestProjectionExpression {
        let mut expression = ManifestProjectionExpression::Constant {
            value: ManifestProjectionValue::Null,
        };
        for _ in 1..depth {
            expression = ManifestProjectionExpression::List {
                values: vec![expression],
            };
        }
        expression
    }

    #[test]
    fn projection_expression_accepts_the_frozen_depth_limit() {
        validate_expression(&nested_expression(MAX_EXPRESSION_DEPTH), 1)
            .expect("server-valid depth 64 must remain compiler-valid");
    }

    #[test]
    fn projection_expression_rejects_limit_plus_one() {
        let error = validate_expression(&nested_expression(MAX_EXPRESSION_DEPTH + 1), 1)
            .expect_err("depth 65 must fail closed");
        assert_eq!(error.code, "client.manifest.projection_expression_depth");
    }
}
