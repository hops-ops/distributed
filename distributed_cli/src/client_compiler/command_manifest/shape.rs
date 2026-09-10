use std::collections::{BTreeMap, BTreeSet};

use crate::client_compiler::manifest::{
    ManifestCommand, ManifestCommandShape, ManifestConsistencyKind, ManifestModel, ManifestRoot,
    ManifestTypeDef, RootOperation,
};
use crate::client_compiler::ClientCompileError;

use super::support::{graphql_name, invalid};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CommandTypeKind {
    Input,
    Output,
}

pub(super) fn projected_output_typename<'a>(
    command: &ManifestCommand,
    models: &'a BTreeMap<String, ManifestModel>,
) -> Option<&'a str> {
    if let Some(target) = command.extensions.direct_projection.as_ref() {
        if let Some(model) = models.get(&target.model) {
            return Some(model.typename.as_str());
        }
    }
    let consistency = &command.extensions.consistency;
    if consistency.kind != ManifestConsistencyKind::Atomic {
        return None;
    }
    let ManifestCommandShape::Object { definition } = &command.output else {
        return None;
    };
    models
        .values()
        .find(|model| model.typename == definition.name)
        .map(|model| model.typename.as_str())
}

pub(super) fn occupied_surface_types(
    models: &BTreeMap<String, ManifestModel>,
    roots: &BTreeMap<(RootOperation, String), ManifestRoot>,
    scalar_codecs: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    let mut occupied = ["Query", "Mutation", "Subscription", "order_by"]
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    occupied.extend(scalar_codecs.keys().cloned());
    for model in models.values() {
        occupied.insert(model.typename.clone());
        for relationship in &model.relationships {
            occupied.insert(relationship.target_typename.clone());
            occupied.extend(
                relationship
                    .arguments
                    .iter()
                    .map(|argument| argument.type_name.clone()),
            );
            if let Some(aggregate) = &relationship.aggregate {
                occupied.insert(aggregate.semantics.wrapper_typename.clone());
                occupied.insert(aggregate.semantics.fields_typename.clone());
                occupied.extend(
                    aggregate
                        .arguments
                        .iter()
                        .map(|argument| argument.type_name.clone()),
                );
            }
        }
    }
    for root in roots.values() {
        occupied.extend(
            root.arguments
                .iter()
                .map(|argument| argument.type_name.clone()),
        );
        if let Some(aggregate) = &root.aggregate {
            occupied.insert(aggregate.wrapper_typename.clone());
            occupied.insert(aggregate.fields_typename.clone());
        }
    }
    occupied
}

pub(super) fn validate_shape(
    shape: &ManifestCommandShape,
    kind: CommandTypeKind,
    scalar_codecs: &BTreeMap<String, String>,
    label: &str,
    occupied_types: &BTreeSet<String>,
    allowed_occupied_type: Option<&str>,
    definitions: &mut BTreeMap<String, (CommandTypeKind, ManifestTypeDef)>,
) -> Result<(), ClientCompileError> {
    match shape {
        ManifestCommandShape::None => Ok(()),
        ManifestCommandShape::Object { definition } => validate_type_def(
            definition,
            kind,
            scalar_codecs,
            label,
            occupied_types,
            allowed_occupied_type,
            definitions,
        ),
    }
}

fn validate_type_def(
    definition: &ManifestTypeDef,
    kind: CommandTypeKind,
    scalar_codecs: &BTreeMap<String, String>,
    label: &str,
    occupied_types: &BTreeSet<String>,
    allowed_occupied_type: Option<&str>,
    definitions: &mut BTreeMap<String, (CommandTypeKind, ManifestTypeDef)>,
) -> Result<(), ClientCompileError> {
    graphql_name(&definition.name, &format!("{label} type"))?;
    if occupied_types.contains(&definition.name)
        && allowed_occupied_type != Some(definition.name.as_str())
    {
        return Err(invalid(
            "client.manifest.command_type_namespace",
            format!(
                "{label} type `{}` collides with an occupied GraphQL surface type",
                definition.name
            ),
        ));
    }
    if let Some((previous_kind, previous)) = definitions.get(&definition.name) {
        return if *previous_kind == kind && previous == definition {
            Ok(())
        } else {
            Err(invalid(
                "client.manifest.command_type_reference",
                format!(
                    "GraphQL type `{}` has ambiguous input/output or structural definitions",
                    definition.name
                ),
            ))
        };
    }
    definitions.insert(definition.name.clone(), (kind, definition.clone()));
    if definition.fields.is_empty() {
        return Err(invalid(
            "client.manifest.command_type_fields",
            format!("{label} type `{}` must contain a field", definition.name),
        ));
    }
    let mut names = BTreeSet::new();
    let mut previous = None;
    for field in &definition.fields {
        graphql_name(&field.name, &format!("{label} field"))?;
        graphql_name(&field.type_name, &format!("{label} field type"))?;
        if !names.insert(field.name.as_str()) {
            return Err(invalid(
                "client.manifest.command_type_field",
                format!("{label} repeats field `{}`", field.name),
            ));
        }
        if previous.is_some_and(|name| name >= field.name.as_str()) {
            return Err(invalid(
                "client.manifest.command_type_field",
                format!("{label} fields must use canonical name order"),
            ));
        }
        previous = Some(field.name.as_str());
        if field.item_nullable && !field.list {
            return Err(invalid(
                "client.manifest.command_type_nullability",
                format!(
                    "{label} field `{}` marks a non-list item nullable",
                    field.name
                ),
            ));
        }
        match (&field.codec, &field.nested) {
            (Some(codec), None) => {
                validate_codec(&field.type_name, codec, scalar_codecs, label)?;
            }
            (None, Some(nested)) if nested.name == field.type_name => {
                validate_type_def(
                    nested,
                    kind,
                    scalar_codecs,
                    &format!("{label}.{}", field.name),
                    occupied_types,
                    None,
                    definitions,
                )?;
            }
            (None, Some(_)) => {
                return Err(invalid(
                    "client.manifest.command_type_reference",
                    format!(
                        "{label} field `{}` type does not match its nested definition",
                        field.name
                    ),
                ));
            }
            _ => {
                return Err(invalid(
                    "client.manifest.command_type_codec",
                    format!(
                        "{label} field `{}` must declare exactly one scalar codec or nested type",
                        field.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn validate_codec(
    scalar: &str,
    codec: &str,
    scalar_codecs: &BTreeMap<String, String>,
    label: &str,
) -> Result<(), ClientCompileError> {
    match scalar_codecs.get(scalar) {
        Some(expected) if expected == codec => Ok(()),
        Some(_)
            if scalar == "BigInt"
                && matches!(codec, "uint8" | "uint16" | "uint32" | "uint64_safe_integer") =>
        {
            Ok(())
        }
        Some(expected) => Err(invalid(
            "client.manifest.command_type_codec",
            format!("{label} codec `{codec}` does not match `{scalar}` codec `{expected}`"),
        )),
        None => Err(invalid(
            "client.manifest.command_type_scalar",
            format!("{label} references unsupported scalar `{scalar}`"),
        )),
    }
}

pub(super) fn canonical_command_operation(command: &ManifestCommand) -> String {
    let operation_name = format!("Client_{}", command.mutation_field);
    let (variables, arguments) = match &command.input {
        ManifestCommandShape::None => ("($commandId: ID!)".to_string(), "(commandId: $commandId)"),
        ManifestCommandShape::Object { definition } => (
            format!("($commandId: ID!, $input: {}!)", definition.name),
            "(commandId: $commandId, input: $input)",
        ),
    };
    let selection = match &command.output {
        ManifestCommandShape::Object { definition } => {
            format!(" {{ {} }}", command_selection(definition))
        }
        ManifestCommandShape::None => String::new(),
    };
    format!(
        "mutation {operation_name}{variables} {{ {}{arguments}{selection} }}",
        command.mutation_field
    )
}

fn command_selection(definition: &ManifestTypeDef) -> String {
    definition
        .fields
        .iter()
        .map(|field| match &field.nested {
            Some(nested) => format!("{} {{ {} }}", field.name, command_selection(nested)),
            None => field.name.clone(),
        })
        .collect::<Vec<_>>()
        .join(" ")
}
