use super::*;

pub(super) fn row_policy_manifest(policy: &SurfaceRowPolicy) -> ClientRowPolicy {
    match policy {
        SurfaceRowPolicy::Unrestricted => ClientRowPolicy::Unrestricted,
        SurfaceRowPolicy::Predicate(predicate) => ClientRowPolicy::Predicate {
            expression: predicate.clone(),
        },
        SurfaceRowPolicy::ServerOnly => ClientRowPolicy::ServerOnly,
    }
}

pub(super) fn filter_semantics(
    surface: &Surface,
    model: &crate::graphql::surface::SurfaceModel,
) -> ClientFilterSemantics {
    ClientFilterSemantics {
        fields: filter_fields(surface, model),
        relationships: filter_relationship_names(model),
        row_policy: row_policy_manifest(&model.row_policy),
    }
}

pub(super) fn filter_input(
    surface: &Surface,
    model: &crate::graphql::surface::SurfaceModel,
) -> Result<ClientFilterInput, ClientManifestError> {
    let mut relationships = model
        .relationships
        .iter()
        .map(|relationship| {
            let target = surface
                .models
                .get(&relationship.target_model)
                .ok_or_else(|| {
                    ClientManifestError(format!(
                        "model `{}` filter relationship `{}` targets absent model `{}`",
                        model.model_name, relationship.name, relationship.target_model
                    ))
                })?;
            Ok(ClientFilterInputRelationship {
                field: relationship.name.clone(),
                target_type: format!("{}_bool_exp", target.table_name),
            })
        })
        .collect::<Result<Vec<_>, ClientManifestError>>()?;
    relationships.sort_by(|left, right| left.field.cmp(&right.field));
    Ok(ClientFilterInput {
        type_name: format!("{}_bool_exp", model.table_name),
        fields: filter_fields(surface, model),
        relationships,
    })
}

fn filter_fields(
    surface: &Surface,
    model: &crate::graphql::surface::SurfaceModel,
) -> Vec<ClientFilterField> {
    let mut fields: Vec<ClientFilterField> = model
        .columns
        .iter()
        .map(|field| ClientFilterField {
            name: field.name.clone(),
            operators: surface
                .comparison_ops_for_scalar(&field.scalar)
                .into_iter()
                .map(str::to_string)
                .collect(),
        })
        .collect();
    fields.sort_by(|a, b| a.name.cmp(&b.name));
    fields
}

fn filter_relationship_names(model: &crate::graphql::surface::SurfaceModel) -> Vec<String> {
    let mut relationships: Vec<String> = model
        .relationships
        .iter()
        .map(|relationship| relationship.name.clone())
        .collect();
    relationships.sort();
    relationships
}

pub(super) fn order_semantics(
    model: &crate::graphql::surface::SurfaceModel,
) -> ClientOrderSemantics {
    let mut fields: Vec<String> = model
        .columns
        .iter()
        .map(|field| field.name.clone())
        .collect();
    fields.sort();
    ClientOrderSemantics {
        fields,
        values: [
            "asc",
            "asc_nulls_first",
            "asc_nulls_last",
            "desc",
            "desc_nulls_first",
            "desc_nulls_last",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
    }
}

pub(super) fn pagination_semantics(
    surface: &Surface,
    model: &crate::graphql::surface::SurfaceModel,
) -> ClientPaginationSemantics {
    let max_limit = model
        .role_limit
        .unwrap_or(surface.max_limit)
        .min(surface.max_limit);
    ClientPaginationSemantics {
        kind: "offset".into(),
        default_limit: surface.default_limit.min(max_limit),
        max_limit,
        coverage: "window".into(),
    }
}

pub(super) fn aggregate_semantics(
    model: &crate::graphql::surface::SurfaceModel,
    wrapper_typename: String,
    nodes_pagination: ClientPaginationSemantics,
) -> ClientAggregateSemantics {
    ClientAggregateSemantics {
        wrapper_typename,
        fields_typename: aggregate_fields_type_name(&model.schema),
        nodes_pagination,
        count: true,
        nodes: true,
        sum: Vec::new(),
        avg: Vec::new(),
        min: Vec::new(),
        max: Vec::new(),
    }
}

pub(super) fn argument_manifest(argument: &SurfaceArgument) -> ClientArgument {
    ClientArgument {
        name: argument.name.clone(),
        kind: match argument.kind {
            SurfaceArgumentKind::Filter => ClientArgumentKind::Filter,
            SurfaceArgumentKind::Order => ClientArgumentKind::Order,
            SurfaceArgumentKind::Limit => ClientArgumentKind::Limit,
            SurfaceArgumentKind::Offset => ClientArgumentKind::Offset,
            SurfaceArgumentKind::PrimaryKey => ClientArgumentKind::PrimaryKey,
        },
        type_name: argument.type_name.clone(),
        nullable: argument.nullable,
        list: argument.list,
        codec: scalar_codec(&argument.type_name).map(str::to_string),
    }
}

pub(super) fn command_shape(
    shape: &SurfaceCommandShape,
) -> Result<ClientCommandShape, ClientManifestError> {
    match shape {
        SurfaceCommandShape::None => Ok(ClientCommandShape::None),
        SurfaceCommandShape::Typed(definition) => Ok(ClientCommandShape::Object {
            definition: client_type(definition)?,
        }),
    }
}

fn client_type(definition: &SurfaceTypeDef) -> Result<ClientTypeDef, ClientManifestError> {
    let mut fields = Vec::new();
    for field in &definition.fields {
        if !field.list && field.item_nullable {
            return Err(ClientManifestError(format!(
                "command type `{}` field `{}` marks non-list items nullable",
                definition.name, field.name
            )));
        }
        let nested = field
            .nested
            .as_deref()
            .map(client_type)
            .transpose()?
            .map(Box::new);
        if field.unsigned_integer.is_some() && (field.type_name != "BigInt" || nested.is_some()) {
            return Err(ClientManifestError(format!(
                "command type `{}` field `{}` has an invalid unsigned refinement",
                definition.name, field.name
            )));
        }
        let codec = field
            .unsigned_integer
            .map(|unsigned| unsigned.client_codec())
            .or_else(|| scalar_codec(&field.type_name))
            .map(str::to_string);
        if codec.is_none() && nested.is_none() {
            return Err(ClientManifestError(format!(
                "command type `{}` field `{}` uses unknown scalar/object `{}`",
                definition.name, field.name, field.type_name
            )));
        }
        fields.push(ClientTypeField {
            name: field.name.clone(),
            type_name: field.type_name.clone(),
            nullable: field.nullable,
            list: field.list,
            item_nullable: field.item_nullable,
            codec,
            nested,
        });
    }
    fields.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(ClientTypeDef {
        name: definition.name.clone(),
        fields,
    })
}
pub(super) fn supported_scalar_codecs() -> Vec<ScalarCodec> {
    [
        // SQL JSON projection currently emits integer JSON numbers. Browsers
        // must treat values outside the JS safe-integer range as lossy.
        ("BigInt", "json_number_precision_limited"),
        ("Boolean", "boolean"),
        ("Bytea", "base64"),
        ("Float", "float64"),
        ("ID", "string"),
        ("Int", "int32"),
        ("JSON", "json"),
        ("String", "string"),
        // Both dialects emit a string, but the framework does not normalize or
        // validate RFC3339 at this layer yet.
        ("Timestamptz", "string_unvalidated_timestamp"),
    ]
    .into_iter()
    .map(|(scalar, codec)| ScalarCodec {
        scalar: scalar.into(),
        codec: codec.into(),
    })
    .collect()
}

pub(super) fn scalar_codec(scalar: &str) -> Option<&'static str> {
    match scalar {
        "BigInt" => Some("json_number_precision_limited"),
        "Boolean" => Some("boolean"),
        "Bytea" => Some("base64"),
        "Float" => Some("float64"),
        "ID" | "String" => Some("string"),
        "Int" => Some("int32"),
        "JSON" => Some("json"),
        "Timestamptz" => Some("string_unvalidated_timestamp"),
        _ => None,
    }
}

pub(super) fn protocol_fingerprint() -> Result<String, ClientManifestError> {
    #[derive(Serialize)]
    struct ProtocolMaterial {
        manifest_version: u32,
        protocol_version: u32,
        key_encoding: &'static str,
        command_extension_slots_version: u32,
        projector_entry_version: u32,
        protocol_operations_version: u32,
        query_capabilities_version: u32,
        projection_program_version: u32,
        projection_binding_version: u32,
        projection_operation_semantics_version: u32,
        command_projection_extension_version: u32,
        unsigned_command_codecs_version: u32,
        scalar_codecs: Vec<ScalarCodec>,
    }
    hash_json(&ProtocolMaterial {
        manifest_version: DISTRIBUTED_CLIENT_PROTOCOL_MANIFEST_EPOCH,
        protocol_version: DISTRIBUTED_CLIENT_PROTOCOL_VERSION,
        key_encoding: KEY_ENCODING,
        command_extension_slots_version: COMMAND_EXTENSION_SLOTS_VERSION,
        projector_entry_version: PROJECTOR_ENTRY_VERSION,
        protocol_operations_version: PROTOCOL_OPERATIONS_VERSION,
        query_capabilities_version: QUERY_CAPABILITIES_VERSION,
        projection_program_version: super::projections::CLIENT_PROJECTION_PROGRAM_VERSION,
        projection_binding_version: super::projections::CLIENT_PROJECTION_BINDING_VERSION,
        projection_operation_semantics_version:
            super::projections::CLIENT_PROJECTION_OPERATION_SEMANTICS_VERSION,
        command_projection_extension_version:
            super::projections::COMMAND_PROJECTION_EXTENSION_VERSION,
        unsigned_command_codecs_version: 1,
        scalar_codecs: supported_scalar_codecs(),
    })
}

pub(super) fn hash_json(value: &impl Serialize) -> Result<String, ClientManifestError> {
    let bytes = serde_json::to_vec(value)?;
    Ok(hash_bytes(&bytes))
}

/// Canonicalize opaque manifest JSON before it enters a fingerprinted slot.
///
/// `serde_json::Value` object order otherwise depends on whether another crate
/// enabled serde_json's `preserve_order` feature in the final Cargo graph.
/// Client-manifest bytes must be identical in the server harness and the
/// distributed CLI.
pub(super) fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => {
            let sorted = values
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect::<BTreeMap<_, _>>();
            serde_json::Value::Object(sorted.into_iter().collect())
        }
        scalar => scalar,
    }
}

pub(super) fn hash_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

pub(super) fn sorted_unique(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values.dedup();
    values
}
