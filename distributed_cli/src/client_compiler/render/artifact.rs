use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value as JsonValue;

use super::super::graphql::{
    Cardinality, CompiledArgument, CompiledBranch, CompiledBranchSemantic, CompiledFilterField,
    CompiledFilterPlan, CompiledMember, CompiledObject, CompiledOperation, CompiledOrderField,
    CompiledOrderPlan, CompiledPaginationPlan, CompiledRelationshipPlan, CompiledStorage,
    CompiledVariableCodec,
};
use super::super::manifest::{
    ClientManifest, ManifestRelationshipKeyMapping, ManifestRelationshipKind,
    ManifestRelationshipMaintenance, ManifestRowPolicy, ManifestTrustedPresetDescriptor,
};
use super::super::ClientCompileError;

#[derive(Serialize)]
struct Artifact<'a> {
    id: &'a str,
    document: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<ArtifactSource<'a>>,
    #[serde(rename = "variableCodec")]
    variable_codec: &'a CompiledVariableCodec,
    roots: Vec<ArtifactRoot<'a>>,
    protocol: ArtifactProtocol<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    live: Option<ArtifactLive<'a>>,
}

#[derive(Serialize)]
struct ArtifactSource<'a> {
    path: &'a str,
    line: usize,
    column: usize,
}

#[derive(Serialize)]
struct ArtifactProtocol<'a> {
    version: u32,
    #[serde(rename = "schemaHash")]
    schema_hash: &'a str,
    #[serde(rename = "protocolHash")]
    protocol_hash: &'a str,
    surface: &'a super::super::manifest::ManifestSurface,
    operation: &'a str,
    #[serde(rename = "trustedPresets")]
    trusted_presets: &'a [ManifestTrustedPresetDescriptor],
}

#[derive(Serialize)]
struct ArtifactLive<'a> {
    id: &'a str,
    document: &'a str,
}

#[derive(Serialize)]
struct ArtifactRoot<'a> {
    #[serde(rename = "responseKey")]
    response_key: &'a str,
    field: &'a str,
    cardinality: &'static str,
    nullable: bool,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    arguments: BTreeMap<&'a str, ArtifactArgument<'a>>,
    dependencies: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    coverage: Option<ArtifactCoverage<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<ArtifactFilter<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    order: Option<ArtifactOrder<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pagination: Option<ArtifactPagination<'a>>,
    selection: ArtifactSelection<'a>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ArtifactArgument<'a> {
    Literal {
        value: &'a JsonValue,
    },
    Variable {
        name: &'a str,
    },
    List {
        items: Vec<ArtifactArgument<'a>>,
    },
    Object {
        fields: BTreeMap<&'a str, ArtifactArgument<'a>>,
    },
}

#[derive(Serialize)]
struct ArtifactCoverage<'a> {
    kind: &'a str,
    #[serde(rename = "offsetArgument", skip_serializing_if = "Option::is_none")]
    offset_argument: Option<&'a str>,
    #[serde(rename = "limitArgument", skip_serializing_if = "Option::is_none")]
    limit_argument: Option<&'a str>,
    #[serde(rename = "defaultLimit", skip_serializing_if = "Option::is_none")]
    default_limit: Option<u64>,
    #[serde(rename = "maxLimit", skip_serializing_if = "Option::is_none")]
    max_limit: Option<u64>,
}

#[derive(Serialize)]
struct ArtifactFilter<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    input: Option<ArtifactArgument<'a>>,
    fields: Vec<ArtifactFilterField<'a>>,
    relationships: Vec<ArtifactRelationship<'a>>,
    #[serde(rename = "rowPolicy")]
    row_policy: &'a ManifestRowPolicy,
}

#[derive(Serialize)]
struct ArtifactFilterField<'a> {
    field: &'a str,
    scalar: &'a str,
    codec: &'a str,
    nullable: bool,
    operators: &'a [String],
}

#[derive(Serialize)]
struct ArtifactRelationship<'a> {
    field: &'a str,
    #[serde(rename = "targetModel")]
    target_model: &'a str,
    kind: ManifestRelationshipKind,
    #[serde(rename = "keyMapping")]
    key_mapping: ArtifactRelationshipKeyMapping<'a>,
    maintenance: ManifestRelationshipMaintenance,
    dependencies: &'a [String],
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ArtifactRelationshipKeyMapping<'a> {
    Direct {
        local: &'a [String],
        remote: &'a [String],
    },
    Through {
        local: &'a [String],
        remote: &'a [String],
        table: &'a str,
        #[serde(rename = "sourceForeignKey")]
        source_foreign_key: &'a [String],
        #[serde(rename = "targetForeignKey")]
        target_foreign_key: &'a [String],
    },
    ThroughOpaque {
        local: &'a [String],
        remote: &'a [String],
        dependency: &'a str,
    },
    Embedded,
}

#[derive(Serialize)]
struct ArtifactOrder<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    input: Option<ArtifactArgument<'a>>,
    fields: Vec<ArtifactOrderField<'a>>,
    #[serde(rename = "tieBreakers")]
    tie_breakers: Vec<ArtifactOrderField<'a>>,
}

#[derive(Serialize)]
struct ArtifactOrderField<'a> {
    field: &'a str,
    scalar: &'a str,
    codec: &'a str,
    nullable: bool,
}

#[derive(Serialize)]
struct ArtifactPagination<'a> {
    kind: &'a str,
    insert: &'a str,
    delete: &'a str,
    reorder: &'a str,
    #[serde(rename = "stableUpdate")]
    stable_update: &'a str,
}

#[derive(Serialize)]
struct ArtifactSelection<'a> {
    typename: &'a str,
    storage: ArtifactStorage<'a>,
    members: Vec<ArtifactMember<'a>>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ArtifactStorage<'a> {
    Normalized {
        model: &'a str,
        #[serde(rename = "identityFields")]
        identity_fields: &'a [String],
    },
    Embedded,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ArtifactMember<'a> {
    Scalar {
        #[serde(rename = "responseKey")]
        response_key: &'a str,
        field: &'a str,
        codec: &'a str,
        nullable: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        expose: Option<bool>,
    },
    Branch {
        semantic: CompiledBranchSemantic,
        #[serde(rename = "responseKey")]
        response_key: &'a str,
        field: &'a str,
        cardinality: &'static str,
        nullable: bool,
        #[serde(skip_serializing_if = "BTreeMap::is_empty")]
        arguments: BTreeMap<&'a str, ArtifactArgument<'a>>,
        dependencies: &'a [String],
        #[serde(skip_serializing_if = "Option::is_none")]
        coverage: Option<ArtifactCoverage<'a>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        filter: Option<Box<ArtifactFilter<'a>>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        order: Option<Box<ArtifactOrder<'a>>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pagination: Option<Box<ArtifactPagination<'a>>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        relationship: Option<Box<ArtifactRelationship<'a>>>,
        selection: Box<ArtifactSelection<'a>>,
    },
}
/// Serialize the exact artifact embedded in the generated TypeScript module.
///
/// Keeping this boundary machine-readable lets cross-runtime contract tests
/// consume Rust output directly instead of recovering JSON from TypeScript.
pub(super) fn render_operation_artifact_json(
    operation: &CompiledOperation,
    manifest: &ClientManifest,
) -> Result<String, ClientCompileError> {
    let artifact = Artifact {
        id: &operation.query_hash,
        document: &operation.query_document,
        source: artifact_source(operation),
        variable_codec: &operation.variable_codec,
        roots: vec![artifact_root(operation)],
        protocol: ArtifactProtocol {
            version: 1,
            schema_hash: &manifest.schema_fingerprint,
            protocol_hash: &manifest.protocol_fingerprint,
            surface: &manifest.surface,
            operation: &operation.query_hash,
            trusted_presets: &manifest.trusted_presets,
        },
        live: operation.live.as_ref().map(|live| ArtifactLive {
            id: &live.hash,
            document: &live.document,
        }),
    };
    let artifact_json = serde_json::to_string_pretty(&artifact).map_err(|error| {
        ClientCompileError::manifest(
            "client.render.operation",
            format!("failed to render operation `{}`: {error}", operation.name),
        )
    })?;
    Ok(artifact_json)
}

fn artifact_source(operation: &CompiledOperation) -> Option<ArtifactSource<'_>> {
    let path = operation.source_path.as_str();
    let drive_absolute = path.as_bytes().get(1) == Some(&b':');
    if path.len() > 4_096
        || path.chars().any(char::is_control)
        || path.starts_with('/')
        || drive_absolute
        || path.split('/').any(|segment| segment == "..")
        || !(path.ends_with(".graphql") || path.ends_with(".gql"))
    {
        return None;
    }
    Some(ArtifactSource {
        path,
        line: operation.source_line,
        column: operation.source_column,
    })
}

fn artifact_root(operation: &CompiledOperation) -> ArtifactRoot<'_> {
    let root = &operation.root;
    ArtifactRoot {
        response_key: &root.response_key,
        field: &root.field,
        cardinality: match root.cardinality {
            Cardinality::One => "one",
            Cardinality::Many => "many",
        },
        nullable: root.nullable,
        arguments: root
            .arguments
            .iter()
            .map(|(name, argument)| (name.as_str(), artifact_argument(argument)))
            .collect(),
        dependencies: &root.dependencies,
        coverage: root.coverage.as_ref().map(|coverage| ArtifactCoverage {
            kind: &coverage.kind,
            offset_argument: coverage.offset_argument.as_deref(),
            limit_argument: coverage.limit_argument.as_deref(),
            default_limit: coverage.default_limit,
            max_limit: coverage.max_limit,
        }),
        filter: root.filter.as_ref().map(artifact_filter),
        order: root.order.as_ref().map(artifact_order),
        pagination: root.pagination.as_ref().map(artifact_pagination),
        selection: artifact_selection(&root.selection),
    }
}

fn artifact_argument(argument: &CompiledArgument) -> ArtifactArgument<'_> {
    match argument {
        CompiledArgument::Literal { value, .. } => ArtifactArgument::Literal { value },
        CompiledArgument::Variable(name) => ArtifactArgument::Variable { name },
        CompiledArgument::List(items) => ArtifactArgument::List {
            items: items.iter().map(artifact_argument).collect(),
        },
        CompiledArgument::Object(fields) => ArtifactArgument::Object {
            fields: fields
                .iter()
                .map(|(name, value)| (name.as_str(), artifact_argument(value)))
                .collect(),
        },
    }
}

fn artifact_filter(plan: &CompiledFilterPlan) -> ArtifactFilter<'_> {
    ArtifactFilter {
        input: plan.input.as_ref().map(artifact_argument),
        fields: plan.fields.iter().map(artifact_filter_field).collect(),
        relationships: plan
            .relationships
            .iter()
            .map(artifact_relationship)
            .collect(),
        row_policy: &plan.row_policy,
    }
}

fn artifact_filter_field(field: &CompiledFilterField) -> ArtifactFilterField<'_> {
    ArtifactFilterField {
        field: &field.name,
        scalar: &field.scalar,
        codec: &field.codec,
        nullable: field.nullable,
        operators: &field.operators,
    }
}

fn artifact_relationship(relationship: &CompiledRelationshipPlan) -> ArtifactRelationship<'_> {
    ArtifactRelationship {
        field: &relationship.field,
        target_model: &relationship.target_model,
        kind: relationship.kind,
        key_mapping: match &relationship.key_mapping {
            ManifestRelationshipKeyMapping::Direct { local, remote } => {
                ArtifactRelationshipKeyMapping::Direct { local, remote }
            }
            ManifestRelationshipKeyMapping::Through {
                local,
                remote,
                table,
                source_foreign_key,
                target_foreign_key,
            } => ArtifactRelationshipKeyMapping::Through {
                local,
                remote,
                table,
                source_foreign_key,
                target_foreign_key,
            },
            ManifestRelationshipKeyMapping::ThroughOpaque {
                local,
                remote,
                dependency,
            } => ArtifactRelationshipKeyMapping::ThroughOpaque {
                local,
                remote,
                dependency,
            },
            ManifestRelationshipKeyMapping::Embedded => ArtifactRelationshipKeyMapping::Embedded,
        },
        maintenance: relationship.maintenance,
        dependencies: &relationship.dependencies,
    }
}

fn artifact_order(plan: &CompiledOrderPlan) -> ArtifactOrder<'_> {
    ArtifactOrder {
        input: plan.input.as_ref().map(artifact_argument),
        fields: plan.fields.iter().map(artifact_order_field).collect(),
        tie_breakers: plan.identity.iter().map(artifact_order_field).collect(),
    }
}

fn artifact_order_field(field: &CompiledOrderField) -> ArtifactOrderField<'_> {
    ArtifactOrderField {
        field: &field.name,
        scalar: &field.scalar,
        codec: &field.codec,
        nullable: field.nullable,
    }
}

fn artifact_pagination(plan: &CompiledPaginationPlan) -> ArtifactPagination<'_> {
    ArtifactPagination {
        kind: &plan.kind,
        insert: &plan.insert,
        delete: &plan.delete,
        reorder: &plan.reorder,
        stable_update: &plan.stable_update,
    }
}

fn artifact_selection(selection: &CompiledObject) -> ArtifactSelection<'_> {
    ArtifactSelection {
        typename: &selection.typename,
        storage: match &selection.storage {
            CompiledStorage::Normalized {
                model_id,
                identity_fields,
            } => ArtifactStorage::Normalized {
                model: model_id,
                identity_fields,
            },
            CompiledStorage::Embedded => ArtifactStorage::Embedded,
        },
        members: selection
            .members
            .iter()
            .map(|member| match member {
                CompiledMember::Scalar(scalar) => ArtifactMember::Scalar {
                    response_key: &scalar.response_key,
                    field: &scalar.field,
                    codec: &scalar.codec,
                    nullable: scalar.nullable,
                    expose: (!scalar.expose).then_some(false),
                },
                CompiledMember::Branch(branch) => artifact_branch(branch),
            })
            .collect(),
    }
}

fn artifact_branch(branch: &CompiledBranch) -> ArtifactMember<'_> {
    ArtifactMember::Branch {
        semantic: branch.semantic,
        response_key: &branch.response_key,
        field: &branch.field,
        cardinality: match branch.cardinality {
            Cardinality::One => "one",
            Cardinality::Many => "many",
        },
        nullable: branch.nullable,
        arguments: branch
            .arguments
            .iter()
            .map(|(name, argument)| (name.as_str(), artifact_argument(argument)))
            .collect(),
        dependencies: &branch.dependencies,
        coverage: branch.coverage.as_ref().map(|coverage| ArtifactCoverage {
            kind: &coverage.kind,
            offset_argument: coverage.offset_argument.as_deref(),
            limit_argument: coverage.limit_argument.as_deref(),
            default_limit: coverage.default_limit,
            max_limit: coverage.max_limit,
        }),
        filter: branch
            .filter
            .as_ref()
            .map(|plan| Box::new(artifact_filter(plan))),
        order: branch
            .order
            .as_ref()
            .map(|plan| Box::new(artifact_order(plan))),
        pagination: branch
            .pagination
            .as_ref()
            .map(|plan| Box::new(artifact_pagination(plan))),
        relationship: branch
            .relationship
            .as_ref()
            .map(|relationship| Box::new(artifact_relationship(relationship))),
        selection: Box::new(artifact_selection(&branch.selection)),
    }
}
