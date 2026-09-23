//! Pure, framework-neutral compiler for Distributed client artifacts.
//!
//! This module deliberately owns no filesystem or glob behavior. Callers load
//! one role/application-selected manifest and the colocated GraphQL documents,
//! then decide how to write or check the returned files.

mod command_manifest;
mod graphql;
mod islands;
mod manifest;
mod projection_delta;
mod render;

#[cfg(test)]
mod command_manifest_tests;

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use graphql::compile_document;
use manifest::ClientManifest;
use render::render_project;

/// Complete input to one deterministic client compilation.
#[derive(Clone, Debug)]
pub struct ClientCompileInput {
    /// The role/application-selected Distributed client manifest.
    pub manifest: JsonValue,
    /// Exact authorization surface expected by the caller.
    pub selector: ClientSurfaceSelector,
    /// GraphQL sources. Filesystem traversal and glob expansion stay in the CLI.
    pub documents: Vec<ClientDocument>,
}

impl ClientCompileInput {
    pub fn new(
        manifest: JsonValue,
        selector: ClientSurfaceSelector,
        documents: Vec<ClientDocument>,
    ) -> Self {
        Self {
            manifest,
            selector,
            documents,
        }
    }
}

/// Explicit client authorization surface. This value only verifies manifest
/// provenance; it can never relabel a broader manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientSurfaceSelector {
    Role {
        name: String,
    },
    Application {
        name: String,
        eligible_roles: Vec<String>,
        schema_roles: Vec<String>,
    },
}

impl ClientSurfaceSelector {
    pub fn role(name: impl Into<String>) -> Self {
        Self::Role { name: name.into() }
    }

    pub fn application(
        name: impl Into<String>,
        eligible_roles: impl IntoIterator<Item = impl Into<String>>,
        schema_roles: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let mut eligible_roles = eligible_roles
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        let mut schema_roles = schema_roles.into_iter().map(Into::into).collect::<Vec<_>>();
        eligible_roles.sort();
        eligible_roles.dedup();
        schema_roles.sort();
        schema_roles.dedup();
        Self::Application {
            name: name.into(),
            eligible_roles,
            schema_roles,
        }
    }
}

/// One already-loaded GraphQL source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientDocument {
    pub path: String,
    pub source: String,
}

impl ClientDocument {
    pub fn new(path: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            source: source.into(),
        }
    }
}

/// Pure compiler output. `files` is sorted by portable relative path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedClientProject {
    pub files: Vec<GeneratedClientFile>,
    pub operations: Vec<GeneratedOperationSummary>,
    pub islands: Vec<GeneratedIslandPlan>,
    pub schema_fingerprint: String,
    pub protocol_fingerprint: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedClientFile {
    pub path: String,
    pub contents: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct GeneratedOperationSummary {
    pub name: String,
    pub source_path: String,
    pub module_path: String,
    pub export_name: String,
    pub operation_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_operation_hash: Option<String>,
}

/// Versioned, framework-neutral placement input for one application operation.
///
/// Framework adapters add component reachability and boundary ownership around
/// this immutable compiler contract. Svelte, Vite, and router concepts must
/// never enter this type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GeneratedIslandPlan {
    pub version: u32,
    pub id: String,
    pub operation: String,
    pub operation_hash: String,
    pub module_path: String,
    pub export_name: String,
    pub source: GeneratedIslandSource,
    pub directives: GeneratedIslandDirectives,
    pub variable_schema: GeneratedIslandVariableSchema,
    pub live_coverage: GeneratedIslandLiveCoverage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct GeneratedIslandSource {
    pub path: String,
    pub line: usize,
    pub column: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct GeneratedIslandDirectives {
    pub load: bool,
    pub live: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GeneratedIslandVariableSchema {
    pub reference: String,
    pub codec_version: u32,
    pub variables: Vec<GeneratedIslandVariable>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GeneratedIslandVariable {
    pub name: String,
    pub graphql_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GeneratedIslandLiveCoverage {
    pub requested: bool,
    pub finite: bool,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_items: Option<u64>,
}

/// Stable, source-located error returned for every unsupported or invalid
/// construct. The first compiler slice never silently weakens an operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientCompileError {
    pub code: &'static str,
    pub message: String,
    pub source: Option<ClientSourceLocation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientSourceLocation {
    pub path: String,
    pub line: usize,
    pub column: usize,
}

impl ClientCompileError {
    pub(crate) fn manifest(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            source: None,
        }
    }

    pub(crate) fn source(
        code: &'static str,
        message: impl Into<String>,
        path: &str,
        line: usize,
        column: usize,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            source: Some(ClientSourceLocation {
                path: path.to_string(),
                line,
                column,
            }),
        }
    }
}

impl fmt::Display for ClientCompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(source) = &self.source {
            write!(
                formatter,
                "{}:{}:{}: {} [{}]",
                source.path, source.line, source.column, self.message, self.code
            )
        } else {
            write!(formatter, "{} [{}]", self.message, self.code)
        }
    }
}

impl std::error::Error for ClientCompileError {}

/// Compile one selected manifest and its colocated GraphQL sources.
pub fn compile_client(
    mut input: ClientCompileInput,
) -> Result<GeneratedClientProject, ClientCompileError> {
    let manifest = ClientManifest::parse(input.manifest, &input.selector)?;

    if input.documents.is_empty() {
        return Err(ClientCompileError::manifest(
            "client.documents.empty",
            "client compilation requires at least one GraphQL document",
        ));
    }

    for document in &mut input.documents {
        document.path = normalize_source_path(&document.path)?;
    }
    input.documents.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.source.cmp(&right.source))
    });
    for pair in input.documents.windows(2) {
        if pair[0].path == pair[1].path {
            return Err(ClientCompileError::manifest(
                "client.documents.duplicate_path",
                format!("duplicate GraphQL document path `{}`", pair[0].path),
            ));
        }
    }

    let mut operations = Vec::with_capacity(input.documents.len());
    for document in &input.documents {
        operations.push(compile_document(document, &manifest)?);
    }

    operations.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.source_path.cmp(&right.source_path))
    });
    let mut names = BTreeMap::<&str, &str>::new();
    let mut modules = BTreeMap::<&str, &str>::new();
    for operation in &operations {
        if let Some(previous_path) =
            names.insert(operation.name.as_str(), operation.source_path.as_str())
        {
            return Err(ClientCompileError::manifest(
                "client.operation.duplicate_name",
                format!(
                    "duplicate GraphQL operation `{}` in `{}` and `{}`",
                    operation.name, previous_path, operation.source_path
                ),
            ));
        }
        if let Some(previous_name) =
            modules.insert(operation.module_path.as_str(), operation.name.as_str())
        {
            return Err(ClientCompileError::manifest(
                "client.operation.module_collision",
                format!(
                    "operations `{}` and `{}` collide at generated module `{}`",
                    previous_name, operation.name, operation.module_path
                ),
            ));
        }
    }

    render_project(&manifest, operations)
}

fn normalize_source_path(path: &str) -> Result<String, ClientCompileError> {
    let normalized = path.trim().replace('\\', "/");
    if normalized.is_empty() {
        return Err(ClientCompileError::manifest(
            "client.documents.empty_path",
            "GraphQL document path must not be empty",
        ));
    }
    if normalized.split('/').any(|segment| segment == "..") {
        return Err(ClientCompileError::manifest(
            "client.documents.parent_path",
            "GraphQL document path must not contain parent segments",
        ));
    }
    let drive_absolute = normalized.as_bytes().get(1) == Some(&b':');
    if normalized.len() > 4_096
        || normalized.starts_with('/')
        || drive_absolute
        || normalized.chars().any(char::is_control)
        || normalized
            .split('/')
            .any(|segment| segment.is_empty() || segment == ".")
        || !(normalized.ends_with(".graphql") || normalized.ends_with(".gql"))
    {
        return Err(ClientCompileError::manifest(
            "client.documents.invalid_path",
            "GraphQL document path must be a safe project-relative .graphql or .gql path",
        ));
    }
    Ok(normalized)
}
pub(crate) fn is_graphql_name(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some('_' | 'A'..='Z' | 'a'..='z'))
        && chars.all(|character| matches!(character, '_' | '0'..='9' | 'A'..='Z' | 'a'..='z'))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod runtime_bridge_tests;
