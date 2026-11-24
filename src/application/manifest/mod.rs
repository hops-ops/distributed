use std::collections::{BTreeSet, HashSet};

use serde::{Deserialize, Serialize};

use super::error::{ApplicationError, ApplicationResult};
use super::identity::{canonical_json, sha256_fingerprint, LogicalId};
use super::module::{ModelSpec, Module, ModuleManifest, ProjectionSpec};
use super::surface::{SurfaceCommandSpec, SurfaceSelectionSpec, SurfaceSpec};
use crate::application::command::CommandSpec;

mod canonical;
mod compose;
mod ownership;
mod validate;

use canonical::*;
use compose::*;
use ownership::*;
use validate::*;

/// Wire/schema version for the complete logical application manifest.
pub const APPLICATION_MANIFEST_SCHEMA_VERSION: u32 = 3;

/// Bounds applied before a portable application artifact is accepted.
///
/// A complete manifest carries authoritative module declarations and selected
/// Surface contracts. Flattened inventories are reconstructed, not serialized.
/// Match the CLI manifest ingress budget so complete retained-selector catalogs
/// can compose many independently bounded contracts without dropping history.
pub const MAX_APPLICATION_MANIFEST_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_MANIFEST_COLLECTION_ITEMS: usize = 4096;
pub const MAX_MANIFEST_STRING_BYTES: usize = 4096;
/// Per-value JSON bound. Several independently bounded contracts may comprise
/// one application manifest, but no opaque value receives the whole budget.
pub const MAX_MANIFEST_JSON_BYTES: usize = 1024 * 1024;
pub const MAX_MANIFEST_JSON_DEPTH: usize = 32;
/// Reserved manifest extension carrying the framework release that compiled it.
pub const FRAMEWORK_COMPATIBILITY_EXTENSION_ID: &str = "distributed.framework";
/// Schema version for the framework compatibility extension value.
pub const FRAMEWORK_COMPATIBILITY_EXTENSION_VERSION: u32 = 1;

/// Framework-owned release identity emitted by typed application introspection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FrameworkCompatibility {
    pub release: String,
}

impl FrameworkCompatibility {
    pub fn current() -> Self {
        Self {
            release: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestFingerprint {
    pub logical: String,
    pub canonical: String,
}

/// Portable provenance carried by the artifact envelope. Provenance is part
/// of canonical artifact bytes, but volatile source metadata is deliberately
/// excluded from the logical fingerprint (see [`ApplicationManifest::logical_fingerprint`]).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestProvenance {
    pub generator: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    #[serde(default)]
    pub sources: Vec<String>,
}

impl Default for ManifestProvenance {
    fn default() -> Self {
        Self {
            generator: "distributed.application.compiler.v1".into(),
            source_revision: None,
            sources: Vec::new(),
        }
    }
}

/// A named, versioned extension value in the explicit application
/// declaration. Extensions are data-only and are included in application
/// identity; executable/runtime configuration must use a deployment layer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationExtension {
    pub id: String,
    pub version: u32,
    pub value: serde_json::Value,
}

impl ApplicationExtension {
    pub fn try_new(
        id: impl Into<String>,
        version: u32,
        value: serde_json::Value,
    ) -> ApplicationResult<Self> {
        let id = LogicalId::try_new("application extension", id)?.into_string();
        if version == 0 {
            return Err(ApplicationError::InvalidSpec(
                "application extension version must be non-zero".into(),
            ));
        }
        let extension = Self { id, version, value };
        validate_json_contract("application extension", &extension.value)?;
        Ok(extension)
    }
}

/// The sole complete logical application manifest owner.
///
/// Physical tables, service endpoints, transports, observability, and
/// executable handlers are intentionally absent. Those belong to named
/// schema/deployment/runtime layers and cannot become portable application
/// identity by accident.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ApplicationManifest {
    /// This field is required during decoding: canonical input may not omit
    /// the explicit schema version and receive a legacy default.
    pub schema_version: u32,
    pub name: String,
    #[serde(default)]
    pub modules: Vec<ModuleManifest>,
    #[serde(skip_serializing)]
    pub commands: Vec<CommandSpec>,
    #[serde(skip_serializing)]
    pub events: Vec<crate::application::command::EventSpec>,
    #[serde(skip_serializing)]
    pub projections: Vec<ProjectionSpec>,
    #[serde(skip_serializing)]
    pub models: Vec<ModelSpec>,
    #[serde(default)]
    pub surfaces: Vec<SurfaceSpec>,
    #[serde(default)]
    pub extensions: Vec<ApplicationExtension>,
    #[serde(default)]
    pub fingerprints: ManifestFingerprint,
    #[serde(default)]
    pub provenance: ManifestProvenance,
}

/// Versioned portable authority. Derived inventories are deliberately absent:
/// accepting them would introduce a second, potentially conflicting authority.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestWire {
    schema_version: u32,
    name: String,
    #[serde(default)]
    modules: Vec<ModuleManifest>,
    #[serde(default)]
    surfaces: Vec<SurfaceSpec>,
    #[serde(default)]
    extensions: Vec<ApplicationExtension>,
    #[serde(default)]
    fingerprints: ManifestFingerprint,
    #[serde(default)]
    provenance: ManifestProvenance,
}

impl<'de> Deserialize<'de> for ApplicationManifest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = ManifestWire::deserialize(deserializer)?;
        let mut manifest = Self {
            schema_version: wire.schema_version,
            name: wire.name,
            modules: wire.modules,
            surfaces: wire.surfaces,
            extensions: wire.extensions,
            fingerprints: wire.fingerprints,
            provenance: wire.provenance,
            commands: Vec::new(),
            events: Vec::new(),
            projections: Vec::new(),
            models: Vec::new(),
        };
        manifest
            .reconstruct_inventories()
            .map_err(serde::de::Error::custom)?;
        Ok(manifest)
    }
}
