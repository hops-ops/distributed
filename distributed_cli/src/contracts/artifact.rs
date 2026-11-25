use serde::{Deserialize, Serialize};
use std::fmt;

/// The reference-level artifact vocabulary shared by lifecycle checks.
///
/// These variants identify semantic owners; they do not carry the payload of
/// the referenced artifact.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum ContractArtifactKind {
    /// Compiler-owned generated client artifact tree.
    GeneratedClientTree,
    /// Framework-owned logical application manifest.
    ApplicationManifest,
    /// Framework-owned process-selection deployment plan.
    DeploymentPlan,
}

impl ContractArtifactKind {
    /// The stable wire spelling for this kind.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GeneratedClientTree => "generated_client_tree",
            Self::ApplicationManifest => "application_manifest",
            Self::DeploymentPlan => "deployment_plan",
        }
    }
}

impl fmt::Display for ContractArtifactKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A portable content or owner-defined identity for one artifact.
///
/// `value` is intentionally opaque to this foundation. Producers may use a
/// content digest or another stable identity, but paths, timestamps, machine
/// locations, environment values, and secrets are not valid identity material.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactIdentity {
    /// The semantic kind whose identity is being represented.
    pub kind: ContractArtifactKind,
    /// Stable identity value, commonly `sha256:<lowercase-hex>`.
    pub value: String,
}

impl ArtifactIdentity {
    /// Construct an identity from an already validated stable value.
    pub fn new(kind: ContractArtifactKind, value: impl Into<String>) -> Self {
        Self {
            kind,
            value: value.into(),
        }
    }
}

/// Source references and generator identity for an artifact.
///
/// `generator` is descriptive metadata only. The catalog never executes it
/// and never interprets it as a shell command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactProvenance {
    /// Catalog-relative authoritative source files or bounded glob patterns.
    pub sources: std::collections::BTreeSet<String>,
    /// Stable generator identifier, not an executable command.
    pub generator: String,
    /// Maximum number of files a source glob may resolve to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub glob_limit: Option<usize>,
}

/// The immediate predecessor link in the lifecycle chain.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactPredecessor {
    /// Catalog entry ID of the predecessor.
    #[serde(alias = "entry")]
    pub entry_id: String,
    /// Identity observed when this artifact was produced.
    pub identity: ArtifactIdentity,
}
