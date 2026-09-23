//! Deterministic application lifecycle graph and generation records.
//!
//! This module composes the existing contract catalog. It never owns artifact
//! payload semantics and never executes a catalog generator as a shell command.

mod build;
mod dev;
mod graph;
mod project;
mod receipt;
mod release;

/// Internal process contract carrying the canonical CLI that initiated a
/// discovered lifecycle. It is deliberately not part of user-authored
/// lifecycle configuration.
pub(crate) const LIFECYCLE_CLI_EXECUTABLE_ENV: &str = "DISTRIBUTED_LIFECYCLE_CLI_EXECUTABLE";

pub use build::{
    activate_lifecycle_project_generation, run_lifecycle_build, run_lifecycle_project_build,
    BuildDrift, LifecycleActivation, LifecycleBuildConfig, LifecycleBuildOptions,
    LifecycleBuildReport, LifecycleBuildRequest, LifecycleCheckBaseline, LifecycleExecutor,
    LifecycleProjectPlan, LIFECYCLE_BUILD_CONFIG_SCHEMA_VERSION,
};
pub use dev::{
    run_lifecycle_dev, run_lifecycle_project_dev, LifecycleDevConfig, LifecycleDevOptions,
    LifecycleDevProbe, LifecycleDevProcess, LifecycleDevReport, LifecycleProjectDevOptions,
};
pub use graph::{
    DistributedSourceIdentity, LifecycleConfig, LifecycleError, LifecycleGraph, LifecycleNode,
    LIFECYCLE_CONFIG_SCHEMA_VERSION, LIFECYCLE_GRAPH_SCHEMA_VERSION, MAX_LIFECYCLE_NODES,
};
pub(crate) use project::discover_ui;
pub use project::{discover_lifecycle_project, DiscoveredLifecycleProject};
pub use receipt::{
    ArtifactNodeReceipt, GenerationManifest, GENERATION_MANIFEST_SCHEMA_VERSION,
    NODE_RECEIPT_SCHEMA_VERSION,
};
pub use release::{ReleaseManifest, ReleaseMember, RELEASE_MANIFEST_SCHEMA_VERSION};

pub(crate) fn digest_bytes(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(bytes);
    let hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("sha256:{hex}")
}

pub(crate) fn validate_stable_value(value: &str, label: &str) -> Result<(), LifecycleError> {
    if value.is_empty() || value != value.trim() || value.len() > 4096 {
        return Err(LifecycleError::new(format!(
            "{label} must be a non-empty bounded stable value"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(LifecycleError::new(format!(
            "{label} contains control characters"
        )));
    }
    Ok(())
}

pub(crate) fn validate_content_identity(value: &str, label: &str) -> Result<(), LifecycleError> {
    let hex = value
        .strip_prefix("sha256:")
        .ok_or_else(|| LifecycleError::new(format!("{label} must be a sha256 content identity")))?;
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(LifecycleError::new(format!(
            "{label} must contain exactly 64 lowercase hexadecimal digits"
        )));
    }
    Ok(())
}

pub(crate) fn validate_portable_path(value: &str, label: &str) -> Result<(), LifecycleError> {
    use std::path::{Component, Path};

    if value.is_empty() || value.starts_with('/') || value.contains(['\\', '\0']) {
        return Err(LifecycleError::new(format!(
            "{label} `{value}` is not a portable relative path"
        )));
    }
    if Path::new(value).components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(LifecycleError::new(format!(
            "{label} `{value}` escapes the lifecycle root"
        )));
    }
    Ok(())
}
