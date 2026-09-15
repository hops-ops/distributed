//! Composable mount algebra and process-role presets.
//!
//! Named presets expand to ordinary mount selectors. They are never a closed
//! capability enum — arbitrary mixes remain expressible through the same
//! algebra.

use serde::{Deserialize, Serialize};

use super::error::{ApplicationError, ApplicationResult};
use super::identity::LogicalId;
use super::manifest::ApplicationManifest;

/// One logical mount selected into a process.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MountSelector {
    /// Local command handler mount for a portable command identity.
    Command { id: String },
    /// Projection program mount (direct or eventual) for a projection id.
    Projector { id: String },
    /// Query/live surface mount for a surface identity.
    Surface { id: String },
    /// Explicit application extension mount.
    Extension { id: String },
}

impl MountSelector {
    pub fn command(id: impl Into<String>) -> ApplicationResult<Self> {
        Ok(Self::Command {
            id: LogicalId::try_new("command mount", id)?.into_string(),
        })
    }

    pub fn projector(id: impl Into<String>) -> ApplicationResult<Self> {
        Ok(Self::Projector {
            id: LogicalId::try_new("projector mount", id)?.into_string(),
        })
    }

    pub fn surface(id: impl Into<String>) -> ApplicationResult<Self> {
        Ok(Self::Surface {
            id: LogicalId::try_new("surface mount", id)?.into_string(),
        })
    }

    pub fn extension(id: impl Into<String>) -> ApplicationResult<Self> {
        Ok(Self::Extension {
            id: LogicalId::try_new("extension mount", id)?.into_string(),
        })
    }

    /// Gateway capabilities use the existing explicit extension mount algebra.
    #[cfg(feature = "gateway")]
    pub fn gateway(id: impl Into<String>) -> ApplicationResult<Self> {
        Self::extension(id)
    }

    pub fn kind_label(&self) -> &'static str {
        match self {
            Self::Command { .. } => "command",
            Self::Projector { .. } => "projector",
            Self::Surface { .. } => "surface",
            Self::Extension { .. } => "extension",
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::Command { id }
            | Self::Projector { id }
            | Self::Surface { id }
            | Self::Extension { id } => id.as_str(),
        }
    }
}

/// Named convenience presets that lower to ordinary mounts.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessPreset {
    /// Every command, projector, surface, and extension in the manifest.
    Full,
    /// Every command mount (writers only).
    Writer,
    /// Every projection mount.
    Projector,
    /// Every query/live surface.
    QueryApi,
}

impl ProcessPreset {
    /// Expand a preset into explicit mounts against one application manifest.
    pub fn expand(self, manifest: &ApplicationManifest) -> ApplicationResult<Vec<MountSelector>> {
        let mut mounts = match self {
            Self::Full => {
                let mut mounts = Vec::new();
                for command in &manifest.commands {
                    mounts.push(MountSelector::command(command.id.clone())?);
                }
                for projection in &manifest.projections {
                    mounts.push(MountSelector::projector(projection.id.clone())?);
                }
                for surface in &manifest.surfaces {
                    mounts.push(MountSelector::surface(surface.id.clone())?);
                }
                for extension in &manifest.extensions {
                    mounts.push(MountSelector::extension(extension.id.clone())?);
                }
                mounts
            }
            Self::Writer => manifest
                .commands
                .iter()
                .map(|command| MountSelector::command(command.id.clone()))
                .collect::<ApplicationResult<Vec<_>>>()?,
            Self::Projector => manifest
                .projections
                .iter()
                .map(|projection| MountSelector::projector(projection.id.clone()))
                .collect::<ApplicationResult<Vec<_>>>()?,
            Self::QueryApi => manifest
                .surfaces
                .iter()
                .map(|surface| MountSelector::surface(surface.id.clone()))
                .collect::<ApplicationResult<Vec<_>>>()?,
        };
        mounts.sort();
        mounts.dedup();
        Ok(mounts)
    }
}

/// Validate that every selector references an identity present in the manifest.
pub fn validate_mounts_against_manifest(
    manifest: &ApplicationManifest,
    mounts: &[MountSelector],
) -> ApplicationResult<()> {
    let mut seen = std::collections::BTreeSet::new();
    for mount in mounts {
        if !seen.insert(mount.clone()) {
            return Err(ApplicationError::Duplicate {
                kind: "mount selector",
                identity: format!("{}:{}", mount.kind_label(), mount.id()),
            });
        }
        match mount {
            MountSelector::Command { id } => {
                if !manifest.commands.iter().any(|command| command.id == *id) {
                    return Err(ApplicationError::Missing {
                        kind: "command mount",
                        identity: id.clone(),
                    });
                }
            }
            MountSelector::Projector { id } => {
                if !manifest
                    .projections
                    .iter()
                    .any(|projection| projection.id == *id)
                {
                    return Err(ApplicationError::Missing {
                        kind: "projector mount",
                        identity: id.clone(),
                    });
                }
            }
            MountSelector::Surface { id } => {
                if !manifest.surfaces.iter().any(|surface| surface.id == *id) {
                    return Err(ApplicationError::Missing {
                        kind: "surface mount",
                        identity: id.clone(),
                    });
                }
            }
            MountSelector::Extension { id } => {
                if !manifest
                    .extensions
                    .iter()
                    .any(|extension| extension.id == *id)
                {
                    return Err(ApplicationError::Missing {
                        kind: "extension mount",
                        identity: id.clone(),
                    });
                }
            }
        }
    }
    Ok(())
}
