//! Client program descriptors and three-way compatibility classification.
//!
//! Hot-code-push handoff for deployable UI programs. No browser staging,
//! service workers, or activation behavior lives here.

use super::artifact::{ArtifactIdentity, ContractArtifactKind};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

/// Descriptor wire version.
pub const CLIENT_PROGRAM_DESCRIPTOR_VERSION: u32 = 1;
/// Compatibility policy version for V1 exact mutation/offline identities.
pub const CLIENT_PROGRAM_POLICY_VERSION: u32 = 1;
pub const MAX_PROGRAM_ASSETS: usize = 8_192;
pub const MAX_PROGRAM_ASSET_BYTES: usize = 32 * 1024 * 1024;

/// One portable asset in a deployable program tree.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProgramAsset {
    /// Portable relative path under the deployable root.
    pub path: String,
    /// SHA-256 digest of file bytes (`sha256:<hex>`).
    pub digest: String,
    /// Optional SRI string when produced by the packaging pipeline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integrity: Option<String>,
    pub size_bytes: u64,
}

/// One client surface bound into the program.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProgramSurface {
    pub name: String,
    /// Schema fingerprint from the accepted surface manifest.
    pub schema_fingerprint: String,
    /// Protocol fingerprint from the accepted surface manifest.
    pub protocol_fingerprint: String,
}

/// One generated operation/artifact identity referenced by the program.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProgramArtifact {
    pub path: String,
    pub digest: String,
}

/// Complete deterministic client program descriptor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProgramDescriptor {
    pub version: u32,
    pub policy_version: u32,
    pub program_name: String,
    /// Complete identity of the program (version + policy + contract set + assets).
    pub program_id: String,
    /// Sorted surface/artifact contract identities.
    pub contract_set_id: String,
    pub surfaces: Vec<ClientProgramSurface>,
    pub artifacts: Vec<ClientProgramArtifact>,
    pub assets: Vec<ClientProgramAsset>,
    /// Selected read-model identities (`RMV-REQ-004`). Empty means unspecified.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read_models: Vec<String>,
    /// Immediate application-manifest predecessor identity.
    pub application_manifest: ArtifactIdentity,
    /// Immediate deployment-plan predecessor identity.
    pub deployment_plan: ArtifactIdentity,
}

/// Three-way compatibility classification for loaded vs advertised programs.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgramCompatibility {
    /// Exact program identity match.
    Current,
    /// Contract set identical; only assets differ (hot asset push).
    CompatibleAssetOnlyUpdate,
    /// Mutation/offline contract set differs or material is incomplete.
    IncompatibleRequiredUpdate,
}

impl ClientProgramDescriptor {
    pub fn builder(program_name: impl Into<String>) -> ClientProgramDescriptorBuilder {
        ClientProgramDescriptorBuilder {
            program_name: program_name.into(),
            surfaces: Vec::new(),
            artifacts: Vec::new(),
            assets: Vec::new(),
            read_models: Vec::new(),
            application_manifest: None,
            deployment_plan: None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version != CLIENT_PROGRAM_DESCRIPTOR_VERSION {
            return Err(format!(
                "unsupported client program descriptor version {}",
                self.version
            ));
        }
        if self.policy_version != CLIENT_PROGRAM_POLICY_VERSION {
            return Err(format!(
                "unsupported client program policy version {}",
                self.policy_version
            ));
        }
        if self.program_name.trim().is_empty() {
            return Err("program_name must not be empty".into());
        }
        if self.surfaces.is_empty() {
            return Err("program must declare at least one surface".into());
        }
        if self.assets.len() > MAX_PROGRAM_ASSETS {
            return Err(format!(
                "program exceeds max asset count {MAX_PROGRAM_ASSETS}"
            ));
        }
        let mut paths = BTreeSet::new();
        for asset in &self.assets {
            validate_portable_path(&asset.path)?;
            if !paths.insert(asset.path.clone()) {
                return Err(format!("duplicate portable asset path `{}`", asset.path));
            }
            if !asset.digest.starts_with("sha256:") {
                return Err(format!("asset `{}` digest must be sha256", asset.path));
            }
        }
        for surface in &self.surfaces {
            if surface.name.trim().is_empty() {
                return Err("surface name must not be empty".into());
            }
        }
        if self.application_manifest.kind != ContractArtifactKind::ApplicationManifest {
            return Err("application_manifest predecessor kind mismatch".into());
        }
        if self.deployment_plan.kind != ContractArtifactKind::DeploymentPlan {
            return Err("deployment_plan predecessor kind mismatch".into());
        }
        let mut canonical_models = self.read_models.clone();
        canonical_models.sort();
        canonical_models.dedup();
        if canonical_models != self.read_models {
            return Err("read_models must be sorted and unique".into());
        }
        let expected_contract = contract_set_id(&self.surfaces, &self.artifacts, &self.read_models);
        if self.contract_set_id != expected_contract {
            return Err(
                "contract_set_id is stale relative to surfaces/artifacts/read_models".into(),
            );
        }
        let expected_program = program_id(
            self.version,
            self.policy_version,
            &self.contract_set_id,
            &self.assets,
        );
        if self.program_id != expected_program {
            return Err("program_id is stale relative to contract set and assets".into());
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| error.to_string())
    }

    pub fn classify_against(&self, loaded: &Self) -> Result<ProgramCompatibility, String> {
        self.validate()?;
        loaded.validate()?;
        if self.program_id == loaded.program_id {
            return Ok(ProgramCompatibility::Current);
        }
        if self.contract_set_id == loaded.contract_set_id {
            return Ok(ProgramCompatibility::CompatibleAssetOnlyUpdate);
        }
        Ok(ProgramCompatibility::IncompatibleRequiredUpdate)
    }
}

pub struct ClientProgramDescriptorBuilder {
    program_name: String,
    surfaces: Vec<ClientProgramSurface>,
    artifacts: Vec<ClientProgramArtifact>,
    assets: Vec<ClientProgramAsset>,
    read_models: Vec<String>,
    application_manifest: Option<ArtifactIdentity>,
    deployment_plan: Option<ArtifactIdentity>,
}

impl ClientProgramDescriptorBuilder {
    pub fn surface(mut self, surface: ClientProgramSurface) -> Self {
        self.surfaces.push(surface);
        self
    }

    pub fn artifact(mut self, artifact: ClientProgramArtifact) -> Self {
        self.artifacts.push(artifact);
        self
    }

    pub fn asset(mut self, asset: ClientProgramAsset) -> Self {
        self.assets.push(asset);
        self
    }

    pub fn application_manifest(mut self, identity: ArtifactIdentity) -> Self {
        self.application_manifest = Some(identity);
        self
    }

    pub fn deployment_plan(mut self, identity: ArtifactIdentity) -> Self {
        self.deployment_plan = Some(identity);
        self
    }

    pub fn read_models(mut self, models: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.read_models = models.into_iter().map(Into::into).collect();
        self.read_models.sort();
        self.read_models.dedup();
        self
    }

    /// Hash every regular file under `root` into portable assets.
    pub fn assets_from_dir(mut self, root: &Path) -> Result<Self, String> {
        let mut assets = collect_assets(root)?;
        assets.sort();
        self.assets.extend(assets);
        Ok(self)
    }

    pub fn build(mut self) -> Result<ClientProgramDescriptor, String> {
        self.surfaces.sort_by(|a, b| a.name.cmp(&b.name));
        self.artifacts.sort_by(|a, b| a.path.cmp(&b.path));
        self.assets.sort();
        let application_manifest = self.application_manifest.ok_or_else(|| {
            "client program requires application_manifest predecessor identity".to_string()
        })?;
        let deployment_plan = self.deployment_plan.ok_or_else(|| {
            "client program requires deployment_plan predecessor identity".to_string()
        })?;
        let contract_set_id = contract_set_id(&self.surfaces, &self.artifacts, &self.read_models);
        let program_id = program_id(
            CLIENT_PROGRAM_DESCRIPTOR_VERSION,
            CLIENT_PROGRAM_POLICY_VERSION,
            &contract_set_id,
            &self.assets,
        );
        let descriptor = ClientProgramDescriptor {
            version: CLIENT_PROGRAM_DESCRIPTOR_VERSION,
            policy_version: CLIENT_PROGRAM_POLICY_VERSION,
            program_name: self.program_name,
            program_id,
            contract_set_id,
            surfaces: self.surfaces,
            artifacts: self.artifacts,
            assets: self.assets,
            read_models: self.read_models,
            application_manifest,
            deployment_plan,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }
}

fn contract_set_id(
    surfaces: &[ClientProgramSurface],
    artifacts: &[ClientProgramArtifact],
    read_models: &[String],
) -> String {
    let mut material = BTreeMap::new();
    if !read_models.is_empty() {
        material.insert(
            "read_models".into(),
            serde_json::to_string(read_models).unwrap_or_default(),
        );
    }
    for surface in surfaces {
        material.insert(
            format!("surface:{}", surface.name),
            format!(
                "{}|{}",
                surface.schema_fingerprint, surface.protocol_fingerprint
            ),
        );
    }
    for artifact in artifacts {
        material.insert(
            format!("artifact:{}", artifact.path),
            artifact.digest.clone(),
        );
    }
    let bytes = serde_json::to_vec(&material).unwrap_or_default();
    identity_digest("distributed.client-program.contract-set.v1", &bytes)
}

fn program_id(
    version: u32,
    policy_version: u32,
    contract_set_id: &str,
    assets: &[ClientProgramAsset],
) -> String {
    let asset_ids = assets
        .iter()
        .map(|asset| format!("{}:{}", asset.path, asset.digest))
        .collect::<Vec<_>>();
    let material = serde_json::json!({
        "version": version,
        "policy_version": policy_version,
        "contract_set_id": contract_set_id,
        "assets": asset_ids,
    });
    let bytes = serde_json::to_vec(&material).unwrap_or_default();
    identity_digest("distributed.client-program.program-id.v1", &bytes)
}

fn collect_assets(root: &Path) -> Result<Vec<ClientProgramAsset>, String> {
    if !root.is_dir() {
        return Err(format!(
            "asset root `{}` is not a directory",
            root.display()
        ));
    }
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let mut assets = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir).map_err(|e| e.to_string())?;
        for entry in entries {
            let entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path();
            let meta = entry.metadata().map_err(|e| e.to_string())?;
            if meta.file_type().is_symlink() {
                return Err(format!("symlink assets are rejected: {}", path.display()));
            }
            if meta.is_dir() {
                stack.push(path);
                continue;
            }
            if !meta.is_file() {
                return Err(format!("special files are rejected: {}", path.display()));
            }
            if meta.len() as usize > MAX_PROGRAM_ASSET_BYTES {
                return Err(format!(
                    "asset `{}` exceeds max bytes {MAX_PROGRAM_ASSET_BYTES}",
                    path.display()
                ));
            }
            let relative = path
                .strip_prefix(&root)
                .map_err(|_| "asset escaped root".to_string())?
                .to_string_lossy()
                .replace('\\', "/");
            validate_portable_path(&relative)?;
            let bytes = fs::read(&path).map_err(|e| e.to_string())?;
            let digest = {
                let mut hasher = Sha256::new();
                hasher.update(&bytes);
                format!("sha256:{:x}", hasher.finalize())
            };
            assets.push(ClientProgramAsset {
                path: relative,
                digest,
                integrity: None,
                size_bytes: meta.len(),
            });
            if assets.len() > MAX_PROGRAM_ASSETS {
                return Err(format!("asset tree exceeds max count {MAX_PROGRAM_ASSETS}"));
            }
        }
    }
    Ok(assets)
}

fn validate_portable_path(path: &str) -> Result<(), String> {
    if path.is_empty() || path.starts_with('/') || path.contains('\0') {
        return Err(format!("invalid portable path `{path}`"));
    }
    if Path::new(path)
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(format!("path traversal rejected: `{path}`"));
    }
    Ok(())
}

// Local helper so program module does not depend on application identity.
fn identity_digest(domain: &str, bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain.as_bytes());
    digest.update(b"\0");
    digest.update(bytes);
    format!("sha256:{:x}", digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("client-program-{nanos}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn base_builder(assets_dir: &Path) -> ClientProgramDescriptorBuilder {
        ClientProgramDescriptor::builder("e2e-ui")
            .surface(ClientProgramSurface {
                name: "e2e-ui".into(),
                schema_fingerprint: "sha256:schema".into(),
                protocol_fingerprint: "sha256:protocol".into(),
            })
            .artifact(ClientProgramArtifact {
                path: "operations/todos.ts".into(),
                digest: "sha256:op".into(),
            })
            .application_manifest(ArtifactIdentity::new(
                ContractArtifactKind::ApplicationManifest,
                "sha256:app",
            ))
            .deployment_plan(ArtifactIdentity::new(
                ContractArtifactKind::DeploymentPlan,
                "sha256:plan",
            ))
            .assets_from_dir(assets_dir)
            .unwrap()
    }

    #[test]
    fn descriptor_is_byte_deterministic_and_classifies_asset_only_updates() {
        let root = temp_dir();
        fs::write(root.join("app.js"), b"console.log(1)").unwrap();
        let first = base_builder(&root).build().unwrap();
        let second = base_builder(&root).build().unwrap();
        assert_eq!(
            first.canonical_bytes().unwrap(),
            second.canonical_bytes().unwrap()
        );
        assert_eq!(
            first.classify_against(&second).unwrap(),
            ProgramCompatibility::Current
        );

        fs::write(root.join("app.js"), b"console.log(2)").unwrap();
        let asset_changed = base_builder(&root).build().unwrap();
        assert_eq!(
            first.classify_against(&asset_changed).unwrap(),
            ProgramCompatibility::CompatibleAssetOnlyUpdate
        );
        assert_eq!(first.contract_set_id, asset_changed.contract_set_id);
        assert_ne!(first.program_id, asset_changed.program_id);

        let mut surface_changed = base_builder(&root).build().unwrap();
        surface_changed.surfaces[0].schema_fingerprint = "sha256:other".into();
        // Rebuild ids after mutation via builder path
        let surface_changed = ClientProgramDescriptor::builder("e2e-ui")
            .surface(ClientProgramSurface {
                name: "e2e-ui".into(),
                schema_fingerprint: "sha256:other".into(),
                protocol_fingerprint: "sha256:protocol".into(),
            })
            .artifact(ClientProgramArtifact {
                path: "operations/todos.ts".into(),
                digest: "sha256:op".into(),
            })
            .application_manifest(ArtifactIdentity::new(
                ContractArtifactKind::ApplicationManifest,
                "sha256:app",
            ))
            .deployment_plan(ArtifactIdentity::new(
                ContractArtifactKind::DeploymentPlan,
                "sha256:plan",
            ))
            .assets_from_dir(&root)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            first.classify_against(&surface_changed).unwrap(),
            ProgramCompatibility::IncompatibleRequiredUpdate
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sibling_program_identity_stays_stable_when_another_read_model_list_changes() {
        let root = temp_dir();
        fs::write(root.join("app.js"), b"console.log(1)").unwrap();
        let web = base_builder(&root)
            .read_models(["operational.todos", "shared.chat"])
            .build()
            .unwrap();
        let mobile = base_builder(&root)
            .read_models(["mobile.todos", "shared.chat"])
            .build()
            .unwrap();
        let web_again = base_builder(&root)
            .read_models(["operational.todos", "shared.chat"])
            .build()
            .unwrap();
        assert_eq!(web.program_id, web_again.program_id);
        assert_ne!(web.program_id, mobile.program_id);
        assert_ne!(web.contract_set_id, mobile.contract_set_id);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn validate_rejects_unsorted_or_duplicate_read_models() {
        let root = temp_dir();
        fs::write(root.join("app.js"), b"console.log(1)").unwrap();
        let mut descriptor = base_builder(&root)
            .read_models(["shared.chat", "operational.todos"])
            .build()
            .unwrap();
        descriptor.read_models = vec!["shared.chat".into(), "operational.todos".into()];
        assert!(descriptor
            .validate()
            .unwrap_err()
            .contains("read_models must be sorted"));
        descriptor.read_models = vec!["operational.todos".into(), "operational.todos".into()];
        assert!(descriptor
            .validate()
            .unwrap_err()
            .contains("read_models must be sorted"));
        let _ = fs::remove_dir_all(root);
    }
}
