//! Validated deployment plan compiler.
//!
//! Compiles an [`ApplicationManifest`] plus explicit process placement into a
//! deterministic, serializable [`DeploymentPlan`]. No I/O, process startup, or
//! cluster types are introduced here.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::capability::{
    derive_process_capabilities, Capability, CapabilityRequirement, SchemaLifecycleRequirement,
};
use super::error::{ApplicationError, ApplicationResult};
use super::identity::{canonical_json, sha256_fingerprint, LogicalId};
use super::manifest::{ApplicationManifest, APPLICATION_MANIFEST_SCHEMA_VERSION};
use super::mount::{validate_mounts_against_manifest, MountSelector, ProcessPreset};
use super::topology::{derive_topology, TopologyIntent};
use crate::command::CommandConsistency;

/// Wire schema version for deployment plans.
pub const DEPLOYMENT_PLAN_SCHEMA_VERSION: u32 = 1;
pub const MAX_DEPLOYMENT_PLAN_BYTES: usize = 1024 * 1024;
pub const MAX_PLAN_PROCESSES: usize = 256;
pub const MAX_PLAN_MOUNTS_PER_PROCESS: usize = 4096;

/// One process entry in a deployment plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessPlan {
    pub id: String,
    /// Mounts selected into this process (sorted, unique).
    pub mounts: Vec<MountSelector>,
    /// When true, command execution is remote for surfaces that need dispatch.
    /// Local command mounts still imply local execution when present.
    #[serde(default)]
    pub remote_commands: bool,
    pub capabilities: Vec<CapabilityRequirement>,
    pub topology: Vec<TopologyIntent>,
}

/// Complete validated deployment plan linked to an application manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentPlan {
    pub schema_version: u32,
    pub name: String,
    /// Application name from the source manifest.
    pub application: String,
    /// Logical fingerprint of the predecessor application manifest.
    pub application_manifest_logical: String,
    /// Canonical fingerprint of the predecessor application manifest.
    pub application_manifest_canonical: String,
    pub processes: Vec<ProcessPlan>,
    /// Union of process capabilities with reasons preserved.
    pub capabilities: Vec<CapabilityRequirement>,
    pub schema_lifecycle: SchemaLifecycleRequirement,
    pub fingerprints: PlanFingerprint,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlanFingerprint {
    pub logical: String,
    pub canonical: String,
}

/// Builder for one process before plan compilation.
#[derive(Clone, Debug)]
pub struct ProcessIntent {
    pub id: String,
    pub mounts: Vec<MountSelector>,
    pub remote_commands: bool,
}

impl ProcessIntent {
    pub fn new(id: impl Into<String>) -> ApplicationResult<Self> {
        Ok(Self {
            id: LogicalId::try_new("process", id)?.into_string(),
            mounts: Vec::new(),
            remote_commands: false,
        })
    }

    pub fn with_preset(
        id: impl Into<String>,
        manifest: &ApplicationManifest,
        preset: ProcessPreset,
    ) -> ApplicationResult<Self> {
        let mut process = Self::new(id)?;
        process.mounts = preset.expand(manifest)?;
        Ok(process)
    }

    pub fn mounts(mut self, mounts: impl IntoIterator<Item = MountSelector>) -> Self {
        self.mounts.extend(mounts);
        self
    }

    pub fn remote_commands(mut self, remote: bool) -> Self {
        self.remote_commands = remote;
        self
    }
}

/// Compile explicit process intents against one application manifest.
pub fn compile_deployment_plan(
    name: impl Into<String>,
    manifest: &ApplicationManifest,
    processes: impl IntoIterator<Item = ProcessIntent>,
) -> ApplicationResult<DeploymentPlan> {
    manifest.validate()?;
    let name = LogicalId::try_new("deployment plan", name)?.into_string();
    let mut processes = processes.into_iter().collect::<Vec<_>>();
    if processes.is_empty() {
        // Zero-config default: one full local process.
        processes.push(ProcessIntent::with_preset(
            "full",
            manifest,
            ProcessPreset::Full,
        )?);
    }
    if processes.len() > MAX_PLAN_PROCESSES {
        return Err(ApplicationError::InvalidSpec(format!(
            "deployment plan exceeds max process count {MAX_PLAN_PROCESSES}"
        )));
    }

    let mut process_ids = BTreeSet::new();
    let mut compiled = Vec::new();
    let mut global_capabilities: BTreeMap<Capability, Vec<super::capability::CapabilityReason>> =
        BTreeMap::new();
    let mut schema_reasons = BTreeSet::new();
    let mut schema_owner: Option<String> = None;

    // Track command and direct-projection placement for Atomic collocation.
    let mut command_hosts: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut direct_projection_hosts: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut command_to_direct: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for command in &manifest.commands {
        if matches!(command.consistency, CommandConsistency::Atomic) {
            // Direct projection targets from projected_model / direct_projection proof.
            if let Some(model) = &command.projected_model {
                command_to_direct
                    .entry(command.id.clone())
                    .or_default()
                    .insert(model.clone());
            }
            // Also match direct projection specs that share the command's projected model.
            for projection in &manifest.projections {
                if projection.direct {
                    if let Some(model) = &command.projected_model {
                        if projection.models.iter().any(|m| m == model) {
                            command_to_direct
                                .entry(command.id.clone())
                                .or_default()
                                .insert(projection.id.clone());
                        }
                    }
                    // If the command lists direct_projection material, treat all
                    // direct projectors as potential collocation partners when
                    // the projected_model is unset but atomic.
                    if command.projected_model.is_none() && command.direct_projection.is_some() {
                        command_to_direct
                            .entry(command.id.clone())
                            .or_default()
                            .insert(projection.id.clone());
                    }
                }
            }
        }
    }

    for mut process in processes {
        if !process_ids.insert(process.id.clone()) {
            return Err(ApplicationError::Duplicate {
                kind: "process",
                identity: process.id,
            });
        }
        process.mounts.sort();
        process.mounts.dedup();
        if process.mounts.len() > MAX_PLAN_MOUNTS_PER_PROCESS {
            return Err(ApplicationError::InvalidSpec(format!(
                "process `{}` exceeds max mount count {MAX_PLAN_MOUNTS_PER_PROCESS}",
                process.id
            )));
        }
        validate_mounts_against_manifest(manifest, &process.mounts)?;

        // Local command mounts force local execution regardless of remote flag.
        let has_local_commands = process
            .mounts
            .iter()
            .any(|mount| matches!(mount, MountSelector::Command { .. }));
        let remote_commands = process.remote_commands && !has_local_commands;

        for mount in &process.mounts {
            match mount {
                MountSelector::Command { id } => {
                    command_hosts
                        .entry(id.clone())
                        .or_default()
                        .insert(process.id.clone());
                }
                MountSelector::Projector { id } => {
                    if let Some(projection) = manifest
                        .projections
                        .iter()
                        .find(|projection| projection.id == *id)
                    {
                        if projection.direct {
                            direct_projection_hosts
                                .entry(id.clone())
                                .or_default()
                                .insert(process.id.clone());
                        }
                    }
                }
                _ => {}
            }
        }

        let (capabilities, schema) =
            derive_process_capabilities(manifest, &process.id, &process.mounts, remote_commands)?;
        for requirement in &capabilities {
            global_capabilities
                .entry(requirement.capability)
                .or_default()
                .extend(requirement.reasons.iter().cloned());
        }
        for reason in schema.reasons {
            schema_reasons.insert(reason);
        }
        if let Some(owner) = schema.logical_owner {
            match &schema_owner {
                Some(existing) if existing != &owner => {
                    return Err(ApplicationError::Collision {
                        kind: "schema lifecycle owner",
                        identity: owner,
                        reason: format!("conflicts with existing owner `{existing}`"),
                    });
                }
                None => schema_owner = Some(owner),
                _ => {}
            }
        }

        let topology = derive_topology(manifest, &process.id, &process.mounts, remote_commands)?;
        compiled.push(ProcessPlan {
            id: process.id,
            mounts: process.mounts,
            remote_commands,
            capabilities,
            topology,
        });
    }

    // Atomic collocation: each Atomic command and its direct projection mounts
    // must share at least one process. Eventual splits are always allowed.
    for (command_id, direct_ids) in &command_to_direct {
        let Some(command_processes) = command_hosts.get(command_id) else {
            // Atomic command not mounted anywhere is fine for pure API plans.
            continue;
        };
        for direct_id in direct_ids {
            // direct_id may be a model name or projection id — match projection hosts.
            let projection_hosts = direct_projection_hosts.get(direct_id);
            let model_hosts: Option<BTreeSet<String>> = {
                // If direct_id is a model, find direct projectors targeting it.
                let mut hosts = BTreeSet::new();
                for projection in &manifest.projections {
                    if projection.direct && projection.models.iter().any(|m| m == direct_id) {
                        if let Some(process_ids) = direct_projection_hosts.get(&projection.id) {
                            hosts.extend(process_ids.iter().cloned());
                        }
                    }
                }
                if hosts.is_empty() {
                    None
                } else {
                    Some(hosts)
                }
            };
            let hosts = match (projection_hosts, model_hosts) {
                (Some(a), Some(b)) => a.union(&b).cloned().collect::<BTreeSet<_>>(),
                (Some(a), None) => a.clone(),
                (None, Some(b)) => b,
                (None, None) => {
                    // No direct projector mounted for this atomic command — fail.
                    return Err(ApplicationError::InvalidSpec(format!(
                        "atomic command `{command_id}` is mounted without collocated direct projection `{direct_id}`"
                    )));
                }
            };
            let shared = command_processes
                .intersection(&hosts)
                .cloned()
                .collect::<BTreeSet<_>>();
            if shared.is_empty() {
                return Err(ApplicationError::InvalidSpec(format!(
                    "atomic command `{command_id}` must be collocated with direct projection `{direct_id}`; command processes {:?}, projection processes {:?}",
                    command_processes, hosts
                )));
            }
        }
    }

    // Reject duplicate command ownership across processes when both claim local execution.
    for (command_id, hosts) in &command_hosts {
        if hosts.len() > 1 {
            return Err(ApplicationError::Collision {
                kind: "command mount",
                identity: command_id.clone(),
                reason: format!(
                    "selected in multiple processes: {}",
                    hosts.iter().cloned().collect::<Vec<_>>().join(", ")
                ),
            });
        }
    }

    compiled.sort_by(|left, right| left.id.cmp(&right.id));

    let capabilities = global_capabilities
        .into_iter()
        .map(|(capability, mut reasons)| {
            reasons.sort();
            reasons.dedup();
            CapabilityRequirement {
                capability,
                reasons,
            }
        })
        .collect::<Vec<_>>();

    let schema_lifecycle = SchemaLifecycleRequirement {
        required: !schema_reasons.is_empty(),
        logical_owner: schema_owner,
        reasons: schema_reasons.into_iter().collect(),
    };

    let mut plan = DeploymentPlan {
        schema_version: DEPLOYMENT_PLAN_SCHEMA_VERSION,
        name,
        application: manifest.name.clone(),
        application_manifest_logical: manifest.fingerprints.logical.clone(),
        application_manifest_canonical: manifest.fingerprints.canonical.clone(),
        processes: compiled,
        capabilities,
        schema_lifecycle,
        fingerprints: PlanFingerprint::default(),
    };
    plan.refresh_fingerprints()?;
    plan.validate()?;
    Ok(plan)
}

impl DeploymentPlan {
    pub fn refresh_fingerprints(&mut self) -> ApplicationResult<()> {
        self.fingerprints = expected_fingerprints(self)?;
        Ok(())
    }

    pub fn canonical_bytes(&self) -> ApplicationResult<Vec<u8>> {
        let mut value = serde_json::to_value(self)?;
        if let serde_json::Value::Object(fields) = &mut value {
            // Fingerprints are derived; zero them for logical identity then re-encode full.
            fields.insert(
                "fingerprints".into(),
                serde_json::json!({ "logical": "", "canonical": "" }),
            );
        }
        let logical = canonical_json(&value);
        Ok(serde_json::to_vec(&logical)?)
    }

    pub fn encode(&self) -> ApplicationResult<Vec<u8>> {
        self.validate()?;
        let bytes = serde_json::to_vec(&canonical_json(&serde_json::to_value(self)?))?;
        if bytes.len() > MAX_DEPLOYMENT_PLAN_BYTES {
            return Err(ApplicationError::InvalidSpec(format!(
                "deployment plan exceeds max bytes {MAX_DEPLOYMENT_PLAN_BYTES}"
            )));
        }
        Ok(bytes)
    }

    pub fn from_canonical_bytes(bytes: &[u8]) -> ApplicationResult<Self> {
        if bytes.len() > MAX_DEPLOYMENT_PLAN_BYTES {
            return Err(ApplicationError::InvalidSpec(
                "deployment plan bytes exceed maximum".into(),
            ));
        }
        let plan: Self = serde_json::from_slice(bytes)?;
        plan.validate()?;
        let reencoded = plan.encode()?;
        if reencoded != bytes {
            return Err(ApplicationError::NonCanonical("deployment plan"));
        }
        Ok(plan)
    }

    pub fn validate(&self) -> ApplicationResult<()> {
        if self.schema_version != DEPLOYMENT_PLAN_SCHEMA_VERSION {
            return Err(ApplicationError::UnsupportedVersion {
                expected: DEPLOYMENT_PLAN_SCHEMA_VERSION,
                actual: self.schema_version,
            });
        }
        LogicalId::try_new("deployment plan", self.name.clone())?;
        LogicalId::try_new("application", self.application.clone())?;
        if self.application_manifest_logical.is_empty()
            || self.application_manifest_canonical.is_empty()
        {
            return Err(ApplicationError::InvalidSpec(
                "deployment plan requires application manifest predecessor fingerprints".into(),
            ));
        }
        if self.processes.is_empty() {
            return Err(ApplicationError::InvalidSpec(
                "deployment plan must declare at least one process".into(),
            ));
        }
        let expected = expected_fingerprints(self)?;
        if self.fingerprints != expected {
            return Err(ApplicationError::NonCanonical(
                "deployment plan fingerprints",
            ));
        }
        // Silence unused import when APPLICATION_MANIFEST_SCHEMA_VERSION is only
        // for documentation linkage in validate paths.
        let _ = APPLICATION_MANIFEST_SCHEMA_VERSION;
        Ok(())
    }

    /// Pure inspection data for CLI/runtime consumers.
    pub fn describe(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "application": self.application,
            "processes": self.processes.iter().map(|process| {
                serde_json::json!({
                    "id": process.id,
                    "mounts": process.mounts,
                    "remote_commands": process.remote_commands,
                    "capability_count": process.capabilities.len(),
                    "topology_count": process.topology.len(),
                })
            }).collect::<Vec<_>>(),
            "capabilities": self.capabilities.iter().map(|cap| {
                serde_json::json!({
                    "capability": cap.capability.as_str(),
                    "reason_count": cap.reasons.len(),
                })
            }).collect::<Vec<_>>(),
            "schema_lifecycle": self.schema_lifecycle,
            "application_manifest_logical": self.application_manifest_logical,
            "application_manifest_canonical": self.application_manifest_canonical,
            "fingerprints": self.fingerprints,
        })
    }
}

fn expected_fingerprints(plan: &DeploymentPlan) -> ApplicationResult<PlanFingerprint> {
    let mut for_logical = plan.clone();
    for_logical.fingerprints = PlanFingerprint::default();
    let logical_value = canonical_json(&serde_json::to_value(&for_logical)?);
    let logical_bytes = serde_json::to_vec(&logical_value)?;
    let logical = sha256_fingerprint(&logical_bytes);

    let mut for_canonical = plan.clone();
    for_canonical.fingerprints = PlanFingerprint {
        logical: logical.clone(),
        canonical: String::new(),
    };
    let canonical_value = canonical_json(&serde_json::to_value(&for_canonical)?);
    let canonical_bytes = serde_json::to_vec(&canonical_value)?;
    let canonical = sha256_fingerprint(&canonical_bytes);
    Ok(PlanFingerprint { logical, canonical })
}
