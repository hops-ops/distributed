//! CTL closeout helpers for non-deployment chain verification (task 8 slice).
//!
//! Deployment/render/XR closeout remains tasks 14–19 and is intentionally
//! out of scope here.

use super::artifact::{ArtifactIdentity, ArtifactPredecessor, ContractArtifactKind};
use super::chain::{check_predecessor_chain, ObservedPredecessor};
use super::diagnostic::ContractCheckResult;
use super::program::{ClientProgramDescriptor, ProgramCompatibility};

/// Verify the local contract chain: application → plan → optional program.
pub fn close_local_contract_chain(
    application: &ArtifactIdentity,
    plan: &ArtifactIdentity,
    plan_claims_application: bool,
    program: Option<&ClientProgramDescriptor>,
) -> ContractCheckResult {
    let mut observations = Vec::new();
    if plan_claims_application {
        observations.push(ObservedPredecessor {
            owner: "deployment-plan".into(),
            source_path: "contracts/deployment-plan".into(),
            claimed: ArtifactPredecessor {
                entry_id: "application-manifest".into(),
                identity: application.clone(),
            },
            observed: Some(application.clone()),
        });
    }
    if let Some(program) = program {
        observations.push(ObservedPredecessor {
            owner: format!("program:{}", program.program_name),
            source_path: "contracts/client-program".into(),
            claimed: ArtifactPredecessor {
                entry_id: "application-manifest".into(),
                identity: program.application_manifest.clone(),
            },
            observed: Some(application.clone()),
        });
        observations.push(ObservedPredecessor {
            owner: format!("program:{}", program.program_name),
            source_path: "contracts/client-program".into(),
            claimed: ArtifactPredecessor {
                entry_id: "deployment-plan".into(),
                identity: program.deployment_plan.clone(),
            },
            observed: Some(plan.clone()),
        });
    }
    let _ = ContractArtifactKind::ApplicationManifest;
    check_predecessor_chain(observations)
}

/// Classify program compatibility when both descriptors validate.
pub fn classify_release_programs(
    advertised: &ClientProgramDescriptor,
    loaded: &ClientProgramDescriptor,
) -> Result<ProgramCompatibility, String> {
    advertised.classify_against(loaded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::program::{
        ClientProgramArtifact, ClientProgramDescriptor, ClientProgramSurface,
    };
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn local_chain_closeout_accepts_matching_predecessors() {
        let app = ArtifactIdentity::new(ContractArtifactKind::ApplicationManifest, "sha256:app");
        let plan = ArtifactIdentity::new(ContractArtifactKind::DeploymentPlan, "sha256:plan");
        let result = close_local_contract_chain(&app, &plan, true, None);
        assert!(result.diagnostics.is_empty());
    }

    #[test]
    fn local_chain_closeout_reports_stale_program_plan() {
        let root = std::env::temp_dir().join(format!(
            "closeout-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("a.js"), b"1").unwrap();
        let app = ArtifactIdentity::new(ContractArtifactKind::ApplicationManifest, "sha256:app");
        let plan = ArtifactIdentity::new(ContractArtifactKind::DeploymentPlan, "sha256:plan");
        let program = ClientProgramDescriptor::builder("e2e-ui")
            .surface(ClientProgramSurface {
                name: "e2e-ui".into(),
                schema_fingerprint: "sha256:s".into(),
                protocol_fingerprint: "sha256:p".into(),
            })
            .artifact(ClientProgramArtifact {
                path: "op.ts".into(),
                digest: "sha256:o".into(),
            })
            .application_manifest(app.clone())
            .deployment_plan(ArtifactIdentity::new(
                ContractArtifactKind::DeploymentPlan,
                "sha256:stale-plan",
            ))
            .assets_from_dir(&root)
            .unwrap()
            .build()
            .unwrap();
        let result = close_local_contract_chain(&app, &plan, true, Some(&program));
        assert!(!result.diagnostics.is_empty());
        let _ = fs::remove_dir_all(root);
    }
}
