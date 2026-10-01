//! Lifecycle decision classification for semantic and wire identity changes.
//!
//! Classification never guesses that every schema diff requires a protocol
//! bump. Each identity owner maps to a distinct required decision.

use super::diagnostic::ContractDiagnosticCode;
use super::snapshots::{SnapshotChange, SnapshotDiff};
use serde::{Deserialize, Serialize};

/// The decision an operator/tool must make for one detected change.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleDecision {
    /// Accept surface/client manifest wire change.
    AcceptManifestWire,
    /// Accept protocol/semantic fingerprint change.
    AcceptProtocolSemantic,
    /// Accept application-manifest logical identity change.
    AcceptApplicationManifest,
    /// Accept deployment-plan identity change.
    AcceptDeploymentPlan,
    /// No acceptance required (informational).
    None,
}

impl LifecycleDecision {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AcceptManifestWire => "accept_manifest_wire",
            Self::AcceptProtocolSemantic => "accept_protocol_semantic",
            Self::AcceptApplicationManifest => "accept_application_manifest",
            Self::AcceptDeploymentPlan => "accept_deployment_plan",
            Self::None => "none",
        }
    }
}

/// One classified change with a stable diagnostic code.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClassifiedChange {
    pub path: String,
    pub decision: LifecycleDecision,
    pub code: ContractDiagnosticCode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<serde_json::Value>,
}

/// Classify a snapshot diff into explicit lifecycle decisions.
pub fn classify_snapshot_diff(diff: &SnapshotDiff) -> Vec<ClassifiedChange> {
    diff.changes
        .iter()
        .map(|change| classify_change(&diff.kind, change))
        .collect()
}

fn classify_change(kind: &str, change: &SnapshotChange) -> ClassifiedChange {
    let (decision, code) = match kind {
        "surface_client_manifest" | "generated_client_tree" => {
            if change.path.contains("protocol") {
                (
                    LifecycleDecision::AcceptProtocolSemantic,
                    ContractDiagnosticCode::ProtocolDrift,
                )
            } else {
                (
                    LifecycleDecision::AcceptManifestWire,
                    ContractDiagnosticCode::ManifestVersion,
                )
            }
        }
        "application_manifest" => (
            LifecycleDecision::AcceptApplicationManifest,
            ContractDiagnosticCode::SchemaDrift,
        ),
        "deployment_plan" => (
            LifecycleDecision::AcceptDeploymentPlan,
            ContractDiagnosticCode::ChainStale,
        ),
        _ => {
            if change.path.contains("protocol") {
                (
                    LifecycleDecision::AcceptProtocolSemantic,
                    ContractDiagnosticCode::ProtocolDrift,
                )
            } else {
                (LifecycleDecision::None, ContractDiagnosticCode::SchemaDrift)
            }
        }
    };
    ClassifiedChange {
        path: change.path.clone(),
        decision,
        code,
        before: change.before.clone(),
        after: change.after.clone(),
    }
}

/// Prove that manifest-wire and protocol-semantic changes require distinct decisions.
pub fn decisions_are_distinct(changes: &[ClassifiedChange]) -> bool {
    let mut seen = std::collections::BTreeSet::new();
    for change in changes {
        if change.decision == LifecycleDecision::None {
            continue;
        }
        seen.insert(change.decision);
    }
    // Distinctness is only meaningful when both families appear.
    let has_wire = seen.contains(&LifecycleDecision::AcceptManifestWire);
    let has_protocol = seen.contains(&LifecycleDecision::AcceptProtocolSemantic);
    !(has_wire
        && has_protocol
        && changes.iter().any(|c| {
            c.decision == LifecycleDecision::AcceptManifestWire && c.path.contains("protocol")
        }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::snapshots::{diff_snapshots, snapshot_from_json};
    use serde_json::json;

    #[test]
    fn manifest_and_protocol_changes_require_distinct_decisions() {
        let before = snapshot_from_json(
            "client",
            "surface_client_manifest",
            &json!({
                "schema_fingerprint": "sha256:aaa",
                "protocol_fingerprint": "sha256:proto-1",
                "commands": [{ "id": "todo.create" }]
            }),
        )
        .unwrap();
        let after = snapshot_from_json(
            "client",
            "surface_client_manifest",
            &json!({
                "schema_fingerprint": "sha256:bbb",
                "protocol_fingerprint": "sha256:proto-2",
                "commands": [{ "id": "todo.create" }]
            }),
        )
        .unwrap();
        let classified = classify_snapshot_diff(&diff_snapshots(&before, &after));
        assert!(classified.iter().any(|c| {
            c.path.contains("schema_fingerprint")
                && c.decision == LifecycleDecision::AcceptManifestWire
        }));
        assert!(classified.iter().any(|c| {
            c.path.contains("protocol_fingerprint")
                && c.decision == LifecycleDecision::AcceptProtocolSemantic
                && c.code == ContractDiagnosticCode::ProtocolDrift
        }));
        assert!(decisions_are_distinct(&classified));
    }
}
