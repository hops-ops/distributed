use crate::contracts::ContractArtifactKind;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use super::{
    digest_bytes, validate_content_identity, validate_stable_value, LifecycleError, LifecycleGraph,
    LifecycleNode,
};

pub const NODE_RECEIPT_SCHEMA_VERSION: u32 = 1;
pub const GENERATION_MANIFEST_SCHEMA_VERSION: u32 = 1;
const MAX_RECEIPT_IDENTITIES: usize = 8192;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactNodeReceipt {
    pub schema_version: u32,
    pub node_id: String,
    pub kind: ContractArtifactKind,
    pub executor_identity: String,
    pub input_identities: BTreeMap<String, String>,
    pub dependency_receipts: BTreeMap<String, String>,
    pub output_identities: BTreeMap<String, String>,
    pub ready: bool,
    pub receipt_id: String,
}

impl ArtifactNodeReceipt {
    pub fn new(
        node: &LifecycleNode,
        executor_identity: impl Into<String>,
        input_identities: BTreeMap<String, String>,
        dependency_receipts: BTreeMap<String, String>,
        output_identities: BTreeMap<String, String>,
        ready: bool,
    ) -> Result<Self, LifecycleError> {
        let mut receipt = Self {
            schema_version: NODE_RECEIPT_SCHEMA_VERSION,
            node_id: node.id.clone(),
            kind: node.kind,
            executor_identity: executor_identity.into(),
            input_identities,
            dependency_receipts,
            output_identities,
            ready,
            receipt_id: String::new(),
        };
        receipt.validate_against(node)?;
        receipt.receipt_id = receipt.expected_id()?;
        receipt.validate_against(node)?;
        Ok(receipt)
    }

    pub fn validate_against(&self, node: &LifecycleNode) -> Result<(), LifecycleError> {
        if self.schema_version != NODE_RECEIPT_SCHEMA_VERSION {
            return Err(LifecycleError::new(
                "unsupported artifact receipt schema version",
            ));
        }
        if self.node_id != node.id || self.kind != node.kind {
            return Err(LifecycleError::new(format!(
                "receipt `{}` does not describe lifecycle node `{}`",
                self.node_id, node.id
            )));
        }
        validate_content_identity(&self.executor_identity, "receipt executor identity")?;
        validate_identity_map(&self.input_identities, "receipt input")?;
        validate_identity_map(&self.dependency_receipts, "dependency receipt")?;
        validate_identity_map(&self.output_identities, "receipt output")?;
        if self.input_identities.len()
            + self.dependency_receipts.len()
            + self.output_identities.len()
            > MAX_RECEIPT_IDENTITIES
        {
            return Err(LifecycleError::new(
                "artifact receipt exceeds identity bounds",
            ));
        }
        let dependency_ids = self
            .dependency_receipts
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        if dependency_ids != node.dependencies {
            return Err(LifecycleError::new(format!(
                "receipt `{}` dependency set differs from its lifecycle node",
                self.node_id
            )));
        }
        let output_paths = self
            .output_identities
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        if output_paths != node.outputs {
            return Err(LifecycleError::new(format!(
                "receipt `{}` output set differs from its lifecycle node",
                self.node_id
            )));
        }
        if !self.receipt_id.is_empty() && self.receipt_id != self.expected_id()? {
            return Err(LifecycleError::new(format!(
                "receipt `{}` identity is stale",
                self.node_id
            )));
        }
        Ok(())
    }

    fn expected_id(&self) -> Result<String, LifecycleError> {
        #[derive(Serialize)]
        struct IdentityView<'a> {
            schema_version: u32,
            node_id: &'a str,
            kind: ContractArtifactKind,
            executor_identity: &'a str,
            input_identities: &'a BTreeMap<String, String>,
            dependency_receipts: &'a BTreeMap<String, String>,
            output_identities: &'a BTreeMap<String, String>,
            ready: bool,
        }
        let bytes = serde_json::to_vec(&IdentityView {
            schema_version: self.schema_version,
            node_id: &self.node_id,
            kind: self.kind,
            executor_identity: &self.executor_identity,
            input_identities: &self.input_identities,
            dependency_receipts: &self.dependency_receipts,
            output_identities: &self.output_identities,
            ready: self.ready,
        })
        .map_err(|error| LifecycleError::new(error.to_string()))?;
        Ok(digest_bytes(&bytes))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationManifest {
    pub schema_version: u32,
    pub application: String,
    pub lifecycle_graph_id: String,
    pub source_identity: String,
    pub receipts: BTreeMap<String, ArtifactNodeReceipt>,
    pub generation_id: String,
}

impl GenerationManifest {
    pub fn new(
        graph: &LifecycleGraph,
        receipts: impl IntoIterator<Item = ArtifactNodeReceipt>,
    ) -> Result<Self, LifecycleError> {
        graph.validate()?;
        let receipts = receipts
            .into_iter()
            .map(|receipt| (receipt.node_id.clone(), receipt))
            .collect::<BTreeMap<_, _>>();
        let mut manifest = Self {
            schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
            application: graph.application.clone(),
            lifecycle_graph_id: graph.graph_id.clone(),
            source_identity: graph.source_identity.clone(),
            receipts,
            generation_id: String::new(),
        };
        manifest.validate_against(graph)?;
        manifest.generation_id = manifest.expected_id()?;
        manifest.validate_against(graph)?;
        Ok(manifest)
    }

    pub fn validate_against(&self, graph: &LifecycleGraph) -> Result<(), LifecycleError> {
        if self.schema_version != GENERATION_MANIFEST_SCHEMA_VERSION {
            return Err(LifecycleError::new(
                "unsupported generation manifest schema version",
            ));
        }
        if self.application != graph.application
            || self.lifecycle_graph_id != graph.graph_id
            || self.source_identity != graph.source_identity
        {
            return Err(LifecycleError::new(
                "generation manifest does not match its lifecycle graph",
            ));
        }
        let expected_nodes = graph.nodes.keys().cloned().collect::<BTreeSet<_>>();
        let receipt_nodes = self.receipts.keys().cloned().collect::<BTreeSet<_>>();
        if receipt_nodes != expected_nodes {
            return Err(LifecycleError::new(
                "generation manifest receipt set is incomplete or contains unknown nodes",
            ));
        }
        for (id, receipt) in &self.receipts {
            receipt.validate_against(&graph.nodes[id])?;
            if !receipt.ready {
                return Err(LifecycleError::new(format!(
                    "generation node `{id}` is not ready"
                )));
            }
            for (dependency, observed_receipt) in &receipt.dependency_receipts {
                let expected_receipt = &self.receipts[dependency].receipt_id;
                if observed_receipt != expected_receipt {
                    return Err(LifecycleError::new(format!(
                        "generation node `{id}` has stale dependency receipt `{dependency}`"
                    )));
                }
            }
        }
        if !self.generation_id.is_empty() && self.generation_id != self.expected_id()? {
            return Err(LifecycleError::new(
                "generation identity is stale relative to its receipts",
            ));
        }
        Ok(())
    }

    pub fn canonical_bytes(&self, graph: &LifecycleGraph) -> Result<Vec<u8>, LifecycleError> {
        self.validate_against(graph)?;
        serde_json::to_vec(self).map_err(|error| LifecycleError::new(error.to_string()))
    }

    fn expected_id(&self) -> Result<String, LifecycleError> {
        #[derive(Serialize)]
        struct IdentityView<'a> {
            schema_version: u32,
            application: &'a str,
            lifecycle_graph_id: &'a str,
            source_identity: &'a str,
            receipts: &'a BTreeMap<String, ArtifactNodeReceipt>,
        }
        let bytes = serde_json::to_vec(&IdentityView {
            schema_version: self.schema_version,
            application: &self.application,
            lifecycle_graph_id: &self.lifecycle_graph_id,
            source_identity: &self.source_identity,
            receipts: &self.receipts,
        })
        .map_err(|error| LifecycleError::new(error.to_string()))?;
        Ok(digest_bytes(&bytes))
    }
}

fn validate_identity_map(
    identities: &BTreeMap<String, String>,
    label: &str,
) -> Result<(), LifecycleError> {
    for (name, identity) in identities {
        validate_stable_value(name, &format!("{label} name"))?;
        validate_content_identity(identity, &format!("{label} identity"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{
        ArtifactIdentity, ArtifactPredecessor, ArtifactProvenance, ContractArtifactKind,
        ContractCatalog, ContractEntry, ContractScope, CONTRACT_CATALOG_SCHEMA_VERSION,
    };
    use crate::lifecycle::{
        DistributedSourceIdentity, LifecycleConfig, LifecycleGraph, LIFECYCLE_CONFIG_SCHEMA_VERSION,
    };

    fn graph() -> LifecycleGraph {
        let app = ContractEntry {
            id: "application".into(),
            kind: ContractArtifactKind::ApplicationManifest,
            scope: ContractScope { id: "app".into() },
            owner: "owner/app".into(),
            identity: ArtifactIdentity::new(
                ContractArtifactKind::ApplicationManifest,
                "ref:application",
            ),
            provenance: ArtifactProvenance {
                sources: ["src/lib.rs".into()].into_iter().collect(),
                generator: "generator.application".into(),
                glob_limit: None,
            },
            predecessor: None,
            outputs: [("manifest".into(), "generated/application.json".into())]
                .into_iter()
                .collect(),
            lifecycle: ["build".into()].into_iter().collect(),
        };
        let plan = ContractEntry {
            id: "plan".into(),
            kind: ContractArtifactKind::DeploymentPlan,
            scope: ContractScope { id: "plan".into() },
            owner: "owner/plan".into(),
            identity: ArtifactIdentity::new(ContractArtifactKind::DeploymentPlan, "ref:plan"),
            provenance: ArtifactProvenance {
                sources: ["generated/application.json".into()].into_iter().collect(),
                generator: "generator.plan".into(),
                glob_limit: None,
            },
            predecessor: Some(ArtifactPredecessor {
                entry_id: "application".into(),
                identity: ArtifactIdentity::new(
                    ContractArtifactKind::ApplicationManifest,
                    "ref:application",
                ),
            }),
            outputs: [("plan".into(), "generated/plan.json".into())]
                .into_iter()
                .collect(),
            lifecycle: ["build".into()].into_iter().collect(),
        };
        let catalog = ContractCatalog {
            schema_version: CONTRACT_CATALOG_SCHEMA_VERSION,
            entries: [app, plan]
                .into_iter()
                .map(|entry| (entry.id.clone(), entry))
                .collect(),
        };
        LifecycleGraph::from_catalog(
            &catalog,
            &LifecycleConfig {
                schema_version: LIFECYCLE_CONFIG_SCHEMA_VERSION,
                application: "fixture".into(),
                source: {
                    let source = digest_bytes(b"fixture-distributed-source");
                    DistributedSourceIdentity {
                        rust: source.clone(),
                        cli: source.clone(),
                        javascript: source,
                    }
                },
                roots: ["plan".into()].into_iter().collect(),
            },
        )
        .unwrap()
    }

    fn receipt(
        graph: &LifecycleGraph,
        node_id: &str,
        dependencies: BTreeMap<String, String>,
        ready: bool,
    ) -> ArtifactNodeReceipt {
        let node = &graph.nodes[node_id];
        ArtifactNodeReceipt::new(
            node,
            digest_bytes(format!("tool:{node_id}:1").as_bytes()),
            node.inputs
                .iter()
                .map(|path| {
                    (
                        path.clone(),
                        digest_bytes(format!("{node_id}-input").as_bytes()),
                    )
                })
                .collect(),
            dependencies,
            node.outputs
                .iter()
                .map(|path| {
                    (
                        path.clone(),
                        digest_bytes(format!("{node_id}-output").as_bytes()),
                    )
                })
                .collect(),
            ready,
        )
        .unwrap()
    }

    #[test]
    fn generation_is_byte_deterministic_and_links_exact_receipts() {
        let graph = graph();
        let application = receipt(&graph, "application", BTreeMap::new(), true);
        let plan = receipt(
            &graph,
            "plan",
            [("application".into(), application.receipt_id.clone())]
                .into_iter()
                .collect(),
            true,
        );
        let first = GenerationManifest::new(&graph, [plan.clone(), application.clone()]).unwrap();
        let second = GenerationManifest::new(&graph, [application, plan]).unwrap();
        assert_eq!(first.generation_id, second.generation_id);
        assert_eq!(
            first.canonical_bytes(&graph).unwrap(),
            second.canonical_bytes(&graph).unwrap()
        );
    }

    #[test]
    fn incomplete_unready_and_stale_dependency_receipts_fail_closed() {
        let graph = graph();
        let application = receipt(&graph, "application", BTreeMap::new(), true);
        assert!(GenerationManifest::new(&graph, [application.clone()])
            .unwrap_err()
            .message()
            .contains("incomplete"));

        let unready = receipt(&graph, "application", BTreeMap::new(), false);
        let plan = receipt(
            &graph,
            "plan",
            [("application".into(), unready.receipt_id.clone())]
                .into_iter()
                .collect(),
            true,
        );
        assert!(GenerationManifest::new(&graph, [unready, plan])
            .unwrap_err()
            .message()
            .contains("not ready"));

        let stale_plan = receipt(
            &graph,
            "plan",
            [("application".into(), digest_bytes(b"stale"))]
                .into_iter()
                .collect(),
            true,
        );
        assert!(GenerationManifest::new(&graph, [application, stale_plan])
            .unwrap_err()
            .message()
            .contains("stale dependency"));
    }
}
