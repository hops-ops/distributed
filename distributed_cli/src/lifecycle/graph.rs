use crate::contracts::{ContractArtifactKind, ContractCatalog};
use glob::{MatchOptions, Pattern};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use super::{
    digest_bytes, validate_content_identity, validate_portable_path, validate_stable_value,
};

pub const LIFECYCLE_CONFIG_SCHEMA_VERSION: u32 = 1;
pub const LIFECYCLE_GRAPH_SCHEMA_VERSION: u32 = 1;
pub const MAX_LIFECYCLE_NODES: usize = 256;
const MAX_LIFECYCLE_ROOTS: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleError {
    message: String,
    reason: LifecycleErrorReason,
    diagnostic: Option<serde_json::Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LifecycleErrorReason {
    Other,
    Canceled,
    Superseded,
}

impl LifecycleError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            reason: LifecycleErrorReason::Other,
            diagnostic: None,
        }
    }

    pub(crate) fn canceled(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            reason: LifecycleErrorReason::Canceled,
            diagnostic: None,
        }
    }

    pub(crate) fn superseded(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            reason: LifecycleErrorReason::Superseded,
            diagnostic: None,
        }
    }

    pub(crate) fn with_diagnostic(mut self, diagnostic: serde_json::Value) -> Self {
        self.diagnostic = Some(diagnostic);
        self
    }

    pub(crate) fn reason(&self) -> LifecycleErrorReason {
        self.reason
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// Stable structured diagnostic supplied by errors that expose one.
    pub fn diagnostic(&self) -> Option<&serde_json::Value> {
        self.diagnostic.as_ref()
    }
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for LifecycleError {}

/// The one Distributed source identity selected for a lifecycle.
///
/// The fields name independently resolved consumers so diagnostics can expose
/// cross-checkout drift. A coherent lifecycle requires all three values to be
/// exactly equal before any graph node executes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DistributedSourceIdentity {
    pub rust: String,
    pub cli: String,
    pub javascript: String,
}

impl DistributedSourceIdentity {
    pub fn validate(&self) -> Result<(), LifecycleError> {
        validate_content_identity(&self.rust, "Distributed Rust source identity")?;
        validate_content_identity(&self.cli, "Distributed CLI source identity")?;
        validate_content_identity(&self.javascript, "Distributed JavaScript source identity")?;
        if self.rust != self.cli || self.rust != self.javascript {
            return Err(LifecycleError::new(format!(
                "mixed Distributed source identities: rust=`{}`, cli=`{}`, javascript=`{}`",
                self.rust, self.cli, self.javascript
            )));
        }
        Ok(())
    }

    pub fn resolved(&self) -> Result<&str, LifecycleError> {
        self.validate()?;
        Ok(&self.rust)
    }
}

/// Small lifecycle selection. Semantic artifact membership stays in the
/// contract catalog; this config only selects final catalog entry IDs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleConfig {
    pub schema_version: u32,
    pub application: String,
    pub source: DistributedSourceIdentity,
    pub roots: BTreeSet<String>,
}

impl LifecycleConfig {
    pub fn validate(&self) -> Result<(), LifecycleError> {
        if self.schema_version != LIFECYCLE_CONFIG_SCHEMA_VERSION {
            return Err(LifecycleError::new(format!(
                "unsupported lifecycle config schema version {}; expected {}",
                self.schema_version, LIFECYCLE_CONFIG_SCHEMA_VERSION
            )));
        }
        validate_stable_value(&self.application, "application identity")?;
        self.source.validate()?;
        if self.roots.is_empty() || self.roots.len() > MAX_LIFECYCLE_ROOTS {
            return Err(LifecycleError::new(format!(
                "lifecycle roots must contain 1..={MAX_LIFECYCLE_ROOTS} catalog entries"
            )));
        }
        for root in &self.roots {
            validate_stable_value(root, "lifecycle root")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleNode {
    pub id: String,
    pub owner: String,
    pub kind: ContractArtifactKind,
    pub inputs: BTreeSet<String>,
    pub outputs: BTreeSet<String>,
    pub dependencies: BTreeSet<String>,
    pub executor: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleGraph {
    pub schema_version: u32,
    pub application: String,
    pub source_identity: String,
    pub roots: BTreeSet<String>,
    pub nodes: BTreeMap<String, LifecycleNode>,
    pub graph_id: String,
}

impl LifecycleGraph {
    pub fn from_catalog(
        catalog: &ContractCatalog,
        config: &LifecycleConfig,
    ) -> Result<Self, LifecycleError> {
        config.validate()?;
        catalog
            .canonical_bytes()
            .map_err(|error| LifecycleError::new(error.to_string()))?;

        for root in &config.roots {
            if !catalog.entries.contains_key(root) {
                return Err(LifecycleError::new(format!(
                    "lifecycle root `{root}` is not present in the contract catalog"
                )));
            }
        }

        let all_dependencies = catalog_dependencies(catalog)?;
        let mut selected = BTreeSet::new();
        let mut pending = config.roots.iter().cloned().collect::<Vec<_>>();
        while let Some(id) = pending.pop() {
            if !selected.insert(id.clone()) {
                continue;
            }
            let dependencies = all_dependencies.get(&id).ok_or_else(|| {
                LifecycleError::new(format!("catalog entry `{id}` has no graph node"))
            })?;
            pending.extend(dependencies.iter().cloned());
            if selected.len() > MAX_LIFECYCLE_NODES {
                return Err(LifecycleError::new(format!(
                    "lifecycle graph exceeds max node count {MAX_LIFECYCLE_NODES}"
                )));
            }
        }

        let mut nodes = BTreeMap::new();
        for id in &selected {
            let entry = &catalog.entries[id];
            let dependencies = all_dependencies[id]
                .intersection(&selected)
                .cloned()
                .collect();
            nodes.insert(
                id.clone(),
                LifecycleNode {
                    id: id.clone(),
                    owner: entry.owner.clone(),
                    kind: entry.kind,
                    inputs: entry.provenance.sources.clone(),
                    outputs: entry.outputs.values().cloned().collect(),
                    dependencies,
                    executor: entry.provenance.generator.clone(),
                },
            );
        }

        let mut graph = Self {
            schema_version: LIFECYCLE_GRAPH_SCHEMA_VERSION,
            application: config.application.clone(),
            source_identity: config.source.resolved()?.to_string(),
            roots: config.roots.clone(),
            nodes,
            graph_id: String::new(),
        };
        graph.validate()?;
        graph.graph_id = graph.expected_id()?;
        graph.validate()?;
        Ok(graph)
    }

    pub fn validate(&self) -> Result<(), LifecycleError> {
        if self.schema_version != LIFECYCLE_GRAPH_SCHEMA_VERSION {
            return Err(LifecycleError::new(
                "unsupported lifecycle graph schema version",
            ));
        }
        validate_stable_value(&self.application, "application identity")?;
        validate_content_identity(&self.source_identity, "Distributed source identity")?;
        if self.nodes.is_empty() || self.nodes.len() > MAX_LIFECYCLE_NODES {
            return Err(LifecycleError::new(format!(
                "lifecycle graph must contain 1..={MAX_LIFECYCLE_NODES} nodes"
            )));
        }
        for root in &self.roots {
            if !self.nodes.contains_key(root) {
                return Err(LifecycleError::new(format!(
                    "lifecycle graph is missing selected root `{root}`"
                )));
            }
        }
        for (id, node) in &self.nodes {
            if node.id != *id {
                return Err(LifecycleError::new(format!(
                    "lifecycle node key `{id}` does not match node ID `{}`",
                    node.id
                )));
            }
            validate_stable_value(&node.owner, "lifecycle node owner")?;
            validate_stable_value(&node.executor, "lifecycle executor key")?;
            if node.inputs.is_empty() || node.outputs.is_empty() {
                return Err(LifecycleError::new(format!(
                    "lifecycle node `{id}` must declare inputs and outputs"
                )));
            }
            for input in &node.inputs {
                validate_portable_path(input, "lifecycle input")?;
            }
            for output in &node.outputs {
                validate_portable_path(output, "lifecycle output")?;
            }
            for dependency in &node.dependencies {
                if dependency == id {
                    return Err(LifecycleError::new(format!(
                        "lifecycle node `{id}` depends on itself"
                    )));
                }
                if !self.nodes.contains_key(dependency) {
                    return Err(LifecycleError::new(format!(
                        "lifecycle node `{id}` references missing dependency `{dependency}`"
                    )));
                }
            }
        }
        self.topological_order()?;
        if !self.graph_id.is_empty() && self.graph_id != self.expected_id()? {
            return Err(LifecycleError::new(
                "lifecycle graph identity is stale relative to its nodes",
            ));
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, LifecycleError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| LifecycleError::new(error.to_string()))
    }

    pub fn topological_order(&self) -> Result<Vec<String>, LifecycleError> {
        let mut remaining = self
            .nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.dependencies.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut order = Vec::with_capacity(remaining.len());
        while !remaining.is_empty() {
            let ready = remaining
                .iter()
                .filter(|(_, dependencies)| dependencies.is_empty())
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            if ready.is_empty() {
                return Err(LifecycleError::new(format!(
                    "lifecycle dependency cycle contains: {}",
                    remaining.keys().cloned().collect::<Vec<_>>().join(", ")
                )));
            }
            for id in ready {
                remaining.remove(&id);
                for dependencies in remaining.values_mut() {
                    dependencies.remove(&id);
                }
                order.push(id);
            }
        }
        Ok(order)
    }

    /// Map content-changed relative paths to their owning nodes and complete
    /// downstream invalidation closure. Callers compare content identities;
    /// modification time is never an input to this API.
    pub fn invalidated_by_paths(
        &self,
        changed_paths: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<BTreeSet<String>, LifecycleError> {
        self.validate()?;
        let mut invalidated = BTreeSet::new();
        for path in changed_paths {
            let path = path.as_ref();
            validate_portable_path(path, "changed lifecycle input")?;
            for (id, node) in &self.nodes {
                let matches = node.inputs.iter().try_fold(false, |matched, input| {
                    if matched {
                        Ok(true)
                    } else {
                        pattern_matches(input, path)
                    }
                })?;
                if matches {
                    invalidated.insert(id.clone());
                }
            }
        }
        let mut changed = true;
        while changed {
            changed = false;
            for (id, node) in &self.nodes {
                if !invalidated.contains(id)
                    && node
                        .dependencies
                        .iter()
                        .any(|dependency| invalidated.contains(dependency))
                {
                    invalidated.insert(id.clone());
                    changed = true;
                }
            }
        }
        Ok(invalidated)
    }

    fn expected_id(&self) -> Result<String, LifecycleError> {
        #[derive(Serialize)]
        struct IdentityView<'a> {
            schema_version: u32,
            application: &'a str,
            source_identity: &'a str,
            roots: &'a BTreeSet<String>,
            nodes: &'a BTreeMap<String, LifecycleNode>,
        }
        let bytes = serde_json::to_vec(&IdentityView {
            schema_version: self.schema_version,
            application: &self.application,
            source_identity: &self.source_identity,
            roots: &self.roots,
            nodes: &self.nodes,
        })
        .map_err(|error| LifecycleError::new(error.to_string()))?;
        Ok(digest_bytes(&bytes))
    }
}

fn catalog_dependencies(
    catalog: &ContractCatalog,
) -> Result<BTreeMap<String, BTreeSet<String>>, LifecycleError> {
    let mut owners = BTreeMap::<String, String>::new();
    for (id, entry) in &catalog.entries {
        for output in entry.outputs.values() {
            if let Some(previous) = owners.insert(output.clone(), id.clone()) {
                return Err(LifecycleError::new(format!(
                    "output `{output}` is owned by both `{previous}` and `{id}`"
                )));
            }
        }
    }

    let mut dependencies = BTreeMap::new();
    for (id, entry) in &catalog.entries {
        let mut node_dependencies = BTreeSet::new();
        if let Some(predecessor) = &entry.predecessor {
            node_dependencies.insert(predecessor.entry_id.clone());
        }
        for source in &entry.provenance.sources {
            for (output, owner) in &owners {
                if owner != id && source_uses_output(source, output) {
                    node_dependencies.insert(owner.clone());
                }
            }
        }
        dependencies.insert(id.clone(), node_dependencies);
    }
    Ok(dependencies)
}

pub(crate) fn source_uses_output(source: &str, output: &str) -> bool {
    let stable_prefix = source
        .split(['*', '?', '['])
        .next()
        .unwrap_or(source)
        .trim_end_matches('/');
    stable_prefix == output
        || stable_prefix.starts_with(&format!("{output}/"))
        || pattern_matches(source, output).unwrap_or(false)
}

fn pattern_matches(pattern: &str, path: &str) -> Result<bool, LifecycleError> {
    if !pattern.contains(['*', '?', '[']) {
        return Ok(pattern == path);
    }
    let pattern = Pattern::new(pattern)
        .map_err(|error| LifecycleError::new(format!("invalid lifecycle glob: {error}")))?;
    Ok(pattern.matches_path_with(
        Path::new(path),
        MatchOptions {
            case_sensitive: true,
            require_literal_separator: true,
            require_literal_leading_dot: true,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{
        ArtifactIdentity, ArtifactPredecessor, ArtifactProvenance, ContractEntry, ContractScope,
        CONTRACT_CATALOG_SCHEMA_VERSION,
    };

    fn entry(
        id: &str,
        kind: ContractArtifactKind,
        sources: &[&str],
        output: &str,
        predecessor: Option<(&str, ContractArtifactKind)>,
    ) -> ContractEntry {
        ContractEntry {
            id: id.into(),
            kind,
            scope: ContractScope {
                id: format!("scope/{id}"),
            },
            owner: format!("owner/{id}"),
            identity: ArtifactIdentity::new(kind, format!("ref:{id}")),
            provenance: ArtifactProvenance {
                sources: sources.iter().map(|source| (*source).into()).collect(),
                generator: format!("generator.{id}"),
                glob_limit: sources
                    .iter()
                    .any(|source| source.contains('*'))
                    .then_some(32),
            },
            predecessor: predecessor.map(|(entry_id, predecessor_kind)| ArtifactPredecessor {
                entry_id: entry_id.into(),
                identity: ArtifactIdentity::new(predecessor_kind, format!("ref:{entry_id}")),
            }),
            outputs: BTreeMap::from([(format!("{id}-artifact"), output.into())]),
            lifecycle: ["build".into(), "dev".into()].into_iter().collect(),
        }
    }

    fn fixture_catalog() -> ContractCatalog {
        let application = entry(
            "application",
            ContractArtifactKind::ApplicationManifest,
            &["src/*.rs"],
            "generated/application.json",
            None,
        );
        let plan = entry(
            "plan",
            ContractArtifactKind::DeploymentPlan,
            &["generated/application.json", "distributed.plan.json"],
            "generated/plan.json",
            Some(("application", ContractArtifactKind::ApplicationManifest)),
        );
        let client = entry(
            "client",
            ContractArtifactKind::GeneratedClientTree,
            &["generated/plan.json", "ui/src/*.graphql"],
            "ui/src/generated",
            Some(("plan", ContractArtifactKind::DeploymentPlan)),
        );
        ContractCatalog {
            schema_version: CONTRACT_CATALOG_SCHEMA_VERSION,
            entries: [application, plan, client]
                .into_iter()
                .map(|entry| (entry.id.clone(), entry))
                .collect(),
        }
    }

    fn config() -> LifecycleConfig {
        let source = digest_bytes(b"fixture-distributed-source");
        LifecycleConfig {
            schema_version: LIFECYCLE_CONFIG_SCHEMA_VERSION,
            application: "fixture".into(),
            source: DistributedSourceIdentity {
                rust: source.clone(),
                cli: source.clone(),
                javascript: source,
            },
            roots: ["client".into()].into_iter().collect(),
        }
    }

    #[test]
    fn graph_is_deterministic_and_invalidates_exact_downstream_closure() {
        let catalog = fixture_catalog();
        let first = LifecycleGraph::from_catalog(&catalog, &config()).unwrap();
        let second = LifecycleGraph::from_catalog(&catalog, &config()).unwrap();
        assert_eq!(
            first.canonical_bytes().unwrap(),
            second.canonical_bytes().unwrap()
        );
        assert_eq!(
            first.topological_order().unwrap(),
            ["application", "plan", "client"]
        );
        assert_eq!(
            first.invalidated_by_paths(["src/todo.rs"]).unwrap(),
            ["application", "client", "plan"]
                .into_iter()
                .map(String::from)
                .collect()
        );
        assert_eq!(
            first.invalidated_by_paths(["ui/src/todo.graphql"]).unwrap(),
            ["client"].into_iter().map(String::from).collect()
        );
        assert!(first
            .invalidated_by_paths(["README.md"])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn mixed_sources_and_missing_roots_fail_before_graph_execution() {
        let catalog = fixture_catalog();
        let mut mixed = config();
        mixed.source.javascript = digest_bytes(b"different-distributed-source");
        let error = LifecycleGraph::from_catalog(&catalog, &mixed).unwrap_err();
        assert!(error.message().contains(&mixed.source.rust));
        assert!(error.message().contains(&mixed.source.javascript));

        let mut missing = config();
        missing.roots = ["missing".into()].into_iter().collect();
        assert!(LifecycleGraph::from_catalog(&catalog, &missing)
            .unwrap_err()
            .message()
            .contains("not present"));
    }

    #[test]
    fn inferred_output_cycle_fails_deterministically() {
        let left = entry(
            "left",
            ContractArtifactKind::ApplicationManifest,
            &["generated/right.json"],
            "generated/left.json",
            None,
        );
        let right = entry(
            "right",
            ContractArtifactKind::DeploymentPlan,
            &["generated/left.json"],
            "generated/right.json",
            None,
        );
        let catalog = ContractCatalog {
            schema_version: CONTRACT_CATALOG_SCHEMA_VERSION,
            entries: [left, right]
                .into_iter()
                .map(|entry| (entry.id.clone(), entry))
                .collect(),
        };
        let mut lifecycle = config();
        lifecycle.roots = ["right".into()].into_iter().collect();
        let error = LifecycleGraph::from_catalog(&catalog, &lifecycle).unwrap_err();
        assert!(error.message().contains("cycle contains: left, right"));
    }

    #[test]
    fn glob_source_infers_dependency_on_matching_owned_output() {
        let producer = entry(
            "producer",
            ContractArtifactKind::ApplicationManifest,
            &["src/*.rs"],
            "generated/plan.json",
            None,
        );
        let consumer = entry(
            "consumer",
            ContractArtifactKind::DeploymentPlan,
            &["generated/*.json"],
            "generated/release.json",
            None,
        );
        let catalog = ContractCatalog {
            schema_version: CONTRACT_CATALOG_SCHEMA_VERSION,
            entries: [producer, consumer]
                .into_iter()
                .map(|entry| (entry.id.clone(), entry))
                .collect(),
        };
        let mut lifecycle = config();
        lifecycle.roots = ["consumer".into()].into_iter().collect();

        let graph = LifecycleGraph::from_catalog(&catalog, &lifecycle).unwrap();
        assert_eq!(
            graph.nodes["consumer"].dependencies,
            ["producer".into()].into_iter().collect()
        );
        assert_eq!(graph.topological_order().unwrap(), ["producer", "consumer"]);
    }
}
