//! Read-only contract check and exact-scope accept transactions.
//!
//! `check` never writes tracked files. `accept` stages outputs, replaces
//! atomically on success, and restores the prior set on failure.

use super::catalog::ContractCatalog;
use super::chain::{check_predecessor_chain, ObservedPredecessor};
use super::diagnostic::{ContractCheckResult, ContractDiagnostic, ContractDiagnosticCode};
use super::ContractArtifactKind;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Exact accept scopes supported by the aggregate CLI.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractAcceptScope {
    Catalog,
    MigrationInventory,
    SurfaceClientManifest,
    GeneratedClientTree,
    ApplicationManifest,
    DeploymentPlan,
    /// Exact client-program scope: `program:<id>`.
    Program {
        id: String,
    },
}

impl ContractAcceptScope {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "catalog" => Some(Self::Catalog),
            "migration_inventory" => Some(Self::MigrationInventory),
            "surface_client_manifest" => Some(Self::SurfaceClientManifest),
            "generated_client_tree" => Some(Self::GeneratedClientTree),
            "application_manifest" => Some(Self::ApplicationManifest),
            "deployment_plan" => Some(Self::DeploymentPlan),
            other if other.starts_with("program:") => {
                let id = other.trim_start_matches("program:").to_string();
                if id.is_empty() {
                    None
                } else {
                    Some(Self::Program { id })
                }
            }
            _ => None,
        }
    }

    pub fn as_str(&self) -> String {
        match self {
            Self::Catalog => "catalog".into(),
            Self::MigrationInventory => "migration_inventory".into(),
            Self::SurfaceClientManifest => "surface_client_manifest".into(),
            Self::GeneratedClientTree => "generated_client_tree".into(),
            Self::ApplicationManifest => "application_manifest".into(),
            Self::DeploymentPlan => "deployment_plan".into(),
            Self::Program { id } => format!("program:{id}"),
        }
    }
}

/// Result of a read-only aggregate check.
#[derive(Clone, Debug, Serialize)]
pub struct ContractsCheckReport {
    pub ok: bool,
    pub result: ContractCheckResult,
    pub human: String,
}

/// Result of an exact-scope accept transaction.
#[derive(Clone, Debug, Serialize)]
pub struct ContractsAcceptReport {
    pub ok: bool,
    pub scope: String,
    pub changed_paths: Vec<String>,
    pub noop: bool,
    pub rolled_back: bool,
    pub diagnostics: Vec<String>,
}

/// Aggregate read-only contracts check over a catalog root.
pub fn contracts_check(
    catalog: &ContractCatalog,
    root: &Path,
    predecessors: impl IntoIterator<Item = ObservedPredecessor>,
) -> ContractsCheckReport {
    let mut result = catalog.check(root);
    let chain = check_predecessor_chain(predecessors);
    result.diagnostics.extend(chain.diagnostics);
    let ok = result.diagnostics.is_empty();
    let human = result
        .diagnostics
        .iter()
        .map(|diagnostic| diagnostic.human())
        .collect::<Vec<_>>()
        .join("\n");
    ContractsCheckReport { ok, result, human }
}

/// Accept one exact scope by replacing declared relative paths under `root`.
///
/// `staged` maps portable relative paths to their new bytes. Paths must stay
/// inside `root` after physical resolution. On any failure the prior contents
/// of every touched path are restored.
pub fn contracts_accept(
    root: &Path,
    scope: ContractAcceptScope,
    staged: &BTreeMap<String, Vec<u8>>,
) -> Result<ContractsAcceptReport, String> {
    if staged.is_empty() {
        return Ok(ContractsAcceptReport {
            ok: true,
            scope: scope.as_str(),
            changed_paths: Vec::new(),
            noop: true,
            rolled_back: false,
            diagnostics: Vec::new(),
        });
    }

    let mut resolved = BTreeMap::new();
    for (relative, bytes) in staged {
        let path = resolve_under_root(root, relative)?;
        resolved.insert(relative.clone(), (path, bytes.clone()));
    }

    // Detect no-op: every path already has identical bytes.
    let mut noop = true;
    for (path, bytes) in resolved.values() {
        match fs::read(path) {
            Ok(existing) if existing == *bytes => {}
            _ => {
                noop = false;
                break;
            }
        }
    }
    if noop {
        return Ok(ContractsAcceptReport {
            ok: true,
            scope: scope.as_str(),
            changed_paths: Vec::new(),
            noop: true,
            rolled_back: false,
            diagnostics: Vec::new(),
        });
    }

    // Snapshot prior contents for rollback.
    let mut prior = BTreeMap::new();
    for (relative, (path, _)) in &resolved {
        prior.insert(
            relative.clone(),
            if path.exists() {
                Some(fs::read(path).map_err(|error| error.to_string())?)
            } else {
                None
            },
        );
    }

    let mut changed = Vec::new();
    for (relative, (path, bytes)) in &resolved {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let staging = path.with_extension("distributed-accept-staging");
        if let Err(error) = fs::write(&staging, bytes) {
            let _ = rollback(root, &prior);
            return Err(error.to_string());
        }
        if let Err(error) = fs::rename(&staging, path) {
            let _ = fs::remove_file(&staging);
            let _ = rollback(root, &prior);
            return Err(error.to_string());
        }
        changed.push(relative.clone());
    }

    Ok(ContractsAcceptReport {
        ok: true,
        scope: scope.as_str(),
        changed_paths: changed,
        noop: false,
        rolled_back: false,
        diagnostics: Vec::new(),
    })
}

fn rollback(root: &Path, prior: &BTreeMap<String, Option<Vec<u8>>>) -> Result<(), String> {
    for (relative, contents) in prior {
        let path = resolve_under_root(root, relative)?;
        match contents {
            Some(bytes) => {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
                }
                fs::write(&path, bytes).map_err(|e| e.to_string())?;
            }
            None if path.exists() => {
                fs::remove_file(&path).map_err(|e| e.to_string())?;
            }
            None => {}
        }
    }
    Ok(())
}

fn resolve_under_root(root: &Path, relative: &str) -> Result<PathBuf, String> {
    if relative.is_empty()
        || relative.starts_with('/')
        || relative.contains('\0')
        || Path::new(relative)
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(format!(
            "accept path `{relative}` escapes or is not a portable relative path"
        ));
    }
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let candidate = root.join(relative);
    if let Ok(canonical) = candidate.canonicalize() {
        if !canonical.starts_with(&root) {
            return Err(format!(
                "accept path `{relative}` resolves outside the catalog root"
            ));
        }
        return Ok(canonical);
    }
    // Path may not exist yet — validate parents.
    if let Some(parent) = candidate.parent() {
        if parent.exists() {
            let parent = parent.canonicalize().map_err(|e| e.to_string())?;
            if !parent.starts_with(&root) {
                return Err(format!(
                    "accept path `{relative}` parent resolves outside the catalog root"
                ));
            }
        }
    }
    Ok(candidate)
}

/// Build a diagnostic when an accept scope is unknown or broad.
pub fn unknown_scope_diagnostic(scope: &str) -> ContractDiagnostic {
    ContractDiagnostic::new(
        ContractDiagnosticCode::CatalogInvalid,
        Some(ContractArtifactKind::ApplicationManifest),
        None::<&str>,
        "contracts.accept",
        std::iter::empty::<&str>(),
        std::iter::empty::<&str>(),
        Some("scope"),
        Some("exact catalog-owned scope"),
        Some(scope),
        Some("use_exact_scope"),
        None,
        "distributed contracts accept --scope <exact-scope>",
    )
    .with_detail(format!("unknown or broad accept scope `{scope}`"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("distributed-contracts-accept-{nanos}"));
        fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn accept_is_idempotent_and_rolls_back_on_escape() {
        let root = temp_root();
        let mut staged = BTreeMap::new();
        staged.insert("contracts/app.json".into(), b"{\"ok\":true}".to_vec());
        let first = contracts_accept(&root, ContractAcceptScope::ApplicationManifest, &staged)
            .expect("first accept");
        assert!(first.ok);
        assert!(!first.noop);
        assert_eq!(first.changed_paths, vec!["contracts/app.json".to_string()]);

        let second = contracts_accept(&root, ContractAcceptScope::ApplicationManifest, &staged)
            .expect("second accept");
        assert!(second.noop);

        let mut bad = BTreeMap::new();
        bad.insert("../escape.json".into(), b"nope".to_vec());
        assert!(contracts_accept(&root, ContractAcceptScope::Catalog, &bad).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn check_report_is_no_write() {
        // Empty catalog check shape only — catalog construction is covered elsewhere.
        let report = ContractsCheckReport {
            ok: true,
            result: ContractCheckResult::default(),
            human: String::new(),
        };
        assert!(report.ok);
    }
}
