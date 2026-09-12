use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use glob::Pattern;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::contracts::{
    ArtifactIdentity, ArtifactPredecessor, ArtifactProvenance, ClientInventory,
    ContractArtifactKind, ContractCatalog, ContractEntry, ContractScope,
    CONTRACT_CATALOG_SCHEMA_VERSION, MAX_CATALOG_GLOB_MATCHES,
};
use crate::js_framework::{
    discover_javascript_framework, JavascriptFrameworkPackage, JavascriptPackageSource,
};

use super::{
    digest_bytes, DistributedSourceIdentity, LifecycleBuildConfig, LifecycleDevConfig,
    LifecycleDevProbe, LifecycleDevProcess, LifecycleError, LifecycleExecutor,
    LifecycleProjectPlan, LIFECYCLE_BUILD_CONFIG_SCHEMA_VERSION, LIFECYCLE_CLI_EXECUTABLE_ENV,
};

const APPLICATION_NODE: &str = "application";
const APPLICATION_OUTPUT: &str = "artifacts/application-manifest.json";
const CLIENT_NODE: &str = "clients";
const CLIENT_CONFIG: &str = "distributed.config.js";
const COLD_RUNTIME_READY_TIMEOUT_MS: u64 = 300_000;

/// Cargo- and convention-derived inputs for one Distributed application.
#[derive(Clone, Debug)]
pub struct DiscoveredLifecycleProject {
    /// Stable project name derived from the Cargo workspace directory.
    pub name: String,
    /// Package exporting the typed `distributed::ApplicationManifest`.
    pub application_package: String,
    /// Fully-qualified zero-argument Rust export used for typed introspection.
    pub application_entrypoint: String,
    /// Resolved source directory of the `distributed` Rust dependency.
    pub distributed_root: PathBuf,
    /// Cargo package containing the local runtime binary.
    pub runtime_package: String,
    /// Binary started by `distributed dev` and compiled by `distributed build`.
    pub runtime_binary: String,
    /// Canonical Cargo workspace root. This can be narrower than the lifecycle
    /// root when the application owns a repository-local sibling UI.
    pub(crate) cargo_root: PathBuf,
    /// Canonical SvelteKit root, discovered conventionally or from Cargo
    /// workspace metadata.
    pub ui: Option<PathBuf>,
    /// Resolved Distributed browser runtime, when the UI consumes it.
    pub(crate) javascript: Option<JavascriptFrameworkPackage>,
    /// Internal lifecycle graph synthesized from the discovered project.
    pub plan: LifecycleProjectPlan,
}

#[derive(Debug, Deserialize)]
struct CargoMetadata {
    workspace_root: PathBuf,
    workspace_members: Vec<String>,
    packages: Vec<CargoPackage>,
    #[serde(default)]
    metadata: Value,
}

#[derive(Debug, Deserialize)]
struct CargoPackage {
    id: String,
    name: String,
    version: String,
    manifest_path: PathBuf,
    source: Option<String>,
    #[serde(default)]
    metadata: Value,
    targets: Vec<CargoTarget>,
}

#[derive(Debug, Deserialize)]
struct CargoTarget {
    name: String,
    kind: Vec<String>,
}

#[derive(Debug)]
struct ApplicationTarget {
    package: String,
    crate_name: String,
    entrypoint: String,
}

#[derive(Debug)]
struct RuntimeTarget {
    package: String,
    binary: String,
}

#[derive(Debug)]
struct ClientLifecycle {
    config: PathBuf,
    sources: BTreeSet<String>,
    outputs: BTreeMap<String, String>,
}

/// Discover a Distributed application without a user-authored lifecycle graph.
pub fn discover_lifecycle_project(
    requested: impl AsRef<Path>,
    executable: impl AsRef<Path>,
    command_prefix: &[&str],
    out: Option<&Path>,
) -> Result<DiscoveredLifecycleProject, LifecycleError> {
    let requested = requested.as_ref();
    let requested = requested.canonicalize().map_err(|error| {
        LifecycleError::new(format!(
            "failed to resolve Distributed project `{}`: {error}",
            requested.display()
        ))
    })?;
    if !requested.is_dir() {
        return Err(LifecycleError::new(format!(
            "Distributed project `{}` is not a directory",
            requested.display()
        )));
    }
    let manifest = requested.join("Cargo.toml");
    if !manifest.is_file() {
        return Err(LifecycleError::new(format!(
            "Distributed project `{}` has no Cargo.toml",
            requested.display()
        )));
    }

    let metadata = cargo_metadata(&manifest)?;
    let root = metadata.workspace_root.canonicalize().map_err(|error| {
        LifecycleError::new(format!("failed to resolve Cargo workspace root: {error}"))
    })?;
    if root != requested {
        return Err(LifecycleError::new(format!(
            "project path `{}` resolves to Cargo workspace `{}`; run the command from the workspace root",
            requested.display(),
            root.display()
        )));
    }
    let workspace_ids = metadata
        .workspace_members
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let workspace_packages = metadata
        .packages
        .iter()
        .filter(|package| workspace_ids.contains(package.id.as_str()))
        .collect::<Vec<_>>();
    let application = discover_application(&metadata.metadata, &workspace_packages)?;
    let runtime = discover_runtime(&metadata.metadata, &workspace_packages)?;
    let distributed = metadata
        .packages
        .iter()
        .find(|package| package.name == "distributed")
        .ok_or_else(|| {
            LifecycleError::new("Cargo workspace does not depend on the `distributed` Rust package")
        })?;
    let distributed_root = distributed
        .manifest_path
        .parent()
        .ok_or_else(|| LifecycleError::new("Distributed Cargo.toml has no parent directory"))?
        .to_path_buf();
    let executable = executable.as_ref().canonicalize().map_err(|error| {
        LifecycleError::new(format!(
            "failed to resolve the distributed executable: {error}"
        ))
    })?;
    let project_name = root
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(&application.package)
        .to_string();
    let ui = discover_ui(&metadata.metadata, &root)?;
    let lifecycle_root = lifecycle_root(&root, ui.as_deref())?;
    let javascript = ui
        .as_ref()
        .map(|ui| discover_javascript_framework(ui))
        .transpose()?
        .flatten();
    let mut sources = workspace_sources(&lifecycle_root, &root, &workspace_packages)?;
    let mut client_lifecycle = discover_client_lifecycle(&lifecycle_root, ui.as_deref())?;
    if let Some(receipt) = javascript
        .as_ref()
        .and_then(|package| package.lifecycle_receipt(&root))
    {
        let receipt = receipt.strip_prefix(&lifecycle_root).map_err(|_| {
            LifecycleError::new("JavaScript lifecycle receipt escapes the project root")
        })?;
        if let Some(client) = &mut client_lifecycle {
            client.sources.insert(portable_path(receipt)?);
        } else {
            sources.insert(portable_path(receipt)?);
        }
    }
    let identities = framework_identities(
        distributed,
        &distributed_root,
        &executable,
        javascript.as_ref(),
    )?;
    let executable_identity = fs::read(&executable)
        .map(|bytes| digest_bytes(&bytes))
        .map_err(|error| {
            LifecycleError::new(format!(
                "failed to read distributed executable `{}`: {error}",
                executable.display()
            ))
        })?;
    let executor_identity = digest_bytes(
        format!(
            "distributed-describe-v1\0{}\0{}\0{}",
            executable_identity, application.package, application.entrypoint
        )
        .as_bytes(),
    );
    let entrypoint = if application.entrypoint.contains("::") {
        application.entrypoint.clone()
    } else {
        format!("{}::{}", application.crate_name, application.entrypoint)
    };

    let application_identity = ArtifactIdentity::new(
        ContractArtifactKind::ApplicationManifest,
        format!("ref:application/{project_name}"),
    );
    let application_entry = ContractEntry {
        id: APPLICATION_NODE.to_string(),
        kind: ContractArtifactKind::ApplicationManifest,
        scope: ContractScope {
            id: format!("application/{project_name}"),
        },
        owner: format!("{project_name}/application"),
        identity: application_identity.clone(),
        provenance: ArtifactProvenance {
            sources,
            generator: "distributed.application-manifest".to_string(),
            source_revision: None,
            glob_limit: None,
        },
        predecessor: None,
        outputs: BTreeMap::from([("manifest".to_string(), APPLICATION_OUTPUT.to_string())]),
        lifecycle: BTreeSet::from(["build".to_string(), "check".to_string(), "dev".to_string()]),
        environment_policy: None,
    };
    let mut entries = BTreeMap::from([(APPLICATION_NODE.to_string(), application_entry)]);
    if let Some(client) = &client_lifecycle {
        let mut client_sources = client.sources.clone();
        client_sources.insert(APPLICATION_OUTPUT.to_string());
        entries.insert(
            CLIENT_NODE.to_string(),
            ContractEntry {
                id: CLIENT_NODE.to_string(),
                kind: ContractArtifactKind::GeneratedClientTree,
                scope: ContractScope {
                    id: format!("application/{project_name}/clients"),
                },
                owner: format!("{project_name}/clients"),
                identity: ArtifactIdentity::new(
                    ContractArtifactKind::GeneratedClientTree,
                    format!("ref:application/{project_name}/clients"),
                ),
                provenance: ArtifactProvenance {
                    sources: client_sources,
                    generator: "distributed.sveltekit-clients".to_string(),
                    source_revision: None,
                    glob_limit: Some(MAX_CATALOG_GLOB_MATCHES),
                },
                predecessor: Some(ArtifactPredecessor {
                    entry_id: APPLICATION_NODE.to_string(),
                    identity: application_identity,
                }),
                outputs: client.outputs.clone(),
                lifecycle: BTreeSet::from([
                    "build".to_string(),
                    "check".to_string(),
                    "dev".to_string(),
                ]),
                environment_policy: None,
            },
        );
    }
    let catalog = ContractCatalog {
        schema_version: CONTRACT_CATALOG_SCHEMA_VERSION,
        entries,
    };

    let dev = lifecycle_dev(
        &lifecycle_root,
        &root,
        &runtime,
        ui.as_deref(),
        javascript.as_ref(),
        client_lifecycle.is_some(),
        &executable,
        command_prefix,
    );
    let cargo_relative = root.strip_prefix(&lifecycle_root).map_err(|_| {
        LifecycleError::new("Cargo workspace escapes the application lifecycle root")
    })?;
    let cargo_manifest = if cargo_relative.as_os_str().is_empty() {
        "{root}/Cargo.toml".to_string()
    } else {
        format!("{{root}}/{}/Cargo.toml", portable_path(cargo_relative)?)
    };
    let mut describe_args = command_prefix
        .iter()
        .map(|arg| (*arg).to_string())
        .chain([
            "describe".to_string(),
            "--manifest-path".to_string(),
            cargo_manifest,
            "--package".to_string(),
            application.package.clone(),
            "--entrypoint".to_string(),
            entrypoint.clone(),
            "--distributed-path".to_string(),
            distributed_root.to_string_lossy().into_owned(),
        ])
        .collect::<Vec<_>>();
    if let Some(wasm_pack_launcher) = javascript
        .as_ref()
        .zip(ui.as_ref())
        .map(|(javascript, ui)| javascript.wasm_pack_launcher(ui))
    {
        describe_args.extend([
            "--wasm-pack-launcher".to_string(),
            wasm_pack_launcher.to_string_lossy().into_owned(),
        ]);
    }
    let mut executors = BTreeMap::from([(
        "distributed.application-manifest".to_string(),
        LifecycleExecutor {
            identity: executor_identity,
            program: executable.to_string_lossy().into_owned(),
            args: describe_args,
            stdout: Some(APPLICATION_OUTPUT.to_string()),
        },
    )]);
    if let Some(client) = &client_lifecycle {
        let javascript = javascript.as_ref().ok_or_else(|| {
            LifecycleError::new(
                "distributed.clients.json requires @hops-ops/distributed in the UI package",
            )
        })?;
        let compiler = ui
            .as_ref()
            .expect("client lifecycle has a UI")
            .join("node_modules/@hops-ops/distributed/dist/sveltekit/lifecycle-compiler.js");
        let identity = digest_bytes(
            format!(
                "distributed-sveltekit-lifecycle-v1\0{}\0{}",
                identities.javascript.identity, javascript.version
            )
            .as_bytes(),
        );
        executors.insert(
            "distributed.sveltekit-clients".to_string(),
            LifecycleExecutor {
                identity,
                program: "node".to_string(),
                args: vec![
                    compiler.to_string_lossy().into_owned(),
                    client.config.to_string_lossy().into_owned(),
                ],
                stdout: None,
            },
        );
    }
    let config = LifecycleBuildConfig {
        schema_version: LIFECYCLE_BUILD_CONFIG_SCHEMA_VERSION,
        application: project_name.clone(),
        source: DistributedSourceIdentity {
            rust: identities.rust.identity,
            cli: identities.cli.identity,
            javascript: identities.javascript.identity,
        },
        roots: BTreeSet::from([if client_lifecycle.is_some() {
            CLIENT_NODE.to_string()
        } else {
            APPLICATION_NODE.to_string()
        }]),
        executors,
        dev: Some(dev),
    };
    config.validate()?;
    catalog
        .canonical_bytes()
        .map_err(|error| LifecycleError::new(error.to_string()))?;

    Ok(DiscoveredLifecycleProject {
        name: project_name,
        application_package: application.package,
        application_entrypoint: entrypoint,
        distributed_root,
        runtime_package: runtime.package,
        runtime_binary: runtime.binary,
        cargo_root: root.clone(),
        ui,
        javascript,
        plan: LifecycleProjectPlan {
            root: lifecycle_root,
            catalog,
            config,
            out: root.join(out.unwrap_or_else(|| Path::new(".distributed/lifecycle"))),
            cli_executable: Some(executable.clone()),
        },
    })
}

fn cargo_metadata(manifest: &Path) -> Result<CargoMetadata, LifecycleError> {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--manifest-path"])
        .arg(manifest)
        .output()
        .map_err(|error| LifecycleError::new(format!("failed to run `cargo metadata`: {error}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(LifecycleError::new(format!(
            "`cargo metadata` failed for `{}`: {}",
            manifest.display(),
            stderr.trim()
        )));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|error| LifecycleError::new(format!("failed to parse Cargo metadata: {error}")))
}

pub(crate) fn discover_ui(
    workspace_metadata: &Value,
    root: &Path,
) -> Result<Option<PathBuf>, LifecycleError> {
    let declared = match workspace_metadata.pointer("/distributed/ui") {
        None => None,
        Some(ui) => Some(ui.get("path").and_then(Value::as_str).ok_or_else(|| {
            LifecycleError::new("workspace Distributed UI metadata requires a string `path`")
        })?),
    };
    let candidate = match declared {
        Some(path) => {
            if path.is_empty()
                || path.trim() != path
                || Path::new(path).is_absolute()
                || path.contains('\0')
            {
                return Err(LifecycleError::new(
                    "workspace Distributed UI path must be a non-empty relative path",
                ));
            }
            root.join(path)
        }
        None => {
            let conventional = root.join("ui");
            if !conventional.join("package.json").is_file() {
                return Ok(None);
            }
            conventional
        }
    };
    let ui = candidate.canonicalize().map_err(|error| {
        LifecycleError::new(format!(
            "failed to resolve Distributed UI `{}`: {error}",
            candidate.display()
        ))
    })?;
    if !ui.join("package.json").is_file() {
        return Err(LifecycleError::new(format!(
            "Distributed UI `{}` has no package.json",
            ui.display()
        )));
    }
    if !ui.starts_with(root) {
        let repository = root
            .ancestors()
            .find(|ancestor| ancestor.join(".git").exists())
            .ok_or_else(|| {
                LifecycleError::new(
                    "an external Distributed UI requires the Cargo workspace to be inside a Git repository",
                )
            })?;
        if !ui.starts_with(repository) {
            return Err(LifecycleError::new(format!(
                "Distributed UI `{}` resolves outside repository `{}`",
                ui.display(),
                repository.display()
            )));
        }
    }
    Ok(Some(ui))
}

fn discover_application(
    workspace_metadata: &Value,
    packages: &[&CargoPackage],
) -> Result<ApplicationTarget, LifecycleError> {
    if let (Some(package), Some(entrypoint)) = (
        workspace_metadata
            .pointer("/distributed/application/package")
            .and_then(Value::as_str),
        workspace_metadata
            .pointer("/distributed/application/entrypoint")
            .and_then(Value::as_str),
    ) {
        let package = packages
            .iter()
            .find(|candidate| candidate.name == package)
            .ok_or_else(|| {
                LifecycleError::new(
                    "workspace Distributed application package is not a workspace member",
                )
            })?;
        return application_target(package, entrypoint);
    }

    let marked = packages
        .iter()
        .filter_map(|package| {
            package
                .metadata
                .pointer("/distributed/application/entrypoint")
                .and_then(Value::as_str)
                .map(|entrypoint| (*package, entrypoint))
        })
        .collect::<Vec<_>>();
    if marked.len() == 1 {
        return application_target(marked[0].0, marked[0].1);
    }
    if marked.len() > 1 {
        return Err(LifecycleError::new(
            "Cargo metadata marks more than one Distributed application target",
        ));
    }

    let conventional = packages
        .iter()
        .filter(|package| package.name.ends_with("-service"))
        .filter(|package| {
            package
                .targets
                .iter()
                .any(|target| target.kind.iter().any(|kind| kind == "lib"))
        })
        .copied()
        .collect::<Vec<_>>();
    if conventional.len() == 1 {
        return application_target(conventional[0], "application_manifest");
    }
    Err(LifecycleError::new(
        "unable to locate one typed Distributed application target in Cargo metadata; expose `application_manifest` from the application crate or mark its package metadata",
    ))
}

fn application_target(
    package: &CargoPackage,
    entrypoint: &str,
) -> Result<ApplicationTarget, LifecycleError> {
    let crate_name = package
        .targets
        .iter()
        .find(|target| target.kind.iter().any(|kind| kind == "lib"))
        .map(|target| target.name.clone())
        .ok_or_else(|| {
            LifecycleError::new(format!(
                "Distributed application package `{}` has no library target",
                package.name
            ))
        })?;
    Ok(ApplicationTarget {
        package: package.name.clone(),
        crate_name,
        entrypoint: entrypoint.to_string(),
    })
}

fn discover_runtime(
    workspace_metadata: &Value,
    packages: &[&CargoPackage],
) -> Result<RuntimeTarget, LifecycleError> {
    if let (Some(package), Some(binary)) = (
        workspace_metadata
            .pointer("/distributed/runtime/package")
            .and_then(Value::as_str),
        workspace_metadata
            .pointer("/distributed/runtime/binary")
            .and_then(Value::as_str),
    ) {
        return runtime_target(packages, package, binary);
    }
    let marked = packages
        .iter()
        .filter_map(|package| {
            package
                .metadata
                .pointer("/distributed/runtime/binary")
                .and_then(Value::as_str)
                .map(|binary| (package.name.as_str(), binary))
        })
        .collect::<Vec<_>>();
    if marked.len() == 1 {
        return runtime_target(packages, marked[0].0, marked[0].1);
    }
    if marked.len() > 1 {
        return Err(LifecycleError::new(
            "Cargo metadata marks more than one Distributed runtime target",
        ));
    }
    let binaries = packages
        .iter()
        .flat_map(|package| {
            package
                .targets
                .iter()
                .filter(|target| target.kind.iter().any(|kind| kind == "bin"))
                .filter(|target| !target.name.ends_with("-manifest"))
                .map(|target| (package.name.as_str(), target.name.as_str()))
        })
        .collect::<Vec<_>>();
    if binaries.len() == 1 {
        return runtime_target(packages, binaries[0].0, binaries[0].1);
    }
    Err(LifecycleError::new(
        "unable to locate one Distributed runtime binary in Cargo metadata",
    ))
}

fn runtime_target(
    packages: &[&CargoPackage],
    package_name: &str,
    binary: &str,
) -> Result<RuntimeTarget, LifecycleError> {
    let package = packages
        .iter()
        .find(|package| package.name == package_name)
        .ok_or_else(|| {
            LifecycleError::new("Distributed runtime package is not a workspace member")
        })?;
    if !package
        .targets
        .iter()
        .any(|target| target.name == binary && target.kind.iter().any(|kind| kind == "bin"))
    {
        return Err(LifecycleError::new(format!(
            "Distributed runtime package `{package_name}` has no `{binary}` binary target"
        )));
    }
    Ok(RuntimeTarget {
        package: package_name.to_string(),
        binary: binary.to_string(),
    })
}

fn workspace_sources(
    lifecycle_root: &Path,
    cargo_root: &Path,
    packages: &[&CargoPackage],
) -> Result<BTreeSet<String>, LifecycleError> {
    let cargo_manifest = cargo_root.join("Cargo.toml");
    let cargo_manifest = cargo_manifest
        .strip_prefix(lifecycle_root)
        .map_err(|_| LifecycleError::new("Cargo workspace manifest escapes the lifecycle root"))?;
    let mut sources = BTreeSet::from([portable_path(cargo_manifest)?]);
    let cargo_lock = cargo_root.join("Cargo.lock");
    if cargo_lock.is_file() {
        let cargo_lock = cargo_lock.strip_prefix(lifecycle_root).map_err(|_| {
            LifecycleError::new("Cargo workspace lockfile escapes the lifecycle root")
        })?;
        sources.insert(portable_path(cargo_lock)?);
    }
    for package in packages {
        let manifest = package
            .manifest_path
            .strip_prefix(lifecycle_root)
            .map_err(|_| {
                LifecycleError::new(format!(
                    "workspace package `{}` resolves outside the Cargo workspace",
                    package.name
                ))
            })?;
        sources.insert(portable_path(manifest)?);
        let source = package
            .manifest_path
            .parent()
            .expect("Cargo manifest has a parent")
            .join("src");
        if source.is_dir() {
            let source = source.strip_prefix(lifecycle_root).map_err(|_| {
                LifecycleError::new("workspace source resolves outside the Cargo workspace")
            })?;
            sources.insert(portable_path(source)?);
        }
    }
    Ok(sources)
}

fn lifecycle_root(cargo_root: &Path, ui: Option<&Path>) -> Result<PathBuf, LifecycleError> {
    let Some(ui) = ui else {
        return Ok(cargo_root.to_path_buf());
    };
    if ui.starts_with(cargo_root) {
        return Ok(cargo_root.to_path_buf());
    }
    cargo_root
        .ancestors()
        .find(|ancestor| ancestor.join(".git").exists() && ui.starts_with(ancestor))
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            LifecycleError::new(
                "repository-local UI and Cargo workspace have no shared lifecycle root",
            )
        })
}

fn discover_client_lifecycle(
    root: &Path,
    ui: Option<&Path>,
) -> Result<Option<ClientLifecycle>, LifecycleError> {
    let Some(ui) = ui else {
        return Ok(None);
    };
    let inventory_path = ui.join("distributed.clients.json");
    if !inventory_path.is_file() {
        return Ok(None);
    }
    let ui_relative = ui.strip_prefix(root).map_err(|_| {
        LifecycleError::new("Distributed UI escapes the application lifecycle root")
    })?;
    let config = ui.join(CLIENT_CONFIG);
    if !config.is_file() {
        return Err(LifecycleError::new(format!(
            "{} requires sibling {CLIENT_CONFIG} exporting distributedViteOptions",
            inventory_path.display()
        )));
    }
    let inventory = ClientInventory::from_path(&inventory_path)
        .map_err(|error| LifecycleError::new(error.to_string()))?;
    let ui_prefix = if ui_relative.as_os_str().is_empty() {
        String::new()
    } else {
        portable_path(ui_relative)?
    };
    let rooted = |path: &str| {
        if ui_prefix.is_empty() {
            path.to_string()
        } else {
            format!("{ui_prefix}/{path}")
        }
    };
    let mut sources = BTreeSet::from([rooted("distributed.clients.json"), rooted(CLIENT_CONFIG)]);
    for source in ["src/routes", "src/lib"] {
        if ui.join(source).is_dir() {
            sources.insert(rooted(source));
        }
    }
    let mut outputs = BTreeMap::new();
    for (index, client) in inventory.clients.into_iter().enumerate() {
        let source_tree_output = ui.join(&client.output);
        if source_tree_output.exists() {
            return Err(LifecycleError::new(format!(
                "lifecycle-owned client output `{}` still exists in the application source tree; remove it and run `distributed build` or `distributed dev`",
                source_tree_output.display()
            )));
        }
        outputs.insert(format!("client-{index}"), rooted(&client.output));
        outputs.insert(
            format!("boundary-plan-{index}"),
            rooted(&format!(
                ".svelte-kit/distributed/clients/{}",
                URL_SAFE_NO_PAD.encode(client.module.as_bytes())
            )),
        );
        for document in client.documents {
            if ["src/routes/", "src/lib/"]
                .iter()
                .any(|prefix| document.starts_with(prefix) && ui.join(prefix).is_dir())
            {
                // The bounded source tree already captures the document, its
                // optional binding sidecar, Svelte ownership, and additions.
                continue;
            }
            let document_source = rooted(&document);
            if ui.join(&document).is_file() {
                // The document is guaranteed to match this pattern, while an
                // optional `.bindings.js` sibling is included without making
                // that sidecar mandatory. Escaping preserves bracketed
                // SvelteKit route segments as literal path components.
                sources.insert(format!("{}*", Pattern::escape(&document_source)));
            } else {
                // Client declarations may place their wildcard in a directory
                // component, while lifecycle catalog globs deliberately may
                // only vary the final component. Hash the bounded stable
                // directory so document and sidecar additions are observed.
                let stable = document.split(['*', '?', '[']).next().unwrap_or(&document);
                let directory = if stable.ends_with('/') {
                    stable.trim_end_matches('/')
                } else {
                    Path::new(stable)
                        .parent()
                        .and_then(Path::to_str)
                        .unwrap_or_default()
                };
                if directory.is_empty() {
                    return Err(LifecycleError::new(format!(
                        "client document pattern `{document}` has no bounded source directory"
                    )));
                }
                sources.insert(rooted(directory));
            }
        }
    }
    Ok(Some(ClientLifecycle {
        config,
        sources,
        outputs,
    }))
}

fn portable_path(path: &Path) -> Result<String, LifecycleError> {
    let value = path
        .components()
        .map(|component| component.as_os_str().to_str())
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| LifecycleError::new("project path is not valid UTF-8"))?
        .join("/");
    if value.is_empty() {
        return Err(LifecycleError::new("project path must not be empty"));
    }
    Ok(value)
}

#[derive(Clone, Debug)]
struct FrameworkComponentIdentity {
    identity: String,
    description: String,
}

#[derive(Clone, Debug)]
struct FrameworkIdentities {
    rust: FrameworkComponentIdentity,
    cli: FrameworkComponentIdentity,
    javascript: FrameworkComponentIdentity,
}

fn framework_identities(
    distributed: &CargoPackage,
    distributed_root: &Path,
    executable: &Path,
    javascript: Option<&JavascriptFrameworkPackage>,
) -> Result<FrameworkIdentities, LifecycleError> {
    let rust = match distributed.source.as_deref() {
        None => checkout_component("rust", &distributed.version, distributed_root)?,
        Some(source) if source.starts_with("registry+") => {
            release_component("rust", &distributed.version)
        }
        Some(source) => external_component("rust", &distributed.version, source),
    };
    let cli_version = env!("CARGO_PKG_VERSION");
    let cli = match cli_checkout_root(executable) {
        Some(root) => checkout_component("cli", cli_version, &root)?,
        None => release_component("cli", cli_version),
    };
    let expected_javascript_root = distributed_root
        .join("js")
        .canonicalize()
        .unwrap_or_else(|_| distributed_root.join("js"));
    let javascript = match javascript {
        None => rust.clone(),
        Some(package) => match &package.source {
            JavascriptPackageSource::Registry => release_component("javascript", &package.version),
            JavascriptPackageSource::Local { root } if root == &expected_javascript_root => {
                checkout_component("javascript", &package.version, distributed_root)?
            }
            JavascriptPackageSource::Local { root } => {
                checkout_component("javascript", &package.version, root)?
            }
        },
    };
    let identities = FrameworkIdentities {
        rust,
        cli,
        javascript,
    };
    ensure_framework_compatibility(
        &identities,
        &distributed.version,
        distributed.source.is_none().then_some(distributed_root),
    )?;
    Ok(identities)
}

fn cli_checkout_root(executable: &Path) -> Option<PathBuf> {
    executable.ancestors().find_map(|root| {
        root.join("distributed_cli")
            .join("Cargo.toml")
            .is_file()
            .then(|| root.to_path_buf())
    })
}

fn ensure_framework_compatibility(
    identities: &FrameworkIdentities,
    rust_version: &str,
    local_root: Option<&Path>,
) -> Result<(), LifecycleError> {
    if identities.rust.identity == identities.cli.identity
        && identities.rust.identity == identities.javascript.identity
    {
        return Ok(());
    }
    let repair = local_root.map_or_else(
        || format!(
            "Install the matching CLI with `cargo install distributed_cli --version {rust_version} --locked --force`."
        ),
        |root| format!(
            "Run this checkout's CLI with `cargo run --manifest-path \"{}/Cargo.toml\" -p distributed_cli --bin distributed -- <build|dev>`.",
            root.display()
        ),
    );
    let message = format!(
        "incompatible Distributed framework members:\n  {}\n  {}\n  {}\nRust, CLI, and @hops-ops/distributed are released together. Use the same published version for all three, or resolve every local member from one checkout. {repair}",
        identities.rust.description,
        identities.cli.description,
        identities.javascript.description,
    );
    let diagnostic = serde_json::json!({
        "schema_version": 1,
        "ok": false,
        "error": {
            "code": "CTL-FRAMEWORK-IDENTITY-MISMATCH",
            "expected": {
                "component": "rust",
                "identity": identities.rust.identity.as_str(),
                "description": identities.rust.description.as_str(),
            },
            "observed": {
                "rust": {
                    "identity": identities.rust.identity.as_str(),
                    "description": identities.rust.description.as_str(),
                },
                "cli": {
                    "identity": identities.cli.identity.as_str(),
                    "description": identities.cli.description.as_str(),
                },
                "javascript": {
                    "identity": identities.javascript.identity.as_str(),
                    "description": identities.javascript.description.as_str(),
                },
            },
            "affected_components": ["rust", "cli", "javascript"],
            "repair": repair,
        },
    });
    Err(LifecycleError::new(message).with_diagnostic(diagnostic))
}

fn release_component(label: &str, version: &str) -> FrameworkComponentIdentity {
    FrameworkComponentIdentity {
        identity: digest_bytes(format!("distributed-release\0{version}").as_bytes()),
        description: format!("{label}: published version={version}"),
    }
}

fn checkout_component(
    label: &str,
    version: &str,
    root: &Path,
) -> Result<FrameworkComponentIdentity, LifecycleError> {
    let root = root.canonicalize().map_err(|error| {
        LifecycleError::new(format!(
            "failed to resolve Distributed {label} checkout `{}`: {error}",
            root.display()
        ))
    })?;
    Ok(FrameworkComponentIdentity {
        identity: digest_bytes(
            format!("distributed-checkout\0{}", root.to_string_lossy()).as_bytes(),
        ),
        description: format!(
            "{label}: local version={version} checkout={}",
            root.display()
        ),
    })
}

fn external_component(label: &str, version: &str, source: &str) -> FrameworkComponentIdentity {
    FrameworkComponentIdentity {
        identity: digest_bytes(format!("distributed-external\0{version}\0{source}").as_bytes()),
        description: format!("{label}: external version={version} source={source}"),
    }
}

fn lifecycle_dev(
    root: &Path,
    cargo_root: &Path,
    runtime: &RuntimeTarget,
    ui: Option<&Path>,
    javascript: Option<&JavascriptFrameworkPackage>,
    lifecycle_owns_client_compile: bool,
    executable: &Path,
    command_prefix: &[&str],
) -> LifecycleDevConfig {
    let bind = std::env::var("BIND").unwrap_or_else(|_| "127.0.0.1:8791".to_string());
    let api_address = loopback_address(&bind);
    let api_url = format!("http://{api_address}");
    let ui_host = std::env::var("UI_HOST").unwrap_or_else(|_| "localhost".to_string());
    let ui_bind = std::env::var("UI_BIND").unwrap_or_else(|_| ui_host.clone());
    let ui_port = std::env::var("UI_PORT").unwrap_or_else(|_| "5180".to_string());
    let ui_url = std::env::var("PUBLIC_ORIGIN")
        .or_else(|_| std::env::var("UI_URL"))
        .or_else(|_| std::env::var("AUTH_URL"))
        .unwrap_or_else(|_| format!("http://{ui_host}:{ui_port}"));
    let cargo_cwd = cargo_root
        .strip_prefix(root)
        .ok()
        .filter(|relative| !relative.as_os_str().is_empty())
        .map(|relative| portable_path(relative).expect("validated Cargo workspace path"));
    let mut processes = BTreeMap::from([(
        "api".to_string(),
        LifecycleDevProcess {
            program: "cargo".to_string(),
            args: vec![
                "run".to_string(),
                "--package".to_string(),
                runtime.package.clone(),
                "--bin".to_string(),
                runtime.binary.clone(),
            ],
            cwd: cargo_cwd,
            external_cwd: false,
            env: BTreeMap::from([
                ("BIND".to_string(), bind.clone()),
                ("BIND_ADDR".to_string(), bind),
            ]),
            url: Some(api_url.clone()),
            restart_on: BTreeSet::from([APPLICATION_NODE.to_string()]),
            ready_after_ms: 100,
            ready: Some(LifecycleDevProbe {
                program: executable.to_string_lossy().into_owned(),
                args: probe_args(command_prefix, api_address),
                interval_ms: 250,
                // `cargo run` may populate an empty target directory in a fresh
                // worktree or development container. A real process failure is
                // observed immediately; this budget only permits valid cold
                // compilation to reach the readiness probe.
                timeout_ms: COLD_RUNTIME_READY_TIMEOUT_MS,
            }),
        },
    )]);
    if let Some(ui) = ui {
        let (cwd, external_cwd) = match ui.strip_prefix(root) {
            Ok(relative) if relative.as_os_str().is_empty() => (None, false),
            Ok(relative) => (
                Some(portable_path(relative).expect("validated UI path")),
                false,
            ),
            Err(_) => (Some(ui.to_string_lossy().into_owned()), true),
        };
        let mut ui_env = BTreeMap::from([
            ("DISTRIBUTED_API_ORIGIN".to_string(), api_url.clone()),
            ("E2E_API_ORIGIN".to_string(), api_url.clone()),
            (
                "AUTH_SECRET".to_string(),
                std::env::var("AUTH_SECRET")
                    .unwrap_or_else(|_| "distributed-local-development-only-secret".to_string()),
            ),
            ("AUTH_URL".to_string(), ui_url.clone()),
            ("AUTH_USE_SECURE_COOKIES".to_string(), "false".to_string()),
            (
                LIFECYCLE_CLI_EXECUTABLE_ENV.to_string(),
                executable.to_string_lossy().into_owned(),
            ),
        ]);
        if lifecycle_owns_client_compile {
            ui_env.insert(
                "DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE".to_string(),
                "1".to_string(),
            );
            ui_env.insert(
                "DISTRIBUTED_LIFECYCLE_PROJECT_ROOT".to_string(),
                root.to_string_lossy().into_owned(),
            );
        }
        processes.insert(
            "ui".to_string(),
            LifecycleDevProcess {
                program: "npm".to_string(),
                args: vec![
                    "run".to_string(),
                    "dev".to_string(),
                    "--".to_string(),
                    "--host".to_string(),
                    ui_bind.clone(),
                    "--port".to_string(),
                    ui_port.clone(),
                ],
                cwd,
                external_cwd,
                env: ui_env,
                url: Some(ui_url.clone()),
                // The lifecycle atomically activates generated client files and
                // the browser lifecycle owns the one document reload. Restarting
                // Vite before activation disconnects its HMR socket and can race
                // that controlled reload with an uncoordinated browser retry.
                restart_on: BTreeSet::new(),
                ready_after_ms: 100,
                ready: Some(LifecycleDevProbe {
                    program: executable.to_string_lossy().into_owned(),
                    args: probe_args(
                        command_prefix,
                        loopback_address(&socket_address(&ui_bind, &ui_port)),
                    ),
                    interval_ms: 250,
                    timeout_ms: 60_000,
                }),
            },
        );
    }
    if let Some(package_root) = javascript.and_then(JavascriptFrameworkPackage::local_root) {
        let args = command_prefix
            .iter()
            .map(|arg| (*arg).to_string())
            .chain([
                "__js-watch".to_string(),
                "--package-root".to_string(),
                package_root.to_string_lossy().into_owned(),
                "--project-root".to_string(),
                cargo_root.to_string_lossy().into_owned(),
            ])
            .collect();
        processes.insert(
            "framework-js".to_string(),
            LifecycleDevProcess {
                program: executable.to_string_lossy().into_owned(),
                args,
                cwd: None,
                external_cwd: false,
                env: BTreeMap::from([(
                    LIFECYCLE_CLI_EXECUTABLE_ENV.to_string(),
                    executable.to_string_lossy().into_owned(),
                )]),
                url: None,
                restart_on: BTreeSet::new(),
                ready_after_ms: 100,
                ready: None,
            },
        );
    }
    LifecycleDevConfig {
        poll_ms: 500,
        debounce_ms: 250,
        shutdown_ms: 5_000,
        prepare_ms: 30_000,
        processes,
    }
}

fn probe_args(command_prefix: &[&str], address: String) -> Vec<String> {
    command_prefix
        .iter()
        .map(|arg| (*arg).to_string())
        .chain(["__probe".to_string(), "--address".to_string(), address])
        .collect()
}

fn loopback_address(bind: &str) -> String {
    let Some((host, port)) = bind.rsplit_once(':') else {
        return bind.to_string();
    };
    let host = match host.trim_matches(['[', ']']) {
        "0.0.0.0" | "::" => "127.0.0.1",
        host => host,
    };
    socket_address(host, port)
}

fn socket_address(host: &str, port: &str) -> String {
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn package(name: &str, metadata: Value, targets: &[(&str, &str)]) -> CargoPackage {
        CargoPackage {
            id: format!("path+file:///fixture#{name}@0.1.0"),
            name: name.to_string(),
            version: "0.1.0".to_string(),
            manifest_path: PathBuf::from(format!("/fixture/{name}/Cargo.toml")),
            source: None,
            metadata,
            targets: targets
                .iter()
                .map(|(name, kind)| CargoTarget {
                    name: (*name).to_string(),
                    kind: vec![(*kind).to_string()],
                })
                .collect(),
        }
    }

    #[test]
    fn package_metadata_selects_typed_application_and_runtime() {
        let service = package(
            "orders-core",
            serde_json::json!({
                "distributed": { "application": { "entrypoint": "orders_application" } }
            }),
            &[("orders_core", "lib")],
        );
        let runner = package(
            "orders-host",
            serde_json::json!({
                "distributed": { "runtime": { "binary": "orders" } }
            }),
            &[("orders", "bin")],
        );
        let packages = vec![&service, &runner];

        let application = discover_application(&Value::Null, &packages).unwrap();
        let runtime = discover_runtime(&Value::Null, &packages).unwrap();

        assert_eq!(application.package, "orders-core");
        assert_eq!(application.crate_name, "orders_core");
        assert_eq!(application.entrypoint, "orders_application");
        assert_eq!(runtime.package, "orders-host");
        assert_eq!(runtime.binary, "orders");
    }

    #[test]
    fn conventional_workspace_needs_no_metadata() {
        let service = package("orders-service", Value::Null, &[("orders_service", "lib")]);
        let runner = package(
            "orders-runner",
            Value::Null,
            &[("orders", "bin"), ("orders-manifest", "bin")],
        );
        let packages = vec![&service, &runner];

        let application = discover_application(&Value::Null, &packages).unwrap();
        let runtime = discover_runtime(&Value::Null, &packages).unwrap();

        assert_eq!(application.entrypoint, "application_manifest");
        assert_eq!(runtime.binary, "orders");
    }

    #[test]
    fn readiness_addresses_are_valid_for_wildcard_and_ipv6_binds() {
        assert_eq!(loopback_address("0.0.0.0:8791"), "127.0.0.1:8791");
        assert_eq!(loopback_address("[::]:8791"), "127.0.0.1:8791");
        assert_eq!(loopback_address("[::1]:8791"), "[::1]:8791");
        assert_eq!(socket_address("localhost", "5180"), "localhost:5180");
        assert_eq!(
            probe_args(&["service"], "localhost:5180".to_string()),
            ["service", "__probe", "--address", "localhost:5180"]
        );
    }

    #[test]
    fn discovered_runtime_allows_a_cold_cargo_build() {
        assert_eq!(COLD_RUNTIME_READY_TIMEOUT_MS, 5 * 60 * 1_000);
    }

    #[test]
    fn workspace_metadata_can_select_a_repository_local_sibling_ui() {
        let repository = tempfile::tempdir().unwrap();
        fs::write(repository.path().join(".git"), "gitdir: fixture").unwrap();
        let application = repository.path().join("services/application");
        let ui = repository.path().join("clients/web");
        fs::create_dir_all(&application).unwrap();
        fs::create_dir_all(&ui).unwrap();
        fs::write(ui.join("package.json"), "{}").unwrap();

        let discovered = discover_ui(
            &serde_json::json!({
                "distributed": { "ui": { "path": "../../clients/web" } }
            }),
            &application.canonicalize().unwrap(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(discovered, ui.canonicalize().unwrap());
    }

    #[test]
    fn workspace_metadata_rejects_a_ui_outside_the_repository() {
        let repository = tempfile::tempdir().unwrap();
        fs::write(repository.path().join(".git"), "gitdir: fixture").unwrap();
        let application = repository.path().join("application");
        fs::create_dir_all(&application).unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("package.json"), "{}").unwrap();
        let relative = format!(
            "../../{}",
            outside.path().file_name().unwrap().to_string_lossy()
        );

        let error = discover_ui(
            &serde_json::json!({
                "distributed": { "ui": { "path": relative } }
            }),
            &application.canonicalize().unwrap(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("outside repository"), "{error}");
    }

    #[test]
    fn repository_sibling_ui_shares_one_lifecycle_root() {
        let repository = tempfile::tempdir().unwrap();
        let application = repository.path().join("services/application");
        let ui = repository.path().join("clients/web");
        let javascript_root = repository.path().join("framework/javascript");
        fs::create_dir_all(&application).unwrap();
        fs::create_dir_all(&ui).unwrap();
        fs::create_dir_all(&javascript_root).unwrap();
        let executable = std::env::current_exe().unwrap();
        let javascript = JavascriptFrameworkPackage {
            version: "1.0.0".into(),
            source: JavascriptPackageSource::Local {
                root: javascript_root.clone(),
            },
        };
        let config = lifecycle_dev(
            repository.path(),
            &application,
            &RuntimeTarget {
                package: "fixture-runner".into(),
                binary: "fixture".into(),
            },
            Some(&ui),
            Some(&javascript),
            true,
            &executable,
            &[],
        );

        config.validate().unwrap();
        let api = &config.processes["api"];
        assert!(!api.external_cwd);
        assert_eq!(api.cwd.as_deref(), Some("services/application"));
        let process = &config.processes["ui"];
        assert!(!process.external_cwd);
        assert_eq!(process.cwd.as_deref(), Some("clients/web"));
        assert_eq!(
            process
                .env
                .get("DISTRIBUTED_LIFECYCLE_PROJECT_ROOT")
                .map(String::as_str),
            repository.path().to_str()
        );
        assert!(process.restart_on.is_empty());
        let framework = &config.processes["framework-js"];
        let project_root = framework
            .args
            .windows(2)
            .find(|args| args[0] == "--project-root")
            .map(|args| args[1].as_str());
        assert_eq!(project_root, application.to_str());
        assert_eq!(
            framework
                .env
                .get(LIFECYCLE_CLI_EXECUTABLE_ENV)
                .map(String::as_str),
            executable.to_str()
        );
    }

    #[test]
    fn client_compiler_sources_cover_svelte_and_binding_sidecars_without_overlap() {
        let project = tempfile::tempdir().unwrap();
        let ui = project.path().join("ui");
        fs::create_dir_all(&ui).unwrap();
        fs::create_dir_all(ui.join("src/lib")).unwrap();
        fs::create_dir_all(ui.join("src/routes/[[itemId]]")).unwrap();
        fs::create_dir_all(ui.join("src/routes/chat")).unwrap();
        fs::write(
            ui.join("src/routes/[[itemId]]/+page.graphql"),
            "query Item { item { id } }\n",
        )
        .unwrap();
        fs::write(
            ui.join("src/routes/chat/+layout.gql"),
            "query Chat { chat { id } }\n",
        )
        .unwrap();
        fs::write(
            ui.join("distributed.clients.json"),
            r#"{
              "schema_version": 1,
              "clients": [{
                "module": "$distributed",
                "surface": "fixture",
                "documents": [
                  "src/routes/*/+page.graphql",
                  "src/routes/chat/+layout.gql",
                  "src/routes/[[itemId]]/+page.graphql"
                ],
                "output": "src/lib/generated"
              }]
            }"#,
        )
        .unwrap();
        fs::write(
            ui.join(CLIENT_CONFIG),
            "export const distributedViteOptions = {};\n",
        )
        .unwrap();

        let lifecycle = discover_client_lifecycle(project.path(), Some(&ui))
            .unwrap()
            .unwrap();
        assert_eq!(
            lifecycle.sources,
            BTreeSet::from([
                "ui/distributed.config.js".to_string(),
                "ui/distributed.clients.json".to_string(),
                "ui/src/lib".to_string(),
                "ui/src/routes".to_string(),
            ])
        );
        assert_eq!(
            lifecycle.outputs,
            BTreeMap::from([
                (
                    "boundary-plan-0".to_string(),
                    "ui/.svelte-kit/distributed/clients/JGRpc3RyaWJ1dGVk".to_string(),
                ),
                ("client-0".to_string(), "ui/src/lib/generated".to_string()),
            ])
        );

        fs::create_dir_all(ui.join("src/lib/generated")).unwrap();
        let error = discover_client_lifecycle(project.path(), Some(&ui)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("still exists in the application source tree"),
            "{error}"
        );
    }

    #[test]
    fn lifecycle_owned_ui_stays_serving_until_generation_activation() {
        let project = tempfile::tempdir().unwrap();
        let ui = project.path().join("ui");
        fs::create_dir_all(&ui).unwrap();
        let executable = std::env::current_exe().unwrap();
        let config = lifecycle_dev(
            project.path(),
            project.path(),
            &RuntimeTarget {
                package: "fixture-runner".into(),
                binary: "fixture".into(),
            },
            Some(&ui),
            None,
            true,
            &executable,
            &[],
        );

        config.validate().unwrap();
        let process = &config.processes["ui"];
        assert_eq!(
            process
                .env
                .get("DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            process
                .env
                .get("DISTRIBUTED_LIFECYCLE_PROJECT_ROOT")
                .map(String::as_str),
            project.path().to_str()
        );
        assert_eq!(
            process
                .env
                .get(LIFECYCLE_CLI_EXECUTABLE_ENV)
                .map(String::as_str),
            executable.to_str()
        );
        assert!(process.restart_on.is_empty());
    }

    #[test]
    fn coordinated_published_versions_share_one_identity() {
        let identity = release_component("rust", "4.6.0");
        let identities = FrameworkIdentities {
            rust: identity.clone(),
            cli: release_component("cli", "4.6.0"),
            javascript: release_component("javascript", "4.6.0"),
        };

        ensure_framework_compatibility(&identities, "4.6.0", None).unwrap();
    }

    #[test]
    fn published_version_skew_names_every_component_and_repair() {
        let identities = FrameworkIdentities {
            rust: release_component("rust", "4.6.0"),
            cli: release_component("cli", "4.5.0"),
            javascript: release_component("javascript", "4.6.0"),
        };

        let error = ensure_framework_compatibility(&identities, "4.6.0", None).unwrap_err();
        let diagnostic = error.diagnostic().unwrap();
        assert_eq!(
            diagnostic["error"]["code"],
            "CTL-FRAMEWORK-IDENTITY-MISMATCH"
        );
        assert_eq!(
            diagnostic["error"]["observed"]["cli"]["description"],
            "cli: published version=4.5.0"
        );
        let error = error.to_string();

        assert!(error.contains("rust: published version=4.6.0"));
        assert!(error.contains("cli: published version=4.5.0"));
        assert!(error.contains("javascript: published version=4.6.0"));
        assert!(error.contains("cargo install distributed_cli --version 4.6.0"));
    }

    #[test]
    fn newer_cli_is_rejected_against_older_project_packages() {
        let identities = FrameworkIdentities {
            rust: release_component("rust", "4.5.0"),
            cli: release_component("cli", "4.6.0"),
            javascript: release_component("javascript", "4.5.0"),
        };

        let error = ensure_framework_compatibility(&identities, "4.5.0", None)
            .unwrap_err()
            .to_string();

        assert!(error.contains("rust: published version=4.5.0"));
        assert!(error.contains("cli: published version=4.6.0"));
        assert!(error.contains("javascript: published version=4.5.0"));
    }

    #[test]
    fn local_skew_points_to_the_checkout_cli() {
        let checkout = tempfile::tempdir().unwrap();
        let local = checkout_component("rust", "0.1.0", checkout.path()).unwrap();
        let identities = FrameworkIdentities {
            rust: local.clone(),
            cli: release_component("cli", "4.6.0"),
            javascript: FrameworkComponentIdentity {
                identity: local.identity,
                description: local.description.replace("rust:", "javascript:"),
            },
        };

        let error = ensure_framework_compatibility(&identities, "0.1.0", Some(checkout.path()))
            .unwrap_err()
            .to_string();

        assert!(error.contains("Run this checkout's CLI"));
        assert!(error.contains(checkout.path().to_string_lossy().as_ref()));
        assert!(!error.contains("cargo install distributed_cli --version 0.1.0"));
    }
}
