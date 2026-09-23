//! Resolution and preparation of the Distributed browser runtime package.
//!
//! Registry packages ship compiled `dist/` artifacts. A local `file:` package
//! is source, so the lifecycle owns installing its build dependencies,
//! compiling it, recording an input/output receipt, and keeping it current in
//! development. Application authors do not maintain a parallel build script.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use crate::lifecycle::{digest_bytes, LifecycleError};

pub(crate) const DISTRIBUTED_JS_PACKAGE: &str = "@hops-ops/distributed";
const RECEIPT_SCHEMA_VERSION: u32 = 1;
const MAX_SOURCE_FILES: usize = 20_000;
const MAX_SOURCE_FILE_BYTES: u64 = 32 * 1024 * 1024;
const MAX_SOURCE_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) enum JavascriptPackageSource {
    Registry,
    Local { root: PathBuf },
}

#[derive(Clone, Debug)]
pub(crate) struct JavascriptFrameworkPackage {
    pub version: String,
    pub source: JavascriptPackageSource,
}

#[derive(Debug, Deserialize)]
struct PackageJson {
    name: Option<String>,
    version: Option<String>,
    #[serde(default)]
    dependencies: BTreeMap<String, String>,
    #[serde(rename = "devDependencies", default)]
    dev_dependencies: BTreeMap<String, String>,
    #[serde(default)]
    scripts: BTreeMap<String, String>,
    #[serde(default)]
    exports: Value,
}

#[derive(Debug, Deserialize)]
struct PackageLock {
    #[serde(default)]
    packages: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct JavascriptReceipt {
    schema_version: u32,
    package: String,
    version: String,
    source_identity: String,
    dependency_identity: String,
    tool_identity: String,
    output_identity: String,
}

pub(crate) fn discover_javascript_framework(
    ui_root: &Path,
) -> Result<Option<JavascriptFrameworkPackage>, LifecycleError> {
    let package = read_package_json(&ui_root.join("package.json"))?;
    let Some(spec) = package
        .dependencies
        .get(DISTRIBUTED_JS_PACKAGE)
        .or_else(|| package.dev_dependencies.get(DISTRIBUTED_JS_PACKAGE))
    else {
        return Ok(None);
    };
    if let Some(relative) = spec.strip_prefix("file:") {
        if relative.is_empty() {
            return Err(LifecycleError::new(format!(
                "{DISTRIBUTED_JS_PACKAGE} has an empty local file dependency"
            )));
        }
        let unresolved = ui_root.join(relative);
        let root = unresolved.canonicalize().map_err(|error| {
            LifecycleError::new(format!(
                "failed to resolve local {DISTRIBUTED_JS_PACKAGE} package `{}`: {error}",
                unresolved.display()
            ))
        })?;
        if !root.is_dir() {
            return Err(LifecycleError::new(format!(
                "local {DISTRIBUTED_JS_PACKAGE} package `{}` is not a directory",
                root.display()
            )));
        }
        return local_package(root).map(Some);
    }

    let version = resolved_registry_version(ui_root)?.ok_or_else(|| {
        LifecycleError::new(format!(
            "cannot resolve the installed {DISTRIBUTED_JS_PACKAGE} version from ui/package-lock.json or ui/node_modules; run `npm install` in `{}`",
            ui_root.display()
        ))
    })?;
    Ok(Some(JavascriptFrameworkPackage {
        version,
        source: JavascriptPackageSource::Registry,
    }))
}

fn local_package(root: PathBuf) -> Result<JavascriptFrameworkPackage, LifecycleError> {
    let package = read_package_json(&root.join("package.json"))?;
    if package.name.as_deref() != Some(DISTRIBUTED_JS_PACKAGE) {
        return Err(LifecycleError::new(format!(
            "local Distributed JavaScript dependency `{}` declares package name `{}` instead of `{DISTRIBUTED_JS_PACKAGE}`",
            root.display(),
            package.name.as_deref().unwrap_or("<missing>")
        )));
    }
    let version = package.version.ok_or_else(|| {
        LifecycleError::new(format!(
            "local {DISTRIBUTED_JS_PACKAGE} package has no version"
        ))
    })?;
    if !package.scripts.contains_key("build") {
        return Err(LifecycleError::new(format!(
            "local {DISTRIBUTED_JS_PACKAGE} package `{}` has no `build` script",
            root.display()
        )));
    }
    Ok(JavascriptFrameworkPackage {
        version,
        source: JavascriptPackageSource::Local { root },
    })
}

fn resolved_registry_version(ui_root: &Path) -> Result<Option<String>, LifecycleError> {
    let lock_path = ui_root.join("package-lock.json");
    if lock_path.is_file() {
        let lock: PackageLock = read_json(&lock_path, "npm package lock")?;
        if let Some(version) = lock
            .packages
            .get(&format!("node_modules/{DISTRIBUTED_JS_PACKAGE}"))
            .and_then(|package| package.get("version"))
            .and_then(Value::as_str)
        {
            return Ok(Some(version.to_string()));
        }
    }
    let installed = ui_root
        .join("node_modules")
        .join(DISTRIBUTED_JS_PACKAGE)
        .join("package.json");
    if installed.is_file() {
        return Ok(read_package_json(&installed)?.version);
    }
    Ok(None)
}

impl JavascriptFrameworkPackage {
    pub(crate) fn local_root(&self) -> Option<&Path> {
        match &self.source {
            JavascriptPackageSource::Registry => None,
            JavascriptPackageSource::Local { root } => Some(root),
        }
    }

    pub(crate) fn prepare(&self, project_root: &Path, check: bool) -> Result<(), LifecycleError> {
        let Some(root) = self.local_root() else {
            return Ok(());
        };
        prepare_local_package(root, project_root, check)
    }

    pub(crate) fn lifecycle_receipt(&self, project_root: &Path) -> Option<PathBuf> {
        self.local_root()
            .map(|root| receipt_path(project_root, root))
    }

    pub(crate) fn wasm_pack_launcher(&self, ui_root: &Path) -> PathBuf {
        match &self.source {
            JavascriptPackageSource::Local { root } => root.join("node_modules/wasm-pack/run.js"),
            JavascriptPackageSource::Registry => ui_root.join("node_modules/wasm-pack/run.js"),
        }
    }

    pub(crate) fn verify_installed(&self, ui_root: &Path) -> Result<(), LifecycleError> {
        let installed_root = ui_root.join("node_modules").join(DISTRIBUTED_JS_PACKAGE);
        match &self.source {
            JavascriptPackageSource::Local { root } => {
                let resolved = installed_root.canonicalize().map_err(|error| {
                    LifecycleError::new(format!(
                        "failed to resolve installed local {DISTRIBUTED_JS_PACKAGE} for `{}`: {error}",
                        ui_root.display()
                    ))
                })?;
                if &resolved != root {
                    return Err(LifecycleError::new(format!(
                        "installed {DISTRIBUTED_JS_PACKAGE} resolves to `{}` instead of declared local source `{}`; run `npm install` in `{}`",
                        resolved.display(),
                        root.display(),
                        ui_root.display()
                    )));
                }
            }
            JavascriptPackageSource::Registry => {
                let metadata = fs::symlink_metadata(&installed_root).map_err(|error| {
                    LifecycleError::new(format!(
                        "failed to inspect installed {DISTRIBUTED_JS_PACKAGE} for `{}`: {error}",
                        ui_root.display()
                    ))
                })?;
                if metadata.file_type().is_symlink() {
                    return Err(LifecycleError::new(format!(
                        "installed {DISTRIBUTED_JS_PACKAGE} is locally linked but the lockfile resolves a published package; run `npm install` in `{}`",
                        ui_root.display()
                    )));
                }
            }
        }
        let installed_manifest = installed_root.join("package.json");
        let package = read_package_json(&installed_manifest).map_err(|error| {
            LifecycleError::new(format!(
                "{DISTRIBUTED_JS_PACKAGE} is not installed for `{}`: {error}",
                ui_root.display()
            ))
        })?;
        if package.version.as_deref() != Some(self.version.as_str()) {
            return Err(LifecycleError::new(format!(
                "installed {DISTRIBUTED_JS_PACKAGE} version `{}` does not match resolved version `{}`; run `npm install` in `{}`",
                package.version.as_deref().unwrap_or("<missing>"),
                self.version,
                ui_root.display()
            )));
        }
        verify_exports(&installed_root, &package)
    }
}

pub(crate) fn watch_local_javascript(
    package_root: &Path,
    project_root: &Path,
) -> Result<(), LifecycleError> {
    let package = local_package(package_root.canonicalize().map_err(|error| {
        LifecycleError::new(format!(
            "failed to resolve local JavaScript watch package: {error}"
        ))
    })?)?;
    package.prepare(project_root, false)?;
    let mut observed_source = source_identity(package_root)?;
    loop {
        std::thread::sleep(Duration::from_millis(500));
        let next_source = match source_identity(package_root) {
            Ok(identity) => identity,
            Err(error) => {
                eprintln!("distributed: cannot inspect local JavaScript changes: {error}");
                continue;
            }
        };
        if next_source == observed_source {
            continue;
        }
        match package.prepare(project_root, false) {
            Ok(()) => observed_source = next_source,
            Err(error) => {
                eprintln!("distributed: local JavaScript rebuild failed: {error}");
            }
        }
    }
}

fn prepare_local_package(
    root: &Path,
    project_root: &Path,
    check: bool,
) -> Result<(), LifecycleError> {
    let package = read_package_json(&root.join("package.json"))?;
    let version = package.version.clone().ok_or_else(|| {
        LifecycleError::new(format!(
            "local {DISTRIBUTED_JS_PACKAGE} package has no version"
        ))
    })?;
    let source_identity = source_identity(root)?;
    let dependency_identity = dependency_identity(root)?;
    let tool_identity = tool_identity(root, &package)?;
    let receipt_path = receipt_path(project_root, root);
    let previous = read_receipt(&receipt_path);
    let current_output = output_identity(root, &package).ok();
    let current = previous.as_ref().is_some_and(|receipt| {
        receipt.schema_version == RECEIPT_SCHEMA_VERSION
            && receipt.package == DISTRIBUTED_JS_PACKAGE
            && receipt.version == version
            && receipt.source_identity == source_identity
            && receipt.dependency_identity == dependency_identity
            && receipt.tool_identity == tool_identity
            && current_output.as_ref() == Some(&receipt.output_identity)
    });
    if current {
        if !check {
            prepare_local_wasm_pack(root)?;
        }
        return Ok(());
    }
    if check {
        return Err(LifecycleError::new(format!(
            "local {DISTRIBUTED_JS_PACKAGE} build is stale for `{}`; run `distributed build`",
            root.display()
        )));
    }

    let dependencies_changed = previous
        .as_ref()
        .is_none_or(|receipt| receipt.dependency_identity != dependency_identity);
    if dependencies_changed || !root.join("node_modules").is_dir() {
        let install = if root.join("package-lock.json").is_file() {
            "ci"
        } else {
            "install"
        };
        eprintln!(
            "distributed: installing local {DISTRIBUTED_JS_PACKAGE} build dependencies with `npm {install}`"
        );
        run_npm(
            root,
            &[install, "--ignore-scripts"],
            "JavaScript framework dependency install",
        )?;
    }
    prepare_local_wasm_pack(root)?;
    eprintln!(
        "distributed: compiling local {DISTRIBUTED_JS_PACKAGE} from {}",
        root.display()
    );
    build_local_package_staged(root, &package)?;
    let output_identity = output_identity(root, &package)?;
    let receipt = JavascriptReceipt {
        schema_version: RECEIPT_SCHEMA_VERSION,
        package: DISTRIBUTED_JS_PACKAGE.to_string(),
        version,
        source_identity,
        dependency_identity,
        tool_identity,
        output_identity,
    };
    if let Some(parent) = receipt_path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            LifecycleError::new(format!(
                "failed to create JavaScript lifecycle receipt directory: {error}"
            ))
        })?;
    }
    let parent = receipt_path
        .parent()
        .ok_or_else(|| LifecycleError::new("JavaScript lifecycle receipt has no parent"))?;
    let bytes =
        serde_json::to_vec(&receipt).map_err(|error| LifecycleError::new(error.to_string()))?;
    let mut staged = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
        LifecycleError::new(format!(
            "failed to stage JavaScript lifecycle receipt: {error}"
        ))
    })?;
    staged.write_all(&bytes).map_err(|error| {
        LifecycleError::new(format!(
            "failed to write JavaScript lifecycle receipt: {error}"
        ))
    })?;
    staged.persist(&receipt_path).map_err(|error| {
        LifecycleError::new(format!(
            "failed to publish JavaScript lifecycle receipt `{}`: {}",
            receipt_path.display(),
            error.error
        ))
    })?;
    Ok(())
}

fn prepare_local_wasm_pack(root: &Path) -> Result<(), LifecycleError> {
    let package = root.join("node_modules/wasm-pack/package.json");
    if !package.is_file() {
        return Ok(());
    }
    let binary = root
        .join("node_modules/wasm-pack/binary")
        .join(if cfg!(windows) {
            "wasm-pack.exe"
        } else {
            "wasm-pack"
        });
    if binary.is_file() {
        return Ok(());
    }
    eprintln!(
        "distributed: installing the pinned wasm-pack binary required by {DISTRIBUTED_JS_PACKAGE}"
    );
    run_npm(
        root,
        &["rebuild", "wasm-pack"],
        "JavaScript framework wasm-pack install",
    )
}

fn build_local_package_staged(root: &Path, package: &PackageJson) -> Result<(), LifecycleError> {
    let parent = root
        .parent()
        .ok_or_else(|| LifecycleError::new("local JavaScript package has no parent directory"))?;
    let stage = tempfile::tempdir_in(parent).map_err(|error| {
        LifecycleError::new(format!(
            "failed to stage JavaScript framework build: {error}"
        ))
    })?;
    for source in build_input_files(root)? {
        let relative = source.strip_prefix(root).map_err(|_| {
            LifecycleError::new("JavaScript package input escapes its package root")
        })?;
        let target = stage.path().join(relative);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                LifecycleError::new(format!("create staged JavaScript input: {error}"))
            })?;
        }
        fs::copy(&source, &target).map_err(|error| {
            LifecycleError::new(format!(
                "copy staged JavaScript input `{}`: {error}",
                relative.display()
            ))
        })?;
    }
    link_build_dependencies(
        &root.join("node_modules"),
        &stage.path().join("node_modules"),
    )?;
    run_npm(
        stage.path(),
        &["run", "build"],
        "JavaScript framework build",
    )?;
    output_identity(stage.path(), package)?;
    publish_dist(&stage.path().join("dist"), &root.join("dist"))
}

#[cfg(unix)]
fn link_build_dependencies(source: &Path, target: &Path) -> Result<(), LifecycleError> {
    std::os::unix::fs::symlink(source, target).map_err(|error| {
        LifecycleError::new(format!("link staged JavaScript dependencies: {error}"))
    })
}

#[cfg(windows)]
fn link_build_dependencies(source: &Path, target: &Path) -> Result<(), LifecycleError> {
    match std::os::windows::fs::symlink_dir(source, target) {
        Ok(()) => Ok(()),
        Err(symlink_error) => create_windows_junction(source, target).map_err(|junction_error| {
            LifecycleError::new(format!(
                "link staged JavaScript dependencies: symlink failed: {symlink_error}; directory junction failed: {junction_error}"
            ))
        }),
    }
}

#[cfg(windows)]
fn create_windows_junction(source: &Path, target: &Path) -> std::io::Result<()> {
    let status = Command::new("cmd")
        .args(["/D", "/C", "mklink", "/J"])
        .arg(target)
        .arg(source)
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "mklink /J exited with {status}"
        )))
    }
}

fn publish_dist(staged: &Path, destination: &Path) -> Result<(), LifecycleError> {
    let staged_root = staged
        .parent()
        .ok_or_else(|| LifecycleError::new("staged JavaScript dist has no package root"))?;
    let mut staged_files = Vec::new();
    collect_source_files(staged_root, staged, &mut staged_files)?;
    let staged_relative = staged_files
        .iter()
        .map(|path| {
            path.strip_prefix(staged)
                .map(Path::to_path_buf)
                .map_err(|_| LifecycleError::new("staged JavaScript output escapes dist"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;

    if destination.is_dir() {
        let destination_root = destination
            .parent()
            .ok_or_else(|| LifecycleError::new("JavaScript dist has no package root"))?;
        let mut existing = Vec::new();
        collect_source_files(destination_root, destination, &mut existing)?;
        for path in existing {
            let relative = path
                .strip_prefix(destination)
                .map_err(|_| LifecycleError::new("existing JavaScript output escapes dist"))?;
            if !staged_relative.contains(relative) {
                fs::remove_file(&path).map_err(|error| {
                    LifecycleError::new(format!(
                        "remove stale JavaScript output `{}`: {error}",
                        relative.display()
                    ))
                })?;
            }
        }
    }

    for source in staged_files {
        let relative = source
            .strip_prefix(staged)
            .map_err(|_| LifecycleError::new("staged JavaScript output escapes dist"))?;
        let target = destination.join(relative);
        let bytes = fs::read(&source).map_err(|error| {
            LifecycleError::new(format!(
                "read staged JavaScript output `{}`: {error}",
                relative.display()
            ))
        })?;
        if fs::read(&target).ok().as_deref() == Some(bytes.as_slice()) {
            continue;
        }
        let parent = target
            .parent()
            .ok_or_else(|| LifecycleError::new("JavaScript output has no parent directory"))?;
        fs::create_dir_all(parent).map_err(|error| {
            LifecycleError::new(format!("create JavaScript output directory: {error}"))
        })?;
        let mut staged_file = tempfile::NamedTempFile::new_in(parent)
            .map_err(|error| LifecycleError::new(format!("stage JavaScript output: {error}")))?;
        staged_file.write_all(&bytes).map_err(|error| {
            LifecycleError::new(format!("write staged JavaScript output: {error}"))
        })?;
        staged_file.persist(&target).map_err(|error| {
            LifecycleError::new(format!(
                "publish JavaScript output `{}`: {}",
                relative.display(),
                error.error
            ))
        })?;
    }
    Ok(())
}

fn run_npm(root: &Path, args: &[&str], label: &str) -> Result<(), LifecycleError> {
    let status = Command::new("npm")
        .args(args)
        .current_dir(root)
        .status()
        .map_err(|error| {
            LifecycleError::new(format!("failed to start npm for {label}: {error}"))
        })?;
    if !status.success() {
        return Err(LifecycleError::new(format!("{label} failed with {status}")));
    }
    Ok(())
}

fn read_receipt(path: &Path) -> Option<JavascriptReceipt> {
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

fn receipt_path(project_root: &Path, package_root: &Path) -> PathBuf {
    let name = format!(
        "{:x}.json",
        Sha256::digest(package_root.to_string_lossy().as_bytes())
    );
    project_root.join(".distributed/javascript").join(name)
}

fn source_identity(root: &Path) -> Result<String, LifecycleError> {
    hash_files(root, build_input_files(root)?)
}

fn build_input_files(root: &Path) -> Result<Vec<PathBuf>, LifecycleError> {
    let mut files = Vec::new();
    for name in ["package.json", "package-lock.json", "src", "scripts"] {
        let path = root.join(name);
        if path.exists() {
            collect_source_files(root, &path, &mut files)?;
        }
    }
    let mut configs = fs::read_dir(root)
        .map_err(|error| LifecycleError::new(format!("read JavaScript package: {error}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| LifecycleError::new(format!("read JavaScript package: {error}")))?;
    configs.sort_by_key(|entry| entry.file_name());
    for entry in configs {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("tsconfig") && name.ends_with(".json") {
            collect_source_files(root, &entry.path(), &mut files)?;
        }
    }
    if files.is_empty() {
        return Err(LifecycleError::new(
            "Distributed JavaScript package has no build inputs",
        ));
    }
    Ok(files)
}

fn collect_source_files(
    root: &Path,
    path: &Path,
    files: &mut Vec<PathBuf>,
) -> Result<(), LifecycleError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        LifecycleError::new(format!(
            "inspect JavaScript source `{}`: {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(LifecycleError::new(format!(
            "JavaScript source `{}` must not be a symlink",
            path.display()
        )));
    }
    if metadata.is_file() {
        if metadata.len() > MAX_SOURCE_FILE_BYTES {
            return Err(LifecycleError::new(format!(
                "JavaScript source `{}` exceeds the file size limit",
                path.display()
            )));
        }
        files.push(path.to_path_buf());
        if files.len() > MAX_SOURCE_FILES {
            return Err(LifecycleError::new(
                "Distributed JavaScript package exceeds the source file limit",
            ));
        }
        return Ok(());
    }
    if !metadata.is_dir() || !path.starts_with(root) {
        return Err(LifecycleError::new(format!(
            "JavaScript source `{}` is not a package file or directory",
            path.display()
        )));
    }
    let mut entries = fs::read_dir(path)
        .map_err(|error| LifecycleError::new(format!("read JavaScript source: {error}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| LifecycleError::new(format!("read JavaScript source: {error}")))?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        collect_source_files(root, &entry.path(), files)?;
    }
    Ok(())
}

fn hash_files(root: &Path, mut files: Vec<PathBuf>) -> Result<String, LifecycleError> {
    files.sort();
    files.dedup();
    let mut total = 0u64;
    let mut hasher = Sha256::new();
    for path in files {
        let relative = path.strip_prefix(root).map_err(|_| {
            LifecycleError::new("JavaScript package input escapes its package root")
        })?;
        let bytes = fs::read(&path).map_err(|error| {
            LifecycleError::new(format!(
                "read JavaScript source `{}`: {error}",
                path.display()
            ))
        })?;
        total = total.saturating_add(bytes.len() as u64);
        if total > MAX_SOURCE_BYTES {
            return Err(LifecycleError::new(
                "Distributed JavaScript package exceeds the total source size limit",
            ));
        }
        hasher.update(relative.to_string_lossy().as_bytes());
        hasher.update([0]);
        hasher.update(&bytes);
        hasher.update([0]);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn dependency_identity(root: &Path) -> Result<String, LifecycleError> {
    let path = if root.join("package-lock.json").is_file() {
        root.join("package-lock.json")
    } else {
        root.join("package.json")
    };
    fs::read(&path)
        .map(|bytes| digest_bytes(&bytes))
        .map_err(|error| LifecycleError::new(format!("read npm dependency input: {error}")))
}

fn tool_identity(root: &Path, package: &PackageJson) -> Result<String, LifecycleError> {
    let node = command_version(root, "node")?;
    let npm = command_version(root, "npm")?;
    let build = package.scripts.get("build").ok_or_else(|| {
        LifecycleError::new(format!(
            "local {DISTRIBUTED_JS_PACKAGE} package has no build script"
        ))
    })?;
    Ok(digest_bytes(
        format!("node={node}\0npm={npm}\0build={build}").as_bytes(),
    ))
}

fn command_version(root: &Path, command: &str) -> Result<String, LifecycleError> {
    let output = Command::new(command)
        .arg("--version")
        .current_dir(root)
        .output()
        .map_err(|error| {
            LifecycleError::new(format!("failed to run `{command} --version`: {error}"))
        })?;
    if !output.status.success() {
        return Err(LifecycleError::new(format!(
            "`{command} --version` failed with {}",
            output.status
        )));
    }
    String::from_utf8(output.stdout)
        .map(|version| version.trim().to_string())
        .map_err(|error| LifecycleError::new(format!("invalid {command} version output: {error}")))
}

fn output_identity(root: &Path, package: &PackageJson) -> Result<String, LifecycleError> {
    verify_exports(root, package)?;
    let dist = root.join("dist");
    let mut files = Vec::new();
    collect_source_files(root, &dist, &mut files)?;
    hash_files(root, files)
}

fn verify_exports(root: &Path, package: &PackageJson) -> Result<(), LifecycleError> {
    let mut exports = Vec::new();
    collect_export_paths(&package.exports, &mut exports);
    if exports.is_empty() {
        return Err(LifecycleError::new(format!(
            "{DISTRIBUTED_JS_PACKAGE} package declares no file exports"
        )));
    }
    for export in exports {
        let Some(relative) = export.strip_prefix("./") else {
            return Err(LifecycleError::new(format!(
                "{DISTRIBUTED_JS_PACKAGE} export `{export}` is not package-relative"
            )));
        };
        if relative.split('/').any(|part| part == "..") || !root.join(relative).is_file() {
            return Err(LifecycleError::new(format!(
                "{DISTRIBUTED_JS_PACKAGE} export `{export}` is missing from `{}`",
                root.display()
            )));
        }
    }
    Ok(())
}

fn collect_export_paths(value: &Value, exports: &mut Vec<String>) {
    match value {
        Value::String(path) if path.starts_with("./") => exports.push(path.clone()),
        Value::Array(values) => {
            for value in values {
                collect_export_paths(value, exports);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_export_paths(value, exports);
            }
        }
        _ => {}
    }
}

fn read_package_json(path: &Path) -> Result<PackageJson, LifecycleError> {
    read_json(path, "npm package manifest")
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path, label: &str) -> Result<T, LifecycleError> {
    let bytes = fs::read(path).map_err(|error| {
        LifecycleError::new(format!(
            "failed to read {label} `{}`: {error}",
            path.display()
        ))
    })?;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(LifecycleError::new(format!("{label} exceeds 16 MiB")));
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        LifecycleError::new(format!(
            "failed to parse {label} `{}`: {error}",
            path.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, value: impl AsRef<[u8]>) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, value).unwrap();
    }

    fn local_manifest() -> String {
        serde_json::json!({
            "name": DISTRIBUTED_JS_PACKAGE,
            "version": "0.1.0",
            "scripts": { "build": "tsc" },
            "exports": { ".": { "import": "./dist/index.js", "types": "./dist/index.d.ts" } }
        })
        .to_string()
    }

    #[test]
    fn discovers_a_local_framework_package_from_the_ui_dependency() {
        let fixture = tempfile::tempdir().unwrap();
        let ui = fixture.path().join("ui");
        let framework = fixture.path().join("framework");
        write(
            &ui.join("package.json"),
            serde_json::json!({
                "dependencies": { (DISTRIBUTED_JS_PACKAGE): "file:../framework" }
            })
            .to_string(),
        );
        write(&framework.join("package.json"), local_manifest());

        let discovered = discover_javascript_framework(&ui).unwrap().unwrap();
        let expected = framework.canonicalize().unwrap();

        assert_eq!(discovered.version, "0.1.0");
        assert_eq!(discovered.local_root(), Some(expected.as_path()));
    }

    #[test]
    fn resolves_the_exact_registry_version_from_the_lockfile() {
        let fixture = tempfile::tempdir().unwrap();
        write(
            &fixture.path().join("package.json"),
            serde_json::json!({
                "dependencies": { (DISTRIBUTED_JS_PACKAGE): "^4.0.0" }
            })
            .to_string(),
        );
        write(
            &fixture.path().join("package-lock.json"),
            serde_json::json!({
                "packages": {
                    "node_modules/@hops-ops/distributed": { "version": "4.6.0" }
                }
            })
            .to_string(),
        );

        let discovered = discover_javascript_framework(fixture.path())
            .unwrap()
            .unwrap();

        assert_eq!(discovered.version, "4.6.0");
        assert!(matches!(
            discovered.source,
            JavascriptPackageSource::Registry
        ));
    }

    #[test]
    fn source_identity_changes_for_sources_but_not_generated_dist() {
        let fixture = tempfile::tempdir().unwrap();
        write(&fixture.path().join("package.json"), local_manifest());
        write(
            &fixture.path().join("src/index.ts"),
            "export const value = 1;\n",
        );
        write(
            &fixture.path().join("dist/index.js"),
            "export const value = 1;\n",
        );

        let initial = source_identity(fixture.path()).unwrap();
        write(&fixture.path().join("dist/index.js"), "generated change\n");
        assert_eq!(source_identity(fixture.path()).unwrap(), initial);

        write(
            &fixture.path().join("src/index.ts"),
            "export const value = 2;\n",
        );
        assert_ne!(source_identity(fixture.path()).unwrap(), initial);
    }

    #[test]
    fn staged_dist_publish_changes_only_changed_outputs() {
        let fixture = tempfile::tempdir().unwrap();
        let staged = fixture.path().join("stage/dist");
        let destination = fixture.path().join("package/dist");
        write(&staged.join("same.js"), "same\n");
        write(&staged.join("changed.js"), "new\n");
        write(&destination.join("same.js"), "same\n");
        write(&destination.join("changed.js"), "old\n");
        write(&destination.join("stale.js"), "stale\n");
        #[cfg(unix)]
        let same_inode = {
            use std::os::unix::fs::MetadataExt;
            fs::metadata(destination.join("same.js")).unwrap().ino()
        };

        publish_dist(&staged, &destination).unwrap();

        assert_eq!(
            fs::read_to_string(destination.join("same.js")).unwrap(),
            "same\n"
        );
        assert_eq!(
            fs::read_to_string(destination.join("changed.js")).unwrap(),
            "new\n"
        );
        assert!(!destination.join("stale.js").exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(
                fs::metadata(destination.join("same.js")).unwrap().ino(),
                same_inode
            );
        }
    }

    #[test]
    fn installed_package_requires_every_declared_export() {
        let fixture = tempfile::tempdir().unwrap();
        write(&fixture.path().join("package.json"), local_manifest());
        write(&fixture.path().join("dist/index.js"), "export {};\n");
        let package = read_package_json(&fixture.path().join("package.json")).unwrap();

        let error = verify_exports(fixture.path(), &package)
            .unwrap_err()
            .to_string();

        assert!(error.contains("./dist/index.d.ts"));
        assert!(error.contains("is missing"));
    }

    #[test]
    fn installed_local_package_must_resolve_to_the_declared_source() {
        let fixture = tempfile::tempdir().unwrap();
        let expected = fixture.path().join("expected");
        let installed = fixture.path().join("ui/node_modules/@hops-ops/distributed");
        fs::create_dir_all(&expected).unwrap();
        fs::create_dir_all(&installed).unwrap();
        let package = JavascriptFrameworkPackage {
            version: "0.1.0".to_string(),
            source: JavascriptPackageSource::Local {
                root: expected.canonicalize().unwrap(),
            },
        };

        let error = package
            .verify_installed(&fixture.path().join("ui"))
            .unwrap_err()
            .to_string();

        assert!(error.contains("instead of declared local source"));
    }

    #[cfg(windows)]
    #[test]
    fn windows_staging_can_fall_back_to_a_directory_junction() {
        let fixture = tempfile::tempdir().unwrap();
        let source = fixture.path().join("node_modules");
        let target = fixture.path().join("stage-node_modules");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("visible.txt"), "junction\n").unwrap();

        create_windows_junction(&source, &target).unwrap();

        assert_eq!(
            fs::read_to_string(target.join("visible.txt")).unwrap(),
            "junction\n"
        );
    }
}
