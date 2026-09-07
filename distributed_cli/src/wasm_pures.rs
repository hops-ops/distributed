//! Compilation of browser-side Rust pure functions declared by command contracts.
//!
//! The declaration is application source. The CLI owns turning that declaration
//! into the JavaScript and WebAssembly files imported by generated clients.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

const MAX_SOURCE_FILES: usize = 20_000;
const MAX_SOURCE_BYTES: u64 = 128 * 1024 * 1024;
const MAX_PURES: usize = 64;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct WasmPure {
    rust_package: String,
    import: String,
}

#[derive(Debug, Deserialize)]
struct CargoMetadata {
    workspace_root: PathBuf,
    packages: Vec<CargoPackage>,
    resolve: Option<CargoResolve>,
    #[serde(default)]
    metadata: Value,
}

#[derive(Debug, Deserialize)]
struct CargoResolve {
    nodes: Vec<CargoResolveNode>,
}

#[derive(Debug, Deserialize)]
struct CargoResolveNode {
    id: String,
    #[serde(default)]
    dependencies: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CargoPackage {
    id: String,
    name: String,
    manifest_path: PathBuf,
    source: Option<String>,
    #[serde(default)]
    features: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    targets: Vec<CargoTarget>,
}

#[derive(Debug, Deserialize)]
struct CargoTarget {
    src_path: PathBuf,
    #[serde(default)]
    kind: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct WasmStamp {
    schema_version: u64,
    source_identity: String,
    compiler_identity: String,
    rust_package: String,
    import: String,
}

pub(crate) fn build_declared_wasm_pures(
    manifest: &Value,
    cargo_manifest: &Path,
    wasm_pack_launcher: Option<&Path>,
) -> Result<usize, Box<dyn Error>> {
    let pures = collect_wasm_pures(manifest)?;
    if pures.is_empty() {
        return Ok(0);
    }
    let metadata = cargo_metadata(cargo_manifest)?;
    let project_root = &metadata.workspace_root;
    let Some(ui_root) = crate::lifecycle::discover_ui(&metadata.metadata, project_root)? else {
        return Ok(0);
    };
    let ui_lib = ui_root.join("src/lib");
    let wasm_pack_launcher = wasm_pack_launcher
        .ok_or("browser WASM pures require @hops-ops/distributed in the application UI")?;
    let compiler_identity = compiler_identity(wasm_pack_launcher)?;
    let mut outputs = BTreeMap::<PathBuf, &WasmPure>::new();
    for pure in &pures {
        let relative = portable_import_path(&pure.import)?;
        let destination = ui_lib.join(relative.parent().ok_or_else(|| {
            format!(
                "WASM import `{}` must include an output directory",
                pure.import
            )
        })?);
        if let Some(existing) = outputs.insert(destination.clone(), pure) {
            if existing != pure {
                return Err(format!(
                    "WASM pures `{}` and `{}` claim the same output directory `{}`",
                    existing.import,
                    pure.import,
                    destination.display()
                )
                .into());
            }
        }
    }

    for (destination, pure) in outputs {
        let package = resolve_local_package(&metadata, pure)?;
        if !package.features.contains_key("wasm") {
            return Err(format!(
                "WASM pure `{}` requires package `{}` to define a `wasm` Cargo feature",
                pure.import, pure.rust_package
            )
            .into());
        }
        let output_name = Path::new(&pure.import)
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("WASM import `{}` has no UTF-8 output name", pure.import))?;
        let stamp = WasmStamp {
            schema_version: 2,
            source_identity: package_source_identity(&metadata, &package.id)?,
            compiler_identity: compiler_identity.clone(),
            rust_package: pure.rust_package.clone(),
            import: pure.import.clone(),
        };
        let stamp_name = format!("{:x}.json", Sha256::digest(pure.import.as_bytes()));
        let stamp_path = project_root.join(".distributed/wasm").join(stamp_name);
        if stamp_matches(&stamp_path, &destination, &stamp)? {
            eprintln!("distributed: browser WASM {} is current", pure.import);
            continue;
        }
        let package_dir = package
            .manifest_path
            .parent()
            .ok_or_else(|| format!("package `{}` Cargo.toml has no parent", package.name))?;
        build_wasm_pure(
            project_root,
            &ui_root,
            wasm_pack_launcher,
            package_dir,
            output_name,
            &destination,
            &stamp_path,
            &stamp,
        )?;
    }
    Ok(pures.len())
}

fn resolve_local_package<'a>(
    metadata: &'a CargoMetadata,
    pure: &WasmPure,
) -> Result<&'a CargoPackage, Box<dyn Error>> {
    let mut candidates = metadata
        .packages
        .iter()
        .filter(|package| package.name == pure.rust_package && package.source.is_none());
    let Some(package) = candidates.next() else {
        return Err(format!(
            "WASM pure `{}` is declared by Cargo package `{}`, which is not a local application dependency",
            pure.import, pure.rust_package
        )
        .into());
    };
    if candidates.next().is_some() {
        return Err(format!(
            "WASM pure `{}` has more than one local Cargo package named `{}`",
            pure.import, pure.rust_package
        )
        .into());
    }
    Ok(package)
}

fn collect_wasm_pures(manifest: &Value) -> Result<BTreeSet<WasmPure>, Box<dyn Error>> {
    fn visit(value: &Value, found: &mut BTreeSet<WasmPure>) -> Result<(), Box<dyn Error>> {
        match value {
            Value::Array(values) => {
                for value in values {
                    visit(value, found)?;
                }
            }
            Value::Object(object) => {
                if let Some(import) = object.get("wasm_package").and_then(Value::as_str) {
                    if !import.is_empty() {
                        let rust_package = object
                            .get("wasm_rust_package")
                            .and_then(Value::as_str)
                            .filter(|package| !package.is_empty())
                            .ok_or_else(|| {
                                format!(
                                    "WASM pure `{import}` does not identify its declaring Cargo package; declare it through `portable_command!`"
                                )
                            })?;
                        found.insert(WasmPure {
                            rust_package: rust_package.to_string(),
                            import: import.to_string(),
                        });
                        if found.len() > MAX_PURES {
                            return Err(format!(
                                "application declares more than {MAX_PURES} browser WASM pures"
                            )
                            .into());
                        }
                    }
                }
                for value in object.values() {
                    visit(value, found)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    let mut found = BTreeSet::new();
    visit(manifest, &mut found)?;
    Ok(found)
}

fn portable_import_path(import: &str) -> Result<PathBuf, Box<dyn Error>> {
    if import.contains('\\') || import.contains('\0') {
        return Err(format!("WASM import `{import}` is not a portable relative path").into());
    }
    let path = Path::new(import);
    if path.is_absolute()
        || path.components().count() < 2
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!("WASM import `{import}` is not a portable relative path").into());
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if name.is_empty()
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
    {
        return Err(format!("WASM import `{import}` has an invalid output name").into());
    }
    Ok(path.to_path_buf())
}

fn cargo_metadata(manifest: &Path) -> Result<CargoMetadata, Box<dyn Error>> {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--manifest-path"])
        .arg(manifest)
        .output()
        .map_err(|error| format!("failed to run `cargo metadata` for browser WASM: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "`cargo metadata` failed while resolving browser WASM packages: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

fn package_source_identity(
    metadata: &CargoMetadata,
    root_package_id: &str,
) -> Result<String, Box<dyn Error>> {
    let mut files = BTreeSet::new();
    for path in [
        metadata.workspace_root.join("Cargo.toml"),
        metadata.workspace_root.join("Cargo.lock"),
    ] {
        if path.is_file() {
            files.insert(path);
        }
    }
    let local_packages = metadata
        .packages
        .iter()
        .filter(|package| package.source.is_none())
        .map(|package| (package.id.as_str(), package))
        .collect::<BTreeMap<_, _>>();
    let dependencies = metadata
        .resolve
        .as_ref()
        .ok_or("cargo metadata did not include a dependency graph")?
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node.dependencies.as_slice()))
        .collect::<BTreeMap<_, _>>();
    let mut pending = vec![root_package_id];
    let mut visited = BTreeSet::new();
    while let Some(package_id) = pending.pop() {
        if !visited.insert(package_id) {
            continue;
        }
        let Some(package) = local_packages.get(package_id) else {
            continue;
        };
        files.insert(package.manifest_path.clone());
        let package_dir = package
            .manifest_path
            .parent()
            .ok_or_else(|| format!("package `{}` Cargo.toml has no parent", package.name))?;
        let mut source_roots = BTreeSet::new();
        for target in package.targets.iter().filter(|target| {
            target.kind.iter().any(|kind| {
                matches!(
                    kind.as_str(),
                    "lib" | "rlib" | "cdylib" | "proc-macro" | "custom-build"
                )
            })
        }) {
            let relative = target.src_path.strip_prefix(package_dir).map_err(|_| {
                format!(
                    "Cargo target `{}` escapes package `{}`",
                    target.src_path.display(),
                    package.name
                )
            })?;
            if relative.components().count() == 1 {
                // A package-root target can declare sibling modules with
                // `mod`; hash the bounded Rust input tree, not only lib.rs or
                // build.rs, so a sibling edit cannot reuse stale WASM.
                source_roots.insert(package_dir.to_path_buf());
            } else if let Some(Component::Normal(top)) = relative.components().next() {
                source_roots.insert(package_dir.join(top));
            }
        }
        for source_root in source_roots {
            collect_source_files(&source_root, &mut files)?;
        }
        if let Some(package_dependencies) = dependencies.get(package_id) {
            pending.extend(package_dependencies.iter().map(String::as_str));
        }
    }
    if !visited.contains(root_package_id) || !local_packages.contains_key(root_package_id) {
        return Err(format!(
            "browser WASM package `{root_package_id}` is not a local Cargo package"
        )
        .into());
    }
    let mut hash = Sha256::new();
    let mut bytes = 0_u64;
    for file in files {
        let content = fs::read(&file)?;
        bytes = bytes.saturating_add(content.len() as u64);
        if bytes > MAX_SOURCE_BYTES {
            return Err(format!(
                "Rust workspace sources exceed the {MAX_SOURCE_BYTES}-byte WASM fingerprint limit"
            )
            .into());
        }
        hash.update(file.to_string_lossy().as_bytes());
        hash.update([0]);
        hash.update(&content);
        hash.update([0]);
    }
    Ok(format!("sha256:{:x}", hash.finalize()))
}

fn collect_source_files(
    directory: &Path,
    files: &mut BTreeSet<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let name = entry.file_name();
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            if matches!(
                name.to_str(),
                Some("target" | "node_modules" | "ui" | ".git" | ".distributed" | ".worktrees")
            ) {
                continue;
            }
            collect_source_files(&entry.path(), files)?;
        } else if file_type.is_file() {
            let path = entry.path();
            let include = matches!(
                path.extension().and_then(|ext| ext.to_str()),
                Some("rs" | "toml" | "lock")
            );
            if include {
                files.insert(path);
                if files.len() > MAX_SOURCE_FILES {
                    return Err(format!(
                        "Rust workspace contains more than {MAX_SOURCE_FILES} source files for WASM fingerprinting"
                    )
                    .into());
                }
            }
        }
    }
    Ok(())
}

fn compiler_identity(launcher: &Path) -> Result<String, Box<dyn Error>> {
    let launcher = launcher.canonicalize().map_err(|error| {
        format!(
            "failed to resolve browser WASM compiler `{}`: {error}",
            launcher.display()
        )
    })?;
    if !launcher.is_file() {
        return Err(format!(
            "browser WASM compiler `{}` is not a file",
            launcher.display()
        )
        .into());
    }
    let package_root = launcher
        .parent()
        .ok_or("browser WASM compiler has no package directory")?;
    let mut files = BTreeSet::new();
    collect_compiler_files(package_root, package_root, &mut files)?;
    let mut hash = Sha256::new();
    let mut bytes = 0_u64;
    for file in files {
        let content = fs::read(&file)?;
        bytes = bytes.saturating_add(content.len() as u64);
        if bytes > MAX_SOURCE_BYTES {
            return Err(format!(
                "browser WASM compiler exceeds the {MAX_SOURCE_BYTES}-byte fingerprint limit"
            )
            .into());
        }
        hash.update(
            file.strip_prefix(package_root)?
                .to_string_lossy()
                .as_bytes(),
        );
        hash.update([0]);
        hash.update(&content);
        hash.update([0]);
    }
    Ok(format!("sha256:{:x}", hash.finalize()))
}

fn collect_compiler_files(
    root: &Path,
    directory: &Path,
    files: &mut BTreeSet<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "browser WASM compiler package contains symlink `{}`",
                path.display()
            )
            .into());
        }
        if metadata.is_dir() {
            if !path.starts_with(root) {
                return Err("browser WASM compiler package escapes its root".into());
            }
            collect_compiler_files(root, &path, files)?;
        } else if metadata.is_file() {
            files.insert(path);
            if files.len() > MAX_SOURCE_FILES {
                return Err(format!(
                    "browser WASM compiler contains more than {MAX_SOURCE_FILES} files"
                )
                .into());
            }
        }
    }
    Ok(())
}

fn stamp_matches(
    stamp_path: &Path,
    destination: &Path,
    expected: &WasmStamp,
) -> Result<bool, Box<dyn Error>> {
    let Ok(bytes) = fs::read(stamp_path) else {
        return Ok(false);
    };
    let Ok(observed) = serde_json::from_slice::<WasmStamp>(&bytes) else {
        return Ok(false);
    };
    Ok(observed.schema_version == expected.schema_version
        && observed.source_identity == expected.source_identity
        && observed.compiler_identity == expected.compiler_identity
        && observed.rust_package == expected.rust_package
        && observed.import == expected.import
        && destination
            .join(format!(
                "{}_bg.wasm",
                Path::new(&expected.import)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
            ))
            .is_file()
        && destination
            .join(format!(
                "{}.js",
                Path::new(&expected.import)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
            ))
            .is_file())
}

fn build_wasm_pure(
    project_root: &Path,
    ui_root: &Path,
    wasm_pack_launcher: &Path,
    package_dir: &Path,
    output_name: &str,
    destination: &Path,
    stamp_path: &Path,
    stamp: &WasmStamp,
) -> Result<(), Box<dyn Error>> {
    let parent = destination
        .parent()
        .ok_or("WASM output has no parent directory")?;
    ensure_real_directory_path(ui_root, parent)?;
    if let Ok(metadata) = fs::symlink_metadata(destination) {
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(format!(
                "WASM output `{}` must be a real directory",
                destination.display()
            )
            .into());
        }
    }
    let staging_root = ui_root.join(".distributed/wasm-staging");
    ensure_real_directory_path(ui_root, &staging_root)?;
    let stage = tempfile::Builder::new()
        .prefix(".distributed-wasm-stage-")
        .tempdir_in(&staging_root)?;
    eprintln!(
        "distributed: compiling required browser WASM {} from Cargo package {}",
        stamp.import, stamp.rust_package
    );
    let output = Command::new("node")
        .arg(wasm_pack_launcher)
        .arg("build")
        .arg(package_dir)
        .args(["--target", "web", "--out-dir"])
        .arg(stage.path())
        .args(["--out-name", output_name, "--", "--no-default-features", "--features", "wasm"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .status()
        .map_err(|error| {
            format!(
                "failed to start the @hops-ops/distributed WASM compiler `{}` for pure `{}`; run `distributed build` from `{}`: {error}",
                wasm_pack_launcher.display(), stamp.import, project_root.display()
            )
        })?;
    if !output.success() {
        return Err(format!(
            "required WASM pure `{}` failed to compile with {output}",
            stamp.import
        )
        .into());
    }
    for required in [
        stage.path().join(format!("{output_name}.js")),
        stage.path().join(format!("{output_name}_bg.wasm")),
    ] {
        if !required.is_file() {
            return Err(format!(
                "wasm-pack succeeded for `{}` but did not produce `{}`",
                stamp.import,
                required.file_name().unwrap().to_string_lossy()
            )
            .into());
        }
    }
    let staged_path = stage.keep();
    let backup = tempfile::Builder::new()
        .prefix(".distributed-wasm-backup-")
        .tempdir_in(&staging_root)?;
    let backup_path = backup.path().to_path_buf();
    backup.close()?;
    let had_destination = destination.exists();
    if had_destination {
        fs::rename(destination, &backup_path)?;
    }
    if let Err(error) = fs::rename(&staged_path, destination) {
        if had_destination {
            let _ = fs::rename(&backup_path, destination);
        }
        let _ = fs::remove_dir_all(&staged_path);
        return Err(format!(
            "failed to activate required WASM output `{}`: {error}",
            destination.display()
        )
        .into());
    }
    if had_destination {
        fs::remove_dir_all(backup_path)?;
    }
    if let Some(parent) = stamp_path.parent() {
        ensure_real_directory_path(project_root, parent)?;
    }
    fs::write(stamp_path, serde_json::to_vec(stamp)?)?;
    Ok(())
}

fn ensure_real_directory_path(root: &Path, directory: &Path) -> Result<(), Box<dyn Error>> {
    let relative = directory.strip_prefix(root).map_err(|_| {
        format!(
            "generated directory `{}` escapes project `{}`",
            directory.display(),
            root.display()
        )
    })?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(format!(
                "generated directory `{}` is not a portable project path",
                directory.display()
            )
            .into());
        }
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(format!(
                    "generated directory component `{}` must be a real directory",
                    current.display()
                )
                .into())
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn collects_declared_wasm_pures_recursively() {
        let manifest = json!({"commands": [{"pure_reduces": [{
            "wasm_package": "blob/pkg/blob_wasm",
            "wasm_rust_package": "blob-domain"
        }]}]});
        let found = collect_wasm_pures(&manifest).unwrap();
        assert!(found.contains(&WasmPure {
            rust_package: "blob-domain".to_string(),
            import: "blob/pkg/blob_wasm".to_string(),
        }));
    }

    #[test]
    fn rejects_declared_wasm_without_a_rust_package() {
        let error = collect_wasm_pures(&json!({"wasm_package": "blob/pkg/blob_wasm"})).unwrap_err();
        assert!(error.to_string().contains("declaring Cargo package"));
    }

    #[test]
    fn rejects_import_path_traversal() {
        let error = portable_import_path("../outside/module").unwrap_err();
        assert!(error.to_string().contains("portable relative path"));
    }

    #[test]
    fn compiler_identity_tracks_the_installed_package() {
        let fixture = tempfile::tempdir().unwrap();
        let launcher = fixture.path().join("run.js");
        let binary = fixture.path().join("binary/wasm-pack");
        fs::create_dir(fixture.path().join("binary")).unwrap();
        fs::write(&launcher, "require('./binary');\n").unwrap();
        fs::write(&binary, "compiler-v1\n").unwrap();

        let initial = compiler_identity(&launcher).unwrap();
        assert_eq!(initial, compiler_identity(&launcher).unwrap());
        fs::write(&binary, "compiler-v2\n").unwrap();
        assert_ne!(initial, compiler_identity(&launcher).unwrap());
    }

    #[test]
    fn wasm_source_identity_tracks_only_the_local_dependency_closure() {
        let fixture = tempfile::tempdir().unwrap();
        let root = fixture.path();
        fs::write(root.join("Cargo.toml"), "[workspace]\n").unwrap();
        fs::write(root.join("Cargo.lock"), "version = 4\n").unwrap();
        let package = |id: &str, name: &str| {
            let directory = root.join(name);
            fs::create_dir_all(directory.join("src")).unwrap();
            fs::write(
                directory.join("Cargo.toml"),
                format!("[package]\nname = \"{name}\"\n"),
            )
            .unwrap();
            fs::write(directory.join("src/lib.rs"), format!("// {name}\n")).unwrap();
            CargoPackage {
                id: id.to_string(),
                name: name.to_string(),
                manifest_path: directory.join("Cargo.toml"),
                source: None,
                features: BTreeMap::new(),
                targets: vec![CargoTarget {
                    src_path: directory.join("src/lib.rs"),
                    kind: vec!["lib".to_string()],
                }],
            }
        };
        let metadata = CargoMetadata {
            workspace_root: root.to_path_buf(),
            packages: vec![
                package("pure", "pure"),
                package("dependency", "dependency"),
                package("unrelated", "unrelated"),
            ],
            resolve: Some(CargoResolve {
                nodes: vec![
                    CargoResolveNode {
                        id: "pure".to_string(),
                        dependencies: vec!["dependency".to_string()],
                    },
                    CargoResolveNode {
                        id: "dependency".to_string(),
                        dependencies: Vec::new(),
                    },
                ],
            }),
            metadata: Value::Null,
        };
        let initial = package_source_identity(&metadata, "pure").unwrap();
        fs::write(root.join("unrelated/src/lib.rs"), "// changed\n").unwrap();
        assert_eq!(initial, package_source_identity(&metadata, "pure").unwrap());
        fs::write(root.join("dependency/src/lib.rs"), "// changed\n").unwrap();
        assert_ne!(initial, package_source_identity(&metadata, "pure").unwrap());
    }

    #[test]
    fn wasm_source_identity_tracks_sibling_modules_for_root_targets() {
        let fixture = tempfile::tempdir().unwrap();
        let workspace = fixture.path();
        let package_dir = workspace.join("pure");
        fs::create_dir_all(&package_dir).unwrap();
        fs::write(workspace.join("Cargo.toml"), "[workspace]\n").unwrap();
        fs::write(
            package_dir.join("Cargo.toml"),
            "[package]\nname = \"pure\"\n",
        )
        .unwrap();
        fs::write(package_dir.join("lib.rs"), "mod sibling;\n").unwrap();
        fs::write(package_dir.join("sibling.rs"), "pub const VALUE: u8 = 1;\n").unwrap();
        let metadata = CargoMetadata {
            workspace_root: workspace.to_path_buf(),
            packages: vec![CargoPackage {
                id: "pure".to_string(),
                name: "pure".to_string(),
                manifest_path: package_dir.join("Cargo.toml"),
                source: None,
                features: BTreeMap::new(),
                targets: vec![CargoTarget {
                    src_path: package_dir.join("lib.rs"),
                    kind: vec!["lib".to_string()],
                }],
            }],
            resolve: Some(CargoResolve {
                nodes: vec![CargoResolveNode {
                    id: "pure".to_string(),
                    dependencies: Vec::new(),
                }],
            }),
            metadata: Value::Null,
        };

        let initial = package_source_identity(&metadata, "pure").unwrap();
        fs::write(package_dir.join("sibling.rs"), "pub const VALUE: u8 = 2;\n").unwrap();
        assert_ne!(initial, package_source_identity(&metadata, "pure").unwrap());
    }

    #[test]
    fn wasm_source_identity_tracks_local_proc_macro_dependencies() {
        let fixture = tempfile::tempdir().unwrap();
        let root = fixture.path();
        fs::write(root.join("Cargo.toml"), "[workspace]\n").unwrap();
        let package = |id: &str, kind: &str| {
            let directory = root.join(id);
            fs::create_dir_all(directory.join("src")).unwrap();
            fs::write(
                directory.join("Cargo.toml"),
                format!("[package]\nname = \"{id}\"\n"),
            )
            .unwrap();
            fs::write(directory.join("src/lib.rs"), format!("// {id}\n")).unwrap();
            CargoPackage {
                id: id.to_string(),
                name: id.to_string(),
                manifest_path: directory.join("Cargo.toml"),
                source: None,
                features: BTreeMap::new(),
                targets: vec![CargoTarget {
                    src_path: directory.join("src/lib.rs"),
                    kind: vec![kind.to_string()],
                }],
            }
        };
        let metadata = CargoMetadata {
            workspace_root: root.to_path_buf(),
            packages: vec![
                package("pure", "cdylib"),
                package("pure-macros", "proc-macro"),
            ],
            resolve: Some(CargoResolve {
                nodes: vec![
                    CargoResolveNode {
                        id: "pure".to_string(),
                        dependencies: vec!["pure-macros".to_string()],
                    },
                    CargoResolveNode {
                        id: "pure-macros".to_string(),
                        dependencies: Vec::new(),
                    },
                ],
            }),
            metadata: Value::Null,
        };

        let initial = package_source_identity(&metadata, "pure").unwrap();
        fs::write(
            root.join("pure-macros/src/lib.rs"),
            "// changed expansion\n",
        )
        .unwrap();
        assert_ne!(initial, package_source_identity(&metadata, "pure").unwrap());
    }

    #[test]
    fn wasm_pure_accepts_a_unique_local_path_dependency() {
        let pure = WasmPure {
            rust_package: "domain".to_string(),
            import: "domain/pkg/domain_wasm".to_string(),
        };
        let package = CargoPackage {
            id: "path+file:///repo/domain#0.1.0".to_string(),
            name: "domain".to_string(),
            manifest_path: PathBuf::from("/repo/domain/Cargo.toml"),
            source: None,
            features: BTreeMap::new(),
            targets: Vec::new(),
        };
        let metadata = CargoMetadata {
            workspace_root: PathBuf::from("/repo/application"),
            packages: vec![package],
            resolve: None,
            metadata: Value::Null,
        };

        assert_eq!(
            resolve_local_package(&metadata, &pure).unwrap().name,
            "domain"
        );
    }

    #[test]
    fn wasm_pure_rejects_a_registry_package() {
        let pure = WasmPure {
            rust_package: "domain".to_string(),
            import: "domain/pkg/domain_wasm".to_string(),
        };
        let metadata = CargoMetadata {
            workspace_root: PathBuf::from("/repo/application"),
            packages: vec![CargoPackage {
                id: "registry+https://example.invalid#domain@0.1.0".to_string(),
                name: "domain".to_string(),
                manifest_path: PathBuf::from("/cargo/registry/domain/Cargo.toml"),
                source: Some("registry+https://example.invalid".to_string()),
                features: BTreeMap::new(),
                targets: Vec::new(),
            }],
            resolve: None,
            metadata: Value::Null,
        };

        let error = resolve_local_package(&metadata, &pure).unwrap_err();
        assert!(error
            .to_string()
            .contains("not a local application dependency"));
    }

    #[test]
    fn wasm_pure_rejects_ambiguous_local_package_names() {
        let pure = WasmPure {
            rust_package: "domain".to_string(),
            import: "domain/pkg/domain_wasm".to_string(),
        };
        let package = |id: &str| CargoPackage {
            id: id.to_string(),
            name: "domain".to_string(),
            manifest_path: PathBuf::from(format!("/repo/{id}/Cargo.toml")),
            source: None,
            features: BTreeMap::new(),
            targets: Vec::new(),
        };
        let metadata = CargoMetadata {
            workspace_root: PathBuf::from("/repo/application"),
            packages: vec![package("domain-one"), package("domain-two")],
            resolve: None,
            metadata: Value::Null,
        };

        let error = resolve_local_package(&metadata, &pure).unwrap_err();
        assert!(error
            .to_string()
            .contains("more than one local Cargo package"));
    }
}
