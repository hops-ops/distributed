//! The artifact harness: `describe`/`schema`/`client-manifest` compile a tiny
//! generated crate that depends on the target service and calls its portable
//! export entrypoint. This module owns that codegen and the nested `cargo`
//! invocations; the `cli` module maps flags onto
//! [`HarnessOptions`]/[`HarnessMode`].

use serde::Deserialize;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::cli::{path_for_toml, resolve_distributed_path};
use crate::SchemaDialect;

const MAX_HARNESS_ERROR_CHARS: usize = 16 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct HarnessOptions {
    pub(crate) path: PathBuf,
    pub(crate) manifest_path: Option<PathBuf>,
    pub(crate) package: Option<String>,
    pub(crate) features: Vec<String>,
    pub(crate) no_default_features: bool,
    pub(crate) entrypoint: Option<String>,
    pub(crate) distributed_path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum HarnessMode {
    DescribeJson,
    SchemaSql(SchemaDialect),
    SchemaGraphql,
    ClientManifest,
    GameManifest,
}

impl HarnessMode {
    fn cache_key(self) -> &'static str {
        match self {
            HarnessMode::DescribeJson => "describe-json",
            HarnessMode::SchemaSql(SchemaDialect::Postgres) => "schema-postgres",
            HarnessMode::SchemaSql(SchemaDialect::Sqlite) => "schema-sqlite",
            HarnessMode::SchemaGraphql => "schema-graphql",
            HarnessMode::ClientManifest => "client-manifest",
            HarnessMode::GameManifest => "game-manifest",
        }
    }

    fn default_entrypoint(self) -> &'static str {
        match self {
            HarnessMode::ClientManifest => "distributed_client_surface",
            HarnessMode::GameManifest => "game_surface",
            HarnessMode::DescribeJson => "application_manifest",
            HarnessMode::SchemaSql(_) | HarnessMode::SchemaGraphql => "read_model_catalog",
        }
    }

    fn requires_distributed_dependency(self) -> bool {
        matches!(self, Self::SchemaSql(_) | Self::SchemaGraphql)
    }
}

pub(crate) fn run_manifest_harness(
    options: &HarnessOptions,
    mode: HarnessMode,
) -> Result<String, Box<dyn Error>> {
    let manifest_path =
        resolve_target_manifest_path(&options.path, options.manifest_path.as_deref())?;
    let package = cargo_package(&manifest_path, options.package.as_deref())?;
    let distributed_path = mode
        .requires_distributed_dependency()
        .then(|| resolve_distributed_path(options.distributed_path.as_deref(), &package.directory))
        .transpose()?;
    let crate_ident = package.name.replace('-', "_");
    let entrypoint = options
        .entrypoint
        .clone()
        .map(|entrypoint| qualify_entrypoint(&crate_ident, &entrypoint))
        .unwrap_or_else(|| Ok(format!("{crate_ident}::{}", mode.default_entrypoint())))?;
    validate_rust_path(&entrypoint)?;

    // Keep the standalone harness beside workspace packages, never underneath
    // one. A harness nested below a package that uses `workspace = true`
    // dependencies becomes that package's nearest workspace ancestor and Cargo
    // resolves its inherited dependencies against the generated harness.
    let harness_root = package
        .target_directory
        .join("distributed-manifest-harness")
        .join(&package.name);
    let harness_dir = harness_root.join(mode.cache_key());
    fs::create_dir_all(harness_dir.join("src"))?;
    // One harness directory serves every entrypoint of this package and mode:
    // hold it from writing `main.rs` until its output is read, or a concurrent
    // run (a dev-server watcher next to a manual regenerate) swaps entrypoints
    // under us and both print the last writer's export.
    let lock = fs::File::create(harness_root.join(format!("{}.lock", mode.cache_key())))?;
    fs2::FileExt::lock_exclusive(&lock)?;
    fs::write(
        harness_dir.join("Cargo.toml"),
        harness_cargo_toml(
            &format!("distributed-manifest-harness-{}", mode.cache_key()),
            &crate_ident,
            &package.name,
            &package.directory,
            distributed_path.as_deref(),
            &options.features,
            options.no_default_features,
        ),
    )?;
    fs::write(
        harness_dir.join("src/main.rs"),
        harness_main_rs(&entrypoint, mode),
    )?;

    let manifest_path = harness_dir.join("Cargo.toml");
    let output = Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "--manifest-path",
            manifest_path.to_string_lossy().as_ref(),
        ])
        .env("CARGO_TARGET_DIR", harness_root.join("target"))
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "manifest harness failed for package `{}` entrypoint `{entrypoint}`:\n{}",
            package.name,
            manifest_harness_error(&stderr)
        )
        .into());
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn manifest_harness_error(stderr: &str) -> String {
    let lines = stderr.lines().collect::<Vec<_>>();
    let first_error = lines
        .iter()
        .position(|line| line.starts_with("error"))
        .unwrap_or(0);
    let excerpt = lines[first_error..].join("\n");
    let mut bounded = excerpt
        .chars()
        .take(MAX_HARNESS_ERROR_CHARS)
        .collect::<String>();
    if excerpt.chars().count() > MAX_HARNESS_ERROR_CHARS {
        bounded.push_str("\n<compiler output truncated>");
    }
    bounded
}

fn harness_cargo_toml(
    harness_package_name: &str,
    crate_ident: &str,
    package_name: &str,
    package_dir: &Path,
    distributed_path: Option<&Path>,
    features: &[String],
    no_default_features: bool,
) -> String {
    let features = features
        .iter()
        .map(toml_string)
        .collect::<Vec<_>>()
        .join(", ");
    let default_features = if no_default_features {
        ", default-features = false"
    } else {
        ""
    };
    let distributed_dependency = distributed_path
        .map(|path| {
            format!(
                "distributed = {{ path = {} }}\n",
                toml_string(path_for_toml(path))
            )
        })
        .unwrap_or_default();

    format!(
        r#"[package]
name = {harness_package_name}
version = "0.1.0"
edition = "2021"

[workspace]

[dependencies]
{distributed_dependency}serde_json = "1"
{crate_ident} = {{ package = {package_name}, path = {package_dir}{default_features}, features = [{features}] }}
"#,
        harness_package_name = toml_string(harness_package_name),
        distributed_dependency = distributed_dependency,
        package_name = toml_string(package_name),
        package_dir = toml_string(path_for_toml(package_dir)),
    )
}

fn harness_main_rs(entrypoint: &str, mode: HarnessMode) -> String {
    match mode {
        HarnessMode::DescribeJson => format!(
            r#"fn main() {{
    let manifest = {entrypoint}();
    println!("{{}}", serde_json::to_string_pretty(&manifest).expect("manifest should serialize"));
}}
"#
        ),
        HarnessMode::SchemaSql(dialect) => {
            let dialect = match dialect {
                SchemaDialect::Postgres => "Postgres",
                SchemaDialect::Sqlite => "Sqlite",
            };
            format!(
                r#"fn main() {{
    let catalog = {entrypoint}();
    let statements = catalog
        .sql_statements(distributed::table::TableSqlDialect::{dialect})
        .expect("read-model SQL should render");
    if !statements.is_empty() {{
        println!("{{}}", statements.join("\n\n"));
    }}
}}
"#
            )
        }
        HarnessMode::SchemaGraphql => format!(
            r#"fn main() {{
    let catalog = {entrypoint}();
    let sdl = distributed::graphql::graphql_sdl_for_tables(&catalog.tables)
        .expect("read-model GraphQL SDL should render");
    print!("{{}}", sdl);
}}
"#
        ),
        HarnessMode::GameManifest => format!(
            r#"fn main() {{
    let surface = {entrypoint}();
    println!("{{}}", serde_json::to_string_pretty(&surface).expect("game surface should serialize"));
}}
"#
        ),
        HarnessMode::ClientManifest => format!(
            r#"fn main() {{
    let export = {entrypoint}();
    let manifest = export
        .manifest()
        .expect("client Surface should compile into a manifest");
    println!("{{}}", serde_json::to_string_pretty(&manifest).expect("client manifest should serialize"));
}}
"#
        ),
    }
}

pub(crate) fn resolve_target_manifest_path(
    path: &Path,
    manifest_path: Option<&Path>,
) -> Result<PathBuf, Box<dyn Error>> {
    let manifest = if let Some(manifest_path) = manifest_path {
        manifest_path.to_path_buf()
    } else if path.is_dir() {
        path.join("Cargo.toml")
    } else {
        path.to_path_buf()
    };

    if !manifest.exists() {
        return Err(format!("target manifest not found: {}", manifest.display()).into());
    }
    Ok(manifest.canonicalize()?)
}

#[derive(Clone, Debug)]
struct CargoPackage {
    name: String,
    directory: PathBuf,
    target_directory: PathBuf,
}

fn cargo_package(
    manifest_path: &Path,
    package_name: Option<&str>,
) -> Result<CargoPackage, Box<dyn Error>> {
    let output = Command::new("cargo")
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
            manifest_path.to_string_lossy().as_ref(),
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("cargo metadata failed: {stderr}").into());
    }

    let metadata: CargoMetadata = serde_json::from_slice(&output.stdout)?;
    let target_directory = PathBuf::from(&metadata.target_directory);
    let selected = if let Some(package_name) = package_name {
        metadata
            .packages
            .into_iter()
            .find(|package| package.name == package_name)
            .ok_or_else(|| format!("package `{package_name}` was not found in cargo metadata"))?
    } else if metadata.packages.len() == 1 {
        metadata
            .packages
            .into_iter()
            .next()
            .expect("single package should exist")
    } else {
        let manifest_path = manifest_path.canonicalize()?;
        metadata
            .packages
            .into_iter()
            .find(|package| {
                Path::new(&package.manifest_path).canonicalize().ok() == Some(manifest_path.clone())
            })
            .ok_or("multiple packages found; pass --package to select one")?
    };
    let manifest_path = PathBuf::from(&selected.manifest_path);
    let directory = manifest_path
        .parent()
        .ok_or("cargo package manifest has no parent directory")?
        .to_path_buf();

    Ok(CargoPackage {
        name: selected.name,
        directory,
        target_directory,
    })
}

#[derive(Debug, Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoMetadataPackage>,
    target_directory: String,
}

#[derive(Debug, Deserialize)]
struct CargoMetadataPackage {
    name: String,
    manifest_path: String,
}

fn qualify_entrypoint(crate_ident: &str, entrypoint: &str) -> Result<String, Box<dyn Error>> {
    let entrypoint = entrypoint.trim();
    if entrypoint.is_empty() {
        return Err("entrypoint cannot be empty".into());
    }
    if entrypoint.contains("::") {
        Ok(entrypoint.to_string())
    } else {
        Ok(format!("{crate_ident}::{entrypoint}"))
    }
}

fn validate_rust_path(path: &str) -> Result<(), Box<dyn Error>> {
    let valid = path
        .split("::")
        .all(|segment| !segment.is_empty() && is_rust_ident(segment));
    if valid {
        Ok(())
    } else {
        Err(format!("invalid Rust entrypoint path `{path}`").into())
    }
}

fn is_rust_ident(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|char| char == '_' || char.is_ascii_alphanumeric())
}

fn toml_string(value: impl AsRef<str>) -> String {
    serde_json::to_string(value.as_ref()).expect("string serialization should succeed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn harness_is_standalone_inside_cached_target_directory() {
        let cargo_toml = harness_cargo_toml(
            "distributed-manifest-harness-schema-postgres",
            "todo_model",
            "todo-model",
            Path::new("/tmp/todo-model"),
            Some(Path::new("/tmp/distributed")),
            &[],
            false,
        );

        assert!(cargo_toml.contains("\n[workspace]\n"));
        assert!(cargo_toml.contains("name = \"distributed-manifest-harness-schema-postgres\""));
    }

    #[test]
    fn schema_harness_uses_public_table_module_sql_dialect() {
        let main_rs = harness_main_rs(
            "orders_service::read_model_catalog",
            HarnessMode::SchemaSql(SchemaDialect::Postgres),
        );

        assert!(
            main_rs.contains("distributed::table::TableSqlDialect::Postgres"),
            "main.rs: {main_rs}"
        );
        assert!(
            !main_rs.contains("distributed::TableSqlDialect"),
            "main.rs: {main_rs}"
        );
    }

    #[test]
    fn client_harness_uses_the_target_surface_export_compiler() {
        assert_eq!(
            HarnessMode::ClientManifest.default_entrypoint(),
            "distributed_client_surface"
        );
        let main_rs = harness_main_rs(
            "orders_service::distributed_client_surface",
            HarnessMode::ClientManifest,
        );
        assert!(main_rs.contains("let export = orders_service::distributed_client_surface();"));
        assert!(!main_rs.contains("distributed::graphql::DistributedClientSurfaceExport"));
        assert!(main_rs.contains(".manifest()"));
        assert!(!main_rs.contains("build_surface"));
    }

    #[test]
    fn game_harness_prints_the_surface_export() {
        assert_eq!(
            HarnessMode::GameManifest.default_entrypoint(),
            "game_surface"
        );
        assert!(!HarnessMode::GameManifest.requires_distributed_dependency());
        let main_rs = harness_main_rs("game_demos::arena::game_surface", HarnessMode::GameManifest);
        assert!(main_rs.contains("let surface = game_demos::arena::game_surface();"));
        assert!(main_rs.contains("serde_json::to_string_pretty(&surface)"));
    }

    #[test]
    fn client_harness_does_not_add_a_second_framework_dependency() {
        assert!(!HarnessMode::ClientManifest.requires_distributed_dependency());
        assert!(!HarnessMode::DescribeJson.requires_distributed_dependency());
        assert!(HarnessMode::SchemaSql(SchemaDialect::Postgres).requires_distributed_dependency());
        assert!(HarnessMode::SchemaGraphql.requires_distributed_dependency());

        let cargo_toml = harness_cargo_toml(
            "distributed-manifest-harness-client-manifest",
            "orders_service",
            "orders-service",
            Path::new("/tmp/orders-service"),
            None,
            &[],
            false,
        );

        assert!(!cargo_toml.contains("distributed ="), "{cargo_toml}");
        assert!(cargo_toml.contains("orders_service ="), "{cargo_toml}");
    }

    #[test]
    fn harness_failure_starts_at_the_compiler_error_and_omits_dependency_warnings() {
        let stderr = "warning: unused import: SystemTime\n  --> dependency.rs:1:1\n\nerror[E0425]: cannot find function `application_manifest`\n  --> src/main.rs:2:1\n\nerror: could not compile harness\n";

        let failure = manifest_harness_error(stderr);

        assert!(failure.starts_with("error[E0425]"), "{failure}");
        assert!(failure.contains("could not compile harness"), "{failure}");
        assert!(!failure.contains("unused import"), "{failure}");
    }
}
