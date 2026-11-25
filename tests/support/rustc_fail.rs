//! Compile-fail checks that assert by substring instead of a full snapshot.
//!
//! trybuild pins the whole rustc rendering, which is right for a macro's own
//! `compile_error!` text and wrong for a trait-bound or type error: rustc's
//! notes, "and N others" lists and standard-library spans change between
//! releases. These cases only need to fail to build with a message that names
//! the cause, so each one lists the substrings its error must contain.
//!
//! Every fixture in `dir` becomes one binary of a scratch crate; a single
//! `cargo check --keep-going --message-format=json` reports each binary's
//! errors. The scratch crate builds in its own target directory (a nested
//! cargo must not wait on the outer build's lock) and reuses the workspace
//! `Cargo.lock`, so dependency versions match the workspace.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Assert that every fixture `<dir>/<name>.rs` fails to build with errors that
/// contain each listed substring, and that `cases` names every fixture in `dir`.
///
/// `dependencies` is the scratch crate's `[dependencies]` table body; the
/// placeholder `{workspace}` is replaced by the workspace root path.
pub fn assert_fail_with(
    manifest_dir: &str,
    dir: &str,
    dependencies: &str,
    cases: &[(&str, &[&str])],
) {
    let manifest_dir = Path::new(manifest_dir);
    let workspace = manifest_dir
        .ancestors()
        .find(|path| path.join("Cargo.lock").is_file())
        .expect("a workspace Cargo.lock above the crate");
    let fixtures = manifest_dir.join(dir);

    let on_disk: BTreeSet<String> = std::fs::read_dir(&fixtures)
        .unwrap_or_else(|error| panic!("read {}: {error}", fixtures.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .map(|path| path.file_stem().unwrap().to_string_lossy().into_owned())
        .collect();
    let listed: BTreeSet<String> = cases.iter().map(|(name, _)| name.to_string()).collect();
    assert_eq!(on_disk, listed, "every fixture needs expected substrings");

    let target_root = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace.join("target"))
        .join("tests")
        .join("rustc-fail");
    let package = format!(
        "{}-rustc-fail",
        manifest_dir.file_name().unwrap().to_string_lossy()
    );
    let project = target_root.join(&package);
    std::fs::create_dir_all(&project).unwrap();

    let mut manifest = format!(
        "[package]\nname = \"{package}\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\
         publish = false\nautobins = false\n\n[workspace]\n\n[dependencies]\n{}\n",
        dependencies.replace("{workspace}", &workspace.display().to_string())
    );
    for (name, _) in cases {
        manifest.push_str(&format!(
            "\n[[bin]]\nname = \"{name}\"\npath = \"{}\"\n",
            fixtures.join(format!("{name}.rs")).display()
        ));
    }
    std::fs::write(project.join("Cargo.toml"), manifest).unwrap();
    std::fs::copy(workspace.join("Cargo.lock"), project.join("Cargo.lock")).unwrap();

    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args(["check", "--bins", "--keep-going", "--message-format=json"])
        .arg("--manifest-path")
        .arg(project.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target_root.join("target"))
        .output()
        .expect("run cargo check");
    let stderr = String::from_utf8_lossy(&output.stderr);

    let mut errors: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Ok(message) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if message["reason"] != "compiler-message" || message["message"]["level"] != "error" {
            continue;
        }
        let target = message["target"]["name"].as_str().unwrap_or_default();
        let rendered = message["message"]["rendered"].as_str().unwrap_or_default();
        errors
            .entry(target.to_owned())
            .or_default()
            .push(rendered.to_owned());
    }

    for (name, expected) in cases {
        let rendered = errors
            .get(*name)
            .map(|errors| errors.join("\n"))
            .unwrap_or_else(|| panic!("`{name}` built without an error; cargo stderr:\n{stderr}"));
        for needle in *expected {
            assert!(
                rendered.contains(needle),
                "`{name}` failed without `{needle}`:\n{rendered}"
            );
        }
    }
}
