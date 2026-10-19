//! `distributed game-wasm`: compile a zone's client-predicted step
//! (`#[predict]`, recorded in its `GameSurface`) to WebAssembly next to the
//! generated client.
//!
//! The surface names the Cargo package that declares the step. The command
//! builds that package's library as a `cdylib` for `wasm32-unknown-unknown`
//! (`cargo rustc --release`), checks the module speaks the prediction ABI
//! (no imports; exports `memory`, `dg_predict_buffer`, `dg_predict_abi`
//! and `dg_predict_<step>`) and writes it to `<out>/wasm/<surface>.wasm`,
//! which the generated `predict.ts` loads. No wasm-bindgen glue, no JSON:
//! the SDK instantiates the module directly.
//!
//! A stamp next to the module (`<surface>.stamp.json`) records the source
//! fingerprint of the package and its local dependencies (the same one the
//! browser WASM pures use), the compiler and the module's hash, so an
//! unchanged step is not rebuilt and `--check` can verify a build is current
//! without compiling.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::game_client::{predict_wasm_path, surface_prediction};
use crate::wasm_pures::{cargo_metadata, ensure_real_directory_path, package_source_identity};

const TARGET: &str = "wasm32-unknown-unknown";
const STAMP_VERSION: u32 = 1;
const MAX_MODULE_BYTES: u64 = 64 * 1024 * 1024;

/// What to build.
pub(crate) struct GameWasmOptions<'a> {
    /// Surface JSON text.
    pub(crate) surface: &'a str,
    /// Cargo.toml of the project containing the declaring package.
    pub(crate) cargo_manifest: &'a Path,
    /// Generated client directory.
    pub(crate) out: &'a Path,
    /// Verify only.
    pub(crate) check: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Stamp {
    version: u32,
    package: String,
    export: String,
    abi: u32,
    source_identity: String,
    compiler_identity: String,
    module_sha256: String,
}

/// Outcome of a run.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum GameWasmOutcome {
    /// The module on disk matches the sources.
    Current(PathBuf),
    /// The module was (re)built.
    Built(PathBuf),
}

pub(crate) fn run_game_wasm(
    options: &GameWasmOptions<'_>,
) -> Result<GameWasmOutcome, Box<dyn Error>> {
    let (surface, predict) = surface_prediction(options.surface)?;
    let predict = predict.ok_or_else(|| {
        format!(
            "game surface `{surface}` declares no predicted step; add `.predict::<Step>()` (a `#[predict]` fn) to its GameSurface"
        )
    })?;
    let metadata = cargo_metadata(options.cargo_manifest)?;
    let mut candidates = metadata
        .packages
        .iter()
        .filter(|p| p.name == predict.package && p.source.is_none());
    let package = candidates.next().ok_or_else(|| {
        format!(
            "predicted step `{}` is declared by Cargo package `{}`, which is not a local package of {}",
            predict.export,
            predict.package,
            options.cargo_manifest.display()
        )
    })?;
    if candidates.next().is_some() {
        return Err(format!(
            "more than one local Cargo package is named `{}`",
            predict.package
        )
        .into());
    }
    let lib = package
        .targets
        .iter()
        .find(|t| {
            t.kind
                .iter()
                .any(|k| matches!(k.as_str(), "lib" | "rlib" | "cdylib"))
        })
        .ok_or_else(|| format!("Cargo package `{}` has no library target", package.name))?;
    let lib_name = lib.name.replace('-', "_");

    let module_rel = predict_wasm_path(&surface);
    let module_path = options.out.join(&module_rel);
    let stamp_path = module_path.with_extension("stamp.json");
    let mut expected = Stamp {
        version: STAMP_VERSION,
        package: predict.package.clone(),
        export: predict.export.clone(),
        abi: predict.abi,
        source_identity: package_source_identity(&metadata, &package.id)?,
        compiler_identity: compiler_identity()?,
        module_sha256: String::new(),
    };
    if stamp_is_current(&stamp_path, &module_path, &expected)? {
        return Ok(GameWasmOutcome::Current(module_path));
    }
    if options.check {
        return Err(format!(
            "compiled step {} is missing or stale; run `distributed game-wasm` without --check",
            module_path.display()
        )
        .into());
    }

    eprintln!(
        "distributed: compiling predicted step `{}` (Cargo package {}) to WebAssembly",
        predict.export, predict.package
    );
    let status = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .arg("rustc")
        .arg("--manifest-path")
        .arg(options.cargo_manifest)
        .args([
            "-p",
            &predict.package,
            "--lib",
            "--release",
            "--target",
            TARGET,
        ])
        .args(["--crate-type", "cdylib"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .status()
        .map_err(|error| format!("failed to run cargo for the predicted step: {error}"))?;
    if !status.success() {
        return Err(format!(
            "compiling predicted step `{}` for {TARGET} failed ({status}); is the target installed (`rustup target add {TARGET}`)?",
            predict.export
        )
        .into());
    }
    let artifact = metadata
        .target_directory
        .join(TARGET)
        .join("release")
        .join(format!("{lib_name}.wasm"));
    let size = fs::metadata(&artifact)
        .map_err(|error| format!("cargo produced no {}: {error}", artifact.display()))?
        .len();
    if size > MAX_MODULE_BYTES {
        return Err(format!(
            "{} is larger than {MAX_MODULE_BYTES} bytes",
            artifact.display()
        )
        .into());
    }
    let module = fs::read(&artifact)?;
    check_module(&module, &predict.export)
        .map_err(|error| format!("{}: {error}", artifact.display()))?;
    expected.module_sha256 = format!("{:x}", Sha256::digest(&module));

    let wasm_dir = module_path.parent().expect("module path has a directory");
    fs::create_dir_all(options.out)?;
    let out_root = options.out.canonicalize()?;
    ensure_real_directory_path(&out_root, &out_root.join("wasm"))?;
    let staged = wasm_dir.join(format!(".{surface}.wasm.tmp"));
    fs::write(&staged, &module)?;
    fs::rename(&staged, &module_path)?;
    fs::write(&stamp_path, serde_json::to_string_pretty(&expected)? + "\n")?;
    Ok(GameWasmOutcome::Built(module_path))
}

fn stamp_is_current(
    stamp_path: &Path,
    module_path: &Path,
    expected: &Stamp,
) -> Result<bool, Box<dyn Error>> {
    let Ok(text) = fs::read_to_string(stamp_path) else {
        return Ok(false);
    };
    let Ok(stamp) = serde_json::from_str::<Stamp>(&text) else {
        return Ok(false);
    };
    let Ok(module) = fs::read(module_path) else {
        return Ok(false);
    };
    let actual = format!("{:x}", Sha256::digest(&module));
    Ok(stamp.module_sha256 == actual
        && Stamp {
            module_sha256: actual,
            ..expected.clone()
        } == stamp)
}

/// `rustc -vV` (the toolchain cargo will use, including rustup overrides).
fn compiler_identity() -> Result<String, Box<dyn Error>> {
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let output = Command::new(rustc)
        .arg("-vV")
        .output()
        .map_err(|error| format!("failed to run `rustc -vV`: {error}"))?;
    if !output.status.success() {
        return Err("`rustc -vV` failed".into());
    }
    Ok(format!("sha256:{:x}", Sha256::digest(&output.stdout)))
}

/// The module imports nothing and exports the prediction ABI.
pub(crate) fn check_module(module: &[u8], export: &str) -> Result<(), String> {
    let sections = wasm_sections(module)?;
    let mut exports = Vec::new();
    for (id, body) in sections {
        match id {
            2 => {
                let mut r = WasmReader::new(body);
                if r.u32()? != 0 {
                    return Err("the module imports functions or memory; a predicted step must be self-contained (no wasm-bindgen, no host calls)".into());
                }
            }
            7 => {
                let mut r = WasmReader::new(body);
                for _ in 0..r.u32()? {
                    let name = r.name()?;
                    let kind = r.byte()?;
                    let _index = r.u32()?;
                    exports.push((name, kind));
                }
            }
            _ => {}
        }
    }
    let step = format!("dg_predict_{export}");
    for (name, kind) in [
        ("memory", 2u8),
        ("dg_predict_buffer", 0),
        ("dg_predict_abi", 0),
        (step.as_str(), 0),
    ] {
        if !exports.iter().any(|(n, k)| n == name && *k == kind) {
            return Err(format!(
                "the module does not export `{name}` (is the step declared with `#[predict]` in this package?)"
            ));
        }
    }
    Ok(())
}

fn wasm_sections(module: &[u8]) -> Result<Vec<(u8, &[u8])>, String> {
    if module.len() < 8 || &module[..4] != b"\0asm" || module[4..8] != [1, 0, 0, 0] {
        return Err("not a WebAssembly v1 module".into());
    }
    let mut r = WasmReader::new(&module[8..]);
    let mut sections = Vec::new();
    while !r.done() {
        let id = r.byte()?;
        let len = r.u32()? as usize;
        sections.push((id, r.take(len)?));
    }
    Ok(sections)
}

struct WasmReader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> WasmReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn done(&self) -> bool {
        self.at >= self.bytes.len()
    }

    fn byte(&mut self) -> Result<u8, String> {
        let b = *self.bytes.get(self.at).ok_or("truncated module")?;
        self.at += 1;
        Ok(b)
    }

    fn u32(&mut self) -> Result<u32, String> {
        let mut value = 0u32;
        for shift in (0..35).step_by(7) {
            let b = self.byte()?;
            value |= u32::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err("malformed LEB128".into())
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self.at.checked_add(len).filter(|e| *e <= self.bytes.len());
        let end = end.ok_or("truncated section")?;
        let out = &self.bytes[self.at..end];
        self.at = end;
        Ok(out)
    }

    fn name(&mut self) -> Result<String, String> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec()).map_err(|_| "export name is not UTF-8".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leb(mut v: u32, out: &mut Vec<u8>) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                return;
            }
            out.push(b | 0x80);
        }
    }

    fn module(imports: u32, exports: &[(&str, u8)]) -> Vec<u8> {
        let mut out = b"\0asm\x01\0\0\0".to_vec();
        let mut body = Vec::new();
        leb(imports, &mut body);
        out.push(2);
        leb(body.len() as u32, &mut out);
        out.extend(&body);
        let mut body = Vec::new();
        leb(exports.len() as u32, &mut body);
        for (name, kind) in exports {
            leb(name.len() as u32, &mut body);
            body.extend(name.as_bytes());
            body.push(*kind);
            leb(0, &mut body);
        }
        out.push(7);
        leb(body.len() as u32, &mut out);
        out.extend(&body);
        out
    }

    const ABI: [(&str, u8); 4] = [
        ("memory", 2),
        ("dg_predict_buffer", 0),
        ("dg_predict_abi", 0),
        ("dg_predict_arena_step", 0),
    ];

    #[test]
    fn accepts_a_self_contained_module_with_the_abi() {
        check_module(&module(0, &ABI), "arena_step").unwrap();
    }

    #[test]
    fn rejects_imports_missing_exports_and_garbage() {
        let error = check_module(&module(1, &ABI), "arena_step").unwrap_err();
        assert!(error.contains("imports"), "{error}");
        let error = check_module(&module(0, &ABI), "grid_step").unwrap_err();
        assert!(error.contains("dg_predict_grid_step"), "{error}");
        assert!(check_module(b"nope", "x").is_err());
        let mut truncated = module(0, &ABI);
        truncated.truncate(truncated.len() - 3);
        assert!(check_module(&truncated, "arena_step").is_err());
    }
}
