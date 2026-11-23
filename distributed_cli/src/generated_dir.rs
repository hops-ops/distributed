//! The one writer for a directory a generator owns.
//!
//! Both client generators (`distributed client`, `distributed game-client`)
//! write and check their output through [`GeneratedDir`], so both get the same
//! guarantees:
//!
//! - nothing is written through a symlink;
//! - an unchanged file is left untouched, so a watching dev server does not
//!   reload for nothing;
//! - a file the generator produced before and no longer produces is removed;
//! - a file the generator cannot prove it produced is never removed or
//!   overwritten around: the write is refused before anything changes.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::path::{Component, Path, PathBuf};

use crate::client_compiler::GeneratedClientFile;

const MAX_FILES: usize = 8_192;
/// Largest existing file read to decide whether the generator owns it.
const MAX_OWNED_FILE_BYTES: u64 = 8 * 1024 * 1024;

pub(crate) struct GeneratedDir<'a> {
    pub(crate) root: &'a Path,
    /// Whether the generator owns the files in subdirectories too. One that
    /// owns only the top level leaves every subdirectory to its own writer.
    pub(crate) nested: bool,
}

impl GeneratedDir<'_> {
    /// Make the directory hold exactly `files`, writing them in the order
    /// given.
    ///
    /// `owned(path, contents)` says whether an existing file that is not in
    /// `files` was produced by this generator. Owned files are removed; any
    /// other makes the write fail before anything is changed.
    pub(crate) fn write(
        &self,
        files: &[GeneratedClientFile],
        owned: impl Fn(&str, &str) -> Result<bool, Box<dyn Error>>,
    ) -> Result<(), Box<dyn Error>> {
        let expected = expected(files)?;
        self.refuse_symlinks(files)?;
        let mut stale = Vec::new();
        let mut foreign = Vec::new();
        for path in self.existing()? {
            if expected.contains_key(path.as_str()) {
                continue;
            }
            let full_path = self.root.join(&path);
            // A file too large or not UTF-8 to inspect is nobody's proof.
            let contents = fs::metadata(&full_path)
                .is_ok_and(|metadata| metadata.len() <= MAX_OWNED_FILE_BYTES)
                .then(|| fs::read_to_string(&full_path).ok())
                .flatten();
            let owned = match contents {
                Some(contents) => owned(&path, &contents)?,
                None => false,
            };
            if owned {
                stale.push(full_path);
            } else {
                foreign.push(path);
            }
        }
        if !foreign.is_empty() {
            return Err(format!(
                "generated output contains files without current or previous compiler ownership:\n  {}; refusing to write until they are moved or removed",
                foreign.join("\n  ")
            )
            .into());
        }

        fs::create_dir_all(self.root)?;
        for path in stale {
            fs::remove_file(&path).map_err(|error| {
                format!("remove stale generated file {}: {error}", path.display())
            })?;
        }
        for file in files {
            let path = self.root.join(&file.path);
            if fs::read(&path).is_ok_and(|current| current == file.contents.as_bytes()) {
                continue;
            }
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&path, &file.contents)
                .map_err(|error| format!("write generated file {}: {error}", path.display()))?;
        }
        Ok(())
    }

    /// How the directory differs from `files`: `changed`, `missing` and
    /// `unexpected` paths, sorted. Empty when it holds exactly `files`.
    /// Reads only.
    pub(crate) fn drift(
        &self,
        files: &[GeneratedClientFile],
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let expected = expected(files)?;
        let mut drift = Vec::new();
        for (path, contents) in &expected {
            let full_path = self.root.join(path);
            match fs::read(&full_path) {
                Ok(actual) if actual == contents.as_bytes() => {}
                Ok(_) => drift.push(format!("changed {path}")),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    drift.push(format!("missing {path}"));
                }
                Err(error) => {
                    return Err(
                        format!("read generated file {}: {error}", full_path.display()).into(),
                    )
                }
            }
        }
        for path in self.existing()? {
            if !expected.contains_key(path.as_str()) {
                drift.push(format!("unexpected {path}"));
            }
        }
        drift.sort();
        Ok(drift)
    }

    /// The files in the generator's part of the directory, as portable
    /// relative paths. Empty when the directory does not exist.
    fn existing(&self) -> Result<BTreeSet<String>, Box<dyn Error>> {
        let mut files = BTreeSet::new();
        if !self.root_exists()? {
            return Ok(files);
        }
        let mut pending = vec![(self.root.to_path_buf(), String::new())];
        while let Some((directory, prefix)) = pending.pop() {
            for entry in fs::read_dir(&directory)? {
                let entry = entry?;
                let file_type = entry.file_type()?;
                let name = entry.file_name();
                let name = name.to_str().ok_or_else(|| {
                    format!(
                        "generated output contains a non-UTF-8 name {}",
                        entry.path().display()
                    )
                })?;
                let relative = format!("{prefix}{name}");
                if file_type.is_file() {
                    files.insert(relative);
                    if files.len() > MAX_FILES {
                        return Err(format!(
                            "generated output {} exceeds {MAX_FILES} files",
                            self.root.display()
                        )
                        .into());
                    }
                } else if !self.nested {
                    // Not this generator's entry.
                } else if file_type.is_dir() {
                    pending.push((entry.path(), format!("{relative}/")));
                } else {
                    return Err(format!(
                        "generated output contains unsupported {} {}",
                        if file_type.is_symlink() {
                            "symlink"
                        } else {
                            "entry"
                        },
                        entry.path().display()
                    )
                    .into());
                }
            }
        }
        Ok(files)
    }

    /// Whether the root exists. One that is not a real directory is an error.
    fn root_exists(&self) -> Result<bool, Box<dyn Error>> {
        match fs::symlink_metadata(self.root) {
            Ok(metadata) if metadata.is_dir() => Ok(true),
            Ok(_) => Err(format!(
                "generated output {} must be a real directory, not a symlink or other entry",
                self.root.display()
            )
            .into()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => {
                Err(format!("inspect generated output {}: {error}", self.root.display()).into())
            }
        }
    }

    /// Refuse a root, a parent directory or a target file that is a symlink
    /// (or anything else than the directory or regular file it must be).
    fn refuse_symlinks(&self, files: &[GeneratedClientFile]) -> Result<(), Box<dyn Error>> {
        if !self.root_exists()? {
            return Ok(());
        }
        for file in files {
            let mut current = self.root.to_path_buf();
            let mut components = Path::new(&file.path).components().peekable();
            while let Some(component) = components.next() {
                current.push(component);
                let metadata = match fs::symlink_metadata(&current) {
                    Ok(metadata) => metadata,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                    Err(error) => {
                        return Err(format!(
                            "inspect generated path {}: {error}",
                            current.display()
                        )
                        .into())
                    }
                };
                // `symlink_metadata` does not follow links, so a link is
                // neither a file nor a directory here.
                let valid = if components.peek().is_none() {
                    metadata.is_file()
                } else {
                    metadata.is_dir()
                };
                if !valid {
                    return Err(format!(
                        "generated path {} contains a symlink or incompatible entry",
                        current.display()
                    )
                    .into());
                }
            }
        }
        Ok(())
    }
}

/// `files` by path, after checking every path stays inside the directory and
/// names one file.
fn expected(files: &[GeneratedClientFile]) -> Result<BTreeMap<&str, &str>, Box<dyn Error>> {
    if files.is_empty() || files.len() > MAX_FILES {
        return Err(format!(
            "generator produced {} files; expected 1..={MAX_FILES}",
            files.len()
        )
        .into());
    }
    let mut expected = BTreeMap::new();
    for file in files {
        let portable = !file.path.is_empty()
            && !file.path.contains('\\')
            && PathBuf::from(&file.path)
                .components()
                .all(|component| matches!(component, Component::Normal(_)))
            && !file.path.split('/').any(str::is_empty);
        if !portable {
            return Err(format!("generator produced unsafe path `{}`", file.path).into());
        }
        if expected
            .insert(file.path.as_str(), file.contents.as_str())
            .is_some()
        {
            return Err(format!("generator produced duplicate path `{}`", file.path).into());
        }
    }
    Ok(expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MARKER: &str = "// generated";

    fn file(path: &str, contents: &str) -> GeneratedClientFile {
        GeneratedClientFile {
            path: path.to_string(),
            contents: contents.to_string(),
        }
    }

    fn marked(_: &str, contents: &str) -> Result<bool, Box<dyn Error>> {
        Ok(contents.starts_with(MARKER))
    }

    fn tree(root: &Path) -> BTreeMap<String, String> {
        let mut files = BTreeMap::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    files.insert(
                        path.strip_prefix(root)
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_string(),
                        fs::read_to_string(&path).unwrap(),
                    );
                }
            }
        }
        files
    }

    #[test]
    fn write_converges_and_leaves_unchanged_files_untouched() {
        let fixture = tempfile::tempdir().unwrap();
        let root = fixture.path().join("out");
        let dir = GeneratedDir {
            root: &root,
            nested: true,
        };
        let first = [
            file("index.ts", "// generated\nindex 1\n"),
            file("operations/a.ts", "// generated\na\n"),
            file("operations/b.ts", "// generated\nb\n"),
        ];
        assert_eq!(
            dir.drift(&first).unwrap(),
            [
                "missing index.ts",
                "missing operations/a.ts",
                "missing operations/b.ts"
            ]
        );
        dir.write(&first, marked).unwrap();
        assert!(dir.drift(&first).unwrap().is_empty());

        // An unchanged file keeps its inode contents and its modification
        // time; a changed one is rewritten; a dropped one is removed.
        let old = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        let unchanged = fs::File::options()
            .write(true)
            .open(root.join("operations/a.ts"))
            .unwrap();
        unchanged.set_modified(old).unwrap();
        let second = [
            file("index.ts", "// generated\nindex 2\n"),
            file("operations/a.ts", "// generated\na\n"),
        ];
        assert_eq!(
            dir.drift(&second).unwrap(),
            ["changed index.ts", "unexpected operations/b.ts"]
        );
        dir.write(&second, marked).unwrap();
        assert_eq!(
            fs::metadata(root.join("operations/a.ts"))
                .unwrap()
                .modified()
                .unwrap(),
            old
        );
        assert_eq!(
            tree(&root),
            BTreeMap::from([
                (
                    "index.ts".to_string(),
                    "// generated\nindex 2\n".to_string()
                ),
                (
                    "operations/a.ts".to_string(),
                    "// generated\na\n".to_string()
                ),
            ])
        );
    }

    #[test]
    fn write_refuses_a_file_it_does_not_own_before_changing_anything() {
        let fixture = tempfile::tempdir().unwrap();
        let root = fixture.path().join("out");
        let dir = GeneratedDir {
            root: &root,
            nested: true,
        };
        dir.write(
            &[
                file("a.ts", "// generated\na\n"),
                file("b.ts", "// generated\nb\n"),
            ],
            marked,
        )
        .unwrap();
        fs::write(root.join("mine.ts"), "export const mine = 1;\n").unwrap();
        let before = tree(&root);

        let error = dir
            .write(&[file("a.ts", "// generated\nchanged\n")], marked)
            .unwrap_err()
            .to_string();
        assert!(error.contains("mine.ts"), "{error}");
        assert!(!error.contains("b.ts"), "{error}");
        assert_eq!(tree(&root), before);

        // The generator's own reason for refusing a file is reported as is.
        let error = dir
            .write(&[file("a.ts", "// generated\nchanged\n")], |path, _| {
                Err(format!("{path} was edited").into())
            })
            .unwrap_err()
            .to_string();
        assert!(error.ends_with("was edited"), "{error}");
        assert_eq!(tree(&root), before);
    }

    #[test]
    fn a_top_level_generator_leaves_subdirectories_alone() {
        let fixture = tempfile::tempdir().unwrap();
        let root = fixture.path().join("out");
        fs::create_dir_all(root.join("wasm")).unwrap();
        fs::write(root.join("wasm/module.wasm"), "binary").unwrap();
        let dir = GeneratedDir {
            root: &root,
            nested: false,
        };
        let files = [file("schema.ts", "// generated\n")];
        dir.write(&files, marked).unwrap();
        assert!(dir.drift(&files).unwrap().is_empty());
        assert_eq!(
            fs::read_to_string(root.join("wasm/module.wasm")).unwrap(),
            "binary"
        );

        let nested = GeneratedDir {
            root: &root,
            nested: true,
        };
        assert_eq!(
            nested.drift(&files).unwrap(),
            ["unexpected wasm/module.wasm"]
        );
    }

    #[test]
    fn unsafe_and_duplicate_paths_are_rejected() {
        let fixture = tempfile::tempdir().unwrap();
        let dir = GeneratedDir {
            root: &fixture.path().join("out"),
            nested: true,
        };
        for path in [
            "../escape.ts",
            "/absolute.ts",
            "a//b.ts",
            "./a.ts",
            "a\\b.ts",
            "",
        ] {
            let error = dir
                .write(&[file(path, "x")], marked)
                .unwrap_err()
                .to_string();
            assert!(error.contains("unsafe path"), "{path}: {error}");
        }
        let error = dir
            .write(&[file("a.ts", "x"), file("a.ts", "y")], marked)
            .unwrap_err()
            .to_string();
        assert!(error.contains("duplicate path"), "{error}");
        assert!(dir.write(&[], marked).is_err());
        assert!(!fixture.path().join("out").exists());
    }

    #[cfg(unix)]
    #[test]
    fn nothing_is_written_through_a_symlink() {
        use std::os::unix::fs::symlink;

        let fixture = tempfile::tempdir().unwrap();
        let outside = fixture.path().join("outside");
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("victim.ts"), "untouched").unwrap();
        let files = [file("victim.ts", "// generated\n")];

        // The root is a link.
        let linked_root = fixture.path().join("linked-root");
        symlink(&outside, &linked_root).unwrap();
        for nested in [false, true] {
            let dir = GeneratedDir {
                root: &linked_root,
                nested,
            };
            assert!(dir.write(&files, marked).is_err());
            assert!(dir.drift(&files).is_err());
        }

        // The target file is a link, in a generator that owns only the top level.
        let root = fixture.path().join("out");
        fs::create_dir_all(&root).unwrap();
        symlink(outside.join("victim.ts"), root.join("victim.ts")).unwrap();
        let dir = GeneratedDir {
            root: &root,
            nested: false,
        };
        let error = dir.write(&files, marked).unwrap_err().to_string();
        assert!(error.contains("contains a symlink"), "{error}");

        // A parent directory is a link.
        fs::remove_file(root.join("victim.ts")).unwrap();
        symlink(&outside, root.join("operations")).unwrap();
        let dir = GeneratedDir {
            root: &root,
            nested: true,
        };
        let error = dir
            .write(&[file("operations/victim.ts", "// generated\n")], marked)
            .unwrap_err()
            .to_string();
        assert!(error.contains("contains a symlink"), "{error}");

        assert_eq!(
            fs::read_to_string(outside.join("victim.ts")).unwrap(),
            "untouched"
        );
    }
}
