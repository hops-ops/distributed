//! Integration tests for `distributed skills init` / `distributed skills list`: drive the
//! real binary against temp directories and assert the extracted skill tree,
//! harness wiring, idempotent re-runs, and drift semantics. No network, no
//! repo checkout — the skills are embedded in the binary.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const SKILL_NAMES: [&str; 3] = ["distributed-usage", "distributed-ci", "distributed-schema"];
const BEGIN: &str =
    "<!-- distributed:skills:begin (managed by distributed skills init; do not edit inside) -->";
const END: &str = "<!-- distributed:skills:end -->";

/// A fresh project directory under the target tmpdir.
fn project_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// Run `distributed skills <args...>` with the given project directory as cwd.
fn distributed_skills(cwd: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_distributed"))
        .arg("skills")
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("distributed should run")
}

fn init_ok(cwd: &Path, args: &[&str]) -> (String, String) {
    let output = distributed_skills(cwd, args);
    assert!(
        output.status.success(),
        "distributed skills {args:?} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    (
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

fn read(dir: &Path, rel: &str) -> String {
    fs::read_to_string(dir.join(rel))
        .unwrap_or_else(|err| panic!("read {rel} in {}: {err}", dir.display()))
}

#[test]
fn init_bootstraps_a_fresh_project_and_reruns_are_noops() {
    let dir = project_dir("skills-fresh");
    let (stdout, _) = init_ok(&dir, &["init"]);

    // Canonical files plus both adapters — nothing detected wires everything.
    // Harness locations are per-skill symlinks resolving to the canonical copy.
    for skill in SKILL_NAMES {
        for root in [".distributed/skills", ".claude/skills", ".agents/skills"] {
            let rel = format!("{root}/{skill}/SKILL.md");
            assert!(dir.join(&rel).is_file(), "missing {rel}");
        }
        for root in [".claude/skills", ".agents/skills"] {
            let link = dir.join(root).join(skill);
            assert!(
                fs::symlink_metadata(&link)
                    .unwrap()
                    .file_type()
                    .is_symlink(),
                "{root}/{skill} should be a symlink"
            );
            assert_eq!(
                fs::read_link(&link).unwrap(),
                Path::new(&format!("../../.distributed/skills/{skill}"))
            );
        }
    }
    let agents_md = read(&dir, "AGENTS.md");
    assert!(agents_md.contains(BEGIN) && agents_md.contains(END));
    assert!(stdout.contains("created .distributed/skills/distributed-ci/SKILL.md"));
    assert!(stdout.contains("Initialized 5 skills at .distributed/skills (wired: claude, agents)"));

    // No strays at the harness skill roots (only skill folders).
    for root in [".agents/skills", ".claude/skills"] {
        for entry in fs::read_dir(dir.join(root)).unwrap() {
            let entry = entry.unwrap();
            assert!(
                entry.path().is_dir(),
                "stray file {:?} in {root}",
                entry.file_name()
            );
        }
    }

    // Re-run: everything unchanged, exit 0, AGENTS.md byte-identical.
    let (stdout, stderr) = init_ok(&dir, &["init"]);
    assert!(
        !stdout.contains("created ") && !stdout.contains("updated "),
        "{stdout}"
    );
    assert!(stderr.is_empty(), "{stderr}");
    assert!(stdout.contains("unchanged .distributed/skills/distributed-usage/SKILL.md"));
    assert_eq!(read(&dir, "AGENTS.md"), agents_md);
}

#[test]
fn local_edits_are_skipped_without_force_and_overwritten_with_it() {
    let dir = project_dir("skills-drift");
    init_ok(&dir, &["init", "--agents", "none"]);

    let edited = dir.join(".distributed/skills/distributed-ci/SKILL.md");
    fs::write(&edited, "my local notes\n").unwrap();
    let user_added = dir.join(".distributed/skills/my-own-skill/SKILL.md");
    fs::create_dir_all(user_added.parent().unwrap()).unwrap();
    fs::write(&user_added, "mine\n").unwrap();

    // Drift is a warning, not an error; the edit and user files survive.
    let (_, stderr) = init_ok(&dir, &["init", "--agents", "none"]);
    assert!(
        stderr.contains("skipped .distributed/skills/distributed-ci/SKILL.md")
            && stderr.contains("--force"),
        "stderr: {stderr}"
    );
    assert_eq!(
        read(&dir, ".distributed/skills/distributed-ci/SKILL.md"),
        "my local notes\n"
    );

    // --force converges the drifted file; user-added files are never touched.
    let (stdout, _) = init_ok(&dir, &["init", "--agents", "none", "--force"]);
    assert!(
        stdout.contains("updated .distributed/skills/distributed-ci/SKILL.md"),
        "{stdout}"
    );
    assert!(read(&dir, ".distributed/skills/distributed-ci/SKILL.md").starts_with("---\n"));
    assert_eq!(
        read(&dir, ".distributed/skills/my-own-skill/SKILL.md"),
        "mine\n"
    );
}

#[test]
fn path_flag_moves_the_container_and_anchors_wiring_at_its_parent() {
    let dir = project_dir("skills-path");
    init_ok(&dir, &["init", "--path", "some/dir", "--agents", "claude"]);

    for skill in SKILL_NAMES {
        assert!(dir
            .join(format!("some/dir/skills/{skill}/SKILL.md"))
            .is_file());
        // Wiring anchors at the container's parent, not the cwd, and the links
        // climb back to the container by its final path component.
        assert!(dir
            .join(format!("some/.claude/skills/{skill}/SKILL.md"))
            .is_file());
        assert_eq!(
            fs::read_link(dir.join(format!("some/.claude/skills/{skill}"))).unwrap(),
            Path::new(&format!("../../dir/skills/{skill}"))
        );
    }
    assert!(!dir.join(".claude").exists());
    assert!(
        !dir.join("some/AGENTS.md").exists(),
        "claude adapter edits no AGENTS.md"
    );
    assert!(!dir.join("some/.agents").exists());
}

#[test]
fn agents_none_writes_canonical_files_only() {
    let dir = project_dir("skills-none");
    let (stdout, _) = init_ok(&dir, &["init", "--agents", "none"]);
    for skill in SKILL_NAMES {
        assert!(dir
            .join(format!(".distributed/skills/{skill}/SKILL.md"))
            .is_file());
    }
    assert!(!dir.join(".claude").exists());
    assert!(!dir.join(".agents").exists());
    assert!(!dir.join("AGENTS.md").exists());
    assert!(stdout.contains("(wired: none)"), "{stdout}");
}

#[test]
fn auto_detection_follows_project_evidence() {
    // Only AGENTS.md present → agents adapter, no .claude/ created.
    let dir = project_dir("skills-auto-agents");
    fs::write(dir.join("AGENTS.md"), "# House rules\n").unwrap();
    init_ok(&dir, &["init"]);
    assert!(dir
        .join(".agents/skills/distributed-usage/SKILL.md")
        .is_file());
    assert!(!dir.join(".claude").exists());

    // Only .claude/ present → claude adapter, no .agents/ or AGENTS.md.
    let dir = project_dir("skills-auto-claude");
    fs::create_dir_all(dir.join(".claude")).unwrap();
    init_ok(&dir, &["init"]);
    assert!(dir
        .join(".claude/skills/distributed-usage/SKILL.md")
        .is_file());
    assert!(!dir.join(".agents").exists());
    assert!(!dir.join("AGENTS.md").exists());
}

#[test]
fn agents_md_managed_block_preserves_user_content() {
    let dir = project_dir("skills-agents-md");
    let before = "# My project\n\nuser prose before.\n\n";
    let after = "\n\n## After\nuser prose after.\n";
    fs::write(
        dir.join("AGENTS.md"),
        format!("{before}{BEGIN}\nstale managed contents\n{END}{after}"),
    )
    .unwrap();

    init_ok(&dir, &["init", "--agents", "codex"]);
    let merged = read(&dir, "AGENTS.md");
    assert!(
        merged.starts_with(before),
        "content before the block changed:\n{merged}"
    );
    assert!(
        merged.ends_with(after),
        "content after the block changed:\n{merged}"
    );
    assert!(!merged.contains("stale managed contents"));
    for skill in SKILL_NAMES {
        assert!(merged.contains(skill), "block should list {skill}");
    }
}

#[test]
fn existing_directory_at_a_harness_location_needs_force_to_become_a_link() {
    let dir = project_dir("skills-link-conversion");
    // An old copy-based layout (or a user skill with a colliding name).
    let stale = dir.join(".claude/skills/distributed-usage");
    fs::create_dir_all(&stale).unwrap();
    fs::write(stale.join("SKILL.md"), "old copy\n").unwrap();

    let (_, stderr) = init_ok(&dir, &["init", "--agents", "claude"]);
    assert!(
        stderr.contains("skipped .claude/skills/distributed-usage")
            && stderr.contains("--force to replace with a symlink"),
        "stderr: {stderr}"
    );
    assert!(!fs::symlink_metadata(&stale)
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(
        read(&dir, ".claude/skills/distributed-usage/SKILL.md"),
        "old copy\n"
    );

    let (stdout, _) = init_ok(&dir, &["init", "--agents", "claude", "--force"]);
    assert!(
        stdout.contains(
            "updated .claude/skills/distributed-usage -> ../../.distributed/skills/distributed-usage"
        ),
        "{stdout}"
    );
    assert!(fs::symlink_metadata(&stale)
        .unwrap()
        .file_type()
        .is_symlink());
    assert!(read(&dir, ".claude/skills/distributed-usage/SKILL.md").starts_with("---\n"));
}

#[test]
fn target_path_that_is_a_file_is_a_clear_error() {
    let dir = project_dir("skills-path-is-file");
    fs::write(dir.join(".distributed"), "not a directory").unwrap();

    let output = distributed_skills(&dir, &["init"]);
    assert!(!output.status.success(), "init should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not a directory"), "stderr: {stderr}");
}

#[test]
fn list_prints_every_skill_with_its_description() {
    let dir = project_dir("skills-list");
    let output = distributed_skills(&dir, &["list"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    for skill in SKILL_NAMES {
        assert!(stdout.contains(skill), "missing {skill}: {stdout}");
    }
    assert!(
        stdout.contains("Use when"),
        "descriptions should be printed: {stdout}"
    );
}
