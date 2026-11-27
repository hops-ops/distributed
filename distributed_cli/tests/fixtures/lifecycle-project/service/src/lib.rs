//! The typed application of the lifecycle test project.
//!
//! `distributed describe` compiles and runs `application_manifest`. A test
//! steers that run with marker files in the project root, and changes the
//! manifest by editing `name.txt`.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

pub fn application_manifest() -> distributed::application::ApplicationManifest {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the service is a member of the project");
    if root.join("fail-manifest").exists() {
        eprintln!("injected manifest failure");
        std::process::exit(17);
    }
    if root.join("slow-manifest").exists() {
        // A descendant that ignores SIGTERM, with none of this process's pipes.
        let descendant = Command::new("/bin/sh")
            .args(["-c", r#"trap "" TERM; while :; do sleep 1; done"#])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start descendant");
        let mut log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join("build-family.log"))
            .expect("open build-family.log");
        // One write, so a reader never sees half of the record.
        log.write_all(format!("{} {}\n", std::process::id(), descendant.id()).as_bytes())
            .expect("record processes");
        while root.join("slow-manifest").exists() {
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    distributed::application::ApplicationManifest::new(include_str!("name.txt").trim())
}
