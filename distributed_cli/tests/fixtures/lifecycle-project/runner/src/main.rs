// A dev process of the lifecycle test project: the UI when given an address,
// the API (which binds `BIND`) otherwise. (`ui/src/main.rs` includes this
// file, so it has no inner doc comment.)
//
// It starts a descendant that ignores SIGTERM and records itself in the
// project root, then listens.

use std::io::Write;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Stdio};

fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("a dev process is a member of the project");
    let (name, address) = match std::env::args().nth(1) {
        Some(address) => ("ui", address),
        None => ("api", std::env::var("BIND").expect("the API is given BIND")),
    };
    let descendant = Command::new("/bin/sh")
        .args(["-c", r#"trap "" TERM; while :; do sleep 1; done"#])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start descendant");
    let record = |file: &str, line: String| {
        let mut log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join(file))
            .expect("open dev process log");
        // One write, so a reader never sees half of the record.
        log.write_all(format!("{line}\n").as_bytes())
            .expect("record dev process");
    };
    let generation = std::env::var("DISTRIBUTED_GENERATION_ID").unwrap_or_default();
    let directory = std::env::current_dir().expect("working directory");
    record(
        "dev-environment.log",
        format!("{name}:{}", directory.display()),
    );
    record(
        "dev-family.log",
        format!("{} {}", std::process::id(), descendant.id()),
    );
    record("dev-process.log", format!("{name}:{generation}"));
    // Readiness is a connection, so whoever sees it also sees the records.
    let listener = TcpListener::bind(&address).expect("bind the dev process address");
    for connection in listener.incoming() {
        drop(connection);
    }
}
