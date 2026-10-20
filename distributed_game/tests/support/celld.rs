//! An isolated `celld dev` serving `tests/celld/fact-worker`, owned by one
//! test: its own temp project, port and process tree (killed on drop; only
//! processes this harness started). Skips (returns `None`) when the worker
//! is not built or celld cannot start, unless `CELLD_FACTS_REQUIRED=1`.

#![allow(dead_code)]

use std::io::Write as _;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub const SECRET: &str = "test-only-internal-secret-change-me-2026";

fn worker_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/celld/fact-worker")
}

pub struct Celld {
    project: PathBuf,
    port: u16,
    child: Option<Child>,
    generation: u32,
}

impl Celld {
    pub fn start_new() -> Option<Self> {
        let build = worker_dir().join("build");
        if !build.join("worker/shim.mjs").exists() {
            assert!(
                std::env::var_os("CELLD_FACTS_REQUIRED").is_none(),
                "CELLD_FACTS_REQUIRED is set but the fact worker is not built"
            );
            eprintln!("skipping celld fact routing: build tests/celld/fact-worker first");
            return None;
        }
        let project = std::env::temp_dir().join(format!(
            "distributed-celld-facts-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        copy_dir(&build, &project.join("build"));
        std::fs::copy(
            worker_dir().join("wrangler.jsonc"),
            project.join("wrangler.jsonc"),
        )
        .unwrap();
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut celld = Self {
            project,
            port,
            child: None,
            generation: 0,
        };
        if let Err(reason) = celld.start() {
            assert!(
                std::env::var_os("CELLD_FACTS_REQUIRED").is_none(),
                "celld dev is required but did not start: {reason}"
            );
            eprintln!("skipping celld fact routing: {reason}");
            return None;
        }
        Some(celld)
    }

    pub fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Start `celld dev`; `Err` when it is unavailable or exits early (no
    /// celld on PATH, or no esbuild for its bundler).
    pub fn start(&mut self) -> Result<(), String> {
        self.generation += 1;
        let binary = std::env::var("CELLD").unwrap_or_else(|_| "celld".into());
        let help = Command::new(&binary)
            .args(["dev", "--help"])
            .output()
            .map_err(|error| format!("cannot run `{binary}`: {error}"))?;
        let mut args = vec![
            "dev".to_string(),
            self.project.display().to_string(),
            "--host".into(),
            "127.0.0.1".into(),
            "--port".into(),
            self.port.to_string(),
            "--logs".into(),
        ];
        if String::from_utf8_lossy(&help.stdout).contains("--no-watch") {
            args.push("--no-watch".into());
        }
        let log_path = self.project.join(format!("celld-{}.log", self.generation));
        let log = std::fs::File::create(&log_path).unwrap();
        let child = Command::new(&binary)
            .args(&args)
            .env("RUST_LOG", "warn")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .map_err(|error| format!("cannot start `{binary} dev`: {error}"))?;
        self.child = Some(child);
        let deadline = Instant::now() + Duration::from_secs(90);
        while Instant::now() < deadline {
            if std::net::TcpStream::connect(("127.0.0.1", self.port)).is_ok()
                && blocking_get(&format!("{}/health", self.base())).is_some_and(|s| s == 200)
            {
                return Ok(());
            }
            let exited = self
                .child
                .as_mut()
                .and_then(|child| child.try_wait().ok().flatten());
            if let Some(status) = exited {
                self.child = None;
                let log = std::fs::read_to_string(&log_path).unwrap_or_default();
                return Err(format!(
                    "celld dev exited ({status}): {}",
                    log.lines().rev().take(3).collect::<Vec<_>>().join(" | ")
                ));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        Err(format!(
            "celld dev did not become ready; logs in {}",
            self.project.display()
        ))
    }

    /// Kill the launcher this test started and every descendant.
    pub fn crash(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let root = child.id();
        let _ = Command::new("kill")
            .args(["-STOP", &root.to_string()])
            .status();
        let table = Command::new("ps")
            .args(["-axo", "pid=,ppid="])
            .output()
            .unwrap();
        let pairs = String::from_utf8_lossy(&table.stdout)
            .lines()
            .filter_map(|line| {
                let mut parts = line.split_whitespace();
                Some((
                    parts.next()?.parse::<u32>().ok()?,
                    parts.next()?.parse::<u32>().ok()?,
                ))
            })
            .collect::<Vec<_>>();
        let mut doomed = vec![root];
        let mut index = 0;
        while index < doomed.len() {
            let parent = doomed[index];
            doomed.extend(
                pairs
                    .iter()
                    .filter(|(_, ppid)| *ppid == parent)
                    .map(|(pid, _)| *pid),
            );
            index += 1;
        }
        for pid in doomed.iter().rev() {
            let _ = Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status();
        }
        let _ = child.wait();
        // The port is released once the processes are gone.
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline
            && std::net::TcpStream::connect(("127.0.0.1", self.port)).is_ok()
        {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for Celld {
    fn drop(&mut self) {
        self.crash();
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.project);
        } else {
            let _ = writeln!(
                std::io::stderr(),
                "celld artifacts kept in {}",
                self.project.display()
            );
        }
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn blocking_get(url: &str) -> Option<u16> {
    let url = url.strip_prefix("http://")?;
    let (host, path) = url.split_once('/')?;
    let mut stream = std::net::TcpStream::connect(host).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    write!(
        stream,
        "GET /{path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut response = String::new();
    std::io::Read::read_to_string(&mut stream, &mut response).ok()?;
    response.split_whitespace().nth(1)?.parse().ok()
}
