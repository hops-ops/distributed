//! `celld dev` serving Mob Pit's cells (`examples/game/cells`) for tests:
//! its own project directory, port and process tree (killed on drop; only
//! the processes this harness started).
//!
//! - celld is found through `CELLD_BIN` (`examples/game/run.sh` sets it);
//!   there is no fallback and no native cell mode, so a missing binary or an
//!   unbuilt worker is an error with the fix in its message.
//! - `CELLD_ESBUILD` (celld's bundler) is inherited from the environment.
//! - State lives in `{project}/.celld/dev` and survives [`DevCelld::restart`]
//!   (crash-recovery tests).

use std::io::Write as _;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use crate::common::CelldOptions;

/// Env var naming the celld binary.
pub const CELLD_BIN_ENV: &str = "CELLD_BIN";

/// The built cells worker (`worker-build --release` in `examples/game/cells`).
pub fn cells_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../cells")
}

/// A free local port (bound then released).
pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .expect("free port")
}

/// A random `test-only-…` internal secret (the Queue relay accepts plain
/// HTTP to localhost only with a `test-only-` secret).
pub fn test_secret() -> String {
    format!("test-only-{}", crate::common::random_hex(32))
}

/// One `celld dev` process tree.
pub struct DevCelld {
    project: PathBuf,
    port: u16,
    secret: String,
    relay_url: Option<String>,
    child: Option<Child>,
    generation: u32,
    keep: bool,
}

impl DevCelld {
    /// Start celld on a fresh temp project. `relay_url` is where the outbox
    /// Queue consumer relays domain events (a Mob Pit server's
    /// `/internal/celld-queue/relay`); `None` keeps them queued.
    pub fn start(relay_url: Option<String>) -> Result<Self, String> {
        let project = std::env::temp_dir().join(format!(
            "mobpit-celld-{}-{}",
            std::process::id(),
            crate::common::random_hex(8)
        ));
        let mut celld = Self::in_project(&project, relay_url)?;
        celld.keep = false;
        Ok(celld)
    }

    /// Start celld on `project` (created; existing state is reused).
    pub fn in_project(project: &Path, relay_url: Option<String>) -> Result<Self, String> {
        let build = cells_dir().join("build");
        if !build.join("worker/shim.mjs").exists() {
            return Err(format!(
                "the Mob Pit cells worker is not built: run `worker-build --release` in {} \
                 (examples/game/run.sh does)",
                cells_dir().display()
            ));
        }
        std::fs::create_dir_all(project).map_err(|e| format!("{}: {e}", project.display()))?;
        copy_dir(&build, &project.join("build"))?;
        std::fs::copy(
            cells_dir().join("wrangler.jsonc"),
            project.join("wrangler.jsonc"),
        )
        .map_err(|e| format!("wrangler.jsonc: {e}"))?;
        let mut celld = Self {
            project: project.to_path_buf(),
            port: free_port(),
            secret: test_secret(),
            relay_url,
            child: None,
            generation: 0,
            keep: true,
        };
        celld.launch()?;
        Ok(celld)
    }

    /// `http://127.0.0.1:{port}`.
    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Options for a Mob Pit world on this celld.
    pub fn options(&self) -> CelldOptions {
        CelldOptions::new(self.url(), self.secret.clone()).expect("celld options")
    }

    /// The internal secret.
    pub fn secret(&self) -> &str {
        &self.secret
    }

    /// Project directory (holds `.celld/dev` and the logs).
    pub fn project(&self) -> &Path {
        &self.project
    }

    fn launch(&mut self) -> Result<(), String> {
        self.generation += 1;
        let binary = std::env::var(CELLD_BIN_ENV).map_err(|_| {
            format!(
                "{CELLD_BIN_ENV} is not set: Mob Pit's cells run on celld (0.6.2+); \
                 point {CELLD_BIN_ENV} at the celld binary"
            )
        })?;
        let mut vars = format!("DISTRIBUTED_INTERNAL_SECRET={}\n", self.secret);
        if let Some(relay) = &self.relay_url {
            vars.push_str(&format!("CELLD_QUEUE_RELAY_URL={relay}\n"));
        }
        std::fs::write(self.project.join(".dev.vars"), vars)
            .map_err(|e| format!(".dev.vars: {e}"))?;
        let log_path = self.project.join(format!("celld-{}.log", self.generation));
        let log = std::fs::File::create(&log_path).map_err(|e| e.to_string())?;
        let mut command = Command::new(&binary);
        command
            .arg("dev")
            .arg(&self.project)
            .args(["--host", "127.0.0.1", "--port", &self.port.to_string()])
            .args(["--no-watch", "--logs"])
            .env("RUST_LOG", "warn")
            .stdin(Stdio::null())
            .stdout(log.try_clone().map_err(|e| e.to_string())?)
            .stderr(log);
        let child = command
            .spawn()
            .map_err(|error| format!("cannot start `{binary} dev`: {error}"))?;
        self.child = Some(child);
        let deadline = Instant::now() + Duration::from_secs(90);
        while Instant::now() < deadline {
            if http_status(self.port, "/health") == Some(200) {
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
                    log.lines().rev().take(5).collect::<Vec<_>>().join(" | ")
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Err(format!(
            "celld dev did not become ready; logs in {}",
            self.project.display()
        ))
    }

    /// Kill the launcher and every descendant (a crash: no graceful
    /// shutdown). `celld dev` starts its node in a session of its own, so
    /// the tree is collected while the launcher is stopped, then killed.
    pub fn crash(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let root = child.id();
        let _ = Command::new("kill")
            .args(["-STOP", &root.to_string()])
            .status();
        let mut doomed = descendants(root);
        doomed.insert(0, root);
        for pid in doomed.iter().rev() {
            let _ = Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status();
        }
        let _ = child.wait();
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline
            && std::net::TcpStream::connect(("127.0.0.1", self.port)).is_ok()
        {
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Crash and start again on the same project, port and secret.
    pub fn restart(&mut self) -> Result<(), String> {
        self.crash();
        self.launch()
    }
}

impl Drop for DevCelld {
    fn drop(&mut self) {
        self.crash();
        if std::thread::panicking() {
            let _ = writeln!(
                std::io::stderr(),
                "celld artifacts kept in {}",
                self.project.display()
            );
        } else if !self.keep {
            let _ = std::fs::remove_dir_all(&self.project);
        }
    }
}

/// Every descendant of `root` (breadth first).
fn descendants(root: u32) -> Vec<u32> {
    let Ok(table) = Command::new("ps").args(["-axo", "pid=,ppid="]).output() else {
        return Vec::new();
    };
    let pairs: Vec<(u32, u32)> = String::from_utf8_lossy(&table.stdout)
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
        })
        .collect();
    let mut out = Vec::new();
    let mut frontier = vec![root];
    while let Some(parent) = frontier.pop() {
        for (pid, _) in pairs.iter().filter(|(_, ppid)| *ppid == parent) {
            out.push(*pid);
            frontier.push(*pid);
        }
    }
    out
}

fn copy_dir(from: &Path, to: &Path) -> Result<(), String> {
    std::fs::create_dir_all(to).map_err(|e| format!("{}: {e}", to.display()))?;
    for entry in std::fs::read_dir(from).map_err(|e| format!("{}: {e}", from.display()))? {
        let entry = entry.map_err(|e| e.to_string())?;
        let target = to.join(entry.file_name());
        if entry.file_type().map_err(|e| e.to_string())?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)
                .map_err(|e| format!("{}: {e}", target.display()))?;
        }
    }
    Ok(())
}

fn http_status(port: u16, path: &str) -> Option<u16> {
    use std::io::Read as _;
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    response.split_whitespace().nth(1)?.parse().ok()
}
