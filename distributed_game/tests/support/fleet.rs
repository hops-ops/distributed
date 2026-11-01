//! A local multi-node celld fleet owned by one test: one RustFS container
//! (docker compose, its own project and port) holding the fleet bucket, the
//! worker deployed to it with `celld deploy`, and N `celld` nodes in
//! production mode. Kills only what it started; removes the container on
//! drop. Skips (None) without docker / aws / a built worker unless env
//! `required` is set.

#![allow(dead_code)]

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use super::celld_support::{blocking_get, copy_dir};

const ACCESS_KEY: &str = "fleettest";
const SECRET_KEY: &str = "fleettestsecret123";

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub struct Node {
    pub name: String,
    pub public: u16,
    pub operator: u16,
    child: Option<Child>,
}

pub struct Fleet {
    dir: PathBuf,
    project: String,
    s3_port: u16,
    bucket: String,
    binary: String,
    pub nodes: Vec<Node>,
}

fn skip(required: &str, reason: String) -> Option<Fleet> {
    assert!(
        std::env::var_os(required).is_none(),
        "{required} is set but the fleet is unavailable: {reason}"
    );
    eprintln!("skipping the celld fleet test: {reason}");
    None
}

impl Fleet {
    /// Start `nodes` nodes serving the worker in `worker` with extra
    /// string `vars` (`{nodes}` in a value is replaced by the operator
    /// listeners, `node-a=127.0.0.1:p,…`).
    pub fn start(
        worker: &Path,
        nodes: usize,
        vars: &[(&str, &str)],
        required: &str,
    ) -> Option<Self> {
        if !worker.join("build/worker/shim.mjs").exists() {
            return skip(required, format!("build {} first", worker.display()));
        }
        for tool in ["docker", "aws"] {
            if Command::new(tool).arg("--version").output().is_err() {
                return skip(required, format!("`{tool}` is not available"));
            }
        }
        let binary = std::env::var("CELLD_BIN").unwrap_or_else(|_| "celld".into());
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let dir = std::env::temp_dir().join(format!(
            "distributed-celld-fleet-{}-{stamp}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut fleet = Self {
            project: format!("dg-fleet-{}-{}", std::process::id(), stamp % 100_000),
            s3_port: free_port(),
            bucket: "dg-fleet".into(),
            binary,
            nodes: Vec::new(),
            dir,
        };
        for i in 0..nodes {
            fleet.nodes.push(Node {
                name: format!("node-{}", (b'a' + i as u8) as char),
                public: free_port(),
                operator: free_port(),
                child: None,
            });
        }
        if let Err(reason) = fleet.s3_up() {
            return skip(required, reason);
        }
        let listeners = fleet
            .nodes
            .iter()
            .map(|n| format!("{}=127.0.0.1:{}", n.name, n.operator))
            .collect::<Vec<_>>()
            .join(",");
        let vars: Vec<(String, String)> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.replace("{nodes}", &listeners)))
            .collect();
        if let Err(reason) = fleet.deploy(worker, &vars) {
            return skip(required, reason);
        }
        for i in 0..fleet.nodes.len() {
            if let Err(reason) = fleet.start_node(i) {
                return skip(required, reason);
            }
        }
        Some(fleet)
    }

    fn env(&self, command: &mut Command) {
        command
            .env("AWS_ACCESS_KEY_ID", ACCESS_KEY)
            .env("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
            .env("AWS_REGION", "us-east-1")
            .env("S3_ENDPOINT", self.endpoint());
    }

    fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.s3_port)
    }

    fn s3_up(&self) -> Result<(), String> {
        let compose = self.dir.join("s3-compose.yaml");
        std::fs::write(
            &compose,
            format!(
                "services:\n  s3:\n    image: rustfs/rustfs:1.0.3\n    command: [\"/data\"]\n    environment:\n      RUSTFS_ADDRESS: \"0.0.0.0:9000\"\n      RUSTFS_ACCESS_KEY: {ACCESS_KEY}\n      RUSTFS_SECRET_KEY: {SECRET_KEY}\n    ports: [\"127.0.0.1:{}:9000\"]\n",
                self.s3_port
            ),
        )
        .unwrap();
        let up = Command::new("docker")
            .args(["compose", "-p", &self.project, "-f"])
            .arg(&compose)
            .args(["up", "-d"])
            .output()
            .map_err(|e| format!("docker compose: {e}"))?;
        if !up.status.success() {
            return Err(format!(
                "docker compose up: {}",
                String::from_utf8_lossy(&up.stderr)
            ));
        }
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let mut mb = Command::new("aws");
            self.env(&mut mb);
            let made = mb
                .args(["--endpoint-url", &self.endpoint(), "s3", "mb"])
                .arg(format!("s3://{}", self.bucket))
                .output()
                .map_err(|e| format!("aws: {e}"))?;
            let text = String::from_utf8_lossy(&made.stderr);
            if made.status.success() || text.contains("BucketAlready") {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!("bucket: {text}"));
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    fn bucket_url(&self) -> String {
        format!("s3://{}/fleet", self.bucket)
    }

    fn deploy(&self, worker: &Path, vars: &[(String, String)]) -> Result<(), String> {
        let project = self.dir.join("worker");
        copy_dir(&worker.join("build"), &project.join("build"));
        let config = std::fs::read_to_string(worker.join("wrangler.jsonc")).unwrap();
        let mut config: serde_json::Value = serde_json::from_str(&strip_comments(&config))
            .map_err(|e| format!("wrangler.jsonc: {e}"))?;
        for (key, value) in vars {
            config["vars"][key] = serde_json::Value::String(value.clone());
        }
        std::fs::write(project.join("wrangler.json"), config.to_string()).unwrap();
        let mut deploy = Command::new(&self.binary);
        self.env(&mut deploy);
        let out = deploy
            .arg("deploy")
            .arg(&project)
            .args(["--bucket", &self.bucket_url()])
            .output()
            .map_err(|e| format!("celld deploy: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "celld deploy: {}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        Ok(())
    }

    /// (Re)start node `i`.
    pub fn start_node(&mut self, i: usize) -> Result<(), String> {
        let node = &self.nodes[i];
        let dir = self.dir.join(&node.name);
        std::fs::create_dir_all(&dir).unwrap();
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("celld.log"))
            .unwrap();
        let mut command = Command::new(&self.binary);
        self.env(&mut command);
        let child = command
            .args(["--bucket", &self.bucket_url()])
            .args(["--listen", &format!("127.0.0.1:{}", node.public)])
            .args(["--internal-listen", &format!("127.0.0.1:{}", node.operator)])
            .args(["--advertise", &format!("127.0.0.1:{}", node.operator)])
            .env("CELLD_NODE", &node.name)
            .env("CELLD_WATCH", dir.join("watch"))
            .env("CELLD_READY_FLEET_GATE_MS", "0")
            .env(
                "RUST_LOG",
                std::env::var("CELLD_RUST_LOG").unwrap_or_else(|_| "warn".into()),
            )
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .map_err(|e| format!("celld: {e}"))?;
        let public = node.public;
        self.nodes[i].child = Some(child);
        let deadline = Instant::now() + Duration::from_secs(90);
        while Instant::now() < deadline {
            if blocking_get(&format!("http://127.0.0.1:{public}/health")) == Some(200) {
                return Ok(());
            }
            let exited = self.nodes[i]
                .child
                .as_mut()
                .and_then(|c| c.try_wait().ok().flatten());
            if let Some(status) = exited {
                // A cold node sometimes fails its first start; retry once.
                self.nodes[i].child = None;
                return Err(format!(
                    "{} exited ({status}); logs in {}",
                    self.nodes[i].name,
                    dir.display()
                ));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        Err(format!(
            "{} not ready; logs in {}",
            self.nodes[i].name,
            dir.display()
        ))
    }

    /// `http://127.0.0.1:{public port}` of node `i`.
    pub fn base(&self, i: usize) -> String {
        format!("http://127.0.0.1:{}", self.nodes[i].public)
    }

    /// Index of the node named `name`.
    pub fn index(&self, name: &str) -> Option<usize> {
        self.nodes.iter().position(|n| n.name == name)
    }

    /// SIGKILL node `i` (a crash).
    pub fn kill(&mut self, i: usize) {
        if let Some(mut child) = self.nodes[i].child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for Fleet {
    fn drop(&mut self) {
        for i in 0..self.nodes.len() {
            self.kill(i);
        }
        let _ = Command::new("docker")
            .args(["compose", "-p", &self.project, "-f"])
            .arg(self.dir.join("s3-compose.yaml"))
            .args(["down", "-v"])
            .output();
        if std::thread::panicking() {
            eprintln!("fleet logs kept in {}", self.dir.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

/// Drop `//` line comments (wrangler.jsonc).
fn strip_comments(text: &str) -> String {
    text.lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}
