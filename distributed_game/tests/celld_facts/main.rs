//! Zone facts over the network into celld cells (`CelldFactSink`).
//!
//! Owns an isolated `celld dev` process (no Docker) serving
//! `tests/celld/fact-worker` (Kills, Match and Presence cells), and kills only
//! the processes it starts. Build the worker first:
//!
//!   (cd tests/celld/fact-worker && worker-build --release)
//!   cargo test -p distributed_game --features celld,runtime --test celld_facts
//!
//! `CELLD` selects the binary (default `celld` on PATH); `celld dev` needs
//! esbuild (`CELLD_ESBUILD`). Without a built worker the test skips, unless
//! `CELLD_FACTS_REQUIRED=1` (CI), which turns that into a failure.

use std::io::Write as _;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use distributed::cell_host::InternalHttpSecret;
use distributed_game::celld::{CelldFactRoute, CelldFactSink};
use distributed_game::lifecycle::{JoinVia, LeftReason, PlayerJoined, PlayerLeft};
use distributed_game::{
    fact_channel, spawn_fact_worker, EmitError, Fact, FactCommand, FactError, FactOutcome,
    FactQueue, FactSettlement, FactSink, FactStamp, RetryPolicy,
};
use serde_json::{json, Value};

const SECRET: &str = "test-only-internal-secret-change-me-2026";
const SERVICE: &str = "zone-server-test";

#[derive(serde::Serialize, FactCommand)]
#[fact(command = "kills.record")]
struct Kill {
    mob: String,
}

#[derive(serde::Serialize, FactCommand)]
#[fact(command = "match.record_result")]
struct MatchResult {
    match_id: String,
    winner_slot: u32,
}

fn worker_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/celld/fact-worker")
}

struct Celld {
    project: PathBuf,
    port: u16,
    child: Option<Child>,
    generation: u32,
}

impl Celld {
    fn start_new() -> Option<Self> {
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

    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Start `celld dev`; `Err` when it is unavailable or exits early (no
    /// celld on PATH, or no esbuild for its bundler).
    fn start(&mut self) -> Result<(), String> {
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
    fn crash(&mut self) {
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

fn sink(base: &str, timeout: Duration) -> CelldFactSink {
    CelldFactSink::with_timeout(
        base,
        InternalHttpSecret::new(SECRET).unwrap(),
        SERVICE,
        "zone:pit-1",
        timeout,
    )
    .unwrap()
    .route(CelldFactRoute::by_principal::<Kill>("kills"))
    .route(CelldFactRoute::by_input_field::<MatchResult>(
        "match", "match_id",
    ))
    .lifecycle_route("presence")
}

fn stamp(tick: u64) -> FactStamp {
    FactStamp {
        zone_id: "pit-1".into(),
        tick,
        content_version: "c1".into(),
        emitted_at_ms: 1_700_000_000_000 + tick,
        activation: 1,
        config_version: "cfg-1".into(),
    }
}

fn fact<C: FactCommand>(key: &str, input: &C, principal: Option<&str>, tick: u64) -> Fact {
    Fact::trusted(
        key,
        C::COMMAND,
        serde_json::to_value(input).unwrap(),
        principal.map(str::to_string),
        stamp(tick),
    )
}

async fn state(base: &str, kind: &str, shard: &str) -> Value {
    reqwest::Client::new()
        .get(format!("{base}/{kind}/{shard}"))
        .header("x-distributed-internal-secret", SECRET)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Drain settlements until `count` arrive (or time out).
async fn settle(queue: &mut FactQueue, count: usize, within: Duration) -> Vec<FactSettlement> {
    let deadline = Instant::now() + within;
    let mut settled = Vec::new();
    while settled.len() < count && Instant::now() < deadline {
        settled.extend(queue.drain());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    settled
}

fn committed(settlement: &FactSettlement) -> bool {
    matches!(settlement.outcome, FactOutcome::Committed(_))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zone_facts_route_to_three_cell_kinds_and_survive_a_celld_outage() {
    let Some(mut celld) = Celld::start_new() else {
        return;
    };
    let base = celld.base();
    let policy = RetryPolicy::until_settled(Duration::from_millis(100), Duration::from_millis(500));
    let (mut queue, feed) = fact_channel(4, policy);
    let worker = spawn_fact_worker(
        feed.with_max_concurrent_commits(2),
        sink(&base, Duration::from_secs(10)),
    );

    // 1. One zone, one sink, three aggregate kinds (lifecycle -> presence).
    let joined = PlayerJoined {
        zone: "pit-1".into(),
        via: JoinVia::Ticket,
        from_zone: None,
    };
    queue
        .try_send(fact(
            "zone.player_joined:pit-1:1:p0:0",
            &joined,
            Some("alice"),
            1,
        ))
        .unwrap();
    queue
        .try_send(fact(
            "kills.record:pit-1:1:k1",
            &Kill { mob: "m1".into() },
            Some("alice"),
            2,
        ))
        .unwrap();
    queue
        .try_send(fact(
            "kills.record:pit-1:1:k2",
            &Kill { mob: "m2".into() },
            Some("bob"),
            3,
        ))
        .unwrap();
    queue
        .try_send(fact(
            "match.record_result:pit-1:1:m-7",
            &MatchResult {
                match_id: "m-7".into(),
                winner_slot: 2,
            },
            None,
            4,
        ))
        .unwrap();
    let settled = settle(&mut queue, 4, Duration::from_secs(30)).await;
    assert_eq!(settled.len(), 4, "{settled:?}");
    assert!(settled.iter().all(committed), "{settled:?}");
    let left = PlayerLeft {
        zone: "pit-1".into(),
        reason: LeftReason::Left,
    };
    queue
        .try_send(fact(
            "zone.player_left:pit-1:1:p0:1",
            &left,
            Some("alice"),
            5,
        ))
        .unwrap();
    assert!(settle(&mut queue, 1, Duration::from_secs(30))
        .await
        .iter()
        .all(committed));

    assert_eq!(
        state(&base, "kills", "alice").await["state"]["mobs"],
        json!(["m1"])
    );
    assert_eq!(
        state(&base, "kills", "bob").await["state"]["mobs"],
        json!(["m2"])
    );
    assert_eq!(
        state(&base, "match", "m-7").await["state"]["results"],
        json!([2])
    );
    assert_eq!(
        state(&base, "presence", "alice").await["state"]["transitions"],
        json!(["joined:pit-1:ticket", "left:pit-1:left"]),
        "lifecycle facts go to presence, not to the player's kill cell"
    );

    // 2. The same key twice (a retry after a lost response) commits once and
    //    replays the ledger receipt.
    let direct = sink(&base, Duration::from_secs(10));
    let again = fact(
        "kills.record:pit-1:1:k1",
        &Kill { mob: "m1".into() },
        Some("alice"),
        2,
    );
    let replay = direct.commit(&again).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(state(&base, "kills", "alice").await["events"], 1);
    //    Forced timeout after commit: the first attempt gives up before the
    //    response; the retry finds the commit in the ledger.
    let hasty = sink(&base, Duration::from_millis(1));
    let late = fact(
        "kills.record:pit-1:1:k3",
        &Kill { mob: "m3".into() },
        Some("alice"),
        6,
    );
    assert!(matches!(
        hasty.commit(&late).await,
        Err(FactError::Retryable(_))
    ));
    tokio::time::sleep(Duration::from_millis(500)).await;
    let receipt = direct.commit(&late).await.unwrap();
    assert_eq!(
        state(&base, "kills", "alice").await["state"]["mobs"],
        json!(["m1", "m3"]),
        "exactly one commit for the key (replayed: {})",
        receipt.replayed
    );

    // 3. An unknown command settles Permanent without a network call.
    queue
        .try_send(Fact::trusted(
            "guild.promote:pit-1:1:x",
            "guild.promote",
            json!({}),
            Some("alice".into()),
            stamp(7),
        ))
        .unwrap();
    let unknown = settle(&mut queue, 1, Duration::from_secs(10)).await;
    assert!(
        matches!(
            &unknown[0].outcome,
            FactOutcome::Failed {
                error: FactError::Permanent(_),
                attempts: 1
            }
        ),
        "{unknown:?}"
    );

    // 4. The cell worker rejects requests without the internal secret.
    let forged = reqwest::Client::new()
        .post(format!("{base}/kills/alice/kills.record"))
        .header("x-distributed-service-id", SERVICE)
        .header("x-distributed-principal-partition", "alice")
        .header("x-user-id", "alice")
        .json(&json!({ "commandId": "0190a000-0000-7000-8000-000000000001", "input": { "mob": "x" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(forged.status(), 401);
    let forged_secret = reqwest::Client::new()
        .post(format!("{base}/kills/alice/kills.record"))
        .header("x-distributed-internal-secret", "forged-secret-for-red-team-request-0000")
        .json(&json!({ "commandId": "0190a000-0000-7000-8000-000000000002", "input": { "mob": "x" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(forged_secret.status(), 401);
    //    With the secret but without the fact headers, handlers refuse.
    let not_a_fact = reqwest::Client::new()
        .post(format!("{base}/kills/alice/kills.record"))
        .header("x-distributed-internal-secret", SECRET)
        .header("x-distributed-service-id", SERVICE)
        .header("x-distributed-principal-partition", "alice")
        .header("x-user-id", "alice")
        .json(&json!({ "commandId": "0190a000-0000-7000-8000-000000000003", "input": { "mob": "x" } }))
        .send()
        .await
        .unwrap();
    assert!(
        not_a_fact.status().is_client_error(),
        "{}",
        not_a_fact.status()
    );
    assert_eq!(
        state(&base, "kills", "alice").await["state"]["mobs"],
        json!(["m1", "m3"])
    );

    // 5. celld down: facts stay pending, the zone is never blocked, the
    //    queue reports Backpressure at capacity.
    celld.crash();
    let outage = [
        ("alice", "a1", 10),
        ("bob", "b1", 11),
        ("alice", "a2", 12),
        ("bob", "b2", 13),
    ];
    for (principal, mob, tick) in outage {
        let started = Instant::now();
        queue
            .try_send(fact(
                &format!("kills.record:pit-1:1:{mob}"),
                &Kill { mob: mob.into() },
                Some(principal),
                tick,
            ))
            .unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "emitting never waits on celld"
        );
    }
    assert_eq!(
        queue.try_send(fact(
            "kills.record:pit-1:1:a3",
            &Kill { mob: "a3".into() },
            Some("alice"),
            14
        )),
        Err(EmitError::Backpressure)
    );
    assert!(
        settle(&mut queue, 1, Duration::from_secs(3))
            .await
            .is_empty(),
        "nothing settles while celld is down"
    );
    assert_eq!(queue.in_flight(), 4);

    //    celld back: the backlog drains, each principal in emission order.
    celld.start().expect("celld restarts");
    let drained = settle(&mut queue, 4, Duration::from_secs(60)).await;
    assert_eq!(drained.len(), 4, "{drained:?}");
    assert!(drained.iter().all(committed), "{drained:?}");
    assert_eq!(
        state(&base, "kills", "alice").await["state"]["mobs"],
        json!(["m1", "m3", "a1", "a2"])
    );
    assert_eq!(
        state(&base, "kills", "bob").await["state"]["mobs"],
        json!(["m2", "b1", "b2"])
    );
    let counts = drained
        .iter()
        .map(|settlement| settlement.key.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(counts.len(), 4);

    drop(queue);
    let _ = tokio::time::timeout(Duration::from_secs(5), worker).await;
}
