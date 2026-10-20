//! The two-host world of **The Freeze** and its dev / admin routes.
//!
//! `game-demos freeze --dev` is the **primary** host (`alpha`: hub +
//! meadow). It starts a **second process** of itself for `beta` (Ember
//! Fields) with `--peer alpha=<addr>`, sharing the ticket secret and the
//! admin secret through the environment; the child exits when the
//! primary's pipe closes. Both hosts share a `StaticDirectory`, so a Hello
//! for a zone on the other host is redirected and handoffs reserve their
//! transfer with the issuing host over `/admin/transfer`.
//!
//! Routes (dev only, mounted with `--dev`; the mutating ones require the
//! `x-game-admin-secret` header):
//!
//! | Route | Effect |
//! |---|---|
//! | `GET /dev/freeze/content/{version}` | content pack, payload-codec encoded (`application/octet-stream`, decoded by the generated `decodePack`; `current` = the world's version; future versions 404 with JSON) |
//! | `GET /dev/freeze/world` | status of every host's zones (freeze state, players, inputs dropped while frozen, recent facts) |
//! | `POST /dev/freeze/rollout` `{mode: hard\|rolling}` | **admin** — primary only: `WorldCoordinator::rollout` to the next version |
//! | `POST /dev/freeze/stuck` `{zone, stuck}` | **admin** — start/stop a zone's ritual (forwarded to the owning host) |
//! | `POST /dev/freeze/host-status` | **admin** — this host's status (peer call) |

use std::future::Future;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use distributed_game::net::peer::{post_json, ADMIN_SECRET_HEADER};
use distributed_game::net::{
    GameServer, RemoteHost, RolloutError, ServerConfig, TicketKey, WorldCoordinator, ZoneFactory,
};
use distributed_game::world::{HostAddr, StaticDirectory};
use distributed_game::{BarrierMode, FreezeState, MemoryFactSink, RetryPolicy};
use serde::Deserialize;
use serde_json::{json, Value};

use super::content::{self, ZoneKind, EMBER, HUB, MEADOW};
use super::{freeze_config, FreezeConfig, FreezeDemo, FreezeZone, Rituals};
use crate::common::{self, dev, random_hex, server_config, DemoArgs, Guests};

/// Default time the "content CDN" takes to stage an update while the world
/// is frozen (makes the pause visible).
pub const STAGE_MS: u64 = 2500;
/// How long the coordinator waits for every zone to freeze.
pub const ACK_TIMEOUT: Duration = Duration::from_secs(3);

/// `game-demos freeze` options.
#[derive(Debug, Clone, clap::Args)]
pub struct FreezeArgs {
    #[command(flatten)]
    pub demo: DemoArgs,
    /// Shared admin secret (barrier, transfers, `/dev/freeze/*` admin
    /// routes). Default: random, printed at startup.
    #[arg(long, env = "DG_DEMO_ADMIN_SECRET", hide_env_values = true)]
    pub admin_secret: Option<String>,
    /// This host's id.
    #[arg(long, default_value = "alpha")]
    pub host_id: String,
    /// Run as the second host of `id=host:port` (set by the primary).
    #[arg(long)]
    pub peer: Option<String>,
    /// Simulated content staging time during the freeze (ms).
    #[arg(long, env = "DG_FREEZE_STAGE_MS", default_value_t = STAGE_MS)]
    pub stage_ms: u64,
}

/// Everything one host needs.
pub struct HostSpec {
    /// This host.
    pub me: HostAddr,
    /// The other hosts.
    pub peers: Vec<HostAddr>,
    /// Zone → host id.
    pub assignment: Vec<(String, String)>,
    /// Ticket key shared by every host.
    pub key: TicketKey,
    /// Admin secret shared by every host.
    pub admin_secret: String,
    /// Mount the dev routes.
    pub dev: bool,
    /// Content staging delay inside the barrier.
    pub stage: Duration,
    /// Barrier acknowledgement timeout.
    pub ack_timeout: Duration,
}

/// A running Freeze host.
pub struct WorldHost {
    /// Bound address.
    pub addr: SocketAddr,
    /// The game server.
    pub server: GameServer<FreezeZone>,
    /// Facts committed by this host's zones.
    pub sink: MemoryFactSink,
    /// Stuck-zone toggles.
    pub rituals: Rituals,
    /// Resolves when the server stopped.
    pub task: tokio::task::JoinHandle<std::io::Result<()>>,
}

/// The Freeze game server for one host (no routes, no directory).
pub fn zone_server(
    config: ServerConfig,
    guests: Guests,
    rituals: Rituals,
    host: &str,
) -> GameServer<FreezeZone> {
    zone_server_with_sink(config, guests, rituals, host, MemoryFactSink::new())
}

fn zone_server_with_sink(
    config: ServerConfig,
    guests: Guests,
    rituals: Rituals,
    host: &str,
    sink: MemoryFactSink,
) -> GameServer<FreezeZone> {
    let host = host.to_string();
    GameServer::new(
        config,
        ZoneFactory::sync(move |activation| {
            let zone_id = activation.zone_id.as_str();
            ZoneKind::of(zone_id).map(|kind| FreezeConfig {
                kind,
                guests: guests.clone(),
                rituals: rituals.clone(),
                host: host.clone(),
            })
        }),
    )
    .with_fact_sink(sink, 256, RetryPolicy::default())
}

/// The default assignment: hub + meadow on `primary`, ember on `secondary`.
pub fn assignment(primary: &str, secondary: &str) -> Vec<(String, String)> {
    vec![
        (HUB.into(), primary.into()),
        (MEADOW.into(), primary.into()),
        (EMBER.into(), secondary.into()),
    ]
}

/// Start one host on `listener` until `signal` resolves; activates its
/// zones so the barrier reaches them before anyone joined.
pub fn start_host(
    listener: tokio::net::TcpListener,
    spec: HostSpec,
    signal: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<WorldHost> {
    let addr = listener.local_addr()?;
    let directory = Arc::new(StaticDirectory::new());
    directory.add_host(spec.me.clone());
    for peer in &spec.peers {
        directory.add_host(peer.clone());
    }
    for (zone, host) in &spec.assignment {
        directory.assign(zone.clone(), host.clone());
    }
    let mut config = freeze_config(server_config(spec.key.clone(), None));
    config.admin_secret = Some(spec.admin_secret.clone());
    let guests = Guests::default();
    let rituals = Rituals::default();
    let sink = MemoryFactSink::new();
    let server = zone_server_with_sink(
        config,
        guests.clone(),
        rituals.clone(),
        &spec.me.id,
        sink.clone(),
    )
    .with_host_id(spec.me.id.clone())
    .with_directory(directory);
    let local: Vec<String> = spec
        .assignment
        .iter()
        .filter(|(_, h)| *h == spec.me.id)
        .map(|(z, _)| z.clone())
        .collect();
    let extra = if spec.dev {
        dev::routes::<FreezeDemo>(server.clone(), spec.key.clone(), guests).merge(routes(
            FreezeRoutes {
                server: server.clone(),
                sink: sink.clone(),
                rituals: rituals.clone(),
                admin_secret: spec.admin_secret.clone(),
                me: spec.me.clone(),
                peers: spec.peers.clone(),
                local: local.clone(),
                assignment: spec.assignment.clone(),
                stage: spec.stage,
                ack_timeout: spec.ack_timeout,
                primary: spec
                    .assignment
                    .iter()
                    .any(|(z, h)| z == HUB && *h == spec.me.id),
                rollout: Arc::new(tokio::sync::Mutex::new(())),
                history: Arc::new(Mutex::new(Vec::new())),
            },
        ))
    } else {
        Router::new()
    };
    let task = tokio::spawn(server.clone().serve_with(listener, extra, signal));
    for zone in &local {
        server.begin_activation(zone);
    }
    Ok(WorldHost {
        addr,
        server,
        sink,
        rituals,
        task,
    })
}

// ---------------------------------------------------------------- routes

#[derive(Clone)]
struct FreezeRoutes {
    server: GameServer<FreezeZone>,
    sink: MemoryFactSink,
    rituals: Rituals,
    admin_secret: String,
    me: HostAddr,
    peers: Vec<HostAddr>,
    local: Vec<String>,
    assignment: Vec<(String, String)>,
    stage: Duration,
    ack_timeout: Duration,
    primary: bool,
    rollout: Arc<tokio::sync::Mutex<()>>,
    history: Arc<Mutex<Vec<Value>>>,
}

type Reply = (StatusCode, Json<Value>);

fn reply(status: StatusCode, body: Value) -> Reply {
    (status, Json(body))
}

fn authorized(state: &FreezeRoutes, headers: &HeaderMap) -> Result<(), Reply> {
    let presented = headers
        .get(ADMIN_SECRET_HEADER)
        .map(|v| v.as_bytes())
        .unwrap_or_default();
    let expected = state.admin_secret.as_bytes();
    let same = presented.len() == expected.len()
        && presented
            .iter()
            .zip(expected)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0;
    if same && !expected.is_empty() {
        Ok(())
    } else {
        Err(reply(
            StatusCode::UNAUTHORIZED,
            json!({ "ok": false, "error": "admin secret required (x-game-admin-secret)" }),
        ))
    }
}

fn routes(state: FreezeRoutes) -> Router {
    Router::new()
        .route("/dev/freeze/content/{version}", get(content_pack))
        .route("/dev/freeze/world", get(world_status))
        .route("/dev/freeze/host-status", post(host_status))
        .route("/dev/freeze/rollout", post(rollout))
        .route("/dev/freeze/stuck", post(stuck))
        .with_state(state)
}

async fn content_pack(
    State(state): State<FreezeRoutes>,
    Path(version): Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let current = state.server.content_version();
    let wanted = if version == "current" {
        current.clone()
    } else {
        version
    };
    let published = content::version_number(&wanted)
        .zip(content::version_number(&current))
        .is_some_and(|(w, c)| w <= c);
    match content::pack(&wanted).filter(|_| published) {
        Some(pack) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
            pack.to_bytes(),
        )
            .into_response(),
        None => reply(
            StatusCode::NOT_FOUND,
            json!({ "error": format!("content {wanted} is not published (world runs {current})") }),
        )
        .into_response(),
    }
}

async fn local_status(state: &FreezeRoutes) -> Value {
    let mut zones = Vec::new();
    for zone in &state.local {
        let info = state
            .server
            .inspect(zone, |h| {
                (
                    h.freeze_state(),
                    h.player_count(),
                    h.inputs_dropped_frozen(),
                    h.config().content_version.clone(),
                    h.zone().pack().name.clone(),
                )
            })
            .await;
        if let Some((freeze, players, dropped, version, pack)) = info {
            zones.push(json!({
                "zone": zone,
                "title": ZoneKind::of(zone).map(ZoneKind::title),
                "state": match freeze {
                    FreezeState::Running => "running",
                    FreezeState::Freezing => "freezing",
                    FreezeState::Frozen => "frozen",
                },
                "players": players,
                "droppedFrozen": dropped,
                "contentVersion": version,
                "pack": pack,
                "stuck": state.rituals.is_stuck(zone),
            }));
        }
    }
    let facts: Vec<Value> = state
        .sink
        .committed()
        .iter()
        .rev()
        .take(8)
        .map(|f| {
            json!({
                "command": f.command(),
                "key": f.key(),
                "principal": f.principal(),
                "input": f.input(),
                "stamp": {
                    "zone": f.stamp().zone_id,
                    "tick": f.stamp().tick,
                    "contentVersion": f.stamp().content_version,
                },
            })
        })
        .collect();
    json!({
        "host": state.me.id,
        "version": state.server.content_version(),
        "zones": zones,
        "facts": facts,
    })
}

async fn host_status(State(state): State<FreezeRoutes>, headers: HeaderMap) -> Reply {
    if let Err(r) = authorized(&state, &headers) {
        return r;
    }
    reply(StatusCode::OK, local_status(&state).await)
}

async fn world_status(State(state): State<FreezeRoutes>) -> Reply {
    let mut hosts = vec![local_status(&state).await];
    for peer in &state.peers {
        match post_json(
            &peer.admin_url,
            "/dev/freeze/host-status",
            &state.admin_secret,
            &json!({}),
            Duration::from_secs(2),
        )
        .await
        {
            Ok((200, status)) => hosts.push(status),
            other => hosts.push(json!({ "host": peer.id, "error": format!("{other:?}") })),
        }
    }
    let history = state.history.lock().expect("history").clone();
    reply(
        StatusCode::OK,
        json!({ "version": state.server.content_version(), "hosts": hosts, "rollouts": history }),
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RolloutRequest {
    /// `hard` or `rolling` (safe room).
    mode: String,
}

async fn rollout(
    State(state): State<FreezeRoutes>,
    headers: HeaderMap,
    Json(body): Json<RolloutRequest>,
) -> Reply {
    if let Err(r) = authorized(&state, &headers) {
        return r;
    }
    if !state.primary {
        return reply(
            StatusCode::BAD_REQUEST,
            json!({ "ok": false, "error": "roll out from the primary host" }),
        );
    }
    let mode = match body.mode.as_str() {
        "hard" => BarrierMode::Hard,
        "rolling" => BarrierMode::Rolling,
        _ => {
            return reply(
                StatusCode::BAD_REQUEST,
                json!({ "ok": false, "error": "mode must be hard or rolling" }),
            )
        }
    };
    let Ok(_running) = state.rollout.try_lock() else {
        return reply(
            StatusCode::CONFLICT,
            json!({ "ok": false, "error": "a rollout is in progress" }),
        );
    };
    let current = state.server.content_version();
    let next = content::version_name(content::version_number(&current).unwrap_or(1) + 1);
    let pack = content::pack(&next).expect("valid version");
    let notes = pack.resume_notes();
    let mut coordinator =
        WorldCoordinator::new(state.ack_timeout).participant(Arc::new(state.server.clone()));
    for peer in &state.peers {
        coordinator = coordinator.participant(Arc::new(RemoteHost::new(
            peer.clone(),
            state.admin_secret.clone(),
        )));
    }
    let started = Instant::now();
    // The "content CDN" stages the pack while every zone is frozen: an
    // async apply, so clients see the SYSTEM "applying update" pause.
    let stage = state.stage;
    let result = coordinator
        .rollout_async(&next, &notes, mode, |_version| async move {
            tokio::time::sleep(stage).await;
            Ok(())
        })
        .await;
    let took_ms = started.elapsed().as_millis() as u64;
    let mode_name = body.mode;
    let outcome = match result {
        Ok(report) => json!({
            "ok": true, "from": current, "version": report.version, "mode": mode_name,
            "name": pack.name, "notes": pack.notes, "commitErrors": report.commit_errors,
            "tookMs": took_ms,
        }),
        Err(RolloutError::NotAcked(missing)) => json!({
            "ok": false, "error": "not_acked", "missing": missing, "attempted": next,
            "version": current, "mode": mode_name, "tookMs": took_ms,
        }),
        Err(RolloutError::ApplyFailed(e)) => json!({
            "ok": false, "error": "apply_failed", "detail": e, "attempted": next,
            "version": current, "mode": mode_name, "tookMs": took_ms,
        }),
    };
    let mut history = state.history.lock().expect("history");
    history.push(outcome.clone());
    if history.len() > 10 {
        history.remove(0);
    }
    reply(StatusCode::OK, outcome)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StuckRequest {
    zone: String,
    stuck: bool,
}

async fn stuck(
    State(state): State<FreezeRoutes>,
    headers: HeaderMap,
    Json(body): Json<StuckRequest>,
) -> Reply {
    if let Err(r) = authorized(&state, &headers) {
        return r;
    }
    let Some(owner) = state
        .assignment
        .iter()
        .find(|(z, _)| *z == body.zone)
        .map(|(_, h)| h.clone())
    else {
        return reply(
            StatusCode::BAD_REQUEST,
            json!({ "ok": false, "error": "unknown zone" }),
        );
    };
    if owner == state.me.id {
        state.rituals.set(&body.zone, body.stuck);
        return reply(
            StatusCode::OK,
            json!({ "ok": true, "host": owner, "stuck": state.rituals.list() }),
        );
    }
    let Some(peer) = state.peers.iter().find(|p| p.id == owner) else {
        return reply(
            StatusCode::BAD_GATEWAY,
            json!({ "ok": false, "error": "owner unknown" }),
        );
    };
    match post_json(
        &peer.admin_url,
        "/dev/freeze/stuck",
        &state.admin_secret,
        &json!({ "zone": body.zone, "stuck": body.stuck }),
        Duration::from_secs(2),
    )
    .await
    {
        Ok((status, value)) => reply(
            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
            value,
        ),
        Err(e) => reply(StatusCode::BAD_GATEWAY, json!({ "ok": false, "error": e })),
    }
}

// ---------------------------------------------------------------- CLI

fn parse_peer(spec: &str) -> std::io::Result<HostAddr> {
    let (id, addr) = spec
        .split_once('=')
        .ok_or_else(|| std::io::Error::other("--peer must be id=host:port"))?;
    Ok(HostAddr::new(id, addr))
}

/// Spawn the second host (this binary, `--peer`), wait for its address.
fn spawn_secondary(
    args: &FreezeArgs,
    primary: &HostAddr,
    secret: &str,
    admin: &str,
) -> std::io::Result<(std::process::Child, HostAddr)> {
    let exe = std::env::current_exe()?;
    let mut command = std::process::Command::new(exe);
    command
        .arg("freeze")
        .args([
            "--host",
            &args.demo.host,
            "--port",
            "0",
            "--host-id",
            "beta",
        ])
        .arg("--peer")
        .arg(format!(
            "{}={}",
            primary.id,
            primary.admin_url.trim_start_matches("http://")
        ))
        .args(["--stage-ms", &args.stage_ms.to_string()])
        .env("DISTRIBUTED_GAME_SECRET", secret)
        .env("DG_DEMO_ADMIN_SECRET", admin)
        .env_remove("GAME_DEMOS_PORT")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit());
    if args.demo.dev {
        command.arg("--dev");
    }
    let mut child = command.spawn()?;
    let stdout = child.stdout.take().expect("piped");
    let mut lines = BufReader::new(stdout).lines();
    let addr = loop {
        match lines.next() {
            Some(Ok(line)) => {
                if let Some(url) = line.strip_prefix("LISTENING http://") {
                    break url.trim().to_string();
                }
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(std::io::Error::other("second host failed to start"));
            }
        }
    };
    // Keep draining so the child never blocks on a full pipe.
    std::thread::spawn(move || for _ in lines.by_ref() {});
    Ok((child, HostAddr::new("beta", addr)))
}

/// [`common::shutdown_signal`], plus (secondary host) the primary going away:
/// the secondary exits when its stdin, held by the primary, closes.
async fn shutdown_signal(watch_stdin: bool) {
    let (orphaned_tx, orphaned) = tokio::sync::oneshot::channel::<()>();
    if watch_stdin {
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = std::io::Read::read_to_end(&mut std::io::stdin(), &mut sink);
            let _ = orphaned_tx.send(());
        });
    } else {
        std::mem::forget(orphaned_tx);
    }
    tokio::select! {
        _ = common::shutdown_signal() => {}
        _ = orphaned => {}
    }
}

/// `game-demos freeze`: the primary host (spawns the secondary) or, with
/// `--peer`, the secondary.
pub async fn run(args: FreezeArgs) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind((args.demo.host.as_str(), args.demo.port)).await?;
    let addr = listener.local_addr()?;
    let secret = args.demo.secret.clone().unwrap_or_else(|| random_hex(32));
    let admin = args.admin_secret.clone().unwrap_or_else(|| random_hex(24));
    let me = HostAddr::new(args.host_id.clone(), addr.to_string());
    let secondary = args.peer.is_some();
    let (peers, child) = match &args.peer {
        Some(spec) => (vec![parse_peer(spec)?], None),
        None => {
            let (child, beta) = spawn_secondary(&args, &me, &secret, &admin)?;
            (vec![beta], Some(child))
        }
    };
    let (primary_id, secondary_id) = if secondary {
        (peers[0].id.clone(), me.id.clone())
    } else {
        (me.id.clone(), peers[0].id.clone())
    };
    let spec = HostSpec {
        me: me.clone(),
        peers: peers.clone(),
        assignment: assignment(&primary_id, &secondary_id),
        key: TicketKey::new(secret),
        admin_secret: admin.clone(),
        dev: args.demo.dev,
        stage: Duration::from_millis(args.stage_ms),
        ack_timeout: ACK_TIMEOUT,
    };
    let host = start_host(listener, spec, shutdown_signal(secondary))?;
    // Parsed by run.sh and the client tests: keep the format.
    println!("LISTENING http://{}", host.addr);
    if secondary {
        println!("freeze host {} (Ember Fields)", me.id);
    } else {
        println!(
            "demo freeze · zones {HUB} {MEADOW} @{} + {EMBER} @{} (second process) · admin secret {admin} (admin panel: /freeze/?admin={admin}) · dev routes {}",
            me.id,
            peers[0].admin_url,
            if args.demo.dev { "ON" } else { "off" }
        );
    }
    let result = host.task.await.map_err(std::io::Error::other)?;
    if let Some(mut child) = child {
        // Closing its stdin is the shutdown signal; kill as a fallback.
        drop(child.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if child.try_wait()?.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if child.try_wait()?.is_none() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    result
}
