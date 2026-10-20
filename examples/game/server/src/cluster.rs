//! Multi-host plumbing for the world demos ([`crate::grid`],
//! [`crate::ripple`]): each host is its own `game-demos <demo>` process with
//! `--host-id` and one `--peer ID=HOST:PORT` per other host.
//!
//! What a host gets on top of [`crate::common`]:
//!
//! - a [`ZoneDirectory`] over every host (the demo chooses: consistent hash,
//!   placement by tier …): a Hello for a zone owned elsewhere gets
//!   `Redirect`, handoffs and tier-up events cross hosts over the admin
//!   routes (`/admin/transfer`, `/admin/child-event`);
//! - the admin secret, derived from the shared ticket secret (all hosts of
//!   a world must be started with the same `--secret`);
//! - zone ownership and hibernation on celld (`--celld-url` / `CELLD_URL`
//!   with `DISTRIBUTED_INTERNAL_SECRET`, the `examples/game/cells` worker):
//!   `CellZoneOwnership`, one `ZoneLease` cell per zone whose alarm declares
//!   `zone.lease_expired` when this host stops renewing, and
//!   `CellZoneStateStore`, one `ZoneState` cell per zone holding its
//!   hibernation snapshot, fenced by the lease token. There is no local
//!   fallback: every host of a world shares the same celld;
//! - guest display names shared with every peer: `/dev/ticket` on any host
//!   forwards the new `principal → name` to the others before it answers,
//!   so a zone on another host greets the player by name.
//!
//! **Dev only**, like everything under `/dev`.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::post;
use axum::{Json, Router};
use distributed_game::celld::{CellZoneOwnership, CellZoneStateStore};
use distributed_game::net::peer::{post_json, ADMIN_SECRET_HEADER};
use distributed_game::net::{GameServer, NetSim, ServerConfig, TicketKey};
use distributed_game::world::{
    HostAddr, MemoryZoneStateStore, ZoneDirectory, ZoneOwnership, ZoneStateStore,
};
use serde_json::{json, Value};

use crate::common::{
    self, dev, random_hex, server_config, CelldOptions, Demo, DemoArgs, DemoOptions, Guests,
    RunningDemo,
};

/// CLI options of a multi-host demo.
#[derive(Debug, Clone, clap::Args)]
pub struct ClusterArgs {
    #[command(flatten)]
    #[allow(missing_docs)]
    pub demo: DemoArgs,
    /// This host's id (directory entry, ownership holder prefix).
    #[arg(long, default_value = "A")]
    pub host_id: String,
    /// Another host of the same world, `ID=HOST:PORT` (repeatable). Every
    /// host must list every other host.
    #[arg(long = "peer", value_name = "ID=HOST:PORT")]
    pub peers: Vec<String>,
    /// Hibernate a zone after it has been Dormant this long (ms); it turns
    /// Dormant after half of this without clients.
    #[arg(long, default_value_t = 4000)]
    pub hibernate_after_ms: u64,
    /// celld serving the `zone-lease` and `zone-state` cells
    /// (`examples/game/cells`): zone ownership and hibernation snapshots.
    #[arg(long, env = "CELLD_URL")]
    pub celld_url: String,
    /// Internal secret for the celld cells.
    #[arg(long, env = "DISTRIBUTED_INTERNAL_SECRET", hide_env_values = true)]
    pub internal_secret: String,
}

/// In-process cluster options (tests and [`run_cluster`]).
#[derive(Clone)]
pub struct ClusterOptions {
    /// This host's id.
    pub host_id: String,
    /// Every other host.
    pub peers: Vec<HostAddr>,
    /// Dormant time before hibernation.
    pub hibernate_after: Duration,
    /// celld holding zone ownership (`ZoneLease`) and hibernation
    /// snapshots (`ZoneState`).
    pub celld: CelldOptions,
}

/// Parse `ID=HOST:PORT`.
pub fn parse_peer(spec: &str) -> Result<HostAddr, String> {
    let (id, addr) = spec
        .split_once('=')
        .ok_or_else(|| format!("peer {spec:?}: expected ID=HOST:PORT"))?;
    if id.is_empty() || addr.is_empty() {
        return Err(format!("peer {spec:?}: expected ID=HOST:PORT"));
    }
    Ok(HostAddr::new(id, addr))
}

/// What a running host knows about its world.
pub struct World {
    /// This host.
    pub host: HostAddr,
    /// Every host, sorted by id (this one included).
    pub hosts: Vec<HostAddr>,
    /// Zone placement.
    pub directory: Arc<dyn ZoneDirectory>,
    /// Hibernation snapshots (celld `ZoneState` cells).
    pub state: Arc<dyn ZoneStateStore>,
    /// Shared admin secret (peer calls).
    pub admin_secret: String,
    /// Display names (shared with peers by `/dev/ticket`).
    pub guests: Guests,
    /// Dormant time before hibernation.
    pub hibernate_after: Duration,
}

impl World {
    /// A world of one host that is not serving (`Demo::server`; snapshots
    /// in memory).
    pub fn standalone<D: ClusterDemo>(
        host_id: &str,
        hibernate_after: Duration,
        guests: Guests,
    ) -> Self {
        let host = HostAddr::new(host_id, "127.0.0.1:0");
        let hosts = vec![host.clone()];
        Self {
            directory: D::directory(&hosts),
            host,
            hosts,
            state: Arc::new(MemoryZoneStateStore::new()),
            admin_secret: String::new(),
            guests,
            hibernate_after,
        }
    }

    /// Hosts other than this one.
    pub fn peers(&self) -> impl Iterator<Item = &HostAddr> + '_ {
        self.hosts.iter().filter(|h| h.id != self.host.id)
    }

    /// Owner host id of `zone_id` (this host when the directory is silent).
    pub fn owner(&self, zone_id: &str) -> String {
        self.directory
            .locate(zone_id)
            .map_or_else(|| self.host.id.clone(), |h| h.id)
    }

    /// True when the request carries the admin secret.
    pub fn authorized(&self, headers: &HeaderMap) -> bool {
        headers
            .get(ADMIN_SECRET_HEADER)
            .is_some_and(|v| v.as_bytes() == self.admin_secret.as_bytes())
    }

    /// POST `body` to `path` on every peer; `(host id, reply)` per answer.
    pub async fn ask_peers(&self, path: &str, body: &Value) -> Vec<(String, Value)> {
        let mut out = Vec::new();
        for peer in self.peers() {
            let reply = post_json(
                &peer.admin_url,
                path,
                &self.admin_secret,
                body,
                Duration::from_millis(800),
            )
            .await;
            if let Ok((200, value)) = reply {
                out.push((peer.id.clone(), value));
            }
        }
        out
    }
}

/// A demo that runs as one host of a multi-host world.
pub trait ClusterDemo: Demo {
    /// Zone placement over `hosts` (sorted by id). Must give the same answer
    /// on every host.
    fn directory(hosts: &[HostAddr]) -> Arc<dyn ZoneDirectory>;

    /// Build this host's server (`config` already has the admin secret and
    /// `hibernate_after`). Host id, directory, ownership and the state store
    /// are attached afterwards; do not clone the server here.
    fn cluster_server(config: ServerConfig, world: &World) -> GameServer<Self::Zone>;

    /// Extra dev routes (mounted with `--dev` only).
    fn cluster_routes(_server: GameServer<Self::Zone>, _world: Arc<World>) -> Router {
        Router::new()
    }
}

/// A started host.
pub struct RunningHost<D: Demo> {
    /// The server, its address and guests.
    pub demo: RunningDemo<D>,
    /// What the host knows about its world.
    pub world: Arc<World>,
}

/// Admin secret every host of a world derives from the shared ticket key.
pub fn admin_secret(key: &TicketKey) -> String {
    key.hmac(b"game-demos|admin")[..16]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Start host `cluster.host_id` of demo `D` on `listener`.
pub fn spawn_cluster<D: ClusterDemo>(
    listener: tokio::net::TcpListener,
    options: DemoOptions,
    cluster: ClusterOptions,
    signal: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<RunningHost<D>> {
    let addr: SocketAddr = listener.local_addr()?;
    let host = HostAddr::new(cluster.host_id.clone(), addr.to_string());
    let mut hosts: Vec<HostAddr> = cluster
        .peers
        .iter()
        .filter(|p| p.id != host.id)
        .cloned()
        .collect();
    hosts.push(host.clone());
    hosts.sort();
    let directory = D::directory(&hosts);
    let service_id = format!("game-demos-{}", D::NAME);
    let state = CellZoneStateStore::new(
        cluster.celld.url.clone(),
        cluster.celld.secret.clone(),
        service_id.clone(),
        host.id.clone(),
    )
    .map_err(std::io::Error::other)?;
    let guests = Guests::default();
    let world = Arc::new(World {
        host,
        hosts,
        directory: directory.clone(),
        state: Arc::new(state),
        admin_secret: admin_secret(&options.key),
        guests: guests.clone(),
        hibernate_after: cluster.hibernate_after,
    });

    let mut config = server_config(options.key.clone(), options.net_sim);
    config.admin_secret = Some(world.admin_secret.clone());
    config.hibernate_after = Some(cluster.hibernate_after);
    config.host.dormant_after = cluster.hibernate_after / 2;
    let ownership: Arc<dyn ZoneOwnership> = Arc::new(
        CellZoneOwnership::new(
            cluster.celld.url.clone(),
            cluster.celld.secret.clone(),
            service_id,
            world.host.id.clone(),
        )
        .map_err(std::io::Error::other)?,
    );
    let server = D::cluster_server(config, &world)
        .with_host_id(world.host.id.clone())
        .with_directory(directory)
        .with_ownership(ownership)
        .with_state_store(world.state.clone());

    let extra = if options.dev {
        let tickets = dev::routes::<D>(server.clone(), options.key, guests.clone()).layer(
            middleware::from_fn_with_state(world.clone(), share_guest_names),
        );
        tickets
            .merge(
                Router::new()
                    .route("/dev/cluster/guest", post(receive_guest))
                    .with_state(world.clone()),
            )
            .merge(D::cluster_routes(server.clone(), world.clone()))
    } else {
        Router::new()
    };
    let task = tokio::spawn(server.clone().serve_with(listener, extra, signal));
    Ok(RunningHost {
        demo: RunningDemo {
            addr,
            server,
            guests,
            task,
        },
        world,
    })
}

/// CLI entry: bind, print the address, serve until Ctrl-C / SIGTERM.
pub async fn run_cluster<D: ClusterDemo>(args: ClusterArgs) -> std::io::Result<()> {
    let demo = args.demo;
    let listener = tokio::net::TcpListener::bind((demo.host.as_str(), demo.port)).await?;
    let secret = demo.secret.unwrap_or_else(|| random_hex(64));
    let peers = args
        .peers
        .iter()
        .map(|p| parse_peer(p))
        .collect::<Result<Vec<_>, _>>()
        .map_err(std::io::Error::other)?;
    let options = DemoOptions {
        key: TicketKey::new(secret),
        dev: demo.dev,
        net_sim: NetSim::from_env(),
    };
    let cluster = ClusterOptions {
        host_id: args.host_id,
        peers,
        hibernate_after: Duration::from_millis(args.hibernate_after_ms.max(100)),
        celld: CelldOptions::new(args.celld_url, args.internal_secret)
            .map_err(std::io::Error::other)?,
    };
    let running = spawn_cluster::<D>(listener, options, cluster, common::shutdown_signal())?;
    // Parsed by run.sh and the client tests: keep the format.
    println!("LISTENING http://{}", running.demo.addr);
    let hosts: Vec<String> = running
        .world
        .hosts
        .iter()
        .map(|h| format!("{}={}", h.id, h.ws_url))
        .collect();
    println!(
        "demo {} · host {} of [{}] · zone {} · dev routes {}",
        D::NAME,
        running.world.host.id,
        hosts.join(", "),
        D::DEFAULT_ZONE,
        if demo.dev { "ON" } else { "off" }
    );
    running.demo.task.await.map_err(std::io::Error::other)?
}

/// After a successful `/dev/ticket`, tell every peer the guest's display
/// name (awaited, so a redirected Hello finds it on the owning host).
async fn share_guest_names(
    State(world): State<Arc<World>>,
    request: Request,
    next: Next,
) -> Response {
    let is_ticket = request.uri().path() == "/dev/ticket";
    let response = next.run(request).await;
    if !is_ticket || !response.status().is_success() || world.peers().next().is_none() {
        return response;
    }
    let (parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, 64 * 1024)
        .await
        .unwrap_or_default();
    if let Ok(ticket) = serde_json::from_slice::<Value>(&bytes) {
        let body = json!({ "principal": ticket["principal"], "name": ticket["name"] });
        world.ask_peers("/dev/cluster/guest", &body).await;
    }
    Response::from_parts(parts, Body::from(bytes))
}

/// `POST /dev/cluster/guest {principal, name}` (admin secret): a peer
/// registered a guest.
async fn receive_guest(
    State(world): State<Arc<World>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> StatusCode {
    if !world.authorized(&headers) {
        return StatusCode::UNAUTHORIZED;
    }
    let (Some(principal), Some(name)) = (body["principal"].as_str(), body["name"].as_str()) else {
        return StatusCode::BAD_REQUEST;
    };
    if !principal.starts_with("guest-") {
        return StatusCode::BAD_REQUEST;
    }
    world.guests.set_name(principal, sanitize_name(name));
    StatusCode::OK
}

/// Same rules as the dev ticket endpoint (letters, digits, ` -_`, ≤ 16).
fn sanitize_name(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_alphanumeric() || matches!(c, ' ' | '-' | '_'))
        .take(16)
        .collect::<String>()
        .trim()
        .to_string()
}

/// 64 random hex characters (OS-seeded SipHash keys; dev quality).
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peers_parse() {
        let peer = parse_peer("B=127.0.0.1:2702").unwrap();
        assert_eq!(peer.id, "B");
        assert_eq!(peer.ws_url, "ws://127.0.0.1:2702");
        assert!(parse_peer("127.0.0.1:2702").is_err());
        assert!(parse_peer("=x").is_err());
    }
}
