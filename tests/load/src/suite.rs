//! Default aggregate load matrix and runner.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use distributed::bus::{InMemoryBus, NatsBus, PostgresBus, SqliteBus};
use serde::Serialize;
use uuid::Uuid;

use crate::client::{run_invoker, ClientConfig, Scenario};
use crate::host::{
    bind_listener, build_service, connect_sqlite, serve_listener, sqlite_pool_size,
    wait_for_health, HostConfig,
};
use crate::invoke::{BusRuntime, Invoker};
use crate::kinds::{BusKind, DispatchKind, LockKind, RepoKind};
use distributed::microsvc::grpc::CommandServiceClient;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

#[derive(Clone, Debug)]
pub struct Cell {
    pub repo: RepoKind,
    pub dispatch: DispatchKind,
    pub bus: Option<BusKind>,
    pub lock: LockKind,
    pub scenario: Scenario,
    pub snapshot_frequency: Option<u64>,
    pub pipelined: bool,
}

impl Cell {
    fn base(
        repo: RepoKind,
        dispatch: DispatchKind,
        bus: Option<BusKind>,
        lock: LockKind,
        scenario: Scenario,
    ) -> Self {
        Self {
            repo,
            dispatch,
            bus,
            lock,
            scenario,
            snapshot_frequency: None,
            pipelined: false,
        }
    }

    pub fn name(&self) -> String {
        let bus = self
            .bus
            .map(|b| format!(" bus={}", b.as_str()))
            .unwrap_or_default();
        let snap = match self.snapshot_frequency {
            Some(n) => format!(" snap={n}"),
            None => " snap=off".into(),
        };
        let mode = if self.pipelined {
            " mode=pipelined"
        } else if self.dispatch == DispatchKind::Bus {
            " mode=applied"
        } else {
            ""
        };
        format!(
            "repo={} dispatch={}{bus} lock={}{snap}{mode} scenario={}",
            self.repo.as_str(),
            self.dispatch.as_str(),
            self.lock.as_str(),
            self.scenario.as_str()
        )
    }
}

#[derive(Clone, Debug)]
pub struct SuiteConfig {
    pub duration: Duration,
    pub warmup: Duration,
    pub concurrency: usize,
    pub database_url: Option<String>,
    pub include_locks: bool,
    pub include_external: bool,
    pub include_snapshots: bool,
    pub snapshot_frequencies: Vec<u64>,
    /// Dispatch modes to overlay snapshot frequencies on. Default is direct
    /// only so snapshot I/O is not mixed with HTTP/gRPC cost. Pass
    /// `--snapshot-dispatch direct,http,grpc` to widen the experiment.
    pub snapshot_dispatches: Vec<DispatchKind>,
    pub scenarios: Vec<Scenario>,
}

impl Default for SuiteConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(5),
            warmup: Duration::from_secs(1),
            concurrency: 16,
            database_url: None,
            include_locks: true,
            include_external: true,
            include_snapshots: true,
            snapshot_frequencies: vec![1, 10, 100],
            snapshot_dispatches: vec![DispatchKind::Direct, DispatchKind::Http, DispatchKind::Grpc],
            scenarios: vec![Scenario::UniqueCreate, Scenario::HotIncrement],
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct CellOutcome {
    pub cell: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report: Option<crate::stats::RunReport>,
}

/// Suite 1: persistence × (direct|http|grpc) + paired bus cells, snapshots off.
/// Suite 1a: same persistence × (direct|http|grpc) with matching durable locks.
/// Suite 1b: every suite 1 / 1a / bus cell × snapshot frequencies {1,10,100}.
///           `--snapshot-dispatch` can narrow the ingress overlay; buses always
///           get the same frequencies when snapshots are enabled.
pub fn default_cells(config: &SuiteConfig) -> Vec<Cell> {
    let mut bases = Vec::new();
    for scenario in &config.scenarios {
        for repo in [RepoKind::Memory, RepoKind::Sqlite, RepoKind::Postgres] {
            for dispatch in [DispatchKind::Direct, DispatchKind::Http, DispatchKind::Grpc] {
                bases.push(Cell::base(
                    repo,
                    dispatch,
                    None,
                    LockKind::Memory,
                    *scenario,
                ));
            }
        }
        bases.extend([
            bus_cell(RepoKind::Memory, BusKind::Memory, *scenario),
            bus_cell(RepoKind::Sqlite, BusKind::Sqlite, *scenario),
            bus_cell(RepoKind::Postgres, BusKind::Postgres, *scenario),
            bus_cell(RepoKind::Postgres, BusKind::Nats, *scenario),
            bus_cell(RepoKind::Postgres, BusKind::Kafka, *scenario),
            bus_cell(RepoKind::Postgres, BusKind::Rabbitmq, *scenario),
        ]);
        if config.include_locks {
            for dispatch in [DispatchKind::Direct, DispatchKind::Http, DispatchKind::Grpc] {
                bases.push(Cell::base(
                    RepoKind::Sqlite,
                    dispatch,
                    None,
                    LockKind::Sqlite,
                    *scenario,
                ));
                bases.push(Cell::base(
                    RepoKind::Postgres,
                    dispatch,
                    None,
                    LockKind::Postgres,
                    *scenario,
                ));
            }
        }
    }
    let mut cells = bases.clone();
    if config.include_snapshots {
        for base in &bases {
            if !overlays_snapshots(base, config) {
                continue;
            }
            for &frequency in &config.snapshot_frequencies {
                let mut cell = base.clone();
                cell.snapshot_frequency = Some(frequency);
                cells.push(cell);
            }
        }
    }
    let mut pipelined = Vec::new();
    for cell in &cells {
        if cell.dispatch == DispatchKind::Bus
            && cell.snapshot_frequency.is_none()
            && cell.scenario == Scenario::UniqueCreate
            && !cell.pipelined
        {
            let mut next = cell.clone();
            next.pipelined = true;
            pipelined.push(next);
        }
    }
    cells.extend(pipelined);
    cells
}

fn overlays_snapshots(cell: &Cell, config: &SuiteConfig) -> bool {
    if cell.dispatch == DispatchKind::Bus {
        return true;
    }
    config.snapshot_dispatches.contains(&cell.dispatch)
}

fn bus_cell(repo: RepoKind, bus: BusKind, scenario: Scenario) -> Cell {
    Cell::base(
        repo,
        DispatchKind::Bus,
        Some(bus),
        LockKind::Memory,
        scenario,
    )
}

pub async fn run_suite(config: SuiteConfig, cells: Vec<Cell>) -> Vec<CellOutcome> {
    let mut outcomes = Vec::with_capacity(cells.len());
    for cell in cells {
        let name = cell.name();
        eprintln!("======== {name} ========");
        if let Some(reason) = skip_reason(&cell, &config) {
            eprintln!("skip: {reason}");
            outcomes.push(CellOutcome {
                cell: name,
                skipped: Some(reason),
                error: None,
                report: None,
            });
            continue;
        }
        match run_cell(&config, &cell).await {
            Ok(report) => {
                eprintln!(
                    "ok={} err={} rps={} p50={} p99={}",
                    report.ok,
                    report.err,
                    report.throughput_rps,
                    report.latency_ms.p50,
                    report.latency_ms.p99
                );
                outcomes.push(CellOutcome {
                    cell: name,
                    skipped: None,
                    error: None,
                    report: Some(report),
                });
            }
            Err(e) => {
                eprintln!("error: {e}");
                outcomes.push(CellOutcome {
                    cell: name,
                    skipped: None,
                    error: Some(e),
                    report: None,
                });
            }
        }
    }
    outcomes
}

fn skip_reason(cell: &Cell, config: &SuiteConfig) -> Option<String> {
    if matches!(cell.repo, RepoKind::Postgres)
        || matches!(cell.bus, Some(BusKind::Postgres))
        || matches!(cell.lock, LockKind::Postgres)
    {
        // Still try; run_cell will fail with a connect error if postgres is down.
    }
    if matches!(cell.bus, Some(BusKind::Nats)) {
        if !config.include_external {
            return Some("external buses disabled (--no-external)".into());
        }
        if !environment_has_value("NATS_URL") {
            return Some("NATS_URL is unset".into());
        }
    }
    if matches!(cell.bus, Some(BusKind::Kafka)) {
        if !cfg!(feature = "kafka") {
            return Some("rebuild with --features kafka".into());
        }
        if !config.include_external {
            return Some("external buses disabled (--no-external)".into());
        }
        if !environment_has_value("KAFKA_BROKERS") {
            return Some("KAFKA_BROKERS is unset".into());
        }
    }
    if matches!(cell.bus, Some(BusKind::Rabbitmq)) {
        if !cfg!(feature = "rabbitmq") {
            return Some("rebuild with --features rabbitmq".into());
        }
        if !config.include_external {
            return Some("external buses disabled (--no-external)".into());
        }
        if !environment_has_value("AMQP_URL") {
            return Some("AMQP_URL is unset".into());
        }
    }
    None
}

fn environment_has_value(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| !value.trim().is_empty())
}

async fn run_cell(config: &SuiteConfig, cell: &Cell) -> Result<crate::stats::RunReport, String> {
    let sqlite_path = PathBuf::from(format!(
        "target/load-{}-{}.sqlite",
        cell.repo.as_str(),
        Uuid::now_v7()
    ));
    let needs_sql_bus = matches!(cell.bus, Some(BusKind::Sqlite));
    let host = HostConfig {
        repo: cell.repo,
        lock: cell.lock,
        bind: "127.0.0.1:0".into(),
        database_url: config.database_url.clone(),
        sqlite_path: sqlite_path.clone(),
        snapshot_frequency: cell.snapshot_frequency,
    };

    let built = if cell.repo == RepoKind::Sqlite && needs_sql_bus {
        // Rebuild sqlite with a wider pool so the bus consumer can share it.
        let inner = connect_sqlite(&sqlite_path, sqlite_pool_size(cell.lock, true))
            .await
            .map_err(|e| e.to_string())?;
        let mut rebuilt = host.clone();
        rebuilt.lock = cell.lock;
        // connect_sqlite already migrated; build_service would wipe the file.
        crate::host::build_service_from_sqlite(inner, cell.lock, cell.snapshot_frequency)
            .map_err(|e| e.to_string())?
    } else {
        build_service(&host).await.map_err(|e| e.to_string())?
    };

    let mut client = client_config(config, cell);
    if matches!(cell.bus, Some(BusKind::Sqlite)) {
        // Concurrent claim+insert+commit on one SQLite file collapses into
        // SQLITE_BUSY. One worker is the honest local-SQLite bus number
        // for both applied and pipelined cells.
        client.concurrency = 1;
    }
    match cell.dispatch {
        DispatchKind::Direct => {
            let invoker = Invoker::Direct(Arc::clone(&built.service));
            run_invoker(invoker, client)
                .await
                .map_err(|e| e.to_string())
        }
        DispatchKind::Http => {
            let listener = bind_listener("127.0.0.1:0")
                .await
                .map_err(|e| e.to_string())?;
            let addr = listener.local_addr().map_err(|e| e.to_string())?;
            let base = format!("http://{addr}");
            let service = Arc::clone(&built.service);
            let server = tokio::spawn(async move {
                let _ = serve_listener(service, listener).await;
            });
            if let Err(error) = wait_for_health(&base, Duration::from_secs(5)).await {
                server.abort();
                return Err(error);
            }
            let mut client = client;
            client.url = base.clone();
            let invoker = Invoker::Http {
                client: reqwest::Client::new(),
                base,
            };
            let report = run_invoker(invoker, client)
                .await
                .map_err(|e| e.to_string());
            server.abort();
            report
        }
        DispatchKind::Grpc => {
            let (server, grpc_client, endpoint) = start_grpc(Arc::clone(&built.service)).await?;
            let mut client = client;
            client.url = endpoint;
            let invoker = Invoker::Grpc(grpc_client);
            let report = run_invoker(invoker, client)
                .await
                .map_err(|e| e.to_string());
            server.abort();
            report
        }
        DispatchKind::Bus => {
            let bus_kind = cell.bus.ok_or("bus dispatch requires --bus")?;
            let (wrap, runtime) = start_bus(config, cell, &host, &built, bus_kind).await?;
            let invoker = Invoker::Bus(runtime.invoker.clone());
            let report = run_invoker(invoker, client)
                .await
                .map_err(|e| e.to_string());
            runtime.stop();
            drop(wrap);
            report
        }
    }
}

fn client_config(config: &SuiteConfig, cell: &Cell) -> ClientConfig {
    ClientConfig {
        url: String::new(),
        scenario: cell.scenario,
        concurrency: config.concurrency,
        duration: config.duration,
        warmup: config.warmup,
        repo: Some(cell.repo.as_str().into()),
        dispatch: Some(cell.dispatch.as_str().into()),
        bus: cell.bus.map(|b| b.as_str().into()),
        lock: Some(cell.lock.as_str().into()),
        snapshot_frequency: cell.snapshot_frequency,
        cell: Some(cell.name()),
        pipelined: cell.pipelined,
    }
}

async fn start_bus(
    _config: &SuiteConfig,
    cell: &Cell,
    _host: &HostConfig,
    built: &crate::host::BuiltService,
    bus_kind: BusKind,
) -> Result<(BusToken, BusRuntime), String> {
    let namespace = format!("load-{}", Uuid::now_v7());
    match bus_kind {
        BusKind::Memory => {
            let bus = InMemoryBus::new();
            let runtime =
                BusRuntime::start(bus.clone(), Arc::clone(&built.service), cell.pipelined);
            Ok((BusToken::Memory(bus), runtime))
        }
        BusKind::Sqlite => {
            let repo = built
                .sqlite
                .clone()
                .ok_or("sqlite bus requires sqlite repo")?;
            let bus = SqliteBus::new(repo.pool().clone()).group("load-counter");
            bus.ensure_tables().await.map_err(|e| e.to_string())?;
            let runtime =
                BusRuntime::start(bus.clone(), Arc::clone(&built.service), cell.pipelined);
            Ok((BusToken::Sqlite(bus), runtime))
        }
        BusKind::Postgres => {
            let repo = built
                .postgres
                .clone()
                .ok_or("postgres bus requires postgres repo")?;
            let bus = PostgresBus::new(repo.pool().clone()).group("load-counter");
            bus.ensure_tables().await.map_err(|e| e.to_string())?;
            // The compose database is shared across cells and leftover
            // `counter.*` queue rows starve applied oneshots (the consumer
            // drains stale initialize commands instead of this cell's).
            sqlx::query("DELETE FROM bus_queue WHERE name = ANY($1)")
                .bind(&[crate::host::INITIALIZE, crate::host::INCREMENT] as &[&str])
                .execute(repo.pool())
                .await
                .map_err(|e| e.to_string())?;
            let runtime =
                BusRuntime::start(bus.clone(), Arc::clone(&built.service), cell.pipelined);
            Ok((BusToken::Postgres(bus), runtime))
        }
        BusKind::Nats => {
            let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into());
            let bus = NatsBus::connect(&url)
                .group("load-counter")
                .namespace(&namespace)
                .with_fetch_timeout(Duration::from_millis(200))
                .await
                .map_err(|e| e.to_string())?;
            let runtime =
                BusRuntime::start(bus.clone(), Arc::clone(&built.service), cell.pipelined);
            // Give JetStream consumers a moment to bind.
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok((BusToken::Nats(bus), runtime))
        }
        BusKind::Kafka => {
            #[cfg(feature = "kafka")]
            {
                let brokers =
                    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "127.0.0.1:9092".into());
                let bus = distributed::bus::KafkaBus::connect(&brokers)
                    .group("load-counter")
                    .namespace(&namespace)
                    .with_fetch_timeout(Duration::from_secs(2))
                    .await
                    .map_err(|e| e.to_string())?;
                let runtime =
                    BusRuntime::start(bus.clone(), Arc::clone(&built.service), cell.pipelined);
                tokio::time::sleep(Duration::from_secs(2)).await;
                Ok((BusToken::Kafka(bus), runtime))
            }
            #[cfg(not(feature = "kafka"))]
            {
                let _ = (built, namespace);
                Err("rebuild with --features kafka".into())
            }
        }
        BusKind::Rabbitmq => {
            #[cfg(feature = "rabbitmq")]
            {
                let url = std::env::var("AMQP_URL")
                    .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/%2f".into());
                let bus = distributed::bus::RabbitBus::connect(&url)
                    .group("load-counter")
                    .namespace(&namespace)
                    .await
                    .map_err(|e| e.to_string())?;
                let runtime = BusRuntime::start(bus, Arc::clone(&built.service), cell.pipelined);
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok((BusToken::Rabbitmq, runtime))
            }
            #[cfg(not(feature = "rabbitmq"))]
            {
                let _ = (built, namespace);
                Err("rebuild with --features rabbitmq".into())
            }
        }
    }
}

async fn start_grpc(
    service: Arc<distributed::microsvc::Service>,
) -> Result<
    (
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        CommandServiceClient<tonic::transport::Channel>,
        String,
    ),
    String,
> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    let grpc_svc = distributed::microsvc::grpc_server(service);
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(grpc_svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    let endpoint = format!("http://{addr}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let client = loop {
        match CommandServiceClient::connect(endpoint.clone()).await {
            Ok(client) => break client,
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    server.abort();
                    return Err(format!("gRPC connect to {endpoint} failed: {e}"));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    };
    Ok((server, client, endpoint))
}

/// Keeps the bus alive for the cell.
pub enum BusToken {
    Memory(InMemoryBus),
    Sqlite(SqliteBus),
    Postgres(PostgresBus),
    Nats(NatsBus),
    #[cfg(feature = "kafka")]
    Kafka(distributed::bus::KafkaBus),
    #[cfg(feature = "rabbitmq")]
    Rabbitmq,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Scenario;

    #[test]
    fn default_matrix_covers_every_requested_axis() {
        let cells = default_cells(&SuiteConfig::default());
        assert!(cells
            .iter()
            .any(|c| c.bus == Some(BusKind::Rabbitmq) && c.scenario == Scenario::HotIncrement));
        assert!(cells
            .iter()
            .any(|c| c.bus == Some(BusKind::Kafka) && c.snapshot_frequency == Some(1)));
        assert!(cells.iter().any(|c| {
            c.dispatch == DispatchKind::Http
                && c.lock == LockKind::Postgres
                && c.snapshot_frequency == Some(100)
                && c.scenario == Scenario::HotIncrement
        }));
        assert!(cells.iter().any(|c| {
            c.dispatch == DispatchKind::Grpc
                && c.snapshot_frequency == Some(10)
                && c.repo == RepoKind::Sqlite
        }));
        assert!(cells.iter().any(|c| {
            c.pipelined && c.bus == Some(BusKind::Nats) && c.scenario == Scenario::UniqueCreate
        }));
    }

    #[test]
    fn snapshot_dispatch_can_narrow_ingress_but_still_overlays_buses() {
        let config = SuiteConfig {
            include_locks: false,
            include_snapshots: true,
            snapshot_frequencies: vec![100],
            snapshot_dispatches: vec![DispatchKind::Direct],
            scenarios: vec![Scenario::HotIncrement],
            ..SuiteConfig::default()
        };
        let snaps: Vec<_> = default_cells(&config)
            .into_iter()
            .filter(|cell| cell.snapshot_frequency.is_some())
            .collect();
        assert!(snaps.iter().any(|c| c.dispatch == DispatchKind::Direct));
        assert!(snaps.iter().any(|c| c.dispatch == DispatchKind::Bus));
        assert!(snaps.iter().all(|c| c.dispatch != DispatchKind::Http));
        assert!(snaps.iter().all(|c| c.snapshot_frequency == Some(100)));
    }
}
